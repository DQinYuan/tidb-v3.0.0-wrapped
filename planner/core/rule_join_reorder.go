// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, LeftJoin(c, d)}.
func extractJoinGroup(p LogicalPlan) (group []LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression) {
	trace_util_0.Count(_rule_join_reorder_00000, 0)
	join, isJoin := p.(*LogicalJoin)
	if !isJoin || join.preferJoinType > uint(0) || join.JoinType != InnerJoin || join.StraightJoin {
		trace_util_0.Count(_rule_join_reorder_00000, 2)
		return []LogicalPlan{p}, nil, nil
	}

	trace_util_0.Count(_rule_join_reorder_00000, 1)
	lhsGroup, lhsEqualConds, lhsOtherConds := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds := extractJoinGroup(join.children[1])

	group = append(group, lhsGroup...)
	group = append(group, rhsGroup...)
	eqEdges = append(eqEdges, join.EqualConditions...)
	eqEdges = append(eqEdges, lhsEqualConds...)
	eqEdges = append(eqEdges, rhsEqualConds...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, lhsOtherConds...)
	otherConds = append(otherConds, rhsOtherConds...)
	return group, eqEdges, otherConds
}

type joinReOrderSolver struct {
}

type jrNode struct {
	p       LogicalPlan
	cumCost float64
}

func (s *joinReOrderSolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_00000, 3)
	return s.optimizeRecursive(p.context(), p)
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *joinReOrderSolver) optimizeRecursive(ctx sessionctx.Context, p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_00000, 4)
	var err error
	curJoinGroup, eqEdges, otherConds := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		trace_util_0.Count(_rule_join_reorder_00000, 7)
		for i := range curJoinGroup {
			trace_util_0.Count(_rule_join_reorder_00000, 11)
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
			if err != nil {
				trace_util_0.Count(_rule_join_reorder_00000, 12)
				return nil, err
			}
		}
		trace_util_0.Count(_rule_join_reorder_00000, 8)
		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:        ctx,
			otherConds: otherConds,
		}
		if len(curJoinGroup) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold {
			trace_util_0.Count(_rule_join_reorder_00000, 13)
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
				eqEdges:                        eqEdges,
			}
			p, err = groupSolver.solve(curJoinGroup)
		} else {
			trace_util_0.Count(_rule_join_reorder_00000, 14)
			{
				dpSolver := &joinReorderDPSolver{
					baseSingleGroupJoinOrderSolver: baseGroupSolver,
				}
				dpSolver.newJoin = dpSolver.newJoinWithEdges
				p, err = dpSolver.solve(curJoinGroup, expression.ScalarFuncs2Exprs(eqEdges))
			}
		}
		trace_util_0.Count(_rule_join_reorder_00000, 9)
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_00000, 15)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_00000, 10)
		return p, nil
	}
	trace_util_0.Count(_rule_join_reorder_00000, 5)
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_join_reorder_00000, 16)
		newChild, err := s.optimizeRecursive(ctx, child)
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_00000, 18)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_00000, 17)
		newChildren = append(newChildren, newChild)
	}
	trace_util_0.Count(_rule_join_reorder_00000, 6)
	p.SetChildren(newChildren...)
	return p, nil
}

type baseSingleGroupJoinOrderSolver struct {
	ctx          sessionctx.Context
	curJoinGroup []*jrNode
	otherConds   []expression.Expression
}

// baseNodeCumCost calculate the cumulative cost of the node in the join group.
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode LogicalPlan) float64 {
	trace_util_0.Count(_rule_join_reorder_00000, 19)
	cost := groupNode.statsInfo().RowCount
	for _, child := range groupNode.Children() {
		trace_util_0.Count(_rule_join_reorder_00000, 21)
		cost += s.baseNodeCumCost(child)
	}
	trace_util_0.Count(_rule_join_reorder_00000, 20)
	return cost
}

// makeBushyJoin build bushy tree for the nodes which have no equal condition to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) LogicalPlan {
	trace_util_0.Count(_rule_join_reorder_00000, 22)
	resultJoinGroup := make([]LogicalPlan, 0, (len(cartesianJoinGroup)+1)/2)
	for len(cartesianJoinGroup) > 1 {
		trace_util_0.Count(_rule_join_reorder_00000, 24)
		resultJoinGroup = resultJoinGroup[:0]
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			trace_util_0.Count(_rule_join_reorder_00000, 26)
			if i+1 == len(cartesianJoinGroup) {
				trace_util_0.Count(_rule_join_reorder_00000, 29)
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			trace_util_0.Count(_rule_join_reorder_00000, 27)
			newJoin := s.newCartesianJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1])
			for i := len(s.otherConds) - 1; i >= 0; i-- {
				trace_util_0.Count(_rule_join_reorder_00000, 30)
				cols := expression.ExtractColumns(s.otherConds[i])
				if newJoin.schema.ColumnsIndices(cols) != nil {
					trace_util_0.Count(_rule_join_reorder_00000, 31)
					newJoin.OtherConditions = append(newJoin.OtherConditions, s.otherConds[i])
					s.otherConds = append(s.otherConds[:i], s.otherConds[i+1:]...)
				}
			}
			trace_util_0.Count(_rule_join_reorder_00000, 28)
			resultJoinGroup = append(resultJoinGroup, newJoin)
		}
		trace_util_0.Count(_rule_join_reorder_00000, 25)
		cartesianJoinGroup, resultJoinGroup = resultJoinGroup, cartesianJoinGroup
	}
	trace_util_0.Count(_rule_join_reorder_00000, 23)
	return cartesianJoinGroup[0]
}

func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild LogicalPlan) *LogicalJoin {
	trace_util_0.Count(_rule_join_reorder_00000, 32)
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan {
	trace_util_0.Count(_rule_join_reorder_00000, 33)
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	for _, eqCond := range newJoin.EqualConditions {
		trace_util_0.Count(_rule_join_reorder_00000, 35)
		newJoin.LeftJoinKeys = append(newJoin.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		newJoin.RightJoinKeys = append(newJoin.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	trace_util_0.Count(_rule_join_reorder_00000, 34)
	return newJoin
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (s *baseSingleGroupJoinOrderSolver) calcJoinCumCost(join LogicalPlan, lNode, rNode *jrNode) float64 {
	trace_util_0.Count(_rule_join_reorder_00000, 36)
	return join.statsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

var _rule_join_reorder_00000 = "planner/core/rule_join_reorder.go"
