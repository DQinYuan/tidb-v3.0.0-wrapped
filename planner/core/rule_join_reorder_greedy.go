// Copyright 2018 PingCAP, Inc.
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
	"math"
	"sort"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges []*expression.ScalarFunction
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//   For base node, its CumCount equals to the sum of the count of its subtree.
//   See baseNodeCumCost for more details.
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 0)
	for _, node := range joinNodePlans {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 4)
		_, err := node.recursiveDeriveStats()
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 6)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 5)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 1)
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 7)
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	trace_util_0.Count(_rule_join_reorder_greedy_00000, 2)
	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 8)
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 10)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 9)
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	trace_util_0.Count(_rule_join_reorder_greedy_00000, 3)
	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 11)
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 13)
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		for i, node := range s.curJoinGroup {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 16)
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if newJoin == nil {
				trace_util_0.Count(_rule_join_reorder_greedy_00000, 19)
				continue
			}
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 17)
			_, err := newJoin.recursiveDeriveStats()
			if err != nil {
				trace_util_0.Count(_rule_join_reorder_greedy_00000, 20)
				return nil, err
			}
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 18)
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			if bestCost > curCost {
				trace_util_0.Count(_rule_join_reorder_greedy_00000, 21)
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 14)
		if bestJoin == nil {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 22)
			break
		}
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 15)
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.otherConds = finalRemainOthers
	}
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 12)
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalPlan) (LogicalPlan, []expression.Expression) {
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 23)
	var usedEdges []*expression.ScalarFunction
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	for _, edge := range s.eqEdges {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 27)
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftNode.Schema().Contains(lCol) && rightNode.Schema().Contains(rCol) {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 28)
			usedEdges = append(usedEdges, edge)
		} else {
			trace_util_0.Count(_rule_join_reorder_greedy_00000, 29)
			if rightNode.Schema().Contains(lCol) && leftNode.Schema().Contains(rCol) {
				trace_util_0.Count(_rule_join_reorder_greedy_00000, 30)
				newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
				usedEdges = append(usedEdges, newSf)
			}
		}
	}
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 24)
	if len(usedEdges) == 0 {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 31)
		return nil, nil
	}
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 25)
	var otherConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftNode.Schema(), rightNode.Schema())
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		trace_util_0.Count(_rule_join_reorder_greedy_00000, 32)
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	trace_util_0.Count(_rule_join_reorder_greedy_00000, 26)
	return s.newJoinWithEdges(leftNode, rightNode, usedEdges, otherConds), remainOtherConds
}

var _rule_join_reorder_greedy_00000 = "planner/core/rule_join_reorder_greedy.go"
