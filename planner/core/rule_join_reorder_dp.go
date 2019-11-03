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
	"math/bits"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan
}

type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
}

func (s *joinReorderDPSolver) solve(joinGroup []LogicalPlan, eqConds []expression.Expression) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 0)
	for _, node := range joinGroup {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 7)
		_, err := node.recursiveDeriveStats()
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 9)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 8)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 1)
	adjacents := make([][]int, len(s.curJoinGroup))
	totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 10)
		totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}
	// Build Graph for join group
	trace_util_0.Count(_rule_join_reorder_dp_00000, 2)
	for _, cond := range eqConds {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 11)
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx, err := findNodeIndexInGroup(joinGroup, lCol)
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 14)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 12)
		rIdx, err := findNodeIndexInGroup(joinGroup, rCol)
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 15)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 13)
		addEqEdge(lIdx, rIdx, sf)
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 3)
	totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 16)
		cols := expression.ExtractColumns(cond)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 18)
			idx, err := findNodeIndexInGroup(joinGroup, col)
			if err != nil {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 20)
				return nil, err
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 19)
			ids = append(ids, idx)
			mask |= 1 << uint(idx)
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 17)
		totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
			nodeIDs:    ids,
			nodeIDMask: mask,
			expr:       cond,
		})
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 4)
	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	var joins []LogicalPlan
	// BFS the tree.
	for i := 0; i < len(joinGroup); i++ {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 21)
		if visited[i] {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 26)
			continue
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 22)
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		nodeIDMask := uint(0)
		for _, nodeID := range visitID2NodeID {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 27)
			nodeIDMask |= 1 << uint(nodeID)
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 23)
		var subNonEqEdges []joinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 28)
			// If this edge is not the subset of the current sub graph.
			if totalNonEqEdges[i].nodeIDMask&nodeIDMask != totalNonEqEdges[i].nodeIDMask {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 31)
				continue
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 29)
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].nodeIDs {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 32)
				newMask |= 1 << uint(nodeID2VisitID[nodeID])
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 30)
			totalNonEqEdges[i].nodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = append(totalNonEqEdges[:i], totalNonEqEdges[i+1:]...)
		}
		// Do DP on each sub graph.
		trace_util_0.Count(_rule_join_reorder_dp_00000, 24)
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEqEdges, subNonEqEdges)
		if err != nil {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 33)
			return nil, err
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 25)
		joins = append(joins, join)
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 5)
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 34)
		remainedOtherConds = append(remainedOtherConds, edge.expr)
	}
	// Build bushy tree for cartesian joins.
	trace_util_0.Count(_rule_join_reorder_dp_00000, 6)
	return s.makeBushyJoin(joins, remainedOtherConds), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (s *joinReorderDPSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VistID []int) []int {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 35)
	queue := []int{startNode}
	visited[startNode] = true
	var visitID2NodeID []int
	for len(queue) > 0 {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 37)
		curNodeID := queue[0]
		queue = queue[1:]
		nodeID2VistID[curNodeID] = len(visitID2NodeID)
		visitID2NodeID = append(visitID2NodeID, curNodeID)
		for _, adjNodeID := range adjacents[curNodeID] {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 38)
			if visited[adjNodeID] {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 40)
				continue
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 39)
			queue = append(queue, adjNodeID)
			visited[adjNodeID] = true
		}
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 36)
	return visitID2NodeID
}

// dpGraph is the core part of this algorithm.
// It implements the traditional join reorder algorithm: DP by subset using the following formula:
//   bestPlan[S:set of node] = the best one among Join(bestPlan[S1:subset of S], bestPlan[S2: S/S1])
func (s *joinReorderDPSolver) dpGraph(visitID2NodeID, nodeID2VisitID []int, joinGroup []LogicalPlan,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 41)
	nodeCnt := uint(len(visitID2NodeID))
	bestPlan := make([]*jrNode, 1<<nodeCnt)
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := uint(0); i < nodeCnt; i++ {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 44)
		bestPlan[1<<i] = s.curJoinGroup[visitID2NodeID[i]]
	}
	// Enumerate the nodeBitmap from small to big, make sure that S1 must be enumerated before S2 if S1 belongs to S2.
	trace_util_0.Count(_rule_join_reorder_dp_00000, 42)
	for nodeBitmap := uint(1); nodeBitmap < (1 << nodeCnt); nodeBitmap++ {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 45)
		if bits.OnesCount(nodeBitmap) == 1 {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 47)
			continue
		}
		// This loop can iterate all its subset.
		trace_util_0.Count(_rule_join_reorder_dp_00000, 46)
		for sub := (nodeBitmap - 1) & nodeBitmap; sub > 0; sub = (sub - 1) & nodeBitmap {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 48)
			remain := nodeBitmap ^ sub
			if sub > remain {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 53)
				continue
			}
			// If this subset is not connected skip it.
			trace_util_0.Count(_rule_join_reorder_dp_00000, 49)
			if bestPlan[sub] == nil || bestPlan[remain] == nil {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 54)
				continue
			}
			// Get the edge connecting the two parts.
			trace_util_0.Count(_rule_join_reorder_dp_00000, 50)
			usedEdges, otherConds := s.nodesAreConnected(sub, remain, nodeID2VisitID, totalEqEdges, totalNonEqEdges)
			// Here we only check equal condition currently.
			if len(usedEdges) == 0 {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 55)
				continue
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 51)
			join, err := s.newJoinWithEdge(bestPlan[sub].p, bestPlan[remain].p, usedEdges, otherConds)
			if err != nil {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 56)
				return nil, err
			}
			trace_util_0.Count(_rule_join_reorder_dp_00000, 52)
			curCost := s.calcJoinCumCost(join, bestPlan[sub], bestPlan[remain])
			if bestPlan[nodeBitmap] == nil {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 57)
				bestPlan[nodeBitmap] = &jrNode{
					p:       join,
					cumCost: curCost,
				}
			} else {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 58)
				if bestPlan[nodeBitmap].cumCost > curCost {
					trace_util_0.Count(_rule_join_reorder_dp_00000, 59)
					bestPlan[nodeBitmap].p = join
					bestPlan[nodeBitmap].cumCost = curCost
				}
			}
		}
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 43)
	return bestPlan[(1<<nodeCnt)-1].p, nil
}

func (s *joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) ([]joinGroupEqEdge, []expression.Expression) {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 60)
	var (
		usedEqEdges []joinGroupEqEdge
		otherConds  []expression.Expression
	)
	for _, edge := range totalEqEdges {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 63)
		lIdx := uint(oldPos2NewPos[edge.nodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.nodeIDs[1]])
		if ((leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0) || ((leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0) {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 64)
			usedEqEdges = append(usedEqEdges, edge)
		}
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 61)
	for _, edge := range totalNonEqEdges {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 65)
		// If the result is false, means that the current group hasn't covered the columns involved in the expression.
		if edge.nodeIDMask&(leftMask|rightMask) != edge.nodeIDMask {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 68)
			continue
		}
		// Check whether this expression is only built from one side of the join.
		trace_util_0.Count(_rule_join_reorder_dp_00000, 66)
		if edge.nodeIDMask&leftMask == 0 || edge.nodeIDMask&rightMask == 0 {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 69)
			continue
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 67)
		otherConds = append(otherConds, edge.expr)
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 62)
	return usedEqEdges, otherConds
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 70)
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 72)
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 73)
			eqConds = append(eqConds, edge.edge)
		} else {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 74)
			{
				newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
				eqConds = append(eqConds, newSf)
			}
		}
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 71)
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds)
	_, err := join.recursiveDeriveStats()
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan, otherConds []expression.Expression) LogicalPlan {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 75)
	for len(cartesianJoinGroup) > 1 {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 77)
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 79)
			if i+1 == len(cartesianJoinGroup) {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 82)
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			trace_util_0.Count(_rule_join_reorder_dp_00000, 80)
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				trace_util_0.Count(_rule_join_reorder_dp_00000, 83)
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			trace_util_0.Count(_rule_join_reorder_dp_00000, 81)
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds))
		}
		trace_util_0.Count(_rule_join_reorder_dp_00000, 78)
		cartesianJoinGroup = resultJoinGroup
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 76)
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []LogicalPlan, col *expression.Column) (int, error) {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 84)
	for i, plan := range group {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 86)
		if plan.Schema().Contains(col) {
			trace_util_0.Count(_rule_join_reorder_dp_00000, 87)
			return i, nil
		}
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 85)
	return -1, ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}

func (s *joinReorderDPSolver) newJoinWithConds(leftPlan, rightPlan LogicalPlan, eqConds []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan {
	trace_util_0.Count(_rule_join_reorder_dp_00000, 88)
	join := s.newCartesianJoin(leftPlan, rightPlan)
	join.EqualConditions = eqConds
	join.OtherConditions = otherConds
	for _, eqCond := range join.EqualConditions {
		trace_util_0.Count(_rule_join_reorder_dp_00000, 90)
		join.LeftJoinKeys = append(join.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		join.RightJoinKeys = append(join.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	trace_util_0.Count(_rule_join_reorder_dp_00000, 89)
	return join
}

var _rule_join_reorder_dp_00000 = "planner/core/rule_join_reorder_dp.go"
