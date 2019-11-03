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

package cascades

import (
	"container/list"
	"math"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// FindBestPlan is the optimization entrance of the cascades planner. The
// optimization is composed of 2 phases: exploration and implementation.
func FindBestPlan(sctx sessionctx.Context, logical plannercore.LogicalPlan) (plannercore.Plan, error) {
	trace_util_0.Count(_optimize_00000, 0)
	rootGroup := convert2Group(logical)
	err := onPhaseExploration(sctx, rootGroup)
	if err != nil {
		trace_util_0.Count(_optimize_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_optimize_00000, 1)
	best, err := onPhaseImplementation(sctx, rootGroup)
	return best, err
}

// convert2Group converts a logical plan to expression groups.
func convert2Group(node plannercore.LogicalPlan) *memo.Group {
	trace_util_0.Count(_optimize_00000, 3)
	e := memo.NewGroupExpr(node)
	e.Children = make([]*memo.Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		trace_util_0.Count(_optimize_00000, 5)
		childGroup := convert2Group(child)
		e.Children = append(e.Children, childGroup)
	}
	trace_util_0.Count(_optimize_00000, 4)
	g := memo.NewGroup(e)
	// Stats property for `Group` would be computed after exploration phase.
	g.Prop = &property.LogicalProperty{Schema: node.Schema()}
	return g
}

func onPhaseExploration(sctx sessionctx.Context, g *memo.Group) error {
	trace_util_0.Count(_optimize_00000, 6)
	for !g.Explored {
		trace_util_0.Count(_optimize_00000, 8)
		err := exploreGroup(g)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 9)
			return err
		}
	}
	trace_util_0.Count(_optimize_00000, 7)
	return nil
}

func exploreGroup(g *memo.Group) error {
	trace_util_0.Count(_optimize_00000, 10)
	if g.Explored {
		trace_util_0.Count(_optimize_00000, 13)
		return nil
	}

	trace_util_0.Count(_optimize_00000, 11)
	g.Explored = true
	for elem := g.Equivalents.Front(); elem != nil; elem.Next() {
		trace_util_0.Count(_optimize_00000, 14)
		curExpr := elem.Value.(*memo.GroupExpr)
		if curExpr.Explored {
			trace_util_0.Count(_optimize_00000, 19)
			continue
		}

		// Explore child groups firstly.
		trace_util_0.Count(_optimize_00000, 15)
		curExpr.Explored = true
		for _, childGroup := range curExpr.Children {
			trace_util_0.Count(_optimize_00000, 20)
			if err := exploreGroup(childGroup); err != nil {
				trace_util_0.Count(_optimize_00000, 22)
				return err
			}
			trace_util_0.Count(_optimize_00000, 21)
			curExpr.Explored = curExpr.Explored && childGroup.Explored
		}

		trace_util_0.Count(_optimize_00000, 16)
		eraseCur, err := findMoreEquiv(g, elem)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 23)
			return err
		}
		trace_util_0.Count(_optimize_00000, 17)
		if eraseCur {
			trace_util_0.Count(_optimize_00000, 24)
			g.Delete(curExpr)
		}

		trace_util_0.Count(_optimize_00000, 18)
		g.Explored = g.Explored && curExpr.Explored
	}
	trace_util_0.Count(_optimize_00000, 12)
	return nil
}

// findMoreEquiv finds and applies the matched transformation rules.
func findMoreEquiv(g *memo.Group, elem *list.Element) (eraseCur bool, err error) {
	trace_util_0.Count(_optimize_00000, 25)
	expr := elem.Value.(*memo.GroupExpr)
	for _, rule := range GetTransformationRules(expr.ExprNode) {
		trace_util_0.Count(_optimize_00000, 27)
		pattern := rule.GetPattern()
		if !pattern.Operand.Match(memo.GetOperand(expr.ExprNode)) {
			trace_util_0.Count(_optimize_00000, 29)
			continue
		}
		// Create a binding of the current Group expression and the pattern of
		// the transformation rule to enumerate all the possible expressions.
		trace_util_0.Count(_optimize_00000, 28)
		iter := memo.NewExprIterFromGroupElem(elem, pattern)
		for ; iter != nil && iter.Matched(); iter.Next() {
			trace_util_0.Count(_optimize_00000, 30)
			if !rule.Match(iter) {
				trace_util_0.Count(_optimize_00000, 34)
				continue
			}

			trace_util_0.Count(_optimize_00000, 31)
			newExpr, erase, err := rule.OnTransform(iter)
			if err != nil {
				trace_util_0.Count(_optimize_00000, 35)
				return false, err
			}

			trace_util_0.Count(_optimize_00000, 32)
			eraseCur = eraseCur || erase
			if !g.Insert(newExpr) {
				trace_util_0.Count(_optimize_00000, 36)
				continue
			}

			// If the new Group expression is successfully inserted into the
			// current Group, we mark the Group expression and the Group as
			// unexplored to enable the exploration on the new Group expression
			// and all the antecedent groups.
			trace_util_0.Count(_optimize_00000, 33)
			newExpr.Explored = false
			g.Explored = false
		}
	}
	trace_util_0.Count(_optimize_00000, 26)
	return eraseCur, nil
}

// fillGroupStats computes Stats property for each Group recursively.
func fillGroupStats(g *memo.Group) (err error) {
	trace_util_0.Count(_optimize_00000, 37)
	if g.Prop.Stats != nil {
		trace_util_0.Count(_optimize_00000, 40)
		return nil
	}
	// All GroupExpr in a Group should share same LogicalProperty, so just use
	// first one to compute Stats property.
	trace_util_0.Count(_optimize_00000, 38)
	elem := g.Equivalents.Front()
	expr := elem.Value.(*memo.GroupExpr)
	childStats := make([]*property.StatsInfo, len(expr.Children))
	for i, childGroup := range expr.Children {
		trace_util_0.Count(_optimize_00000, 41)
		err = fillGroupStats(childGroup)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 43)
			return err
		}
		trace_util_0.Count(_optimize_00000, 42)
		childStats[i] = childGroup.Prop.Stats
	}
	trace_util_0.Count(_optimize_00000, 39)
	planNode := expr.ExprNode
	g.Prop.Stats, err = planNode.DeriveStats(childStats)
	return err
}

// onPhaseImplementation starts implementation physical operators from given root Group.
func onPhaseImplementation(sctx sessionctx.Context, g *memo.Group) (plannercore.Plan, error) {
	trace_util_0.Count(_optimize_00000, 44)
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	// TODO replace MaxFloat64 costLimit by variable from sctx, or other sources.
	impl, err := implGroup(g, prop, math.MaxFloat64)
	if err != nil {
		trace_util_0.Count(_optimize_00000, 47)
		return nil, err
	}
	trace_util_0.Count(_optimize_00000, 45)
	if impl == nil {
		trace_util_0.Count(_optimize_00000, 48)
		return nil, plannercore.ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}
	trace_util_0.Count(_optimize_00000, 46)
	return impl.GetPlan(), nil
}

// implGroup picks one implementation with lowest cost for a Group, which satisfies specified physical property.
func implGroup(g *memo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (memo.Implementation, error) {
	trace_util_0.Count(_optimize_00000, 49)
	groupImpl := g.GetImpl(reqPhysProp)
	if groupImpl != nil {
		trace_util_0.Count(_optimize_00000, 55)
		if groupImpl.GetCost() <= costLimit {
			trace_util_0.Count(_optimize_00000, 57)
			return groupImpl, nil
		}
		trace_util_0.Count(_optimize_00000, 56)
		return nil, nil
	}
	// Handle implementation rules for each equivalent GroupExpr.
	trace_util_0.Count(_optimize_00000, 50)
	var cumCost float64
	var childCosts []float64
	var childPlans []plannercore.PhysicalPlan
	err := fillGroupStats(g)
	if err != nil {
		trace_util_0.Count(_optimize_00000, 58)
		return nil, err
	}
	trace_util_0.Count(_optimize_00000, 51)
	outCount := math.Min(g.Prop.Stats.RowCount, reqPhysProp.ExpectedCnt)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		trace_util_0.Count(_optimize_00000, 59)
		curExpr := elem.Value.(*memo.GroupExpr)
		impls, err := implGroupExpr(curExpr, reqPhysProp)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 61)
			return nil, err
		}
		trace_util_0.Count(_optimize_00000, 60)
		for _, impl := range impls {
			trace_util_0.Count(_optimize_00000, 62)
			cumCost = 0.0
			childCosts = childCosts[:0]
			childPlans = childPlans[:0]
			for i, childGroup := range curExpr.Children {
				trace_util_0.Count(_optimize_00000, 66)
				childImpl, err := implGroup(childGroup, impl.GetPlan().GetChildReqProps(i), costLimit-cumCost)
				if err != nil {
					trace_util_0.Count(_optimize_00000, 69)
					return nil, err
				}
				trace_util_0.Count(_optimize_00000, 67)
				if childImpl == nil {
					trace_util_0.Count(_optimize_00000, 70)
					impl.SetCost(math.MaxFloat64)
					break
				}
				trace_util_0.Count(_optimize_00000, 68)
				childCost := childImpl.GetCost()
				childCosts = append(childCosts, childCost)
				cumCost += childCost
				childPlans = append(childPlans, childImpl.GetPlan())
			}
			trace_util_0.Count(_optimize_00000, 63)
			if impl.GetCost() == math.MaxFloat64 {
				trace_util_0.Count(_optimize_00000, 71)
				continue
			}
			trace_util_0.Count(_optimize_00000, 64)
			cumCost = impl.CalcCost(outCount, childCosts, curExpr.Children...)
			if cumCost > costLimit {
				trace_util_0.Count(_optimize_00000, 72)
				continue
			}
			trace_util_0.Count(_optimize_00000, 65)
			if groupImpl == nil || groupImpl.GetCost() > cumCost {
				trace_util_0.Count(_optimize_00000, 73)
				impl.GetPlan().SetChildren(childPlans...)
				groupImpl = impl
				costLimit = cumCost
			}
		}
	}
	// Handle enforcer rules for required physical property.
	trace_util_0.Count(_optimize_00000, 52)
	for _, rule := range GetEnforcerRules(reqPhysProp) {
		trace_util_0.Count(_optimize_00000, 74)
		newReqPhysProp := rule.NewProperty(reqPhysProp)
		enforceCost := rule.GetEnforceCost(outCount)
		childImpl, err := implGroup(g, newReqPhysProp, costLimit-enforceCost)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 77)
			return nil, err
		}
		trace_util_0.Count(_optimize_00000, 75)
		if childImpl == nil {
			trace_util_0.Count(_optimize_00000, 78)
			continue
		}
		trace_util_0.Count(_optimize_00000, 76)
		impl := rule.OnEnforce(reqPhysProp, childImpl)
		cumCost = enforceCost + childImpl.GetCost()
		impl.SetCost(cumCost)
		if groupImpl == nil || groupImpl.GetCost() > cumCost {
			trace_util_0.Count(_optimize_00000, 79)
			groupImpl = impl
			costLimit = cumCost
		}
	}
	trace_util_0.Count(_optimize_00000, 53)
	if groupImpl == nil || groupImpl.GetCost() == math.MaxFloat64 {
		trace_util_0.Count(_optimize_00000, 80)
		return nil, nil
	}
	trace_util_0.Count(_optimize_00000, 54)
	g.InsertImpl(reqPhysProp, groupImpl)
	return groupImpl, nil
}

func implGroupExpr(cur *memo.GroupExpr, reqPhysProp *property.PhysicalProperty) (impls []memo.Implementation, err error) {
	trace_util_0.Count(_optimize_00000, 81)
	for _, rule := range GetImplementationRules(cur.ExprNode) {
		trace_util_0.Count(_optimize_00000, 83)
		if !rule.Match(cur, reqPhysProp) {
			trace_util_0.Count(_optimize_00000, 86)
			continue
		}
		trace_util_0.Count(_optimize_00000, 84)
		impl, err := rule.OnImplement(cur, reqPhysProp)
		if err != nil {
			trace_util_0.Count(_optimize_00000, 87)
			return nil, err
		}
		trace_util_0.Count(_optimize_00000, 85)
		impls = append(impls, impl)
	}
	trace_util_0.Count(_optimize_00000, 82)
	return impls, nil
}

var _optimize_00000 = "planner/cascades/optimize.go"
