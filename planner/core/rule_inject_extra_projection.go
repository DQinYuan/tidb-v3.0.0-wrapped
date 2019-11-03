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
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// injectExtraProjection is used to extract the expressions of specific
// operators into a physical Projection operator and inject the Projection below
// the operators. Thus we can accelerate the expression evaluation by eager
// evaluation.
func injectExtraProjection(plan PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 0)
	return NewProjInjector().inject(plan)
}

type projInjector struct {
}

// NewProjInjector builds a projInjector.
func NewProjInjector() *projInjector {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 1)
	return &projInjector{}
}

func (pe *projInjector) inject(plan PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 2)
	for i, child := range plan.Children() {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 5)
		plan.Children()[i] = pe.inject(child)
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 3)
	switch p := plan.(type) {
	case *PhysicalHashAgg:
		trace_util_0.Count(_rule_inject_extra_projection_00000, 6)
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		trace_util_0.Count(_rule_inject_extra_projection_00000, 7)
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		trace_util_0.Count(_rule_inject_extra_projection_00000, 8)
		plan = injectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		trace_util_0.Count(_rule_inject_extra_projection_00000, 9)
		plan = injectProjBelowSort(p, p.ByItems)
	}
	trace_util_0.Count(_rule_inject_extra_projection_00000, 4)
	return plan
}

// wrapCastForAggFunc wraps the args of an aggregate function with a cast function.
// If the mode is FinalMode or Partial2Mode, we do not need to wrap cast upon the args,
// since the types of the args are already the expected.
func wrapCastForAggFuncs(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc) {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 10)
	for i := range aggFuncs {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 11)
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			trace_util_0.Count(_rule_inject_extra_projection_00000, 12)
			aggFuncs[i].WrapCastForAggArgs(sctx)
		}
	}
}

// injectProjBelowAgg injects a ProjOperator below AggOperator. If all the args
// of `aggFuncs`, and all the item of `groupByItems` are columns or constants,
// we do not need to build the `proj`.
func injectProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 13)
	hasScalarFunc := false

	wrapCastForAggFuncs(aggPlan.context(), aggFuncs)
	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 19)
		for _, arg := range aggFuncs[i].Args {
			trace_util_0.Count(_rule_inject_extra_projection_00000, 20)
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	trace_util_0.Count(_rule_inject_extra_projection_00000, 14)
	for i := 0; !hasScalarFunc && i < len(groupByItems); i++ {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 21)
		_, isScalarFunc := groupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	trace_util_0.Count(_rule_inject_extra_projection_00000, 15)
	if !hasScalarFunc {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 22)
		return aggPlan
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 16)
	projSchemaCols := make([]*expression.Column, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaCols))
	cursor := 0

	for _, f := range aggFuncs {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 23)
		for i, arg := range f.Args {
			trace_util_0.Count(_rule_inject_extra_projection_00000, 24)
			if _, isCnst := arg.(*expression.Constant); isCnst {
				trace_util_0.Count(_rule_inject_extra_projection_00000, 26)
				continue
			}
			trace_util_0.Count(_rule_inject_extra_projection_00000, 25)
			projExprs = append(projExprs, arg)
			newArg := &expression.Column{
				RetType: arg.GetType(),
				ColName: model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
				Index:   cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			f.Args[i] = newArg
			cursor++
		}
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 17)
	for i, item := range groupByItems {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 27)
		if _, isCnst := item.(*expression.Constant); isCnst {
			trace_util_0.Count(_rule_inject_extra_projection_00000, 29)
			continue
		}
		trace_util_0.Count(_rule_inject_extra_projection_00000, 28)
		projExprs = append(projExprs, item)
		newArg := &expression.Column{
			UniqueID: aggPlan.context().GetSessionVars().AllocPlanColumnID(),
			RetType:  item.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
			Index:    cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 18)
	child := aggPlan.Children()[0]
	prop := aggPlan.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(aggPlan.context(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

// injectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schema
// of PhysicalSort and PhysicalTopN are the same as the schema of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schema of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func injectProjBelowSort(p PhysicalPlan, orderByItems []*ByItems) PhysicalPlan {
	trace_util_0.Count(_rule_inject_extra_projection_00000, 30)
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 37)
		_, isScalarFunc := orderByItems[i].Expr.(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	trace_util_0.Count(_rule_inject_extra_projection_00000, 31)
	if !hasScalarFunc {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 38)
		return p
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 32)
	topProjExprs := make([]expression.Expression, 0, p.Schema().Len())
	for i := range p.Schema().Columns {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 39)
		col := p.Schema().Columns[i].Clone().(*expression.Column)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	trace_util_0.Count(_rule_inject_extra_projection_00000, 33)
	topProj := PhysicalProjection{
		Exprs:                topProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.context(), p.statsInfo(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for i, col := range childPlan.Schema().Columns {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 40)
		newCol := col.Clone().(*expression.Column)
		newCol.Index = i
		bottomProjSchemaCols = append(bottomProjSchemaCols, newCol)
		bottomProjExprs = append(bottomProjExprs, newCol)
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 34)
	for _, item := range orderByItems {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 41)
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			trace_util_0.Count(_rule_inject_extra_projection_00000, 43)
			continue
		}
		trace_util_0.Count(_rule_inject_extra_projection_00000, 42)
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.Column{
			UniqueID: p.context().GetSessionVars().AllocPlanColumnID(),
			RetType:  itemExpr.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(bottomProjSchemaCols))),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
		item.Expr = newArg
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 35)
	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.context(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		trace_util_0.Count(_rule_inject_extra_projection_00000, 44)
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	trace_util_0.Count(_rule_inject_extra_projection_00000, 36)
	return topProj
}

var _rule_inject_extra_projection_00000 = "planner/core/rule_inject_extra_projection.go"
