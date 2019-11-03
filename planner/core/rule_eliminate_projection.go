// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/trace_util_0"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if every expression is a single column.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
	trace_util_0.Count(_rule_eliminate_projection_00000, 0)
	for _, expr := range p.Exprs {
		trace_util_0.Count(_rule_eliminate_projection_00000, 2)
		_, ok := expr.(*expression.Column)
		if !ok {
			trace_util_0.Count(_rule_eliminate_projection_00000, 3)
			return false
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 1)
	return true
}

// canProjectionBeEliminatedStrict checks whether a projection can be
// eliminated, returns true if the projection just copy its child's output.
func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
	trace_util_0.Count(_rule_eliminate_projection_00000, 4)
	// If this projection is specially added for `DO`, we keep it.
	if p.CalculateNoDelay {
		trace_util_0.Count(_rule_eliminate_projection_00000, 9)
		return false
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 5)
	if p.Schema().Len() == 0 {
		trace_util_0.Count(_rule_eliminate_projection_00000, 10)
		return true
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 6)
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		trace_util_0.Count(_rule_eliminate_projection_00000, 11)
		return false
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 7)
	for i, expr := range p.Exprs {
		trace_util_0.Count(_rule_eliminate_projection_00000, 12)
		col, ok := expr.(*expression.Column)
		if !ok || !col.Equal(nil, child.Schema().Columns[i]) {
			trace_util_0.Count(_rule_eliminate_projection_00000, 13)
			return false
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 8)
	return true
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 14)
	dst := replace[string(origin.HashCode(nil))]
	if dst != nil {
		trace_util_0.Count(_rule_eliminate_projection_00000, 15)
		colName, retType, inOperand := origin.ColName, origin.RetType, origin.InOperand
		*origin = *dst
		origin.ColName, origin.RetType, origin.InOperand = colName, retType, inOperand
	}
}

func resolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 16)
	switch expr := origin.(type) {
	case *expression.Column:
		trace_util_0.Count(_rule_eliminate_projection_00000, 17)
		resolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		trace_util_0.Count(_rule_eliminate_projection_00000, 18)
		resolveColumnAndReplace(&expr.Column, replace)
	case *expression.ScalarFunction:
		trace_util_0.Count(_rule_eliminate_projection_00000, 19)
		for _, arg := range expr.GetArgs() {
			trace_util_0.Count(_rule_eliminate_projection_00000, 20)
			resolveExprAndReplace(arg, replace)
		}
	}
}

func doPhysicalProjectionElimination(p PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_rule_eliminate_projection_00000, 21)
	for i, child := range p.Children() {
		trace_util_0.Count(_rule_eliminate_projection_00000, 24)
		p.Children()[i] = doPhysicalProjectionElimination(child)
	}

	trace_util_0.Count(_rule_eliminate_projection_00000, 22)
	proj, isProj := p.(*PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		trace_util_0.Count(_rule_eliminate_projection_00000, 25)
		return p
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 23)
	child := p.Children()[0]
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to
// eliminate the redundant projection left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_rule_eliminate_projection_00000, 26)
	oldSchema := p.Schema()
	newRoot := doPhysicalProjectionElimination(p)
	newCols := newRoot.Schema().Columns
	for i, oldCol := range oldSchema.Columns {
		trace_util_0.Count(_rule_eliminate_projection_00000, 28)
		oldCol.Index = newCols[i].Index
		newRoot.Schema().Columns[i] = oldCol
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 27)
	return newRoot
}

type projectionEliminater struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminater) optimize(lp LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 29)
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminater) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool) LogicalPlan {
	trace_util_0.Count(_rule_eliminate_projection_00000, 30)
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		trace_util_0.Count(_rule_eliminate_projection_00000, 36)
		childFlag = false
	} else {
		trace_util_0.Count(_rule_eliminate_projection_00000, 37)
		if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
			trace_util_0.Count(_rule_eliminate_projection_00000, 38)
			childFlag = true
		} else {
			trace_util_0.Count(_rule_eliminate_projection_00000, 39)
			if _, isWindow := p.(*LogicalWindow); isWindow {
				trace_util_0.Count(_rule_eliminate_projection_00000, 40)
				childFlag = true
			}
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 31)
	for i, child := range p.Children() {
		trace_util_0.Count(_rule_eliminate_projection_00000, 41)
		p.Children()[i] = pe.eliminate(child, replace, childFlag)
	}

	trace_util_0.Count(_rule_eliminate_projection_00000, 32)
	switch x := p.(type) {
	case *LogicalJoin:
		trace_util_0.Count(_rule_eliminate_projection_00000, 42)
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	case *LogicalApply:
		trace_util_0.Count(_rule_eliminate_projection_00000, 43)
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	default:
		trace_util_0.Count(_rule_eliminate_projection_00000, 44)
		for _, dst := range p.Schema().Columns {
			trace_util_0.Count(_rule_eliminate_projection_00000, 45)
			resolveColumnAndReplace(dst, replace)
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 33)
	p.replaceExprColumns(replace)

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		trace_util_0.Count(_rule_eliminate_projection_00000, 46)
		return p
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 34)
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		trace_util_0.Count(_rule_eliminate_projection_00000, 47)
		replace[string(col.HashCode(nil))] = exprs[i].(*expression.Column)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 35)
	return p.Children()[0]
}

func (p *LogicalJoin) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 48)
	for _, equalExpr := range p.EqualConditions {
		trace_util_0.Count(_rule_eliminate_projection_00000, 52)
		resolveExprAndReplace(equalExpr, replace)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 49)
	for _, leftExpr := range p.LeftConditions {
		trace_util_0.Count(_rule_eliminate_projection_00000, 53)
		resolveExprAndReplace(leftExpr, replace)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 50)
	for _, rightExpr := range p.RightConditions {
		trace_util_0.Count(_rule_eliminate_projection_00000, 54)
		resolveExprAndReplace(rightExpr, replace)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 51)
	for _, otherExpr := range p.OtherConditions {
		trace_util_0.Count(_rule_eliminate_projection_00000, 55)
		resolveExprAndReplace(otherExpr, replace)
	}
}

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 56)
	for _, expr := range p.Exprs {
		trace_util_0.Count(_rule_eliminate_projection_00000, 57)
		resolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 58)
	for _, agg := range la.AggFuncs {
		trace_util_0.Count(_rule_eliminate_projection_00000, 61)
		for _, aggExpr := range agg.Args {
			trace_util_0.Count(_rule_eliminate_projection_00000, 62)
			resolveExprAndReplace(aggExpr, replace)
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 59)
	for _, gbyItem := range la.GroupByItems {
		trace_util_0.Count(_rule_eliminate_projection_00000, 63)
		resolveExprAndReplace(gbyItem, replace)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 60)
	la.collectGroupByColumns()
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 64)
	for _, expr := range p.Conditions {
		trace_util_0.Count(_rule_eliminate_projection_00000, 65)
		resolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 66)
	la.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range la.corCols {
		trace_util_0.Count(_rule_eliminate_projection_00000, 67)
		dst := replace[string(coCol.Column.HashCode(nil))]
		if dst != nil {
			trace_util_0.Count(_rule_eliminate_projection_00000, 68)
			coCol.Column = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 69)
	for _, byItem := range ls.ByItems {
		trace_util_0.Count(_rule_eliminate_projection_00000, 70)
		resolveExprAndReplace(byItem.Expr, replace)
	}
}

func (lt *LogicalTopN) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 71)
	for _, byItem := range lt.ByItems {
		trace_util_0.Count(_rule_eliminate_projection_00000, 72)
		resolveExprAndReplace(byItem.Expr, replace)
	}
}

func (p *LogicalWindow) replaceExprColumns(replace map[string]*expression.Column) {
	trace_util_0.Count(_rule_eliminate_projection_00000, 73)
	for _, desc := range p.WindowFuncDescs {
		trace_util_0.Count(_rule_eliminate_projection_00000, 76)
		for _, arg := range desc.Args {
			trace_util_0.Count(_rule_eliminate_projection_00000, 77)
			resolveExprAndReplace(arg, replace)
		}
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 74)
	for _, item := range p.PartitionBy {
		trace_util_0.Count(_rule_eliminate_projection_00000, 78)
		resolveColumnAndReplace(item.Col, replace)
	}
	trace_util_0.Count(_rule_eliminate_projection_00000, 75)
	for _, item := range p.OrderBy {
		trace_util_0.Count(_rule_eliminate_projection_00000, 79)
		resolveColumnAndReplace(item.Col, replace)
	}
}

var _rule_eliminate_projection_00000 = "planner/core/rule_eliminate_projection.go"
