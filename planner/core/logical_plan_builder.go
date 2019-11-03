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
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"strings"
	"unicode"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// TiDBIndexNestedLoopJoin is hint enforce index nested loop join.
	TiDBIndexNestedLoopJoin = "tidb_inlj"
	// TiDBHashJoin is hint enforce hash join.
	TiDBHashJoin = "tidb_hj"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

func (la *LogicalAggregation) collectGroupByColumns() {
	trace_util_0.Count(_logical_plan_builder_00000, 0)
	la.groupByCols = la.groupByCols[:0]
	for _, item := range la.GroupByItems {
		trace_util_0.Count(_logical_plan_builder_00000, 1)
		if col, ok := item.(*expression.Column); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 2)
			la.groupByCols = append(la.groupByCols, col)
		}
	}
}

func (b *PlanBuilder) buildAggregation(p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 3)
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag = b.optFlag | flagMaxMinEliminate
	b.optFlag = b.optFlag | flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagEliminateAgg
	b.optFlag = b.optFlag | flagEliminateProjection

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx)
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	for i, aggFunc := range aggFuncList {
		trace_util_0.Count(_logical_plan_builder_00000, 6)
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			trace_util_0.Count(_logical_plan_builder_00000, 10)
			newArg, np, err := b.rewrite(arg, p, nil, true)
			if err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 12)
				return nil, nil, err
			}
			trace_util_0.Count(_logical_plan_builder_00000, 11)
			p = np
			newArgList = append(newArgList, newArg)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 7)
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 13)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 8)
		combined := false
		for j, oldFunc := range plan4Agg.AggFuncs {
			trace_util_0.Count(_logical_plan_builder_00000, 14)
			if oldFunc.Equal(b.ctx, newFunc) {
				trace_util_0.Count(_logical_plan_builder_00000, 15)
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 9)
		if !combined {
			trace_util_0.Count(_logical_plan_builder_00000, 16)
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			schema4Agg.Append(&expression.Column{
				ColName:      model.NewCIStr(fmt.Sprintf("%d_col_%d", plan4Agg.id, position)),
				UniqueID:     b.ctx.GetSessionVars().AllocPlanColumnID(),
				IsReferenced: true,
				RetType:      newFunc.RetTp,
			})
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 4)
	for _, col := range p.Schema().Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 17)
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 19)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 18)
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 5)
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	plan4Agg.collectGroupByColumns()
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildResultSetNode(node ast.ResultSetNode) (p LogicalPlan, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 20)
	switch x := node.(type) {
	case *ast.Join:
		trace_util_0.Count(_logical_plan_builder_00000, 21)
		return b.buildJoin(x)
	case *ast.TableSource:
		trace_util_0.Count(_logical_plan_builder_00000, 22)
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			trace_util_0.Count(_logical_plan_builder_00000, 31)
			p, err = b.buildSelect(v)
		case *ast.UnionStmt:
			trace_util_0.Count(_logical_plan_builder_00000, 32)
			p, err = b.buildUnion(v)
		case *ast.TableName:
			trace_util_0.Count(_logical_plan_builder_00000, 33)
			p, err = b.buildDataSource(v)
		default:
			trace_util_0.Count(_logical_plan_builder_00000, 34)
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 23)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 35)
			return nil, err
		}

		trace_util_0.Count(_logical_plan_builder_00000, 24)
		if v, ok := p.(*DataSource); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 36)
			v.TableAsName = &x.AsName
		}
		trace_util_0.Count(_logical_plan_builder_00000, 25)
		for _, col := range p.Schema().Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 37)
			col.OrigTblName = col.TblName
			if x.AsName.L != "" {
				trace_util_0.Count(_logical_plan_builder_00000, 38)
				col.TblName = x.AsName
			}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		trace_util_0.Count(_logical_plan_builder_00000, 26)
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, col := range p.Schema().Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 39)
			name := col.ColName.O
			if _, ok := dupNames[name]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 41)
				return nil, ErrDupFieldName.GenWithStackByArgs(name)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 40)
			dupNames[name] = struct{}{}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 27)
		return p, nil
	case *ast.SelectStmt:
		trace_util_0.Count(_logical_plan_builder_00000, 28)
		return b.buildSelect(x)
	case *ast.UnionStmt:
		trace_util_0.Count(_logical_plan_builder_00000, 29)
		return b.buildUnion(x)
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 30)
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	trace_util_0.Count(_logical_plan_builder_00000, 42)
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		trace_util_0.Count(_logical_plan_builder_00000, 44)
		if filterCond {
			trace_util_0.Count(_logical_plan_builder_00000, 47)
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 48)
			{
				rightCond = append(rightCond, expr)
			}
		}
	case RightOuterJoin:
		trace_util_0.Count(_logical_plan_builder_00000, 45)
		if filterCond {
			trace_util_0.Count(_logical_plan_builder_00000, 49)
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 50)
			{
				leftCond = append(leftCond, expr)
			}
		}
	case SemiJoin, AntiSemiJoin, InnerJoin:
		trace_util_0.Count(_logical_plan_builder_00000, 46)
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 43)
	return leftCond, rightCond
}

// extractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	trace_util_0.Count(_logical_plan_builder_00000, 51)
	left, right := p.children[0], p.children[1]
	for _, expr := range conditions {
		trace_util_0.Count(_logical_plan_builder_00000, 53)
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			trace_util_0.Count(_logical_plan_builder_00000, 57)
			ctx := binop.GetCtx()
			arg0, lOK := binop.GetArgs()[0].(*expression.Column)
			arg1, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				trace_util_0.Count(_logical_plan_builder_00000, 58)
				var leftCol, rightCol *expression.Column
				if left.Schema().Contains(arg0) && right.Schema().Contains(arg1) {
					trace_util_0.Count(_logical_plan_builder_00000, 62)
					leftCol, rightCol = arg0, arg1
				}
				trace_util_0.Count(_logical_plan_builder_00000, 59)
				if leftCol == nil && left.Schema().Contains(arg1) && right.Schema().Contains(arg0) {
					trace_util_0.Count(_logical_plan_builder_00000, 63)
					leftCol, rightCol = arg1, arg0
				}
				trace_util_0.Count(_logical_plan_builder_00000, 60)
				if leftCol != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 64)
					// Do not derive `is not null` for anti join, since it may cause wrong results.
					// For example:
					// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
					// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
					// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
					if deriveLeft && p.JoinType != AntiSemiJoin {
						trace_util_0.Count(_logical_plan_builder_00000, 66)
						if isNullRejected(ctx, left.Schema(), expr) && !mysql.HasNotNullFlag(leftCol.RetType.Flag) {
							trace_util_0.Count(_logical_plan_builder_00000, 67)
							notNullExpr := expression.BuildNotNullExpr(ctx, leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					trace_util_0.Count(_logical_plan_builder_00000, 65)
					if deriveRight && p.JoinType != AntiSemiJoin {
						trace_util_0.Count(_logical_plan_builder_00000, 68)
						if isNullRejected(ctx, right.Schema(), expr) && !mysql.HasNotNullFlag(rightCol.RetType.Flag) {
							trace_util_0.Count(_logical_plan_builder_00000, 69)
							notNullExpr := expression.BuildNotNullExpr(ctx, rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
				}
				// For quries like `select a in (select a from s where s.b = t.b) from t`,
				// if subquery is empty caused by `s.b = t.b`, the result should always be
				// false even if t.a is null or s.a is null. To make this join "empty aware",
				// we should differentiate `t.a = s.a` from other column equal conditions, so
				// we put it into OtherConditions instead of EqualConditions of join.
				trace_util_0.Count(_logical_plan_builder_00000, 61)
				if leftCol != nil && binop.FuncName.L == ast.EQ && !leftCol.InOperand && !rightCol.InOperand {
					trace_util_0.Count(_logical_plan_builder_00000, 70)
					cond := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightCol)
					eqCond = append(eqCond, cond.(*expression.ScalarFunction))
					continue
				}
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 54)
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 71)
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 55)
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			trace_util_0.Count(_logical_plan_builder_00000, 72)
			if !left.Schema().Contains(col) {
				trace_util_0.Count(_logical_plan_builder_00000, 74)
				allFromLeft = false
			}
			trace_util_0.Count(_logical_plan_builder_00000, 73)
			if !right.Schema().Contains(col) {
				trace_util_0.Count(_logical_plan_builder_00000, 75)
				allFromRight = false
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 56)
		if allFromRight {
			trace_util_0.Count(_logical_plan_builder_00000, 76)
			rightCond = append(rightCond, expr)
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 77)
			if allFromLeft {
				trace_util_0.Count(_logical_plan_builder_00000, 78)
				leftCond = append(leftCond, expr)
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 79)
				{
					// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
					// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
					// children as much as possible.
					if deriveLeft {
						trace_util_0.Count(_logical_plan_builder_00000, 82)
						leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, left.Schema())
						if leftRelaxedCond != nil {
							trace_util_0.Count(_logical_plan_builder_00000, 83)
							leftCond = append(leftCond, leftRelaxedCond)
						}
					}
					trace_util_0.Count(_logical_plan_builder_00000, 80)
					if deriveRight {
						trace_util_0.Count(_logical_plan_builder_00000, 84)
						rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, right.Schema())
						if rightRelaxedCond != nil {
							trace_util_0.Count(_logical_plan_builder_00000, 85)
							rightCond = append(rightCond, rightRelaxedCond)
						}
					}
					trace_util_0.Count(_logical_plan_builder_00000, 81)
					otherCond = append(otherCond, expr)
				}
			}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 52)
	return
}

func extractTableAlias(p LogicalPlan) *model.CIStr {
	trace_util_0.Count(_logical_plan_builder_00000, 86)
	if p.Schema().Len() > 0 && p.Schema().Columns[0].TblName.L != "" {
		trace_util_0.Count(_logical_plan_builder_00000, 88)
		return &(p.Schema().Columns[0].TblName)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 87)
	return nil
}

func (p *LogicalJoin) setPreferredJoinType(hintInfo *tableHintInfo) error {
	trace_util_0.Count(_logical_plan_builder_00000, 89)
	if hintInfo == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 97)
		return nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 90)
	lhsAlias := extractTableAlias(p.children[0])
	rhsAlias := extractTableAlias(p.children[1])
	if hintInfo.ifPreferMergeJoin(lhsAlias, rhsAlias) {
		trace_util_0.Count(_logical_plan_builder_00000, 98)
		p.preferJoinType |= preferMergeJoin
	}
	trace_util_0.Count(_logical_plan_builder_00000, 91)
	if hintInfo.ifPreferHashJoin(lhsAlias, rhsAlias) {
		trace_util_0.Count(_logical_plan_builder_00000, 99)
		p.preferJoinType |= preferHashJoin
	}
	trace_util_0.Count(_logical_plan_builder_00000, 92)
	if hintInfo.ifPreferINLJ(lhsAlias) {
		trace_util_0.Count(_logical_plan_builder_00000, 100)
		p.preferJoinType |= preferLeftAsIndexInner
	}
	trace_util_0.Count(_logical_plan_builder_00000, 93)
	if hintInfo.ifPreferINLJ(rhsAlias) {
		trace_util_0.Count(_logical_plan_builder_00000, 101)
		p.preferJoinType |= preferRightAsIndexInner
	}

	// set hintInfo for further usage if this hint info can be used.
	trace_util_0.Count(_logical_plan_builder_00000, 94)
	if p.preferJoinType != 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 102)
		p.hintInfo = hintInfo
	}

	// If there're multiple join types and one of them is not index join hint,
	// then there is a conflict of join types.
	trace_util_0.Count(_logical_plan_builder_00000, 95)
	if bits.OnesCount(p.preferJoinType) > 1 && (p.preferJoinType^preferRightAsIndexInner^preferLeftAsIndexInner) > 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 103)
		return errors.New("Join hints are conflict, you can only specify one type of join")
	}
	trace_util_0.Count(_logical_plan_builder_00000, 96)
	return nil
}

func resetNotNullFlag(schema *expression.Schema, start, end int) {
	trace_util_0.Count(_logical_plan_builder_00000, 104)
	for i := start; i < end; i++ {
		trace_util_0.Count(_logical_plan_builder_00000, 105)
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.Flag &= ^mysql.NotNullFlag
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

func (b *PlanBuilder) buildJoin(joinNode *ast.Join) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 106)
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 115)
		return b.buildResultSetNode(joinNode.Left)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 107)
	b.optFlag = b.optFlag | flagPredicatePushDown

	leftPlan, err := b.buildResultSetNode(joinNode.Left)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 116)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 108)
	rightPlan, err := b.buildResultSetNode(joinNode.Right)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 117)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 109)
	joinPlan := LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx)
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		trace_util_0.Count(_logical_plan_builder_00000, 118)
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = LeftOuterJoin
		resetNotNullFlag(joinPlan.schema, leftPlan.Schema().Len(), joinPlan.schema.Len())
	case ast.RightJoin:
		trace_util_0.Count(_logical_plan_builder_00000, 119)
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = RightOuterJoin
		resetNotNullFlag(joinPlan.schema, 0, leftPlan.Schema().Len())
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 120)
		b.optFlag = b.optFlag | flagJoinReOrder
		joinPlan.JoinType = InnerJoin
	}

	// Merge sub join's redundantSchema into this join plan. When handle query like
	// select t2.a from (t1 join t2 using (a)) join t3 using (a);
	// we can simply search in the top level join plan to find redundant column.
	trace_util_0.Count(_logical_plan_builder_00000, 110)
	var lRedundant, rRedundant *expression.Schema
	if left, ok := leftPlan.(*LogicalJoin); ok && left.redundantSchema != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 121)
		lRedundant = left.redundantSchema
	}
	trace_util_0.Count(_logical_plan_builder_00000, 111)
	if right, ok := rightPlan.(*LogicalJoin); ok && right.redundantSchema != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 122)
		rRedundant = right.redundantSchema
	}
	trace_util_0.Count(_logical_plan_builder_00000, 112)
	joinPlan.redundantSchema = expression.MergeSchema(lRedundant, rRedundant)

	// Set preferred join algorithm if some join hints is specified by user.
	err = joinPlan.setPreferredJoinType(b.TableHints())
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 123)
		return nil, err
	}

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	trace_util_0.Count(_logical_plan_builder_00000, 113)
	if joinNode.NaturalJoin {
		trace_util_0.Count(_logical_plan_builder_00000, 124)
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 125)
			return nil, err
		}
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 126)
		if joinNode.Using != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 127)
			err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
			if err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 128)
				return nil, err
			}
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 129)
			if joinNode.On != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 130)
				b.curClause = onClause
				onExpr, newPlan, err := b.rewrite(joinNode.On.Expr, joinPlan, nil, false)
				if err != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 133)
					return nil, err
				}
				trace_util_0.Count(_logical_plan_builder_00000, 131)
				if newPlan != joinPlan {
					trace_util_0.Count(_logical_plan_builder_00000, 134)
					return nil, errors.New("ON condition doesn't support subqueries yet")
				}
				trace_util_0.Count(_logical_plan_builder_00000, 132)
				onCondition := expression.SplitCNFItems(onExpr)
				joinPlan.attachOnConds(onCondition)
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 135)
				if joinPlan.JoinType == InnerJoin {
					trace_util_0.Count(_logical_plan_builder_00000, 136)
					// If a inner join without "ON" or "USING" clause, it's a cartesian
					// product over the join tables.
					joinPlan.cartesianJoin = true
				}
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 114)
	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
// 1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//    appears in "leftPlan".
// 2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
// 3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	trace_util_0.Count(_logical_plan_builder_00000, 137)
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		trace_util_0.Count(_logical_plan_builder_00000, 139)
		filter[col.Name.L] = true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 138)
	return b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp == ast.RightJoin, filter)
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
// 	All the common columns
// 	Every column in the first (left) table that is not a common column
// 	Every column in the second (right) table that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	trace_util_0.Count(_logical_plan_builder_00000, 140)
	return b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp == ast.RightJoin, nil)
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonColumns(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, rightJoin bool, filter map[string]bool) error {
	trace_util_0.Count(_logical_plan_builder_00000, 141)
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	lColumns, rColumns := lsc.Columns, rsc.Columns
	if rightJoin {
		trace_util_0.Count(_logical_plan_builder_00000, 146)
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Find out all the common columns and put them ahead.
	trace_util_0.Count(_logical_plan_builder_00000, 142)
	commonLen := 0
	for i, lCol := range lColumns {
		trace_util_0.Count(_logical_plan_builder_00000, 147)
		for j := commonLen; j < len(rColumns); j++ {
			trace_util_0.Count(_logical_plan_builder_00000, 148)
			if lCol.ColName.L != rColumns[j].ColName.L {
				trace_util_0.Count(_logical_plan_builder_00000, 151)
				continue
			}

			trace_util_0.Count(_logical_plan_builder_00000, 149)
			if len(filter) > 0 {
				trace_util_0.Count(_logical_plan_builder_00000, 152)
				if !filter[lCol.ColName.L] {
					trace_util_0.Count(_logical_plan_builder_00000, 154)
					break
				}
				// Mark this column exist.
				trace_util_0.Count(_logical_plan_builder_00000, 153)
				filter[lCol.ColName.L] = false
			}

			trace_util_0.Count(_logical_plan_builder_00000, 150)
			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			commonLen++
			break
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 143)
	if len(filter) > 0 && len(filter) != commonLen {
		trace_util_0.Count(_logical_plan_builder_00000, 155)
		for col, notExist := range filter {
			trace_util_0.Count(_logical_plan_builder_00000, 156)
			if notExist {
				trace_util_0.Count(_logical_plan_builder_00000, 157)
				return ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 144)
	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := 0; i < commonLen; i++ {
		trace_util_0.Count(_logical_plan_builder_00000, 158)
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 160)
			return err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 159)
		conds = append(conds, cond)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 145)
	p.SetSchema(expression.NewSchema(schemaCols...))
	p.redundantSchema = expression.MergeSchema(p.redundantSchema, expression.NewSchema(rColumns[:commonLen]...))
	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *PlanBuilder) buildSelection(p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 161)
	b.optFlag = b.optFlag | flagPredicatePushDown
	if b.curClause != havingClause {
		trace_util_0.Count(_logical_plan_builder_00000, 165)
		b.curClause = whereClause
	}

	trace_util_0.Count(_logical_plan_builder_00000, 162)
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx)
	for _, cond := range conditions {
		trace_util_0.Count(_logical_plan_builder_00000, 166)
		expr, np, err := b.rewrite(cond, p, AggMapper, false)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 169)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 167)
		p = np
		if expr == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 170)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 168)
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			trace_util_0.Count(_logical_plan_builder_00000, 171)
			if con, ok := item.(*expression.Constant); ok && con.DeferredExpr == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 173)
				ret, _, err := expression.EvalBool(b.ctx, expression.CNFExprs{con}, chunk.Row{})
				if err != nil || ret {
					trace_util_0.Count(_logical_plan_builder_00000, 175)
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				trace_util_0.Count(_logical_plan_builder_00000, 174)
				dual := LogicalTableDual{}.Init(b.ctx)
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			trace_util_0.Count(_logical_plan_builder_00000, 172)
			expressions = append(expressions, item)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 163)
	if len(expressions) == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 176)
		return p, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 164)
	selection.Conditions = expressions
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (b *PlanBuilder) buildProjectionFieldNameFromColumns(field *ast.SelectField, c *expression.Column) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	trace_util_0.Count(_logical_plan_builder_00000, 177)
	if astCol, ok := getInnerFromParenthesesAndUnaryPlus(field.Expr).(*ast.ColumnNameExpr); ok {
		trace_util_0.Count(_logical_plan_builder_00000, 182)
		origColName, tblName, dbName = astCol.Name.Name, astCol.Name.Table, astCol.Name.Schema
	}
	trace_util_0.Count(_logical_plan_builder_00000, 178)
	if field.AsName.L != "" {
		trace_util_0.Count(_logical_plan_builder_00000, 183)
		colName = field.AsName
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 184)
		{
			colName = origColName
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 179)
	if tblName.L == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 185)
		tblName = c.TblName
	}
	trace_util_0.Count(_logical_plan_builder_00000, 180)
	if dbName.L == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 186)
		dbName = c.DBName
	}
	trace_util_0.Count(_logical_plan_builder_00000, 181)
	return colName, origColName, tblName, c.OrigTblName, c.DBName
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(field *ast.SelectField) (model.CIStr, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 187)
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		trace_util_0.Count(_logical_plan_builder_00000, 191)
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 188)
	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	funcCall, isFuncCall := innerExpr.(*ast.FuncCallExpr)
	// When used to produce a result set column, NAME_CONST() causes the column to have the given name.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
	if isFuncCall && funcCall.FnName.L == ast.NameConst {
		trace_util_0.Count(_logical_plan_builder_00000, 192)
		if v, err := evalAstExpr(b.ctx, funcCall.Args[0]); err == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 194)
			if s, err := v.ToString(); err == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 195)
				return model.NewCIStr(s), nil
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 193)
		return model.NewCIStr(""), ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
	}
	trace_util_0.Count(_logical_plan_builder_00000, 189)
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		trace_util_0.Count(_logical_plan_builder_00000, 196)
		return model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	trace_util_0.Count(_logical_plan_builder_00000, 190)
	switch valueExpr.Kind() {
	case types.KindString:
		trace_util_0.Count(_logical_plan_builder_00000, 197)
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 205)
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		trace_util_0.Count(_logical_plan_builder_00000, 198)
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			trace_util_0.Count(_logical_plan_builder_00000, 206)
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		trace_util_0.Count(_logical_plan_builder_00000, 199)
		return model.NewCIStr(fieldName), nil
	case types.KindNull:
		trace_util_0.Count(_logical_plan_builder_00000, 200)
		// See #4053, #3685
		return model.NewCIStr("NULL"), nil
	case types.KindBinaryLiteral:
		trace_util_0.Count(_logical_plan_builder_00000, 201)
		// Don't rewrite BIT literal or HEX literals
		return model.NewCIStr(field.Text()), nil
	case types.KindInt64:
		trace_util_0.Count(_logical_plan_builder_00000, 202)
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.Flag) {
			trace_util_0.Count(_logical_plan_builder_00000, 207)
			if i := valueExpr.GetValue().(int64); i == 0 {
				trace_util_0.Count(_logical_plan_builder_00000, 209)
				return model.NewCIStr("FALSE"), nil
			}
			trace_util_0.Count(_logical_plan_builder_00000, 208)
			return model.NewCIStr("TRUE"), nil
		}
		trace_util_0.Count(_logical_plan_builder_00000, 203)
		fallthrough

	default:
		trace_util_0.Count(_logical_plan_builder_00000, 204)
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return model.NewCIStr(fieldName), nil
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(id, position int, field *ast.SelectField, expr expression.Expression) (*expression.Column, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 210)
	var origTblName, tblName, origColName, colName, dbName model.CIStr
	if c, ok := expr.(*expression.Column); ok && !c.IsReferenced {
		trace_util_0.Count(_logical_plan_builder_00000, 212)
		// Field is a column reference.
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, c)
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 213)
		if field.AsName.L != "" {
			trace_util_0.Count(_logical_plan_builder_00000, 214)
			// Field has alias.
			colName = field.AsName
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 215)
			{
				// Other: field is an expression.
				var err error
				if colName, err = b.buildProjectionFieldNameFromExpressions(field); err != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 216)
					return nil, err
				}
			}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 211)
	return &expression.Column{
		UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
		RetType:     expr.GetType(),
	}, nil
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int, considerWindow bool) (LogicalPlan, int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 217)
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	for i, field := range fields {
		trace_util_0.Count(_logical_plan_builder_00000, 219)
		if !field.Auxiliary {
			trace_util_0.Count(_logical_plan_builder_00000, 225)
			oldLen++
		}

		trace_util_0.Count(_logical_plan_builder_00000, 220)
		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schema columns.
		if considerWindow && !isWindowFuncField {
			trace_util_0.Count(_logical_plan_builder_00000, 226)
			col := p.Schema().Columns[i]
			proj.Exprs = append(proj.Exprs, col)
			schema.Append(col)
			continue
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 227)
			if !considerWindow && isWindowFuncField {
				trace_util_0.Count(_logical_plan_builder_00000, 228)
				expr := expression.Zero
				proj.Exprs = append(proj.Exprs, expr)
				col, err := b.buildProjectionField(proj.id, schema.Len()+1, field, expr)
				if err != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 230)
					return nil, 0, err
				}
				trace_util_0.Count(_logical_plan_builder_00000, 229)
				schema.Append(col)
				continue
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 221)
		newExpr, np, err := b.rewriteWithPreprocess(field.Expr, p, mapper, windowMapper, true, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 231)
			return nil, 0, err
		}

		// For window functions in the order by clause, we will append an field for it.
		// We need rewrite the window mapper here so order by clause could find the added field.
		trace_util_0.Count(_logical_plan_builder_00000, 222)
		if considerWindow && isWindowFuncField && field.Auxiliary {
			trace_util_0.Count(_logical_plan_builder_00000, 232)
			if windowExpr, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				trace_util_0.Count(_logical_plan_builder_00000, 233)
				windowMapper[windowExpr] = i
			}
		}

		trace_util_0.Count(_logical_plan_builder_00000, 223)
		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, err := b.buildProjectionField(proj.id, schema.Len()+1, field, newExpr)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 234)
			return nil, 0, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 224)
		schema.Append(col)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 218)
	proj.SetSchema(schema)
	proj.SetChildren(p)
	return proj, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child LogicalPlan, length int) (*LogicalAggregation, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 235)
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.Init(b.ctx)
	plan4Agg.collectGroupByColumns()
	for _, col := range child.Schema().Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 238)
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 240)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 239)
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 236)
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schema.Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 241)
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	trace_util_0.Count(_logical_plan_builder_00000, 237)
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	trace_util_0.Count(_logical_plan_builder_00000, 242)
	resultTp := types.NewFieldType(types.MergeFieldType(a.Tp, b.Tp))
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.Tp == mysql.TypeNewDecimal {
		trace_util_0.Count(_logical_plan_builder_00000, 245)
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.Flag &= b.Flag & mysql.UnsignedFlag
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 246)
		{
			// Non-decimal results will be unsigned when the first SQL statement result in the union is unsigned.
			resultTp.Flag |= a.Flag & mysql.UnsignedFlag
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 243)
	resultTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
	// `Flen - Decimal` is the fraction before '.'
	resultTp.Flen = mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal) + resultTp.Decimal
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) && resultTp.Flen < mysql.MaxIntWidth {
		trace_util_0.Count(_logical_plan_builder_00000, 247)
		resultTp.Flen = mysql.MaxIntWidth
	}
	trace_util_0.Count(_logical_plan_builder_00000, 244)
	resultTp.Charset = a.Charset
	resultTp.Collate = a.Collate
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

func (b *PlanBuilder) buildProjection4Union(u *LogicalUnionAll) {
	trace_util_0.Count(_logical_plan_builder_00000, 248)
	unionCols := make([]*expression.Column, 0, u.children[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.children[0].Schema().Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 250)
		resultTp := col.RetType
		for j := 1; j < len(u.children); j++ {
			trace_util_0.Count(_logical_plan_builder_00000, 252)
			childTp := u.children[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 251)
		unionCols = append(unionCols, &expression.Column{
			ColName:  col.ColName,
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	trace_util_0.Count(_logical_plan_builder_00000, 249)
	u.schema = expression.NewSchema(unionCols...)
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.children {
		trace_util_0.Count(_logical_plan_builder_00000, 253)
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 255)
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				trace_util_0.Count(_logical_plan_builder_00000, 256)
				exprs[i] = expression.BuildCastFunction4Union(b.ctx, srcCol, dstType)
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 257)
				{
					exprs[i] = srcCol
				}
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 254)
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, avoidColumnEvaluator: true}.Init(b.ctx)
		proj.SetSchema(u.schema.Clone())
		proj.SetChildren(child)
		u.children[childID] = proj
	}
}

func (b *PlanBuilder) buildUnion(union *ast.UnionStmt) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 258)
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(union.SelectList.Selects)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 265)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 259)
	unionDistinctPlan := b.buildUnionAll(distinctSelectPlans)
	if unionDistinctPlan != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 266)
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 268)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 267)
		if len(allSelectPlans) > 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 269)
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 260)
	unionAllPlan := b.buildUnionAll(allSelectPlans)
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 270)
		unionPlan = unionAllPlan
	}

	trace_util_0.Count(_logical_plan_builder_00000, 261)
	oldLen := unionPlan.Schema().Len()

	if union.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 271)
		unionPlan, err = b.buildSort(unionPlan, union.OrderBy.Items, nil, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 272)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 262)
	if union.Limit != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 273)
		unionPlan, err = b.buildLimit(unionPlan, union.Limit)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 274)
			return nil, err
		}
	}

	// Fix issue #8189 (https://github.com/pingcap/tidb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	trace_util_0.Count(_logical_plan_builder_00000, 263)
	if oldLen != unionPlan.Schema().Len() {
		trace_util_0.Count(_logical_plan_builder_00000, 275)
		proj := LogicalProjection{Exprs: expression.Column2Exprs(unionPlan.Schema().Columns[:oldLen])}.Init(b.ctx)
		proj.SetChildren(unionPlan)
		schema := expression.NewSchema(unionPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 277)
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		trace_util_0.Count(_logical_plan_builder_00000, 276)
		proj.SetSchema(schema)
		return proj, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 264)
	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref: https://dev.mysql.com/doc/refman/5.7/en/union.html
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (b *PlanBuilder) divideUnionSelectPlans(selects []*ast.SelectStmt) (distinctSelects []LogicalPlan, allSelects []LogicalPlan, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 278)
	firstUnionAllIdx, columnNums := 0, -1
	// The last slot is reserved for appending distinct union outside this function.
	children := make([]LogicalPlan, len(selects), len(selects)+1)
	for i := len(selects) - 1; i >= 0; i-- {
		trace_util_0.Count(_logical_plan_builder_00000, 280)
		stmt := selects[i]
		if firstUnionAllIdx == 0 && stmt.IsAfterUnionDistinct {
			trace_util_0.Count(_logical_plan_builder_00000, 285)
			firstUnionAllIdx = i + 1
		}

		trace_util_0.Count(_logical_plan_builder_00000, 281)
		selectPlan, err := b.buildSelect(stmt)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 286)
			return nil, nil, err
		}

		trace_util_0.Count(_logical_plan_builder_00000, 282)
		if columnNums == -1 {
			trace_util_0.Count(_logical_plan_builder_00000, 287)
			columnNums = selectPlan.Schema().Len()
		}
		trace_util_0.Count(_logical_plan_builder_00000, 283)
		if selectPlan.Schema().Len() != columnNums {
			trace_util_0.Count(_logical_plan_builder_00000, 288)
			return nil, nil, ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		trace_util_0.Count(_logical_plan_builder_00000, 284)
		children[i] = selectPlan
	}
	trace_util_0.Count(_logical_plan_builder_00000, 279)
	return children[:firstUnionAllIdx], children[firstUnionAllIdx:], nil
}

func (b *PlanBuilder) buildUnionAll(subPlan []LogicalPlan) LogicalPlan {
	trace_util_0.Count(_logical_plan_builder_00000, 289)
	if len(subPlan) == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 291)
		return nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 290)
	u := LogicalUnionAll{}.Init(b.ctx)
	u.children = subPlan
	b.buildProjection4Union(u)
	return u
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// String implements fmt.Stringer interface.
func (by *ByItems) String() string {
	trace_util_0.Count(_logical_plan_builder_00000, 292)
	if by.Desc {
		trace_util_0.Count(_logical_plan_builder_00000, 294)
		return fmt.Sprintf("%s true", by.Expr)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 293)
	return by.Expr.String()
}

// Clone makes a copy of ByItems.
func (by *ByItems) Clone() *ByItems {
	trace_util_0.Count(_logical_plan_builder_00000, 295)
	return &ByItems{Expr: by.Expr.Clone(), Desc: by.Desc}
}

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct {
}

func (t *itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 296)
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 298)
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 297)
	return inNode, false
}

func (t *itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 299)
	return inNode, false
}

func (b *PlanBuilder) buildSort(p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int) (*LogicalSort, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 300)
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		trace_util_0.Count(_logical_plan_builder_00000, 303)
		b.curClause = globalOrderByClause
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 304)
		{
			b.curClause = orderByClause
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 301)
	sort := LogicalSort{}.Init(b.ctx)
	exprs := make([]*ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for _, item := range byItems {
		trace_util_0.Count(_logical_plan_builder_00000, 305)
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewriteWithPreprocess(item.Expr, p, aggMapper, windowMapper, true, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 307)
			return nil, err
		}

		trace_util_0.Count(_logical_plan_builder_00000, 306)
		p = np
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	trace_util_0.Count(_logical_plan_builder_00000, 302)
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx sessionctx.Context, n ast.Node) (uVal uint64, isNull bool, isExpectedType bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 308)
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 311)
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 312)
		if !v.InExecute {
			trace_util_0.Count(_logical_plan_builder_00000, 318)
			return 0, false, true
		}
		trace_util_0.Count(_logical_plan_builder_00000, 313)
		param, err := expression.GetParamExpression(ctx, v)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 319)
			return 0, false, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 314)
		str, isNull, err := expression.GetStringFromConstant(ctx, param)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 320)
			return 0, false, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 315)
		if isNull {
			trace_util_0.Count(_logical_plan_builder_00000, 321)
			return 0, true, true
		}
		trace_util_0.Count(_logical_plan_builder_00000, 316)
		val = str
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 317)
		return 0, false, false
	}
	trace_util_0.Count(_logical_plan_builder_00000, 309)
	switch v := val.(type) {
	case uint64:
		trace_util_0.Count(_logical_plan_builder_00000, 322)
		return v, false, true
	case int64:
		trace_util_0.Count(_logical_plan_builder_00000, 323)
		if v >= 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 326)
			return uint64(v), false, true
		}
	case string:
		trace_util_0.Count(_logical_plan_builder_00000, 324)
		sc := ctx.GetSessionVars().StmtCtx
		uVal, err := types.StrToUint(sc, v)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 327)
			return 0, false, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 325)
		return uVal, false, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 310)
	return 0, false, false
}

func extractLimitCountOffset(ctx sessionctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 328)
	var isExpectedType bool
	if limit.Count != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 331)
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count)
		if !isExpectedType {
			trace_util_0.Count(_logical_plan_builder_00000, 332)
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 329)
	if limit.Offset != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 333)
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset)
		if !isExpectedType {
			trace_util_0.Count(_logical_plan_builder_00000, 334)
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 330)
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 335)
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 339)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 336)
	if count > math.MaxUint64-offset {
		trace_util_0.Count(_logical_plan_builder_00000, 340)
		count = math.MaxUint64 - offset
	}
	trace_util_0.Count(_logical_plan_builder_00000, 337)
	if offset+count == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 341)
		tableDual := LogicalTableDual{RowCount: 0}.Init(b.ctx)
		tableDual.schema = src.Schema()
		return tableDual, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 338)
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx)
	li.SetChildren(src)
	return li, nil
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.ColumnName, b *ast.ColumnName) bool {
	trace_util_0.Count(_logical_plan_builder_00000, 342)
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		trace_util_0.Count(_logical_plan_builder_00000, 344)
		if a.Table.L == "" || a.Table.L == b.Table.L {
			trace_util_0.Count(_logical_plan_builder_00000, 345)
			return a.Name.L == b.Name.L
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 343)
	return false
}

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr, ignoreAsName bool) bool {
	trace_util_0.Count(_logical_plan_builder_00000, 346)
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 348)
		if f.AsName.L == "" || ignoreAsName {
			trace_util_0.Count(_logical_plan_builder_00000, 350)
			if curCol, isCol := f.Expr.(*ast.ColumnNameExpr); isCol {
				trace_util_0.Count(_logical_plan_builder_00000, 352)
				return curCol.Name.Name.L == col.Name.Name.L
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 353)
				if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
					trace_util_0.Count(_logical_plan_builder_00000, 354)
					// Fix issue 7331
					// If there are some function calls in SelectField, we check if
					// ColumnNameExpr in GroupByClause matches one of these function calls.
					// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
					// `concat(k1,k2)` matches with function call concat(k1, k2).
					return strings.ToLower(f.Text()) == col.Name.Name.L
				}
			}
			// a expression without as name can't be matched.
			trace_util_0.Count(_logical_plan_builder_00000, 351)
			return false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 349)
		return f.AsName.L == col.Name.Name.L
	}
	trace_util_0.Count(_logical_plan_builder_00000, 347)
	return false
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 355)
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		trace_util_0.Count(_logical_plan_builder_00000, 357)
		if field.Auxiliary {
			trace_util_0.Count(_logical_plan_builder_00000, 359)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 358)
		if matchField(field, v, ignoreAsName) {
			trace_util_0.Count(_logical_plan_builder_00000, 360)
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				trace_util_0.Count(_logical_plan_builder_00000, 362)
				return i, nil
			}
			trace_util_0.Count(_logical_plan_builder_00000, 361)
			if matchedExpr == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 363)
				matchedExpr = curCol
				index = i
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 364)
				if !colMatch(matchedExpr.(*ast.ColumnNameExpr).Name, curCol.Name) &&
					!colMatch(curCol.Name, matchedExpr.(*ast.ColumnNameExpr).Name) {
					trace_util_0.Count(_logical_plan_builder_00000, 365)
					return -1, ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
				}
			}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 356)
	return
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	orderBy      bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	curClause    clauseCode
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 366)
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 368)
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 369)
		a.inWindowFunc = true
	case *ast.WindowSpec:
		trace_util_0.Count(_logical_plan_builder_00000, 370)
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
		trace_util_0.Count(_logical_plan_builder_00000, 371)
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 372)
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 373)
		a.inExpr = true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 367)
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromSchema(v *ast.ColumnNameExpr, schema *expression.Schema) (int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 374)
	col, err := schema.FindColumn(v.Name)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 378)
		return -1, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 375)
	if col == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 379)
		return -1, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 376)
	newColName := &ast.ColumnName{
		Schema: col.DBName,
		Table:  col.TblName,
		Name:   col.ColName,
	}
	for i, field := range a.selectFields {
		trace_util_0.Count(_logical_plan_builder_00000, 380)
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(c.Name, newColName) {
			trace_util_0.Count(_logical_plan_builder_00000, 381)
			return i, nil
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 377)
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 382)
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 384)
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 385)
		a.inWindowFunc = false
		if a.curClause == havingClause {
			trace_util_0.Count(_logical_plan_builder_00000, 395)
			a.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(v.F)
			return node, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 386)
		if a.curClause == orderByClause {
			trace_util_0.Count(_logical_plan_builder_00000, 396)
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    model.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		trace_util_0.Count(_logical_plan_builder_00000, 387)
		a.inWindowSpec = false
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 388)
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.orderBy && a.inExpr) {
			trace_util_0.Count(_logical_plan_builder_00000, 397)
			resolveFieldsFirst = false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 389)
		if !a.inAggFunc && !a.orderBy {
			trace_util_0.Count(_logical_plan_builder_00000, 398)
			for _, item := range a.gbyItems {
				trace_util_0.Count(_logical_plan_builder_00000, 399)
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					trace_util_0.Count(_logical_plan_builder_00000, 400)
					resolveFieldsFirst = false
					break
				}
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 390)
		var index int
		if resolveFieldsFirst {
			trace_util_0.Count(_logical_plan_builder_00000, 401)
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 404)
				return node, false
			}
			trace_util_0.Count(_logical_plan_builder_00000, 402)
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				trace_util_0.Count(_logical_plan_builder_00000, 405)
				a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			trace_util_0.Count(_logical_plan_builder_00000, 403)
			if index == -1 {
				trace_util_0.Count(_logical_plan_builder_00000, 406)
				if a.orderBy {
					trace_util_0.Count(_logical_plan_builder_00000, 407)
					index, a.err = a.resolveFromSchema(v, a.p.Schema())
				} else {
					trace_util_0.Count(_logical_plan_builder_00000, 408)
					{
						index, a.err = resolveFromSelectFields(v, a.selectFields, true)
					}
				}
			}
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 409)
			{
				// We should ignore the err when resolving from schema. Because we could resolve successfully
				// when considering select fields.
				var err error
				index, err = a.resolveFromSchema(v, a.p.Schema())
				_ = err
				if index == -1 && a.curClause != windowClause {
					trace_util_0.Count(_logical_plan_builder_00000, 410)
					index, a.err = resolveFromSelectFields(v, a.selectFields, false)
					if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
						trace_util_0.Count(_logical_plan_builder_00000, 411)
						a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
						return node, false
					}
				}
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 391)
		if a.err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 412)
			return node, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 392)
		if index == -1 {
			trace_util_0.Count(_logical_plan_builder_00000, 413)
			// If we can't find it any where, it may be a correlated columns.
			for _, schema := range a.outerSchemas {
				trace_util_0.Count(_logical_plan_builder_00000, 415)
				col, err1 := schema.FindColumn(v.Name)
				if err1 != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 417)
					a.err = err1
					return node, false
				}
				trace_util_0.Count(_logical_plan_builder_00000, 416)
				if col != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 418)
					return n, true
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 414)
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 393)
		if a.inAggFunc {
			trace_util_0.Count(_logical_plan_builder_00000, 419)
			return a.selectFields[index].Expr, true
		}
		trace_util_0.Count(_logical_plan_builder_00000, 394)
		a.colMapper[v] = index
	}
	trace_util_0.Count(_logical_plan_builder_00000, 383)
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 420)
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
	}
	if sel.GroupBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 424)
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	trace_util_0.Count(_logical_plan_builder_00000, 421)
	if sel.Having != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 425)
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			trace_util_0.Count(_logical_plan_builder_00000, 427)
			return nil, nil, errors.Trace(extractor.err)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 426)
		sel.Having.Expr = n.(ast.ExprNode)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 422)
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.orderBy = true
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 428)
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			trace_util_0.Count(_logical_plan_builder_00000, 429)
			if ast.HasWindowFlag(item.Expr) {
				trace_util_0.Count(_logical_plan_builder_00000, 432)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 430)
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				trace_util_0.Count(_logical_plan_builder_00000, 433)
				return nil, nil, errors.Trace(extractor.err)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 431)
			item.Expr = n.(ast.ExprNode)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 423)
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncs(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	trace_util_0.Count(_logical_plan_builder_00000, 434)
	extractor := &AggregateFuncExtractor{}
	for _, f := range fields {
		trace_util_0.Count(_logical_plan_builder_00000, 437)
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 435)
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int)

	for i, agg := range aggList {
		trace_util_0.Count(_logical_plan_builder_00000, 438)
		totalAggMapper[agg] = i
	}
	trace_util_0.Count(_logical_plan_builder_00000, 436)
	return aggList, totalAggMapper
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 439)
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
	}
	extractor.curClause = windowClause
	for _, field := range sel.Fields.Fields {
		trace_util_0.Count(_logical_plan_builder_00000, 443)
		if !ast.HasWindowFlag(field.Expr) {
			trace_util_0.Count(_logical_plan_builder_00000, 446)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 444)
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			trace_util_0.Count(_logical_plan_builder_00000, 447)
			return nil, extractor.err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 445)
		field.Expr = n.(ast.ExprNode)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 440)
	for _, spec := range sel.WindowSpecs {
		trace_util_0.Count(_logical_plan_builder_00000, 448)
		_, ok := spec.Accept(extractor)
		if !ok {
			trace_util_0.Count(_logical_plan_builder_00000, 449)
			return nil, extractor.err
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 441)
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 450)
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			trace_util_0.Count(_logical_plan_builder_00000, 451)
			if !ast.HasWindowFlag(item.Expr) {
				trace_util_0.Count(_logical_plan_builder_00000, 454)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 452)
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				trace_util_0.Count(_logical_plan_builder_00000, 455)
				return nil, extractor.err
			}
			trace_util_0.Count(_logical_plan_builder_00000, 453)
			item.Expr = n.(ast.ExprNode)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 442)
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx     sessionctx.Context
	fields  []*ast.SelectField
	schema  *expression.Schema
	err     error
	inExpr  bool
	isParam bool
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 456)
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 458)
		return inNode, true
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 459)
		newNode := expression.ConstructPositionExpr(n)
		g.isParam = true
		return newNode, true
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
		trace_util_0.Count(_logical_plan_builder_00000, 460)
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 461)
		g.inExpr = true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 457)
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 462)
	extractor := &AggregateFuncExtractor{}
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 464)
		col, err := g.schema.FindColumn(v.Name)
		if col == nil || !g.inExpr {
			trace_util_0.Count(_logical_plan_builder_00000, 471)
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 475)
				return inNode, false
			}
			trace_util_0.Count(_logical_plan_builder_00000, 472)
			if col != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 476)
				return inNode, true
			}
			trace_util_0.Count(_logical_plan_builder_00000, 473)
			if index != -1 {
				trace_util_0.Count(_logical_plan_builder_00000, 477)
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					trace_util_0.Count(_logical_plan_builder_00000, 478)
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else {
					trace_util_0.Count(_logical_plan_builder_00000, 479)
					if ast.HasWindowFlag(ret) {
						trace_util_0.Count(_logical_plan_builder_00000, 480)
						err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to window function")
					} else {
						trace_util_0.Count(_logical_plan_builder_00000, 481)
						{
							return ret, true
						}
					}
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 474)
			g.err = err
			return inNode, false
		}
	case *ast.PositionExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 465)
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx, v)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 482)
			g.err = ErrUnknown.GenWithStackByArgs()
		}
		trace_util_0.Count(_logical_plan_builder_00000, 466)
		if err != nil || isNull {
			trace_util_0.Count(_logical_plan_builder_00000, 483)
			return inNode, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 467)
		if pos < 1 || pos > len(g.fields) {
			trace_util_0.Count(_logical_plan_builder_00000, 484)
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 468)
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 485)
			g.err = ErrWrongGroupField.GenWithStackByArgs(g.fields[pos-1].Text())
			return inNode, false
		}
		trace_util_0.Count(_logical_plan_builder_00000, 469)
		return ret, true
	case *ast.ValuesExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 470)
		if v.Column == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 486)
			g.err = ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 463)
	return inNode, true
}

func tblInfoFromCol(from ast.ResultSetNode, col *expression.Column) *model.TableInfo {
	trace_util_0.Count(_logical_plan_builder_00000, 487)
	var tableList []*ast.TableName
	tableList = extractTableList(from, tableList, true)
	for _, field := range tableList {
		trace_util_0.Count(_logical_plan_builder_00000, 489)
		if field.Name.L == col.TblName.L {
			trace_util_0.Count(_logical_plan_builder_00000, 492)
			return field.TableInfo
		}
		trace_util_0.Count(_logical_plan_builder_00000, 490)
		if field.Name.L != col.TblName.L {
			trace_util_0.Count(_logical_plan_builder_00000, 493)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 491)
		if field.Schema.L == col.DBName.L {
			trace_util_0.Count(_logical_plan_builder_00000, 494)
			return field.TableInfo
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 488)
	return nil
}

func buildFuncDependCol(p LogicalPlan, cond ast.ExprNode) (*expression.Column, *expression.Column) {
	trace_util_0.Count(_logical_plan_builder_00000, 495)
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 502)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 496)
	if binOpExpr.Op != opcode.EQ {
		trace_util_0.Count(_logical_plan_builder_00000, 503)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 497)
	lColExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr)
	if !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 504)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 498)
	rColExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr)
	if !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 505)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 499)
	lCol, err := p.Schema().FindColumn(lColExpr.Name)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 506)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 500)
	rCol, err := p.Schema().FindColumn(rColExpr.Name)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 507)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 501)
	return lCol, rCol
}

func buildWhereFuncDepend(p LogicalPlan, where ast.ExprNode) map[*expression.Column]*expression.Column {
	trace_util_0.Count(_logical_plan_builder_00000, 508)
	whereConditions := splitWhere(where)
	colDependMap := make(map[*expression.Column]*expression.Column, 2*len(whereConditions))
	for _, cond := range whereConditions {
		trace_util_0.Count(_logical_plan_builder_00000, 510)
		lCol, rCol := buildFuncDependCol(p, cond)
		if lCol == nil || rCol == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 512)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 511)
		colDependMap[lCol] = rCol
		colDependMap[rCol] = lCol
	}
	trace_util_0.Count(_logical_plan_builder_00000, 509)
	return colDependMap
}

func buildJoinFuncDepend(p LogicalPlan, from ast.ResultSetNode) map[*expression.Column]*expression.Column {
	trace_util_0.Count(_logical_plan_builder_00000, 513)
	switch x := from.(type) {
	case *ast.Join:
		trace_util_0.Count(_logical_plan_builder_00000, 514)
		if x.On == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 518)
			return nil
		}
		trace_util_0.Count(_logical_plan_builder_00000, 515)
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*expression.Column]*expression.Column, len(onConditions))
		for _, cond := range onConditions {
			trace_util_0.Count(_logical_plan_builder_00000, 519)
			lCol, rCol := buildFuncDependCol(p, cond)
			if lCol == nil || rCol == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 522)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 520)
			lTbl := tblInfoFromCol(x.Left, lCol)
			if lTbl == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 523)
				lCol, rCol = rCol, lCol
			}
			trace_util_0.Count(_logical_plan_builder_00000, 521)
			switch x.Tp {
			case ast.CrossJoin:
				trace_util_0.Count(_logical_plan_builder_00000, 524)
				colDependMap[lCol] = rCol
				colDependMap[rCol] = lCol
			case ast.LeftJoin:
				trace_util_0.Count(_logical_plan_builder_00000, 525)
				colDependMap[rCol] = lCol
			case ast.RightJoin:
				trace_util_0.Count(_logical_plan_builder_00000, 526)
				colDependMap[lCol] = rCol
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 516)
		return colDependMap
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 517)
		return nil
	}
}

func checkColFuncDepend(p LogicalPlan, col *expression.Column, tblInfo *model.TableInfo, gbyCols map[*expression.Column]struct{}, whereDepends, joinDepends map[*expression.Column]*expression.Column) bool {
	trace_util_0.Count(_logical_plan_builder_00000, 527)
	for _, index := range tblInfo.Indices {
		trace_util_0.Count(_logical_plan_builder_00000, 530)
		if !index.Unique {
			trace_util_0.Count(_logical_plan_builder_00000, 533)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 531)
		funcDepend := true
		for _, indexCol := range index.Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 534)
			iColInfo := tblInfo.Columns[indexCol.Offset]
			if !mysql.HasNotNullFlag(iColInfo.Flag) {
				trace_util_0.Count(_logical_plan_builder_00000, 540)
				funcDepend = false
				break
			}
			trace_util_0.Count(_logical_plan_builder_00000, 535)
			cn := &ast.ColumnName{
				Schema: col.DBName,
				Table:  col.TblName,
				Name:   iColInfo.Name,
			}
			iCol, err := p.Schema().FindColumn(cn)
			if err != nil || iCol == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 541)
				funcDepend = false
				break
			}
			trace_util_0.Count(_logical_plan_builder_00000, 536)
			if _, ok := gbyCols[iCol]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 542)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 537)
			if wCol, ok := whereDepends[iCol]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 543)
				if _, ok = gbyCols[wCol]; ok {
					trace_util_0.Count(_logical_plan_builder_00000, 544)
					continue
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 538)
			if jCol, ok := joinDepends[iCol]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 545)
				if _, ok = gbyCols[jCol]; ok {
					trace_util_0.Count(_logical_plan_builder_00000, 546)
					continue
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 539)
			funcDepend = false
			break
		}
		trace_util_0.Count(_logical_plan_builder_00000, 532)
		if funcDepend {
			trace_util_0.Count(_logical_plan_builder_00000, 547)
			return true
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 528)
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 548)
		if !mysql.HasPriKeyFlag(colInfo.Flag) {
			trace_util_0.Count(_logical_plan_builder_00000, 554)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 549)
		hasPrimaryField = true
		pCol, err := p.Schema().FindColumn(&ast.ColumnName{
			Schema: col.DBName,
			Table:  col.TblName,
			Name:   colInfo.Name,
		})
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 555)
			primaryFuncDepend = false
			break
		}
		trace_util_0.Count(_logical_plan_builder_00000, 550)
		if _, ok := gbyCols[pCol]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 556)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 551)
		if wCol, ok := whereDepends[pCol]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 557)
			if _, ok = gbyCols[wCol]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 558)
				continue
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 552)
		if jCol, ok := joinDepends[pCol]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 559)
			if _, ok = gbyCols[jCol]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 560)
				continue
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 553)
		primaryFuncDepend = false
		break
	}
	trace_util_0.Count(_logical_plan_builder_00000, 529)
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupBy(p LogicalPlan, expr ast.ExprNode, offset int, loc string, gbyCols map[*expression.Column]struct{}, gbyExprs []ast.ExprNode, notInGbyCols map[*expression.Column]ErrExprLoc) {
	trace_util_0.Count(_logical_plan_builder_00000, 561)
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		trace_util_0.Count(_logical_plan_builder_00000, 565)
		return
	}
	trace_util_0.Count(_logical_plan_builder_00000, 562)
	if _, ok := expr.(*ast.ColumnNameExpr); !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 566)
		for _, gbyExpr := range gbyExprs {
			trace_util_0.Count(_logical_plan_builder_00000, 567)
			if reflect.DeepEqual(gbyExpr, expr) {
				trace_util_0.Count(_logical_plan_builder_00000, 568)
				return
			}
		}
	}
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	trace_util_0.Count(_logical_plan_builder_00000, 563)
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		trace_util_0.Count(_logical_plan_builder_00000, 569)
		if f.FnName.L == ast.AnyValue {
			trace_util_0.Count(_logical_plan_builder_00000, 570)
			return
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 564)
	colMap := make(map[*expression.Column]struct{}, len(p.Schema().Columns))
	allColFromExprNode(p, expr, colMap)
	for col := range colMap {
		trace_util_0.Count(_logical_plan_builder_00000, 571)
		if _, ok := gbyCols[col]; !ok {
			trace_util_0.Count(_logical_plan_builder_00000, 572)
			notInGbyCols[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupBy(p LogicalPlan, sel *ast.SelectStmt) (err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 573)
	if sel.GroupBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 575)
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 576)
		{
			err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel.Fields.Fields)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 574)
	return err
}

func (b *PlanBuilder) checkOnlyFullGroupByWithGroupClause(p LogicalPlan, sel *ast.SelectStmt) error {
	trace_util_0.Count(_logical_plan_builder_00000, 577)
	gbyCols := make(map[*expression.Column]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	schema := p.Schema()
	for _, byItem := range sel.GroupBy.Items {
		trace_util_0.Count(_logical_plan_builder_00000, 583)
		if colExpr, ok := byItem.Expr.(*ast.ColumnNameExpr); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 584)
			col, err := schema.FindColumn(colExpr.Name)
			if err != nil || col == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 586)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 585)
			gbyCols[col] = struct{}{}
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 587)
			{
				gbyExprs = append(gbyExprs, byItem.Expr)
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 578)
	notInGbyCols := make(map[*expression.Column]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		trace_util_0.Count(_logical_plan_builder_00000, 588)
		if field.Auxiliary {
			trace_util_0.Count(_logical_plan_builder_00000, 590)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 589)
		checkExprInGroupBy(p, field.Expr, offset, ErrExprInSelect, gbyCols, gbyExprs, notInGbyCols)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 579)
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 591)
		for offset, item := range sel.OrderBy.Items {
			trace_util_0.Count(_logical_plan_builder_00000, 592)
			checkExprInGroupBy(p, item.Expr, offset, ErrExprInOrderBy, gbyCols, gbyExprs, notInGbyCols)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 580)
	if len(notInGbyCols) == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 593)
		return nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 581)
	whereDepends := buildWhereFuncDepend(p, sel.Where)
	joinDepends := buildJoinFuncDepend(p, sel.From.TableRefs)
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyCols))
	for col, errExprLoc := range notInGbyCols {
		trace_util_0.Count(_logical_plan_builder_00000, 594)
		tblInfo := tblInfoFromCol(sel.From.TableRefs, col)
		if tblInfo == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 599)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 595)
		if _, ok := tblMap[tblInfo]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 600)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 596)
		if checkColFuncDepend(p, col, tblInfo, gbyCols, whereDepends, joinDepends) {
			trace_util_0.Count(_logical_plan_builder_00000, 601)
			tblMap[tblInfo] = struct{}{}
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 597)
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			trace_util_0.Count(_logical_plan_builder_00000, 602)
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.Fields.Fields[errExprLoc.Offset].Text())
		case ErrExprInOrderBy:
			trace_util_0.Count(_logical_plan_builder_00000, 603)
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		trace_util_0.Count(_logical_plan_builder_00000, 598)
		return nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 582)
	return nil
}

func (b *PlanBuilder) checkOnlyFullGroupByWithOutGroupClause(p LogicalPlan, fields []*ast.SelectField) error {
	trace_util_0.Count(_logical_plan_builder_00000, 604)
	resolver := colResolverForOnlyFullGroupBy{}
	for idx, field := range fields {
		trace_util_0.Count(_logical_plan_builder_00000, 606)
		resolver.exprIdx = idx
		field.Accept(&resolver)
		err := resolver.Check()
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 607)
			return err
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 605)
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	firstNonAggCol       *ast.ColumnName
	exprIdx              int
	firstNonAggColIdx    int
	hasAggFuncOrAnyValue bool
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 608)
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 610)
		c.hasAggFuncOrAnyValue = true
		return node, true
	case *ast.FuncCallExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 611)
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			trace_util_0.Count(_logical_plan_builder_00000, 615)
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 612)
		if c.firstNonAggCol == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 616)
			c.firstNonAggCol, c.firstNonAggColIdx = t.Name, c.exprIdx
		}
		trace_util_0.Count(_logical_plan_builder_00000, 613)
		return node, true
	case *ast.SubqueryExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 614)
		return node, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 609)
	return node, false
}

func (c *colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 617)
	return node, true
}

func (c *colResolverForOnlyFullGroupBy) Check() error {
	trace_util_0.Count(_logical_plan_builder_00000, 618)
	if c.hasAggFuncOrAnyValue && c.firstNonAggCol != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 620)
		return ErrMixOfGroupFuncAndFields.GenWithStackByArgs(c.firstNonAggColIdx+1, c.firstNonAggCol.Name.O)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 619)
	return nil
}

type colResolver struct {
	p    LogicalPlan
	cols map[*expression.Column]struct{}
}

func (c *colResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 621)
	switch inNode.(type) {
	case *ast.ColumnNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 623)
		return inNode, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 622)
	return inNode, false
}

func (c *colResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 624)
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 626)
		col, err := c.p.Schema().FindColumn(v.Name)
		if err == nil && col != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 627)
			c.cols[col] = struct{}{}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 625)
	return inNode, true
}

func allColFromExprNode(p LogicalPlan, n ast.Node, cols map[*expression.Column]struct{}) {
	trace_util_0.Count(_logical_plan_builder_00000, 628)
	extractor := &colResolver{
		p:    p,
		cols: cols,
	}
	n.Accept(extractor)
}

func (b *PlanBuilder) resolveGbyExprs(p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 629)
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:    b.ctx,
		fields: fields,
		schema: p.Schema(),
	}
	for _, item := range gby.Items {
		trace_util_0.Count(_logical_plan_builder_00000, 631)
		resolver.inExpr = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 635)
			return nil, nil, errors.Trace(resolver.err)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 632)
		if !resolver.isParam {
			trace_util_0.Count(_logical_plan_builder_00000, 636)
			item.Expr = retExpr.(ast.ExprNode)
		}

		trace_util_0.Count(_logical_plan_builder_00000, 633)
		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(itemExpr, p, nil, true)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 637)
			return nil, nil, err
		}

		trace_util_0.Count(_logical_plan_builder_00000, 634)
		exprs = append(exprs, expr)
		p = np
	}
	trace_util_0.Count(_logical_plan_builder_00000, 630)
	return p, exprs, nil
}

func (b *PlanBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 638)
	for i, field := range selectFields {
		trace_util_0.Count(_logical_plan_builder_00000, 640)
		if field.WildCard == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 644)
			resultList = append(resultList, field)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 641)
		if field.WildCard.Table.L == "" && i > 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 645)
			return nil, ErrInvalidWildCard
		}
		trace_util_0.Count(_logical_plan_builder_00000, 642)
		dbName := field.WildCard.Schema
		tblName := field.WildCard.Table
		findTblNameInSchema := false
		for _, col := range p.Schema().Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 646)
			if (dbName.L == "" || dbName.L == col.DBName.L) &&
				(tblName.L == "" || tblName.L == col.TblName.L) &&
				col.ID != model.ExtraHandleID {
				trace_util_0.Count(_logical_plan_builder_00000, 647)
				findTblNameInSchema = true
				colName := &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Schema: col.DBName,
						Table:  col.TblName,
						Name:   col.ColName,
					}}
				colName.SetType(col.GetType())
				field := &ast.SelectField{Expr: colName}
				field.SetText(col.ColName.O)
				resultList = append(resultList, field)
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 643)
		if !findTblNameInSchema {
			trace_util_0.Count(_logical_plan_builder_00000, 648)
			return nil, ErrBadTable.GenWithStackByArgs(tblName)
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 639)
	return resultList, nil
}

func (b *PlanBuilder) pushTableHints(hints []*ast.TableOptimizerHint) bool {
	trace_util_0.Count(_logical_plan_builder_00000, 649)
	var sortMergeTables, INLJTables, hashJoinTables []hintTableInfo
	for _, hint := range hints {
		trace_util_0.Count(_logical_plan_builder_00000, 652)
		switch hint.HintName.L {
		case TiDBMergeJoin:
			trace_util_0.Count(_logical_plan_builder_00000, 653)
			sortMergeTables = tableNames2HintTableInfo(hint.Tables)
		case TiDBIndexNestedLoopJoin:
			trace_util_0.Count(_logical_plan_builder_00000, 654)
			INLJTables = tableNames2HintTableInfo(hint.Tables)
		case TiDBHashJoin:
			trace_util_0.Count(_logical_plan_builder_00000, 655)
			hashJoinTables = tableNames2HintTableInfo(hint.Tables)
		default:
			trace_util_0.Count(_logical_plan_builder_00000, 656)
			// ignore hints that not implemented
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 650)
	if len(sortMergeTables)+len(INLJTables)+len(hashJoinTables) > 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 657)
		b.tableHintInfo = append(b.tableHintInfo, tableHintInfo{
			sortMergeJoinTables:       sortMergeTables,
			indexNestedLoopJoinTables: INLJTables,
			hashJoinTables:            hashJoinTables,
		})
		return true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 651)
	return false
}

func (b *PlanBuilder) popTableHints() {
	trace_util_0.Count(_logical_plan_builder_00000, 658)
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	b.appendUnmatchedJoinHintWarning(TiDBIndexNestedLoopJoin, hintInfo.indexNestedLoopJoinTables)
	b.appendUnmatchedJoinHintWarning(TiDBMergeJoin, hintInfo.sortMergeJoinTables)
	b.appendUnmatchedJoinHintWarning(TiDBHashJoin, hintInfo.hashJoinTables)
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

func (b *PlanBuilder) appendUnmatchedJoinHintWarning(joinType string, hintTables []hintTableInfo) {
	trace_util_0.Count(_logical_plan_builder_00000, 659)
	unMatchedTables := extractUnmatchedTables(hintTables)
	if len(unMatchedTables) == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 661)
		return
	}
	trace_util_0.Count(_logical_plan_builder_00000, 660)
	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s. Maybe you can use the table alias name",
		strings.Join(unMatchedTables, ", "), restore2JoinHint(joinType, hintTables))
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

// TableHints returns the *tableHintInfo of PlanBuilder.
func (b *PlanBuilder) TableHints() *tableHintInfo {
	trace_util_0.Count(_logical_plan_builder_00000, 662)
	if len(b.tableHintInfo) == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 664)
		return nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 663)
	return &(b.tableHintInfo[len(b.tableHintInfo)-1])
}

func (b *PlanBuilder) buildSelect(sel *ast.SelectStmt) (p LogicalPlan, err error) {
	trace_util_0.Count(_logical_plan_builder_00000, 665)
	if b.pushTableHints(sel.TableHints) {
		trace_util_0.Count(_logical_plan_builder_00000, 685)
		// table hints are only visible in the current SELECT statement.
		defer b.popTableHints()
	}
	trace_util_0.Count(_logical_plan_builder_00000, 666)
	if sel.SelectStmtOpts != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 686)
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { trace_util_0.Count(_logical_plan_builder_00000, 687); b.inStraightJoin = origin }()
	}

	trace_util_0.Count(_logical_plan_builder_00000, 667)
	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		windowAggMap                  map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
	)

	if sel.From != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 688)
		p, err = b.buildResultSetNode(sel.From.TableRefs)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 689)
			return nil, err
		}
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 690)
		{
			p = b.buildTableDual()
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 668)
	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 691)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 669)
	if sel.GroupBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 692)
		p, gbyCols, err = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 693)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 670)
	if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() && sel.From != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 694)
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 695)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 671)
	hasWindowFuncField := b.detectSelectWindow(sel)
	if hasWindowFuncField {
		trace_util_0.Count(_logical_plan_builder_00000, 696)
		windowAggMap, err = b.resolveWindowFunction(sel, p)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 697)
			return nil, err
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	trace_util_0.Count(_logical_plan_builder_00000, 672)
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 698)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 673)
	if sel.Where != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 699)
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 700)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 674)
	if sel.LockTp != ast.SelectLockNone {
		trace_util_0.Count(_logical_plan_builder_00000, 701)
		p = b.buildSelectLock(p, sel.LockTp)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 675)
	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		trace_util_0.Count(_logical_plan_builder_00000, 702)
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(p, aggFuncs, gbyCols)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 704)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 703)
		for k, v := range totalMap {
			trace_util_0.Count(_logical_plan_builder_00000, 705)
			totalMap[k] = aggIndexMap[v]
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 676)
	var oldLen int
	// According to https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html,
	// we can only process window functions after having clause, so `considerWindow` is false now.
	p, oldLen, err = b.buildProjection(p, sel.Fields.Fields, totalMap, nil, false)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 706)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 677)
	if sel.Having != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 707)
		b.curClause = havingClause
		p, err = b.buildSelection(p, sel.Having.Expr, havingMap)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 708)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 678)
	b.windowSpecs, err = buildWindowSpecs(sel.WindowSpecs)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 709)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 679)
	var windowMapper map[*ast.WindowFuncExpr]int
	if hasWindowFuncField {
		trace_util_0.Count(_logical_plan_builder_00000, 710)
		windowFuncs := extractWindowFuncs(sel.Fields.Fields)
		groupedFuncs, err := b.groupWindowFuncs(windowFuncs)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 713)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 711)
		p, windowMapper, err = b.buildWindowFunctions(p, groupedFuncs, windowAggMap)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 714)
			return nil, err
		}
		// Now we build the window function fields.
		trace_util_0.Count(_logical_plan_builder_00000, 712)
		p, oldLen, err = b.buildProjection(p, sel.Fields.Fields, windowAggMap, windowMapper, true)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 715)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 680)
	if sel.Distinct {
		trace_util_0.Count(_logical_plan_builder_00000, 716)
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 717)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 681)
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 718)
		p, err = b.buildSort(p, sel.OrderBy.Items, orderMap, windowMapper)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 719)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 682)
	if sel.Limit != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 720)
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 721)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 683)
	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		trace_util_0.Count(_logical_plan_builder_00000, 722)
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx)
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 724)
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		trace_util_0.Count(_logical_plan_builder_00000, 723)
		proj.SetSchema(schema)
		return proj, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 684)
	return p, nil
}

func (b *PlanBuilder) buildTableDual() *LogicalTableDual {
	trace_util_0.Count(_logical_plan_builder_00000, 725)
	return LogicalTableDual{RowCount: 1}.Init(b.ctx)
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	trace_util_0.Count(_logical_plan_builder_00000, 726)
	return &expression.Column{
		DBName:   ds.DBName,
		TblName:  ds.tableInfo.Name,
		ColName:  model.ExtraHandleName,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
	}
}

// getStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
func getStatsTable(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	trace_util_0.Count(_logical_plan_builder_00000, 727)
	statsHandle := domain.GetDomain(ctx).StatsHandle()

	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 732)
		return statistics.PseudoTable(tblInfo)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 728)
	var statsTbl *statistics.Table
	if pid != tblInfo.ID {
		trace_util_0.Count(_logical_plan_builder_00000, 733)
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 734)
		{
			statsTbl = statsHandle.GetTableStats(tblInfo)
		}
	}

	// 2. table row count from statistics is zero.
	trace_util_0.Count(_logical_plan_builder_00000, 729)
	if statsTbl.Count == 0 {
		trace_util_0.Count(_logical_plan_builder_00000, 735)
		return statistics.PseudoTable(tblInfo)
	}

	// 3. statistics is outdated.
	trace_util_0.Count(_logical_plan_builder_00000, 730)
	if statsTbl.IsOutdated() {
		trace_util_0.Count(_logical_plan_builder_00000, 736)
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		metrics.PseudoEstimation.Inc()
	}
	trace_util_0.Count(_logical_plan_builder_00000, 731)
	return statsTbl
}

func (b *PlanBuilder) buildDataSource(tn *ast.TableName) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 737)
	dbName := tn.Schema
	if dbName.L == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 753)
		dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 738)
	tbl, err := b.is.TableByName(dbName, tn.Name)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 754)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 739)
	tableInfo := tbl.Meta()
	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 755)
		authErr = ErrTableaccessDenied.GenWithStackByArgs("SELECT", b.ctx.GetSessionVars().User.Username, b.ctx.GetSessionVars().User.Hostname, tableInfo.Name.L)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 740)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName.L, tableInfo.Name.L, "", authErr)

	if tableInfo.IsView() {
		trace_util_0.Count(_logical_plan_builder_00000, 756)
		return b.BuildDataSourceFromView(dbName, tableInfo)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 741)
	if tableInfo.GetPartitionInfo() != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 757)
		b.optFlag = b.optFlag | flagPartitionProcessor
		// check partition by name.
		for _, name := range tn.PartitionNames {
			trace_util_0.Count(_logical_plan_builder_00000, 758)
			_, err = tables.FindPartitionByName(tableInfo, name.L)
			if err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 759)
				return nil, err
			}
		}
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 760)
		if len(tn.PartitionNames) != 0 {
			trace_util_0.Count(_logical_plan_builder_00000, 761)
			return nil, ErrPartitionClauseOnNonpartitioned
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 742)
	possiblePaths, err := getPossibleAccessPaths(tn.IndexHints, tableInfo)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 762)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 743)
	var columns []*table.Column
	if b.inUpdateStmt {
		trace_util_0.Count(_logical_plan_builder_00000, 763)
		// create table t(a int, b int).
		// Imagine that, There are 2 TiDB instances in the cluster, name A, B. We add a column `c` to table t in the TiDB cluster.
		// One of the TiDB, A, the column type in its infoschema is changed to public. And in the other TiDB, the column type is
		// still StateWriteReorganization.
		// TiDB A: insert into t values(1, 2, 3);
		// TiDB B: update t set a = 2 where b = 2;
		// If we use tbl.Cols() here, the update statement, will ignore the col `c`, and the data `3` will lost.
		columns = tbl.WritableCols()
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 764)
		{
			columns = tbl.Cols()
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 744)
	var statisticTable *statistics.Table
	if _, ok := tbl.(table.PartitionedTable); !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 765)
		statisticTable = getStatsTable(b.ctx, tbl.Meta(), tbl.Meta().ID)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 745)
	ds := DataSource{
		DBName:              dbName,
		table:               tbl,
		tableInfo:           tableInfo,
		statisticTable:      statisticTable,
		indexHints:          tn.IndexHints,
		possibleAccessPaths: possiblePaths,
		Columns:             make([]*model.ColumnInfo, 0, len(columns)),
		partitionNames:      tn.PartitionNames,
	}.Init(b.ctx)

	var handleCol *expression.Column
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for _, col := range columns {
		trace_util_0.Count(_logical_plan_builder_00000, 766)
		ds.Columns = append(ds.Columns, col.ToInfo())
		newCol := &expression.Column{
			UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigColName: col.Name,
			ID:          col.ID,
			RetType:     &col.FieldType,
		}

		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_logical_plan_builder_00000, 768)
			handleCol = newCol
		}
		trace_util_0.Count(_logical_plan_builder_00000, 767)
		schema.Append(newCol)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 746)
	ds.SetSchema(schema)

	// We append an extra handle column to the schema when "ds" is not a memory
	// table e.g. table in the "INFORMATION_SCHEMA" database, and the handle
	// column is not the primary key of "ds".
	isMemDB := infoschema.IsMemoryDB(ds.DBName.L)
	if !isMemDB && handleCol == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 769)
		ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
		handleCol = ds.newExtraHandleSchemaCol()
		schema.Append(handleCol)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 747)
	if handleCol != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 770)
		schema.TblID2Handle[tableInfo.ID] = []*expression.Column{handleCol}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 748)
	var result LogicalPlan = ds

	// If this SQL is executed in a non-readonly transaction, we need a
	// "UnionScan" operator to read the modifications of former SQLs, which is
	// buffered in tidb-server memory.
	txn, err := b.ctx.Txn(false)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 771)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 749)
	if txn.Valid() && !txn.IsReadOnly() {
		trace_util_0.Count(_logical_plan_builder_00000, 772)
		us := LogicalUnionScan{}.Init(b.ctx)
		us.SetChildren(ds)
		result = us
	}

	// If this table contains any virtual generated columns, we need a
	// "Projection" to calculate these columns.
	trace_util_0.Count(_logical_plan_builder_00000, 750)
	proj, err := b.projectVirtualColumns(ds, columns)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 773)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 751)
	if proj != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 774)
		proj.SetChildren(result)
		result = proj
	}
	trace_util_0.Count(_logical_plan_builder_00000, 752)
	return result, nil
}

// BuildDataSourceFromView is used to build LogicalPlan from view
func (b *PlanBuilder) BuildDataSourceFromView(dbName model.CIStr, tableInfo *model.TableInfo) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 775)
	charset, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	viewParser := parser.New()
	viewParser.EnableWindowFunc(b.ctx.GetSessionVars().EnableWindowFunction)
	selectNode, err := viewParser.ParseOneStmt(tableInfo.View.SelectStmt, charset, collation)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 781)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 776)
	originalVisitInfo := b.visitInfo
	b.visitInfo = make([]visitInfo, 0)
	selectLogicalPlan, err := b.Build(selectNode)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 782)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 777)
	if tableInfo.View.Security == model.SecurityDefiner {
		trace_util_0.Count(_logical_plan_builder_00000, 783)
		if pm := privilege.GetPrivilegeManager(b.ctx); pm != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 785)
			for _, v := range b.visitInfo {
				trace_util_0.Count(_logical_plan_builder_00000, 786)
				if !pm.RequestVerificationWithUser(v.db, v.table, v.column, v.privilege, tableInfo.View.Definer) {
					trace_util_0.Count(_logical_plan_builder_00000, 787)
					return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
				}
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 784)
		b.visitInfo = b.visitInfo[:0]
	}
	trace_util_0.Count(_logical_plan_builder_00000, 778)
	b.visitInfo = append(originalVisitInfo, b.visitInfo...)

	if b.ctx.GetSessionVars().StmtCtx.InExplainStmt {
		trace_util_0.Count(_logical_plan_builder_00000, 788)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, dbName.L, tableInfo.Name.L, "", ErrViewNoExplain)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 779)
	projSchema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.View.Cols))...)
	projExprs := make([]expression.Expression, 0, len(tableInfo.View.Cols))
	for i := range tableInfo.View.Cols {
		trace_util_0.Count(_logical_plan_builder_00000, 789)
		col := selectLogicalPlan.Schema().FindColumnByName(tableInfo.View.Cols[i].L)
		if col == nil {
			trace_util_0.Count(_logical_plan_builder_00000, 791)
			return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 790)
		projSchema.Append(&expression.Column{
			UniqueID:    b.ctx.GetSessionVars().AllocPlanColumnID(),
			TblName:     col.TblName,
			OrigTblName: col.OrigTblName,
			ColName:     tableInfo.Cols()[i].Name,
			OrigColName: tableInfo.View.Cols[i],
			DBName:      col.DBName,
			RetType:     col.GetType(),
		})
		projExprs = append(projExprs, col)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 780)
	projUponView := LogicalProjection{Exprs: projExprs}.Init(b.ctx)
	projUponView.SetChildren(selectLogicalPlan.(LogicalPlan))
	projUponView.SetSchema(projSchema)
	return projUponView, nil
}

// projectVirtualColumns is only for DataSource. If some table has virtual generated columns,
// we add a projection on the original DataSource, and calculate those columns in the projection
// so that plans above it can reference generated columns by their name.
func (b *PlanBuilder) projectVirtualColumns(ds *DataSource, columns []*table.Column) (*LogicalProjection, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 792)
	var hasVirtualGeneratedColumn = false
	for _, column := range columns {
		trace_util_0.Count(_logical_plan_builder_00000, 797)
		if column.IsGenerated() && !column.GeneratedStored {
			trace_util_0.Count(_logical_plan_builder_00000, 798)
			hasVirtualGeneratedColumn = true
			break
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 793)
	if !hasVirtualGeneratedColumn {
		trace_util_0.Count(_logical_plan_builder_00000, 799)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 794)
	var proj = LogicalProjection{
		Exprs:            make([]expression.Expression, 0, len(columns)),
		calculateGenCols: true,
	}.Init(b.ctx)

	for i, colExpr := range ds.Schema().Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 800)
		var exprIsGen = false
		var expr expression.Expression
		if i < len(columns) {
			trace_util_0.Count(_logical_plan_builder_00000, 803)
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				trace_util_0.Count(_logical_plan_builder_00000, 804)
				var err error
				expr, _, err = b.rewrite(columns[i].GeneratedExpr, ds, nil, true)
				if err != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 806)
					return nil, err
				}
				// Because the expression might return different type from
				// the generated column, we should wrap a CAST on the result.
				trace_util_0.Count(_logical_plan_builder_00000, 805)
				expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
				exprIsGen = true
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 801)
		if !exprIsGen {
			trace_util_0.Count(_logical_plan_builder_00000, 807)
			expr = colExpr
		}
		trace_util_0.Count(_logical_plan_builder_00000, 802)
		proj.Exprs = append(proj.Exprs, expr)
	}

	// Re-iterate expressions to handle those virtual generated columns that refers to the other generated columns, for
	// example, given:
	//  column a, column b as (a * 2), column c as (b + 1)
	// we'll get:
	//  column a, column b as (a * 2), column c as ((a * 2) + 1)
	// A generated column definition can refer to only generated columns occurring earlier in the table definition, so
	// it's safe to iterate in index-ascending order.
	trace_util_0.Count(_logical_plan_builder_00000, 795)
	for i, expr := range proj.Exprs {
		trace_util_0.Count(_logical_plan_builder_00000, 808)
		proj.Exprs[i] = expression.ColumnSubstitute(expr, ds.Schema(), proj.Exprs)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 796)
	proj.SetSchema(ds.Schema().Clone())
	return proj, nil
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildApplyWithJoinType(outerPlan, innerPlan LogicalPlan, tp JoinType) LogicalPlan {
	trace_util_0.Count(_logical_plan_builder_00000, 809)
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}}.Init(b.ctx)
	ap.SetChildren(outerPlan, innerPlan)
	ap.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
	for i := outerPlan.Schema().Len(); i < ap.Schema().Len(); i++ {
		trace_util_0.Count(_logical_plan_builder_00000, 811)
		ap.schema.Columns[i].IsReferenced = true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 810)
	return ap
}

// buildSemiApply builds apply plan with outerPlan and innerPlan, which apply semi-join for every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildSemiApply(outerPlan, innerPlan LogicalPlan, condition []expression.Expression, asScalar, not bool) (LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 812)
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagDecorrelate

	join, err := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 814)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 813)
	ap := &LogicalApply{LogicalJoin: *join}
	ap.tp = TypeApply
	ap.self = ap
	return ap, nil
}

func (b *PlanBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	trace_util_0.Count(_logical_plan_builder_00000, 815)
	maxOneRow := LogicalMaxOneRow{}.Init(b.ctx)
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *PlanBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) (*LogicalJoin, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 816)
	joinPlan := LogicalJoin{}.Init(b.ctx)
	for i, expr := range onCondition {
		trace_util_0.Count(_logical_plan_builder_00000, 820)
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	trace_util_0.Count(_logical_plan_builder_00000, 817)
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.attachOnConds(onCondition)
	if asScalar {
		trace_util_0.Count(_logical_plan_builder_00000, 821)
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			ColName:      model.NewCIStr(fmt.Sprintf("%d_aux_0", joinPlan.id)),
			RetType:      types.NewFieldType(mysql.TypeTiny),
			IsReferenced: true,
			UniqueID:     b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
		joinPlan.SetSchema(newSchema)
		if not {
			trace_util_0.Count(_logical_plan_builder_00000, 822)
			joinPlan.JoinType = AntiLeftOuterSemiJoin
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 823)
			{
				joinPlan.JoinType = LeftOuterSemiJoin
			}
		}
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 824)
		{
			joinPlan.SetSchema(outerPlan.Schema().Clone())
			if not {
				trace_util_0.Count(_logical_plan_builder_00000, 825)
				joinPlan.JoinType = AntiSemiJoin
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 826)
				{
					joinPlan.JoinType = SemiJoin
				}
			}
		}
	}
	// Apply forces to choose hash join currently, so don't worry the hints will take effect if the semi join is in one apply.
	trace_util_0.Count(_logical_plan_builder_00000, 818)
	if b.TableHints() != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 827)
		outerAlias := extractTableAlias(outerPlan)
		innerAlias := extractTableAlias(innerPlan)
		if b.TableHints().ifPreferMergeJoin(outerAlias, innerAlias) {
			trace_util_0.Count(_logical_plan_builder_00000, 831)
			joinPlan.preferJoinType |= preferMergeJoin
		}
		trace_util_0.Count(_logical_plan_builder_00000, 828)
		if b.TableHints().ifPreferHashJoin(outerAlias, innerAlias) {
			trace_util_0.Count(_logical_plan_builder_00000, 832)
			joinPlan.preferJoinType |= preferHashJoin
		}
		trace_util_0.Count(_logical_plan_builder_00000, 829)
		if b.TableHints().ifPreferINLJ(innerAlias) {
			trace_util_0.Count(_logical_plan_builder_00000, 833)
			joinPlan.preferJoinType = preferLeftAsIndexInner
		}
		// If there're multiple join hints, they're conflict.
		trace_util_0.Count(_logical_plan_builder_00000, 830)
		if bits.OnesCount(joinPlan.preferJoinType) > 1 {
			trace_util_0.Count(_logical_plan_builder_00000, 834)
			return nil, errors.New("Join hints are conflict, you can only specify one type of join")
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 819)
	return joinPlan, nil
}

func (b *PlanBuilder) buildUpdate(update *ast.UpdateStmt) (Plan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 835)
	if b.pushTableHints(update.TableHints) {
		trace_util_0.Count(_logical_plan_builder_00000, 845)
		// table hints are only visible in the current UPDATE statement.
		defer b.popTableHints()
	}

	// update subquery table should be forbidden
	trace_util_0.Count(_logical_plan_builder_00000, 836)
	var asNameList []string
	asNameList = extractTableSourceAsNames(update.TableRefs.TableRefs, asNameList, true)
	for _, asName := range asNameList {
		trace_util_0.Count(_logical_plan_builder_00000, 846)
		for _, assign := range update.List {
			trace_util_0.Count(_logical_plan_builder_00000, 847)
			if assign.Column.Table.L == asName {
				trace_util_0.Count(_logical_plan_builder_00000, 848)
				return nil, ErrNonUpdatableTable.GenWithStackByArgs(asName, "UPDATE")
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 837)
	b.inUpdateStmt = true
	sel := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    update.TableRefs,
		Where:   update.Where,
		OrderBy: update.Order,
		Limit:   update.Limit,
	}

	p, err := b.buildResultSetNode(sel.From.TableRefs)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 849)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 838)
	var tableList []*ast.TableName
	tableList = extractTableList(sel.From.TableRefs, tableList, false)
	for _, t := range tableList {
		trace_util_0.Count(_logical_plan_builder_00000, 850)
		dbName := t.Schema.L
		if dbName == "" {
			trace_util_0.Count(_logical_plan_builder_00000, 853)
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		trace_util_0.Count(_logical_plan_builder_00000, 851)
		if t.TableInfo.IsView() {
			trace_util_0.Count(_logical_plan_builder_00000, 854)
			return nil, errors.Errorf("update view %s is not supported now.", t.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 852)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, t.Name.L, "", nil)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 839)
	if sel.Where != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 855)
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 856)
			return nil, err
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 840)
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 857)
		p, err = b.buildSort(p, sel.OrderBy.Items, nil, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 858)
			return nil, err
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 841)
	if sel.Limit != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 859)
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 860)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 842)
	var updateTableList []*ast.TableName
	updateTableList = extractTableList(sel.From.TableRefs, updateTableList, true)
	orderedList, np, err := b.buildUpdateLists(updateTableList, update.List, p)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 861)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 843)
	p = np

	updt := Update{OrderedList: orderedList}.Init(b.ctx)
	updt.SetSchema(p.Schema())
	updt.SelectPlan, err = DoOptimize(b.optFlag, p)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 862)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 844)
	err = updt.ResolveIndices()
	return updt, err
}

func (b *PlanBuilder) buildUpdateLists(tableList []*ast.TableName, list []*ast.Assignment, p LogicalPlan) ([]*expression.Assignment, LogicalPlan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 863)
	b.curClause = fieldList
	modifyColumns := make(map[string]struct{}, p.Schema().Len()) // Which columns are in set list.
	for _, assign := range list {
		trace_util_0.Count(_logical_plan_builder_00000, 869)
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 871)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 870)
		columnFullName := fmt.Sprintf("%s.%s.%s", col.DBName.L, col.TblName.L, col.ColName.L)
		modifyColumns[columnFullName] = struct{}{}
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	trace_util_0.Count(_logical_plan_builder_00000, 864)
	virtualAssignments := make([]*ast.Assignment, 0)
	tableAsName := make(map[*model.TableInfo][]*model.CIStr)
	extractTableAsNameForUpdate(p, tableAsName)

	for _, tn := range tableList {
		trace_util_0.Count(_logical_plan_builder_00000, 872)
		tableInfo := tn.TableInfo
		tableVal, found := b.is.TableByID(tableInfo.ID)
		if !found {
			trace_util_0.Count(_logical_plan_builder_00000, 874)
			return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tn.DBInfo.Name.O, tableInfo.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 873)
		for i, colInfo := range tableInfo.Columns {
			trace_util_0.Count(_logical_plan_builder_00000, 875)
			if !colInfo.IsGenerated() {
				trace_util_0.Count(_logical_plan_builder_00000, 878)
				continue
			}
			trace_util_0.Count(_logical_plan_builder_00000, 876)
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.Schema.L, tn.Name.L, colInfo.Name.L)
			if _, ok := modifyColumns[columnFullName]; ok {
				trace_util_0.Count(_logical_plan_builder_00000, 879)
				return nil, nil, ErrBadGeneratedColumn.GenWithStackByArgs(colInfo.Name.O, tableInfo.Name.O)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 877)
			for _, asName := range tableAsName[tableInfo] {
				trace_util_0.Count(_logical_plan_builder_00000, 880)
				virtualAssignments = append(virtualAssignments, &ast.Assignment{
					Column: &ast.ColumnName{Table: *asName, Name: colInfo.Name},
					Expr:   tableVal.Cols()[i].GeneratedExpr,
				})
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 865)
	newList := make([]*expression.Assignment, 0, p.Schema().Len())
	allAssignments := append(list, virtualAssignments...)
	for i, assign := range allAssignments {
		trace_util_0.Count(_logical_plan_builder_00000, 881)
		col, _, err := p.findColumn(assign.Column)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 885)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 882)
		var newExpr expression.Expression
		var np LogicalPlan
		if i < len(list) {
			trace_util_0.Count(_logical_plan_builder_00000, 886)
			newExpr, np, err = b.rewrite(assign.Expr, p, nil, false)
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 887)
			{
				// rewrite with generation expression
				rewritePreprocess := func(expr ast.Node) ast.Node {
					trace_util_0.Count(_logical_plan_builder_00000, 889)
					switch x := expr.(type) {
					case *ast.ColumnName:
						trace_util_0.Count(_logical_plan_builder_00000, 890)
						return &ast.ColumnName{
							Schema: assign.Column.Schema,
							Table:  assign.Column.Table,
							Name:   x.Name,
						}
					default:
						trace_util_0.Count(_logical_plan_builder_00000, 891)
						return expr
					}
				}
				trace_util_0.Count(_logical_plan_builder_00000, 888)
				newExpr, np, err = b.rewriteWithPreprocess(assign.Expr, p, nil, nil, false, rewritePreprocess)
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 883)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 892)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 884)
		newExpr = expression.BuildCastFunction(b.ctx, newExpr, col.GetType())
		p = np
		newList = append(newList, &expression.Assignment{Col: col, Expr: newExpr})
	}

	trace_util_0.Count(_logical_plan_builder_00000, 866)
	tblDbMap := make(map[string]string, len(tableList))
	for _, tbl := range tableList {
		trace_util_0.Count(_logical_plan_builder_00000, 893)
		tblDbMap[tbl.Name.L] = tbl.DBInfo.Name.L
	}
	trace_util_0.Count(_logical_plan_builder_00000, 867)
	for _, assign := range newList {
		trace_util_0.Count(_logical_plan_builder_00000, 894)
		col := assign.Col

		dbName := col.DBName.L
		// To solve issue#10028, we need to get database name by the table alias name.
		if dbNameTmp, ok := tblDbMap[col.TblName.L]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 897)
			dbName = dbNameTmp
		}
		trace_util_0.Count(_logical_plan_builder_00000, 895)
		if dbName == "" {
			trace_util_0.Count(_logical_plan_builder_00000, 898)
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		trace_util_0.Count(_logical_plan_builder_00000, 896)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, col.OrigTblName.L, "", nil)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 868)
	return newList, p, nil
}

// extractTableAsNameForUpdate extracts tables' alias names for update.
func extractTableAsNameForUpdate(p LogicalPlan, asNames map[*model.TableInfo][]*model.CIStr) {
	trace_util_0.Count(_logical_plan_builder_00000, 899)
	switch x := p.(type) {
	case *DataSource:
		trace_util_0.Count(_logical_plan_builder_00000, 900)
		alias := extractTableAlias(p)
		if alias != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 906)
			if _, ok := asNames[x.tableInfo]; !ok {
				trace_util_0.Count(_logical_plan_builder_00000, 908)
				asNames[x.tableInfo] = make([]*model.CIStr, 0, 1)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 907)
			asNames[x.tableInfo] = append(asNames[x.tableInfo], alias)
		}
	case *LogicalProjection:
		trace_util_0.Count(_logical_plan_builder_00000, 901)
		if !x.calculateGenCols {
			trace_util_0.Count(_logical_plan_builder_00000, 909)
			return
		}

		trace_util_0.Count(_logical_plan_builder_00000, 902)
		ds, isDS := x.Children()[0].(*DataSource)
		if !isDS {
			trace_util_0.Count(_logical_plan_builder_00000, 910)
			// try to extract the DataSource below a LogicalUnionScan.
			if us, isUS := x.Children()[0].(*LogicalUnionScan); isUS {
				trace_util_0.Count(_logical_plan_builder_00000, 911)
				ds, isDS = us.Children()[0].(*DataSource)
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 903)
		if !isDS {
			trace_util_0.Count(_logical_plan_builder_00000, 912)
			return
		}

		trace_util_0.Count(_logical_plan_builder_00000, 904)
		alias := extractTableAlias(x)
		if alias != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 913)
			if _, ok := asNames[ds.tableInfo]; !ok {
				trace_util_0.Count(_logical_plan_builder_00000, 915)
				asNames[ds.tableInfo] = make([]*model.CIStr, 0, 1)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 914)
			asNames[ds.tableInfo] = append(asNames[ds.tableInfo], alias)
		}
	default:
		trace_util_0.Count(_logical_plan_builder_00000, 905)
		for _, child := range p.Children() {
			trace_util_0.Count(_logical_plan_builder_00000, 916)
			extractTableAsNameForUpdate(child, asNames)
		}
	}
}

func (b *PlanBuilder) buildDelete(delete *ast.DeleteStmt) (Plan, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 917)
	if b.pushTableHints(delete.TableHints) {
		trace_util_0.Count(_logical_plan_builder_00000, 927)
		// table hints are only visible in the current DELETE statement.
		defer b.popTableHints()
	}

	trace_util_0.Count(_logical_plan_builder_00000, 918)
	sel := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delete.TableRefs,
		Where:   delete.Where,
		OrderBy: delete.Order,
		Limit:   delete.Limit,
	}
	p, err := b.buildResultSetNode(sel.From.TableRefs)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 928)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 919)
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	if sel.Where != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 929)
		p, err = b.buildSelection(p, sel.Where, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 930)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 920)
	if sel.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 931)
		p, err = b.buildSort(p, sel.OrderBy.Items, nil, nil)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 932)
			return nil, err
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 921)
	if sel.Limit != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 933)
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 934)
			return nil, err
		}
	}

	// Add a projection for the following case, otherwise the final schema will be the schema of the join.
	// delete from t where a in (select ...) or b in (select ...)
	trace_util_0.Count(_logical_plan_builder_00000, 922)
	if !delete.IsMultiTable && oldLen != p.Schema().Len() {
		trace_util_0.Count(_logical_plan_builder_00000, 935)
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx)
		proj.SetChildren(p)
		proj.SetSchema(oldSchema.Clone())
		p = proj
	}

	trace_util_0.Count(_logical_plan_builder_00000, 923)
	var tables []*ast.TableName
	if delete.Tables != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 936)
		tables = delete.Tables.Tables
	}

	trace_util_0.Count(_logical_plan_builder_00000, 924)
	del := Delete{
		Tables:       tables,
		IsMultiTable: delete.IsMultiTable,
	}.Init(b.ctx)

	del.SelectPlan, err = DoOptimize(b.optFlag, p)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 937)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 925)
	del.SetSchema(expression.NewSchema())

	var tableList []*ast.TableName
	tableList = extractTableList(delete.TableRefs.TableRefs, tableList, true)

	// Collect visitInfo.
	if delete.Tables != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 938)
		// Delete a, b from a, b, c, d... add a and b.
		for _, tn := range delete.Tables.Tables {
			trace_util_0.Count(_logical_plan_builder_00000, 939)
			foundMatch := false
			for _, v := range tableList {
				trace_util_0.Count(_logical_plan_builder_00000, 943)
				dbName := v.Schema.L
				if dbName == "" {
					trace_util_0.Count(_logical_plan_builder_00000, 945)
					dbName = b.ctx.GetSessionVars().CurrentDB
				}
				trace_util_0.Count(_logical_plan_builder_00000, 944)
				if (tn.Schema.L == "" || tn.Schema.L == dbName) && tn.Name.L == v.Name.L {
					trace_util_0.Count(_logical_plan_builder_00000, 946)
					tn.Schema.L = dbName
					tn.DBInfo = v.DBInfo
					tn.TableInfo = v.TableInfo
					foundMatch = true
					break
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 940)
			if !foundMatch {
				trace_util_0.Count(_logical_plan_builder_00000, 947)
				var asNameList []string
				asNameList = extractTableSourceAsNames(delete.TableRefs.TableRefs, asNameList, false)
				for _, asName := range asNameList {
					trace_util_0.Count(_logical_plan_builder_00000, 949)
					tblName := tn.Name.L
					if tn.Schema.L != "" {
						trace_util_0.Count(_logical_plan_builder_00000, 951)
						tblName = tn.Schema.L + "." + tblName
					}
					trace_util_0.Count(_logical_plan_builder_00000, 950)
					if asName == tblName {
						trace_util_0.Count(_logical_plan_builder_00000, 952)
						// check sql like: `delete a from (select * from t) as a, t`
						return nil, ErrNonUpdatableTable.GenWithStackByArgs(tn.Name.O, "DELETE")
					}
				}
				// check sql like: `delete b from (select * from t) as a, t`
				trace_util_0.Count(_logical_plan_builder_00000, 948)
				return nil, ErrUnknownTable.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
			}
			trace_util_0.Count(_logical_plan_builder_00000, 941)
			if tn.TableInfo.IsView() {
				trace_util_0.Count(_logical_plan_builder_00000, 953)
				return nil, errors.Errorf("delete view %s is not supported now.", tn.Name.O)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 942)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, tn.Schema.L, tn.TableInfo.Name.L, "", nil)
		}
	} else {
		trace_util_0.Count(_logical_plan_builder_00000, 954)
		{
			// Delete from a, b, c, d.
			for _, v := range tableList {
				trace_util_0.Count(_logical_plan_builder_00000, 955)
				if v.TableInfo.IsView() {
					trace_util_0.Count(_logical_plan_builder_00000, 958)
					return nil, errors.Errorf("delete view %s is not supported now.", v.Name.O)
				}
				trace_util_0.Count(_logical_plan_builder_00000, 956)
				dbName := v.Schema.L
				if dbName == "" {
					trace_util_0.Count(_logical_plan_builder_00000, 959)
					dbName = b.ctx.GetSessionVars().CurrentDB
				}
				trace_util_0.Count(_logical_plan_builder_00000, 957)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, dbName, v.Name.L, "", nil)
			}
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 926)
	return del, nil
}

func getWindowName(name string) string {
	trace_util_0.Count(_logical_plan_builder_00000, 960)
	if name == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 962)
		return "<unnamed window>"
	}
	trace_util_0.Count(_logical_plan_builder_00000, 961)
	return name
}

// buildProjectionForWindow builds the projection for expressions in the window specification that is not an column,
// so after the projection, window functions only needs to deal with columns.
func (b *PlanBuilder) buildProjectionForWindow(p LogicalPlan, spec *ast.WindowSpec, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, []property.Item, []property.Item, []expression.Expression, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 963)
	b.optFlag |= flagEliminateProjection

	var partitionItems, orderItems []*ast.ByItem
	if spec.PartitionBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 970)
		partitionItems = spec.PartitionBy.Items
	}
	trace_util_0.Count(_logical_plan_builder_00000, 964)
	if spec.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 971)
		orderItems = spec.OrderBy.Items
	}

	trace_util_0.Count(_logical_plan_builder_00000, 965)
	projLen := len(p.Schema().Columns) + len(partitionItems) + len(orderItems) + len(args)
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, projLen)}.Init(b.ctx)
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, 0, projLen)...))
	for _, col := range p.Schema().Columns {
		trace_util_0.Count(_logical_plan_builder_00000, 972)
		proj.Exprs = append(proj.Exprs, col)
		proj.schema.Append(col)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 966)
	propertyItems := make([]property.Item, 0, len(partitionItems)+len(orderItems))
	var err error
	p, propertyItems, err = b.buildByItemsForWindow(p, proj, partitionItems, propertyItems, aggMap)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 973)
		return nil, nil, nil, nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 967)
	lenPartition := len(propertyItems)
	p, propertyItems, err = b.buildByItemsForWindow(p, proj, orderItems, propertyItems, aggMap)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 974)
		return nil, nil, nil, nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 968)
	newArgList := make([]expression.Expression, 0, len(args))
	for _, arg := range args {
		trace_util_0.Count(_logical_plan_builder_00000, 975)
		newArg, np, err := b.rewrite(arg, p, aggMap, true)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 978)
			return nil, nil, nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 976)
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			trace_util_0.Count(_logical_plan_builder_00000, 979)
			newArgList = append(newArgList, newArg)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 977)
		proj.Exprs = append(proj.Exprs, newArg)
		col := &expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("%d_proj_window_%d", p.ID(), proj.schema.Len())),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(),
		}
		proj.schema.Append(col)
		newArgList = append(newArgList, col)
	}

	trace_util_0.Count(_logical_plan_builder_00000, 969)
	proj.SetChildren(p)
	return proj, propertyItems[:lenPartition], propertyItems[lenPartition:], newArgList, nil
}

func (b *PlanBuilder) buildByItemsForWindow(
	p LogicalPlan,
	proj *LogicalProjection,
	items []*ast.ByItem,
	retItems []property.Item,
	aggMap map[*ast.AggregateFuncExpr]int,
) (LogicalPlan, []property.Item, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 980)
	transformer := &itemTransformer{}
	for _, item := range items {
		trace_util_0.Count(_logical_plan_builder_00000, 982)
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(item.Expr, p, aggMap, true)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 986)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 983)
		p = np
		if it.GetType().Tp == mysql.TypeNull {
			trace_util_0.Count(_logical_plan_builder_00000, 987)
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 984)
		if col, ok := it.(*expression.Column); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 988)
			retItems = append(retItems, property.Item{Col: col, Desc: item.Desc})
			continue
		}
		trace_util_0.Count(_logical_plan_builder_00000, 985)
		proj.Exprs = append(proj.Exprs, it)
		col := &expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("%d_proj_window_%d", p.ID(), proj.schema.Len())),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  it.GetType(),
		}
		proj.schema.Append(col)
		retItems = append(retItems, property.Item{Col: col, Desc: item.Desc})
	}
	trace_util_0.Count(_logical_plan_builder_00000, 981)
	return p, retItems, nil
}

// buildWindowFunctionFrameBound builds the bounds of window function frames.
// For type `Rows`, the bound expr must be an unsigned integer.
// For type `Range`, the bound expr must be temporal or numeric types.
func (b *PlanBuilder) buildWindowFunctionFrameBound(spec *ast.WindowSpec, orderByItems []property.Item, boundClause *ast.FrameBound) (*FrameBound, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 989)
	frameType := spec.Frame.Type
	bound := &FrameBound{Type: boundClause.Type, UnBounded: boundClause.UnBounded}
	if bound.UnBounded {
		trace_util_0.Count(_logical_plan_builder_00000, 1002)
		return bound, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 990)
	if frameType == ast.Rows {
		trace_util_0.Count(_logical_plan_builder_00000, 1003)
		if bound.Type == ast.CurrentRow {
			trace_util_0.Count(_logical_plan_builder_00000, 1007)
			return bound, nil
		}
		// Rows type does not support interval range.
		trace_util_0.Count(_logical_plan_builder_00000, 1004)
		if boundClause.Unit != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1008)
			return nil, ErrWindowRowsIntervalUse.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1005)
		numRows, isNull, isExpectedType := getUintFromNode(b.ctx, boundClause.Expr)
		if isNull || !isExpectedType {
			trace_util_0.Count(_logical_plan_builder_00000, 1009)
			return nil, ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1006)
		bound.Num = numRows
		return bound, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 991)
	bound.CalcFuncs = make([]expression.Expression, len(orderByItems))
	bound.CmpFuncs = make([]expression.CompareFunc, len(orderByItems))
	if bound.Type == ast.CurrentRow {
		trace_util_0.Count(_logical_plan_builder_00000, 1010)
		for i, item := range orderByItems {
			trace_util_0.Count(_logical_plan_builder_00000, 1012)
			col := item.Col
			bound.CalcFuncs[i] = col
			bound.CmpFuncs[i] = expression.GetCmpFunction(col, col)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1011)
		return bound, nil
	}

	trace_util_0.Count(_logical_plan_builder_00000, 992)
	if len(orderByItems) != 1 {
		trace_util_0.Count(_logical_plan_builder_00000, 1013)
		return nil, ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	trace_util_0.Count(_logical_plan_builder_00000, 993)
	col := orderByItems[0].Col
	isNumeric, isTemporal := types.IsTypeNumeric(col.RetType.Tp), types.IsTypeTemporal(col.RetType.Tp)
	if !isNumeric && !isTemporal {
		trace_util_0.Count(_logical_plan_builder_00000, 1014)
		return nil, ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	// Interval bounds only support order by temporal types.
	trace_util_0.Count(_logical_plan_builder_00000, 994)
	if boundClause.Unit != nil && isNumeric {
		trace_util_0.Count(_logical_plan_builder_00000, 1015)
		return nil, ErrWindowRangeFrameNumericType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	// Non-interval bound only support order by numeric types.
	trace_util_0.Count(_logical_plan_builder_00000, 995)
	if boundClause.Unit == nil && !isNumeric {
		trace_util_0.Count(_logical_plan_builder_00000, 1016)
		return nil, ErrWindowRangeFrameTemporalType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}

	// TODO: We also need to raise error for non-deterministic expressions, like rand().
	trace_util_0.Count(_logical_plan_builder_00000, 996)
	val, err := evalAstExpr(b.ctx, boundClause.Expr)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1017)
		return nil, ErrWindowRangeBoundNotConstant.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	trace_util_0.Count(_logical_plan_builder_00000, 997)
	expr := expression.Constant{Value: val, RetType: boundClause.Expr.GetType()}

	checker := &paramMarkerInPrepareChecker{}
	boundClause.Expr.Accept(checker)

	// If it has paramMarker and is in prepare stmt. We don't need to eval it since its value is not decided yet.
	if !checker.inPrepareStmt {
		trace_util_0.Count(_logical_plan_builder_00000, 1018)
		// Do not raise warnings for truncate.
		oriIgnoreTruncate := b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate
		b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
		uVal, isNull, err := expr.EvalInt(b.ctx, chunk.Row{})
		b.ctx.GetSessionVars().StmtCtx.IgnoreTruncate = oriIgnoreTruncate
		if uVal < 0 || isNull || err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1019)
			return nil, ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
	}

	trace_util_0.Count(_logical_plan_builder_00000, 998)
	desc := orderByItems[0].Desc
	if boundClause.Unit != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1020)
		// It can be guaranteed by the parser.
		unitVal := boundClause.Unit.(*driver.ValueExpr)
		unit := expression.Constant{Value: unitVal.Datum, RetType: unitVal.GetType()}

		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName := ast.DateAdd
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			trace_util_0.Count(_logical_plan_builder_00000, 1023)
			funcName = ast.DateSub
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1021)
		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr, &unit)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1024)
			return nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1022)
		bound.CmpFuncs[0] = expression.GetCmpFunction(orderByItems[0].Col, bound.CalcFuncs[0])
		return bound, nil
	}
	// When the order is asc:
	//   `+` for following, and `-` for the preceding
	// When the order is desc, `+` becomes `-` and vice-versa.
	trace_util_0.Count(_logical_plan_builder_00000, 999)
	funcName := ast.Plus
	if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
		trace_util_0.Count(_logical_plan_builder_00000, 1025)
		funcName = ast.Minus
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1000)
	bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1026)
		return nil, err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1001)
	bound.CmpFuncs[0] = expression.GetCmpFunction(orderByItems[0].Col, bound.CalcFuncs[0])
	return bound, nil
}

// paramMarkerInPrepareChecker checks whether the given ast tree has paramMarker and is in prepare statement.
type paramMarkerInPrepareChecker struct {
	inPrepareStmt bool
}

// Enter implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 1027)
	switch v := in.(type) {
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_logical_plan_builder_00000, 1029)
		pc.inPrepareStmt = !v.InExecute
		return v, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1028)
	return in, false
}

// Leave implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 1030)
	return in, true
}

// buildWindowFunctionFrame builds the window function frames.
// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
func (b *PlanBuilder) buildWindowFunctionFrame(spec *ast.WindowSpec, orderByItems []property.Item) (*WindowFrame, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 1031)
	frameClause := spec.Frame
	if frameClause == nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1037)
		return nil, nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1032)
	if frameClause.Type == ast.Groups {
		trace_util_0.Count(_logical_plan_builder_00000, 1038)
		return nil, ErrNotSupportedYet.GenWithStackByArgs("GROUPS")
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1033)
	frame := &WindowFrame{Type: frameClause.Type}
	start := frameClause.Extent.Start
	if start.Type == ast.Following && start.UnBounded {
		trace_util_0.Count(_logical_plan_builder_00000, 1039)
		return nil, ErrWindowFrameStartIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1034)
	var err error
	frame.Start, err = b.buildWindowFunctionFrameBound(spec, orderByItems, &start)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1040)
		return nil, err
	}

	trace_util_0.Count(_logical_plan_builder_00000, 1035)
	end := frameClause.Extent.End
	if end.Type == ast.Preceding && end.UnBounded {
		trace_util_0.Count(_logical_plan_builder_00000, 1041)
		return nil, ErrWindowFrameEndIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1036)
	frame.End, err = b.buildWindowFunctionFrameBound(spec, orderByItems, &end)
	return frame, err
}

func (b *PlanBuilder) buildWindowFunctions(p LogicalPlan, groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, map[*ast.WindowFuncExpr]int, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 1042)
	args := make([]ast.ExprNode, 0, 4)
	windowMap := make(map[*ast.WindowFuncExpr]int)
	for spec, funcs := range groupedFuncs {
		trace_util_0.Count(_logical_plan_builder_00000, 1044)
		args = args[:0]
		for _, windowFunc := range funcs {
			trace_util_0.Count(_logical_plan_builder_00000, 1049)
			args = append(args, windowFunc.Args...)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1045)
		np, partitionBy, orderBy, args, err := b.buildProjectionForWindow(p, spec, args, aggMap)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1050)
			return nil, nil, err
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1046)
		frame, err := b.buildWindowFunctionFrame(spec, orderBy)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1051)
			return nil, nil, err
		}

		trace_util_0.Count(_logical_plan_builder_00000, 1047)
		window := LogicalWindow{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Frame:       frame,
		}.Init(b.ctx)
		schema := np.Schema().Clone()
		descs := make([]*aggregation.WindowFuncDesc, 0, len(funcs))
		preArgs := 0
		for _, windowFunc := range funcs {
			trace_util_0.Count(_logical_plan_builder_00000, 1052)
			desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFunc.F, args[preArgs:preArgs+len(windowFunc.Args)])
			if err != nil {
				trace_util_0.Count(_logical_plan_builder_00000, 1055)
				return nil, nil, err
			}
			trace_util_0.Count(_logical_plan_builder_00000, 1053)
			if desc == nil {
				trace_util_0.Count(_logical_plan_builder_00000, 1056)
				return nil, nil, ErrWrongArguments.GenWithStackByArgs(windowFunc.F)
			}
			trace_util_0.Count(_logical_plan_builder_00000, 1054)
			preArgs += len(windowFunc.Args)
			desc.WrapCastForAggArgs(b.ctx)
			descs = append(descs, desc)
			windowMap[windowFunc] = schema.Len()
			schema.Append(&expression.Column{
				ColName:      model.NewCIStr(fmt.Sprintf("%d_window_%d", window.id, schema.Len())),
				UniqueID:     b.ctx.GetSessionVars().AllocPlanColumnID(),
				IsReferenced: true,
				RetType:      desc.RetTp,
			})
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1048)
		window.WindowFuncDescs = descs
		window.SetChildren(np)
		window.SetSchema(schema)
		p = window
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1043)
	return p, windowMap, nil
}

func extractWindowFuncs(fields []*ast.SelectField) []*ast.WindowFuncExpr {
	trace_util_0.Count(_logical_plan_builder_00000, 1057)
	extractor := &WindowFuncExtractor{}
	for _, f := range fields {
		trace_util_0.Count(_logical_plan_builder_00000, 1059)
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1058)
	return extractor.windowFuncs
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	trace_util_0.Count(_logical_plan_builder_00000, 1060)
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MySQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       which is the same as an empty frame.
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1063)
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// For functions that operate on the entire partition, the frame clause will be ignored.
	trace_util_0.Count(_logical_plan_builder_00000, 1061)
	if !needFrame && spec.Frame != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1064)
		specName := spec.Name.O
		b.ctx.GetSessionVars().StmtCtx.AppendNote(ErrWindowFunctionIgnoresFrame.GenWithStackByArgs(windowFuncName, getWindowName(specName)))
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1062)
	return spec, false
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 1065)
	// updatedSpecMap is used to handle the specifications that have frame clause changed.
	updatedSpecMap := make(map[string]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	for _, windowFunc := range windowFuncs {
		trace_util_0.Count(_logical_plan_builder_00000, 1067)
		if windowFunc.Spec.Name.L == "" {
			trace_util_0.Count(_logical_plan_builder_00000, 1070)
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				trace_util_0.Count(_logical_plan_builder_00000, 1072)
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					trace_util_0.Count(_logical_plan_builder_00000, 1074)
					return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				trace_util_0.Count(_logical_plan_builder_00000, 1073)
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					trace_util_0.Count(_logical_plan_builder_00000, 1075)
					return nil, err
				}
			}
			trace_util_0.Count(_logical_plan_builder_00000, 1071)
			spec, _ = b.handleDefaultFrame(spec, windowFunc.F)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			continue
		}

		trace_util_0.Count(_logical_plan_builder_00000, 1068)
		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			trace_util_0.Count(_logical_plan_builder_00000, 1076)
			return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1069)
		newSpec, updated := b.handleDefaultFrame(spec, windowFunc.F)
		if !updated {
			trace_util_0.Count(_logical_plan_builder_00000, 1077)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
		} else {
			trace_util_0.Count(_logical_plan_builder_00000, 1078)
			{
				if _, ok := updatedSpecMap[name]; !ok {
					trace_util_0.Count(_logical_plan_builder_00000, 1080)
					updatedSpecMap[name] = newSpec
				}
				trace_util_0.Count(_logical_plan_builder_00000, 1079)
				updatedSpec := updatedSpecMap[name]
				groupedWindow[updatedSpec] = append(groupedWindow[updatedSpec], windowFunc)
			}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1066)
	return groupedWindow, nil
}

// resolveWindowSpec resolve window specifications for sql like `select ... from t window w1 as (w2), w2 as (partition by a)`.
// We need to resolve the referenced window to get the definition of current window spec.
func resolveWindowSpec(spec *ast.WindowSpec, specs map[string]*ast.WindowSpec, inStack map[string]bool) error {
	trace_util_0.Count(_logical_plan_builder_00000, 1081)
	if inStack[spec.Name.L] {
		trace_util_0.Count(_logical_plan_builder_00000, 1086)
		return errors.Trace(ErrWindowCircularityInWindowGraph)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1082)
	if spec.Ref.L == "" {
		trace_util_0.Count(_logical_plan_builder_00000, 1087)
		return nil
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1083)
	ref, ok := specs[spec.Ref.L]
	if !ok {
		trace_util_0.Count(_logical_plan_builder_00000, 1088)
		return ErrWindowNoSuchWindow.GenWithStackByArgs(spec.Ref.O)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1084)
	inStack[spec.Name.L] = true
	err := resolveWindowSpec(ref, specs, inStack)
	if err != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1089)
		return err
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1085)
	inStack[spec.Name.L] = false
	return mergeWindowSpec(spec, ref)
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	trace_util_0.Count(_logical_plan_builder_00000, 1090)
	if ref.Frame != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1094)
		return ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1091)
	if ref.OrderBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1095)
		if spec.OrderBy != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1097)
			return ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1096)
		spec.OrderBy = ref.OrderBy
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1092)
	if spec.PartitionBy != nil {
		trace_util_0.Count(_logical_plan_builder_00000, 1098)
		return errors.Trace(ErrWindowNoChildPartitioning)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1093)
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = model.NewCIStr("")
	return nil
}

func buildWindowSpecs(specs []ast.WindowSpec) (map[string]*ast.WindowSpec, error) {
	trace_util_0.Count(_logical_plan_builder_00000, 1099)
	specsMap := make(map[string]*ast.WindowSpec, len(specs))
	for _, spec := range specs {
		trace_util_0.Count(_logical_plan_builder_00000, 1102)
		if _, ok := specsMap[spec.Name.L]; ok {
			trace_util_0.Count(_logical_plan_builder_00000, 1104)
			return nil, ErrWindowDuplicateName.GenWithStackByArgs(spec.Name.O)
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1103)
		newSpec := spec
		specsMap[spec.Name.L] = &newSpec
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1100)
	inStack := make(map[string]bool, len(specs))
	for _, spec := range specsMap {
		trace_util_0.Count(_logical_plan_builder_00000, 1105)
		err := resolveWindowSpec(spec, specsMap, inStack)
		if err != nil {
			trace_util_0.Count(_logical_plan_builder_00000, 1106)
			return nil, err
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1101)
	return specsMap, nil
}

// extractTableList extracts all the TableNames from node.
// If asName is true, extract AsName prior to OrigName.
// Privilege check should use OrigName, while expression may use AsName.
func extractTableList(node ast.ResultSetNode, input []*ast.TableName, asName bool) []*ast.TableName {
	trace_util_0.Count(_logical_plan_builder_00000, 1107)
	switch x := node.(type) {
	case *ast.Join:
		trace_util_0.Count(_logical_plan_builder_00000, 1109)
		input = extractTableList(x.Left, input, asName)
		input = extractTableList(x.Right, input, asName)
	case *ast.TableSource:
		trace_util_0.Count(_logical_plan_builder_00000, 1110)
		if s, ok := x.Source.(*ast.TableName); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 1111)
			if x.AsName.L != "" && asName {
				trace_util_0.Count(_logical_plan_builder_00000, 1112)
				newTableName := *s
				newTableName.Name = x.AsName
				input = append(input, &newTableName)
			} else {
				trace_util_0.Count(_logical_plan_builder_00000, 1113)
				{
					input = append(input, s)
				}
			}
		}
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1108)
	return input
}

// extractTableSourceAsNames extracts TableSource.AsNames from node.
// if onlySelectStmt is set to be true, only extracts AsNames when TableSource.Source.(type) == *ast.SelectStmt
func extractTableSourceAsNames(node ast.ResultSetNode, input []string, onlySelectStmt bool) []string {
	trace_util_0.Count(_logical_plan_builder_00000, 1114)
	switch x := node.(type) {
	case *ast.Join:
		trace_util_0.Count(_logical_plan_builder_00000, 1116)
		input = extractTableSourceAsNames(x.Left, input, onlySelectStmt)
		input = extractTableSourceAsNames(x.Right, input, onlySelectStmt)
	case *ast.TableSource:
		trace_util_0.Count(_logical_plan_builder_00000, 1117)
		if _, ok := x.Source.(*ast.SelectStmt); !ok && onlySelectStmt {
			trace_util_0.Count(_logical_plan_builder_00000, 1120)
			break
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1118)
		if s, ok := x.Source.(*ast.TableName); ok {
			trace_util_0.Count(_logical_plan_builder_00000, 1121)
			if x.AsName.L == "" {
				trace_util_0.Count(_logical_plan_builder_00000, 1122)
				input = append(input, s.Name.L)
				break
			}
		}
		trace_util_0.Count(_logical_plan_builder_00000, 1119)
		input = append(input, x.AsName.L)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1115)
	return input
}

func appendVisitInfo(vi []visitInfo, priv mysql.PrivilegeType, db, tbl, col string, err error) []visitInfo {
	trace_util_0.Count(_logical_plan_builder_00000, 1123)
	return append(vi, visitInfo{
		privilege: priv,
		db:        db,
		table:     tbl,
		column:    col,
		err:       err,
	})
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	trace_util_0.Count(_logical_plan_builder_00000, 1124)
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		trace_util_0.Count(_logical_plan_builder_00000, 1127)
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1125)
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		trace_util_0.Count(_logical_plan_builder_00000, 1128)
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	trace_util_0.Count(_logical_plan_builder_00000, 1126)
	return expr
}

var _logical_plan_builder_00000 = "planner/core/logical_plan_builder.go"
