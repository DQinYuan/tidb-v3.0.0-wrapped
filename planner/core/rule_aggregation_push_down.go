// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

type aggregationPushDownSolver struct {
	aggregationEliminateChecker
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// It's easy to see that max, min, first row is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (a *aggregationPushDownSolver) isDecomposable(fun *aggregation.AggFuncDesc) bool {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 0)
	switch fun.Name {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		trace_util_0.Count(_rule_aggregation_push_down_00000, 1)
		// TODO: Support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		trace_util_0.Count(_rule_aggregation_push_down_00000, 2)
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		trace_util_0.Count(_rule_aggregation_push_down_00000, 3)
		return !fun.HasDistinct
	default:
		trace_util_0.Count(_rule_aggregation_push_down_00000, 4)
		return false
	}
}

// getAggFuncChildIdx gets which children it belongs to, 0 stands for left, 1 stands for right, -1 stands for both.
func (a *aggregationPushDownSolver) getAggFuncChildIdx(aggFunc *aggregation.AggFuncDesc, schema *expression.Schema) int {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 5)
	fromLeft, fromRight := false, false
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, aggFunc.Args, nil)
	for _, col := range cols {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 8)
		if schema.Contains(col) {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 9)
			fromLeft = true
		} else {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 10)
			{
				fromRight = true
			}
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 6)
	if fromLeft && fromRight {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 11)
		return -1
	} else {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 12)
		if fromLeft {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 13)
			return 0
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 7)
	return 1
}

// collectAggFuncs collects all aggregate functions and splits them into two parts: "leftAggFuncs" and "rightAggFuncs" whose
// arguments are all from left child or right child separately. If some aggregate functions have the arguments that have
// columns both from left and right children, the whole aggregation is forbidden to push down.
func (a *aggregationPushDownSolver) collectAggFuncs(agg *LogicalAggregation, join *LogicalJoin) (valid bool, leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 14)
	valid = true
	leftChild := join.children[0]
	for _, aggFunc := range agg.AggFuncs {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 16)
		if !a.isDecomposable(aggFunc) {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 18)
			return false, nil, nil
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 17)
		index := a.getAggFuncChildIdx(aggFunc, leftChild.Schema())
		switch index {
		case 0:
			trace_util_0.Count(_rule_aggregation_push_down_00000, 19)
			leftAggFuncs = append(leftAggFuncs, aggFunc)
		case 1:
			trace_util_0.Count(_rule_aggregation_push_down_00000, 20)
			rightAggFuncs = append(rightAggFuncs, aggFunc)
		default:
			trace_util_0.Count(_rule_aggregation_push_down_00000, 21)
			return false, nil, nil
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 15)
	return
}

// collectGbyCols collects all columns from gby-items and join-conditions and splits them into two parts: "leftGbyCols" and
// "rightGbyCols". e.g. For query "SELECT SUM(B.id) FROM A, B WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3" , the optimized
// query should be "SELECT SUM(B.agg) FROM A, (SELECT SUM(id) as agg, c1, c2, c3 FROM B GROUP BY id, c1, c2, c3) as B
// WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3". As you see, all the columns appearing in join-conditions should be
// treated as group by columns in join subquery.
func (a *aggregationPushDownSolver) collectGbyCols(agg *LogicalAggregation, join *LogicalJoin) (leftGbyCols, rightGbyCols []*expression.Column) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 22)
	leftChild := join.children[0]
	ctx := agg.ctx
	for _, gbyExpr := range agg.GroupByItems {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 28)
		cols := expression.ExtractColumns(gbyExpr)
		for _, col := range cols {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 29)
			if leftChild.Schema().Contains(col) {
				trace_util_0.Count(_rule_aggregation_push_down_00000, 30)
				leftGbyCols = append(leftGbyCols, col)
			} else {
				trace_util_0.Count(_rule_aggregation_push_down_00000, 31)
				{
					rightGbyCols = append(rightGbyCols, col)
				}
			}
		}
	}
	// extract equal conditions
	trace_util_0.Count(_rule_aggregation_push_down_00000, 23)
	for _, eqFunc := range join.EqualConditions {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 32)
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, eqFunc.GetArgs()[0].(*expression.Column))
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, eqFunc.GetArgs()[1].(*expression.Column))
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 24)
	for _, leftCond := range join.LeftConditions {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 33)
		cols := expression.ExtractColumns(leftCond)
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, cols...)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 25)
	for _, rightCond := range join.RightConditions {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 34)
		cols := expression.ExtractColumns(rightCond)
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, cols...)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 26)
	for _, otherCond := range join.OtherConditions {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 35)
		cols := expression.ExtractColumns(otherCond)
		for _, col := range cols {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 36)
			if leftChild.Schema().Contains(col) {
				trace_util_0.Count(_rule_aggregation_push_down_00000, 37)
				leftGbyCols = a.addGbyCol(ctx, leftGbyCols, col)
			} else {
				trace_util_0.Count(_rule_aggregation_push_down_00000, 38)
				{
					rightGbyCols = a.addGbyCol(ctx, rightGbyCols, col)
				}
			}
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 27)
	return
}

func (a *aggregationPushDownSolver) splitAggFuncsAndGbyCols(agg *LogicalAggregation, join *LogicalJoin) (valid bool,
	leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc,
	leftGbyCols, rightGbyCols []*expression.Column) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 39)
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 41)
		return
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 40)
	leftGbyCols, rightGbyCols = a.collectGbyCols(agg, join)
	return
}

// addGbyCol adds a column to gbyCols. If a group by column has existed, it will not be added repeatedly.
func (a *aggregationPushDownSolver) addGbyCol(ctx sessionctx.Context, gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 42)
	for _, c := range cols {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 44)
		duplicate := false
		for _, gbyCol := range gbyCols {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 46)
			if c.Equal(ctx, gbyCol) {
				trace_util_0.Count(_rule_aggregation_push_down_00000, 47)
				duplicate = true
				break
			}
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 45)
		if !duplicate {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 48)
			gbyCols = append(gbyCols, c)
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 43)
	return gbyCols
}

// checkValidJoin checks if this join should be pushed across.
func (a *aggregationPushDownSolver) checkValidJoin(join *LogicalJoin) bool {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 49)
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (a *aggregationPushDownSolver) decompose(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc, schema *expression.Schema) ([]*aggregation.AggFuncDesc, *expression.Schema) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 50)
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []*aggregation.AggFuncDesc{aggFunc.Clone()}
	for _, aggFunc := range result {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 52)
		schema.Append(&expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("join_agg_%d", schema.Len())), // useless but for debug
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  aggFunc.RetTp,
		})
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 51)
	aggFunc.Args = expression.Column2Exprs(schema.Columns[schema.Len()-len(result):])
	aggFunc.Mode = aggregation.FinalMode
	return result, schema
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first row, we won't
// process it temporarily. If not, We will add additional group by columns and first row functions. We make a new aggregation operator.
// If the pushed aggregation is grouped by unique key, it's no need to push it down.
func (a *aggregationPushDownSolver) tryToPushDownAgg(aggFuncs []*aggregation.AggFuncDesc, gbyCols []*expression.Column, join *LogicalJoin, childIdx int) (_ LogicalPlan, err error) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 53)
	child := join.children[childIdx]
	if aggregation.IsAllFirstRow(aggFuncs) {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 60)
		return child, nil
	}
	// If the join is multiway-join, we forbid pushing down.
	trace_util_0.Count(_rule_aggregation_push_down_00000, 54)
	if _, ok := join.children[childIdx].(*LogicalJoin); ok {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 61)
		return child, nil
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 55)
	tmpSchema := expression.NewSchema(gbyCols...)
	for _, key := range child.Schema().Keys {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 62)
		if tmpSchema.ColumnsIndices(key) != nil {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 63)
			return child, nil
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 56)
	agg, err := a.makeNewAgg(join.ctx, aggFuncs, gbyCols)
	if err != nil {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 64)
		return nil, err
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 57)
	agg.SetChildren(child)
	// If agg has no group-by item, it will return a default value, which may cause some bugs.
	// So here we add a group-by item forcely.
	if len(agg.GroupByItems) == 0 {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 65)
		agg.GroupByItems = []expression.Expression{&expression.Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeLong)}}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 58)
	if (childIdx == 0 && join.JoinType == RightOuterJoin) || (childIdx == 1 && join.JoinType == LeftOuterJoin) {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 66)
		var existsDefaultValues bool
		join.DefaultValues, existsDefaultValues = a.getDefaultValues(agg)
		if !existsDefaultValues {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 67)
			return child, nil
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 59)
	return agg, nil
}

func (a *aggregationPushDownSolver) getDefaultValues(agg *LogicalAggregation) ([]types.Datum, bool) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 68)
	defaultValues := make([]types.Datum, 0, agg.Schema().Len())
	for _, aggFunc := range agg.AggFuncs {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 70)
		value, existsDefaultValue := aggFunc.EvalNullValueInOuterJoin(agg.ctx, agg.children[0].Schema())
		if !existsDefaultValue {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 72)
			return nil, false
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 71)
		defaultValues = append(defaultValues, value)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 69)
	return defaultValues, true
}

func (a *aggregationPushDownSolver) checkAnyCountAndSum(aggFuncs []*aggregation.AggFuncDesc) bool {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 73)
	for _, fun := range aggFuncs {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 75)
		if fun.Name == ast.AggFuncSum || fun.Name == ast.AggFuncCount {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 76)
			return true
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 74)
	return false
}

func (a *aggregationPushDownSolver) makeNewAgg(ctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, gbyCols []*expression.Column) (*LogicalAggregation, error) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 77)
	agg := LogicalAggregation{
		GroupByItems: expression.Column2Exprs(gbyCols),
		groupByCols:  gbyCols,
	}.Init(ctx)
	aggLen := len(aggFuncs) + len(gbyCols)
	newAggFuncDescs := make([]*aggregation.AggFuncDesc, 0, aggLen)
	schema := expression.NewSchema(make([]*expression.Column, 0, aggLen)...)
	for _, aggFunc := range aggFuncs {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 80)
		var newFuncs []*aggregation.AggFuncDesc
		newFuncs, schema = a.decompose(ctx, aggFunc, schema)
		newAggFuncDescs = append(newAggFuncDescs, newFuncs...)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 78)
	for _, gbyCol := range gbyCols {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 81)
		firstRow, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{gbyCol}, false)
		if err != nil {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 83)
			return nil, err
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 82)
		newCol, _ := gbyCol.Clone().(*expression.Column)
		newCol.RetType = firstRow.RetTp
		newAggFuncDescs = append(newAggFuncDescs, firstRow)
		schema.Append(newCol)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 79)
	agg.AggFuncs = newAggFuncDescs
	agg.SetSchema(schema)
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// agg.buildProjectionIfNecessary()
	return agg, nil
}

// pushAggCrossUnion will try to push the agg down to the union. If the new aggregation's group-by columns doesn't contain unique key.
// We will return the new aggregation. Otherwise we will transform the aggregation to projection.
func (a *aggregationPushDownSolver) pushAggCrossUnion(agg *LogicalAggregation, unionSchema *expression.Schema, unionChild LogicalPlan) LogicalPlan {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 84)
	ctx := agg.ctx
	newAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(agg.GroupByItems)),
	}.Init(ctx)
	newAgg.SetSchema(agg.schema.Clone())
	for _, aggFunc := range agg.AggFuncs {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 88)
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.Args))
		for _, arg := range newAggFunc.Args {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 90)
			newArgs = append(newArgs, expression.ColumnSubstitute(arg, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns)))
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 89)
		newAggFunc.Args = newArgs
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 85)
	for _, gbyExpr := range agg.GroupByItems {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 91)
		newExpr := expression.ColumnSubstitute(gbyExpr, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 86)
	newAgg.collectGroupByColumns()
	tmpSchema := expression.NewSchema(newAgg.groupByCols...)
	// e.g. Union distinct will add a aggregation like `select join_agg_0, join_agg_1, join_agg_2 from t group by a, b, c` above UnionAll.
	// And the pushed agg will be something like `select a, b, c, a, b, c from t group by a, b, c`. So if we just return child as join does,
	// this will cause error during executor phase.
	for _, key := range unionChild.Schema().Keys {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 92)
		if tmpSchema.ColumnsIndices(key) != nil {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 93)
			proj := a.convertAggToProj(newAgg)
			proj.SetChildren(unionChild)
			return proj
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 87)
	newAgg.SetChildren(unionChild)
	return newAgg
}

func (a *aggregationPushDownSolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 94)
	if !p.context().GetSessionVars().AllowAggPushDown {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 96)
		return p, nil
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 95)
	return a.aggPushDown(p)
}

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggregationPushDownSolver) aggPushDown(p LogicalPlan) (_ LogicalPlan, err error) {
	trace_util_0.Count(_rule_aggregation_push_down_00000, 97)
	if agg, ok := p.(*LogicalAggregation); ok {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 100)
		proj := a.tryToEliminateAggregation(agg)
		if proj != nil {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 101)
			p = proj
		} else {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 102)
			{
				child := agg.children[0]
				if join, ok1 := child.(*LogicalJoin); ok1 && a.checkValidJoin(join) {
					trace_util_0.Count(_rule_aggregation_push_down_00000, 103)
					if valid, leftAggFuncs, rightAggFuncs, leftGbyCols, rightGbyCols := a.splitAggFuncsAndGbyCols(agg, join); valid {
						trace_util_0.Count(_rule_aggregation_push_down_00000, 104)
						var lChild, rChild LogicalPlan
						// If there exist count or sum functions in left join path, we can't push any
						// aggregate function into right join path.
						rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
						leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
						if rightInvalid {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 107)
							rChild = join.children[1]
						} else {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 108)
							{
								rChild, err = a.tryToPushDownAgg(rightAggFuncs, rightGbyCols, join, 1)
								if err != nil {
									trace_util_0.Count(_rule_aggregation_push_down_00000, 109)
									return nil, err
								}
							}
						}
						trace_util_0.Count(_rule_aggregation_push_down_00000, 105)
						if leftInvalid {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 110)
							lChild = join.children[0]
						} else {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 111)
							{
								lChild, err = a.tryToPushDownAgg(leftAggFuncs, leftGbyCols, join, 0)
								if err != nil {
									trace_util_0.Count(_rule_aggregation_push_down_00000, 112)
									return nil, err
								}
							}
						}
						trace_util_0.Count(_rule_aggregation_push_down_00000, 106)
						join.SetChildren(lChild, rChild)
						join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
						join.buildKeyInfo()
						proj := a.tryToEliminateAggregation(agg)
						if proj != nil {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 113)
							p = proj
						}
					}
				} else {
					trace_util_0.Count(_rule_aggregation_push_down_00000, 114)
					if proj, ok1 := child.(*LogicalProjection); ok1 {
						trace_util_0.Count(_rule_aggregation_push_down_00000, 115)
						// TODO: This optimization is not always reasonable. We have not supported pushing projection to kv layer yet,
						// so we must do this optimization.
						for i, gbyItem := range agg.GroupByItems {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 118)
							agg.GroupByItems[i] = expression.ColumnSubstitute(gbyItem, proj.schema, proj.Exprs)
						}
						trace_util_0.Count(_rule_aggregation_push_down_00000, 116)
						agg.collectGroupByColumns()
						for _, aggFunc := range agg.AggFuncs {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 119)
							newArgs := make([]expression.Expression, 0, len(aggFunc.Args))
							for _, arg := range aggFunc.Args {
								trace_util_0.Count(_rule_aggregation_push_down_00000, 121)
								newArgs = append(newArgs, expression.ColumnSubstitute(arg, proj.schema, proj.Exprs))
							}
							trace_util_0.Count(_rule_aggregation_push_down_00000, 120)
							aggFunc.Args = newArgs
						}
						trace_util_0.Count(_rule_aggregation_push_down_00000, 117)
						projChild := proj.children[0]
						agg.SetChildren(projChild)
					} else {
						trace_util_0.Count(_rule_aggregation_push_down_00000, 122)
						if union, ok1 := child.(*LogicalUnionAll); ok1 {
							trace_util_0.Count(_rule_aggregation_push_down_00000, 123)
							var gbyCols []*expression.Column
							gbyCols = expression.ExtractColumnsFromExpressions(gbyCols, agg.GroupByItems, nil)
							pushedAgg, err := a.makeNewAgg(agg.ctx, agg.AggFuncs, gbyCols)
							if err != nil {
								trace_util_0.Count(_rule_aggregation_push_down_00000, 126)
								return nil, err
							}
							trace_util_0.Count(_rule_aggregation_push_down_00000, 124)
							newChildren := make([]LogicalPlan, 0, len(union.children))
							for _, child := range union.children {
								trace_util_0.Count(_rule_aggregation_push_down_00000, 127)
								newChild := a.pushAggCrossUnion(pushedAgg, union.Schema(), child)
								newChildren = append(newChildren, newChild)
							}
							trace_util_0.Count(_rule_aggregation_push_down_00000, 125)
							union.SetSchema(expression.NewSchema(newChildren[0].Schema().Columns...))
							union.SetChildren(newChildren...)
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 98)
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_aggregation_push_down_00000, 128)
		newChild, err := a.aggPushDown(child)
		if err != nil {
			trace_util_0.Count(_rule_aggregation_push_down_00000, 130)
			return nil, err
		}
		trace_util_0.Count(_rule_aggregation_push_down_00000, 129)
		newChildren = append(newChildren, newChild)
	}
	trace_util_0.Count(_rule_aggregation_push_down_00000, 99)
	p.SetChildren(newChildren...)
	return p, nil
}

var _rule_aggregation_push_down_00000 = "planner/core/rule_aggregation_push_down.go"
