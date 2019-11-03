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

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

type aggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *LogicalAggregation) *LogicalProjection {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 0)
	for _, af := range agg.AggFuncs {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 4)
		// TODO(issue #9968): Actually, we can rewrite GROUP_CONCAT when all the
		// arguments it accepts are promised to be NOT-NULL.
		// When it accepts only 1 argument, we can extract this argument into a
		// projection.
		// When it accepts multiple arguments, we can wrap the arguments with a
		// function CONCAT_WS and extract this function into a projection.
		// BUT, GROUP_CONCAT should truncate the final result according to the
		// system variable `group_concat_max_len`. To ensure the correctness of
		// the result, we close the elimination of GROUP_CONCAT here.
		if af.Name == ast.AggFuncGroupConcat {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 5)
			return nil
		}
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 1)
	schemaByGroupby := expression.NewSchema(agg.groupByCols...)
	coveredByUniqueKey := false
	for _, key := range agg.children[0].Schema().Keys {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 6)
		if schemaByGroupby.ColumnsIndices(key) != nil {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 7)
			coveredByUniqueKey = true
			break
		}
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 2)
	if coveredByUniqueKey {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 8)
		// GroupByCols has unique key, so this aggregation can be removed.
		proj := a.convertAggToProj(agg)
		proj.SetChildren(agg.children[0])
		return proj
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 3)
	return nil
}

func (a *aggregationEliminateChecker) convertAggToProj(agg *LogicalAggregation) *LogicalProjection {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 9)
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx)
	for _, fun := range agg.AggFuncs {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 11)
		expr := a.rewriteExpr(agg.ctx, fun)
		proj.Exprs = append(proj.Exprs, expr)
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 10)
	proj.SetSchema(agg.schema.Clone())
	return proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (a *aggregationEliminateChecker) rewriteExpr(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc) expression.Expression {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 12)
	switch aggFunc.Name {
	case ast.AggFuncCount:
		trace_util_0.Count(_rule_aggregation_elimination_00000, 13)
		if aggFunc.Mode == aggregation.FinalMode {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 18)
			return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
		}
		trace_util_0.Count(_rule_aggregation_elimination_00000, 14)
		return a.rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		trace_util_0.Count(_rule_aggregation_elimination_00000, 15)
		return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		trace_util_0.Count(_rule_aggregation_elimination_00000, 16)
		return a.rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		trace_util_0.Count(_rule_aggregation_elimination_00000, 17)
		panic("Unsupported function")
	}
}

func (a *aggregationEliminateChecker) rewriteCount(ctx sessionctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 19)
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 21)
		if mysql.HasNotNullFlag(expr.GetType().Flag) {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 22)
			isNullExprs = append(isNullExprs, expression.Zero)
		} else {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 23)
			{
				isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
				isNullExprs = append(isNullExprs, isNullExpr)
			}
		}
	}

	trace_util_0.Count(_rule_aggregation_elimination_00000, 20)
	innerExpr := expression.ComposeDNFCondition(ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(ctx, ast.If, targetTp, innerExpr, expression.Zero, expression.One)
	return newExpr
}

func (a *aggregationEliminateChecker) rewriteBitFunc(ctx sessionctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 24)
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg)
	outerCast := a.wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 26)
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.Zero.Clone())
	} else {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 27)
		{
			finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(), outerCast, &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: targetTp})
		}
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 25)
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func (a *aggregationEliminateChecker) wrapCastFunction(ctx sessionctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 28)
	if arg.GetType() == targetTp {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 30)
		return arg
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 29)
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

func (a *aggregationEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_aggregation_elimination_00000, 31)
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 35)
		newChild, err := a.optimize(child)
		if err != nil {
			trace_util_0.Count(_rule_aggregation_elimination_00000, 37)
			return nil, err
		}
		trace_util_0.Count(_rule_aggregation_elimination_00000, 36)
		newChildren = append(newChildren, newChild)
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 32)
	p.SetChildren(newChildren...)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 38)
		return p, nil
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 33)
	if proj := a.tryToEliminateAggregation(agg); proj != nil {
		trace_util_0.Count(_rule_aggregation_elimination_00000, 39)
		return proj, nil
	}
	trace_util_0.Count(_rule_aggregation_elimination_00000, 34)
	return p, nil
}

var _rule_aggregation_elimination_00000 = "planner/core/rule_aggregation_elimination.go"
