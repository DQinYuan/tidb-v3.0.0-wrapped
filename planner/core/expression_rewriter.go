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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

// EvalSubquery evaluates incorrelated subqueries once.
var EvalSubquery func(p PhysicalPlan, is infoschema.InfoSchema, ctx sessionctx.Context) ([][]types.Datum, error)

// evalAstExpr evaluates ast expression directly.
func evalAstExpr(ctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error) {
	trace_util_0.Count(_expression_rewriter_00000, 0)
	if val, ok := expr.(*driver.ValueExpr); ok {
		trace_util_0.Count(_expression_rewriter_00000, 4)
		return val.Datum, nil
	}
	trace_util_0.Count(_expression_rewriter_00000, 1)
	b := &PlanBuilder{
		ctx:       ctx,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	if ctx.GetSessionVars().TxnCtx.InfoSchema != nil {
		trace_util_0.Count(_expression_rewriter_00000, 5)
		b.is = ctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	trace_util_0.Count(_expression_rewriter_00000, 2)
	fakePlan := LogicalTableDual{}.Init(ctx)
	newExpr, _, err := b.rewrite(expr, fakePlan, nil, true)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 6)
		return types.Datum{}, err
	}
	trace_util_0.Count(_expression_rewriter_00000, 3)
	return newExpr.Eval(chunk.Row{})
}

func (b *PlanBuilder) rewriteInsertOnDuplicateUpdate(exprNode ast.ExprNode, mockPlan LogicalPlan, insertPlan *Insert) (expression.Expression, error) {
	trace_util_0.Count(_expression_rewriter_00000, 7)
	b.rewriterCounter++
	defer func() { trace_util_0.Count(_expression_rewriter_00000, 10); b.rewriterCounter-- }()

	trace_util_0.Count(_expression_rewriter_00000, 8)
	rewriter := b.getExpressionRewriter(mockPlan)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 11)
		return nil, rewriter.err
	}

	trace_util_0.Count(_expression_rewriter_00000, 9)
	rewriter.insertPlan = insertPlan
	rewriter.asScalar = true

	expr, _, err := b.rewriteExprNode(rewriter, exprNode, true)
	return expr, err
}

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *PlanBuilder) rewrite(exprNode ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (expression.Expression, LogicalPlan, error) {
	trace_util_0.Count(_expression_rewriter_00000, 12)
	expr, resultPlan, err := b.rewriteWithPreprocess(exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultPlan, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *PlanBuilder) rewriteWithPreprocess(exprNode ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int, asScalar bool, preprocess func(ast.Node) ast.Node) (expression.Expression, LogicalPlan, error) {
	trace_util_0.Count(_expression_rewriter_00000, 13)
	b.rewriterCounter++
	defer func() { trace_util_0.Count(_expression_rewriter_00000, 16); b.rewriterCounter-- }()

	trace_util_0.Count(_expression_rewriter_00000, 14)
	rewriter := b.getExpressionRewriter(p)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 17)
		return nil, nil, rewriter.err
	}

	trace_util_0.Count(_expression_rewriter_00000, 15)
	rewriter.aggrMap = aggMapper
	rewriter.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.preprocess = preprocess

	expr, resultPlan, err := b.rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultPlan, err
}

func (b *PlanBuilder) getExpressionRewriter(p LogicalPlan) (rewriter *expressionRewriter) {
	trace_util_0.Count(_expression_rewriter_00000, 18)
	defer func() {
		trace_util_0.Count(_expression_rewriter_00000, 21)
		if p != nil {
			trace_util_0.Count(_expression_rewriter_00000, 22)
			rewriter.schema = p.Schema()
		}
	}()

	trace_util_0.Count(_expression_rewriter_00000, 19)
	if len(b.rewriterPool) < b.rewriterCounter {
		trace_util_0.Count(_expression_rewriter_00000, 23)
		rewriter = &expressionRewriter{p: p, b: b, ctx: b.ctx}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	trace_util_0.Count(_expression_rewriter_00000, 20)
	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.p = p
	rewriter.asScalar = false
	rewriter.aggrMap = nil
	rewriter.preprocess = nil
	rewriter.insertPlan = nil
	rewriter.disableFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	return
}

func (b *PlanBuilder) rewriteExprNode(rewriter *expressionRewriter, exprNode ast.ExprNode, asScalar bool) (expression.Expression, LogicalPlan, error) {
	trace_util_0.Count(_expression_rewriter_00000, 24)
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 29)
		return nil, nil, errors.Trace(rewriter.err)
	}
	trace_util_0.Count(_expression_rewriter_00000, 25)
	if !asScalar && len(rewriter.ctxStack) == 0 {
		trace_util_0.Count(_expression_rewriter_00000, 30)
		return nil, rewriter.p, nil
	}
	trace_util_0.Count(_expression_rewriter_00000, 26)
	if len(rewriter.ctxStack) != 1 {
		trace_util_0.Count(_expression_rewriter_00000, 31)
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	trace_util_0.Count(_expression_rewriter_00000, 27)
	rewriter.err = expression.CheckArgsNotMultiColumnRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 32)
		return nil, nil, errors.Trace(rewriter.err)
	}
	trace_util_0.Count(_expression_rewriter_00000, 28)
	return rewriter.ctxStack[0], rewriter.p, nil
}

type expressionRewriter struct {
	ctxStack  []expression.Expression
	p         LogicalPlan
	schema    *expression.Schema
	err       error
	aggrMap   map[*ast.AggregateFuncExpr]int
	windowMap map[*ast.WindowFuncExpr]int
	b         *PlanBuilder
	ctx       sessionctx.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during expression rewritten.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// insertPlan is only used to rewrite the expressions inside the assignment
	// of the "INSERT" statement.
	insertPlan *Insert

	// disableFoldCounter controls fold-disabled scope. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the scope(disable), the counter will +1; while
	// leaving the scope(enable again), the counter will -1.
	// NOTE: This value can be changed during expression rewritten.
	disableFoldCounter int
}

// constructBinaryOpFunction converts binary operator functions
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. Else constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
// 		IF ( isNull(a0 NE b0), Null,
// 			IF ( a1 NE b1, a1 op b1,
// 				IF ( isNull(a1 NE b1), Null, a2 op b2))))`
func (er *expressionRewriter) constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	trace_util_0.Count(_expression_rewriter_00000, 33)
	lLen, rLen := expression.GetRowLen(l), expression.GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		trace_util_0.Count(_expression_rewriter_00000, 35)
		return er.newFunction(op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 36)
		if rLen != lLen {
			trace_util_0.Count(_expression_rewriter_00000, 37)
			return nil, expression.ErrOperandColumns.GenWithStackByArgs(lLen)
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 34)
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		trace_util_0.Count(_expression_rewriter_00000, 38)
		funcs := make([]expression.Expression, lLen)
		for i := 0; i < lLen; i++ {
			trace_util_0.Count(_expression_rewriter_00000, 46)
			var err error
			funcs[i], err = er.constructBinaryOpFunction(expression.GetFuncArg(l, i), expression.GetFuncArg(r, i), op)
			if err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 47)
				return nil, err
			}
		}
		trace_util_0.Count(_expression_rewriter_00000, 39)
		if op == ast.NE {
			trace_util_0.Count(_expression_rewriter_00000, 48)
			return expression.ComposeDNFCondition(er.ctx, funcs...), nil
		}
		trace_util_0.Count(_expression_rewriter_00000, 40)
		return expression.ComposeCNFCondition(er.ctx, funcs...), nil
	default:
		trace_util_0.Count(_expression_rewriter_00000, 41)
		larg0, rarg0 := expression.GetFuncArg(l, 0), expression.GetFuncArg(r, 0)
		var expr1, expr2, expr3, expr4, expr5 expression.Expression
		expr1 = expression.NewFunctionInternal(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr2 = expression.NewFunctionInternal(er.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr3 = expression.NewFunctionInternal(er.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr1)
		var err error
		l, err = expression.PopRowFirstArg(er.ctx, l)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 49)
			return nil, err
		}
		trace_util_0.Count(_expression_rewriter_00000, 42)
		r, err = expression.PopRowFirstArg(er.ctx, r)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 50)
			return nil, err
		}
		trace_util_0.Count(_expression_rewriter_00000, 43)
		expr4, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 51)
			return nil, err
		}
		trace_util_0.Count(_expression_rewriter_00000, 44)
		expr5, err = er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr3, expression.Null, expr4)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 52)
			return nil, err
		}
		trace_util_0.Count(_expression_rewriter_00000, 45)
		return er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr5)
	}
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (LogicalPlan, error) {
	trace_util_0.Count(_expression_rewriter_00000, 53)
	if er.schema != nil {
		trace_util_0.Count(_expression_rewriter_00000, 56)
		outerSchema := er.schema.Clone()
		er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
		defer func() {
			trace_util_0.Count(_expression_rewriter_00000, 57)
			er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
		}()
	}

	trace_util_0.Count(_expression_rewriter_00000, 54)
	np, err := er.b.buildResultSetNode(subq.Query)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 58)
		return nil, err
	}
	trace_util_0.Count(_expression_rewriter_00000, 55)
	return np, nil
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_expression_rewriter_00000, 59)
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_expression_rewriter_00000, 61)
		index, ok := -1, false
		if er.aggrMap != nil {
			trace_util_0.Count(_expression_rewriter_00000, 81)
			index, ok = er.aggrMap[v]
		}
		trace_util_0.Count(_expression_rewriter_00000, 62)
		if !ok {
			trace_util_0.Count(_expression_rewriter_00000, 82)
			er.err = ErrInvalidGroupFuncUse
			return inNode, true
		}
		trace_util_0.Count(_expression_rewriter_00000, 63)
		er.ctxStack = append(er.ctxStack, er.schema.Columns[index])
		return inNode, true
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_expression_rewriter_00000, 64)
		if index, ok := er.b.colMapper[v]; ok {
			trace_util_0.Count(_expression_rewriter_00000, 83)
			er.ctxStack = append(er.ctxStack, er.schema.Columns[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		trace_util_0.Count(_expression_rewriter_00000, 65)
		return er.handleCompareSubquery(v)
	case *ast.ExistsSubqueryExpr:
		trace_util_0.Count(_expression_rewriter_00000, 66)
		return er.handleExistSubquery(v)
	case *ast.PatternInExpr:
		trace_util_0.Count(_expression_rewriter_00000, 67)
		if v.Sel != nil {
			trace_util_0.Count(_expression_rewriter_00000, 84)
			return er.handleInSubquery(v)
		}
		trace_util_0.Count(_expression_rewriter_00000, 68)
		if len(v.List) != 1 {
			trace_util_0.Count(_expression_rewriter_00000, 85)
			break
		}
		// For 10 in ((select * from t)), the parser won't set v.Sel.
		// So we must process this case here.
		trace_util_0.Count(_expression_rewriter_00000, 69)
		x := v.List[0]
		for {
			trace_util_0.Count(_expression_rewriter_00000, 86)
			switch y := x.(type) {
			case *ast.SubqueryExpr:
				trace_util_0.Count(_expression_rewriter_00000, 87)
				v.Sel = y
				return er.handleInSubquery(v)
			case *ast.ParenthesesExpr:
				trace_util_0.Count(_expression_rewriter_00000, 88)
				x = y.Expr
			default:
				trace_util_0.Count(_expression_rewriter_00000, 89)
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		trace_util_0.Count(_expression_rewriter_00000, 70)
		return er.handleScalarSubquery(v)
	case *ast.ParenthesesExpr:
		trace_util_0.Count(_expression_rewriter_00000, 71)
	case *ast.ValuesExpr:
		trace_util_0.Count(_expression_rewriter_00000, 72)
		schema := er.schema
		// NOTE: "er.insertPlan != nil" means that we are rewriting the
		// expressions inside the assignment of "INSERT" statement. we have to
		// use the "tableSchema" of that "insertPlan".
		if er.insertPlan != nil {
			trace_util_0.Count(_expression_rewriter_00000, 90)
			schema = er.insertPlan.tableSchema
		}
		trace_util_0.Count(_expression_rewriter_00000, 73)
		col, err := schema.FindColumn(v.Column.Name)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 91)
			er.err = err
			return inNode, false
		}
		trace_util_0.Count(_expression_rewriter_00000, 74)
		if col == nil {
			trace_util_0.Count(_expression_rewriter_00000, 92)
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Column.Name.OrigColName(), "field list")
			return inNode, false
		}
		trace_util_0.Count(_expression_rewriter_00000, 75)
		er.ctxStack = append(er.ctxStack, expression.NewValuesFunc(er.ctx, col.Index, col.RetType))
		return inNode, true
	case *ast.WindowFuncExpr:
		trace_util_0.Count(_expression_rewriter_00000, 76)
		index, ok := -1, false
		if er.windowMap != nil {
			trace_util_0.Count(_expression_rewriter_00000, 93)
			index, ok = er.windowMap[v]
		}
		trace_util_0.Count(_expression_rewriter_00000, 77)
		if !ok {
			trace_util_0.Count(_expression_rewriter_00000, 94)
			er.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(v.F)
			return inNode, true
		}
		trace_util_0.Count(_expression_rewriter_00000, 78)
		er.ctxStack = append(er.ctxStack, er.schema.Columns[index])
		return inNode, true
	case *ast.FuncCallExpr:
		trace_util_0.Count(_expression_rewriter_00000, 79)
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			trace_util_0.Count(_expression_rewriter_00000, 95)
			er.disableFoldCounter++
		}
	default:
		trace_util_0.Count(_expression_rewriter_00000, 80)
		er.asScalar = true
	}
	trace_util_0.Count(_expression_rewriter_00000, 60)
	return inNode, false
}

func (er *expressionRewriter) buildSemiApplyFromEqualSubq(np LogicalPlan, l, r expression.Expression, not bool) {
	trace_util_0.Count(_expression_rewriter_00000, 96)
	var condition expression.Expression
	if rCol, ok := r.(*expression.Column); ok && (er.asScalar || not) {
		trace_util_0.Count(_expression_rewriter_00000, 99)
		rCol.InOperand = true
		// If both input columns of `!= all / = any` expression are not null, we can treat the expression
		// as normal column equal condition.
		if lCol, ok := l.(*expression.Column); ok && mysql.HasNotNullFlag(lCol.GetType().Flag) && mysql.HasNotNullFlag(rCol.GetType().Flag) {
			trace_util_0.Count(_expression_rewriter_00000, 100)
			rCol.InOperand = false
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 97)
	condition, er.err = er.constructBinaryOpFunction(l, r, ast.EQ)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 101)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 98)
	er.p, er.err = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, not)
}

func (er *expressionRewriter) handleCompareSubquery(v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	trace_util_0.Count(_expression_rewriter_00000, 102)
	v.L.Accept(er)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 111)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 103)
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		trace_util_0.Count(_expression_rewriter_00000, 112)
		er.err = errors.Errorf("Unknown compare type %T.", v.R)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 104)
	np, err := er.buildSubquery(subq)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 113)
		er.err = err
		return v, true
	}
	// Only (a,b,c) = any (...) and (a,b,c) != all (...) can use row expression.
	trace_util_0.Count(_expression_rewriter_00000, 105)
	canMultiCol := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiCol && (expression.GetRowLen(lexpr) != 1 || np.Schema().Len() != 1) {
		trace_util_0.Count(_expression_rewriter_00000, 114)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 106)
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		trace_util_0.Count(_expression_rewriter_00000, 115)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(lLen)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 107)
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		trace_util_0.Count(_expression_rewriter_00000, 116)
		rexpr = np.Schema().Columns[0]
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 117)
		{
			args := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().Columns {
				trace_util_0.Count(_expression_rewriter_00000, 119)
				args = append(args, col)
			}
			trace_util_0.Count(_expression_rewriter_00000, 118)
			rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
			if er.err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 120)
				return v, true
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 108)
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		trace_util_0.Count(_expression_rewriter_00000, 121)
		if v.Op == opcode.EQ {
			trace_util_0.Count(_expression_rewriter_00000, 123)
			if v.All {
				trace_util_0.Count(_expression_rewriter_00000, 124)
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				trace_util_0.Count(_expression_rewriter_00000, 125)
				{
					// `a = any(subq)` will be rewriten as `a in (subq)`.
					er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, false)
					if er.err != nil {
						trace_util_0.Count(_expression_rewriter_00000, 126)
						return v, true
					}
				}
			}
		} else {
			trace_util_0.Count(_expression_rewriter_00000, 127)
			if v.Op == opcode.NE {
				trace_util_0.Count(_expression_rewriter_00000, 128)
				if v.All {
					trace_util_0.Count(_expression_rewriter_00000, 129)
					// `a != all(subq)` will be rewriten as `a not in (subq)`.
					er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, true)
					if er.err != nil {
						trace_util_0.Count(_expression_rewriter_00000, 130)
						return v, true
					}
				} else {
					trace_util_0.Count(_expression_rewriter_00000, 131)
					{
						er.handleNEAny(lexpr, rexpr, np)
					}
				}
			} else {
				trace_util_0.Count(_expression_rewriter_00000, 132)
				{
					// TODO: Support this in future.
					er.err = errors.New("We don't support <=> all or <=> any now")
					return v, true
				}
			}
		}
	default:
		trace_util_0.Count(_expression_rewriter_00000, 122)
		// When < all or > any , the agg function should use min.
		useMin := ((v.Op == opcode.LT || v.Op == opcode.LE) && v.All) || ((v.Op == opcode.GT || v.Op == opcode.GE) && !v.All)
		er.handleOtherComparableSubq(lexpr, rexpr, np, useMin, v.Op.String(), v.All)
	}
	trace_util_0.Count(_expression_rewriter_00000, 109)
	if er.asScalar {
		trace_util_0.Count(_expression_rewriter_00000, 133)
		// The parent expression only use the last column in schema, which represents whether the condition is matched.
		er.ctxStack[len(er.ctxStack)-1] = er.p.Schema().Columns[er.p.Schema().Len()-1]
	}
	trace_util_0.Count(_expression_rewriter_00000, 110)
	return v, true
}

// handleOtherComparableSubq handles the queries like < any, < max, etc. For example, if the query is t.id < any (select s.id from s),
// it will be rewrote to t.id < (select max(s.id) from s).
func (er *expressionRewriter) handleOtherComparableSubq(lexpr, rexpr expression.Expression, np LogicalPlan, useMin bool, cmpFunc string, all bool) {
	trace_util_0.Count(_expression_rewriter_00000, 134)
	plan4Agg := LogicalAggregation{}.Init(er.ctx)
	plan4Agg.SetChildren(np)

	// Create a "max" or "min" aggregation.
	funcName := ast.AggFuncMax
	if useMin {
		trace_util_0.Count(_expression_rewriter_00000, 137)
		funcName = ast.AggFuncMin
	}
	trace_util_0.Count(_expression_rewriter_00000, 135)
	funcMaxOrMin, err := aggregation.NewAggFuncDesc(er.ctx, funcName, []expression.Expression{rexpr}, false)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 138)
		er.err = err
		return
	}

	// Create a column and append it to the schema of that aggregation.
	trace_util_0.Count(_expression_rewriter_00000, 136)
	colMaxOrMin := &expression.Column{
		ColName:  model.NewCIStr("agg_Col_0"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcMaxOrMin.RetTp,
	}
	schema := expression.NewSchema(colMaxOrMin)

	plan4Agg.SetSchema(schema)
	plan4Agg.AggFuncs = []*aggregation.AggFuncDesc{funcMaxOrMin}

	cond := expression.NewFunctionInternal(er.ctx, cmpFunc, types.NewFieldType(mysql.TypeTiny), lexpr, colMaxOrMin)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, all)
}

// buildQuantifierPlan adds extra condition for any / all subquery.
func (er *expressionRewriter) buildQuantifierPlan(plan4Agg *LogicalAggregation, cond, lexpr, rexpr expression.Expression, all bool) {
	trace_util_0.Count(_expression_rewriter_00000, 139)
	innerIsNull := expression.NewFunctionInternal(er.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), rexpr)
	outerIsNull := expression.NewFunctionInternal(er.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), lexpr)

	funcSum, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncSum, []expression.Expression{innerIsNull}, false)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 144)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 140)
	colSum := &expression.Column{
		ColName:  model.NewCIStr("agg_col_sum"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcSum.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcSum)
	plan4Agg.schema.Append(colSum)
	innerHasNull := expression.NewFunctionInternal(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colSum, expression.Zero)

	// Build `count(1)` aggregation to check if subquery is empty.
	funcCount, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncCount, []expression.Expression{expression.One}, false)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 145)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 141)
	colCount := &expression.Column{
		ColName:  model.NewCIStr("agg_col_cnt"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcCount.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcCount)
	plan4Agg.schema.Append(colCount)

	if all {
		trace_util_0.Count(_expression_rewriter_00000, 146)
		// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
		// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) != 0, null, true).
		innerNullChecker := expression.NewFunctionInternal(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.Null, expression.One)
		cond = expression.ComposeCNFCondition(er.ctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return true.
		emptyChecker := expression.NewFunctionInternal(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), colCount, expression.Zero)
		// If outer key is null, and subquery is not empty, it should always return null, even when it is `null = all (1, 2)`.
		outerNullChecker := expression.NewFunctionInternal(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.Null, expression.Zero)
		cond = expression.ComposeDNFCondition(er.ctx, cond, emptyChecker, outerNullChecker)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 147)
		{
			// For "any" expression, if the subquery has null and the cond returns false, the result should be NULL.
			// Specifically, `t.id < any (select s.id from s)` would be rewrote to `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`
			innerNullChecker := expression.NewFunctionInternal(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.Null, expression.Zero)
			cond = expression.ComposeDNFCondition(er.ctx, cond, innerNullChecker)
			// If the subquery is empty, it should always return false.
			emptyChecker := expression.NewFunctionInternal(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colCount, expression.Zero)
			// If outer key is null, and subquery is not empty, it should return null.
			outerNullChecker := expression.NewFunctionInternal(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.Null, expression.One)
			cond = expression.ComposeCNFCondition(er.ctx, cond, emptyChecker, outerNullChecker)
		}
	}

	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// plan4Agg.buildProjectionIfNecessary()
	trace_util_0.Count(_expression_rewriter_00000, 142)
	if !er.asScalar {
		trace_util_0.Count(_expression_rewriter_00000, 148)
		// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
		er.p, er.err = er.b.buildSemiApply(er.p, plan4Agg, []expression.Expression{cond}, false, false)
		return
	}
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	trace_util_0.Count(_expression_rewriter_00000, 143)
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, plan4Agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(joinSchema.Clone().Columns[:outerSchemaLen]),
	}.Init(er.ctx)
	proj.SetSchema(expression.NewSchema(joinSchema.Clone().Columns[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schema.Append(&expression.Column{
		ColName:      model.NewCIStr("aux_col"),
		UniqueID:     er.ctx.GetSessionVars().AllocPlanColumnID(),
		IsReferenced: true,
		RetType:      cond.GetType(),
	})
	proj.SetChildren(er.p)
	er.p = proj
}

// handleNEAny handles the case of != any. For example, if the query is t.id != any (select s.id from s), it will be rewrote to
// t.id != s.id or count(distinct s.id) > 1 or [any checker]. If there are two different values in s.id ,
// there must exist a s.id that doesn't equal to t.id.
func (er *expressionRewriter) handleNEAny(lexpr, rexpr expression.Expression, np LogicalPlan) {
	trace_util_0.Count(_expression_rewriter_00000, 149)
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 152)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 150)
	countFunc, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 153)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 151)
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.ctx)
	plan4Agg.SetChildren(np)
	firstRowResultCol := &expression.Column{
		ColName:  model.NewCIStr("col_firstRow"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  firstRowFunc.RetTp,
	}
	count := &expression.Column{
		ColName:  model.NewCIStr("col_count"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	gtFunc := expression.NewFunctionInternal(er.ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), count, expression.One)
	neCond := expression.NewFunctionInternal(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol)
	cond := expression.ComposeDNFCondition(er.ctx, gtFunc, neCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, false)
}

// handleEQAll handles the case of = all. For example, if the query is t.id = all (select s.id from s), it will be rewrote to
// t.id = (select s.id from s having count(distinct s.id) <= 1 and [all checker]).
func (er *expressionRewriter) handleEQAll(lexpr, rexpr expression.Expression, np LogicalPlan) {
	trace_util_0.Count(_expression_rewriter_00000, 154)
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 157)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 155)
	countFunc, err := aggregation.NewAggFuncDesc(er.ctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 158)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 156)
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.ctx)
	plan4Agg.SetChildren(np)
	firstRowResultCol := &expression.Column{
		ColName:  model.NewCIStr("col_firstRow"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  firstRowFunc.RetTp,
	}
	count := &expression.Column{
		ColName:  model.NewCIStr("col_count"),
		UniqueID: er.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	leFunc := expression.NewFunctionInternal(er.ctx, ast.LE, types.NewFieldType(mysql.TypeTiny), count, expression.One)
	eqCond := expression.NewFunctionInternal(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol)
	cond := expression.ComposeCNFCondition(er.ctx, leFunc, eqCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, true)
}

func (er *expressionRewriter) handleExistSubquery(v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	trace_util_0.Count(_expression_rewriter_00000, 159)
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		trace_util_0.Count(_expression_rewriter_00000, 163)
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 160)
	np, err := er.buildSubquery(subq)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 164)
		er.err = err
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 161)
	np = er.popExistsSubPlan(np)
	if len(np.extractCorrelatedCols()) > 0 {
		trace_util_0.Count(_expression_rewriter_00000, 165)
		er.p, er.err = er.b.buildSemiApply(er.p, np, nil, er.asScalar, v.Not)
		if er.err != nil || !er.asScalar {
			trace_util_0.Count(_expression_rewriter_00000, 167)
			return v, true
		}
		trace_util_0.Count(_expression_rewriter_00000, 166)
		er.ctxStack = append(er.ctxStack, er.p.Schema().Columns[er.p.Schema().Len()-1])
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 168)
		{
			physicalPlan, err := DoOptimize(er.b.optFlag, np)
			if err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 171)
				er.err = err
				return v, true
			}
			trace_util_0.Count(_expression_rewriter_00000, 169)
			rows, err := EvalSubquery(physicalPlan, er.b.is, er.b.ctx)
			if err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 172)
				er.err = err
				return v, true
			}
			trace_util_0.Count(_expression_rewriter_00000, 170)
			if (len(rows) > 0 && !v.Not) || (len(rows) == 0 && v.Not) {
				trace_util_0.Count(_expression_rewriter_00000, 173)
				er.ctxStack = append(er.ctxStack, expression.One.Clone())
			} else {
				trace_util_0.Count(_expression_rewriter_00000, 174)
				{
					er.ctxStack = append(er.ctxStack, expression.Zero.Clone())
				}
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 162)
	return v, true
}

// popExistsSubPlan will remove the useless plan in exist's child.
// See comments inside the method for more details.
func (er *expressionRewriter) popExistsSubPlan(p LogicalPlan) LogicalPlan {
	trace_util_0.Count(_expression_rewriter_00000, 175)
out:
	for {
		trace_util_0.Count(_expression_rewriter_00000, 177)
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *LogicalProjection, *LogicalSort:
			trace_util_0.Count(_expression_rewriter_00000, 178)
			p = p.Children()[0]
		case *LogicalAggregation:
			trace_util_0.Count(_expression_rewriter_00000, 179)
			if len(plan.GroupByItems) == 0 {
				trace_util_0.Count(_expression_rewriter_00000, 182)
				p = LogicalTableDual{RowCount: 1}.Init(er.ctx)
				break out
			}
			trace_util_0.Count(_expression_rewriter_00000, 180)
			p = p.Children()[0]
		default:
			trace_util_0.Count(_expression_rewriter_00000, 181)
			break out
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 176)
	return p
}

func (er *expressionRewriter) handleInSubquery(v *ast.PatternInExpr) (ast.Node, bool) {
	trace_util_0.Count(_expression_rewriter_00000, 183)
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 192)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 184)
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		trace_util_0.Count(_expression_rewriter_00000, 193)
		er.err = errors.Errorf("Unknown compare type %T.", v.Sel)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 185)
	np, err := er.buildSubquery(subq)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 194)
		er.err = err
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 186)
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		trace_util_0.Count(_expression_rewriter_00000, 195)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(lLen)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 187)
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		trace_util_0.Count(_expression_rewriter_00000, 196)
		rexpr = np.Schema().Columns[0]
		rCol := rexpr.(*expression.Column)
		// For AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, we cannot treat `in` expression as
		// normal column equal condition, so we specially mark the inner operand here.
		if v.Not || asScalar {
			trace_util_0.Count(_expression_rewriter_00000, 197)
			rCol.InOperand = true
			// If both input columns of `in` expression are not null, we can treat the expression
			// as normal column equal condition instead.
			lCol, ok := lexpr.(*expression.Column)
			if ok && mysql.HasNotNullFlag(lCol.GetType().Flag) && mysql.HasNotNullFlag(rCol.GetType().Flag) {
				trace_util_0.Count(_expression_rewriter_00000, 198)
				rCol.InOperand = false
			}
		}
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 199)
		{
			args := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().Columns {
				trace_util_0.Count(_expression_rewriter_00000, 201)
				args = append(args, col)
			}
			trace_util_0.Count(_expression_rewriter_00000, 200)
			rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
			if er.err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 202)
				return v, true
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 188)
	checkCondition, err := er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 203)
		er.err = err
		return v, true
	}
	// If it's not the form of `not in (SUBQUERY)`,
	// and has no correlated column from the current level plan(if the correlated column is from upper level,
	// we can treat it as constant, because the upper LogicalApply cannot be eliminated since current node is a join node),
	// and don't need to append a scalar value, we can rewrite it to inner join.
	trace_util_0.Count(_expression_rewriter_00000, 189)
	if er.ctx.GetSessionVars().AllowInSubqToJoinAndAgg && !v.Not && !asScalar && len(extractCorColumnsBySchema(np, er.p.Schema())) == 0 {
		trace_util_0.Count(_expression_rewriter_00000, 204)
		// We need to try to eliminate the agg and the projection produced by this operation.
		er.b.optFlag |= flagEliminateAgg
		er.b.optFlag |= flagEliminateProjection
		er.b.optFlag |= flagJoinReOrder
		// Build distinct for the inner query.
		agg, err := er.b.buildDistinct(np, np.Schema().Len())
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 208)
			er.err = err
			return v, true
		}
		trace_util_0.Count(_expression_rewriter_00000, 205)
		for _, col := range agg.schema.Columns {
			trace_util_0.Count(_expression_rewriter_00000, 209)
			col.IsReferenced = true
		}
		// Build inner join above the aggregation.
		trace_util_0.Count(_expression_rewriter_00000, 206)
		join := LogicalJoin{JoinType: InnerJoin}.Init(er.ctx)
		join.SetChildren(er.p, agg)
		join.SetSchema(expression.MergeSchema(er.p.Schema(), agg.schema))
		join.attachOnConds(expression.SplitCNFItems(checkCondition))
		// Set join hint for this join.
		if er.b.TableHints() != nil {
			trace_util_0.Count(_expression_rewriter_00000, 210)
			er.err = join.setPreferredJoinType(er.b.TableHints())
			if er.err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 211)
				return v, true
			}
		}
		trace_util_0.Count(_expression_rewriter_00000, 207)
		er.p = join
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 212)
		{
			er.p, er.err = er.b.buildSemiApply(er.p, np, expression.SplitCNFItems(checkCondition), asScalar, v.Not)
			if er.err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 213)
				return v, true
			}
		}
	}

	trace_util_0.Count(_expression_rewriter_00000, 190)
	if asScalar {
		trace_util_0.Count(_expression_rewriter_00000, 214)
		col := er.p.Schema().Columns[er.p.Schema().Len()-1]
		er.ctxStack[len(er.ctxStack)-1] = col
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 215)
		{
			er.ctxStack = er.ctxStack[:len(er.ctxStack)-1]
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 191)
	return v, true
}

func (er *expressionRewriter) handleScalarSubquery(v *ast.SubqueryExpr) (ast.Node, bool) {
	trace_util_0.Count(_expression_rewriter_00000, 216)
	np, err := er.buildSubquery(v)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 222)
		er.err = err
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 217)
	np = er.b.buildMaxOneRow(np)
	if len(np.extractCorrelatedCols()) > 0 {
		trace_util_0.Count(_expression_rewriter_00000, 223)
		er.p = er.b.buildApplyWithJoinType(er.p, np, LeftOuterJoin)
		if np.Schema().Len() > 1 {
			trace_util_0.Count(_expression_rewriter_00000, 225)
			newCols := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().Columns {
				trace_util_0.Count(_expression_rewriter_00000, 228)
				newCols = append(newCols, col)
			}
			trace_util_0.Count(_expression_rewriter_00000, 226)
			expr, err1 := er.newFunction(ast.RowFunc, newCols[0].GetType(), newCols...)
			if err1 != nil {
				trace_util_0.Count(_expression_rewriter_00000, 229)
				er.err = err1
				return v, true
			}
			trace_util_0.Count(_expression_rewriter_00000, 227)
			er.ctxStack = append(er.ctxStack, expr)
		} else {
			trace_util_0.Count(_expression_rewriter_00000, 230)
			{
				er.ctxStack = append(er.ctxStack, er.p.Schema().Columns[er.p.Schema().Len()-1])
			}
		}
		trace_util_0.Count(_expression_rewriter_00000, 224)
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 218)
	physicalPlan, err := DoOptimize(er.b.optFlag, np)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 231)
		er.err = err
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 219)
	rows, err := EvalSubquery(physicalPlan, er.b.is, er.b.ctx)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 232)
		er.err = err
		return v, true
	}
	trace_util_0.Count(_expression_rewriter_00000, 220)
	if np.Schema().Len() > 1 {
		trace_util_0.Count(_expression_rewriter_00000, 233)
		newCols := make([]expression.Expression, 0, np.Schema().Len())
		for i, data := range rows[0] {
			trace_util_0.Count(_expression_rewriter_00000, 236)
			newCols = append(newCols, &expression.Constant{
				Value:   data,
				RetType: np.Schema().Columns[i].GetType()})
		}
		trace_util_0.Count(_expression_rewriter_00000, 234)
		expr, err1 := er.newFunction(ast.RowFunc, newCols[0].GetType(), newCols...)
		if err1 != nil {
			trace_util_0.Count(_expression_rewriter_00000, 237)
			er.err = err1
			return v, true
		}
		trace_util_0.Count(_expression_rewriter_00000, 235)
		er.ctxStack = append(er.ctxStack, expr)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 238)
		{
			er.ctxStack = append(er.ctxStack, &expression.Constant{
				Value:   rows[0][0],
				RetType: np.Schema().Columns[0].GetType(),
			})
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 221)
	return v, true
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	trace_util_0.Count(_expression_rewriter_00000, 239)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 244)
		return retNode, false
	}
	trace_util_0.Count(_expression_rewriter_00000, 240)
	var inNode = originInNode
	if er.preprocess != nil {
		trace_util_0.Count(_expression_rewriter_00000, 245)
		inNode = er.preprocess(inNode)
	}
	trace_util_0.Count(_expression_rewriter_00000, 241)
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr:
		trace_util_0.Count(_expression_rewriter_00000, 246)
	case *driver.ValueExpr:
		trace_util_0.Count(_expression_rewriter_00000, 247)
		value := &expression.Constant{Value: v.Datum, RetType: &v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_expression_rewriter_00000, 248)
		var value expression.Expression
		value, er.err = expression.GetParamExpression(er.ctx, v)
		if er.err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 269)
			return retNode, false
		}
		trace_util_0.Count(_expression_rewriter_00000, 249)
		er.ctxStack = append(er.ctxStack, value)
	case *ast.VariableExpr:
		trace_util_0.Count(_expression_rewriter_00000, 250)
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		trace_util_0.Count(_expression_rewriter_00000, 251)
		er.funcCallToExpression(v)
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			trace_util_0.Count(_expression_rewriter_00000, 270)
			er.disableFoldCounter--
		}
	case *ast.ColumnName:
		trace_util_0.Count(_expression_rewriter_00000, 252)
		er.toColumn(v)
	case *ast.UnaryOperationExpr:
		trace_util_0.Count(_expression_rewriter_00000, 253)
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		trace_util_0.Count(_expression_rewriter_00000, 254)
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		trace_util_0.Count(_expression_rewriter_00000, 255)
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		trace_util_0.Count(_expression_rewriter_00000, 256)
		er.caseToExpression(v)
	case *ast.FuncCastExpr:
		trace_util_0.Count(_expression_rewriter_00000, 257)
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = expression.CheckArgsNotMultiColumnRow(arg)
		if er.err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 271)
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		trace_util_0.Count(_expression_rewriter_00000, 258)
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 272)
			return retNode, false
		}

		trace_util_0.Count(_expression_rewriter_00000, 259)
		er.ctxStack[len(er.ctxStack)-1] = expression.BuildCastFunction(er.ctx, arg, v.Tp)
	case *ast.PatternLikeExpr:
		trace_util_0.Count(_expression_rewriter_00000, 260)
		er.patternLikeToExpression(v)
	case *ast.PatternRegexpExpr:
		trace_util_0.Count(_expression_rewriter_00000, 261)
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		trace_util_0.Count(_expression_rewriter_00000, 262)
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		trace_util_0.Count(_expression_rewriter_00000, 263)
		if v.Sel == nil {
			trace_util_0.Count(_expression_rewriter_00000, 273)
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		trace_util_0.Count(_expression_rewriter_00000, 264)
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		trace_util_0.Count(_expression_rewriter_00000, 265)
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		trace_util_0.Count(_expression_rewriter_00000, 266)
		er.isTrueToScalarFunc(v)
	case *ast.DefaultExpr:
		trace_util_0.Count(_expression_rewriter_00000, 267)
		er.evalDefaultExpr(v)
	default:
		trace_util_0.Count(_expression_rewriter_00000, 268)
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	trace_util_0.Count(_expression_rewriter_00000, 242)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 274)
		return retNode, false
	}
	trace_util_0.Count(_expression_rewriter_00000, 243)
	return originInNode, true
}

// newFunction chooses which expression.NewFunctionImpl() will be used.
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (expression.Expression, error) {
	trace_util_0.Count(_expression_rewriter_00000, 275)
	if er.disableFoldCounter > 0 {
		trace_util_0.Count(_expression_rewriter_00000, 277)
		return expression.NewFunctionBase(er.ctx, funcName, retType, args...)
	}
	trace_util_0.Count(_expression_rewriter_00000, 276)
	return expression.NewFunction(er.ctx, funcName, retType, args...)
}

func (er *expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	trace_util_0.Count(_expression_rewriter_00000, 278)
	if ft.EvalType() == types.ETDuration && ft.Decimal > types.MaxFsp {
		trace_util_0.Count(_expression_rewriter_00000, 280)
		return errTooBigPrecision.GenWithStackByArgs(ft.Decimal, "CAST", types.MaxFsp)
	}
	trace_util_0.Count(_expression_rewriter_00000, 279)
	return nil
}

func (er *expressionRewriter) useCache() bool {
	trace_util_0.Count(_expression_rewriter_00000, 281)
	return er.ctx.GetSessionVars().StmtCtx.UseCache
}

func (er *expressionRewriter) rewriteVariable(v *ast.VariableExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 282)
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	sessionVars := er.b.ctx.GetSessionVars()
	if !v.IsSystem {
		trace_util_0.Count(_expression_rewriter_00000, 288)
		if v.Value != nil {
			trace_util_0.Count(_expression_rewriter_00000, 291)
			er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				expression.DatumToConstant(types.NewDatum(name), mysql.TypeString),
				er.ctxStack[stkLen-1])
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 289)
		f, err := er.newFunction(ast.GetVar,
			// TODO: Here is wrong, the sessionVars should store a name -> Datum map. Will fix it later.
			types.NewFieldType(mysql.TypeString),
			expression.DatumToConstant(types.NewStringDatum(name), mysql.TypeString))
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 292)
			er.err = err
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 290)
		er.ctxStack = append(er.ctxStack, f)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 283)
	var val string
	var err error
	if v.ExplicitScope {
		trace_util_0.Count(_expression_rewriter_00000, 293)
		err = variable.ValidateGetSystemVar(name, v.IsGlobal)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 294)
			er.err = err
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 284)
	sysVar := variable.SysVars[name]
	if sysVar == nil {
		trace_util_0.Count(_expression_rewriter_00000, 295)
		er.err = variable.UnknownSystemVar.GenWithStackByArgs(name)
		return
	}
	// Variable is @@gobal.variable_name or variable is only global scope variable.
	trace_util_0.Count(_expression_rewriter_00000, 285)
	if v.IsGlobal || sysVar.Scope == variable.ScopeGlobal {
		trace_util_0.Count(_expression_rewriter_00000, 296)
		val, err = variable.GetGlobalSystemVar(sessionVars, name)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 297)
		{
			val, err = variable.GetSessionSystemVar(sessionVars, name)
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 286)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 298)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 287)
	e := expression.DatumToConstant(types.NewStringDatum(val), mysql.TypeVarString)
	e.RetType.Charset, _ = er.ctx.GetSessionVars().GetSystemVar(variable.CharacterSetConnection)
	e.RetType.Collate, _ = er.ctx.GetSessionVars().GetSystemVar(variable.CollationConnection)
	er.ctxStack = append(er.ctxStack, e)
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 299)
	stkLen := len(er.ctxStack)
	var op string
	switch v.Op {
	case opcode.Plus:
		trace_util_0.Count(_expression_rewriter_00000, 302)
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		trace_util_0.Count(_expression_rewriter_00000, 303)
		op = ast.UnaryMinus
	case opcode.BitNeg:
		trace_util_0.Count(_expression_rewriter_00000, 304)
		op = ast.BitNeg
	case opcode.Not:
		trace_util_0.Count(_expression_rewriter_00000, 305)
		op = ast.UnaryNot
	default:
		trace_util_0.Count(_expression_rewriter_00000, 306)
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 300)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		trace_util_0.Count(_expression_rewriter_00000, 307)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 301)
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 308)
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		trace_util_0.Count(_expression_rewriter_00000, 311)
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		trace_util_0.Count(_expression_rewriter_00000, 312)
		lLen := expression.GetRowLen(er.ctxStack[stkLen-2])
		rLen := expression.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			trace_util_0.Count(_expression_rewriter_00000, 314)
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 313)
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	trace_util_0.Count(_expression_rewriter_00000, 309)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 315)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 310)
	er.ctxStack = er.ctxStack[:stkLen-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	trace_util_0.Count(_expression_rewriter_00000, 316)
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 320)
		er.err = err
		return nil
	}
	trace_util_0.Count(_expression_rewriter_00000, 317)
	if !hasNot {
		trace_util_0.Count(_expression_rewriter_00000, 321)
		return opFunc
	}

	trace_util_0.Count(_expression_rewriter_00000, 318)
	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 322)
		er.err = err
		return nil
	}
	trace_util_0.Count(_expression_rewriter_00000, 319)
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 323)
	stkLen := len(er.ctxStack)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		trace_util_0.Count(_expression_rewriter_00000, 325)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 324)
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 326)
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		trace_util_0.Count(_expression_rewriter_00000, 328)
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := expression.GetIntFromConstant(er.ctx, val)
		str = "?"
		if err == nil {
			trace_util_0.Count(_expression_rewriter_00000, 330)
			if isNull {
				trace_util_0.Count(_expression_rewriter_00000, 332)
				return
			}
			trace_util_0.Count(_expression_rewriter_00000, 331)
			pos = intNum
			er.ctxStack = er.ctxStack[:stkLen-1]
		}
		trace_util_0.Count(_expression_rewriter_00000, 329)
		er.err = err
	}
	trace_util_0.Count(_expression_rewriter_00000, 327)
	if er.err == nil && pos > 0 && pos <= er.schema.Len() {
		trace_util_0.Count(_expression_rewriter_00000, 333)
		er.ctxStack = append(er.ctxStack, er.schema.Columns[pos-1])
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 334)
		{
			er.err = ErrUnknownColumn.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
		}
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 335)
	stkLen := len(er.ctxStack)
	op := ast.IsTruth
	if v.True == 0 {
		trace_util_0.Count(_expression_rewriter_00000, 338)
		op = ast.IsFalsity
	}
	trace_util_0.Count(_expression_rewriter_00000, 336)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		trace_util_0.Count(_expression_rewriter_00000, 339)
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 337)
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	trace_util_0.Count(_expression_rewriter_00000, 340)
	stkLen := len(er.ctxStack)
	l := expression.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		trace_util_0.Count(_expression_rewriter_00000, 346)
		if l != expression.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			trace_util_0.Count(_expression_rewriter_00000, 347)
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 341)
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType()
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.Tp == mysql.TypeNull
	if leftIsNull {
		trace_util_0.Count(_expression_rewriter_00000, 348)
		er.ctxStack = er.ctxStack[:stkLen-lLen-1]
		er.ctxStack = append(er.ctxStack, expression.Null.Clone())
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 342)
	if leftEt == types.ETInt {
		trace_util_0.Count(_expression_rewriter_00000, 349)
		for i := 1; i < len(args); i++ {
			trace_util_0.Count(_expression_rewriter_00000, 350)
			if c, ok := args[i].(*expression.Constant); ok {
				trace_util_0.Count(_expression_rewriter_00000, 351)
				args[i], _ = expression.RefineComparedConstant(er.ctx, mysql.HasUnsignedFlag(leftFt.Flag), c, opcode.EQ)
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 343)
	allSameType := true
	for _, arg := range args[1:] {
		trace_util_0.Count(_expression_rewriter_00000, 352)
		if arg.GetType().Tp != mysql.TypeNull && expression.GetAccurateCmpType(args[0], arg) != leftEt {
			trace_util_0.Count(_expression_rewriter_00000, 353)
			allSameType = false
			break
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 344)
	var function expression.Expression
	if allSameType && l == 1 {
		trace_util_0.Count(_expression_rewriter_00000, 354)
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 355)
		{
			eqFunctions := make([]expression.Expression, 0, lLen)
			for i := stkLen - lLen; i < stkLen; i++ {
				trace_util_0.Count(_expression_rewriter_00000, 357)
				expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
				if err != nil {
					trace_util_0.Count(_expression_rewriter_00000, 359)
					er.err = err
					return
				}
				trace_util_0.Count(_expression_rewriter_00000, 358)
				eqFunctions = append(eqFunctions, expr)
			}
			trace_util_0.Count(_expression_rewriter_00000, 356)
			function = expression.ComposeDNFCondition(er.ctx, eqFunctions...)
			if not {
				trace_util_0.Count(_expression_rewriter_00000, 360)
				var err error
				function, err = er.newFunction(ast.UnaryNot, tp, function)
				if err != nil {
					trace_util_0.Count(_expression_rewriter_00000, 361)
					er.err = err
					return
				}
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 345)
	er.ctxStack = er.ctxStack[:stkLen-lLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 362)
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		trace_util_0.Count(_expression_rewriter_00000, 367)
		argsLen++
	}
	trace_util_0.Count(_expression_rewriter_00000, 363)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 368)
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	trace_util_0.Count(_expression_rewriter_00000, 364)
	var args []expression.Expression
	if v.Value != nil {
		trace_util_0.Count(_expression_rewriter_00000, 369)
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			trace_util_0.Count(_expression_rewriter_00000, 372)
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				trace_util_0.Count(_expression_rewriter_00000, 374)
				er.err = err
				return
			}
			trace_util_0.Count(_expression_rewriter_00000, 373)
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		trace_util_0.Count(_expression_rewriter_00000, 370)
		if v.ElseClause != nil {
			trace_util_0.Count(_expression_rewriter_00000, 375)
			args = append(args, er.ctxStack[stkLen-1])
		}
		trace_util_0.Count(_expression_rewriter_00000, 371)
		argsLen++ // for trimming the value element later
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 376)
		{
			// args:  condition1, result1,
			//        condition2, result2,
			//        ...
			//        else clause
			args = er.ctxStack[stkLen-argsLen:]
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 365)
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 377)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 366)
	er.ctxStack = er.ctxStack[:stkLen-argsLen]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) patternLikeToExpression(v *ast.PatternLikeExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 378)
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 382)
		return
	}

	trace_util_0.Count(_expression_rewriter_00000, 379)
	var function expression.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match.
	if patExpression, ok := er.ctxStack[l-1].(*expression.Constant); ok {
		trace_util_0.Count(_expression_rewriter_00000, 383)
		patString, isNull, err := patExpression.EvalString(nil, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 385)
			er.err = err
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 384)
		if !isNull {
			trace_util_0.Count(_expression_rewriter_00000, 386)
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) {
				trace_util_0.Count(_expression_rewriter_00000, 387)
				op := ast.EQ
				if v.Not {
					trace_util_0.Count(_expression_rewriter_00000, 389)
					op = ast.NE
				}
				trace_util_0.Count(_expression_rewriter_00000, 388)
				types.DefaultTypeForValue(string(patValue), fieldType)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&expression.Constant{Value: types.NewStringDatum(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 380)
	if !isPatternExactMatch {
		trace_util_0.Count(_expression_rewriter_00000, 390)
		types.DefaultTypeForValue(int(v.Escape), fieldType)
		function = er.notToExpression(v.Not, ast.Like, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: fieldType})
	}

	trace_util_0.Count(_expression_rewriter_00000, 381)
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 391)
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 393)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 392)
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 394)
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		trace_util_0.Count(_expression_rewriter_00000, 397)
		rows = append(rows, er.ctxStack[i])
	}
	trace_util_0.Count(_expression_rewriter_00000, 395)
	er.ctxStack = er.ctxStack[:stkLen-length]
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 398)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 396)
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 399)
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 406)
		return
	}

	trace_util_0.Count(_expression_rewriter_00000, 400)
	expr, lexp, rexp := er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]

	if expression.GetCmpTp4MinMax([]expression.Expression{expr, lexp, rexp}) == types.ETDatetime {
		trace_util_0.Count(_expression_rewriter_00000, 407)
		expr = expression.WrapWithCastAsTime(er.ctx, expr, types.NewFieldType(mysql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.ctx, lexp, types.NewFieldType(mysql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.ctx, rexp, types.NewFieldType(mysql.TypeDatetime))
	}

	trace_util_0.Count(_expression_rewriter_00000, 401)
	var op string
	var l, r expression.Expression
	l, er.err = er.newFunction(ast.GE, &v.Type, expr, lexp)
	if er.err == nil {
		trace_util_0.Count(_expression_rewriter_00000, 408)
		r, er.err = er.newFunction(ast.LE, &v.Type, expr, rexp)
	}
	trace_util_0.Count(_expression_rewriter_00000, 402)
	op = ast.LogicAnd
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 409)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 403)
	function, err := er.newFunction(op, &v.Type, l, r)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 410)
		er.err = err
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 404)
	if v.Not {
		trace_util_0.Count(_expression_rewriter_00000, 411)
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 412)
			er.err = err
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 405)
	er.ctxStack = er.ctxStack[:stkLen-3]
	er.ctxStack = append(er.ctxStack, function)
}

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *expressionRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	trace_util_0.Count(_expression_rewriter_00000, 413)
	switch v.FnName.L {
	// when column is not null, ifnull on such column is not necessary.
	case ast.Ifnull:
		trace_util_0.Count(_expression_rewriter_00000, 414)
		if len(v.Args) != 2 {
			trace_util_0.Count(_expression_rewriter_00000, 422)
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		trace_util_0.Count(_expression_rewriter_00000, 415)
		stackLen := len(er.ctxStack)
		arg1 := er.ctxStack[stackLen-2]
		col, isColumn := arg1.(*expression.Column)
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isColumn && mysql.HasNotNullFlag(col.RetType.Flag) {
			trace_util_0.Count(_expression_rewriter_00000, 423)
			newCol := col.Clone().(*expression.Column)
			newCol.IsReferenced = true
			er.ctxStack = er.ctxStack[:stackLen-len(v.Args)]
			er.ctxStack = append(er.ctxStack, newCol)
			return true
		}

		trace_util_0.Count(_expression_rewriter_00000, 416)
		return false
	case ast.Nullif:
		trace_util_0.Count(_expression_rewriter_00000, 417)
		if len(v.Args) != 2 {
			trace_util_0.Count(_expression_rewriter_00000, 424)
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		trace_util_0.Count(_expression_rewriter_00000, 418)
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 425)
			er.err = err
			return true
		}
		// NULL
		trace_util_0.Count(_expression_rewriter_00000, 419)
		nullTp := types.NewFieldType(mysql.TypeNull)
		nullTp.Flen, nullTp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		paramNull := &expression.Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 426)
			er.err = err
			return true
		}
		trace_util_0.Count(_expression_rewriter_00000, 420)
		er.ctxStack = er.ctxStack[:stackLen-len(v.Args)]
		er.ctxStack = append(er.ctxStack, funcIf)
		return true
	default:
		trace_util_0.Count(_expression_rewriter_00000, 421)
		return false
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 427)
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiColumnRow(args...)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 430)
		return
	}

	trace_util_0.Count(_expression_rewriter_00000, 428)
	if er.rewriteFuncCall(v) {
		trace_util_0.Count(_expression_rewriter_00000, 431)
		return
	}

	trace_util_0.Count(_expression_rewriter_00000, 429)
	var function expression.Expression
	er.ctxStack = er.ctxStack[:stackLen-len(v.Args)]
	if _, ok := expression.DeferredFunctions[v.FnName.L]; er.useCache() && ok {
		trace_util_0.Count(_expression_rewriter_00000, 432)
		function, er.err = expression.NewFunctionBase(er.ctx, v.FnName.L, &v.Type, args...)
		c := &expression.Constant{Value: types.NewDatum(nil), RetType: function.GetType().Clone(), DeferredExpr: function}
		er.ctxStack = append(er.ctxStack, c)
	} else {
		trace_util_0.Count(_expression_rewriter_00000, 433)
		{
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStack = append(er.ctxStack, function)
		}
	}
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	trace_util_0.Count(_expression_rewriter_00000, 434)
	column, err := er.schema.FindColumn(v)
	if err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 441)
		er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 435)
	if column != nil {
		trace_util_0.Count(_expression_rewriter_00000, 442)
		er.ctxStack = append(er.ctxStack, column)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 436)
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		trace_util_0.Count(_expression_rewriter_00000, 443)
		outerSchema := er.b.outerSchemas[i]
		column, err = outerSchema.FindColumn(v)
		if column != nil {
			trace_util_0.Count(_expression_rewriter_00000, 445)
			er.ctxStack = append(er.ctxStack, &expression.CorrelatedColumn{Column: *column, Data: new(types.Datum)})
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 444)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 446)
			er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 437)
	if join, ok := er.p.(*LogicalJoin); ok && join.redundantSchema != nil {
		trace_util_0.Count(_expression_rewriter_00000, 447)
		column, err := join.redundantSchema.FindColumn(v)
		if err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 449)
			er.err = err
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 448)
		if column != nil {
			trace_util_0.Count(_expression_rewriter_00000, 450)
			er.ctxStack = append(er.ctxStack, column)
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 438)
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Table.O != "" {
		trace_util_0.Count(_expression_rewriter_00000, 451)
		er.err = ErrTablenameNotAllowedHere.GenWithStackByArgs(v.Table.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 439)
	if er.b.curClause == globalOrderByClause {
		trace_util_0.Count(_expression_rewriter_00000, 452)
		er.b.curClause = orderByClause
	}
	trace_util_0.Count(_expression_rewriter_00000, 440)
	er.err = ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func (er *expressionRewriter) evalDefaultExpr(v *ast.DefaultExpr) {
	trace_util_0.Count(_expression_rewriter_00000, 453)
	stkLen := len(er.ctxStack)
	var colExpr *expression.Column
	switch c := er.ctxStack[stkLen-1].(type) {
	case *expression.Column:
		trace_util_0.Count(_expression_rewriter_00000, 462)
		colExpr = c
	case *expression.CorrelatedColumn:
		trace_util_0.Count(_expression_rewriter_00000, 463)
		colExpr = &c.Column
	default:
		trace_util_0.Count(_expression_rewriter_00000, 464)
		colExpr, er.err = er.schema.FindColumn(v.Name)
		if er.err != nil {
			trace_util_0.Count(_expression_rewriter_00000, 466)
			return
		}
		trace_util_0.Count(_expression_rewriter_00000, 465)
		if colExpr == nil {
			trace_util_0.Count(_expression_rewriter_00000, 467)
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), "field_list")
			return
		}
	}
	trace_util_0.Count(_expression_rewriter_00000, 454)
	dbName := colExpr.DBName
	if dbName.O == "" {
		trace_util_0.Count(_expression_rewriter_00000, 468)
		// if database name is not specified, use current database name
		dbName = model.NewCIStr(er.ctx.GetSessionVars().CurrentDB)
	}
	trace_util_0.Count(_expression_rewriter_00000, 455)
	if colExpr.OrigTblName.O == "" {
		trace_util_0.Count(_expression_rewriter_00000, 469)
		// column is evaluated by some expressions, for example:
		// `select default(c) from (select (a+1) as c from t) as t0`
		// in such case, a 'no default' error is returned
		er.err = table.ErrNoDefaultValue.GenWithStackByArgs(colExpr.ColName)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 456)
	var tbl table.Table
	tbl, er.err = er.b.is.TableByName(dbName, colExpr.OrigTblName)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 470)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 457)
	colName := colExpr.OrigColName.O
	if colName == "" {
		trace_util_0.Count(_expression_rewriter_00000, 471)
		// in some cases, OrigColName is empty, use ColName instead
		colName = colExpr.ColName.O
	}
	trace_util_0.Count(_expression_rewriter_00000, 458)
	col := table.FindCol(tbl.Cols(), colName)
	if col == nil {
		trace_util_0.Count(_expression_rewriter_00000, 472)
		er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name, "field_list")
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 459)
	isCurrentTimestamp := hasCurrentDatetimeDefault(col)
	var val *expression.Constant
	switch {
	case isCurrentTimestamp && col.Tp == mysql.TypeDatetime:
		trace_util_0.Count(_expression_rewriter_00000, 473)
		// for DATETIME column with current_timestamp, use NULL to be compatible with MySQL 5.7
		val = expression.Null
	case isCurrentTimestamp && col.Tp == mysql.TypeTimestamp:
		trace_util_0.Count(_expression_rewriter_00000, 474)
		// for TIMESTAMP column with current_timestamp, use 0 to be compatible with MySQL 5.7
		zero := types.Time{
			Time: types.ZeroTime,
			Type: mysql.TypeTimestamp,
			Fsp:  col.Decimal,
		}
		val = &expression.Constant{
			Value:   types.NewDatum(zero),
			RetType: types.NewFieldType(mysql.TypeTimestamp),
		}
	default:
		trace_util_0.Count(_expression_rewriter_00000, 475)
		// for other columns, just use what it is
		val, er.err = er.b.getDefaultValue(col)
	}
	trace_util_0.Count(_expression_rewriter_00000, 460)
	if er.err != nil {
		trace_util_0.Count(_expression_rewriter_00000, 476)
		return
	}
	trace_util_0.Count(_expression_rewriter_00000, 461)
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, val)
}

// hasCurrentDatetimeDefault checks if column has current_timestamp default value
func hasCurrentDatetimeDefault(col *table.Column) bool {
	trace_util_0.Count(_expression_rewriter_00000, 477)
	x, ok := col.DefaultValue.(string)
	if !ok {
		trace_util_0.Count(_expression_rewriter_00000, 479)
		return false
	}
	trace_util_0.Count(_expression_rewriter_00000, 478)
	return strings.ToLower(x) == ast.CurrentTimestamp
}

var _expression_rewriter_00000 = "planner/core/expression_rewriter.go"
