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

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
)

type simpleRewriter struct {
	exprStack

	schema *Schema
	err    error
	ctx    sessionctx.Context
}

// ParseSimpleExprWithTableInfo parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExprWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 0)
	exprStr = "select " + exprStr
	stmts, warns, err := parser.New().Parse(exprStr, "", "")
	for _, warn := range warns {
		trace_util_0.Count(_simple_rewriter_00000, 3)
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	trace_util_0.Count(_simple_rewriter_00000, 1)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 4)
		return nil, util.SyntaxError(err)
	}
	trace_util_0.Count(_simple_rewriter_00000, 2)
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return RewriteSimpleExprWithTableInfo(ctx, tableInfo, expr)
}

// ParseSimpleExprCastWithTableInfo parses simple expression string to Expression.
// And the expr returns will cast to the target type.
func ParseSimpleExprCastWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo, targetFt *types.FieldType) (Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 5)
	e, err := ParseSimpleExprWithTableInfo(ctx, exprStr, tableInfo)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 7)
		return nil, err
	}
	trace_util_0.Count(_simple_rewriter_00000, 6)
	e = BuildCastFunction(ctx, e, targetFt)
	return e, nil
}

// RewriteSimpleExprWithTableInfo rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithTableInfo(ctx sessionctx.Context, tbl *model.TableInfo, expr ast.ExprNode) (Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 8)
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns := ColumnInfos2ColumnsWithDBName(ctx, dbName, tbl.Name, tbl.Columns)
	rewriter := &simpleRewriter{ctx: ctx, schema: NewSchema(columns...)}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 10)
		return nil, rewriter.err
	}
	trace_util_0.Count(_simple_rewriter_00000, 9)
	return rewriter.pop(), nil
}

// ParseSimpleExprsWithSchema parses simple expression string to Expression.
// The expression string must only reference the column in the given schema.
func ParseSimpleExprsWithSchema(ctx sessionctx.Context, exprStr string, schema *Schema) ([]Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 11)
	exprStr = "select " + exprStr
	stmts, warns, err := parser.New().Parse(exprStr, "", "")
	for _, warn := range warns {
		trace_util_0.Count(_simple_rewriter_00000, 15)
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	trace_util_0.Count(_simple_rewriter_00000, 12)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 16)
		return nil, util.SyntaxWarn(err)
	}
	trace_util_0.Count(_simple_rewriter_00000, 13)
	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		trace_util_0.Count(_simple_rewriter_00000, 17)
		expr, err := RewriteSimpleExprWithSchema(ctx, field.Expr, schema)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 19)
			return nil, err
		}
		trace_util_0.Count(_simple_rewriter_00000, 18)
		exprs = append(exprs, expr)
	}
	trace_util_0.Count(_simple_rewriter_00000, 14)
	return exprs, nil
}

// RewriteSimpleExprWithSchema rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithSchema(ctx sessionctx.Context, expr ast.ExprNode, schema *Schema) (Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 20)
	rewriter := &simpleRewriter{ctx: ctx, schema: schema}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 22)
		return nil, rewriter.err
	}
	trace_util_0.Count(_simple_rewriter_00000, 21)
	return rewriter.pop(), nil
}

func (sr *simpleRewriter) rewriteColumn(nodeColName *ast.ColumnNameExpr) (*Column, error) {
	trace_util_0.Count(_simple_rewriter_00000, 23)
	col, err := sr.schema.FindColumn(nodeColName.Name)
	if col != nil && err == nil {
		trace_util_0.Count(_simple_rewriter_00000, 25)
		return col, nil
	}
	trace_util_0.Count(_simple_rewriter_00000, 24)
	return nil, errBadField.GenWithStackByArgs(nodeColName.Name.Name.O, "expression")
}

func (sr *simpleRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_simple_rewriter_00000, 26)
	return inNode, false
}

func (sr *simpleRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	trace_util_0.Count(_simple_rewriter_00000, 27)
	switch v := originInNode.(type) {
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_simple_rewriter_00000, 30)
		column, err := sr.rewriteColumn(v)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 50)
			sr.err = err
			return originInNode, false
		}
		trace_util_0.Count(_simple_rewriter_00000, 31)
		sr.push(column)
	case *driver.ValueExpr:
		trace_util_0.Count(_simple_rewriter_00000, 32)
		value := &Constant{Value: v.Datum, RetType: &v.Type}
		sr.push(value)
	case *ast.FuncCallExpr:
		trace_util_0.Count(_simple_rewriter_00000, 33)
		sr.funcCallToExpression(v)
	case *ast.FuncCastExpr:
		trace_util_0.Count(_simple_rewriter_00000, 34)
		arg := sr.pop()
		sr.err = CheckArgsNotMultiColumnRow(arg)
		if sr.err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 51)
			return retNode, false
		}
		trace_util_0.Count(_simple_rewriter_00000, 35)
		sr.push(BuildCastFunction(sr.ctx, arg, v.Tp))
	case *ast.BinaryOperationExpr:
		trace_util_0.Count(_simple_rewriter_00000, 36)
		sr.binaryOpToExpression(v)
	case *ast.UnaryOperationExpr:
		trace_util_0.Count(_simple_rewriter_00000, 37)
		sr.unaryOpToExpression(v)
	case *ast.BetweenExpr:
		trace_util_0.Count(_simple_rewriter_00000, 38)
		sr.betweenToExpression(v)
	case *ast.IsNullExpr:
		trace_util_0.Count(_simple_rewriter_00000, 39)
		sr.isNullToExpression(v)
	case *ast.IsTruthExpr:
		trace_util_0.Count(_simple_rewriter_00000, 40)
		sr.isTrueToScalarFunc(v)
	case *ast.PatternLikeExpr:
		trace_util_0.Count(_simple_rewriter_00000, 41)
		sr.likeToScalarFunc(v)
	case *ast.PatternRegexpExpr:
		trace_util_0.Count(_simple_rewriter_00000, 42)
		sr.regexpToScalarFunc(v)
	case *ast.PatternInExpr:
		trace_util_0.Count(_simple_rewriter_00000, 43)
		if v.Sel == nil {
			trace_util_0.Count(_simple_rewriter_00000, 52)
			sr.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *driver.ParamMarkerExpr:
		trace_util_0.Count(_simple_rewriter_00000, 44)
		var value Expression
		value, sr.err = GetParamExpression(sr.ctx, v)
		if sr.err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 53)
			return retNode, false
		}
		trace_util_0.Count(_simple_rewriter_00000, 45)
		sr.push(value)
	case *ast.RowExpr:
		trace_util_0.Count(_simple_rewriter_00000, 46)
		sr.rowToScalarFunc(v)
	case *ast.ParenthesesExpr:
		trace_util_0.Count(_simple_rewriter_00000, 47)
	case *ast.ColumnName:
		trace_util_0.Count(_simple_rewriter_00000, 48)
	default:
		trace_util_0.Count(_simple_rewriter_00000, 49)
		sr.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	trace_util_0.Count(_simple_rewriter_00000, 28)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 54)
		return retNode, false
	}
	trace_util_0.Count(_simple_rewriter_00000, 29)
	return originInNode, true
}

func (sr *simpleRewriter) useCache() bool {
	trace_util_0.Count(_simple_rewriter_00000, 55)
	return sr.ctx.GetSessionVars().StmtCtx.UseCache
}

func (sr *simpleRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 56)
	right := sr.pop()
	left := sr.pop()
	var function Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		trace_util_0.Count(_simple_rewriter_00000, 59)
		function, sr.err = sr.constructBinaryOpFunction(left, right,
			v.Op.String())
	default:
		trace_util_0.Count(_simple_rewriter_00000, 60)
		lLen := GetRowLen(left)
		rLen := GetRowLen(right)
		if lLen != 1 || rLen != 1 {
			trace_util_0.Count(_simple_rewriter_00000, 62)
			sr.err = ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		trace_util_0.Count(_simple_rewriter_00000, 61)
		function, sr.err = NewFunction(sr.ctx, v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), left, right)
	}
	trace_util_0.Count(_simple_rewriter_00000, 57)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 63)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 58)
	sr.push(function)
}

func (sr *simpleRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 64)
	args := sr.popN(len(v.Args))
	sr.err = CheckArgsNotMultiColumnRow(args...)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 67)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 65)
	if sr.rewriteFuncCall(v) {
		trace_util_0.Count(_simple_rewriter_00000, 68)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 66)
	var function Expression
	function, sr.err = NewFunction(sr.ctx, v.FnName.L, &v.Type, args...)
	sr.push(function)
}

func (sr *simpleRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	trace_util_0.Count(_simple_rewriter_00000, 69)
	switch v.FnName.L {
	case ast.Nullif:
		trace_util_0.Count(_simple_rewriter_00000, 70)
		if len(v.Args) != 2 {
			trace_util_0.Count(_simple_rewriter_00000, 75)
			sr.err = ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		trace_util_0.Count(_simple_rewriter_00000, 71)
		param2 := sr.pop()
		param1 := sr.pop()
		// param1 = param2
		funcCompare, err := sr.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 76)
			sr.err = err
			return true
		}
		// NULL
		trace_util_0.Count(_simple_rewriter_00000, 72)
		nullTp := types.NewFieldType(mysql.TypeNull)
		nullTp.Flen, nullTp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		paramNull := &Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := NewFunction(sr.ctx, ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 77)
			sr.err = err
			return true
		}
		trace_util_0.Count(_simple_rewriter_00000, 73)
		sr.push(funcIf)
		return true
	default:
		trace_util_0.Count(_simple_rewriter_00000, 74)
		return false
	}
}

// constructBinaryOpFunction works as following:
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. If op are LE or GE, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( (a0 op b0) EQ 0, 0,
//      IF ( (a1 op b1) EQ 0, 0, a2 op b2))`
// 3. If op are LT or GT, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
//      IF( a1 NE b1,
//          a1 op b1,
//          a2 op b2)
// )`
func (sr *simpleRewriter) constructBinaryOpFunction(l Expression, r Expression, op string) (Expression, error) {
	trace_util_0.Count(_simple_rewriter_00000, 78)
	lLen, rLen := GetRowLen(l), GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		trace_util_0.Count(_simple_rewriter_00000, 80)
		return NewFunction(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else {
		trace_util_0.Count(_simple_rewriter_00000, 81)
		if rLen != lLen {
			trace_util_0.Count(_simple_rewriter_00000, 82)
			return nil, ErrOperandColumns.GenWithStackByArgs(lLen)
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 79)
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		trace_util_0.Count(_simple_rewriter_00000, 83)
		funcs := make([]Expression, lLen)
		for i := 0; i < lLen; i++ {
			trace_util_0.Count(_simple_rewriter_00000, 91)
			var err error
			funcs[i], err = sr.constructBinaryOpFunction(GetFuncArg(l, i), GetFuncArg(r, i), op)
			if err != nil {
				trace_util_0.Count(_simple_rewriter_00000, 92)
				return nil, err
			}
		}
		trace_util_0.Count(_simple_rewriter_00000, 84)
		if op == ast.NE {
			trace_util_0.Count(_simple_rewriter_00000, 93)
			return ComposeDNFCondition(sr.ctx, funcs...), nil
		}
		trace_util_0.Count(_simple_rewriter_00000, 85)
		return ComposeCNFCondition(sr.ctx, funcs...), nil
	default:
		trace_util_0.Count(_simple_rewriter_00000, 86)
		larg0, rarg0 := GetFuncArg(l, 0), GetFuncArg(r, 0)
		var expr1, expr2, expr3 Expression
		if op == ast.LE || op == ast.GE {
			trace_util_0.Count(_simple_rewriter_00000, 94)
			expr1 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr1 = NewFunctionInternal(sr.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), expr1, Zero)
			expr2 = Zero
		} else {
			trace_util_0.Count(_simple_rewriter_00000, 95)
			if op == ast.LT || op == ast.GT {
				trace_util_0.Count(_simple_rewriter_00000, 96)
				expr1 = NewFunctionInternal(sr.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
				expr2 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			}
		}
		trace_util_0.Count(_simple_rewriter_00000, 87)
		var err error
		l, err = PopRowFirstArg(sr.ctx, l)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 97)
			return nil, err
		}
		trace_util_0.Count(_simple_rewriter_00000, 88)
		r, err = PopRowFirstArg(sr.ctx, r)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 98)
			return nil, err
		}
		trace_util_0.Count(_simple_rewriter_00000, 89)
		expr3, err = sr.constructBinaryOpFunction(l, r, op)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 99)
			return nil, err
		}
		trace_util_0.Count(_simple_rewriter_00000, 90)
		return NewFunction(sr.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr3)
	}
}

func (sr *simpleRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 100)
	var op string
	switch v.Op {
	case opcode.Plus:
		trace_util_0.Count(_simple_rewriter_00000, 103)
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		trace_util_0.Count(_simple_rewriter_00000, 104)
		op = ast.UnaryMinus
	case opcode.BitNeg:
		trace_util_0.Count(_simple_rewriter_00000, 105)
		op = ast.BitNeg
	case opcode.Not:
		trace_util_0.Count(_simple_rewriter_00000, 106)
		op = ast.UnaryNot
	default:
		trace_util_0.Count(_simple_rewriter_00000, 107)
		sr.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 101)
	expr := sr.pop()
	if GetRowLen(expr) != 1 {
		trace_util_0.Count(_simple_rewriter_00000, 108)
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 102)
	newExpr, err := NewFunction(sr.ctx, op, &v.Type, expr)
	sr.err = err
	sr.push(newExpr)
}

func (sr *simpleRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 109)
	pattern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, pattern)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 111)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 110)
	escapeTp := &types.FieldType{}
	types.DefaultTypeForValue(int(v.Escape), escapeTp)
	function := sr.notToExpression(v.Not, ast.Like, &v.Type,
		expr, pattern, &Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: escapeTp})
	sr.push(function)
}

func (sr *simpleRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 112)
	parttern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, parttern)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 114)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 113)
	function := sr.notToExpression(v.Not, ast.Regexp, &v.Type, expr, parttern)
	sr.push(function)
}

func (sr *simpleRewriter) rowToScalarFunc(v *ast.RowExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 115)
	elems := sr.popN(len(v.Values))
	function, err := NewFunction(sr.ctx, ast.RowFunc, elems[0].GetType(), elems...)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 117)
		sr.err = err
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 116)
	sr.push(function)
}

func (sr *simpleRewriter) betweenToExpression(v *ast.BetweenExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 118)
	right := sr.pop()
	left := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiColumnRow(expr)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 124)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 119)
	var l, r Expression
	l, sr.err = NewFunction(sr.ctx, ast.GE, &v.Type, expr, left)
	if sr.err == nil {
		trace_util_0.Count(_simple_rewriter_00000, 125)
		r, sr.err = NewFunction(sr.ctx, ast.LE, &v.Type, expr, right)
	}
	trace_util_0.Count(_simple_rewriter_00000, 120)
	if sr.err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 126)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 121)
	function, err := NewFunction(sr.ctx, ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 127)
		sr.err = err
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 122)
	if v.Not {
		trace_util_0.Count(_simple_rewriter_00000, 128)
		function, err = NewFunction(sr.ctx, ast.UnaryNot, &v.Type, function)
		if err != nil {
			trace_util_0.Count(_simple_rewriter_00000, 129)
			sr.err = err
			return
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 123)
	sr.push(function)
}

func (sr *simpleRewriter) isNullToExpression(v *ast.IsNullExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 130)
	arg := sr.pop()
	if GetRowLen(arg) != 1 {
		trace_util_0.Count(_simple_rewriter_00000, 132)
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 131)
	function := sr.notToExpression(v.Not, ast.IsNull, &v.Type, arg)
	sr.push(function)
}

func (sr *simpleRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...Expression) Expression {
	trace_util_0.Count(_simple_rewriter_00000, 133)
	opFunc, err := NewFunction(sr.ctx, op, tp, args...)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 137)
		sr.err = err
		return nil
	}
	trace_util_0.Count(_simple_rewriter_00000, 134)
	if !hasNot {
		trace_util_0.Count(_simple_rewriter_00000, 138)
		return opFunc
	}

	trace_util_0.Count(_simple_rewriter_00000, 135)
	opFunc, err = NewFunction(sr.ctx, ast.UnaryNot, tp, opFunc)
	if err != nil {
		trace_util_0.Count(_simple_rewriter_00000, 139)
		sr.err = err
		return nil
	}
	trace_util_0.Count(_simple_rewriter_00000, 136)
	return opFunc
}

func (sr *simpleRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	trace_util_0.Count(_simple_rewriter_00000, 140)
	arg := sr.pop()
	op := ast.IsTruth
	if v.True == 0 {
		trace_util_0.Count(_simple_rewriter_00000, 143)
		op = ast.IsFalsity
	}
	trace_util_0.Count(_simple_rewriter_00000, 141)
	if GetRowLen(arg) != 1 {
		trace_util_0.Count(_simple_rewriter_00000, 144)
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 142)
	function := sr.notToExpression(v.Not, op, &v.Type, arg)
	sr.push(function)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (sr *simpleRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	trace_util_0.Count(_simple_rewriter_00000, 145)
	exprs := sr.popN(lLen + 1)
	leftExpr := exprs[0]
	elems := exprs[1:]
	l, leftFt := GetRowLen(leftExpr), leftExpr.GetType()
	for i := 0; i < lLen; i++ {
		trace_util_0.Count(_simple_rewriter_00000, 151)
		if l != GetRowLen(elems[i]) {
			trace_util_0.Count(_simple_rewriter_00000, 152)
			sr.err = ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 146)
	leftIsNull := leftFt.Tp == mysql.TypeNull
	if leftIsNull {
		trace_util_0.Count(_simple_rewriter_00000, 153)
		sr.push(Null.Clone())
		return
	}
	trace_util_0.Count(_simple_rewriter_00000, 147)
	leftEt := leftFt.EvalType()
	if leftEt == types.ETInt {
		trace_util_0.Count(_simple_rewriter_00000, 154)
		for i := 0; i < len(elems); i++ {
			trace_util_0.Count(_simple_rewriter_00000, 155)
			if c, ok := elems[i].(*Constant); ok {
				trace_util_0.Count(_simple_rewriter_00000, 156)
				elems[i], _ = RefineComparedConstant(sr.ctx, mysql.HasUnsignedFlag(leftFt.Flag), c, opcode.EQ)
			}
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 148)
	allSameType := true
	for _, elem := range elems {
		trace_util_0.Count(_simple_rewriter_00000, 157)
		if elem.GetType().Tp != mysql.TypeNull && GetAccurateCmpType(leftExpr, elem) != leftEt {
			trace_util_0.Count(_simple_rewriter_00000, 158)
			allSameType = false
			break
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 149)
	var function Expression
	if allSameType && l == 1 {
		trace_util_0.Count(_simple_rewriter_00000, 159)
		function = sr.notToExpression(not, ast.In, tp, exprs...)
	} else {
		trace_util_0.Count(_simple_rewriter_00000, 160)
		{
			eqFunctions := make([]Expression, 0, lLen)
			for i := 0; i < len(elems); i++ {
				trace_util_0.Count(_simple_rewriter_00000, 162)
				expr, err := sr.constructBinaryOpFunction(leftExpr, elems[i], ast.EQ)
				if err != nil {
					trace_util_0.Count(_simple_rewriter_00000, 164)
					sr.err = err
					return
				}
				trace_util_0.Count(_simple_rewriter_00000, 163)
				eqFunctions = append(eqFunctions, expr)
			}
			trace_util_0.Count(_simple_rewriter_00000, 161)
			function = ComposeDNFCondition(sr.ctx, eqFunctions...)
			if not {
				trace_util_0.Count(_simple_rewriter_00000, 165)
				var err error
				function, err = NewFunction(sr.ctx, ast.UnaryNot, tp, function)
				if err != nil {
					trace_util_0.Count(_simple_rewriter_00000, 166)
					sr.err = err
					return
				}
			}
		}
	}
	trace_util_0.Count(_simple_rewriter_00000, 150)
	sr.push(function)
}

var _simple_rewriter_00000 = "expression/simple_rewriter.go"
