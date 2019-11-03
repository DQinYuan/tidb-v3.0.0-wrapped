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

package expression

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/tools/container/intsets"
)

// Filter the input expressions, append the results to result.
func Filter(result []Expression, input []Expression, filter func(Expression) bool) []Expression {
	trace_util_0.Count(_util_00000, 0)
	for _, e := range input {
		trace_util_0.Count(_util_00000, 2)
		if filter(e) {
			trace_util_0.Count(_util_00000, 3)
			result = append(result, e)
		}
	}
	trace_util_0.Count(_util_00000, 1)
	return result
}

// FilterOutInPlace do the filtering out in place.
// The remained are the ones who doesn't match the filter, storing in the original slice.
// The filteredOut are the ones match the filter, storing in a new slice.
func FilterOutInPlace(input []Expression, filter func(Expression) bool) (remained, filteredOut []Expression) {
	trace_util_0.Count(_util_00000, 4)
	for i := len(input) - 1; i >= 0; i-- {
		trace_util_0.Count(_util_00000, 6)
		if filter(input[i]) {
			trace_util_0.Count(_util_00000, 7)
			filteredOut = append(filteredOut, input[i])
			input = append(input[:i], input[i+1:]...)
		}
	}
	trace_util_0.Count(_util_00000, 5)
	return input, filteredOut
}

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) (cols []*Column) {
	trace_util_0.Count(_util_00000, 8)
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*Column, 0, 8)
	return extractColumns(result, expr, nil)
}

// ExtractCorColumns extracts correlated column from given expression.
func ExtractCorColumns(expr Expression) (cols []*CorrelatedColumn) {
	trace_util_0.Count(_util_00000, 9)
	switch v := expr.(type) {
	case *CorrelatedColumn:
		trace_util_0.Count(_util_00000, 11)
		return []*CorrelatedColumn{v}
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 12)
		for _, arg := range v.GetArgs() {
			trace_util_0.Count(_util_00000, 13)
			cols = append(cols, ExtractCorColumns(arg)...)
		}
	}
	trace_util_0.Count(_util_00000, 10)
	return
}

// ExtractColumnsFromExpressions is a more efficient version of ExtractColumns for batch operation.
// filter can be nil, or a function to filter the result column.
// It's often observed that the pattern of the caller like this:
//
// cols := ExtractColumns(...)
// for _, col := range cols {
//     if xxx(col) {...}
// }
//
// Provide an additional filter argument, this can be done in one step.
// To avoid allocation for cols that not need.
func ExtractColumnsFromExpressions(result []*Column, exprs []Expression, filter func(*Column) bool) []*Column {
	trace_util_0.Count(_util_00000, 14)
	for _, expr := range exprs {
		trace_util_0.Count(_util_00000, 16)
		result = extractColumns(result, expr, filter)
	}
	trace_util_0.Count(_util_00000, 15)
	return result
}

func extractColumns(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	trace_util_0.Count(_util_00000, 17)
	switch v := expr.(type) {
	case *Column:
		trace_util_0.Count(_util_00000, 19)
		if filter == nil || filter(v) {
			trace_util_0.Count(_util_00000, 21)
			result = append(result, v)
		}
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 20)
		for _, arg := range v.GetArgs() {
			trace_util_0.Count(_util_00000, 22)
			result = extractColumns(result, arg, filter)
		}
	}
	trace_util_0.Count(_util_00000, 18)
	return result
}

// ExtractColumnSet extracts the different values of `UniqueId` for columns in expressions.
func ExtractColumnSet(exprs []Expression) *intsets.Sparse {
	trace_util_0.Count(_util_00000, 23)
	set := &intsets.Sparse{}
	for _, expr := range exprs {
		trace_util_0.Count(_util_00000, 25)
		extractColumnSet(expr, set)
	}
	trace_util_0.Count(_util_00000, 24)
	return set
}

func extractColumnSet(expr Expression, set *intsets.Sparse) {
	trace_util_0.Count(_util_00000, 26)
	switch v := expr.(type) {
	case *Column:
		trace_util_0.Count(_util_00000, 27)
		set.Insert(int(v.UniqueID))
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 28)
		for _, arg := range v.GetArgs() {
			trace_util_0.Count(_util_00000, 29)
			extractColumnSet(arg, set)
		}
	}
}

func setExprColumnInOperand(expr Expression) Expression {
	trace_util_0.Count(_util_00000, 30)
	switch v := expr.(type) {
	case *Column:
		trace_util_0.Count(_util_00000, 32)
		col := v.Clone().(*Column)
		col.InOperand = true
		return col
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 33)
		args := v.GetArgs()
		for i, arg := range args {
			trace_util_0.Count(_util_00000, 34)
			args[i] = setExprColumnInOperand(arg)
		}
	}
	trace_util_0.Count(_util_00000, 31)
	return expr
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema *Schema, newExprs []Expression) Expression {
	trace_util_0.Count(_util_00000, 35)
	switch v := expr.(type) {
	case *Column:
		trace_util_0.Count(_util_00000, 37)
		id := schema.ColumnIndex(v)
		if id == -1 {
			trace_util_0.Count(_util_00000, 43)
			return v
		}
		trace_util_0.Count(_util_00000, 38)
		newExpr := newExprs[id]
		if v.InOperand {
			trace_util_0.Count(_util_00000, 44)
			newExpr = setExprColumnInOperand(newExpr)
		}
		trace_util_0.Count(_util_00000, 39)
		return newExpr
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 40)
		if v.FuncName.L == ast.Cast {
			trace_util_0.Count(_util_00000, 45)
			newFunc := v.Clone().(*ScalarFunction)
			newFunc.GetArgs()[0] = ColumnSubstitute(newFunc.GetArgs()[0], schema, newExprs)
			return newFunc
		}
		trace_util_0.Count(_util_00000, 41)
		newArgs := make([]Expression, 0, len(v.GetArgs()))
		for _, arg := range v.GetArgs() {
			trace_util_0.Count(_util_00000, 46)
			newArgs = append(newArgs, ColumnSubstitute(arg, schema, newExprs))
		}
		trace_util_0.Count(_util_00000, 42)
		return NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, newArgs...)
	}
	trace_util_0.Count(_util_00000, 36)
	return expr
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	trace_util_0.Count(_util_00000, 47)
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		trace_util_0.Count(_util_00000, 51)
		upper = rune('0' + base)
	case base <= 36:
		trace_util_0.Count(_util_00000, 52)
		upper = rune('A' + base - 10)
	default:
		trace_util_0.Count(_util_00000, 53)
		return ""
	}
	trace_util_0.Count(_util_00000, 48)
Loop:
	for i := 0; i < len(s); i++ {
		trace_util_0.Count(_util_00000, 54)
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			trace_util_0.Count(_util_00000, 55)
			c = unicode.ToUpper(c)
			if c < upper {
				trace_util_0.Count(_util_00000, 58)
				validLen = i + 1
			} else {
				trace_util_0.Count(_util_00000, 59)
				{
					break Loop
				}
			}
		case c == '+' || c == '-':
			trace_util_0.Count(_util_00000, 56)
			if i != 0 {
				trace_util_0.Count(_util_00000, 60)
				break Loop
			}
		default:
			trace_util_0.Count(_util_00000, 57)
			break Loop
		}
	}
	trace_util_0.Count(_util_00000, 49)
	if validLen > 1 && s[0] == '+' {
		trace_util_0.Count(_util_00000, 61)
		return s[1:validLen]
	}
	trace_util_0.Count(_util_00000, 50)
	return s[:validLen]
}

// SubstituteCorCol2Constant will substitute correlated column to constant value which it contains.
// If the args of one scalar function are all constant, we will substitute it to constant.
func SubstituteCorCol2Constant(expr Expression) (Expression, error) {
	trace_util_0.Count(_util_00000, 62)
	switch x := expr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 64)
		allConstant := true
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			trace_util_0.Count(_util_00000, 70)
			newArg, err := SubstituteCorCol2Constant(arg)
			if err != nil {
				trace_util_0.Count(_util_00000, 72)
				return nil, err
			}
			trace_util_0.Count(_util_00000, 71)
			_, ok := newArg.(*Constant)
			newArgs = append(newArgs, newArg)
			allConstant = allConstant && ok
		}
		trace_util_0.Count(_util_00000, 65)
		if allConstant {
			trace_util_0.Count(_util_00000, 73)
			val, err := x.Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_util_00000, 75)
				return nil, err
			}
			trace_util_0.Count(_util_00000, 74)
			return &Constant{Value: val, RetType: x.GetType()}, nil
		}
		trace_util_0.Count(_util_00000, 66)
		var newSf Expression
		if x.FuncName.L == ast.Cast {
			trace_util_0.Count(_util_00000, 76)
			newSf = BuildCastFunction(x.GetCtx(), newArgs[0], x.RetType)
		} else {
			trace_util_0.Count(_util_00000, 77)
			{
				newSf = NewFunctionInternal(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
			}
		}
		trace_util_0.Count(_util_00000, 67)
		return newSf, nil
	case *CorrelatedColumn:
		trace_util_0.Count(_util_00000, 68)
		return &Constant{Value: *x.Data, RetType: x.GetType()}, nil
	case *Constant:
		trace_util_0.Count(_util_00000, 69)
		if x.DeferredExpr != nil {
			trace_util_0.Count(_util_00000, 78)
			newExpr := FoldConstant(x)
			return &Constant{Value: newExpr.(*Constant).Value, RetType: x.GetType()}, nil
		}
	}
	trace_util_0.Count(_util_00000, 63)
	return expr, nil
}

// timeZone2Duration converts timezone whose format should satisfy the regular condition
// `(^(+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^+13:00$)` to time.Duration.
func timeZone2Duration(tz string) time.Duration {
	trace_util_0.Count(_util_00000, 79)
	sign := 1
	if strings.HasPrefix(tz, "-") {
		trace_util_0.Count(_util_00000, 81)
		sign = -1
	}

	trace_util_0.Count(_util_00000, 80)
	i := strings.Index(tz, ":")
	h, err := strconv.Atoi(tz[1:i])
	terror.Log(err)
	m, err := strconv.Atoi(tz[i+1:])
	terror.Log(err)
	return time.Duration(sign) * (time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

var oppositeOp = map[string]string{
	ast.LT: ast.GE,
	ast.GE: ast.LT,
	ast.GT: ast.LE,
	ast.LE: ast.GT,
	ast.EQ: ast.NE,
	ast.NE: ast.EQ,
}

// a op b is equal to b symmetricOp a
var symmetricOp = map[opcode.Op]opcode.Op{
	opcode.LT:     opcode.GT,
	opcode.GE:     opcode.LE,
	opcode.GT:     opcode.LT,
	opcode.LE:     opcode.GE,
	opcode.EQ:     opcode.EQ,
	opcode.NE:     opcode.NE,
	opcode.NullEQ: opcode.NullEQ,
}

func doPushDownNot(ctx sessionctx.Context, exprs []Expression, not bool) []Expression {
	trace_util_0.Count(_util_00000, 82)
	newExprs := make([]Expression, 0, len(exprs))
	for _, expr := range exprs {
		trace_util_0.Count(_util_00000, 84)
		newExprs = append(newExprs, PushDownNot(ctx, expr, not))
	}
	trace_util_0.Count(_util_00000, 83)
	return newExprs
}

// PushDownNot pushes the `not` function down to the expression's arguments.
func PushDownNot(ctx sessionctx.Context, expr Expression, not bool) Expression {
	trace_util_0.Count(_util_00000, 85)
	if f, ok := expr.(*ScalarFunction); ok {
		trace_util_0.Count(_util_00000, 88)
		switch f.FuncName.L {
		case ast.UnaryNot:
			trace_util_0.Count(_util_00000, 89)
			return PushDownNot(f.GetCtx(), f.GetArgs()[0], !not)
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			trace_util_0.Count(_util_00000, 90)
			if not {
				trace_util_0.Count(_util_00000, 96)
				return NewFunctionInternal(f.GetCtx(), oppositeOp[f.FuncName.L], f.GetType(), f.GetArgs()...)
			}
			trace_util_0.Count(_util_00000, 91)
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		case ast.LogicAnd:
			trace_util_0.Count(_util_00000, 92)
			if not {
				trace_util_0.Count(_util_00000, 97)
				newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), true)
				return NewFunctionInternal(f.GetCtx(), ast.LogicOr, f.GetType(), newArgs...)
			}
			trace_util_0.Count(_util_00000, 93)
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		case ast.LogicOr:
			trace_util_0.Count(_util_00000, 94)
			if not {
				trace_util_0.Count(_util_00000, 98)
				newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), true)
				return NewFunctionInternal(f.GetCtx(), ast.LogicAnd, f.GetType(), newArgs...)
			}
			trace_util_0.Count(_util_00000, 95)
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		}
	}
	trace_util_0.Count(_util_00000, 86)
	if not {
		trace_util_0.Count(_util_00000, 99)
		expr = NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), expr)
	}
	trace_util_0.Count(_util_00000, 87)
	return expr
}

// Contains tests if `exprs` contains `e`.
func Contains(exprs []Expression, e Expression) bool {
	trace_util_0.Count(_util_00000, 100)
	for _, expr := range exprs {
		trace_util_0.Count(_util_00000, 102)
		if e == expr {
			trace_util_0.Count(_util_00000, 103)
			return true
		}
	}
	trace_util_0.Count(_util_00000, 101)
	return false
}

// ExtractFiltersFromDNFs checks whether the cond is DNF. If so, it will get the extracted part and the remained part.
// The original DNF will be replaced by the remained part or just be deleted if remained part is nil.
// And the extracted part will be appended to the end of the orignal slice.
func ExtractFiltersFromDNFs(ctx sessionctx.Context, conditions []Expression) []Expression {
	trace_util_0.Count(_util_00000, 104)
	var allExtracted []Expression
	for i := len(conditions) - 1; i >= 0; i-- {
		trace_util_0.Count(_util_00000, 106)
		if sf, ok := conditions[i].(*ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			trace_util_0.Count(_util_00000, 107)
			extracted, remained := extractFiltersFromDNF(ctx, sf)
			allExtracted = append(allExtracted, extracted...)
			if remained == nil {
				trace_util_0.Count(_util_00000, 108)
				conditions = append(conditions[:i], conditions[i+1:]...)
			} else {
				trace_util_0.Count(_util_00000, 109)
				{
					conditions[i] = remained
				}
			}
		}
	}
	trace_util_0.Count(_util_00000, 105)
	return append(conditions, allExtracted...)
}

// extractFiltersFromDNF extracts the same condition that occurs in every DNF item and remove them from dnf leaves.
func extractFiltersFromDNF(ctx sessionctx.Context, dnfFunc *ScalarFunction) ([]Expression, Expression) {
	trace_util_0.Count(_util_00000, 110)
	dnfItems := FlattenDNFConditions(dnfFunc)
	sc := ctx.GetSessionVars().StmtCtx
	codeMap := make(map[string]int)
	hashcode2Expr := make(map[string]Expression)
	for i, dnfItem := range dnfItems {
		trace_util_0.Count(_util_00000, 117)
		innerMap := make(map[string]struct{})
		cnfItems := SplitCNFItems(dnfItem)
		for _, cnfItem := range cnfItems {
			trace_util_0.Count(_util_00000, 118)
			code := cnfItem.HashCode(sc)
			if i == 0 {
				trace_util_0.Count(_util_00000, 119)
				codeMap[string(code)] = 1
				hashcode2Expr[string(code)] = cnfItem
			} else {
				trace_util_0.Count(_util_00000, 120)
				if _, ok := codeMap[string(code)]; ok {
					trace_util_0.Count(_util_00000, 121)
					// We need this check because there may be the case like `select * from t, t1 where (t.a=t1.a and t.a=t1.a) or (something).
					// We should make sure that the two `t.a=t1.a` contributes only once.
					// TODO: do this out of this function.
					if _, ok = innerMap[string(code)]; !ok {
						trace_util_0.Count(_util_00000, 122)
						codeMap[string(code)]++
						innerMap[string(code)] = struct{}{}
					}
				}
			}
		}
	}
	// We should make sure that this item occurs in every DNF item.
	trace_util_0.Count(_util_00000, 111)
	for hashcode, cnt := range codeMap {
		trace_util_0.Count(_util_00000, 123)
		if cnt < len(dnfItems) {
			trace_util_0.Count(_util_00000, 124)
			delete(hashcode2Expr, hashcode)
		}
	}
	trace_util_0.Count(_util_00000, 112)
	if len(hashcode2Expr) == 0 {
		trace_util_0.Count(_util_00000, 125)
		return nil, dnfFunc
	}
	trace_util_0.Count(_util_00000, 113)
	newDNFItems := make([]Expression, 0, len(dnfItems))
	onlyNeedExtracted := false
	for _, dnfItem := range dnfItems {
		trace_util_0.Count(_util_00000, 126)
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			trace_util_0.Count(_util_00000, 129)
			code := cnfItem.HashCode(sc)
			_, ok := hashcode2Expr[string(code)]
			if !ok {
				trace_util_0.Count(_util_00000, 130)
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If the extracted part is just one leaf of the DNF expression. Then the value of the total DNF expression is
		// always the same with the value of the extracted part.
		trace_util_0.Count(_util_00000, 127)
		if len(newCNFItems) == 0 {
			trace_util_0.Count(_util_00000, 131)
			onlyNeedExtracted = true
			break
		}
		trace_util_0.Count(_util_00000, 128)
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	trace_util_0.Count(_util_00000, 114)
	extractedExpr := make([]Expression, 0, len(hashcode2Expr))
	for _, expr := range hashcode2Expr {
		trace_util_0.Count(_util_00000, 132)
		extractedExpr = append(extractedExpr, expr)
	}
	trace_util_0.Count(_util_00000, 115)
	if onlyNeedExtracted {
		trace_util_0.Count(_util_00000, 133)
		return extractedExpr, nil
	}
	trace_util_0.Count(_util_00000, 116)
	return extractedExpr, ComposeDNFCondition(ctx, newDNFItems...)
}

// DeriveRelaxedFiltersFromDNF given a DNF expression, derive a relaxed DNF expression which only contains columns
// in specified schema; the derived expression is a superset of original expression, i.e, any tuple satisfying
// the original expression must satisfy the derived expression. Return nil when the derived expression is universal set.
// A running example is: for schema of t1, `(t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)` would be derived as
// `t1.a=1 or t1.a=2`, while `t1.a=1 or t2.a=1` would get nil.
func DeriveRelaxedFiltersFromDNF(expr Expression, schema *Schema) Expression {
	trace_util_0.Count(_util_00000, 134)
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.LogicOr {
		trace_util_0.Count(_util_00000, 137)
		return nil
	}
	trace_util_0.Count(_util_00000, 135)
	ctx := sf.GetCtx()
	dnfItems := FlattenDNFConditions(sf)
	newDNFItems := make([]Expression, 0, len(dnfItems))
	for _, dnfItem := range dnfItems {
		trace_util_0.Count(_util_00000, 138)
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			trace_util_0.Count(_util_00000, 141)
			if itemSF, ok := cnfItem.(*ScalarFunction); ok && itemSF.FuncName.L == ast.LogicOr {
				trace_util_0.Count(_util_00000, 143)
				relaxedCNFItem := DeriveRelaxedFiltersFromDNF(cnfItem, schema)
				if relaxedCNFItem != nil {
					trace_util_0.Count(_util_00000, 145)
					newCNFItems = append(newCNFItems, relaxedCNFItem)
				}
				// If relaxed expression for embedded DNF is universal set, just drop this CNF item
				trace_util_0.Count(_util_00000, 144)
				continue
			}
			// This cnfItem must be simple expression now
			// If it cannot be fully covered by schema, just drop this CNF item
			trace_util_0.Count(_util_00000, 142)
			if ExprFromSchema(cnfItem, schema) {
				trace_util_0.Count(_util_00000, 146)
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If this DNF item involves no column of specified schema, the relaxed expression must be universal set
		trace_util_0.Count(_util_00000, 139)
		if len(newCNFItems) == 0 {
			trace_util_0.Count(_util_00000, 147)
			return nil
		}
		trace_util_0.Count(_util_00000, 140)
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	trace_util_0.Count(_util_00000, 136)
	return ComposeDNFCondition(ctx, newDNFItems...)
}

// GetRowLen gets the length if the func is row, returns 1 if not row.
func GetRowLen(e Expression) int {
	trace_util_0.Count(_util_00000, 148)
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		trace_util_0.Count(_util_00000, 150)
		return len(f.GetArgs())
	}
	trace_util_0.Count(_util_00000, 149)
	return 1
}

// CheckArgsNotMultiColumnRow checks the args are not multi-column row.
func CheckArgsNotMultiColumnRow(args ...Expression) error {
	trace_util_0.Count(_util_00000, 151)
	for _, arg := range args {
		trace_util_0.Count(_util_00000, 153)
		if GetRowLen(arg) != 1 {
			trace_util_0.Count(_util_00000, 154)
			return ErrOperandColumns.GenWithStackByArgs(1)
		}
	}
	trace_util_0.Count(_util_00000, 152)
	return nil
}

// GetFuncArg gets the argument of the function at idx.
func GetFuncArg(e Expression, idx int) Expression {
	trace_util_0.Count(_util_00000, 155)
	if f, ok := e.(*ScalarFunction); ok {
		trace_util_0.Count(_util_00000, 157)
		return f.GetArgs()[idx]
	}
	trace_util_0.Count(_util_00000, 156)
	return nil
}

// PopRowFirstArg pops the first element and returns the rest of row.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func PopRowFirstArg(ctx sessionctx.Context, e Expression) (ret Expression, err error) {
	trace_util_0.Count(_util_00000, 158)
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		trace_util_0.Count(_util_00000, 160)
		args := f.GetArgs()
		if len(args) == 2 {
			trace_util_0.Count(_util_00000, 162)
			return args[1], nil
		}
		trace_util_0.Count(_util_00000, 161)
		ret, err = NewFunction(ctx, ast.RowFunc, f.GetType(), args[1:]...)
		return ret, err
	}
	trace_util_0.Count(_util_00000, 159)
	return
}

// exprStack is a stack of expressions.
type exprStack struct {
	stack []Expression
}

// pop pops an expression from the stack.
func (s *exprStack) pop() Expression {
	trace_util_0.Count(_util_00000, 163)
	if s.len() == 0 {
		trace_util_0.Count(_util_00000, 165)
		return nil
	}
	trace_util_0.Count(_util_00000, 164)
	lastIdx := s.len() - 1
	expr := s.stack[lastIdx]
	s.stack = s.stack[:lastIdx]
	return expr
}

// popN pops n expressions from the stack.
// If n greater than stack length or n is negative, it pops all the expressions.
func (s *exprStack) popN(n int) []Expression {
	trace_util_0.Count(_util_00000, 166)
	if n > s.len() || n < 0 {
		trace_util_0.Count(_util_00000, 168)
		n = s.len()
	}
	trace_util_0.Count(_util_00000, 167)
	idx := s.len() - n
	exprs := s.stack[idx:]
	s.stack = s.stack[:idx]
	return exprs
}

// push pushes one expression to the stack.
func (s *exprStack) push(expr Expression) {
	trace_util_0.Count(_util_00000, 169)
	s.stack = append(s.stack, expr)
}

// len returns the length of th stack.
func (s *exprStack) len() int {
	trace_util_0.Count(_util_00000, 170)
	return len(s.stack)
}

// ColumnSliceIsIntersect checks whether two column slice is intersected.
func ColumnSliceIsIntersect(s1, s2 []*Column) bool {
	trace_util_0.Count(_util_00000, 171)
	intSet := map[int64]struct{}{}
	for _, col := range s1 {
		trace_util_0.Count(_util_00000, 174)
		intSet[col.UniqueID] = struct{}{}
	}
	trace_util_0.Count(_util_00000, 172)
	for _, col := range s2 {
		trace_util_0.Count(_util_00000, 175)
		if _, ok := intSet[col.UniqueID]; ok {
			trace_util_0.Count(_util_00000, 176)
			return true
		}
	}
	trace_util_0.Count(_util_00000, 173)
	return false
}

// DatumToConstant generates a Constant expression from a Datum.
func DatumToConstant(d types.Datum, tp byte) *Constant {
	trace_util_0.Count(_util_00000, 177)
	return &Constant{Value: d, RetType: types.NewFieldType(tp)}
}

// GetParamExpression generate a getparam function expression.
func GetParamExpression(ctx sessionctx.Context, v *driver.ParamMarkerExpr) (Expression, error) {
	trace_util_0.Count(_util_00000, 178)
	useCache := ctx.GetSessionVars().StmtCtx.UseCache
	tp := types.NewFieldType(mysql.TypeUnspecified)
	types.DefaultParamTypeForValue(v.GetValue(), tp)
	value := &Constant{Value: v.Datum, RetType: tp}
	if useCache {
		trace_util_0.Count(_util_00000, 180)
		f, err := NewFunctionBase(ctx, ast.GetParam, &v.Type,
			DatumToConstant(types.NewIntDatum(int64(v.Order)), mysql.TypeLonglong))
		if err != nil {
			trace_util_0.Count(_util_00000, 182)
			return nil, err
		}
		trace_util_0.Count(_util_00000, 181)
		f.GetType().Tp = v.Type.Tp
		value.DeferredExpr = f
	}
	trace_util_0.Count(_util_00000, 179)
	return value, nil
}

// DisableParseJSONFlag4Expr disables ParseToJSONFlag for `expr` except Column.
// We should not *PARSE* a string as JSON under some scenarios. ParseToJSONFlag
// is 0 for JSON column yet(as well as JSON correlated column), so we can skip
// it. Moreover, Column.RetType refers to the infoschema, if we modify it, data
// race may happen if another goroutine read from the infoschema at the same
// time.
func DisableParseJSONFlag4Expr(expr Expression) {
	trace_util_0.Count(_util_00000, 183)
	if _, isColumn := expr.(*Column); isColumn {
		trace_util_0.Count(_util_00000, 186)
		return
	}
	trace_util_0.Count(_util_00000, 184)
	if _, isCorCol := expr.(*CorrelatedColumn); isCorCol {
		trace_util_0.Count(_util_00000, 187)
		return
	}
	trace_util_0.Count(_util_00000, 185)
	expr.GetType().Flag &= ^mysql.ParseToJSONFlag
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	trace_util_0.Count(_util_00000, 188)
	return &ast.PositionExpr{P: p}
}

// PosFromPositionExpr generates a position value from PositionExpr.
func PosFromPositionExpr(ctx sessionctx.Context, v *ast.PositionExpr) (int, bool, error) {
	trace_util_0.Count(_util_00000, 189)
	if v.P == nil {
		trace_util_0.Count(_util_00000, 193)
		return v.N, false, nil
	}
	trace_util_0.Count(_util_00000, 190)
	value, err := GetParamExpression(ctx, v.P.(*driver.ParamMarkerExpr))
	if err != nil {
		trace_util_0.Count(_util_00000, 194)
		return 0, true, err
	}
	trace_util_0.Count(_util_00000, 191)
	pos, isNull, err := GetIntFromConstant(ctx, value)
	if err != nil || isNull {
		trace_util_0.Count(_util_00000, 195)
		return 0, true, err
	}
	trace_util_0.Count(_util_00000, 192)
	return pos, false, nil
}

// GetStringFromConstant gets a string value from the Constant expression.
func GetStringFromConstant(ctx sessionctx.Context, value Expression) (string, bool, error) {
	trace_util_0.Count(_util_00000, 196)
	con, ok := value.(*Constant)
	if !ok {
		trace_util_0.Count(_util_00000, 199)
		err := errors.Errorf("Not a Constant expression %+v", value)
		return "", true, err
	}
	trace_util_0.Count(_util_00000, 197)
	str, isNull, err := con.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		trace_util_0.Count(_util_00000, 200)
		return "", true, err
	}
	trace_util_0.Count(_util_00000, 198)
	return str, false, nil
}

// GetIntFromConstant gets an interger value from the Constant expression.
func GetIntFromConstant(ctx sessionctx.Context, value Expression) (int, bool, error) {
	trace_util_0.Count(_util_00000, 201)
	str, isNull, err := GetStringFromConstant(ctx, value)
	if err != nil || isNull {
		trace_util_0.Count(_util_00000, 204)
		return 0, true, err
	}
	trace_util_0.Count(_util_00000, 202)
	intNum, err := strconv.Atoi(str)
	if err != nil {
		trace_util_0.Count(_util_00000, 205)
		return 0, true, nil
	}
	trace_util_0.Count(_util_00000, 203)
	return intNum, false, nil
}

// BuildNotNullExpr wraps up `not(isnull())` for given expression.
func BuildNotNullExpr(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_util_00000, 206)
	isNull := NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
	notNull := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNull)
	return notNull
}

// isMutableEffectsExpr checks if expr contains function which is mutable or has side effects.
func isMutableEffectsExpr(expr Expression) bool {
	trace_util_0.Count(_util_00000, 207)
	switch x := expr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_util_00000, 209)
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			trace_util_0.Count(_util_00000, 213)
			return true
		}
		trace_util_0.Count(_util_00000, 210)
		for _, arg := range x.GetArgs() {
			trace_util_0.Count(_util_00000, 214)
			if isMutableEffectsExpr(arg) {
				trace_util_0.Count(_util_00000, 215)
				return true
			}
		}
	case *Column:
		trace_util_0.Count(_util_00000, 211)
	case *Constant:
		trace_util_0.Count(_util_00000, 212)
		if x.DeferredExpr != nil {
			trace_util_0.Count(_util_00000, 216)
			return isMutableEffectsExpr(x.DeferredExpr)
		}
	}
	trace_util_0.Count(_util_00000, 208)
	return false
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mutable or have side effects, we cannot remove it even if it has duplicates.
func RemoveDupExprs(ctx sessionctx.Context, exprs []Expression) []Expression {
	trace_util_0.Count(_util_00000, 217)
	res := make([]Expression, 0, len(exprs))
	exists := make(map[string]struct{}, len(exprs))
	sc := ctx.GetSessionVars().StmtCtx
	for _, expr := range exprs {
		trace_util_0.Count(_util_00000, 219)
		key := string(expr.HashCode(sc))
		if _, ok := exists[key]; !ok || isMutableEffectsExpr(expr) {
			trace_util_0.Count(_util_00000, 220)
			res = append(res, expr)
			exists[key] = struct{}{}
		}
	}
	trace_util_0.Count(_util_00000, 218)
	return res
}

// GetUint64FromConstant gets a uint64 from constant expression.
func GetUint64FromConstant(expr Expression) (uint64, bool, bool) {
	trace_util_0.Count(_util_00000, 221)
	con, ok := expr.(*Constant)
	if !ok {
		trace_util_0.Count(_util_00000, 225)
		logutil.Logger(context.Background()).Warn("not a constant expression", zap.String("expression", expr.ExplainInfo()))
		return 0, false, false
	}
	trace_util_0.Count(_util_00000, 222)
	dt := con.Value
	if con.DeferredExpr != nil {
		trace_util_0.Count(_util_00000, 226)
		var err error
		dt, err = con.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_util_00000, 227)
			logutil.Logger(context.Background()).Warn("eval deferred expr failed", zap.Error(err))
			return 0, false, false
		}
	}
	trace_util_0.Count(_util_00000, 223)
	switch dt.Kind() {
	case types.KindNull:
		trace_util_0.Count(_util_00000, 228)
		return 0, true, true
	case types.KindInt64:
		trace_util_0.Count(_util_00000, 229)
		val := dt.GetInt64()
		if val < 0 {
			trace_util_0.Count(_util_00000, 232)
			return 0, false, false
		}
		trace_util_0.Count(_util_00000, 230)
		return uint64(val), false, true
	case types.KindUint64:
		trace_util_0.Count(_util_00000, 231)
		return dt.GetUint64(), false, true
	}
	trace_util_0.Count(_util_00000, 224)
	return 0, false, false
}

var _util_00000 = "expression/util.go"
