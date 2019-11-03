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
	goJSON "encoding/json"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(ctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler

	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx sessionctx.Context, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no tables.
	// refers no subqueries that refers any tables.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	ConstItem() bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema. It will copy the original expression and return the copied one.
	ResolveIndices(schema *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schema *Schema) error

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	trace_util_0.Count(_expression_00000, 0)
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		trace_util_0.Count(_expression_00000, 2)
		cnf = append(cnf, expr.Clone())
	}
	trace_util_0.Count(_expression_00000, 1)
	return cnf
}

func isColumnInOperand(c *Column) bool {
	trace_util_0.Count(_expression_00000, 3)
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	trace_util_0.Count(_expression_00000, 4)
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		trace_util_0.Count(_expression_00000, 6)
		return false
	}
	trace_util_0.Count(_expression_00000, 5)
	cols := make([]*Column, 0, 1)
	cols = ExtractColumnsFromExpressions(cols, sf.GetArgs(), isColumnInOperand)
	return len(cols) > 0
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx sessionctx.Context, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	trace_util_0.Count(_expression_00000, 7)
	hasNull := false
	for _, expr := range exprList {
		trace_util_0.Count(_expression_00000, 10)
		data, err := expr.Eval(row)
		if err != nil {
			trace_util_0.Count(_expression_00000, 14)
			return false, false, err
		}
		trace_util_0.Count(_expression_00000, 11)
		if data.IsNull() {
			trace_util_0.Count(_expression_00000, 15)
			// For queries like `select a in (select a from s where t.b = s.b) from t`,
			// if result of `t.a = s.a` is null, we cannot return immediately until
			// we have checked if `t.b = s.b` is null or false, because it means
			// subquery is empty, and we should return false as the result of the whole
			// exprList in that case, instead of null.
			if !IsEQCondFromIn(expr) {
				trace_util_0.Count(_expression_00000, 17)
				return false, false, nil
			}
			trace_util_0.Count(_expression_00000, 16)
			hasNull = true
			continue
		}

		trace_util_0.Count(_expression_00000, 12)
		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			trace_util_0.Count(_expression_00000, 18)
			return false, false, err
		}
		trace_util_0.Count(_expression_00000, 13)
		if i == 0 {
			trace_util_0.Count(_expression_00000, 19)
			return false, false, nil
		}
	}
	trace_util_0.Count(_expression_00000, 8)
	if hasNull {
		trace_util_0.Count(_expression_00000, 20)
		return false, true, nil
	}
	trace_util_0.Count(_expression_00000, 9)
	return true, false, nil
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx sessionctx.Context, conditions []Expression, funcName string) Expression {
	trace_util_0.Count(_expression_00000, 21)
	length := len(conditions)
	if length == 0 {
		trace_util_0.Count(_expression_00000, 24)
		return nil
	}
	trace_util_0.Count(_expression_00000, 22)
	if length == 1 {
		trace_util_0.Count(_expression_00000, 25)
		return conditions[0]
	}
	trace_util_0.Count(_expression_00000, 23)
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	trace_util_0.Count(_expression_00000, 26)
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	trace_util_0.Count(_expression_00000, 27)
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	trace_util_0.Count(_expression_00000, 28)
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		trace_util_0.Count(_expression_00000, 30)
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			trace_util_0.Count(_expression_00000, 31)
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			trace_util_0.Count(_expression_00000, 32)
			{
				ret = append(ret, arg)
			}
		}
	}
	trace_util_0.Count(_expression_00000, 29)
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	trace_util_0.Count(_expression_00000, 33)
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	trace_util_0.Count(_expression_00000, 34)
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col  *Column
	Expr Expression
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	trace_util_0.Count(_expression_00000, 35)
	switch v := onExpr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_expression_00000, 37)
		if v.FuncName.L == funcName {
			trace_util_0.Count(_expression_00000, 38)
			var ret []Expression
			for _, arg := range v.GetArgs() {
				trace_util_0.Count(_expression_00000, 40)
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			trace_util_0.Count(_expression_00000, 39)
			return ret
		}
	}
	trace_util_0.Count(_expression_00000, 36)
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	trace_util_0.Count(_expression_00000, 41)
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	trace_util_0.Count(_expression_00000, 42)
	return splitNormalFormItems(onExpr, ast.LogicOr)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	trace_util_0.Count(_expression_00000, 43)
	switch x := expr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_expression_00000, 45)
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			trace_util_0.Count(_expression_00000, 50)
			args[i] = EvaluateExprWithNull(ctx, schema, arg)
		}
		trace_util_0.Count(_expression_00000, 46)
		return NewFunctionInternal(ctx, x.FuncName.L, types.NewFieldType(mysql.TypeTiny), args...)
	case *Column:
		trace_util_0.Count(_expression_00000, 47)
		if !schema.Contains(x) {
			trace_util_0.Count(_expression_00000, 51)
			return x
		}
		trace_util_0.Count(_expression_00000, 48)
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		trace_util_0.Count(_expression_00000, 49)
		if x.DeferredExpr != nil {
			trace_util_0.Count(_expression_00000, 52)
			return FoldConstant(x)
		}
	}
	trace_util_0.Count(_expression_00000, 44)
	return expr
}

// TableInfo2Schema converts table info to schema with empty DBName.
func TableInfo2Schema(ctx sessionctx.Context, tbl *model.TableInfo) *Schema {
	trace_util_0.Count(_expression_00000, 53)
	return TableInfo2SchemaWithDBName(ctx, model.CIStr{}, tbl)
}

// TableInfo2SchemaWithDBName converts table info to schema.
func TableInfo2SchemaWithDBName(ctx sessionctx.Context, dbName model.CIStr, tbl *model.TableInfo) *Schema {
	trace_util_0.Count(_expression_00000, 54)
	cols := ColumnInfos2ColumnsWithDBName(ctx, dbName, tbl.Name, tbl.Columns)
	keys := make([]KeyInfo, 0, len(tbl.Indices)+1)
	for _, idx := range tbl.Indices {
		trace_util_0.Count(_expression_00000, 57)
		if !idx.Unique || idx.State != model.StatePublic {
			trace_util_0.Count(_expression_00000, 60)
			continue
		}
		trace_util_0.Count(_expression_00000, 58)
		ok := true
		newKey := make([]*Column, 0, len(idx.Columns))
		for _, idxCol := range idx.Columns {
			trace_util_0.Count(_expression_00000, 61)
			find := false
			for i, col := range tbl.Columns {
				trace_util_0.Count(_expression_00000, 63)
				if idxCol.Name.L == col.Name.L {
					trace_util_0.Count(_expression_00000, 64)
					if !mysql.HasNotNullFlag(col.Flag) {
						trace_util_0.Count(_expression_00000, 66)
						break
					}
					trace_util_0.Count(_expression_00000, 65)
					newKey = append(newKey, cols[i])
					find = true
					break
				}
			}
			trace_util_0.Count(_expression_00000, 62)
			if !find {
				trace_util_0.Count(_expression_00000, 67)
				ok = false
				break
			}
		}
		trace_util_0.Count(_expression_00000, 59)
		if ok {
			trace_util_0.Count(_expression_00000, 68)
			keys = append(keys, newKey)
		}
	}
	trace_util_0.Count(_expression_00000, 55)
	if tbl.PKIsHandle {
		trace_util_0.Count(_expression_00000, 69)
		for i, col := range tbl.Columns {
			trace_util_0.Count(_expression_00000, 70)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_expression_00000, 71)
				keys = append(keys, KeyInfo{cols[i]})
				break
			}
		}
	}
	trace_util_0.Count(_expression_00000, 56)
	schema := NewSchema(cols...)
	schema.SetUniqueKeys(keys)
	return schema
}

// ColumnInfos2ColumnsWithDBName converts a slice of ColumnInfo to a slice of Column.
func ColumnInfos2ColumnsWithDBName(ctx sessionctx.Context, dbName, tblName model.CIStr, colInfos []*model.ColumnInfo) []*Column {
	trace_util_0.Count(_expression_00000, 72)
	columns := make([]*Column, 0, len(colInfos))
	for _, col := range colInfos {
		trace_util_0.Count(_expression_00000, 74)
		if col.State != model.StatePublic {
			trace_util_0.Count(_expression_00000, 76)
			continue
		}
		trace_util_0.Count(_expression_00000, 75)
		newCol := &Column{
			ColName:  col.Name,
			TblName:  tblName,
			DBName:   dbName,
			RetType:  &col.FieldType,
			ID:       col.ID,
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			Index:    col.Offset,
		}
		columns = append(columns, newCol)
	}
	trace_util_0.Count(_expression_00000, 73)
	return columns
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(ctx sessionctx.Context, offset int, retTp *types.FieldType) *ScalarFunction {
	trace_util_0.Count(_expression_00000, 77)
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, err := fc.getFunction(ctx, nil)
	terror.Log(err)
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt,
	}
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	trace_util_0.Count(_expression_00000, 78)
	con, ok := expr.(*Constant)
	return ok && con.Value.Kind() == types.KindBinaryLiteral
}

var _expression_00000 = "expression/expression.go"
