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

package aggregation

import (
	"bytes"
	"math"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// baseFuncDesc describes an function signature, only used in planner.
type baseFuncDesc struct {
	// Name represents the function name.
	Name string
	// Args represents the arguments of the function.
	Args []expression.Expression
	// RetTp represents the return type of the function.
	RetTp *types.FieldType
}

func newBaseFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (baseFuncDesc, error) {
	trace_util_0.Count(_base_func_00000, 0)
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	err := b.typeInfer(ctx)
	return b, err
}

func (a *baseFuncDesc) equal(ctx sessionctx.Context, other *baseFuncDesc) bool {
	trace_util_0.Count(_base_func_00000, 1)
	if a.Name != other.Name || len(a.Args) != len(other.Args) {
		trace_util_0.Count(_base_func_00000, 4)
		return false
	}
	trace_util_0.Count(_base_func_00000, 2)
	for i := range a.Args {
		trace_util_0.Count(_base_func_00000, 5)
		if !a.Args[i].Equal(ctx, other.Args[i]) {
			trace_util_0.Count(_base_func_00000, 6)
			return false
		}
	}
	trace_util_0.Count(_base_func_00000, 3)
	return true
}

func (a *baseFuncDesc) clone() *baseFuncDesc {
	trace_util_0.Count(_base_func_00000, 7)
	clone := *a
	newTp := *a.RetTp
	clone.RetTp = &newTp
	clone.Args = make([]expression.Expression, len(a.Args))
	for i := range a.Args {
		trace_util_0.Count(_base_func_00000, 9)
		clone.Args[i] = a.Args[i].Clone()
	}
	trace_util_0.Count(_base_func_00000, 8)
	return &clone
}

// String implements the fmt.Stringer interface.
func (a *baseFuncDesc) String() string {
	trace_util_0.Count(_base_func_00000, 10)
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	for i, arg := range a.Args {
		trace_util_0.Count(_base_func_00000, 12)
		buffer.WriteString(arg.String())
		if i+1 != len(a.Args) {
			trace_util_0.Count(_base_func_00000, 13)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_base_func_00000, 11)
	buffer.WriteString(")")
	return buffer.String()
}

// typeInfer infers the arguments and return types of an function.
func (a *baseFuncDesc) typeInfer(ctx sessionctx.Context) error {
	trace_util_0.Count(_base_func_00000, 14)
	switch a.Name {
	case ast.AggFuncCount:
		trace_util_0.Count(_base_func_00000, 16)
		a.typeInfer4Count(ctx)
	case ast.AggFuncSum:
		trace_util_0.Count(_base_func_00000, 17)
		a.typeInfer4Sum(ctx)
	case ast.AggFuncAvg:
		trace_util_0.Count(_base_func_00000, 18)
		a.typeInfer4Avg(ctx)
	case ast.AggFuncGroupConcat:
		trace_util_0.Count(_base_func_00000, 19)
		a.typeInfer4GroupConcat(ctx)
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow,
		ast.WindowFuncFirstValue, ast.WindowFuncLastValue, ast.WindowFuncNthValue:
		trace_util_0.Count(_base_func_00000, 20)
		a.typeInfer4MaxMin(ctx)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		trace_util_0.Count(_base_func_00000, 21)
		a.typeInfer4BitFuncs(ctx)
	case ast.WindowFuncRowNumber, ast.WindowFuncRank, ast.WindowFuncDenseRank:
		trace_util_0.Count(_base_func_00000, 22)
		a.typeInfer4NumberFuncs()
	case ast.WindowFuncCumeDist:
		trace_util_0.Count(_base_func_00000, 23)
		a.typeInfer4CumeDist()
	case ast.WindowFuncNtile:
		trace_util_0.Count(_base_func_00000, 24)
		a.typeInfer4Ntile()
	case ast.WindowFuncPercentRank:
		trace_util_0.Count(_base_func_00000, 25)
		a.typeInfer4PercentRank()
	case ast.WindowFuncLead, ast.WindowFuncLag:
		trace_util_0.Count(_base_func_00000, 26)
		a.typeInfer4LeadLag(ctx)
	default:
		trace_util_0.Count(_base_func_00000, 27)
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	trace_util_0.Count(_base_func_00000, 15)
	return nil
}

func (a *baseFuncDesc) typeInfer4Count(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 28)
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Sum should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Sum(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 29)
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_base_func_00000, 31)
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxDecimalWidth, 0
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_base_func_00000, 32)
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxDecimalWidth, a.Args[0].GetType().Decimal
		if a.RetTp.Decimal < 0 || a.RetTp.Decimal > mysql.MaxDecimalScale {
			trace_util_0.Count(_base_func_00000, 35)
			a.RetTp.Decimal = mysql.MaxDecimalScale
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		trace_util_0.Count(_base_func_00000, 33)
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		trace_util_0.Count(_base_func_00000, 34)
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	trace_util_0.Count(_base_func_00000, 30)
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Avg should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Avg(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 36)
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal:
		trace_util_0.Count(_base_func_00000, 38)
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		if a.Args[0].GetType().Decimal < 0 {
			trace_util_0.Count(_base_func_00000, 42)
			a.RetTp.Decimal = mysql.MaxDecimalScale
		} else {
			trace_util_0.Count(_base_func_00000, 43)
			{
				a.RetTp.Decimal = mathutil.Min(a.Args[0].GetType().Decimal+types.DivFracIncr, mysql.MaxDecimalScale)
			}
		}
		trace_util_0.Count(_base_func_00000, 39)
		a.RetTp.Flen = mysql.MaxDecimalWidth
	case mysql.TypeDouble, mysql.TypeFloat:
		trace_util_0.Count(_base_func_00000, 40)
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		trace_util_0.Count(_base_func_00000, 41)
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	trace_util_0.Count(_base_func_00000, 37)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4GroupConcat(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 44)
	a.RetTp = types.NewFieldType(mysql.TypeVarString)
	a.RetTp.Charset, a.RetTp.Collate = charset.GetDefaultCharsetAndCollate()

	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxBlobWidth, 0
	// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
}

func (a *baseFuncDesc) typeInfer4MaxMin(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 45)
	_, argIsScalaFunc := a.Args[0].(*expression.ScalarFunction)
	if argIsScalaFunc && a.Args[0].GetType().Tp == mysql.TypeFloat {
		trace_util_0.Count(_base_func_00000, 47)
		// For scalar function, the result of "float32" is set to the "float64"
		// field in the "Datum". If we do not wrap a cast-as-double function on a.Args[0],
		// error would happen when extracting the evaluation of a.Args[0] to a ProjectionExec.
		tp := types.NewFieldType(mysql.TypeDouble)
		tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
		types.SetBinChsClnFlag(tp)
		a.Args[0] = expression.BuildCastFunction(ctx, a.Args[0], tp)
	}
	trace_util_0.Count(_base_func_00000, 46)
	a.RetTp = a.Args[0].GetType()
	if a.RetTp.Tp == mysql.TypeEnum || a.RetTp.Tp == mysql.TypeSet {
		trace_util_0.Count(_base_func_00000, 48)
		a.RetTp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxFieldCharLength}
	}
}

func (a *baseFuncDesc) typeInfer4BitFuncs(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 49)
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= mysql.UnsignedFlag | mysql.NotNullFlag
	// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
}

func (a *baseFuncDesc) typeInfer4NumberFuncs() {
	trace_util_0.Count(_base_func_00000, 50)
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4CumeDist() {
	trace_util_0.Count(_base_func_00000, 51)
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, mysql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4Ntile() {
	trace_util_0.Count(_base_func_00000, 52)
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= mysql.UnsignedFlag
}

func (a *baseFuncDesc) typeInfer4PercentRank() {
	trace_util_0.Count(_base_func_00000, 53)
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.Flag, a.RetTp.Decimal = mysql.MaxRealWidth, mysql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4LeadLag(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 54)
	if len(a.Args) <= 2 {
		trace_util_0.Count(_base_func_00000, 55)
		a.typeInfer4MaxMin(ctx)
	} else {
		trace_util_0.Count(_base_func_00000, 56)
		{
			// Merge the type of first and third argument.
			a.RetTp = expression.InferType4ControlFuncs(a.Args[0].GetType(), a.Args[2].GetType())
		}
	}
}

// GetDefaultValue gets the default value when the function's input is null.
// According to MySQL, default values of the function are listed as follows:
// e.g.
// Table t which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select a, avg(a), sum(a), count(a), bit_xor(a), bit_or(a), bit_and(a), max(a), min(a), group_concat(a) from t;`
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | a    | avg(a) | sum(a) | count(a) | bit_xor(a) | bit_or(a) | bit_and(a)           | max(a) | min(a) | group_concat(a) |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | NULL |   NULL |   NULL |        0 |          0 |         0 | 18446744073709551615 |   NULL |   NULL | NULL            |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
func (a *baseFuncDesc) GetDefaultValue() (v types.Datum) {
	trace_util_0.Count(_base_func_00000, 57)
	switch a.Name {
	case ast.AggFuncCount, ast.AggFuncBitOr, ast.AggFuncBitXor:
		trace_util_0.Count(_base_func_00000, 59)
		v = types.NewIntDatum(0)
	case ast.AggFuncFirstRow, ast.AggFuncAvg, ast.AggFuncSum, ast.AggFuncMax,
		ast.AggFuncMin, ast.AggFuncGroupConcat:
		trace_util_0.Count(_base_func_00000, 60)
		v = types.Datum{}
	case ast.AggFuncBitAnd:
		trace_util_0.Count(_base_func_00000, 61)
		v = types.NewUintDatum(uint64(math.MaxUint64))
	}
	trace_util_0.Count(_base_func_00000, 58)
	return
}

// We do not need to wrap cast upon these functions,
// since the EvalXXX method called by the arg is determined by the corresponding arg type.
var noNeedCastAggFuncs = map[string]struct{}{
	ast.AggFuncCount:    {},
	ast.AggFuncMax:      {},
	ast.AggFuncMin:      {},
	ast.AggFuncFirstRow: {},
	ast.WindowFuncNtile: {},
}

// WrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (a *baseFuncDesc) WrapCastForAggArgs(ctx sessionctx.Context) {
	trace_util_0.Count(_base_func_00000, 62)
	if len(a.Args) == 0 {
		trace_util_0.Count(_base_func_00000, 66)
		return
	}
	trace_util_0.Count(_base_func_00000, 63)
	if _, ok := noNeedCastAggFuncs[a.Name]; ok {
		trace_util_0.Count(_base_func_00000, 67)
		return
	}
	trace_util_0.Count(_base_func_00000, 64)
	var castFunc func(ctx sessionctx.Context, expr expression.Expression) expression.Expression
	switch retTp := a.RetTp; retTp.EvalType() {
	case types.ETInt:
		trace_util_0.Count(_base_func_00000, 68)
		castFunc = expression.WrapWithCastAsInt
	case types.ETReal:
		trace_util_0.Count(_base_func_00000, 69)
		castFunc = expression.WrapWithCastAsReal
	case types.ETString:
		trace_util_0.Count(_base_func_00000, 70)
		castFunc = expression.WrapWithCastAsString
	case types.ETDecimal:
		trace_util_0.Count(_base_func_00000, 71)
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_base_func_00000, 72)
		castFunc = func(ctx sessionctx.Context, expr expression.Expression) expression.Expression {
			trace_util_0.Count(_base_func_00000, 76)
			return expression.WrapWithCastAsTime(ctx, expr, retTp)
		}
	case types.ETDuration:
		trace_util_0.Count(_base_func_00000, 73)
		castFunc = expression.WrapWithCastAsDuration
	case types.ETJson:
		trace_util_0.Count(_base_func_00000, 74)
		castFunc = expression.WrapWithCastAsJSON
	default:
		trace_util_0.Count(_base_func_00000, 75)
		panic("should never happen in baseFuncDesc.WrapCastForAggArgs")
	}
	trace_util_0.Count(_base_func_00000, 65)
	for i := range a.Args {
		trace_util_0.Count(_base_func_00000, 77)
		// Do not cast the second args of these functions, as they are simply non-negative numbers.
		if i == 1 && (a.Name == ast.WindowFuncLead || a.Name == ast.WindowFuncLag || a.Name == ast.WindowFuncNthValue) {
			trace_util_0.Count(_base_func_00000, 81)
			continue
		}
		trace_util_0.Count(_base_func_00000, 78)
		a.Args[i] = castFunc(ctx, a.Args[i])
		if a.Name != ast.AggFuncAvg && a.Name != ast.AggFuncSum {
			trace_util_0.Count(_base_func_00000, 82)
			continue
		}
		// After wrapping cast on the argument, flen etc. may not the same
		// as the type of the aggregation function. The following part set
		// the type of the argument exactly as the type of the aggregation
		// function.
		// Note: If the `Tp` of argument is the same as the `Tp` of the
		// aggregation function, it will not wrap cast function on it
		// internally. The reason of the special handling for `Column` is
		// that the `RetType` of `Column` refers to the `infoschema`, so we
		// need to set a new variable for it to avoid modifying the
		// definition in `infoschema`.
		trace_util_0.Count(_base_func_00000, 79)
		if col, ok := a.Args[i].(*expression.Column); ok {
			trace_util_0.Count(_base_func_00000, 83)
			col.RetType = types.NewFieldType(col.RetType.Tp)
		}
		// originTp is used when the the `Tp` of column is TypeFloat32 while
		// the type of the aggregation function is TypeFloat64.
		trace_util_0.Count(_base_func_00000, 80)
		originTp := a.Args[i].GetType().Tp
		*(a.Args[i].GetType()) = *(a.RetTp)
		a.Args[i].GetType().Tp = originTp
	}
}

var _base_func_00000 = "expression/aggregation/base_func.go"
