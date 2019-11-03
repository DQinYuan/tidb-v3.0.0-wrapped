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
	"bytes"
	"fmt"

	"github.com/pingcap/errors"
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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName model.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType  *types.FieldType
	Function builtinFunc
	hashcode []byte
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	trace_util_0.Count(_scalar_function_00000, 0)
	return sf.Function.getArgs()
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() sessionctx.Context {
	trace_util_0.Count(_scalar_function_00000, 1)
	return sf.Function.getCtx()
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	trace_util_0.Count(_scalar_function_00000, 2)
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.FuncName.L)
	for i, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 4)
		buffer.WriteString(arg.String())
		if i+1 != len(sf.GetArgs()) {
			trace_util_0.Count(_scalar_function_00000, 5)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_scalar_function_00000, 3)
	buffer.WriteString(")")
	return buffer.String()
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	trace_util_0.Count(_scalar_function_00000, 6)
	return []byte(fmt.Sprintf("\"%s\"", sf)), nil
}

// newFunctionImpl creates a new scalar function or constant.
func newFunctionImpl(ctx sessionctx.Context, fold bool, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	trace_util_0.Count(_scalar_function_00000, 7)
	if retType == nil {
		trace_util_0.Count(_scalar_function_00000, 14)
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction.")
	}
	trace_util_0.Count(_scalar_function_00000, 8)
	if funcName == ast.Cast {
		trace_util_0.Count(_scalar_function_00000, 15)
		return BuildCastFunction(ctx, args[0], retType), nil
	}
	trace_util_0.Count(_scalar_function_00000, 9)
	fc, ok := funcs[funcName]
	if !ok {
		trace_util_0.Count(_scalar_function_00000, 16)
		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", funcName)
	}
	trace_util_0.Count(_scalar_function_00000, 10)
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		trace_util_0.Count(_scalar_function_00000, 17)
		return nil, err
	}
	trace_util_0.Count(_scalar_function_00000, 11)
	if builtinRetTp := f.getRetTp(); builtinRetTp.Tp != mysql.TypeUnspecified || retType.Tp == mysql.TypeUnspecified {
		trace_util_0.Count(_scalar_function_00000, 18)
		retType = builtinRetTp
	}
	trace_util_0.Count(_scalar_function_00000, 12)
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	if fold {
		trace_util_0.Count(_scalar_function_00000, 19)
		return FoldConstant(sf), nil
	}
	trace_util_0.Count(_scalar_function_00000, 13)
	return sf, nil
}

// NewFunction creates a new scalar function or constant via a constant folding.
func NewFunction(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	trace_util_0.Count(_scalar_function_00000, 20)
	return newFunctionImpl(ctx, true, funcName, retType, args...)
}

// NewFunctionBase creates a new scalar function with no constant folding.
func NewFunctionBase(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	trace_util_0.Count(_scalar_function_00000, 21)
	return newFunctionImpl(ctx, false, funcName, retType, args...)
}

// NewFunctionInternal is similar to NewFunction, but do not returns error, should only be used internally.
func NewFunctionInternal(ctx sessionctx.Context, funcName string, retType *types.FieldType, args ...Expression) Expression {
	trace_util_0.Count(_scalar_function_00000, 22)
	expr, err := NewFunction(ctx, funcName, retType, args...)
	terror.Log(err)
	return expr
}

// ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	trace_util_0.Count(_scalar_function_00000, 23)
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		trace_util_0.Count(_scalar_function_00000, 25)
		result = append(result, col)
	}
	trace_util_0.Count(_scalar_function_00000, 24)
	return result
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	trace_util_0.Count(_scalar_function_00000, 26)
	return &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	trace_util_0.Count(_scalar_function_00000, 27)
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(ctx sessionctx.Context, e Expression) bool {
	trace_util_0.Count(_scalar_function_00000, 28)
	fun, ok := e.(*ScalarFunction)
	if !ok {
		trace_util_0.Count(_scalar_function_00000, 31)
		return false
	}
	trace_util_0.Count(_scalar_function_00000, 29)
	if sf.FuncName.L != fun.FuncName.L {
		trace_util_0.Count(_scalar_function_00000, 32)
		return false
	}
	trace_util_0.Count(_scalar_function_00000, 30)
	return sf.Function.equal(fun.Function)
}

// IsCorrelated implements Expression interface.
func (sf *ScalarFunction) IsCorrelated() bool {
	trace_util_0.Count(_scalar_function_00000, 33)
	for _, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 35)
		if arg.IsCorrelated() {
			trace_util_0.Count(_scalar_function_00000, 36)
			return true
		}
	}
	trace_util_0.Count(_scalar_function_00000, 34)
	return false
}

// ConstItem implements Expression interface.
func (sf *ScalarFunction) ConstItem() bool {
	trace_util_0.Count(_scalar_function_00000, 37)
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		trace_util_0.Count(_scalar_function_00000, 40)
		return false
	}
	trace_util_0.Count(_scalar_function_00000, 38)
	for _, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 41)
		if !arg.ConstItem() {
			trace_util_0.Count(_scalar_function_00000, 42)
			return false
		}
	}
	trace_util_0.Count(_scalar_function_00000, 39)
	return true
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schema *Schema) Expression {
	trace_util_0.Count(_scalar_function_00000, 43)
	for i, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 45)
		sf.GetArgs()[i] = arg.Decorrelate(schema)
	}
	trace_util_0.Count(_scalar_function_00000, 44)
	return sf
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row chunk.Row) (d types.Datum, err error) {
	trace_util_0.Count(_scalar_function_00000, 46)
	var (
		res    interface{}
		isNull bool
	)
	switch tp, evalType := sf.GetType(), sf.GetType().EvalType(); evalType {
	case types.ETInt:
		trace_util_0.Count(_scalar_function_00000, 49)
		var intRes int64
		intRes, isNull, err = sf.EvalInt(sf.GetCtx(), row)
		if mysql.HasUnsignedFlag(tp.Flag) {
			trace_util_0.Count(_scalar_function_00000, 56)
			res = uint64(intRes)
		} else {
			trace_util_0.Count(_scalar_function_00000, 57)
			{
				res = intRes
			}
		}
	case types.ETReal:
		trace_util_0.Count(_scalar_function_00000, 50)
		res, isNull, err = sf.EvalReal(sf.GetCtx(), row)
	case types.ETDecimal:
		trace_util_0.Count(_scalar_function_00000, 51)
		res, isNull, err = sf.EvalDecimal(sf.GetCtx(), row)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_scalar_function_00000, 52)
		res, isNull, err = sf.EvalTime(sf.GetCtx(), row)
	case types.ETDuration:
		trace_util_0.Count(_scalar_function_00000, 53)
		res, isNull, err = sf.EvalDuration(sf.GetCtx(), row)
	case types.ETJson:
		trace_util_0.Count(_scalar_function_00000, 54)
		res, isNull, err = sf.EvalJSON(sf.GetCtx(), row)
	case types.ETString:
		trace_util_0.Count(_scalar_function_00000, 55)
		res, isNull, err = sf.EvalString(sf.GetCtx(), row)
	}

	trace_util_0.Count(_scalar_function_00000, 47)
	if isNull || err != nil {
		trace_util_0.Count(_scalar_function_00000, 58)
		d.SetValue(nil)
		return d, err
	}
	trace_util_0.Count(_scalar_function_00000, 48)
	d.SetValue(res)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 59)
	return sf.Function.evalInt(row)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 60)
	return sf.Function.evalReal(row)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 61)
	return sf.Function.evalDecimal(row)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 62)
	return sf.Function.evalString(row)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 63)
	return sf.Function.evalTime(row)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 64)
	return sf.Function.evalDuration(row)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx sessionctx.Context, row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_scalar_function_00000, 65)
	return sf.Function.evalJSON(row)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode(sc *stmtctx.StatementContext) []byte {
	trace_util_0.Count(_scalar_function_00000, 66)
	if len(sf.hashcode) > 0 {
		trace_util_0.Count(_scalar_function_00000, 69)
		return sf.hashcode
	}
	trace_util_0.Count(_scalar_function_00000, 67)
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, hack.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 70)
		sf.hashcode = append(sf.hashcode, arg.HashCode(sc)...)
	}
	trace_util_0.Count(_scalar_function_00000, 68)
	return sf.hashcode
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schema *Schema) (Expression, error) {
	trace_util_0.Count(_scalar_function_00000, 71)
	newSf := sf.Clone()
	err := newSf.resolveIndices(schema)
	return newSf, err
}

func (sf *ScalarFunction) resolveIndices(schema *Schema) error {
	trace_util_0.Count(_scalar_function_00000, 72)
	for _, arg := range sf.GetArgs() {
		trace_util_0.Count(_scalar_function_00000, 74)
		err := arg.resolveIndices(schema)
		if err != nil {
			trace_util_0.Count(_scalar_function_00000, 75)
			return err
		}
	}
	trace_util_0.Count(_scalar_function_00000, 73)
	return nil
}

var _scalar_function_00000 = "expression/scalar_function.go"
