// Copyright 2017 PingCAP, Inc.
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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, Decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &castAsIntFunctionClass{}
	_ functionClass = &castAsRealFunctionClass{}
	_ functionClass = &castAsStringFunctionClass{}
	_ functionClass = &castAsDecimalFunctionClass{}
	_ functionClass = &castAsTimeFunctionClass{}
	_ functionClass = &castAsDurationFunctionClass{}
	_ functionClass = &castAsJSONFunctionClass{}
)

var (
	_ builtinFunc = &builtinCastIntAsIntSig{}
	_ builtinFunc = &builtinCastIntAsRealSig{}
	_ builtinFunc = &builtinCastIntAsStringSig{}
	_ builtinFunc = &builtinCastIntAsDecimalSig{}
	_ builtinFunc = &builtinCastIntAsTimeSig{}
	_ builtinFunc = &builtinCastIntAsDurationSig{}
	_ builtinFunc = &builtinCastIntAsJSONSig{}

	_ builtinFunc = &builtinCastRealAsIntSig{}
	_ builtinFunc = &builtinCastRealAsRealSig{}
	_ builtinFunc = &builtinCastRealAsStringSig{}
	_ builtinFunc = &builtinCastRealAsDecimalSig{}
	_ builtinFunc = &builtinCastRealAsTimeSig{}
	_ builtinFunc = &builtinCastRealAsDurationSig{}
	_ builtinFunc = &builtinCastRealAsJSONSig{}

	_ builtinFunc = &builtinCastDecimalAsIntSig{}
	_ builtinFunc = &builtinCastDecimalAsRealSig{}
	_ builtinFunc = &builtinCastDecimalAsStringSig{}
	_ builtinFunc = &builtinCastDecimalAsDecimalSig{}
	_ builtinFunc = &builtinCastDecimalAsTimeSig{}
	_ builtinFunc = &builtinCastDecimalAsDurationSig{}
	_ builtinFunc = &builtinCastDecimalAsJSONSig{}

	_ builtinFunc = &builtinCastStringAsIntSig{}
	_ builtinFunc = &builtinCastStringAsRealSig{}
	_ builtinFunc = &builtinCastStringAsStringSig{}
	_ builtinFunc = &builtinCastStringAsDecimalSig{}
	_ builtinFunc = &builtinCastStringAsTimeSig{}
	_ builtinFunc = &builtinCastStringAsDurationSig{}
	_ builtinFunc = &builtinCastStringAsJSONSig{}

	_ builtinFunc = &builtinCastTimeAsIntSig{}
	_ builtinFunc = &builtinCastTimeAsRealSig{}
	_ builtinFunc = &builtinCastTimeAsStringSig{}
	_ builtinFunc = &builtinCastTimeAsDecimalSig{}
	_ builtinFunc = &builtinCastTimeAsTimeSig{}
	_ builtinFunc = &builtinCastTimeAsDurationSig{}
	_ builtinFunc = &builtinCastTimeAsJSONSig{}

	_ builtinFunc = &builtinCastDurationAsIntSig{}
	_ builtinFunc = &builtinCastDurationAsRealSig{}
	_ builtinFunc = &builtinCastDurationAsStringSig{}
	_ builtinFunc = &builtinCastDurationAsDecimalSig{}
	_ builtinFunc = &builtinCastDurationAsTimeSig{}
	_ builtinFunc = &builtinCastDurationAsDurationSig{}
	_ builtinFunc = &builtinCastDurationAsJSONSig{}

	_ builtinFunc = &builtinCastJSONAsIntSig{}
	_ builtinFunc = &builtinCastJSONAsRealSig{}
	_ builtinFunc = &builtinCastJSONAsStringSig{}
	_ builtinFunc = &builtinCastJSONAsDecimalSig{}
	_ builtinFunc = &builtinCastJSONAsTimeSig{}
	_ builtinFunc = &builtinCastJSONAsDurationSig{}
	_ builtinFunc = &builtinCastJSONAsJSONSig{}
)

type castAsIntFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsIntFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 4)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 1)
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 5)
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsInt)
		return sig, nil
	}
	trace_util_0.Count(_builtin_cast_00000, 2)
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 6)
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsInt)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 7)
		sig = &builtinCastRealAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsInt)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 8)
		sig = &builtinCastDecimalAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsInt)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 9)
		sig = &builtinCastTimeAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsInt)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 10)
		sig = &builtinCastDurationAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsInt)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 11)
		sig = &builtinCastJSONAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsInt)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 12)
		sig = &builtinCastStringAsIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsInt)
	default:
		trace_util_0.Count(_builtin_cast_00000, 13)
		panic("unsupported types.EvalType in castAsIntFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 3)
	return sig, nil
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsRealFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 14)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 19)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 15)
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if IsBinaryLiteral(args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 20)
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
		return sig, nil
	}
	trace_util_0.Count(_builtin_cast_00000, 16)
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		trace_util_0.Count(_builtin_cast_00000, 21)
		argTp = types.ETInt
	} else {
		trace_util_0.Count(_builtin_cast_00000, 22)
		{
			argTp = args[0].GetType().EvalType()
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 17)
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 23)
		sig = &builtinCastIntAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsReal)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 24)
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 25)
		sig = &builtinCastDecimalAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsReal)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 26)
		sig = &builtinCastTimeAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsReal)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 27)
		sig = &builtinCastDurationAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsReal)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 28)
		sig = &builtinCastJSONAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsReal)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 29)
		sig = &builtinCastStringAsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsReal)
	default:
		trace_util_0.Count(_builtin_cast_00000, 30)
		panic("unsupported types.EvalType in castAsRealFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 18)
	return sig, nil
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDecimalFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 31)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 36)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 32)
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if IsBinaryLiteral(args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 37)
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
		return sig, nil
	}
	trace_util_0.Count(_builtin_cast_00000, 33)
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		trace_util_0.Count(_builtin_cast_00000, 38)
		argTp = types.ETInt
	} else {
		trace_util_0.Count(_builtin_cast_00000, 39)
		{
			argTp = args[0].GetType().EvalType()
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 34)
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 40)
		sig = &builtinCastIntAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsDecimal)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 41)
		sig = &builtinCastRealAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsDecimal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 42)
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 43)
		sig = &builtinCastTimeAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsDecimal)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 44)
		sig = &builtinCastDurationAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsDecimal)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 45)
		sig = &builtinCastJSONAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsDecimal)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 46)
		sig = &builtinCastStringAsDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsDecimal)
	default:
		trace_util_0.Count(_builtin_cast_00000, 47)
		panic("unsupported types.EvalType in castAsDecimalFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 35)
	return sig, nil
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsStringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 48)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 52)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 49)
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 53)
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
		return sig, nil
	}
	trace_util_0.Count(_builtin_cast_00000, 50)
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 54)
		sig = &builtinCastIntAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsString)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 55)
		sig = &builtinCastRealAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsString)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 56)
		sig = &builtinCastDecimalAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsString)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 57)
		sig = &builtinCastTimeAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsString)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 58)
		sig = &builtinCastDurationAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsString)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 59)
		sig = &builtinCastJSONAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsString)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 60)
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsString)
	default:
		trace_util_0.Count(_builtin_cast_00000, 61)
		panic("unsupported types.EvalType in castAsStringFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 51)
	return sig, nil
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 62)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 65)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 63)
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 66)
		sig = &builtinCastIntAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsTime)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 67)
		sig = &builtinCastRealAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsTime)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 68)
		sig = &builtinCastDecimalAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsTime)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 69)
		sig = &builtinCastTimeAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 70)
		sig = &builtinCastDurationAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsTime)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 71)
		sig = &builtinCastJSONAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsTime)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 72)
		sig = &builtinCastStringAsTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsTime)
	default:
		trace_util_0.Count(_builtin_cast_00000, 73)
		panic("unsupported types.EvalType in castAsTimeFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 64)
	return sig, nil
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDurationFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 74)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 77)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 75)
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 78)
		sig = &builtinCastIntAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsDuration)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 79)
		sig = &builtinCastRealAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsDuration)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 80)
		sig = &builtinCastDecimalAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsDuration)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 81)
		sig = &builtinCastTimeAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsDuration)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 82)
		sig = &builtinCastDurationAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 83)
		sig = &builtinCastJSONAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsDuration)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 84)
		sig = &builtinCastStringAsDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsDuration)
	default:
		trace_util_0.Count(_builtin_cast_00000, 85)
		panic("unsupported types.EvalType in castAsDurationFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 76)
	return sig, nil
}

type castAsJSONFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsJSONFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_cast_00000, 86)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 89)
		return nil, err
	}
	trace_util_0.Count(_builtin_cast_00000, 87)
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 90)
		sig = &builtinCastIntAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastIntAsJson)
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 91)
		sig = &builtinCastRealAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastRealAsJson)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 92)
		sig = &builtinCastDecimalAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDecimalAsJson)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 93)
		sig = &builtinCastTimeAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastTimeAsJson)
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 94)
		sig = &builtinCastDurationAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastDurationAsJson)
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 95)
		sig = &builtinCastJSONAsJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CastJsonAsJson)
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 96)
		sig = &builtinCastStringAsJSONSig{bf}
		sig.getRetTp().Flag |= mysql.ParseToJSONFlag
		sig.setPbCode(tipb.ScalarFuncSig_CastStringAsJson)
	default:
		trace_util_0.Count(_builtin_cast_00000, 97)
		panic("unsupported types.EvalType in castAsJSONFunctionClass")
	}
	trace_util_0.Count(_builtin_cast_00000, 88)
	return sig, nil
}

type builtinCastIntAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 98)
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 99)
	res, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		trace_util_0.Count(_builtin_cast_00000, 101)
		res = 0
	}
	trace_util_0.Count(_builtin_cast_00000, 100)
	return
}

type builtinCastIntAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 102)
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 103)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 106)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 104)
	if !mysql.HasUnsignedFlag(b.tp.Flag) {
		trace_util_0.Count(_builtin_cast_00000, 107)
		res = float64(val)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 108)
		if b.inUnion && val < 0 {
			trace_util_0.Count(_builtin_cast_00000, 109)
			res = 0
		} else {
			trace_util_0.Count(_builtin_cast_00000, 110)
			{
				var uVal uint64
				sc := b.ctx.GetSessionVars().StmtCtx
				uVal, err = types.ConvertIntToUint(sc, val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
				res = float64(uVal)
			}
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 105)
	return res, false, err
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 111)
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 112)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 115)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 113)
	if !mysql.HasUnsignedFlag(b.tp.Flag) && !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_cast_00000, 116)
		res = types.NewDecFromInt(val)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 117)
		if b.inUnion && val < 0 {
			trace_util_0.Count(_builtin_cast_00000, 118)
			res = &types.MyDecimal{}
		} else {
			trace_util_0.Count(_builtin_cast_00000, 119)
			{
				var uVal uint64
				sc := b.ctx.GetSessionVars().StmtCtx
				uVal, err = types.ConvertIntToUint(sc, val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
				if err != nil {
					trace_util_0.Count(_builtin_cast_00000, 121)
					return res, false, err
				}
				trace_util_0.Count(_builtin_cast_00000, 120)
				res = types.NewDecFromUint(uVal)
			}
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 114)
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, b.ctx.GetSessionVars().StmtCtx)
	return res, isNull, err
}

type builtinCastIntAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 122)
	newSig := &builtinCastIntAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 123)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 127)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 124)
	if !mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_cast_00000, 128)
		res = strconv.FormatInt(val, 10)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 129)
		{
			var uVal uint64
			sc := b.ctx.GetSessionVars().StmtCtx
			uVal, err = types.ConvertIntToUint(sc, val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
			if err != nil {
				trace_util_0.Count(_builtin_cast_00000, 131)
				return res, false, err
			}
			trace_util_0.Count(_builtin_cast_00000, 130)
			res = strconv.FormatUint(uVal, 10)
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 125)
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, b.ctx.GetSessionVars().StmtCtx, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 132)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 126)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastIntAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 133)
	newSig := &builtinCastIntAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 134)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 138)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 135)
	res, err = types.ParseTimeFromNum(b.ctx.GetSessionVars().StmtCtx, val, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 139)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 136)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 140)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_builtin_cast_00000, 137)
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 141)
	newSig := &builtinCastIntAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 142)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 145)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 143)
	dur, err := types.NumberToDuration(val, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 146)
		if types.ErrOverflow.Equal(err) {
			trace_util_0.Count(_builtin_cast_00000, 148)
			err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
		}
		trace_util_0.Count(_builtin_cast_00000, 147)
		return res, true, err
	}
	trace_util_0.Count(_builtin_cast_00000, 144)
	return dur, false, err
}

type builtinCastIntAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 149)
	newSig := &builtinCastIntAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 150)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 153)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 151)
	if mysql.HasIsBooleanFlag(b.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_cast_00000, 154)
		res = json.CreateBinary(val != 0)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 155)
		if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
			trace_util_0.Count(_builtin_cast_00000, 156)
			res = json.CreateBinary(uint64(val))
		} else {
			trace_util_0.Count(_builtin_cast_00000, 157)
			{
				res = json.CreateBinary(val)
			}
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 152)
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 158)
	newSig := &builtinCastRealAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 159)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return json.CreateBinary(val), isNull, err
}

type builtinCastDecimalAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 160)
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_builtin_cast_00000, 161)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 164)
		return json.BinaryJSON{}, true, err
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	trace_util_0.Count(_builtin_cast_00000, 162)
	f64, err := val.ToFloat64()
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 165)
		return json.BinaryJSON{}, true, err
	}
	trace_util_0.Count(_builtin_cast_00000, 163)
	return json.CreateBinary(f64), isNull, err
}

type builtinCastStringAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 166)
	newSig := &builtinCastStringAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 167)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 170)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 168)
	if mysql.HasParseToJSONFlag(b.tp.Flag) {
		trace_util_0.Count(_builtin_cast_00000, 171)
		res, err = json.ParseBinaryFromString(val)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 172)
		{
			res = json.CreateBinary(val)
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 169)
	return res, false, err
}

type builtinCastDurationAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 173)
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 174)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 176)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 175)
	val.Fsp = types.MaxFsp
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 177)
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 178)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 181)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 179)
	if val.Type == mysql.TypeDatetime || val.Type == mysql.TypeTimestamp {
		trace_util_0.Count(_builtin_cast_00000, 182)
		val.Fsp = types.MaxFsp
	}
	trace_util_0.Count(_builtin_cast_00000, 180)
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 183)
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 184)
	res, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		trace_util_0.Count(_builtin_cast_00000, 186)
		res = 0
	}
	trace_util_0.Count(_builtin_cast_00000, 185)
	return
}

type builtinCastRealAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 187)
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 188)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 191)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 189)
	if !mysql.HasUnsignedFlag(b.tp.Flag) {
		trace_util_0.Count(_builtin_cast_00000, 192)
		res, err = types.ConvertFloatToInt(val, types.IntergerSignedLowerBound(mysql.TypeLonglong), types.IntergerSignedUpperBound(mysql.TypeLonglong), mysql.TypeDouble)
	} else {
		trace_util_0.Count(_builtin_cast_00000, 193)
		if b.inUnion && val < 0 {
			trace_util_0.Count(_builtin_cast_00000, 194)
			res = 0
		} else {
			trace_util_0.Count(_builtin_cast_00000, 195)
			{
				var uintVal uint64
				sc := b.ctx.GetSessionVars().StmtCtx
				uintVal, err = types.ConvertFloatToUint(sc, val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeDouble)
				res = int64(uintVal)
			}
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 190)
	return res, isNull, err
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 196)
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 197)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 200)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 198)
	res = new(types.MyDecimal)
	if !b.inUnion || val >= 0 {
		trace_util_0.Count(_builtin_cast_00000, 201)
		err = res.FromFloat64(val)
		if err != nil {
			trace_util_0.Count(_builtin_cast_00000, 202)
			return res, false, err
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 199)
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, b.ctx.GetSessionVars().StmtCtx)
	return res, false, err
}

type builtinCastRealAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 203)
	newSig := &builtinCastRealAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 204)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 208)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_cast_00000, 205)
	bits := 64
	if b.args[0].GetType().Tp == mysql.TypeFloat {
		trace_util_0.Count(_builtin_cast_00000, 209)
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}
	trace_util_0.Count(_builtin_cast_00000, 206)
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, bits), b.tp, b.ctx.GetSessionVars().StmtCtx, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 210)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 207)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastRealAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 211)
	newSig := &builtinCastRealAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_cast_00000, 212)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 216)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_cast_00000, 213)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err := types.ParseTime(sc, strconv.FormatFloat(val, 'f', -1, 64), b.tp.Tp, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 217)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 214)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 218)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_builtin_cast_00000, 215)
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 219)
	newSig := &builtinCastRealAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 220)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 222)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 221)
	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, strconv.FormatFloat(val, 'f', -1, 64), b.tp.Decimal)
	return res, false, err
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 223)
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 224)
	evalDecimal, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 227)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 225)
	res = &types.MyDecimal{}
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && evalDecimal.IsNegative()) {
		trace_util_0.Count(_builtin_cast_00000, 228)
		*res = *evalDecimal
	}
	trace_util_0.Count(_builtin_cast_00000, 226)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 229)
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 230)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 235)
		return res, isNull, err
	}

	// Round is needed for both unsigned and signed.
	trace_util_0.Count(_builtin_cast_00000, 231)
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 236)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_cast_00000, 232)
	if !mysql.HasUnsignedFlag(b.tp.Flag) {
		trace_util_0.Count(_builtin_cast_00000, 237)
		res, err = to.ToInt()
	} else {
		trace_util_0.Count(_builtin_cast_00000, 238)
		if b.inUnion && to.IsNegative() {
			trace_util_0.Count(_builtin_cast_00000, 239)
			res = 0
		} else {
			trace_util_0.Count(_builtin_cast_00000, 240)
			{
				var uintRes uint64
				uintRes, err = to.ToUint()
				res = int64(uintRes)
			}
		}
	}

	trace_util_0.Count(_builtin_cast_00000, 233)
	if types.ErrOverflow.Equal(err) {
		trace_util_0.Count(_builtin_cast_00000, 241)
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, warnErr)
	}

	trace_util_0.Count(_builtin_cast_00000, 234)
	return res, false, err
}

type builtinCastDecimalAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 242)
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 243)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 246)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 244)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, sc, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 247)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 245)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 248)
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 249)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 252)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 250)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && val.IsNegative() {
		trace_util_0.Count(_builtin_cast_00000, 253)
		res = 0
	} else {
		trace_util_0.Count(_builtin_cast_00000, 254)
		{
			res, err = val.ToFloat64()
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 251)
	return res, false, err
}

type builtinCastDecimalAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 255)
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 256)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 260)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 257)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTimeFromFloatString(sc, string(val.ToString()), b.tp.Tp, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 261)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 258)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 262)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_builtin_cast_00000, 259)
	return res, false, err
}

type builtinCastDecimalAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 263)
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 264)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 267)
		return res, true, err
	}
	trace_util_0.Count(_builtin_cast_00000, 265)
	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, string(val.ToString()), b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_cast_00000, 268)
		err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			trace_util_0.Count(_builtin_cast_00000, 269)
			return res, true, err
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 266)
	return res, false, err
}

type builtinCastStringAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 270)
	newSig := &builtinCastStringAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 271)
	res, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 274)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 272)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 275)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 273)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 276)
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (b *builtinCastStringAsIntSig) handleOverflow(origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	trace_util_0.Count(_builtin_cast_00000, 277)
	res, err = origRes, origErr
	if err == nil {
		trace_util_0.Count(_builtin_cast_00000, 280)
		return
	}

	trace_util_0.Count(_builtin_cast_00000, 278)
	sc := b.ctx.GetSessionVars().StmtCtx
	if sc.InSelectStmt && types.ErrOverflow.Equal(origErr) {
		trace_util_0.Count(_builtin_cast_00000, 281)
		if isNegative {
			trace_util_0.Count(_builtin_cast_00000, 283)
			res = math.MinInt64
		} else {
			trace_util_0.Count(_builtin_cast_00000, 284)
			{
				uval := uint64(math.MaxUint64)
				res = int64(uval)
			}
		}
		trace_util_0.Count(_builtin_cast_00000, 282)
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", origStr)
		err = sc.HandleOverflow(origErr, warnErr)
	}
	trace_util_0.Count(_builtin_cast_00000, 279)
	return
}

func (b *builtinCastStringAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 285)
	if b.args[0].GetType().Hybrid() || IsBinaryLiteral(b.args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 290)
		return b.args[0].EvalInt(b.ctx, row)
	}
	trace_util_0.Count(_builtin_cast_00000, 286)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 291)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_cast_00000, 287)
	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' {
		trace_util_0.Count(_builtin_cast_00000, 292) // negative number
		isNegative = true
	}

	trace_util_0.Count(_builtin_cast_00000, 288)
	var ures uint64
	sc := b.ctx.GetSessionVars().StmtCtx
	if !isNegative {
		trace_util_0.Count(_builtin_cast_00000, 293)
		ures, err = types.StrToUint(sc, val)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.tp.Flag) && ures > uint64(math.MaxInt64) {
			trace_util_0.Count(_builtin_cast_00000, 294)
			sc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	} else {
		trace_util_0.Count(_builtin_cast_00000, 295)
		if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) {
			trace_util_0.Count(_builtin_cast_00000, 296)
			res = 0
		} else {
			trace_util_0.Count(_builtin_cast_00000, 297)
			{
				res, err = types.StrToInt(sc, val)
				if err == nil && mysql.HasUnsignedFlag(b.tp.Flag) {
					trace_util_0.Count(_builtin_cast_00000, 298)
					// If overflow, don't append this warnings
					sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
				}
			}
		}
	}

	trace_util_0.Count(_builtin_cast_00000, 289)
	res, err = b.handleOverflow(res, val, err, isNegative)
	return res, false, err
}

type builtinCastStringAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 299)
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 300)
	if IsBinaryLiteral(b.args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 305)
		return b.args[0].EvalReal(b.ctx, row)
	}
	trace_util_0.Count(_builtin_cast_00000, 301)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 306)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 302)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.StrToFloat(sc, val)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 307)
		return 0, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 303)
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		trace_util_0.Count(_builtin_cast_00000, 308)
		res = 0
	}
	trace_util_0.Count(_builtin_cast_00000, 304)
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 309)
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 310)
	if IsBinaryLiteral(b.args[0]) {
		trace_util_0.Count(_builtin_cast_00000, 314)
		return b.args[0].EvalDecimal(b.ctx, row)
	}
	trace_util_0.Count(_builtin_cast_00000, 311)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 315)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 312)
	res = new(types.MyDecimal)
	sc := b.ctx.GetSessionVars().StmtCtx
	if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res.IsNegative()) {
		trace_util_0.Count(_builtin_cast_00000, 316)
		err = sc.HandleTruncate(res.FromString([]byte(val)))
		if err != nil {
			trace_util_0.Count(_builtin_cast_00000, 317)
			return res, false, err
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 313)
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastStringAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 318)
	newSig := &builtinCastStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 319)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 323)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 320)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTime(sc, val, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 324)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 321)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 325)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_builtin_cast_00000, 322)
	return res, false, nil
}

type builtinCastStringAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 326)
	newSig := &builtinCastStringAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 327)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 330)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 328)
	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, val, b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_cast_00000, 331)
		sc := b.ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			trace_util_0.Count(_builtin_cast_00000, 332)
			return res, true, err
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 329)
	return res, false, err
}

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 333)
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 334)
	res, isNull, err = b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 338)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_cast_00000, 335)
	sc := b.ctx.GetSessionVars().StmtCtx
	if res, err = res.Convert(sc, b.tp.Tp); err != nil {
		trace_util_0.Count(_builtin_cast_00000, 339)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 336)
	res, err = res.RoundFrac(sc, b.tp.Decimal)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 340)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
		res.Type = b.tp.Tp
	}
	trace_util_0.Count(_builtin_cast_00000, 337)
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 341)
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 342)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 345)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 343)
	sc := b.ctx.GetSessionVars().StmtCtx
	t, err := val.RoundFrac(sc, types.DefaultFsp)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 346)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 344)
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 347)
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 348)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 350)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 349)
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 351)
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 352)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 354)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 353)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 355)
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 356)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 359)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 357)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 360)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 358)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 361)
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 362)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 365)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 363)
	res, err = val.ConvertToDuration()
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 366)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 364)
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 367)
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 368)
	res, isNull, err = b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 370)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 369)
	res, err = res.RoundFrac(b.tp.Decimal)
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 371)
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 372)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 375)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 373)
	dur, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 376)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 374)
	res, err = dur.ToNumber().ToInt()
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 377)
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 378)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 380)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 379)
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 381)
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 382)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 384)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 383)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 385)
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 386)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 389)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 387)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc, false)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 390)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 388)
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx sessionctx.Context) (string, bool, error) {
	trace_util_0.Count(_builtin_cast_00000, 391)
	flen := tp.Flen
	if tp.Tp == mysql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		trace_util_0.Count(_builtin_cast_00000, 393)
		sc := ctx.GetSessionVars().StmtCtx
		valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
		maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			trace_util_0.Count(_builtin_cast_00000, 396)
			return "", false, err
		}
		trace_util_0.Count(_builtin_cast_00000, 394)
		if uint64(flen) > maxAllowedPacket {
			trace_util_0.Count(_builtin_cast_00000, 397)
			sc.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("cast_as_binary", maxAllowedPacket))
			return "", true, nil
		}
		trace_util_0.Count(_builtin_cast_00000, 395)
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	trace_util_0.Count(_builtin_cast_00000, 392)
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 398)
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 399)
	val, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 402)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 400)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = val.ConvertToTime(sc, b.tp.Tp)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 403)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 401)
	res, err = res.RoundFrac(sc, b.tp.Decimal)
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 404)
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 405)
	return b.args[0].EvalJSON(b.ctx, row)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 406)
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 407)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 409)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 408)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToInt(sc, val, mysql.HasUnsignedFlag(b.tp.Flag))
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 410)
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 411)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 413)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 412)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToFloat(sc, val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 414)
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 415)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 418)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 416)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToDecimal(sc, val)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 419)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 417)
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 420)
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 421)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 423)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 422)
	return val.String(), false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 424)
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 425)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 430)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 426)
	s, err := val.Unquote()
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 431)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 427)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTime(sc, s, b.tp.Tp, b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 432)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_cast_00000, 428)
	if b.tp.Tp == mysql.TypeDate {
		trace_util_0.Count(_builtin_cast_00000, 433)
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_builtin_cast_00000, 429)
	return
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_cast_00000, 434)
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_cast_00000, 435)
	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_cast_00000, 439)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_cast_00000, 436)
	s, err := val.Unquote()
	if err != nil {
		trace_util_0.Count(_builtin_cast_00000, 440)
		return res, false, err
	}
	trace_util_0.Count(_builtin_cast_00000, 437)
	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, b.tp.Decimal)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_cast_00000, 441)
		sc := b.ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	trace_util_0.Count(_builtin_cast_00000, 438)
	return
}

// inCastContext is session key type that indicates whether executing
// in special cast context that negative unsigned num will be zero.
type inCastContext int

func (i inCastContext) String() string {
	trace_util_0.Count(_builtin_cast_00000, 442)
	return "__cast_ctx"
}

// inUnionCastContext is session key value that indicates whether executing in
// union cast context.
// @see BuildCastFunction4Union
const inUnionCastContext inCastContext = 0

// hasSpecialCast checks if this expr has its own special cast function.
// for example(#9713): when doing arithmetic using results of function DayName,
// "Monday" should be regarded as 0, "Tuesday" should be regarded as 1 and so on.
func hasSpecialCast(ctx sessionctx.Context, expr Expression, tp *types.FieldType) bool {
	trace_util_0.Count(_builtin_cast_00000, 443)
	switch f := expr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_builtin_cast_00000, 445)
		switch f.FuncName.L {
		case ast.DayName:
			trace_util_0.Count(_builtin_cast_00000, 446)
			switch tp.EvalType() {
			case types.ETInt, types.ETReal:
				trace_util_0.Count(_builtin_cast_00000, 447)
				return true
			}
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 444)
	return false
}

// BuildCastFunction4Union build a implicitly CAST ScalarFunction from the Union
// Expression.
func BuildCastFunction4Union(ctx sessionctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	trace_util_0.Count(_builtin_cast_00000, 448)
	ctx.SetValue(inUnionCastContext, struct{}{})
	defer func() {
		trace_util_0.Count(_builtin_cast_00000, 450)
		ctx.SetValue(inUnionCastContext, nil)
	}()
	trace_util_0.Count(_builtin_cast_00000, 449)
	return BuildCastFunction(ctx, expr, tp)
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx sessionctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	trace_util_0.Count(_builtin_cast_00000, 451)
	if hasSpecialCast(ctx, expr, tp) {
		trace_util_0.Count(_builtin_cast_00000, 455)
		return expr
	}

	trace_util_0.Count(_builtin_cast_00000, 452)
	var fc functionClass
	switch tp.EvalType() {
	case types.ETInt:
		trace_util_0.Count(_builtin_cast_00000, 456)
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_cast_00000, 457)
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETReal:
		trace_util_0.Count(_builtin_cast_00000, 458)
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 459)
		fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		trace_util_0.Count(_builtin_cast_00000, 460)
		fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		trace_util_0.Count(_builtin_cast_00000, 461)
		fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETString:
		trace_util_0.Count(_builtin_cast_00000, 462)
		fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	}
	trace_util_0.Count(_builtin_cast_00000, 453)
	f, err := fc.getFunction(ctx, []Expression{expr})
	terror.Log(err)
	res = &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which
	// would affect the evaluation of it.
	if tp.EvalType() != types.ETJson {
		trace_util_0.Count(_builtin_cast_00000, 463)
		res = FoldConstant(res)
	}
	trace_util_0.Count(_builtin_cast_00000, 454)
	return res
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type of expr is not
// type int, otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 464)
	if expr.GetType().EvalType() == types.ETInt {
		trace_util_0.Count(_builtin_cast_00000, 466)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 465)
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen, tp.Decimal = expr.GetType().Flen, 0
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type of expr is not
// type real, otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 467)
	if expr.GetType().EvalType() == types.ETReal {
		trace_util_0.Count(_builtin_cast_00000, 469)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 468)
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type of expr is
// not type decimal, otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 470)
	if expr.GetType().EvalType() == types.ETDecimal {
		trace_util_0.Count(_builtin_cast_00000, 472)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 471)
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.Flen, tp.Decimal = expr.GetType().Flen, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type of expr is
// not type string, otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 473)
	exprTp := expr.GetType()
	if exprTp.EvalType() == types.ETString {
		trace_util_0.Count(_builtin_cast_00000, 477)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 474)
	argLen := exprTp.Flen
	// If expr is decimal, we should take the decimal point and negative sign
	// into consideration, so we set `expr.GetType().Flen + 2` as the `argLen`.
	// Since the length of float and double is not accurate, we do not handle
	// them.
	if exprTp.Tp == mysql.TypeNewDecimal && argLen != types.UnspecifiedFsp {
		trace_util_0.Count(_builtin_cast_00000, 478)
		argLen += 2
	}
	trace_util_0.Count(_builtin_cast_00000, 475)
	if exprTp.EvalType() == types.ETInt {
		trace_util_0.Count(_builtin_cast_00000, 479)
		argLen = mysql.MaxIntWidth
	}
	trace_util_0.Count(_builtin_cast_00000, 476)
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.Charset, tp.Collate = charset.GetDefaultCharsetAndCollate()
	tp.Flen, tp.Decimal = argLen, types.UnspecifiedLength
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type of expr is not
// same as type of the specified `tp` , otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx sessionctx.Context, expr Expression, tp *types.FieldType) Expression {
	trace_util_0.Count(_builtin_cast_00000, 480)
	exprTp := expr.GetType().Tp
	if tp.Tp == exprTp {
		trace_util_0.Count(_builtin_cast_00000, 484)
		return expr
	} else {
		trace_util_0.Count(_builtin_cast_00000, 485)
		if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.Tp == mysql.TypeDatetime {
			trace_util_0.Count(_builtin_cast_00000, 486)
			return expr
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 481)
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration:
		trace_util_0.Count(_builtin_cast_00000, 487)
		tp.Decimal = x.Decimal
	default:
		trace_util_0.Count(_builtin_cast_00000, 488)
		tp.Decimal = types.MaxFsp
	}
	trace_util_0.Count(_builtin_cast_00000, 482)
	switch tp.Tp {
	case mysql.TypeDate:
		trace_util_0.Count(_builtin_cast_00000, 489)
		tp.Flen = mysql.MaxDateWidth
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_builtin_cast_00000, 490)
		tp.Flen = mysql.MaxDatetimeWidthNoFsp
		if tp.Decimal > 0 {
			trace_util_0.Count(_builtin_cast_00000, 491)
			tp.Flen = tp.Flen + 1 + tp.Decimal
		}
	}
	trace_util_0.Count(_builtin_cast_00000, 483)
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type of expr is
// not type duration, otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 492)
	if expr.GetType().Tp == mysql.TypeDuration {
		trace_util_0.Count(_builtin_cast_00000, 496)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 493)
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate:
		trace_util_0.Count(_builtin_cast_00000, 497)
		tp.Decimal = x.Decimal
	default:
		trace_util_0.Count(_builtin_cast_00000, 498)
		tp.Decimal = types.MaxFsp
	}
	trace_util_0.Count(_builtin_cast_00000, 494)
	tp.Flen = mysql.MaxDurationWidthNoFsp
	if tp.Decimal > 0 {
		trace_util_0.Count(_builtin_cast_00000, 499)
		tp.Flen = tp.Flen + 1 + tp.Decimal
	}
	trace_util_0.Count(_builtin_cast_00000, 495)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type of expr is not
// type json, otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx sessionctx.Context, expr Expression) Expression {
	trace_util_0.Count(_builtin_cast_00000, 500)
	if expr.GetType().Tp == mysql.TypeJSON && !mysql.HasParseToJSONFlag(expr.GetType().Flag) {
		trace_util_0.Count(_builtin_cast_00000, 502)
		return expr
	}
	trace_util_0.Count(_builtin_cast_00000, 501)
	tp := &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flen:    12582912, // FIXME: Here the Flen is not trusted.
		Decimal: 0,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Flag:    mysql.BinaryFlag,
	}
	return BuildCastFunction(ctx, expr, tp)
}

var _builtin_cast_00000 = "expression/builtin_cast.go"
