// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &absFunctionClass{}
	_ functionClass = &roundFunctionClass{}
	_ functionClass = &ceilFunctionClass{}
	_ functionClass = &floorFunctionClass{}
	_ functionClass = &logFunctionClass{}
	_ functionClass = &log2FunctionClass{}
	_ functionClass = &log10FunctionClass{}
	_ functionClass = &randFunctionClass{}
	_ functionClass = &powFunctionClass{}
	_ functionClass = &convFunctionClass{}
	_ functionClass = &crc32FunctionClass{}
	_ functionClass = &signFunctionClass{}
	_ functionClass = &sqrtFunctionClass{}
	_ functionClass = &acosFunctionClass{}
	_ functionClass = &asinFunctionClass{}
	_ functionClass = &atanFunctionClass{}
	_ functionClass = &cosFunctionClass{}
	_ functionClass = &cotFunctionClass{}
	_ functionClass = &degreesFunctionClass{}
	_ functionClass = &expFunctionClass{}
	_ functionClass = &piFunctionClass{}
	_ functionClass = &radiansFunctionClass{}
	_ functionClass = &sinFunctionClass{}
	_ functionClass = &tanFunctionClass{}
	_ functionClass = &truncateFunctionClass{}
)

var (
	_ builtinFunc = &builtinAbsRealSig{}
	_ builtinFunc = &builtinAbsIntSig{}
	_ builtinFunc = &builtinAbsUIntSig{}
	_ builtinFunc = &builtinAbsDecSig{}
	_ builtinFunc = &builtinRoundRealSig{}
	_ builtinFunc = &builtinRoundIntSig{}
	_ builtinFunc = &builtinRoundDecSig{}
	_ builtinFunc = &builtinRoundWithFracRealSig{}
	_ builtinFunc = &builtinRoundWithFracIntSig{}
	_ builtinFunc = &builtinRoundWithFracDecSig{}
	_ builtinFunc = &builtinCeilRealSig{}
	_ builtinFunc = &builtinCeilIntToDecSig{}
	_ builtinFunc = &builtinCeilIntToIntSig{}
	_ builtinFunc = &builtinCeilDecToIntSig{}
	_ builtinFunc = &builtinCeilDecToDecSig{}
	_ builtinFunc = &builtinFloorRealSig{}
	_ builtinFunc = &builtinFloorIntToDecSig{}
	_ builtinFunc = &builtinFloorIntToIntSig{}
	_ builtinFunc = &builtinFloorDecToIntSig{}
	_ builtinFunc = &builtinFloorDecToDecSig{}
	_ builtinFunc = &builtinLog1ArgSig{}
	_ builtinFunc = &builtinLog2ArgsSig{}
	_ builtinFunc = &builtinLog2Sig{}
	_ builtinFunc = &builtinLog10Sig{}
	_ builtinFunc = &builtinRandSig{}
	_ builtinFunc = &builtinRandWithSeedSig{}
	_ builtinFunc = &builtinPowSig{}
	_ builtinFunc = &builtinConvSig{}
	_ builtinFunc = &builtinCRC32Sig{}
	_ builtinFunc = &builtinSignSig{}
	_ builtinFunc = &builtinSqrtSig{}
	_ builtinFunc = &builtinAcosSig{}
	_ builtinFunc = &builtinAsinSig{}
	_ builtinFunc = &builtinAtan1ArgSig{}
	_ builtinFunc = &builtinAtan2ArgsSig{}
	_ builtinFunc = &builtinCosSig{}
	_ builtinFunc = &builtinCotSig{}
	_ builtinFunc = &builtinDegreesSig{}
	_ builtinFunc = &builtinExpSig{}
	_ builtinFunc = &builtinPISig{}
	_ builtinFunc = &builtinRadiansSig{}
	_ builtinFunc = &builtinSinSig{}
	_ builtinFunc = &builtinTanSig{}
	_ builtinFunc = &builtinTruncateIntSig{}
	_ builtinFunc = &builtinTruncateRealSig{}
	_ builtinFunc = &builtinTruncateDecimalSig{}
	_ builtinFunc = &builtinTruncateUintSig{}
)

type absFunctionClass struct {
	baseFunctionClass
}

func (c *absFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 6)
		return nil, c.verifyArgs(args)
	}

	trace_util_0.Count(_builtin_math_00000, 1)
	argFieldTp := args[0].GetType()
	argTp := argFieldTp.EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		trace_util_0.Count(_builtin_math_00000, 7)
		argTp = types.ETReal
	}
	trace_util_0.Count(_builtin_math_00000, 2)
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, argTp)
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		trace_util_0.Count(_builtin_math_00000, 8)
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_builtin_math_00000, 3)
	if argTp == types.ETReal {
		trace_util_0.Count(_builtin_math_00000, 9)
		bf.tp.Flen, bf.tp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDouble)
	} else {
		trace_util_0.Count(_builtin_math_00000, 10)
		{
			bf.tp.Flen = argFieldTp.Flen
			bf.tp.Decimal = argFieldTp.Decimal
		}
	}
	trace_util_0.Count(_builtin_math_00000, 4)
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_math_00000, 11)
		if mysql.HasUnsignedFlag(argFieldTp.Flag) {
			trace_util_0.Count(_builtin_math_00000, 15)
			sig = &builtinAbsUIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AbsUInt)
		} else {
			trace_util_0.Count(_builtin_math_00000, 16)
			{
				sig = &builtinAbsIntSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_AbsInt)
			}
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_math_00000, 12)
		sig = &builtinAbsDecSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsDecimal)
	case types.ETReal:
		trace_util_0.Count(_builtin_math_00000, 13)
		sig = &builtinAbsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsReal)
	default:
		trace_util_0.Count(_builtin_math_00000, 14)
		panic("unexpected argTp")
	}
	trace_util_0.Count(_builtin_math_00000, 5)
	return sig, nil
}

type builtinAbsRealSig struct {
	baseBuiltinFunc
}

func (b *builtinAbsRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 17)
	newSig := &builtinAbsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 18)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 20)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 19)
	return math.Abs(val), false, nil
}

type builtinAbsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinAbsIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 21)
	newSig := &builtinAbsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 22)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 26)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 23)
	if val >= 0 {
		trace_util_0.Count(_builtin_math_00000, 27)
		return val, false, nil
	}
	trace_util_0.Count(_builtin_math_00000, 24)
	if val == math.MinInt64 {
		trace_util_0.Count(_builtin_math_00000, 28)
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("abs(%d)", val))
	}
	trace_util_0.Count(_builtin_math_00000, 25)
	return -val, false, nil
}

type builtinAbsUIntSig struct {
	baseBuiltinFunc
}

func (b *builtinAbsUIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 29)
	newSig := &builtinAbsUIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsUIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 30)
	return b.args[0].EvalInt(b.ctx, row)
}

type builtinAbsDecSig struct {
	baseBuiltinFunc
}

func (b *builtinAbsDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 31)
	newSig := &builtinAbsDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 32)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 35)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 33)
	to := new(types.MyDecimal)
	if !val.IsNegative() {
		trace_util_0.Count(_builtin_math_00000, 36)
		*to = *val
	} else {
		trace_util_0.Count(_builtin_math_00000, 37)
		{
			if err = types.DecimalSub(new(types.MyDecimal), val, to); err != nil {
				trace_util_0.Count(_builtin_math_00000, 38)
				return nil, true, err
			}
		}
	}
	trace_util_0.Count(_builtin_math_00000, 34)
	return to, false, nil
}

func (c *roundFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 39)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 45)
		return nil, c.verifyArgs(args)
	}
	trace_util_0.Count(_builtin_math_00000, 40)
	argTp := args[0].GetType().EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		trace_util_0.Count(_builtin_math_00000, 46)
		argTp = types.ETReal
	}
	trace_util_0.Count(_builtin_math_00000, 41)
	argTps := []types.EvalType{argTp}
	if len(args) > 1 {
		trace_util_0.Count(_builtin_math_00000, 47)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_math_00000, 42)
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, argTps...)
	argFieldTp := args[0].GetType()
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		trace_util_0.Count(_builtin_math_00000, 48)
		bf.tp.Flag |= mysql.UnsignedFlag
	}

	trace_util_0.Count(_builtin_math_00000, 43)
	bf.tp.Flen = argFieldTp.Flen
	bf.tp.Decimal = calculateDecimal4RoundAndTruncate(ctx, args, argTp)

	var sig builtinFunc
	if len(args) > 1 {
		trace_util_0.Count(_builtin_math_00000, 49)
		switch argTp {
		case types.ETInt:
			trace_util_0.Count(_builtin_math_00000, 50)
			sig = &builtinRoundWithFracIntSig{bf}
		case types.ETDecimal:
			trace_util_0.Count(_builtin_math_00000, 51)
			sig = &builtinRoundWithFracDecSig{bf}
		case types.ETReal:
			trace_util_0.Count(_builtin_math_00000, 52)
			sig = &builtinRoundWithFracRealSig{bf}
		default:
			trace_util_0.Count(_builtin_math_00000, 53)
			panic("unexpected argTp")
		}
	} else {
		trace_util_0.Count(_builtin_math_00000, 54)
		{
			switch argTp {
			case types.ETInt:
				trace_util_0.Count(_builtin_math_00000, 55)
				sig = &builtinRoundIntSig{bf}
			case types.ETDecimal:
				trace_util_0.Count(_builtin_math_00000, 56)
				sig = &builtinRoundDecSig{bf}
			case types.ETReal:
				trace_util_0.Count(_builtin_math_00000, 57)
				sig = &builtinRoundRealSig{bf}
			default:
				trace_util_0.Count(_builtin_math_00000, 58)
				panic("unexpected argTp")
			}
		}
	}
	trace_util_0.Count(_builtin_math_00000, 44)
	return sig, nil
}

// calculateDecimal4RoundAndTruncate calculates tp.decimals of round/truncate func.
func calculateDecimal4RoundAndTruncate(ctx sessionctx.Context, args []Expression, retType types.EvalType) int {
	trace_util_0.Count(_builtin_math_00000, 59)
	if retType == types.ETInt || len(args) <= 1 {
		trace_util_0.Count(_builtin_math_00000, 64)
		return 0
	}
	trace_util_0.Count(_builtin_math_00000, 60)
	secondConst, secondIsConst := args[1].(*Constant)
	if !secondIsConst {
		trace_util_0.Count(_builtin_math_00000, 65)
		return args[0].GetType().Decimal
	}
	trace_util_0.Count(_builtin_math_00000, 61)
	argDec, isNull, err := secondConst.EvalInt(ctx, chunk.Row{})
	if err != nil || isNull || argDec < 0 {
		trace_util_0.Count(_builtin_math_00000, 66)
		return 0
	}
	trace_util_0.Count(_builtin_math_00000, 62)
	if argDec > mysql.MaxDecimalScale {
		trace_util_0.Count(_builtin_math_00000, 67)
		return mysql.MaxDecimalScale
	}
	trace_util_0.Count(_builtin_math_00000, 63)
	return int(argDec)
}

type builtinRoundRealSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 68)
	newSig := &builtinRoundRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 69)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 71)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 70)
	return types.Round(val, 0), false, nil
}

type builtinRoundIntSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 72)
	newSig := &builtinRoundIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 73)
	return b.args[0].EvalInt(b.ctx, row)
}

type builtinRoundDecSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 74)
	newSig := &builtinRoundDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 75)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 78)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 76)
	to := new(types.MyDecimal)
	if err = val.Round(to, 0, types.ModeHalfEven); err != nil {
		trace_util_0.Count(_builtin_math_00000, 79)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_math_00000, 77)
	return to, false, nil
}

type builtinRoundWithFracRealSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundWithFracRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 80)
	newSig := &builtinRoundWithFracRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 81)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 84)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 82)
	frac, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 85)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 83)
	return types.Round(val, int(frac)), false, nil
}

type builtinRoundWithFracIntSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundWithFracIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 86)
	newSig := &builtinRoundWithFracIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 87)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 90)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 88)
	frac, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 91)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 89)
	return int64(types.Round(float64(val), int(frac))), false, nil
}

type builtinRoundWithFracDecSig struct {
	baseBuiltinFunc
}

func (b *builtinRoundWithFracDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 92)
	newSig := &builtinRoundWithFracDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 93)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 97)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 94)
	frac, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 98)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 95)
	to := new(types.MyDecimal)
	if err = val.Round(to, mathutil.Min(int(frac), b.tp.Decimal), types.ModeHalfEven); err != nil {
		trace_util_0.Count(_builtin_math_00000, 99)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_math_00000, 96)
	return to, false, nil
}

type ceilFunctionClass struct {
	baseFunctionClass
}

func (c *ceilFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_math_00000, 100)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 103)
		return nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 101)
	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTp)
	setFlag4FloorAndCeil(bf.tp, args[0])
	argFieldTp := args[0].GetType()
	bf.tp.Flen, bf.tp.Decimal = argFieldTp.Flen, 0

	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_math_00000, 104)
		if retTp == types.ETInt {
			trace_util_0.Count(_builtin_math_00000, 107)
			sig = &builtinCeilIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToInt)
		} else {
			trace_util_0.Count(_builtin_math_00000, 108)
			{
				sig = &builtinCeilIntToDecSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_CeilIntToDec)
			}
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_math_00000, 105)
		if retTp == types.ETInt {
			trace_util_0.Count(_builtin_math_00000, 109)
			sig = &builtinCeilDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToInt)
		} else {
			trace_util_0.Count(_builtin_math_00000, 110)
			{
				sig = &builtinCeilDecToDecSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_CeilDecToDec)
			}
		}
	default:
		trace_util_0.Count(_builtin_math_00000, 106)
		sig = &builtinCeilRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CeilReal)
	}
	trace_util_0.Count(_builtin_math_00000, 102)
	return sig, nil
}

type builtinCeilRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCeilRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 111)
	newSig := &builtinCeilRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 112)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 114)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 113)
	return math.Ceil(val), false, nil
}

type builtinCeilIntToIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCeilIntToIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 115)
	newSig := &builtinCeilIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilIntToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 116)
	return b.args[0].EvalInt(b.ctx, row)
}

type builtinCeilIntToDecSig struct {
	baseBuiltinFunc
}

func (b *builtinCeilIntToDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 117)
	newSig := &builtinCeilIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_Ceil
func (b *builtinCeilIntToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 118)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 121)
		return nil, true, err
	}

	trace_util_0.Count(_builtin_math_00000, 119)
	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) || val >= 0 {
		trace_util_0.Count(_builtin_math_00000, 122)
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	trace_util_0.Count(_builtin_math_00000, 120)
	return types.NewDecFromInt(val), false, nil
}

type builtinCeilDecToIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCeilDecToIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 123)
	newSig := &builtinCeilDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilDecToIntSig.
// Ceil receives
func (b *builtinCeilDecToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 124)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 127)
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	trace_util_0.Count(_builtin_math_00000, 125)
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		trace_util_0.Count(_builtin_math_00000, 128)
		err = nil
		if !val.IsNegative() {
			trace_util_0.Count(_builtin_math_00000, 129)
			res = res + 1
		}
	}
	trace_util_0.Count(_builtin_math_00000, 126)
	return res, false, err
}

type builtinCeilDecToDecSig struct {
	baseBuiltinFunc
}

func (b *builtinCeilDecToDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 130)
	newSig := &builtinCeilDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilDecToDecSig.
func (b *builtinCeilDecToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 131)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 135)
		return nil, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 132)
	res := new(types.MyDecimal)
	if val.IsNegative() {
		trace_util_0.Count(_builtin_math_00000, 136)
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 133)
	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		trace_util_0.Count(_builtin_math_00000, 137)
		return res, err != nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 134)
	err = types.DecimalAdd(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type floorFunctionClass struct {
	baseFunctionClass
}

// getEvalTp4FloorAndCeil gets the types.EvalType of FLOOR and CEIL.
func getEvalTp4FloorAndCeil(arg Expression) (retTp, argTp types.EvalType) {
	trace_util_0.Count(_builtin_math_00000, 138)
	fieldTp := arg.GetType()
	retTp, argTp = types.ETInt, fieldTp.EvalType()
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_math_00000, 140)
		if fieldTp.Tp == mysql.TypeLonglong {
			trace_util_0.Count(_builtin_math_00000, 143)
			retTp = types.ETDecimal
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_math_00000, 141)
		if fieldTp.Flen-fieldTp.Decimal > mysql.MaxIntWidth-2 {
			trace_util_0.Count(_builtin_math_00000, 144) // len(math.MaxInt64) - 1
			retTp = types.ETDecimal
		}
	default:
		trace_util_0.Count(_builtin_math_00000, 142)
		retTp, argTp = types.ETReal, types.ETReal
	}
	trace_util_0.Count(_builtin_math_00000, 139)
	return retTp, argTp
}

// setFlag4FloorAndCeil sets return flag of FLOOR and CEIL.
func setFlag4FloorAndCeil(tp *types.FieldType, arg Expression) {
	trace_util_0.Count(_builtin_math_00000, 145)
	fieldTp := arg.GetType()
	if (fieldTp.Tp == mysql.TypeLong || fieldTp.Tp == mysql.TypeNewDecimal) && mysql.HasUnsignedFlag(fieldTp.Flag) {
		trace_util_0.Count(_builtin_math_00000, 146)
		tp.Flag |= mysql.UnsignedFlag
	}
	// TODO: when argument type is timestamp, add not null flag.
}

func (c *floorFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_math_00000, 147)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 150)
		return nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 148)
	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTp)
	setFlag4FloorAndCeil(bf.tp, args[0])
	bf.tp.Flen, bf.tp.Decimal = args[0].GetType().Flen, 0
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_math_00000, 151)
		if retTp == types.ETInt {
			trace_util_0.Count(_builtin_math_00000, 154)
			sig = &builtinFloorIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToInt)
		} else {
			trace_util_0.Count(_builtin_math_00000, 155)
			{
				sig = &builtinFloorIntToDecSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_FloorIntToDec)
			}
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_math_00000, 152)
		if retTp == types.ETInt {
			trace_util_0.Count(_builtin_math_00000, 156)
			sig = &builtinFloorDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToInt)
		} else {
			trace_util_0.Count(_builtin_math_00000, 157)
			{
				sig = &builtinFloorDecToDecSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_FloorDecToDec)
			}
		}
	default:
		trace_util_0.Count(_builtin_math_00000, 153)
		sig = &builtinFloorRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FloorReal)
	}
	trace_util_0.Count(_builtin_math_00000, 149)
	return sig, nil
}

type builtinFloorRealSig struct {
	baseBuiltinFunc
}

func (b *builtinFloorRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 158)
	newSig := &builtinFloorRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinFloorRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 159)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 161)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 160)
	return math.Floor(val), false, nil
}

type builtinFloorIntToIntSig struct {
	baseBuiltinFunc
}

func (b *builtinFloorIntToIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 162)
	newSig := &builtinFloorIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 163)
	return b.args[0].EvalInt(b.ctx, row)
}

type builtinFloorIntToDecSig struct {
	baseBuiltinFunc
}

func (b *builtinFloorIntToDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 164)
	newSig := &builtinFloorIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 165)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 168)
		return nil, true, err
	}

	trace_util_0.Count(_builtin_math_00000, 166)
	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) || val >= 0 {
		trace_util_0.Count(_builtin_math_00000, 169)
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	trace_util_0.Count(_builtin_math_00000, 167)
	return types.NewDecFromInt(val), false, nil
}

type builtinFloorDecToIntSig struct {
	baseBuiltinFunc
}

func (b *builtinFloorDecToIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 170)
	newSig := &builtinFloorDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorDecToIntSig.
// floor receives
func (b *builtinFloorDecToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 171)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 174)
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	trace_util_0.Count(_builtin_math_00000, 172)
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		trace_util_0.Count(_builtin_math_00000, 175)
		err = nil
		if val.IsNegative() {
			trace_util_0.Count(_builtin_math_00000, 176)
			res--
		}
	}
	trace_util_0.Count(_builtin_math_00000, 173)
	return res, false, err
}

type builtinFloorDecToDecSig struct {
	baseBuiltinFunc
}

func (b *builtinFloorDecToDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 177)
	newSig := &builtinFloorDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorDecToDecSig.
func (b *builtinFloorDecToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 178)
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 182)
		return nil, true, err
	}

	trace_util_0.Count(_builtin_math_00000, 179)
	res := new(types.MyDecimal)
	if !val.IsNegative() {
		trace_util_0.Count(_builtin_math_00000, 183)
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 180)
	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		trace_util_0.Count(_builtin_math_00000, 184)
		return res, err != nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 181)
	err = types.DecimalSub(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 185)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 189)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 186)
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	if argsLen == 1 {
		trace_util_0.Count(_builtin_math_00000, 190)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	} else {
		trace_util_0.Count(_builtin_math_00000, 191)
		{
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		}
	}

	trace_util_0.Count(_builtin_math_00000, 187)
	if argsLen == 1 {
		trace_util_0.Count(_builtin_math_00000, 192)
		sig = &builtinLog1ArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_math_00000, 193)
		{
			sig = &builtinLog2ArgsSig{bf}
		}
	}

	trace_util_0.Count(_builtin_math_00000, 188)
	return sig, nil
}

type builtinLog1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinLog1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 194)
	newSig := &builtinLog1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog1ArgSig, corresponding to log(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog1ArgSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 195)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 198)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 196)
	if val <= 0 {
		trace_util_0.Count(_builtin_math_00000, 199)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_math_00000, 197)
	return math.Log(val), false, nil
}

type builtinLog2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLog2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 200)
	newSig := &builtinLog2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2ArgsSig, corresponding to log(b, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog2ArgsSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 201)
	val1, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 205)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 202)
	val2, isNull, err := b.args[1].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 206)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 203)
	if val1 <= 0 || val1 == 1 || val2 <= 0 {
		trace_util_0.Count(_builtin_math_00000, 207)
		return 0, true, nil
	}

	trace_util_0.Count(_builtin_math_00000, 204)
	return math.Log(val2) / math.Log(val1), false, nil
}

type log2FunctionClass struct {
	baseFunctionClass
}

func (c *log2FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 208)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 210)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 209)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinLog2Sig{bf}
	return sig, nil
}

type builtinLog2Sig struct {
	baseBuiltinFunc
}

func (b *builtinLog2Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 211)
	newSig := &builtinLog2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 212)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 215)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 213)
	if val <= 0 {
		trace_util_0.Count(_builtin_math_00000, 216)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_math_00000, 214)
	return math.Log2(val), false, nil
}

type log10FunctionClass struct {
	baseFunctionClass
}

func (c *log10FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 217)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 219)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 218)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinLog10Sig{bf}
	return sig, nil
}

type builtinLog10Sig struct {
	baseBuiltinFunc
}

func (b *builtinLog10Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 220)
	newSig := &builtinLog10Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog10Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 221)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 224)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 222)
	if val <= 0 {
		trace_util_0.Count(_builtin_math_00000, 225)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_math_00000, 223)
	return math.Log10(val), false, nil
}

type randFunctionClass struct {
	baseFunctionClass
}

func (c *randFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 226)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 230)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 227)
	var sig builtinFunc
	var argTps []types.EvalType
	if len(args) > 0 {
		trace_util_0.Count(_builtin_math_00000, 231)
		argTps = []types.EvalType{types.ETInt}
	}
	trace_util_0.Count(_builtin_math_00000, 228)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, argTps...)
	bt := bf
	if len(args) == 0 {
		trace_util_0.Count(_builtin_math_00000, 232)
		seed := time.Now().UnixNano()
		sig = &builtinRandSig{bt, rand.New(rand.NewSource(seed))}
	} else {
		trace_util_0.Count(_builtin_math_00000, 233)
		if _, isConstant := args[0].(*Constant); isConstant {
			trace_util_0.Count(_builtin_math_00000, 234)
			// According to MySQL manual:
			// If an integer argument N is specified, it is used as the seed value:
			// With a constant initializer argument, the seed is initialized once
			// when the statement is prepared, prior to execution.
			seed, isNull, err := args[0].EvalInt(ctx, chunk.Row{})
			if err != nil {
				trace_util_0.Count(_builtin_math_00000, 237)
				return nil, err
			}
			trace_util_0.Count(_builtin_math_00000, 235)
			if isNull {
				trace_util_0.Count(_builtin_math_00000, 238)
				seed = time.Now().UnixNano()
			}
			trace_util_0.Count(_builtin_math_00000, 236)
			sig = &builtinRandSig{bt, rand.New(rand.NewSource(seed))}
		} else {
			trace_util_0.Count(_builtin_math_00000, 239)
			{
				sig = &builtinRandWithSeedSig{bt}
			}
		}
	}
	trace_util_0.Count(_builtin_math_00000, 229)
	return sig, nil
}

type builtinRandSig struct {
	baseBuiltinFunc
	randGen *rand.Rand
}

func (b *builtinRandSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 240)
	newSig := &builtinRandSig{randGen: b.randGen}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND().
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 241)
	return b.randGen.Float64(), false, nil
}

type builtinRandWithSeedSig struct {
	baseBuiltinFunc
}

func (b *builtinRandWithSeedSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 242)
	newSig := &builtinRandWithSeedSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND(N).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandWithSeedSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 243)
	seed, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_math_00000, 246)
		return 0, true, err
	}
	// b.args[0] is promised to be a non-constant(such as a column name) in
	// builtinRandWithSeedSig, the seed is initialized with the value for each
	// invocation of RAND().
	trace_util_0.Count(_builtin_math_00000, 244)
	var randGen *rand.Rand
	if isNull {
		trace_util_0.Count(_builtin_math_00000, 247)
		randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		trace_util_0.Count(_builtin_math_00000, 248)
		{
			randGen = rand.New(rand.NewSource(seed))
		}
	}
	trace_util_0.Count(_builtin_math_00000, 245)
	return randGen.Float64(), false, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 249)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 251)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 250)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
	sig := &builtinPowSig{bf}
	return sig, nil
}

type builtinPowSig struct {
	baseBuiltinFunc
}

func (b *builtinPowSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 252)
	newSig := &builtinPowSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals POW(x, y).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 253)
	x, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 257)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 254)
	y, isNull, err := b.args[1].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 258)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 255)
	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		trace_util_0.Count(_builtin_math_00000, 259)
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	trace_util_0.Count(_builtin_math_00000, 256)
	return power, false, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

type convFunctionClass struct {
	baseFunctionClass
}

func (c *convFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 260)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 262)
		return nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 261)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETInt)
	bf.tp.Flen = 64
	sig := &builtinConvSig{bf}
	return sig, nil
}

type builtinConvSig struct {
	baseBuiltinFunc
}

func (b *builtinConvSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 263)
	newSig := &builtinConvSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONV(N,from_base,to_base).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv.
func (b *builtinConvSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_math_00000, 264)
	n, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 279)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 265)
	fromBase, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 280)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 266)
	toBase, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 281)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 267)
	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	if fromBase < 0 {
		trace_util_0.Count(_builtin_math_00000, 282)
		fromBase = -fromBase
		signed = true
	}

	trace_util_0.Count(_builtin_math_00000, 268)
	if toBase < 0 {
		trace_util_0.Count(_builtin_math_00000, 283)
		toBase = -toBase
		ignoreSign = true
	}

	trace_util_0.Count(_builtin_math_00000, 269)
	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		trace_util_0.Count(_builtin_math_00000, 284)
		return res, true, nil
	}

	trace_util_0.Count(_builtin_math_00000, 270)
	n = getValidPrefix(strings.TrimSpace(n), fromBase)
	if len(n) == 0 {
		trace_util_0.Count(_builtin_math_00000, 285)
		return "0", false, nil
	}

	trace_util_0.Count(_builtin_math_00000, 271)
	if n[0] == '-' {
		trace_util_0.Count(_builtin_math_00000, 286)
		negative = true
		n = n[1:]
	}

	trace_util_0.Count(_builtin_math_00000, 272)
	val, err := strconv.ParseUint(n, int(fromBase), 64)
	if err != nil {
		trace_util_0.Count(_builtin_math_00000, 287)
		return res, false, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSINGED", n)
	}
	trace_util_0.Count(_builtin_math_00000, 273)
	if signed {
		trace_util_0.Count(_builtin_math_00000, 288)
		if negative && val > -math.MinInt64 {
			trace_util_0.Count(_builtin_math_00000, 290)
			val = -math.MinInt64
		}
		trace_util_0.Count(_builtin_math_00000, 289)
		if !negative && val > math.MaxInt64 {
			trace_util_0.Count(_builtin_math_00000, 291)
			val = math.MaxInt64
		}
	}
	trace_util_0.Count(_builtin_math_00000, 274)
	if negative {
		trace_util_0.Count(_builtin_math_00000, 292)
		val = -val
	}

	trace_util_0.Count(_builtin_math_00000, 275)
	if int64(val) < 0 {
		trace_util_0.Count(_builtin_math_00000, 293)
		negative = true
	} else {
		trace_util_0.Count(_builtin_math_00000, 294)
		{
			negative = false
		}
	}
	trace_util_0.Count(_builtin_math_00000, 276)
	if ignoreSign && negative {
		trace_util_0.Count(_builtin_math_00000, 295)
		val = 0 - val
	}

	trace_util_0.Count(_builtin_math_00000, 277)
	s := strconv.FormatUint(val, int(toBase))
	if negative && ignoreSign {
		trace_util_0.Count(_builtin_math_00000, 296)
		s = "-" + s
	}
	trace_util_0.Count(_builtin_math_00000, 278)
	res = strings.ToUpper(s)
	return res, false, nil
}

type crc32FunctionClass struct {
	baseFunctionClass
}

func (c *crc32FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 297)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 299)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 298)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinCRC32Sig{bf}
	return sig, nil
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
}

func (b *builtinCRC32Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 300)
	newSig := &builtinCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 301)
	x, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 303)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 302)
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 304)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 306)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 305)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETReal)
	sig := &builtinSignSig{bf}
	return sig, nil
}

type builtinSignSig struct {
	baseBuiltinFunc
}

func (b *builtinSignSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 307)
	newSig := &builtinSignSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 308)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 310)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 309)
	if val > 0 {
		trace_util_0.Count(_builtin_math_00000, 311)
		return 1, false, nil
	} else {
		trace_util_0.Count(_builtin_math_00000, 312)
		if val == 0 {
			trace_util_0.Count(_builtin_math_00000, 313)
			return 0, false, nil
		} else {
			trace_util_0.Count(_builtin_math_00000, 314)
			{
				return -1, false, nil
			}
		}
	}
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 315)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 317)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 316)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinSqrtSig{bf}
	return sig, nil
}

type builtinSqrtSig struct {
	baseBuiltinFunc
}

func (b *builtinSqrtSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 318)
	newSig := &builtinSqrtSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 319)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 322)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 320)
	if val < 0 {
		trace_util_0.Count(_builtin_math_00000, 323)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_math_00000, 321)
	return math.Sqrt(val), false, nil
}

type acosFunctionClass struct {
	baseFunctionClass
}

func (c *acosFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 324)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 326)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 325)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinAcosSig{bf}
	return sig, nil
}

type builtinAcosSig struct {
	baseBuiltinFunc
}

func (b *builtinAcosSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 327)
	newSig := &builtinAcosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAcosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 328)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 331)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 329)
	if val < -1 || val > 1 {
		trace_util_0.Count(_builtin_math_00000, 332)
		return 0, true, nil
	}

	trace_util_0.Count(_builtin_math_00000, 330)
	return math.Acos(val), false, nil
}

type asinFunctionClass struct {
	baseFunctionClass
}

func (c *asinFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 333)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 335)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 334)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinAsinSig{bf}
	return sig, nil
}

type builtinAsinSig struct {
	baseBuiltinFunc
}

func (b *builtinAsinSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 336)
	newSig := &builtinAsinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAsinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 337)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 340)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 338)
	if val < -1 || val > 1 {
		trace_util_0.Count(_builtin_math_00000, 341)
		return 0, true, nil
	}

	trace_util_0.Count(_builtin_math_00000, 339)
	return math.Asin(val), false, nil
}

type atanFunctionClass struct {
	baseFunctionClass
}

func (c *atanFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 342)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 346)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 343)
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	if argsLen == 1 {
		trace_util_0.Count(_builtin_math_00000, 347)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	} else {
		trace_util_0.Count(_builtin_math_00000, 348)
		{
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		}
	}

	trace_util_0.Count(_builtin_math_00000, 344)
	if argsLen == 1 {
		trace_util_0.Count(_builtin_math_00000, 349)
		sig = &builtinAtan1ArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_math_00000, 350)
		{
			sig = &builtinAtan2ArgsSig{bf}
		}
	}

	trace_util_0.Count(_builtin_math_00000, 345)
	return sig, nil
}

type builtinAtan1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinAtan1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 351)
	newSig := &builtinAtan1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan1ArgSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 352)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 354)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 353)
	return math.Atan(val), false, nil
}

type builtinAtan2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinAtan2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 355)
	newSig := &builtinAtan2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(y, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan2ArgsSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 356)
	val1, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 359)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 357)
	val2, isNull, err := b.args[1].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 360)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 358)
	return math.Atan2(val1, val2), false, nil
}

type cosFunctionClass struct {
	baseFunctionClass
}

func (c *cosFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 361)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 363)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 362)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinCosSig{bf}
	return sig, nil
}

type builtinCosSig struct {
	baseBuiltinFunc
}

func (b *builtinCosSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 364)
	newSig := &builtinCosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 365)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 367)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 366)
	return math.Cos(val), false, nil
}

type cotFunctionClass struct {
	baseFunctionClass
}

func (c *cotFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 368)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 370)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 369)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinCotSig{bf}
	return sig, nil
}

type builtinCotSig struct {
	baseBuiltinFunc
}

func (b *builtinCotSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 371)
	newSig := &builtinCotSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 372)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 375)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 373)
	tan := math.Tan(val)
	if tan != 0 {
		trace_util_0.Count(_builtin_math_00000, 376)
		cot := 1 / tan
		if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
			trace_util_0.Count(_builtin_math_00000, 377)
			return cot, false, nil
		}
	}
	trace_util_0.Count(_builtin_math_00000, 374)
	return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(val, 'f', -1, 64)))
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 378)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 380)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 379)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinDegreesSig{bf}
	return sig, nil
}

type builtinDegreesSig struct {
	baseBuiltinFunc
}

func (b *builtinDegreesSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 381)
	newSig := &builtinDegreesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinDegreesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 382)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 384)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 383)
	res := val * 180 / math.Pi
	return res, false, nil
}

type expFunctionClass struct {
	baseFunctionClass
}

func (c *expFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 385)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 387)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 386)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinExpSig{bf}
	return sig, nil
}

type builtinExpSig struct {
	baseBuiltinFunc
}

func (b *builtinExpSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 388)
	newSig := &builtinExpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 389)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 392)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 390)
	exp := math.Exp(val)
	if math.IsInf(exp, 0) || math.IsNaN(exp) {
		trace_util_0.Count(_builtin_math_00000, 393)
		s := fmt.Sprintf("exp(%s)", strconv.FormatFloat(val, 'f', -1, 64))
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", s)
	}
	trace_util_0.Count(_builtin_math_00000, 391)
	return exp, false, nil
}

type piFunctionClass struct {
	baseFunctionClass
}

func (c *piFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 394)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 396)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 395)
	var (
		bf  baseBuiltinFunc
		sig builtinFunc
	)

	bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal)
	bf.tp.Decimal = 6
	bf.tp.Flen = 8
	sig = &builtinPISig{bf}
	return sig, nil
}

type builtinPISig struct {
	baseBuiltinFunc
}

func (b *builtinPISig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 397)
	newSig := &builtinPISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) evalReal(_ chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 398)
	return float64(math.Pi), false, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 399)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 401)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 400)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinRadiansSig{bf}
	return sig, nil
}

type builtinRadiansSig struct {
	baseBuiltinFunc
}

func (b *builtinRadiansSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 402)
	newSig := &builtinRadiansSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RADIANS(X).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 403)
	x, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 405)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 404)
	return x * math.Pi / 180, false, nil
}

type sinFunctionClass struct {
	baseFunctionClass
}

func (c *sinFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 406)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 408)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 407)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinSinSig{bf}
	return sig, nil
}

type builtinSinSig struct {
	baseBuiltinFunc
}

func (b *builtinSinSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 409)
	newSig := &builtinSinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinSinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 410)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 412)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 411)
	return math.Sin(val), false, nil
}

type tanFunctionClass struct {
	baseFunctionClass
}

func (c *tanFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 413)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 415)
		return nil, err
	}
	trace_util_0.Count(_builtin_math_00000, 414)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinTanSig{bf}
	return sig, nil
}

type builtinTanSig struct {
	baseBuiltinFunc
}

func (b *builtinTanSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 416)
	newSig := &builtinTanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinTanSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 417)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 419)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 418)
	return math.Tan(val), false, nil
}

type truncateFunctionClass struct {
	baseFunctionClass
}

func (c *truncateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_math_00000, 420)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_math_00000, 424)
		return nil, err
	}

	trace_util_0.Count(_builtin_math_00000, 421)
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration || argTp == types.ETString {
		trace_util_0.Count(_builtin_math_00000, 425)
		argTp = types.ETReal
	}

	trace_util_0.Count(_builtin_math_00000, 422)
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, argTp, types.ETInt)

	bf.tp.Decimal = calculateDecimal4RoundAndTruncate(ctx, args, argTp)
	bf.tp.Flen = args[0].GetType().Flen - args[0].GetType().Decimal + bf.tp.Decimal
	bf.tp.Flag |= args[0].GetType().Flag

	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_math_00000, 426)
		if mysql.HasUnsignedFlag(args[0].GetType().Flag) {
			trace_util_0.Count(_builtin_math_00000, 429)
			sig = &builtinTruncateUintSig{bf}
		} else {
			trace_util_0.Count(_builtin_math_00000, 430)
			{
				sig = &builtinTruncateIntSig{bf}
			}
		}
	case types.ETReal:
		trace_util_0.Count(_builtin_math_00000, 427)
		sig = &builtinTruncateRealSig{bf}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_math_00000, 428)
		sig = &builtinTruncateDecimalSig{bf}
	}

	trace_util_0.Count(_builtin_math_00000, 423)
	return sig, nil
}

type builtinTruncateDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinTruncateDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 431)
	newSig := &builtinTruncateDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 432)
	x, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 436)
		return nil, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 433)
	d, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 437)
		return nil, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 434)
	result := new(types.MyDecimal)
	if err := x.Round(result, mathutil.Min(int(d), b.getRetTp().Decimal), types.ModeTruncate); err != nil {
		trace_util_0.Count(_builtin_math_00000, 438)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_math_00000, 435)
	return result, false, nil
}

type builtinTruncateRealSig struct {
	baseBuiltinFunc
}

func (b *builtinTruncateRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 439)
	newSig := &builtinTruncateRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 440)
	x, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 443)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 441)
	d, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 444)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 442)
	return types.Truncate(x, int(d)), false, nil
}

type builtinTruncateIntSig struct {
	baseBuiltinFunc
}

func (b *builtinTruncateIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 445)
	newSig := &builtinTruncateIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 446)
	x, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 450)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 447)
	d, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 451)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_math_00000, 448)
	if d >= 0 {
		trace_util_0.Count(_builtin_math_00000, 452)
		return x, false, nil
	}
	trace_util_0.Count(_builtin_math_00000, 449)
	shift := int64(math.Pow10(int(-d)))
	return x / shift * shift, false, nil
}

func (b *builtinTruncateUintSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_math_00000, 453)
	newSig := &builtinTruncateUintSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinTruncateUintSig struct {
	baseBuiltinFunc
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateUintSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_math_00000, 454)
	x, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 458)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 455)
	uintx := uint64(x)

	d, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_math_00000, 459)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_math_00000, 456)
	if d >= 0 {
		trace_util_0.Count(_builtin_math_00000, 460)
		return x, false, nil
	}
	trace_util_0.Count(_builtin_math_00000, 457)
	shift := uint64(math.Pow10(int(-d)))
	return int64(uintx / shift * shift), false, nil
}

var _builtin_math_00000 = "expression/builtin_math.go"
