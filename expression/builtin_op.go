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
	"fmt"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &logicAndFunctionClass{}
	_ functionClass = &logicOrFunctionClass{}
	_ functionClass = &logicXorFunctionClass{}
	_ functionClass = &isTrueOrFalseFunctionClass{}
	_ functionClass = &unaryMinusFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
	_ functionClass = &unaryNotFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
	_ builtinFunc = &builtinLogicOrSig{}
	_ builtinFunc = &builtinLogicXorSig{}
	_ builtinFunc = &builtinRealIsTrueSig{}
	_ builtinFunc = &builtinDecimalIsTrueSig{}
	_ builtinFunc = &builtinIntIsTrueSig{}
	_ builtinFunc = &builtinRealIsFalseSig{}
	_ builtinFunc = &builtinDecimalIsFalseSig{}
	_ builtinFunc = &builtinIntIsFalseSig{}
	_ builtinFunc = &builtinUnaryMinusIntSig{}
	_ builtinFunc = &builtinDecimalIsNullSig{}
	_ builtinFunc = &builtinDurationIsNullSig{}
	_ builtinFunc = &builtinIntIsNullSig{}
	_ builtinFunc = &builtinRealIsNullSig{}
	_ builtinFunc = &builtinStringIsNullSig{}
	_ builtinFunc = &builtinTimeIsNullSig{}
	_ builtinFunc = &builtinUnaryNotSig{}
)

type logicAndFunctionClass struct {
	baseFunctionClass
}

func (c *logicAndFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 0)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 1)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.tp.Flen = 1
	return sig, nil
}

type builtinLogicAndSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicAndSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 3)
	newSig := &builtinLogicAndSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicAndSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 4)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil || (!isNull0 && arg0 == 0) {
		trace_util_0.Count(_builtin_op_00000, 8)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 5)
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil || (!isNull1 && arg1 == 0) {
		trace_util_0.Count(_builtin_op_00000, 9)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 6)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_op_00000, 10)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_op_00000, 7)
	return 1, false, nil
}

type logicOrFunctionClass struct {
	baseFunctionClass
}

func (c *logicOrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 11)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 13)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 12)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 1
	sig := &builtinLogicOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig, nil
}

type builtinLogicOrSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicOrSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 14)
	newSig := &builtinLogicOrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicOrSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 15)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 21)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 16)
	if !isNull0 && arg0 != 0 {
		trace_util_0.Count(_builtin_op_00000, 22)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 17)
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 23)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 18)
	if !isNull1 && arg1 != 0 {
		trace_util_0.Count(_builtin_op_00000, 24)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 19)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_op_00000, 25)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_op_00000, 20)
	return 0, false, nil
}

type logicXorFunctionClass struct {
	baseFunctionClass
}

func (c *logicXorFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 26)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 28)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 27)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalXor)
	sig.tp.Flen = 1
	return sig, nil
}

type builtinLogicXorSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicXorSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 29)
	newSig := &builtinLogicXorSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicXorSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 30)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 34)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 31)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 35)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 32)
	if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
		trace_util_0.Count(_builtin_op_00000, 36)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 33)
	return 1, false, nil
}

type bitAndFunctionClass struct {
	baseFunctionClass
}

func (c *bitAndFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 37)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 39)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 38)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitAndSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitAndSig struct {
	baseBuiltinFunc
}

func (b *builtinBitAndSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 40)
	newSig := &builtinBitAndSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitAndSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 41)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 44)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 42)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 45)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 43)
	return arg0 & arg1, false, nil
}

type bitOrFunctionClass struct {
	baseFunctionClass
}

func (c *bitOrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 46)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 48)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 47)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitOrSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitOrSig struct {
	baseBuiltinFunc
}

func (b *builtinBitOrSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 49)
	newSig := &builtinBitOrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitOrSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 50)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 53)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 51)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 54)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 52)
	return arg0 | arg1, false, nil
}

type bitXorFunctionClass struct {
	baseFunctionClass
}

func (c *bitXorFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 55)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 57)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 56)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitXorSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitXorSig struct {
	baseBuiltinFunc
}

func (b *builtinBitXorSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 58)
	newSig := &builtinBitXorSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitXorSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 59)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 62)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 60)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 63)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 61)
	return arg0 ^ arg1, false, nil
}

type leftShiftFunctionClass struct {
	baseFunctionClass
}

func (c *leftShiftFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 64)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 66)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 65)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLeftShiftSig{bf}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinLeftShiftSig struct {
	baseBuiltinFunc
}

func (b *builtinLeftShiftSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 67)
	newSig := &builtinLeftShiftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeftShiftSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 68)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 71)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 69)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 72)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 70)
	return int64(uint64(arg0) << uint64(arg1)), false, nil
}

type rightShiftFunctionClass struct {
	baseFunctionClass
}

func (c *rightShiftFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 73)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 75)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 74)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinRightShiftSig{bf}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinRightShiftSig struct {
	baseBuiltinFunc
}

func (b *builtinRightShiftSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 76)
	newSig := &builtinRightShiftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRightShiftSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 77)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 80)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 78)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 81)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 79)
	return int64(uint64(arg0) >> uint64(arg1)), false, nil
}

type isTrueOrFalseFunctionClass struct {
	baseFunctionClass
	op opcode.Op
}

func (c *isTrueOrFalseFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 82)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_op_00000, 86)
		return nil, err
	}

	trace_util_0.Count(_builtin_op_00000, 83)
	argTp := args[0].GetType().EvalType()
	if argTp != types.ETReal && argTp != types.ETDecimal {
		trace_util_0.Count(_builtin_op_00000, 87)
		argTp = types.ETInt
	}

	trace_util_0.Count(_builtin_op_00000, 84)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1

	var sig builtinFunc
	switch c.op {
	case opcode.IsTruth:
		trace_util_0.Count(_builtin_op_00000, 88)
		switch argTp {
		case types.ETReal:
			trace_util_0.Count(_builtin_op_00000, 90)
			sig = &builtinRealIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RealIsTrue)
		case types.ETDecimal:
			trace_util_0.Count(_builtin_op_00000, 91)
			sig = &builtinDecimalIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_DecimalIsTrue)
		case types.ETInt:
			trace_util_0.Count(_builtin_op_00000, 92)
			sig = &builtinIntIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_IntIsTrue)
		}
	case opcode.IsFalsity:
		trace_util_0.Count(_builtin_op_00000, 89)
		switch argTp {
		case types.ETReal:
			trace_util_0.Count(_builtin_op_00000, 93)
			sig = &builtinRealIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RealIsFalse)
		case types.ETDecimal:
			trace_util_0.Count(_builtin_op_00000, 94)
			sig = &builtinDecimalIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_DecimalIsFalse)
		case types.ETInt:
			trace_util_0.Count(_builtin_op_00000, 95)
			sig = &builtinIntIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_IntIsFalse)
		}
	}
	trace_util_0.Count(_builtin_op_00000, 85)
	return sig, nil
}

type builtinRealIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsTrueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 96)
	newSig := &builtinRealIsTrueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 97)
	input, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 100)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 98)
	if isNull || input == 0 {
		trace_util_0.Count(_builtin_op_00000, 101)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 99)
	return 1, false, nil
}

type builtinDecimalIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalIsTrueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 102)
	newSig := &builtinDecimalIsTrueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 103)
	input, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 106)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 104)
	if isNull || input.IsZero() {
		trace_util_0.Count(_builtin_op_00000, 107)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 105)
	return 1, false, nil
}

type builtinIntIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsTrueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 108)
	newSig := &builtinIntIsTrueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 109)
	input, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 112)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 110)
	if isNull || input == 0 {
		trace_util_0.Count(_builtin_op_00000, 113)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 111)
	return 1, false, nil
}

type builtinRealIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsFalseSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 114)
	newSig := &builtinRealIsFalseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 115)
	input, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 118)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 116)
	if isNull || input != 0 {
		trace_util_0.Count(_builtin_op_00000, 119)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 117)
	return 1, false, nil
}

type builtinDecimalIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalIsFalseSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 120)
	newSig := &builtinDecimalIsFalseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 121)
	input, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 124)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 122)
	if isNull || !input.IsZero() {
		trace_util_0.Count(_builtin_op_00000, 125)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 123)
	return 1, false, nil
}

type builtinIntIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsFalseSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 126)
	newSig := &builtinIntIsFalseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 127)
	input, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 130)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 128)
	if isNull || input != 0 {
		trace_util_0.Count(_builtin_op_00000, 131)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 129)
	return 1, false, nil
}

type bitNegFunctionClass struct {
	baseFunctionClass
}

func (c *bitNegFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 132)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_op_00000, 134)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 133)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinBitNegSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitNegSig)
	return sig, nil
}

type builtinBitNegSig struct {
	baseBuiltinFunc
}

func (b *builtinBitNegSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 135)
	newSig := &builtinBitNegSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitNegSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 136)
	arg, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 138)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 137)
	return ^arg, false, nil
}

type unaryNotFunctionClass struct {
	baseFunctionClass
}

func (c *unaryNotFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 139)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_op_00000, 141)
		return nil, err
	}

	trace_util_0.Count(_builtin_op_00000, 140)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flen = 1

	sig := &builtinUnaryNotSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UnaryNot)
	return sig, nil
}

type builtinUnaryNotSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryNotSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 142)
	newSig := &builtinUnaryNotSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 143)
	arg, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_op_00000, 146)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 144)
	if arg != 0 {
		trace_util_0.Count(_builtin_op_00000, 147)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 145)
	return 1, false, nil
}

type unaryMinusFunctionClass struct {
	baseFunctionClass
}

func (c *unaryMinusFunctionClass) handleIntOverflow(arg *Constant) (overflow bool) {
	trace_util_0.Count(_builtin_op_00000, 148)
	if mysql.HasUnsignedFlag(arg.GetType().Flag) {
		trace_util_0.Count(_builtin_op_00000, 150)
		uval := arg.Value.GetUint64()
		// -math.MinInt64 is 9223372036854775808, so if uval is more than 9223372036854775808, like
		// 9223372036854775809, -9223372036854775809 is less than math.MinInt64, overflow occurs.
		if uval > uint64(-math.MinInt64) {
			trace_util_0.Count(_builtin_op_00000, 151)
			return true
		}
	} else {
		trace_util_0.Count(_builtin_op_00000, 152)
		{
			val := arg.Value.GetInt64()
			// The math.MinInt64 is -9223372036854775808, the math.MaxInt64 is 9223372036854775807,
			// which is less than abs(-9223372036854775808). When val == math.MinInt64, overflow occurs.
			if val == math.MinInt64 {
				trace_util_0.Count(_builtin_op_00000, 153)
				return true
			}
		}
	}
	trace_util_0.Count(_builtin_op_00000, 149)
	return false
}

// typeInfer infers unaryMinus function return type. when the arg is an int constant and overflow,
// typerInfer will infers the return type as types.ETDecimal, not types.ETInt.
func (c *unaryMinusFunctionClass) typeInfer(ctx sessionctx.Context, argExpr Expression) (types.EvalType, bool) {
	trace_util_0.Count(_builtin_op_00000, 154)
	tp := argExpr.GetType().EvalType()
	if tp != types.ETInt && tp != types.ETDecimal {
		trace_util_0.Count(_builtin_op_00000, 157)
		tp = types.ETReal
	}

	trace_util_0.Count(_builtin_op_00000, 155)
	sc := ctx.GetSessionVars().StmtCtx
	overflow := false
	// TODO: Handle float overflow.
	if arg, ok := argExpr.(*Constant); sc.InSelectStmt && ok && tp == types.ETInt {
		trace_util_0.Count(_builtin_op_00000, 158)
		overflow = c.handleIntOverflow(arg)
		if overflow {
			trace_util_0.Count(_builtin_op_00000, 159)
			tp = types.ETDecimal
		}
	}
	trace_util_0.Count(_builtin_op_00000, 156)
	return tp, overflow
}

func (c *unaryMinusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_op_00000, 160)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_op_00000, 163)
		return nil, err
	}

	trace_util_0.Count(_builtin_op_00000, 161)
	argExpr, argExprTp := args[0], args[0].GetType()
	_, intOverflow := c.typeInfer(ctx, argExpr)

	var bf baseBuiltinFunc
	switch argExprTp.EvalType() {
	case types.ETInt:
		trace_util_0.Count(_builtin_op_00000, 164)
		if intOverflow {
			trace_util_0.Count(_builtin_op_00000, 169)
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			trace_util_0.Count(_builtin_op_00000, 170)
			{
				bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
				sig = &builtinUnaryMinusIntSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusInt)
			}
		}
		trace_util_0.Count(_builtin_op_00000, 165)
		bf.tp.Decimal = 0
	case types.ETDecimal:
		trace_util_0.Count(_builtin_op_00000, 166)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
		bf.tp.Decimal = argExprTp.Decimal
		sig = &builtinUnaryMinusDecimalSig{bf, false}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
	case types.ETReal:
		trace_util_0.Count(_builtin_op_00000, 167)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
		sig = &builtinUnaryMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	default:
		trace_util_0.Count(_builtin_op_00000, 168)
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
			trace_util_0.Count(_builtin_op_00000, 171)
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			trace_util_0.Count(_builtin_op_00000, 172)
			{
				bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
				sig = &builtinUnaryMinusRealSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
			}
		}
	}
	trace_util_0.Count(_builtin_op_00000, 162)
	bf.tp.Flen = argExprTp.Flen + 1
	return sig, err
}

type builtinUnaryMinusIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 173)
	newSig := &builtinUnaryMinusIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_op_00000, 174)
	var val int64
	val, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_op_00000, 177)
		return val, isNull, err
	}

	trace_util_0.Count(_builtin_op_00000, 175)
	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_op_00000, 178)
		uval := uint64(val)
		if uval > uint64(-math.MinInt64) {
			trace_util_0.Count(_builtin_op_00000, 179)
			return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", uval))
		} else {
			trace_util_0.Count(_builtin_op_00000, 180)
			if uval == uint64(-math.MinInt64) {
				trace_util_0.Count(_builtin_op_00000, 181)
				return math.MinInt64, false, nil
			}
		}
	} else {
		trace_util_0.Count(_builtin_op_00000, 182)
		if val == math.MinInt64 {
			trace_util_0.Count(_builtin_op_00000, 183)
			return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", val))
		}
	}
	trace_util_0.Count(_builtin_op_00000, 176)
	return -val, false, nil
}

type builtinUnaryMinusDecimalSig struct {
	baseBuiltinFunc

	constantArgOverflow bool
}

func (b *builtinUnaryMinusDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 184)
	newSig := &builtinUnaryMinusDecimalSig{constantArgOverflow: b.constantArgOverflow}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 185)
	dec, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_op_00000, 187)
		return dec, isNull, err
	}
	trace_util_0.Count(_builtin_op_00000, 186)
	return types.DecimalNeg(dec), false, nil
}

type builtinUnaryMinusRealSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 188)
	newSig := &builtinUnaryMinusRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 189)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	return -val, isNull, err
}

type isNullFunctionClass struct {
	baseFunctionClass
}

func (c *isNullFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_op_00000, 190)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_op_00000, 194)
		return nil, err
	}
	trace_util_0.Count(_builtin_op_00000, 191)
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp {
		trace_util_0.Count(_builtin_op_00000, 195)
		argTp = types.ETDatetime
	} else {
		trace_util_0.Count(_builtin_op_00000, 196)
		if argTp == types.ETJson {
			trace_util_0.Count(_builtin_op_00000, 197)
			argTp = types.ETString
		}
	}
	trace_util_0.Count(_builtin_op_00000, 192)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_op_00000, 198)
		sig = &builtinIntIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntIsNull)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_op_00000, 199)
		sig = &builtinDecimalIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DecimalIsNull)
	case types.ETReal:
		trace_util_0.Count(_builtin_op_00000, 200)
		sig = &builtinRealIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_RealIsNull)
	case types.ETDatetime:
		trace_util_0.Count(_builtin_op_00000, 201)
		sig = &builtinTimeIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_TimeIsNull)
	case types.ETDuration:
		trace_util_0.Count(_builtin_op_00000, 202)
		sig = &builtinDurationIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DurationIsNull)
	case types.ETString:
		trace_util_0.Count(_builtin_op_00000, 203)
		sig = &builtinStringIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StringIsNull)
	default:
		trace_util_0.Count(_builtin_op_00000, 204)
		panic("unexpected types.EvalType")
	}
	trace_util_0.Count(_builtin_op_00000, 193)
	return sig, nil
}

type builtinDecimalIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 205)
	newSig := &builtinDecimalIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func evalIsNull(isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 206)
	if err != nil {
		trace_util_0.Count(_builtin_op_00000, 209)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_op_00000, 207)
	if isNull {
		trace_util_0.Count(_builtin_op_00000, 210)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_op_00000, 208)
	return 0, false, nil
}

func (b *builtinDecimalIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 211)
	_, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinDurationIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 212)
	newSig := &builtinDurationIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDurationIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 213)
	_, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinIntIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 214)
	newSig := &builtinIntIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 215)
	_, isNull, err := b.args[0].EvalInt(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinRealIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 216)
	newSig := &builtinRealIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 217)
	_, isNull, err := b.args[0].EvalReal(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinStringIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinStringIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 218)
	newSig := &builtinStringIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStringIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 219)
	_, isNull, err := b.args[0].EvalString(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinTimeIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeIsNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_op_00000, 220)
	newSig := &builtinTimeIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTimeIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_op_00000, 221)
	_, isNull, err := b.args[0].EvalTime(b.ctx, row)
	return evalIsNull(isNull, err)
}

var _builtin_op_00000 = "expression/builtin_op.go"
