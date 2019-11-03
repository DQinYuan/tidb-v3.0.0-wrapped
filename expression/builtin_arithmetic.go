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

package expression

import (
	"fmt"
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticIntDivideFunctionClass{}
	_ functionClass = &arithmeticModFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusDecimalSig{}
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticMinusRealSig{}
	_ builtinFunc = &builtinArithmeticMinusDecimalSig{}
	_ builtinFunc = &builtinArithmeticMinusIntSig{}
	_ builtinFunc = &builtinArithmeticDivideRealSig{}
	_ builtinFunc = &builtinArithmeticDivideDecimalSig{}
	_ builtinFunc = &builtinArithmeticMultiplyRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyDecimalSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntSig{}
	_ builtinFunc = &builtinArithmeticIntDivideIntSig{}
	_ builtinFunc = &builtinArithmeticIntDivideDecimalSig{}
	_ builtinFunc = &builtinArithmeticModIntSig{}
	_ builtinFunc = &builtinArithmeticModRealSig{}
	_ builtinFunc = &builtinArithmeticModDecimalSig{}
)

// precIncrement indicates the number of digits by which to increase the scale of the result of division operations
// performed with the / operator.
const precIncrement = 4

// numericContextResultType returns types.EvalType for numeric function's parameters.
// the returned types.EvalType should be one of: types.ETInt, types.ETDecimal, types.ETReal
func numericContextResultType(ft *types.FieldType) types.EvalType {
	trace_util_0.Count(_builtin_arithmetic_00000, 0)
	if types.IsTypeTemporal(ft.Tp) {
		trace_util_0.Count(_builtin_arithmetic_00000, 4)
		if ft.Decimal > 0 {
			trace_util_0.Count(_builtin_arithmetic_00000, 6)
			return types.ETDecimal
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 5)
		return types.ETInt
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 1)
	if types.IsBinaryStr(ft) {
		trace_util_0.Count(_builtin_arithmetic_00000, 7)
		return types.ETInt
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 2)
	evalTp4Ft := types.ETReal
	if !ft.Hybrid() {
		trace_util_0.Count(_builtin_arithmetic_00000, 8)
		evalTp4Ft = ft.EvalType()
		if evalTp4Ft != types.ETDecimal && evalTp4Ft != types.ETInt {
			trace_util_0.Count(_builtin_arithmetic_00000, 9)
			evalTp4Ft = types.ETReal
		}
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 3)
	return evalTp4Ft
}

// setFlenDecimal4Int is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4Int(retTp, a, b *types.FieldType) {
	trace_util_0.Count(_builtin_arithmetic_00000, 10)
	retTp.Decimal = 0
	retTp.Flen = mysql.MaxIntWidth
}

// setFlenDecimal4RealOrDecimal is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4RealOrDecimal(retTp, a, b *types.FieldType, isReal bool) {
	trace_util_0.Count(_builtin_arithmetic_00000, 11)
	if a.Decimal != types.UnspecifiedLength && b.Decimal != types.UnspecifiedLength {
		trace_util_0.Count(_builtin_arithmetic_00000, 13)
		retTp.Decimal = a.Decimal + b.Decimal
		if !isReal && retTp.Decimal > mysql.MaxDecimalScale {
			trace_util_0.Count(_builtin_arithmetic_00000, 17)
			retTp.Decimal = mysql.MaxDecimalScale
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 14)
		if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
			trace_util_0.Count(_builtin_arithmetic_00000, 18)
			retTp.Flen = types.UnspecifiedLength
			return
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 15)
		digitsInt := mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal)
		retTp.Flen = digitsInt + retTp.Decimal + 3
		if isReal {
			trace_util_0.Count(_builtin_arithmetic_00000, 19)
			retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxRealWidth)
			return
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 16)
		retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxDecimalWidth)
		return
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 12)
	if isReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 20)
		retTp.Flen, retTp.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 21)
		{
			retTp.Flen, retTp.Decimal = mysql.MaxDecimalWidth, mysql.MaxDecimalScale
		}
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivDecimal(retTp, a, b *types.FieldType) {
	trace_util_0.Count(_builtin_arithmetic_00000, 22)
	var deca, decb = a.Decimal, b.Decimal
	if deca == types.UnspecifiedFsp {
		trace_util_0.Count(_builtin_arithmetic_00000, 27)
		deca = 0
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 23)
	if decb == types.UnspecifiedFsp {
		trace_util_0.Count(_builtin_arithmetic_00000, 28)
		decb = 0
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 24)
	retTp.Decimal = deca + precIncrement
	if retTp.Decimal > mysql.MaxDecimalScale {
		trace_util_0.Count(_builtin_arithmetic_00000, 29)
		retTp.Decimal = mysql.MaxDecimalScale
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 25)
	if a.Flen == types.UnspecifiedLength {
		trace_util_0.Count(_builtin_arithmetic_00000, 30)
		retTp.Flen = mysql.MaxDecimalWidth
		return
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 26)
	retTp.Flen = a.Flen + decb + precIncrement
	if retTp.Flen > mysql.MaxDecimalWidth {
		trace_util_0.Count(_builtin_arithmetic_00000, 31)
		retTp.Flen = mysql.MaxDecimalWidth
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivReal(retTp *types.FieldType) {
	trace_util_0.Count(_builtin_arithmetic_00000, 32)
	retTp.Decimal = mysql.NotFixedDec
	retTp.Flen = mysql.MaxRealWidth
}

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 33)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 35)
		return nil, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 34)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 36)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticPlusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_PlusReal)
		return sig, nil
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 37)
		if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
			trace_util_0.Count(_builtin_arithmetic_00000, 38)
			bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
			setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
			sig := &builtinArithmeticPlusDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_PlusDecimal)
			return sig, nil
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 39)
			{
				bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
				if mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag) {
					trace_util_0.Count(_builtin_arithmetic_00000, 41)
					bf.tp.Flag |= mysql.UnsignedFlag
				}
				trace_util_0.Count(_builtin_arithmetic_00000, 40)
				setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
				sig := &builtinArithmeticPlusIntSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_PlusInt)
				return sig, nil
			}
		}
	}
}

type builtinArithmeticPlusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 42)
	newSig := &builtinArithmeticPlusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 43)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 47)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 44)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 48)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 45)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 49)
		if uint64(a) > math.MaxUint64-uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 55)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 50)
		if b < 0 && uint64(-b) > uint64(a) {
			trace_util_0.Count(_builtin_arithmetic_00000, 56)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 51)
		if b > 0 && uint64(a) > math.MaxUint64-uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 57)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 52)
		if a < 0 && uint64(-a) > uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 58)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 53)
		if a > 0 && uint64(b) > math.MaxUint64-uint64(a) {
			trace_util_0.Count(_builtin_arithmetic_00000, 59)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 54)
		if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
			trace_util_0.Count(_builtin_arithmetic_00000, 60)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 46)
	return a + b, false, nil
}

type builtinArithmeticPlusDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 61)
	newSig := &builtinArithmeticPlusDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 62)
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 66)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 63)
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 67)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 64)
	c := &types.MyDecimal{}
	err = types.DecimalAdd(a, b, c)
	if err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 68)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 65)
	return c, false, nil
}

type builtinArithmeticPlusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 69)
	newSig := &builtinArithmeticPlusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 70)
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 74)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 71)
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 75)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 72)
	if (a > 0 && b > math.MaxFloat64-a) || (a < 0 && b < -math.MaxFloat64-a) {
		trace_util_0.Count(_builtin_arithmetic_00000, 76)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 73)
	return a + b, false, nil
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 77)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 79)
		return nil, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 78)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 80)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MinusReal)
		return sig, nil
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 81)
		if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
			trace_util_0.Count(_builtin_arithmetic_00000, 82)
			bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
			setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
			sig := &builtinArithmeticMinusDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_MinusDecimal)
			return sig, nil
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 83)
			{

				bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
				setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
				if (mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag)) && !ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode() {
					trace_util_0.Count(_builtin_arithmetic_00000, 85)
					bf.tp.Flag |= mysql.UnsignedFlag
				}
				trace_util_0.Count(_builtin_arithmetic_00000, 84)
				sig := &builtinArithmeticMinusIntSig{baseBuiltinFunc: bf}
				sig.setPbCode(tipb.ScalarFuncSig_MinusInt)
				return sig, nil
			}
		}
	}
}

type builtinArithmeticMinusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 86)
	newSig := &builtinArithmeticMinusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 87)
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 91)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 88)
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 92)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 89)
	if (a > 0 && -b > math.MaxFloat64-a) || (a < 0 && -b < -math.MaxFloat64-a) {
		trace_util_0.Count(_builtin_arithmetic_00000, 93)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 90)
	return a - b, false, nil
}

type builtinArithmeticMinusDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 94)
	newSig := &builtinArithmeticMinusDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 95)
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 99)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 96)
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 100)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 97)
	c := &types.MyDecimal{}
	err = types.DecimalSub(a, b, c)
	if err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 101)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 98)
	return c, false, nil
}

type builtinArithmeticMinusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 102)
	newSig := &builtinArithmeticMinusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 103)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 109)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 104)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 110)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 105)
	forceToSigned := s.ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode()
	isLHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	if forceToSigned && mysql.HasUnsignedFlag(s.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_arithmetic_00000, 111)
		if a < 0 || (a > math.MaxInt64) {
			trace_util_0.Count(_builtin_arithmetic_00000, 112)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 106)
	if forceToSigned && mysql.HasUnsignedFlag(s.args[1].GetType().Flag) {
		trace_util_0.Count(_builtin_arithmetic_00000, 113)
		if b < 0 || (b > math.MaxInt64) {
			trace_util_0.Count(_builtin_arithmetic_00000, 114)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 107)
	switch {
	case isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 115)
		if uint64(a) < uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 120)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 116)
		if b >= 0 && uint64(a) < uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 121)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 117)
		if b < 0 && uint64(a) > math.MaxUint64-uint64(-b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 122)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 118)
		if uint64(a-math.MinInt64) < uint64(b) {
			trace_util_0.Count(_builtin_arithmetic_00000, 123)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 119)
		if (a > 0 && -b > math.MaxInt64-a) || (a < 0 && -b < math.MinInt64-a) {
			trace_util_0.Count(_builtin_arithmetic_00000, 124)
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 108)
	return a - b, false, nil
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 125)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 127)
		return nil, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 126)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 128)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMultiplyRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig, nil
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 129)
		if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
			trace_util_0.Count(_builtin_arithmetic_00000, 130)
			bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
			setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
			sig := &builtinArithmeticMultiplyDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_MultiplyDecimal)
			return sig, nil
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 131)
			{
				bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
				if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
					trace_util_0.Count(_builtin_arithmetic_00000, 133)
					bf.tp.Flag |= mysql.UnsignedFlag
					setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
					sig := &builtinArithmeticMultiplyIntUnsignedSig{bf}
					sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
					return sig, nil
				}
				trace_util_0.Count(_builtin_arithmetic_00000, 132)
				setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
				sig := &builtinArithmeticMultiplyIntSig{bf}
				sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
				return sig, nil
			}
		}
	}
}

type builtinArithmeticMultiplyRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 134)
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 135)
	newSig := &builtinArithmeticMultiplyDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntUnsignedSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntUnsignedSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 136)
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 137)
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 138)
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 142)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 139)
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 143)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 140)
	result := a * b
	if math.IsInf(result, 0) {
		trace_util_0.Count(_builtin_arithmetic_00000, 144)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 141)
	return result, false, nil
}

func (s *builtinArithmeticMultiplyDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 145)
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 149)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 146)
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 150)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 147)
	c := &types.MyDecimal{}
	err = types.DecimalMul(a, b, c)
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		trace_util_0.Count(_builtin_arithmetic_00000, 151)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 148)
	return c, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 152)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 156)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 153)
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 157)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 154)
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		trace_util_0.Count(_builtin_arithmetic_00000, 158)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 155)
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 159)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 163)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 160)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 164)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 161)
	result := a * b
	if a != 0 && result/a != b {
		trace_util_0.Count(_builtin_arithmetic_00000, 165)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 162)
	return result, false, nil
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 166)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 169)
		return nil, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 167)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 170)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		c.setType4DivReal(bf.tp)
		sig := &builtinArithmeticDivideRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DivideReal)
		return sig, nil
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 168)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
	c.setType4DivDecimal(bf.tp, lhsTp, rhsTp)
	sig := &builtinArithmeticDivideDecimalSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DivideDecimal)
	return sig, nil
}

type builtinArithmeticDivideRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticDivideRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 171)
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticDivideDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticDivideDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 172)
	newSig := &builtinArithmeticDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 173)
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 178)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 174)
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 179)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 175)
	if b == 0 {
		trace_util_0.Count(_builtin_arithmetic_00000, 180)
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 176)
	result := a / b
	if math.IsInf(result, 0) {
		trace_util_0.Count(_builtin_arithmetic_00000, 181)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 177)
	return result, false, nil
}

func (s *builtinArithmeticDivideDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 182)
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 186)
		return nil, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 183)
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 187)
		return nil, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 184)
	c := &types.MyDecimal{}
	err = types.DecimalDiv(a, b, c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		trace_util_0.Count(_builtin_arithmetic_00000, 188)
		return c, true, handleDivisionByZeroError(s.ctx)
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 189)
		if err == nil {
			trace_util_0.Count(_builtin_arithmetic_00000, 190)
			_, frac := c.PrecisionAndFrac()
			if frac < s.baseBuiltinFunc.tp.Decimal {
				trace_util_0.Count(_builtin_arithmetic_00000, 191)
				err = c.Round(c, s.baseBuiltinFunc.tp.Decimal, types.ModeHalfEven)
			}
		}
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 185)
	return c, false, err
}

type arithmeticIntDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticIntDivideFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 192)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 196)
		return nil, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 193)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETInt && rhsEvalTp == types.ETInt {
		trace_util_0.Count(_builtin_arithmetic_00000, 197)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
			trace_util_0.Count(_builtin_arithmetic_00000, 199)
			bf.tp.Flag |= mysql.UnsignedFlag
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 198)
		sig := &builtinArithmeticIntDivideIntSig{bf}
		return sig, nil
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 194)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDecimal, types.ETDecimal)
	if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
		trace_util_0.Count(_builtin_arithmetic_00000, 200)
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 195)
	sig := &builtinArithmeticIntDivideDecimalSig{bf}
	return sig, nil
}

type builtinArithmeticIntDivideIntSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticIntDivideIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 201)
	newSig := &builtinArithmeticIntDivideIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticIntDivideDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticIntDivideDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 202)
	newSig := &builtinArithmeticIntDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticIntDivideIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 203)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 208)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 204)
	if b == 0 {
		trace_util_0.Count(_builtin_arithmetic_00000, 209)
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 205)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 210)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 206)
	var (
		ret int64
		val uint64
	)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 211)
		ret = int64(uint64(a) / uint64(b))
	case isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 212)
		val, err = types.DivUintWithInt(uint64(a), b)
		ret = int64(val)
	case !isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 213)
		val, err = types.DivIntWithUint(a, uint64(b))
		ret = int64(val)
	case !isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 214)
		ret, err = types.DivInt64(a, b)
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 207)
	return ret, err != nil, err
}

func (s *builtinArithmeticIntDivideDecimalSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 215)
	sc := s.ctx.GetSessionVars().StmtCtx
	var num [2]*types.MyDecimal
	for i, arg := range s.args {
		trace_util_0.Count(_builtin_arithmetic_00000, 222)
		num[i], isNull, err = arg.EvalDecimal(s.ctx, row)
		// Its behavior is consistent with MySQL.
		if terror.ErrorEqual(err, types.ErrTruncated) {
			trace_util_0.Count(_builtin_arithmetic_00000, 225)
			err = nil
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 223)
		if terror.ErrorEqual(err, types.ErrOverflow) {
			trace_util_0.Count(_builtin_arithmetic_00000, 226)
			newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", arg)
			err = sc.HandleOverflow(newErr, newErr)
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 224)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_arithmetic_00000, 227)
			return 0, isNull, err
		}
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 216)
	c := &types.MyDecimal{}
	err = types.DecimalDiv(num[0], num[1], c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		trace_util_0.Count(_builtin_arithmetic_00000, 228)
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 217)
	if err == types.ErrTruncated {
		trace_util_0.Count(_builtin_arithmetic_00000, 229)
		err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 218)
	if err == types.ErrOverflow {
		trace_util_0.Count(_builtin_arithmetic_00000, 230)
		newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c)
		err = sc.HandleOverflow(newErr, newErr)
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 219)
	if err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 231)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 220)
	ret, err = c.ToInt()
	// err returned by ToInt may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
	if err == types.ErrOverflow {
		trace_util_0.Count(_builtin_arithmetic_00000, 232)
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s DIV %s)", s.args[0].String(), s.args[1].String()))
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 221)
	return ret, false, nil
}

type arithmeticModFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticModFunctionClass) setType4ModRealOrDecimal(retTp, a, b *types.FieldType, isDecimal bool) {
	trace_util_0.Count(_builtin_arithmetic_00000, 233)
	if a.Decimal == types.UnspecifiedLength || b.Decimal == types.UnspecifiedLength {
		trace_util_0.Count(_builtin_arithmetic_00000, 235)
		retTp.Decimal = types.UnspecifiedLength
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 236)
		{
			retTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
			if isDecimal && retTp.Decimal > mysql.MaxDecimalScale {
				trace_util_0.Count(_builtin_arithmetic_00000, 237)
				retTp.Decimal = mysql.MaxDecimalScale
			}
		}
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 234)
	if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
		trace_util_0.Count(_builtin_arithmetic_00000, 238)
		retTp.Flen = types.UnspecifiedLength
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 239)
		{
			retTp.Flen = mathutil.Max(a.Flen, b.Flen)
			if isDecimal {
				trace_util_0.Count(_builtin_arithmetic_00000, 241)
				retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxDecimalWidth)
				return
			}
			trace_util_0.Count(_builtin_arithmetic_00000, 240)
			retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxRealWidth)
		}
	}
}

func (c *arithmeticModFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 242)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 244)
		return nil, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 243)
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		trace_util_0.Count(_builtin_arithmetic_00000, 245)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		c.setType4ModRealOrDecimal(bf.tp, lhsTp, rhsTp, false)
		if mysql.HasUnsignedFlag(lhsTp.Flag) {
			trace_util_0.Count(_builtin_arithmetic_00000, 247)
			bf.tp.Flag |= mysql.UnsignedFlag
		}
		trace_util_0.Count(_builtin_arithmetic_00000, 246)
		sig := &builtinArithmeticModRealSig{bf}
		return sig, nil
	} else {
		trace_util_0.Count(_builtin_arithmetic_00000, 248)
		if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
			trace_util_0.Count(_builtin_arithmetic_00000, 249)
			bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
			c.setType4ModRealOrDecimal(bf.tp, lhsTp, rhsTp, true)
			if mysql.HasUnsignedFlag(lhsTp.Flag) {
				trace_util_0.Count(_builtin_arithmetic_00000, 251)
				bf.tp.Flag |= mysql.UnsignedFlag
			}
			trace_util_0.Count(_builtin_arithmetic_00000, 250)
			sig := &builtinArithmeticModDecimalSig{bf}
			return sig, nil
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 252)
			{
				bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
				if mysql.HasUnsignedFlag(lhsTp.Flag) {
					trace_util_0.Count(_builtin_arithmetic_00000, 254)
					bf.tp.Flag |= mysql.UnsignedFlag
				}
				trace_util_0.Count(_builtin_arithmetic_00000, 253)
				sig := &builtinArithmeticModIntSig{bf}
				return sig, nil
			}
		}
	}
}

type builtinArithmeticModRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 255)
	newSig := &builtinArithmeticModRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 256)
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 260)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 257)
	if b == 0 {
		trace_util_0.Count(_builtin_arithmetic_00000, 261)
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 258)
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 262)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 259)
	return math.Mod(a, b), false, nil
}

type builtinArithmeticModDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 263)
	newSig := &builtinArithmeticModDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 264)
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 268)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 265)
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 269)
		return nil, isNull, err
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 266)
	c := &types.MyDecimal{}
	err = types.DecimalMod(a, b, c)
	if err == types.ErrDivByZero {
		trace_util_0.Count(_builtin_arithmetic_00000, 270)
		return c, true, handleDivisionByZeroError(s.ctx)
	}
	trace_util_0.Count(_builtin_arithmetic_00000, 267)
	return c, err != nil, err
}

type builtinArithmeticModIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_arithmetic_00000, 271)
	newSig := &builtinArithmeticModIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_arithmetic_00000, 272)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 277)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 273)
	if b == 0 {
		trace_util_0.Count(_builtin_arithmetic_00000, 278)
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 274)
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_arithmetic_00000, 279)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 275)
	var ret int64
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 280)
		ret = int64(uint64(a) % uint64(b))
	case isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 281)
		if b < 0 {
			trace_util_0.Count(_builtin_arithmetic_00000, 284)
			ret = int64(uint64(a) % uint64(-b))
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 285)
			{
				ret = int64(uint64(a) % uint64(b))
			}
		}
	case !isLHSUnsigned && isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 282)
		if a < 0 {
			trace_util_0.Count(_builtin_arithmetic_00000, 286)
			ret = -int64(uint64(-a) % uint64(b))
		} else {
			trace_util_0.Count(_builtin_arithmetic_00000, 287)
			{
				ret = int64(uint64(a) % uint64(b))
			}
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		trace_util_0.Count(_builtin_arithmetic_00000, 283)
		ret = a % b
	}

	trace_util_0.Count(_builtin_arithmetic_00000, 276)
	return ret, false, nil
}

var _builtin_arithmetic_00000 = "expression/builtin_arithmetic.go"
