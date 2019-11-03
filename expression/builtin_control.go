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
	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &caseWhenFunctionClass{}
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinCaseWhenIntSig{}
	_ builtinFunc = &builtinCaseWhenRealSig{}
	_ builtinFunc = &builtinCaseWhenDecimalSig{}
	_ builtinFunc = &builtinCaseWhenStringSig{}
	_ builtinFunc = &builtinCaseWhenTimeSig{}
	_ builtinFunc = &builtinCaseWhenDurationSig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
	_ builtinFunc = &builtinIfNullJSONSig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfDecimalSig{}
	_ builtinFunc = &builtinIfStringSig{}
	_ builtinFunc = &builtinIfTimeSig{}
	_ builtinFunc = &builtinIfDurationSig{}
	_ builtinFunc = &builtinIfJSONSig{}
)

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, LEAD and LAG.
func InferType4ControlFuncs(lhs, rhs *types.FieldType) *types.FieldType {
	trace_util_0.Count(_builtin_control_00000, 0)
	resultFieldType := &types.FieldType{}
	if lhs.Tp == mysql.TypeNull {
		trace_util_0.Count(_builtin_control_00000, 3)
		*resultFieldType = *rhs
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.Tp == mysql.TypeNull {
			trace_util_0.Count(_builtin_control_00000, 4)
			resultFieldType.Tp = mysql.TypeString
			resultFieldType.Flen, resultFieldType.Decimal = 0, 0
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else {
		trace_util_0.Count(_builtin_control_00000, 5)
		if rhs.Tp == mysql.TypeNull {
			trace_util_0.Count(_builtin_control_00000, 6)
			*resultFieldType = *lhs
		} else {
			trace_util_0.Count(_builtin_control_00000, 7)
			{
				var unsignedFlag uint
				evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &unsignedFlag)
				resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
				if evalType == types.ETInt {
					trace_util_0.Count(_builtin_control_00000, 10)
					resultFieldType.Decimal = 0
				} else {
					trace_util_0.Count(_builtin_control_00000, 11)
					{
						if lhs.Decimal == types.UnspecifiedLength || rhs.Decimal == types.UnspecifiedLength {
							trace_util_0.Count(_builtin_control_00000, 12)
							resultFieldType.Decimal = types.UnspecifiedLength
						} else {
							trace_util_0.Count(_builtin_control_00000, 13)
							{
								resultFieldType.Decimal = mathutil.Max(lhs.Decimal, rhs.Decimal)
							}
						}
					}
				}
				trace_util_0.Count(_builtin_control_00000, 8)
				if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
					trace_util_0.Count(_builtin_control_00000, 14)
					resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
					if mysql.HasBinaryFlag(lhs.Flag) || !types.IsNonBinaryStr(rhs) {
						trace_util_0.Count(_builtin_control_00000, 15)
						resultFieldType.Flag |= mysql.BinaryFlag
					}
				} else {
					trace_util_0.Count(_builtin_control_00000, 16)
					if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
						trace_util_0.Count(_builtin_control_00000, 17)
						resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
						if mysql.HasBinaryFlag(rhs.Flag) || !types.IsNonBinaryStr(lhs) {
							trace_util_0.Count(_builtin_control_00000, 18)
							resultFieldType.Flag |= mysql.BinaryFlag
						}
					} else {
						trace_util_0.Count(_builtin_control_00000, 19)
						if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringKind() {
							trace_util_0.Count(_builtin_control_00000, 20)
							types.SetBinChsClnFlag(resultFieldType)
						} else {
							trace_util_0.Count(_builtin_control_00000, 21)
							{
								resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
							}
						}
					}
				}
				trace_util_0.Count(_builtin_control_00000, 9)
				if evalType == types.ETDecimal || evalType == types.ETInt {
					trace_util_0.Count(_builtin_control_00000, 22)
					lhsUnsignedFlag, rhsUnsignedFlag := mysql.HasUnsignedFlag(lhs.Flag), mysql.HasUnsignedFlag(rhs.Flag)
					lhsFlagLen, rhsFlagLen := 0, 0
					if !lhsUnsignedFlag {
						trace_util_0.Count(_builtin_control_00000, 27)
						lhsFlagLen = 1
					}
					trace_util_0.Count(_builtin_control_00000, 23)
					if !rhsUnsignedFlag {
						trace_util_0.Count(_builtin_control_00000, 28)
						rhsFlagLen = 1
					}
					trace_util_0.Count(_builtin_control_00000, 24)
					lhsFlen := lhs.Flen - lhsFlagLen
					rhsFlen := rhs.Flen - rhsFlagLen
					if lhs.Decimal != types.UnspecifiedLength {
						trace_util_0.Count(_builtin_control_00000, 29)
						lhsFlen -= lhs.Decimal
					}
					trace_util_0.Count(_builtin_control_00000, 25)
					if lhs.Decimal != types.UnspecifiedLength {
						trace_util_0.Count(_builtin_control_00000, 30)
						rhsFlen -= rhs.Decimal
					}
					trace_util_0.Count(_builtin_control_00000, 26)
					resultFieldType.Flen = mathutil.Max(lhsFlen, rhsFlen) + resultFieldType.Decimal + 1
				} else {
					trace_util_0.Count(_builtin_control_00000, 31)
					{
						resultFieldType.Flen = mathutil.Max(lhs.Flen, rhs.Flen)
					}
				}
			}
		}
	}
	// Fix decimal for int and string.
	trace_util_0.Count(_builtin_control_00000, 1)
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.ETInt {
		trace_util_0.Count(_builtin_control_00000, 32)
		resultFieldType.Decimal = 0
	} else {
		trace_util_0.Count(_builtin_control_00000, 33)
		if resultEvalType == types.ETString {
			trace_util_0.Count(_builtin_control_00000, 34)
			if lhs.Tp != mysql.TypeNull || rhs.Tp != mysql.TypeNull {
				trace_util_0.Count(_builtin_control_00000, 35)
				resultFieldType.Decimal = types.UnspecifiedLength
			}
		}
	}
	trace_util_0.Count(_builtin_control_00000, 2)
	return resultFieldType
}

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_control_00000, 36)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_control_00000, 47)
		return nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 37)
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	fieldTps := make([]*types.FieldType, 0, (l+1)/2)
	decimal, flen, isBinaryStr, isBinaryFlag := args[1].GetType().Decimal, 0, false, false
	for i := 1; i < l; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 48)
		fieldTps = append(fieldTps, args[i].GetType())
		decimal = mathutil.Max(decimal, args[i].GetType().Decimal)
		flen = mathutil.Max(flen, args[i].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[i].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[i].GetType())
	}
	trace_util_0.Count(_builtin_control_00000, 38)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 49)
		fieldTps = append(fieldTps, args[l-1].GetType())
		decimal = mathutil.Max(decimal, args[l-1].GetType().Decimal)
		flen = mathutil.Max(flen, args[l-1].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[l-1].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[l-1].GetType())
	}

	trace_util_0.Count(_builtin_control_00000, 39)
	fieldTp := types.AggFieldType(fieldTps)
	tp := fieldTp.EvalType()

	if tp == types.ETInt {
		trace_util_0.Count(_builtin_control_00000, 50)
		decimal = 0
	}
	trace_util_0.Count(_builtin_control_00000, 40)
	fieldTp.Decimal, fieldTp.Flen = decimal, flen
	if fieldTp.EvalType().IsStringKind() && !isBinaryStr {
		trace_util_0.Count(_builtin_control_00000, 51)
		fieldTp.Charset, fieldTp.Collate = charset.CharsetUTF8MB4, charset.CollationUTF8MB4
	}
	trace_util_0.Count(_builtin_control_00000, 41)
	if isBinaryFlag {
		trace_util_0.Count(_builtin_control_00000, 52)
		fieldTp.Flag |= mysql.BinaryFlag
	}
	// Set retType to BINARY(0) if all arguments are of type NULL.
	trace_util_0.Count(_builtin_control_00000, 42)
	if fieldTp.Tp == mysql.TypeNull {
		trace_util_0.Count(_builtin_control_00000, 53)
		fieldTp.Flen, fieldTp.Decimal = 0, -1
		types.SetBinChsClnFlag(fieldTp)
	}
	trace_util_0.Count(_builtin_control_00000, 43)
	argTps := make([]types.EvalType, 0, l)
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 54)
		argTps = append(argTps, types.ETInt, tp)
	}
	trace_util_0.Count(_builtin_control_00000, 44)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 55)
		argTps = append(argTps, tp)
	}
	trace_util_0.Count(_builtin_control_00000, 45)
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	bf.tp = fieldTp

	switch tp {
	case types.ETInt:
		trace_util_0.Count(_builtin_control_00000, 56)
		bf.tp.Decimal = 0
		sig = &builtinCaseWhenIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		trace_util_0.Count(_builtin_control_00000, 57)
		sig = &builtinCaseWhenRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_control_00000, 58)
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		trace_util_0.Count(_builtin_control_00000, 59)
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinCaseWhenStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_control_00000, 60)
		sig = &builtinCaseWhenTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_control_00000, 61)
		sig = &builtinCaseWhenDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_control_00000, 62)
		sig = &builtinCaseWhenJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenJson)
	}
	trace_util_0.Count(_builtin_control_00000, 46)
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 63)
	newSig := &builtinCaseWhenIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 64)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 67)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 70)
			return 0, isNull, err
		}
		trace_util_0.Count(_builtin_control_00000, 68)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 71)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 69)
		ret, isNull, err = args[i+1].EvalInt(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 65)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 72)
		ret, isNull, err = args[l-1].EvalInt(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 66)
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 73)
	newSig := &builtinCaseWhenRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 74)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 77)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 80)
			return 0, isNull, err
		}
		trace_util_0.Count(_builtin_control_00000, 78)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 81)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 79)
		ret, isNull, err = args[i+1].EvalReal(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 75)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 82)
		ret, isNull, err = args[l-1].EvalReal(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 76)
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 83)
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 84)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 87)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 90)
			return nil, isNull, err
		}
		trace_util_0.Count(_builtin_control_00000, 88)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 91)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 89)
		ret, isNull, err = args[i+1].EvalDecimal(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 85)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 92)
		ret, isNull, err = args[l-1].EvalDecimal(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 86)
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 93)
	newSig := &builtinCaseWhenStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenStringSig) evalString(row chunk.Row) (ret string, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 94)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 97)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 100)
			return "", isNull, err
		}
		trace_util_0.Count(_builtin_control_00000, 98)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 101)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 99)
		ret, isNull, err = args[i+1].EvalString(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 95)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 102)
		ret, isNull, err = args[l-1].EvalString(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 96)
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 103)
	newSig := &builtinCaseWhenTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 104)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 107)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 110)
			return ret, isNull, err
		}
		trace_util_0.Count(_builtin_control_00000, 108)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 111)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 109)
		ret, isNull, err = args[i+1].EvalTime(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 105)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 112)
		ret, isNull, err = args[l-1].EvalTime(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 106)
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 113)
	newSig := &builtinCaseWhenDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 114)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 117)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 120)
			return ret, true, err
		}
		trace_util_0.Count(_builtin_control_00000, 118)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 121)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 119)
		ret, isNull, err = args[i+1].EvalDuration(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 115)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 122)
		ret, isNull, err = args[l-1].EvalDuration(b.ctx, row)
		return ret, isNull, err
	}
	trace_util_0.Count(_builtin_control_00000, 116)
	return ret, true, nil
}

type builtinCaseWhenJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 123)
	newSig := &builtinCaseWhenJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinCaseWhenJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 124)
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		trace_util_0.Count(_builtin_control_00000, 127)
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_control_00000, 130)
			return
		}
		trace_util_0.Count(_builtin_control_00000, 128)
		if isNull || condition == 0 {
			trace_util_0.Count(_builtin_control_00000, 131)
			continue
		}
		trace_util_0.Count(_builtin_control_00000, 129)
		return args[i+1].EvalJSON(b.ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	trace_util_0.Count(_builtin_control_00000, 125)
	if l%2 == 1 {
		trace_util_0.Count(_builtin_control_00000, 132)
		return args[l-1].EvalJSON(b.ctx, row)
	}
	trace_util_0.Count(_builtin_control_00000, 126)
	return ret, true, nil
}

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_control_00000, 133)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_control_00000, 136)
		return nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 134)
	retTp := InferType4ControlFuncs(args[1].GetType(), args[2].GetType())
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, types.ETInt, evalTps, evalTps)
	retTp.Flag |= bf.tp.Flag
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		trace_util_0.Count(_builtin_control_00000, 137)
		sig = &builtinIfIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfInt)
	case types.ETReal:
		trace_util_0.Count(_builtin_control_00000, 138)
		sig = &builtinIfRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_control_00000, 139)
		sig = &builtinIfDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDecimal)
	case types.ETString:
		trace_util_0.Count(_builtin_control_00000, 140)
		sig = &builtinIfStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfString)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_control_00000, 141)
		sig = &builtinIfTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_control_00000, 142)
		sig = &builtinIfDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_control_00000, 143)
		sig = &builtinIfJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfJson)
	}
	trace_util_0.Count(_builtin_control_00000, 135)
	return sig, nil
}

type builtinIfIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 144)
	newSig := &builtinIfIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 145)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 148)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 146)
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 149)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 147)
	arg2, isNull2, err := b.args[2].EvalInt(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 150)
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 151)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 154)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 152)
	arg1, isNull1, err := b.args[1].EvalReal(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 155)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 153)
	arg2, isNull2, err := b.args[2].EvalReal(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 156)
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 157)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 160)
		return nil, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 158)
	arg1, isNull1, err := b.args[1].EvalDecimal(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 161)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 159)
	arg2, isNull2, err := b.args[2].EvalDecimal(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 162)
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(row chunk.Row) (ret string, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 163)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 166)
		return "", true, err
	}
	trace_util_0.Count(_builtin_control_00000, 164)
	arg1, isNull1, err := b.args[1].EvalString(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 167)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 165)
	arg2, isNull2, err := b.args[2].EvalString(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 168)
	newSig := &builtinIfTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 169)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 172)
		return ret, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 170)
	arg1, isNull1, err := b.args[1].EvalTime(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 173)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 171)
	arg2, isNull2, err := b.args[2].EvalTime(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 174)
	newSig := &builtinIfDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 175)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 178)
		return ret, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 176)
	arg1, isNull1, err := b.args[1].EvalDuration(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		trace_util_0.Count(_builtin_control_00000, 179)
		return arg1, isNull1, err
	}
	trace_util_0.Count(_builtin_control_00000, 177)
	arg2, isNull2, err := b.args[2].EvalDuration(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 180)
	newSig := &builtinIfJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_control_00000, 181)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 186)
		return ret, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 182)
	arg1, isNull1, err := b.args[1].EvalJSON(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 187)
		return ret, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 183)
	arg2, isNull2, err := b.args[2].EvalJSON(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_control_00000, 188)
		return ret, true, err
	}
	trace_util_0.Count(_builtin_control_00000, 184)
	switch {
	case isNull0 || arg0 == 0:
		trace_util_0.Count(_builtin_control_00000, 189)
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		trace_util_0.Count(_builtin_control_00000, 190)
		ret, isNull = arg1, isNull1
	}
	trace_util_0.Count(_builtin_control_00000, 185)
	return
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_control_00000, 191)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_control_00000, 195)
		return nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 192)
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp := InferType4ControlFuncs(lhs, rhs)
	retTp.Flag |= (lhs.Flag & mysql.NotNullFlag) | (rhs.Flag & mysql.NotNullFlag)
	if lhs.Tp == mysql.TypeNull && rhs.Tp == mysql.TypeNull {
		trace_util_0.Count(_builtin_control_00000, 196)
		retTp.Tp = mysql.TypeNull
		retTp.Flen, retTp.Decimal = 0, -1
		types.SetBinChsClnFlag(retTp)
	}
	trace_util_0.Count(_builtin_control_00000, 193)
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, evalTps, evalTps)
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		trace_util_0.Count(_builtin_control_00000, 197)
		sig = &builtinIfNullIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		trace_util_0.Count(_builtin_control_00000, 198)
		sig = &builtinIfNullRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_control_00000, 199)
		sig = &builtinIfNullDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDecimal)
	case types.ETString:
		trace_util_0.Count(_builtin_control_00000, 200)
		sig = &builtinIfNullStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullString)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_control_00000, 201)
		sig = &builtinIfNullTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_control_00000, 202)
		sig = &builtinIfNullDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_control_00000, 203)
		sig = &builtinIfNullJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullJson)
	}
	trace_util_0.Count(_builtin_control_00000, 194)
	return sig, nil
}

type builtinIfNullIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 204)
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 205)
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 207)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 206)
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 208)
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 209)
	arg0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 211)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 210)
	arg1, isNull, err := b.args[1].EvalReal(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 212)
	newSig := &builtinIfNullDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 213)
	arg0, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 215)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 214)
	arg1, isNull, err := b.args[1].EvalDecimal(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 216)
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 217)
	arg0, isNull, err := b.args[0].EvalString(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 219)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 218)
	arg1, isNull, err := b.args[1].EvalString(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 220)
	newSig := &builtinIfNullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 221)
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 223)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 222)
	arg1, isNull, err := b.args[1].EvalTime(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 224)
	newSig := &builtinIfNullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 225)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if !isNull || err != nil {
		trace_util_0.Count(_builtin_control_00000, 227)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 226)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_control_00000, 228)
	newSig := &builtinIfNullJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_builtin_control_00000, 229)
	arg0, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if !isNull {
		trace_util_0.Count(_builtin_control_00000, 231)
		return arg0, err != nil, err
	}
	trace_util_0.Count(_builtin_control_00000, 230)
	arg1, isNull, err := b.args[1].EvalJSON(b.ctx, row)
	return arg1, isNull || err != nil, err
}

var _builtin_control_00000 = "expression/builtin_control.go"
