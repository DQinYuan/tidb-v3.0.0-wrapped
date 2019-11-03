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
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &coalesceFunctionClass{}
	_ functionClass = &greatestFunctionClass{}
	_ functionClass = &leastFunctionClass{}
	_ functionClass = &intervalFunctionClass{}
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinCoalesceIntSig{}
	_ builtinFunc = &builtinCoalesceRealSig{}
	_ builtinFunc = &builtinCoalesceDecimalSig{}
	_ builtinFunc = &builtinCoalesceStringSig{}
	_ builtinFunc = &builtinCoalesceTimeSig{}
	_ builtinFunc = &builtinCoalesceDurationSig{}

	_ builtinFunc = &builtinGreatestIntSig{}
	_ builtinFunc = &builtinGreatestRealSig{}
	_ builtinFunc = &builtinGreatestDecimalSig{}
	_ builtinFunc = &builtinGreatestStringSig{}
	_ builtinFunc = &builtinGreatestTimeSig{}
	_ builtinFunc = &builtinLeastIntSig{}
	_ builtinFunc = &builtinLeastRealSig{}
	_ builtinFunc = &builtinLeastDecimalSig{}
	_ builtinFunc = &builtinLeastStringSig{}
	_ builtinFunc = &builtinLeastTimeSig{}
	_ builtinFunc = &builtinIntervalIntSig{}
	_ builtinFunc = &builtinIntervalRealSig{}

	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}

	_ builtinFunc = &builtinNullEQIntSig{}
	_ builtinFunc = &builtinNullEQRealSig{}
	_ builtinFunc = &builtinNullEQDecimalSig{}
	_ builtinFunc = &builtinNullEQStringSig{}
	_ builtinFunc = &builtinNullEQTimeSig{}
	_ builtinFunc = &builtinNullEQDurationSig{}
)

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	baseFunctionClass
}

func (c *coalesceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_compare_00000, 0)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_compare_00000, 6)
		return nil, err
	}

	trace_util_0.Count(_builtin_compare_00000, 1)
	fieldTps := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		trace_util_0.Count(_builtin_compare_00000, 7)
		fieldTps = append(fieldTps, arg.GetType())
	}

	// Use the aggregated field type as retType.
	trace_util_0.Count(_builtin_compare_00000, 2)
	resultFieldType := types.AggFieldType(fieldTps)
	resultEvalType := types.AggregateEvalType(fieldTps, &resultFieldType.Flag)
	retEvalTp := resultFieldType.EvalType()

	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_compare_00000, 8)
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	trace_util_0.Count(_builtin_compare_00000, 3)
	bf := newBaseBuiltinFuncWithTp(ctx, args, retEvalTp, fieldEvalTps...)

	bf.tp.Flag |= resultFieldType.Flag
	resultFieldType.Flen, resultFieldType.Decimal = 0, types.UnspecifiedLength

	// Set retType to BINARY(0) if all arguments are of type NULL.
	if resultFieldType.Tp == mysql.TypeNull {
		trace_util_0.Count(_builtin_compare_00000, 9)
		types.SetBinChsClnFlag(bf.tp)
	} else {
		trace_util_0.Count(_builtin_compare_00000, 10)
		{
			maxIntLen := 0
			maxFlen := 0

			// Find the max length of field in `maxFlen`,
			// and max integer-part length in `maxIntLen`.
			for _, argTp := range fieldTps {
				trace_util_0.Count(_builtin_compare_00000, 13)
				if argTp.Decimal > resultFieldType.Decimal {
					trace_util_0.Count(_builtin_compare_00000, 18)
					resultFieldType.Decimal = argTp.Decimal
				}
				trace_util_0.Count(_builtin_compare_00000, 14)
				argIntLen := argTp.Flen
				if argTp.Decimal > 0 {
					trace_util_0.Count(_builtin_compare_00000, 19)
					argIntLen -= argTp.Decimal + 1
				}

				// Reduce the sign bit if it is a signed integer/decimal
				trace_util_0.Count(_builtin_compare_00000, 15)
				if !mysql.HasUnsignedFlag(argTp.Flag) {
					trace_util_0.Count(_builtin_compare_00000, 20)
					argIntLen--
				}
				trace_util_0.Count(_builtin_compare_00000, 16)
				if argIntLen > maxIntLen {
					trace_util_0.Count(_builtin_compare_00000, 21)
					maxIntLen = argIntLen
				}
				trace_util_0.Count(_builtin_compare_00000, 17)
				if argTp.Flen > maxFlen || argTp.Flen == types.UnspecifiedLength {
					trace_util_0.Count(_builtin_compare_00000, 22)
					maxFlen = argTp.Flen
				}
			}
			// For integer, field length = maxIntLen + (1/0 for sign bit)
			// For decimal, field length = maxIntLen + maxDecimal + (1/0 for sign bit)
			trace_util_0.Count(_builtin_compare_00000, 11)
			if resultEvalType == types.ETInt || resultEvalType == types.ETDecimal {
				trace_util_0.Count(_builtin_compare_00000, 23)
				resultFieldType.Flen = maxIntLen + resultFieldType.Decimal
				if resultFieldType.Decimal > 0 {
					trace_util_0.Count(_builtin_compare_00000, 26)
					resultFieldType.Flen++
				}
				trace_util_0.Count(_builtin_compare_00000, 24)
				if !mysql.HasUnsignedFlag(resultFieldType.Flag) {
					trace_util_0.Count(_builtin_compare_00000, 27)
					resultFieldType.Flen++
				}
				trace_util_0.Count(_builtin_compare_00000, 25)
				bf.tp = resultFieldType
			} else {
				trace_util_0.Count(_builtin_compare_00000, 28)
				{
					bf.tp.Flen = maxFlen
				}
			}
			// Set the field length to maxFlen for other types.
			trace_util_0.Count(_builtin_compare_00000, 12)
			if bf.tp.Flen > mysql.MaxDecimalWidth {
				trace_util_0.Count(_builtin_compare_00000, 29)
				bf.tp.Flen = mysql.MaxDecimalWidth
			}
		}
	}

	trace_util_0.Count(_builtin_compare_00000, 4)
	switch retEvalTp {
	case types.ETInt:
		trace_util_0.Count(_builtin_compare_00000, 30)
		sig = &builtinCoalesceIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		trace_util_0.Count(_builtin_compare_00000, 31)
		sig = &builtinCoalesceRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_compare_00000, 32)
		sig = &builtinCoalesceDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		trace_util_0.Count(_builtin_compare_00000, 33)
		sig = &builtinCoalesceStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceString)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_compare_00000, 34)
		sig = &builtinCoalesceTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_compare_00000, 35)
		bf.tp.Decimal, err = getExpressionFsp(ctx, args[0])
		if err != nil {
			trace_util_0.Count(_builtin_compare_00000, 38)
			return nil, err
		}
		trace_util_0.Count(_builtin_compare_00000, 36)
		sig = &builtinCoalesceDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_compare_00000, 37)
		sig = &builtinCoalesceJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CoalesceJson)
	}

	trace_util_0.Count(_builtin_compare_00000, 5)
	return sig, nil
}

// builtinCoalesceIntSig is buitin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 39)
	newSig := &builtinCoalesceIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 40)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 42)
		res, isNull, err = a.EvalInt(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 43)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 41)
	return res, isNull, err
}

// builtinCoalesceRealSig is buitin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 44)
	newSig := &builtinCoalesceRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 45)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 47)
		res, isNull, err = a.EvalReal(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 48)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 46)
	return res, isNull, err
}

// builtinCoalesceDecimalSig is buitin function coalesce signature which return type Decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 49)
	newSig := &builtinCoalesceDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 50)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 52)
		res, isNull, err = a.EvalDecimal(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 53)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 51)
	return res, isNull, err
}

// builtinCoalesceStringSig is buitin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 54)
	newSig := &builtinCoalesceStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceStringSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 55)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 57)
		res, isNull, err = a.EvalString(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 58)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 56)
	return res, isNull, err
}

// builtinCoalesceTimeSig is buitin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 59)
	newSig := &builtinCoalesceTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 60)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 62)
		res, isNull, err = a.EvalTime(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 63)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 61)
	return res, isNull, err
}

// builtinCoalesceDurationSig is buitin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 64)
	newSig := &builtinCoalesceDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 65)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 67)
		res, isNull, err = a.EvalDuration(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 68)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 66)
	return res, isNull, err
}

// builtinCoalesceJSONSig is buitin function coalesce signature which return type json.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCoalesceJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 69)
	newSig := &builtinCoalesceJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 70)
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_compare_00000, 72)
		res, isNull, err = a.EvalJSON(b.ctx, row)
		if err != nil || !isNull {
			trace_util_0.Count(_builtin_compare_00000, 73)
			break
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 71)
	return res, isNull, err
}

// temporalWithDateAsNumEvalType makes DATE, DATETIME, TIMESTAMP pretend to be numbers rather than strings.
func temporalWithDateAsNumEvalType(argTp *types.FieldType) (argEvalType types.EvalType, isStr bool, isTemporalWithDate bool) {
	trace_util_0.Count(_builtin_compare_00000, 74)
	argEvalType = argTp.EvalType()
	isStr, isTemporalWithDate = argEvalType.IsStringKind(), types.IsTemporalWithDate(argTp.Tp)
	if !isTemporalWithDate {
		trace_util_0.Count(_builtin_compare_00000, 77)
		return
	}
	trace_util_0.Count(_builtin_compare_00000, 75)
	if argTp.Decimal > 0 {
		trace_util_0.Count(_builtin_compare_00000, 78)
		argEvalType = types.ETDecimal
	} else {
		trace_util_0.Count(_builtin_compare_00000, 79)
		{
			argEvalType = types.ETInt
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 76)
	return
}

// GetCmpTp4MinMax gets compare type for GREATEST and LEAST and BETWEEN (mainly for datetime).
func GetCmpTp4MinMax(args []Expression) (argTp types.EvalType) {
	trace_util_0.Count(_builtin_compare_00000, 80)
	datetimeFound, isAllStr := false, true
	cmpEvalType, isStr, isTemporalWithDate := temporalWithDateAsNumEvalType(args[0].GetType())
	if !isStr {
		trace_util_0.Count(_builtin_compare_00000, 86)
		isAllStr = false
	}
	trace_util_0.Count(_builtin_compare_00000, 81)
	if isTemporalWithDate {
		trace_util_0.Count(_builtin_compare_00000, 87)
		datetimeFound = true
	}
	trace_util_0.Count(_builtin_compare_00000, 82)
	lft := args[0].GetType()
	for i := range args {
		trace_util_0.Count(_builtin_compare_00000, 88)
		rft := args[i].GetType()
		var tp types.EvalType
		tp, isStr, isTemporalWithDate = temporalWithDateAsNumEvalType(rft)
		if isTemporalWithDate {
			trace_util_0.Count(_builtin_compare_00000, 91)
			datetimeFound = true
		}
		trace_util_0.Count(_builtin_compare_00000, 89)
		if !isStr {
			trace_util_0.Count(_builtin_compare_00000, 92)
			isAllStr = false
		}
		trace_util_0.Count(_builtin_compare_00000, 90)
		cmpEvalType = getBaseCmpType(cmpEvalType, tp, lft, rft)
		lft = rft
	}
	trace_util_0.Count(_builtin_compare_00000, 83)
	argTp = cmpEvalType
	if cmpEvalType.IsStringKind() {
		trace_util_0.Count(_builtin_compare_00000, 93)
		argTp = types.ETString
	}
	trace_util_0.Count(_builtin_compare_00000, 84)
	if isAllStr && datetimeFound {
		trace_util_0.Count(_builtin_compare_00000, 94)
		argTp = types.ETDatetime
	}
	trace_util_0.Count(_builtin_compare_00000, 85)
	return argTp
}

type greatestFunctionClass struct {
	baseFunctionClass
}

func (c *greatestFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_compare_00000, 95)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_compare_00000, 101)
		return nil, err
	}
	trace_util_0.Count(_builtin_compare_00000, 96)
	tp, cmpAsDatetime := GetCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		trace_util_0.Count(_builtin_compare_00000, 102)
		cmpAsDatetime = true
		tp = types.ETString
	}
	trace_util_0.Count(_builtin_compare_00000, 97)
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		trace_util_0.Count(_builtin_compare_00000, 103)
		argTps[i] = tp
	}
	trace_util_0.Count(_builtin_compare_00000, 98)
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		trace_util_0.Count(_builtin_compare_00000, 104)
		tp = types.ETDatetime
	}
	trace_util_0.Count(_builtin_compare_00000, 99)
	switch tp {
	case types.ETInt:
		trace_util_0.Count(_builtin_compare_00000, 105)
		sig = &builtinGreatestIntSig{bf}
	case types.ETReal:
		trace_util_0.Count(_builtin_compare_00000, 106)
		sig = &builtinGreatestRealSig{bf}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_compare_00000, 107)
		sig = &builtinGreatestDecimalSig{bf}
	case types.ETString:
		trace_util_0.Count(_builtin_compare_00000, 108)
		sig = &builtinGreatestStringSig{bf}
	case types.ETDatetime:
		trace_util_0.Count(_builtin_compare_00000, 109)
		sig = &builtinGreatestTimeSig{bf}
	}
	trace_util_0.Count(_builtin_compare_00000, 100)
	return sig, nil
}

type builtinGreatestIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 110)
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(row chunk.Row) (max int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 111)
	max, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 114)
		return max, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 112)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 115)
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 117)
			return max, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 116)
		if v > max {
			trace_util_0.Count(_builtin_compare_00000, 118)
			max = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 113)
	return
}

type builtinGreatestRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 119)
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(row chunk.Row) (max float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 120)
	max, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 123)
		return max, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 121)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 124)
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 126)
			return max, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 125)
		if v > max {
			trace_util_0.Count(_builtin_compare_00000, 127)
			max = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 122)
	return
}

type builtinGreatestDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 128)
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(row chunk.Row) (max *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 129)
	max, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 132)
		return max, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 130)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 133)
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 135)
			return max, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 134)
		if v.Compare(max) > 0 {
			trace_util_0.Count(_builtin_compare_00000, 136)
			max = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 131)
	return
}

type builtinGreatestStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 137)
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) evalString(row chunk.Row) (max string, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 138)
	max, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 141)
		return max, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 139)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 142)
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 144)
			return max, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 143)
		if types.CompareString(v, max) > 0 {
			trace_util_0.Count(_builtin_compare_00000, 145)
			max = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 140)
	return
}

type builtinGreatestTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGreatestTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 146)
	newSig := &builtinGreatestTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestTimeSig) evalString(row chunk.Row) (_ string, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 147)
	var (
		v string
		t types.Time
	)
	max := types.ZeroDatetime
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 149)
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 152)
			return "", true, err
		}
		trace_util_0.Count(_builtin_compare_00000, 150)
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			trace_util_0.Count(_builtin_compare_00000, 153)
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				trace_util_0.Count(_builtin_compare_00000, 155)
				return v, true, err
			}
			trace_util_0.Count(_builtin_compare_00000, 154)
			continue
		}
		trace_util_0.Count(_builtin_compare_00000, 151)
		if t.Compare(max) > 0 {
			trace_util_0.Count(_builtin_compare_00000, 156)
			max = t
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 148)
	return max.String(), false, nil
}

type leastFunctionClass struct {
	baseFunctionClass
}

func (c *leastFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_compare_00000, 157)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_compare_00000, 163)
		return nil, err
	}
	trace_util_0.Count(_builtin_compare_00000, 158)
	tp, cmpAsDatetime := GetCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		trace_util_0.Count(_builtin_compare_00000, 164)
		cmpAsDatetime = true
		tp = types.ETString
	}
	trace_util_0.Count(_builtin_compare_00000, 159)
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		trace_util_0.Count(_builtin_compare_00000, 165)
		argTps[i] = tp
	}
	trace_util_0.Count(_builtin_compare_00000, 160)
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		trace_util_0.Count(_builtin_compare_00000, 166)
		tp = types.ETDatetime
	}
	trace_util_0.Count(_builtin_compare_00000, 161)
	switch tp {
	case types.ETInt:
		trace_util_0.Count(_builtin_compare_00000, 167)
		sig = &builtinLeastIntSig{bf}
	case types.ETReal:
		trace_util_0.Count(_builtin_compare_00000, 168)
		sig = &builtinLeastRealSig{bf}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_compare_00000, 169)
		sig = &builtinLeastDecimalSig{bf}
	case types.ETString:
		trace_util_0.Count(_builtin_compare_00000, 170)
		sig = &builtinLeastStringSig{bf}
	case types.ETDatetime:
		trace_util_0.Count(_builtin_compare_00000, 171)
		sig = &builtinLeastTimeSig{bf}
	}
	trace_util_0.Count(_builtin_compare_00000, 162)
	return sig, nil
}

type builtinLeastIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 172)
	newSig := &builtinLeastIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastIntSig) evalInt(row chunk.Row) (min int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 173)
	min, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 176)
		return min, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 174)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 177)
		var v int64
		v, isNull, err = b.args[i].EvalInt(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 179)
			return min, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 178)
		if v < min {
			trace_util_0.Count(_builtin_compare_00000, 180)
			min = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 175)
	return
}

type builtinLeastRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 181)
	newSig := &builtinLeastRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(row chunk.Row) (min float64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 182)
	min, isNull, err = b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 185)
		return min, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 183)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 186)
		var v float64
		v, isNull, err = b.args[i].EvalReal(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 188)
			return min, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 187)
		if v < min {
			trace_util_0.Count(_builtin_compare_00000, 189)
			min = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 184)
	return
}

type builtinLeastDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 190)
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(row chunk.Row) (min *types.MyDecimal, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 191)
	min, isNull, err = b.args[0].EvalDecimal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 194)
		return min, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 192)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 195)
		var v *types.MyDecimal
		v, isNull, err = b.args[i].EvalDecimal(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 197)
			return min, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 196)
		if v.Compare(min) < 0 {
			trace_util_0.Count(_builtin_compare_00000, 198)
			min = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 193)
	return
}

type builtinLeastStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 199)
	newSig := &builtinLeastStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) evalString(row chunk.Row) (min string, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 200)
	min, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 203)
		return min, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 201)
	for i := 1; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 204)
		var v string
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 206)
			return min, isNull, err
		}
		trace_util_0.Count(_builtin_compare_00000, 205)
		if types.CompareString(v, min) < 0 {
			trace_util_0.Count(_builtin_compare_00000, 207)
			min = v
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 202)
	return
}

type builtinLeastTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLeastTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 208)
	newSig := &builtinLeastTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastTimeSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 209)
	var (
		v string
		t types.Time
	)
	min := types.Time{
		Time: types.MaxDatetime,
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	findInvalidTime := false
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_compare_00000, 212)
		v, isNull, err = b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_compare_00000, 215)
			return "", true, err
		}
		trace_util_0.Count(_builtin_compare_00000, 213)
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			trace_util_0.Count(_builtin_compare_00000, 216)
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				trace_util_0.Count(_builtin_compare_00000, 217)
				return v, true, err
			} else {
				trace_util_0.Count(_builtin_compare_00000, 218)
				if !findInvalidTime {
					trace_util_0.Count(_builtin_compare_00000, 219)
					res = v
					findInvalidTime = true
				}
			}
		}
		trace_util_0.Count(_builtin_compare_00000, 214)
		if t.Compare(min) < 0 {
			trace_util_0.Count(_builtin_compare_00000, 220)
			min = t
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 210)
	if !findInvalidTime {
		trace_util_0.Count(_builtin_compare_00000, 221)
		res = min.String()
	}
	trace_util_0.Count(_builtin_compare_00000, 211)
	return res, false, nil
}

type intervalFunctionClass struct {
	baseFunctionClass
}

func (c *intervalFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_compare_00000, 222)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_compare_00000, 228)
		return nil, err
	}

	trace_util_0.Count(_builtin_compare_00000, 223)
	allInt := true
	for i := range args {
		trace_util_0.Count(_builtin_compare_00000, 229)
		if args[i].GetType().EvalType() != types.ETInt {
			trace_util_0.Count(_builtin_compare_00000, 230)
			allInt = false
		}
	}

	trace_util_0.Count(_builtin_compare_00000, 224)
	argTps, argTp := make([]types.EvalType, 0, len(args)), types.ETReal
	if allInt {
		trace_util_0.Count(_builtin_compare_00000, 231)
		argTp = types.ETInt
	}
	trace_util_0.Count(_builtin_compare_00000, 225)
	for range args {
		trace_util_0.Count(_builtin_compare_00000, 232)
		argTps = append(argTps, argTp)
	}
	trace_util_0.Count(_builtin_compare_00000, 226)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig builtinFunc
	if allInt {
		trace_util_0.Count(_builtin_compare_00000, 233)
		sig = &builtinIntervalIntSig{bf}
	} else {
		trace_util_0.Count(_builtin_compare_00000, 234)
		{
			sig = &builtinIntervalRealSig{bf}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 227)
	return sig, nil
}

type builtinIntervalIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIntervalIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 235)
	newSig := &builtinIntervalIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 236)
	args0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 239)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 237)
	if isNull {
		trace_util_0.Count(_builtin_compare_00000, 240)
		return -1, false, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 238)
	idx, err := b.binSearch(args0, mysql.HasUnsignedFlag(b.args[0].GetType().Flag), b.args[1:], row)
	return int64(idx), err != nil, err
}

// binSearch is a binary search method.
// All arguments are treated as integers.
// It is required that arg[0] < args[1] < args[2] < ... < args[n] for this function to work correctly.
// This is because a binary search is used (very fast).
func (b *builtinIntervalIntSig) binSearch(target int64, isUint1 bool, args []Expression, row chunk.Row) (_ int, err error) {
	trace_util_0.Count(_builtin_compare_00000, 241)
	i, j, cmp := 0, len(args), false
	for i < j {
		trace_util_0.Count(_builtin_compare_00000, 243)
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(b.ctx, row)
		if err1 != nil {
			trace_util_0.Count(_builtin_compare_00000, 247)
			err = err1
			break
		}
		trace_util_0.Count(_builtin_compare_00000, 244)
		if isNull {
			trace_util_0.Count(_builtin_compare_00000, 248)
			v = target
		}
		trace_util_0.Count(_builtin_compare_00000, 245)
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType().Flag)
		switch {
		case !isUint1 && !isUint2:
			trace_util_0.Count(_builtin_compare_00000, 249)
			cmp = target < v
		case isUint1 && isUint2:
			trace_util_0.Count(_builtin_compare_00000, 250)
			cmp = uint64(target) < uint64(v)
		case !isUint1 && isUint2:
			trace_util_0.Count(_builtin_compare_00000, 251)
			cmp = target < 0 || uint64(target) < uint64(v)
		case isUint1 && !isUint2:
			trace_util_0.Count(_builtin_compare_00000, 252)
			cmp = v > 0 && uint64(target) < uint64(v)
		}
		trace_util_0.Count(_builtin_compare_00000, 246)
		if !cmp {
			trace_util_0.Count(_builtin_compare_00000, 253)
			i = mid + 1
		} else {
			trace_util_0.Count(_builtin_compare_00000, 254)
			{
				j = mid
			}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 242)
	return i, err
}

type builtinIntervalRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIntervalRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 255)
	newSig := &builtinIntervalRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 256)
	args0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 259)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 257)
	if isNull {
		trace_util_0.Count(_builtin_compare_00000, 260)
		return -1, false, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 258)
	idx, err := b.binSearch(args0, b.args[1:], row)
	return int64(idx), err != nil, err
}

func (b *builtinIntervalRealSig) binSearch(target float64, args []Expression, row chunk.Row) (_ int, err error) {
	trace_util_0.Count(_builtin_compare_00000, 261)
	i, j := 0, len(args)
	for i < j {
		trace_util_0.Count(_builtin_compare_00000, 263)
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_compare_00000, 265)
			err = err1
			break
		}
		trace_util_0.Count(_builtin_compare_00000, 264)
		if isNull {
			trace_util_0.Count(_builtin_compare_00000, 266)
			i = mid + 1
		} else {
			trace_util_0.Count(_builtin_compare_00000, 267)
			if cmp := target < v; !cmp {
				trace_util_0.Count(_builtin_compare_00000, 268)
				i = mid + 1
			} else {
				trace_util_0.Count(_builtin_compare_00000, 269)
				{
					j = mid
				}
			}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 262)
	return i, err
}

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	trace_util_0.Count(_builtin_compare_00000, 270)
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		trace_util_0.Count(_builtin_compare_00000, 273)
		if lft.Tp == rft.Tp {
			trace_util_0.Count(_builtin_compare_00000, 275)
			return types.ETString
		}
		trace_util_0.Count(_builtin_compare_00000, 274)
		if lft.Tp == mysql.TypeUnspecified {
			trace_util_0.Count(_builtin_compare_00000, 276)
			lhs = rhs
		} else {
			trace_util_0.Count(_builtin_compare_00000, 277)
			{
				rhs = lhs
			}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 271)
	if lhs.IsStringKind() && rhs.IsStringKind() {
		trace_util_0.Count(_builtin_compare_00000, 278)
		return types.ETString
	} else {
		trace_util_0.Count(_builtin_compare_00000, 279)
		if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
			trace_util_0.Count(_builtin_compare_00000, 280)
			return types.ETInt
		} else {
			trace_util_0.Count(_builtin_compare_00000, 281)
			if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
				((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
				trace_util_0.Count(_builtin_compare_00000, 282)
				return types.ETDecimal
			}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 272)
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	trace_util_0.Count(_builtin_compare_00000, 283)
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.Tp == mysql.TypeJSON) ||
		(lhsFieldType.Tp == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		trace_util_0.Count(_builtin_compare_00000, 285)
		cmpType = types.ETJson
	} else {
		trace_util_0.Count(_builtin_compare_00000, 286)
		if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.Tp) || types.IsTypeTime(rhsFieldType.Tp)) {
			trace_util_0.Count(_builtin_compare_00000, 287)
			// date[time] <cmp> date[time]
			// string <cmp> date[time]
			// compare as time
			if lhsFieldType.Tp == rhsFieldType.Tp {
				trace_util_0.Count(_builtin_compare_00000, 288)
				cmpType = lhsFieldType.EvalType()
			} else {
				trace_util_0.Count(_builtin_compare_00000, 289)
				{
					cmpType = types.ETDatetime
				}
			}
		} else {
			trace_util_0.Count(_builtin_compare_00000, 290)
			if lhsFieldType.Tp == mysql.TypeDuration && rhsFieldType.Tp == mysql.TypeDuration {
				trace_util_0.Count(_builtin_compare_00000, 291)
				// duration <cmp> duration
				// compare as duration
				cmpType = types.ETDuration
			} else {
				trace_util_0.Count(_builtin_compare_00000, 292)
				if cmpType == types.ETReal || cmpType == types.ETString {
					trace_util_0.Count(_builtin_compare_00000, 293)
					_, isLHSConst := lhs.(*Constant)
					_, isRHSConst := rhs.(*Constant)
					if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
						(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
						trace_util_0.Count(_builtin_compare_00000, 294)
						/*
							<non-const decimal expression> <cmp> <const string expression>
							or
							<const string expression> <cmp> <non-const decimal expression>

							Do comparison as decimal rather than float, in order not to lose precision.
						)*/
						cmpType = types.ETDecimal
					} else {
						trace_util_0.Count(_builtin_compare_00000, 295)
						if isTemporalColumn(lhs) && isRHSConst ||
							isTemporalColumn(rhs) && isLHSConst {
							trace_util_0.Count(_builtin_compare_00000, 296)
							/*
								<temporal column> <cmp> <non-temporal constant>
								or
								<non-temporal constant> <cmp> <temporal column>

								Convert the constant to temporal type.
							*/
							col, isLHSColumn := lhs.(*Column)
							if !isLHSColumn {
								trace_util_0.Count(_builtin_compare_00000, 298)
								col = rhs.(*Column)
							}
							trace_util_0.Count(_builtin_compare_00000, 297)
							if col.GetType().Tp == mysql.TypeDuration {
								trace_util_0.Count(_builtin_compare_00000, 299)
								cmpType = types.ETDuration
							} else {
								trace_util_0.Count(_builtin_compare_00000, 300)
								{
									cmpType = types.ETDatetime
								}
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 284)
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(lhs, rhs Expression) CompareFunc {
	trace_util_0.Count(_builtin_compare_00000, 301)
	switch GetAccurateCmpType(lhs, rhs) {
	case types.ETInt:
		trace_util_0.Count(_builtin_compare_00000, 303)
		return CompareInt
	case types.ETReal:
		trace_util_0.Count(_builtin_compare_00000, 304)
		return CompareReal
	case types.ETDecimal:
		trace_util_0.Count(_builtin_compare_00000, 305)
		return CompareDecimal
	case types.ETString:
		trace_util_0.Count(_builtin_compare_00000, 306)
		return CompareString
	case types.ETDuration:
		trace_util_0.Count(_builtin_compare_00000, 307)
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_compare_00000, 308)
		return CompareTime
	case types.ETJson:
		trace_util_0.Count(_builtin_compare_00000, 309)
		return CompareJSON
	}
	trace_util_0.Count(_builtin_compare_00000, 302)
	return nil
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	trace_util_0.Count(_builtin_compare_00000, 310)
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		trace_util_0.Count(_builtin_compare_00000, 313)
		return false
	}
	trace_util_0.Count(_builtin_compare_00000, 311)
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		trace_util_0.Count(_builtin_compare_00000, 314)
		return false
	}
	trace_util_0.Count(_builtin_compare_00000, 312)
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
func tryToConvertConstantInt(ctx sessionctx.Context, isUnsigned bool, con *Constant) (_ *Constant, isAlwaysFalse bool) {
	trace_util_0.Count(_builtin_compare_00000, 315)
	if con.GetType().EvalType() == types.ETInt {
		trace_util_0.Count(_builtin_compare_00000, 320)
		return con, false
	}
	trace_util_0.Count(_builtin_compare_00000, 316)
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 321)
		return con, false
	}
	trace_util_0.Count(_builtin_compare_00000, 317)
	sc := ctx.GetSessionVars().StmtCtx
	fieldType := types.NewFieldType(mysql.TypeLonglong)
	if isUnsigned {
		trace_util_0.Count(_builtin_compare_00000, 322)
		fieldType.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_builtin_compare_00000, 318)
	dt, err = dt.ConvertTo(sc, fieldType)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 323)
		return con, terror.ErrorEqual(err, types.ErrOverflow)
	}
	trace_util_0.Count(_builtin_compare_00000, 319)
	return &Constant{
		Value:        dt,
		RetType:      fieldType,
		DeferredExpr: con.DeferredExpr,
	}, false
}

// RefineComparedConstant changes an non-integer constant argument to its ceiling or floor result by the given op.
// isAlwaysFalse indicates whether the int column "con" is false.
func RefineComparedConstant(ctx sessionctx.Context, isUnsigned bool, con *Constant, op opcode.Op) (_ *Constant, isAlwaysFalse bool) {
	trace_util_0.Count(_builtin_compare_00000, 324)
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 331)
		return con, false
	}
	trace_util_0.Count(_builtin_compare_00000, 325)
	sc := ctx.GetSessionVars().StmtCtx
	intFieldType := types.NewFieldType(mysql.TypeLonglong)
	if isUnsigned {
		trace_util_0.Count(_builtin_compare_00000, 332)
		intFieldType.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_builtin_compare_00000, 326)
	var intDatum types.Datum
	intDatum, err = dt.ConvertTo(sc, intFieldType)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 333)
		return con, terror.ErrorEqual(err, types.ErrOverflow)
	}
	trace_util_0.Count(_builtin_compare_00000, 327)
	c, err := intDatum.CompareDatum(sc, &con.Value)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 334)
		return con, false
	}
	trace_util_0.Count(_builtin_compare_00000, 328)
	if c == 0 {
		trace_util_0.Count(_builtin_compare_00000, 335)
		return &Constant{
			Value:        intDatum,
			RetType:      intFieldType,
			DeferredExpr: con.DeferredExpr,
		}, false
	}
	trace_util_0.Count(_builtin_compare_00000, 329)
	switch op {
	case opcode.LT, opcode.GE:
		trace_util_0.Count(_builtin_compare_00000, 336)
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			trace_util_0.Count(_builtin_compare_00000, 339)
			return tryToConvertConstantInt(ctx, isUnsigned, resultCon)
		}
	case opcode.LE, opcode.GT:
		trace_util_0.Count(_builtin_compare_00000, 337)
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			trace_util_0.Count(_builtin_compare_00000, 340)
			return tryToConvertConstantInt(ctx, isUnsigned, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		trace_util_0.Count(_builtin_compare_00000, 338)
		switch con.RetType.EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			trace_util_0.Count(_builtin_compare_00000, 341)
			return con, true
		case types.ETString:
			trace_util_0.Count(_builtin_compare_00000, 342)
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(sc, types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				trace_util_0.Count(_builtin_compare_00000, 346)
				return con, false
			}
			trace_util_0.Count(_builtin_compare_00000, 343)
			if c, err = doubleDatum.CompareDatum(sc, &intDatum); err != nil {
				trace_util_0.Count(_builtin_compare_00000, 347)
				return con, false
			}
			trace_util_0.Count(_builtin_compare_00000, 344)
			if c != 0 {
				trace_util_0.Count(_builtin_compare_00000, 348)
				return con, true
			}
			trace_util_0.Count(_builtin_compare_00000, 345)
			return &Constant{
				Value:        intDatum,
				RetType:      intFieldType,
				DeferredExpr: con.DeferredExpr,
			}, false
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 330)
	return con, false
}

// refineArgs will rewrite the arguments if the compare expression is `int column <cmp> non-int constant` or
// `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
func (c *compareFunctionClass) refineArgs(ctx sessionctx.Context, args []Expression) []Expression {
	trace_util_0.Count(_builtin_compare_00000, 349)
	arg0Type, arg1Type := args[0].GetType(), args[1].GetType()
	arg0IsInt := arg0Type.EvalType() == types.ETInt
	arg1IsInt := arg1Type.EvalType() == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isAlways, finalArg0, finalArg1 := false, args[0], args[1]
	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		trace_util_0.Count(_builtin_compare_00000, 354)
		finalArg1, isAlways = RefineComparedConstant(ctx, mysql.HasUnsignedFlag(arg0Type.Flag), arg1, c.op)
	}
	// non-int constant [cmp] int non-constant
	trace_util_0.Count(_builtin_compare_00000, 350)
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		trace_util_0.Count(_builtin_compare_00000, 355)
		finalArg0, isAlways = RefineComparedConstant(ctx, mysql.HasUnsignedFlag(arg1Type.Flag), arg0, symmetricOp[c.op])
	}
	trace_util_0.Count(_builtin_compare_00000, 351)
	if !isAlways {
		trace_util_0.Count(_builtin_compare_00000, 356)
		return []Expression{finalArg0, finalArg1}
	}
	trace_util_0.Count(_builtin_compare_00000, 352)
	switch c.op {
	case opcode.LT, opcode.LE:
		trace_util_0.Count(_builtin_compare_00000, 357)
		// This will always be true.
		return []Expression{Zero.Clone(), One.Clone()}
	case opcode.EQ, opcode.NullEQ, opcode.GT, opcode.GE:
		trace_util_0.Count(_builtin_compare_00000, 358)
		// This will always be false.
		return []Expression{One.Clone(), Zero.Clone()}
	}
	trace_util_0.Count(_builtin_compare_00000, 353)
	return args
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx sessionctx.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_compare_00000, 359)
	if err = c.verifyArgs(rawArgs); err != nil {
		trace_util_0.Count(_builtin_compare_00000, 361)
		return nil, err
	}
	trace_util_0.Count(_builtin_compare_00000, 360)
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sessionctx.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_compare_00000, 362)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, tp, tp)
	if tp == types.ETJson {
		trace_util_0.Count(_builtin_compare_00000, 365)
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			trace_util_0.Count(_builtin_compare_00000, 366)
			DisableParseJSONFlag4Expr(args[i])
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 363)
	bf.tp.Flen = 1
	switch tp {
	case types.ETInt:
		trace_util_0.Count(_builtin_compare_00000, 367)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 374)
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 375)
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 376)
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 377)
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 378)
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 379)
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 380)
			sig = &builtinNullEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETReal:
		trace_util_0.Count(_builtin_compare_00000, 368)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 381)
			sig = &builtinLTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 382)
			sig = &builtinLERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 383)
			sig = &builtinGTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 384)
			sig = &builtinGERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 385)
			sig = &builtinEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 386)
			sig = &builtinNERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEReal)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 387)
			sig = &builtinNullEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQReal)
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_compare_00000, 369)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 388)
			sig = &builtinLTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 389)
			sig = &builtinLEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 390)
			sig = &builtinGTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 391)
			sig = &builtinGEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 392)
			sig = &builtinEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 393)
			sig = &builtinNEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDecimal)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 394)
			sig = &builtinNullEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDecimal)
		}
	case types.ETString:
		trace_util_0.Count(_builtin_compare_00000, 370)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 395)
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 396)
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 397)
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 398)
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 399)
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 400)
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 401)
			sig = &builtinNullEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	case types.ETDuration:
		trace_util_0.Count(_builtin_compare_00000, 371)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 402)
			sig = &builtinLTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 403)
			sig = &builtinLEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 404)
			sig = &builtinGTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 405)
			sig = &builtinGEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 406)
			sig = &builtinEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 407)
			sig = &builtinNEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDuration)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 408)
			sig = &builtinNullEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_compare_00000, 372)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 409)
			sig = &builtinLTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 410)
			sig = &builtinLETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 411)
			sig = &builtinGTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 412)
			sig = &builtinGETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 413)
			sig = &builtinEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 414)
			sig = &builtinNETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NETime)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 415)
			sig = &builtinNullEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQTime)
		}
	case types.ETJson:
		trace_util_0.Count(_builtin_compare_00000, 373)
		switch c.op {
		case opcode.LT:
			trace_util_0.Count(_builtin_compare_00000, 416)
			sig = &builtinLTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			trace_util_0.Count(_builtin_compare_00000, 417)
			sig = &builtinLEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			trace_util_0.Count(_builtin_compare_00000, 418)
			sig = &builtinGTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			trace_util_0.Count(_builtin_compare_00000, 419)
			sig = &builtinGEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			trace_util_0.Count(_builtin_compare_00000, 420)
			sig = &builtinEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			trace_util_0.Count(_builtin_compare_00000, 421)
			sig = &builtinNEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEJson)
		case opcode.NullEQ:
			trace_util_0.Count(_builtin_compare_00000, 422)
			sig = &builtinNullEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQJson)
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 364)
	return
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 423)
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 424)
	return resOfLT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLTRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 425)
	newSig := &builtinLTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 426)
	return resOfLT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 427)
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 428)
	return resOfLT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 429)
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 430)
	return resOfLT(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 431)
	newSig := &builtinLTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 432)
	return resOfLT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLTTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 433)
	newSig := &builtinLTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 434)
	return resOfLT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLTJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 435)
	newSig := &builtinLTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 436)
	return resOfLT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 437)
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 438)
	return resOfLE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLERealSig struct {
	baseBuiltinFunc
}

func (b *builtinLERealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 439)
	newSig := &builtinLERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 440)
	return resOfLE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 441)
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 442)
	return resOfLE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 443)
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 444)
	return resOfLE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 445)
	newSig := &builtinLEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 446)
	return resOfLE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLETimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 447)
	newSig := &builtinLETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 448)
	return resOfLE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLEJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 449)
	newSig := &builtinLEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 450)
	return resOfLE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 451)
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 452)
	return resOfGT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGTRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 453)
	newSig := &builtinGTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 454)
	return resOfGT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 455)
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 456)
	return resOfGT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 457)
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 458)
	return resOfGT(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 459)
	newSig := &builtinGTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 460)
	return resOfGT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGTTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 461)
	newSig := &builtinGTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 462)
	return resOfGT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGTJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 463)
	newSig := &builtinGTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 464)
	return resOfGT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 465)
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 466)
	return resOfGE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGERealSig struct {
	baseBuiltinFunc
}

func (b *builtinGERealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 467)
	newSig := &builtinGERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 468)
	return resOfGE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 469)
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 470)
	return resOfGE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 471)
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 472)
	return resOfGE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 473)
	newSig := &builtinGEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 474)
	return resOfGE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGETimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 475)
	newSig := &builtinGETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 476)
	return resOfGE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGEJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 477)
	newSig := &builtinGEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 478)
	return resOfGE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 479)
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 480)
	return resOfEQ(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinEQRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 481)
	newSig := &builtinEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 482)
	return resOfEQ(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 483)
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 484)
	return resOfEQ(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 485)
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 486)
	return resOfEQ(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 487)
	newSig := &builtinEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 488)
	return resOfEQ(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinEQTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 489)
	newSig := &builtinEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 490)
	return resOfEQ(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinEQJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 491)
	newSig := &builtinEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 492)
	return resOfEQ(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 493)
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 494)
	return resOfNE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNERealSig struct {
	baseBuiltinFunc
}

func (b *builtinNERealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 495)
	newSig := &builtinNERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 496)
	return resOfNE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 497)
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 498)
	return resOfNE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 499)
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 500)
	return resOfNE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 501)
	newSig := &builtinNEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 502)
	return resOfNE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNETimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 503)
	newSig := &builtinNETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 504)
	return resOfNE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNEJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 505)
	newSig := &builtinNEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 506)
	return resOfNE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNullEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 507)
	newSig := &builtinNullEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 508)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 512)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_compare_00000, 509)
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 513)
		return 0, isNull1, err
	}
	trace_util_0.Count(_builtin_compare_00000, 510)
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 514)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 515)
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		trace_util_0.Count(_builtin_compare_00000, 516)
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 517)
		res = 1
	case isUnsigned0 && !isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 518)
		if arg1 < 0 || arg0 > math.MaxInt64 {
			trace_util_0.Count(_builtin_compare_00000, 522)
			break
		}
		trace_util_0.Count(_builtin_compare_00000, 519)
		if types.CompareInt64(arg0, arg1) == 0 {
			trace_util_0.Count(_builtin_compare_00000, 523)
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 520)
		if arg0 < 0 || arg1 > math.MaxInt64 {
			trace_util_0.Count(_builtin_compare_00000, 524)
			break
		}
		trace_util_0.Count(_builtin_compare_00000, 521)
		if types.CompareInt64(arg0, arg1) == 0 {
			trace_util_0.Count(_builtin_compare_00000, 525)
			res = 1
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 511)
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 526)
	newSig := &builtinNullEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 527)
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 531)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 528)
	arg1, isNull1, err := b.args[1].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 532)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 529)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 533)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 534)
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 535)
		res = 1
	}
	trace_util_0.Count(_builtin_compare_00000, 530)
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 536)
	newSig := &builtinNullEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 537)
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 541)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 538)
	arg1, isNull1, err := b.args[1].EvalDecimal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 542)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 539)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 543)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 544)
		break
	case arg0.Compare(arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 545)
		res = 1
	}
	trace_util_0.Count(_builtin_compare_00000, 540)
	return res, false, nil
}

type builtinNullEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 546)
	newSig := &builtinNullEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 547)
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 551)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 548)
	arg1, isNull1, err := b.args[1].EvalString(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 552)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 549)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 553)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 554)
		break
	case types.CompareString(arg0, arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 555)
		res = 1
	}
	trace_util_0.Count(_builtin_compare_00000, 550)
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 556)
	newSig := &builtinNullEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 557)
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 561)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 558)
	arg1, isNull1, err := b.args[1].EvalDuration(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 562)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 559)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 563)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 564)
		break
	case arg0.Compare(arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 565)
		res = 1
	}
	trace_util_0.Count(_builtin_compare_00000, 560)
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 566)
	newSig := &builtinNullEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 567)
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 571)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 568)
	arg1, isNull1, err := b.args[1].EvalTime(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 572)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 569)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 573)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 574)
		break
	case arg0.Compare(arg1) == 0:
		trace_util_0.Count(_builtin_compare_00000, 575)
		res = 1
	}
	trace_util_0.Count(_builtin_compare_00000, 570)
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_compare_00000, 576)
	newSig := &builtinNullEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_compare_00000, 577)
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 581)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 578)
	arg1, isNull1, err := b.args[1].EvalJSON(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 582)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_compare_00000, 579)
	var res int64
	switch {
	case isNull0 && isNull1:
		trace_util_0.Count(_builtin_compare_00000, 583)
		res = 1
	case isNull0 != isNull1:
		trace_util_0.Count(_builtin_compare_00000, 584)
		break
	default:
		trace_util_0.Count(_builtin_compare_00000, 585)
		cmpRes := json.CompareBinary(arg0, arg1)
		if cmpRes == 0 {
			trace_util_0.Count(_builtin_compare_00000, 586)
			res = 1
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 580)
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 587)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 590)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 588)
	if val < 0 {
		trace_util_0.Count(_builtin_compare_00000, 591)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 592)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 589)
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 593)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 596)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 594)
	if val <= 0 {
		trace_util_0.Count(_builtin_compare_00000, 597)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 598)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 595)
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 599)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 602)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 600)
	if val > 0 {
		trace_util_0.Count(_builtin_compare_00000, 603)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 604)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 601)
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 605)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 608)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 606)
	if val >= 0 {
		trace_util_0.Count(_builtin_compare_00000, 609)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 610)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 607)
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 611)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 614)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 612)
	if val == 0 {
		trace_util_0.Count(_builtin_compare_00000, 615)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 616)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 613)
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 617)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_compare_00000, 620)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_compare_00000, 618)
	if val != 0 {
		trace_util_0.Count(_builtin_compare_00000, 621)
		val = 1
	} else {
		trace_util_0.Count(_builtin_compare_00000, 622)
		{
			val = 0
		}
	}
	trace_util_0.Count(_builtin_compare_00000, 619)
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	trace_util_0.Count(_builtin_compare_00000, 623)
	if lhsIsNull && rhsIsNull {
		trace_util_0.Count(_builtin_compare_00000, 626)
		return 0
	}
	trace_util_0.Count(_builtin_compare_00000, 624)
	if lhsIsNull {
		trace_util_0.Count(_builtin_compare_00000, 627)
		return -1
	}
	trace_util_0.Count(_builtin_compare_00000, 625)
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 628)
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 633)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 629)
	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 634)
		return 0, true, err
	}

	// compare null values.
	trace_util_0.Count(_builtin_compare_00000, 630)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 635)
		return compareNull(isNull0, isNull1), true, nil
	}

	trace_util_0.Count(_builtin_compare_00000, 631)
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().Flag), mysql.HasUnsignedFlag(rhsArg.GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 636)
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 637)
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			trace_util_0.Count(_builtin_compare_00000, 640)
			res = 1
		} else {
			trace_util_0.Count(_builtin_compare_00000, 641)
			{
				res = types.CompareInt64(arg0, arg1)
			}
		}
	case !isUnsigned0 && isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 638)
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			trace_util_0.Count(_builtin_compare_00000, 642)
			res = -1
		} else {
			trace_util_0.Count(_builtin_compare_00000, 643)
			{
				res = types.CompareInt64(arg0, arg1)
			}
		}
	case !isUnsigned0 && !isUnsigned1:
		trace_util_0.Count(_builtin_compare_00000, 639)
		res = types.CompareInt64(arg0, arg1)
	}
	trace_util_0.Count(_builtin_compare_00000, 632)
	return int64(res), false, nil
}

// CompareString compares two strings.
func CompareString(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 644)
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 648)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 645)
	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 649)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 646)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 650)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 647)
	return int64(types.CompareString(arg0, arg1)), false, nil
}

// CompareReal compares two float-point values.
func CompareReal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 651)
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 655)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 652)
	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 656)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 653)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 657)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 654)
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 658)
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 662)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 659)
	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 663)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 660)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 664)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 661)
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 665)
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 669)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 666)
	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 670)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 667)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 671)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 668)
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 672)
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 676)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 673)
	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 677)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 674)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 678)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 675)
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_compare_00000, 679)
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 683)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 680)
	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		trace_util_0.Count(_builtin_compare_00000, 684)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_compare_00000, 681)
	if isNull0 || isNull1 {
		trace_util_0.Count(_builtin_compare_00000, 685)
		return compareNull(isNull0, isNull1), true, nil
	}
	trace_util_0.Count(_builtin_compare_00000, 682)
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}

var _builtin_compare_00000 = "expression/builtin_compare.go"
