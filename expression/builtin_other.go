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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
	_ functionClass = &bitCountFunctionClass{}
	_ functionClass = &getParamFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinInIntSig{}
	_ builtinFunc = &builtinInStringSig{}
	_ builtinFunc = &builtinInDecimalSig{}
	_ builtinFunc = &builtinInRealSig{}
	_ builtinFunc = &builtinInTimeSig{}
	_ builtinFunc = &builtinInDurationSig{}
	_ builtinFunc = &builtinInJSONSig{}
	_ builtinFunc = &builtinRowSig{}
	_ builtinFunc = &builtinSetVarSig{}
	_ builtinFunc = &builtinGetVarSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinValuesIntSig{}
	_ builtinFunc = &builtinValuesRealSig{}
	_ builtinFunc = &builtinValuesDecimalSig{}
	_ builtinFunc = &builtinValuesStringSig{}
	_ builtinFunc = &builtinValuesTimeSig{}
	_ builtinFunc = &builtinValuesDurationSig{}
	_ builtinFunc = &builtinValuesJSONSig{}
	_ builtinFunc = &builtinBitCountSig{}
	_ builtinFunc = &builtinGetParamStringSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_other_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 4)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 1)
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		trace_util_0.Count(_builtin_other_00000, 5)
		argTps[i] = args[0].GetType().EvalType()
	}
	trace_util_0.Count(_builtin_other_00000, 2)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	bf.tp.Flen = 1
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		trace_util_0.Count(_builtin_other_00000, 6)
		sig = &builtinInIntSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InInt)
	case types.ETString:
		trace_util_0.Count(_builtin_other_00000, 7)
		sig = &builtinInStringSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InString)
	case types.ETReal:
		trace_util_0.Count(_builtin_other_00000, 8)
		sig = &builtinInRealSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InReal)
	case types.ETDecimal:
		trace_util_0.Count(_builtin_other_00000, 9)
		sig = &builtinInDecimalSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InDecimal)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_other_00000, 10)
		sig = &builtinInTimeSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InTime)
	case types.ETDuration:
		trace_util_0.Count(_builtin_other_00000, 11)
		sig = &builtinInDurationSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InDuration)
	case types.ETJson:
		trace_util_0.Count(_builtin_other_00000, 12)
		sig = &builtinInJSONSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InJson)
	}
	trace_util_0.Count(_builtin_other_00000, 3)
	return sig, nil
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseBuiltinFunc
}

func (b *builtinInIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 13)
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 14)
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 17)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 15)
	isUnsigned0 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 18)
		evaledArg, isNull, err := arg.EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 21)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 19)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 22)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 20)
		isUnsigned := mysql.HasUnsignedFlag(arg.GetType().Flag)
		if isUnsigned0 && isUnsigned {
			trace_util_0.Count(_builtin_other_00000, 23)
			if evaledArg == arg0 {
				trace_util_0.Count(_builtin_other_00000, 24)
				return 1, false, nil
			}
		} else {
			trace_util_0.Count(_builtin_other_00000, 25)
			if !isUnsigned0 && !isUnsigned {
				trace_util_0.Count(_builtin_other_00000, 26)
				if evaledArg == arg0 {
					trace_util_0.Count(_builtin_other_00000, 27)
					return 1, false, nil
				}
			} else {
				trace_util_0.Count(_builtin_other_00000, 28)
				if !isUnsigned0 && isUnsigned {
					trace_util_0.Count(_builtin_other_00000, 29)
					if arg0 >= 0 && evaledArg == arg0 {
						trace_util_0.Count(_builtin_other_00000, 30)
						return 1, false, nil
					}
				} else {
					trace_util_0.Count(_builtin_other_00000, 31)
					{
						if evaledArg >= 0 && evaledArg == arg0 {
							trace_util_0.Count(_builtin_other_00000, 32)
							return 1, false, nil
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_builtin_other_00000, 16)
	return 0, hasNull, nil
}

// builtinInStringSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseBuiltinFunc
}

func (b *builtinInStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 33)
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 34)
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 37)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 35)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 38)
		evaledArg, isNull, err := arg.EvalString(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 41)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 39)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 42)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 40)
		if arg0 == evaledArg {
			trace_util_0.Count(_builtin_other_00000, 43)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 36)
	return 0, hasNull, nil
}

// builtinInRealSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	baseBuiltinFunc
}

func (b *builtinInRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 44)
	newSig := &builtinInRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 45)
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 48)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 46)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 49)
		evaledArg, isNull, err := arg.EvalReal(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 52)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 50)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 53)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 51)
		if arg0 == evaledArg {
			trace_util_0.Count(_builtin_other_00000, 54)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 47)
	return 0, hasNull, nil
}

// builtinInDecimalSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinInDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 55)
	newSig := &builtinInDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInDecimalSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 56)
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 59)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 57)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 60)
		evaledArg, isNull, err := arg.EvalDecimal(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 63)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 61)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 64)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 62)
		if arg0.Compare(evaledArg) == 0 {
			trace_util_0.Count(_builtin_other_00000, 65)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 58)
	return 0, hasNull, nil
}

// builtinInTimeSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinInTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 66)
	newSig := &builtinInTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInTimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 67)
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 70)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 68)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 71)
		evaledArg, isNull, err := arg.EvalTime(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 74)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 72)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 75)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 73)
		if arg0.Compare(evaledArg) == 0 {
			trace_util_0.Count(_builtin_other_00000, 76)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 69)
	return 0, hasNull, nil
}

// builtinInDurationSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinInDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 77)
	newSig := &builtinInDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 78)
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 81)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 79)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 82)
		evaledArg, isNull, err := arg.EvalDuration(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 85)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 83)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 86)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 84)
		if arg0.Compare(evaledArg) == 0 {
			trace_util_0.Count(_builtin_other_00000, 87)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 80)
	return 0, hasNull, nil
}

// builtinInJSONSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinInJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 88)
	newSig := &builtinInJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInJSONSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 89)
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull0 || err != nil {
		trace_util_0.Count(_builtin_other_00000, 92)
		return 0, isNull0, err
	}
	trace_util_0.Count(_builtin_other_00000, 90)
	var hasNull bool
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_other_00000, 93)
		evaledArg, isNull, err := arg.EvalJSON(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_other_00000, 96)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_other_00000, 94)
		if isNull {
			trace_util_0.Count(_builtin_other_00000, 97)
			hasNull = true
			continue
		}
		trace_util_0.Count(_builtin_other_00000, 95)
		result := json.CompareBinary(evaledArg, arg0)
		if result == 0 {
			trace_util_0.Count(_builtin_other_00000, 98)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_other_00000, 91)
	return 0, hasNull, nil
}

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_other_00000, 99)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 102)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 100)
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		trace_util_0.Count(_builtin_other_00000, 103)
		argTps[i] = args[i].GetType().EvalType()
	}
	trace_util_0.Count(_builtin_other_00000, 101)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	sig = &builtinRowSig{bf}
	return sig, nil
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 104)
	newSig := &builtinRowSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString rowFunc should always be flattened in expression rewrite phrase.
func (b *builtinRowSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 105)
	panic("builtinRowSig.evalString() should never be called.")
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_other_00000, 106)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 108)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 107)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = args[1].GetType().Flen
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	sig = &builtinSetVarSig{bf}
	return sig, err
}

type builtinSetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinSetVarSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 109)
	newSig := &builtinSetVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetVarSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_other_00000, 110)
	var varName string
	sessionVars := b.ctx.GetSessionVars()
	varName, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_other_00000, 113)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_other_00000, 111)
	res, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_other_00000, 114)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_other_00000, 112)
	varName = strings.ToLower(varName)
	sessionVars.UsersLock.Lock()
	sessionVars.Users[varName] = res
	sessionVars.UsersLock.Unlock()
	return res, false, nil
}

type getVarFunctionClass struct {
	baseFunctionClass
}

func (c *getVarFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_other_00000, 115)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 117)
		return nil, err
	}
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	trace_util_0.Count(_builtin_other_00000, 116)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.MaxFieldVarCharLength
	sig = &builtinGetVarSig{bf}
	return sig, nil
}

type builtinGetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinGetVarSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 118)
	newSig := &builtinGetVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetVarSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 119)
	sessionVars := b.ctx.GetSessionVars()
	varName, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_other_00000, 122)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_other_00000, 120)
	varName = strings.ToLower(varName)
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
	if v, ok := sessionVars.Users[varName]; ok {
		trace_util_0.Count(_builtin_other_00000, 123)
		return v, false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 121)
	return "", true, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_other_00000, 124)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 127)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 125)
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	switch c.tp.EvalType() {
	case types.ETInt:
		trace_util_0.Count(_builtin_other_00000, 128)
		sig = &builtinValuesIntSig{bf, c.offset}
	case types.ETReal:
		trace_util_0.Count(_builtin_other_00000, 129)
		sig = &builtinValuesRealSig{bf, c.offset}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_other_00000, 130)
		sig = &builtinValuesDecimalSig{bf, c.offset}
	case types.ETString:
		trace_util_0.Count(_builtin_other_00000, 131)
		sig = &builtinValuesStringSig{bf, c.offset}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_other_00000, 132)
		sig = &builtinValuesTimeSig{bf, c.offset}
	case types.ETDuration:
		trace_util_0.Count(_builtin_other_00000, 133)
		sig = &builtinValuesDurationSig{bf, c.offset}
	case types.ETJson:
		trace_util_0.Count(_builtin_other_00000, 134)
		sig = &builtinValuesJSONSig{bf, c.offset}
	}
	trace_util_0.Count(_builtin_other_00000, 126)
	return sig, nil
}

type builtinValuesIntSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 135)
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(_ chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 136)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 140)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 137)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 141)
		return 0, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 138)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 142)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 144)
			return 0, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 143)
		return row.GetInt64(b.offset), false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 139)
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesRealSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 145)
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(_ chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 146)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 150)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 147)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 151)
		return 0, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 148)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 152)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 155)
			return 0, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 153)
		if b.getRetTp().Tp == mysql.TypeFloat {
			trace_util_0.Count(_builtin_other_00000, 156)
			return float64(row.GetFloat32(b.offset)), false, nil
		}
		trace_util_0.Count(_builtin_other_00000, 154)
		return row.GetFloat64(b.offset), false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 149)
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 157)
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(_ chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 158)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 162)
		return nil, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 159)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 163)
		return nil, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 160)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 164)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 166)
			return nil, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 165)
		return row.GetMyDecimal(b.offset), false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 161)
	return nil, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesStringSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 167)
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(_ chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 168)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 174)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 169)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 175)
		return "", true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 170)
	if b.offset >= row.Len() {
		trace_util_0.Count(_builtin_other_00000, 176)
		return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
	}

	trace_util_0.Count(_builtin_other_00000, 171)
	if row.IsNull(b.offset) {
		trace_util_0.Count(_builtin_other_00000, 177)
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	trace_util_0.Count(_builtin_other_00000, 172)
	if retType := b.getRetTp(); retType.Hybrid() {
		trace_util_0.Count(_builtin_other_00000, 178)
		val := row.GetDatum(b.offset, retType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	trace_util_0.Count(_builtin_other_00000, 173)
	return row.GetString(b.offset), false, nil
}

type builtinValuesTimeSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 179)
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(_ chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 180)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 184)
		return types.Time{}, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 181)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 185)
		return types.Time{}, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 182)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 186)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 188)
			return types.Time{}, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 187)
		return row.GetTime(b.offset), false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 183)
	return types.Time{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 189)
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(_ chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 190)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 194)
		return types.Duration{}, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 191)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 195)
		return types.Duration{}, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 192)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 196)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 198)
			return types.Duration{}, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 197)
		duration := row.GetDuration(b.offset, b.getRetTp().Decimal)
		return duration, false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 193)
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 199)
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(_ chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 200)
	if !b.ctx.GetSessionVars().StmtCtx.InInsertStmt {
		trace_util_0.Count(_builtin_other_00000, 204)
		return json.BinaryJSON{}, true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 201)
	row := b.ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		trace_util_0.Count(_builtin_other_00000, 205)
		return json.BinaryJSON{}, true, errors.New("Session current insert values is nil")
	}
	trace_util_0.Count(_builtin_other_00000, 202)
	if b.offset < row.Len() {
		trace_util_0.Count(_builtin_other_00000, 206)
		if row.IsNull(b.offset) {
			trace_util_0.Count(_builtin_other_00000, 208)
			return json.BinaryJSON{}, true, nil
		}
		trace_util_0.Count(_builtin_other_00000, 207)
		return row.GetJSON(b.offset), false, nil
	}
	trace_util_0.Count(_builtin_other_00000, 203)
	return json.BinaryJSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type bitCountFunctionClass struct {
	baseFunctionClass
}

func (c *bitCountFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_other_00000, 209)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 211)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 210)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flen = 2
	sig := &builtinBitCountSig{bf}
	return sig, nil
}

type builtinBitCountSig struct {
	baseBuiltinFunc
}

func (b *builtinBitCountSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 212)
	newSig := &builtinBitCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals BIT_COUNT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 213)
	n, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_other_00000, 216)
		if err != nil && types.ErrOverflow.Equal(err) {
			trace_util_0.Count(_builtin_other_00000, 218)
			return 64, false, nil
		}
		trace_util_0.Count(_builtin_other_00000, 217)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_other_00000, 214)
	var count int64
	for ; n != 0; n = (n - 1) & n {
		trace_util_0.Count(_builtin_other_00000, 219)
		count++
	}
	trace_util_0.Count(_builtin_other_00000, 215)
	return count, false, nil
}

// getParamFunctionClass for plan cache of prepared statements
type getParamFunctionClass struct {
	baseFunctionClass
}

// getFunction gets function
// TODO: more typed functions will be added when typed parameters are supported.
func (c *getParamFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_other_00000, 220)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_other_00000, 222)
		return nil, err
	}
	trace_util_0.Count(_builtin_other_00000, 221)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = mysql.MaxFieldVarCharLength
	sig := &builtinGetParamStringSig{bf}
	return sig, nil
}

type builtinGetParamStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGetParamStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_other_00000, 223)
	newSig := &builtinGetParamStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetParamStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_other_00000, 224)
	sessionVars := b.ctx.GetSessionVars()
	idx, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_other_00000, 227)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_other_00000, 225)
	v := sessionVars.PreparedParams[idx]

	str, err := v.ToString()
	if err != nil {
		trace_util_0.Count(_builtin_other_00000, 228)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_other_00000, 226)
	return str, false, nil
}

var _builtin_other_00000 = "expression/builtin_other.go"
