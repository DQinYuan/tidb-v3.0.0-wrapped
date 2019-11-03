// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ functionClass = &sleepFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &anyValueFunctionClass{}
	_ functionClass = &defaultFunctionClass{}
	_ functionClass = &inetAtonFunctionClass{}
	_ functionClass = &inetNtoaFunctionClass{}
	_ functionClass = &inet6AtonFunctionClass{}
	_ functionClass = &inet6NtoaFunctionClass{}
	_ functionClass = &isFreeLockFunctionClass{}
	_ functionClass = &isIPv4FunctionClass{}
	_ functionClass = &isIPv4CompatFunctionClass{}
	_ functionClass = &isIPv4MappedFunctionClass{}
	_ functionClass = &isIPv6FunctionClass{}
	_ functionClass = &isUsedLockFunctionClass{}
	_ functionClass = &masterPosWaitFunctionClass{}
	_ functionClass = &nameConstFunctionClass{}
	_ functionClass = &releaseAllLocksFunctionClass{}
	_ functionClass = &uuidFunctionClass{}
	_ functionClass = &uuidShortFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinDecimalAnyValueSig{}
	_ builtinFunc = &builtinDurationAnyValueSig{}
	_ builtinFunc = &builtinIntAnyValueSig{}
	_ builtinFunc = &builtinJSONAnyValueSig{}
	_ builtinFunc = &builtinRealAnyValueSig{}
	_ builtinFunc = &builtinStringAnyValueSig{}
	_ builtinFunc = &builtinTimeAnyValueSig{}
	_ builtinFunc = &builtinInetAtonSig{}
	_ builtinFunc = &builtinInetNtoaSig{}
	_ builtinFunc = &builtinInet6AtonSig{}
	_ builtinFunc = &builtinInet6NtoaSig{}
	_ builtinFunc = &builtinIsIPv4Sig{}
	_ builtinFunc = &builtinIsIPv4CompatSig{}
	_ builtinFunc = &builtinIsIPv4MappedSig{}
	_ builtinFunc = &builtinIsIPv6Sig{}
	_ builtinFunc = &builtinUUIDSig{}

	_ builtinFunc = &builtinNameConstIntSig{}
	_ builtinFunc = &builtinNameConstRealSig{}
	_ builtinFunc = &builtinNameConstDecimalSig{}
	_ builtinFunc = &builtinNameConstTimeSig{}
	_ builtinFunc = &builtinNameConstDurationSig{}
	_ builtinFunc = &builtinNameConstStringSig{}
	_ builtinFunc = &builtinNameConstJSONSig{}
)

type sleepFunctionClass struct {
	baseFunctionClass
}

func (c *sleepFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 1)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETReal)
	bf.tp.Flen = 21
	sig := &builtinSleepSig{bf}
	return sig, nil
}

type builtinSleepSig struct {
	baseBuiltinFunc
}

func (b *builtinSleepSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 3)
	newSig := &builtinSleepSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinSleepSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 4)
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 10)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 5)
	sessVars := b.ctx.GetSessionVars()
	if isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 11)
		if sessVars.StrictSQLMode {
			trace_util_0.Count(_builtin_miscellaneous_00000, 13)
			return 0, true, errIncorrectArgs.GenWithStackByArgs("sleep")
		}
		trace_util_0.Count(_builtin_miscellaneous_00000, 12)
		return 0, true, nil
	}
	// processing argument is negative
	trace_util_0.Count(_builtin_miscellaneous_00000, 6)
	if val < 0 {
		trace_util_0.Count(_builtin_miscellaneous_00000, 14)
		if sessVars.StrictSQLMode {
			trace_util_0.Count(_builtin_miscellaneous_00000, 16)
			return 0, false, errIncorrectArgs.GenWithStackByArgs("sleep")
		}
		trace_util_0.Count(_builtin_miscellaneous_00000, 15)
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 7)
	if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
		trace_util_0.Count(_builtin_miscellaneous_00000, 17)
		return 0, false, errIncorrectArgs.GenWithStackByArgs("sleep")
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 8)
	dur := time.Duration(val * float64(time.Second.Nanoseconds()))
	select {
	case <-time.After(dur):
		trace_util_0.Count(_builtin_miscellaneous_00000, 18)
		// TODO: Handle Ctrl-C is pressed in `mysql` client.
		// return 1 when SLEEP() is KILLed
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 9)
	return 0, false, nil
}

type lockFunctionClass struct {
	baseFunctionClass
}

func (c *lockFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 19)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 21)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 20)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETInt)
	sig := &builtinLockSig{bf}
	bf.tp.Flen = 1
	return sig, nil
}

type builtinLockSig struct {
	baseBuiltinFunc
}

func (b *builtinLockSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 22)
	newSig := &builtinLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
// The lock function will do nothing.
// Warning: get_lock() function is parsed but ignored.
func (b *builtinLockSig) evalInt(_ chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 23)
	return 1, false, nil
}

type releaseLockFunctionClass struct {
	baseFunctionClass
}

func (c *releaseLockFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 24)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 26)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 25)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	sig := &builtinReleaseLockSig{bf}
	bf.tp.Flen = 1
	return sig, nil
}

type builtinReleaseLockSig struct {
	baseBuiltinFunc
}

func (b *builtinReleaseLockSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 27)
	newSig := &builtinReleaseLockSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinReleaseLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_release-lock
// The release lock function will do nothing.
// Warning: release_lock() function is parsed but ignored.
func (b *builtinReleaseLockSig) evalInt(_ chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 28)
	return 1, false, nil
}

type anyValueFunctionClass struct {
	baseFunctionClass
}

func (c *anyValueFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 29)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 32)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 30)
	argTp := args[0].GetType().EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, argTp)
	args[0].GetType().Flag |= bf.tp.Flag
	*bf.tp = *args[0].GetType()
	var sig builtinFunc
	switch argTp {
	case types.ETDecimal:
		trace_util_0.Count(_builtin_miscellaneous_00000, 33)
		sig = &builtinDecimalAnyValueSig{bf}
	case types.ETDuration:
		trace_util_0.Count(_builtin_miscellaneous_00000, 34)
		sig = &builtinDurationAnyValueSig{bf}
	case types.ETInt:
		trace_util_0.Count(_builtin_miscellaneous_00000, 35)
		bf.tp.Decimal = 0
		sig = &builtinIntAnyValueSig{bf}
	case types.ETJson:
		trace_util_0.Count(_builtin_miscellaneous_00000, 36)
		sig = &builtinJSONAnyValueSig{bf}
	case types.ETReal:
		trace_util_0.Count(_builtin_miscellaneous_00000, 37)
		sig = &builtinRealAnyValueSig{bf}
	case types.ETString:
		trace_util_0.Count(_builtin_miscellaneous_00000, 38)
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinStringAnyValueSig{bf}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_miscellaneous_00000, 39)
		bf.tp.Charset, bf.tp.Collate, bf.tp.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		sig = &builtinTimeAnyValueSig{bf}
	default:
		trace_util_0.Count(_builtin_miscellaneous_00000, 40)
		return nil, errIncorrectArgs.GenWithStackByArgs("ANY_VALUE")
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 31)
	return sig, nil
}

type builtinDecimalAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 41)
	newSig := &builtinDecimalAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinDecimalAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDecimalAnyValueSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 42)
	return b.args[0].EvalDecimal(b.ctx, row)
}

type builtinDurationAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 43)
	newSig := &builtinDurationAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDurationAnyValueSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 44)
	return b.args[0].EvalDuration(b.ctx, row)
}

type builtinIntAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinIntAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 45)
	newSig := &builtinIntAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinIntAnyValueSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 46)
	return b.args[0].EvalInt(b.ctx, row)
}

type builtinJSONAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 47)
	newSig := &builtinJSONAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinJSONAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinJSONAnyValueSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 48)
	return b.args[0].EvalJSON(b.ctx, row)
}

type builtinRealAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinRealAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 49)
	newSig := &builtinRealAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinRealAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinRealAnyValueSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 50)
	return b.args[0].EvalReal(b.ctx, row)
}

type builtinStringAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinStringAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 51)
	newSig := &builtinStringAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinStringAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinStringAnyValueSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 52)
	return b.args[0].EvalString(b.ctx, row)
}

type builtinTimeAnyValueSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeAnyValueSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 53)
	newSig := &builtinTimeAnyValueSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimeAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinTimeAnyValueSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 54)
	return b.args[0].EvalTime(b.ctx, row)
}

type defaultFunctionClass struct {
	baseFunctionClass
}

func (c *defaultFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 55)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DEFAULT")
}

type inetAtonFunctionClass struct {
	baseFunctionClass
}

func (c *inetAtonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 56)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 58)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 57)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 21
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinInetAtonSig{bf}
	return sig, nil
}

type builtinInetAtonSig struct {
	baseBuiltinFunc
}

func (b *builtinInetAtonSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 59)
	newSig := &builtinInetAtonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinInetAtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-aton
func (b *builtinInetAtonSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 60)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 65)
		return 0, true, err
	}
	// ip address should not end with '.'.
	trace_util_0.Count(_builtin_miscellaneous_00000, 61)
	if len(val) == 0 || val[len(val)-1] == '.' {
		trace_util_0.Count(_builtin_miscellaneous_00000, 66)
		return 0, true, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 62)
	var (
		byteResult, result uint64
		dotCount           int
	)
	for _, c := range val {
		trace_util_0.Count(_builtin_miscellaneous_00000, 67)
		if c >= '0' && c <= '9' {
			trace_util_0.Count(_builtin_miscellaneous_00000, 68)
			digit := uint64(c - '0')
			byteResult = byteResult*10 + digit
			if byteResult > 255 {
				trace_util_0.Count(_builtin_miscellaneous_00000, 69)
				return 0, true, nil
			}
		} else {
			trace_util_0.Count(_builtin_miscellaneous_00000, 70)
			if c == '.' {
				trace_util_0.Count(_builtin_miscellaneous_00000, 71)
				dotCount++
				if dotCount > 3 {
					trace_util_0.Count(_builtin_miscellaneous_00000, 73)
					return 0, true, nil
				}
				trace_util_0.Count(_builtin_miscellaneous_00000, 72)
				result = (result << 8) + byteResult
				byteResult = 0
			} else {
				trace_util_0.Count(_builtin_miscellaneous_00000, 74)
				{
					return 0, true, nil
				}
			}
		}
	}
	// 127 		-> 0.0.0.127
	// 127.255 	-> 127.0.0.255
	// 127.256	-> NULL
	// 127.2.1	-> 127.2.0.1
	trace_util_0.Count(_builtin_miscellaneous_00000, 63)
	switch dotCount {
	case 1:
		trace_util_0.Count(_builtin_miscellaneous_00000, 75)
		result <<= 8
		fallthrough
	case 2:
		trace_util_0.Count(_builtin_miscellaneous_00000, 76)
		result <<= 8
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 64)
	return int64((result << 8) + byteResult), false, nil
}

type inetNtoaFunctionClass struct {
	baseFunctionClass
}

func (c *inetNtoaFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 77)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 79)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 78)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = 93
	bf.tp.Decimal = 0
	sig := &builtinInetNtoaSig{bf}
	return sig, nil
}

type builtinInetNtoaSig struct {
	baseBuiltinFunc
}

func (b *builtinInetNtoaSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 80)
	newSig := &builtinInetNtoaSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInetNtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-ntoa
func (b *builtinInetNtoaSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 81)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 85)
		return "", true, err
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 82)
	if val < 0 || uint64(val) > math.MaxUint32 {
		trace_util_0.Count(_builtin_miscellaneous_00000, 86)
		//not an IPv4 address.
		return "", true, nil
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 83)
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, uint32(val))
	ipv4 := ip.To4()
	if ipv4 == nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 87)
		//Not a vaild ipv4 address.
		return "", true, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 84)
	return ipv4.String(), false, nil
}

type inet6AtonFunctionClass struct {
	baseFunctionClass
}

func (c *inet6AtonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 88)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 90)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 89)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = 16
	types.SetBinChsClnFlag(bf.tp)
	bf.tp.Decimal = 0
	sig := &builtinInet6AtonSig{bf}
	return sig, nil
}

type builtinInet6AtonSig struct {
	baseBuiltinFunc
}

func (b *builtinInet6AtonSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 91)
	newSig := &builtinInet6AtonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6AtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-aton
func (b *builtinInet6AtonSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 92)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 99)
		return "", true, err
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 93)
	if len(val) == 0 {
		trace_util_0.Count(_builtin_miscellaneous_00000, 100)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 94)
	ip := net.ParseIP(val)
	if ip == nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 101)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 95)
	var isMappedIpv6 bool
	if ip.To4() != nil && strings.Contains(val, ":") {
		trace_util_0.Count(_builtin_miscellaneous_00000, 102)
		//mapped ipv6 address.
		isMappedIpv6 = true
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 96)
	var result []byte
	if isMappedIpv6 || ip.To4() == nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 103)
		result = make([]byte, net.IPv6len)
	} else {
		trace_util_0.Count(_builtin_miscellaneous_00000, 104)
		{
			result = make([]byte, net.IPv4len)
		}
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 97)
	if isMappedIpv6 {
		trace_util_0.Count(_builtin_miscellaneous_00000, 105)
		copy(result[12:], ip.To4())
		result[11] = 0xff
		result[10] = 0xff
	} else {
		trace_util_0.Count(_builtin_miscellaneous_00000, 106)
		if ip.To4() == nil {
			trace_util_0.Count(_builtin_miscellaneous_00000, 107)
			copy(result, ip.To16())
		} else {
			trace_util_0.Count(_builtin_miscellaneous_00000, 108)
			{
				copy(result, ip.To4())
			}
		}
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 98)
	return string(result[:]), false, nil
}

type inet6NtoaFunctionClass struct {
	baseFunctionClass
}

func (c *inet6NtoaFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 109)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 111)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 110)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = 117
	bf.tp.Decimal = 0
	sig := &builtinInet6NtoaSig{bf}
	return sig, nil
}

type builtinInet6NtoaSig struct {
	baseBuiltinFunc
}

func (b *builtinInet6NtoaSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 112)
	newSig := &builtinInet6NtoaSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6NtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-ntoa
func (b *builtinInet6NtoaSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 113)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 117)
		return "", true, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 114)
	ip := net.IP([]byte(val)).String()
	if len(val) == net.IPv6len && !strings.Contains(ip, ":") {
		trace_util_0.Count(_builtin_miscellaneous_00000, 118)
		ip = fmt.Sprintf("::ffff:%s", ip)
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 115)
	if net.ParseIP(ip) == nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 119)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 116)
	return ip, false, nil
}

type isFreeLockFunctionClass struct {
	baseFunctionClass
}

func (c *isFreeLockFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 120)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "IS_FREE_LOCK")
}

type isIPv4FunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 121)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 123)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 122)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4Sig{bf}
	return sig, nil
}

type builtinIsIPv4Sig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 124)
	newSig := &builtinIsIPv4Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv4Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4
func (b *builtinIsIPv4Sig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 125)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 128)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 126)
	if isIPv4(val) {
		trace_util_0.Count(_builtin_miscellaneous_00000, 129)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 127)
	return 0, false, nil
}

// isIPv4 checks IPv4 address which satisfying the format A.B.C.D(0<=A/B/C/D<=255).
// Mapped IPv6 address like '::ffff:1.2.3.4' would return false.
func isIPv4(ip string) bool {
	trace_util_0.Count(_builtin_miscellaneous_00000, 130)
	// acc: keep the decimal value of each segment under check, which should between 0 and 255 for valid IPv4 address.
	// pd: sentinel for '.'
	dots, acc, pd := 0, 0, true
	for _, c := range ip {
		trace_util_0.Count(_builtin_miscellaneous_00000, 133)
		switch {
		case '0' <= c && c <= '9':
			trace_util_0.Count(_builtin_miscellaneous_00000, 134)
			acc = acc*10 + int(c-'0')
			pd = false
		case c == '.':
			trace_util_0.Count(_builtin_miscellaneous_00000, 135)
			dots++
			if dots > 3 || acc > 255 || pd {
				trace_util_0.Count(_builtin_miscellaneous_00000, 138)
				return false
			}
			trace_util_0.Count(_builtin_miscellaneous_00000, 136)
			acc, pd = 0, true
		default:
			trace_util_0.Count(_builtin_miscellaneous_00000, 137)
			return false
		}
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 131)
	if dots != 3 || acc > 255 || pd {
		trace_util_0.Count(_builtin_miscellaneous_00000, 139)
		return false
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 132)
	return true
}

type isIPv4CompatFunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4CompatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 140)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 142)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 141)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4CompatSig{bf}
	return sig, nil
}

type builtinIsIPv4CompatSig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4CompatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 143)
	newSig := &builtinIsIPv4CompatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Compat
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
func (b *builtinIsIPv4CompatSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 144)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 148)
		return 0, err != nil, err
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 145)
	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		trace_util_0.Count(_builtin_miscellaneous_00000, 149)
		//Not an IPv6 address, return false
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 146)
	prefixCompat := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.HasPrefix(ipAddress, prefixCompat) {
		trace_util_0.Count(_builtin_miscellaneous_00000, 150)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 147)
	return 1, false, nil
}

type isIPv4MappedFunctionClass struct {
	baseFunctionClass
}

func (c *isIPv4MappedFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 151)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 153)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 152)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv4MappedSig{bf}
	return sig, nil
}

type builtinIsIPv4MappedSig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv4MappedSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 154)
	newSig := &builtinIsIPv4MappedSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Mapped
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
func (b *builtinIsIPv4MappedSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 155)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 159)
		return 0, err != nil, err
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 156)
	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		trace_util_0.Count(_builtin_miscellaneous_00000, 160)
		//Not an IPv6 address, return false
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_miscellaneous_00000, 157)
	prefixMapped := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}
	if !bytes.HasPrefix(ipAddress, prefixMapped) {
		trace_util_0.Count(_builtin_miscellaneous_00000, 161)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 158)
	return 1, false, nil
}

type isIPv6FunctionClass struct {
	baseFunctionClass
}

func (c *isIPv6FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 162)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 164)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 163)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 1
	sig := &builtinIsIPv6Sig{bf}
	return sig, nil
}

type builtinIsIPv6Sig struct {
	baseBuiltinFunc
}

func (b *builtinIsIPv6Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 165)
	newSig := &builtinIsIPv6Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv6Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv6
func (b *builtinIsIPv6Sig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 166)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_miscellaneous_00000, 169)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 167)
	ip := net.ParseIP(val)
	if ip != nil && !isIPv4(val) {
		trace_util_0.Count(_builtin_miscellaneous_00000, 170)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 168)
	return 0, false, nil
}

type isUsedLockFunctionClass struct {
	baseFunctionClass
}

func (c *isUsedLockFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 171)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "IS_USED_LOCK")
}

type masterPosWaitFunctionClass struct {
	baseFunctionClass
}

func (c *masterPosWaitFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 172)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "MASTER_POS_WAIT")
}

type nameConstFunctionClass struct {
	baseFunctionClass
}

func (c *nameConstFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 173)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 176)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 174)
	argTp := args[1].GetType().EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, types.ETString, argTp)
	*bf.tp = *args[1].GetType()
	var sig builtinFunc
	switch argTp {
	case types.ETDecimal:
		trace_util_0.Count(_builtin_miscellaneous_00000, 177)
		sig = &builtinNameConstDecimalSig{bf}
	case types.ETDuration:
		trace_util_0.Count(_builtin_miscellaneous_00000, 178)
		sig = &builtinNameConstDurationSig{bf}
	case types.ETInt:
		trace_util_0.Count(_builtin_miscellaneous_00000, 179)
		bf.tp.Decimal = 0
		sig = &builtinNameConstIntSig{bf}
	case types.ETJson:
		trace_util_0.Count(_builtin_miscellaneous_00000, 180)
		sig = &builtinNameConstJSONSig{bf}
	case types.ETReal:
		trace_util_0.Count(_builtin_miscellaneous_00000, 181)
		sig = &builtinNameConstRealSig{bf}
	case types.ETString:
		trace_util_0.Count(_builtin_miscellaneous_00000, 182)
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinNameConstStringSig{bf}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_miscellaneous_00000, 183)
		bf.tp.Charset, bf.tp.Collate, bf.tp.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		sig = &builtinNameConstTimeSig{bf}
	default:
		trace_util_0.Count(_builtin_miscellaneous_00000, 184)
		return nil, errIncorrectArgs.GenWithStackByArgs("NAME_CONST")
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 175)
	return sig, nil
}

type builtinNameConstDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 185)
	newSig := &builtinNameConstDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 186)
	return b.args[1].EvalDecimal(b.ctx, row)
}

type builtinNameConstIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 187)
	newSig := &builtinNameConstIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 188)
	return b.args[1].EvalInt(b.ctx, row)
}

type builtinNameConstRealSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 189)
	newSig := &builtinNameConstRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 190)
	return b.args[1].EvalReal(b.ctx, row)
}

type builtinNameConstStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 191)
	newSig := &builtinNameConstStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 192)
	return b.args[1].EvalString(b.ctx, row)
}

type builtinNameConstJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstJSONSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 193)
	newSig := &builtinNameConstJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 194)
	return b.args[1].EvalJSON(b.ctx, row)
}

type builtinNameConstDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 195)
	newSig := &builtinNameConstDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 196)
	return b.args[1].EvalDuration(b.ctx, row)
}

type builtinNameConstTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNameConstTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 197)
	newSig := &builtinNameConstTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 198)
	return b.args[1].EvalTime(b.ctx, row)
}

type releaseAllLocksFunctionClass struct {
	baseFunctionClass
}

func (c *releaseAllLocksFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 199)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "RELEASE_ALL_LOCKS")
}

type uuidFunctionClass struct {
	baseFunctionClass
}

func (c *uuidFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 200)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 202)
		return nil, err
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 201)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 36
	sig := &builtinUUIDSig{bf}
	return sig, nil
}

type builtinUUIDSig struct {
	baseBuiltinFunc
}

func (b *builtinUUIDSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_miscellaneous_00000, 203)
	newSig := &builtinUUIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_uuid
func (b *builtinUUIDSig) evalString(_ chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 204)
	var id uuid.UUID
	id, err = uuid.NewUUID()
	if err != nil {
		trace_util_0.Count(_builtin_miscellaneous_00000, 206)
		return
	}
	trace_util_0.Count(_builtin_miscellaneous_00000, 205)
	d = id.String()
	return
}

type uuidShortFunctionClass struct {
	baseFunctionClass
}

func (c *uuidShortFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_miscellaneous_00000, 207)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "UUID_SHORT")
}

var _builtin_miscellaneous_00000 = "expression/builtin_miscellaneous.go"
