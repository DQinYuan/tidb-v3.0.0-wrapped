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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/text/transform"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &asciiFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &concatWSFunctionClass{}
	_ functionClass = &leftFunctionClass{}
	_ functionClass = &repeatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &reverseFunctionClass{}
	_ functionClass = &spaceFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &convertFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &substringIndexFunctionClass{}
	_ functionClass = &locateFunctionClass{}
	_ functionClass = &hexFunctionClass{}
	_ functionClass = &unhexFunctionClass{}
	_ functionClass = &trimFunctionClass{}
	_ functionClass = &lTrimFunctionClass{}
	_ functionClass = &rTrimFunctionClass{}
	_ functionClass = &lpadFunctionClass{}
	_ functionClass = &rpadFunctionClass{}
	_ functionClass = &bitLengthFunctionClass{}
	_ functionClass = &charFunctionClass{}
	_ functionClass = &charLengthFunctionClass{}
	_ functionClass = &findInSetFunctionClass{}
	_ functionClass = &fieldFunctionClass{}
	_ functionClass = &makeSetFunctionClass{}
	_ functionClass = &octFunctionClass{}
	_ functionClass = &ordFunctionClass{}
	_ functionClass = &quoteFunctionClass{}
	_ functionClass = &binFunctionClass{}
	_ functionClass = &eltFunctionClass{}
	_ functionClass = &exportSetFunctionClass{}
	_ functionClass = &formatFunctionClass{}
	_ functionClass = &fromBase64FunctionClass{}
	_ functionClass = &toBase64FunctionClass{}
	_ functionClass = &insertFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftBinarySig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinRightBinarySig{}
	_ builtinFunc = &builtinRightSig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinReverseBinarySig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstringBinary2ArgsSig{}
	_ builtinFunc = &builtinSubstringBinary3ArgsSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocate2ArgsSig{}
	_ builtinFunc = &builtinLocate3ArgsSig{}
	_ builtinFunc = &builtinLocateBinary2ArgsSig{}
	_ builtinFunc = &builtinLocateBinary3ArgsSig{}
	_ builtinFunc = &builtinHexStrArgSig{}
	_ builtinFunc = &builtinHexIntArgSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrim1ArgSig{}
	_ builtinFunc = &builtinTrim2ArgsSig{}
	_ builtinFunc = &builtinTrim3ArgsSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinLpadSig{}
	_ builtinFunc = &builtinLpadBinarySig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinRpadBinarySig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthSig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctIntSig{}
	_ builtinFunc = &builtinOctStringSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSet3ArgSig{}
	_ builtinFunc = &builtinExportSet4ArgSig{}
	_ builtinFunc = &builtinExportSet5ArgSig{}
	_ builtinFunc = &builtinFormatWithLocaleSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertBinarySig{}
	_ builtinFunc = &builtinInsertSig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinInstrBinarySig{}
	_ builtinFunc = &builtinFieldRealSig{}
	_ builtinFunc = &builtinFieldIntSig{}
	_ builtinFunc = &builtinFieldStringSig{}
)

func reverseBytes(origin []byte) []byte {
	trace_util_0.Count(_builtin_string_00000, 0)
	for i, length := 0, len(origin); i < length/2; i++ {
		trace_util_0.Count(_builtin_string_00000, 2)
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	trace_util_0.Count(_builtin_string_00000, 1)
	return origin
}

func reverseRunes(origin []rune) []rune {
	trace_util_0.Count(_builtin_string_00000, 3)
	for i, length := 0, len(origin); i < length/2; i++ {
		trace_util_0.Count(_builtin_string_00000, 5)
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	trace_util_0.Count(_builtin_string_00000, 4)
	return origin
}

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	trace_util_0.Count(_builtin_string_00000, 6)
	if types.IsBinaryStr(argTp) {
		trace_util_0.Count(_builtin_string_00000, 7)
		types.SetBinChsClnFlag(resTp)
	} else {
		trace_util_0.Count(_builtin_string_00000, 8)
		if mysql.HasBinaryFlag(argTp.Flag) || !types.IsNonBinaryStr(argTp) {
			trace_util_0.Count(_builtin_string_00000, 9)
			resTp.Flag |= mysql.BinaryFlag
		}
	}
}

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 10)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 12)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 11)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinLengthSig{bf}
	return sig, nil
}

type builtinLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinLengthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 13)
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 14)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 16)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 15)
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 17)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 19)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 18)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 3
	sig := &builtinASCIISig{bf}
	return sig, nil
}

type builtinASCIISig struct {
	baseBuiltinFunc
}

func (b *builtinASCIISig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 20)
	newSig := &builtinASCIISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 21)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 24)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 22)
	if len(val) == 0 {
		trace_util_0.Count(_builtin_string_00000, 25)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 23)
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 26)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 31)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 27)
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		trace_util_0.Count(_builtin_string_00000, 32)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_string_00000, 28)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for i := range args {
		trace_util_0.Count(_builtin_string_00000, 33)
		argType := args[i].GetType()
		SetBinFlagOrBinStr(argType, bf.tp)

		if argType.Flen < 0 {
			trace_util_0.Count(_builtin_string_00000, 35)
			bf.tp.Flen = mysql.MaxBlobWidth
			logutil.Logger(context.Background()).Warn("unexpected `Flen` value(-1) in CONCAT's args", zap.Int("arg's index", i))
		}
		trace_util_0.Count(_builtin_string_00000, 34)
		bf.tp.Flen += argType.Flen
	}
	trace_util_0.Count(_builtin_string_00000, 29)
	if bf.tp.Flen >= mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 36)
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_string_00000, 30)
	sig := &builtinConcatSig{bf}
	return sig, nil
}

type builtinConcatSig struct {
	baseBuiltinFunc
}

func (b *builtinConcatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 37)
	newSig := &builtinConcatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinConcatSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 38)
	var s []byte
	for _, a := range b.getArgs() {
		trace_util_0.Count(_builtin_string_00000, 40)
		d, isNull, err = a.EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_string_00000, 42)
			return d, isNull, err
		}
		trace_util_0.Count(_builtin_string_00000, 41)
		s = append(s, []byte(d)...)
	}
	trace_util_0.Count(_builtin_string_00000, 39)
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 43)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 48)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 44)
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		trace_util_0.Count(_builtin_string_00000, 49)
		argTps = append(argTps, types.ETString)
	}

	trace_util_0.Count(_builtin_string_00000, 45)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)

	for i := range args {
		trace_util_0.Count(_builtin_string_00000, 50)
		argType := args[i].GetType()
		SetBinFlagOrBinStr(argType, bf.tp)

		// skip separator param
		if i != 0 {
			trace_util_0.Count(_builtin_string_00000, 51)
			if argType.Flen < 0 {
				trace_util_0.Count(_builtin_string_00000, 53)
				bf.tp.Flen = mysql.MaxBlobWidth
				logutil.Logger(context.Background()).Warn("unexpected `Flen` value(-1) in CONCAT_WS's args", zap.Int("arg's index", i))
			}
			trace_util_0.Count(_builtin_string_00000, 52)
			bf.tp.Flen += argType.Flen
		}
	}

	// add separator
	trace_util_0.Count(_builtin_string_00000, 46)
	argsLen := len(args) - 1
	bf.tp.Flen += argsLen - 1

	if bf.tp.Flen >= mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 54)
		bf.tp.Flen = mysql.MaxBlobWidth
	}

	trace_util_0.Count(_builtin_string_00000, 47)
	sig := &builtinConcatWSSig{bf}
	return sig, nil
}

type builtinConcatWSSig struct {
	baseBuiltinFunc
}

func (b *builtinConcatWSSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 55)
	newSig := &builtinConcatWSSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinConcatWSSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 56)
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	for i, arg := range args {
		trace_util_0.Count(_builtin_string_00000, 58)
		val, isNull, err := arg.EvalString(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 62)
			return val, isNull, err
		}

		trace_util_0.Count(_builtin_string_00000, 59)
		if isNull {
			trace_util_0.Count(_builtin_string_00000, 63)
			// If the separator is NULL, the result is NULL.
			if i == 0 {
				trace_util_0.Count(_builtin_string_00000, 65)
				return val, isNull, nil
			}
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			trace_util_0.Count(_builtin_string_00000, 64)
			continue
		}

		trace_util_0.Count(_builtin_string_00000, 60)
		if i == 0 {
			trace_util_0.Count(_builtin_string_00000, 66)
			sep = val
			continue
		}
		trace_util_0.Count(_builtin_string_00000, 61)
		strs = append(strs, val)
	}

	// TODO: check whether the length of result is larger than Flen
	trace_util_0.Count(_builtin_string_00000, 57)
	return strings.Join(strs, sep), false, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 67)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 70)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 68)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		trace_util_0.Count(_builtin_string_00000, 71)
		sig := &builtinLeftBinarySig{bf}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 69)
	sig := &builtinLeftSig{bf}
	return sig, nil
}

type builtinLeftBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinLeftBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 72)
	newSig := &builtinLeftBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 73)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 77)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 74)
	left, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 78)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 75)
	leftLength := int(left)
	if strLength := len(str); leftLength > strLength {
		trace_util_0.Count(_builtin_string_00000, 79)
		leftLength = strLength
	} else {
		trace_util_0.Count(_builtin_string_00000, 80)
		if leftLength < 0 {
			trace_util_0.Count(_builtin_string_00000, 81)
			leftLength = 0
		}
	}
	trace_util_0.Count(_builtin_string_00000, 76)
	return str[:leftLength], false, nil
}

type builtinLeftSig struct {
	baseBuiltinFunc
}

func (b *builtinLeftSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 82)
	newSig := &builtinLeftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 83)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 87)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 84)
	left, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 88)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 85)
	runes, leftLength := []rune(str), int(left)
	if runeLength := len(runes); leftLength > runeLength {
		trace_util_0.Count(_builtin_string_00000, 89)
		leftLength = runeLength
	} else {
		trace_util_0.Count(_builtin_string_00000, 90)
		if leftLength < 0 {
			trace_util_0.Count(_builtin_string_00000, 91)
			leftLength = 0
		}
	}
	trace_util_0.Count(_builtin_string_00000, 86)
	return string(runes[:leftLength]), false, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 92)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 95)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 93)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		trace_util_0.Count(_builtin_string_00000, 96)
		sig := &builtinRightBinarySig{bf}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 94)
	sig := &builtinRightSig{bf}
	return sig, nil
}

type builtinRightBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinRightBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 97)
	newSig := &builtinRightBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 98)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 102)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 99)
	right, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 103)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 100)
	strLength, rightLength := len(str), int(right)
	if rightLength > strLength {
		trace_util_0.Count(_builtin_string_00000, 104)
		rightLength = strLength
	} else {
		trace_util_0.Count(_builtin_string_00000, 105)
		if rightLength < 0 {
			trace_util_0.Count(_builtin_string_00000, 106)
			rightLength = 0
		}
	}
	trace_util_0.Count(_builtin_string_00000, 101)
	return str[strLength-rightLength:], false, nil
}

type builtinRightSig struct {
	baseBuiltinFunc
}

func (b *builtinRightSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 107)
	newSig := &builtinRightSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 108)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 112)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 109)
	right, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 113)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 110)
	runes := []rune(str)
	strLength, rightLength := len(runes), int(right)
	if rightLength > strLength {
		trace_util_0.Count(_builtin_string_00000, 114)
		rightLength = strLength
	} else {
		trace_util_0.Count(_builtin_string_00000, 115)
		if rightLength < 0 {
			trace_util_0.Count(_builtin_string_00000, 116)
			rightLength = 0
		}
	}
	trace_util_0.Count(_builtin_string_00000, 111)
	return string(runes[strLength-rightLength:]), false, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 117)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 120)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 118)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	bf.tp.Flen = mysql.MaxBlobWidth
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 121)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 119)
	sig := &builtinRepeatSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinRepeatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRepeatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 122)
	newSig := &builtinRepeatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 123)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 130)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 124)
	byteLength := len(str)

	num, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 131)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 125)
	if num < 1 {
		trace_util_0.Count(_builtin_string_00000, 132)
		return "", false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 126)
	if num > math.MaxInt32 {
		trace_util_0.Count(_builtin_string_00000, 133)
		num = math.MaxInt32
	}

	trace_util_0.Count(_builtin_string_00000, 127)
	if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 134)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("repeat", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 128)
	if int64(byteLength) > int64(b.tp.Flen)/num {
		trace_util_0.Count(_builtin_string_00000, 135)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 129)
	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 136)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 138)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 137)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinLowerSig{bf}
	return sig, nil
}

type builtinLowerSig struct {
	baseBuiltinFunc
}

func (b *builtinLowerSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 139)
	newSig := &builtinLowerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 140)
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 143)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 141)
	if types.IsBinaryStr(b.args[0].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 144)
		return d, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 142)
	return strings.ToLower(d), false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 145)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 148)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 146)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	retTp := *args[0].GetType()
	retTp.Tp = mysql.TypeVarString
	retTp.Decimal = types.UnspecifiedLength
	bf.tp = &retTp
	var sig builtinFunc
	if types.IsBinaryStr(bf.tp) {
		trace_util_0.Count(_builtin_string_00000, 149)
		sig = &builtinReverseBinarySig{bf}
	} else {
		trace_util_0.Count(_builtin_string_00000, 150)
		{
			sig = &builtinReverseSig{bf}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 147)
	return sig, nil
}

type builtinReverseBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinReverseBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 151)
	newSig := &builtinReverseBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 152)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 154)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 153)
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseSig struct {
	baseBuiltinFunc
}

func (b *builtinReverseSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 155)
	newSig := &builtinReverseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 156)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 158)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 157)
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 159)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 162)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 160)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = mysql.MaxBlobWidth
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 163)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 161)
	sig := &builtinSpaceSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinSpaceSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinSpaceSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 164)
	newSig := &builtinSpaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 165)
	var x int64

	x, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 170)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 166)
	if x < 0 {
		trace_util_0.Count(_builtin_string_00000, 171)
		x = 0
	}
	trace_util_0.Count(_builtin_string_00000, 167)
	if uint64(x) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 172)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("space", b.maxAllowedPacket))
		return d, true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 168)
	if x > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 173)
		return d, true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 169)
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 174)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 176)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 175)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinUpperSig{bf}
	return sig, nil
}

type builtinUpperSig struct {
	baseBuiltinFunc
}

func (b *builtinUpperSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 177)
	newSig := &builtinUpperSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 178)
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 181)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 179)
	if types.IsBinaryStr(b.args[0].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 182)
		return d, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 180)
	return strings.ToUpper(d), false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 183)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 185)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 184)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 2
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{bf}
	return sig, nil
}

type builtinStrcmpSig struct {
	baseBuiltinFunc
}

func (b *builtinStrcmpSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 186)
	newSig := &builtinStrcmpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 187)
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 190)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 188)
	right, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 191)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 189)
	res := types.CompareString(left, right)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 192)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 195)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 193)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = c.fixLength(args)
	for _, a := range args {
		trace_util_0.Count(_builtin_string_00000, 196)
		SetBinFlagOrBinStr(a.GetType(), bf.tp)
	}
	trace_util_0.Count(_builtin_string_00000, 194)
	sig := &builtinReplaceSig{bf}
	return sig, nil
}

// fixLength calculate the Flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	trace_util_0.Count(_builtin_string_00000, 197)
	charLen := args[0].GetType().Flen
	oldStrLen := args[1].GetType().Flen
	diff := args[2].GetType().Flen - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		trace_util_0.Count(_builtin_string_00000, 199)
		charLen += (charLen / oldStrLen) * diff
	}
	trace_util_0.Count(_builtin_string_00000, 198)
	return charLen
}

type builtinReplaceSig struct {
	baseBuiltinFunc
}

func (b *builtinReplaceSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 200)
	newSig := &builtinReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 201)
	var str, oldStr, newStr string

	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 206)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 202)
	oldStr, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 207)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 203)
	newStr, isNull, err = b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 208)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 204)
	if oldStr == "" {
		trace_util_0.Count(_builtin_string_00000, 209)
		return str, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 205)
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 210)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 215)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 211)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	charsetArg, ok := args[1].(*Constant)
	if !ok {
		trace_util_0.Count(_builtin_string_00000, 216)
		// `args[1]` is limited by parser to be a constant string,
		// should never go into here.
		return nil, errIncorrectArgs.GenWithStackByArgs("charset")
	}
	trace_util_0.Count(_builtin_string_00000, 212)
	transcodingName := charsetArg.Value.GetString()
	bf.tp.Charset = strings.ToLower(transcodingName)
	// Quoted about the behavior of syntax CONVERT(expr, type) to CHAR():
	// In all cases, the string has the default collation for the character set.
	// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
	// Here in syntax CONVERT(expr USING transcoding_name), behavior is kept the same,
	// picking the default collation of target charset.
	var err error
	bf.tp.Collate, err = charset.GetDefaultCollation(bf.tp.Charset)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 217)
		return nil, errUnknownCharacterSet.GenWithStackByArgs(transcodingName)
	}
	// Result will be a binary string if converts charset to BINARY.
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-binary-set.html
	trace_util_0.Count(_builtin_string_00000, 213)
	if types.IsBinaryStr(bf.tp) {
		trace_util_0.Count(_builtin_string_00000, 218)
		types.SetBinChsClnFlag(bf.tp)
	} else {
		trace_util_0.Count(_builtin_string_00000, 219)
		{
			bf.tp.Flag &= ^mysql.BinaryFlag
		}
	}

	trace_util_0.Count(_builtin_string_00000, 214)
	bf.tp.Flen = mysql.MaxBlobWidth
	sig := &builtinConvertSig{bf}
	return sig, nil
}

type builtinConvertSig struct {
	baseBuiltinFunc
}

func (b *builtinConvertSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 220)
	newSig := &builtinConvertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONVERT(expr USING transcoding_name).
// Syntax CONVERT(expr, type) is parsed as cast expr so not handled here.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 221)
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 224)
		return "", true, err
	}

	// Since charset is already validated and set from getFunction(), there's no
	// need to get charset from args again.
	trace_util_0.Count(_builtin_string_00000, 222)
	encoding, _ := charset.Lookup(b.tp.Charset)
	// However, if `b.tp.Charset` is abnormally set to a wrong charset, we still
	// return with error.
	if encoding == nil {
		trace_util_0.Count(_builtin_string_00000, 225)
		return "", true, errUnknownCharacterSet.GenWithStackByArgs(b.tp.Charset)
	}

	trace_util_0.Count(_builtin_string_00000, 223)
	target, _, err := transform.String(encoding.NewDecoder(), expr)
	return target, err != nil, err
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 226)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 230)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 227)
	argTps := []types.EvalType{types.ETString, types.ETInt}
	if len(args) == 3 {
		trace_util_0.Count(_builtin_string_00000, 231)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_string_00000, 228)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)

	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)

	var sig builtinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		trace_util_0.Count(_builtin_string_00000, 232)
		sig = &builtinSubstringBinary3ArgsSig{bf}
	case len(args) == 3:
		trace_util_0.Count(_builtin_string_00000, 233)
		sig = &builtinSubstring3ArgsSig{bf}
	case len(args) == 2 && types.IsBinaryStr(argType):
		trace_util_0.Count(_builtin_string_00000, 234)
		sig = &builtinSubstringBinary2ArgsSig{bf}
	case len(args) == 2:
		trace_util_0.Count(_builtin_string_00000, 235)
		sig = &builtinSubstring2ArgsSig{bf}
	default:
		trace_util_0.Count(_builtin_string_00000, 236)
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	trace_util_0.Count(_builtin_string_00000, 229)
	return sig, nil
}

type builtinSubstringBinary2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstringBinary2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 237)
	newSig := &builtinSubstringBinary2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary2ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 238)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 243)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 239)
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 244)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 240)
	length := int64(len(str))
	if pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 245)
		pos += length
	} else {
		trace_util_0.Count(_builtin_string_00000, 246)
		{
			pos--
		}
	}
	trace_util_0.Count(_builtin_string_00000, 241)
	if pos > length || pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 247)
		pos = length
	}
	trace_util_0.Count(_builtin_string_00000, 242)
	return str[pos:], false, nil
}

type builtinSubstring2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 248)
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 249)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 254)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 250)
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 255)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 251)
	runes := []rune(str)
	length := int64(len(runes))
	if pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 256)
		pos += length
	} else {
		trace_util_0.Count(_builtin_string_00000, 257)
		{
			pos--
		}
	}
	trace_util_0.Count(_builtin_string_00000, 252)
	if pos > length || pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 258)
		pos = length
	}
	trace_util_0.Count(_builtin_string_00000, 253)
	return string(runes[pos:]), false, nil
}

type builtinSubstringBinary3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstringBinary3ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 259)
	newSig := &builtinSubstringBinary3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary3ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 260)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 267)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 261)
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 268)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 262)
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 269)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 263)
	byteLen := int64(len(str))
	if pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 270)
		pos += byteLen
	} else {
		trace_util_0.Count(_builtin_string_00000, 271)
		{
			pos--
		}
	}
	trace_util_0.Count(_builtin_string_00000, 264)
	if pos > byteLen || pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 272)
		pos = byteLen
	}
	trace_util_0.Count(_builtin_string_00000, 265)
	end := pos + length
	if end < pos {
		trace_util_0.Count(_builtin_string_00000, 273)
		return "", false, nil
	} else {
		trace_util_0.Count(_builtin_string_00000, 274)
		if end < byteLen {
			trace_util_0.Count(_builtin_string_00000, 275)
			return str[pos:end], false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 266)
	return str[pos:], false, nil
}

type builtinSubstring3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring3ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 276)
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 277)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 284)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 278)
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 285)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 279)
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 286)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 280)
	runes := []rune(str)
	numRunes := int64(len(runes))
	if pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 287)
		pos += numRunes
	} else {
		trace_util_0.Count(_builtin_string_00000, 288)
		{
			pos--
		}
	}
	trace_util_0.Count(_builtin_string_00000, 281)
	if pos > numRunes || pos < 0 {
		trace_util_0.Count(_builtin_string_00000, 289)
		pos = numRunes
	}
	trace_util_0.Count(_builtin_string_00000, 282)
	end := pos + length
	if end < pos {
		trace_util_0.Count(_builtin_string_00000, 290)
		return "", false, nil
	} else {
		trace_util_0.Count(_builtin_string_00000, 291)
		if end < numRunes {
			trace_util_0.Count(_builtin_string_00000, 292)
			return string(runes[pos:end]), false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 283)
	return string(runes[pos:]), false, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 293)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 295)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 294)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinSubstringIndexSig{bf}
	return sig, nil
}

type builtinSubstringIndexSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstringIndexSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 296)
	newSig := &builtinSubstringIndexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 297)
	var (
		str, delim string
		count      int64
	)
	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 303)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 298)
	delim, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 304)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 299)
	count, isNull, err = b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 305)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 300)
	if len(delim) == 0 {
		trace_util_0.Count(_builtin_string_00000, 306)
		return "", false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 301)
	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		trace_util_0.Count(_builtin_string_00000, 307)
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			trace_util_0.Count(_builtin_string_00000, 308)
			end = count
		}
	} else {
		trace_util_0.Count(_builtin_string_00000, 309)
		{
			// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
			count = -count
			if count < 0 {
				trace_util_0.Count(_builtin_string_00000, 311)
				// -count overflows max int64, returns an empty string.
				return "", false, nil
			}

			trace_util_0.Count(_builtin_string_00000, 310)
			if count < end {
				trace_util_0.Count(_builtin_string_00000, 312)
				start = end - count
			}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 302)
	substrs := strs[start:end]
	return strings.Join(substrs, delim), false, nil
}

type locateFunctionClass struct {
	baseFunctionClass
}

func (c *locateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 313)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 317)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 314)
	hasStartPos, argTps := len(args) == 3, []types.EvalType{types.ETString, types.ETString}
	if hasStartPos {
		trace_util_0.Count(_builtin_string_00000, 318)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_string_00000, 315)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig builtinFunc
	// Loacte is multibyte safe, and is case-sensitive only if at least one argument is a binary string.
	hasBianryInput := types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType())
	switch {
	case hasStartPos && hasBianryInput:
		trace_util_0.Count(_builtin_string_00000, 319)
		sig = &builtinLocateBinary3ArgsSig{bf}
	case hasStartPos:
		trace_util_0.Count(_builtin_string_00000, 320)
		sig = &builtinLocate3ArgsSig{bf}
	case hasBianryInput:
		trace_util_0.Count(_builtin_string_00000, 321)
		sig = &builtinLocateBinary2ArgsSig{bf}
	default:
		trace_util_0.Count(_builtin_string_00000, 322)
		sig = &builtinLocate2ArgsSig{bf}
	}
	trace_util_0.Count(_builtin_string_00000, 316)
	return sig, nil
}

type builtinLocateBinary2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLocateBinary2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 323)
	newSig := &builtinLocateBinary2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary2ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 324)
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 329)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 325)
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 330)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 326)
	subStrLen := len(subStr)
	if subStrLen == 0 {
		trace_util_0.Count(_builtin_string_00000, 331)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 327)
	ret, idx := 0, strings.Index(str, subStr)
	if idx != -1 {
		trace_util_0.Count(_builtin_string_00000, 332)
		ret = idx + 1
	}
	trace_util_0.Count(_builtin_string_00000, 328)
	return int64(ret), false, nil
}

type builtinLocate2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLocate2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 333)
	newSig := &builtinLocate2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 334)
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 339)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 335)
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 340)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 336)
	if int64(len([]rune(subStr))) == 0 {
		trace_util_0.Count(_builtin_string_00000, 341)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 337)
	slice := string([]rune(strings.ToLower(str)))
	ret, idx := 0, strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		trace_util_0.Count(_builtin_string_00000, 342)
		ret = utf8.RuneCountInString(slice[:idx]) + 1
	}
	trace_util_0.Count(_builtin_string_00000, 338)
	return int64(ret), false, nil
}

type builtinLocateBinary3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLocateBinary3ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 343)
	newSig := &builtinLocateBinary3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary3ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 344)
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 350)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 345)
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 351)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 346)
	pos, isNull, err := b.args[2].EvalInt(b.ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 352)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 347)
	subStrLen := len(subStr)
	if pos < 0 || pos > int64(len(str)-subStrLen) {
		trace_util_0.Count(_builtin_string_00000, 353)
		return 0, false, nil
	} else {
		trace_util_0.Count(_builtin_string_00000, 354)
		if subStrLen == 0 {
			trace_util_0.Count(_builtin_string_00000, 355)
			return pos + 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 348)
	slice := str[pos:]
	idx := strings.Index(slice, subStr)
	if idx != -1 {
		trace_util_0.Count(_builtin_string_00000, 356)
		return pos + int64(idx) + 1, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 349)
	return 0, false, nil
}

type builtinLocate3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLocate3ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 357)
	newSig := &builtinLocate3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 358)
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 364)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 359)
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 365)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 360)
	pos, isNull, err := b.args[2].EvalInt(b.ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 366)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 361)
	subStrLen := len([]rune(subStr))
	if pos < 0 || pos > int64(len([]rune(strings.ToLower(str)))-subStrLen) {
		trace_util_0.Count(_builtin_string_00000, 367)
		return 0, false, nil
	} else {
		trace_util_0.Count(_builtin_string_00000, 368)
		if subStrLen == 0 {
			trace_util_0.Count(_builtin_string_00000, 369)
			return pos + 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 362)
	slice := string([]rune(strings.ToLower(str))[pos:])
	idx := strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		trace_util_0.Count(_builtin_string_00000, 370)
		return pos + int64(utf8.RuneCountInString(slice[:idx])) + 1, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 363)
	return 0, false, nil
}

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 371)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 373)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 372)
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		trace_util_0.Count(_builtin_string_00000, 374)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		// Use UTF-8 as default
		bf.tp.Flen = args[0].GetType().Flen * 3 * 2
		sig := &builtinHexStrArgSig{bf}
		return sig, nil
	case types.ETInt, types.ETReal, types.ETDecimal:
		trace_util_0.Count(_builtin_string_00000, 375)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
		bf.tp.Flen = args[0].GetType().Flen * 2
		sig := &builtinHexIntArgSig{bf}
		return sig, nil
	default:
		trace_util_0.Count(_builtin_string_00000, 376)
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", args[0].GetType())
	}
}

type builtinHexStrArgSig struct {
	baseBuiltinFunc
}

func (b *builtinHexStrArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 377)
	newSig := &builtinHexStrArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 378)
	d, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 380)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 379)
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	baseBuiltinFunc
}

func (b *builtinHexIntArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 381)
	newSig := &builtinHexIntArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 382)
	x, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 384)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 383)
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 385)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 389)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 386)
	var retFlen int

	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 390)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 387)
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	switch argEvalTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		trace_util_0.Count(_builtin_string_00000, 391)
		// Use UTF-8 as default charset, so there're (Flen * 3 + 1) / 2 byte-pairs
		retFlen = (argType.Flen*3 + 1) / 2
	case types.ETInt, types.ETReal, types.ETDecimal:
		trace_util_0.Count(_builtin_string_00000, 392)
		// For number value, there're (Flen + 1) / 2 byte-pairs
		retFlen = (argType.Flen + 1) / 2
	default:
		trace_util_0.Count(_builtin_string_00000, 393)
		return nil, errors.Errorf("Unhex invalid args, need int or string but get %s", argType)
	}

	trace_util_0.Count(_builtin_string_00000, 388)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = retFlen
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUnHexSig{bf}
	return sig, nil
}

type builtinUnHexSig struct {
	baseBuiltinFunc
}

func (b *builtinUnHexSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 394)
	newSig := &builtinUnHexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 395)
	var bs []byte

	d, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 399)
		return d, isNull, err
	}
	// Add a '0' to the front, if the length is not the multiple of 2
	trace_util_0.Count(_builtin_string_00000, 396)
	if len(d)%2 != 0 {
		trace_util_0.Count(_builtin_string_00000, 400)
		d = "0" + d
	}
	trace_util_0.Count(_builtin_string_00000, 397)
	bs, err = hex.DecodeString(d)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 401)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 398)
	return string(bs), false, nil
}

const spaceChars = " "

type trimFunctionClass struct {
	baseFunctionClass
}

// getFunction sets trim built-in function signature.
// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *trimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 402)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 404)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 403)
	switch len(args) {
	case 1:
		trace_util_0.Count(_builtin_string_00000, 405)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim1ArgSig{bf}
		return sig, nil

	case 2:
		trace_util_0.Count(_builtin_string_00000, 406)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
		argType := args[0].GetType()
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim2ArgsSig{bf}
		return sig, nil

	case 3:
		trace_util_0.Count(_builtin_string_00000, 407)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETInt)
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim3ArgsSig{bf}
		return sig, nil

	default:
		trace_util_0.Count(_builtin_string_00000, 408)
		return nil, c.verifyArgs(args)
	}
}

type builtinTrim1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 409)
	newSig := &builtinTrim1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 410)
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 412)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 411)
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 413)
	newSig := &builtinTrim2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 414)
	var str, remstr string

	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 417)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 415)
	remstr, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 418)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 416)
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim3ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 419)
	newSig := &builtinTrim3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 420)
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 425)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 421)
	remstr, isRemStrNull, err = b.args[1].EvalString(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 426)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 422)
	x, isNull, err = b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 427)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 423)
	direction = ast.TrimDirectionType(x)
	if direction == ast.TrimLeading {
		trace_util_0.Count(_builtin_string_00000, 428)
		if isRemStrNull {
			trace_util_0.Count(_builtin_string_00000, 429)
			d = strings.TrimLeft(str, spaceChars)
		} else {
			trace_util_0.Count(_builtin_string_00000, 430)
			{
				d = trimLeft(str, remstr)
			}
		}
	} else {
		trace_util_0.Count(_builtin_string_00000, 431)
		if direction == ast.TrimTrailing {
			trace_util_0.Count(_builtin_string_00000, 432)
			if isRemStrNull {
				trace_util_0.Count(_builtin_string_00000, 433)
				d = strings.TrimRight(str, spaceChars)
			} else {
				trace_util_0.Count(_builtin_string_00000, 434)
				{
					d = trimRight(str, remstr)
				}
			}
		} else {
			trace_util_0.Count(_builtin_string_00000, 435)
			{
				if isRemStrNull {
					trace_util_0.Count(_builtin_string_00000, 436)
					d = strings.Trim(str, spaceChars)
				} else {
					trace_util_0.Count(_builtin_string_00000, 437)
					{
						d = trimLeft(str, remstr)
						d = trimRight(d, remstr)
					}
				}
			}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 424)
	return d, false, nil
}

type lTrimFunctionClass struct {
	baseFunctionClass
}

func (c *lTrimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 438)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 440)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 439)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinLTrimSig{bf}
	return sig, nil
}

type builtinLTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinLTrimSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 441)
	newSig := &builtinLTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 442)
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 444)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 443)
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 445)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 447)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 446)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinRTrimSig{bf}
	return sig, nil
}

type builtinRTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinRTrimSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 448)
	newSig := &builtinRTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 449)
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 451)
		return d, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 450)
	return strings.TrimRight(d, spaceChars), false, nil
}

func trimLeft(str, remstr string) string {
	trace_util_0.Count(_builtin_string_00000, 452)
	for {
		trace_util_0.Count(_builtin_string_00000, 453)
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			trace_util_0.Count(_builtin_string_00000, 455)
			return x
		}
		trace_util_0.Count(_builtin_string_00000, 454)
		str = x
	}
}

func trimRight(str, remstr string) string {
	trace_util_0.Count(_builtin_string_00000, 456)
	for {
		trace_util_0.Count(_builtin_string_00000, 457)
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			trace_util_0.Count(_builtin_string_00000, 459)
			return x
		}
		trace_util_0.Count(_builtin_string_00000, 458)
		str = x
	}
}

func getFlen4LpadAndRpad(ctx sessionctx.Context, arg Expression) int {
	trace_util_0.Count(_builtin_string_00000, 460)
	if constant, ok := arg.(*Constant); ok {
		trace_util_0.Count(_builtin_string_00000, 462)
		length, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 465)
			logutil.Logger(context.Background()).Error("eval `Flen` for LPAD/RPAD", zap.Error(err))
		}
		trace_util_0.Count(_builtin_string_00000, 463)
		if isNull || err != nil || length > mysql.MaxBlobWidth {
			trace_util_0.Count(_builtin_string_00000, 466)
			return mysql.MaxBlobWidth
		}
		trace_util_0.Count(_builtin_string_00000, 464)
		return int(length)
	}
	trace_util_0.Count(_builtin_string_00000, 461)
	return mysql.MaxBlobWidth
}

type lpadFunctionClass struct {
	baseFunctionClass
}

func (c *lpadFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 467)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 472)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 468)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	bf.tp.Flen = getFlen4LpadAndRpad(bf.ctx, args[1])
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	SetBinFlagOrBinStr(args[2].GetType(), bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 473)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 469)
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 474)
		sig := &builtinLpadBinarySig{bf, maxAllowedPacket}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 470)
	if bf.tp.Flen *= 4; bf.tp.Flen > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 475)
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_string_00000, 471)
	sig := &builtinLpadSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinLpadBinarySig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 476)
	newSig := &builtinLpadBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 477)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 484)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 478)
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 485)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 479)
	targetLength := int(length)

	if uint64(targetLength) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 486)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("lpad", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 480)
	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 487)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 481)
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.Flen || (byteLength < targetLength && padLength == 0) {
		trace_util_0.Count(_builtin_string_00000, 488)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 482)
	if tailLen := targetLength - byteLength; tailLen > 0 {
		trace_util_0.Count(_builtin_string_00000, 489)
		repeatCount := tailLen/padLength + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	trace_util_0.Count(_builtin_string_00000, 483)
	return str[:targetLength], false, nil
}

type builtinLpadSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 490)
	newSig := &builtinLpadSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 491)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 498)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 492)
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 499)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 493)
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 500)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("lpad", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 494)
	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 501)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 495)
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
		trace_util_0.Count(_builtin_string_00000, 502)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 496)
	if tailLen := targetLength - runeLength; tailLen > 0 {
		trace_util_0.Count(_builtin_string_00000, 503)
		repeatCount := tailLen/padLength + 1
		str = string([]rune(strings.Repeat(padStr, repeatCount))[:tailLen]) + str
	}
	trace_util_0.Count(_builtin_string_00000, 497)
	return string([]rune(str)[:targetLength]), false, nil
}

type rpadFunctionClass struct {
	baseFunctionClass
}

func (c *rpadFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 504)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 509)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 505)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	bf.tp.Flen = getFlen4LpadAndRpad(bf.ctx, args[1])
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	SetBinFlagOrBinStr(args[2].GetType(), bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 510)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 506)
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 511)
		sig := &builtinRpadBinarySig{bf, maxAllowedPacket}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 507)
	if bf.tp.Flen *= 4; bf.tp.Flen > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 512)
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_string_00000, 508)
	sig := &builtinRpadSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinRpadBinarySig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 513)
	newSig := &builtinRpadBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 514)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 521)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 515)
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 522)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 516)
	targetLength := int(length)
	if uint64(targetLength) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 523)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 517)
	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 524)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 518)
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.Flen || (byteLength < targetLength && padLength == 0) {
		trace_util_0.Count(_builtin_string_00000, 525)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 519)
	if tailLen := targetLength - byteLength; tailLen > 0 {
		trace_util_0.Count(_builtin_string_00000, 526)
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	trace_util_0.Count(_builtin_string_00000, 520)
	return str[:targetLength], false, nil
}

type builtinRpadSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 527)
	newSig := &builtinRpadSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 528)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 535)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 529)
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 536)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 530)
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 537)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 531)
	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 538)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 532)
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
		trace_util_0.Count(_builtin_string_00000, 539)
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 533)
	if tailLen := targetLength - runeLength; tailLen > 0 {
		trace_util_0.Count(_builtin_string_00000, 540)
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	trace_util_0.Count(_builtin_string_00000, 534)
	return string([]rune(str)[:targetLength]), false, nil
}

type bitLengthFunctionClass struct {
	baseFunctionClass
}

func (c *bitLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 541)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 543)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 542)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinBitLengthSig{bf}
	return sig, nil
}

type builtinBitLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinBitLengthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 544)
	newSig := &builtinBitLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 545)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 547)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 546)
	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 548)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 551)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 549)
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i++ {
		trace_util_0.Count(_builtin_string_00000, 552)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_string_00000, 550)
	argTps = append(argTps, types.ETString)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.tp.Flen = 4 * (len(args) - 1)
	types.SetBinChsClnFlag(bf.tp)

	sig := &builtinCharSig{bf}
	return sig, nil
}

type builtinCharSig struct {
	baseBuiltinFunc
}

func (b *builtinCharSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 553)
	newSig := &builtinCharSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCharSig) convertToBytes(ints []int64) []byte {
	trace_util_0.Count(_builtin_string_00000, 554)
	buffer := bytes.NewBuffer([]byte{})
	for i := len(ints) - 1; i >= 0; i-- {
		trace_util_0.Count(_builtin_string_00000, 556)
		for count, val := 0, ints[i]; count < 4; count++ {
			trace_util_0.Count(_builtin_string_00000, 557)
			buffer.WriteByte(byte(val & 0xff))
			if val >>= 8; val == 0 {
				trace_util_0.Count(_builtin_string_00000, 558)
				break
			}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 555)
	return reverseBytes(buffer.Bytes())
}

// evalString evals CHAR(N,... [USING charset_name]).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char.
func (b *builtinCharSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 559)
	bigints := make([]int64, 0, len(b.args)-1)

	for i := 0; i < len(b.args)-1; i++ {
		trace_util_0.Count(_builtin_string_00000, 565)
		val, IsNull, err := b.args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 568)
			return "", true, err
		}
		trace_util_0.Count(_builtin_string_00000, 566)
		if IsNull {
			trace_util_0.Count(_builtin_string_00000, 569)
			continue
		}
		trace_util_0.Count(_builtin_string_00000, 567)
		bigints = append(bigints, val)
	}
	// The last argument represents the charset name after "using".
	// Use default charset utf8 if it is nil.
	trace_util_0.Count(_builtin_string_00000, 560)
	argCharset, IsNull, err := b.args[len(b.args)-1].EvalString(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 570)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 561)
	result := string(b.convertToBytes(bigints))
	charsetLabel := strings.ToLower(argCharset)
	if IsNull || charsetLabel == "ascii" || strings.HasPrefix(charsetLabel, "utf8") {
		trace_util_0.Count(_builtin_string_00000, 571)
		return result, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 562)
	encoding, charsetName := charset.Lookup(charsetLabel)
	if encoding == nil {
		trace_util_0.Count(_builtin_string_00000, 572)
		return "", true, errors.Errorf("unknown encoding: %s", argCharset)
	}

	trace_util_0.Count(_builtin_string_00000, 563)
	oldStr := result
	result, _, err = transform.String(encoding.NewDecoder(), result)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 573)
		logutil.Logger(context.Background()).Warn("change charset of string",
			zap.String("string", oldStr),
			zap.String("charset", charsetName),
			zap.Error(err))
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 564)
	return result, false, nil
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 574)
	if argsErr := c.verifyArgs(args); argsErr != nil {
		trace_util_0.Count(_builtin_string_00000, 577)
		return nil, argsErr
	}
	trace_util_0.Count(_builtin_string_00000, 575)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	if types.IsBinaryStr(args[0].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 578)
		sig := &builtinCharLengthBinarySig{bf}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 576)
	sig := &builtinCharLengthSig{bf}
	return sig, nil
}

type builtinCharLengthBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinCharLengthBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 579)
	newSig := &builtinCharLengthBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthSig for binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 580)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 582)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 581)
	return int64(len(val)), false, nil
}

type builtinCharLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinCharLengthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 583)
	newSig := &builtinCharLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthSig for non-binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 584)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 586)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 585)
	return int64(len([]rune(val))), false, nil
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 587)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 589)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 588)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 3
	sig := &builtinFindInSetSig{bf}
	return sig, nil
}

type builtinFindInSetSig struct {
	baseBuiltinFunc
}

func (b *builtinFindInSetSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 590)
	newSig := &builtinFindInSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIND_IN_SET(str,strlist).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 591)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 596)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 592)
	strlist, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 597)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 593)
	if len(strlist) == 0 {
		trace_util_0.Count(_builtin_string_00000, 598)
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 594)
	for i, strInSet := range strings.Split(strlist, ",") {
		trace_util_0.Count(_builtin_string_00000, 599)
		if str == strInSet {
			trace_util_0.Count(_builtin_string_00000, 600)
			return int64(i + 1), false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 595)
	return 0, false, nil
}

type fieldFunctionClass struct {
	baseFunctionClass
}

func (c *fieldFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 601)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 607)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 602)
	isAllString, isAllNumber := true, true
	for i, length := 0, len(args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 608)
		argTp := args[i].GetType().EvalType()
		isAllString = isAllString && (argTp == types.ETString)
		isAllNumber = isAllNumber && (argTp == types.ETInt)
	}

	trace_util_0.Count(_builtin_string_00000, 603)
	argTps := make([]types.EvalType, len(args))
	argTp := types.ETReal
	if isAllString {
		trace_util_0.Count(_builtin_string_00000, 609)
		argTp = types.ETString
	} else {
		trace_util_0.Count(_builtin_string_00000, 610)
		if isAllNumber {
			trace_util_0.Count(_builtin_string_00000, 611)
			argTp = types.ETInt
		}
	}
	trace_util_0.Count(_builtin_string_00000, 604)
	for i, length := 0, len(args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 612)
		argTps[i] = argTp
	}
	trace_util_0.Count(_builtin_string_00000, 605)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig builtinFunc
	switch argTp {
	case types.ETReal:
		trace_util_0.Count(_builtin_string_00000, 613)
		sig = &builtinFieldRealSig{bf}
	case types.ETInt:
		trace_util_0.Count(_builtin_string_00000, 614)
		sig = &builtinFieldIntSig{bf}
	case types.ETString:
		trace_util_0.Count(_builtin_string_00000, 615)
		sig = &builtinFieldStringSig{bf}
	}
	trace_util_0.Count(_builtin_string_00000, 606)
	return sig, nil
}

type builtinFieldIntSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 616)
	newSig := &builtinFieldIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 617)
	str, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 620)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 618)
	for i, length := 1, len(b.args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 621)
		stri, isNull, err := b.args[i].EvalInt(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 623)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_string_00000, 622)
		if !isNull && str == stri {
			trace_util_0.Count(_builtin_string_00000, 624)
			return int64(i), false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 619)
	return 0, false, nil
}

type builtinFieldRealSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 625)
	newSig := &builtinFieldRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 626)
	str, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 629)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 627)
	for i, length := 1, len(b.args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 630)
		stri, isNull, err := b.args[i].EvalReal(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 632)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_string_00000, 631)
		if !isNull && str == stri {
			trace_util_0.Count(_builtin_string_00000, 633)
			return int64(i), false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 628)
	return 0, false, nil
}

type builtinFieldStringSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 634)
	newSig := &builtinFieldStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 635)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 638)
		return 0, err != nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 636)
	for i, length := 1, len(b.args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 639)
		stri, isNull, err := b.args[i].EvalString(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 641)
			return 0, true, err
		}
		trace_util_0.Count(_builtin_string_00000, 640)
		if !isNull && str == stri {
			trace_util_0.Count(_builtin_string_00000, 642)
			return int64(i), false, nil
		}
	}
	trace_util_0.Count(_builtin_string_00000, 637)
	return 0, false, nil
}

type makeSetFunctionClass struct {
	baseFunctionClass
}

func (c *makeSetFunctionClass) getFlen(ctx sessionctx.Context, args []Expression) int {
	trace_util_0.Count(_builtin_string_00000, 643)
	flen, count := 0, 0
	if constant, ok := args[0].(*Constant); ok {
		trace_util_0.Count(_builtin_string_00000, 646)
		bits, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err == nil && !isNull {
			trace_util_0.Count(_builtin_string_00000, 647)
			for i, length := 1, len(args); i < length; i++ {
				trace_util_0.Count(_builtin_string_00000, 650)
				if (bits & (1 << uint(i-1))) != 0 {
					trace_util_0.Count(_builtin_string_00000, 651)
					flen += args[i].GetType().Flen
					count++
				}
			}
			trace_util_0.Count(_builtin_string_00000, 648)
			if count > 0 {
				trace_util_0.Count(_builtin_string_00000, 652)
				flen += count - 1
			}
			trace_util_0.Count(_builtin_string_00000, 649)
			return flen
		}
	}
	trace_util_0.Count(_builtin_string_00000, 644)
	for i, length := 1, len(args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 653)
		flen += args[i].GetType().Flen
	}
	trace_util_0.Count(_builtin_string_00000, 645)
	return flen + len(args) - 1 - 1
}

func (c *makeSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 654)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 659)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 655)
	argTps := make([]types.EvalType, len(args))
	argTps[0] = types.ETInt
	for i, length := 1, len(args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 660)
		argTps[i] = types.ETString
	}
	trace_util_0.Count(_builtin_string_00000, 656)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for i, length := 0, len(args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 661)
		SetBinFlagOrBinStr(args[i].GetType(), bf.tp)
	}
	trace_util_0.Count(_builtin_string_00000, 657)
	bf.tp.Flen = c.getFlen(bf.ctx, args)
	if bf.tp.Flen > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 662)
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_string_00000, 658)
	sig := &builtinMakeSetSig{bf}
	return sig, nil
}

type builtinMakeSetSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeSetSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 663)
	newSig := &builtinMakeSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 664)
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 667)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 665)
	sets := make([]string, 0, len(b.args)-1)
	for i, length := 1, len(b.args); i < length; i++ {
		trace_util_0.Count(_builtin_string_00000, 668)
		if (bits & (1 << uint(i-1))) == 0 {
			trace_util_0.Count(_builtin_string_00000, 671)
			continue
		}
		trace_util_0.Count(_builtin_string_00000, 669)
		str, isNull, err := b.args[i].EvalString(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_string_00000, 672)
			return "", true, err
		}
		trace_util_0.Count(_builtin_string_00000, 670)
		if !isNull {
			trace_util_0.Count(_builtin_string_00000, 673)
			sets = append(sets, str)
		}
	}

	trace_util_0.Count(_builtin_string_00000, 666)
	return strings.Join(sets, ","), false, nil
}

type octFunctionClass struct {
	baseFunctionClass
}

func (c *octFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 674)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 677)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 675)
	var sig builtinFunc
	if IsBinaryLiteral(args[0]) || args[0].GetType().EvalType() == types.ETInt {
		trace_util_0.Count(_builtin_string_00000, 678)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
		bf.tp.Flen, bf.tp.Decimal = 64, types.UnspecifiedLength
		sig = &builtinOctIntSig{bf}
	} else {
		trace_util_0.Count(_builtin_string_00000, 679)
		{
			bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
			bf.tp.Flen, bf.tp.Decimal = 64, types.UnspecifiedLength
			sig = &builtinOctStringSig{bf}
		}
	}

	trace_util_0.Count(_builtin_string_00000, 676)
	return sig, nil
}

type builtinOctIntSig struct {
	baseBuiltinFunc
}

func (b *builtinOctIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 680)
	newSig := &builtinOctIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctIntSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 681)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 683)
		return "", isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 682)
	return strconv.FormatUint(uint64(val), 8), false, nil
}

type builtinOctStringSig struct {
	baseBuiltinFunc
}

func (b *builtinOctStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 684)
	newSig := &builtinOctStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 685)
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 691)
		return "", isNull, err
	}

	trace_util_0.Count(_builtin_string_00000, 686)
	negative, overflow := false, false
	val = getValidPrefix(strings.TrimSpace(val), 10)
	if len(val) == 0 {
		trace_util_0.Count(_builtin_string_00000, 692)
		return "0", false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 687)
	if val[0] == '-' {
		trace_util_0.Count(_builtin_string_00000, 693)
		negative, val = true, val[1:]
	}
	trace_util_0.Count(_builtin_string_00000, 688)
	numVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 694)
		numError, ok := err.(*strconv.NumError)
		if !ok || numError.Err != strconv.ErrRange {
			trace_util_0.Count(_builtin_string_00000, 696)
			return "", true, err
		}
		trace_util_0.Count(_builtin_string_00000, 695)
		overflow = true
	}
	trace_util_0.Count(_builtin_string_00000, 689)
	if negative && !overflow {
		trace_util_0.Count(_builtin_string_00000, 697)
		numVal = -numVal
	}
	trace_util_0.Count(_builtin_string_00000, 690)
	return strconv.FormatUint(numVal, 8), false, nil
}

type ordFunctionClass struct {
	baseFunctionClass
}

func (c *ordFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 698)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 700)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 699)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinOrdSig{bf}
	return sig, nil
}

type builtinOrdSig struct {
	baseBuiltinFunc
}

func (b *builtinOrdSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 701)
	newSig := &builtinOrdSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 702)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 706)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 703)
	if len(str) == 0 {
		trace_util_0.Count(_builtin_string_00000, 707)
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 704)
	_, size := utf8.DecodeRuneInString(str)
	leftMost := str[:size]
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		trace_util_0.Count(_builtin_string_00000, 708)
		result += int64(leftMost[i]) * factor
		factor *= 256
	}

	trace_util_0.Count(_builtin_string_00000, 705)
	return result, false, nil
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 709)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 712)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 710)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	bf.tp.Flen = 2*args[0].GetType().Flen + 2
	if bf.tp.Flen > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 713)
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	trace_util_0.Count(_builtin_string_00000, 711)
	sig := &builtinQuoteSig{bf}
	return sig, nil
}

type builtinQuoteSig struct {
	baseBuiltinFunc
}

func (b *builtinQuoteSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 714)
	newSig := &builtinQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals QUOTE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 715)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 718)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 716)
	runes := []rune(str)
	buffer := bytes.NewBufferString("")
	buffer.WriteRune('\'')
	for i, runeLength := 0, len(runes); i < runeLength; i++ {
		trace_util_0.Count(_builtin_string_00000, 719)
		switch runes[i] {
		case '\\', '\'':
			trace_util_0.Count(_builtin_string_00000, 720)
			buffer.WriteRune('\\')
			buffer.WriteRune(runes[i])
		case 0:
			trace_util_0.Count(_builtin_string_00000, 721)
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			trace_util_0.Count(_builtin_string_00000, 722)
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			trace_util_0.Count(_builtin_string_00000, 723)
			buffer.WriteRune(runes[i])
		}
	}
	trace_util_0.Count(_builtin_string_00000, 717)
	buffer.WriteRune('\'')

	return buffer.String(), false, nil
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 724)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 726)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 725)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = 64
	sig := &builtinBinSig{bf}
	return sig, nil
}

type builtinBinSig struct {
	baseBuiltinFunc
}

func (b *builtinBinSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 727)
	newSig := &builtinBinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals BIN(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bin
func (b *builtinBinSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 728)
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 730)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 729)
	return fmt.Sprintf("%b", uint64(val)), false, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 731)
	if argsErr := c.verifyArgs(args); argsErr != nil {
		trace_util_0.Count(_builtin_string_00000, 735)
		return nil, argsErr
	}
	trace_util_0.Count(_builtin_string_00000, 732)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETInt)
	for i := 1; i < len(args); i++ {
		trace_util_0.Count(_builtin_string_00000, 736)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_string_00000, 733)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for _, arg := range args[1:] {
		trace_util_0.Count(_builtin_string_00000, 737)
		argType := arg.GetType()
		if types.IsBinaryStr(argType) {
			trace_util_0.Count(_builtin_string_00000, 739)
			types.SetBinChsClnFlag(bf.tp)
		}
		trace_util_0.Count(_builtin_string_00000, 738)
		if argType.Flen > bf.tp.Flen {
			trace_util_0.Count(_builtin_string_00000, 740)
			bf.tp.Flen = argType.Flen
		}
	}
	trace_util_0.Count(_builtin_string_00000, 734)
	sig := &builtinEltSig{bf}
	return sig, nil
}

type builtinEltSig struct {
	baseBuiltinFunc
}

func (b *builtinEltSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 741)
	newSig := &builtinEltSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinEltSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 742)
	idx, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 746)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 743)
	if idx < 1 || idx >= int64(len(b.args)) {
		trace_util_0.Count(_builtin_string_00000, 747)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 744)
	arg, isNull, err := b.args[idx].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 748)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 745)
	return arg, false, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_string_00000, 749)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 754)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 750)
	argTps := make([]types.EvalType, 0, 5)
	argTps = append(argTps, types.ETInt, types.ETString, types.ETString)
	if len(args) > 3 {
		trace_util_0.Count(_builtin_string_00000, 755)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_string_00000, 751)
	if len(args) > 4 {
		trace_util_0.Count(_builtin_string_00000, 756)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_string_00000, 752)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.tp.Flen = mysql.MaxBlobWidth
	switch len(args) {
	case 3:
		trace_util_0.Count(_builtin_string_00000, 757)
		sig = &builtinExportSet3ArgSig{bf}
	case 4:
		trace_util_0.Count(_builtin_string_00000, 758)
		sig = &builtinExportSet4ArgSig{bf}
	case 5:
		trace_util_0.Count(_builtin_string_00000, 759)
		sig = &builtinExportSet5ArgSig{bf}
	}
	trace_util_0.Count(_builtin_string_00000, 753)
	return sig, nil
}

// exportSet evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func exportSet(bits int64, on, off, separator string, numberOfBits int64) string {
	trace_util_0.Count(_builtin_string_00000, 760)
	result := ""
	for i := uint64(0); i < uint64(numberOfBits); i++ {
		trace_util_0.Count(_builtin_string_00000, 762)
		if (bits & (1 << i)) > 0 {
			trace_util_0.Count(_builtin_string_00000, 764)
			result += on
		} else {
			trace_util_0.Count(_builtin_string_00000, 765)
			{
				result += off
			}
		}
		trace_util_0.Count(_builtin_string_00000, 763)
		if i < uint64(numberOfBits)-1 {
			trace_util_0.Count(_builtin_string_00000, 766)
			result += separator
		}
	}
	trace_util_0.Count(_builtin_string_00000, 761)
	return result
}

type builtinExportSet3ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet3ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 767)
	newSig := &builtinExportSet3ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 768)
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 772)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 769)
	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 773)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 770)
	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 774)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 771)
	return exportSet(bits, on, off, ",", 64), false, nil
}

type builtinExportSet4ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet4ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 775)
	newSig := &builtinExportSet4ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 776)
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 781)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 777)
	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 782)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 778)
	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 783)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 779)
	separator, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 784)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 780)
	return exportSet(bits, on, off, separator, 64), false, nil
}

type builtinExportSet5ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet5ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 785)
	newSig := &builtinExportSet5ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 786)
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 793)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 787)
	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 794)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 788)
	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 795)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 789)
	separator, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 796)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 790)
	numberOfBits, isNull, err := b.args[4].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 797)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 791)
	if numberOfBits < 0 || numberOfBits > 64 {
		trace_util_0.Count(_builtin_string_00000, 798)
		numberOfBits = 64
	}

	trace_util_0.Count(_builtin_string_00000, 792)
	return exportSet(bits, on, off, separator, numberOfBits), false, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 799)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 804)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 800)
	argTps := make([]types.EvalType, 2, 3)
	argTps[1] = types.ETInt
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETDecimal || argTp == types.ETInt {
		trace_util_0.Count(_builtin_string_00000, 805)
		argTps[0] = types.ETDecimal
	} else {
		trace_util_0.Count(_builtin_string_00000, 806)
		{
			argTps[0] = types.ETReal
		}
	}
	trace_util_0.Count(_builtin_string_00000, 801)
	if len(args) == 3 {
		trace_util_0.Count(_builtin_string_00000, 807)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_string_00000, 802)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.tp.Flen = mysql.MaxBlobWidth
	var sig builtinFunc
	if len(args) == 3 {
		trace_util_0.Count(_builtin_string_00000, 808)
		sig = &builtinFormatWithLocaleSig{bf}
	} else {
		trace_util_0.Count(_builtin_string_00000, 809)
		{
			sig = &builtinFormatSig{bf}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 803)
	return sig, nil
}

// formatMaxDecimals limits the maximum number of decimal digits for result of
// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
const formatMaxDecimals int64 = 30

// evalNumDecArgsForFormat evaluates first 2 arguments, i.e, x and d, for function `format`.
func evalNumDecArgsForFormat(f builtinFunc, row chunk.Row) (string, string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 810)
	var xStr string
	arg0, arg1 := f.getArgs()[0], f.getArgs()[1]
	ctx := f.getCtx()
	if arg0.GetType().EvalType() == types.ETDecimal {
		trace_util_0.Count(_builtin_string_00000, 814)
		x, isNull, err := arg0.EvalDecimal(ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_string_00000, 816)
			return "", "", isNull, err
		}
		trace_util_0.Count(_builtin_string_00000, 815)
		xStr = x.String()
	} else {
		trace_util_0.Count(_builtin_string_00000, 817)
		{
			x, isNull, err := arg0.EvalReal(ctx, row)
			if isNull || err != nil {
				trace_util_0.Count(_builtin_string_00000, 819)
				return "", "", isNull, err
			}
			trace_util_0.Count(_builtin_string_00000, 818)
			xStr = strconv.FormatFloat(x, 'f', -1, 64)
		}
	}
	trace_util_0.Count(_builtin_string_00000, 811)
	d, isNull, err := arg1.EvalInt(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 820)
		return "", "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 812)
	if d < 0 {
		trace_util_0.Count(_builtin_string_00000, 821)
		d = 0
	} else {
		trace_util_0.Count(_builtin_string_00000, 822)
		if d > formatMaxDecimals {
			trace_util_0.Count(_builtin_string_00000, 823)
			d = formatMaxDecimals
		}
	}
	trace_util_0.Count(_builtin_string_00000, 813)
	xStr = roundFormatArgs(xStr, int(d))
	dStr := strconv.FormatInt(d, 10)
	return xStr, dStr, false, nil
}

func roundFormatArgs(xStr string, maxNumDecimals int) string {
	trace_util_0.Count(_builtin_string_00000, 824)
	if !strings.Contains(xStr, ".") {
		trace_util_0.Count(_builtin_string_00000, 829)
		return xStr
	}

	trace_util_0.Count(_builtin_string_00000, 825)
	sign := false
	// xStr cannot have '+' prefix now.
	// It is built in `evalNumDecArgsFormat` after evaluating `Evalxxx` method.
	if strings.HasPrefix(xStr, "-") {
		trace_util_0.Count(_builtin_string_00000, 830)
		xStr = strings.Trim(xStr, "-")
		sign = true
	}

	trace_util_0.Count(_builtin_string_00000, 826)
	xArr := strings.Split(xStr, ".")
	integerPart := xArr[0]
	decimalPart := xArr[1]

	if len(decimalPart) > maxNumDecimals {
		trace_util_0.Count(_builtin_string_00000, 831)
		t := []byte(decimalPart)
		carry := false
		if t[maxNumDecimals] >= '5' {
			trace_util_0.Count(_builtin_string_00000, 835)
			carry = true
		}
		trace_util_0.Count(_builtin_string_00000, 832)
		for i := maxNumDecimals - 1; i >= 0 && carry; i-- {
			trace_util_0.Count(_builtin_string_00000, 836)
			if t[i] == '9' {
				trace_util_0.Count(_builtin_string_00000, 837)
				t[i] = '0'
			} else {
				trace_util_0.Count(_builtin_string_00000, 838)
				{
					t[i] = t[i] + 1
					carry = false
				}
			}
		}
		trace_util_0.Count(_builtin_string_00000, 833)
		decimalPart = string(t)
		t = []byte(integerPart)
		for i := len(integerPart) - 1; i >= 0 && carry; i-- {
			trace_util_0.Count(_builtin_string_00000, 839)
			if t[i] == '9' {
				trace_util_0.Count(_builtin_string_00000, 840)
				t[i] = '0'
			} else {
				trace_util_0.Count(_builtin_string_00000, 841)
				{
					t[i] = t[i] + 1
					carry = false
				}
			}
		}
		trace_util_0.Count(_builtin_string_00000, 834)
		if carry {
			trace_util_0.Count(_builtin_string_00000, 842)
			integerPart = "1" + string(t)
		} else {
			trace_util_0.Count(_builtin_string_00000, 843)
			{
				integerPart = string(t)
			}
		}
	}

	trace_util_0.Count(_builtin_string_00000, 827)
	xStr = integerPart + "." + decimalPart
	if sign {
		trace_util_0.Count(_builtin_string_00000, 844)
		xStr = "-" + xStr
	}
	trace_util_0.Count(_builtin_string_00000, 828)
	return xStr
}

type builtinFormatWithLocaleSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatWithLocaleSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 845)
	newSig := &builtinFormatWithLocaleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D,locale).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatWithLocaleSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 846)
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 850)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 847)
	locale, isNull, err := b.args[2].EvalString(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 851)
		return "", false, err
	}
	trace_util_0.Count(_builtin_string_00000, 848)
	if isNull {
		trace_util_0.Count(_builtin_string_00000, 852)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errUnknownLocale.GenWithStackByArgs("NULL"))
		locale = "en_US"
	}
	trace_util_0.Count(_builtin_string_00000, 849)
	formatString, err := mysql.GetLocaleFormatFunction(locale)(x, d)
	return formatString, false, err
}

type builtinFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 853)
	newSig := &builtinFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 854)
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 856)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 855)
	formatString, err := mysql.GetLocaleFormatFunction("en_US")(x, d)
	return formatString, false, err
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 857)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 860)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 858)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.MaxBlobWidth

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 861)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 859)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinFromBase64Sig{bf, maxAllowedPacket}
	return sig, nil
}

// base64NeededDecodedLength return the base64 decoded string length.
func base64NeededDecodedLength(n int) int {
	trace_util_0.Count(_builtin_string_00000, 862)
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 && n > math.MaxInt64/3 {
		trace_util_0.Count(_builtin_string_00000, 865)
		return -1
	}
	trace_util_0.Count(_builtin_string_00000, 863)
	if strconv.IntSize == 32 && n > math.MaxInt32/3 {
		trace_util_0.Count(_builtin_string_00000, 866)
		return -1
	}
	trace_util_0.Count(_builtin_string_00000, 864)
	return n * 3 / 4
}

type builtinFromBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinFromBase64Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 867)
	newSig := &builtinFromBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 868)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 873)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 869)
	needDecodeLen := base64NeededDecodedLength(len(str))
	if needDecodeLen == -1 {
		trace_util_0.Count(_builtin_string_00000, 874)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 870)
	if needDecodeLen > int(b.maxAllowedPacket) {
		trace_util_0.Count(_builtin_string_00000, 875)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("from_base64", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 871)
	str = strings.Replace(str, "\t", "", -1)
	str = strings.Replace(str, " ", "", -1)
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 876)
		// When error happens, take `from_base64("asc")` as an example, we should return NULL.
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 872)
	return string(result), false, nil
}

type toBase64FunctionClass struct {
	baseFunctionClass
}

func (c *toBase64FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 877)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 880)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 878)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = base64NeededEncodedLength(bf.args[0].GetType().Flen)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 881)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 879)
	sig := &builtinToBase64Sig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinToBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinToBase64Sig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 882)
	newSig := &builtinToBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// base64NeededEncodedLength return the base64 encoded string length.
func base64NeededEncodedLength(n int) int {
	trace_util_0.Count(_builtin_string_00000, 883)
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 {
		trace_util_0.Count(_builtin_string_00000, 885)
		// len(arg)            -> len(to_base64(arg))
		// 6827690988321067803 -> 9223372036854775804
		// 6827690988321067804 -> -9223372036854775808
		if n > 6827690988321067803 {
			trace_util_0.Count(_builtin_string_00000, 886)
			return -1
		}
	} else {
		trace_util_0.Count(_builtin_string_00000, 887)
		{
			// len(arg)   -> len(to_base64(arg))
			// 1589695686 -> 2147483645
			// 1589695687 -> -2147483646
			if n > 1589695686 {
				trace_util_0.Count(_builtin_string_00000, 888)
				return -1
			}
		}
	}

	trace_util_0.Count(_builtin_string_00000, 884)
	length := (n + 2) / 3 * 4
	return length + (length-1)/76
}

// evalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	trace_util_0.Count(_builtin_string_00000, 889)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 895)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_string_00000, 890)
	needEncodeLen := base64NeededEncodedLength(len(str))
	if needEncodeLen == -1 {
		trace_util_0.Count(_builtin_string_00000, 896)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 891)
	if needEncodeLen > int(b.maxAllowedPacket) {
		trace_util_0.Count(_builtin_string_00000, 897)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("to_base64", b.maxAllowedPacket))
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 892)
	if b.tp.Flen == -1 || b.tp.Flen > mysql.MaxBlobWidth {
		trace_util_0.Count(_builtin_string_00000, 898)
		return "", true, nil
	}

	//encode
	trace_util_0.Count(_builtin_string_00000, 893)
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		trace_util_0.Count(_builtin_string_00000, 899)
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}

	trace_util_0.Count(_builtin_string_00000, 894)
	return result, false, nil
}

// splitToSubN splits a string every n runes into a string[]
func splitToSubN(s string, n int) []string {
	trace_util_0.Count(_builtin_string_00000, 900)
	subs := make([]string, 0, len(s)/n+1)
	for len(s) > n {
		trace_util_0.Count(_builtin_string_00000, 902)
		subs = append(subs, s[:n])
		s = s[n:]
	}
	trace_util_0.Count(_builtin_string_00000, 901)
	subs = append(subs, s)
	return subs
}

type insertFunctionClass struct {
	baseFunctionClass
}

func (c *insertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_string_00000, 903)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 907)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 904)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETString)
	bf.tp.Flen = mysql.MaxBlobWidth
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	SetBinFlagOrBinStr(args[3].GetType(), bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_builtin_string_00000, 908)
		return nil, err
	}

	trace_util_0.Count(_builtin_string_00000, 905)
	if types.IsBinaryStr(args[0].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 909)
		sig = &builtinInsertBinarySig{bf, maxAllowedPacket}
	} else {
		trace_util_0.Count(_builtin_string_00000, 910)
		{
			sig = &builtinInsertSig{bf, maxAllowedPacket}
		}
	}
	trace_util_0.Count(_builtin_string_00000, 906)
	return sig, nil
}

type builtinInsertBinarySig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 911)
	newSig := &builtinInsertBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertBinarySig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 912)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 920)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 913)
	strLength := int64(len(str))

	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 921)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 914)
	if pos < 1 || pos > strLength {
		trace_util_0.Count(_builtin_string_00000, 922)
		return str, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 915)
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 923)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 916)
	newstr, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 924)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 917)
	if length > strLength-pos+1 || length < 0 {
		trace_util_0.Count(_builtin_string_00000, 925)
		length = strLength - pos + 1
	}

	trace_util_0.Count(_builtin_string_00000, 918)
	if uint64(strLength-length+int64(len(newstr))) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 926)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("insert", b.maxAllowedPacket))
		return "", true, nil
	}

	trace_util_0.Count(_builtin_string_00000, 919)
	return str[0:pos-1] + newstr + str[pos+length-1:], false, nil
}

type builtinInsertSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 927)
	newSig := &builtinInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 928)
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 936)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 929)
	runes := []rune(str)
	runeLength := int64(len(runes))

	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 937)
		return "", true, err
	}
	trace_util_0.Count(_builtin_string_00000, 930)
	if pos < 1 || pos > runeLength {
		trace_util_0.Count(_builtin_string_00000, 938)
		return str, false, nil
	}

	trace_util_0.Count(_builtin_string_00000, 931)
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 939)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 932)
	newstr, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 940)
		return "", true, err
	}

	trace_util_0.Count(_builtin_string_00000, 933)
	if length > runeLength-pos+1 || length < 0 {
		trace_util_0.Count(_builtin_string_00000, 941)
		length = runeLength - pos + 1
	}

	trace_util_0.Count(_builtin_string_00000, 934)
	strHead := string(runes[0 : pos-1])
	strTail := string(runes[pos+length-1:])
	if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
		trace_util_0.Count(_builtin_string_00000, 942)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("insert", b.maxAllowedPacket))
		return "", true, nil
	}
	trace_util_0.Count(_builtin_string_00000, 935)
	return strHead + newstr + strTail, false, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 943)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_string_00000, 946)
		return nil, err
	}
	trace_util_0.Count(_builtin_string_00000, 944)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 11
	if types.IsBinaryStr(bf.args[0].GetType()) || types.IsBinaryStr(bf.args[1].GetType()) {
		trace_util_0.Count(_builtin_string_00000, 947)
		sig := &builtinInstrBinarySig{bf}
		return sig, nil
	}
	trace_util_0.Count(_builtin_string_00000, 945)
	sig := &builtinInstrSig{bf}
	return sig, nil
}

type builtinInstrSig struct{ baseBuiltinFunc }

func (b *builtinInstrSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 948)
	newSig := &builtinInstrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinInstrBinarySig struct{ baseBuiltinFunc }

func (b *builtinInstrBinarySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_string_00000, 949)
	newSig := &builtinInstrBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals INSTR(str,substr), case insensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 950)
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 954)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_string_00000, 951)
	str = strings.ToLower(str)

	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 955)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_string_00000, 952)
	substr = strings.ToLower(substr)

	idx := strings.Index(str, substr)
	if idx == -1 {
		trace_util_0.Count(_builtin_string_00000, 956)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 953)
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_string_00000, 957)
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 961)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_string_00000, 958)
	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		trace_util_0.Count(_builtin_string_00000, 962)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_string_00000, 959)
	idx := strings.Index(str, substr)
	if idx == -1 {
		trace_util_0.Count(_builtin_string_00000, 963)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_string_00000, 960)
	return int64(idx + 1), false, nil
}

type loadFileFunctionClass struct {
	baseFunctionClass
}

func (c *loadFileFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_string_00000, 964)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "load_file")
}

var _builtin_string_00000 = "expression/builtin_string.go"
