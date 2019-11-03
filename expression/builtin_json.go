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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &jsonTypeFunctionClass{}
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonRemoveFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
	_ functionClass = &jsonObjectFunctionClass{}
	_ functionClass = &jsonArrayFunctionClass{}
	_ functionClass = &jsonContainsFunctionClass{}
	_ functionClass = &jsonContainsPathFunctionClass{}
	_ functionClass = &jsonValidFunctionClass{}
	_ functionClass = &jsonArrayAppendFunctionClass{}
	_ functionClass = &jsonArrayInsertFunctionClass{}
	_ functionClass = &jsonMergePatchFunctionClass{}
	_ functionClass = &jsonMergePreserveFunctionClass{}
	_ functionClass = &jsonPrettyFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSearchFunctionClass{}
	_ functionClass = &jsonStorageSizeFunctionClass{}
	_ functionClass = &jsonDepthFunctionClass{}
	_ functionClass = &jsonKeysFunctionClass{}
	_ functionClass = &jsonLengthFunctionClass{}

	_ builtinFunc = &builtinJSONTypeSig{}
	_ builtinFunc = &builtinJSONQuoteSig{}
	_ builtinFunc = &builtinJSONUnquoteSig{}
	_ builtinFunc = &builtinJSONArraySig{}
	_ builtinFunc = &builtinJSONArrayAppendSig{}
	_ builtinFunc = &builtinJSONObjectSig{}
	_ builtinFunc = &builtinJSONExtractSig{}
	_ builtinFunc = &builtinJSONSetSig{}
	_ builtinFunc = &builtinJSONInsertSig{}
	_ builtinFunc = &builtinJSONReplaceSig{}
	_ builtinFunc = &builtinJSONRemoveSig{}
	_ builtinFunc = &builtinJSONMergeSig{}
	_ builtinFunc = &builtinJSONContainsSig{}
	_ builtinFunc = &builtinJSONDepthSig{}
	_ builtinFunc = &builtinJSONSearchSig{}
	_ builtinFunc = &builtinJSONKeysSig{}
	_ builtinFunc = &builtinJSONKeys2ArgsSig{}
	_ builtinFunc = &builtinJSONLengthSig{}
)

type jsonTypeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONTypeSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONTypeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 0)
	newSig := &builtinJSONTypeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonTypeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 1)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 3)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 2)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	bf.tp.Flen = 51 // Flen of JSON_TYPE is length of UNSIGNED INTEGER.
	sig := &builtinJSONTypeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig, nil
}

func (b *builtinJSONTypeSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 4)
	var j json.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 6)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 5)
	return j.Type(), false, nil
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONExtractSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 7)
	newSig := &builtinJSONExtractSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonExtractFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 8)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 11)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 9)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		trace_util_0.Count(_builtin_json_00000, 12)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 10)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONExtractSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinJSONExtractSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 13)
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 17)
		return
	}
	trace_util_0.Count(_builtin_json_00000, 14)
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_json_00000, 18)
		var s string
		s, isNull, err = arg.EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 21)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 19)
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 22)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 20)
		pathExprs = append(pathExprs, pathExpr)
	}
	trace_util_0.Count(_builtin_json_00000, 15)
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		trace_util_0.Count(_builtin_json_00000, 23)
		return res, true, nil
	}
	trace_util_0.Count(_builtin_json_00000, 16)
	return res, false, nil
}

type jsonUnquoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONUnquoteSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONUnquoteSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 24)
	newSig := &builtinJSONUnquoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonUnquoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 25)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 27)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 26)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONUnquoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig, nil
}

func (b *builtinJSONUnquoteSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 28)
	var j json.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 30)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 29)
	res, err = j.Unquote()
	return res, err != nil, err
}

type jsonSetFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSetSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONSetSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 31)
	newSig := &builtinJSONSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 32)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 37)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 33)
	if len(args)&1 != 1 {
		trace_util_0.Count(_builtin_json_00000, 38)
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	trace_util_0.Count(_builtin_json_00000, 34)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 39)
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 35)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 40)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 36)
	sig := &builtinJSONSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig, nil
}

func (b *builtinJSONSetSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 41)
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifySet)
	return res, isNull, err
}

type jsonInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONInsertSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONInsertSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 42)
	newSig := &builtinJSONInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonInsertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 43)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 48)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 44)
	if len(args)&1 != 1 {
		trace_util_0.Count(_builtin_json_00000, 49)
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	trace_util_0.Count(_builtin_json_00000, 45)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 50)
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 46)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 51)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 47)
	sig := &builtinJSONInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig, nil
}

func (b *builtinJSONInsertSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 52)
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifyInsert)
	return res, isNull, err
}

type jsonReplaceFunctionClass struct {
	baseFunctionClass
}

type builtinJSONReplaceSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONReplaceSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 53)
	newSig := &builtinJSONReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonReplaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 54)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 59)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 55)
	if len(args)&1 != 1 {
		trace_util_0.Count(_builtin_json_00000, 60)
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	trace_util_0.Count(_builtin_json_00000, 56)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 61)
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 57)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 62)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 58)
	sig := &builtinJSONReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig, nil
}

func (b *builtinJSONReplaceSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 63)
	res, isNull, err = jsonModify(b.ctx, b.args, row, json.ModifyReplace)
	return res, isNull, err
}

type jsonRemoveFunctionClass struct {
	baseFunctionClass
}

type builtinJSONRemoveSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONRemoveSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 64)
	newSig := &builtinJSONRemoveSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonRemoveFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 65)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 68)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 66)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		trace_util_0.Count(_builtin_json_00000, 69)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 67)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONRemoveSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig, nil
}

func (b *builtinJSONRemoveSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 70)
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 74)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 71)
	pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		trace_util_0.Count(_builtin_json_00000, 75)
		var s string
		s, isNull, err = arg.EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 78)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 76)
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 79)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 77)
		pathExprs = append(pathExprs, pathExpr)
	}
	trace_util_0.Count(_builtin_json_00000, 72)
	res, err = res.Remove(pathExprs)
	if err != nil {
		trace_util_0.Count(_builtin_json_00000, 80)
		return res, true, err
	}
	trace_util_0.Count(_builtin_json_00000, 73)
	return res, false, nil
}

type jsonMergeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONMergeSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONMergeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 81)
	newSig := &builtinJSONMergeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonMergeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 82)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 85)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 83)
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_json_00000, 86)
		argTps = append(argTps, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 84)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig, nil
}

func (b *builtinJSONMergeSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 87)
	values := make([]json.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		trace_util_0.Count(_builtin_json_00000, 90)
		var value json.BinaryJSON
		value, isNull, err = arg.EvalJSON(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 92)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 91)
		values = append(values, value)
	}
	trace_util_0.Count(_builtin_json_00000, 88)
	res = json.MergeBinary(values)
	// function "JSON_MERGE" is deprecated since MySQL 5.7.22. Synonym for function "JSON_MERGE_PRESERVE".
	// See https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		trace_util_0.Count(_builtin_json_00000, 93)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("JSON_MERGE"))
	}
	trace_util_0.Count(_builtin_json_00000, 89)
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	baseFunctionClass
}

type builtinJSONObjectSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONObjectSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 94)
	newSig := &builtinJSONObjectSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonObjectFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 95)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 100)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 96)
	if len(args)&1 != 0 {
		trace_util_0.Count(_builtin_json_00000, 101)
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	trace_util_0.Count(_builtin_json_00000, 97)
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 102)
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 98)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 1; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 103)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 99)
	sig := &builtinJSONObjectSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig, nil
}

func (b *builtinJSONObjectSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 104)
	if len(b.args)&1 == 1 {
		trace_util_0.Count(_builtin_json_00000, 107)
		err = ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return res, true, err
	}
	trace_util_0.Count(_builtin_json_00000, 105)
	jsons := make(map[string]interface{}, len(b.args)>>1)
	var key string
	var value json.BinaryJSON
	for i, arg := range b.args {
		trace_util_0.Count(_builtin_json_00000, 108)
		if i&1 == 0 {
			trace_util_0.Count(_builtin_json_00000, 109)
			key, isNull, err = arg.EvalString(b.ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_json_00000, 111)
				return res, true, err
			}
			trace_util_0.Count(_builtin_json_00000, 110)
			if isNull {
				trace_util_0.Count(_builtin_json_00000, 112)
				err = errors.New("JSON documents may not contain NULL member names")
				return res, true, err
			}
		} else {
			trace_util_0.Count(_builtin_json_00000, 113)
			{
				value, isNull, err = arg.EvalJSON(b.ctx, row)
				if err != nil {
					trace_util_0.Count(_builtin_json_00000, 116)
					return res, true, err
				}
				trace_util_0.Count(_builtin_json_00000, 114)
				if isNull {
					trace_util_0.Count(_builtin_json_00000, 117)
					value = json.CreateBinary(nil)
				}
				trace_util_0.Count(_builtin_json_00000, 115)
				jsons[key] = value
			}
		}
	}
	trace_util_0.Count(_builtin_json_00000, 106)
	return json.CreateBinary(jsons), false, nil
}

type jsonArrayFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArraySig struct {
	baseBuiltinFunc
}

func (b *builtinJSONArraySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 118)
	newSig := &builtinJSONArraySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonArrayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 119)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 123)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 120)
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_json_00000, 124)
		argTps = append(argTps, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 121)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := range args {
		trace_util_0.Count(_builtin_json_00000, 125)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 122)
	sig := &builtinJSONArraySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig, nil
}

func (b *builtinJSONArraySig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 126)
	jsons := make([]interface{}, 0, len(b.args))
	for _, arg := range b.args {
		trace_util_0.Count(_builtin_json_00000, 128)
		j, isNull, err := arg.EvalJSON(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 131)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 129)
		if isNull {
			trace_util_0.Count(_builtin_json_00000, 132)
			j = json.CreateBinary(nil)
		}
		trace_util_0.Count(_builtin_json_00000, 130)
		jsons = append(jsons, j)
	}
	trace_util_0.Count(_builtin_json_00000, 127)
	return json.CreateBinary(jsons), false, nil
}

type jsonContainsPathFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsPathSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONContainsPathSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 133)
	newSig := &builtinJSONContainsPathSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsPathFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 134)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 137)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 135)
	argTps := []types.EvalType{types.ETJson, types.ETString}
	for i := 3; i <= len(args); i++ {
		trace_util_0.Count(_builtin_json_00000, 138)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 136)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsPathSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsPathSig)
	return sig, nil
}

func (b *builtinJSONContainsPathSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 139)
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 144)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 140)
	containType, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 145)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 141)
	containType = strings.ToLower(containType)
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		trace_util_0.Count(_builtin_json_00000, 146)
		return res, true, json.ErrInvalidJSONContainsPathType
	}
	trace_util_0.Count(_builtin_json_00000, 142)
	var pathExpr json.PathExpression
	contains := int64(1)
	for i := 2; i < len(b.args); i++ {
		trace_util_0.Count(_builtin_json_00000, 147)
		path, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 150)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 148)
		if pathExpr, err = json.ParseJSONPathExpr(path); err != nil {
			trace_util_0.Count(_builtin_json_00000, 151)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 149)
		_, exists := obj.Extract([]json.PathExpression{pathExpr})
		switch {
		case exists && containType == json.ContainsPathOne:
			trace_util_0.Count(_builtin_json_00000, 152)
			return 1, false, nil
		case !exists && containType == json.ContainsPathOne:
			trace_util_0.Count(_builtin_json_00000, 153)
			contains = 0
		case !exists && containType == json.ContainsPathAll:
			trace_util_0.Count(_builtin_json_00000, 154)
			return 0, false, nil
		}
	}
	trace_util_0.Count(_builtin_json_00000, 143)
	return contains, false, nil
}

func jsonModify(ctx sessionctx.Context, args []Expression, row chunk.Row, mt json.ModifyType) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 155)
	res, isNull, err = args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 160)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 156)
	pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 161)
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 164)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 162)
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 165)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 163)
		pathExprs = append(pathExprs, pathExpr)
	}
	trace_util_0.Count(_builtin_json_00000, 157)
	values := make([]json.BinaryJSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 166)
		var value json.BinaryJSON
		value, isNull, err = args[i].EvalJSON(ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 169)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 167)
		if isNull {
			trace_util_0.Count(_builtin_json_00000, 170)
			value = json.CreateBinary(nil)
		}
		trace_util_0.Count(_builtin_json_00000, 168)
		values = append(values, value)
	}
	trace_util_0.Count(_builtin_json_00000, 158)
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		trace_util_0.Count(_builtin_json_00000, 171)
		return res, true, err
	}
	trace_util_0.Count(_builtin_json_00000, 159)
	return res, false, nil
}

type jsonContainsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONContainsSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONContainsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 172)
	newSig := &builtinJSONContainsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonContainsFunctionClass) verifyArgs(args []Expression) error {
	trace_util_0.Count(_builtin_json_00000, 173)
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 177)
		return err
	}
	trace_util_0.Count(_builtin_json_00000, 174)
	if evalType := args[0].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		trace_util_0.Count(_builtin_json_00000, 178)
		return json.ErrInvalidJSONData.GenWithStackByArgs(1, "json_contains")
	}
	trace_util_0.Count(_builtin_json_00000, 175)
	if evalType := args[1].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		trace_util_0.Count(_builtin_json_00000, 179)
		return json.ErrInvalidJSONData.GenWithStackByArgs(2, "json_contains")
	}
	trace_util_0.Count(_builtin_json_00000, 176)
	return nil
}

func (c *jsonContainsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 180)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 183)
		return nil, err
	}

	trace_util_0.Count(_builtin_json_00000, 181)
	argTps := []types.EvalType{types.ETJson, types.ETJson}
	if len(args) == 3 {
		trace_util_0.Count(_builtin_json_00000, 184)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 182)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonContainsSig)
	return sig, nil
}

func (b *builtinJSONContainsSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 185)
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 190)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 186)
	target, isNull, err := b.args[1].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 191)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 187)
	var pathExpr json.PathExpression
	if len(b.args) == 3 {
		trace_util_0.Count(_builtin_json_00000, 192)
		path, isNull, err := b.args[2].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 196)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 193)
		pathExpr, err = json.ParseJSONPathExpr(path)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 197)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 194)
		if pathExpr.ContainsAnyAsterisk() {
			trace_util_0.Count(_builtin_json_00000, 198)
			return res, true, json.ErrInvalidJSONPathWildcard
		}
		trace_util_0.Count(_builtin_json_00000, 195)
		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			trace_util_0.Count(_builtin_json_00000, 199)
			return res, true, nil
		}
	}

	trace_util_0.Count(_builtin_json_00000, 188)
	if json.ContainsBinary(obj, target) {
		trace_util_0.Count(_builtin_json_00000, 200)
		return 1, false, nil
	}
	trace_util_0.Count(_builtin_json_00000, 189)
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonValidFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 201)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_VALID")
}

type jsonArrayAppendFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	baseBuiltinFunc
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(args []Expression) error {
	trace_util_0.Count(_builtin_json_00000, 202)
	if len(args) < 3 || (len(args)&1 != 1) {
		trace_util_0.Count(_builtin_json_00000, 204)
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	trace_util_0.Count(_builtin_json_00000, 203)
	return nil
}

func (c *jsonArrayAppendFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 205)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 209)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 206)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 210)
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 207)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		trace_util_0.Count(_builtin_json_00000, 211)
		DisableParseJSONFlag4Expr(args[i])
	}
	trace_util_0.Count(_builtin_json_00000, 208)
	sig := &builtinJSONArrayAppendSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayAppendSig)
	return sig, nil
}

func (b *builtinJSONArrayAppendSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 212)
	newSig := &builtinJSONArrayAppendSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayAppendSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 213)
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_json_00000, 216)
		return res, true, err
	}

	trace_util_0.Count(_builtin_json_00000, 214)
	for i := 1; i < len(b.args)-1; i += 2 {
		trace_util_0.Count(_builtin_json_00000, 217)
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 225)
			return res, true, err
		}

		// We should do the following checks to get correct values in res.Extract
		trace_util_0.Count(_builtin_json_00000, 218)
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 226)
			return res, true, json.ErrInvalidJSONPath.GenWithStackByArgs(s)
		}
		trace_util_0.Count(_builtin_json_00000, 219)
		if pathExpr.ContainsAnyAsterisk() {
			trace_util_0.Count(_builtin_json_00000, 227)
			return res, true, json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(s)
		}

		trace_util_0.Count(_builtin_json_00000, 220)
		obj, exists := res.Extract([]json.PathExpression{pathExpr})
		if !exists {
			trace_util_0.Count(_builtin_json_00000, 228)
			// If path not exists, just do nothing and no errors.
			continue
		}

		trace_util_0.Count(_builtin_json_00000, 221)
		if obj.TypeCode != json.TypeCodeArray {
			trace_util_0.Count(_builtin_json_00000, 229)
			// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
			// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
			// We should wrap them to a single array first.
			obj = json.CreateBinary([]interface{}{obj})
		}

		trace_util_0.Count(_builtin_json_00000, 222)
		value, isnull, err := b.args[i+1].EvalJSON(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 230)
			return res, true, err
		}

		trace_util_0.Count(_builtin_json_00000, 223)
		if isnull {
			trace_util_0.Count(_builtin_json_00000, 231)
			value = json.CreateBinary(nil)
		}

		trace_util_0.Count(_builtin_json_00000, 224)
		obj = json.MergeBinary([]json.BinaryJSON{obj, value})
		res, err = res.Modify([]json.PathExpression{pathExpr}, []json.BinaryJSON{obj}, json.ModifySet)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 232)
			// We checked pathExpr in the same way as res.Modify do.
			// So err should always be nil, the function should never return here.
			return res, true, err
		}
	}
	trace_util_0.Count(_builtin_json_00000, 215)
	return res, false, nil
}

type jsonArrayInsertFunctionClass struct {
	baseFunctionClass
}

func (c *jsonArrayInsertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 233)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_ARRAY_INSERT")
}

type jsonMergePatchFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePatchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 234)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_MERGE_PATCH")
}

type jsonMergePreserveFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 235)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 238)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 236)
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		trace_util_0.Count(_builtin_json_00000, 239)
		argTps = append(argTps, types.ETJson)
	}
	trace_util_0.Count(_builtin_json_00000, 237)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

type jsonPrettyFunctionClass struct {
	baseFunctionClass
}

func (c *jsonPrettyFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 240)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_PRETTY")
}

type jsonQuoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONQuoteSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONQuoteSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 241)
	newSig := &builtinJSONQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonQuoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 242)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 244)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 243)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonQuoteSig)
	return sig, nil
}

func (b *builtinJSONQuoteSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 245)
	var j json.BinaryJSON
	j, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 247)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 246)
	return j.Quote(), false, nil
}

type jsonSearchFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSearchSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONSearchSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 248)
	newSig := &builtinJSONSearchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSearchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 249)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 252)
		return nil, err
	}
	// json_doc, one_or_all, search_str[, escape_char[, path] ...])
	trace_util_0.Count(_builtin_json_00000, 250)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		trace_util_0.Count(_builtin_json_00000, 253)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 251)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONSearchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSearchSig)
	return sig, nil
}

func (b *builtinJSONSearchSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 254)
	// json_doc
	var obj json.BinaryJSON
	obj, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 262)
		return res, isNull, err
	}

	// one_or_all
	trace_util_0.Count(_builtin_json_00000, 255)
	var containType string
	containType, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 263)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 256)
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		trace_util_0.Count(_builtin_json_00000, 264)
		return res, true, errors.AddStack(json.ErrInvalidJSONContainsPathType)
	}

	// search_str & escape_char
	trace_util_0.Count(_builtin_json_00000, 257)
	var searchStr string
	searchStr, isNull, err = b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 265)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 258)
	escape := byte('\\')
	if len(b.args) >= 4 {
		trace_util_0.Count(_builtin_json_00000, 266)
		var escapeStr string
		escapeStr, isNull, err = b.args[3].EvalString(b.ctx, row)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 268)
			return res, isNull, err
		}
		trace_util_0.Count(_builtin_json_00000, 267)
		if isNull || len(escapeStr) == 0 {
			trace_util_0.Count(_builtin_json_00000, 269)
			escape = byte('\\')
		} else {
			trace_util_0.Count(_builtin_json_00000, 270)
			if len(escapeStr) == 1 {
				trace_util_0.Count(_builtin_json_00000, 271)
				escape = byte(escapeStr[0])
			} else {
				trace_util_0.Count(_builtin_json_00000, 272)
				{
					return res, true, errIncorrectArgs.GenWithStackByArgs("ESCAPE")
				}
			}
		}
	}
	trace_util_0.Count(_builtin_json_00000, 259)
	patChars, patTypes := stringutil.CompilePattern(searchStr, escape)

	// result
	result := make([]interface{}, 0)

	// walk json_doc
	walkFn := func(fullpath json.PathExpression, bj json.BinaryJSON) (stop bool, err error) {
		trace_util_0.Count(_builtin_json_00000, 273)
		if bj.TypeCode == json.TypeCodeString && stringutil.DoMatch(string(bj.GetString()), patChars, patTypes) {
			trace_util_0.Count(_builtin_json_00000, 275)
			result = append(result, fullpath.String())
			if containType == json.ContainsPathOne {
				trace_util_0.Count(_builtin_json_00000, 276)
				return true, nil
			}
		}
		trace_util_0.Count(_builtin_json_00000, 274)
		return false, nil
	}
	trace_util_0.Count(_builtin_json_00000, 260)
	if len(b.args) >= 5 {
		trace_util_0.Count(_builtin_json_00000, 277) // path...
		pathExprs := make([]json.PathExpression, 0, len(b.args)-4)
		for i := 4; i < len(b.args); i++ {
			trace_util_0.Count(_builtin_json_00000, 279)
			var s string
			s, isNull, err = b.args[i].EvalString(b.ctx, row)
			if isNull || err != nil {
				trace_util_0.Count(_builtin_json_00000, 282)
				return res, isNull, err
			}
			trace_util_0.Count(_builtin_json_00000, 280)
			var pathExpr json.PathExpression
			pathExpr, err = json.ParseJSONPathExpr(s)
			if err != nil {
				trace_util_0.Count(_builtin_json_00000, 283)
				return res, true, err
			}
			trace_util_0.Count(_builtin_json_00000, 281)
			pathExprs = append(pathExprs, pathExpr)
		}
		trace_util_0.Count(_builtin_json_00000, 278)
		err = obj.Walk(walkFn, pathExprs...)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 284)
			return res, true, err
		}
	} else {
		trace_util_0.Count(_builtin_json_00000, 285)
		{
			err = obj.Walk(walkFn)
			if err != nil {
				trace_util_0.Count(_builtin_json_00000, 286)
				return res, true, err
			}
		}
	}

	// return
	trace_util_0.Count(_builtin_json_00000, 261)
	switch len(result) {
	case 0:
		trace_util_0.Count(_builtin_json_00000, 287)
		return res, true, nil
	case 1:
		trace_util_0.Count(_builtin_json_00000, 288)
		return json.CreateBinary(result[0]), false, nil
	default:
		trace_util_0.Count(_builtin_json_00000, 289)
		return json.CreateBinary(result), false, nil
	}
}

type jsonStorageSizeFunctionClass struct {
	baseFunctionClass
}

func (c *jsonStorageSizeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 290)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_STORAGE_SIZE")
}

type jsonDepthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONDepthSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONDepthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 291)
	newSig := &builtinJSONDepthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonDepthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 292)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 294)
		return nil, err
	}

	trace_util_0.Count(_builtin_json_00000, 293)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
	sig := &builtinJSONDepthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonDepthSig)
	return sig, nil
}

func (b *builtinJSONDepthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 295)
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 297)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_json_00000, 296)
	return int64(obj.GetElemDepth()), false, nil
}

type jsonKeysFunctionClass struct {
	baseFunctionClass
}

func (c *jsonKeysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 298)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 302)
		return nil, err
	}
	trace_util_0.Count(_builtin_json_00000, 299)
	argTps := []types.EvalType{types.ETJson}
	if len(args) == 2 {
		trace_util_0.Count(_builtin_json_00000, 303)
		argTps = append(argTps, types.ETString)
	}
	trace_util_0.Count(_builtin_json_00000, 300)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	var sig builtinFunc
	switch len(args) {
	case 1:
		trace_util_0.Count(_builtin_json_00000, 304)
		sig = &builtinJSONKeysSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeysSig)
	case 2:
		trace_util_0.Count(_builtin_json_00000, 305)
		sig = &builtinJSONKeys2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeys2ArgsSig)
	}
	trace_util_0.Count(_builtin_json_00000, 301)
	return sig, nil
}

type builtinJSONKeysSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONKeysSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 306)
	newSig := &builtinJSONKeysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeysSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 307)
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 310)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 308)
	if res.TypeCode != json.TypeCodeObject {
		trace_util_0.Count(_builtin_json_00000, 311)
		return res, true, json.ErrInvalidJSONData
	}
	trace_util_0.Count(_builtin_json_00000, 309)
	return res.GetKeys(), false, nil
}

type builtinJSONKeys2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONKeys2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 312)
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 313)
	res, isNull, err = b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 321)
		return res, isNull, err
	}
	trace_util_0.Count(_builtin_json_00000, 314)
	if res.TypeCode != json.TypeCodeObject {
		trace_util_0.Count(_builtin_json_00000, 322)
		return res, true, json.ErrInvalidJSONData
	}

	trace_util_0.Count(_builtin_json_00000, 315)
	path, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 323)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_json_00000, 316)
	pathExpr, err := json.ParseJSONPathExpr(path)
	if err != nil {
		trace_util_0.Count(_builtin_json_00000, 324)
		return res, true, err
	}
	trace_util_0.Count(_builtin_json_00000, 317)
	if pathExpr.ContainsAnyAsterisk() {
		trace_util_0.Count(_builtin_json_00000, 325)
		return res, true, json.ErrInvalidJSONPathWildcard
	}

	trace_util_0.Count(_builtin_json_00000, 318)
	res, exists := res.Extract([]json.PathExpression{pathExpr})
	if !exists {
		trace_util_0.Count(_builtin_json_00000, 326)
		return res, true, nil
	}
	trace_util_0.Count(_builtin_json_00000, 319)
	if res.TypeCode != json.TypeCodeObject {
		trace_util_0.Count(_builtin_json_00000, 327)
		return res, true, nil
	}

	trace_util_0.Count(_builtin_json_00000, 320)
	return res.GetKeys(), false, nil
}

type jsonLengthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONLengthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_json_00000, 328)
	newSig := &builtinJSONLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_json_00000, 329)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_json_00000, 332)
		return nil, err
	}

	trace_util_0.Count(_builtin_json_00000, 330)
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	if len(args) == 2 {
		trace_util_0.Count(_builtin_json_00000, 333)
		argTps = append(argTps, types.ETString)
	}

	trace_util_0.Count(_builtin_json_00000, 331)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonLengthSig)
	return sig, nil
}

func (b *builtinJSONLengthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_json_00000, 334)
	obj, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_json_00000, 338)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_json_00000, 335)
	if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
		trace_util_0.Count(_builtin_json_00000, 339)
		return 1, false, nil
	}

	trace_util_0.Count(_builtin_json_00000, 336)
	if len(b.args) == 2 {
		trace_util_0.Count(_builtin_json_00000, 340)
		path, isNull, err := b.args[1].EvalString(b.ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_json_00000, 345)
			return res, isNull, err
		}

		trace_util_0.Count(_builtin_json_00000, 341)
		pathExpr, err := json.ParseJSONPathExpr(path)
		if err != nil {
			trace_util_0.Count(_builtin_json_00000, 346)
			return res, true, err
		}
		trace_util_0.Count(_builtin_json_00000, 342)
		if pathExpr.ContainsAnyAsterisk() {
			trace_util_0.Count(_builtin_json_00000, 347)
			return res, true, json.ErrInvalidJSONPathWildcard
		}

		trace_util_0.Count(_builtin_json_00000, 343)
		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			trace_util_0.Count(_builtin_json_00000, 348)
			return res, true, nil
		}
		trace_util_0.Count(_builtin_json_00000, 344)
		if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
			trace_util_0.Count(_builtin_json_00000, 349)
			return 1, false, nil
		}
	}
	trace_util_0.Count(_builtin_json_00000, 337)
	return int64(obj.GetElemCount()), false, nil
}

var _builtin_json_00000 = "expression/builtin_json.go"
