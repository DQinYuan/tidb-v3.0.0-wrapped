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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/printer"
)

var (
	_ functionClass = &databaseFunctionClass{}
	_ functionClass = &foundRowsFunctionClass{}
	_ functionClass = &currentUserFunctionClass{}
	_ functionClass = &currentRoleFunctionClass{}
	_ functionClass = &userFunctionClass{}
	_ functionClass = &connectionIDFunctionClass{}
	_ functionClass = &lastInsertIDFunctionClass{}
	_ functionClass = &versionFunctionClass{}
	_ functionClass = &benchmarkFunctionClass{}
	_ functionClass = &charsetFunctionClass{}
	_ functionClass = &coercibilityFunctionClass{}
	_ functionClass = &collationFunctionClass{}
	_ functionClass = &rowCountFunctionClass{}
	_ functionClass = &tidbVersionFunctionClass{}
	_ functionClass = &tidbIsDDLOwnerFunctionClass{}
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundRowsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinConnectionIDSig{}
	_ builtinFunc = &builtinLastInsertIDSig{}
	_ builtinFunc = &builtinLastInsertIDWithIDSig{}
	_ builtinFunc = &builtinVersionSig{}
	_ builtinFunc = &builtinTiDBVersionSig{}
	_ builtinFunc = &builtinRowCountSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 0)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 1)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &builtinDatabaseSig{bf}
	return sig, nil
}

type builtinDatabaseSig struct {
	baseBuiltinFunc
}

func (b *builtinDatabaseSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 3)
	newSig := &builtinDatabaseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 4)
	currentDB := b.ctx.GetSessionVars().CurrentDB
	return currentDB, currentDB == "", nil
}

type foundRowsFunctionClass struct {
	baseFunctionClass
}

func (c *foundRowsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 5)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 7)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 6)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinFoundRowsSig{bf}
	return sig, nil
}

type builtinFoundRowsSig struct {
	baseBuiltinFunc
}

func (b *builtinFoundRowsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 8)
	newSig := &builtinFoundRowsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFoundRowsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_found-rows
// TODO: SQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundRowsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 9)
	data := b.ctx.GetSessionVars()
	if data == nil {
		trace_util_0.Count(_builtin_info_00000, 11)
		return 0, true, errors.Errorf("Missing session variable when eval builtin")
	}
	trace_util_0.Count(_builtin_info_00000, 10)
	return int64(data.LastFoundRows), false, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 12)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 14)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 13)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &builtinCurrentUserSig{bf}
	return sig, nil
}

type builtinCurrentUserSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentUserSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 15)
	newSig := &builtinCurrentUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 16)
	data := b.ctx.GetSessionVars()
	if data == nil || data.User == nil {
		trace_util_0.Count(_builtin_info_00000, 18)
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	trace_util_0.Count(_builtin_info_00000, 17)
	return data.User.AuthIdentityString(), false, nil
}

type currentRoleFunctionClass struct {
	baseFunctionClass
}

func (c *currentRoleFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 19)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 21)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 20)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &builtinCurrentRoleSig{bf}
	return sig, nil
}

type builtinCurrentRoleSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentRoleSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 22)
	newSig := &builtinCurrentRoleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentRoleSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 23)
	data := b.ctx.GetSessionVars()
	if data == nil || data.ActiveRoles == nil {
		trace_util_0.Count(_builtin_info_00000, 27)
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	trace_util_0.Count(_builtin_info_00000, 24)
	if len(data.ActiveRoles) == 0 {
		trace_util_0.Count(_builtin_info_00000, 28)
		return "", false, nil
	}
	trace_util_0.Count(_builtin_info_00000, 25)
	res := ""
	for i, r := range data.ActiveRoles {
		trace_util_0.Count(_builtin_info_00000, 29)
		res += r.String()
		if i != len(data.ActiveRoles)-1 {
			trace_util_0.Count(_builtin_info_00000, 30)
			res += ","
		}
	}
	trace_util_0.Count(_builtin_info_00000, 26)
	return res, false, nil
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 31)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 33)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 32)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &builtinUserSig{bf}
	return sig, nil
}

type builtinUserSig struct {
	baseBuiltinFunc
}

func (b *builtinUserSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 34)
	newSig := &builtinUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 35)
	data := b.ctx.GetSessionVars()
	if data == nil || data.User == nil {
		trace_util_0.Count(_builtin_info_00000, 37)
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}

	trace_util_0.Count(_builtin_info_00000, 36)
	return data.User.String(), false, nil
}

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 38)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 40)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 39)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinConnectionIDSig{bf}
	return sig, nil
}

type builtinConnectionIDSig struct {
	baseBuiltinFunc
}

func (b *builtinConnectionIDSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 41)
	newSig := &builtinConnectionIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinConnectionIDSig) evalInt(_ chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 42)
	data := b.ctx.GetSessionVars()
	if data == nil {
		trace_util_0.Count(_builtin_info_00000, 44)
		return 0, true, errors.Errorf("Missing session variable when evalue builtin")
	}
	trace_util_0.Count(_builtin_info_00000, 43)
	return int64(data.ConnectionID), false, nil
}

type lastInsertIDFunctionClass struct {
	baseFunctionClass
}

func (c *lastInsertIDFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_info_00000, 45)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 49)
		return nil, err
	}

	trace_util_0.Count(_builtin_info_00000, 46)
	var argsTp []types.EvalType
	if len(args) == 1 {
		trace_util_0.Count(_builtin_info_00000, 50)
		argsTp = append(argsTp, types.ETInt)
	}
	trace_util_0.Count(_builtin_info_00000, 47)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argsTp...)
	bf.tp.Flag |= mysql.UnsignedFlag

	if len(args) == 1 {
		trace_util_0.Count(_builtin_info_00000, 51)
		sig = &builtinLastInsertIDWithIDSig{bf}
	} else {
		trace_util_0.Count(_builtin_info_00000, 52)
		{
			sig = &builtinLastInsertIDSig{bf}
		}
	}
	trace_util_0.Count(_builtin_info_00000, 48)
	return sig, err
}

type builtinLastInsertIDSig struct {
	baseBuiltinFunc
}

func (b *builtinLastInsertIDSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 53)
	newSig := &builtinLastInsertIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_info_00000, 54)
	res = int64(b.ctx.GetSessionVars().StmtCtx.PrevLastInsertID)
	return res, false, nil
}

type builtinLastInsertIDWithIDSig struct {
	baseBuiltinFunc
}

func (b *builtinLastInsertIDWithIDSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 55)
	newSig := &builtinLastInsertIDWithIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDWithIDSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_info_00000, 56)
	res, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_info_00000, 58)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_info_00000, 57)
	b.ctx.GetSessionVars().SetLastInsertID(uint64(res))
	return res, false, nil
}

type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 59)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 61)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 60)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &builtinVersionSig{bf}
	return sig, nil
}

type builtinVersionSig struct {
	baseBuiltinFunc
}

func (b *builtinVersionSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 62)
	newSig := &builtinVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinVersionSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 63)
	return mysql.ServerVersion, false, nil
}

type tidbVersionFunctionClass struct {
	baseFunctionClass
}

func (c *tidbVersionFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 64)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 66)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 65)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = len(printer.GetTiDBInfo())
	sig := &builtinTiDBVersionSig{bf}
	return sig, nil
}

type builtinTiDBVersionSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBVersionSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 67)
	newSig := &builtinTiDBVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBVersionSig.
// This will show git hash and build time for tidb-server.
func (b *builtinTiDBVersionSig) evalString(_ chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 68)
	return printer.GetTiDBInfo(), false, nil
}

type tidbIsDDLOwnerFunctionClass struct {
	baseFunctionClass
}

func (c *tidbIsDDLOwnerFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 69)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 71)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 70)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	sig := &builtinTiDBIsDDLOwnerSig{bf}
	return sig, nil
}

type builtinTiDBIsDDLOwnerSig struct {
	baseBuiltinFunc
}

func (b *builtinTiDBIsDDLOwnerSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 72)
	newSig := &builtinTiDBIsDDLOwnerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTiDBIsDDLOwnerSig.
func (b *builtinTiDBIsDDLOwnerSig) evalInt(_ chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_info_00000, 73)
	ddlOwnerChecker := b.ctx.DDLOwnerChecker()
	if ddlOwnerChecker.IsOwner() {
		trace_util_0.Count(_builtin_info_00000, 75)
		res = 1
	}

	trace_util_0.Count(_builtin_info_00000, 74)
	return res, false, nil
}

type benchmarkFunctionClass struct {
	baseFunctionClass
}

func (c *benchmarkFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 76)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 78)
		return nil, err
	}

	// Syntax: BENCHMARK(loop_count, expression)
	// Define with same eval type of input arg to avoid unnecessary cast function.
	trace_util_0.Count(_builtin_info_00000, 77)
	sameEvalType := args[1].GetType().EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, sameEvalType)
	sig := &builtinBenchmarkSig{bf}
	return sig, nil
}

type builtinBenchmarkSig struct {
	baseBuiltinFunc
}

func (b *builtinBenchmarkSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 79)
	newSig := &builtinBenchmarkSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinBenchmarkSig. It will execute expression repeatedly count times.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
func (b *builtinBenchmarkSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_info_00000, 80)
	// Get loop count.
	loopCount, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_info_00000, 84)
		return 0, isNull, err
	}

	// BENCHMARK() will return NULL if loop count < 0,
	// behavior observed on MySQL 5.7.24.
	trace_util_0.Count(_builtin_info_00000, 81)
	if loopCount < 0 {
		trace_util_0.Count(_builtin_info_00000, 85)
		return 0, true, nil
	}

	// Eval loop count times based on arg type.
	// BENCHMARK() will pass-through the eval error,
	// behavior observed on MySQL 5.7.24.
	trace_util_0.Count(_builtin_info_00000, 82)
	var i int64
	arg, ctx := b.args[1], b.ctx
	switch evalType := arg.GetType().EvalType(); evalType {
	case types.ETInt:
		trace_util_0.Count(_builtin_info_00000, 86)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 94)
			_, isNull, err = arg.EvalInt(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 95)
				return 0, isNull, err
			}
		}
	case types.ETReal:
		trace_util_0.Count(_builtin_info_00000, 87)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 96)
			_, isNull, err = arg.EvalReal(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 97)
				return 0, isNull, err
			}
		}
	case types.ETDecimal:
		trace_util_0.Count(_builtin_info_00000, 88)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 98)
			_, isNull, err = arg.EvalDecimal(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 99)
				return 0, isNull, err
			}
		}
	case types.ETString:
		trace_util_0.Count(_builtin_info_00000, 89)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 100)
			_, isNull, err = arg.EvalString(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 101)
				return 0, isNull, err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_info_00000, 90)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 102)
			_, isNull, err = arg.EvalTime(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 103)
				return 0, isNull, err
			}
		}
	case types.ETDuration:
		trace_util_0.Count(_builtin_info_00000, 91)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 104)
			_, isNull, err = arg.EvalDuration(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 105)
				return 0, isNull, err
			}
		}
	case types.ETJson:
		trace_util_0.Count(_builtin_info_00000, 92)
		for ; i < loopCount; i++ {
			trace_util_0.Count(_builtin_info_00000, 106)
			_, isNull, err = arg.EvalJSON(ctx, row)
			if err != nil {
				trace_util_0.Count(_builtin_info_00000, 107)
				return 0, isNull, err
			}
		}
	default:
		trace_util_0.Count(_builtin_info_00000, 93) // Should never go into here.
		return 0, true, errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	trace_util_0.Count(_builtin_info_00000, 83)
	return 0, false, nil
}

type charsetFunctionClass struct {
	baseFunctionClass
}

func (c *charsetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 108)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "CHARSET")
}

type coercibilityFunctionClass struct {
	baseFunctionClass
}

func (c *coercibilityFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 109)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "COERCIBILITY")
}

type collationFunctionClass struct {
	baseFunctionClass
}

func (c *collationFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_info_00000, 110)
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "COLLATION")
}

type rowCountFunctionClass struct {
	baseFunctionClass
}

func (c *rowCountFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_info_00000, 111)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_info_00000, 113)
		return nil, err
	}
	trace_util_0.Count(_builtin_info_00000, 112)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	sig = &builtinRowCountSig{bf}
	return sig, nil
}

type builtinRowCountSig struct {
	baseBuiltinFunc
}

func (b *builtinRowCountSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_info_00000, 114)
	newSig := &builtinRowCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count.
func (b *builtinRowCountSig) evalInt(_ chunk.Row) (res int64, isNull bool, err error) {
	trace_util_0.Count(_builtin_info_00000, 115)
	res = int64(b.ctx.GetSessionVars().StmtCtx.PrevAffectedRows)
	return res, false, nil
}

var _builtin_info_00000 = "expression/builtin_info.go"
