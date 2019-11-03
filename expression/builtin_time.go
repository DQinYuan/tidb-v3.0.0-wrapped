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
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const ( // GET_FORMAT first argument.
	dateFormat      = "DATE"
	datetimeFormat  = "DATETIME"
	timestampFormat = "TIMESTAMP"
	timeFormat      = "TIME"
)

const ( // GET_FORMAT location.
	usaLocation      = "USA"
	jisLocation      = "JIS"
	isoLocation      = "ISO"
	eurLocation      = "EUR"
	internalLocation = "INTERNAL"
)

var (
	// durationPattern checks whether a string matchs the format of duration.
	durationPattern = regexp.MustCompile(`^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$`)

	// timestampPattern checks whether a string matchs the format of timestamp.
	timestampPattern = regexp.MustCompile(`^\s*0*\d{1,4}([^\d]0*\d{1,2}){2}\s+(0*\d{0,2}([^\d]0*\d{1,2}){2})?(\.\d*)?\s*$`)

	// datePattern determine whether to match the format of date.
	datePattern = regexp.MustCompile(`^\s*((0*\d{1,4}([^\d]0*\d{1,2}){2})|(\d{2,4}(\d{2}){2}))\s*$`)
)

var (
	_ functionClass = &dateFunctionClass{}
	_ functionClass = &dateLiteralFunctionClass{}
	_ functionClass = &dateDiffFunctionClass{}
	_ functionClass = &timeDiffFunctionClass{}
	_ functionClass = &dateFormatFunctionClass{}
	_ functionClass = &hourFunctionClass{}
	_ functionClass = &minuteFunctionClass{}
	_ functionClass = &secondFunctionClass{}
	_ functionClass = &microSecondFunctionClass{}
	_ functionClass = &monthFunctionClass{}
	_ functionClass = &monthNameFunctionClass{}
	_ functionClass = &nowFunctionClass{}
	_ functionClass = &dayNameFunctionClass{}
	_ functionClass = &dayOfMonthFunctionClass{}
	_ functionClass = &dayOfWeekFunctionClass{}
	_ functionClass = &dayOfYearFunctionClass{}
	_ functionClass = &weekFunctionClass{}
	_ functionClass = &weekDayFunctionClass{}
	_ functionClass = &weekOfYearFunctionClass{}
	_ functionClass = &yearFunctionClass{}
	_ functionClass = &yearWeekFunctionClass{}
	_ functionClass = &fromUnixTimeFunctionClass{}
	_ functionClass = &getFormatFunctionClass{}
	_ functionClass = &strToDateFunctionClass{}
	_ functionClass = &sysDateFunctionClass{}
	_ functionClass = &currentDateFunctionClass{}
	_ functionClass = &currentTimeFunctionClass{}
	_ functionClass = &timeFunctionClass{}
	_ functionClass = &timeLiteralFunctionClass{}
	_ functionClass = &utcDateFunctionClass{}
	_ functionClass = &utcTimestampFunctionClass{}
	_ functionClass = &extractFunctionClass{}
	_ functionClass = &unixTimestampFunctionClass{}
	_ functionClass = &addTimeFunctionClass{}
	_ functionClass = &convertTzFunctionClass{}
	_ functionClass = &makeDateFunctionClass{}
	_ functionClass = &makeTimeFunctionClass{}
	_ functionClass = &periodAddFunctionClass{}
	_ functionClass = &periodDiffFunctionClass{}
	_ functionClass = &quarterFunctionClass{}
	_ functionClass = &secToTimeFunctionClass{}
	_ functionClass = &subTimeFunctionClass{}
	_ functionClass = &timeFormatFunctionClass{}
	_ functionClass = &timeToSecFunctionClass{}
	_ functionClass = &timestampAddFunctionClass{}
	_ functionClass = &toDaysFunctionClass{}
	_ functionClass = &toSecondsFunctionClass{}
	_ functionClass = &utcTimeFunctionClass{}
	_ functionClass = &timestampFunctionClass{}
	_ functionClass = &timestampLiteralFunctionClass{}
	_ functionClass = &lastDayFunctionClass{}
	_ functionClass = &addDateFunctionClass{}
	_ functionClass = &subDateFunctionClass{}
)

var (
	_ builtinFunc = &builtinDateSig{}
	_ builtinFunc = &builtinDateLiteralSig{}
	_ builtinFunc = &builtinDateDiffSig{}
	_ builtinFunc = &builtinNullTimeDiffSig{}
	_ builtinFunc = &builtinTimeStringTimeDiffSig{}
	_ builtinFunc = &builtinDurationStringTimeDiffSig{}
	_ builtinFunc = &builtinDurationDurationTimeDiffSig{}
	_ builtinFunc = &builtinStringTimeTimeDiffSig{}
	_ builtinFunc = &builtinStringDurationTimeDiffSig{}
	_ builtinFunc = &builtinStringStringTimeDiffSig{}
	_ builtinFunc = &builtinTimeTimeTimeDiffSig{}
	_ builtinFunc = &builtinDateFormatSig{}
	_ builtinFunc = &builtinHourSig{}
	_ builtinFunc = &builtinMinuteSig{}
	_ builtinFunc = &builtinSecondSig{}
	_ builtinFunc = &builtinMicroSecondSig{}
	_ builtinFunc = &builtinMonthSig{}
	_ builtinFunc = &builtinMonthNameSig{}
	_ builtinFunc = &builtinNowWithArgSig{}
	_ builtinFunc = &builtinNowWithoutArgSig{}
	_ builtinFunc = &builtinDayNameSig{}
	_ builtinFunc = &builtinDayOfMonthSig{}
	_ builtinFunc = &builtinDayOfWeekSig{}
	_ builtinFunc = &builtinDayOfYearSig{}
	_ builtinFunc = &builtinWeekWithModeSig{}
	_ builtinFunc = &builtinWeekWithoutModeSig{}
	_ builtinFunc = &builtinWeekDaySig{}
	_ builtinFunc = &builtinWeekOfYearSig{}
	_ builtinFunc = &builtinYearSig{}
	_ builtinFunc = &builtinYearWeekWithModeSig{}
	_ builtinFunc = &builtinYearWeekWithoutModeSig{}
	_ builtinFunc = &builtinGetFormatSig{}
	_ builtinFunc = &builtinSysDateWithFspSig{}
	_ builtinFunc = &builtinSysDateWithoutFspSig{}
	_ builtinFunc = &builtinCurrentDateSig{}
	_ builtinFunc = &builtinCurrentTime0ArgSig{}
	_ builtinFunc = &builtinCurrentTime1ArgSig{}
	_ builtinFunc = &builtinTimeSig{}
	_ builtinFunc = &builtinTimeLiteralSig{}
	_ builtinFunc = &builtinUTCDateSig{}
	_ builtinFunc = &builtinUTCTimestampWithArgSig{}
	_ builtinFunc = &builtinUTCTimestampWithoutArgSig{}
	_ builtinFunc = &builtinAddDatetimeAndDurationSig{}
	_ builtinFunc = &builtinAddDatetimeAndStringSig{}
	_ builtinFunc = &builtinAddTimeDateTimeNullSig{}
	_ builtinFunc = &builtinAddStringAndDurationSig{}
	_ builtinFunc = &builtinAddStringAndStringSig{}
	_ builtinFunc = &builtinAddTimeStringNullSig{}
	_ builtinFunc = &builtinAddDurationAndDurationSig{}
	_ builtinFunc = &builtinAddDurationAndStringSig{}
	_ builtinFunc = &builtinAddTimeDurationNullSig{}
	_ builtinFunc = &builtinAddDateAndDurationSig{}
	_ builtinFunc = &builtinAddDateAndStringSig{}
	_ builtinFunc = &builtinSubDatetimeAndDurationSig{}
	_ builtinFunc = &builtinSubDatetimeAndStringSig{}
	_ builtinFunc = &builtinSubTimeDateTimeNullSig{}
	_ builtinFunc = &builtinSubStringAndDurationSig{}
	_ builtinFunc = &builtinSubStringAndStringSig{}
	_ builtinFunc = &builtinSubTimeStringNullSig{}
	_ builtinFunc = &builtinSubDurationAndDurationSig{}
	_ builtinFunc = &builtinSubDurationAndStringSig{}
	_ builtinFunc = &builtinSubTimeDurationNullSig{}
	_ builtinFunc = &builtinSubDateAndDurationSig{}
	_ builtinFunc = &builtinSubDateAndStringSig{}
	_ builtinFunc = &builtinUnixTimestampCurrentSig{}
	_ builtinFunc = &builtinUnixTimestampIntSig{}
	_ builtinFunc = &builtinUnixTimestampDecSig{}
	_ builtinFunc = &builtinConvertTzSig{}
	_ builtinFunc = &builtinMakeDateSig{}
	_ builtinFunc = &builtinMakeTimeSig{}
	_ builtinFunc = &builtinPeriodAddSig{}
	_ builtinFunc = &builtinPeriodDiffSig{}
	_ builtinFunc = &builtinQuarterSig{}
	_ builtinFunc = &builtinSecToTimeSig{}
	_ builtinFunc = &builtinTimeToSecSig{}
	_ builtinFunc = &builtinTimestampAddSig{}
	_ builtinFunc = &builtinToDaysSig{}
	_ builtinFunc = &builtinToSecondsSig{}
	_ builtinFunc = &builtinUTCTimeWithArgSig{}
	_ builtinFunc = &builtinUTCTimeWithoutArgSig{}
	_ builtinFunc = &builtinTimestamp1ArgSig{}
	_ builtinFunc = &builtinTimestamp2ArgsSig{}
	_ builtinFunc = &builtinTimestampLiteralSig{}
	_ builtinFunc = &builtinLastDaySig{}
	_ builtinFunc = &builtinStrToDateDateSig{}
	_ builtinFunc = &builtinStrToDateDatetimeSig{}
	_ builtinFunc = &builtinStrToDateDurationSig{}
	_ builtinFunc = &builtinFromUnixTime1ArgSig{}
	_ builtinFunc = &builtinFromUnixTime2ArgSig{}
	_ builtinFunc = &builtinExtractDatetimeSig{}
	_ builtinFunc = &builtinExtractDurationSig{}
	_ builtinFunc = &builtinAddDateStringStringSig{}
	_ builtinFunc = &builtinAddDateStringIntSig{}
	_ builtinFunc = &builtinAddDateStringRealSig{}
	_ builtinFunc = &builtinAddDateStringDecimalSig{}
	_ builtinFunc = &builtinAddDateIntStringSig{}
	_ builtinFunc = &builtinAddDateIntIntSig{}
	_ builtinFunc = &builtinAddDateIntRealSig{}
	_ builtinFunc = &builtinAddDateIntDecimalSig{}
	_ builtinFunc = &builtinAddDateDatetimeStringSig{}
	_ builtinFunc = &builtinAddDateDatetimeIntSig{}
	_ builtinFunc = &builtinAddDateDatetimeRealSig{}
	_ builtinFunc = &builtinAddDateDatetimeDecimalSig{}
	_ builtinFunc = &builtinSubDateStringStringSig{}
	_ builtinFunc = &builtinSubDateStringIntSig{}
	_ builtinFunc = &builtinSubDateStringRealSig{}
	_ builtinFunc = &builtinSubDateStringDecimalSig{}
	_ builtinFunc = &builtinSubDateIntStringSig{}
	_ builtinFunc = &builtinSubDateIntIntSig{}
	_ builtinFunc = &builtinSubDateIntRealSig{}
	_ builtinFunc = &builtinSubDateIntDecimalSig{}
	_ builtinFunc = &builtinSubDateDatetimeStringSig{}
	_ builtinFunc = &builtinSubDateDatetimeIntSig{}
	_ builtinFunc = &builtinSubDateDatetimeRealSig{}
	_ builtinFunc = &builtinSubDateDatetimeDecimalSig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int, roundMode types.RoundMode) (types.Time, error) {
	trace_util_0.Count(_builtin_time_00000, 0)
	var tr time.Time
	var err error
	if roundMode == types.ModeTruncate {
		trace_util_0.Count(_builtin_time_00000, 3)
		tr, err = types.TruncateFrac(t, fsp)
	} else {
		trace_util_0.Count(_builtin_time_00000, 4)
		{
			tr, err = types.RoundFrac(t, fsp)
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 5)
		return types.Time{}, err
	}

	trace_util_0.Count(_builtin_time_00000, 2)
	return types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		Fsp:  fsp,
	}, nil
}

type dateFunctionClass struct {
	baseFunctionClass
}

func (c *dateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 6)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 8)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 7)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateSig{bf}
	return sig, nil
}

type builtinDateSig struct {
	baseBuiltinFunc
}

func (b *builtinDateSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 9)
	newSig := &builtinDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals DATE(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 10)
	expr, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 13)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 11)
	if expr.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 14)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(expr.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 12)
	expr.Time = types.FromDate(expr.Time.Year(), expr.Time.Month(), expr.Time.Day(), 0, 0, 0, 0)
	expr.Type = mysql.TypeDate
	return expr, false, nil
}

type dateLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *dateLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 15)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 21)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 16)
	con, ok := args[0].(*Constant)
	if !ok {
		trace_util_0.Count(_builtin_time_00000, 22)
		panic("Unexpected parameter for date literal")
	}
	trace_util_0.Count(_builtin_time_00000, 17)
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 23)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 18)
	str := dt.GetString()
	if !datePattern.MatchString(str) {
		trace_util_0.Count(_builtin_time_00000, 24)
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	trace_util_0.Count(_builtin_time_00000, 19)
	tm, err := types.ParseDate(ctx.GetSessionVars().StmtCtx, str)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 25)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 20)
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateLiteralSig{bf, tm}
	return sig, nil
}

type builtinDateLiteralSig struct {
	baseBuiltinFunc
	literal types.Time
}

func (b *builtinDateLiteralSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 26)
	newSig := &builtinDateLiteralSig{literal: b.literal}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals DATE 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinDateLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 27)
	mode := b.ctx.GetSessionVars().SQLMode
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 30)
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(b.literal.String())
	}
	trace_util_0.Count(_builtin_time_00000, 28)
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		trace_util_0.Count(_builtin_time_00000, 31)
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(b.literal.String())
	}
	trace_util_0.Count(_builtin_time_00000, 29)
	return b.literal, false, nil
}

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 32)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 34)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 33)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime, types.ETDatetime)
	sig := &builtinDateDiffSig{bf}
	return sig, nil
}

type builtinDateDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDateDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 35)
	newSig := &builtinDateDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 36)
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 40)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 37)
	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 41)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 38)
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		trace_util_0.Count(_builtin_time_00000, 42)
		if invalidLHS {
			trace_util_0.Count(_builtin_time_00000, 45)
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(lhs.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 43)
		if invalidRHS {
			trace_util_0.Count(_builtin_time_00000, 46)
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(rhs.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 44)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 39)
	return int64(types.DateDiff(lhs.Time, rhs.Time)), false, nil
}

type timeDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timeDiffFunctionClass) getArgEvalTp(fieldTp *types.FieldType) types.EvalType {
	trace_util_0.Count(_builtin_time_00000, 47)
	argTp := types.ETString
	switch tp := fieldTp.EvalType(); tp {
	case types.ETDuration, types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_time_00000, 49)
		argTp = tp
	}
	trace_util_0.Count(_builtin_time_00000, 48)
	return argTp
}

func (c *timeDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 50)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 55)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 51)
	arg0FieldTp, arg1FieldTp := args[0].GetType(), args[1].GetType()
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Tp, arg1Tp)

	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 56)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 52)
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 57)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 53)
	bf.tp.Decimal = mathutil.Max(arg0Dec, arg1Dec)

	var sig builtinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
	// TODO: we don't really need Duration type, actually in MySQL, it use Time class to represent
	// all the time type, and use filed type to distinguish datetime, date, timestamp or time(duration).
	// With the duration type, we are hard to port all the MySQL behavior.
	switch arg0Tp {
	case types.ETDuration:
		trace_util_0.Count(_builtin_time_00000, 58)
		switch arg1Tp {
		case types.ETDuration:
			trace_util_0.Count(_builtin_time_00000, 61)
			sig = &builtinDurationDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_builtin_time_00000, 62)
			sig = &builtinNullTimeDiffSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 63)
			sig = &builtinDurationStringTimeDiffSig{bf}
		}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_builtin_time_00000, 59)
		switch arg1Tp {
		case types.ETDuration:
			trace_util_0.Count(_builtin_time_00000, 64)
			sig = &builtinNullTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_builtin_time_00000, 65)
			sig = &builtinTimeTimeTimeDiffSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 66)
			sig = &builtinTimeStringTimeDiffSig{bf}
		}
	default:
		trace_util_0.Count(_builtin_time_00000, 60)
		switch arg1Tp {
		case types.ETDuration:
			trace_util_0.Count(_builtin_time_00000, 67)
			sig = &builtinStringDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_builtin_time_00000, 68)
			sig = &builtinStringTimeTimeDiffSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 69)
			sig = &builtinStringStringTimeDiffSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 54)
	return sig, nil
}

type builtinDurationDurationTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationDurationTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 70)
	newSig := &builtinDurationDurationTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 71)
	lhs, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 74)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 72)
	rhs, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 75)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 73)
	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

type builtinTimeTimeTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeTimeTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 76)
	newSig := &builtinTimeTimeTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 77)
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 80)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 78)
	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 81)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 79)
	sc := b.ctx.GetSessionVars().StmtCtx
	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinDurationStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationStringTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 82)
	newSig := &builtinDurationStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 83)
	lhs, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 87)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 84)
	rhsStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 88)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 85)
	sc := b.ctx.GetSessionVars().StmtCtx
	rhs, _, isDuration, err := convertStringToDuration(sc, rhsStr, b.tp.Decimal)
	if err != nil || !isDuration {
		trace_util_0.Count(_builtin_time_00000, 89)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 86)
	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

type builtinStringDurationTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringDurationTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 90)
	newSig := &builtinStringDurationTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 91)
	lhsStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 95)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 92)
	rhs, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 96)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 93)
	sc := b.ctx.GetSessionVars().StmtCtx
	lhs, _, isDuration, err := convertStringToDuration(sc, lhsStr, b.tp.Decimal)
	if err != nil || !isDuration {
		trace_util_0.Count(_builtin_time_00000, 97)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 94)
	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

// calculateTimeDiff calculates interval difference of two types.Time.
func calculateTimeDiff(sc *stmtctx.StatementContext, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 98)
	d = lhs.Sub(sc, &rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_time_00000, 100)
		err = sc.HandleTruncate(err)
	}
	trace_util_0.Count(_builtin_time_00000, 99)
	return d, err != nil, err
}

// calculateDurationTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(ctx sessionctx.Context, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 101)
	d, err = lhs.Sub(rhs)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 104)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 102)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_time_00000, 105)
		sc := ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	trace_util_0.Count(_builtin_time_00000, 103)
	return d, err != nil, err
}

type builtinTimeStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeStringTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 106)
	newSig := &builtinTimeStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 107)
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 111)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 108)
	rhsStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 112)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 109)
	sc := b.ctx.GetSessionVars().StmtCtx
	_, rhs, isDuration, err := convertStringToDuration(sc, rhsStr, b.tp.Decimal)
	if err != nil || isDuration {
		trace_util_0.Count(_builtin_time_00000, 113)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 110)
	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringTimeTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringTimeTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 114)
	newSig := &builtinStringTimeTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 115)
	lhsStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 119)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 116)
	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 120)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 117)
	sc := b.ctx.GetSessionVars().StmtCtx
	_, lhs, isDuration, err := convertStringToDuration(sc, lhsStr, b.tp.Decimal)
	if err != nil || isDuration {
		trace_util_0.Count(_builtin_time_00000, 121)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 118)
	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringStringTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 122)
	newSig := &builtinStringStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 123)
	lhs, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 130)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 124)
	rhs, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 131)
		return d, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 125)
	sc := b.ctx.GetSessionVars().StmtCtx
	fsp := b.tp.Decimal
	lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(sc, lhs, fsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 132)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 126)
	rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(sc, rhs, fsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 133)
		return d, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 127)
	if lhsIsDuration != rhsIsDuration {
		trace_util_0.Count(_builtin_time_00000, 134)
		return d, true, nil
	}

	trace_util_0.Count(_builtin_time_00000, 128)
	if lhsIsDuration {
		trace_util_0.Count(_builtin_time_00000, 135)
		d, isNull, err = calculateDurationTimeDiff(b.ctx, lhsDur, rhsDur)
	} else {
		trace_util_0.Count(_builtin_time_00000, 136)
		{
			d, isNull, err = calculateTimeDiff(sc, lhsTime, rhsTime)
		}
	}

	trace_util_0.Count(_builtin_time_00000, 129)
	return d, isNull, err
}

type builtinNullTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinNullTimeDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 137)
	newSig := &builtinNullTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinNullTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinNullTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 138)
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(sc *stmtctx.StatementContext, str string, fsp int) (d types.Duration, t types.Time,
	isDuration bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 139)
	if n := strings.IndexByte(str, '.'); n >= 0 {
		trace_util_0.Count(_builtin_time_00000, 141)
		lenStrFsp := len(str[n+1:])
		if lenStrFsp <= types.MaxFsp {
			trace_util_0.Count(_builtin_time_00000, 142)
			fsp = mathutil.Max(lenStrFsp, fsp)
		}
	}
	trace_util_0.Count(_builtin_time_00000, 140)
	return types.StrToDuration(sc, str, fsp)
}

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 143)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 145)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 144)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DateFormatSig)
	return sig, nil
}

type builtinDateFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinDateFormatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 146)
	newSig := &builtinDateFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 147)
	t, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 151)
		return "", isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 148)
	if t.InvalidZero() {
		trace_util_0.Count(_builtin_time_00000, 152)
		return "", true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 149)
	formatMask, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 153)
		return "", isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 150)
	res, err := t.DateFormat(formatMask)
	return res, isNull, err
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 154)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 156)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 155)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinFromDaysSig{bf}
	return sig, nil
}

type builtinFromDaysSig struct {
	baseBuiltinFunc
}

func (b *builtinFromDaysSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 157)
	newSig := &builtinFromDaysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals FROM_DAYS(N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 158)
	n, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 160)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 159)
	return types.TimeFromDays(n), false, nil
}

type hourFunctionClass struct {
	baseFunctionClass
}

func (c *hourFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 161)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 163)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 162)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 3, 0
	sig := &builtinHourSig{bf}
	return sig, nil
}

type builtinHourSig struct {
	baseBuiltinFunc
}

func (b *builtinHourSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 164)
	newSig := &builtinHourSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals HOUR(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 165)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 167)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 166)
	return int64(dur.Hour()), false, nil
}

type minuteFunctionClass struct {
	baseFunctionClass
}

func (c *minuteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 168)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 170)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 169)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMinuteSig{bf}
	return sig, nil
}

type builtinMinuteSig struct {
	baseBuiltinFunc
}

func (b *builtinMinuteSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 171)
	newSig := &builtinMinuteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MINUTE(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 172)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 174)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 173)
	return int64(dur.Minute()), false, nil
}

type secondFunctionClass struct {
	baseFunctionClass
}

func (c *secondFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 175)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 177)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 176)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinSecondSig{bf}
	return sig, nil
}

type builtinSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinSecondSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 178)
	newSig := &builtinSecondSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SECOND(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 179)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 181)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 180)
	return int64(dur.Second()), false, nil
}

type microSecondFunctionClass struct {
	baseFunctionClass
}

func (c *microSecondFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 182)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 184)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 183)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 6, 0
	sig := &builtinMicroSecondSig{bf}
	return sig, nil
}

type builtinMicroSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinMicroSecondSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 185)
	newSig := &builtinMicroSecondSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MICROSECOND(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 186)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 188)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 187)
	return int64(dur.MicroSecond()), false, nil
}

type monthFunctionClass struct {
	baseFunctionClass
}

func (c *monthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 189)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 191)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 190)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMonthSig{bf}
	return sig, nil
}

type builtinMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 192)
	newSig := &builtinMonthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MONTH(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func (b *builtinMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 193)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 196)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 194)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 197)
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			trace_util_0.Count(_builtin_time_00000, 199)
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 198)
		return 0, false, nil
	}

	trace_util_0.Count(_builtin_time_00000, 195)
	return int64(date.Time.Month()), false, nil
}

// monthNameFunctionClass see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 200)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 202)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 201)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.tp.Flen = 10
	sig := &builtinMonthNameSig{bf}
	return sig, nil
}

type builtinMonthNameSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthNameSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 203)
	newSig := &builtinMonthNameSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMonthNameSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 204)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 207)
		return "", true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 205)
	mon := arg.Time.Month()
	if (arg.IsZero() && b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
		trace_util_0.Count(_builtin_time_00000, 208)
		return "", true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	} else {
		trace_util_0.Count(_builtin_time_00000, 209)
		if mon == 0 || arg.IsZero() {
			trace_util_0.Count(_builtin_time_00000, 210)
			return "", true, nil
		}
	}
	trace_util_0.Count(_builtin_time_00000, 206)
	return types.MonthNames[mon-1], false, nil
}

type dayNameFunctionClass struct {
	baseFunctionClass
}

func (c *dayNameFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 211)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 213)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 212)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.tp.Flen = 10
	sig := &builtinDayNameSig{bf}
	return sig, nil
}

type builtinDayNameSig struct {
	baseBuiltinFunc
}

func (b *builtinDayNameSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 214)
	newSig := &builtinDayNameSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDayNameSig) evalIndex(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 215)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 218)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 216)
	if arg.InvalidZero() {
		trace_util_0.Count(_builtin_time_00000, 219)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	trace_util_0.Count(_builtin_time_00000, 217)
	res := (int64(arg.Time.Weekday()) + 6) % 7
	return res, false, nil
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 220)
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 222)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 221)
	return types.WeekdayNames[idx], false, nil
}

func (b *builtinDayNameSig) evalReal(row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 223)
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 225)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 224)
	return float64(idx), false, nil
}

func (b *builtinDayNameSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 226)
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 228)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 227)
	return idx, false, nil
}

type dayOfMonthFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfMonthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 229)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 231)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 230)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 2
	sig := &builtinDayOfMonthSig{bf}
	return sig, nil
}

type builtinDayOfMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfMonthSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 232)
	newSig := &builtinDayOfMonthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 233)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 236)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 234)
	if arg.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 237)
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			trace_util_0.Count(_builtin_time_00000, 239)
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 238)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 235)
	return int64(arg.Time.Day()), false, nil
}

type dayOfWeekFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfWeekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 240)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 242)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 241)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1
	sig := &builtinDayOfWeekSig{bf}
	return sig, nil
}

type builtinDayOfWeekSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfWeekSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 243)
	newSig := &builtinDayOfWeekSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 244)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 247)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 245)
	if arg.InvalidZero() {
		trace_util_0.Count(_builtin_time_00000, 248)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	trace_util_0.Count(_builtin_time_00000, 246)
	return int64(arg.Time.Weekday() + 1), false, nil
}

type dayOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfYearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 249)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 251)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 250)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 3
	sig := &builtinDayOfYearSig{bf}
	return sig, nil
}

type builtinDayOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfYearSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 252)
	newSig := &builtinDayOfYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 253)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 256)
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 254)
	if arg.InvalidZero() {
		trace_util_0.Count(_builtin_time_00000, 257)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 255)
	return int64(arg.Time.YearDay()), false, nil
}

type weekFunctionClass struct {
	baseFunctionClass
}

func (c *weekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 258)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 262)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 259)
	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		trace_util_0.Count(_builtin_time_00000, 263)
		argTps = append(argTps, types.ETInt)
	}

	trace_util_0.Count(_builtin_time_00000, 260)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 2, 0

	var sig builtinFunc
	if len(args) == 2 {
		trace_util_0.Count(_builtin_time_00000, 264)
		sig = &builtinWeekWithModeSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 265)
		{
			sig = &builtinWeekWithoutModeSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 261)
	return sig, nil
}

type builtinWeekWithModeSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekWithModeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 266)
	newSig := &builtinWeekWithModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date, mode).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 267)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 271)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 268)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 272)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 269)
	mode, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 273)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 270)
	week := date.Time.Week(int(mode))
	return int64(week), false, nil
}

type builtinWeekWithoutModeSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekWithoutModeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 274)
	newSig := &builtinWeekWithoutModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 275)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 279)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 276)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 280)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 277)
	mode := 0
	modeStr, ok := b.ctx.GetSessionVars().GetSystemVar(variable.DefaultWeekFormat)
	if ok && modeStr != "" {
		trace_util_0.Count(_builtin_time_00000, 281)
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 282)
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}

	trace_util_0.Count(_builtin_time_00000, 278)
	week := date.Time.Week(mode)
	return int64(week), false, nil
}

type weekDayFunctionClass struct {
	baseFunctionClass
}

func (c *weekDayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 283)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 285)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 284)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1

	sig := &builtinWeekDaySig{bf}
	return sig, nil
}

type builtinWeekDaySig struct {
	baseBuiltinFunc
}

func (b *builtinWeekDaySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 286)
	newSig := &builtinWeekDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKDAY(date).
func (b *builtinWeekDaySig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 287)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 290)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 288)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 291)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 289)
	return int64(date.Time.Weekday()+6) % 7, false, nil
}

type weekOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *weekOfYearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 292)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 294)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 293)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinWeekOfYearSig{bf}
	return sig, nil
}

type builtinWeekOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekOfYearSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 295)
	newSig := &builtinWeekOfYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKOFYEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 296)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 299)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 297)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 300)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 298)
	week := date.Time.Week(3)
	return int64(week), false, nil
}

type yearFunctionClass struct {
	baseFunctionClass
}

func (c *yearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 301)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 303)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 302)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 4, 0
	sig := &builtinYearSig{bf}
	return sig, nil
}

type builtinYearSig struct {
	baseBuiltinFunc
}

func (b *builtinYearSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 304)
	newSig := &builtinYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 305)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 308)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 306)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 309)
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			trace_util_0.Count(_builtin_time_00000, 311)
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 310)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 307)
	return int64(date.Time.Year()), false, nil
}

type yearWeekFunctionClass struct {
	baseFunctionClass
}

func (c *yearWeekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 312)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 316)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 313)
	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		trace_util_0.Count(_builtin_time_00000, 317)
		argTps = append(argTps, types.ETInt)
	}

	trace_util_0.Count(_builtin_time_00000, 314)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 6, 0

	var sig builtinFunc
	if len(args) == 2 {
		trace_util_0.Count(_builtin_time_00000, 318)
		sig = &builtinYearWeekWithModeSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 319)
		{
			sig = &builtinYearWeekWithoutModeSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 315)
	return sig, nil
}

type builtinYearWeekWithModeSig struct {
	baseBuiltinFunc
}

func (b *builtinYearWeekWithModeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 320)
	newSig := &builtinYearWeekWithModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 321)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 327)
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 322)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 328)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 323)
	mode, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 329)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 324)
	if isNull {
		trace_util_0.Count(_builtin_time_00000, 330)
		mode = 0
	}

	trace_util_0.Count(_builtin_time_00000, 325)
	year, week := date.Time.YearWeek(int(mode))
	result := int64(week + year*100)
	if result < 0 {
		trace_util_0.Count(_builtin_time_00000, 331)
		return int64(math.MaxUint32), false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 326)
	return result, false, nil
}

type builtinYearWeekWithoutModeSig struct {
	baseBuiltinFunc
}

func (b *builtinYearWeekWithoutModeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 332)
	newSig := &builtinYearWeekWithoutModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 333)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 337)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 334)
	if date.InvalidZero() {
		trace_util_0.Count(_builtin_time_00000, 338)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 335)
	year, week := date.Time.YearWeek(0)
	result := int64(week + year*100)
	if result < 0 {
		trace_util_0.Count(_builtin_time_00000, 339)
		return int64(math.MaxUint32), false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 336)
	return result, false, nil
}

type fromUnixTimeFunctionClass struct {
	baseFunctionClass
}

func (c *fromUnixTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 340)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 344)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 341)
	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		trace_util_0.Count(_builtin_time_00000, 345)
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	trace_util_0.Count(_builtin_time_00000, 342)
	_, isArg0Con := args[0].(*Constant)
	isArg0Str := args[0].GetType().EvalType() == types.ETString
	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 346)
		if isArg0Str {
			trace_util_0.Count(_builtin_time_00000, 348)
			bf.tp.Decimal = types.MaxFsp
		} else {
			trace_util_0.Count(_builtin_time_00000, 349)
			if isArg0Con {
				trace_util_0.Count(_builtin_time_00000, 350)
				arg0, _, err1 := args[0].EvalDecimal(ctx, chunk.Row{})
				if err1 != nil {
					trace_util_0.Count(_builtin_time_00000, 353)
					return sig, err1
				}
				trace_util_0.Count(_builtin_time_00000, 351)
				fsp := int(arg0.GetDigitsFrac())
				if fsp > types.MaxFsp {
					trace_util_0.Count(_builtin_time_00000, 354)
					fsp = types.MaxFsp
				}
				trace_util_0.Count(_builtin_time_00000, 352)
				bf.tp.Decimal = fsp
			}
		}
		trace_util_0.Count(_builtin_time_00000, 347)
		sig = &builtinFromUnixTime1ArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 355)
		{
			bf.tp.Flen = args[1].GetType().Flen
			sig = &builtinFromUnixTime2ArgSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 343)
	return sig, nil
}

func evalFromUnixTime(ctx sessionctx.Context, fsp int, row chunk.Row, arg Expression) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 356)
	unixTimeStamp, isNull, err := arg.EvalDecimal(ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_time_00000, 367)
		return res, isNull, err
	}
	// 0 <= unixTimeStamp <= INT32_MAX
	trace_util_0.Count(_builtin_time_00000, 357)
	if unixTimeStamp.IsNegative() {
		trace_util_0.Count(_builtin_time_00000, 368)
		return res, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 358)
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		trace_util_0.Count(_builtin_time_00000, 369)
		return res, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 359)
	if integralPart > int64(math.MaxInt32) {
		trace_util_0.Count(_builtin_time_00000, 370)
		return res, true, nil
	}
	// Split the integral part and fractional part of a decimal timestamp.
	// e.g. for timestamp 12345.678,
	// first get the integral part 12345,
	// then (12345.678 - 12345) * (10^9) to get the decimal part and convert it to nanosecond precision.
	trace_util_0.Count(_builtin_time_00000, 360)
	integerDecimalTp := new(types.MyDecimal).FromInt(integralPart)
	fracDecimalTp := new(types.MyDecimal)
	err = types.DecimalSub(unixTimeStamp, integerDecimalTp, fracDecimalTp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 371)
		return res, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 361)
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 372)
		return res, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 362)
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		trace_util_0.Count(_builtin_time_00000, 373)
		return res, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 363)
	fracDigitsNumber := int(unixTimeStamp.GetDigitsFrac())
	if fsp < 0 {
		trace_util_0.Count(_builtin_time_00000, 374)
		fsp = types.MaxFsp
	}
	trace_util_0.Count(_builtin_time_00000, 364)
	fsp = mathutil.Max(fracDigitsNumber, fsp)
	if fsp > types.MaxFsp {
		trace_util_0.Count(_builtin_time_00000, 375)
		fsp = types.MaxFsp
	}

	trace_util_0.Count(_builtin_time_00000, 365)
	sc := ctx.GetSessionVars().StmtCtx
	tmp := time.Unix(integralPart, fractionalPart).In(sc.TimeZone)
	t, err := convertTimeToMysqlTime(tmp, fsp, types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 376)
		return res, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 366)
	return t, false, nil
}

type builtinFromUnixTime1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinFromUnixTime1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 377)
	newSig := &builtinFromUnixTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 378)
	return evalFromUnixTime(b.ctx, b.tp.Decimal, row, b.args[0])
}

type builtinFromUnixTime2ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinFromUnixTime2ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 379)
	newSig := &builtinFromUnixTime2ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 380)
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 383)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 381)
	t, isNull, err := evalFromUnixTime(b.ctx, b.tp.Decimal, row, b.args[0])
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 384)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 382)
	res, err = t.DateFormat(format)
	return res, err != nil, err
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 385)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 387)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 386)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = 17
	sig := &builtinGetFormatSig{bf}
	return sig, nil
}

type builtinGetFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinGetFormatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 388)
	newSig := &builtinGetFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 389)
	t, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 393)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 390)
	l, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 394)
		return "", isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 391)
	var res string
	switch t {
	case dateFormat:
		trace_util_0.Count(_builtin_time_00000, 395)
		switch l {
		case usaLocation:
			trace_util_0.Count(_builtin_time_00000, 398)
			res = "%m.%d.%Y"
		case jisLocation:
			trace_util_0.Count(_builtin_time_00000, 399)
			res = "%Y-%m-%d"
		case isoLocation:
			trace_util_0.Count(_builtin_time_00000, 400)
			res = "%Y-%m-%d"
		case eurLocation:
			trace_util_0.Count(_builtin_time_00000, 401)
			res = "%d.%m.%Y"
		case internalLocation:
			trace_util_0.Count(_builtin_time_00000, 402)
			res = "%Y%m%d"
		}
	case datetimeFormat, timestampFormat:
		trace_util_0.Count(_builtin_time_00000, 396)
		switch l {
		case usaLocation:
			trace_util_0.Count(_builtin_time_00000, 403)
			res = "%Y-%m-%d %H.%i.%s"
		case jisLocation:
			trace_util_0.Count(_builtin_time_00000, 404)
			res = "%Y-%m-%d %H:%i:%s"
		case isoLocation:
			trace_util_0.Count(_builtin_time_00000, 405)
			res = "%Y-%m-%d %H:%i:%s"
		case eurLocation:
			trace_util_0.Count(_builtin_time_00000, 406)
			res = "%Y-%m-%d %H.%i.%s"
		case internalLocation:
			trace_util_0.Count(_builtin_time_00000, 407)
			res = "%Y%m%d%H%i%s"
		}
	case timeFormat:
		trace_util_0.Count(_builtin_time_00000, 397)
		switch l {
		case usaLocation:
			trace_util_0.Count(_builtin_time_00000, 408)
			res = "%h:%i:%s %p"
		case jisLocation:
			trace_util_0.Count(_builtin_time_00000, 409)
			res = "%H:%i:%s"
		case isoLocation:
			trace_util_0.Count(_builtin_time_00000, 410)
			res = "%H:%i:%s"
		case eurLocation:
			trace_util_0.Count(_builtin_time_00000, 411)
			res = "%H.%i.%s"
		case internalLocation:
			trace_util_0.Count(_builtin_time_00000, 412)
			res = "%H%i%s"
		}
	}

	trace_util_0.Count(_builtin_time_00000, 392)
	return res, false, nil
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx sessionctx.Context, arg Expression) (tp byte, fsp int) {
	trace_util_0.Count(_builtin_time_00000, 413)
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		trace_util_0.Count(_builtin_time_00000, 418)
		return tp, types.MaxFsp
	}
	trace_util_0.Count(_builtin_time_00000, 414)
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		trace_util_0.Count(_builtin_time_00000, 419)
		return
	}
	trace_util_0.Count(_builtin_time_00000, 415)
	isDuration, isDate := types.GetFormatType(format)
	if isDuration && !isDate {
		trace_util_0.Count(_builtin_time_00000, 420)
		tp = mysql.TypeDuration
	} else {
		trace_util_0.Count(_builtin_time_00000, 421)
		if !isDuration && isDate {
			trace_util_0.Count(_builtin_time_00000, 422)
			tp = mysql.TypeDate
		}
	}
	trace_util_0.Count(_builtin_time_00000, 416)
	if strings.Contains(format, "%f") {
		trace_util_0.Count(_builtin_time_00000, 423)
		fsp = types.MaxFsp
	}
	trace_util_0.Count(_builtin_time_00000, 417)
	return
}

// getFunction see https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 424)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 427)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 425)
	retTp, fsp := c.getRetTp(ctx, args[1])
	switch retTp {
	case mysql.TypeDate:
		trace_util_0.Count(_builtin_time_00000, 428)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.MinFsp
		sig = &builtinStrToDateDateSig{bf}
	case mysql.TypeDatetime:
		trace_util_0.Count(_builtin_time_00000, 429)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			trace_util_0.Count(_builtin_time_00000, 433)
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, types.MinFsp
		} else {
			trace_util_0.Count(_builtin_time_00000, 434)
			{
				bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthWithFsp, types.MaxFsp
			}
		}
		trace_util_0.Count(_builtin_time_00000, 430)
		sig = &builtinStrToDateDatetimeSig{bf}
	case mysql.TypeDuration:
		trace_util_0.Count(_builtin_time_00000, 431)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			trace_util_0.Count(_builtin_time_00000, 435)
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		} else {
			trace_util_0.Count(_builtin_time_00000, 436)
			{
				bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, types.MaxFsp
			}
		}
		trace_util_0.Count(_builtin_time_00000, 432)
		sig = &builtinStrToDateDurationSig{bf}
	}
	trace_util_0.Count(_builtin_time_00000, 426)
	return sig, nil
}

type builtinStrToDateDateSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDateSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 437)
	newSig := &builtinStrToDateDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 438)
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 443)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 439)
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 444)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 440)
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		trace_util_0.Count(_builtin_time_00000, 445)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 441)
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		trace_util_0.Count(_builtin_time_00000, 446)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 442)
	t.Type, t.Fsp = mysql.TypeDate, types.MinFsp
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 447)
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 448)
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 453)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 449)
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 454)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 450)
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		trace_util_0.Count(_builtin_time_00000, 455)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 451)
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		trace_util_0.Count(_builtin_time_00000, 456)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 452)
	t.Type, t.Fsp = mysql.TypeDatetime, b.tp.Decimal
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 457)
	newSig := &builtinStrToDateDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration
// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 458)
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 463)
		return types.Duration{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 459)
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 464)
		return types.Duration{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 460)
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		trace_util_0.Count(_builtin_time_00000, 465)
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 461)
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		trace_util_0.Count(_builtin_time_00000, 466)
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 462)
	t.Fsp = b.tp.Decimal
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 467)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 471)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 468)
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 472)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_time_00000, 469)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = 19, 0

	var sig builtinFunc
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 473)
		sig = &builtinSysDateWithFspSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 474)
		{
			sig = &builtinSysDateWithoutFspSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 470)
	return sig, nil
}

type builtinSysDateWithFspSig struct {
	baseBuiltinFunc
}

func (b *builtinSysDateWithFspSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 475)
	newSig := &builtinSysDateWithFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 476)
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 479)
		return types.Time{}, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 477)
	loc := b.ctx.GetSessionVars().Location()
	now := time.Now().In(loc)
	result, err := convertTimeToMysqlTime(now, int(fsp), types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 480)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 478)
	return result, false, nil
}

type builtinSysDateWithoutFspSig struct {
	baseBuiltinFunc
}

func (b *builtinSysDateWithoutFspSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 481)
	newSig := &builtinSysDateWithoutFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 482)
	tz := b.ctx.GetSessionVars().Location()
	now := time.Now().In(tz)
	result, err := convertTimeToMysqlTime(now, 0, types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 484)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 483)
	return result, false, nil
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 485)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 487)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 486)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentDateSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 488)
	newSig := &builtinCurrentDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 489)
	tz := b.ctx.GetSessionVars().Location()
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 491)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 490)
	year, month, day := nowTs.In(tz).Date()
	result := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0}
	return result, false, nil
}

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 492)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 496)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 493)
	if len(args) == 0 {
		trace_util_0.Count(_builtin_time_00000, 497)
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		sig = &builtinCurrentTime0ArgSig{bf}
		return sig, nil
	}
	// args[0] must be a constant which should not be null.
	trace_util_0.Count(_builtin_time_00000, 494)
	_, ok := args[0].(*Constant)
	fsp := int64(types.MaxFsp)
	if ok {
		trace_util_0.Count(_builtin_time_00000, 498)
		fsp, _, err = args[0].EvalInt(ctx, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 500)
			return nil, err
		}
		trace_util_0.Count(_builtin_time_00000, 499)
		if fsp > int64(types.MaxFsp) {
			trace_util_0.Count(_builtin_time_00000, 501)
			return nil, errors.Errorf("Too-big precision %v specified for 'curtime'. Maximum is %v.", fsp, types.MaxFsp)
		} else {
			trace_util_0.Count(_builtin_time_00000, 502)
			if fsp < int64(types.MinFsp) {
				trace_util_0.Count(_builtin_time_00000, 503)
				return nil, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 495)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETInt)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, int(fsp)
	sig = &builtinCurrentTime1ArgSig{bf}
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 504)
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime0ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 505)
	tz := b.ctx.GetSessionVars().Location()
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 508)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 506)
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, dur, types.MinFsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 509)
		return types.Duration{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 507)
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 510)
	newSig := &builtinCurrentTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime1ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 511)
	fsp, _, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 515)
		return types.Duration{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 512)
	tz := b.ctx.GetSessionVars().Location()
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 516)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 513)
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, dur, int(fsp))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 517)
		return types.Duration{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 514)
	return res, false, nil
}

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 518)
	err := c.verifyArgs(args)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 521)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 519)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString)
	bf.tp.Decimal, err = getExpressionFsp(ctx, args[0])
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 522)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 520)
	sig := &builtinTimeSig{bf}
	return sig, nil
}

type builtinTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 523)
	newSig := &builtinTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 524)
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 529)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 525)
	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		trace_util_0.Count(_builtin_time_00000, 530)
		fsp = len(expr) - idx - 1
	}

	trace_util_0.Count(_builtin_time_00000, 526)
	if fsp, err = types.CheckFsp(fsp); err != nil {
		trace_util_0.Count(_builtin_time_00000, 531)
		return res, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 527)
	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ParseDuration(sc, expr, fsp)
	if types.ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_builtin_time_00000, 532)
		err = sc.HandleTruncate(err)
	}
	trace_util_0.Count(_builtin_time_00000, 528)
	return res, isNull, err
}

type timeLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timeLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 533)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 540)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 534)
	con, ok := args[0].(*Constant)
	if !ok {
		trace_util_0.Count(_builtin_time_00000, 541)
		panic("Unexpected parameter for time literal")
	}
	trace_util_0.Count(_builtin_time_00000, 535)
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 542)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 536)
	str := dt.GetString()
	if !isDuration(str) {
		trace_util_0.Count(_builtin_time_00000, 543)
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	trace_util_0.Count(_builtin_time_00000, 537)
	duration, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, str, types.GetFsp(str))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 544)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 538)
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 10, duration.Fsp
	if duration.Fsp > 0 {
		trace_util_0.Count(_builtin_time_00000, 545)
		bf.tp.Flen += 1 + duration.Fsp
	}
	trace_util_0.Count(_builtin_time_00000, 539)
	sig := &builtinTimeLiteralSig{bf, duration}
	return sig, nil
}

type builtinTimeLiteralSig struct {
	baseBuiltinFunc
	duration types.Duration
}

func (b *builtinTimeLiteralSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 546)
	newSig := &builtinTimeLiteralSig{duration: b.duration}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 547)
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 548)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 550)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 549)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinUTCDateSig{bf}
	return sig, nil
}

type builtinUTCDateSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCDateSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 551)
	newSig := &builtinUTCDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 552)
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 554)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 553)
	year, month, day := nowTs.UTC().Date()
	result := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate,
		Fsp:  types.UnspecifiedFsp}
	return result, false, nil
}

type utcTimestampFunctionClass struct {
	baseFunctionClass
}

func getFlenAndDecimal4UTCTimestampAndNow(ctx sessionctx.Context, arg Expression) (flen, decimal int) {
	trace_util_0.Count(_builtin_time_00000, 555)
	if constant, ok := arg.(*Constant); ok {
		trace_util_0.Count(_builtin_time_00000, 558)
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			trace_util_0.Count(_builtin_time_00000, 559)
			decimal = types.MaxFsp
		} else {
			trace_util_0.Count(_builtin_time_00000, 560)
			if fsp < int64(types.MinFsp) {
				trace_util_0.Count(_builtin_time_00000, 561)
				decimal = types.MinFsp
			} else {
				trace_util_0.Count(_builtin_time_00000, 562)
				{
					decimal = int(fsp)
				}
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 556)
	if decimal > 0 {
		trace_util_0.Count(_builtin_time_00000, 563)
		flen = 19 + 1 + decimal
	} else {
		trace_util_0.Count(_builtin_time_00000, 564)
		{
			flen = 19
		}
	}
	trace_util_0.Count(_builtin_time_00000, 557)
	return flen, decimal
}

func (c *utcTimestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 565)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 570)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 566)
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 571)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_time_00000, 567)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 572)
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx, args[0])
	} else {
		trace_util_0.Count(_builtin_time_00000, 573)
		{
			bf.tp.Flen, bf.tp.Decimal = 19, 0
		}
	}

	trace_util_0.Count(_builtin_time_00000, 568)
	var sig builtinFunc
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 574)
		sig = &builtinUTCTimestampWithArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 575)
		{
			sig = &builtinUTCTimestampWithoutArgSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 569)
	return sig, nil
}

func evalUTCTimestampWithFsp(ctx sessionctx.Context, fsp int) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 576)
	var nowTs = &ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 579)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 577)
	result, err := convertTimeToMysqlTime(nowTs.UTC(), fsp, types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 580)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 578)
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimestampWithArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 581)
	newSig := &builtinUTCTimestampWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 582)
	num, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 586)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 583)
	if !isNull && num > int64(types.MaxFsp) {
		trace_util_0.Count(_builtin_time_00000, 587)
		return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v.", num, types.MaxFsp)
	}
	trace_util_0.Count(_builtin_time_00000, 584)
	if !isNull && num < int64(types.MinFsp) {
		trace_util_0.Count(_builtin_time_00000, 588)
		return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", num)
	}

	trace_util_0.Count(_builtin_time_00000, 585)
	result, isNull, err := evalUTCTimestampWithFsp(b.ctx, int(num))
	return result, isNull, err
}

type builtinUTCTimestampWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimestampWithoutArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 589)
	newSig := &builtinUTCTimestampWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 590)
	result, isNull, err := evalUTCTimestampWithFsp(b.ctx, 0)
	return result, isNull, err
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 591)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 596)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 592)
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 597)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_time_00000, 593)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 598)
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx, args[0])
	} else {
		trace_util_0.Count(_builtin_time_00000, 599)
		{
			bf.tp.Flen, bf.tp.Decimal = 19, 0
		}
	}

	trace_util_0.Count(_builtin_time_00000, 594)
	var sig builtinFunc
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 600)
		sig = &builtinNowWithArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 601)
		{
			sig = &builtinNowWithoutArgSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 595)
	return sig, nil
}

func evalNowWithFsp(ctx sessionctx.Context, fsp int) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 602)
	var sysTs = &ctx.GetSessionVars().StmtCtx.SysTs
	if sysTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 606)
		var err error
		*sysTs, err = getSystemTimestamp(ctx)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 607)
			return types.Time{}, true, err
		}
	}

	// In MySQL's implementation, now() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	// mysql> select now(6), now(3), now();
	//	+----------------------------+-------------------------+---------------------+
	//	| now(6)                     | now(3)                  | now()               |
	//	+----------------------------+-------------------------+---------------------+
	//	| 2019-03-25 15:57:56.612966 | 2019-03-25 15:57:56.612 | 2019-03-25 15:57:56 |
	//	+----------------------------+-------------------------+---------------------+
	trace_util_0.Count(_builtin_time_00000, 603)
	result, err := convertTimeToMysqlTime(*sysTs, fsp, types.ModeTruncate)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 608)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 604)
	err = result.ConvertTimeZone(time.Local, ctx.GetSessionVars().Location())
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 609)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 605)
	return result, false, nil
}

type builtinNowWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 610)
	newSig := &builtinNowWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 611)
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)

	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 614)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 612)
	if isNull {
		trace_util_0.Count(_builtin_time_00000, 615)
		fsp = 0
	} else {
		trace_util_0.Count(_builtin_time_00000, 616)
		if fsp > int64(types.MaxFsp) {
			trace_util_0.Count(_builtin_time_00000, 617)
			return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v.", fsp, types.MaxFsp)
		} else {
			trace_util_0.Count(_builtin_time_00000, 618)
			if fsp < int64(types.MinFsp) {
				trace_util_0.Count(_builtin_time_00000, 619)
				return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
			}
		}
	}

	trace_util_0.Count(_builtin_time_00000, 613)
	result, isNull, err := evalNowWithFsp(b.ctx, int(fsp))
	return result, isNull, err
}

type builtinNowWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithoutArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 620)
	newSig := &builtinNowWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 621)
	result, isNull, err := evalNowWithFsp(b.ctx, 0)
	return result, isNull, err
}

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 622)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 626)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 623)
	datetimeUnits := map[string]struct{}{
		"DAY":             {},
		"WEEK":            {},
		"MONTH":           {},
		"QUARTER":         {},
		"YEAR":            {},
		"DAY_MICROSECOND": {},
		"DAY_SECOND":      {},
		"DAY_MINUTE":      {},
		"DAY_HOUR":        {},
		"YEAR_MONTH":      {},
	}
	isDatetimeUnit := true
	args[0] = WrapWithCastAsString(ctx, args[0])
	if _, isCon := args[0].(*Constant); isCon {
		trace_util_0.Count(_builtin_time_00000, 627)
		unit, _, err1 := args[0].EvalString(ctx, chunk.Row{})
		if err1 != nil {
			trace_util_0.Count(_builtin_time_00000, 629)
			return nil, err1
		}
		trace_util_0.Count(_builtin_time_00000, 628)
		_, isDatetimeUnit = datetimeUnits[unit]
	}
	trace_util_0.Count(_builtin_time_00000, 624)
	var bf baseBuiltinFunc
	if isDatetimeUnit {
		trace_util_0.Count(_builtin_time_00000, 630)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime)
		sig = &builtinExtractDatetimeSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 631)
		{
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDuration)
			sig = &builtinExtractDurationSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 625)
	return sig, nil
}

type builtinExtractDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractDatetimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 632)
	newSig := &builtinExtractDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 633)
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 636)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 634)
	dt, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 637)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 635)
	res, err := types.ExtractDatetimeNum(&dt, unit)
	return res, err != nil, err
}

type builtinExtractDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 638)
	newSig := &builtinExtractDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 639)
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 642)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 640)
	dur, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 643)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 641)
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, err
}

// baseDateArithmitical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmitical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArighmeticalUtil() baseDateArithmitical {
	trace_util_0.Count(_builtin_time_00000, 644)
	return baseDateArithmitical{
		intervalRegexp: regexp.MustCompile(`[\d]+`),
	}
}

func (du *baseDateArithmitical) getDateFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 645)
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 648)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 646)
	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		trace_util_0.Count(_builtin_time_00000, 649)
		dateTp = mysql.TypeDatetime
	}

	trace_util_0.Count(_builtin_time_00000, 647)
	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
	return date, err != nil, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmitical) getDateFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 650)
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 654)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 651)
	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTimeFromInt64(sc, dateInt)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 655)
		return types.Time{}, true, handleInvalidTimeError(ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 652)
	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		trace_util_0.Count(_builtin_time_00000, 656)
		dateTp = mysql.TypeDatetime
	}
	trace_util_0.Count(_builtin_time_00000, 653)
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getDateFromDatetime(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 657)
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 660)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 658)
	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		trace_util_0.Count(_builtin_time_00000, 661)
		dateTp = mysql.TypeDatetime
	}
	trace_util_0.Count(_builtin_time_00000, 659)
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getIntervalFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 662)
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 665)
		return "", true, err
	}
	// unit "DAY" and "HOUR" has to be specially handled.
	trace_util_0.Count(_builtin_time_00000, 663)
	if toLower := strings.ToLower(unit); toLower == "day" || toLower == "hour" {
		trace_util_0.Count(_builtin_time_00000, 666)
		if strings.ToLower(interval) == "true" {
			trace_util_0.Count(_builtin_time_00000, 667)
			interval = "1"
		} else {
			trace_util_0.Count(_builtin_time_00000, 668)
			if strings.ToLower(interval) == "false" {
				trace_util_0.Count(_builtin_time_00000, 669)
				interval = "0"
			} else {
				trace_util_0.Count(_builtin_time_00000, 670)
				{
					interval = du.intervalRegexp.FindString(interval)
				}
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 664)
	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromDecimal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 671)
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 674)
		return "", true, err
	}

	trace_util_0.Count(_builtin_time_00000, 672)
	switch strings.ToUpper(unit) {
	case "HOUR_MINUTE", "MINUTE_SECOND":
		trace_util_0.Count(_builtin_time_00000, 675)
		interval = strings.Replace(interval, ".", ":", -1)
	case "YEAR_MONTH":
		trace_util_0.Count(_builtin_time_00000, 676)
		interval = strings.Replace(interval, ".", "-", -1)
	case "DAY_HOUR":
		trace_util_0.Count(_builtin_time_00000, 677)
		interval = strings.Replace(interval, ".", " ", -1)
	case "DAY_MINUTE":
		trace_util_0.Count(_builtin_time_00000, 678)
		interval = "0 " + strings.Replace(interval, ".", ":", -1)
	case "DAY_SECOND":
		trace_util_0.Count(_builtin_time_00000, 679)
		interval = "0 00:" + strings.Replace(interval, ".", ":", -1)
	case "DAY_MICROSECOND":
		trace_util_0.Count(_builtin_time_00000, 680)
		interval = "0 00:00:" + interval
	case "HOUR_MICROSECOND":
		trace_util_0.Count(_builtin_time_00000, 681)
		interval = "00:00:" + interval
	case "HOUR_SECOND":
		trace_util_0.Count(_builtin_time_00000, 682)
		interval = "00:" + strings.Replace(interval, ".", ":", -1)
	case "MINUTE_MICROSECOND":
		trace_util_0.Count(_builtin_time_00000, 683)
		interval = "00:" + interval
	case "SECOND_MICROSECOND":
		trace_util_0.Count(_builtin_time_00000, 684)
		/* keep interval as original decimal */
	case "SECOND":
		trace_util_0.Count(_builtin_time_00000, 685)
		// Decimal's EvalString is like %f format.
		interval, isNull, err = args[1].EvalString(ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_time_00000, 687)
			return "", true, err
		}
	default:
		trace_util_0.Count(_builtin_time_00000, 686)
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		args[1] = WrapWithCastAsInt(ctx, args[1])
		interval, isNull, err = args[1].EvalString(ctx, row)
		if isNull || err != nil {
			trace_util_0.Count(_builtin_time_00000, 688)
			return "", true, err
		}
	}

	trace_util_0.Count(_builtin_time_00000, 673)
	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 689)
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 691)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 690)
	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmitical) getIntervalFromReal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 692)
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 694)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 693)
	return strconv.FormatFloat(interval, 'f', -1, 64), false, nil
}

func (du *baseDateArithmitical) add(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 695)
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 701)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 696)
	goTime, err := date.Time.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 702)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 697)
	goTime = goTime.Add(time.Duration(nano))
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 703)
		date.Fsp = 0
	} else {
		trace_util_0.Count(_builtin_time_00000, 704)
		{
			date.Fsp = 6
		}
	}

	trace_util_0.Count(_builtin_time_00000, 698)
	date.Time = types.FromGoTime(goTime)
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 705)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 699)
	if overflow {
		trace_util_0.Count(_builtin_time_00000, 706)
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	trace_util_0.Count(_builtin_time_00000, 700)
	return date, false, nil
}

func (du *baseDateArithmitical) addDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 707)
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 710)
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 708)
	retDur, err := d.Add(dur)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 711)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 709)
	return retDur, false, nil
}

func (du *baseDateArithmitical) subDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 712)
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 715)
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 713)
	retDur, err := d.Sub(dur)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 716)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 714)
	return retDur, false, nil
}

func (du *baseDateArithmitical) sub(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 717)
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 723)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 718)
	year, month, day, nano = -year, -month, -day, -nano

	goTime, err := date.Time.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 724)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 719)
	duration := time.Duration(nano)
	goTime = goTime.Add(duration)
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 725)
		date.Fsp = 0
	} else {
		trace_util_0.Count(_builtin_time_00000, 726)
		{
			date.Fsp = 6
		}
	}

	trace_util_0.Count(_builtin_time_00000, 720)
	date.Time = types.FromGoTime(goTime)
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		trace_util_0.Count(_builtin_time_00000, 727)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 721)
	if overflow {
		trace_util_0.Count(_builtin_time_00000, 728)
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	trace_util_0.Count(_builtin_time_00000, 722)
	return date, false, nil
}

type addDateFunctionClass struct {
	baseFunctionClass
}

func (c *addDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 729)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 735)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 730)
	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		trace_util_0.Count(_builtin_time_00000, 736)
		dateEvalTp = types.ETDatetime
	}

	trace_util_0.Count(_builtin_time_00000, 731)
	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		trace_util_0.Count(_builtin_time_00000, 737)
		intervalEvalTp = types.ETInt
	}

	trace_util_0.Count(_builtin_time_00000, 732)
	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		trace_util_0.Count(_builtin_time_00000, 738)
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 742)
			return nil, err
		}
		trace_util_0.Count(_builtin_time_00000, 739)
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			trace_util_0.Count(_builtin_time_00000, 743)
			internalFsp = types.MaxFsp
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			trace_util_0.Count(_builtin_time_00000, 744)
			internalFsp = types.MaxFsp
			if intervalEvalTp != types.ETString {
				trace_util_0.Count(_builtin_time_00000, 745)
				internalFsp = mathutil.Min(args[1].GetType().Decimal, types.MaxFsp)
			}
			// Otherwise, the fsp should be 0.
		}
		trace_util_0.Count(_builtin_time_00000, 740)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 746)
			return nil, err
		}
		trace_util_0.Count(_builtin_time_00000, 741)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		trace_util_0.Count(_builtin_time_00000, 747)
		{
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
		}
	}

	trace_util_0.Count(_builtin_time_00000, 733)
	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 748)
		sig = &builtinAddDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 749)
		sig = &builtinAddDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 750)
		sig = &builtinAddDateStringRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 751)
		sig = &builtinAddDateStringDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 752)
		sig = &builtinAddDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 753)
		sig = &builtinAddDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 754)
		sig = &builtinAddDateIntRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 755)
		sig = &builtinAddDateIntDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 756)
		sig = &builtinAddDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 757)
		sig = &builtinAddDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 758)
		sig = &builtinAddDateDatetimeRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 759)
		sig = &builtinAddDateDatetimeDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 760)
		sig = &builtinAddDateDurationStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 761)
		sig = &builtinAddDateDurationIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 762)
		sig = &builtinAddDateDurationRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 763)
		sig = &builtinAddDateDurationDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	trace_util_0.Count(_builtin_time_00000, 734)
	return sig, nil
}

type builtinAddDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 764)
	newSig := &builtinAddDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 765)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 769)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 766)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 770)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 767)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 771)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 768)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 772)
	newSig := &builtinAddDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 773)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 777)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 774)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 778)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 775)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 779)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 776)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 780)
	newSig := &builtinAddDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 781)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 785)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 782)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 786)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 783)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 787)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 784)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 788)
	newSig := &builtinAddDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 789)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 793)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 790)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 794)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 791)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 795)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 792)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 796)
	newSig := &builtinAddDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 797)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 801)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 798)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 802)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 799)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 803)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 800)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 804)
	newSig := &builtinAddDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 805)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 809)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 806)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 810)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 807)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 811)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 808)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 812)
	newSig := &builtinAddDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 813)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 817)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 814)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 818)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 815)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 819)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 816)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 820)
	newSig := &builtinAddDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 821)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 825)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 822)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 826)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 823)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 827)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 824)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 828)
	newSig := &builtinAddDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 829)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 833)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 830)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 834)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 831)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 835)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 832)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 836)
	newSig := &builtinAddDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 837)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 841)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 838)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 842)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 839)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 843)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 840)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 844)
	newSig := &builtinAddDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 845)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 849)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 846)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 850)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 847)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 851)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 848)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 852)
	newSig := &builtinAddDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 853)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 857)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 854)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 858)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 855)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 859)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 856)
	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 860)
	newSig := &builtinAddDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 861)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 865)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 862)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 866)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 863)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 867)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 864)
	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 868)
	newSig := &builtinAddDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 869)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 873)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 870)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 874)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 871)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 875)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 872)
	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 876)
	newSig := &builtinAddDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 877)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 881)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 878)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 882)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 879)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 883)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 880)
	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 884)
	newSig := &builtinAddDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 885)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 889)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 886)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 890)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 887)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 891)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 888)
	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type subDateFunctionClass struct {
	baseFunctionClass
}

func (c *subDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 892)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 898)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 893)
	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		trace_util_0.Count(_builtin_time_00000, 899)
		dateEvalTp = types.ETDatetime
	}

	trace_util_0.Count(_builtin_time_00000, 894)
	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		trace_util_0.Count(_builtin_time_00000, 900)
		intervalEvalTp = types.ETInt
	}

	trace_util_0.Count(_builtin_time_00000, 895)
	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		trace_util_0.Count(_builtin_time_00000, 901)
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 905)
			return nil, err
		}
		trace_util_0.Count(_builtin_time_00000, 902)
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			trace_util_0.Count(_builtin_time_00000, 906)
			internalFsp = types.MaxFsp
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			trace_util_0.Count(_builtin_time_00000, 907)
			internalFsp = types.MaxFsp
			if intervalEvalTp != types.ETString {
				trace_util_0.Count(_builtin_time_00000, 908)
				internalFsp = mathutil.Min(args[1].GetType().Decimal, types.MaxFsp)
			}
			// Otherwise, the fsp should be 0.
		}
		trace_util_0.Count(_builtin_time_00000, 903)
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 909)
			return nil, err
		}
		trace_util_0.Count(_builtin_time_00000, 904)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		trace_util_0.Count(_builtin_time_00000, 910)
		{
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
		}
	}

	trace_util_0.Count(_builtin_time_00000, 896)
	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 911)
		sig = &builtinSubDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 912)
		sig = &builtinSubDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 913)
		sig = &builtinSubDateStringRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 914)
		sig = &builtinSubDateStringDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 915)
		sig = &builtinSubDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 916)
		sig = &builtinSubDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 917)
		sig = &builtinSubDateIntRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 918)
		sig = &builtinSubDateIntDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 919)
		sig = &builtinSubDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 920)
		sig = &builtinSubDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 921)
		sig = &builtinSubDateDatetimeRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 922)
		sig = &builtinSubDateDatetimeDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		trace_util_0.Count(_builtin_time_00000, 923)
		sig = &builtinSubDateDurationStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 924)
		sig = &builtinSubDateDurationIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		trace_util_0.Count(_builtin_time_00000, 925)
		sig = &builtinSubDateDurationRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 926)
		sig = &builtinSubDateDurationDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	trace_util_0.Count(_builtin_time_00000, 897)
	return sig, nil
}

type builtinSubDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 927)
	newSig := &builtinSubDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 928)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 932)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 929)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 933)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 930)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 934)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 931)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 935)
	newSig := &builtinSubDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 936)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 940)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 937)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 941)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 938)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 942)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 939)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 943)
	newSig := &builtinSubDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 944)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 948)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 945)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 949)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 946)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 950)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 947)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 951)
	newSig := &builtinSubDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 952)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 956)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 953)
	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 957)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 954)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 958)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 955)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 959)
	newSig := &builtinSubDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 960)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 964)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 961)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 965)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 962)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 966)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 963)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 967)
	newSig := &builtinSubDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 968)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 972)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 969)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 973)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 970)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 974)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 971)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 975)
	newSig := &builtinSubDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 976)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 980)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 977)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 981)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 978)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 982)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 979)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

type builtinSubDateIntDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 983)
	newSig := &builtinSubDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 984)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 988)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 985)
	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 989)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 986)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 990)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 987)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

func (b *builtinSubDateDatetimeStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 991)
	newSig := &builtinSubDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 992)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 996)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 993)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 997)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 994)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 998)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 995)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 999)
	newSig := &builtinSubDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1000)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1004)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1001)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1005)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1002)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1006)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1003)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1007)
	newSig := &builtinSubDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1008)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1012)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1009)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1013)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1010)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1014)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1011)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1015)
	newSig := &builtinSubDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1016)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1020)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1017)
	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1021)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1018)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1022)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1019)
	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1023)
	newSig := &builtinSubDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1024)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1028)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1025)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1029)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1026)
	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1030)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1027)
	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1031)
	newSig := &builtinSubDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1032)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1036)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1033)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1037)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1034)
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1038)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1035)
	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationDecimalSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1039)
	newSig := &builtinSubDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1040)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1044)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1041)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1045)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1042)
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1046)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1043)
	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationRealSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1047)
	newSig := &builtinSubDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1048)
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1052)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1049)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1053)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1050)
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1054)
		return types.ZeroDuration, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1051)
	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timestampDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1055)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1057)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1056)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime, types.ETDatetime)
	sig := &builtinTimestampDiffSig{bf}
	return sig, nil
}

type builtinTimestampDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimestampDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1058)
	newSig := &builtinTimestampDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1059)
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1064)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1060)
	lhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1065)
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1061)
	rhs, isNull, err := b.args[2].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1066)
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1062)
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		trace_util_0.Count(_builtin_time_00000, 1067)
		if invalidLHS {
			trace_util_0.Count(_builtin_time_00000, 1070)
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(lhs.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 1068)
		if invalidRHS {
			trace_util_0.Count(_builtin_time_00000, 1071)
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(rhs.String()))
		}
		trace_util_0.Count(_builtin_time_00000, 1069)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1063)
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1072)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1077)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1073)
	var (
		argTps              []types.EvalType
		retTp               types.EvalType
		retFLen, retDecimal int
	)

	if len(args) == 0 {
		trace_util_0.Count(_builtin_time_00000, 1078)
		retTp, retDecimal = types.ETInt, 0
	} else {
		trace_util_0.Count(_builtin_time_00000, 1079)
		{
			argTps = []types.EvalType{types.ETDatetime}
			argType := args[0].GetType()
			argEvaltp := argType.EvalType()
			if argEvaltp == types.ETString {
				trace_util_0.Count(_builtin_time_00000, 1082)
				// Treat types.ETString as unspecified decimal.
				retDecimal = types.UnspecifiedLength
				if cnst, ok := args[0].(*Constant); ok {
					trace_util_0.Count(_builtin_time_00000, 1083)
					tmpStr, _, err := cnst.EvalString(ctx, chunk.Row{})
					if err != nil {
						trace_util_0.Count(_builtin_time_00000, 1085)
						return nil, err
					}
					trace_util_0.Count(_builtin_time_00000, 1084)
					retDecimal = 0
					if dotIdx := strings.LastIndex(tmpStr, "."); dotIdx >= 0 {
						trace_util_0.Count(_builtin_time_00000, 1086)
						retDecimal = len(tmpStr) - dotIdx - 1
					}
				}
			} else {
				trace_util_0.Count(_builtin_time_00000, 1087)
				{
					retDecimal = argType.Decimal
				}
			}
			trace_util_0.Count(_builtin_time_00000, 1080)
			if retDecimal > 6 || retDecimal == types.UnspecifiedLength {
				trace_util_0.Count(_builtin_time_00000, 1088)
				retDecimal = 6
			}
			trace_util_0.Count(_builtin_time_00000, 1081)
			if retDecimal == 0 {
				trace_util_0.Count(_builtin_time_00000, 1089)
				retTp = types.ETInt
			} else {
				trace_util_0.Count(_builtin_time_00000, 1090)
				{
					retTp = types.ETDecimal
				}
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1074)
	if retTp == types.ETInt {
		trace_util_0.Count(_builtin_time_00000, 1091)
		retFLen = 11
	} else {
		trace_util_0.Count(_builtin_time_00000, 1092)
		if retTp == types.ETDecimal {
			trace_util_0.Count(_builtin_time_00000, 1093)
			retFLen = 12 + retDecimal
		} else {
			trace_util_0.Count(_builtin_time_00000, 1094)
			{
				panic("Unexpected retTp")
			}
		}
	}

	trace_util_0.Count(_builtin_time_00000, 1075)
	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)
	bf.tp.Flen = retFLen
	bf.tp.Decimal = retDecimal

	var sig builtinFunc
	if len(args) == 0 {
		trace_util_0.Count(_builtin_time_00000, 1095)
		sig = &builtinUnixTimestampCurrentSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 1096)
		if retTp == types.ETInt {
			trace_util_0.Count(_builtin_time_00000, 1097)
			sig = &builtinUnixTimestampIntSig{bf}
		} else {
			trace_util_0.Count(_builtin_time_00000, 1098)
			if retTp == types.ETDecimal {
				trace_util_0.Count(_builtin_time_00000, 1099)
				sig = &builtinUnixTimestampDecSig{bf}
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1076)
	return sig, nil
}

// goTimeToMysqlUnixTimestamp converts go time into MySQL's Unix timestamp.
// MySQL's Unix timestamp ranges in int32. Values out of range should be rewritten to 0.
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) (*types.MyDecimal, error) {
	trace_util_0.Count(_builtin_time_00000, 1100)
	nanoSeconds := t.UnixNano()
	if nanoSeconds < 0 || (nanoSeconds/1e3) >= (math.MaxInt32+1)*1e6 {
		trace_util_0.Count(_builtin_time_00000, 1103)
		return new(types.MyDecimal), nil
	}
	trace_util_0.Count(_builtin_time_00000, 1101)
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromInt(nanoSeconds)
	err := dec.Shift(-9)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1104)
		return nil, err
	}

	// In MySQL's implementation, unix_timestamp() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	//	mysql> select unix_timestamp(), unix_timestamp(now(0)), now(0), unix_timestamp(now(3)), now(3), now(6);
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	| unix_timestamp() | unix_timestamp(now(0)) | now(0)              | unix_timestamp(now(3)) | now(3)                  | now(6)                     |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	|       1553503194 |             1553503194 | 2019-03-25 16:39:54 |         1553503194.992 | 2019-03-25 16:39:54.992 | 2019-03-25 16:39:54.992969 |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	trace_util_0.Count(_builtin_time_00000, 1102)
	err = dec.Round(dec, decimal, types.ModeTruncate)
	return dec, err
}

type builtinUnixTimestampCurrentSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampCurrentSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1105)
	newSig := &builtinUnixTimestampCurrentSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1106)
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 1109)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 1107)
	dec, err := goTimeToMysqlUnixTimestamp(*nowTs, 1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1110)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1108)
	intVal, err := dec.ToInt()
	terror.Log(err)
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampIntSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1111)
	newSig := &builtinUnixTimestampIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1112)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrInvalidTimeFormat.GenWithStackByArgs(val), err) {
		trace_util_0.Count(_builtin_time_00000, 1117)
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1113)
	if isNull {
		trace_util_0.Count(_builtin_time_00000, 1118)
		return 0, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1114)
	t, err := val.Time.GoTime(getTimeZone(b.ctx))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1119)
		return 0, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1115)
	dec, err := goTimeToMysqlUnixTimestamp(t, 1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1120)
		return 0, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1116)
	intVal, err := dec.ToInt()
	terror.Log(err)
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampDecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1121)
	newSig := &builtinUnixTimestampDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1122)
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1125)
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1123)
	t, err := val.Time.GoTime(getTimeZone(b.ctx))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1126)
		return new(types.MyDecimal), false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1124)
	result, err := goTimeToMysqlUnixTimestamp(t, b.tp.Decimal)
	return result, err != nil, err
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getDefaultFsp(tp *types.FieldType) int {
	trace_util_0.Count(_builtin_time_00000, 1127)
	if tp.Tp == mysql.TypeDatetime || tp.Tp == mysql.TypeDate || tp.Tp == mysql.TypeDuration ||
		tp.Tp == mysql.TypeTimestamp {
		trace_util_0.Count(_builtin_time_00000, 1130)
		return tp.Decimal
	}
	trace_util_0.Count(_builtin_time_00000, 1128)
	switch cls := tp.EvalType(); cls {
	case types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 1131)
		return types.MinFsp
	case types.ETReal, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson, types.ETString:
		trace_util_0.Count(_builtin_time_00000, 1132)
		return types.MaxFsp
	case types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 1133)
		if tp.Decimal < types.MaxFsp {
			trace_util_0.Count(_builtin_time_00000, 1135)
			return tp.Decimal
		}
		trace_util_0.Count(_builtin_time_00000, 1134)
		return types.MaxFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1129)
	return types.MaxFsp
}

func (c *timestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1136)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1143)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1137)
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		trace_util_0.Count(_builtin_time_00000, 1144)
		evalTps = append(evalTps, types.ETString)
	}
	trace_util_0.Count(_builtin_time_00000, 1138)
	fsp := c.getDefaultFsp(args[0].GetType())
	if argLen == 2 {
		trace_util_0.Count(_builtin_time_00000, 1145)
		fsp = mathutil.Max(fsp, c.getDefaultFsp(args[1].GetType()))
	}
	trace_util_0.Count(_builtin_time_00000, 1139)
	isFloat := false
	switch args[0].GetType().Tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDecimal:
		trace_util_0.Count(_builtin_time_00000, 1146)
		isFloat = true
	}
	trace_util_0.Count(_builtin_time_00000, 1140)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, evalTps...)
	bf.tp.Decimal, bf.tp.Flen = fsp, 19
	if fsp != 0 {
		trace_util_0.Count(_builtin_time_00000, 1147)
		bf.tp.Flen += 1 + fsp
	}
	trace_util_0.Count(_builtin_time_00000, 1141)
	var sig builtinFunc
	if argLen == 2 {
		trace_util_0.Count(_builtin_time_00000, 1148)
		sig = &builtinTimestamp2ArgsSig{bf, isFloat}
	} else {
		trace_util_0.Count(_builtin_time_00000, 1149)
		{
			sig = &builtinTimestamp1ArgSig{bf, isFloat}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1142)
	return sig, nil
}

type builtinTimestamp1ArgSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp1ArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1150)
	newSig := &builtinTimestamp1ArgSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1151)
	s, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1155)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1152)
	var tm types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		trace_util_0.Count(_builtin_time_00000, 1156)
		tm, err = types.ParseTimeFromFloatString(sc, s, mysql.TypeDatetime, types.GetFsp(s))
	} else {
		trace_util_0.Count(_builtin_time_00000, 1157)
		{
			tm, err = types.ParseTime(sc, s, mysql.TypeDatetime, types.GetFsp(s))
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1153)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1158)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1154)
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp2ArgsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1159)
	newSig := &builtinTimestamp2ArgsSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1160)
	arg0, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1168)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1161)
	var tm types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		trace_util_0.Count(_builtin_time_00000, 1169)
		tm, err = types.ParseTimeFromFloatString(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	} else {
		trace_util_0.Count(_builtin_time_00000, 1170)
		{
			tm, err = types.ParseTime(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1162)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1171)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1163)
	arg1, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1172)
		return types.Time{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1164)
	if !isDuration(arg1) {
		trace_util_0.Count(_builtin_time_00000, 1173)
		return types.Time{}, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1165)
	duration, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1174)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1166)
	tmp, err := tm.Add(sc, duration)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1175)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1167)
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timestampLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1176)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1184)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1177)
	con, ok := args[0].(*Constant)
	if !ok {
		trace_util_0.Count(_builtin_time_00000, 1185)
		panic("Unexpected parameter for timestamp literal")
	}
	trace_util_0.Count(_builtin_time_00000, 1178)
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1186)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1179)
	str, err := dt.ToString()
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1187)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1180)
	if !timestampPattern.MatchString(str) {
		trace_util_0.Count(_builtin_time_00000, 1188)
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	trace_util_0.Count(_builtin_time_00000, 1181)
	tm, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, str, mysql.TypeTimestamp, types.GetFsp(str))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1189)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1182)
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, tm.Fsp
	if tm.Fsp > 0 {
		trace_util_0.Count(_builtin_time_00000, 1190)
		bf.tp.Flen += tm.Fsp + 1
	}
	trace_util_0.Count(_builtin_time_00000, 1183)
	sig := &builtinTimestampLiteralSig{bf, tm}
	return sig, nil
}

type builtinTimestampLiteralSig struct {
	baseBuiltinFunc
	tm types.Time
}

func (b *builtinTimestampLiteralSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1191)
	newSig := &builtinTimestampLiteralSig{tm: b.tm}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1192)
	return b.tm, false, nil
}

// getFsp4TimeAddSub is used to in function 'ADDTIME' and 'SUBTIME' to evaluate `fsp` for the
// second parameter. It's used only if the second parameter is of string type. It's different
// from getFsp in that the result of getFsp4TimeAddSub is either 6 or 0.
func getFsp4TimeAddSub(s string) int {
	trace_util_0.Count(_builtin_time_00000, 1193)
	if len(s)-strings.Index(s, ".")-1 == len(s) {
		trace_util_0.Count(_builtin_time_00000, 1196)
		return types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1194)
	for _, c := range s[strings.Index(s, ".")+1:] {
		trace_util_0.Count(_builtin_time_00000, 1197)
		if c != '0' {
			trace_util_0.Count(_builtin_time_00000, 1198)
			return types.MaxFsp
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1195)
	return types.MinFsp
}

// getBf4TimeAddSub parses input types, generates baseBuiltinFunc and set related attributes for
// builtin function 'ADDTIME' and 'SUBTIME'
func getBf4TimeAddSub(ctx sessionctx.Context, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 1199)
	tp1, tp2 = args[0].GetType(), args[1].GetType()
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_builtin_time_00000, 1205)
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		trace_util_0.Count(_builtin_time_00000, 1206)
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		trace_util_0.Count(_builtin_time_00000, 1207)
		argTp1, retTp = types.ETDuration, types.ETString
	default:
		trace_util_0.Count(_builtin_time_00000, 1208)
		argTp1, retTp = types.ETString, types.ETString
	}
	trace_util_0.Count(_builtin_time_00000, 1200)
	switch tp2.Tp {
	case mysql.TypeDatetime, mysql.TypeDuration:
		trace_util_0.Count(_builtin_time_00000, 1209)
		argTp2 = types.ETDuration
	default:
		trace_util_0.Count(_builtin_time_00000, 1210)
		argTp2 = types.ETString
	}
	trace_util_0.Count(_builtin_time_00000, 1201)
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1211)
		return
	}
	trace_util_0.Count(_builtin_time_00000, 1202)
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1212)
		return
	}

	trace_util_0.Count(_builtin_time_00000, 1203)
	bf = newBaseBuiltinFuncWithTp(ctx, args, retTp, argTp1, argTp2)
	bf.tp.Decimal = mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), types.MaxFsp)
	if retTp == types.ETString {
		trace_util_0.Count(_builtin_time_00000, 1213)
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeString, mysql.MaxDatetimeWidthWithFsp, types.UnspecifiedLength
	}
	trace_util_0.Count(_builtin_time_00000, 1204)
	return
}

func getTimeZone(ctx sessionctx.Context) *time.Location {
	trace_util_0.Count(_builtin_time_00000, 1214)
	ret := ctx.GetSessionVars().TimeZone
	if ret == nil {
		trace_util_0.Count(_builtin_time_00000, 1216)
		ret = time.Local
	}
	trace_util_0.Count(_builtin_time_00000, 1215)
	return ret
}

// isDuration returns a boolean indicating whether the str matches the format of duration.
// See https://dev.mysql.com/doc/refman/5.7/en/time.html
func isDuration(str string) bool {
	trace_util_0.Count(_builtin_time_00000, 1217)
	return durationPattern.MatchString(str)
}

// strDatetimeAddDuration adds duration to datetime string, returns a string value.
func strDatetimeAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	trace_util_0.Count(_builtin_time_00000, 1218)
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1222)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1219)
	ret, err := arg0.Add(sc, arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1223)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1220)
	fsp := types.MaxFsp
	if ret.Time.Microsecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 1224)
		fsp = types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1221)
	ret.Fsp = fsp
	return ret.String(), nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	trace_util_0.Count(_builtin_time_00000, 1225)
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1229)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1226)
	tmpDuration, err := arg0.Add(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1230)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1227)
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 1231)
		tmpDuration.Fsp = types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1228)
	return tmpDuration.String(), nil
}

// strDatetimeSubDuration subtracts duration from datetime string, returns a string value.
func strDatetimeSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	trace_util_0.Count(_builtin_time_00000, 1232)
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1237)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1233)
	arg1time, err := arg1.ConvertToTime(sc, uint8(types.GetFsp(arg1.String())))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1238)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1234)
	tmpDuration := arg0.Sub(sc, &arg1time)
	fsp := types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 1239)
		fsp = types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1235)
	resultDuration, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1240)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1236)
	resultDuration.Fsp = fsp
	return resultDuration.String(), nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	trace_util_0.Count(_builtin_time_00000, 1241)
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1245)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1242)
	tmpDuration, err := arg0.Sub(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1246)
		return "", err
	}
	trace_util_0.Count(_builtin_time_00000, 1243)
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		trace_util_0.Count(_builtin_time_00000, 1247)
		tmpDuration.Fsp = types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1244)
	return tmpDuration.String(), nil
}

type addTimeFunctionClass struct {
	baseFunctionClass
}

func (c *addTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 1248)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1252)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1249)
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1253)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1250)
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_builtin_time_00000, 1254)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1258)
			sig = &builtinAddDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1259)
			sig = &builtinAddTimeDateTimeNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1260)
			sig = &builtinAddDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		trace_util_0.Count(_builtin_time_00000, 1255)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1261)
			sig = &builtinAddDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1262)
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1263)
			sig = &builtinAddDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		trace_util_0.Count(_builtin_time_00000, 1256)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1264)
			sig = &builtinAddDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1265)
			sig = &builtinAddTimeDurationNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1266)
			sig = &builtinAddDurationAndStringSig{bf}
		}
	default:
		trace_util_0.Count(_builtin_time_00000, 1257)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1267)
			sig = &builtinAddStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1268)
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1269)
			sig = &builtinAddStringAndStringSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1251)
	return sig, nil
}

type builtinAddTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeDateTimeNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1270)
	newSig := &builtinAddTimeDateTimeNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1271)
	return types.ZeroDatetime, true, nil
}

type builtinAddDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDatetimeAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1272)
	newSig := &builtinAddDatetimeAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1273)
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1276)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1274)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1277)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1275)
	result, err := arg0.Add(b.ctx.GetSessionVars().StmtCtx, arg1)
	return result, err != nil, err
}

type builtinAddDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDatetimeAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1278)
	newSig := &builtinAddDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1279)
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1284)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1280)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1285)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1281)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1286)
		return types.ZeroDatetime, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1282)
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1287)
		return types.ZeroDatetime, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1283)
	result, err := arg0.Add(sc, arg1)
	return result, err != nil, err
}

type builtinAddTimeDurationNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeDurationNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1288)
	newSig := &builtinAddTimeDurationNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1289)
	return types.ZeroDuration, true, nil
}

type builtinAddDurationAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDurationAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1290)
	newSig := &builtinAddDurationAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1291)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1295)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1292)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1296)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1293)
	result, err := arg0.Add(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1297)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1294)
	return result, false, nil
}

type builtinAddDurationAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDurationAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1298)
	newSig := &builtinAddDurationAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1299)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1305)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1300)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1306)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1301)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1307)
		return types.ZeroDuration, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1302)
	arg1, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, types.GetFsp(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1308)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1303)
	result, err := arg0.Add(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1309)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1304)
	return result, false, nil
}

type builtinAddTimeStringNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeStringNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1310)
	newSig := &builtinAddTimeStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeStringNullSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1311)
	return "", true, nil
}

type builtinAddStringAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddStringAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1312)
	newSig := &builtinAddStringAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndDurationSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 1313)
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1317)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1314)
	arg1, isNull, err = b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1318)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1315)
	sc := b.ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		trace_util_0.Count(_builtin_time_00000, 1319)
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1321)
			return "", true, err
		}
		trace_util_0.Count(_builtin_time_00000, 1320)
		return result, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1316)
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddStringAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddStringAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1322)
	newSig := &builtinAddStringAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndStringSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 1323)
	var (
		arg0, arg1Str string
		arg1          types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1329)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1324)
	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		trace_util_0.Count(_builtin_time_00000, 1330)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1325)
	arg1Str, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1331)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1326)
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err = types.ParseDuration(sc, arg1Str, getFsp4TimeAddSub(arg1Str))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1332)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1327)
	if isDuration(arg0) {
		trace_util_0.Count(_builtin_time_00000, 1333)
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1335)
			return "", true, err
		}
		trace_util_0.Count(_builtin_time_00000, 1334)
		return result, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1328)
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddDateAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDateAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1336)
	newSig := &builtinAddDateAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndDurationSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1337)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1340)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1338)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1341)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1339)
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type builtinAddDateAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDateAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1342)
	newSig := &builtinAddDateAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1343)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1348)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1344)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1349)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1345)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1350)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1346)
	arg1, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, getFsp4TimeAddSub(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1351)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1347)
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type convertTzFunctionClass struct {
	baseFunctionClass
}

func (c *convertTzFunctionClass) getDecimal(ctx sessionctx.Context, arg Expression) int {
	trace_util_0.Count(_builtin_time_00000, 1352)
	decimal := types.MaxFsp
	if dt, isConstant := arg.(*Constant); isConstant {
		trace_util_0.Count(_builtin_time_00000, 1356)
		switch arg.GetType().EvalType() {
		case types.ETInt:
			trace_util_0.Count(_builtin_time_00000, 1357)
			decimal = 0
		case types.ETReal, types.ETDecimal:
			trace_util_0.Count(_builtin_time_00000, 1358)
			decimal = arg.GetType().Decimal
		case types.ETString:
			trace_util_0.Count(_builtin_time_00000, 1359)
			str, isNull, err := dt.EvalString(ctx, chunk.Row{})
			if err == nil && !isNull {
				trace_util_0.Count(_builtin_time_00000, 1360)
				decimal = types.DateFSP(str)
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1353)
	if decimal > types.MaxFsp {
		trace_util_0.Count(_builtin_time_00000, 1361)
		return types.MaxFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1354)
	if decimal < types.MinFsp {
		trace_util_0.Count(_builtin_time_00000, 1362)
		return types.MinFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1355)
	return decimal
}

func (c *convertTzFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1363)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1366)
		return nil, err
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	trace_util_0.Count(_builtin_time_00000, 1364)
	tzRegex, err := regexp.Compile(`(^(\+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^\+13:00$)`)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1367)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 1365)
	decimal := c.getDecimal(ctx, args[0])
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	bf.tp.Decimal = decimal
	sig := &builtinConvertTzSig{
		baseBuiltinFunc: bf,
		timezoneRegex:   tzRegex,
	}
	return sig, nil
}

type builtinConvertTzSig struct {
	baseBuiltinFunc
	timezoneRegex *regexp.Regexp
}

func (b *builtinConvertTzSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1368)
	newSig := &builtinConvertTzSig{timezoneRegex: b.timezoneRegex}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1369)
	dt, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1375)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1370)
	fromTzStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1376)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1371)
	toTzStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1377)
		return types.Time{}, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1372)
	fromTzMatched := b.timezoneRegex.MatchString(fromTzStr)
	toTzMatched := b.timezoneRegex.MatchString(toTzStr)

	if !fromTzMatched && !toTzMatched {
		trace_util_0.Count(_builtin_time_00000, 1378)
		fromTz, err := time.LoadLocation(fromTzStr)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1382)
			return types.Time{}, true, err
		}

		trace_util_0.Count(_builtin_time_00000, 1379)
		toTz, err := time.LoadLocation(toTzStr)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1383)
			return types.Time{}, true, err
		}

		trace_util_0.Count(_builtin_time_00000, 1380)
		t, err := dt.Time.GoTime(fromTz)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1384)
			return types.Time{}, true, err
		}

		trace_util_0.Count(_builtin_time_00000, 1381)
		return types.Time{
			Time: types.FromGoTime(t.In(toTz)),
			Type: mysql.TypeDatetime,
			Fsp:  b.tp.Decimal,
		}, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1373)
	if fromTzMatched && toTzMatched {
		trace_util_0.Count(_builtin_time_00000, 1385)
		t, err := dt.Time.GoTime(time.Local)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1387)
			return types.Time{}, true, err
		}

		trace_util_0.Count(_builtin_time_00000, 1386)
		return types.Time{
			Time: types.FromGoTime(t.Add(timeZone2Duration(toTzStr) - timeZone2Duration(fromTzStr))),
			Type: mysql.TypeDatetime,
			Fsp:  b.tp.Decimal,
		}, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1374)
	return types.Time{}, true, nil
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1388)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1390)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1389)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt, types.ETInt)
	tp := bf.tp
	tp.Tp, tp.Flen, tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, 0
	sig := &builtinMakeDateSig{bf}
	return sig, nil
}

type builtinMakeDateSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeDateSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1391)
	newSig := &builtinMakeDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 1392)
	args := b.getArgs()
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1399)
		return d, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1393)
	dayOfYear, isNull, err = args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1400)
		return d, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1394)
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		trace_util_0.Count(_builtin_time_00000, 1401)
		return d, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1395)
	if year < 70 {
		trace_util_0.Count(_builtin_time_00000, 1402)
		year += 2000
	} else {
		trace_util_0.Count(_builtin_time_00000, 1403)
		if year < 100 {
			trace_util_0.Count(_builtin_time_00000, 1404)
			year += 1900
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1396)
	startTime := types.Time{
		Time: types.FromDate(int(year), 1, 1, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0,
	}
	retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
	if retTimestamp == 0 {
		trace_util_0.Count(_builtin_time_00000, 1405)
		return d, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(startTime.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 1397)
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Time.Year() > 9999 {
		trace_util_0.Count(_builtin_time_00000, 1406)
		return d, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1398)
	return ret, false, nil
}

type makeTimeFunctionClass struct {
	baseFunctionClass
}

func (c *makeTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1407)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1412)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1408)
	tp, flen, decimal := args[2].GetType().EvalType(), 10, 0
	switch tp {
	case types.ETInt:
		trace_util_0.Count(_builtin_time_00000, 1413)
	case types.ETReal, types.ETDecimal:
		trace_util_0.Count(_builtin_time_00000, 1414)
		decimal = args[2].GetType().Decimal
		if decimal > 6 || decimal == types.UnspecifiedLength {
			trace_util_0.Count(_builtin_time_00000, 1417)
			decimal = 6
		}
		trace_util_0.Count(_builtin_time_00000, 1415)
		if decimal > 0 {
			trace_util_0.Count(_builtin_time_00000, 1418)
			flen += 1 + decimal
		}
	default:
		trace_util_0.Count(_builtin_time_00000, 1416)
		flen, decimal = 17, 6
	}
	trace_util_0.Count(_builtin_time_00000, 1409)
	arg0Type, arg1Type := args[0].GetType().EvalType(), args[1].GetType().EvalType()
	// For ETString type, arg must be evaluated rounding down to int64
	// For other types, arg is evaluated rounding to int64
	if arg0Type == types.ETString {
		trace_util_0.Count(_builtin_time_00000, 1419)
		arg0Type = types.ETReal
	} else {
		trace_util_0.Count(_builtin_time_00000, 1420)
		{
			arg0Type = types.ETInt
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1410)
	if arg1Type == types.ETString {
		trace_util_0.Count(_builtin_time_00000, 1421)
		arg1Type = types.ETReal
	} else {
		trace_util_0.Count(_builtin_time_00000, 1422)
		{
			arg1Type = types.ETInt
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1411)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Type, arg1Type, types.ETReal)
	bf.tp.Flen, bf.tp.Decimal = flen, decimal
	sig := &builtinMakeTimeSig{bf}
	return sig, nil
}

type builtinMakeTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1423)
	newSig := &builtinMakeTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMakeTimeSig) getIntParam(arg Expression, row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1424)
	if arg.GetType().EvalType() == types.ETReal {
		trace_util_0.Count(_builtin_time_00000, 1426)
		fRes, isNull, err := arg.EvalReal(b.ctx, row)
		return int64(fRes), isNull, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1425)
	iRes, isNull, err := arg.EvalInt(b.ctx, row)
	return iRes, isNull, handleInvalidTimeError(b.ctx, err)
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1427)
	dur := types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.getIntParam(b.args[0], row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1438)
		return dur, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1428)
	minute, isNull, err := b.getIntParam(b.args[1], row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1439)
		return dur, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1429)
	if minute < 0 || minute >= 60 {
		trace_util_0.Count(_builtin_time_00000, 1440)
		return dur, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1430)
	second, isNull, err := b.args[2].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1441)
		return dur, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1431)
	if second < 0 || second >= 60 {
		trace_util_0.Count(_builtin_time_00000, 1442)
		return dur, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1432)
	var overflow bool
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < 0 && mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		trace_util_0.Count(_builtin_time_00000, 1443)
		hour = 838
		overflow = true
	}
	trace_util_0.Count(_builtin_time_00000, 1433)
	if hour < -838 {
		trace_util_0.Count(_builtin_time_00000, 1444)
		hour = -838
		overflow = true
	} else {
		trace_util_0.Count(_builtin_time_00000, 1445)
		if hour > 838 {
			trace_util_0.Count(_builtin_time_00000, 1446)
			hour = 838
			overflow = true
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1434)
	if hour == -838 || hour == 838 {
		trace_util_0.Count(_builtin_time_00000, 1447)
		if second > 59 {
			trace_util_0.Count(_builtin_time_00000, 1448)
			second = 59
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1435)
	if overflow {
		trace_util_0.Count(_builtin_time_00000, 1449)
		minute = 59
		second = 59
	}
	trace_util_0.Count(_builtin_time_00000, 1436)
	fsp := b.tp.Decimal
	dur, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%02d:%02d:%v", hour, minute, second), fsp)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1450)
		return dur, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1437)
	return dur, false, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1451)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1453)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 1452)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodAddSig{bf}
	return sig, nil
}

// validPeriod checks if this period is valid, it comes from MySQL 8.0+.
func validPeriod(p int64) bool {
	trace_util_0.Count(_builtin_time_00000, 1454)
	return !(p < 0 || p%100 == 0 || p%100 > 12)
}

// period2Month converts a period to months, in which period is represented in the format of YYMM or YYYYMM.
// Note that the period argument is not a date value.
func period2Month(period uint64) uint64 {
	trace_util_0.Count(_builtin_time_00000, 1455)
	if period == 0 {
		trace_util_0.Count(_builtin_time_00000, 1458)
		return 0
	}

	trace_util_0.Count(_builtin_time_00000, 1456)
	year, month := period/100, period%100
	if year < 70 {
		trace_util_0.Count(_builtin_time_00000, 1459)
		year += 2000
	} else {
		trace_util_0.Count(_builtin_time_00000, 1460)
		if year < 100 {
			trace_util_0.Count(_builtin_time_00000, 1461)
			year += 1900
		}
	}

	trace_util_0.Count(_builtin_time_00000, 1457)
	return year*12 + month - 1
}

// month2Period converts a month to a period.
func month2Period(month uint64) uint64 {
	trace_util_0.Count(_builtin_time_00000, 1462)
	if month == 0 {
		trace_util_0.Count(_builtin_time_00000, 1465)
		return 0
	}

	trace_util_0.Count(_builtin_time_00000, 1463)
	year := month / 12
	if year < 70 {
		trace_util_0.Count(_builtin_time_00000, 1466)
		year += 2000
	} else {
		trace_util_0.Count(_builtin_time_00000, 1467)
		if year < 100 {
			trace_util_0.Count(_builtin_time_00000, 1468)
			year += 1900
		}
	}

	trace_util_0.Count(_builtin_time_00000, 1464)
	return year*100 + month%12 + 1
}

type builtinPeriodAddSig struct {
	baseBuiltinFunc
}

func (b *builtinPeriodAddSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1469)
	newSig := &builtinPeriodAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1470)
	p, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1474)
		return 0, true, err
	}

	trace_util_0.Count(_builtin_time_00000, 1471)
	n, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1475)
		return 0, true, err
	}

	// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
	trace_util_0.Count(_builtin_time_00000, 1472)
	if !validPeriod(p) {
		trace_util_0.Count(_builtin_time_00000, 1476)
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_add")
	}

	trace_util_0.Count(_builtin_time_00000, 1473)
	sumMonth := int64(period2Month(uint64(p))) + n
	return int64(month2Period(uint64(sumMonth))), false, nil
}

type periodDiffFunctionClass struct {
	baseFunctionClass
}

func (c *periodDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1477)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1479)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1478)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodDiffSig{bf}
	return sig, nil
}

type builtinPeriodDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinPeriodDiffSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1480)
	newSig := &builtinPeriodDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1481)
	p1, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1486)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 1482)
	p2, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1487)
		return 0, isNull, err
	}

	trace_util_0.Count(_builtin_time_00000, 1483)
	if !validPeriod(p1) {
		trace_util_0.Count(_builtin_time_00000, 1488)
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	trace_util_0.Count(_builtin_time_00000, 1484)
	if !validPeriod(p2) {
		trace_util_0.Count(_builtin_time_00000, 1489)
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	trace_util_0.Count(_builtin_time_00000, 1485)
	return int64(period2Month(uint64(p1)) - period2Month(uint64(p2))), false, nil
}

type quarterFunctionClass struct {
	baseFunctionClass
}

func (c *quarterFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1490)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1492)
		return nil, err
	}

	trace_util_0.Count(_builtin_time_00000, 1491)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1

	sig := &builtinQuarterSig{bf}
	return sig, nil
}

type builtinQuarterSig struct {
	baseBuiltinFunc
}

func (b *builtinQuarterSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1493)
	newSig := &builtinQuarterSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1494)
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1497)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 1495)
	if date.IsZero() {
		trace_util_0.Count(_builtin_time_00000, 1498)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	trace_util_0.Count(_builtin_time_00000, 1496)
	return int64((date.Time.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1499)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1504)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1500)
	var retFlen, retFsp int
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	if argEvalTp == types.ETString {
		trace_util_0.Count(_builtin_time_00000, 1505)
		retFsp = types.UnspecifiedLength
	} else {
		trace_util_0.Count(_builtin_time_00000, 1506)
		{
			retFsp = argType.Decimal
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1501)
	if retFsp > types.MaxFsp || retFsp == types.UnspecifiedLength {
		trace_util_0.Count(_builtin_time_00000, 1507)
		retFsp = types.MaxFsp
	} else {
		trace_util_0.Count(_builtin_time_00000, 1508)
		if retFsp < types.MinFsp {
			trace_util_0.Count(_builtin_time_00000, 1509)
			retFsp = types.MinFsp
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1502)
	retFlen = 10
	if retFsp > 0 {
		trace_util_0.Count(_builtin_time_00000, 1510)
		retFlen += 1 + retFsp
	}
	trace_util_0.Count(_builtin_time_00000, 1503)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETReal)
	bf.tp.Flen, bf.tp.Decimal = retFlen, retFsp
	sig := &builtinSecToTimeSig{bf}
	return sig, nil
}

type builtinSecToTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinSecToTimeSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1511)
	newSig := &builtinSecToTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1512)
	secondsFloat, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1517)
		return types.Duration{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1513)
	var (
		hour          int64
		minute        int64
		second        int64
		demical       float64
		secondDemical float64
		negative      string
	)

	if secondsFloat < 0 {
		trace_util_0.Count(_builtin_time_00000, 1518)
		negative = "-"
		secondsFloat = math.Abs(secondsFloat)
	}
	trace_util_0.Count(_builtin_time_00000, 1514)
	seconds := int64(secondsFloat)
	demical = secondsFloat - float64(seconds)

	hour = seconds / 3600
	if hour > 838 {
		trace_util_0.Count(_builtin_time_00000, 1519)
		hour = 838
		minute = 59
		second = 59
	} else {
		trace_util_0.Count(_builtin_time_00000, 1520)
		{
			minute = seconds % 3600 / 60
			second = seconds % 60
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1515)
	secondDemical = float64(second) + demical

	var dur types.Duration
	dur, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%s%02d:%02d:%v", negative, hour, minute, secondDemical), b.tp.Decimal)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1521)
		return types.Duration{}, err != nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1516)
	return dur, false, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	trace_util_0.Count(_builtin_time_00000, 1522)
	if err = c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1526)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1523)
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1527)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1524)
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_builtin_time_00000, 1528)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1532)
			sig = &builtinSubDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1533)
			sig = &builtinSubTimeDateTimeNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1534)
			sig = &builtinSubDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		trace_util_0.Count(_builtin_time_00000, 1529)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1535)
			sig = &builtinSubDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1536)
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1537)
			sig = &builtinSubDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		trace_util_0.Count(_builtin_time_00000, 1530)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1538)
			sig = &builtinSubDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1539)
			sig = &builtinSubTimeDurationNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1540)
			sig = &builtinSubDurationAndStringSig{bf}
		}
	default:
		trace_util_0.Count(_builtin_time_00000, 1531)
		switch tp2.Tp {
		case mysql.TypeDuration:
			trace_util_0.Count(_builtin_time_00000, 1541)
			sig = &builtinSubStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_builtin_time_00000, 1542)
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			trace_util_0.Count(_builtin_time_00000, 1543)
			sig = &builtinSubStringAndStringSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1525)
	return sig, nil
}

type builtinSubDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDatetimeAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1544)
	newSig := &builtinSubDatetimeAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1545)
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1549)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1546)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1550)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1547)
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1551)
		return arg1time, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1548)
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, arg0.Type)
	return result, err != nil, err
}

type builtinSubDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDatetimeAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1552)
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1553)
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1560)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1554)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1561)
		return types.ZeroDatetime, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1555)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1562)
		return types.ZeroDatetime, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1556)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1563)
		return types.ZeroDatetime, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1557)
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1564)
		return types.ZeroDatetime, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1558)
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1565)
		return types.ZeroDatetime, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1559)
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	return result, err != nil, err
}

type builtinSubTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeDateTimeNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1566)
	newSig := &builtinSubTimeDateTimeNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1567)
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubStringAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1568)
	newSig := &builtinSubStringAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 1569)
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1573)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1570)
	arg1, isNull, err = b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1574)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1571)
	sc := b.ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		trace_util_0.Count(_builtin_time_00000, 1575)
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1577)
			return "", true, err
		}
		trace_util_0.Count(_builtin_time_00000, 1576)
		return result, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1572)
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubStringAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubStringAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1578)
	newSig := &builtinSubStringAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	trace_util_0.Count(_builtin_time_00000, 1579)
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1585)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1580)
	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		trace_util_0.Count(_builtin_time_00000, 1586)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1581)
	s, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1587)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1582)
	arg1, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, getFsp4TimeAddSub(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1588)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1583)
	sc := b.ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		trace_util_0.Count(_builtin_time_00000, 1589)
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			trace_util_0.Count(_builtin_time_00000, 1591)
			return "", true, err
		}
		trace_util_0.Count(_builtin_time_00000, 1590)
		return result, false, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1584)
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubTimeStringNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeStringNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1592)
	newSig := &builtinSubTimeStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1593)
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDurationAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1594)
	newSig := &builtinSubDurationAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1595)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1599)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1596)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1600)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1597)
	result, err := arg0.Sub(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1601)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1598)
	return result, false, nil
}

type builtinSubDurationAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDurationAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1602)
	newSig := &builtinSubDurationAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1603)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1608)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1604)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1609)
		return types.ZeroDuration, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1605)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1610)
		return types.ZeroDuration, true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1606)
	arg1, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, types.GetFsp(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1611)
		return types.ZeroDuration, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1607)
	result, err := arg0.Sub(arg1)
	return result, err != nil, err
}

type builtinSubTimeDurationNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeDurationNullSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1612)
	newSig := &builtinSubTimeDurationNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1613)
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDateAndDurationSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1614)
	newSig := &builtinSubDateAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1615)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1618)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1616)
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1619)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1617)
	result, err := arg0.Sub(arg1)
	return result.String(), err != nil, err
}

type builtinSubDateAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDateAndStringSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1620)
	newSig := &builtinSubDateAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1621)
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1627)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1622)
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1628)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1623)
	if !isDuration(s) {
		trace_util_0.Count(_builtin_time_00000, 1629)
		return "", true, nil
	}
	trace_util_0.Count(_builtin_time_00000, 1624)
	arg1, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, getFsp4TimeAddSub(s))
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1630)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1625)
	result, err := arg0.Sub(arg1)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1631)
		return "", true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1626)
	return result.String(), false, nil
}

type timeFormatFunctionClass struct {
	baseFunctionClass
}

func (c *timeFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1632)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1634)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1633)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDuration, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinTimeFormatSig{bf}
	return sig, nil
}

type builtinTimeFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeFormatSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1635)
	newSig := &builtinTimeFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1636)
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1640)
		logutil.Logger(context.Background()).Warn("time_format.args[0].EvalDuration failed", zap.Error(err))
	}
	trace_util_0.Count(_builtin_time_00000, 1637)
	if isNull {
		trace_util_0.Count(_builtin_time_00000, 1641)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1638)
	formatMask, isNull, err := b.args[1].EvalString(b.ctx, row)
	if err != nil || isNull {
		trace_util_0.Count(_builtin_time_00000, 1642)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1639)
	res, err := b.formatTime(b.ctx, dur, formatMask)
	return res, isNull, err
}

// formatTime see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(ctx sessionctx.Context, t types.Duration, formatMask string) (res string, err error) {
	trace_util_0.Count(_builtin_time_00000, 1643)
	t2 := types.Time{
		Time: types.FromDate(0, 0, 0, t.Hour(), t.Minute(), t.Second(), t.MicroSecond()),
		Type: mysql.TypeDate, Fsp: 0}

	str, err := t2.DateFormat(formatMask)
	return str, err
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1644)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1646)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1645)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen = 10
	sig := &builtinTimeToSecSig{bf}
	return sig, nil
}

type builtinTimeToSecSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeToSecSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1647)
	newSig := &builtinTimeToSecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1648)
	duration, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1651)
		return 0, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1649)
	var sign int
	if duration.Duration >= 0 {
		trace_util_0.Count(_builtin_time_00000, 1652)
		sign = 1
	} else {
		trace_util_0.Count(_builtin_time_00000, 1653)
		{
			sign = -1
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1650)
	return int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second())), false, nil
}

type timestampAddFunctionClass struct {
	baseFunctionClass
}

func (c *timestampAddFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1654)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1656)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1655)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETDatetime)
	bf.tp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxDatetimeWidthNoFsp, Decimal: types.UnspecifiedLength}
	sig := &builtinTimestampAddSig{bf}
	return sig, nil

}

type builtinTimestampAddSig struct {
	baseBuiltinFunc
}

func (b *builtinTimestampAddSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1657)
	newSig := &builtinTimestampAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1658)
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1665)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1659)
	v, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1666)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1660)
	arg, isNull, err := b.args[2].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1667)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1661)
	tm1, err := arg.Time.GoTime(time.Local)
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1668)
		return "", isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1662)
	var tb time.Time
	fsp := types.DefaultFsp
	switch unit {
	case "MICROSECOND":
		trace_util_0.Count(_builtin_time_00000, 1669)
		tb = tm1.Add(time.Duration(v) * time.Microsecond)
		fsp = types.MaxFsp
	case "SECOND":
		trace_util_0.Count(_builtin_time_00000, 1670)
		tb = tm1.Add(time.Duration(v) * time.Second)
	case "MINUTE":
		trace_util_0.Count(_builtin_time_00000, 1671)
		tb = tm1.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		trace_util_0.Count(_builtin_time_00000, 1672)
		tb = tm1.Add(time.Duration(v) * time.Hour)
	case "DAY":
		trace_util_0.Count(_builtin_time_00000, 1673)
		tb = tm1.AddDate(0, 0, int(v))
	case "WEEK":
		trace_util_0.Count(_builtin_time_00000, 1674)
		tb = tm1.AddDate(0, 0, 7*int(v))
	case "MONTH":
		trace_util_0.Count(_builtin_time_00000, 1675)
		tb = tm1.AddDate(0, int(v), 0)
	case "QUARTER":
		trace_util_0.Count(_builtin_time_00000, 1676)
		tb = tm1.AddDate(0, 3*int(v), 0)
	case "YEAR":
		trace_util_0.Count(_builtin_time_00000, 1677)
		tb = tm1.AddDate(int(v), 0, 0)
	default:
		trace_util_0.Count(_builtin_time_00000, 1678)
		return "", true, types.ErrInvalidTimeFormat.GenWithStackByArgs(unit)
	}
	trace_util_0.Count(_builtin_time_00000, 1663)
	r := types.Time{Time: types.FromGoTime(tb), Type: b.resolveType(arg.Type, unit), Fsp: fsp}
	if err = r.Check(b.ctx.GetSessionVars().StmtCtx); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1679)
		return "", true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1664)
	return r.String(), false, nil
}

func (b *builtinTimestampAddSig) resolveType(typ uint8, unit string) uint8 {
	trace_util_0.Count(_builtin_time_00000, 1680)
	// The approach below is from MySQL.
	// The field type for the result of an Item_date function is defined as
	// follows:
	//
	//- If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
	//- If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
	//	minutes, seconds or microsecond then type is MYSQL_TYPE_DATETIME.
	//- Otherwise the result is MYSQL_TYPE_STRING
	//	(This is because you can't know if the string contains a DATE, MYSQL_TIME
	//	or DATETIME argument)
	if typ == mysql.TypeDate && (unit == "HOUR" || unit == "MINUTE" || unit == "SECOND" || unit == "MICROSECOND") {
		trace_util_0.Count(_builtin_time_00000, 1682)
		return mysql.TypeDatetime
	}
	trace_util_0.Count(_builtin_time_00000, 1681)
	return typ
}

type toDaysFunctionClass struct {
	baseFunctionClass
}

func (c *toDaysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1683)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1685)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1684)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToDaysSig{bf}
	return sig, nil
}

type builtinToDaysSig struct {
	baseBuiltinFunc
}

func (b *builtinToDaysSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1686)
	newSig := &builtinToDaysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1687)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1690)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1688)
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		trace_util_0.Count(_builtin_time_00000, 1691)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 1689)
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1692)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1694)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1693)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToSecondsSig{bf}
	return sig, nil
}

type builtinToSecondsSig struct {
	baseBuiltinFunc
}

func (b *builtinToSecondsSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1695)
	newSig := &builtinToSecondsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1696)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1699)
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1697)
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		trace_util_0.Count(_builtin_time_00000, 1700)
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 1698)
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFlenAndDecimal4UTCTime(ctx sessionctx.Context, args []Expression) (flen, decimal int) {
	trace_util_0.Count(_builtin_time_00000, 1701)
	if len(args) == 0 {
		trace_util_0.Count(_builtin_time_00000, 1705)
		flen, decimal = 8, 0
		return
	}
	trace_util_0.Count(_builtin_time_00000, 1702)
	if constant, ok := args[0].(*Constant); ok {
		trace_util_0.Count(_builtin_time_00000, 1706)
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			trace_util_0.Count(_builtin_time_00000, 1707)
			decimal = types.MaxFsp
		} else {
			trace_util_0.Count(_builtin_time_00000, 1708)
			if fsp < int64(types.MinFsp) {
				trace_util_0.Count(_builtin_time_00000, 1709)
				decimal = types.MinFsp
			} else {
				trace_util_0.Count(_builtin_time_00000, 1710)
				{
					decimal = int(fsp)
				}
			}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1703)
	if decimal > 0 {
		trace_util_0.Count(_builtin_time_00000, 1711)
		flen = 8 + 1 + decimal
	} else {
		trace_util_0.Count(_builtin_time_00000, 1712)
		{
			flen = 8
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1704)
	return flen, decimal
}

func (c *utcTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1713)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1717)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1714)
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 1718)
		argTps = append(argTps, types.ETInt)
	}
	trace_util_0.Count(_builtin_time_00000, 1715)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
	bf.tp.Flen, bf.tp.Decimal = c.getFlenAndDecimal4UTCTime(bf.ctx, args)

	var sig builtinFunc
	if len(args) == 1 {
		trace_util_0.Count(_builtin_time_00000, 1719)
		sig = &builtinUTCTimeWithArgSig{bf}
	} else {
		trace_util_0.Count(_builtin_time_00000, 1720)
		{
			sig = &builtinUTCTimeWithoutArgSig{bf}
		}
	}
	trace_util_0.Count(_builtin_time_00000, 1716)
	return sig, nil
}

type builtinUTCTimeWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimeWithoutArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1721)
	newSig := &builtinUTCTimeWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1722)
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 1724)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 1723)
	v, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFormat), 0)
	return v, false, err
}

type builtinUTCTimeWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimeWithArgSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1725)
	newSig := &builtinUTCTimeWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1726)
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1731)
		return types.Duration{}, isNull, err
	}
	trace_util_0.Count(_builtin_time_00000, 1727)
	if fsp > int64(types.MaxFsp) {
		trace_util_0.Count(_builtin_time_00000, 1732)
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
	}
	trace_util_0.Count(_builtin_time_00000, 1728)
	if fsp < int64(types.MinFsp) {
		trace_util_0.Count(_builtin_time_00000, 1733)
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	trace_util_0.Count(_builtin_time_00000, 1729)
	var nowTs = &b.ctx.GetSessionVars().StmtCtx.NowTs
	if nowTs.Equal(time.Time{}) {
		trace_util_0.Count(_builtin_time_00000, 1734)
		*nowTs = time.Now()
	}
	trace_util_0.Count(_builtin_time_00000, 1730)
	v, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFSPFormat), int(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1735)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1737)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1736)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.DefaultFsp
	sig := &builtinLastDaySig{bf}
	return sig, nil
}

type builtinLastDaySig struct {
	baseBuiltinFunc
}

func (b *builtinLastDaySig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1738)
	newSig := &builtinLastDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1739)
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		trace_util_0.Count(_builtin_time_00000, 1742)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}
	trace_util_0.Count(_builtin_time_00000, 1740)
	tm := arg.Time
	var day int
	year, month := tm.Year(), tm.Month()
	if month == 0 {
		trace_util_0.Count(_builtin_time_00000, 1743)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	trace_util_0.Count(_builtin_time_00000, 1741)
	day = types.GetLastDay(year, month)
	ret := types.Time{
		Time: types.FromDate(year, month, day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  types.DefaultFsp,
	}
	return ret, false, nil
}

// getExpressionFsp calculates the fsp from given expression.
func getExpressionFsp(ctx sessionctx.Context, expression Expression) (int, error) {
	trace_util_0.Count(_builtin_time_00000, 1744)
	constExp, isConstant := expression.(*Constant)
	if isConstant && types.IsString(expression.GetType().Tp) && !isTemporalColumn(expression) {
		trace_util_0.Count(_builtin_time_00000, 1746)
		str, isNil, err := constExp.EvalString(ctx, chunk.Row{})
		if isNil || err != nil {
			trace_util_0.Count(_builtin_time_00000, 1748)
			return 0, err
		}
		trace_util_0.Count(_builtin_time_00000, 1747)
		return types.GetFsp(str), nil
	}
	trace_util_0.Count(_builtin_time_00000, 1745)
	return mathutil.Min(expression.GetType().Decimal, types.MaxFsp), nil
}

// tidbParseTsoFunctionClass extracts physical time from a tso
type tidbParseTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	trace_util_0.Count(_builtin_time_00000, 1749)
	if err := c.verifyArgs(args); err != nil {
		trace_util_0.Count(_builtin_time_00000, 1751)
		return nil, err
	}
	trace_util_0.Count(_builtin_time_00000, 1750)
	argTp := args[0].GetType().EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, argTp, types.ETInt)

	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.DefaultFsp
	sig := &builtinTidbParseTsoSig{bf}
	return sig, nil
}

type builtinTidbParseTsoSig struct {
	baseBuiltinFunc
}

func (b *builtinTidbParseTsoSig) Clone() builtinFunc {
	trace_util_0.Count(_builtin_time_00000, 1752)
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoSig.
func (b *builtinTidbParseTsoSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_builtin_time_00000, 1753)
	arg, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil || arg <= 0 {
		trace_util_0.Count(_builtin_time_00000, 1756)
		return types.Time{}, true, handleInvalidTimeError(b.ctx, err)
	}

	trace_util_0.Count(_builtin_time_00000, 1754)
	t := oracle.GetTimeFromTS(uint64(arg))
	result := types.Time{
		Time: types.FromGoTime(t),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	err = result.ConvertTimeZone(time.Local, b.ctx.GetSessionVars().Location())
	if err != nil {
		trace_util_0.Count(_builtin_time_00000, 1757)
		return types.Time{}, true, err
	}
	trace_util_0.Count(_builtin_time_00000, 1755)
	return result, false, nil
}

var _builtin_time_00000 = "expression/builtin_time.go"
