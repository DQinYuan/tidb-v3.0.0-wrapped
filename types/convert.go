// Copyright 2014 The ql Authors. All rights reserved.
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

package types

import (
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

func truncateStr(str string, flen int) string {
	trace_util_0.Count(_convert_00000, 0)
	if flen != UnspecifiedLength && len(str) > flen {
		trace_util_0.Count(_convert_00000, 2)
		str = str[:flen]
	}
	trace_util_0.Count(_convert_00000, 1)
	return str
}

// IntergerUnsignedUpperBound indicates the max uint64 values of different mysql types.
func IntergerUnsignedUpperBound(intType byte) uint64 {
	trace_util_0.Count(_convert_00000, 3)
	switch intType {
	case mysql.TypeTiny:
		trace_util_0.Count(_convert_00000, 4)
		return math.MaxUint8
	case mysql.TypeShort:
		trace_util_0.Count(_convert_00000, 5)
		return math.MaxUint16
	case mysql.TypeInt24:
		trace_util_0.Count(_convert_00000, 6)
		return mysql.MaxUint24
	case mysql.TypeLong:
		trace_util_0.Count(_convert_00000, 7)
		return math.MaxUint32
	case mysql.TypeLonglong:
		trace_util_0.Count(_convert_00000, 8)
		return math.MaxUint64
	case mysql.TypeBit:
		trace_util_0.Count(_convert_00000, 9)
		return math.MaxUint64
	case mysql.TypeEnum:
		trace_util_0.Count(_convert_00000, 10)
		return math.MaxUint64
	case mysql.TypeSet:
		trace_util_0.Count(_convert_00000, 11)
		return math.MaxUint64
	default:
		trace_util_0.Count(_convert_00000, 12)
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedUpperBound indicates the max int64 values of different mysql types.
func IntergerSignedUpperBound(intType byte) int64 {
	trace_util_0.Count(_convert_00000, 13)
	switch intType {
	case mysql.TypeTiny:
		trace_util_0.Count(_convert_00000, 14)
		return math.MaxInt8
	case mysql.TypeShort:
		trace_util_0.Count(_convert_00000, 15)
		return math.MaxInt16
	case mysql.TypeInt24:
		trace_util_0.Count(_convert_00000, 16)
		return mysql.MaxInt24
	case mysql.TypeLong:
		trace_util_0.Count(_convert_00000, 17)
		return math.MaxInt32
	case mysql.TypeLonglong:
		trace_util_0.Count(_convert_00000, 18)
		return math.MaxInt64
	default:
		trace_util_0.Count(_convert_00000, 19)
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedLowerBound indicates the min int64 values of different mysql types.
func IntergerSignedLowerBound(intType byte) int64 {
	trace_util_0.Count(_convert_00000, 20)
	switch intType {
	case mysql.TypeTiny:
		trace_util_0.Count(_convert_00000, 21)
		return math.MinInt8
	case mysql.TypeShort:
		trace_util_0.Count(_convert_00000, 22)
		return math.MinInt16
	case mysql.TypeInt24:
		trace_util_0.Count(_convert_00000, 23)
		return mysql.MinInt24
	case mysql.TypeLong:
		trace_util_0.Count(_convert_00000, 24)
		return math.MinInt32
	case mysql.TypeLonglong:
		trace_util_0.Count(_convert_00000, 25)
		return math.MinInt64
	default:
		trace_util_0.Count(_convert_00000, 26)
		panic("Input byte is not a mysql type")
	}
}

// ConvertFloatToInt converts a float64 value to a int value.
func ConvertFloatToInt(fval float64, lowerBound, upperBound int64, tp byte) (int64, error) {
	trace_util_0.Count(_convert_00000, 27)
	val := RoundFloat(fval)
	if val < float64(lowerBound) {
		trace_util_0.Count(_convert_00000, 30)
		return lowerBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 28)
	if val >= float64(upperBound) {
		trace_util_0.Count(_convert_00000, 31)
		if val == float64(upperBound) {
			trace_util_0.Count(_convert_00000, 33)
			return upperBound, nil
		}
		trace_util_0.Count(_convert_00000, 32)
		return upperBound, overflow(val, tp)
	}
	trace_util_0.Count(_convert_00000, 29)
	return int64(val), nil
}

// ConvertIntToInt converts an int value to another int value of different precision.
func ConvertIntToInt(val int64, lowerBound int64, upperBound int64, tp byte) (int64, error) {
	trace_util_0.Count(_convert_00000, 34)
	if val < lowerBound {
		trace_util_0.Count(_convert_00000, 37)
		return lowerBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 35)
	if val > upperBound {
		trace_util_0.Count(_convert_00000, 38)
		return upperBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 36)
	return val, nil
}

// ConvertUintToInt converts an uint value to an int value.
func ConvertUintToInt(val uint64, upperBound int64, tp byte) (int64, error) {
	trace_util_0.Count(_convert_00000, 39)
	if val > uint64(upperBound) {
		trace_util_0.Count(_convert_00000, 41)
		return upperBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 40)
	return int64(val), nil
}

// ConvertIntToUint converts an int value to an uint value.
func ConvertIntToUint(sc *stmtctx.StatementContext, val int64, upperBound uint64, tp byte) (uint64, error) {
	trace_util_0.Count(_convert_00000, 42)
	if sc.ShouldClipToZero() && val < 0 {
		trace_util_0.Count(_convert_00000, 45)
		return 0, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 43)
	if uint64(val) > upperBound {
		trace_util_0.Count(_convert_00000, 46)
		return upperBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 44)
	return uint64(val), nil
}

// ConvertUintToUint converts an uint value to another uint value of different precision.
func ConvertUintToUint(val uint64, upperBound uint64, tp byte) (uint64, error) {
	trace_util_0.Count(_convert_00000, 47)
	if val > upperBound {
		trace_util_0.Count(_convert_00000, 49)
		return upperBound, overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 48)
	return val, nil
}

// ConvertFloatToUint converts a float value to an uint value.
func ConvertFloatToUint(sc *stmtctx.StatementContext, fval float64, upperBound uint64, tp byte) (uint64, error) {
	trace_util_0.Count(_convert_00000, 50)
	val := RoundFloat(fval)
	if val < 0 {
		trace_util_0.Count(_convert_00000, 53)
		if sc.ShouldClipToZero() {
			trace_util_0.Count(_convert_00000, 55)
			return 0, overflow(val, tp)
		}
		trace_util_0.Count(_convert_00000, 54)
		return uint64(int64(val)), overflow(val, tp)
	}

	trace_util_0.Count(_convert_00000, 51)
	if val > float64(upperBound) {
		trace_util_0.Count(_convert_00000, 56)
		return upperBound, overflow(val, tp)
	}
	trace_util_0.Count(_convert_00000, 52)
	return uint64(val), nil
}

// convertScientificNotation converts a decimal string with scientific notation to a normal decimal string.
// 1E6 => 1000000, .12345E+5 => 12345
func convertScientificNotation(str string) (string, error) {
	trace_util_0.Count(_convert_00000, 57)
	// https://golang.org/ref/spec#Floating-point_literals
	eIdx := -1
	point := -1
	for i := 0; i < len(str); i++ {
		trace_util_0.Count(_convert_00000, 61)
		if str[i] == '.' {
			trace_util_0.Count(_convert_00000, 63)
			point = i
		}
		trace_util_0.Count(_convert_00000, 62)
		if str[i] == 'e' || str[i] == 'E' {
			trace_util_0.Count(_convert_00000, 64)
			eIdx = i
			if point == -1 {
				trace_util_0.Count(_convert_00000, 66)
				point = i
			}
			trace_util_0.Count(_convert_00000, 65)
			break
		}
	}
	trace_util_0.Count(_convert_00000, 58)
	if eIdx == -1 {
		trace_util_0.Count(_convert_00000, 67)
		return str, nil
	}
	trace_util_0.Count(_convert_00000, 59)
	exp, err := strconv.ParseInt(str[eIdx+1:], 10, 64)
	if err != nil {
		trace_util_0.Count(_convert_00000, 68)
		return "", errors.WithStack(err)
	}

	trace_util_0.Count(_convert_00000, 60)
	f := str[:eIdx]
	if exp == 0 {
		trace_util_0.Count(_convert_00000, 69)
		return f, nil
	} else {
		trace_util_0.Count(_convert_00000, 70)
		if exp > 0 {
			trace_util_0.Count(_convert_00000, 71) // move point right
			if point+int(exp) == len(f)-1 {
				trace_util_0.Count(_convert_00000, 73) // 123.456 >> 3 = 123456. = 123456
				return f[:point] + f[point+1:], nil
			} else {
				trace_util_0.Count(_convert_00000, 74)
				if point+int(exp) < len(f)-1 {
					trace_util_0.Count(_convert_00000, 75) // 123.456 >> 2 = 12345.6
					return f[:point] + f[point+1:point+1+int(exp)] + "." + f[point+1+int(exp):], nil
				}
			}
			// 123.456 >> 5 = 12345600
			trace_util_0.Count(_convert_00000, 72)
			return f[:point] + f[point+1:] + strings.Repeat("0", point+int(exp)-len(f)+1), nil
		} else {
			trace_util_0.Count(_convert_00000, 76)
			{ // move point left
				exp = -exp
				if int(exp) < point {
					trace_util_0.Count(_convert_00000, 78) // 123.456 << 2 = 1.23456
					return f[:point-int(exp)] + "." + f[point-int(exp):point] + f[point+1:], nil
				}
				// 123.456 << 5 = 0.00123456
				trace_util_0.Count(_convert_00000, 77)
				return "0." + strings.Repeat("0", int(exp)-point) + f[:point] + f[point+1:], nil
			}
		}
	}
}

func convertDecimalStrToUint(sc *stmtctx.StatementContext, str string, upperBound uint64, tp byte) (uint64, error) {
	trace_util_0.Count(_convert_00000, 79)
	str, err := convertScientificNotation(str)
	if err != nil {
		trace_util_0.Count(_convert_00000, 87)
		return 0, err
	}

	trace_util_0.Count(_convert_00000, 80)
	var intStr, fracStr string
	p := strings.Index(str, ".")
	if p == -1 {
		trace_util_0.Count(_convert_00000, 88)
		intStr = str
	} else {
		trace_util_0.Count(_convert_00000, 89)
		{
			intStr = str[:p]
			fracStr = str[p+1:]
		}
	}
	trace_util_0.Count(_convert_00000, 81)
	intStr = strings.TrimLeft(intStr, "0")
	if intStr == "" {
		trace_util_0.Count(_convert_00000, 90)
		intStr = "0"
	}
	trace_util_0.Count(_convert_00000, 82)
	if sc.ShouldClipToZero() && intStr[0] == '-' {
		trace_util_0.Count(_convert_00000, 91)
		return 0, overflow(str, tp)
	}

	trace_util_0.Count(_convert_00000, 83)
	var round uint64
	if fracStr != "" && fracStr[0] >= '5' {
		trace_util_0.Count(_convert_00000, 92)
		round++
	}

	trace_util_0.Count(_convert_00000, 84)
	upperBound -= round
	upperStr := strconv.FormatUint(upperBound, 10)
	if len(intStr) > len(upperStr) ||
		(len(intStr) == len(upperStr) && intStr > upperStr) {
		trace_util_0.Count(_convert_00000, 93)
		return upperBound, overflow(str, tp)
	}

	trace_util_0.Count(_convert_00000, 85)
	val, err := strconv.ParseUint(intStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_convert_00000, 94)
		return val, err
	}
	trace_util_0.Count(_convert_00000, 86)
	return val + round, nil
}

// ConvertDecimalToUint converts a decimal to a uint by converting it to a string first to avoid float overflow (#10181).
func ConvertDecimalToUint(sc *stmtctx.StatementContext, d *MyDecimal, upperBound uint64, tp byte) (uint64, error) {
	trace_util_0.Count(_convert_00000, 95)
	return convertDecimalStrToUint(sc, string(d.ToString()), upperBound, tp)
}

// StrToInt converts a string to an integer at the best-effort.
func StrToInt(sc *stmtctx.StatementContext, str string) (int64, error) {
	trace_util_0.Count(_convert_00000, 96)
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	iVal, err1 := strconv.ParseInt(validPrefix, 10, 64)
	if err1 != nil {
		trace_util_0.Count(_convert_00000, 98)
		return iVal, ErrOverflow.GenWithStackByArgs("BIGINT", validPrefix)
	}
	trace_util_0.Count(_convert_00000, 97)
	return iVal, errors.Trace(err)
}

// StrToUint converts a string to an unsigned integer at the best-effortt.
func StrToUint(sc *stmtctx.StatementContext, str string) (uint64, error) {
	trace_util_0.Count(_convert_00000, 99)
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	if validPrefix[0] == '+' {
		trace_util_0.Count(_convert_00000, 102)
		validPrefix = validPrefix[1:]
	}
	trace_util_0.Count(_convert_00000, 100)
	uVal, err1 := strconv.ParseUint(validPrefix, 10, 64)
	if err1 != nil {
		trace_util_0.Count(_convert_00000, 103)
		return uVal, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", validPrefix)
	}
	trace_util_0.Count(_convert_00000, 101)
	return uVal, errors.Trace(err)
}

// StrToDateTime converts str to MySQL DateTime.
func StrToDateTime(sc *stmtctx.StatementContext, str string, fsp int) (Time, error) {
	trace_util_0.Count(_convert_00000, 104)
	return ParseTime(sc, str, mysql.TypeDatetime, fsp)
}

// StrToDuration converts str to Duration. It returns Duration in normal case,
// and returns Time when str is in datetime format.
// when isDuration is true, the d is returned, when it is false, the t is returned.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-literals.html.
func StrToDuration(sc *stmtctx.StatementContext, str string, fsp int) (d Duration, t Time, isDuration bool, err error) {
	trace_util_0.Count(_convert_00000, 105)
	str = strings.TrimSpace(str)
	length := len(str)
	if length > 0 && str[0] == '-' {
		trace_util_0.Count(_convert_00000, 109)
		length--
	}
	// Timestamp format is 'YYYYMMDDHHMMSS' or 'YYMMDDHHMMSS', which length is 12.
	// See #3923, it explains what we do here.
	trace_util_0.Count(_convert_00000, 106)
	if length >= 12 {
		trace_util_0.Count(_convert_00000, 110)
		t, err = StrToDateTime(sc, str, fsp)
		if err == nil {
			trace_util_0.Count(_convert_00000, 111)
			return d, t, false, nil
		}
	}

	trace_util_0.Count(_convert_00000, 107)
	d, err = ParseDuration(sc, str, fsp)
	if ErrTruncatedWrongVal.Equal(err) {
		trace_util_0.Count(_convert_00000, 112)
		err = sc.HandleTruncate(err)
	}
	trace_util_0.Count(_convert_00000, 108)
	return d, t, true, errors.Trace(err)
}

// NumberToDuration converts number to Duration.
func NumberToDuration(number int64, fsp int) (Duration, error) {
	trace_util_0.Count(_convert_00000, 113)
	if number > TimeMaxValue {
		trace_util_0.Count(_convert_00000, 119)
		// Try to parse DATETIME.
		if number >= 10000000000 {
			trace_util_0.Count(_convert_00000, 121) // '2001-00-00 00-00-00'
			if t, err := ParseDatetimeFromNum(nil, number); err == nil {
				trace_util_0.Count(_convert_00000, 122)
				dur, err1 := t.ConvertToDuration()
				return dur, errors.Trace(err1)
			}
		}
		trace_util_0.Count(_convert_00000, 120)
		dur, err1 := MaxMySQLTime(fsp).ConvertToDuration()
		terror.Log(err1)
		return dur, ErrOverflow.GenWithStackByArgs("Duration", strconv.Itoa(int(number)))
	} else {
		trace_util_0.Count(_convert_00000, 123)
		if number < -TimeMaxValue {
			trace_util_0.Count(_convert_00000, 124)
			dur, err1 := MaxMySQLTime(fsp).ConvertToDuration()
			terror.Log(err1)
			dur.Duration = -dur.Duration
			return dur, ErrOverflow.GenWithStackByArgs("Duration", strconv.Itoa(int(number)))
		}
	}
	trace_util_0.Count(_convert_00000, 114)
	var neg bool
	if neg = number < 0; neg {
		trace_util_0.Count(_convert_00000, 125)
		number = -number
	}

	trace_util_0.Count(_convert_00000, 115)
	if number/10000 > TimeMaxHour || number%100 >= 60 || (number/100)%100 >= 60 {
		trace_util_0.Count(_convert_00000, 126)
		return ZeroDuration, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(number))
	}
	trace_util_0.Count(_convert_00000, 116)
	t := Time{Time: FromDate(0, 0, 0, int(number/10000), int((number/100)%100), int(number%100), 0), Type: mysql.TypeDuration, Fsp: fsp}
	dur, err := t.ConvertToDuration()
	if err != nil {
		trace_util_0.Count(_convert_00000, 127)
		return ZeroDuration, errors.Trace(err)
	}
	trace_util_0.Count(_convert_00000, 117)
	if neg {
		trace_util_0.Count(_convert_00000, 128)
		dur.Duration = -dur.Duration
	}
	trace_util_0.Count(_convert_00000, 118)
	return dur, nil
}

// getValidIntPrefix gets prefix of the string which can be successfully parsed as int.
func getValidIntPrefix(sc *stmtctx.StatementContext, str string) (string, error) {
	trace_util_0.Count(_convert_00000, 129)
	floatPrefix, err := getValidFloatPrefix(sc, str)
	if err != nil {
		trace_util_0.Count(_convert_00000, 131)
		return floatPrefix, errors.Trace(err)
	}
	trace_util_0.Count(_convert_00000, 130)
	return floatStrToIntStr(sc, floatPrefix, str)
}

// roundIntStr is to round int string base on the number following dot.
func roundIntStr(numNextDot byte, intStr string) string {
	trace_util_0.Count(_convert_00000, 132)
	if numNextDot < '5' {
		trace_util_0.Count(_convert_00000, 135)
		return intStr
	}
	trace_util_0.Count(_convert_00000, 133)
	retStr := []byte(intStr)
	for i := len(intStr) - 1; i >= 0; i-- {
		trace_util_0.Count(_convert_00000, 136)
		if retStr[i] != '9' {
			trace_util_0.Count(_convert_00000, 139)
			retStr[i]++
			break
		}
		trace_util_0.Count(_convert_00000, 137)
		if i == 0 {
			trace_util_0.Count(_convert_00000, 140)
			retStr[i] = '1'
			retStr = append(retStr, '0')
			break
		}
		trace_util_0.Count(_convert_00000, 138)
		retStr[i] = '0'
	}
	trace_util_0.Count(_convert_00000, 134)
	return string(retStr)
}

// floatStrToIntStr converts a valid float string into valid integer string which can be parsed by
// strconv.ParseInt, we can't parse float first then convert it to string because precision will
// be lost. For example, the string value "18446744073709551615" which is the max number of unsigned
// int will cause some precision to lose. intStr[0] may be a positive and negative sign like '+' or '-'.
func floatStrToIntStr(sc *stmtctx.StatementContext, validFloat string, oriStr string) (intStr string, _ error) {
	trace_util_0.Count(_convert_00000, 141)
	var dotIdx = -1
	var eIdx = -1
	for i := 0; i < len(validFloat); i++ {
		trace_util_0.Count(_convert_00000, 150)
		switch validFloat[i] {
		case '.':
			trace_util_0.Count(_convert_00000, 151)
			dotIdx = i
		case 'e', 'E':
			trace_util_0.Count(_convert_00000, 152)
			eIdx = i
		}
	}
	trace_util_0.Count(_convert_00000, 142)
	if eIdx == -1 {
		trace_util_0.Count(_convert_00000, 153)
		if dotIdx == -1 {
			trace_util_0.Count(_convert_00000, 159)
			return validFloat, nil
		}
		trace_util_0.Count(_convert_00000, 154)
		var digits []byte
		if validFloat[0] == '-' || validFloat[0] == '+' {
			trace_util_0.Count(_convert_00000, 160)
			dotIdx--
			digits = []byte(validFloat[1:])
		} else {
			trace_util_0.Count(_convert_00000, 161)
			{
				digits = []byte(validFloat)
			}
		}
		trace_util_0.Count(_convert_00000, 155)
		if dotIdx == 0 {
			trace_util_0.Count(_convert_00000, 162)
			intStr = "0"
		} else {
			trace_util_0.Count(_convert_00000, 163)
			{
				intStr = string(digits)[:dotIdx]
			}
		}
		trace_util_0.Count(_convert_00000, 156)
		if len(digits) > dotIdx+1 {
			trace_util_0.Count(_convert_00000, 164)
			intStr = roundIntStr(digits[dotIdx+1], intStr)
		}
		trace_util_0.Count(_convert_00000, 157)
		if (len(intStr) > 1 || intStr[0] != '0') && validFloat[0] == '-' {
			trace_util_0.Count(_convert_00000, 165)
			intStr = "-" + intStr
		}
		trace_util_0.Count(_convert_00000, 158)
		return intStr, nil
	}
	trace_util_0.Count(_convert_00000, 143)
	var intCnt int
	digits := make([]byte, 0, len(validFloat))
	if dotIdx == -1 {
		trace_util_0.Count(_convert_00000, 166)
		digits = append(digits, validFloat[:eIdx]...)
		intCnt = len(digits)
	} else {
		trace_util_0.Count(_convert_00000, 167)
		{
			digits = append(digits, validFloat[:dotIdx]...)
			intCnt = len(digits)
			digits = append(digits, validFloat[dotIdx+1:eIdx]...)
		}
	}
	trace_util_0.Count(_convert_00000, 144)
	exp, err := strconv.Atoi(validFloat[eIdx+1:])
	if err != nil {
		trace_util_0.Count(_convert_00000, 168)
		return validFloat, errors.Trace(err)
	}
	trace_util_0.Count(_convert_00000, 145)
	if exp > 0 && int64(intCnt) > (math.MaxInt64-int64(exp)) {
		trace_util_0.Count(_convert_00000, 169)
		// (exp + incCnt) overflows MaxInt64.
		sc.AppendWarning(ErrOverflow.GenWithStackByArgs("BIGINT", oriStr))
		return validFloat[:eIdx], nil
	}
	trace_util_0.Count(_convert_00000, 146)
	intCnt += exp
	if intCnt <= 0 {
		trace_util_0.Count(_convert_00000, 170)
		intStr = "0"
		if intCnt == 0 && len(digits) > 0 {
			trace_util_0.Count(_convert_00000, 172)
			intStr = roundIntStr(digits[0], intStr)
		}
		trace_util_0.Count(_convert_00000, 171)
		return intStr, nil
	}
	trace_util_0.Count(_convert_00000, 147)
	if intCnt == 1 && (digits[0] == '-' || digits[0] == '+') {
		trace_util_0.Count(_convert_00000, 173)
		intStr = "0"
		if len(digits) > 1 {
			trace_util_0.Count(_convert_00000, 176)
			intStr = roundIntStr(digits[1], intStr)
		}
		trace_util_0.Count(_convert_00000, 174)
		if intStr[0] == '1' {
			trace_util_0.Count(_convert_00000, 177)
			intStr = string(digits[:1]) + intStr
		}
		trace_util_0.Count(_convert_00000, 175)
		return intStr, nil
	}
	trace_util_0.Count(_convert_00000, 148)
	if intCnt <= len(digits) {
		trace_util_0.Count(_convert_00000, 178)
		intStr = string(digits[:intCnt])
		if intCnt < len(digits) {
			trace_util_0.Count(_convert_00000, 179)
			intStr = roundIntStr(digits[intCnt], intStr)
		}
	} else {
		trace_util_0.Count(_convert_00000, 180)
		{
			// convert scientific notation decimal number
			extraZeroCount := intCnt - len(digits)
			if extraZeroCount > 20 {
				trace_util_0.Count(_convert_00000, 182)
				// Append overflow warning and return to avoid allocating too much memory.
				sc.AppendWarning(ErrOverflow.GenWithStackByArgs("BIGINT", oriStr))
				return validFloat[:eIdx], nil
			}
			trace_util_0.Count(_convert_00000, 181)
			intStr = string(digits) + strings.Repeat("0", extraZeroCount)
		}
	}
	trace_util_0.Count(_convert_00000, 149)
	return intStr, nil
}

// StrToFloat converts a string to a float64 at the best-effort.
func StrToFloat(sc *stmtctx.StatementContext, str string) (float64, error) {
	trace_util_0.Count(_convert_00000, 183)
	str = strings.TrimSpace(str)
	validStr, err := getValidFloatPrefix(sc, str)
	f, err1 := strconv.ParseFloat(validStr, 64)
	if err1 != nil {
		trace_util_0.Count(_convert_00000, 185)
		if err2, ok := err1.(*strconv.NumError); ok {
			trace_util_0.Count(_convert_00000, 187)
			// value will truncate to MAX/MIN if out of range.
			if err2.Err == strconv.ErrRange {
				trace_util_0.Count(_convert_00000, 188)
				err1 = sc.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("DOUBLE", str))
				if math.IsInf(f, 1) {
					trace_util_0.Count(_convert_00000, 189)
					f = math.MaxFloat64
				} else {
					trace_util_0.Count(_convert_00000, 190)
					if math.IsInf(f, -1) {
						trace_util_0.Count(_convert_00000, 191)
						f = -math.MaxFloat64
					}
				}
			}
		}
		trace_util_0.Count(_convert_00000, 186)
		return f, errors.Trace(err1)
	}
	trace_util_0.Count(_convert_00000, 184)
	return f, errors.Trace(err)
}

// ConvertJSONToInt casts JSON into int64.
func ConvertJSONToInt(sc *stmtctx.StatementContext, j json.BinaryJSON, unsigned bool) (int64, error) {
	trace_util_0.Count(_convert_00000, 192)
	switch j.TypeCode {
	case json.TypeCodeObject, json.TypeCodeArray:
		trace_util_0.Count(_convert_00000, 194)
		return 0, nil
	case json.TypeCodeLiteral:
		trace_util_0.Count(_convert_00000, 195)
		switch j.Value[0] {
		case json.LiteralNil, json.LiteralFalse:
			trace_util_0.Count(_convert_00000, 200)
			return 0, nil
		default:
			trace_util_0.Count(_convert_00000, 201)
			return 1, nil
		}
	case json.TypeCodeInt64, json.TypeCodeUint64:
		trace_util_0.Count(_convert_00000, 196)
		return j.GetInt64(), nil
	case json.TypeCodeFloat64:
		trace_util_0.Count(_convert_00000, 197)
		f := j.GetFloat64()
		if !unsigned {
			trace_util_0.Count(_convert_00000, 202)
			lBound := IntergerSignedLowerBound(mysql.TypeLonglong)
			uBound := IntergerSignedUpperBound(mysql.TypeLonglong)
			return ConvertFloatToInt(f, lBound, uBound, mysql.TypeDouble)
		}
		trace_util_0.Count(_convert_00000, 198)
		bound := IntergerUnsignedUpperBound(mysql.TypeLonglong)
		u, err := ConvertFloatToUint(sc, f, bound, mysql.TypeDouble)
		return int64(u), errors.Trace(err)
	case json.TypeCodeString:
		trace_util_0.Count(_convert_00000, 199)
		str := string(hack.String(j.GetString()))
		return StrToInt(sc, str)
	}
	trace_util_0.Count(_convert_00000, 193)
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToFloat casts JSON into float64.
func ConvertJSONToFloat(sc *stmtctx.StatementContext, j json.BinaryJSON) (float64, error) {
	trace_util_0.Count(_convert_00000, 203)
	switch j.TypeCode {
	case json.TypeCodeObject, json.TypeCodeArray:
		trace_util_0.Count(_convert_00000, 205)
		return 0, nil
	case json.TypeCodeLiteral:
		trace_util_0.Count(_convert_00000, 206)
		switch j.Value[0] {
		case json.LiteralNil, json.LiteralFalse:
			trace_util_0.Count(_convert_00000, 211)
			return 0, nil
		default:
			trace_util_0.Count(_convert_00000, 212)
			return 1, nil
		}
	case json.TypeCodeInt64:
		trace_util_0.Count(_convert_00000, 207)
		return float64(j.GetInt64()), nil
	case json.TypeCodeUint64:
		trace_util_0.Count(_convert_00000, 208)
		u, err := ConvertIntToUint(sc, j.GetInt64(), IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		return float64(u), errors.Trace(err)
	case json.TypeCodeFloat64:
		trace_util_0.Count(_convert_00000, 209)
		return j.GetFloat64(), nil
	case json.TypeCodeString:
		trace_util_0.Count(_convert_00000, 210)
		str := string(hack.String(j.GetString()))
		return StrToFloat(sc, str)
	}
	trace_util_0.Count(_convert_00000, 204)
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToDecimal casts JSON into decimal.
func ConvertJSONToDecimal(sc *stmtctx.StatementContext, j json.BinaryJSON) (*MyDecimal, error) {
	trace_util_0.Count(_convert_00000, 213)
	res := new(MyDecimal)
	if j.TypeCode != json.TypeCodeString {
		trace_util_0.Count(_convert_00000, 215)
		f64, err := ConvertJSONToFloat(sc, j)
		if err != nil {
			trace_util_0.Count(_convert_00000, 217)
			return res, errors.Trace(err)
		}
		trace_util_0.Count(_convert_00000, 216)
		err = res.FromFloat64(f64)
		return res, errors.Trace(err)
	}
	trace_util_0.Count(_convert_00000, 214)
	err := sc.HandleTruncate(res.FromString([]byte(j.GetString())))
	return res, errors.Trace(err)
}

// getValidFloatPrefix gets prefix of string which can be successfully parsed as float.
func getValidFloatPrefix(sc *stmtctx.StatementContext, s string) (valid string, err error) {
	trace_util_0.Count(_convert_00000, 218)
	var (
		sawDot   bool
		sawDigit bool
		validLen int
		eIdx     int
	)
	for i := 0; i < len(s); i++ {
		trace_util_0.Count(_convert_00000, 222)
		c := s[i]
		if c == '+' || c == '-' {
			trace_util_0.Count(_convert_00000, 223)
			if i != 0 && i != eIdx+1 {
				trace_util_0.Count(_convert_00000, 224) // "1e+1" is valid.
				break
			}
		} else {
			trace_util_0.Count(_convert_00000, 225)
			if c == '.' {
				trace_util_0.Count(_convert_00000, 226)
				if sawDot || eIdx > 0 {
					trace_util_0.Count(_convert_00000, 228) // "1.1." or "1e1.1"
					break
				}
				trace_util_0.Count(_convert_00000, 227)
				sawDot = true
				if sawDigit {
					trace_util_0.Count(_convert_00000, 229) // "123." is valid.
					validLen = i + 1
				}
			} else {
				trace_util_0.Count(_convert_00000, 230)
				if c == 'e' || c == 'E' {
					trace_util_0.Count(_convert_00000, 231)
					if !sawDigit {
						trace_util_0.Count(_convert_00000, 234) // "+.e"
						break
					}
					trace_util_0.Count(_convert_00000, 232)
					if eIdx != 0 {
						trace_util_0.Count(_convert_00000, 235) // "1e5e"
						break
					}
					trace_util_0.Count(_convert_00000, 233)
					eIdx = i
				} else {
					trace_util_0.Count(_convert_00000, 236)
					if c < '0' || c > '9' {
						trace_util_0.Count(_convert_00000, 237)
						break
					} else {
						trace_util_0.Count(_convert_00000, 238)
						{
							sawDigit = true
							validLen = i + 1
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_convert_00000, 219)
	valid = s[:validLen]
	if valid == "" {
		trace_util_0.Count(_convert_00000, 239)
		valid = "0"
	}
	trace_util_0.Count(_convert_00000, 220)
	if validLen == 0 || validLen != len(s) {
		trace_util_0.Count(_convert_00000, 240)
		err = errors.Trace(handleTruncateError(sc))
	}
	trace_util_0.Count(_convert_00000, 221)
	return valid, err
}

// ToString converts an interface to a string.
func ToString(value interface{}) (string, error) {
	trace_util_0.Count(_convert_00000, 241)
	switch v := value.(type) {
	case bool:
		trace_util_0.Count(_convert_00000, 242)
		if v {
			trace_util_0.Count(_convert_00000, 258)
			return "1", nil
		}
		trace_util_0.Count(_convert_00000, 243)
		return "0", nil
	case int:
		trace_util_0.Count(_convert_00000, 244)
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		trace_util_0.Count(_convert_00000, 245)
		return strconv.FormatInt(v, 10), nil
	case uint64:
		trace_util_0.Count(_convert_00000, 246)
		return strconv.FormatUint(v, 10), nil
	case float32:
		trace_util_0.Count(_convert_00000, 247)
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		trace_util_0.Count(_convert_00000, 248)
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		trace_util_0.Count(_convert_00000, 249)
		return v, nil
	case []byte:
		trace_util_0.Count(_convert_00000, 250)
		return string(v), nil
	case Time:
		trace_util_0.Count(_convert_00000, 251)
		return v.String(), nil
	case Duration:
		trace_util_0.Count(_convert_00000, 252)
		return v.String(), nil
	case *MyDecimal:
		trace_util_0.Count(_convert_00000, 253)
		return v.String(), nil
	case BinaryLiteral:
		trace_util_0.Count(_convert_00000, 254)
		return v.ToString(), nil
	case Enum:
		trace_util_0.Count(_convert_00000, 255)
		return v.String(), nil
	case Set:
		trace_util_0.Count(_convert_00000, 256)
		return v.String(), nil
	default:
		trace_util_0.Count(_convert_00000, 257)
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}

var _convert_00000 = "types/convert.go"
