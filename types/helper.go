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
	"strings"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

// RoundFloat rounds float val to the nearest integer value with float64 format, like MySQL Round function.
// RoundFloat uses default rounding mode, see https://dev.mysql.com/doc/refman/5.7/en/precision-math-rounding.html
// so rounding use "round half away from zero".
// e.g, 1.5 -> 2, -1.5 -> -2.
func RoundFloat(f float64) float64 {
	trace_util_0.Count(_helper_00000, 0)
	if math.Abs(f) < 0.5 {
		trace_util_0.Count(_helper_00000, 2)
		return 0
	}

	trace_util_0.Count(_helper_00000, 1)
	return math.Trunc(f + math.Copysign(0.5, f))
}

// Round rounds the argument f to dec decimal places.
// dec defaults to 0 if not specified. dec can be negative
// to cause dec digits left of the decimal point of the
// value f to become zero.
func Round(f float64, dec int) float64 {
	trace_util_0.Count(_helper_00000, 3)
	shift := math.Pow10(dec)
	tmp := f * shift
	if math.IsInf(tmp, 0) {
		trace_util_0.Count(_helper_00000, 5)
		return f
	}
	trace_util_0.Count(_helper_00000, 4)
	return RoundFloat(tmp) / shift
}

// Truncate truncates the argument f to dec decimal places.
// dec defaults to 0 if not specified. dec can be negative
// to cause dec digits left of the decimal point of the
// value f to become zero.
func Truncate(f float64, dec int) float64 {
	trace_util_0.Count(_helper_00000, 6)
	shift := math.Pow10(dec)
	tmp := f * shift
	if math.IsInf(tmp, 0) {
		trace_util_0.Count(_helper_00000, 8)
		return f
	}
	trace_util_0.Count(_helper_00000, 7)
	return math.Trunc(tmp) / shift
}

// GetMaxFloat gets the max float for given flen and decimal.
func GetMaxFloat(flen int, decimal int) float64 {
	trace_util_0.Count(_helper_00000, 9)
	intPartLen := flen - decimal
	f := math.Pow10(intPartLen)
	f -= math.Pow10(-decimal)
	return f
}

// TruncateFloat tries to truncate f.
// If the result exceeds the max/min float that flen/decimal allowed, returns the max/min float allowed.
func TruncateFloat(f float64, flen int, decimal int) (float64, error) {
	trace_util_0.Count(_helper_00000, 10)
	if math.IsNaN(f) {
		trace_util_0.Count(_helper_00000, 14)
		// nan returns 0
		return 0, ErrOverflow.GenWithStackByArgs("DOUBLE", "")
	}

	trace_util_0.Count(_helper_00000, 11)
	maxF := GetMaxFloat(flen, decimal)

	if !math.IsInf(f, 0) {
		trace_util_0.Count(_helper_00000, 15)
		f = Round(f, decimal)
	}

	trace_util_0.Count(_helper_00000, 12)
	var err error
	if f > maxF {
		trace_util_0.Count(_helper_00000, 16)
		f = maxF
		err = ErrOverflow.GenWithStackByArgs("DOUBLE", "")
	} else {
		trace_util_0.Count(_helper_00000, 17)
		if f < -maxF {
			trace_util_0.Count(_helper_00000, 18)
			f = -maxF
			err = ErrOverflow.GenWithStackByArgs("DOUBLE", "")
		}
	}

	trace_util_0.Count(_helper_00000, 13)
	return f, errors.Trace(err)
}

func isSpace(c byte) bool {
	trace_util_0.Count(_helper_00000, 19)
	return c == ' ' || c == '\t'
}

func isDigit(c byte) bool {
	trace_util_0.Count(_helper_00000, 20)
	return c >= '0' && c <= '9'
}

func myMax(a, b int) int {
	trace_util_0.Count(_helper_00000, 21)
	if a > b {
		trace_util_0.Count(_helper_00000, 23)
		return a
	}
	trace_util_0.Count(_helper_00000, 22)
	return b
}

func myMaxInt8(a, b int8) int8 {
	trace_util_0.Count(_helper_00000, 24)
	if a > b {
		trace_util_0.Count(_helper_00000, 26)
		return a
	}
	trace_util_0.Count(_helper_00000, 25)
	return b
}

func myMin(a, b int) int {
	trace_util_0.Count(_helper_00000, 27)
	if a < b {
		trace_util_0.Count(_helper_00000, 29)
		return a
	}
	trace_util_0.Count(_helper_00000, 28)
	return b
}

func myMinInt8(a, b int8) int8 {
	trace_util_0.Count(_helper_00000, 30)
	if a < b {
		trace_util_0.Count(_helper_00000, 32)
		return a
	}
	trace_util_0.Count(_helper_00000, 31)
	return b
}

const (
	maxUint    = uint64(math.MaxUint64)
	uintCutOff = maxUint/uint64(10) + 1
	intCutOff  = uint64(math.MaxInt64) + 1
)

// strToInt converts a string to an integer in best effort.
func strToInt(str string) (int64, error) {
	trace_util_0.Count(_helper_00000, 33)
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		trace_util_0.Count(_helper_00000, 41)
		return 0, ErrTruncated
	}
	trace_util_0.Count(_helper_00000, 34)
	negative := false
	i := 0
	if str[i] == '-' {
		trace_util_0.Count(_helper_00000, 42)
		negative = true
		i++
	} else {
		trace_util_0.Count(_helper_00000, 43)
		if str[i] == '+' {
			trace_util_0.Count(_helper_00000, 44)
			i++
		}
	}

	trace_util_0.Count(_helper_00000, 35)
	var (
		err    error
		hasNum = false
	)
	r := uint64(0)
	for ; i < len(str); i++ {
		trace_util_0.Count(_helper_00000, 45)
		if !unicode.IsDigit(rune(str[i])) {
			trace_util_0.Count(_helper_00000, 49)
			err = ErrTruncated
			break
		}
		trace_util_0.Count(_helper_00000, 46)
		hasNum = true
		if r >= uintCutOff {
			trace_util_0.Count(_helper_00000, 50)
			r = 0
			err = errors.Trace(ErrBadNumber)
			break
		}
		trace_util_0.Count(_helper_00000, 47)
		r = r * uint64(10)

		r1 := r + uint64(str[i]-'0')
		if r1 < r || r1 > maxUint {
			trace_util_0.Count(_helper_00000, 51)
			r = 0
			err = errors.Trace(ErrBadNumber)
			break
		}
		trace_util_0.Count(_helper_00000, 48)
		r = r1
	}
	trace_util_0.Count(_helper_00000, 36)
	if !hasNum {
		trace_util_0.Count(_helper_00000, 52)
		err = ErrTruncated
	}

	trace_util_0.Count(_helper_00000, 37)
	if !negative && r >= intCutOff {
		trace_util_0.Count(_helper_00000, 53)
		return math.MaxInt64, errors.Trace(ErrBadNumber)
	}

	trace_util_0.Count(_helper_00000, 38)
	if negative && r > intCutOff {
		trace_util_0.Count(_helper_00000, 54)
		return math.MinInt64, errors.Trace(ErrBadNumber)
	}

	trace_util_0.Count(_helper_00000, 39)
	if negative {
		trace_util_0.Count(_helper_00000, 55)
		r = -r
	}
	trace_util_0.Count(_helper_00000, 40)
	return int64(r), err
}

var _helper_00000 = "types/helper.go"
