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
	"github.com/pingcap/tidb/trace_util_0"
)

const (
	// UnspecifiedFsp is the unspecified fractional seconds part.
	UnspecifiedFsp = -1
	// MaxFsp is the maximum digit of fractional seconds part.
	MaxFsp = 6
	// MinFsp is the minimum digit of fractional seconds part.
	MinFsp = 0
	// DefaultFsp is the default digit of fractional seconds part.
	// MySQL use 0 as the default Fsp.
	DefaultFsp = 0
)

// CheckFsp checks whether fsp is in valid range.
func CheckFsp(fsp int) (int, error) {
	trace_util_0.Count(_fsp_00000, 0)
	if fsp == UnspecifiedFsp {
		trace_util_0.Count(_fsp_00000, 3)
		return DefaultFsp, nil
	}
	trace_util_0.Count(_fsp_00000, 1)
	if fsp < MinFsp || fsp > MaxFsp {
		trace_util_0.Count(_fsp_00000, 4)
		return DefaultFsp, errors.Errorf("Invalid fsp %d", fsp)
	}
	trace_util_0.Count(_fsp_00000, 2)
	return fsp, nil
}

// ParseFrac parses the input string according to fsp, returns the microsecond,
// and also a bool value to indice overflow. eg:
// "999" fsp=2 will overflow.
func ParseFrac(s string, fsp int) (v int, overflow bool, err error) {
	trace_util_0.Count(_fsp_00000, 5)
	if len(s) == 0 {
		trace_util_0.Count(_fsp_00000, 11)
		return 0, false, nil
	}

	trace_util_0.Count(_fsp_00000, 6)
	fsp, err = CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_fsp_00000, 12)
		return 0, false, errors.Trace(err)
	}

	trace_util_0.Count(_fsp_00000, 7)
	if fsp >= len(s) {
		trace_util_0.Count(_fsp_00000, 13)
		tmp, e := strconv.ParseInt(s, 10, 64)
		if e != nil {
			trace_util_0.Count(_fsp_00000, 15)
			return 0, false, errors.Trace(e)
		}
		trace_util_0.Count(_fsp_00000, 14)
		v = int(float64(tmp) * math.Pow10(MaxFsp-len(s)))
		return
	}

	// Round when fsp < string length.
	trace_util_0.Count(_fsp_00000, 8)
	tmp, e := strconv.ParseInt(s[:fsp+1], 10, 64)
	if e != nil {
		trace_util_0.Count(_fsp_00000, 16)
		return 0, false, errors.Trace(e)
	}
	trace_util_0.Count(_fsp_00000, 9)
	tmp = (tmp + 5) / 10

	if float64(tmp) >= math.Pow10(fsp) {
		trace_util_0.Count(_fsp_00000, 17)
		// overflow
		return 0, true, nil
	}

	// Get the final frac, with 6 digit number
	//  1236 round 3 -> 124 -> 124000
	//  0312 round 2 -> 3 -> 30000
	//  999 round 2 -> 100 -> overflow
	trace_util_0.Count(_fsp_00000, 10)
	v = int(float64(tmp) * math.Pow10(MaxFsp-fsp))
	return
}

// alignFrac is used to generate alignment frac, like `100` -> `100000`
func alignFrac(s string, fsp int) string {
	trace_util_0.Count(_fsp_00000, 18)
	sl := len(s)
	if sl < fsp {
		trace_util_0.Count(_fsp_00000, 20)
		return s + strings.Repeat("0", fsp-sl)
	}

	trace_util_0.Count(_fsp_00000, 19)
	return s
}

var _fsp_00000 = "types/fsp.go"
