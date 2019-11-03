// Copyright 2019 PingCAP, Inc.
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

package math

import (
	"math"
	"github.com/pingcap/tidb/trace_util_0"

	// Abs implement the abs function according to http://cavaliercoder.com/blog/optimized-abs-for-int64-in-go.html
)

func Abs(n int64) int64 {
	trace_util_0.Count(_math_00000, 0)
	y := n >> 63
	return (n ^ y) - y
}

// uintSizeTable is used as a table to do comparison to get uint length is faster than doing loop on division with 10
var uintSizeTable = [21]uint64{
	0, // redundant 0 here, so to make function StrLenOfUint64Fast to count from 1 and return i directly
	9, 99, 999, 9999, 99999,
	999999, 9999999, 99999999, 999999999, 9999999999,
	99999999999, 999999999999, 9999999999999, 99999999999999, 999999999999999,
	9999999999999999, 99999999999999999, 999999999999999999, 9999999999999999999,
	math.MaxUint64,
} // math.MaxUint64 is 18446744073709551615 and it has 20 digits

// StrLenOfUint64Fast efficiently calculate the string character lengths of an uint64 as input
func StrLenOfUint64Fast(x uint64) int {
	trace_util_0.Count(_math_00000, 1)
	for i := 1; ; i++ {
		trace_util_0.Count(_math_00000, 2)
		if x <= uintSizeTable[i] {
			trace_util_0.Count(_math_00000, 3)
			return i
		}
	}
}

// StrLenOfInt64Fast efficiently calculate the string character lengths of an int64 as input
func StrLenOfInt64Fast(x int64) int {
	trace_util_0.Count(_math_00000, 4)
	size := 0
	if x < 0 {
		trace_util_0.Count(_math_00000, 6)
		size = 1 // add "-" sign on the length count
	}
	trace_util_0.Count(_math_00000, 5)
	return size + StrLenOfUint64Fast(uint64(Abs(x)))
}

var _math_00000 = "util/math/math.go"
