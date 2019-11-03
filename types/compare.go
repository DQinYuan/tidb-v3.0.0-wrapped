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

import

// CompareInt64 returns an integer comparing the int64 x to y.
"github.com/pingcap/tidb/trace_util_0"

func CompareInt64(x, y int64) int {
	trace_util_0.Count(_compare_00000, 0)
	if x < y {
		trace_util_0.Count(_compare_00000, 2)
		return -1
	} else {
		trace_util_0.Count(_compare_00000, 3)
		if x == y {
			trace_util_0.Count(_compare_00000, 4)
			return 0
		}
	}

	trace_util_0.Count(_compare_00000, 1)
	return 1
}

// CompareUint64 returns an integer comparing the uint64 x to y.
func CompareUint64(x, y uint64) int {
	trace_util_0.Count(_compare_00000, 5)
	if x < y {
		trace_util_0.Count(_compare_00000, 7)
		return -1
	} else {
		trace_util_0.Count(_compare_00000, 8)
		if x == y {
			trace_util_0.Count(_compare_00000, 9)
			return 0
		}
	}

	trace_util_0.Count(_compare_00000, 6)
	return 1
}

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y float64) int {
	trace_util_0.Count(_compare_00000, 10)
	if x < y {
		trace_util_0.Count(_compare_00000, 12)
		return -1
	} else {
		trace_util_0.Count(_compare_00000, 13)
		if x == y {
			trace_util_0.Count(_compare_00000, 14)
			return 0
		}
	}

	trace_util_0.Count(_compare_00000, 11)
	return 1
}

// CompareString returns an integer comparing the string x to y.
func CompareString(x, y string) int {
	trace_util_0.Count(_compare_00000, 15)
	if x < y {
		trace_util_0.Count(_compare_00000, 17)
		return -1
	} else {
		trace_util_0.Count(_compare_00000, 18)
		if x == y {
			trace_util_0.Count(_compare_00000, 19)
			return 0
		}
	}

	trace_util_0.Count(_compare_00000, 16)
	return 1
}

var _compare_00000 = "types/compare.go"
