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
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

// AddUint64 adds uint64 a and b if no overflow, else returns error.
func AddUint64(a uint64, b uint64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 0)
	if math.MaxUint64-a < b {
		trace_util_0.Count(_overflow_00000, 2)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 1)
	return a + b, nil
}

// AddInt64 adds int64 a and b if no overflow, otherwise returns error.
func AddInt64(a int64, b int64) (int64, error) {
	trace_util_0.Count(_overflow_00000, 3)
	if (a > 0 && b > 0 && math.MaxInt64-a < b) ||
		(a < 0 && b < 0 && math.MinInt64-a > b) {
		trace_util_0.Count(_overflow_00000, 5)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}

	trace_util_0.Count(_overflow_00000, 4)
	return a + b, nil
}

// AddInteger adds uint64 a and int64 b and returns uint64 if no overflow error.
func AddInteger(a uint64, b int64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 6)
	if b >= 0 {
		trace_util_0.Count(_overflow_00000, 9)
		return AddUint64(a, uint64(b))
	}

	trace_util_0.Count(_overflow_00000, 7)
	if uint64(-b) > a {
		trace_util_0.Count(_overflow_00000, 10)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 8)
	return a - uint64(-b), nil
}

// SubUint64 subtracts uint64 a with b and returns uint64 if no overflow error.
func SubUint64(a uint64, b uint64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 11)
	if a < b {
		trace_util_0.Count(_overflow_00000, 13)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 12)
	return a - b, nil
}

// SubInt64 subtracts int64 a with b and returns int64 if no overflow error.
func SubInt64(a int64, b int64) (int64, error) {
	trace_util_0.Count(_overflow_00000, 14)
	if (a > 0 && b < 0 && math.MaxInt64-a < -b) ||
		(a < 0 && b > 0 && math.MinInt64-a > -b) ||
		(a == 0 && b == math.MinInt64) {
		trace_util_0.Count(_overflow_00000, 16)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 15)
	return a - b, nil
}

// SubUintWithInt subtracts uint64 a with int64 b and returns uint64 if no overflow error.
func SubUintWithInt(a uint64, b int64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 17)
	if b < 0 {
		trace_util_0.Count(_overflow_00000, 19)
		return AddUint64(a, uint64(-b))
	}
	trace_util_0.Count(_overflow_00000, 18)
	return SubUint64(a, uint64(b))
}

// SubIntWithUint subtracts int64 a with uint64 b and returns uint64 if no overflow error.
func SubIntWithUint(a int64, b uint64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 20)
	if a < 0 || uint64(a) < b {
		trace_util_0.Count(_overflow_00000, 22)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 21)
	return uint64(a) - b, nil
}

// MulUint64 multiplies uint64 a and b and returns uint64 if no overflow error.
func MulUint64(a uint64, b uint64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 23)
	if b > 0 && a > math.MaxUint64/b {
		trace_util_0.Count(_overflow_00000, 25)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}
	trace_util_0.Count(_overflow_00000, 24)
	return a * b, nil
}

// MulInt64 multiplies int64 a and b and returns int64 if no overflow error.
func MulInt64(a int64, b int64) (int64, error) {
	trace_util_0.Count(_overflow_00000, 26)
	if a == 0 || b == 0 {
		trace_util_0.Count(_overflow_00000, 32)
		return 0, nil
	}

	trace_util_0.Count(_overflow_00000, 27)
	var (
		res      uint64
		err      error
		negative = false
	)

	if a > 0 && b > 0 {
		trace_util_0.Count(_overflow_00000, 33)
		res, err = MulUint64(uint64(a), uint64(b))
	} else {
		trace_util_0.Count(_overflow_00000, 34)
		if a < 0 && b < 0 {
			trace_util_0.Count(_overflow_00000, 35)
			res, err = MulUint64(uint64(-a), uint64(-b))
		} else {
			trace_util_0.Count(_overflow_00000, 36)
			if a < 0 && b > 0 {
				trace_util_0.Count(_overflow_00000, 37)
				negative = true
				res, err = MulUint64(uint64(-a), uint64(b))
			} else {
				trace_util_0.Count(_overflow_00000, 38)
				{
					negative = true
					res, err = MulUint64(uint64(a), uint64(-b))
				}
			}
		}
	}

	trace_util_0.Count(_overflow_00000, 28)
	if err != nil {
		trace_util_0.Count(_overflow_00000, 39)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_overflow_00000, 29)
	if negative {
		trace_util_0.Count(_overflow_00000, 40)
		// negative result
		if res > math.MaxInt64+1 {
			trace_util_0.Count(_overflow_00000, 42)
			return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
		}

		trace_util_0.Count(_overflow_00000, 41)
		return -int64(res), nil
	}

	// positive result
	trace_util_0.Count(_overflow_00000, 30)
	if res > math.MaxInt64 {
		trace_util_0.Count(_overflow_00000, 43)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}

	trace_util_0.Count(_overflow_00000, 31)
	return int64(res), nil
}

// MulInteger multiplies uint64 a and int64 b, and returns uint64 if no overflow error.
func MulInteger(a uint64, b int64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 44)
	if a == 0 || b == 0 {
		trace_util_0.Count(_overflow_00000, 47)
		return 0, nil
	}

	trace_util_0.Count(_overflow_00000, 45)
	if b < 0 {
		trace_util_0.Count(_overflow_00000, 48)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
	}

	trace_util_0.Count(_overflow_00000, 46)
	return MulUint64(a, uint64(b))
}

// DivInt64 divides int64 a with b, returns int64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivInt64(a int64, b int64) (int64, error) {
	trace_util_0.Count(_overflow_00000, 49)
	if a == math.MinInt64 && b == -1 {
		trace_util_0.Count(_overflow_00000, 51)
		return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}

	trace_util_0.Count(_overflow_00000, 50)
	return a / b, nil
}

// DivUintWithInt divides uint64 a with int64 b, returns uint64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivUintWithInt(a uint64, b int64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 52)
	if b < 0 {
		trace_util_0.Count(_overflow_00000, 54)
		if a != 0 && uint64(-b) <= a {
			trace_util_0.Count(_overflow_00000, 56)
			return 0, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
		}

		trace_util_0.Count(_overflow_00000, 55)
		return 0, nil
	}

	trace_util_0.Count(_overflow_00000, 53)
	return a / uint64(b), nil
}

// DivIntWithUint divides int64 a with uint64 b, returns uint64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivIntWithUint(a int64, b uint64) (uint64, error) {
	trace_util_0.Count(_overflow_00000, 57)
	if a < 0 {
		trace_util_0.Count(_overflow_00000, 59)
		if uint64(-a) >= b {
			trace_util_0.Count(_overflow_00000, 61)
			return 0, ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
		}

		trace_util_0.Count(_overflow_00000, 60)
		return 0, nil
	}

	trace_util_0.Count(_overflow_00000, 58)
	return uint64(a) / b, nil
}

var _overflow_00000 = "types/overflow.go"
