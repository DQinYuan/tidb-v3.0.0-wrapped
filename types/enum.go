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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

// Enum is for MySQL enum type.
type Enum struct {
	Name  string
	Value uint64
}

// String implements fmt.Stringer interface.
func (e Enum) String() string {
	trace_util_0.Count(_enum_00000, 0)
	return e.Name
}

// ToNumber changes enum index to float64 for numeric operation.
func (e Enum) ToNumber() float64 {
	trace_util_0.Count(_enum_00000, 1)
	return float64(e.Value)
}

// ParseEnumName creates a Enum with item name.
func ParseEnumName(elems []string, name string) (Enum, error) {
	trace_util_0.Count(_enum_00000, 2)
	for i, n := range elems {
		trace_util_0.Count(_enum_00000, 5)
		if strings.EqualFold(n, name) {
			trace_util_0.Count(_enum_00000, 6)
			return Enum{Name: n, Value: uint64(i) + 1}, nil
		}
	}

	// name doesn't exist, maybe an integer?
	trace_util_0.Count(_enum_00000, 3)
	if num, err := strconv.ParseUint(name, 0, 64); err == nil {
		trace_util_0.Count(_enum_00000, 7)
		return ParseEnumValue(elems, num)
	}

	trace_util_0.Count(_enum_00000, 4)
	return Enum{}, errors.Errorf("item %s is not in enum %v", name, elems)
}

// ParseEnumValue creates a Enum with special number.
func ParseEnumValue(elems []string, number uint64) (Enum, error) {
	trace_util_0.Count(_enum_00000, 8)
	if number == 0 || number > uint64(len(elems)) {
		trace_util_0.Count(_enum_00000, 10)
		return Enum{}, errors.Errorf("number %d overflow enum boundary [1, %d]", number, len(elems))
	}

	trace_util_0.Count(_enum_00000, 9)
	return Enum{Name: elems[number-1], Value: number}, nil
}

var _enum_00000 = "types/enum.go"
