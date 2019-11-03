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

var zeroSet = Set{Name: "", Value: 0}

// Set is for MySQL Set type.
type Set struct {
	Name  string
	Value uint64
}

// String implements fmt.Stringer interface.
func (e Set) String() string {
	trace_util_0.Count(_set_00000, 0)
	return e.Name
}

// ToNumber changes Set to float64 for numeric operation.
func (e Set) ToNumber() float64 {
	trace_util_0.Count(_set_00000, 1)
	return float64(e.Value)
}

// ParseSetName creates a Set with name.
func ParseSetName(elems []string, name string) (Set, error) {
	trace_util_0.Count(_set_00000, 2)
	if len(name) == 0 {
		trace_util_0.Count(_set_00000, 8)
		return zeroSet, nil
	}

	trace_util_0.Count(_set_00000, 3)
	seps := strings.Split(name, ",")
	marked := make(map[string]struct{}, len(seps))
	for _, s := range seps {
		trace_util_0.Count(_set_00000, 9)
		marked[strings.ToLower(s)] = struct{}{}
	}
	trace_util_0.Count(_set_00000, 4)
	items := make([]string, 0, len(seps))

	value := uint64(0)
	for i, n := range elems {
		trace_util_0.Count(_set_00000, 10)
		key := strings.ToLower(n)
		if _, ok := marked[key]; ok {
			trace_util_0.Count(_set_00000, 11)
			value |= 1 << uint64(i)
			delete(marked, key)
			items = append(items, n)
		}
	}

	trace_util_0.Count(_set_00000, 5)
	if len(marked) == 0 {
		trace_util_0.Count(_set_00000, 12)
		return Set{Name: strings.Join(items, ","), Value: value}, nil
	}

	// name doesn't exist, maybe an integer?
	trace_util_0.Count(_set_00000, 6)
	if num, err := strconv.ParseUint(name, 0, 64); err == nil {
		trace_util_0.Count(_set_00000, 13)
		return ParseSetValue(elems, num)
	}

	trace_util_0.Count(_set_00000, 7)
	return Set{}, errors.Errorf("item %s is not in Set %v", name, elems)
}

var (
	setIndexValue       []uint64
	setIndexInvertValue []uint64
)

func init() {
	trace_util_0.Count(_set_00000, 14)
	setIndexValue = make([]uint64, 64)
	setIndexInvertValue = make([]uint64, 64)

	for i := 0; i < 64; i++ {
		trace_util_0.Count(_set_00000, 15)
		setIndexValue[i] = 1 << uint64(i)
		setIndexInvertValue[i] = ^setIndexValue[i]
	}
}

// ParseSetValue creates a Set with special number.
func ParseSetValue(elems []string, number uint64) (Set, error) {
	trace_util_0.Count(_set_00000, 16)
	if number == 0 {
		trace_util_0.Count(_set_00000, 20)
		return zeroSet, nil
	}

	trace_util_0.Count(_set_00000, 17)
	value := number
	var items []string
	for i := 0; i < len(elems); i++ {
		trace_util_0.Count(_set_00000, 21)
		if number&setIndexValue[i] > 0 {
			trace_util_0.Count(_set_00000, 22)
			items = append(items, elems[i])
			number &= setIndexInvertValue[i]
		}
	}

	trace_util_0.Count(_set_00000, 18)
	if number != 0 {
		trace_util_0.Count(_set_00000, 23)
		return Set{}, errors.Errorf("invalid number %d for Set %v", number, elems)
	}

	trace_util_0.Count(_set_00000, 19)
	return Set{Name: strings.Join(items, ","), Value: value}, nil
}

var _set_00000 = "types/set.go"
