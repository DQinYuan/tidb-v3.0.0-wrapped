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

package set

import

// IntSet is a int set.
"github.com/pingcap/tidb/trace_util_0"

type IntSet map[int]struct{}

// NewIntSet builds a IntSet.
func NewIntSet() IntSet {
	trace_util_0.Count(_int_set_00000, 0)
	return make(map[int]struct{})
}

// Exist checks whether `val` exists in `s`.
func (s IntSet) Exist(val int) bool {
	trace_util_0.Count(_int_set_00000, 1)
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s IntSet) Insert(val int) {
	trace_util_0.Count(_int_set_00000, 2)
	s[val] = struct{}{}
}

// Int64Set is a int64 set.
type Int64Set map[int64]struct{}

// NewInt64Set builds a Int64Set.
func NewInt64Set() Int64Set {
	trace_util_0.Count(_int_set_00000, 3)
	return make(map[int64]struct{})
}

// Exist checks whether `val` exists in `s`.
func (s Int64Set) Exist(val int64) bool {
	trace_util_0.Count(_int_set_00000, 4)
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s Int64Set) Insert(val int64) {
	trace_util_0.Count(_int_set_00000, 5)
	s[val] = struct{}{}
}

var _int_set_00000 = "util/set/int_set.go"
