// Copyright 2018 PingCAP, Inc.
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

// StringSet is a string set.
"github.com/pingcap/tidb/trace_util_0"

type StringSet map[string]struct{}

// NewStringSet builds a float64 set.
func NewStringSet() StringSet {
	trace_util_0.Count(_string_set_00000, 0)
	return make(map[string]struct{})
}

// Exist checks whether `val` exists in `s`.
func (s StringSet) Exist(val string) bool {
	trace_util_0.Count(_string_set_00000, 1)
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s StringSet) Insert(val string) {
	trace_util_0.Count(_string_set_00000, 2)
	s[val] = struct{}{}
}

var _string_set_00000 = "util/set/string_set.go"
