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

package disjointset

import

// IntSet is the int disjoint set.
"github.com/pingcap/tidb/trace_util_0"

type IntSet struct {
	parent []int
}

// NewIntSet returns a new int disjoint set.
func NewIntSet(size int) *IntSet {
	trace_util_0.Count(_int_set_00000, 0)
	p := make([]int, size)
	for i := range p {
		trace_util_0.Count(_int_set_00000, 2)
		p[i] = i
	}
	trace_util_0.Count(_int_set_00000, 1)
	return &IntSet{parent: p}
}

// Union unions two sets in int disjoint set.
func (m *IntSet) Union(a int, b int) {
	trace_util_0.Count(_int_set_00000, 3)
	m.parent[m.FindRoot(a)] = m.FindRoot(b)
}

// FindRoot finds the representative element of the set that `a` belongs to.
func (m *IntSet) FindRoot(a int) int {
	trace_util_0.Count(_int_set_00000, 4)
	if a == m.parent[a] {
		trace_util_0.Count(_int_set_00000, 6)
		return a
	}
	trace_util_0.Count(_int_set_00000, 5)
	m.parent[a] = m.FindRoot(m.parent[a])
	return m.parent[a]
}

var _int_set_00000 = "util/disjointset/int_set.go"
