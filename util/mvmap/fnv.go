// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2017 PingCAP, Inc.
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

package mvmap

import "github.com/pingcap/tidb/trace_util_0"

const (
	offset64 uint64 = 14695981039346656037
	prime64         = 1099511628211
)

// fnvHash64 is ported from go library, which is thread-safe.
func fnvHash64(data []byte) uint64 {
	trace_util_0.Count(_fnv_00000, 0)
	hash := offset64
	for _, c := range data {
		trace_util_0.Count(_fnv_00000, 2)
		hash *= prime64
		hash ^= uint64(c)
	}
	trace_util_0.Count(_fnv_00000, 1)
	return hash
}

var _fnv_00000 = "util/mvmap/fnv.go"
