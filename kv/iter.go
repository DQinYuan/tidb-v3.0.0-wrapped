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

package kv

import

// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
"github.com/pingcap/tidb/trace_util_0"

func NextUntil(it Iterator, fn FnKeyCmp) error {
	trace_util_0.Count(_iter_00000, 0)
	var err error
	for it.Valid() && !fn(it.Key()) {
		trace_util_0.Count(_iter_00000, 2)
		err = it.Next()
		if err != nil {
			trace_util_0.Count(_iter_00000, 3)
			return err
		}
	}
	trace_util_0.Count(_iter_00000, 1)
	return nil
}

var _iter_00000 = "kv/iter.go"
