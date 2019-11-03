// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package util

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
)

// ScanMetaWithPrefix scans metadata with the prefix.
func ScanMetaWithPrefix(retriever kv.Retriever, prefix kv.Key, filter func(kv.Key, []byte) bool) error {
	trace_util_0.Count(_prefix_helper_00000, 0)
	iter, err := retriever.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		trace_util_0.Count(_prefix_helper_00000, 3)
		return errors.Trace(err)
	}
	trace_util_0.Count(_prefix_helper_00000, 1)
	defer iter.Close()

	for {
		trace_util_0.Count(_prefix_helper_00000, 4)
		if err != nil {
			trace_util_0.Count(_prefix_helper_00000, 6)
			return errors.Trace(err)
		}

		trace_util_0.Count(_prefix_helper_00000, 5)
		if iter.Valid() && iter.Key().HasPrefix(prefix) {
			trace_util_0.Count(_prefix_helper_00000, 7)
			if !filter(iter.Key(), iter.Value()) {
				trace_util_0.Count(_prefix_helper_00000, 9)
				break
			}
			trace_util_0.Count(_prefix_helper_00000, 8)
			err = iter.Next()
			if err != nil {
				trace_util_0.Count(_prefix_helper_00000, 10)
				return errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_prefix_helper_00000, 11)
			{
				break
			}
		}
	}

	trace_util_0.Count(_prefix_helper_00000, 2)
	return nil
}

// DelKeyWithPrefix deletes keys with prefix.
func DelKeyWithPrefix(rm kv.RetrieverMutator, prefix kv.Key) error {
	trace_util_0.Count(_prefix_helper_00000, 12)
	var keys []kv.Key
	iter, err := rm.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		trace_util_0.Count(_prefix_helper_00000, 16)
		return errors.Trace(err)
	}

	trace_util_0.Count(_prefix_helper_00000, 13)
	defer iter.Close()
	for {
		trace_util_0.Count(_prefix_helper_00000, 17)
		if err != nil {
			trace_util_0.Count(_prefix_helper_00000, 19)
			return errors.Trace(err)
		}

		trace_util_0.Count(_prefix_helper_00000, 18)
		if iter.Valid() && iter.Key().HasPrefix(prefix) {
			trace_util_0.Count(_prefix_helper_00000, 20)
			keys = append(keys, iter.Key().Clone())
			err = iter.Next()
			if err != nil {
				trace_util_0.Count(_prefix_helper_00000, 21)
				return errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_prefix_helper_00000, 22)
			{
				break
			}
		}
	}

	trace_util_0.Count(_prefix_helper_00000, 14)
	for _, key := range keys {
		trace_util_0.Count(_prefix_helper_00000, 23)
		err := rm.Delete(key)
		if err != nil {
			trace_util_0.Count(_prefix_helper_00000, 24)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_prefix_helper_00000, 15)
	return nil
}

// RowKeyPrefixFilter returns a function which checks whether currentKey has decoded rowKeyPrefix as prefix.
func RowKeyPrefixFilter(rowKeyPrefix kv.Key) kv.FnKeyCmp {
	trace_util_0.Count(_prefix_helper_00000, 25)
	return func(currentKey kv.Key) bool {
		trace_util_0.Count(_prefix_helper_00000, 26)
		// Next until key without prefix of this record.
		return !bytes.HasPrefix(currentKey, rowKeyPrefix)
	}
}

var _prefix_helper_00000 = "util/prefix_helper.go"
