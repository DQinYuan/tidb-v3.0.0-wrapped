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

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

// IncInt64 increases the value for key k in kv store by step.
func IncInt64(rm RetrieverMutator, k Key, step int64) (int64, error) {
	trace_util_0.Count(_utils_00000, 0)
	val, err := rm.Get(k)
	if IsErrNotFound(err) {
		trace_util_0.Count(_utils_00000, 5)
		err = rm.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			trace_util_0.Count(_utils_00000, 7)
			return 0, err
		}
		trace_util_0.Count(_utils_00000, 6)
		return step, nil
	}
	trace_util_0.Count(_utils_00000, 1)
	if err != nil {
		trace_util_0.Count(_utils_00000, 8)
		return 0, err
	}

	trace_util_0.Count(_utils_00000, 2)
	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		trace_util_0.Count(_utils_00000, 9)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_utils_00000, 3)
	intVal += step
	err = rm.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		trace_util_0.Count(_utils_00000, 10)
		return 0, err
	}
	trace_util_0.Count(_utils_00000, 4)
	return intVal, nil
}

// GetInt64 get int64 value which created by IncInt64 method.
func GetInt64(r Retriever, k Key) (int64, error) {
	trace_util_0.Count(_utils_00000, 11)
	val, err := r.Get(k)
	if IsErrNotFound(err) {
		trace_util_0.Count(_utils_00000, 15)
		return 0, nil
	}
	trace_util_0.Count(_utils_00000, 12)
	if err != nil {
		trace_util_0.Count(_utils_00000, 16)
		return 0, err
	}
	trace_util_0.Count(_utils_00000, 13)
	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		trace_util_0.Count(_utils_00000, 17)
		return intVal, errors.Trace(err)
	}
	trace_util_0.Count(_utils_00000, 14)
	return intVal, nil
}

var _utils_00000 = "kv/utils.go"
