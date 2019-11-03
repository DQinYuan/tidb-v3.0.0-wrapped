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

package structure

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
)

// Set sets the string value of the key.
func (t *TxStructure) Set(key []byte, value []byte) error {
	trace_util_0.Count(_string_00000, 0)
	if t.readWriter == nil {
		trace_util_0.Count(_string_00000, 2)
		return errWriteOnSnapshot
	}
	trace_util_0.Count(_string_00000, 1)
	ek := t.encodeStringDataKey(key)
	return t.readWriter.Set(ek, value)
}

// Get gets the string value of a key.
func (t *TxStructure) Get(key []byte) ([]byte, error) {
	trace_util_0.Count(_string_00000, 3)
	ek := t.encodeStringDataKey(key)
	value, err := t.reader.Get(ek)
	if kv.ErrNotExist.Equal(err) {
		trace_util_0.Count(_string_00000, 5)
		err = nil
	}
	trace_util_0.Count(_string_00000, 4)
	return value, errors.Trace(err)
}

// GetInt64 gets the int64 value of a key.
func (t *TxStructure) GetInt64(key []byte) (int64, error) {
	trace_util_0.Count(_string_00000, 6)
	v, err := t.Get(key)
	if err != nil || v == nil {
		trace_util_0.Count(_string_00000, 8)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_string_00000, 7)
	n, err := strconv.ParseInt(string(v), 10, 64)
	return n, errors.Trace(err)
}

// Inc increments the integer value of a key by step, returns
// the value after the increment.
func (t *TxStructure) Inc(key []byte, step int64) (int64, error) {
	trace_util_0.Count(_string_00000, 9)
	if t.readWriter == nil {
		trace_util_0.Count(_string_00000, 12)
		return 0, errWriteOnSnapshot
	}
	trace_util_0.Count(_string_00000, 10)
	ek := t.encodeStringDataKey(key)
	// txn Inc will lock this key, so we don't lock it here.
	n, err := kv.IncInt64(t.readWriter, ek, step)
	if kv.ErrNotExist.Equal(err) {
		trace_util_0.Count(_string_00000, 13)
		err = nil
	}
	trace_util_0.Count(_string_00000, 11)
	return n, errors.Trace(err)
}

// Clear removes the string value of the key.
func (t *TxStructure) Clear(key []byte) error {
	trace_util_0.Count(_string_00000, 14)
	if t.readWriter == nil {
		trace_util_0.Count(_string_00000, 17)
		return errWriteOnSnapshot
	}
	trace_util_0.Count(_string_00000, 15)
	ek := t.encodeStringDataKey(key)
	err := t.readWriter.Delete(ek)
	if kv.ErrNotExist.Equal(err) {
		trace_util_0.Count(_string_00000, 18)
		err = nil
	}
	trace_util_0.Count(_string_00000, 16)
	return errors.Trace(err)
}

var _string_00000 = "structure/string.go"
