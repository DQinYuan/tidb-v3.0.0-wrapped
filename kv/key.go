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
	"bytes"
	"github.com/pingcap/tidb/trace_util_0"

	// Key represents high-level Key type.
)

type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	trace_util_0.Count(_key_00000, 0)
	// add 0x0 to the end of key
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

// PrefixNext returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' Next, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
func (k Key) PrefixNext() Key {
	trace_util_0.Count(_key_00000, 1)
	buf := make([]byte, len([]byte(k)))
	copy(buf, []byte(k))
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		trace_util_0.Count(_key_00000, 4)
		buf[i]++
		if buf[i] != 0 {
			trace_util_0.Count(_key_00000, 5)
			break
		}
	}
	trace_util_0.Count(_key_00000, 2)
	if i == -1 {
		trace_util_0.Count(_key_00000, 6)
		copy(buf, k)
		buf = append(buf, 0)
	}
	trace_util_0.Count(_key_00000, 3)
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	trace_util_0.Count(_key_00000, 7)
	return bytes.Compare(k, another)
}

// HasPrefix tests whether the Key begins with prefix.
func (k Key) HasPrefix(prefix Key) bool {
	trace_util_0.Count(_key_00000, 8)
	return bytes.HasPrefix(k, prefix)
}

// Clone returns a copy of the Key.
func (k Key) Clone() Key {
	trace_util_0.Count(_key_00000, 9)
	return append([]byte(nil), k...)
}

// KeyRange represents a range where StartKey <= key < EndKey.
type KeyRange struct {
	StartKey Key
	EndKey   Key
}

// IsPoint checks if the key range represents a point.
func (r *KeyRange) IsPoint() bool {
	trace_util_0.Count(_key_00000, 10)
	if len(r.StartKey) != len(r.EndKey) {
		trace_util_0.Count(_key_00000, 14)
		// Works like
		//   return bytes.Equal(r.StartKey.Next(), r.EndKey)

		startLen := len(r.StartKey)
		return startLen+1 == len(r.EndKey) &&
			r.EndKey[startLen] == 0 &&
			bytes.Equal(r.StartKey, r.EndKey[:startLen])
	}
	// Works like
	//   return bytes.Equal(r.StartKey.PrefixNext(), r.EndKey)

	trace_util_0.Count(_key_00000, 11)
	i := len(r.StartKey) - 1
	for ; i >= 0; i-- {
		trace_util_0.Count(_key_00000, 15)
		if r.StartKey[i] != 255 {
			trace_util_0.Count(_key_00000, 17)
			break
		}
		trace_util_0.Count(_key_00000, 16)
		if r.EndKey[i] != 0 {
			trace_util_0.Count(_key_00000, 18)
			return false
		}
	}
	trace_util_0.Count(_key_00000, 12)
	if i < 0 {
		trace_util_0.Count(_key_00000, 19)
		// In case all bytes in StartKey are 255.
		return false
	}
	// The byte at diffIdx in StartKey should be one less than the byte at diffIdx in EndKey.
	// And bytes in StartKey and EndKey before diffIdx should be equal.
	trace_util_0.Count(_key_00000, 13)
	diffOneIdx := i
	return r.StartKey[diffOneIdx]+1 == r.EndKey[diffOneIdx] &&
		bytes.Equal(r.StartKey[:diffOneIdx], r.EndKey[:diffOneIdx])
}

// EncodedKey represents encoded key in low-level storage engine.
type EncodedKey []byte

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k EncodedKey) Cmp(another EncodedKey) int {
	trace_util_0.Count(_key_00000, 20)
	return bytes.Compare(k, another)
}

// Next returns the next key in byte-order.
func (k EncodedKey) Next() EncodedKey {
	trace_util_0.Count(_key_00000, 21)
	return EncodedKey(bytes.Join([][]byte{k, Key{0}}, nil))
}

var _key_00000 = "kv/key.go"
