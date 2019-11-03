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
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
)

type listMeta struct {
	LIndex int64
	RIndex int64
}

func (meta listMeta) Value() []byte {
	trace_util_0.Count(_list_00000, 0)
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(meta.LIndex))
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.RIndex))
	return buf
}

func (meta listMeta) IsEmpty() bool {
	trace_util_0.Count(_list_00000, 1)
	return meta.LIndex >= meta.RIndex
}

// LPush prepends one or multiple values to a list.
func (t *TxStructure) LPush(key []byte, values ...[]byte) error {
	trace_util_0.Count(_list_00000, 2)
	return t.listPush(key, true, values...)
}

// RPush appends one or multiple values to a list.
func (t *TxStructure) RPush(key []byte, values ...[]byte) error {
	trace_util_0.Count(_list_00000, 3)
	return t.listPush(key, false, values...)
}

func (t *TxStructure) listPush(key []byte, left bool, values ...[]byte) error {
	trace_util_0.Count(_list_00000, 4)
	if t.readWriter == nil {
		trace_util_0.Count(_list_00000, 9)
		return errWriteOnSnapshot
	}
	trace_util_0.Count(_list_00000, 5)
	if len(values) == 0 {
		trace_util_0.Count(_list_00000, 10)
		return nil
	}

	trace_util_0.Count(_list_00000, 6)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil {
		trace_util_0.Count(_list_00000, 11)
		return errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 7)
	var index int64
	for _, v := range values {
		trace_util_0.Count(_list_00000, 12)
		if left {
			trace_util_0.Count(_list_00000, 14)
			meta.LIndex--
			index = meta.LIndex
		} else {
			trace_util_0.Count(_list_00000, 15)
			{
				index = meta.RIndex
				meta.RIndex++
			}
		}

		trace_util_0.Count(_list_00000, 13)
		dataKey := t.encodeListDataKey(key, index)
		if err = t.readWriter.Set(dataKey, v); err != nil {
			trace_util_0.Count(_list_00000, 16)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_list_00000, 8)
	return t.readWriter.Set(metaKey, meta.Value())
}

// LPop removes and gets the first element in a list.
func (t *TxStructure) LPop(key []byte) ([]byte, error) {
	trace_util_0.Count(_list_00000, 17)
	return t.listPop(key, true)
}

// RPop removes and gets the last element in a list.
func (t *TxStructure) RPop(key []byte) ([]byte, error) {
	trace_util_0.Count(_list_00000, 18)
	return t.listPop(key, false)
}

func (t *TxStructure) listPop(key []byte, left bool) ([]byte, error) {
	trace_util_0.Count(_list_00000, 19)
	if t.readWriter == nil {
		trace_util_0.Count(_list_00000, 26)
		return nil, errWriteOnSnapshot
	}
	trace_util_0.Count(_list_00000, 20)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 27)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 21)
	var index int64
	if left {
		trace_util_0.Count(_list_00000, 28)
		index = meta.LIndex
		meta.LIndex++
	} else {
		trace_util_0.Count(_list_00000, 29)
		{
			meta.RIndex--
			index = meta.RIndex
		}
	}

	trace_util_0.Count(_list_00000, 22)
	dataKey := t.encodeListDataKey(key, index)

	var data []byte
	data, err = t.reader.Get(dataKey)
	if err != nil {
		trace_util_0.Count(_list_00000, 30)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 23)
	if err = t.readWriter.Delete(dataKey); err != nil {
		trace_util_0.Count(_list_00000, 31)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 24)
	if !meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 32)
		err = t.readWriter.Set(metaKey, meta.Value())
	} else {
		trace_util_0.Count(_list_00000, 33)
		{
			err = t.readWriter.Delete(metaKey)
		}
	}

	trace_util_0.Count(_list_00000, 25)
	return data, errors.Trace(err)
}

// LLen gets the length of a list.
func (t *TxStructure) LLen(key []byte) (int64, error) {
	trace_util_0.Count(_list_00000, 34)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	return meta.RIndex - meta.LIndex, errors.Trace(err)
}

// LGetAll gets all elements of this list in order from right to left.
func (t *TxStructure) LGetAll(key []byte) ([][]byte, error) {
	trace_util_0.Count(_list_00000, 35)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 38)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 36)
	length := int(meta.RIndex - meta.LIndex)
	elements := make([][]byte, 0, length)
	for index := meta.RIndex - 1; index >= meta.LIndex; index-- {
		trace_util_0.Count(_list_00000, 39)
		e, err := t.reader.Get(t.encodeListDataKey(key, index))
		if err != nil {
			trace_util_0.Count(_list_00000, 41)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_list_00000, 40)
		elements = append(elements, e)
	}
	trace_util_0.Count(_list_00000, 37)
	return elements, nil
}

// LIndex gets an element from a list by its index.
func (t *TxStructure) LIndex(key []byte, index int64) ([]byte, error) {
	trace_util_0.Count(_list_00000, 42)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 45)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 43)
	index = adjustIndex(index, meta.LIndex, meta.RIndex)

	if index >= meta.LIndex && index < meta.RIndex {
		trace_util_0.Count(_list_00000, 46)
		return t.reader.Get(t.encodeListDataKey(key, index))
	}
	trace_util_0.Count(_list_00000, 44)
	return nil, nil
}

// LSet updates an element in the list by its index.
func (t *TxStructure) LSet(key []byte, index int64, value []byte) error {
	trace_util_0.Count(_list_00000, 47)
	if t.readWriter == nil {
		trace_util_0.Count(_list_00000, 51)
		return errWriteOnSnapshot
	}
	trace_util_0.Count(_list_00000, 48)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 52)
		return errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 49)
	index = adjustIndex(index, meta.LIndex, meta.RIndex)

	if index >= meta.LIndex && index < meta.RIndex {
		trace_util_0.Count(_list_00000, 53)
		return t.readWriter.Set(t.encodeListDataKey(key, index), value)
	}
	trace_util_0.Count(_list_00000, 50)
	return errInvalidListIndex.GenWithStack("invalid list index %d", index)
}

// LClear removes the list of the key.
func (t *TxStructure) LClear(key []byte) error {
	trace_util_0.Count(_list_00000, 54)
	if t.readWriter == nil {
		trace_util_0.Count(_list_00000, 58)
		return errWriteOnSnapshot
	}
	trace_util_0.Count(_list_00000, 55)
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		trace_util_0.Count(_list_00000, 59)
		return errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 56)
	for index := meta.LIndex; index < meta.RIndex; index++ {
		trace_util_0.Count(_list_00000, 60)
		dataKey := t.encodeListDataKey(key, index)
		if err = t.readWriter.Delete(dataKey); err != nil {
			trace_util_0.Count(_list_00000, 61)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_list_00000, 57)
	return t.readWriter.Delete(metaKey)
}

func (t *TxStructure) loadListMeta(metaKey []byte) (listMeta, error) {
	trace_util_0.Count(_list_00000, 62)
	v, err := t.reader.Get(metaKey)
	if kv.ErrNotExist.Equal(err) {
		trace_util_0.Count(_list_00000, 67)
		err = nil
	}
	trace_util_0.Count(_list_00000, 63)
	if err != nil {
		trace_util_0.Count(_list_00000, 68)
		return listMeta{}, errors.Trace(err)
	}

	trace_util_0.Count(_list_00000, 64)
	meta := listMeta{0, 0}
	if v == nil {
		trace_util_0.Count(_list_00000, 69)
		return meta, nil
	}

	trace_util_0.Count(_list_00000, 65)
	if len(v) != 16 {
		trace_util_0.Count(_list_00000, 70)
		return meta, errInvalidListMetaData
	}

	trace_util_0.Count(_list_00000, 66)
	meta.LIndex = int64(binary.BigEndian.Uint64(v[0:8]))
	meta.RIndex = int64(binary.BigEndian.Uint64(v[8:16]))
	return meta, nil
}

func adjustIndex(index int64, min, max int64) int64 {
	trace_util_0.Count(_list_00000, 71)
	if index >= 0 {
		trace_util_0.Count(_list_00000, 73)
		return index + min
	}

	trace_util_0.Count(_list_00000, 72)
	return index + max
}

var _list_00000 = "structure/list.go"
