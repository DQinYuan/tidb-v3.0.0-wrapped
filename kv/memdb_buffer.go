// Copyright 2015 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
)

// memDbBuffer implements the MemBuffer interface.
type memDbBuffer struct {
	db              *memdb.DB
	entrySizeLimit  int
	bufferLenLimit  uint64
	bufferSizeLimit int
}

type memDbIter struct {
	iter    iterator.Iterator
	reverse bool
}

// NewMemDbBuffer creates a new memDbBuffer.
func NewMemDbBuffer(cap int) MemBuffer {
	trace_util_0.Count(_memdb_buffer_00000, 0)
	return &memDbBuffer{
		db:              memdb.New(comparer.DefaultComparer, cap),
		entrySizeLimit:  TxnEntrySizeLimit,
		bufferLenLimit:  atomic.LoadUint64(&TxnEntryCountLimit),
		bufferSizeLimit: TxnTotalSizeLimit,
	}
}

// Iter creates an Iterator.
func (m *memDbBuffer) Iter(k Key, upperBound Key) (Iterator, error) {
	trace_util_0.Count(_memdb_buffer_00000, 1)
	i := &memDbIter{iter: m.db.NewIterator(&util.Range{Start: []byte(k), Limit: []byte(upperBound)}), reverse: false}

	err := i.Next()
	if err != nil {
		trace_util_0.Count(_memdb_buffer_00000, 3)
		return nil, err
	}
	trace_util_0.Count(_memdb_buffer_00000, 2)
	return i, nil
}

func (m *memDbBuffer) SetCap(cap int) {
	trace_util_0.Count(_memdb_buffer_00000, 4)

}

func (m *memDbBuffer) IterReverse(k Key) (Iterator, error) {
	trace_util_0.Count(_memdb_buffer_00000, 5)
	var i *memDbIter
	if k == nil {
		trace_util_0.Count(_memdb_buffer_00000, 7)
		i = &memDbIter{iter: m.db.NewIterator(&util.Range{}), reverse: true}
	} else {
		trace_util_0.Count(_memdb_buffer_00000, 8)
		{
			i = &memDbIter{iter: m.db.NewIterator(&util.Range{Limit: []byte(k)}), reverse: true}
		}
	}
	trace_util_0.Count(_memdb_buffer_00000, 6)
	i.iter.Last()
	return i, nil
}

// Get returns the value associated with key.
func (m *memDbBuffer) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_memdb_buffer_00000, 9)
	v, err := m.db.Get(k)
	if terror.ErrorEqual(err, leveldb.ErrNotFound) {
		trace_util_0.Count(_memdb_buffer_00000, 11)
		return nil, ErrNotExist
	}
	trace_util_0.Count(_memdb_buffer_00000, 10)
	return v, nil
}

// Set associates key with value.
func (m *memDbBuffer) Set(k Key, v []byte) error {
	trace_util_0.Count(_memdb_buffer_00000, 12)
	if len(v) == 0 {
		trace_util_0.Count(_memdb_buffer_00000, 17)
		return errors.Trace(ErrCannotSetNilValue)
	}
	trace_util_0.Count(_memdb_buffer_00000, 13)
	if len(k)+len(v) > m.entrySizeLimit {
		trace_util_0.Count(_memdb_buffer_00000, 18)
		return ErrEntryTooLarge.GenWithStackByArgs(m.entrySizeLimit, len(k)+len(v))
	}

	trace_util_0.Count(_memdb_buffer_00000, 14)
	err := m.db.Put(k, v)
	if m.Size() > m.bufferSizeLimit {
		trace_util_0.Count(_memdb_buffer_00000, 19)
		return ErrTxnTooLarge.GenWithStack("transaction too large, size:%d", m.Size())
	}
	trace_util_0.Count(_memdb_buffer_00000, 15)
	if m.Len() > int(m.bufferLenLimit) {
		trace_util_0.Count(_memdb_buffer_00000, 20)
		return ErrTxnTooLarge.GenWithStack("transaction too large, len:%d", m.Len())
	}
	trace_util_0.Count(_memdb_buffer_00000, 16)
	return errors.Trace(err)
}

// Delete removes the entry from buffer with provided key.
func (m *memDbBuffer) Delete(k Key) error {
	trace_util_0.Count(_memdb_buffer_00000, 21)
	err := m.db.Put(k, nil)
	return errors.Trace(err)
}

// Size returns sum of keys and values length.
func (m *memDbBuffer) Size() int {
	trace_util_0.Count(_memdb_buffer_00000, 22)
	return m.db.Size()
}

// Len returns the number of entries in the DB.
func (m *memDbBuffer) Len() int {
	trace_util_0.Count(_memdb_buffer_00000, 23)
	return m.db.Len()
}

// Reset cleanup the MemBuffer.
func (m *memDbBuffer) Reset() {
	trace_util_0.Count(_memdb_buffer_00000, 24)
	m.db.Reset()
}

// Next implements the Iterator Next.
func (i *memDbIter) Next() error {
	trace_util_0.Count(_memdb_buffer_00000, 25)
	if i.reverse {
		trace_util_0.Count(_memdb_buffer_00000, 27)
		i.iter.Prev()
	} else {
		trace_util_0.Count(_memdb_buffer_00000, 28)
		{
			i.iter.Next()
		}
	}
	trace_util_0.Count(_memdb_buffer_00000, 26)
	return nil
}

// Valid implements the Iterator Valid.
func (i *memDbIter) Valid() bool {
	trace_util_0.Count(_memdb_buffer_00000, 29)
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *memDbIter) Key() Key {
	trace_util_0.Count(_memdb_buffer_00000, 30)
	return i.iter.Key()
}

// Value implements the Iterator Value.
func (i *memDbIter) Value() []byte {
	trace_util_0.Count(_memdb_buffer_00000, 31)
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *memDbIter) Close() {
	trace_util_0.Count(_memdb_buffer_00000, 32)
	i.iter.Release()
}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf MemBuffer, f func(k Key, v []byte) error) error {
	trace_util_0.Count(_memdb_buffer_00000, 33)
	iter, err := memBuf.Iter(nil, nil)
	if err != nil {
		trace_util_0.Count(_memdb_buffer_00000, 36)
		return errors.Trace(err)
	}

	trace_util_0.Count(_memdb_buffer_00000, 34)
	defer iter.Close()
	for iter.Valid() {
		trace_util_0.Count(_memdb_buffer_00000, 37)
		if err = f(iter.Key(), iter.Value()); err != nil {
			trace_util_0.Count(_memdb_buffer_00000, 39)
			return errors.Trace(err)
		}
		trace_util_0.Count(_memdb_buffer_00000, 38)
		err = iter.Next()
		if err != nil {
			trace_util_0.Count(_memdb_buffer_00000, 40)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_memdb_buffer_00000, 35)
	return nil
}

var _memdb_buffer_00000 = "kv/memdb_buffer.go"
