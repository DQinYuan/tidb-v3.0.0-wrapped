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

import "github.com/pingcap/tidb/trace_util_0"

var (
	// DefaultTxnMembufCap is the default transaction membuf capability.
	DefaultTxnMembufCap = 4 * 1024
	// ImportingTxnMembufCap is the capability of tidb importing data situation.
	ImportingTxnMembufCap = 32 * 1024
	// TempTxnMemBufCap is the capability of temporary membuf.
	TempTxnMemBufCap = 64
)

// BufferStore wraps a Retriever for read and a MemBuffer for buffered write.
// Common usage pattern:
//	bs := NewBufferStore(r) // use BufferStore to wrap a Retriever
//	// ...
//	// read/write on bs
//	// ...
//	bs.SaveTo(m)	        // save above operations to a Mutator
type BufferStore struct {
	MemBuffer
	r Retriever
}

// NewBufferStore creates a BufferStore using r for read.
func NewBufferStore(r Retriever, cap int) *BufferStore {
	trace_util_0.Count(_buffer_store_00000, 0)
	if cap <= 0 {
		trace_util_0.Count(_buffer_store_00000, 2)
		cap = DefaultTxnMembufCap
	}
	trace_util_0.Count(_buffer_store_00000, 1)
	return &BufferStore{
		r:         r,
		MemBuffer: &lazyMemBuffer{cap: cap},
	}
}

// Reset resets s.MemBuffer.
func (s *BufferStore) Reset() {
	trace_util_0.Count(_buffer_store_00000, 3)
	s.MemBuffer.Reset()
}

// SetCap sets the MemBuffer capability.
func (s *BufferStore) SetCap(cap int) {
	trace_util_0.Count(_buffer_store_00000, 4)
	s.MemBuffer.SetCap(cap)
}

// Get implements the Retriever interface.
func (s *BufferStore) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_buffer_store_00000, 5)
	val, err := s.MemBuffer.Get(k)
	if IsErrNotFound(err) {
		trace_util_0.Count(_buffer_store_00000, 9)
		val, err = s.r.Get(k)
	}
	trace_util_0.Count(_buffer_store_00000, 6)
	if err != nil {
		trace_util_0.Count(_buffer_store_00000, 10)
		return nil, err
	}
	trace_util_0.Count(_buffer_store_00000, 7)
	if len(val) == 0 {
		trace_util_0.Count(_buffer_store_00000, 11)
		return nil, ErrNotExist
	}
	trace_util_0.Count(_buffer_store_00000, 8)
	return val, nil
}

// Iter implements the Retriever interface.
func (s *BufferStore) Iter(k Key, upperBound Key) (Iterator, error) {
	trace_util_0.Count(_buffer_store_00000, 12)
	bufferIt, err := s.MemBuffer.Iter(k, upperBound)
	if err != nil {
		trace_util_0.Count(_buffer_store_00000, 15)
		return nil, err
	}
	trace_util_0.Count(_buffer_store_00000, 13)
	retrieverIt, err := s.r.Iter(k, upperBound)
	if err != nil {
		trace_util_0.Count(_buffer_store_00000, 16)
		return nil, err
	}
	trace_util_0.Count(_buffer_store_00000, 14)
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (s *BufferStore) IterReverse(k Key) (Iterator, error) {
	trace_util_0.Count(_buffer_store_00000, 17)
	bufferIt, err := s.MemBuffer.IterReverse(k)
	if err != nil {
		trace_util_0.Count(_buffer_store_00000, 20)
		return nil, err
	}
	trace_util_0.Count(_buffer_store_00000, 18)
	retrieverIt, err := s.r.IterReverse(k)
	if err != nil {
		trace_util_0.Count(_buffer_store_00000, 21)
		return nil, err
	}
	trace_util_0.Count(_buffer_store_00000, 19)
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// WalkBuffer iterates all buffered kv pairs.
func (s *BufferStore) WalkBuffer(f func(k Key, v []byte) error) error {
	trace_util_0.Count(_buffer_store_00000, 22)
	return WalkMemBuffer(s.MemBuffer, f)
}

// SaveTo saves all buffered kv pairs into a Mutator.
func (s *BufferStore) SaveTo(m Mutator) error {
	trace_util_0.Count(_buffer_store_00000, 23)
	err := s.WalkBuffer(func(k Key, v []byte) error {
		trace_util_0.Count(_buffer_store_00000, 25)
		if len(v) == 0 {
			trace_util_0.Count(_buffer_store_00000, 27)
			return m.Delete(k)
		}
		trace_util_0.Count(_buffer_store_00000, 26)
		return m.Set(k, v)
	})
	trace_util_0.Count(_buffer_store_00000, 24)
	return err
}

var _buffer_store_00000 = "kv/buffer_store.go"
