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

// UnionStore is a store that wraps a snapshot for read and a BufferStore for buffered write.
// Also, it provides some transaction related utilities.
"github.com/pingcap/tidb/trace_util_0"

type UnionStore interface {
	MemBuffer
	// Returns related condition pair
	LookupConditionPair(k Key) *conditionPair
	// DeleteConditionPair deletes a condition pair.
	DeleteConditionPair(k Key)
	// WalkBuffer iterates all buffered kv pairs.
	WalkBuffer(f func(k Key, v []byte) error) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// GetOption gets an option.
	GetOption(opt Option) interface{}
	// GetMemBuffer return the MemBuffer binding to this UnionStore.
	GetMemBuffer() MemBuffer
}

// AssertionType is the type of a assertion.
type AssertionType int

// The AssertionType constants.
const (
	None AssertionType = iota
	Exist
	NotExist
)

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

// conditionPair is used to store lazy check condition.
// If condition not match (value is not equal as expected one), returns err.
type conditionPair struct {
	key   Key
	value []byte
	err   error
}

func (c *conditionPair) ShouldNotExist() bool {
	trace_util_0.Count(_union_store_00000, 0)
	return len(c.value) == 0
}

func (c *conditionPair) Err() error {
	trace_util_0.Count(_union_store_00000, 1)
	return c.err
}

// unionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
	*BufferStore
	lazyConditionPairs map[string]*conditionPair // for delay check
	opts               options
}

// NewUnionStore builds a new UnionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	trace_util_0.Count(_union_store_00000, 2)
	return &unionStore{
		BufferStore:        NewBufferStore(snapshot, DefaultTxnMembufCap),
		lazyConditionPairs: make(map[string]*conditionPair),
		opts:               make(map[Option]interface{}),
	}
}

// invalidIterator implements Iterator interface.
// It is used for read-only transaction which has no data written, the iterator is always invalid.
type invalidIterator struct{}

func (it invalidIterator) Valid() bool {
	trace_util_0.Count(_union_store_00000, 3)
	return false
}

func (it invalidIterator) Next() error {
	trace_util_0.Count(_union_store_00000, 4)
	return nil
}

func (it invalidIterator) Key() Key {
	trace_util_0.Count(_union_store_00000, 5)
	return nil
}

func (it invalidIterator) Value() []byte {
	trace_util_0.Count(_union_store_00000, 6)
	return nil
}

func (it invalidIterator) Close() { trace_util_0.Count(_union_store_00000, 7) }

// lazyMemBuffer wraps a MemBuffer which is to be initialized when it is modified.
type lazyMemBuffer struct {
	mb  MemBuffer
	cap int
}

func (lmb *lazyMemBuffer) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_union_store_00000, 8)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 10)
		return nil, ErrNotExist
	}

	trace_util_0.Count(_union_store_00000, 9)
	return lmb.mb.Get(k)
}

func (lmb *lazyMemBuffer) Set(key Key, value []byte) error {
	trace_util_0.Count(_union_store_00000, 11)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 13)
		lmb.mb = NewMemDbBuffer(lmb.cap)
	}

	trace_util_0.Count(_union_store_00000, 12)
	return lmb.mb.Set(key, value)
}

func (lmb *lazyMemBuffer) Delete(k Key) error {
	trace_util_0.Count(_union_store_00000, 14)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 16)
		lmb.mb = NewMemDbBuffer(lmb.cap)
	}

	trace_util_0.Count(_union_store_00000, 15)
	return lmb.mb.Delete(k)
}

func (lmb *lazyMemBuffer) Iter(k Key, upperBound Key) (Iterator, error) {
	trace_util_0.Count(_union_store_00000, 17)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 19)
		return invalidIterator{}, nil
	}
	trace_util_0.Count(_union_store_00000, 18)
	return lmb.mb.Iter(k, upperBound)
}

func (lmb *lazyMemBuffer) IterReverse(k Key) (Iterator, error) {
	trace_util_0.Count(_union_store_00000, 20)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 22)
		return invalidIterator{}, nil
	}
	trace_util_0.Count(_union_store_00000, 21)
	return lmb.mb.IterReverse(k)
}

func (lmb *lazyMemBuffer) Size() int {
	trace_util_0.Count(_union_store_00000, 23)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 25)
		return 0
	}
	trace_util_0.Count(_union_store_00000, 24)
	return lmb.mb.Size()
}

func (lmb *lazyMemBuffer) Len() int {
	trace_util_0.Count(_union_store_00000, 26)
	if lmb.mb == nil {
		trace_util_0.Count(_union_store_00000, 28)
		return 0
	}
	trace_util_0.Count(_union_store_00000, 27)
	return lmb.mb.Len()
}

func (lmb *lazyMemBuffer) Reset() {
	trace_util_0.Count(_union_store_00000, 29)
	if lmb.mb != nil {
		trace_util_0.Count(_union_store_00000, 30)
		lmb.mb.Reset()
	}
}

func (lmb *lazyMemBuffer) SetCap(cap int) {
	trace_util_0.Count(_union_store_00000, 31)
	lmb.cap = cap
}

// Get implements the Retriever interface.
func (us *unionStore) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_union_store_00000, 32)
	v, err := us.MemBuffer.Get(k)
	if IsErrNotFound(err) {
		trace_util_0.Count(_union_store_00000, 37)
		if _, ok := us.opts.Get(PresumeKeyNotExists); ok {
			trace_util_0.Count(_union_store_00000, 38)
			e, ok := us.opts.Get(PresumeKeyNotExistsError)
			if ok && e != nil {
				trace_util_0.Count(_union_store_00000, 40)
				us.markLazyConditionPair(k, nil, e.(error))
			} else {
				trace_util_0.Count(_union_store_00000, 41)
				{
					us.markLazyConditionPair(k, nil, ErrKeyExists)
				}
			}
			trace_util_0.Count(_union_store_00000, 39)
			return nil, ErrNotExist
		}
	}
	trace_util_0.Count(_union_store_00000, 33)
	if IsErrNotFound(err) {
		trace_util_0.Count(_union_store_00000, 42)
		v, err = us.BufferStore.r.Get(k)
	}
	trace_util_0.Count(_union_store_00000, 34)
	if err != nil {
		trace_util_0.Count(_union_store_00000, 43)
		return v, err
	}
	trace_util_0.Count(_union_store_00000, 35)
	if len(v) == 0 {
		trace_util_0.Count(_union_store_00000, 44)
		return nil, ErrNotExist
	}
	trace_util_0.Count(_union_store_00000, 36)
	return v, nil
}

// markLazyConditionPair marks a kv pair for later check.
// If condition not match, should return e as error.
func (us *unionStore) markLazyConditionPair(k Key, v []byte, e error) {
	trace_util_0.Count(_union_store_00000, 45)
	us.lazyConditionPairs[string(k)] = &conditionPair{
		key:   k.Clone(),
		value: v,
		err:   e,
	}
}

func (us *unionStore) LookupConditionPair(k Key) *conditionPair {
	trace_util_0.Count(_union_store_00000, 46)
	if c, ok := us.lazyConditionPairs[string(k)]; ok {
		trace_util_0.Count(_union_store_00000, 48)
		return c
	}
	trace_util_0.Count(_union_store_00000, 47)
	return nil
}

func (us *unionStore) DeleteConditionPair(k Key) {
	trace_util_0.Count(_union_store_00000, 49)
	delete(us.lazyConditionPairs, string(k))
}

// SetOption implements the UnionStore SetOption interface.
func (us *unionStore) SetOption(opt Option, val interface{}) {
	trace_util_0.Count(_union_store_00000, 50)
	us.opts[opt] = val
}

// DelOption implements the UnionStore DelOption interface.
func (us *unionStore) DelOption(opt Option) {
	trace_util_0.Count(_union_store_00000, 51)
	delete(us.opts, opt)
}

// GetOption implements the UnionStore GetOption interface.
func (us *unionStore) GetOption(opt Option) interface{} {
	trace_util_0.Count(_union_store_00000, 52)
	return us.opts[opt]
}

// GetMemBuffer return the MemBuffer binding to this UnionStore.
func (us *unionStore) GetMemBuffer() MemBuffer {
	trace_util_0.Count(_union_store_00000, 53)
	return us.BufferStore.MemBuffer
}

// SetCap sets membuffer capability.
func (us *unionStore) SetCap(cap int) {
	trace_util_0.Count(_union_store_00000, 54)
	us.BufferStore.SetCap(cap)
}

func (us *unionStore) Reset() {
	trace_util_0.Count(_union_store_00000, 55)
	us.BufferStore.Reset()
}

type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	trace_util_0.Count(_union_store_00000, 56)
	v, ok := opts[opt]
	return v, ok
}

var _union_store_00000 = "kv/union_store.go"
