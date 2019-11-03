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

package kvcache

import (
	"container/list"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/memory"
)

// Key is the interface that every key in LRU Cache should implement.
type Key interface {
	Hash() []byte
}

// Value is the interface that every value in LRU Cache should implement.
type Value interface {
}

// cacheEntry wraps Key and Value. It's the value of list.Element.
type cacheEntry struct {
	key   Key
	value Value
}

// SimpleLRUCache is a simple least recently used cache, not thread-safe, use it carefully.
type SimpleLRUCache struct {
	capacity uint
	size     uint
	quota    uint64
	guard    float64
	elements map[string]*list.Element
	cache    *list.List
}

// NewSimpleLRUCache creates a SimpleLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewSimpleLRUCache(capacity uint, guard float64, quota uint64) *SimpleLRUCache {
	trace_util_0.Count(_simple_lru_00000, 0)
	if capacity <= 0 {
		trace_util_0.Count(_simple_lru_00000, 2)
		panic("capacity of LRU Cache should be positive.")
	}
	trace_util_0.Count(_simple_lru_00000, 1)
	return &SimpleLRUCache{
		capacity: capacity,
		size:     0,
		quota:    quota,
		guard:    guard,
		elements: make(map[string]*list.Element),
		cache:    list.New(),
	}
}

// Get tries to find the corresponding value according to the given key.
func (l *SimpleLRUCache) Get(key Key) (value Value, ok bool) {
	trace_util_0.Count(_simple_lru_00000, 3)
	element, exists := l.elements[string(key.Hash())]
	if !exists {
		trace_util_0.Count(_simple_lru_00000, 5)
		return nil, false
	}
	trace_util_0.Count(_simple_lru_00000, 4)
	l.cache.MoveToFront(element)
	return element.Value.(*cacheEntry).value, true
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *SimpleLRUCache) Put(key Key, value Value) {
	trace_util_0.Count(_simple_lru_00000, 6)
	hash := string(key.Hash())
	element, exists := l.elements[hash]
	if exists {
		trace_util_0.Count(_simple_lru_00000, 9)
		l.cache.MoveToFront(element)
		return
	}

	trace_util_0.Count(_simple_lru_00000, 7)
	newCacheEntry := &cacheEntry{
		key:   key,
		value: value,
	}

	element = l.cache.PushFront(newCacheEntry)
	l.elements[hash] = element
	l.size++

	memUsed, err := memory.MemUsed()
	if err != nil {
		trace_util_0.Count(_simple_lru_00000, 10)
		l.DeleteAll()
		return
	}

	trace_util_0.Count(_simple_lru_00000, 8)
	for memUsed > uint64(float64(l.quota)*(1.0-l.guard)) || l.size > l.capacity {
		trace_util_0.Count(_simple_lru_00000, 11)
		lru := l.cache.Back()
		if lru == nil {
			trace_util_0.Count(_simple_lru_00000, 13)
			break
		}
		trace_util_0.Count(_simple_lru_00000, 12)
		l.cache.Remove(lru)
		delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
		l.size--
		if memUsed > uint64(float64(l.quota)*(1.0-l.guard)) {
			trace_util_0.Count(_simple_lru_00000, 14)
			memUsed, err = memory.MemUsed()
			if err != nil {
				trace_util_0.Count(_simple_lru_00000, 15)
				l.DeleteAll()
				return
			}
		}
	}
}

// Delete deletes the key-value pair from the LRU Cache.
func (l *SimpleLRUCache) Delete(key Key) {
	trace_util_0.Count(_simple_lru_00000, 16)
	k := string(key.Hash())
	element := l.elements[k]
	if element == nil {
		trace_util_0.Count(_simple_lru_00000, 18)
		return
	}
	trace_util_0.Count(_simple_lru_00000, 17)
	l.cache.Remove(element)
	delete(l.elements, k)
	l.size--
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *SimpleLRUCache) DeleteAll() {
	trace_util_0.Count(_simple_lru_00000, 19)
	for lru := l.cache.Back(); lru != nil; lru = l.cache.Back() {
		trace_util_0.Count(_simple_lru_00000, 20)
		l.cache.Remove(lru)
		delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
		l.size--
	}
}

// Size gets the current cache size.
func (l *SimpleLRUCache) Size() int {
	trace_util_0.Count(_simple_lru_00000, 21)
	return int(l.size)
}

var _simple_lru_00000 = "util/kvcache/simple_lru.go"
