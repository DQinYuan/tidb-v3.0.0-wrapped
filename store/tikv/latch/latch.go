// Copyright 2018 PingCAP, Inc.
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

package latch

import (
	"bytes"
	"context"
	"math/bits"
	"sort"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type node struct {
	slotID      int
	key         []byte
	maxCommitTS uint64
	value       *Lock

	next *node
}

// latch stores a key's waiting transactions information.
type latch struct {
	queue   *node
	count   int
	waiting []*Lock
	sync.Mutex
}

// Lock is the locks' information required for a transaction.
type Lock struct {
	keys [][]byte
	// requiredSlots represents required slots.
	// The slot IDs of the latches(keys) that a startTS must acquire before being able to processed.
	requiredSlots []int
	// acquiredCount represents the number of latches that the transaction has acquired.
	// For status is stale, it include the latch whose front is current lock already.
	acquiredCount int
	// startTS represents current transaction's.
	startTS uint64
	// commitTS represents current transaction's.
	commitTS uint64

	wg      sync.WaitGroup
	isStale bool
}

// acquireResult is the result type for acquire()
type acquireResult int32

const (
	// acquireSuccess is a type constant for acquireResult.
	// which means acquired success
	acquireSuccess acquireResult = iota
	// acquireLocked is a type constant for acquireResult
	// which means still locked by other Lock.
	acquireLocked
	// acquireStale is a type constant for acquireResult
	// which means current Lock's startTS is stale.
	acquireStale
)

// IsStale returns whether the status is stale.
func (l *Lock) IsStale() bool {
	trace_util_0.Count(_latch_00000, 0)
	return l.isStale
}

func (l *Lock) isLocked() bool {
	trace_util_0.Count(_latch_00000, 1)
	return !l.isStale && l.acquiredCount != len(l.requiredSlots)
}

// SetCommitTS sets the lock's commitTS.
func (l *Lock) SetCommitTS(commitTS uint64) {
	trace_util_0.Count(_latch_00000, 2)
	l.commitTS = commitTS
}

// Latches which are used for concurrency control.
// Each latch is indexed by a slot's ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches struct {
	slots []latch
}

type bytesSlice [][]byte

func (s bytesSlice) Len() int {
	trace_util_0.Count(_latch_00000, 3)
	return len(s)
}

func (s bytesSlice) Swap(i, j int) {
	trace_util_0.Count(_latch_00000, 4)
	s[i], s[j] = s[j], s[i]
}

func (s bytesSlice) Less(i, j int) bool {
	trace_util_0.Count(_latch_00000, 5)
	return bytes.Compare(s[i], s[j]) < 0
}

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size uint) *Latches {
	trace_util_0.Count(_latch_00000, 6)
	powerOfTwoSize := 1 << uint32(bits.Len32(uint32(size-1)))
	slots := make([]latch, powerOfTwoSize)
	return &Latches{
		slots: slots,
	}
}

// genLock generates Lock for the transaction with startTS and keys.
func (latches *Latches) genLock(startTS uint64, keys [][]byte) *Lock {
	trace_util_0.Count(_latch_00000, 7)
	sort.Sort(bytesSlice(keys))
	return &Lock{
		keys:          keys,
		requiredSlots: latches.genSlotIDs(keys),
		acquiredCount: 0,
		startTS:       startTS,
	}
}

func (latches *Latches) genSlotIDs(keys [][]byte) []int {
	trace_util_0.Count(_latch_00000, 8)
	slots := make([]int, 0, len(keys))
	for _, key := range keys {
		trace_util_0.Count(_latch_00000, 10)
		slots = append(slots, latches.slotID(key))
	}
	trace_util_0.Count(_latch_00000, 9)
	return slots
}

// slotID return slotID for current key.
func (latches *Latches) slotID(key []byte) int {
	trace_util_0.Count(_latch_00000, 11)
	return int(murmur3.Sum32(key)) & (len(latches.slots) - 1)
}

// acquire tries to acquire the lock for a transaction.
func (latches *Latches) acquire(lock *Lock) acquireResult {
	trace_util_0.Count(_latch_00000, 12)
	if lock.IsStale() {
		trace_util_0.Count(_latch_00000, 15)
		return acquireStale
	}
	trace_util_0.Count(_latch_00000, 13)
	for lock.acquiredCount < len(lock.requiredSlots) {
		trace_util_0.Count(_latch_00000, 16)
		status := latches.acquireSlot(lock)
		if status != acquireSuccess {
			trace_util_0.Count(_latch_00000, 17)
			return status
		}
	}
	trace_util_0.Count(_latch_00000, 14)
	return acquireSuccess
}

// release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction's status is not locked.
func (latches *Latches) release(lock *Lock, wakeupList []*Lock) []*Lock {
	trace_util_0.Count(_latch_00000, 18)
	wakeupList = wakeupList[:0]
	for lock.acquiredCount > 0 {
		trace_util_0.Count(_latch_00000, 20)
		if nextLock := latches.releaseSlot(lock); nextLock != nil {
			trace_util_0.Count(_latch_00000, 21)
			wakeupList = append(wakeupList, nextLock)
		}
	}
	trace_util_0.Count(_latch_00000, 19)
	return wakeupList
}

func (latches *Latches) releaseSlot(lock *Lock) (nextLock *Lock) {
	trace_util_0.Count(_latch_00000, 22)
	key := lock.keys[lock.acquiredCount-1]
	slotID := lock.requiredSlots[lock.acquiredCount-1]
	latch := &latches.slots[slotID]
	lock.acquiredCount--
	latch.Lock()
	defer latch.Unlock()

	find := findNode(latch.queue, key)
	if find.value != lock {
		trace_util_0.Count(_latch_00000, 27)
		panic("releaseSlot wrong")
	}
	trace_util_0.Count(_latch_00000, 23)
	find.maxCommitTS = mathutil.MaxUint64(find.maxCommitTS, lock.commitTS)
	find.value = nil
	// Make a copy of the key, so latch does not reference the transaction's memory.
	// If we do not do it, transaction memory can't be recycle by GC and there will
	// be a leak.
	copyKey := make([]byte, len(find.key))
	copy(copyKey, find.key)
	find.key = copyKey
	if len(latch.waiting) == 0 {
		trace_util_0.Count(_latch_00000, 28)
		return nil
	}

	trace_util_0.Count(_latch_00000, 24)
	var idx int
	for idx = 0; idx < len(latch.waiting); idx++ {
		trace_util_0.Count(_latch_00000, 29)
		waiting := latch.waiting[idx]
		if bytes.Equal(waiting.keys[waiting.acquiredCount], key) {
			trace_util_0.Count(_latch_00000, 30)
			break
		}
	}
	// Wake up the first one in waiting queue.
	trace_util_0.Count(_latch_00000, 25)
	if idx < len(latch.waiting) {
		trace_util_0.Count(_latch_00000, 31)
		nextLock = latch.waiting[idx]
		// Delete element latch.waiting[idx] from the array.
		copy(latch.waiting[idx:], latch.waiting[idx+1:])
		latch.waiting[len(latch.waiting)-1] = nil
		latch.waiting = latch.waiting[:len(latch.waiting)-1]

		if find.maxCommitTS > nextLock.startTS {
			trace_util_0.Count(_latch_00000, 32)
			find.value = nextLock
			nextLock.acquiredCount++
			nextLock.isStale = true
		}
	}

	trace_util_0.Count(_latch_00000, 26)
	return
}

func (latches *Latches) acquireSlot(lock *Lock) acquireResult {
	trace_util_0.Count(_latch_00000, 33)
	key := lock.keys[lock.acquiredCount]
	slotID := lock.requiredSlots[lock.acquiredCount]
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()

	// Try to recycle to limit the memory usage.
	if latch.count >= latchListCount {
		trace_util_0.Count(_latch_00000, 38)
		latch.recycle(lock.startTS)
	}

	trace_util_0.Count(_latch_00000, 34)
	find := findNode(latch.queue, key)
	if find == nil {
		trace_util_0.Count(_latch_00000, 39)
		tmp := &node{
			slotID: slotID,
			key:    key,
			value:  lock,
		}
		tmp.next = latch.queue
		latch.queue = tmp
		latch.count++

		lock.acquiredCount++
		return acquireSuccess
	}

	trace_util_0.Count(_latch_00000, 35)
	if find.maxCommitTS > lock.startTS {
		trace_util_0.Count(_latch_00000, 40)
		lock.isStale = true
		return acquireStale
	}

	trace_util_0.Count(_latch_00000, 36)
	if find.value == nil {
		trace_util_0.Count(_latch_00000, 41)
		find.value = lock
		lock.acquiredCount++
		return acquireSuccess
	}

	// Push the current transaction into waitingQueue.
	trace_util_0.Count(_latch_00000, 37)
	latch.waiting = append(latch.waiting, lock)
	return acquireLocked
}

// recycle is not thread safe, the latch should acquire its lock before executing this function.
func (l *latch) recycle(currentTS uint64) int {
	trace_util_0.Count(_latch_00000, 42)
	total := 0
	fakeHead := node{next: l.queue}
	prev := &fakeHead
	for curr := prev.next; curr != nil; curr = curr.next {
		trace_util_0.Count(_latch_00000, 44)
		if tsoSub(currentTS, curr.maxCommitTS) >= expireDuration && curr.value == nil {
			trace_util_0.Count(_latch_00000, 45)
			l.count--
			prev.next = curr.next
			total++
		} else {
			trace_util_0.Count(_latch_00000, 46)
			{
				prev = curr
			}
		}
	}
	trace_util_0.Count(_latch_00000, 43)
	l.queue = fakeHead.next
	return total
}

func (latches *Latches) recycle(currentTS uint64) {
	trace_util_0.Count(_latch_00000, 47)
	total := 0
	for i := 0; i < len(latches.slots); i++ {
		trace_util_0.Count(_latch_00000, 49)
		latch := &latches.slots[i]
		latch.Lock()
		total += latch.recycle(currentTS)
		latch.Unlock()
	}
	trace_util_0.Count(_latch_00000, 48)
	logutil.Logger(context.Background()).Debug("recycle",
		zap.Time("start at", time.Now()),
		zap.Int("count", total))
}

func findNode(list *node, key []byte) *node {
	trace_util_0.Count(_latch_00000, 50)
	for n := list; n != nil; n = n.next {
		trace_util_0.Count(_latch_00000, 52)
		if bytes.Equal(n.key, key) {
			trace_util_0.Count(_latch_00000, 53)
			return n
		}
	}
	trace_util_0.Count(_latch_00000, 51)
	return nil
}

var _latch_00000 = "store/tikv/latch/latch.go"
