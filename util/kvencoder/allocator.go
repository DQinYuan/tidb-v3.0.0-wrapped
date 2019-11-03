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

package kvenc

import (
	"sync/atomic"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/trace_util_0"
)

var _ autoid.Allocator = &Allocator{}

var (
	step = int64(5000)
)

// NewAllocator creates an Allocator.
func NewAllocator() *Allocator {
	trace_util_0.Count(_allocator_00000, 0)
	return &Allocator{}
}

// Allocator is an id allocator, it is only used in lightning.
type Allocator struct {
	base int64
}

// Alloc allocs a next autoID for table with tableID.
func (alloc *Allocator) Alloc(tableID int64) (int64, error) {
	trace_util_0.Count(_allocator_00000, 1)
	return atomic.AddInt64(&alloc.base, 1), nil
}

// Reset allow newBase smaller than alloc.base, and will set the alloc.base to newBase.
func (alloc *Allocator) Reset(newBase int64) {
	trace_util_0.Count(_allocator_00000, 2)
	atomic.StoreInt64(&alloc.base, newBase)
}

// Rebase not allow newBase smaller than alloc.base, and will skip the smaller newBase.
func (alloc *Allocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	trace_util_0.Count(_allocator_00000, 3)
	// CAS
	for {
		trace_util_0.Count(_allocator_00000, 5)
		oldBase := atomic.LoadInt64(&alloc.base)
		if newBase <= oldBase {
			trace_util_0.Count(_allocator_00000, 7)
			break
		}
		trace_util_0.Count(_allocator_00000, 6)
		if atomic.CompareAndSwapInt64(&alloc.base, oldBase, newBase) {
			trace_util_0.Count(_allocator_00000, 8)
			break
		}
	}

	trace_util_0.Count(_allocator_00000, 4)
	return nil
}

// Base returns the current base of Allocator.
func (alloc *Allocator) Base() int64 {
	trace_util_0.Count(_allocator_00000, 9)
	return atomic.LoadInt64(&alloc.base)
}

// End is only used for test.
func (alloc *Allocator) End() int64 {
	trace_util_0.Count(_allocator_00000, 10)
	return alloc.Base() + step
}

// NextGlobalAutoID returns the next global autoID.
func (alloc *Allocator) NextGlobalAutoID(tableID int64) (int64, error) {
	trace_util_0.Count(_allocator_00000, 11)
	return alloc.End() + 1, nil
}

var _allocator_00000 = "util/kvencoder/allocator.go"
