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

package autoid

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Test needs to change it, so it's a variable.
var step = int64(30000)

var errInvalidTableID = terror.ClassAutoid.New(codeInvalidTableID, "invalid TableID")

// Allocator is an auto increment id generator.
// Just keep id unique actually.
type Allocator interface {
	// Alloc allocs the next autoID for table with tableID.
	// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
	Alloc(tableID int64) (int64, error)
	// Rebase rebases the autoID base for table with tableID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(tableID, newBase int64, allocIDs bool) error
	// Base return the current base of Allocator.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID(tableID int64) (int64, error)
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
	// dbID is current database's ID.
	dbID       int64
	isUnsigned bool
}

// GetStep is only used by tests
func GetStep() int64 {
	trace_util_0.Count(_autoid_00000, 0)
	return step
}

// SetStep is only used by tests
func SetStep(s int64) {
	trace_util_0.Count(_autoid_00000, 1)
	step = s
}

// Base implements autoid.Allocator Base interface.
func (alloc *allocator) Base() int64 {
	trace_util_0.Count(_autoid_00000, 2)
	return alloc.base
}

// End implements autoid.Allocator End interface.
func (alloc *allocator) End() int64 {
	trace_util_0.Count(_autoid_00000, 3)
	return alloc.end
}

// NextGlobalAutoID implements autoid.Allocator NextGlobalAutoID interface.
func (alloc *allocator) NextGlobalAutoID(tableID int64) (int64, error) {
	trace_util_0.Count(_autoid_00000, 4)
	var autoID int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_autoid_00000, 7)
		var err1 error
		m := meta.NewMeta(txn)
		autoID, err1 = m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			trace_util_0.Count(_autoid_00000, 9)
			return errors.Trace(err1)
		}
		trace_util_0.Count(_autoid_00000, 8)
		return nil
	})
	trace_util_0.Count(_autoid_00000, 5)
	metrics.AutoIDHistogram.WithLabelValues(metrics.GlobalAutoID, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if alloc.isUnsigned {
		trace_util_0.Count(_autoid_00000, 10)
		return int64(uint64(autoID) + 1), err
	}
	trace_util_0.Count(_autoid_00000, 6)
	return autoID + 1, err
}

func (alloc *allocator) rebase4Unsigned(tableID int64, requiredBase uint64, allocIDs bool) error {
	trace_util_0.Count(_autoid_00000, 11)
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= uint64(alloc.base) {
		trace_util_0.Count(_autoid_00000, 16)
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	trace_util_0.Count(_autoid_00000, 12)
	if requiredBase <= uint64(alloc.end) {
		trace_util_0.Count(_autoid_00000, 17)
		alloc.base = int64(requiredBase)
		return nil
	}
	trace_util_0.Count(_autoid_00000, 13)
	var newBase, newEnd uint64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_autoid_00000, 18)
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			trace_util_0.Count(_autoid_00000, 21)
			return err1
		}
		trace_util_0.Count(_autoid_00000, 19)
		uCurrentEnd := uint64(currentEnd)
		if allocIDs {
			trace_util_0.Count(_autoid_00000, 22)
			newBase = mathutil.MaxUint64(uCurrentEnd, requiredBase)
			newEnd = mathutil.MinUint64(math.MaxUint64-uint64(step), newBase) + uint64(step)
		} else {
			trace_util_0.Count(_autoid_00000, 23)
			{
				if uCurrentEnd >= requiredBase {
					trace_util_0.Count(_autoid_00000, 25)
					newBase = uCurrentEnd
					newEnd = uCurrentEnd
					// Required base satisfied, we don't need to update KV.
					return nil
				}
				// If we don't want to allocate IDs, for example when creating a table with a given base value,
				// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
				// will be allocated, so we need to increase the end to exactly the requiredBase.
				trace_util_0.Count(_autoid_00000, 24)
				newBase = requiredBase
				newEnd = requiredBase
			}
		}
		trace_util_0.Count(_autoid_00000, 20)
		_, err1 = m.GenAutoTableID(alloc.dbID, tableID, int64(newEnd-uCurrentEnd))
		return err1
	})
	trace_util_0.Count(_autoid_00000, 14)
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		trace_util_0.Count(_autoid_00000, 26)
		return err
	}
	trace_util_0.Count(_autoid_00000, 15)
	alloc.base, alloc.end = int64(newBase), int64(newEnd)
	return nil
}

func (alloc *allocator) rebase4Signed(tableID, requiredBase int64, allocIDs bool) error {
	trace_util_0.Count(_autoid_00000, 27)
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= alloc.base {
		trace_util_0.Count(_autoid_00000, 32)
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	trace_util_0.Count(_autoid_00000, 28)
	if requiredBase <= alloc.end {
		trace_util_0.Count(_autoid_00000, 33)
		alloc.base = requiredBase
		return nil
	}
	trace_util_0.Count(_autoid_00000, 29)
	var newBase, newEnd int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_autoid_00000, 34)
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			trace_util_0.Count(_autoid_00000, 37)
			return err1
		}
		trace_util_0.Count(_autoid_00000, 35)
		if allocIDs {
			trace_util_0.Count(_autoid_00000, 38)
			newBase = mathutil.MaxInt64(currentEnd, requiredBase)
			newEnd = mathutil.MinInt64(math.MaxInt64-step, newBase) + step
		} else {
			trace_util_0.Count(_autoid_00000, 39)
			{
				if currentEnd >= requiredBase {
					trace_util_0.Count(_autoid_00000, 41)
					newBase = currentEnd
					newEnd = currentEnd
					// Required base satisfied, we don't need to update KV.
					return nil
				}
				// If we don't want to allocate IDs, for example when creating a table with a given base value,
				// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
				// will be allocated, so we need to increase the end to exactly the requiredBase.
				trace_util_0.Count(_autoid_00000, 40)
				newBase = requiredBase
				newEnd = requiredBase
			}
		}
		trace_util_0.Count(_autoid_00000, 36)
		_, err1 = m.GenAutoTableID(alloc.dbID, tableID, newEnd-currentEnd)
		return err1
	})
	trace_util_0.Count(_autoid_00000, 30)
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		trace_util_0.Count(_autoid_00000, 42)
		return err
	}
	trace_util_0.Count(_autoid_00000, 31)
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// Rebase implements autoid.Allocator Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *allocator) Rebase(tableID, requiredBase int64, allocIDs bool) error {
	trace_util_0.Count(_autoid_00000, 43)
	if tableID == 0 {
		trace_util_0.Count(_autoid_00000, 46)
		return errInvalidTableID.GenWithStack("Invalid tableID")
	}

	trace_util_0.Count(_autoid_00000, 44)
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.isUnsigned {
		trace_util_0.Count(_autoid_00000, 47)
		return alloc.rebase4Unsigned(tableID, uint64(requiredBase), allocIDs)
	}
	trace_util_0.Count(_autoid_00000, 45)
	return alloc.rebase4Signed(tableID, requiredBase, allocIDs)
}

func (alloc *allocator) alloc4Unsigned(tableID int64) (int64, error) {
	trace_util_0.Count(_autoid_00000, 48)
	if alloc.base == alloc.end {
		trace_util_0.Count(_autoid_00000, 50) // step
		var newBase, newEnd int64
		startTime := time.Now()
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			trace_util_0.Count(_autoid_00000, 54)
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.dbID, tableID)
			if err1 != nil {
				trace_util_0.Count(_autoid_00000, 56)
				return err1
			}
			trace_util_0.Count(_autoid_00000, 55)
			tmpStep := int64(mathutil.MinUint64(math.MaxUint64-uint64(newBase), uint64(step)))
			newEnd, err1 = m.GenAutoTableID(alloc.dbID, tableID, tmpStep)
			return err1
		})
		trace_util_0.Count(_autoid_00000, 51)
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			trace_util_0.Count(_autoid_00000, 57)
			return 0, err
		}
		trace_util_0.Count(_autoid_00000, 52)
		if uint64(newBase) == math.MaxUint64 {
			trace_util_0.Count(_autoid_00000, 58)
			return 0, ErrAutoincReadFailed
		}
		trace_util_0.Count(_autoid_00000, 53)
		alloc.base, alloc.end = newBase, newEnd
	}

	trace_util_0.Count(_autoid_00000, 49)
	alloc.base = int64(uint64(alloc.base) + 1)
	logutil.Logger(context.Background()).Debug("alloc unsigned ID",
		zap.Uint64("ID", uint64(alloc.base)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	return alloc.base, nil
}

func (alloc *allocator) alloc4Signed(tableID int64) (int64, error) {
	trace_util_0.Count(_autoid_00000, 59)
	if alloc.base == alloc.end {
		trace_util_0.Count(_autoid_00000, 61) // step
		var newBase, newEnd int64
		startTime := time.Now()
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			trace_util_0.Count(_autoid_00000, 65)
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.dbID, tableID)
			if err1 != nil {
				trace_util_0.Count(_autoid_00000, 67)
				return err1
			}
			trace_util_0.Count(_autoid_00000, 66)
			tmpStep := mathutil.MinInt64(math.MaxInt64-newBase, step)
			newEnd, err1 = m.GenAutoTableID(alloc.dbID, tableID, tmpStep)
			return err1
		})
		trace_util_0.Count(_autoid_00000, 62)
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			trace_util_0.Count(_autoid_00000, 68)
			return 0, err
		}
		trace_util_0.Count(_autoid_00000, 63)
		if newBase == math.MaxInt64 {
			trace_util_0.Count(_autoid_00000, 69)
			return 0, ErrAutoincReadFailed
		}
		trace_util_0.Count(_autoid_00000, 64)
		alloc.base, alloc.end = newBase, newEnd
	}

	trace_util_0.Count(_autoid_00000, 60)
	alloc.base++
	logutil.Logger(context.Background()).Debug("alloc signed ID",
		zap.Uint64("ID", uint64(alloc.base)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	return alloc.base, nil
}

// Alloc implements autoid.Allocator Alloc interface.
func (alloc *allocator) Alloc(tableID int64) (int64, error) {
	trace_util_0.Count(_autoid_00000, 70)
	if tableID == 0 {
		trace_util_0.Count(_autoid_00000, 73)
		return 0, errInvalidTableID.GenWithStack("Invalid tableID")
	}
	trace_util_0.Count(_autoid_00000, 71)
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.isUnsigned {
		trace_util_0.Count(_autoid_00000, 74)
		return alloc.alloc4Unsigned(tableID)
	}
	trace_util_0.Count(_autoid_00000, 72)
	return alloc.alloc4Signed(tableID)
}

// NewAllocator returns a new auto increment id generator on the store.
func NewAllocator(store kv.Storage, dbID int64, isUnsigned bool) Allocator {
	trace_util_0.Count(_autoid_00000, 75)
	return &allocator{
		store:      store,
		dbID:       dbID,
		isUnsigned: isUnsigned,
	}
}

//autoid error codes.
const codeInvalidTableID terror.ErrCode = 1

var localSchemaID = int64(math.MaxInt64)

// GenLocalSchemaID generates a local schema ID.
func GenLocalSchemaID() int64 {
	trace_util_0.Count(_autoid_00000, 76)
	return atomic.AddInt64(&localSchemaID, -1)
}

var _autoid_00000 = "meta/autoid/autoid.go"
