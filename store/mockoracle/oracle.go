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

package mockoracle

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
)

var errStopped = errors.New("stopped")

// MockOracle is a mock oracle for test.
type MockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

// Enable enables the Oracle
func (o *MockOracle) Enable() {
	trace_util_0.Count(_oracle_00000, 0)
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

// Disable disables the Oracle
func (o *MockOracle) Disable() {
	trace_util_0.Count(_oracle_00000, 1)
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

func (o *MockOracle) setOffset(offset time.Duration) {
	trace_util_0.Count(_oracle_00000, 2)
	o.Lock()
	defer o.Unlock()

	o.offset = offset
}

// AddOffset adds the offset of the oracle.
func (o *MockOracle) AddOffset(d time.Duration) {
	trace_util_0.Count(_oracle_00000, 3)
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

// GetTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetTimestamp(context.Context) (uint64, error) {
	trace_util_0.Count(_oracle_00000, 4)
	o.Lock()
	defer o.Unlock()

	if o.stop {
		trace_util_0.Count(_oracle_00000, 7)
		return 0, errors.Trace(errStopped)
	}
	trace_util_0.Count(_oracle_00000, 5)
	physical := oracle.GetPhysical(time.Now().Add(o.offset))
	ts := oracle.ComposeTS(physical, 0)
	if oracle.ExtractPhysical(o.lastTS) == physical {
		trace_util_0.Count(_oracle_00000, 8)
		ts = o.lastTS + 1
	}
	trace_util_0.Count(_oracle_00000, 6)
	o.lastTS = ts
	return ts, nil
}

type mockOracleFuture struct {
	o   *MockOracle
	ctx context.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	trace_util_0.Count(_oracle_00000, 9)
	return m.o.GetTimestamp(m.ctx)
}

// GetTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_oracle_00000, 10)
	return &mockOracleFuture{o, ctx}
}

// GetLowResolutionTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	trace_util_0.Count(_oracle_00000, 11)
	return o.GetTimestamp(ctx)
}

// GetLowResolutionTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_oracle_00000, 12)
	return o.GetTimestampAsync(ctx)
}

// IsExpired implements oracle.Oracle interface.
func (o *MockOracle) IsExpired(lockTimestamp uint64, TTL uint64) bool {
	trace_util_0.Count(_oracle_00000, 13)
	o.RLock()
	defer o.RUnlock()

	return oracle.GetPhysical(time.Now().Add(o.offset)) >= oracle.ExtractPhysical(lockTimestamp)+int64(TTL)
}

// UntilExpired implement oracle.Oracle interface.
func (o *MockOracle) UntilExpired(lockTimeStamp uint64, TTL uint64) int64 {
	trace_util_0.Count(_oracle_00000, 14)
	o.RLock()
	defer o.RUnlock()
	return oracle.ExtractPhysical(lockTimeStamp) + int64(TTL) - oracle.GetPhysical(time.Now().Add(o.offset))
}

// Close implements oracle.Oracle interface.
func (o *MockOracle) Close() {
	trace_util_0.Count(_oracle_00000, 15)

}

var _oracle_00000 = "store/mockoracle/oracle.go"
