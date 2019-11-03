// Copyright 2016 PingCAP, Inc.
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

package oracles

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
)

var _ oracle.Oracle = &localOracle{}

type localOracle struct {
	sync.Mutex
	lastTimeStampTS uint64
	n               uint64
	hook            *struct {
		currentTime time.Time
	}
}

// NewLocalOracle creates an Oracle that uses local time as data source.
func NewLocalOracle() oracle.Oracle {
	trace_util_0.Count(_local_00000, 0)
	return &localOracle{}
}

func (l *localOracle) IsExpired(lockTS uint64, TTL uint64) bool {
	trace_util_0.Count(_local_00000, 1)
	now := time.Now()
	if l.hook != nil {
		trace_util_0.Count(_local_00000, 3)
		now = l.hook.currentTime
	}
	trace_util_0.Count(_local_00000, 2)
	return oracle.GetPhysical(now) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

func (l *localOracle) GetTimestamp(context.Context) (uint64, error) {
	trace_util_0.Count(_local_00000, 4)
	l.Lock()
	defer l.Unlock()
	now := time.Now()
	if l.hook != nil {
		trace_util_0.Count(_local_00000, 7)
		now = l.hook.currentTime
	}
	trace_util_0.Count(_local_00000, 5)
	physical := oracle.GetPhysical(now)
	ts := oracle.ComposeTS(physical, 0)
	if l.lastTimeStampTS == ts {
		trace_util_0.Count(_local_00000, 8)
		l.n++
		return ts + l.n, nil
	}
	trace_util_0.Count(_local_00000, 6)
	l.lastTimeStampTS = ts
	l.n = 0
	return ts, nil
}

func (l *localOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_local_00000, 9)
	return &future{
		ctx: ctx,
		l:   l,
	}
}

func (l *localOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	trace_util_0.Count(_local_00000, 10)
	return l.GetTimestamp(ctx)
}

func (l *localOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_local_00000, 11)
	return l.GetTimestampAsync(ctx)
}

type future struct {
	ctx context.Context
	l   *localOracle
}

func (f *future) Wait() (uint64, error) {
	trace_util_0.Count(_local_00000, 12)
	return f.l.GetTimestamp(f.ctx)
}

// UntilExpired implement oracle.Oracle interface.
func (l *localOracle) UntilExpired(lockTimeStamp uint64, TTL uint64) int64 {
	trace_util_0.Count(_local_00000, 13)
	now := time.Now()
	if l.hook != nil {
		trace_util_0.Count(_local_00000, 15)
		now = l.hook.currentTime
	}
	trace_util_0.Count(_local_00000, 14)
	return oracle.ExtractPhysical(lockTimeStamp) + int64(TTL) - oracle.GetPhysical(now)
}

func (l *localOracle) Close() {
	trace_util_0.Count(_local_00000, 16)
}

var _local_00000 = "store/tikv/oracle/oracles/local.go"
