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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var _ oracle.Oracle = &pdOracle{}

const slowDist = 30 * time.Millisecond

// pdOracle is an Oracle that uses a placement driver client as source.
type pdOracle struct {
	c      pd.Client
	lastTS uint64
	quit   chan struct{}
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/pingcap/pd/blob/master/client/client.go for more details.
// PdOracle mantains `lastTS` to store the last timestamp got from PD server. If
// `GetTimestamp()` is not called after `updateInterval`, it will be called by
// itself to keep up with the timestamp on PD server.
func NewPdOracle(pdClient pd.Client, updateInterval time.Duration) (oracle.Oracle, error) {
	trace_util_0.Count(_pd_00000, 0)
	o := &pdOracle{
		c:    pdClient,
		quit: make(chan struct{}),
	}
	ctx := context.TODO()
	go o.updateTS(ctx, updateInterval)
	// Initialize lastTS by Get.
	_, err := o.GetTimestamp(ctx)
	if err != nil {
		trace_util_0.Count(_pd_00000, 2)
		o.Close()
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_pd_00000, 1)
	return o, nil
}

// IsExpired returns whether lockTS+TTL is expired, both are ms. It uses `lastTS`
// to compare, may return false negative result temporarily.
func (o *pdOracle) IsExpired(lockTS, TTL uint64) bool {
	trace_util_0.Count(_pd_00000, 3)
	lastTS := atomic.LoadUint64(&o.lastTS)
	return oracle.ExtractPhysical(lastTS) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

// GetTimestamp gets a new increasing time.
func (o *pdOracle) GetTimestamp(ctx context.Context) (uint64, error) {
	trace_util_0.Count(_pd_00000, 4)
	ts, err := o.getTimestamp(ctx)
	if err != nil {
		trace_util_0.Count(_pd_00000, 6)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_pd_00000, 5)
	o.setLastTS(ts)
	return ts, nil
}

type tsFuture struct {
	pd.TSFuture
	o *pdOracle
}

// Wait implements the oracle.Future interface.
func (f *tsFuture) Wait() (uint64, error) {
	trace_util_0.Count(_pd_00000, 7)
	now := time.Now()
	physical, logical, err := f.TSFuture.Wait()
	metrics.TSFutureWaitDuration.Observe(time.Since(now).Seconds())
	if err != nil {
		trace_util_0.Count(_pd_00000, 9)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_pd_00000, 8)
	ts := oracle.ComposeTS(physical, logical)
	f.o.setLastTS(ts)
	return ts, nil
}

func (o *pdOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_pd_00000, 10)
	ts := o.c.GetTSAsync(ctx)
	return &tsFuture{ts, o}
}

func (o *pdOracle) getTimestamp(ctx context.Context) (uint64, error) {
	trace_util_0.Count(_pd_00000, 11)
	now := time.Now()
	physical, logical, err := o.c.GetTS(ctx)
	if err != nil {
		trace_util_0.Count(_pd_00000, 14)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_pd_00000, 12)
	dist := time.Since(now)
	if dist > slowDist {
		trace_util_0.Count(_pd_00000, 15)
		logutil.Logger(ctx).Warn("get timestamp too slow",
			zap.Duration("cost time", dist))
	}
	trace_util_0.Count(_pd_00000, 13)
	return oracle.ComposeTS(physical, logical), nil
}

func (o *pdOracle) setLastTS(ts uint64) {
	trace_util_0.Count(_pd_00000, 16)
	lastTS := atomic.LoadUint64(&o.lastTS)
	if ts > lastTS {
		trace_util_0.Count(_pd_00000, 17)
		atomic.CompareAndSwapUint64(&o.lastTS, lastTS, ts)
	}
}

func (o *pdOracle) updateTS(ctx context.Context, interval time.Duration) {
	trace_util_0.Count(_pd_00000, 18)
	ticker := time.NewTicker(interval)
	for {
		trace_util_0.Count(_pd_00000, 19)
		select {
		case <-ticker.C:
			trace_util_0.Count(_pd_00000, 20)
			ts, err := o.getTimestamp(ctx)
			if err != nil {
				trace_util_0.Count(_pd_00000, 23)
				logutil.Logger(ctx).Error("updateTS error", zap.Error(err))
				break
			}
			trace_util_0.Count(_pd_00000, 21)
			o.setLastTS(ts)
		case <-o.quit:
			trace_util_0.Count(_pd_00000, 22)
			ticker.Stop()
			return
		}
	}
}

// UntilExpired implement oracle.Oracle interface.
func (o *pdOracle) UntilExpired(lockTS uint64, TTL uint64) int64 {
	trace_util_0.Count(_pd_00000, 24)
	lastTS := atomic.LoadUint64(&o.lastTS)
	return oracle.ExtractPhysical(lockTS) + int64(TTL) - oracle.ExtractPhysical(lastTS)
}

func (o *pdOracle) Close() {
	trace_util_0.Count(_pd_00000, 25)
	close(o.quit)
}

// A future that resolves immediately to a low resolution timestamp.
type lowResolutionTsFuture uint64

// Wait implements the oracle.Future interface.
func (f lowResolutionTsFuture) Wait() (uint64, error) {
	trace_util_0.Count(_pd_00000, 26)
	return uint64(f), nil
}

// GetLowResolutionTimestamp gets a new increasing time.
func (o *pdOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	trace_util_0.Count(_pd_00000, 27)
	lastTS := atomic.LoadUint64(&o.lastTS)
	return lastTS, nil
}

func (o *pdOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	trace_util_0.Count(_pd_00000, 28)
	lastTS := atomic.LoadUint64(&o.lastTS)
	return lowResolutionTsFuture(lastTS)
}

var _pd_00000 = "store/tikv/oracle/oracles/pd.go"
