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

package tikv

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// NoJitter makes the backoff sequence strict exponential.
	NoJitter = 1 + iota
	// FullJitter applies random factors to strict exponential.
	FullJitter
	// EqualJitter is also randomized, but prevents very short sleeps.
	EqualJitter
	// DecorrJitter increases the maximum jitter based on the last random value.
	DecorrJitter
)

var (
	tikvBackoffCounterRPC          = metrics.TiKVBackoffCounter.WithLabelValues("tikvRPC")
	tikvBackoffCounterLock         = metrics.TiKVBackoffCounter.WithLabelValues("txnLock")
	tikvBackoffCounterLockFast     = metrics.TiKVBackoffCounter.WithLabelValues("tikvLockFast")
	tikvBackoffCounterPD           = metrics.TiKVBackoffCounter.WithLabelValues("pdRPC")
	tikvBackoffCounterRegionMiss   = metrics.TiKVBackoffCounter.WithLabelValues("regionMiss")
	tikvBackoffCounterUpdateLeader = metrics.TiKVBackoffCounter.WithLabelValues("updateLeader")
	tikvBackoffCounterServerBusy   = metrics.TiKVBackoffCounter.WithLabelValues("serverBusy")
	tikvBackoffCounterEmpty        = metrics.TiKVBackoffCounter.WithLabelValues("")
)

func (t backoffType) Counter() prometheus.Counter {
	trace_util_0.Count(_backoff_00000, 0)
	switch t {
	case boTiKVRPC:
		trace_util_0.Count(_backoff_00000, 2)
		return tikvBackoffCounterRPC
	case BoTxnLock:
		trace_util_0.Count(_backoff_00000, 3)
		return tikvBackoffCounterLock
	case boTxnLockFast:
		trace_util_0.Count(_backoff_00000, 4)
		return tikvBackoffCounterLockFast
	case BoPDRPC:
		trace_util_0.Count(_backoff_00000, 5)
		return tikvBackoffCounterPD
	case BoRegionMiss:
		trace_util_0.Count(_backoff_00000, 6)
		return tikvBackoffCounterRegionMiss
	case BoUpdateLeader:
		trace_util_0.Count(_backoff_00000, 7)
		return tikvBackoffCounterUpdateLeader
	case boServerBusy:
		trace_util_0.Count(_backoff_00000, 8)
		return tikvBackoffCounterServerBusy
	}
	trace_util_0.Count(_backoff_00000, 1)
	return tikvBackoffCounterEmpty
}

// NewBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func NewBackoffFn(base, cap, jitter int) func(ctx context.Context, maxSleepMs int) int {
	trace_util_0.Count(_backoff_00000, 9)
	if base < 2 {
		trace_util_0.Count(_backoff_00000, 11)
		// Top prevent panic in 'rand.Intn'.
		base = 2
	}
	trace_util_0.Count(_backoff_00000, 10)
	attempts := 0
	lastSleep := base
	return func(ctx context.Context, maxSleepMs int) int {
		trace_util_0.Count(_backoff_00000, 12)
		var sleep int
		switch jitter {
		case NoJitter:
			trace_util_0.Count(_backoff_00000, 15)
			sleep = expo(base, cap, attempts)
		case FullJitter:
			trace_util_0.Count(_backoff_00000, 16)
			v := expo(base, cap, attempts)
			sleep = rand.Intn(v)
		case EqualJitter:
			trace_util_0.Count(_backoff_00000, 17)
			v := expo(base, cap, attempts)
			sleep = v/2 + rand.Intn(v/2)
		case DecorrJitter:
			trace_util_0.Count(_backoff_00000, 18)
			sleep = int(math.Min(float64(cap), float64(base+rand.Intn(lastSleep*3-base))))
		}
		trace_util_0.Count(_backoff_00000, 13)
		logutil.Logger(context.Background()).Debug("backoff",
			zap.Int("base", base),
			zap.Int("sleep", sleep))

		realSleep := sleep
		// when set maxSleepMs >= 0 in `tikv.BackoffWithMaxSleep` will force sleep maxSleepMs milliseconds.
		if maxSleepMs >= 0 && realSleep > maxSleepMs {
			trace_util_0.Count(_backoff_00000, 19)
			realSleep = maxSleepMs
		}
		trace_util_0.Count(_backoff_00000, 14)
		select {
		case <-time.After(time.Duration(realSleep) * time.Millisecond):
			trace_util_0.Count(_backoff_00000, 20)
			attempts++
			lastSleep = sleep
			return realSleep
		case <-ctx.Done():
			trace_util_0.Count(_backoff_00000, 21)
			return 0
		}
	}
}

func expo(base, cap, n int) int {
	trace_util_0.Count(_backoff_00000, 22)
	return int(math.Min(float64(cap), float64(base)*math.Pow(2.0, float64(n))))
}

type backoffType int

// Back off types.
const (
	boTiKVRPC backoffType = iota
	BoTxnLock
	boTxnLockFast
	BoPDRPC
	BoRegionMiss
	BoUpdateLeader
	boServerBusy
)

func (t backoffType) createFn(vars *kv.Variables) func(context.Context, int) int {
	trace_util_0.Count(_backoff_00000, 23)
	if vars.Hook != nil {
		trace_util_0.Count(_backoff_00000, 26)
		vars.Hook(t.String(), vars)
	}
	trace_util_0.Count(_backoff_00000, 24)
	switch t {
	case boTiKVRPC:
		trace_util_0.Count(_backoff_00000, 27)
		return NewBackoffFn(100, 2000, EqualJitter)
	case BoTxnLock:
		trace_util_0.Count(_backoff_00000, 28)
		return NewBackoffFn(200, 3000, EqualJitter)
	case boTxnLockFast:
		trace_util_0.Count(_backoff_00000, 29)
		return NewBackoffFn(vars.BackoffLockFast, 3000, EqualJitter)
	case BoPDRPC:
		trace_util_0.Count(_backoff_00000, 30)
		return NewBackoffFn(500, 3000, EqualJitter)
	case BoRegionMiss:
		trace_util_0.Count(_backoff_00000, 31)
		// change base time to 2ms, because it may recover soon.
		return NewBackoffFn(2, 500, NoJitter)
	case BoUpdateLeader:
		trace_util_0.Count(_backoff_00000, 32)
		return NewBackoffFn(1, 10, NoJitter)
	case boServerBusy:
		trace_util_0.Count(_backoff_00000, 33)
		return NewBackoffFn(2000, 10000, EqualJitter)
	}
	trace_util_0.Count(_backoff_00000, 25)
	return nil
}

func (t backoffType) String() string {
	trace_util_0.Count(_backoff_00000, 34)
	switch t {
	case boTiKVRPC:
		trace_util_0.Count(_backoff_00000, 36)
		return "tikvRPC"
	case BoTxnLock:
		trace_util_0.Count(_backoff_00000, 37)
		return "txnLock"
	case boTxnLockFast:
		trace_util_0.Count(_backoff_00000, 38)
		return "txnLockFast"
	case BoPDRPC:
		trace_util_0.Count(_backoff_00000, 39)
		return "pdRPC"
	case BoRegionMiss:
		trace_util_0.Count(_backoff_00000, 40)
		return "regionMiss"
	case BoUpdateLeader:
		trace_util_0.Count(_backoff_00000, 41)
		return "updateLeader"
	case boServerBusy:
		trace_util_0.Count(_backoff_00000, 42)
		return "serverBusy"
	}
	trace_util_0.Count(_backoff_00000, 35)
	return ""
}

func (t backoffType) TError() error {
	trace_util_0.Count(_backoff_00000, 43)
	switch t {
	case boTiKVRPC:
		trace_util_0.Count(_backoff_00000, 45)
		return ErrTiKVServerTimeout
	case BoTxnLock, boTxnLockFast:
		trace_util_0.Count(_backoff_00000, 46)
		return ErrResolveLockTimeout
	case BoPDRPC:
		trace_util_0.Count(_backoff_00000, 47)
		return ErrPDServerTimeout
	case BoRegionMiss, BoUpdateLeader:
		trace_util_0.Count(_backoff_00000, 48)
		return ErrRegionUnavailable
	case boServerBusy:
		trace_util_0.Count(_backoff_00000, 49)
		return ErrTiKVServerBusy
	}
	trace_util_0.Count(_backoff_00000, 44)
	return terror.ClassTiKV.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
}

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	copBuildTaskMaxBackoff         = 5000
	tsoMaxBackoff                  = 15000
	scannerNextMaxBackoff          = 20000
	batchGetMaxBackoff             = 20000
	copNextMaxBackoff              = 20000
	getMaxBackoff                  = 20000
	prewriteMaxBackoff             = 20000
	cleanupMaxBackoff              = 20000
	GcOneRegionMaxBackoff          = 20000
	GcResolveLockMaxBackoff        = 100000
	deleteRangeOneRegionMaxBackoff = 100000
	rawkvMaxBackoff                = 20000
	splitRegionBackoff             = 20000
	scatterRegionBackoff           = 20000
	waitScatterRegionFinishBackoff = 120000
	locateRegionMaxBackoff         = 20000
	pessimisticLockMaxBackoff      = 10000
	pessimisticRollbackMaxBackoff  = 10000
)

// CommitMaxBackoff is max sleep time of the 'commit' command
var CommitMaxBackoff = 41000

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn         map[backoffType]func(context.Context, int) int
	maxSleep   int
	totalSleep int
	errors     []error
	types      []backoffType
	vars       *kv.Variables
}

// txnStartKey is a key for transaction start_ts info in context.Context.
const txnStartKey = "_txn_start_key"

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	trace_util_0.Count(_backoff_00000, 50)
	return &Backoffer{
		ctx:      ctx,
		maxSleep: maxSleep,
		vars:     kv.DefaultVars,
	}
}

// WithVars sets the kv.Variables to the Backoffer and return it.
func (b *Backoffer) WithVars(vars *kv.Variables) *Backoffer {
	trace_util_0.Count(_backoff_00000, 51)
	if vars != nil {
		trace_util_0.Count(_backoff_00000, 54)
		b.vars = vars
	}
	// maxSleep is the max sleep time in millisecond.
	// When it is multiplied by BackOffWeight, it should not be greater than MaxInt32.
	trace_util_0.Count(_backoff_00000, 52)
	if math.MaxInt32/b.vars.BackOffWeight >= b.maxSleep {
		trace_util_0.Count(_backoff_00000, 55)
		b.maxSleep *= b.vars.BackOffWeight
	}
	trace_util_0.Count(_backoff_00000, 53)
	return b
}

// Backoff sleeps a while base on the backoffType and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(typ backoffType, err error) error {
	trace_util_0.Count(_backoff_00000, 56)
	return b.BackoffWithMaxSleep(typ, -1, err)
}

// BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleep(typ backoffType, maxSleepMs int, err error) error {
	trace_util_0.Count(_backoff_00000, 57)
	if strings.Contains(err.Error(), mismatchClusterID) {
		trace_util_0.Count(_backoff_00000, 64)
		logutil.Logger(context.Background()).Fatal("critical error", zap.Error(err))
	}
	trace_util_0.Count(_backoff_00000, 58)
	select {
	case <-b.ctx.Done():
		trace_util_0.Count(_backoff_00000, 65)
		return errors.Trace(err)
	default:
		trace_util_0.Count(_backoff_00000, 66)
	}

	trace_util_0.Count(_backoff_00000, 59)
	typ.Counter().Inc()
	// Lazy initialize.
	if b.fn == nil {
		trace_util_0.Count(_backoff_00000, 67)
		b.fn = make(map[backoffType]func(context.Context, int) int)
	}
	trace_util_0.Count(_backoff_00000, 60)
	f, ok := b.fn[typ]
	if !ok {
		trace_util_0.Count(_backoff_00000, 68)
		f = typ.createFn(b.vars)
		b.fn[typ] = f
	}

	trace_util_0.Count(_backoff_00000, 61)
	b.totalSleep += f(b.ctx, maxSleepMs)
	b.types = append(b.types, typ)

	var startTs interface{}
	if ts := b.ctx.Value(txnStartKey); ts != nil {
		trace_util_0.Count(_backoff_00000, 69)
		startTs = ts
	}
	trace_util_0.Count(_backoff_00000, 62)
	logutil.Logger(context.Background()).Debug("retry later",
		zap.Error(err),
		zap.Int("totalSleep", b.totalSleep),
		zap.Int("maxSleep", b.maxSleep),
		zap.Stringer("type", typ),
		zap.Reflect("txnStartTS", startTs))

	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	if b.maxSleep > 0 && b.totalSleep >= b.maxSleep {
		trace_util_0.Count(_backoff_00000, 70)
		errMsg := fmt.Sprintf("%s backoffer.maxSleep %dms is exceeded, errors:", typ.String(), b.maxSleep)
		for i, err := range b.errors {
			trace_util_0.Count(_backoff_00000, 72)
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetLevel() == zapcore.DebugLevel || i >= len(b.errors)-3 {
				trace_util_0.Count(_backoff_00000, 73)
				errMsg += "\n" + err.Error()
			}
		}
		trace_util_0.Count(_backoff_00000, 71)
		logutil.Logger(context.Background()).Warn(errMsg)
		// Use the first backoff type to generate a MySQL error.
		return b.types[0].TError()
	}
	trace_util_0.Count(_backoff_00000, 63)
	return nil
}

func (b *Backoffer) String() string {
	trace_util_0.Count(_backoff_00000, 74)
	if b.totalSleep == 0 {
		trace_util_0.Count(_backoff_00000, 76)
		return ""
	}
	trace_util_0.Count(_backoff_00000, 75)
	return fmt.Sprintf(" backoff(%dms %v)", b.totalSleep, b.types)
}

// Clone creates a new Backoffer which keeps current Backoffer's sleep time and errors, and shares
// current Backoffer's context.
func (b *Backoffer) Clone() *Backoffer {
	trace_util_0.Count(_backoff_00000, 77)
	return &Backoffer{
		ctx:        b.ctx,
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		vars:       b.vars,
	}
}

// Fork creates a new Backoffer which keeps current Backoffer's sleep time and errors, and holds
// a child context of current Backoffer's context.
func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc) {
	trace_util_0.Count(_backoff_00000, 78)
	ctx, cancel := context.WithCancel(b.ctx)
	return &Backoffer{
		ctx:        ctx,
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		vars:       b.vars,
	}, cancel
}

var _backoff_00000 = "store/tikv/backoff.go"
