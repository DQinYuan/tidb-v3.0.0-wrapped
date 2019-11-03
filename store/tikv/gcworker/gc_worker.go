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

package gcworker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid        string
	desc        string
	store       tikv.Storage
	pdClient    pd.Client
	gcIsRunning bool
	lastFinish  time.Time
	cancel      context.CancelFunc
	done        chan error

	session session.Session
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store tikv.Storage, pdClient pd.Client) (tikv.GCHandler, error) {
	trace_util_0.Count(_gc_worker_00000, 0)
	ver, err := store.CurrentVersion()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 3)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 1)
	hostName, err := os.Hostname()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 4)
		hostName = "unknown"
	}
	trace_util_0.Count(_gc_worker_00000, 2)
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store,
		pdClient:    pdClient,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	trace_util_0.Count(_gc_worker_00000, 5)
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go w.start(ctx, &wg)
	wg.Wait() // Wait create session finish in worker, some test code depend on this to avoid race.
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	trace_util_0.Count(_gc_worker_00000, 6)
	w.cancel()
}

const (
	booleanTrue  = "true"
	booleanFalse = "false"

	gcWorkerTickInterval = time.Minute
	gcJobLogTickInterval = time.Minute * 10
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderDescKey      = "tikv_gc_leader_desc"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"

	gcLastRunTimeKey       = "tikv_gc_last_run_time"
	gcRunIntervalKey       = "tikv_gc_run_interval"
	gcDefaultRunInterval   = time.Minute * 10
	gcWaitTime             = time.Minute * 1
	gcRedoDeleteRangeDelay = 24 * time.Hour

	gcLifeTimeKey        = "tikv_gc_life_time"
	gcDefaultLifeTime    = time.Minute * 10
	gcSafePointKey       = "tikv_gc_safe_point"
	gcConcurrencyKey     = "tikv_gc_concurrency"
	gcDefaultConcurrency = 2
	gcMinConcurrency     = 1
	gcMaxConcurrency     = 128
	// We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
	gcScanLockLimit = tikv.ResolvedCacheSize / 2

	gcEnableKey          = "tikv_gc_enable"
	gcDefaultEnableValue = true

	gcModeKey         = "tikv_gc_mode"
	gcModeCentral     = "central"
	gcModeDistributed = "distributed"
	gcModeDefault     = gcModeDistributed

	gcAutoConcurrencyKey     = "tikv_gc_auto_concurrency"
	gcDefaultAutoConcurrency = true
)

var gcSafePointCacheInterval = tikv.GcSafePointCacheInterval

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:      "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:      "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey:     "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey:     "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey:     "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:        "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:       "All versions after safe point can be accessed. (DO NOT EDIT)",
	gcConcurrencyKey:     "How many goroutines used to do GC parallel, [1, 128], default 2",
	gcEnableKey:          "Current GC enable status",
	gcModeKey:            "Mode of GC, \"central\" or \"distributed\"",
	gcAutoConcurrencyKey: "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used",
}

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	trace_util_0.Count(_gc_worker_00000, 7)
	logutil.Logger(ctx).Info("[gc worker] start",
		zap.String("uuid", w.uuid))

	w.session = createSession(w.store)

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		trace_util_0.Count(_gc_worker_00000, 9)
		r := recover()
		if r != nil {
			trace_util_0.Count(_gc_worker_00000, 10)
			logutil.Logger(ctx).Error("gcWorker",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelGCWorker).Inc()
		}
	}()
	trace_util_0.Count(_gc_worker_00000, 8)
	for {
		trace_util_0.Count(_gc_worker_00000, 11)
		select {
		case <-ticker.C:
			trace_util_0.Count(_gc_worker_00000, 12)
			w.tick(ctx)
		case err := <-w.done:
			trace_util_0.Count(_gc_worker_00000, 13)
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				trace_util_0.Count(_gc_worker_00000, 15)
				logutil.Logger(ctx).Error("[gc worker] runGCJob", zap.Error(err))
				break
			}
		case <-ctx.Done():
			trace_util_0.Count(_gc_worker_00000, 14)
			logutil.Logger(ctx).Info("[gc worker] quit", zap.String("uuid", w.uuid))
			return
		}
	}
}

func createSession(store kv.Storage) session.Session {
	trace_util_0.Count(_gc_worker_00000, 16)
	for {
		trace_util_0.Count(_gc_worker_00000, 17)
		se, err := session.CreateSession(store)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 19)
			logutil.Logger(context.Background()).Warn("[gc worker] create session", zap.Error(err))
			continue
		}
		// Disable privilege check for gc worker session.
		trace_util_0.Count(_gc_worker_00000, 18)
		privilege.BindPrivilegeManager(se, nil)
		se.GetSessionVars().InRestrictedSQL = true
		return se
	}
}

func (w *GCWorker) tick(ctx context.Context) {
	trace_util_0.Count(_gc_worker_00000, 20)
	isLeader, err := w.checkLeader()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 22)
		logutil.Logger(ctx).Warn("[gc worker] check leader", zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_leader").Inc()
		return
	}
	trace_util_0.Count(_gc_worker_00000, 21)
	if isLeader {
		trace_util_0.Count(_gc_worker_00000, 23)
		err = w.leaderTick(ctx)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 24)
			logutil.Logger(ctx).Warn("[gc worker] leader tick", zap.Error(err))
		}
	} else {
		trace_util_0.Count(_gc_worker_00000, 25)
		{
			// Config metrics should always be updated by leader, set them to 0 when current instance is not leader.
			metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
			metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
		}
	}
}

const notBootstrappedVer = 0

func (w *GCWorker) storeIsBootstrapped() bool {
	trace_util_0.Count(_gc_worker_00000, 26)
	var ver int64
	err := kv.RunInNewTxn(w.store, false, func(txn kv.Transaction) error {
		trace_util_0.Count(_gc_worker_00000, 29)
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return errors.Trace(err)
	})
	trace_util_0.Count(_gc_worker_00000, 27)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 30)
		logutil.Logger(context.Background()).Error("[gc worker] check bootstrapped", zap.Error(err))
		return false
	}
	trace_util_0.Count(_gc_worker_00000, 28)
	return ver > notBootstrappedVer
}

// leaderTick of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx context.Context) error {
	trace_util_0.Count(_gc_worker_00000, 31)
	if w.gcIsRunning {
		trace_util_0.Count(_gc_worker_00000, 36)
		logutil.Logger(ctx).Info("[gc worker] there's already a gc job running, skipped",
			zap.String("leaderTick on", w.uuid))
		return nil
	}

	trace_util_0.Count(_gc_worker_00000, 32)
	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		trace_util_0.Count(_gc_worker_00000, 37)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 39)
			metrics.GCJobFailureCounter.WithLabelValues("prepare").Inc()
		}
		trace_util_0.Count(_gc_worker_00000, 38)
		w.gcIsRunning = false
		return errors.Trace(err)
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	trace_util_0.Count(_gc_worker_00000, 33)
	if time.Since(w.lastFinish) < gcWaitTime {
		trace_util_0.Count(_gc_worker_00000, 40)
		w.gcIsRunning = false
		logutil.Logger(ctx).Info("[gc worker] another gc job has just finished, skipped.",
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	trace_util_0.Count(_gc_worker_00000, 34)
	concurrency, err := w.getGCConcurrency(ctx)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 41)
		logutil.Logger(ctx).Info("[gc worker] failed to get gc concurrency.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 35)
	w.gcIsRunning = true
	logutil.Logger(ctx).Info("[gc worker] starts the whole job",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	go w.runGCJob(ctx, safePoint, concurrency)
	return nil
}

// prepare checks preconditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare() (bool, uint64, error) {
	trace_util_0.Count(_gc_worker_00000, 42)
	// Add a transaction here is to prevent following situations:
	// 1. GC check gcEnable is true, continue to do GC
	// 2. The user sets gcEnable to false
	// 3. The user gets `tikv_gc_safe_point` value is t1, then the user thinks the data after time t1 won't be clean by GC.
	// 4. GC update `tikv_gc_safe_point` value to t2, continue do GC in this round.
	// Then the data record that has been dropped between time t1 and t2, will be cleaned by GC, but the user thinks the data after t1 won't be clean by GC.
	ctx := context.Background()
	_, err := w.session.Execute(ctx, "BEGIN")
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 45)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 43)
	doGC, safePoint, err := w.checkPrepare(ctx)
	if doGC {
		trace_util_0.Count(_gc_worker_00000, 46)
		_, err = w.session.Execute(ctx, "COMMIT")
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 47)
			return false, 0, errors.Trace(err)
		}
	} else {
		trace_util_0.Count(_gc_worker_00000, 48)
		{
			_, err1 := w.session.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
		}
	}
	trace_util_0.Count(_gc_worker_00000, 44)
	return doGC, safePoint, errors.Trace(err)
}

func (w *GCWorker) checkPrepare(ctx context.Context) (bool, uint64, error) {
	trace_util_0.Count(_gc_worker_00000, 49)
	enable, err := w.checkGCEnable()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 57)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 50)
	if !enable {
		trace_util_0.Count(_gc_worker_00000, 58)
		logutil.Logger(ctx).Warn("[gc worker] gc status is disabled.")
		return false, 0, nil
	}
	trace_util_0.Count(_gc_worker_00000, 51)
	now, err := w.getOracleTime()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 59)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 52)
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		trace_util_0.Count(_gc_worker_00000, 60)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 53)
	newSafePoint, err := w.calculateNewSafePoint(now)
	if err != nil || newSafePoint == nil {
		trace_util_0.Count(_gc_worker_00000, 61)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 54)
	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 62)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 55)
	err = w.saveTime(gcSafePointKey, *newSafePoint)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 63)
		return false, 0, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 56)
	return true, oracle.ComposeTS(oracle.GetPhysical(*newSafePoint), 0), nil
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	trace_util_0.Count(_gc_worker_00000, 64)
	currentVer, err := w.store.CurrentVersion()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 66)
		return time.Time{}, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 65)
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}

func (w *GCWorker) checkGCEnable() (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 67)
	return w.loadBooleanWithDefault(gcEnableKey, gcDefaultEnableValue)
}

func (w *GCWorker) checkUseAutoConcurrency() (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 68)
	return w.loadBooleanWithDefault(gcAutoConcurrencyKey, gcDefaultAutoConcurrency)
}

func (w *GCWorker) loadBooleanWithDefault(key string, defaultValue bool) (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 69)
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 72)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 70)
	if str == "" {
		trace_util_0.Count(_gc_worker_00000, 73)
		// Save default value for gc enable key. The default value is always true.
		defaultValueStr := booleanFalse
		if defaultValue {
			trace_util_0.Count(_gc_worker_00000, 76)
			defaultValueStr = booleanTrue
		}
		trace_util_0.Count(_gc_worker_00000, 74)
		err = w.saveValueToSysTable(key, defaultValueStr)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 77)
			return defaultValue, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 75)
		return defaultValue, nil
	}
	trace_util_0.Count(_gc_worker_00000, 71)
	return strings.EqualFold(str, booleanTrue), nil
}

func (w *GCWorker) getGCConcurrency(ctx context.Context) (int, error) {
	trace_util_0.Count(_gc_worker_00000, 78)
	useAutoConcurrency, err := w.checkUseAutoConcurrency()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 83)
		logutil.Logger(ctx).Error("[gc worker] failed to load config gc_auto_concurrency. use default value.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		useAutoConcurrency = gcDefaultAutoConcurrency
	}
	trace_util_0.Count(_gc_worker_00000, 79)
	if !useAutoConcurrency {
		trace_util_0.Count(_gc_worker_00000, 84)
		return w.loadGCConcurrencyWithDefault()
	}

	trace_util_0.Count(_gc_worker_00000, 80)
	stores, err := w.getUpStores(ctx)
	concurrency := len(stores)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 85)
		logutil.Logger(ctx).Error("[gc worker] failed to get up stores to calculate concurrency. use config.",
			zap.String("uuid", w.uuid),
			zap.Error(err))

		concurrency, err = w.loadGCConcurrencyWithDefault()
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 86)
			logutil.Logger(ctx).Error("[gc worker] failed to load gc concurrency from config. use default value.",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			concurrency = gcDefaultConcurrency
		}
	}

	trace_util_0.Count(_gc_worker_00000, 81)
	if concurrency == 0 {
		trace_util_0.Count(_gc_worker_00000, 87)
		logutil.Logger(ctx).Error("[gc worker] no store is up",
			zap.String("uuid", w.uuid))
		return 0, errors.New("[gc worker] no store is up")
	}

	trace_util_0.Count(_gc_worker_00000, 82)
	return concurrency, nil
}

func (w *GCWorker) checkGCInterval(now time.Time) (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 88)
	runInterval, err := w.loadDurationWithDefault(gcRunIntervalKey, gcDefaultRunInterval)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 92)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 89)
	metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(runInterval.Seconds())
	lastRun, err := w.loadTime(gcLastRunTimeKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 93)
		return false, errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 90)
	if lastRun != nil && lastRun.Add(*runInterval).After(now) {
		trace_util_0.Count(_gc_worker_00000, 94)
		logutil.Logger(context.Background()).Debug("[gc worker] skipping garbage collection because gc interval hasn't elapsed since last run",
			zap.String("leaderTick on", w.uuid),
			zap.Duration("interval", *runInterval),
			zap.Time("last run", *lastRun))
		return false, nil
	}

	trace_util_0.Count(_gc_worker_00000, 91)
	return true, nil
}

func (w *GCWorker) calculateNewSafePoint(now time.Time) (*time.Time, error) {
	trace_util_0.Count(_gc_worker_00000, 95)
	lifeTime, err := w.loadDurationWithDefault(gcLifeTimeKey, gcDefaultLifeTime)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 99)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 96)
	metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(lifeTime.Seconds())
	lastSafePoint, err := w.loadTime(gcSafePointKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 100)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 97)
	safePoint := now.Add(-*lifeTime)
	// We should never decrease safePoint.
	if lastSafePoint != nil && safePoint.Before(*lastSafePoint) {
		trace_util_0.Count(_gc_worker_00000, 101)
		logutil.Logger(context.Background()).Info("[gc worker] last safe point is later than current one."+
			"No need to gc."+
			"This might be caused by manually enlarging gc lifetime",
			zap.String("leaderTick on", w.uuid),
			zap.Time("last safe point", *lastSafePoint),
			zap.Time("current safe point", safePoint))
		return nil, nil
	}
	trace_util_0.Count(_gc_worker_00000, 98)
	return &safePoint, nil
}

func (w *GCWorker) runGCJob(ctx context.Context, safePoint uint64, concurrency int) {
	trace_util_0.Count(_gc_worker_00000, 102)
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()
	err := w.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 109)
		logutil.Logger(ctx).Error("[gc worker] resolve locks returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		w.done <- errors.Trace(err)
		return
	}
	// Save safe point to pd.
	trace_util_0.Count(_gc_worker_00000, 103)
	err = w.saveSafePoint(w.store.GetSafePointKV(), tikv.GcSavedSafePoint, safePoint)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 110)
		logutil.Logger(ctx).Error("[gc worker] failed to save safe point to PD",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		w.gcIsRunning = false
		metrics.GCJobFailureCounter.WithLabelValues("save_safe_point").Inc()
		w.done <- errors.Trace(err)
		return
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	trace_util_0.Count(_gc_worker_00000, 104)
	time.Sleep(gcSafePointCacheInterval)

	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 111)
		logutil.Logger(ctx).Error("[gc worker] delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		w.done <- errors.Trace(err)
		return
	}
	trace_util_0.Count(_gc_worker_00000, 105)
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 112)
		logutil.Logger(ctx).Error("[gc worker] redo-delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		w.done <- errors.Trace(err)
		return
	}

	trace_util_0.Count(_gc_worker_00000, 106)
	useDistributedGC, err := w.checkUseDistributedGC()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 113)
		logutil.Logger(ctx).Error("[gc worker] failed to load gc mode, fall back to central mode.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_gc_mode").Inc()
		useDistributedGC = false
	}

	trace_util_0.Count(_gc_worker_00000, 107)
	if useDistributedGC {
		trace_util_0.Count(_gc_worker_00000, 114)
		err = w.uploadSafePointToPD(ctx, safePoint)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 115)
			logutil.Logger(ctx).Error("[gc worker] failed to upload safe point to PD",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			w.gcIsRunning = false
			metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
			w.done <- errors.Trace(err)
			return
		}
	} else {
		trace_util_0.Count(_gc_worker_00000, 116)
		{
			err = w.doGC(ctx, safePoint, concurrency)
			if err != nil {
				trace_util_0.Count(_gc_worker_00000, 117)
				logutil.Logger(ctx).Error("[gc worker] do GC returns an error",
					zap.String("uuid", w.uuid),
					zap.Error(err))
				w.gcIsRunning = false
				metrics.GCJobFailureCounter.WithLabelValues("gc").Inc()
				w.done <- errors.Trace(err)
				return
			}
		}
	}

	trace_util_0.Count(_gc_worker_00000, 108)
	w.done <- nil
}

// deleteRanges processes all delete range records whose ts < safePoint in table `gc_delete_range`
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) deleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 118)
	metrics.GCWorkerCounter.WithLabelValues("delete_range").Inc()

	se := createSession(w.store)
	ranges, err := util.LoadDeleteRanges(se, safePoint)
	se.Close()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 121)
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 119)
	logutil.Logger(ctx).Info("[gc worker] start delete",
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		trace_util_0.Count(_gc_worker_00000, 122)
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 124)
			return errors.Trace(err)
		}

		trace_util_0.Count(_gc_worker_00000, 123)
		se := createSession(w.store)
		err = util.CompleteDeleteRange(se, r)
		se.Close()
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 125)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_gc_worker_00000, 120)
	logutil.Logger(ctx).Info("[gc worker] finish delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

// redoDeleteRanges checks all deleted ranges whose ts is at least `lifetime + 24h` ago. See TiKV RFC #2.
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) redoDeleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 126)
	metrics.GCWorkerCounter.WithLabelValues("redo_delete_range").Inc()

	// We check delete range records that are deleted about 24 hours ago.
	redoDeleteRangesTs := safePoint - oracle.ComposeTS(int64(gcRedoDeleteRangeDelay.Seconds())*1000, 0)

	se := createSession(w.store)
	ranges, err := util.LoadDoneDeleteRanges(se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 129)
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 127)
	logutil.Logger(ctx).Info("[gc worker] start redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		trace_util_0.Count(_gc_worker_00000, 130)
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 132)
			return errors.Trace(err)
		}

		trace_util_0.Count(_gc_worker_00000, 131)
		se := createSession(w.store)
		err := util.DeleteDoneRecord(se, r)
		se.Close()
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 133)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_gc_worker_00000, 128)
	logutil.Logger(ctx).Info("[gc worker] finish redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("redo_delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(ctx context.Context, startKey []byte, endKey []byte, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 134)
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := w.getUpStores(ctx)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 138)
		logutil.Logger(ctx).Error("[gc worker] delete ranges: got an error while trying to get store list from PD",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 135)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdUnsafeDestroyRange,
		UnsafeDestroyRange: &kvrpcpb.UnsafeDestroyRangeRequest{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}

	var wg sync.WaitGroup

	for _, store := range stores {
		trace_util_0.Count(_gc_worker_00000, 139)
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			trace_util_0.Count(_gc_worker_00000, 140)
			defer wg.Done()
			_, err1 := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.UnsafeDestroyRangeTimeout)
			if err1 != nil {
				trace_util_0.Count(_gc_worker_00000, 141)
				logutil.Logger(ctx).Error("[gc worker] destroy range on store failed with error",
					zap.String("uuid", w.uuid),
					zap.Uint64("storeID", storeID),
					zap.Error(err))
				err = err1
			}
		}()
	}

	trace_util_0.Count(_gc_worker_00000, 136)
	wg.Wait()

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := tikv.NewNotifyDeleteRangeTask(w.store, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 142)
		logutil.Logger(ctx).Error("[gc worker] failed notifying regions affected by UnsafeDestroyRange",
			zap.String("uuid", w.uuid),
			zap.Binary("startKey", startKey),
			zap.Binary("endKey", endKey),
			zap.Error(err))
	}

	trace_util_0.Count(_gc_worker_00000, 137)
	return errors.Trace(err)
}

func (w *GCWorker) getUpStores(ctx context.Context) ([]*metapb.Store, error) {
	trace_util_0.Count(_gc_worker_00000, 143)
	stores, err := w.pdClient.GetAllStores(ctx)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 146)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 144)
	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		trace_util_0.Count(_gc_worker_00000, 147)
		if store.State == metapb.StoreState_Up {
			trace_util_0.Count(_gc_worker_00000, 148)
			upStores = append(upStores, store)
		}
	}
	trace_util_0.Count(_gc_worker_00000, 145)
	return upStores, nil
}

func (w *GCWorker) loadGCConcurrencyWithDefault() (int, error) {
	trace_util_0.Count(_gc_worker_00000, 149)
	str, err := w.loadValueFromSysTable(gcConcurrencyKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 155)
		return gcDefaultConcurrency, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 150)
	if str == "" {
		trace_util_0.Count(_gc_worker_00000, 156)
		err = w.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 158)
			return gcDefaultConcurrency, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 157)
		return gcDefaultConcurrency, nil
	}

	trace_util_0.Count(_gc_worker_00000, 151)
	jobConcurrency, err := strconv.Atoi(str)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 159)
		return gcDefaultConcurrency, err
	}

	trace_util_0.Count(_gc_worker_00000, 152)
	if jobConcurrency < gcMinConcurrency {
		trace_util_0.Count(_gc_worker_00000, 160)
		jobConcurrency = gcMinConcurrency
	}

	trace_util_0.Count(_gc_worker_00000, 153)
	if jobConcurrency > gcMaxConcurrency {
		trace_util_0.Count(_gc_worker_00000, 161)
		jobConcurrency = gcMaxConcurrency
	}

	trace_util_0.Count(_gc_worker_00000, 154)
	return jobConcurrency, nil
}

func (w *GCWorker) checkUseDistributedGC() (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 162)
	str, err := w.loadValueFromSysTable(gcModeKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 167)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 163)
	if str == "" {
		trace_util_0.Count(_gc_worker_00000, 168)
		err = w.saveValueToSysTable(gcModeKey, gcModeDefault)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 170)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 169)
		str = gcModeDefault
	}
	trace_util_0.Count(_gc_worker_00000, 164)
	if strings.EqualFold(str, gcModeDistributed) {
		trace_util_0.Count(_gc_worker_00000, 171)
		return true, nil
	}
	trace_util_0.Count(_gc_worker_00000, 165)
	if strings.EqualFold(str, gcModeCentral) {
		trace_util_0.Count(_gc_worker_00000, 172)
		return false, nil
	}
	trace_util_0.Count(_gc_worker_00000, 166)
	logutil.Logger(context.Background()).Warn("[gc worker] distributed mode will be used",
		zap.String("invalid gc mode", str))
	return true, nil
}

func (w *GCWorker) resolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 173)
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	logutil.Logger(ctx).Info("[gc worker] start resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	handler := func(ctx context.Context, r kv.KeyRange) (int, error) {
		trace_util_0.Count(_gc_worker_00000, 176)
		return w.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}

	trace_util_0.Count(_gc_worker_00000, 174)
	runner := tikv.NewRangeTaskRunner("resolve-locks-runner", w.store, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 177)
		logutil.Logger(ctx).Error("[gc worker] resolve locks failed",
			zap.String("uuid", w.uuid),
			zap.Uint64("safePoint", safePoint),
			zap.Error(err))
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 175)
	logutil.Logger(ctx).Info("[gc worker] finish resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int32("regions", runner.CompletedRegions()))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) resolveLocksForRange(
	ctx context.Context,
	safePoint uint64,
	startKey []byte,
	endKey []byte,
) (int, error) {
	trace_util_0.Count(_gc_worker_00000, 178)
	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdScanLock,
		ScanLock: &kvrpcpb.ScanLockRequest{
			MaxVersion: safePoint,
			Limit:      gcScanLockLimit,
		},
	}

	regions := 0
	key := startKey
	for {
		trace_util_0.Count(_gc_worker_00000, 180)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_gc_worker_00000, 192)
			return regions, errors.New("[gc worker] gc job canceled")
		default:
			trace_util_0.Count(_gc_worker_00000, 193)
		}

		trace_util_0.Count(_gc_worker_00000, 181)
		bo := tikv.NewBackoffer(ctx, tikv.GcResolveLockMaxBackoff)

		req.ScanLock.StartKey = key
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 194)
			return regions, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 182)
		resp, err := w.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 195)
			return regions, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 183)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 196)
			return regions, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 184)
		if regionErr != nil {
			trace_util_0.Count(_gc_worker_00000, 197)
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_gc_worker_00000, 199)
				return regions, errors.Trace(err)
			}
			trace_util_0.Count(_gc_worker_00000, 198)
			continue
		}
		trace_util_0.Count(_gc_worker_00000, 185)
		locksResp := resp.ScanLock
		if locksResp == nil {
			trace_util_0.Count(_gc_worker_00000, 200)
			return regions, errors.Trace(tikv.ErrBodyMissing)
		}
		trace_util_0.Count(_gc_worker_00000, 186)
		if locksResp.GetError() != nil {
			trace_util_0.Count(_gc_worker_00000, 201)
			return regions, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		trace_util_0.Count(_gc_worker_00000, 187)
		locksInfo := locksResp.GetLocks()
		locks := make([]*tikv.Lock, len(locksInfo))
		for i := range locksInfo {
			trace_util_0.Count(_gc_worker_00000, 202)
			locks[i] = tikv.NewLock(locksInfo[i])
		}

		trace_util_0.Count(_gc_worker_00000, 188)
		ok, err1 := w.store.GetLockResolver().BatchResolveLocks(bo, locks, loc.Region)
		if err1 != nil {
			trace_util_0.Count(_gc_worker_00000, 203)
			return regions, errors.Trace(err1)
		}
		trace_util_0.Count(_gc_worker_00000, 189)
		if !ok {
			trace_util_0.Count(_gc_worker_00000, 204)
			err = bo.Backoff(tikv.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
			if err != nil {
				trace_util_0.Count(_gc_worker_00000, 206)
				return regions, errors.Trace(err)
			}
			trace_util_0.Count(_gc_worker_00000, 205)
			continue
		}

		trace_util_0.Count(_gc_worker_00000, 190)
		if len(locks) < gcScanLockLimit {
			trace_util_0.Count(_gc_worker_00000, 207)
			regions++
			key = loc.EndKey
		} else {
			trace_util_0.Count(_gc_worker_00000, 208)
			{
				logutil.Logger(ctx).Info("[gc worker] region has more than limit locks",
					zap.String("uuid", w.uuid),
					zap.Uint64("region", loc.Region.GetID()),
					zap.Int("scan lock limit", gcScanLockLimit))
				metrics.GCRegionTooManyLocksCounter.Inc()
				key = locks[len(locks)-1].Key
			}
		}

		trace_util_0.Count(_gc_worker_00000, 191)
		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			trace_util_0.Count(_gc_worker_00000, 209)
			break
		}
	}
	trace_util_0.Count(_gc_worker_00000, 179)
	return regions, nil
}

func (w *GCWorker) uploadSafePointToPD(ctx context.Context, safePoint uint64) error {
	trace_util_0.Count(_gc_worker_00000, 210)
	var newSafePoint uint64
	var err error

	bo := tikv.NewBackoffer(ctx, tikv.GcOneRegionMaxBackoff)
	for {
		trace_util_0.Count(_gc_worker_00000, 213)
		newSafePoint, err = w.pdClient.UpdateGCSafePoint(ctx, safePoint)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 215)
			if errors.Cause(err) == context.Canceled {
				trace_util_0.Count(_gc_worker_00000, 218)
				return errors.Trace(err)
			}
			trace_util_0.Count(_gc_worker_00000, 216)
			err = bo.Backoff(tikv.BoPDRPC, errors.Errorf("failed to upload safe point to PD, err: %v", err))
			if err != nil {
				trace_util_0.Count(_gc_worker_00000, 219)
				return errors.Trace(err)
			}
			trace_util_0.Count(_gc_worker_00000, 217)
			continue
		}
		trace_util_0.Count(_gc_worker_00000, 214)
		break
	}

	trace_util_0.Count(_gc_worker_00000, 211)
	if newSafePoint != safePoint {
		trace_util_0.Count(_gc_worker_00000, 220)
		logutil.Logger(ctx).Warn("[gc worker] PD rejected safe point",
			zap.String("uuid", w.uuid),
			zap.Uint64("our safe point", safePoint),
			zap.Uint64("using another safe point", newSafePoint))
		return errors.Errorf("PD rejected our safe point %v but is using another safe point %v", safePoint, newSafePoint)
	}
	trace_util_0.Count(_gc_worker_00000, 212)
	logutil.Logger(ctx).Info("[gc worker] sent safe point to PD",
		zap.String("uuid", w.uuid),
		zap.Uint64("safe point", safePoint))
	return nil
}

type gcTask struct {
	startKey  []byte
	endKey    []byte
	safePoint uint64
}

type gcTaskWorker struct {
	identifier string
	store      tikv.Storage
	taskCh     chan *gcTask
	wg         *sync.WaitGroup
	// successRegions and failedRegions use atomic to read and set.
	successRegions *int32
	failedRegions  *int32
}

func newGCTaskWorker(store tikv.Storage, taskCh chan *gcTask, wg *sync.WaitGroup, identifer string, successRegions *int32, failedRegions *int32) *gcTaskWorker {
	trace_util_0.Count(_gc_worker_00000, 221)
	return &gcTaskWorker{
		identifer,
		store,
		taskCh,
		wg,
		successRegions,
		failedRegions,
	}
}

func (w *gcTaskWorker) run() {
	trace_util_0.Count(_gc_worker_00000, 222)
	defer w.wg.Done()
	for task := range w.taskCh {
		trace_util_0.Count(_gc_worker_00000, 223)
		err := w.doGCForRange(task.startKey, task.endKey, task.safePoint)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 224)
			logutil.Logger(context.Background()).Error("[gc worker] gc interrupted because get region error",
				zap.String("uuid", w.identifier),
				zap.Binary("startKey", task.startKey),
				zap.Binary("endKey", task.endKey),
				zap.Error(err))
		}
	}
}

func (w *gcTaskWorker) doGCForRange(startKey []byte, endKey []byte, safePoint uint64) error {
	trace_util_0.Count(_gc_worker_00000, 225)
	var successRegions int32
	var failedRegions int32
	defer func() {
		trace_util_0.Count(_gc_worker_00000, 228)
		atomic.AddInt32(w.successRegions, successRegions)
		atomic.AddInt32(w.failedRegions, failedRegions)
		metrics.GCActionRegionResultCounter.WithLabelValues("success").Add(float64(successRegions))
		metrics.GCActionRegionResultCounter.WithLabelValues("fail").Add(float64(failedRegions))
	}()
	trace_util_0.Count(_gc_worker_00000, 226)
	key := startKey
	for {
		trace_util_0.Count(_gc_worker_00000, 229)
		bo := tikv.NewBackoffer(context.Background(), tikv.GcOneRegionMaxBackoff)
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 233)
			return errors.Trace(err)
		}

		trace_util_0.Count(_gc_worker_00000, 230)
		var regionErr *errorpb.Error
		regionErr, err = w.doGCForRegion(bo, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			trace_util_0.Count(_gc_worker_00000, 234)
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err == nil {
				trace_util_0.Count(_gc_worker_00000, 235)
				continue
			}
		}

		trace_util_0.Count(_gc_worker_00000, 231)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 236)
			logutil.Logger(context.Background()).Warn("[gc worker]",
				zap.String("uuid", w.identifier),
				zap.String("gc for range", fmt.Sprintf("[%d, %d)", startKey, endKey)),
				zap.Uint64("safePoint", safePoint),
				zap.Error(err))
			failedRegions++
		} else {
			trace_util_0.Count(_gc_worker_00000, 237)
			{
				successRegions++
			}
		}

		trace_util_0.Count(_gc_worker_00000, 232)
		key = loc.EndKey
		if len(key) == 0 || bytes.Compare(key, endKey) >= 0 {
			trace_util_0.Count(_gc_worker_00000, 238)
			break
		}
	}

	trace_util_0.Count(_gc_worker_00000, 227)
	return nil
}

// doGCForRegion used for gc for region.
// these two errors should not return together, for more, see the func 'doGC'
func (w *gcTaskWorker) doGCForRegion(bo *tikv.Backoffer, safePoint uint64, region tikv.RegionVerID) (*errorpb.Error, error) {
	trace_util_0.Count(_gc_worker_00000, 239)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGC,
		GC: &kvrpcpb.GCRequest{
			SafePoint: safePoint,
		},
	}

	resp, err := w.store.SendReq(bo, req, region, tikv.GCTimeout)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 245)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 240)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 246)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 241)
	if regionErr != nil {
		trace_util_0.Count(_gc_worker_00000, 247)
		return regionErr, nil
	}

	trace_util_0.Count(_gc_worker_00000, 242)
	gcResp := resp.GC
	if gcResp == nil {
		trace_util_0.Count(_gc_worker_00000, 248)
		return nil, errors.Trace(tikv.ErrBodyMissing)
	}
	trace_util_0.Count(_gc_worker_00000, 243)
	if gcResp.GetError() != nil {
		trace_util_0.Count(_gc_worker_00000, 249)
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	trace_util_0.Count(_gc_worker_00000, 244)
	return nil, nil
}

func (w *GCWorker) genNextGCTask(bo *tikv.Backoffer, safePoint uint64, key kv.Key) (*gcTask, error) {
	trace_util_0.Count(_gc_worker_00000, 250)
	loc, err := w.store.GetRegionCache().LocateKey(bo, key)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 252)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 251)
	task := &gcTask{
		startKey:  key,
		endKey:    loc.EndKey,
		safePoint: safePoint,
	}
	return task, nil
}

func (w *GCWorker) doGC(ctx context.Context, safePoint uint64, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 253)
	metrics.GCWorkerCounter.WithLabelValues("do_gc").Inc()
	logutil.Logger(ctx).Info("[gc worker]",
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()
	var successRegions int32
	var failedRegions int32

	ticker := time.NewTicker(gcJobLogTickInterval)
	defer ticker.Stop()

	// Create task queue and start task workers.
	gcTaskCh := make(chan *gcTask, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		trace_util_0.Count(_gc_worker_00000, 256)
		w := newGCTaskWorker(w.store, gcTaskCh, &wg, w.uuid, &successRegions, &failedRegions)
		wg.Add(1)
		go w.run()
	}

	trace_util_0.Count(_gc_worker_00000, 254)
	var key []byte
	defer func() {
		trace_util_0.Count(_gc_worker_00000, 257)
		close(gcTaskCh)
		wg.Wait()
		logutil.Logger(ctx).Info("[gc worker]",
			zap.String("uuid", w.uuid),
			zap.Uint64("safePoint", safePoint),
			zap.Int32("successful regions", atomic.LoadInt32(&successRegions)),
			zap.Int32("failed regions", atomic.LoadInt32(&failedRegions)),
			zap.Duration("total cost time", time.Since(startTime)))
		metrics.GCHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())
	}()

	trace_util_0.Count(_gc_worker_00000, 255)
	for {
		trace_util_0.Count(_gc_worker_00000, 258)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_gc_worker_00000, 262)
			return errors.New("[gc worker] gc job canceled")
		case <-ticker.C:
			trace_util_0.Count(_gc_worker_00000, 263)
			logutil.Logger(ctx).Info("[gc worker]",
				zap.String("gc in process", w.uuid),
				zap.Uint64("safePoint", safePoint),
				zap.Int32("successful regions", atomic.LoadInt32(&successRegions)),
				zap.Int32("failed regions", atomic.LoadInt32(&failedRegions)),
				zap.Duration("total cost time", time.Since(startTime)))
		default:
			trace_util_0.Count(_gc_worker_00000, 264)
		}

		trace_util_0.Count(_gc_worker_00000, 259)
		bo := tikv.NewBackoffer(ctx, tikv.GcOneRegionMaxBackoff)
		task, err := w.genNextGCTask(bo, safePoint, key)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 265)
			return errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 260)
		if task != nil {
			trace_util_0.Count(_gc_worker_00000, 266)
			gcTaskCh <- task
			key = task.endKey
		}

		trace_util_0.Count(_gc_worker_00000, 261)
		if len(key) == 0 {
			trace_util_0.Count(_gc_worker_00000, 267)
			return nil
		}
	}
}

func (w *GCWorker) checkLeader() (bool, error) {
	trace_util_0.Count(_gc_worker_00000, 268)
	metrics.GCWorkerCounter.WithLabelValues("check_leader").Inc()
	se := createSession(w.store)
	defer se.Close()

	ctx := context.Background()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 275)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 269)
	w.session = se
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 276)
		_, err1 := se.Execute(ctx, "ROLLBACK")
		terror.Log(errors.Trace(err1))
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 270)
	logutil.Logger(context.Background()).Debug("[gc worker] got leader", zap.String("uuid", leader))
	if leader == w.uuid {
		trace_util_0.Count(_gc_worker_00000, 277)
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 280)
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 278)
		_, err = se.Execute(ctx, "COMMIT")
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 281)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 279)
		return true, nil
	}

	trace_util_0.Count(_gc_worker_00000, 271)
	_, err = se.Execute(ctx, "ROLLBACK")
	terror.Log(errors.Trace(err))

	_, err = se.Execute(ctx, "BEGIN")
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 282)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 272)
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 283)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 273)
	if lease == nil || lease.Before(time.Now()) {
		trace_util_0.Count(_gc_worker_00000, 284)
		logutil.Logger(context.Background()).Debug("[gc worker] register as leader",
			zap.String("uuid", w.uuid))
		metrics.GCWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 289)
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 285)
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 290)
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 286)
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 291)
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 287)
		_, err = se.Execute(ctx, "COMMIT")
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 292)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 288)
		return true, nil
	}
	trace_util_0.Count(_gc_worker_00000, 274)
	_, err1 := se.Execute(ctx, "ROLLBACK")
	terror.Log(errors.Trace(err1))
	return false, nil
}

func (w *GCWorker) saveSafePoint(kv tikv.SafePointKV, key string, t uint64) error {
	trace_util_0.Count(_gc_worker_00000, 293)
	s := strconv.FormatUint(t, 10)
	err := kv.Put(tikv.GcSavedSafePoint, s)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 295)
		logutil.Logger(context.Background()).Error("save safepoint failed", zap.Error(err))
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 294)
	return nil
}

func (w *GCWorker) saveTime(key string, t time.Time) error {
	trace_util_0.Count(_gc_worker_00000, 296)
	err := w.saveValueToSysTable(key, t.Format(tidbutil.GCTimeFormat))
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string) (*time.Time, error) {
	trace_util_0.Count(_gc_worker_00000, 297)
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 301)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 298)
	if str == "" {
		trace_util_0.Count(_gc_worker_00000, 302)
		return nil, nil
	}
	trace_util_0.Count(_gc_worker_00000, 299)
	t, err := tidbutil.CompatibleParseGCTime(str)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 303)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 300)
	return &t, nil
}

func (w *GCWorker) saveDuration(key string, d time.Duration) error {
	trace_util_0.Count(_gc_worker_00000, 304)
	err := w.saveValueToSysTable(key, d.String())
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	trace_util_0.Count(_gc_worker_00000, 305)
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 309)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 306)
	if str == "" {
		trace_util_0.Count(_gc_worker_00000, 310)
		return nil, nil
	}
	trace_util_0.Count(_gc_worker_00000, 307)
	d, err := time.ParseDuration(str)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 311)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 308)
	return &d, nil
}

func (w *GCWorker) loadDurationWithDefault(key string, def time.Duration) (*time.Duration, error) {
	trace_util_0.Count(_gc_worker_00000, 312)
	d, err := w.loadDuration(key)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 315)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 313)
	if d == nil {
		trace_util_0.Count(_gc_worker_00000, 316)
		err = w.saveDuration(key, def)
		if err != nil {
			trace_util_0.Count(_gc_worker_00000, 318)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_gc_worker_00000, 317)
		return &def, nil
	}
	trace_util_0.Count(_gc_worker_00000, 314)
	return d, nil
}

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	trace_util_0.Count(_gc_worker_00000, 319)
	ctx := context.Background()
	stmt := fmt.Sprintf(`SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := w.session.Execute(ctx, stmt)
	if len(rs) > 0 {
		trace_util_0.Count(_gc_worker_00000, 324)
		defer terror.Call(rs[0].Close)
	}
	trace_util_0.Count(_gc_worker_00000, 320)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 325)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 321)
	req := rs[0].NewRecordBatch()
	err = rs[0].Next(ctx, req)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 326)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 322)
	if req.NumRows() == 0 {
		trace_util_0.Count(_gc_worker_00000, 327)
		logutil.Logger(context.Background()).Debug("[gc worker] load kv",
			zap.String("key", key))
		return "", nil
	}
	trace_util_0.Count(_gc_worker_00000, 323)
	value := req.GetRow(0).GetString(0)
	logutil.Logger(context.Background()).Debug("[gc worker] load kv",
		zap.String("key", key),
		zap.String("value", value))
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	trace_util_0.Count(_gc_worker_00000, 328)
	stmt := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	if w.session == nil {
		trace_util_0.Count(_gc_worker_00000, 330)
		return errors.New("[saveValueToSysTable session is nil]")
	}
	trace_util_0.Count(_gc_worker_00000, 329)
	_, err := w.session.Execute(context.Background(), stmt)
	logutil.Logger(context.Background()).Debug("[gc worker] save kv",
		zap.String("key", key),
		zap.String("value", value),
		zap.Error(err))
	return errors.Trace(err)
}

// RunGCJob sends GC command to KV. It is exported for kv api, do not use it with GCWorker at the same time.
func RunGCJob(ctx context.Context, s tikv.Storage, safePoint uint64, identifier string, concurrency int) error {
	trace_util_0.Count(_gc_worker_00000, 331)
	gcWorker := &GCWorker{
		store: s,
		uuid:  identifier,
	}

	err := gcWorker.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 336)
		return errors.Trace(err)
	}

	trace_util_0.Count(_gc_worker_00000, 332)
	if concurrency <= 0 {
		trace_util_0.Count(_gc_worker_00000, 337)
		return errors.Errorf("[gc worker] gc concurrency should greater than 0, current concurrency: %v", concurrency)
	}

	trace_util_0.Count(_gc_worker_00000, 333)
	err = gcWorker.saveSafePoint(gcWorker.store.GetSafePointKV(), tikv.GcSavedSafePoint, safePoint)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 338)
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	trace_util_0.Count(_gc_worker_00000, 334)
	time.Sleep(gcSafePointCacheInterval)

	err = gcWorker.doGC(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 339)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 335)
	return nil
}

// RunDistributedGCJob notifies TiKVs to do GC. It is exported for kv api, do not use it with GCWorker at the same time.
// This function may not finish immediately because it may take some time to do resolveLocks.
// Param concurrency specifies the concurrency of resolveLocks phase.
func RunDistributedGCJob(
	ctx context.Context,
	s tikv.Storage,
	pd pd.Client,
	safePoint uint64,
	identifier string,
	concurrency int,
) error {
	trace_util_0.Count(_gc_worker_00000, 340)
	gcWorker := &GCWorker{
		store:    s,
		uuid:     identifier,
		pdClient: pd,
	}

	err := gcWorker.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 344)
		return errors.Trace(err)
	}

	// Save safe point to pd.
	trace_util_0.Count(_gc_worker_00000, 341)
	err = gcWorker.saveSafePoint(gcWorker.store.GetSafePointKV(), tikv.GcSavedSafePoint, safePoint)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 345)
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	trace_util_0.Count(_gc_worker_00000, 342)
	time.Sleep(gcSafePointCacheInterval)

	err = gcWorker.uploadSafePointToPD(ctx, safePoint)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 346)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 343)
	return nil
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store tikv.Storage) (*MockGCWorker, error) {
	trace_util_0.Count(_gc_worker_00000, 347)
	ver, err := store.CurrentVersion()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 351)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 348)
	hostName, err := os.Hostname()
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 352)
		hostName = "unknown"
	}
	trace_util_0.Count(_gc_worker_00000, 349)
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	worker.session, err = session.CreateSession(worker.store)
	if err != nil {
		trace_util_0.Count(_gc_worker_00000, 353)
		logutil.Logger(context.Background()).Error("initialize MockGCWorker session fail", zap.Error(err))
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_gc_worker_00000, 350)
	privilege.BindPrivilegeManager(worker.session, nil)
	worker.session.GetSessionVars().InRestrictedSQL = true
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	trace_util_0.Count(_gc_worker_00000, 354)
	logutil.Logger(ctx).Error("deleteRanges is called")
	return w.worker.deleteRanges(ctx, safePoint, 1)
}

var _gc_worker_00000 = "store/tikv/gcworker/gc_worker.go"
