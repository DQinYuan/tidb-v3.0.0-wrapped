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
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

var (
	tikvTxnCmdCountWithGet             = metrics.TiKVTxnCmdCounter.WithLabelValues("get")
	tikvTxnCmdHistogramWithGet         = metrics.TiKVTxnCmdHistogram.WithLabelValues("get")
	tikvTxnCmdCountWithSeek            = metrics.TiKVTxnCmdCounter.WithLabelValues("seek")
	tikvTxnCmdHistogramWithSeek        = metrics.TiKVTxnCmdHistogram.WithLabelValues("seek")
	tikvTxnCmdCountWithSeekReverse     = metrics.TiKVTxnCmdCounter.WithLabelValues("seek_reverse")
	tikvTxnCmdHistogramWithSeekReverse = metrics.TiKVTxnCmdHistogram.WithLabelValues("seek_reverse")
	tikvTxnCmdCountWithDelete          = metrics.TiKVTxnCmdCounter.WithLabelValues("delete")
	tikvTxnCmdCountWithSet             = metrics.TiKVTxnCmdCounter.WithLabelValues("set")
	tikvTxnCmdCountWithCommit          = metrics.TiKVTxnCmdCounter.WithLabelValues("commit")
	tikvTxnCmdHistogramWithCommit      = metrics.TiKVTxnCmdHistogram.WithLabelValues("commit")
	tikvTxnCmdCountWithRollback        = metrics.TiKVTxnCmdCounter.WithLabelValues("rollback")
	tikvTxnCmdHistogramWithLockKeys    = metrics.TiKVTxnCmdCounter.WithLabelValues("lock_keys")
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *tikvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	valid     bool
	lockKeys  [][]byte
	lockedMap map[string]struct{}
	mu        sync.Mutex // For thread-safe LockKeys function.
	dirty     bool
	setCnt    int64
	vars      *kv.Variables
	committer *twoPhaseCommitter

	// For data consistency check.
	assertions []assertionPair
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	trace_util_0.Count(_txn_00000, 0)
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		trace_util_0.Count(_txn_00000, 2)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_txn_00000, 1)
	return newTikvTxnWithStartTS(store, startTS)
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *tikvStore, startTS uint64) (*tikvTxn, error) {
	trace_util_0.Count(_txn_00000, 3)
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
		lockedMap: map[string]struct{}{},
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      kv.DefaultVars,
	}, nil
}

type assertionPair struct {
	key       kv.Key
	assertion kv.AssertionType
}

func (a assertionPair) String() string {
	trace_util_0.Count(_txn_00000, 4)
	return fmt.Sprintf("key: %s, assertion type: %d", a.key, a.assertion)
}

// SetAssertion sets a assertion for the key operation.
func (txn *tikvTxn) SetAssertion(key kv.Key, assertion kv.AssertionType) {
	trace_util_0.Count(_txn_00000, 5)
	txn.assertions = append(txn.assertions, assertionPair{key, assertion})
}

func (txn *tikvTxn) SetVars(vars *kv.Variables) {
	trace_util_0.Count(_txn_00000, 6)
	txn.vars = vars
	txn.snapshot.vars = vars
}

// SetCap sets the transaction's MemBuffer capability, to reduce memory allocations.
func (txn *tikvTxn) SetCap(cap int) {
	trace_util_0.Count(_txn_00000, 7)
	txn.us.SetCap(cap)
}

// Reset reset tikvTxn's membuf.
func (txn *tikvTxn) Reset() {
	trace_util_0.Count(_txn_00000, 8)
	txn.us.Reset()
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	trace_util_0.Count(_txn_00000, 9)
	tikvTxnCmdCountWithGet.Inc()
	start := time.Now()
	defer func() {
		trace_util_0.Count(_txn_00000, 14)
		tikvTxnCmdHistogramWithGet.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_txn_00000, 10)
	ret, err := txn.us.Get(k)
	if kv.IsErrNotFound(err) {
		trace_util_0.Count(_txn_00000, 15)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 11)
	if err != nil {
		trace_util_0.Count(_txn_00000, 16)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_txn_00000, 12)
	err = txn.store.CheckVisibility(txn.startTS)
	if err != nil {
		trace_util_0.Count(_txn_00000, 17)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_txn_00000, 13)
	return ret, nil
}

func (txn *tikvTxn) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	trace_util_0.Count(_txn_00000, 18)
	if txn.IsReadOnly() {
		trace_util_0.Count(_txn_00000, 23)
		return txn.snapshot.BatchGet(keys)
	}
	trace_util_0.Count(_txn_00000, 19)
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		trace_util_0.Count(_txn_00000, 24)
		val, err := txn.GetMemBuffer().Get(key)
		if kv.IsErrNotFound(err) {
			trace_util_0.Count(_txn_00000, 27)
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		trace_util_0.Count(_txn_00000, 25)
		if err != nil {
			trace_util_0.Count(_txn_00000, 28)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_txn_00000, 26)
		if len(val) != 0 {
			trace_util_0.Count(_txn_00000, 29)
			bufferValues[i] = val
		}
	}
	trace_util_0.Count(_txn_00000, 20)
	storageValues, err := txn.snapshot.BatchGet(shrinkKeys)
	if err != nil {
		trace_util_0.Count(_txn_00000, 30)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_txn_00000, 21)
	for i, key := range keys {
		trace_util_0.Count(_txn_00000, 31)
		if bufferValues[i] == nil {
			trace_util_0.Count(_txn_00000, 33)
			continue
		}
		trace_util_0.Count(_txn_00000, 32)
		storageValues[string(key)] = bufferValues[i]
	}
	trace_util_0.Count(_txn_00000, 22)
	return storageValues, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	trace_util_0.Count(_txn_00000, 34)
	txn.setCnt++

	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	trace_util_0.Count(_txn_00000, 35)
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_txn_00000, 36)
	tikvTxnCmdCountWithSeek.Inc()
	start := time.Now()
	defer func() {
		trace_util_0.Count(_txn_00000, 38)
		tikvTxnCmdHistogramWithSeek.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_txn_00000, 37)
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_txn_00000, 39)
	tikvTxnCmdCountWithSeekReverse.Inc()
	start := time.Now()
	defer func() {
		trace_util_0.Count(_txn_00000, 41)
		tikvTxnCmdHistogramWithSeekReverse.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_txn_00000, 40)
	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	trace_util_0.Count(_txn_00000, 42)
	tikvTxnCmdCountWithDelete.Inc()

	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	trace_util_0.Count(_txn_00000, 43)
	txn.us.SetOption(opt, val)
	switch opt {
	case kv.Priority:
		trace_util_0.Count(_txn_00000, 44)
		txn.snapshot.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		trace_util_0.Count(_txn_00000, 45)
		txn.snapshot.notFillCache = val.(bool)
	case kv.SyncLog:
		trace_util_0.Count(_txn_00000, 46)
		txn.snapshot.syncLog = val.(bool)
	case kv.KeyOnly:
		trace_util_0.Count(_txn_00000, 47)
		txn.snapshot.keyOnly = val.(bool)
	case kv.SnapshotTS:
		trace_util_0.Count(_txn_00000, 48)
		txn.snapshot.version.Ver = val.(uint64)
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	trace_util_0.Count(_txn_00000, 49)
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) IsPessimistic() bool {
	trace_util_0.Count(_txn_00000, 50)
	return txn.us.GetOption(kv.Pessimistic) != nil
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	trace_util_0.Count(_txn_00000, 51)
	if !txn.valid {
		trace_util_0.Count(_txn_00000, 64)
		return kv.ErrInvalidTxn
	}
	trace_util_0.Count(_txn_00000, 52)
	defer txn.close()

	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		trace_util_0.Count(_txn_00000, 65)
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			trace_util_0.Count(_txn_00000, 66)
			kv.MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	trace_util_0.Count(_txn_00000, 53)
	tikvTxnCmdCountWithSet.Add(float64(txn.setCnt))
	tikvTxnCmdCountWithCommit.Inc()
	start := time.Now()
	defer func() {
		trace_util_0.Count(_txn_00000, 67)
		tikvTxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds())
	}()

	// connID is used for log.
	trace_util_0.Count(_txn_00000, 54)
	var connID uint64
	val := ctx.Value(sessionctx.ConnID)
	if val != nil {
		trace_util_0.Count(_txn_00000, 68)
		connID = val.(uint64)
	}

	trace_util_0.Count(_txn_00000, 55)
	var err error
	// If the txn use pessimistic lock, committer is initialized.
	committer := txn.committer
	if committer == nil {
		trace_util_0.Count(_txn_00000, 69)
		committer, err = newTwoPhaseCommitter(txn, connID)
		if err != nil {
			trace_util_0.Count(_txn_00000, 70)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_txn_00000, 56)
	if err := committer.initKeysAndMutations(); err != nil {
		trace_util_0.Count(_txn_00000, 71)
		return errors.Trace(err)
	}
	trace_util_0.Count(_txn_00000, 57)
	if len(committer.keys) == 0 {
		trace_util_0.Count(_txn_00000, 72)
		return nil
	}

	trace_util_0.Count(_txn_00000, 58)
	defer func() {
		trace_util_0.Count(_txn_00000, 73)
		ctxValue := ctx.Value(execdetails.CommitDetailCtxKey)
		if ctxValue != nil {
			trace_util_0.Count(_txn_00000, 74)
			commitDetail := ctxValue.(**execdetails.CommitDetails)
			if *commitDetail != nil {
				trace_util_0.Count(_txn_00000, 75)
				(*commitDetail).TxnRetry += 1
			} else {
				trace_util_0.Count(_txn_00000, 76)
				{
					*commitDetail = committer.detail
				}
			}
		}
	}()
	// latches disabled
	// pessimistic transaction should also bypass latch.
	trace_util_0.Count(_txn_00000, 59)
	if txn.store.txnLatches == nil || txn.IsPessimistic() {
		trace_util_0.Count(_txn_00000, 77)
		err = committer.executeAndWriteFinishBinlog(ctx)
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return errors.Trace(err)
	}

	// latches enabled
	// for transactions which need to acquire latches
	trace_util_0.Count(_txn_00000, 60)
	start = time.Now()
	lock := txn.store.txnLatches.Lock(committer.startTS, committer.keys)
	committer.detail.LocalLatchTime = time.Since(start)
	if committer.detail.LocalLatchTime > 0 {
		trace_util_0.Count(_txn_00000, 78)
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(committer.detail.LocalLatchTime.Seconds())
	}
	trace_util_0.Count(_txn_00000, 61)
	defer txn.store.txnLatches.UnLock(lock)
	if lock.IsStale() {
		trace_util_0.Count(_txn_00000, 79)
		return kv.ErrWriteConflictInTiDB.FastGenByArgs(txn.startTS)
	}
	trace_util_0.Count(_txn_00000, 62)
	err = committer.executeAndWriteFinishBinlog(ctx)
	if err == nil {
		trace_util_0.Count(_txn_00000, 80)
		lock.SetCommitTS(committer.commitTS)
	}
	trace_util_0.Count(_txn_00000, 63)
	logutil.Logger(ctx).Debug("[kv] txnLatches enabled while txn retryable", zap.Error(err))
	return errors.Trace(err)
}

func (txn *tikvTxn) close() {
	trace_util_0.Count(_txn_00000, 81)
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	trace_util_0.Count(_txn_00000, 82)
	if !txn.valid {
		trace_util_0.Count(_txn_00000, 85)
		return kv.ErrInvalidTxn
	}
	// Clean up pessimistic lock.
	trace_util_0.Count(_txn_00000, 83)
	if txn.IsPessimistic() && txn.committer != nil {
		trace_util_0.Count(_txn_00000, 86)
		err := txn.rollbackPessimisticLocks()
		if err != nil {
			trace_util_0.Count(_txn_00000, 87)
			logutil.Logger(context.Background()).Error(err.Error())
		}
	}
	trace_util_0.Count(_txn_00000, 84)
	txn.close()
	logutil.Logger(context.Background()).Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	tikvTxnCmdCountWithRollback.Inc()

	return nil
}

func (txn *tikvTxn) rollbackPessimisticLocks() error {
	trace_util_0.Count(_txn_00000, 88)
	if len(txn.lockKeys) == 0 {
		trace_util_0.Count(_txn_00000, 90)
		return nil
	}
	trace_util_0.Count(_txn_00000, 89)
	return txn.committer.pessimisticRollbackKeys(NewBackoffer(context.Background(), cleanupMaxBackoff), txn.lockKeys)
}

func (txn *tikvTxn) LockKeys(ctx context.Context, forUpdateTS uint64, keysInput ...kv.Key) error {
	trace_util_0.Count(_txn_00000, 91)
	// Exclude keys that are already locked.
	keys := make([][]byte, 0, len(keysInput))
	txn.mu.Lock()
	for _, key := range keysInput {
		trace_util_0.Count(_txn_00000, 96)
		if _, ok := txn.lockedMap[string(key)]; !ok {
			trace_util_0.Count(_txn_00000, 97)
			keys = append(keys, key)
		}
	}
	trace_util_0.Count(_txn_00000, 92)
	txn.mu.Unlock()
	if len(keys) == 0 {
		trace_util_0.Count(_txn_00000, 98)
		return nil
	}
	trace_util_0.Count(_txn_00000, 93)
	tikvTxnCmdHistogramWithLockKeys.Inc()
	if txn.IsPessimistic() && forUpdateTS > 0 {
		trace_util_0.Count(_txn_00000, 99)
		if txn.committer == nil {
			trace_util_0.Count(_txn_00000, 102)
			// connID is used for log.
			var connID uint64
			var err error
			val := ctx.Value(sessionctx.ConnID)
			if val != nil {
				trace_util_0.Count(_txn_00000, 104)
				connID = val.(uint64)
			}
			trace_util_0.Count(_txn_00000, 103)
			txn.committer, err = newTwoPhaseCommitter(txn, connID)
			if err != nil {
				trace_util_0.Count(_txn_00000, 105)
				return err
			}
		}
		trace_util_0.Count(_txn_00000, 100)
		var assignedPrimaryKey bool
		if txn.committer.primaryKey == nil {
			trace_util_0.Count(_txn_00000, 106)
			txn.committer.primaryKey = keys[0]
			assignedPrimaryKey = true
		}

		trace_util_0.Count(_txn_00000, 101)
		bo := NewBackoffer(ctx, pessimisticLockMaxBackoff).WithVars(txn.vars)
		txn.committer.forUpdateTS = forUpdateTS
		// If the number of keys greater than 1, it can be on different region,
		// concurrently execute on multiple regions may lead to deadlock.
		txn.committer.isFirstLock = len(txn.lockKeys) == 0 && len(keys) == 1
		err := txn.committer.pessimisticLockKeys(bo, keys)
		if err != nil {
			trace_util_0.Count(_txn_00000, 107)
			for _, key := range keys {
				trace_util_0.Count(_txn_00000, 111)
				txn.us.DeleteConditionPair(key)
			}
			trace_util_0.Count(_txn_00000, 108)
			wg := txn.asyncPessimisticRollback(ctx, keys)
			if dl, ok := errors.Cause(err).(*ErrDeadlock); ok && hashInKeys(dl.DeadlockKeyHash, keys) {
				trace_util_0.Count(_txn_00000, 112)
				dl.IsRetryable = true
				// Wait for the pessimistic rollback to finish before we retry the statement.
				wg.Wait()
				// Sleep a little, wait for the other transaction that blocked by this transaction to acquire the lock.
				time.Sleep(time.Millisecond * 5)
			}
			trace_util_0.Count(_txn_00000, 109)
			if assignedPrimaryKey {
				trace_util_0.Count(_txn_00000, 113)
				// unset the primary key if we assigned primary key when failed to lock it.
				txn.committer.primaryKey = nil
			}
			trace_util_0.Count(_txn_00000, 110)
			return err
		}
	}
	trace_util_0.Count(_txn_00000, 94)
	txn.mu.Lock()
	txn.lockKeys = append(txn.lockKeys, keys...)
	for _, key := range keys {
		trace_util_0.Count(_txn_00000, 114)
		txn.lockedMap[string(key)] = struct{}{}
	}
	trace_util_0.Count(_txn_00000, 95)
	txn.dirty = true
	txn.mu.Unlock()
	return nil
}

func (txn *tikvTxn) asyncPessimisticRollback(ctx context.Context, keys [][]byte) *sync.WaitGroup {
	trace_util_0.Count(_txn_00000, 115)
	// Clone a new committer for execute in background.
	committer := &twoPhaseCommitter{
		store:       txn.committer.store,
		connID:      txn.committer.connID,
		startTS:     txn.committer.startTS,
		forUpdateTS: txn.committer.forUpdateTS,
		primaryKey:  txn.committer.primaryKey,
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		trace_util_0.Count(_txn_00000, 117)
		err := committer.pessimisticRollbackKeys(NewBackoffer(ctx, pessimisticRollbackMaxBackoff), keys)
		if err != nil {
			trace_util_0.Count(_txn_00000, 119)
			logutil.Logger(ctx).Warn("[kv] pessimisticRollback failed.", zap.Error(err))
		}
		trace_util_0.Count(_txn_00000, 118)
		wg.Done()
	}()
	trace_util_0.Count(_txn_00000, 116)
	return wg
}

func hashInKeys(deadlockKeyHash uint64, keys [][]byte) bool {
	trace_util_0.Count(_txn_00000, 120)
	for _, key := range keys {
		trace_util_0.Count(_txn_00000, 122)
		if farm.Fingerprint64(key) == deadlockKeyHash {
			trace_util_0.Count(_txn_00000, 123)
			return true
		}
	}
	trace_util_0.Count(_txn_00000, 121)
	return false
}

func (txn *tikvTxn) IsReadOnly() bool {
	trace_util_0.Count(_txn_00000, 124)
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	trace_util_0.Count(_txn_00000, 125)
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	trace_util_0.Count(_txn_00000, 126)
	return txn.valid
}

func (txn *tikvTxn) Len() int {
	trace_util_0.Count(_txn_00000, 127)
	return txn.us.Len()
}

func (txn *tikvTxn) Size() int {
	trace_util_0.Count(_txn_00000, 128)
	return txn.us.Size()
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	trace_util_0.Count(_txn_00000, 129)
	return txn.us.GetMemBuffer()
}

var _txn_00000 = "store/tikv/txn.go"
