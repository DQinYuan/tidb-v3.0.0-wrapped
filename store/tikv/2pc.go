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
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

type twoPhaseCommitAction int

const (
	actionPrewrite twoPhaseCommitAction = 1 + iota
	actionCommit
	actionCleanup
	actionPessimisticLock
	actionPessimisticRollback
)

var (
	tikvSecondaryLockCleanupFailureCounterCommit   = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit")
	tikvSecondaryLockCleanupFailureCounterRollback = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")
)

// Global variable set by config file.
var (
	PessimisticLockTTL uint64
)

func (ca twoPhaseCommitAction) String() string {
	trace_util_0.Count(_2pc_00000, 0)
	switch ca {
	case actionPrewrite:
		trace_util_0.Count(_2pc_00000, 2)
		return "prewrite"
	case actionCommit:
		trace_util_0.Count(_2pc_00000, 3)
		return "commit"
	case actionCleanup:
		trace_util_0.Count(_2pc_00000, 4)
		return "cleanup"
	case actionPessimisticLock:
		trace_util_0.Count(_2pc_00000, 5)
		return "pessimistic_lock"
	case actionPessimisticRollback:
		trace_util_0.Count(_2pc_00000, 6)
		return "pessimistic_rollback"
	}
	trace_util_0.Count(_2pc_00000, 1)
	return "unknown"
}

// MetricsTag returns detail tag for metrics.
func (ca twoPhaseCommitAction) MetricsTag() string {
	trace_util_0.Count(_2pc_00000, 7)
	return "2pc_" + ca.String()
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*mutationEx
	lockTTL   uint64
	commitTS  uint64
	mu        struct {
		sync.RWMutex
		committed       bool
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
	}
	priority pb.CommandPri
	syncLog  bool
	connID   uint64 // connID is used for log.
	cleanWg  sync.WaitGroup
	// maxTxnTimeUse represents max time a Txn may use (in ms) from its startTS to commitTS.
	// We use it to guarantee GC worker will not influence any active txn. The value
	// should be less than GC life time.
	maxTxnTimeUse uint64
	detail        *execdetails.CommitDetails
	// For pessimistic transaction
	isPessimistic bool
	primaryKey    []byte
	forUpdateTS   uint64
	isFirstLock   bool
}

type mutationEx struct {
	pb.Mutation
	asserted          bool
	isPessimisticLock bool
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	trace_util_0.Count(_2pc_00000, 8)
	return &twoPhaseCommitter{
		store:   txn.store,
		txn:     txn,
		startTS: txn.StartTS(),
		connID:  connID,
	}, nil
}

func (c *twoPhaseCommitter) initKeysAndMutations() error {
	trace_util_0.Count(_2pc_00000, 9)
	var (
		keys    [][]byte
		size    int
		putCnt  int
		delCnt  int
		lockCnt int
	)
	mutations := make(map[string]*mutationEx)
	txn := c.txn
	c.isPessimistic = txn.IsPessimistic()
	if c.isPessimistic && len(c.primaryKey) > 0 {
		trace_util_0.Count(_2pc_00000, 19)
		keys = append(keys, c.primaryKey)
		mutations[string(c.primaryKey)] = &mutationEx{
			Mutation: pb.Mutation{
				Op:  pb.Op_Lock,
				Key: c.primaryKey,
			},
			isPessimisticLock: true,
		}
	}
	trace_util_0.Count(_2pc_00000, 10)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		trace_util_0.Count(_2pc_00000, 20)
		if len(v) > 0 {
			trace_util_0.Count(_2pc_00000, 24)
			op := pb.Op_Put
			if c := txn.us.LookupConditionPair(k); c != nil && c.ShouldNotExist() {
				trace_util_0.Count(_2pc_00000, 26)
				op = pb.Op_Insert
			}
			trace_util_0.Count(_2pc_00000, 25)
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:    op,
					Key:   k,
					Value: v,
				},
			}
			putCnt++
		} else {
			trace_util_0.Count(_2pc_00000, 27)
			{
				mutations[string(k)] = &mutationEx{
					Mutation: pb.Mutation{
						Op:  pb.Op_Del,
						Key: k,
					},
				}
				delCnt++
			}
		}
		trace_util_0.Count(_2pc_00000, 21)
		if c.isPessimistic {
			trace_util_0.Count(_2pc_00000, 28)
			if !bytes.Equal(k, c.primaryKey) {
				trace_util_0.Count(_2pc_00000, 29)
				keys = append(keys, k)
			}
		} else {
			trace_util_0.Count(_2pc_00000, 30)
			{
				keys = append(keys, k)
			}
		}
		trace_util_0.Count(_2pc_00000, 22)
		entrySize := len(k) + len(v)
		if entrySize > kv.TxnEntrySizeLimit {
			trace_util_0.Count(_2pc_00000, 31)
			return kv.ErrEntryTooLarge.GenWithStackByArgs(kv.TxnEntrySizeLimit, entrySize)
		}
		trace_util_0.Count(_2pc_00000, 23)
		size += entrySize
		return nil
	})
	trace_util_0.Count(_2pc_00000, 11)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 32)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 12)
	for _, lockKey := range txn.lockKeys {
		trace_util_0.Count(_2pc_00000, 33)
		muEx, ok := mutations[string(lockKey)]
		if !ok {
			trace_util_0.Count(_2pc_00000, 34)
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
				isPessimisticLock: c.isPessimistic,
			}
			lockCnt++
			keys = append(keys, lockKey)
			size += len(lockKey)
		} else {
			trace_util_0.Count(_2pc_00000, 35)
			{
				muEx.isPessimisticLock = c.isPessimistic
			}
		}
	}
	trace_util_0.Count(_2pc_00000, 13)
	if len(keys) == 0 {
		trace_util_0.Count(_2pc_00000, 36)
		return nil
	}

	trace_util_0.Count(_2pc_00000, 14)
	for _, pair := range txn.assertions {
		trace_util_0.Count(_2pc_00000, 37)
		mutation, ok := mutations[string(pair.key)]
		if !ok {
			trace_util_0.Count(_2pc_00000, 41)
			// It's possible when a transaction inserted a key then deleted it later.
			continue
		}
		// Only apply the first assertion!
		trace_util_0.Count(_2pc_00000, 38)
		if mutation.asserted {
			trace_util_0.Count(_2pc_00000, 42)
			continue
		}
		trace_util_0.Count(_2pc_00000, 39)
		switch pair.assertion {
		case kv.Exist:
			trace_util_0.Count(_2pc_00000, 43)
			mutation.Assertion = pb.Assertion_Exist
		case kv.NotExist:
			trace_util_0.Count(_2pc_00000, 44)
			mutation.Assertion = pb.Assertion_NotExist
		default:
			trace_util_0.Count(_2pc_00000, 45)
			mutation.Assertion = pb.Assertion_None
		}
		trace_util_0.Count(_2pc_00000, 40)
		mutation.asserted = true
	}

	trace_util_0.Count(_2pc_00000, 15)
	entrylimit := atomic.LoadUint64(&kv.TxnEntryCountLimit)
	if len(keys) > int(entrylimit) || size > kv.TxnTotalSizeLimit {
		trace_util_0.Count(_2pc_00000, 46)
		return kv.ErrTxnTooLarge
	}
	trace_util_0.Count(_2pc_00000, 16)
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		trace_util_0.Count(_2pc_00000, 47)
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.Logger(context.Background()).Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Convert from sec to ms
	trace_util_0.Count(_2pc_00000, 17)
	maxTxnTimeUse := uint64(config.GetGlobalConfig().TiKVClient.MaxTxnTimeUse) * 1000

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		trace_util_0.Count(_2pc_00000, 48)
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.Logger(context.Background()).Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	trace_util_0.Count(_2pc_00000, 18)
	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: len(keys)}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.maxTxnTimeUse = maxTxnTimeUse
	c.keys = keys
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	c.detail = commitDetail
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	trace_util_0.Count(_2pc_00000, 49)
	if len(c.primaryKey) == 0 {
		trace_util_0.Count(_2pc_00000, 51)
		return c.keys[0]
	}
	trace_util_0.Count(_2pc_00000, 50)
	return c.primaryKey
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	trace_util_0.Count(_2pc_00000, 52)
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		trace_util_0.Count(_2pc_00000, 54)
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			trace_util_0.Count(_2pc_00000, 56)
			lockTTL = defaultLockTTL
		}
		trace_util_0.Count(_2pc_00000, 55)
		if lockTTL > maxLockTTL {
			trace_util_0.Count(_2pc_00000, 57)
			lockTTL = maxLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	trace_util_0.Count(_2pc_00000, 53)
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

// doActionOnKeys groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doActionOnKeys(bo *Backoffer, action twoPhaseCommitAction, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 58)
	if len(keys) == 0 {
		trace_util_0.Count(_2pc_00000, 65)
		return nil
	}
	trace_util_0.Count(_2pc_00000, 59)
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 66)
		return errors.Trace(err)
	}

	trace_util_0.Count(_2pc_00000, 60)
	metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(action.MetricsTag()).Observe(float64(len(groups)))

	var batches []batchKeys
	var sizeFunc = c.keySize
	if action == actionPrewrite {
		trace_util_0.Count(_2pc_00000, 67)
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.detail.PrewriteRegionNum, int32(len(groups)))
	}
	// Make sure the group that contains primary key goes first.
	trace_util_0.Count(_2pc_00000, 61)
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		trace_util_0.Count(_2pc_00000, 68)
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	trace_util_0.Count(_2pc_00000, 62)
	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	if firstIsPrimary && (action == actionCommit || action == actionCleanup) {
		trace_util_0.Count(_2pc_00000, 69)
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			trace_util_0.Count(_2pc_00000, 71)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 70)
		batches = batches[1:]
	}
	trace_util_0.Count(_2pc_00000, 63)
	if action == actionCommit {
		trace_util_0.Count(_2pc_00000, 72)
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potencial data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackoffer(context.Background(), CommitMaxBackoff)
		go func() {
			trace_util_0.Count(_2pc_00000, 73)
			e := c.doActionOnBatches(secondaryBo, action, batches)
			if e != nil {
				trace_util_0.Count(_2pc_00000, 74)
				logutil.Logger(context.Background()).Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
				tikvSecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
	} else {
		trace_util_0.Count(_2pc_00000, 75)
		{
			err = c.doActionOnBatches(bo, action, batches)
		}
	}
	trace_util_0.Count(_2pc_00000, 64)
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchKeys) error {
	trace_util_0.Count(_2pc_00000, 76)
	if len(batches) == 0 {
		trace_util_0.Count(_2pc_00000, 83)
		return nil
	}
	trace_util_0.Count(_2pc_00000, 77)
	var singleBatchActionFunc func(bo *Backoffer, batch batchKeys) error
	switch action {
	case actionPrewrite:
		trace_util_0.Count(_2pc_00000, 84)
		singleBatchActionFunc = c.prewriteSingleBatch
	case actionCommit:
		trace_util_0.Count(_2pc_00000, 85)
		singleBatchActionFunc = c.commitSingleBatch
	case actionCleanup:
		trace_util_0.Count(_2pc_00000, 86)
		singleBatchActionFunc = c.cleanupSingleBatch
	case actionPessimisticLock:
		trace_util_0.Count(_2pc_00000, 87)
		singleBatchActionFunc = c.pessimisticLockSingleBatch
	case actionPessimisticRollback:
		trace_util_0.Count(_2pc_00000, 88)
		singleBatchActionFunc = c.pessimisticRollbackSingleBatch
	}
	trace_util_0.Count(_2pc_00000, 78)
	if len(batches) == 1 {
		trace_util_0.Count(_2pc_00000, 89)
		e := singleBatchActionFunc(bo, batches[0])
		if e != nil {
			trace_util_0.Count(_2pc_00000, 91)
			logutil.Logger(context.Background()).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
		}
		trace_util_0.Count(_2pc_00000, 90)
		return errors.Trace(e)
	}

	// For prewrite, stop sending other requests after receiving first error.
	trace_util_0.Count(_2pc_00000, 79)
	backoffer := bo
	var cancel context.CancelFunc
	if action == actionPrewrite {
		trace_util_0.Count(_2pc_00000, 92)
		backoffer, cancel = bo.Fork()
		defer cancel()
	}

	// Concurrently do the work for each batch.
	trace_util_0.Count(_2pc_00000, 80)
	ch := make(chan error, len(batches))
	for _, batch1 := range batches {
		trace_util_0.Count(_2pc_00000, 93)

		batch := batch1
		go func() {
			trace_util_0.Count(_2pc_00000, 94)
			if action == actionCommit {
				trace_util_0.Count(_2pc_00000, 95)
				// Because the secondary batches of the commit actions are implemented to be
				// committed asynchronously in background goroutines, we should not
				// fork a child context and call cancel() while the foreground goroutine exits.
				// Otherwise the background goroutines will be canceled execeptionally.
				// Here we makes a new clone of the original backoffer for this goroutine
				// exclusively to avoid the data race when using the same backoffer
				// in concurrent goroutines.
				singleBatchBackoffer := backoffer.Clone()
				ch <- singleBatchActionFunc(singleBatchBackoffer, batch)
			} else {
				trace_util_0.Count(_2pc_00000, 96)
				{
					singleBatchBackoffer, singleBatchCancel := backoffer.Fork()
					defer singleBatchCancel()
					ch <- singleBatchActionFunc(singleBatchBackoffer, batch)
				}
			}
		}()
	}
	trace_util_0.Count(_2pc_00000, 81)
	var err error
	for i := 0; i < len(batches); i++ {
		trace_util_0.Count(_2pc_00000, 97)
		if e := <-ch; e != nil {
			trace_util_0.Count(_2pc_00000, 98)
			logutil.Logger(context.Background()).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				trace_util_0.Count(_2pc_00000, 100)
				logutil.Logger(context.Background()).Debug("2PC doActionOnBatches to cancel other actions",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Uint64("txnStartTS", c.startTS))
				cancel()
			}
			trace_util_0.Count(_2pc_00000, 99)
			if err == nil {
				trace_util_0.Count(_2pc_00000, 101)
				err = e
			}
		}
	}
	trace_util_0.Count(_2pc_00000, 82)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key []byte) int {
	trace_util_0.Count(_2pc_00000, 102)
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		trace_util_0.Count(_2pc_00000, 104)
		size += len(mutation.Value)
	}
	trace_util_0.Count(_2pc_00000, 103)
	return size
}

func (c *twoPhaseCommitter) keySize(key []byte) int {
	trace_util_0.Count(_2pc_00000, 105)
	return len(key)
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchKeys) *tikvrpc.Request {
	trace_util_0.Count(_2pc_00000, 106)
	mutations := make([]*pb.Mutation, len(batch.keys))
	var isPessimisticLock []bool
	if c.isPessimistic {
		trace_util_0.Count(_2pc_00000, 109)
		isPessimisticLock = make([]bool, len(mutations))
	}
	trace_util_0.Count(_2pc_00000, 107)
	for i, k := range batch.keys {
		trace_util_0.Count(_2pc_00000, 110)
		tmp := c.mutations[string(k)]
		mutations[i] = &tmp.Mutation
		if tmp.isPessimisticLock {
			trace_util_0.Count(_2pc_00000, 111)
			isPessimisticLock[i] = true
		}
	}
	trace_util_0.Count(_2pc_00000, 108)
	return &tikvrpc.Request{
		Type: tikvrpc.CmdPrewrite,
		Prewrite: &pb.PrewriteRequest{
			Mutations:         mutations,
			PrimaryLock:       c.primary(),
			StartVersion:      c.startTS,
			LockTtl:           c.lockTTL,
			IsPessimisticLock: isPessimisticLock,
			ForUpdateTs:       c.forUpdateTS,
			TxnSize:           uint64(len(batch.keys)),
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
}

func (c *twoPhaseCommitter) prewriteSingleBatch(bo *Backoffer, batch batchKeys) error {
	trace_util_0.Count(_2pc_00000, 112)
	req := c.buildPrewriteRequest(batch)
	for {
		trace_util_0.Count(_2pc_00000, 113)
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 121)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 114)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_2pc_00000, 122)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 115)
		if regionErr != nil {
			trace_util_0.Count(_2pc_00000, 123)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_2pc_00000, 125)
				return errors.Trace(err)
			}
			trace_util_0.Count(_2pc_00000, 124)
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 116)
		prewriteResp := resp.Prewrite
		if prewriteResp == nil {
			trace_util_0.Count(_2pc_00000, 126)
			return errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_2pc_00000, 117)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			trace_util_0.Count(_2pc_00000, 127)
			return nil
		}
		trace_util_0.Count(_2pc_00000, 118)
		var locks []*Lock
		for _, keyErr := range keyErrs {
			trace_util_0.Count(_2pc_00000, 128)
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				trace_util_0.Count(_2pc_00000, 132)
				key := alreadyExist.GetKey()
				conditionPair := c.txn.us.LookupConditionPair(key)
				if conditionPair == nil {
					trace_util_0.Count(_2pc_00000, 134)
					return errors.Errorf("conn%d, conditionPair for key:%s should not be nil", c.connID, key)
				}
				trace_util_0.Count(_2pc_00000, 133)
				logutil.Logger(context.Background()).Debug("key already exists",
					zap.Uint64("conn", c.connID),
					zap.Binary("key", key))
				return errors.Trace(conditionPair.Err())
			}

			// Extract lock from key error
			trace_util_0.Count(_2pc_00000, 129)
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				trace_util_0.Count(_2pc_00000, 135)
				return errors.Trace(err1)
			}
			trace_util_0.Count(_2pc_00000, 130)
			if !c.isPessimistic && c.lockTTL < lock.TTL && lock.TTL >= uint64(config.MinPessimisticTTL/time.Millisecond) {
				trace_util_0.Count(_2pc_00000, 136)
				// An optimistic prewrite meets a pessimistic or large transaction lock.
				// If we wait for the lock, other written optimistic locks would block reads for long time.
				// And it is very unlikely this transaction would succeed after wait for the long TTL lock.
				// Return write conflict error to cleanup locks.
				return newWriteConflictError(&pb.WriteConflict{
					StartTs:          c.startTS,
					ConflictTs:       lock.TxnID,
					ConflictCommitTs: 0,
					Key:              lock.Key,
					Primary:          lock.Primary,
				})
			}
			trace_util_0.Count(_2pc_00000, 131)
			logutil.Logger(context.Background()).Debug("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}
		trace_util_0.Count(_2pc_00000, 119)
		start := time.Now()
		msBeforeExpired, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 137)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 120)
		atomic.AddInt64(&c.detail.ResolveLockTime, int64(time.Since(start)))
		if msBeforeExpired > 0 {
			trace_util_0.Count(_2pc_00000, 138)
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				trace_util_0.Count(_2pc_00000, 139)
				return errors.Trace(err)
			}
		}
	}
}

func (c *twoPhaseCommitter) pessimisticLockSingleBatch(bo *Backoffer, batch batchKeys) error {
	trace_util_0.Count(_2pc_00000, 140)
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		trace_util_0.Count(_2pc_00000, 142)
		mut := &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: k,
		}
		conditionPair := c.txn.us.LookupConditionPair(k)
		if conditionPair != nil && conditionPair.ShouldNotExist() {
			trace_util_0.Count(_2pc_00000, 144)
			mut.Assertion = pb.Assertion_NotExist
		}
		trace_util_0.Count(_2pc_00000, 143)
		mutations[i] = mut
	}

	trace_util_0.Count(_2pc_00000, 141)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdPessimisticLock,
		PessimisticLock: &pb.PessimisticLockRequest{
			Mutations:    mutations,
			PrimaryLock:  c.primary(),
			StartVersion: c.startTS,
			ForUpdateTs:  c.forUpdateTS,
			LockTtl:      PessimisticLockTTL,
			IsFirstLock:  c.isFirstLock,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
	for {
		trace_util_0.Count(_2pc_00000, 145)
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 152)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 146)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_2pc_00000, 153)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 147)
		if regionErr != nil {
			trace_util_0.Count(_2pc_00000, 154)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_2pc_00000, 156)
				return errors.Trace(err)
			}
			trace_util_0.Count(_2pc_00000, 155)
			err = c.pessimisticLockKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 148)
		lockResp := resp.PessimisticLock
		if lockResp == nil {
			trace_util_0.Count(_2pc_00000, 157)
			return errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_2pc_00000, 149)
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			trace_util_0.Count(_2pc_00000, 158)
			return nil
		}
		trace_util_0.Count(_2pc_00000, 150)
		var locks []*Lock
		for _, keyErr := range keyErrs {
			trace_util_0.Count(_2pc_00000, 159)
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				trace_util_0.Count(_2pc_00000, 163)
				key := alreadyExist.GetKey()
				conditionPair := c.txn.us.LookupConditionPair(key)
				if conditionPair == nil {
					trace_util_0.Count(_2pc_00000, 165)
					panic(fmt.Sprintf("con:%d, conditionPair for key:%s should not be nil", c.connID, key))
				}
				trace_util_0.Count(_2pc_00000, 164)
				return errors.Trace(conditionPair.Err())
			}
			trace_util_0.Count(_2pc_00000, 160)
			if deadlock := keyErr.Deadlock; deadlock != nil {
				trace_util_0.Count(_2pc_00000, 166)
				return &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract lock from key error
			trace_util_0.Count(_2pc_00000, 161)
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				trace_util_0.Count(_2pc_00000, 167)
				return errors.Trace(err1)
			}
			trace_util_0.Count(_2pc_00000, 162)
			locks = append(locks, lock)
		}
		trace_util_0.Count(_2pc_00000, 151)
		_, err = c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 168)
			return errors.Trace(err)
		}
		// Because we already waited on tikv, no need to Backoff here.
	}
}

func (c *twoPhaseCommitter) pessimisticRollbackSingleBatch(bo *Backoffer, batch batchKeys) error {
	trace_util_0.Count(_2pc_00000, 169)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdPessimisticRollback,
		PessimisticRollback: &pb.PessimisticRollbackRequest{
			StartVersion: c.startTS,
			ForUpdateTs:  c.forUpdateTS,
			Keys:         batch.keys,
		},
	}
	for {
		trace_util_0.Count(_2pc_00000, 170)
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 174)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 171)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_2pc_00000, 175)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 172)
		if regionErr != nil {
			trace_util_0.Count(_2pc_00000, 176)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_2pc_00000, 178)
				return errors.Trace(err)
			}
			trace_util_0.Count(_2pc_00000, 177)
			err = c.pessimisticRollbackKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 173)
		return nil
	}
}

func getTxnPriority(txn *tikvTxn) pb.CommandPri {
	trace_util_0.Count(_2pc_00000, 179)
	if pri := txn.us.GetOption(kv.Priority); pri != nil {
		trace_util_0.Count(_2pc_00000, 181)
		return kvPriorityToCommandPri(pri.(int))
	}
	trace_util_0.Count(_2pc_00000, 180)
	return pb.CommandPri_Normal
}

func getTxnSyncLog(txn *tikvTxn) bool {
	trace_util_0.Count(_2pc_00000, 182)
	if syncOption := txn.us.GetOption(kv.SyncLog); syncOption != nil {
		trace_util_0.Count(_2pc_00000, 184)
		return syncOption.(bool)
	}
	trace_util_0.Count(_2pc_00000, 183)
	return false
}

func kvPriorityToCommandPri(pri int) pb.CommandPri {
	trace_util_0.Count(_2pc_00000, 185)
	switch pri {
	case kv.PriorityLow:
		trace_util_0.Count(_2pc_00000, 187)
		return pb.CommandPri_Low
	case kv.PriorityHigh:
		trace_util_0.Count(_2pc_00000, 188)
		return pb.CommandPri_High
	}
	trace_util_0.Count(_2pc_00000, 186)
	return pb.CommandPri_Normal
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	trace_util_0.Count(_2pc_00000, 189)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	trace_util_0.Count(_2pc_00000, 190)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

func (c *twoPhaseCommitter) commitSingleBatch(bo *Backoffer, batch batchKeys) error {
	trace_util_0.Count(_2pc_00000, 191)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdCommit,
		Commit: &pb.CommitRequest{
			StartVersion:  c.startTS,
			Keys:          batch.keys,
			CommitVersion: c.commitTS,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
	req.Context.Priority = c.priority

	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	isPrimary := bytes.Equal(batch.keys[0], c.primary())
	if isPrimary && sender.rpcError != nil {
		trace_util_0.Count(_2pc_00000, 199)
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	trace_util_0.Count(_2pc_00000, 192)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 200)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 193)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_2pc_00000, 201)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 194)
	if regionErr != nil {
		trace_util_0.Count(_2pc_00000, 202)
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			trace_util_0.Count(_2pc_00000, 204)
			return errors.Trace(err)
		}
		// re-split keys and commit again.
		trace_util_0.Count(_2pc_00000, 203)
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 195)
	commitResp := resp.Commit
	if commitResp == nil {
		trace_util_0.Count(_2pc_00000, 205)
		return errors.Trace(ErrBodyMissing)
	}
	// Here we can make sure tikv has processed the commit primary key request. So
	// we can clean undetermined error.
	trace_util_0.Count(_2pc_00000, 196)
	if isPrimary {
		trace_util_0.Count(_2pc_00000, 206)
		c.setUndeterminedErr(nil)
	}
	trace_util_0.Count(_2pc_00000, 197)
	if keyErr := commitResp.GetError(); keyErr != nil {
		trace_util_0.Count(_2pc_00000, 207)
		c.mu.RLock()
		defer c.mu.RUnlock()
		err = extractKeyErr(keyErr)
		if c.mu.committed {
			trace_util_0.Count(_2pc_00000, 209)
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			logutil.Logger(context.Background()).Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		trace_util_0.Count(_2pc_00000, 208)
		logutil.Logger(context.Background()).Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}

	trace_util_0.Count(_2pc_00000, 198)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (c *twoPhaseCommitter) cleanupSingleBatch(bo *Backoffer, batch batchKeys) error {
	trace_util_0.Count(_2pc_00000, 210)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdBatchRollback,
		BatchRollback: &pb.BatchRollbackRequest{
			Keys:         batch.keys,
			StartVersion: c.startTS,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
	resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 215)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 211)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_2pc_00000, 216)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 212)
	if regionErr != nil {
		trace_util_0.Count(_2pc_00000, 217)
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			trace_util_0.Count(_2pc_00000, 219)
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 218)
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 213)
	if keyErr := resp.BatchRollback.GetError(); keyErr != nil {
		trace_util_0.Count(_2pc_00000, 220)
		err = errors.Errorf("conn%d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.Logger(context.Background()).Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 214)
	return nil
}

func (c *twoPhaseCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 221)
	return c.doActionOnKeys(bo, actionPrewrite, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 222)
	return c.doActionOnKeys(bo, actionCommit, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 223)
	return c.doActionOnKeys(bo, actionCleanup, keys)
}

func (c *twoPhaseCommitter) pessimisticLockKeys(bo *Backoffer, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 224)
	return c.doActionOnKeys(bo, actionPessimisticLock, keys)
}

func (c *twoPhaseCommitter) pessimisticRollbackKeys(bo *Backoffer, keys [][]byte) error {
	trace_util_0.Count(_2pc_00000, 225)
	return c.doActionOnKeys(bo, actionPessimisticRollback, keys)
}

func (c *twoPhaseCommitter) executeAndWriteFinishBinlog(ctx context.Context) error {
	trace_util_0.Count(_2pc_00000, 226)
	err := c.execute(ctx)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 228)
		c.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		trace_util_0.Count(_2pc_00000, 229)
		{
			c.txn.commitTS = c.commitTS
			c.writeFinishBinlog(binlog.BinlogType_Commit, int64(c.commitTS))
		}
	}
	trace_util_0.Count(_2pc_00000, 227)
	return errors.Trace(err)
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) error {
	trace_util_0.Count(_2pc_00000, 230)
	defer func() {
		trace_util_0.Count(_2pc_00000, 240)
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			trace_util_0.Count(_2pc_00000, 241)
			c.cleanWg.Add(1)
			go func() {
				trace_util_0.Count(_2pc_00000, 242)
				cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
				err := c.cleanupKeys(NewBackoffer(cleanupKeysCtx, cleanupMaxBackoff).WithVars(c.txn.vars), c.keys)
				if err != nil {
					trace_util_0.Count(_2pc_00000, 244)
					tikvSecondaryLockCleanupFailureCounterRollback.Inc()
					logutil.Logger(ctx).Info("2PC cleanup failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
				} else {
					trace_util_0.Count(_2pc_00000, 245)
					{
						logutil.Logger(ctx).Info("2PC clean up done",
							zap.Uint64("txnStartTS", c.startTS))
					}
				}
				trace_util_0.Count(_2pc_00000, 243)
				c.cleanWg.Done()
			}()
		}
	}()

	trace_util_0.Count(_2pc_00000, 231)
	binlogChan := c.prewriteBinlog()
	prewriteBo := NewBackoffer(ctx, prewriteMaxBackoff).WithVars(c.txn.vars)
	start := time.Now()
	err := c.prewriteKeys(prewriteBo, c.keys)
	c.detail.PrewriteTime = time.Since(start)
	c.detail.TotalBackoffTime += time.Duration(prewriteBo.totalSleep) * time.Millisecond
	if binlogChan != nil {
		trace_util_0.Count(_2pc_00000, 246)
		binlogErr := <-binlogChan
		if binlogErr != nil {
			trace_util_0.Count(_2pc_00000, 247)
			return errors.Trace(binlogErr)
		}
	}
	trace_util_0.Count(_2pc_00000, 232)
	if err != nil {
		trace_util_0.Count(_2pc_00000, 248)
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	trace_util_0.Count(_2pc_00000, 233)
	start = time.Now()
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		trace_util_0.Count(_2pc_00000, 249)
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 234)
	c.detail.GetCommitTsTime = time.Since(start)

	// check commitTS
	if commitTS <= c.startTS {
		trace_util_0.Count(_2pc_00000, 250)
		err = errors.Errorf("conn%d Invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.Logger(context.Background()).Error("invalid transaction", zap.Error(err))
		return errors.Trace(err)
	}
	trace_util_0.Count(_2pc_00000, 235)
	c.commitTS = commitTS
	if err = c.checkSchemaValid(); err != nil {
		trace_util_0.Count(_2pc_00000, 251)
		return errors.Trace(err)
	}

	trace_util_0.Count(_2pc_00000, 236)
	failpoint.Inject("tmpMaxTxnTime", func(val failpoint.Value) {
		trace_util_0.Count(_2pc_00000, 252)
		if tmpMaxTxnTime := uint64(val.(int)); tmpMaxTxnTime > 0 {
			trace_util_0.Count(_2pc_00000, 253)
			c.maxTxnTimeUse = tmpMaxTxnTime
		}
	})

	trace_util_0.Count(_2pc_00000, 237)
	if c.store.oracle.IsExpired(c.startTS, c.maxTxnTimeUse) {
		trace_util_0.Count(_2pc_00000, 254)
		err = errors.Errorf("conn%d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	trace_util_0.Count(_2pc_00000, 238)
	start = time.Now()
	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	err = c.commitKeys(commitBo, c.keys)
	c.detail.CommitTime = time.Since(start)
	c.detail.TotalBackoffTime += time.Duration(commitBo.totalSleep) * time.Millisecond
	if err != nil {
		trace_util_0.Count(_2pc_00000, 255)
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			trace_util_0.Count(_2pc_00000, 258)
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		trace_util_0.Count(_2pc_00000, 256)
		if !c.mu.committed {
			trace_util_0.Count(_2pc_00000, 259)
			logutil.Logger(ctx).Debug("2PC failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		trace_util_0.Count(_2pc_00000, 257)
		logutil.Logger(ctx).Debug("2PC succeed with error",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	trace_util_0.Count(_2pc_00000, 239)
	return nil
}

type schemaLeaseChecker interface {
	Check(txnTS uint64) error
}

func (c *twoPhaseCommitter) checkSchemaValid() error {
	trace_util_0.Count(_2pc_00000, 260)
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if ok {
		trace_util_0.Count(_2pc_00000, 262)
		err := checker.Check(c.commitTS)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 263)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_2pc_00000, 261)
	return nil
}

func (c *twoPhaseCommitter) prewriteBinlog() chan error {
	trace_util_0.Count(_2pc_00000, 264)
	if !c.shouldWriteBinlog() {
		trace_util_0.Count(_2pc_00000, 267)
		return nil
	}
	trace_util_0.Count(_2pc_00000, 265)
	ch := make(chan error, 1)
	go func() {
		trace_util_0.Count(_2pc_00000, 268)
		binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			trace_util_0.Count(_2pc_00000, 270)
			bin.PrewriteKey = c.keys[0]
		}
		trace_util_0.Count(_2pc_00000, 269)
		err := binInfo.WriteBinlog(c.store.clusterID)
		ch <- errors.Trace(err)
	}()
	trace_util_0.Count(_2pc_00000, 266)
	return ch
}

func (c *twoPhaseCommitter) writeFinishBinlog(tp binlog.BinlogType, commitTS int64) {
	trace_util_0.Count(_2pc_00000, 271)
	if !c.shouldWriteBinlog() {
		trace_util_0.Count(_2pc_00000, 273)
		return
	}
	trace_util_0.Count(_2pc_00000, 272)
	binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
	binInfo.Data.Tp = tp
	binInfo.Data.CommitTs = commitTS
	binInfo.Data.PrewriteValue = nil
	go func() {
		trace_util_0.Count(_2pc_00000, 274)
		err := binInfo.WriteBinlog(c.store.clusterID)
		if err != nil {
			trace_util_0.Count(_2pc_00000, 275)
			logutil.Logger(context.Background()).Error("failed to write binlog",
				zap.Error(err))
		}
	}()
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	trace_util_0.Count(_2pc_00000, 276)
	return c.txn.us.GetOption(kv.BinlogInfo) != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

// batchKeys is a batch of keys in the same region.
type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchBySize appends keys to []batchKeys. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	trace_util_0.Count(_2pc_00000, 277)
	var start, end int
	for start = 0; start < len(keys); start = end {
		trace_util_0.Count(_2pc_00000, 279)
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			trace_util_0.Count(_2pc_00000, 281)
			size += sizeFn(keys[end])
		}
		trace_util_0.Count(_2pc_00000, 280)
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	trace_util_0.Count(_2pc_00000, 278)
	return b
}

var _2pc_00000 = "store/tikv/2pc.go"
