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
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ResolvedCacheSize is max number of cached txn status.
const ResolvedCacheSize = 2048

// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
const bigTxnThreshold = 16

var (
	tikvLockResolverCountWithBatchResolve             = metrics.TiKVLockResolverCounter.WithLabelValues("batch_resolve")
	tikvLockResolverCountWithExpired                  = metrics.TiKVLockResolverCounter.WithLabelValues("expired")
	tikvLockResolverCountWithNotExpired               = metrics.TiKVLockResolverCounter.WithLabelValues("not_expired")
	tikvLockResolverCountWithWaitExpired              = metrics.TiKVLockResolverCounter.WithLabelValues("wait_expired")
	tikvLockResolverCountWithResolve                  = metrics.TiKVLockResolverCounter.WithLabelValues("resolve")
	tikvLockResolverCountWithQueryTxnStatus           = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status")
	tikvLockResolverCountWithQueryTxnStatusCommitted  = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status_committed")
	tikvLockResolverCountWithQueryTxnStatusRolledBack = metrics.TiKVLockResolverCounter.WithLabelValues("query_txn_status_rolled_back")
	tikvLockResolverCountWithResolveLocks             = metrics.TiKVLockResolverCounter.WithLabelValues("query_resolve_locks")
	tikvLockResolverCountWithResolveLockLite          = metrics.TiKVLockResolverCounter.WithLabelValues("query_resolve_lock_lite")
)

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	store Storage
	mu    struct {
		sync.RWMutex
		// resolved caches resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]TxnStatus
		recentResolved *list.List
	}
}

func newLockResolver(store Storage) *LockResolver {
	trace_util_0.Count(_lock_resolver_00000, 0)
	r := &LockResolver{
		store: store,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.recentResolved = list.New()
	return r
}

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
func NewLockResolver(etcdAddrs []string, security config.Security) (*LockResolver, error) {
	trace_util_0.Count(_lock_resolver_00000, 1)
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 6)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_lock_resolver_00000, 2)
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 7)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_lock_resolver_00000, 3)
	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 8)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_lock_resolver_00000, 4)
	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), false)
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 9)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_lock_resolver_00000, 5)
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Commit or Rollback.
type TxnStatus uint64

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { trace_util_0.Count(_lock_resolver_00000, 10); return s > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { trace_util_0.Count(_lock_resolver_00000, 11); return uint64(s) }

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// TODO: Consider if it's appropriate.
var maxLockTTL uint64 = 120000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// Lock represents a lock from tikv server.
type Lock struct {
	Key     []byte
	Primary []byte
	TxnID   uint64
	TTL     uint64
	TxnSize uint64
}

func (l *Lock) String() string {
	trace_util_0.Count(_lock_resolver_00000, 12)
	return fmt.Sprintf("key: %s, primary: %s, txnStartTS: %d, ttl: %d", l.Key, l.Primary, l.TxnID, l.TTL)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	trace_util_0.Count(_lock_resolver_00000, 13)
	return &Lock{
		Key:     l.GetKey(),
		Primary: l.GetPrimaryLock(),
		TxnID:   l.GetLockVersion(),
		TTL:     l.GetLockTtl(),
		TxnSize: l.GetTxnSize(),
	}
}

func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	trace_util_0.Count(_lock_resolver_00000, 14)
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		trace_util_0.Count(_lock_resolver_00000, 16)
		return
	}
	trace_util_0.Count(_lock_resolver_00000, 15)
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		trace_util_0.Count(_lock_resolver_00000, 17)
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	trace_util_0.Count(_lock_resolver_00000, 18)
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// BatchResolveLocks resolve locks in a batch
func (lr *LockResolver) BatchResolveLocks(bo *Backoffer, locks []*Lock, loc RegionVerID) (bool, error) {
	trace_util_0.Count(_lock_resolver_00000, 19)
	if len(locks) == 0 {
		trace_util_0.Count(_lock_resolver_00000, 30)
		return true, nil
	}

	trace_util_0.Count(_lock_resolver_00000, 20)
	tikvLockResolverCountWithBatchResolve.Inc()

	var expiredLocks []*Lock
	for _, l := range locks {
		trace_util_0.Count(_lock_resolver_00000, 31)
		if lr.store.GetOracle().IsExpired(l.TxnID, l.TTL) {
			trace_util_0.Count(_lock_resolver_00000, 32)
			tikvLockResolverCountWithExpired.Inc()
			expiredLocks = append(expiredLocks, l)
		} else {
			trace_util_0.Count(_lock_resolver_00000, 33)
			{
				tikvLockResolverCountWithNotExpired.Inc()
			}
		}
	}
	trace_util_0.Count(_lock_resolver_00000, 21)
	if len(expiredLocks) != len(locks) {
		trace_util_0.Count(_lock_resolver_00000, 34)
		logutil.Logger(context.Background()).Error("BatchResolveLocks: maybe safe point is wrong!",
			zap.Int("get locks", len(locks)),
			zap.Int("expired locks", len(expiredLocks)))
		return false, nil
	}

	trace_util_0.Count(_lock_resolver_00000, 22)
	startTime := time.Now()
	txnInfos := make(map[uint64]uint64)
	for _, l := range expiredLocks {
		trace_util_0.Count(_lock_resolver_00000, 35)
		if _, ok := txnInfos[l.TxnID]; ok {
			trace_util_0.Count(_lock_resolver_00000, 38)
			continue
		}

		trace_util_0.Count(_lock_resolver_00000, 36)
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 39)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 37)
		txnInfos[l.TxnID] = uint64(status)
	}
	trace_util_0.Count(_lock_resolver_00000, 23)
	logutil.Logger(context.Background()).Info("BatchResolveLocks: lookup txn status",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of txn", len(txnInfos)))

	listTxnInfos := make([]*kvrpcpb.TxnInfo, 0, len(txnInfos))
	for txnID, status := range txnInfos {
		trace_util_0.Count(_lock_resolver_00000, 40)
		listTxnInfos = append(listTxnInfos, &kvrpcpb.TxnInfo{
			Txn:    txnID,
			Status: status,
		})
	}

	trace_util_0.Count(_lock_resolver_00000, 24)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdResolveLock,
		ResolveLock: &kvrpcpb.ResolveLockRequest{
			TxnInfos: listTxnInfos,
		},
	}
	startTime = time.Now()
	resp, err := lr.store.SendReq(bo, req, loc, readTimeoutShort)
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 41)
		return false, errors.Trace(err)
	}

	trace_util_0.Count(_lock_resolver_00000, 25)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_lock_resolver_00000, 42)
		return false, errors.Trace(err)
	}

	trace_util_0.Count(_lock_resolver_00000, 26)
	if regionErr != nil {
		trace_util_0.Count(_lock_resolver_00000, 43)
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 45)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 44)
		return false, nil
	}

	trace_util_0.Count(_lock_resolver_00000, 27)
	cmdResp := resp.ResolveLock
	if cmdResp == nil {
		trace_util_0.Count(_lock_resolver_00000, 46)
		return false, errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_lock_resolver_00000, 28)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		trace_util_0.Count(_lock_resolver_00000, 47)
		return false, errors.Errorf("unexpected resolve err: %s", keyErr)
	}

	trace_util_0.Count(_lock_resolver_00000, 29)
	logutil.Logger(context.Background()).Info("BatchResolveLocks: resolve locks in a batch",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of locks", len(expiredLocks)))
	return true, nil
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
// 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
//    old are considered orphan locks and will be handled later. If all locks
//    are expired then all locks will be resolved so the returned `ok` will be
//    true, otherwise caller should sleep a while before retry.
// 2) For each lock, query the primary key to get txn(which left the lock)'s
//    commit status.
// 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
//    the same transaction.
func (lr *LockResolver) ResolveLocks(bo *Backoffer, locks []*Lock) (msBeforeTxnExpired int64, err error) {
	trace_util_0.Count(_lock_resolver_00000, 48)
	if len(locks) == 0 {
		trace_util_0.Count(_lock_resolver_00000, 53)
		return
	}

	trace_util_0.Count(_lock_resolver_00000, 49)
	tikvLockResolverCountWithResolve.Inc()

	var expiredLocks []*Lock
	for _, l := range locks {
		trace_util_0.Count(_lock_resolver_00000, 54)
		msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, l.TTL)
		if msBeforeLockExpired <= 0 {
			trace_util_0.Count(_lock_resolver_00000, 55)
			tikvLockResolverCountWithExpired.Inc()
			expiredLocks = append(expiredLocks, l)
		} else {
			trace_util_0.Count(_lock_resolver_00000, 56)
			{
				if msBeforeTxnExpired == 0 || msBeforeLockExpired < msBeforeTxnExpired {
					trace_util_0.Count(_lock_resolver_00000, 58)
					msBeforeTxnExpired = msBeforeLockExpired
				}
				trace_util_0.Count(_lock_resolver_00000, 57)
				tikvLockResolverCountWithNotExpired.Inc()
			}
		}
	}
	trace_util_0.Count(_lock_resolver_00000, 50)
	if len(expiredLocks) == 0 {
		trace_util_0.Count(_lock_resolver_00000, 59)
		if msBeforeTxnExpired > 0 {
			trace_util_0.Count(_lock_resolver_00000, 61)
			tikvLockResolverCountWithWaitExpired.Inc()
		}
		trace_util_0.Count(_lock_resolver_00000, 60)
		return
	}

	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	trace_util_0.Count(_lock_resolver_00000, 51)
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	for _, l := range expiredLocks {
		trace_util_0.Count(_lock_resolver_00000, 62)
		var status TxnStatus
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 65)
			msBeforeTxnExpired = 0
			err = errors.Trace(err)
			return
		}

		trace_util_0.Count(_lock_resolver_00000, 63)
		cleanRegions, exists := cleanTxns[l.TxnID]
		if !exists {
			trace_util_0.Count(_lock_resolver_00000, 66)
			cleanRegions = make(map[RegionVerID]struct{})
			cleanTxns[l.TxnID] = cleanRegions
		}

		trace_util_0.Count(_lock_resolver_00000, 64)
		err = lr.resolveLock(bo, l, status, cleanRegions)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 67)
			msBeforeTxnExpired = 0
			err = errors.Trace(err)
			return
		}
	}
	trace_util_0.Count(_lock_resolver_00000, 52)
	return
}

// GetTxnStatus queries tikv-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
func (lr *LockResolver) GetTxnStatus(txnID uint64, primary []byte) (TxnStatus, error) {
	trace_util_0.Count(_lock_resolver_00000, 68)
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	status, err := lr.getTxnStatus(bo, txnID, primary)
	return status, errors.Trace(err)
}

func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte) (TxnStatus, error) {
	trace_util_0.Count(_lock_resolver_00000, 69)
	if s, ok := lr.getResolved(txnID); ok {
		trace_util_0.Count(_lock_resolver_00000, 71)
		return s, nil
	}

	trace_util_0.Count(_lock_resolver_00000, 70)
	tikvLockResolverCountWithQueryTxnStatus.Inc()

	var status TxnStatus
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdCleanup,
		Cleanup: &kvrpcpb.CleanupRequest{
			Key:          primary,
			StartVersion: txnID,
		},
	}
	for {
		trace_util_0.Count(_lock_resolver_00000, 72)
		loc, err := lr.store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 80)
			return status, errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 73)
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 81)
			return status, errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 74)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 82)
			return status, errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 75)
		if regionErr != nil {
			trace_util_0.Count(_lock_resolver_00000, 83)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_lock_resolver_00000, 85)
				return status, errors.Trace(err)
			}
			trace_util_0.Count(_lock_resolver_00000, 84)
			continue
		}
		trace_util_0.Count(_lock_resolver_00000, 76)
		cmdResp := resp.Cleanup
		if cmdResp == nil {
			trace_util_0.Count(_lock_resolver_00000, 86)
			return status, errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_lock_resolver_00000, 77)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			trace_util_0.Count(_lock_resolver_00000, 87)
			err = errors.Errorf("unexpected cleanup err: %s, tid: %v", keyErr, txnID)
			logutil.Logger(context.Background()).Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
		trace_util_0.Count(_lock_resolver_00000, 78)
		if cmdResp.CommitVersion != 0 {
			trace_util_0.Count(_lock_resolver_00000, 88)
			status = TxnStatus(cmdResp.GetCommitVersion())
			tikvLockResolverCountWithQueryTxnStatusCommitted.Inc()
		} else {
			trace_util_0.Count(_lock_resolver_00000, 89)
			{
				tikvLockResolverCountWithQueryTxnStatusRolledBack.Inc()
			}
		}
		trace_util_0.Count(_lock_resolver_00000, 79)
		lr.saveResolved(txnID, status)
		return status, nil
	}
}

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, cleanRegions map[RegionVerID]struct{}) error {
	trace_util_0.Count(_lock_resolver_00000, 90)
	tikvLockResolverCountWithResolveLocks.Inc()
	cleanWholeRegion := l.TxnSize >= bigTxnThreshold
	for {
		trace_util_0.Count(_lock_resolver_00000, 91)
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 102)
			return errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 92)
		if _, ok := cleanRegions[loc.Region]; ok {
			trace_util_0.Count(_lock_resolver_00000, 103)
			return nil
		}
		trace_util_0.Count(_lock_resolver_00000, 93)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdResolveLock,
			ResolveLock: &kvrpcpb.ResolveLockRequest{
				StartVersion: l.TxnID,
			},
		}
		if status.IsCommitted() {
			trace_util_0.Count(_lock_resolver_00000, 104)
			req.ResolveLock.CommitVersion = status.CommitTS()
		}
		trace_util_0.Count(_lock_resolver_00000, 94)
		if l.TxnSize < bigTxnThreshold {
			trace_util_0.Count(_lock_resolver_00000, 105)
			// Only resolve specified keys when it is a small transaction,
			// prevent from scanning the whole region in this case.
			tikvLockResolverCountWithResolveLockLite.Inc()
			req.ResolveLock.Keys = [][]byte{l.Key}
		}
		trace_util_0.Count(_lock_resolver_00000, 95)
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 106)
			return errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 96)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_lock_resolver_00000, 107)
			return errors.Trace(err)
		}
		trace_util_0.Count(_lock_resolver_00000, 97)
		if regionErr != nil {
			trace_util_0.Count(_lock_resolver_00000, 108)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_lock_resolver_00000, 110)
				return errors.Trace(err)
			}
			trace_util_0.Count(_lock_resolver_00000, 109)
			continue
		}
		trace_util_0.Count(_lock_resolver_00000, 98)
		cmdResp := resp.ResolveLock
		if cmdResp == nil {
			trace_util_0.Count(_lock_resolver_00000, 111)
			return errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_lock_resolver_00000, 99)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			trace_util_0.Count(_lock_resolver_00000, 112)
			err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
			logutil.Logger(context.Background()).Error("resolveLock error", zap.Error(err))
			return err
		}
		trace_util_0.Count(_lock_resolver_00000, 100)
		if cleanWholeRegion {
			trace_util_0.Count(_lock_resolver_00000, 113)
			cleanRegions[loc.Region] = struct{}{}
		}
		trace_util_0.Count(_lock_resolver_00000, 101)
		return nil
	}
}

var _lock_resolver_00000 = "store/tikv/lock_resolver.go"
