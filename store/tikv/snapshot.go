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

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
)

const (
	scanBatchSize = 256
	batchGetSize  = 5120
)

var (
	tikvTxnCmdCounterWithBatchGet          = metrics.TiKVTxnCmdCounter.WithLabelValues("batch_get")
	tikvTxnCmdHistogramWithBatchGet        = metrics.TiKVTxnCmdHistogram.WithLabelValues("batch_get")
	tikvTxnRegionsNumHistogramWithSnapshot = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("snapshot")
)

// tikvSnapshot implements the kv.Snapshot interface.
type tikvSnapshot struct {
	store        *tikvStore
	version      kv.Version
	priority     pb.CommandPri
	notFillCache bool
	syncLog      bool
	keyOnly      bool
	vars         *kv.Variables
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *tikvStore, ver kv.Version) *tikvSnapshot {
	trace_util_0.Count(_snapshot_00000, 0)
	return &tikvSnapshot{
		store:    store,
		version:  ver,
		priority: pb.CommandPri_Normal,
		vars:     kv.DefaultVars,
	}
}

func (s *tikvSnapshot) SetPriority(priority int) {
	trace_util_0.Count(_snapshot_00000, 1)
	s.priority = pb.CommandPri(priority)
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	trace_util_0.Count(_snapshot_00000, 2)
	m := make(map[string][]byte)
	if len(keys) == 0 {
		trace_util_0.Count(_snapshot_00000, 8)
		return m, nil
	}
	trace_util_0.Count(_snapshot_00000, 3)
	tikvTxnCmdCounterWithBatchGet.Inc()
	start := time.Now()
	defer func() {
		trace_util_0.Count(_snapshot_00000, 9)
		tikvTxnCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}()

	// We want [][]byte instead of []kv.Key, use some magic to save memory.
	trace_util_0.Count(_snapshot_00000, 4)
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))
	ctx := context.WithValue(context.Background(), txnStartKey, s.version.Ver)
	bo := NewBackoffer(ctx, batchGetMaxBackoff).WithVars(s.vars)

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	err := s.batchGetKeysByRegions(bo, bytesKeys, func(k, v []byte) {
		trace_util_0.Count(_snapshot_00000, 10)
		if len(v) == 0 {
			trace_util_0.Count(_snapshot_00000, 12)
			return
		}
		trace_util_0.Count(_snapshot_00000, 11)
		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	})
	trace_util_0.Count(_snapshot_00000, 5)
	if err != nil {
		trace_util_0.Count(_snapshot_00000, 13)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_snapshot_00000, 6)
	err = s.store.CheckVisibility(s.version.Ver)
	if err != nil {
		trace_util_0.Count(_snapshot_00000, 14)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_snapshot_00000, 7)
	return m, nil
}

func (s *tikvSnapshot) batchGetKeysByRegions(bo *Backoffer, keys [][]byte, collectF func(k, v []byte)) error {
	trace_util_0.Count(_snapshot_00000, 15)
	groups, _, err := s.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		trace_util_0.Count(_snapshot_00000, 22)
		return errors.Trace(err)
	}

	trace_util_0.Count(_snapshot_00000, 16)
	tikvTxnRegionsNumHistogramWithSnapshot.Observe(float64(len(groups)))

	var batches []batchKeys
	for id, g := range groups {
		trace_util_0.Count(_snapshot_00000, 23)
		batches = appendBatchBySize(batches, id, g, func([]byte) int { trace_util_0.Count(_snapshot_00000, 24); return 1 }, batchGetSize)
	}

	trace_util_0.Count(_snapshot_00000, 17)
	if len(batches) == 0 {
		trace_util_0.Count(_snapshot_00000, 25)
		return nil
	}
	trace_util_0.Count(_snapshot_00000, 18)
	if len(batches) == 1 {
		trace_util_0.Count(_snapshot_00000, 26)
		return errors.Trace(s.batchGetSingleRegion(bo, batches[0], collectF))
	}
	trace_util_0.Count(_snapshot_00000, 19)
	ch := make(chan error)
	for _, batch1 := range batches {
		trace_util_0.Count(_snapshot_00000, 27)
		batch := batch1
		go func() {
			trace_util_0.Count(_snapshot_00000, 28)
			backoffer, cancel := bo.Fork()
			defer cancel()
			ch <- s.batchGetSingleRegion(backoffer, batch, collectF)
		}()
	}
	trace_util_0.Count(_snapshot_00000, 20)
	for i := 0; i < len(batches); i++ {
		trace_util_0.Count(_snapshot_00000, 29)
		if e := <-ch; e != nil {
			trace_util_0.Count(_snapshot_00000, 30)
			logutil.Logger(context.Background()).Debug("snapshot batchGet failed",
				zap.Error(e),
				zap.Uint64("txnStartTS", s.version.Ver))
			err = e
		}
	}
	trace_util_0.Count(_snapshot_00000, 21)
	return errors.Trace(err)
}

func (s *tikvSnapshot) batchGetSingleRegion(bo *Backoffer, batch batchKeys, collectF func(k, v []byte)) error {
	trace_util_0.Count(_snapshot_00000, 31)
	sender := NewRegionRequestSender(s.store.regionCache, s.store.client)

	pending := batch.keys
	for {
		trace_util_0.Count(_snapshot_00000, 32)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdBatchGet,
			BatchGet: &pb.BatchGetRequest{
				Keys:    pending,
				Version: s.version.Ver,
			},
			Context: pb.Context{
				Priority:     s.priority,
				NotFillCache: s.notFillCache,
			},
		}
		resp, err := sender.SendReq(bo, req, batch.region, ReadTimeoutMedium)
		if err != nil {
			trace_util_0.Count(_snapshot_00000, 39)
			return errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 33)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_snapshot_00000, 40)
			return errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 34)
		if regionErr != nil {
			trace_util_0.Count(_snapshot_00000, 41)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 43)
				return errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 42)
			err = s.batchGetKeysByRegions(bo, pending, collectF)
			return errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 35)
		batchGetResp := resp.BatchGet
		if batchGetResp == nil {
			trace_util_0.Count(_snapshot_00000, 44)
			return errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_snapshot_00000, 36)
		var (
			lockedKeys [][]byte
			locks      []*Lock
		)
		for _, pair := range batchGetResp.Pairs {
			trace_util_0.Count(_snapshot_00000, 45)
			keyErr := pair.GetError()
			if keyErr == nil {
				trace_util_0.Count(_snapshot_00000, 48)
				collectF(pair.GetKey(), pair.GetValue())
				continue
			}
			trace_util_0.Count(_snapshot_00000, 46)
			lock, err := extractLockFromKeyErr(keyErr)
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 49)
				return errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 47)
			lockedKeys = append(lockedKeys, lock.Key)
			locks = append(locks, lock)
		}
		trace_util_0.Count(_snapshot_00000, 37)
		if len(lockedKeys) > 0 {
			trace_util_0.Count(_snapshot_00000, 50)
			msBeforeExpired, err := s.store.lockResolver.ResolveLocks(bo, locks)
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 53)
				return errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 51)
			if msBeforeExpired > 0 {
				trace_util_0.Count(_snapshot_00000, 54)
				err = bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.Errorf("batchGet lockedKeys: %d", len(lockedKeys)))
				if err != nil {
					trace_util_0.Count(_snapshot_00000, 55)
					return errors.Trace(err)
				}
			}
			trace_util_0.Count(_snapshot_00000, 52)
			pending = lockedKeys
			continue
		}
		trace_util_0.Count(_snapshot_00000, 38)
		return nil
	}
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(k kv.Key) ([]byte, error) {
	trace_util_0.Count(_snapshot_00000, 56)
	ctx := context.WithValue(context.Background(), txnStartKey, s.version.Ver)
	val, err := s.get(NewBackoffer(ctx, getMaxBackoff), k)
	if err != nil {
		trace_util_0.Count(_snapshot_00000, 59)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_snapshot_00000, 57)
	if len(val) == 0 {
		trace_util_0.Count(_snapshot_00000, 60)
		return nil, kv.ErrNotExist
	}
	trace_util_0.Count(_snapshot_00000, 58)
	return val, nil
}

func (s *tikvSnapshot) get(bo *Backoffer, k kv.Key) ([]byte, error) {
	trace_util_0.Count(_snapshot_00000, 61)
	sender := NewRegionRequestSender(s.store.regionCache, s.store.client)

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGet,
		Get: &pb.GetRequest{
			Key:     k,
			Version: s.version.Ver,
		},
		Context: pb.Context{
			Priority:     s.priority,
			NotFillCache: s.notFillCache,
		},
	}
	for {
		trace_util_0.Count(_snapshot_00000, 62)
		loc, err := s.store.regionCache.LocateKey(bo, k)
		if err != nil {
			trace_util_0.Count(_snapshot_00000, 69)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 63)
		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_snapshot_00000, 70)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 64)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_snapshot_00000, 71)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_snapshot_00000, 65)
		if regionErr != nil {
			trace_util_0.Count(_snapshot_00000, 72)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 74)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 73)
			continue
		}
		trace_util_0.Count(_snapshot_00000, 66)
		cmdGetResp := resp.Get
		if cmdGetResp == nil {
			trace_util_0.Count(_snapshot_00000, 75)
			return nil, errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_snapshot_00000, 67)
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			trace_util_0.Count(_snapshot_00000, 76)
			lock, err := extractLockFromKeyErr(keyErr)
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 80)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 77)
			msBeforeExpired, err := s.store.lockResolver.ResolveLocks(bo, []*Lock{lock})
			if err != nil {
				trace_util_0.Count(_snapshot_00000, 81)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_snapshot_00000, 78)
			if msBeforeExpired > 0 {
				trace_util_0.Count(_snapshot_00000, 82)
				err = bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(keyErr.String()))
				if err != nil {
					trace_util_0.Count(_snapshot_00000, 83)
					return nil, errors.Trace(err)
				}
			}
			trace_util_0.Count(_snapshot_00000, 79)
			continue
		}
		trace_util_0.Count(_snapshot_00000, 68)
		return val, nil
	}
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_snapshot_00000, 84)
	scanner, err := newScanner(s, k, upperBound, scanBatchSize, false)
	return scanner, errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_snapshot_00000, 85)
	scanner, err := newScanner(s, nil, k, scanBatchSize, true)
	return scanner, errors.Trace(err)
}

func extractLockFromKeyErr(keyErr *pb.KeyError) (*Lock, error) {
	trace_util_0.Count(_snapshot_00000, 86)
	if locked := keyErr.GetLocked(); locked != nil {
		trace_util_0.Count(_snapshot_00000, 88)
		return NewLock(locked), nil
	}
	trace_util_0.Count(_snapshot_00000, 87)
	return nil, extractKeyErr(keyErr)
}

func extractKeyErr(keyErr *pb.KeyError) error {
	trace_util_0.Count(_snapshot_00000, 89)
	failpoint.Inject("ErrMockRetryableOnly", func(val failpoint.Value) {
		trace_util_0.Count(_snapshot_00000, 94)
		if val.(bool) {
			trace_util_0.Count(_snapshot_00000, 95)
			keyErr.Conflict = nil
			keyErr.Retryable = "mock retryable error"
		}
	})

	trace_util_0.Count(_snapshot_00000, 90)
	if keyErr.Conflict != nil {
		trace_util_0.Count(_snapshot_00000, 96)
		return newWriteConflictError(keyErr.Conflict)
	}
	trace_util_0.Count(_snapshot_00000, 91)
	if keyErr.Retryable != "" {
		trace_util_0.Count(_snapshot_00000, 97)
		return kv.ErrTxnRetryable.FastGenByArgs("tikv restarts txn: " + keyErr.GetRetryable())
	}
	trace_util_0.Count(_snapshot_00000, 92)
	if keyErr.Abort != "" {
		trace_util_0.Count(_snapshot_00000, 98)
		err := errors.Errorf("tikv aborts txn: %s", keyErr.GetAbort())
		logutil.Logger(context.Background()).Warn("error", zap.Error(err))
		return errors.Trace(err)
	}
	trace_util_0.Count(_snapshot_00000, 93)
	return errors.Errorf("unexpected KeyError: %s", keyErr.String())
}

func newWriteConflictError(conflict *pb.WriteConflict) error {
	trace_util_0.Count(_snapshot_00000, 99)
	var buf bytes.Buffer
	prettyWriteKey(&buf, conflict.Key)
	buf.WriteString(" primary=")
	prettyWriteKey(&buf, conflict.Primary)
	return kv.ErrWriteConflict.FastGenByArgs(conflict.StartTs, conflict.ConflictTs, conflict.ConflictCommitTs, buf.String())
}

func prettyWriteKey(buf *bytes.Buffer, key []byte) {
	trace_util_0.Count(_snapshot_00000, 100)
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		trace_util_0.Count(_snapshot_00000, 103)
		_, err1 := fmt.Fprintf(buf, "{tableID=%d, indexID=%d, indexValues={", tableID, indexID)
		if err1 != nil {
			trace_util_0.Count(_snapshot_00000, 106)
			logutil.Logger(context.Background()).Error("error", zap.Error(err1))
		}
		trace_util_0.Count(_snapshot_00000, 104)
		for _, v := range indexValues {
			trace_util_0.Count(_snapshot_00000, 107)
			_, err2 := fmt.Fprintf(buf, "%s, ", v)
			if err2 != nil {
				trace_util_0.Count(_snapshot_00000, 108)
				logutil.Logger(context.Background()).Error("error", zap.Error(err2))
			}
		}
		trace_util_0.Count(_snapshot_00000, 105)
		buf.WriteString("}}")
		return
	}

	trace_util_0.Count(_snapshot_00000, 101)
	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		trace_util_0.Count(_snapshot_00000, 109)
		_, err3 := fmt.Fprintf(buf, "{tableID=%d, handle=%d}", tableID, handle)
		if err3 != nil {
			trace_util_0.Count(_snapshot_00000, 111)
			logutil.Logger(context.Background()).Error("error", zap.Error(err3))
		}
		trace_util_0.Count(_snapshot_00000, 110)
		return
	}

	trace_util_0.Count(_snapshot_00000, 102)
	_, err4 := fmt.Fprintf(buf, "%#v", key)
	if err4 != nil {
		trace_util_0.Count(_snapshot_00000, 112)
		logutil.Logger(context.Background()).Error("error", zap.Error(err4))
	}
}

var _snapshot_00000 = "store/tikv/snapshot.go"
