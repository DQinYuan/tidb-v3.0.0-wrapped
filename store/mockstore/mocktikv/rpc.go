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

package mocktikv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

const requestMaxSize = 8 * 1024 * 1024

func checkGoContext(ctx context.Context) error {
	trace_util_0.Count(_rpc_00000, 0)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_rpc_00000, 1)
		return ctx.Err()
	default:
		trace_util_0.Count(_rpc_00000, 2)
		return nil
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	trace_util_0.Count(_rpc_00000, 3)
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		trace_util_0.Count(_rpc_00000, 9)
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         locked.Key.Raw(),
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 4)
	if alreadyExist, ok := errors.Cause(err).(*ErrKeyAlreadyExist); ok {
		trace_util_0.Count(_rpc_00000, 10)
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: alreadyExist.Key,
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 5)
	if writeConflict, ok := errors.Cause(err).(*ErrConflict); ok {
		trace_util_0.Count(_rpc_00000, 11)
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				Key:        writeConflict.Key,
				ConflictTs: writeConflict.ConflictTS,
				StartTs:    writeConflict.StartTS,
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 6)
	if dead, ok := errors.Cause(err).(*ErrDeadlock); ok {
		trace_util_0.Count(_rpc_00000, 12)
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockTs:          dead.LockTS,
				LockKey:         dead.LockKey,
				DeadlockKeyHash: dead.DealockKeyHash,
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 7)
	if retryable, ok := errors.Cause(err).(ErrRetryable); ok {
		trace_util_0.Count(_rpc_00000, 13)
		return &kvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	trace_util_0.Count(_rpc_00000, 8)
	return &kvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*kvrpcpb.KeyError {
	trace_util_0.Count(_rpc_00000, 14)
	var keyErrors = make([]*kvrpcpb.KeyError, 0)
	for _, err := range errs {
		trace_util_0.Count(_rpc_00000, 16)
		if err != nil {
			trace_util_0.Count(_rpc_00000, 17)
			keyErrors = append(keyErrors, convertToKeyError(err))
		}
	}
	trace_util_0.Count(_rpc_00000, 15)
	return keyErrors
}

func convertToPbPairs(pairs []Pair) []*kvrpcpb.KvPair {
	trace_util_0.Count(_rpc_00000, 18)
	kvPairs := make([]*kvrpcpb.KvPair, 0, len(pairs))
	for _, p := range pairs {
		trace_util_0.Count(_rpc_00000, 20)
		var kvPair *kvrpcpb.KvPair
		if p.Err == nil {
			trace_util_0.Count(_rpc_00000, 22)
			kvPair = &kvrpcpb.KvPair{
				Key:   p.Key,
				Value: p.Value,
			}
		} else {
			trace_util_0.Count(_rpc_00000, 23)
			{
				kvPair = &kvrpcpb.KvPair{
					Error: convertToKeyError(p.Err),
				}
			}
		}
		trace_util_0.Count(_rpc_00000, 21)
		kvPairs = append(kvPairs, kvPair)
	}
	trace_util_0.Count(_rpc_00000, 19)
	return kvPairs
}

// rpcHandler mocks tikv's side handler behavior. In general, you may assume
// TiKV just translate the logic from Go to Rust.
type rpcHandler struct {
	cluster   *Cluster
	mvccStore MVCCStore

	// storeID stores id for current request
	storeID uint64
	// startKey is used for handling normal request.
	startKey []byte
	endKey   []byte
	// rawStartKey is used for handling coprocessor request.
	rawStartKey []byte
	rawEndKey   []byte
	// isolationLevel is used for current request.
	isolationLevel kvrpcpb.IsolationLevel
}

func (h *rpcHandler) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	trace_util_0.Count(_rpc_00000, 24)
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != h.storeID {
		trace_util_0.Count(_rpc_00000, 32)
		return &errorpb.Error{
			Message:       *proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	trace_util_0.Count(_rpc_00000, 25)
	region, leaderID := h.cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		trace_util_0.Count(_rpc_00000, 33)
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 26)
	var storePeer, leaderPeer *metapb.Peer
	for _, p := range region.Peers {
		trace_util_0.Count(_rpc_00000, 34)
		if p.GetStoreId() == h.storeID {
			trace_util_0.Count(_rpc_00000, 36)
			storePeer = p
		}
		trace_util_0.Count(_rpc_00000, 35)
		if p.GetId() == leaderID {
			trace_util_0.Count(_rpc_00000, 37)
			leaderPeer = p
		}
	}
	// The Store does not contain a Peer of the Region.
	trace_util_0.Count(_rpc_00000, 27)
	if storePeer == nil {
		trace_util_0.Count(_rpc_00000, 38)
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	trace_util_0.Count(_rpc_00000, 28)
	if leaderPeer == nil {
		trace_util_0.Count(_rpc_00000, 39)
		return &errorpb.Error{
			Message: *proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the Store is not leader.
	trace_util_0.Count(_rpc_00000, 29)
	if storePeer.GetId() != leaderPeer.GetId() {
		trace_util_0.Count(_rpc_00000, 40)
		return &errorpb.Error{
			Message: *proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	trace_util_0.Count(_rpc_00000, 30)
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		trace_util_0.Count(_rpc_00000, 41)
		nextRegion, _ := h.cluster.GetRegionByKey(region.GetEndKey())
		currentRegions := []*metapb.Region{region}
		if nextRegion != nil {
			trace_util_0.Count(_rpc_00000, 43)
			currentRegions = append(currentRegions, nextRegion)
		}
		trace_util_0.Count(_rpc_00000, 42)
		return &errorpb.Error{
			Message: *proto.String("epoch not match"),
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: currentRegions,
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 31)
	h.startKey, h.endKey = region.StartKey, region.EndKey
	h.isolationLevel = ctx.IsolationLevel
	return nil
}

func (h *rpcHandler) checkRequestSize(size int) *errorpb.Error {
	trace_util_0.Count(_rpc_00000, 44)
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		trace_util_0.Count(_rpc_00000, 46)
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	trace_util_0.Count(_rpc_00000, 45)
	return nil
}

func (h *rpcHandler) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	trace_util_0.Count(_rpc_00000, 47)
	if err := h.checkRequestContext(ctx); err != nil {
		trace_util_0.Count(_rpc_00000, 49)
		return err
	}
	trace_util_0.Count(_rpc_00000, 48)
	return h.checkRequestSize(size)
}

func (h *rpcHandler) checkKeyInRegion(key []byte) bool {
	trace_util_0.Count(_rpc_00000, 50)
	return regionContains(h.startKey, h.endKey, []byte(NewMvccKey(key)))
}

func (h *rpcHandler) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	trace_util_0.Count(_rpc_00000, 51)
	if !h.checkKeyInRegion(req.Key) {
		trace_util_0.Count(_rpc_00000, 54)
		panic("KvGet: key not in region")
	}

	trace_util_0.Count(_rpc_00000, 52)
	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 55)
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	trace_util_0.Count(_rpc_00000, 53)
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (h *rpcHandler) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	trace_util_0.Count(_rpc_00000, 56)
	endKey := MvccKey(h.endKey).Raw()
	var pairs []Pair
	if !req.Reverse {
		trace_util_0.Count(_rpc_00000, 58)
		if !h.checkKeyInRegion(req.GetStartKey()) {
			trace_util_0.Count(_rpc_00000, 61)
			panic("KvScan: startKey not in region")
		}
		trace_util_0.Count(_rpc_00000, 59)
		if len(req.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.EndKey), h.endKey) < 0) {
			trace_util_0.Count(_rpc_00000, 62)
			endKey = req.EndKey
		}
		trace_util_0.Count(_rpc_00000, 60)
		pairs = h.mvccStore.Scan(req.GetStartKey(), endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel)
	} else {
		trace_util_0.Count(_rpc_00000, 63)
		{
			// TiKV use range [end_key, start_key) for reverse scan.
			// Should use the req.EndKey to check in region.
			if !h.checkKeyInRegion(req.GetEndKey()) {
				trace_util_0.Count(_rpc_00000, 66)
				panic("KvScan: startKey not in region")
			}

			// TiKV use range [end_key, start_key) for reverse scan.
			// So the req.StartKey actually is the end_key.
			trace_util_0.Count(_rpc_00000, 64)
			if len(req.StartKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.StartKey), h.endKey) < 0) {
				trace_util_0.Count(_rpc_00000, 67)
				endKey = req.StartKey
			}

			trace_util_0.Count(_rpc_00000, 65)
			pairs = h.mvccStore.ReverseScan(req.EndKey, endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel)
		}
	}

	trace_util_0.Count(_rpc_00000, 57)
	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	trace_util_0.Count(_rpc_00000, 68)
	for _, m := range req.Mutations {
		trace_util_0.Count(_rpc_00000, 70)
		if !h.checkKeyInRegion(m.Key) {
			trace_util_0.Count(_rpc_00000, 71)
			panic("KvPrewrite: key not in region")
		}
	}
	trace_util_0.Count(_rpc_00000, 69)
	errs := h.mvccStore.Prewrite(req)
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleKvPessimisticLock(req *kvrpcpb.PessimisticLockRequest) *kvrpcpb.PessimisticLockResponse {
	trace_util_0.Count(_rpc_00000, 72)
	for _, m := range req.Mutations {
		trace_util_0.Count(_rpc_00000, 74)
		if !h.checkKeyInRegion(m.Key) {
			trace_util_0.Count(_rpc_00000, 75)
			panic("KvPessimisticLock: key not in region")
		}
	}
	trace_util_0.Count(_rpc_00000, 73)
	startTS := req.StartVersion
	regionID := req.Context.RegionId
	h.cluster.handleDelay(startTS, regionID)
	errs := h.mvccStore.PessimisticLock(req.Mutations, req.PrimaryLock, req.GetStartVersion(), req.GetForUpdateTs(), req.GetLockTtl())

	// TODO: remove this when implement sever side wait.
	h.simulateServerSideWaitLock(errs)
	return &kvrpcpb.PessimisticLockResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) simulateServerSideWaitLock(errs []error) {
	trace_util_0.Count(_rpc_00000, 76)
	for _, err := range errs {
		trace_util_0.Count(_rpc_00000, 77)
		if _, ok := err.(*ErrLocked); ok {
			trace_util_0.Count(_rpc_00000, 78)
			time.Sleep(time.Millisecond * 5)
			break
		}
	}
}

func (h *rpcHandler) handleKvPessimisticRollback(req *kvrpcpb.PessimisticRollbackRequest) *kvrpcpb.PessimisticRollbackResponse {
	trace_util_0.Count(_rpc_00000, 79)
	for _, key := range req.Keys {
		trace_util_0.Count(_rpc_00000, 81)
		if !h.checkKeyInRegion(key) {
			trace_util_0.Count(_rpc_00000, 82)
			panic("KvPessimisticRollback: key not in region")
		}
	}
	trace_util_0.Count(_rpc_00000, 80)
	errs := h.mvccStore.PessimisticRollback(req.Keys, req.StartVersion, req.ForUpdateTs)
	return &kvrpcpb.PessimisticRollbackResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	trace_util_0.Count(_rpc_00000, 83)
	for _, k := range req.Keys {
		trace_util_0.Count(_rpc_00000, 86)
		if !h.checkKeyInRegion(k) {
			trace_util_0.Count(_rpc_00000, 87)
			panic("KvCommit: key not in region")
		}
	}
	trace_util_0.Count(_rpc_00000, 84)
	var resp kvrpcpb.CommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		trace_util_0.Count(_rpc_00000, 88)
		resp.Error = convertToKeyError(err)
	}
	trace_util_0.Count(_rpc_00000, 85)
	return &resp
}

func (h *rpcHandler) handleKvCleanup(req *kvrpcpb.CleanupRequest) *kvrpcpb.CleanupResponse {
	trace_util_0.Count(_rpc_00000, 89)
	if !h.checkKeyInRegion(req.Key) {
		trace_util_0.Count(_rpc_00000, 92)
		panic("KvCleanup: key not in region")
	}
	trace_util_0.Count(_rpc_00000, 90)
	var resp kvrpcpb.CleanupResponse
	err := h.mvccStore.Cleanup(req.Key, req.GetStartVersion())
	if err != nil {
		trace_util_0.Count(_rpc_00000, 93)
		if commitTS, ok := errors.Cause(err).(ErrAlreadyCommitted); ok {
			trace_util_0.Count(_rpc_00000, 94)
			resp.CommitVersion = uint64(commitTS)
		} else {
			trace_util_0.Count(_rpc_00000, 95)
			{
				resp.Error = convertToKeyError(err)
			}
		}
	}
	trace_util_0.Count(_rpc_00000, 91)
	return &resp
}

func (h *rpcHandler) handleKvBatchGet(req *kvrpcpb.BatchGetRequest) *kvrpcpb.BatchGetResponse {
	trace_util_0.Count(_rpc_00000, 96)
	for _, k := range req.Keys {
		trace_util_0.Count(_rpc_00000, 98)
		if !h.checkKeyInRegion(k) {
			trace_util_0.Count(_rpc_00000, 99)
			panic("KvBatchGet: key not in region")
		}
	}
	trace_util_0.Count(_rpc_00000, 97)
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion(), h.isolationLevel)
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleMvccGetByKey(req *kvrpcpb.MvccGetByKeyRequest) *kvrpcpb.MvccGetByKeyResponse {
	trace_util_0.Count(_rpc_00000, 100)
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		trace_util_0.Count(_rpc_00000, 103)
		return &kvrpcpb.MvccGetByKeyResponse{
			Error: "not implement",
		}
	}

	trace_util_0.Count(_rpc_00000, 101)
	if !h.checkKeyInRegion(req.Key) {
		trace_util_0.Count(_rpc_00000, 104)
		panic("MvccGetByKey: key not in region")
	}
	trace_util_0.Count(_rpc_00000, 102)
	var resp kvrpcpb.MvccGetByKeyResponse
	resp.Info = debugger.MvccGetByKey(req.Key)
	return &resp
}

func (h *rpcHandler) handleMvccGetByStartTS(req *kvrpcpb.MvccGetByStartTsRequest) *kvrpcpb.MvccGetByStartTsResponse {
	trace_util_0.Count(_rpc_00000, 105)
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		trace_util_0.Count(_rpc_00000, 107)
		return &kvrpcpb.MvccGetByStartTsResponse{
			Error: "not implement",
		}
	}
	trace_util_0.Count(_rpc_00000, 106)
	var resp kvrpcpb.MvccGetByStartTsResponse
	resp.Info, resp.Key = debugger.MvccGetByStartTS(h.startKey, h.endKey, req.StartTs)
	return &resp
}

func (h *rpcHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	trace_util_0.Count(_rpc_00000, 108)
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 110)
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	trace_util_0.Count(_rpc_00000, 109)
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	trace_util_0.Count(_rpc_00000, 111)
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	locks, err := h.mvccStore.ScanLock(startKey, endKey, req.GetMaxVersion())
	if err != nil {
		trace_util_0.Count(_rpc_00000, 113)
		return &kvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	trace_util_0.Count(_rpc_00000, 112)
	return &kvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (h *rpcHandler) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	trace_util_0.Count(_rpc_00000, 114)
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.ResolveLock(startKey, endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		trace_util_0.Count(_rpc_00000, 116)
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	trace_util_0.Count(_rpc_00000, 115)
	return &kvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleKvDeleteRange(req *kvrpcpb.DeleteRangeRequest) *kvrpcpb.DeleteRangeResponse {
	trace_util_0.Count(_rpc_00000, 117)
	if !h.checkKeyInRegion(req.StartKey) {
		trace_util_0.Count(_rpc_00000, 120)
		panic("KvDeleteRange: key not in region")
	}
	trace_util_0.Count(_rpc_00000, 118)
	var resp kvrpcpb.DeleteRangeResponse
	err := h.mvccStore.DeleteRange(req.StartKey, req.EndKey)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 121)
		resp.Error = err.Error()
	}
	trace_util_0.Count(_rpc_00000, 119)
	return &resp
}

func (h *rpcHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	trace_util_0.Count(_rpc_00000, 122)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 124)
		return &kvrpcpb.RawGetResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 123)
	return &kvrpcpb.RawGetResponse{
		Value: rawKV.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleKvRawBatchGet(req *kvrpcpb.RawBatchGetRequest) *kvrpcpb.RawBatchGetResponse {
	trace_util_0.Count(_rpc_00000, 125)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 128)
		// TODO should we add error ?
		return &kvrpcpb.RawBatchGetResponse{
			RegionError: &errorpb.Error{
				Message: "not implemented",
			},
		}
	}
	trace_util_0.Count(_rpc_00000, 126)
	values := rawKV.RawBatchGet(req.Keys)
	kvPairs := make([]*kvrpcpb.KvPair, len(values))
	for i, key := range req.Keys {
		trace_util_0.Count(_rpc_00000, 129)
		kvPairs[i] = &kvrpcpb.KvPair{
			Key:   key,
			Value: values[i],
		}
	}
	trace_util_0.Count(_rpc_00000, 127)
	return &kvrpcpb.RawBatchGetResponse{
		Pairs: kvPairs,
	}
}

func (h *rpcHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	trace_util_0.Count(_rpc_00000, 130)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 132)
		return &kvrpcpb.RawPutResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 131)
	rawKV.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleKvRawBatchPut(req *kvrpcpb.RawBatchPutRequest) *kvrpcpb.RawBatchPutResponse {
	trace_util_0.Count(_rpc_00000, 133)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 136)
		return &kvrpcpb.RawBatchPutResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 134)
	keys := make([][]byte, 0, len(req.Pairs))
	values := make([][]byte, 0, len(req.Pairs))
	for _, pair := range req.Pairs {
		trace_util_0.Count(_rpc_00000, 137)
		keys = append(keys, pair.Key)
		values = append(values, pair.Value)
	}
	trace_util_0.Count(_rpc_00000, 135)
	rawKV.RawBatchPut(keys, values)
	return &kvrpcpb.RawBatchPutResponse{}
}

func (h *rpcHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	trace_util_0.Count(_rpc_00000, 138)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 140)
		return &kvrpcpb.RawDeleteResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 139)
	rawKV.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (h *rpcHandler) handleKvRawBatchDelete(req *kvrpcpb.RawBatchDeleteRequest) *kvrpcpb.RawBatchDeleteResponse {
	trace_util_0.Count(_rpc_00000, 141)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 143)
		return &kvrpcpb.RawBatchDeleteResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 142)
	rawKV.RawBatchDelete(req.Keys)
	return &kvrpcpb.RawBatchDeleteResponse{}
}

func (h *rpcHandler) handleKvRawDeleteRange(req *kvrpcpb.RawDeleteRangeRequest) *kvrpcpb.RawDeleteRangeResponse {
	trace_util_0.Count(_rpc_00000, 144)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 146)
		return &kvrpcpb.RawDeleteRangeResponse{
			Error: "not implemented",
		}
	}
	trace_util_0.Count(_rpc_00000, 145)
	rawKV.RawDeleteRange(req.GetStartKey(), req.GetEndKey())
	return &kvrpcpb.RawDeleteRangeResponse{}
}

func (h *rpcHandler) handleKvRawScan(req *kvrpcpb.RawScanRequest) *kvrpcpb.RawScanResponse {
	trace_util_0.Count(_rpc_00000, 147)
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		trace_util_0.Count(_rpc_00000, 150)
		errStr := "not implemented"
		return &kvrpcpb.RawScanResponse{
			RegionError: &errorpb.Error{
				Message: errStr,
			},
		}
	}

	trace_util_0.Count(_rpc_00000, 148)
	var pairs []Pair
	if req.Reverse {
		trace_util_0.Count(_rpc_00000, 151)
		lowerBound := h.startKey
		if bytes.Compare(req.EndKey, lowerBound) > 0 {
			trace_util_0.Count(_rpc_00000, 153)
			lowerBound = req.EndKey
		}
		trace_util_0.Count(_rpc_00000, 152)
		pairs = rawKV.RawReverseScan(
			req.StartKey,
			lowerBound,
			int(req.GetLimit()),
		)
	} else {
		trace_util_0.Count(_rpc_00000, 154)
		{
			upperBound := h.endKey
			if len(req.EndKey) > 0 && (len(upperBound) == 0 || bytes.Compare(req.EndKey, upperBound) < 0) {
				trace_util_0.Count(_rpc_00000, 156)
				upperBound = req.EndKey
			}
			trace_util_0.Count(_rpc_00000, 155)
			pairs = rawKV.RawScan(
				req.StartKey,
				upperBound,
				int(req.GetLimit()),
			)
		}
	}

	trace_util_0.Count(_rpc_00000, 149)
	return &kvrpcpb.RawScanResponse{
		Kvs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleSplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	trace_util_0.Count(_rpc_00000, 157)
	key := NewMvccKey(req.GetSplitKey())
	region, _ := h.cluster.GetRegionByKey(key)
	if bytes.Equal(region.GetStartKey(), key) {
		trace_util_0.Count(_rpc_00000, 159)
		return &kvrpcpb.SplitRegionResponse{}
	}
	trace_util_0.Count(_rpc_00000, 158)
	newRegionID, newPeerIDs := h.cluster.AllocID(), h.cluster.AllocIDs(len(region.Peers))
	h.cluster.SplitRaw(region.GetId(), newRegionID, key, newPeerIDs, newPeerIDs[0])
	return &kvrpcpb.SplitRegionResponse{}
}

// RPCClient sends kv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at tikv's side.
type RPCClient struct {
	Cluster       *Cluster
	MvccStore     MVCCStore
	streamTimeout chan *tikvrpc.Lease
}

// NewRPCClient creates an RPCClient.
// Note that close the RPCClient may close the underlying MvccStore.
func NewRPCClient(cluster *Cluster, mvccStore MVCCStore) *RPCClient {
	trace_util_0.Count(_rpc_00000, 160)
	ch := make(chan *tikvrpc.Lease)
	go tikvrpc.CheckStreamTimeoutLoop(ch)
	return &RPCClient{
		Cluster:       cluster,
		MvccStore:     mvccStore,
		streamTimeout: ch,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*metapb.Store, error) {
	trace_util_0.Count(_rpc_00000, 161)
	store, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 165)
		return nil, err
	}
	trace_util_0.Count(_rpc_00000, 162)
	if store == nil {
		trace_util_0.Count(_rpc_00000, 166)
		return nil, errors.New("connect fail")
	}
	trace_util_0.Count(_rpc_00000, 163)
	if store.GetState() == metapb.StoreState_Offline ||
		store.GetState() == metapb.StoreState_Tombstone {
		trace_util_0.Count(_rpc_00000, 167)
		return nil, errors.New("connection refused")
	}
	trace_util_0.Count(_rpc_00000, 164)
	return store, nil
}

func (c *RPCClient) checkArgs(ctx context.Context, addr string) (*rpcHandler, error) {
	trace_util_0.Count(_rpc_00000, 168)
	if err := checkGoContext(ctx); err != nil {
		trace_util_0.Count(_rpc_00000, 171)
		return nil, err
	}

	trace_util_0.Count(_rpc_00000, 169)
	store, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 172)
		return nil, err
	}
	trace_util_0.Count(_rpc_00000, 170)
	handler := &rpcHandler{
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return handler, nil
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	trace_util_0.Count(_rpc_00000, 173)
	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		trace_util_0.Count(_rpc_00000, 177)
		if val.(bool) {
			trace_util_0.Count(_rpc_00000, 178)
			failpoint.Return(tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	trace_util_0.Count(_rpc_00000, 174)
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		trace_util_0.Count(_rpc_00000, 179)
		return nil, err
	}
	trace_util_0.Count(_rpc_00000, 175)
	reqCtx := &req.Context
	resp := &tikvrpc.Response{}
	resp.Type = req.Type
	switch req.Type {
	case tikvrpc.CmdGet:
		trace_util_0.Count(_rpc_00000, 180)
		r := req.Get
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 239)
			resp.Get = &kvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 181)
		resp.Get = handler.handleKvGet(r)
	case tikvrpc.CmdScan:
		trace_util_0.Count(_rpc_00000, 182)
		r := req.Scan
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 240)
			resp.Scan = &kvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 183)
		resp.Scan = handler.handleKvScan(r)

	case tikvrpc.CmdPrewrite:
		trace_util_0.Count(_rpc_00000, 184)
		r := req.Prewrite
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 241)
			resp.Prewrite = &kvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 185)
		resp.Prewrite = handler.handleKvPrewrite(r)
	case tikvrpc.CmdPessimisticLock:
		trace_util_0.Count(_rpc_00000, 186)
		r := req.PessimisticLock
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 242)
			resp.PessimisticLock = &kvrpcpb.PessimisticLockResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 187)
		resp.PessimisticLock = handler.handleKvPessimisticLock(r)
	case tikvrpc.CmdPessimisticRollback:
		trace_util_0.Count(_rpc_00000, 188)
		r := req.PessimisticRollback
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 243)
			resp.PessimisticRollback = &kvrpcpb.PessimisticRollbackResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 189)
		resp.PessimisticRollback = handler.handleKvPessimisticRollback(r)
	case tikvrpc.CmdCommit:
		trace_util_0.Count(_rpc_00000, 190)
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			trace_util_0.Count(_rpc_00000, 244)
			switch val.(string) {
			case "timeout":
				trace_util_0.Count(_rpc_00000, 245)
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				trace_util_0.Count(_rpc_00000, 246)
				failpoint.Return(&tikvrpc.Response{
					Type:   tikvrpc.CmdCommit,
					Commit: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				trace_util_0.Count(_rpc_00000, 247)
				failpoint.Return(&tikvrpc.Response{
					Type:   tikvrpc.CmdCommit,
					Commit: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}},
				}, nil)
			}
		})

		trace_util_0.Count(_rpc_00000, 191)
		r := req.Commit
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 248)
			resp.Commit = &kvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 192)
		resp.Commit = handler.handleKvCommit(r)
		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			trace_util_0.Count(_rpc_00000, 249)
			if val.(bool) {
				trace_util_0.Count(_rpc_00000, 250)
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case tikvrpc.CmdCleanup:
		trace_util_0.Count(_rpc_00000, 193)
		r := req.Cleanup
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 251)
			resp.Cleanup = &kvrpcpb.CleanupResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 194)
		resp.Cleanup = handler.handleKvCleanup(r)
	case tikvrpc.CmdBatchGet:
		trace_util_0.Count(_rpc_00000, 195)
		r := req.BatchGet
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 252)
			resp.BatchGet = &kvrpcpb.BatchGetResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 196)
		resp.BatchGet = handler.handleKvBatchGet(r)
	case tikvrpc.CmdBatchRollback:
		trace_util_0.Count(_rpc_00000, 197)
		r := req.BatchRollback
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 253)
			resp.BatchRollback = &kvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 198)
		resp.BatchRollback = handler.handleKvBatchRollback(r)
	case tikvrpc.CmdScanLock:
		trace_util_0.Count(_rpc_00000, 199)
		r := req.ScanLock
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 254)
			resp.ScanLock = &kvrpcpb.ScanLockResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 200)
		resp.ScanLock = handler.handleKvScanLock(r)
	case tikvrpc.CmdResolveLock:
		trace_util_0.Count(_rpc_00000, 201)
		r := req.ResolveLock
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 255)
			resp.ResolveLock = &kvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 202)
		resp.ResolveLock = handler.handleKvResolveLock(r)
	case tikvrpc.CmdGC:
		trace_util_0.Count(_rpc_00000, 203)
		r := req.GC
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 256)
			resp.GC = &kvrpcpb.GCResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 204)
		resp.GC = &kvrpcpb.GCResponse{}
	case tikvrpc.CmdDeleteRange:
		trace_util_0.Count(_rpc_00000, 205)
		r := req.DeleteRange
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 257)
			resp.DeleteRange = &kvrpcpb.DeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 206)
		resp.DeleteRange = handler.handleKvDeleteRange(r)
	case tikvrpc.CmdRawGet:
		trace_util_0.Count(_rpc_00000, 207)
		r := req.RawGet
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 258)
			resp.RawGet = &kvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 208)
		resp.RawGet = handler.handleKvRawGet(r)
	case tikvrpc.CmdRawBatchGet:
		trace_util_0.Count(_rpc_00000, 209)
		r := req.RawBatchGet
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 259)
			resp.RawBatchGet = &kvrpcpb.RawBatchGetResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 210)
		resp.RawBatchGet = handler.handleKvRawBatchGet(r)
	case tikvrpc.CmdRawPut:
		trace_util_0.Count(_rpc_00000, 211)
		r := req.RawPut
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 260)
			resp.RawPut = &kvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 212)
		resp.RawPut = handler.handleKvRawPut(r)
	case tikvrpc.CmdRawBatchPut:
		trace_util_0.Count(_rpc_00000, 213)
		r := req.RawBatchPut
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 261)
			resp.RawBatchPut = &kvrpcpb.RawBatchPutResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 214)
		resp.RawBatchPut = handler.handleKvRawBatchPut(r)
	case tikvrpc.CmdRawDelete:
		trace_util_0.Count(_rpc_00000, 215)
		r := req.RawDelete
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 262)
			resp.RawDelete = &kvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 216)
		resp.RawDelete = handler.handleKvRawDelete(r)
	case tikvrpc.CmdRawBatchDelete:
		trace_util_0.Count(_rpc_00000, 217)
		r := req.RawBatchDelete
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 263)
			resp.RawBatchDelete = &kvrpcpb.RawBatchDeleteResponse{RegionError: err}
		}
		trace_util_0.Count(_rpc_00000, 218)
		resp.RawBatchDelete = handler.handleKvRawBatchDelete(r)
	case tikvrpc.CmdRawDeleteRange:
		trace_util_0.Count(_rpc_00000, 219)
		r := req.RawDeleteRange
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 264)
			resp.RawDeleteRange = &kvrpcpb.RawDeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 220)
		resp.RawDeleteRange = handler.handleKvRawDeleteRange(r)
	case tikvrpc.CmdRawScan:
		trace_util_0.Count(_rpc_00000, 221)
		r := req.RawScan
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 265)
			resp.RawScan = &kvrpcpb.RawScanResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 222)
		resp.RawScan = handler.handleKvRawScan(r)
	case tikvrpc.CmdUnsafeDestroyRange:
		trace_util_0.Count(_rpc_00000, 223)
		panic("unimplemented")
	case tikvrpc.CmdCop:
		trace_util_0.Count(_rpc_00000, 224)
		r := req.Cop
		if err := handler.checkRequestContext(reqCtx); err != nil {
			trace_util_0.Count(_rpc_00000, 266)
			resp.Cop = &coprocessor.Response{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 225)
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		var res *coprocessor.Response
		switch r.GetTp() {
		case kv.ReqTypeDAG:
			trace_util_0.Count(_rpc_00000, 267)
			res = handler.handleCopDAGRequest(r)
		case kv.ReqTypeAnalyze:
			trace_util_0.Count(_rpc_00000, 268)
			res = handler.handleCopAnalyzeRequest(r)
		case kv.ReqTypeChecksum:
			trace_util_0.Count(_rpc_00000, 269)
			res = handler.handleCopChecksumRequest(r)
		default:
			trace_util_0.Count(_rpc_00000, 270)
			panic(fmt.Sprintf("unknown coprocessor request type: %v", r.GetTp()))
		}
		trace_util_0.Count(_rpc_00000, 226)
		resp.Cop = res
	case tikvrpc.CmdCopStream:
		trace_util_0.Count(_rpc_00000, 227)
		r := req.Cop
		if err := handler.checkRequestContext(reqCtx); err != nil {
			trace_util_0.Count(_rpc_00000, 271)
			resp.CopStream = &tikvrpc.CopStreamResponse{
				Tikv_CoprocessorStreamClient: &mockCopStreamErrClient{Error: err},
				Response: &coprocessor.Response{
					RegionError: err,
				},
			}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 228)
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		ctx1, cancel := context.WithCancel(ctx)
		copStream, err := handler.handleCopStream(ctx1, r)
		if err != nil {
			trace_util_0.Count(_rpc_00000, 272)
			cancel()
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_rpc_00000, 229)
		streamResp := &tikvrpc.CopStreamResponse{
			Tikv_CoprocessorStreamClient: copStream,
		}
		streamResp.Lease.Cancel = cancel
		streamResp.Timeout = timeout
		c.streamTimeout <- &streamResp.Lease

		first, err := streamResp.Recv()
		if err != nil {
			trace_util_0.Count(_rpc_00000, 273)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_rpc_00000, 230)
		streamResp.Response = first
		resp.CopStream = streamResp
	case tikvrpc.CmdMvccGetByKey:
		trace_util_0.Count(_rpc_00000, 231)
		r := req.MvccGetByKey
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 274)
			resp.MvccGetByKey = &kvrpcpb.MvccGetByKeyResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 232)
		resp.MvccGetByKey = handler.handleMvccGetByKey(r)
	case tikvrpc.CmdMvccGetByStartTs:
		trace_util_0.Count(_rpc_00000, 233)
		r := req.MvccGetByStartTs
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 275)
			resp.MvccGetByStartTS = &kvrpcpb.MvccGetByStartTsResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 234)
		resp.MvccGetByStartTS = handler.handleMvccGetByStartTS(r)
	case tikvrpc.CmdSplitRegion:
		trace_util_0.Count(_rpc_00000, 235)
		r := req.SplitRegion
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			trace_util_0.Count(_rpc_00000, 276)
			resp.SplitRegion = &kvrpcpb.SplitRegionResponse{RegionError: err}
			return resp, nil
		}
		trace_util_0.Count(_rpc_00000, 236)
		resp.SplitRegion = handler.handleSplitRegion(r)
	// DebugGetRegionProperties is for fast analyze in mock tikv.
	case tikvrpc.CmdDebugGetRegionProperties:
		trace_util_0.Count(_rpc_00000, 237)
		r := req.DebugGetRegionProperties
		region, _ := c.Cluster.GetRegion(r.RegionId)
		scanResp := handler.handleKvScan(&kvrpcpb.ScanRequest{StartKey: MvccKey(region.StartKey).Raw(), EndKey: MvccKey(region.EndKey).Raw(), Version: math.MaxUint64, Limit: math.MaxUint32})
		resp.DebugGetRegionProperties = &debugpb.GetRegionPropertiesResponse{
			Props: []*debugpb.Property{{
				Name:  "mvcc.num_rows",
				Value: strconv.Itoa(len(scanResp.Pairs)),
			}}}
	default:
		trace_util_0.Count(_rpc_00000, 238)
		return nil, errors.Errorf("unsupport this request type %v", req.Type)
	}
	trace_util_0.Count(_rpc_00000, 176)
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	trace_util_0.Count(_rpc_00000, 277)
	close(c.streamTimeout)
	if raw, ok := c.MvccStore.(io.Closer); ok {
		trace_util_0.Count(_rpc_00000, 279)
		return raw.Close()
	}
	trace_util_0.Count(_rpc_00000, 278)
	return nil
}

var _rpc_00000 = "store/mockstore/mocktikv/rpc.go"
