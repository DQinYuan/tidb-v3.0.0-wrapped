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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// ShuttingDown is a flag to indicate tidb-server is exiting (Ctrl+C signal
// receved for example). If this flag is set, tikv client should not retry on
// network error because tidb-server expect tikv client to exit as soon as possible.
var ShuttingDown uint32

// RegionRequestSender sends KV/Cop requests to tikv server. It handles network
// errors and some region errors internally.
//
// Typically, a KV/Cop request is bind to a region, all keys that are involved
// in the request should be located in the region.
// The sending process begins with looking for the address of leader store's
// address of the target region from cache, and the request is then sent to the
// destination tikv server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region
// merge, or region balance, tikv server may not able to process request and
// send back a RegionError.
// RegionRequestSender takes care of errors that does not relevant to region
// range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'. For other
// errors, since region range have changed, the request may need to split, so we
// simply return the error to caller.
type RegionRequestSender struct {
	regionCache  *RegionCache
	client       Client
	storeAddr    string
	rpcError     error
	failStoreIDs map[uint64]struct{}
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client Client) *RegionRequestSender {
	trace_util_0.Count(_region_request_00000, 0)
	return &RegionRequestSender{
		regionCache: regionCache,
		client:      client,
	}
}

// SendReq sends a request to tikv server.
func (s *RegionRequestSender) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	trace_util_0.Count(_region_request_00000, 1)
	resp, _, err := s.SendReqCtx(bo, req, regionID, timeout)
	return resp, err
}

// SendReqCtx sends a request to tikv server and return response and RPCCtx of this RPC.
func (s *RegionRequestSender) SendReqCtx(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, *RPCContext, error) {
	trace_util_0.Count(_region_request_00000, 2)
	failpoint.Inject("tikvStoreSendReqResult", func(val failpoint.Value) {
		trace_util_0.Count(_region_request_00000, 4)
		switch val.(string) {
		case "timeout":
			trace_util_0.Count(_region_request_00000, 5)
			failpoint.Return(nil, nil, errors.New("timeout"))
		case "GCNotLeader":
			trace_util_0.Count(_region_request_00000, 6)
			if req.Type == tikvrpc.CmdGC {
				trace_util_0.Count(_region_request_00000, 8)
				failpoint.Return(&tikvrpc.Response{
					Type: tikvrpc.CmdGC,
					GC:   &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil, nil)
			}
		case "GCServerIsBusy":
			trace_util_0.Count(_region_request_00000, 7)
			if req.Type == tikvrpc.CmdGC {
				trace_util_0.Count(_region_request_00000, 9)
				failpoint.Return(&tikvrpc.Response{
					Type: tikvrpc.CmdGC,
					GC:   &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil, nil)
			}
		}
	})

	trace_util_0.Count(_region_request_00000, 3)
	for {
		trace_util_0.Count(_region_request_00000, 10)
		ctx, err := s.regionCache.GetRPCContext(bo, regionID)
		if err != nil {
			trace_util_0.Count(_region_request_00000, 17)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_region_request_00000, 11)
		if ctx == nil {
			trace_util_0.Count(_region_request_00000, 18)
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.

			// TODO: Change the returned error to something like "region missing in cache",
			// and handle this error like EpochNotMatch, which means to re-split the request and retry.
			resp, err := tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
			return resp, nil, err
		}

		trace_util_0.Count(_region_request_00000, 12)
		s.storeAddr = ctx.Addr
		resp, retry, err := s.sendReqToRegion(bo, ctx, req, timeout)
		if err != nil {
			trace_util_0.Count(_region_request_00000, 19)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_region_request_00000, 13)
		if retry {
			trace_util_0.Count(_region_request_00000, 20)
			continue
		}

		trace_util_0.Count(_region_request_00000, 14)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_region_request_00000, 21)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_region_request_00000, 15)
		if regionErr != nil {
			trace_util_0.Count(_region_request_00000, 22)
			retry, err := s.onRegionError(bo, ctx, regionErr)
			if err != nil {
				trace_util_0.Count(_region_request_00000, 24)
				return nil, nil, errors.Trace(err)
			}
			trace_util_0.Count(_region_request_00000, 23)
			if retry {
				trace_util_0.Count(_region_request_00000, 25)
				continue
			}
		}
		trace_util_0.Count(_region_request_00000, 16)
		return resp, ctx, nil
	}
}

func (s *RegionRequestSender) sendReqToRegion(bo *Backoffer, ctx *RPCContext, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, retry bool, err error) {
	trace_util_0.Count(_region_request_00000, 26)
	if e := tikvrpc.SetContext(req, ctx.Meta, ctx.Peer); e != nil {
		trace_util_0.Count(_region_request_00000, 29)
		return nil, false, errors.Trace(e)
	}
	trace_util_0.Count(_region_request_00000, 27)
	resp, err = s.client.SendRequest(bo.ctx, ctx.Addr, req, timeout)
	if err != nil {
		trace_util_0.Count(_region_request_00000, 30)
		s.rpcError = err
		if e := s.onSendFail(bo, ctx, err); e != nil {
			trace_util_0.Count(_region_request_00000, 32)
			return nil, false, errors.Trace(e)
		}
		trace_util_0.Count(_region_request_00000, 31)
		return nil, true, nil
	}
	trace_util_0.Count(_region_request_00000, 28)
	return
}

func (s *RegionRequestSender) onSendFail(bo *Backoffer, ctx *RPCContext, err error) error {
	trace_util_0.Count(_region_request_00000, 33)
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		trace_util_0.Count(_region_request_00000, 36)
		return errors.Trace(err)
	} else {
		trace_util_0.Count(_region_request_00000, 37)
		if atomic.LoadUint32(&ShuttingDown) > 0 {
			trace_util_0.Count(_region_request_00000, 38)
			return errTiDBShuttingDown
		}
	}
	trace_util_0.Count(_region_request_00000, 34)
	if grpc.Code(errors.Cause(err)) == codes.Canceled {
		trace_util_0.Count(_region_request_00000, 39)
		select {
		case <-bo.ctx.Done():
			trace_util_0.Count(_region_request_00000, 40)
			return errors.Trace(err)
		default:
			trace_util_0.Count(_region_request_00000, 41)
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			logutil.Logger(context.Background()).Warn("receive a grpc cancel signal from remote", zap.Error(err))
		}
	}

	trace_util_0.Count(_region_request_00000, 35)
	s.regionCache.OnSendFail(bo, ctx, s.needReloadRegion(ctx), err)

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	err = bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %v, try next peer later", err, ctx))
	return errors.Trace(err)
}

// needReloadRegion checks is all peers has sent failed, if so need reload.
func (s *RegionRequestSender) needReloadRegion(ctx *RPCContext) (need bool) {
	trace_util_0.Count(_region_request_00000, 42)
	if s.failStoreIDs == nil {
		trace_util_0.Count(_region_request_00000, 45)
		s.failStoreIDs = make(map[uint64]struct{})
	}
	trace_util_0.Count(_region_request_00000, 43)
	s.failStoreIDs[ctx.Store.storeID] = struct{}{}
	need = len(s.failStoreIDs) == len(ctx.Meta.Peers)
	if need {
		trace_util_0.Count(_region_request_00000, 46)
		s.failStoreIDs = nil
	}
	trace_util_0.Count(_region_request_00000, 44)
	return
}

func regionErrorToLabel(e *errorpb.Error) string {
	trace_util_0.Count(_region_request_00000, 47)
	if e.GetNotLeader() != nil {
		trace_util_0.Count(_region_request_00000, 49)
		return "not_leader"
	} else {
		trace_util_0.Count(_region_request_00000, 50)
		if e.GetRegionNotFound() != nil {
			trace_util_0.Count(_region_request_00000, 51)
			return "region_not_found"
		} else {
			trace_util_0.Count(_region_request_00000, 52)
			if e.GetKeyNotInRegion() != nil {
				trace_util_0.Count(_region_request_00000, 53)
				return "key_not_in_region"
			} else {
				trace_util_0.Count(_region_request_00000, 54)
				if e.GetEpochNotMatch() != nil {
					trace_util_0.Count(_region_request_00000, 55)
					return "epoch_not_match"
				} else {
					trace_util_0.Count(_region_request_00000, 56)
					if e.GetServerIsBusy() != nil {
						trace_util_0.Count(_region_request_00000, 57)
						return "server_is_busy"
					} else {
						trace_util_0.Count(_region_request_00000, 58)
						if e.GetStaleCommand() != nil {
							trace_util_0.Count(_region_request_00000, 59)
							return "stale_command"
						} else {
							trace_util_0.Count(_region_request_00000, 60)
							if e.GetStoreNotMatch() != nil {
								trace_util_0.Count(_region_request_00000, 61)
								return "store_not_match"
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_region_request_00000, 48)
	return "unknown"
}

func (s *RegionRequestSender) onRegionError(bo *Backoffer, ctx *RPCContext, regionErr *errorpb.Error) (retry bool, err error) {
	trace_util_0.Count(_region_request_00000, 62)
	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrorToLabel(regionErr)).Inc()
	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		trace_util_0.Count(_region_request_00000, 69)
		// Retry if error is `NotLeader`.
		logutil.Logger(context.Background()).Debug("tikv reports `NotLeader` retry later",
			zap.String("notLeader", notLeader.String()),
			zap.String("ctx", ctx.String()))
		s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader().GetStoreId(), ctx.PeerIdx)

		var boType backoffType
		if notLeader.GetLeader() != nil {
			trace_util_0.Count(_region_request_00000, 72)
			boType = BoUpdateLeader
		} else {
			trace_util_0.Count(_region_request_00000, 73)
			{
				boType = BoRegionMiss
			}
		}

		trace_util_0.Count(_region_request_00000, 70)
		if err = bo.Backoff(boType, errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)); err != nil {
			trace_util_0.Count(_region_request_00000, 74)
			return false, errors.Trace(err)
		}

		trace_util_0.Count(_region_request_00000, 71)
		return true, nil
	}

	trace_util_0.Count(_region_request_00000, 63)
	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		trace_util_0.Count(_region_request_00000, 75)
		// store not match
		logutil.Logger(context.Background()).Warn("tikv reports `StoreNotMatch` retry later",
			zap.Stringer("storeNotMatch", storeNotMatch),
			zap.Stringer("ctx", ctx))
		ctx.Store.markNeedCheck(s.regionCache.notifyCheckCh)
		return true, nil
	}

	trace_util_0.Count(_region_request_00000, 64)
	if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
		trace_util_0.Count(_region_request_00000, 76)
		logutil.Logger(context.Background()).Debug("tikv reports `EpochNotMatch` retry later",
			zap.Stringer("EpochNotMatch", epochNotMatch),
			zap.Stringer("ctx", ctx))
		err = s.regionCache.OnRegionEpochNotMatch(bo, ctx, epochNotMatch.CurrentRegions)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_region_request_00000, 65)
	if regionErr.GetServerIsBusy() != nil {
		trace_util_0.Count(_region_request_00000, 77)
		logutil.Logger(context.Background()).Warn("tikv reports `ServerIsBusy` retry later",
			zap.String("reason", regionErr.GetServerIsBusy().GetReason()),
			zap.Stringer("ctx", ctx))
		err = bo.Backoff(boServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		if err != nil {
			trace_util_0.Count(_region_request_00000, 79)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_region_request_00000, 78)
		return true, nil
	}
	trace_util_0.Count(_region_request_00000, 66)
	if regionErr.GetStaleCommand() != nil {
		trace_util_0.Count(_region_request_00000, 80)
		logutil.Logger(context.Background()).Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
		return true, nil
	}
	trace_util_0.Count(_region_request_00000, 67)
	if regionErr.GetRaftEntryTooLarge() != nil {
		trace_util_0.Count(_region_request_00000, 81)
		logutil.Logger(context.Background()).Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", ctx))
		return false, errors.New(regionErr.String())
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	trace_util_0.Count(_region_request_00000, 68)
	logutil.Logger(context.Background()).Debug("tikv reports region error",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", ctx))
	s.regionCache.InvalidateCachedRegion(ctx.Region)
	return false, nil
}

func pbIsolationLevel(level kv.IsoLevel) kvrpcpb.IsolationLevel {
	trace_util_0.Count(_region_request_00000, 82)
	switch level {
	case kv.RC:
		trace_util_0.Count(_region_request_00000, 83)
		return kvrpcpb.IsolationLevel_RC
	case kv.SI:
		trace_util_0.Count(_region_request_00000, 84)
		return kvrpcpb.IsolationLevel_SI
	default:
		trace_util_0.Count(_region_request_00000, 85)
		return kvrpcpb.IsolationLevel_SI
	}
}

var _region_request_00000 = "store/tikv/region_request.go"
