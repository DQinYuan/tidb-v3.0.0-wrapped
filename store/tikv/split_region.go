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

package tikv

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitRegion splits the region contains splitKey into 2 regions: [start,
// splitKey) and [splitKey, end).
func (s *tikvStore) SplitRegion(splitKey kv.Key) error {
	trace_util_0.Count(_split_region_00000, 0)
	_, err := s.splitRegion(splitKey)
	return err
}

func (s *tikvStore) splitRegion(splitKey kv.Key) (*metapb.Region, error) {
	trace_util_0.Count(_split_region_00000, 1)
	logutil.Logger(context.Background()).Info("start split region",
		zap.Binary("at", splitKey))
	bo := NewBackoffer(context.Background(), splitRegionBackoff)
	sender := NewRegionRequestSender(s.regionCache, s.client)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdSplitRegion,
		SplitRegion: &kvrpcpb.SplitRegionRequest{
			SplitKey: splitKey,
		},
	}
	req.Context.Priority = kvrpcpb.CommandPri_Normal
	for {
		trace_util_0.Count(_split_region_00000, 2)
		loc, err := s.regionCache.LocateKey(bo, splitKey)
		if err != nil {
			trace_util_0.Count(_split_region_00000, 8)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_split_region_00000, 3)
		if bytes.Equal(splitKey, loc.StartKey) {
			trace_util_0.Count(_split_region_00000, 9)
			logutil.Logger(context.Background()).Info("skip split region",
				zap.Binary("at", splitKey))
			return nil, nil
		}
		trace_util_0.Count(_split_region_00000, 4)
		res, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_split_region_00000, 10)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_split_region_00000, 5)
		regionErr, err := res.GetRegionError()
		if err != nil {
			trace_util_0.Count(_split_region_00000, 11)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_split_region_00000, 6)
		if regionErr != nil {
			trace_util_0.Count(_split_region_00000, 12)
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_split_region_00000, 14)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_split_region_00000, 13)
			continue
		}
		trace_util_0.Count(_split_region_00000, 7)
		logutil.Logger(context.Background()).Info("split region complete",
			zap.Binary("at", splitKey),
			zap.Stringer("new region left", res.SplitRegion.GetLeft()),
			zap.Stringer("new region right", res.SplitRegion.GetRight()))
		return res.SplitRegion.GetLeft(), nil
	}
}

func (s *tikvStore) scatterRegion(regionID uint64) error {
	trace_util_0.Count(_split_region_00000, 15)
	logutil.Logger(context.Background()).Info("start scatter region",
		zap.Uint64("regionID", regionID))
	bo := NewBackoffer(context.Background(), scatterRegionBackoff)
	for {
		trace_util_0.Count(_split_region_00000, 17)
		err := s.pdClient.ScatterRegion(context.Background(), regionID)
		if err != nil {
			trace_util_0.Count(_split_region_00000, 19)
			err = bo.Backoff(BoRegionMiss, errors.New(err.Error()))
			if err != nil {
				trace_util_0.Count(_split_region_00000, 21)
				return errors.Trace(err)
			}
			trace_util_0.Count(_split_region_00000, 20)
			continue
		}
		trace_util_0.Count(_split_region_00000, 18)
		break
	}
	trace_util_0.Count(_split_region_00000, 16)
	logutil.Logger(context.Background()).Info("scatter region complete",
		zap.Uint64("regionID", regionID))
	return nil
}

func (s *tikvStore) WaitScatterRegionFinish(regionID uint64) error {
	trace_util_0.Count(_split_region_00000, 22)
	logutil.Logger(context.Background()).Info("wait scatter region",
		zap.Uint64("regionID", regionID))
	bo := NewBackoffer(context.Background(), waitScatterRegionFinishBackoff)
	logFreq := 0
	for {
		trace_util_0.Count(_split_region_00000, 23)
		resp, err := s.pdClient.GetOperator(context.Background(), regionID)
		if err == nil && resp != nil {
			trace_util_0.Count(_split_region_00000, 26)
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				trace_util_0.Count(_split_region_00000, 29)
				logutil.Logger(context.Background()).Info("wait scatter region finished",
					zap.Uint64("regionID", regionID))
				return nil
			}
			trace_util_0.Count(_split_region_00000, 27)
			if logFreq%10 == 0 {
				trace_util_0.Count(_split_region_00000, 30)
				logutil.Logger(context.Background()).Info("wait scatter region",
					zap.Uint64("regionID", regionID),
					zap.String("reverse", string(resp.Desc)),
					zap.String("status", pdpb.OperatorStatus_name[int32(resp.Status)]))
			}
			trace_util_0.Count(_split_region_00000, 28)
			logFreq++
		}
		trace_util_0.Count(_split_region_00000, 24)
		if err != nil {
			trace_util_0.Count(_split_region_00000, 31)
			err = bo.Backoff(BoRegionMiss, errors.New(err.Error()))
		} else {
			trace_util_0.Count(_split_region_00000, 32)
			{
				err = bo.Backoff(BoRegionMiss, errors.New("wait scatter region timeout"))
			}
		}
		trace_util_0.Count(_split_region_00000, 25)
		if err != nil {
			trace_util_0.Count(_split_region_00000, 33)
			return errors.Trace(err)
		}
	}

}

func (s *tikvStore) SplitRegionAndScatter(splitKey kv.Key) (uint64, error) {
	trace_util_0.Count(_split_region_00000, 34)
	left, err := s.splitRegion(splitKey)
	if err != nil {
		trace_util_0.Count(_split_region_00000, 38)
		return 0, err
	}
	trace_util_0.Count(_split_region_00000, 35)
	if left == nil {
		trace_util_0.Count(_split_region_00000, 39)
		return 0, nil
	}
	trace_util_0.Count(_split_region_00000, 36)
	err = s.scatterRegion(left.Id)
	if err != nil {
		trace_util_0.Count(_split_region_00000, 40)
		return 0, err
	}
	trace_util_0.Count(_split_region_00000, 37)
	return left.Id, nil
}

var _split_region_00000 = "store/tikv/split_region.go"
