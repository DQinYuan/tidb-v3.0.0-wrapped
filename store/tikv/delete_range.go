// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
)

// DeleteRangeTask is used to delete all keys in a range. After
// performing DeleteRange, it keeps how many ranges it affects and
// if the task was canceled or not.
type DeleteRangeTask struct {
	completedRegions int
	store            Storage
	startKey         []byte
	endKey           []byte
	notifyOnly       bool
	concurrency      int
}

// NewDeleteRangeTask creates a DeleteRangeTask. Deleting will be performed when `Execute` method is invoked.
// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to TiKV.
func NewDeleteRangeTask(store Storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	trace_util_0.Count(_delete_range_00000, 0)
	return &DeleteRangeTask{
		completedRegions: 0,
		store:            store,
		startKey:         startKey,
		endKey:           endKey,
		notifyOnly:       false,
		concurrency:      concurrency,
	}
}

// NewNotifyDeleteRangeTask creates a task that sends delete range requests to all regions in the range, but with the
// flag `notifyOnly` set. TiKV will not actually delete the range after receiving request, but it will be replicated via
// raft. This is used to notify the involved regions before sending UnsafeDestroyRange requests.
func NewNotifyDeleteRangeTask(store Storage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	trace_util_0.Count(_delete_range_00000, 1)
	task := NewDeleteRangeTask(store, startKey, endKey, concurrency)
	task.notifyOnly = true
	return task
}

// getRunnerName returns a name for RangeTaskRunner.
func (t *DeleteRangeTask) getRunnerName() string {
	trace_util_0.Count(_delete_range_00000, 2)
	if t.notifyOnly {
		trace_util_0.Count(_delete_range_00000, 4)
		return "delete-range-notify"
	}
	trace_util_0.Count(_delete_range_00000, 3)
	return "delete-range"
}

// Execute performs the delete range operation.
func (t *DeleteRangeTask) Execute(ctx context.Context) error {
	trace_util_0.Count(_delete_range_00000, 5)
	runnerName := t.getRunnerName()

	runner := NewRangeTaskRunner(runnerName, t.store, t.concurrency, t.sendReqOnRange)
	err := runner.RunOnRange(ctx, t.startKey, t.endKey)
	t.completedRegions = int(runner.CompletedRegions())

	return err
}

// Execute performs the delete range operation.
func (t *DeleteRangeTask) sendReqOnRange(ctx context.Context, r kv.KeyRange) (int, error) {
	trace_util_0.Count(_delete_range_00000, 6)
	startKey, rangeEndKey := r.StartKey, r.EndKey
	completedRegions := 0
	for {
		trace_util_0.Count(_delete_range_00000, 8)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_delete_range_00000, 18)
			return completedRegions, errors.Trace(ctx.Err())
		default:
			trace_util_0.Count(_delete_range_00000, 19)
		}

		trace_util_0.Count(_delete_range_00000, 9)
		if bytes.Compare(startKey, rangeEndKey) >= 0 {
			trace_util_0.Count(_delete_range_00000, 20)
			break
		}

		trace_util_0.Count(_delete_range_00000, 10)
		bo := NewBackoffer(ctx, deleteRangeOneRegionMaxBackoff)
		loc, err := t.store.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			trace_util_0.Count(_delete_range_00000, 21)
			return completedRegions, errors.Trace(err)
		}

		// Delete to the end of the region, except if it's the last region overlapping the range
		trace_util_0.Count(_delete_range_00000, 11)
		endKey := loc.EndKey
		// If it is the last region
		if loc.Contains(rangeEndKey) {
			trace_util_0.Count(_delete_range_00000, 22)
			endKey = rangeEndKey
		}

		trace_util_0.Count(_delete_range_00000, 12)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDeleteRange,
			DeleteRange: &kvrpcpb.DeleteRangeRequest{
				StartKey:   startKey,
				EndKey:     endKey,
				NotifyOnly: t.notifyOnly,
			},
		}

		resp, err := t.store.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			trace_util_0.Count(_delete_range_00000, 23)
			return completedRegions, errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 13)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_delete_range_00000, 24)
			return completedRegions, errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 14)
		if regionErr != nil {
			trace_util_0.Count(_delete_range_00000, 25)
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_delete_range_00000, 27)
				return completedRegions, errors.Trace(err)
			}
			trace_util_0.Count(_delete_range_00000, 26)
			continue
		}
		trace_util_0.Count(_delete_range_00000, 15)
		deleteRangeResp := resp.DeleteRange
		if deleteRangeResp == nil {
			trace_util_0.Count(_delete_range_00000, 28)
			return completedRegions, errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_delete_range_00000, 16)
		if err := deleteRangeResp.GetError(); err != "" {
			trace_util_0.Count(_delete_range_00000, 29)
			return completedRegions, errors.Errorf("unexpected delete range err: %v", err)
		}
		trace_util_0.Count(_delete_range_00000, 17)
		completedRegions++
		startKey = endKey
	}

	trace_util_0.Count(_delete_range_00000, 7)
	return completedRegions, nil
}

// CompletedRegions returns the number of regions that are affected by this delete range task
func (t *DeleteRangeTask) CompletedRegions() int {
	trace_util_0.Count(_delete_range_00000, 30)
	return t.completedRegions
}

var _delete_range_00000 = "store/tikv/delete_range.go"
