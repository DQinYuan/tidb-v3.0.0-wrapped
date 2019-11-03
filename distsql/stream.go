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

package distsql

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

// streamResult implements the SelectResult interface.
type streamResult struct {
	resp       kv.Response
	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	// NOTE: curr == nil means stream finish, while len(curr.RowsData) == 0 doesn't.
	curr         *tipb.Chunk
	partialCount int64
	feedback     *statistics.QueryFeedback
}

func (r *streamResult) Fetch(context.Context) { trace_util_0.Count(_stream_00000, 0) }

func (r *streamResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_stream_00000, 1)
	chk.Reset()
	for !chk.IsFull() {
		trace_util_0.Count(_stream_00000, 3)
		err := r.readDataIfNecessary(ctx)
		if err != nil {
			trace_util_0.Count(_stream_00000, 6)
			return err
		}
		trace_util_0.Count(_stream_00000, 4)
		if r.curr == nil {
			trace_util_0.Count(_stream_00000, 7)
			return nil
		}

		trace_util_0.Count(_stream_00000, 5)
		err = r.flushToChunk(chk)
		if err != nil {
			trace_util_0.Count(_stream_00000, 8)
			return err
		}
	}
	trace_util_0.Count(_stream_00000, 2)
	return nil
}

// readDataFromResponse read the data to result. Returns true means the resp is finished.
func (r *streamResult) readDataFromResponse(ctx context.Context, resp kv.Response, result *tipb.Chunk) (bool, error) {
	trace_util_0.Count(_stream_00000, 9)
	resultSubset, err := resp.Next(ctx)
	if err != nil {
		trace_util_0.Count(_stream_00000, 16)
		return false, err
	}
	trace_util_0.Count(_stream_00000, 10)
	if resultSubset == nil {
		trace_util_0.Count(_stream_00000, 17)
		return true, nil
	}

	trace_util_0.Count(_stream_00000, 11)
	var stream tipb.StreamResponse
	err = stream.Unmarshal(resultSubset.GetData())
	if err != nil {
		trace_util_0.Count(_stream_00000, 18)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_stream_00000, 12)
	if stream.Error != nil {
		trace_util_0.Count(_stream_00000, 19)
		return false, errors.Errorf("stream response error: [%d]%s\n", stream.Error.Code, stream.Error.Msg)
	}
	trace_util_0.Count(_stream_00000, 13)
	for _, warning := range stream.Warnings {
		trace_util_0.Count(_stream_00000, 20)
		r.ctx.GetSessionVars().StmtCtx.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
	}

	trace_util_0.Count(_stream_00000, 14)
	err = result.Unmarshal(stream.Data)
	if err != nil {
		trace_util_0.Count(_stream_00000, 21)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_stream_00000, 15)
	r.feedback.Update(resultSubset.GetStartKey(), stream.OutputCounts)
	r.partialCount++
	return false, nil
}

// readDataIfNecessary ensures there are some data in current chunk. If no more data, r.curr == nil.
func (r *streamResult) readDataIfNecessary(ctx context.Context) error {
	trace_util_0.Count(_stream_00000, 22)
	if r.curr != nil && len(r.curr.RowsData) > 0 {
		trace_util_0.Count(_stream_00000, 26)
		return nil
	}

	trace_util_0.Count(_stream_00000, 23)
	tmp := new(tipb.Chunk)
	finish, err := r.readDataFromResponse(ctx, r.resp, tmp)
	if err != nil {
		trace_util_0.Count(_stream_00000, 27)
		return err
	}
	trace_util_0.Count(_stream_00000, 24)
	if finish {
		trace_util_0.Count(_stream_00000, 28)
		r.curr = nil
		return nil
	}
	trace_util_0.Count(_stream_00000, 25)
	r.curr = tmp
	return nil
}

func (r *streamResult) flushToChunk(chk *chunk.Chunk) (err error) {
	trace_util_0.Count(_stream_00000, 29)
	remainRowsData := r.curr.RowsData
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for !chk.IsFull() && len(remainRowsData) > 0 {
		trace_util_0.Count(_stream_00000, 32)
		for i := 0; i < r.rowLen; i++ {
			trace_util_0.Count(_stream_00000, 33)
			remainRowsData, err = decoder.DecodeOne(remainRowsData, i, r.fieldTypes[i])
			if err != nil {
				trace_util_0.Count(_stream_00000, 34)
				return err
			}
		}
	}
	trace_util_0.Count(_stream_00000, 30)
	if len(remainRowsData) == 0 {
		trace_util_0.Count(_stream_00000, 35)
		r.curr = nil // Current chunk is finished.
	} else {
		trace_util_0.Count(_stream_00000, 36)
		{
			r.curr.RowsData = remainRowsData
		}
	}
	trace_util_0.Count(_stream_00000, 31)
	return nil
}

func (r *streamResult) NextRaw(ctx context.Context) ([]byte, error) {
	trace_util_0.Count(_stream_00000, 37)
	r.partialCount++
	r.feedback.Invalidate()
	resultSubset, err := r.resp.Next(ctx)
	if resultSubset == nil || err != nil {
		trace_util_0.Count(_stream_00000, 39)
		return nil, err
	}
	trace_util_0.Count(_stream_00000, 38)
	return resultSubset.GetData(), err
}

func (r *streamResult) Close() error {
	trace_util_0.Count(_stream_00000, 40)
	if r.feedback.Actual() > 0 {
		trace_util_0.Count(_stream_00000, 42)
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	trace_util_0.Count(_stream_00000, 41)
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	return nil
}

var _stream_00000 = "distsql/stream.go"
