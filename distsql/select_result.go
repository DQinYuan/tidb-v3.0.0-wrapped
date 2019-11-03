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

package distsql

import (
	"context"
	"fmt"
	"time"

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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*streamResult)(nil)
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Fetch fetches partial results from client.
	Fetch(context.Context)
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
}

type resultWithErr struct {
	result kv.ResultSubset
	err    error
}

type selectResult struct {
	label string
	resp  kv.Response

	results chan resultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	selectResp *tipb.SelectResponse
	respChkIdx int

	feedback     *statistics.QueryFeedback
	partialCount int64 // number of partial results.
	sqlType      string

	// copPlanIDs contains all copTasks' planIDs,
	// which help to collect copTasks' runtime stats.
	copPlanIDs []fmt.Stringer

	memTracker *memory.Tracker
}

func (r *selectResult) Fetch(ctx context.Context) {
	trace_util_0.Count(_select_result_00000, 0)
	go r.fetch(ctx)
}

func (r *selectResult) fetch(ctx context.Context) {
	trace_util_0.Count(_select_result_00000, 1)
	startTime := time.Now()
	defer func() {
		trace_util_0.Count(_select_result_00000, 3)
		if c := recover(); c != nil {
			trace_util_0.Count(_select_result_00000, 5)
			err := fmt.Errorf("%v", c)
			logutil.Logger(ctx).Error("OOM", zap.Error(err))
			r.results <- resultWithErr{err: err}
		}

		trace_util_0.Count(_select_result_00000, 4)
		close(r.results)
		duration := time.Since(startTime)
		metrics.DistSQLQueryHistgram.WithLabelValues(r.label, r.sqlType).Observe(duration.Seconds())
	}()
	trace_util_0.Count(_select_result_00000, 2)
	for {
		trace_util_0.Count(_select_result_00000, 6)
		var result resultWithErr
		resultSubset, err := r.resp.Next(ctx)
		if err != nil {
			trace_util_0.Count(_select_result_00000, 8)
			result.err = err
		} else {
			trace_util_0.Count(_select_result_00000, 9)
			if resultSubset == nil {
				trace_util_0.Count(_select_result_00000, 10)
				return
			} else {
				trace_util_0.Count(_select_result_00000, 11)
				{
					result.result = resultSubset
					if r.memTracker != nil {
						trace_util_0.Count(_select_result_00000, 12)
						r.memTracker.Consume(int64(resultSubset.MemSize()))
					}
				}
			}
		}

		trace_util_0.Count(_select_result_00000, 7)
		select {
		case r.results <- result:
			trace_util_0.Count(_select_result_00000, 13)
		case <-r.closed:
			trace_util_0.Count(_select_result_00000, 14)
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-ctx.Done():
			trace_util_0.Count(_select_result_00000, 15)
			return
		}
	}
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) ([]byte, error) {
	trace_util_0.Count(_select_result_00000, 16)
	re := <-r.results
	r.partialCount++
	r.feedback.Invalidate()
	if re.result == nil || re.err != nil {
		trace_util_0.Count(_select_result_00000, 18)
		return nil, errors.Trace(re.err)
	}
	trace_util_0.Count(_select_result_00000, 17)
	return re.result.GetData(), nil
}

// Next reads data to the chunk.
func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_select_result_00000, 19)
	chk.Reset()
	for !chk.IsFull() {
		trace_util_0.Count(_select_result_00000, 21)
		if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
			trace_util_0.Count(_select_result_00000, 24)
			err := r.getSelectResp()
			if err != nil || r.selectResp == nil {
				trace_util_0.Count(_select_result_00000, 25)
				return err
			}
		}
		trace_util_0.Count(_select_result_00000, 22)
		err := r.readRowsData(chk)
		if err != nil {
			trace_util_0.Count(_select_result_00000, 26)
			return err
		}
		trace_util_0.Count(_select_result_00000, 23)
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			trace_util_0.Count(_select_result_00000, 27)
			r.respChkIdx++
		}
	}
	trace_util_0.Count(_select_result_00000, 20)
	return nil
}

func (r *selectResult) getSelectResp() error {
	trace_util_0.Count(_select_result_00000, 28)
	r.respChkIdx = 0
	for {
		trace_util_0.Count(_select_result_00000, 29)
		re := <-r.results
		if re.err != nil {
			trace_util_0.Count(_select_result_00000, 39)
			return errors.Trace(re.err)
		}
		trace_util_0.Count(_select_result_00000, 30)
		if r.memTracker != nil && r.selectResp != nil {
			trace_util_0.Count(_select_result_00000, 40)
			r.memTracker.Consume(-int64(r.selectResp.Size()))
		}
		trace_util_0.Count(_select_result_00000, 31)
		if re.result == nil {
			trace_util_0.Count(_select_result_00000, 41)
			r.selectResp = nil
			return nil
		}
		trace_util_0.Count(_select_result_00000, 32)
		if r.memTracker != nil {
			trace_util_0.Count(_select_result_00000, 42)
			r.memTracker.Consume(-int64(re.result.MemSize()))
		}
		trace_util_0.Count(_select_result_00000, 33)
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result.GetData())
		if err != nil {
			trace_util_0.Count(_select_result_00000, 43)
			return errors.Trace(err)
		}
		trace_util_0.Count(_select_result_00000, 34)
		if r.memTracker != nil && r.selectResp != nil {
			trace_util_0.Count(_select_result_00000, 44)
			r.memTracker.Consume(int64(r.selectResp.Size()))
		}
		trace_util_0.Count(_select_result_00000, 35)
		if err := r.selectResp.Error; err != nil {
			trace_util_0.Count(_select_result_00000, 45)
			return terror.ClassTiKV.New(terror.ErrCode(err.Code), err.Msg)
		}
		trace_util_0.Count(_select_result_00000, 36)
		sc := r.ctx.GetSessionVars().StmtCtx
		for _, warning := range r.selectResp.Warnings {
			trace_util_0.Count(_select_result_00000, 46)
			sc.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
		}
		trace_util_0.Count(_select_result_00000, 37)
		r.updateCopRuntimeStats(re.result.GetExecDetails().CalleeAddress)
		r.feedback.Update(re.result.GetStartKey(), r.selectResp.OutputCounts)
		r.partialCount++
		sc.MergeExecDetails(re.result.GetExecDetails(), nil)
		if len(r.selectResp.Chunks) == 0 {
			trace_util_0.Count(_select_result_00000, 47)
			continue
		}
		trace_util_0.Count(_select_result_00000, 38)
		return nil
	}
}

func (r *selectResult) updateCopRuntimeStats(callee string) {
	trace_util_0.Count(_select_result_00000, 48)
	if r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl == nil || callee == "" {
		trace_util_0.Count(_select_result_00000, 51)
		return
	}
	trace_util_0.Count(_select_result_00000, 49)
	if len(r.selectResp.GetExecutionSummaries()) != len(r.copPlanIDs) {
		trace_util_0.Count(_select_result_00000, 52)
		logutil.Logger(context.Background()).Error("invalid cop task execution summaries length",
			zap.Int("expected", len(r.copPlanIDs)),
			zap.Int("received", len(r.selectResp.GetExecutionSummaries())))

		return
	}

	trace_util_0.Count(_select_result_00000, 50)
	for i, detail := range r.selectResp.GetExecutionSummaries() {
		trace_util_0.Count(_select_result_00000, 53)
		if detail != nil && detail.TimeProcessedNs != nil &&
			detail.NumProducedRows != nil && detail.NumIterations != nil {
			trace_util_0.Count(_select_result_00000, 54)
			planID := r.copPlanIDs[i]
			r.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
				RecordOneCopTask(planID.String(), callee, detail)
		}
	}
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	trace_util_0.Count(_select_result_00000, 55)
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for !chk.IsFull() && len(rowsData) > 0 {
		trace_util_0.Count(_select_result_00000, 57)
		for i := 0; i < r.rowLen; i++ {
			trace_util_0.Count(_select_result_00000, 58)
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				trace_util_0.Count(_select_result_00000, 59)
				return err
			}
		}
	}
	trace_util_0.Count(_select_result_00000, 56)
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	trace_util_0.Count(_select_result_00000, 60)
	// Close this channel tell fetch goroutine to exit.
	if r.feedback.Actual() >= 0 {
		trace_util_0.Count(_select_result_00000, 62)
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	trace_util_0.Count(_select_result_00000, 61)
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	close(r.closed)
	return r.resp.Close()
}

var _select_result_00000 = "distsql/select_result.go"
