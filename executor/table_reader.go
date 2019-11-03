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

package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// selectResultHook is used to hack distsql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
		fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []fmt.Stringer) (distsql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []fmt.Stringer) (distsql.SelectResult, error) {
	trace_util_0.Count(_table_reader_00000, 0)
	if sr.selectResultFunc == nil {
		trace_util_0.Count(_table_reader_00000, 2)
		return distsql.SelectWithRuntimeStats(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs)
	}
	trace_util_0.Count(_table_reader_00000, 1)
	return sr.selectResultFunc(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs)
}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table     table.Table
	keepOrder bool
	desc      bool
	ranges    []*ranger.Range
	dagPB     *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	streaming     bool
	feedback      *statistics.QueryFeedback

	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	plans          []plannercore.PhysicalPlan

	selectResultHook // for testing
}

// Open initialzes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	trace_util_0.Count(_table_reader_00000, 3)
	var err error
	if e.corColInFilter {
		trace_util_0.Count(_table_reader_00000, 11)
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			trace_util_0.Count(_table_reader_00000, 12)
			return err
		}
	}
	trace_util_0.Count(_table_reader_00000, 4)
	if e.runtimeStats != nil {
		trace_util_0.Count(_table_reader_00000, 13)
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	trace_util_0.Count(_table_reader_00000, 5)
	if e.corColInAccess {
		trace_util_0.Count(_table_reader_00000, 14)
		ts := e.plans[0].(*plannercore.PhysicalTableScan)
		access := ts.AccessCondition
		pkTP := ts.Table.GetPkColInfo().FieldType
		e.ranges, err = ranger.BuildTableRange(access, e.ctx.GetSessionVars().StmtCtx, &pkTP)
		if err != nil {
			trace_util_0.Count(_table_reader_00000, 15)
			return err
		}
	}

	trace_util_0.Count(_table_reader_00000, 6)
	e.resultHandler = &tableResultHandler{}
	if e.feedback != nil && e.feedback.Hist != nil {
		trace_util_0.Count(_table_reader_00000, 16)
		// EncodeInt don't need *statement.Context.
		var ok bool
		e.ranges, ok = e.feedback.Hist.SplitRange(nil, e.ranges, false)
		if !ok {
			trace_util_0.Count(_table_reader_00000, 17)
			e.feedback.Invalidate()
		}
	}
	trace_util_0.Count(_table_reader_00000, 7)
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder, e.desc)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		trace_util_0.Count(_table_reader_00000, 18)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_table_reader_00000, 8)
	if len(secondPartRanges) == 0 {
		trace_util_0.Count(_table_reader_00000, 19)
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	trace_util_0.Count(_table_reader_00000, 9)
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		trace_util_0.Count(_table_reader_00000, 20)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_table_reader_00000, 10)
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_table_reader_00000, 21)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_table_reader_00000, 25)
		span1 := span.Tracer().StartSpan("tableReader.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_table_reader_00000, 22)
	if e.runtimeStats != nil {
		trace_util_0.Count(_table_reader_00000, 26)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_table_reader_00000, 27)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_table_reader_00000, 23)
	if err := e.resultHandler.nextChunk(ctx, req.Chunk); err != nil {
		trace_util_0.Count(_table_reader_00000, 28)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_table_reader_00000, 24)
	return nil
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	trace_util_0.Count(_table_reader_00000, 29)
	err := e.resultHandler.Close()
	if e.runtimeStats != nil {
		trace_util_0.Count(_table_reader_00000, 31)
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.plans[0].ExplainID().String())
		copStats.SetRowNum(e.feedback.Actual())
	}
	trace_util_0.Count(_table_reader_00000, 30)
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

var tableReaderDistSQLTrackerLabel fmt.Stringer = stringutil.StringerStr("TableReaderDistSQLTracker")

// buildResp first builds request and sends it to tikv using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	trace_util_0.Count(_table_reader_00000, 32)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(getPhysicalTableID(e.table), ranges, e.feedback).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(e.ctx, tableReaderDistSQLTrackerLabel).
		Build()
	if err != nil {
		trace_util_0.Count(_table_reader_00000, 35)
		return nil, err
	}
	trace_util_0.Count(_table_reader_00000, 33)
	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans))
	if err != nil {
		trace_util_0.Count(_table_reader_00000, 36)
		return nil, err
	}
	trace_util_0.Count(_table_reader_00000, 34)
	result.Fetch(ctx)
	return result, nil
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true and want ascending order,
	// `optionalResult` will handles the request whose range is in signed int range, and
	// `result` will handle the request whose range is exceed signed int range.
	// If we want descending order, `optionalResult` will handles the request whose range is exceed signed, and
	// the `result` will handle the request whose range is in signed.
	// Otherwise, we just set `optionalFinished` true and the `result` handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	trace_util_0.Count(_table_reader_00000, 37)
	if optionalResult == nil {
		trace_util_0.Count(_table_reader_00000, 39)
		tr.optionalFinished = true
		tr.result = result
		return
	}
	trace_util_0.Count(_table_reader_00000, 38)
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_table_reader_00000, 40)
	if !tr.optionalFinished {
		trace_util_0.Count(_table_reader_00000, 42)
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			trace_util_0.Count(_table_reader_00000, 45)
			return err
		}
		trace_util_0.Count(_table_reader_00000, 43)
		if chk.NumRows() > 0 {
			trace_util_0.Count(_table_reader_00000, 46)
			return nil
		}
		trace_util_0.Count(_table_reader_00000, 44)
		tr.optionalFinished = true
	}
	trace_util_0.Count(_table_reader_00000, 41)
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	trace_util_0.Count(_table_reader_00000, 47)
	if !tr.optionalFinished {
		trace_util_0.Count(_table_reader_00000, 50)
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			trace_util_0.Count(_table_reader_00000, 53)
			return nil, err
		}
		trace_util_0.Count(_table_reader_00000, 51)
		if data != nil {
			trace_util_0.Count(_table_reader_00000, 54)
			return data, nil
		}
		trace_util_0.Count(_table_reader_00000, 52)
		tr.optionalFinished = true
	}
	trace_util_0.Count(_table_reader_00000, 48)
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		trace_util_0.Count(_table_reader_00000, 55)
		return nil, err
	}
	trace_util_0.Count(_table_reader_00000, 49)
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	trace_util_0.Count(_table_reader_00000, 56)
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}

var _table_reader_00000 = "executor/table_reader.go"
