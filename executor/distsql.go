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

package executor

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	handles []int64
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	cursor  int

	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder map[int64]int

	// memUsage records the memory usage of this task calculated by table worker.
	// memTracker is used to release memUsage after task is done and unused.
	//
	// The sequence of function calls are:
	//   1. calculate task.memUsage.
	//   2. task.memTracker = tableWorker.memTracker
	//   3. task.memTracker.Consume(task.memUsage)
	//   4. task.memTracker.Consume(-task.memUsage)
	//
	// Step 1~3 are completed in "tableWorker.executeTask".
	// Step 4   is  completed in "IndexLookUpExecutor.Next".
	memUsage   int64
	memTracker *memory.Tracker
}

func (task *lookupTableTask) Len() int {
	trace_util_0.Count(_distsql_00000, 0)
	return len(task.rows)
}

func (task *lookupTableTask) Less(i, j int) bool {
	trace_util_0.Count(_distsql_00000, 1)
	return task.rowIdx[i] < task.rowIdx[j]
}

func (task *lookupTableTask) Swap(i, j int) {
	trace_util_0.Count(_distsql_00000, 2)
	task.rowIdx[i], task.rowIdx[j] = task.rowIdx[j], task.rowIdx[i]
	task.rows[i], task.rows[j] = task.rows[j], task.rows[i]
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	trace_util_0.Count(_distsql_00000, 3)
	var err error
	for _, obj := range objs {
		trace_util_0.Count(_distsql_00000, 6)
		if obj != nil {
			trace_util_0.Count(_distsql_00000, 7)
			err1 := obj.Close()
			if err == nil && err1 != nil {
				trace_util_0.Count(_distsql_00000, 8)
				err = err1
			}
		}
	}
	trace_util_0.Count(_distsql_00000, 4)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 9)
		return errors.Trace(err)
	}
	trace_util_0.Count(_distsql_00000, 5)
	return nil
}

// statementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func statementContextToFlags(sc *stmtctx.StatementContext) uint64 {
	trace_util_0.Count(_distsql_00000, 10)
	var flags uint64
	if sc.InInsertStmt {
		trace_util_0.Count(_distsql_00000, 17)
		flags |= model.FlagInInsertStmt
	} else {
		trace_util_0.Count(_distsql_00000, 18)
		if sc.InUpdateStmt || sc.InDeleteStmt {
			trace_util_0.Count(_distsql_00000, 19)
			flags |= model.FlagInUpdateOrDeleteStmt
		} else {
			trace_util_0.Count(_distsql_00000, 20)
			if sc.InSelectStmt {
				trace_util_0.Count(_distsql_00000, 21)
				flags |= model.FlagInSelectStmt
			}
		}
	}
	trace_util_0.Count(_distsql_00000, 11)
	if sc.IgnoreTruncate {
		trace_util_0.Count(_distsql_00000, 22)
		flags |= model.FlagIgnoreTruncate
	} else {
		trace_util_0.Count(_distsql_00000, 23)
		if sc.TruncateAsWarning {
			trace_util_0.Count(_distsql_00000, 24)
			flags |= model.FlagTruncateAsWarning
		}
	}
	trace_util_0.Count(_distsql_00000, 12)
	if sc.OverflowAsWarning {
		trace_util_0.Count(_distsql_00000, 25)
		flags |= model.FlagOverflowAsWarning
	}
	trace_util_0.Count(_distsql_00000, 13)
	if sc.IgnoreZeroInDate {
		trace_util_0.Count(_distsql_00000, 26)
		flags |= model.FlagIgnoreZeroInDate
	}
	trace_util_0.Count(_distsql_00000, 14)
	if sc.DividedByZeroAsWarning {
		trace_util_0.Count(_distsql_00000, 27)
		flags |= model.FlagDividedByZeroAsWarning
	}
	trace_util_0.Count(_distsql_00000, 15)
	if sc.PadCharToFullLength {
		trace_util_0.Count(_distsql_00000, 28)
		flags |= model.FlagPadCharToFullLength
	}
	trace_util_0.Count(_distsql_00000, 16)
	return flags
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	trace_util_0.Count(_distsql_00000, 29)
	if col != nil && col.ID == model.ExtraHandleID {
		trace_util_0.Count(_distsql_00000, 31)
		return true
	}
	trace_util_0.Count(_distsql_00000, 30)
	return false
}

func splitRanges(ranges []*ranger.Range, keepOrder bool, desc bool) ([]*ranger.Range, []*ranger.Range) {
	trace_util_0.Count(_distsql_00000, 32)
	if len(ranges) == 0 || ranges[0].LowVal[0].Kind() == types.KindInt64 {
		trace_util_0.Count(_distsql_00000, 42)
		return ranges, nil
	}
	trace_util_0.Count(_distsql_00000, 33)
	idx := sort.Search(len(ranges), func(i int) bool {
		trace_util_0.Count(_distsql_00000, 43)
		return ranges[i].HighVal[0].GetUint64() > math.MaxInt64
	})
	trace_util_0.Count(_distsql_00000, 34)
	if idx == len(ranges) {
		trace_util_0.Count(_distsql_00000, 44)
		return ranges, nil
	}
	trace_util_0.Count(_distsql_00000, 35)
	if ranges[idx].LowVal[0].GetUint64() > math.MaxInt64 {
		trace_util_0.Count(_distsql_00000, 45)
		signedRanges := ranges[0:idx]
		unsignedRanges := ranges[idx:]
		if !keepOrder {
			trace_util_0.Count(_distsql_00000, 48)
			return append(unsignedRanges, signedRanges...), nil
		}
		trace_util_0.Count(_distsql_00000, 46)
		if desc {
			trace_util_0.Count(_distsql_00000, 49)
			return unsignedRanges, signedRanges
		}
		trace_util_0.Count(_distsql_00000, 47)
		return signedRanges, unsignedRanges
	}
	trace_util_0.Count(_distsql_00000, 36)
	signedRanges := make([]*ranger.Range, 0, idx+1)
	unsignedRanges := make([]*ranger.Range, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	if !(ranges[idx].LowVal[0].GetUint64() == math.MaxInt64 && ranges[idx].LowExclude) {
		trace_util_0.Count(_distsql_00000, 50)
		signedRanges = append(signedRanges, &ranger.Range{
			LowVal:     ranges[idx].LowVal,
			LowExclude: ranges[idx].LowExclude,
			HighVal:    []types.Datum{types.NewUintDatum(math.MaxInt64)},
		})
	}
	trace_util_0.Count(_distsql_00000, 37)
	if !(ranges[idx].HighVal[0].GetUint64() == math.MaxInt64+1 && ranges[idx].HighExclude) {
		trace_util_0.Count(_distsql_00000, 51)
		unsignedRanges = append(unsignedRanges, &ranger.Range{
			LowVal:      []types.Datum{types.NewUintDatum(math.MaxInt64 + 1)},
			HighVal:     ranges[idx].HighVal,
			HighExclude: ranges[idx].HighExclude,
		})
	}
	trace_util_0.Count(_distsql_00000, 38)
	if idx < len(ranges) {
		trace_util_0.Count(_distsql_00000, 52)
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	trace_util_0.Count(_distsql_00000, 39)
	if !keepOrder {
		trace_util_0.Count(_distsql_00000, 53)
		return append(unsignedRanges, signedRanges...), nil
	}
	trace_util_0.Count(_distsql_00000, 40)
	if desc {
		trace_util_0.Count(_distsql_00000, 54)
		return unsignedRanges, signedRanges
	}
	trace_util_0.Count(_distsql_00000, 41)
	return signedRanges, unsignedRanges
}

// rebuildIndexRanges will be called if there's correlated column in access conditions. We will rebuild the range
// by substitute correlated column with the constant.
func rebuildIndexRanges(ctx sessionctx.Context, is *plannercore.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
	trace_util_0.Count(_distsql_00000, 55)
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		trace_util_0.Count(_distsql_00000, 57)
		newCond, err1 := expression.SubstituteCorCol2Constant(cond)
		if err1 != nil {
			trace_util_0.Count(_distsql_00000, 59)
			return nil, err1
		}
		trace_util_0.Count(_distsql_00000, 58)
		access = append(access, newCond)
	}
	trace_util_0.Count(_distsql_00000, 56)
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(ctx, access, idxCols, colLens)
	return ranges, err
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	baseExecutor

	// For a partitioned table, the IndexReaderExecutor works on a partition, so
	// the type of this table field is actually `table.PhysicalTable`.
	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// columns are only required by union scan.
	columns   []*model.ColumnInfo
	streaming bool
	feedback  *statistics.QueryFeedback

	corColInFilter bool
	corColInAccess bool
	idxCols        []*expression.Column
	colLens        []int
	plans          []plannercore.PhysicalPlan

	selectResultHook // for testing
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() error {
	trace_util_0.Count(_distsql_00000, 60)
	err := e.result.Close()
	e.result = nil
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 62)
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.plans[0].ExplainID().String())
		copStats.SetRowNum(e.feedback.Actual())
	}
	trace_util_0.Count(_distsql_00000, 61)
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_distsql_00000, 63)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_distsql_00000, 67)
		span1 := span.Tracer().StartSpan("tableReader.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_distsql_00000, 64)
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 68)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_distsql_00000, 69)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_distsql_00000, 65)
	err := e.result.Next(ctx, req.Chunk)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 70)
		e.feedback.Invalidate()
	}
	trace_util_0.Count(_distsql_00000, 66)
	return err
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	trace_util_0.Count(_distsql_00000, 71)
	var err error
	if e.corColInAccess {
		trace_util_0.Count(_distsql_00000, 74)
		e.ranges, err = rebuildIndexRanges(e.ctx, e.plans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 75)
			return err
		}
	}
	trace_util_0.Count(_distsql_00000, 72)
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, e.ranges, e.feedback)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 76)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_distsql_00000, 73)
	return e.open(ctx, kvRanges)
}

var indexReaderDistSQLTrackerLabel fmt.Stringer = stringutil.StringerStr("IndexReaderDistSQLTracker")

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	trace_util_0.Count(_distsql_00000, 77)
	var err error
	if e.corColInFilter {
		trace_util_0.Count(_distsql_00000, 82)
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 83)
			return err
		}
	}

	trace_util_0.Count(_distsql_00000, 78)
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 84)
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}

	trace_util_0.Count(_distsql_00000, 79)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(e.ctx, indexReaderDistSQLTrackerLabel).
		Build()
	if err != nil {
		trace_util_0.Count(_distsql_00000, 85)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_distsql_00000, 80)
	e.result, err = e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans))
	if err != nil {
		trace_util_0.Count(_distsql_00000, 86)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_distsql_00000, 81)
	e.result.Fetch(ctx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	baseExecutor

	table     table.Table
	index     *model.IndexInfo
	keepOrder bool
	desc      bool
	ranges    []*ranger.Range
	dagPB     *tipb.DAGRequest
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx    int
	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns        []*model.ColumnInfo
	indexStreaming bool
	tableStreaming bool
	*dataReaderBuilder
	// All fields above are immutable.

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	finished    chan struct{}

	kvRanges      []kv.KeyRange
	workerStarted bool

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
	feedback   *statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// isCheckOp is used to determine whether we need to check the consistency of the index data.
	isCheckOp bool

	corColInIdxSide bool
	idxPlans        []plannercore.PhysicalPlan
	corColInTblSide bool
	tblPlans        []plannercore.PhysicalPlan
	corColInAccess  bool
	idxCols         []*expression.Column
	colLens         []int
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(ctx context.Context) error {
	trace_util_0.Count(_distsql_00000, 87)
	var err error
	if e.corColInAccess {
		trace_util_0.Count(_distsql_00000, 91)
		e.ranges, err = rebuildIndexRanges(e.ctx, e.idxPlans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 92)
			return err
		}
	}
	trace_util_0.Count(_distsql_00000, 88)
	e.kvRanges, err = distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, getPhysicalTableID(e.table), e.index.ID, e.ranges, e.feedback)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 93)
		e.feedback.Invalidate()
		return err
	}
	trace_util_0.Count(_distsql_00000, 89)
	err = e.open(ctx)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 94)
		e.feedback.Invalidate()
	}
	trace_util_0.Count(_distsql_00000, 90)
	return err
}

func (e *IndexLookUpExecutor) open(ctx context.Context) error {
	trace_util_0.Count(_distsql_00000, 95)
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupReader)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	var err error
	if e.corColInIdxSide {
		trace_util_0.Count(_distsql_00000, 98)
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.idxPlans)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 99)
			return err
		}
	}

	trace_util_0.Count(_distsql_00000, 96)
	if e.corColInTblSide {
		trace_util_0.Count(_distsql_00000, 100)
		e.tableRequest.Executors, _, err = constructDistExec(e.ctx, e.tblPlans)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 101)
			return err
		}
	}
	trace_util_0.Count(_distsql_00000, 97)
	return nil
}

func (e *IndexLookUpExecutor) startWorkers(ctx context.Context, initBatchSize int) error {
	trace_util_0.Count(_distsql_00000, 102)
	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	if err := e.startIndexWorker(ctx, e.kvRanges, workCh, initBatchSize); err != nil {
		trace_util_0.Count(_distsql_00000, 104)
		return err
	}
	trace_util_0.Count(_distsql_00000, 103)
	e.startTableWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

var indexLookupDistSQLTrackerLabel fmt.Stringer = stringutil.StringerStr("IndexLookupDistSQLTracker")

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask, initBatchSize int) error {
	trace_util_0.Count(_distsql_00000, 105)
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 111)
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}

	trace_util_0.Count(_distsql_00000, 106)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.indexStreaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(e.ctx, indexLookupDistSQLTrackerLabel).
		Build()
	if err != nil {
		trace_util_0.Count(_distsql_00000, 112)
		return err
	}
	// Since the first read only need handle information. So its returned col is only 1.
	trace_util_0.Count(_distsql_00000, 107)
	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.feedback, getPhysicalPlanIDs(e.idxPlans))
	if err != nil {
		trace_util_0.Count(_distsql_00000, 113)
		return err
	}
	trace_util_0.Count(_distsql_00000, 108)
	result.Fetch(ctx)
	worker := &indexWorker{
		idxLookup:    e,
		workCh:       workCh,
		finished:     e.finished,
		resultCh:     e.resultCh,
		keepOrder:    e.keepOrder,
		batchSize:    initBatchSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}
	if worker.batchSize > worker.maxBatchSize {
		trace_util_0.Count(_distsql_00000, 114)
		worker.batchSize = worker.maxBatchSize
	}
	trace_util_0.Count(_distsql_00000, 109)
	e.idxWorkerWg.Add(1)
	go func() {
		trace_util_0.Count(_distsql_00000, 115)
		ctx1, cancel := context.WithCancel(ctx)
		count, err := worker.fetchHandles(ctx1, result)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 119)
			e.feedback.Invalidate()
		}
		trace_util_0.Count(_distsql_00000, 116)
		cancel()
		if err := result.Close(); err != nil {
			trace_util_0.Count(_distsql_00000, 120)
			logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
		}
		trace_util_0.Count(_distsql_00000, 117)
		if e.runtimeStats != nil {
			trace_util_0.Count(_distsql_00000, 121)
			copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.idxPlans[len(e.idxPlans)-1].ExplainID().String())
			copStats.SetRowNum(count)
			copStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.tblPlans[0].ExplainID().String())
			copStats.SetRowNum(count)
		}
		trace_util_0.Count(_distsql_00000, 118)
		e.ctx.StoreQueryFeedback(e.feedback)
		close(workCh)
		close(e.resultCh)
		e.idxWorkerWg.Done()
	}()
	trace_util_0.Count(_distsql_00000, 110)
	return nil
}

var tableWorkerLabel fmt.Stringer = stringutil.StringerStr("tableWorker")

// startTableWorker launchs some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startTableWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	trace_util_0.Count(_distsql_00000, 122)
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		trace_util_0.Count(_distsql_00000, 123)
		worker := &tableWorker{
			idxLookup:      e,
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildTableReader,
			keepOrder:      e.keepOrder,
			handleIdx:      e.handleIdx,
			isCheckOp:      e.isCheckOp,
			memTracker:     memory.NewTracker(tableWorkerLabel, -1),
		}
		worker.memTracker.AttachTo(e.memTracker)
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			trace_util_0.Count(_distsql_00000, 124)
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, handles []int64) (Executor, error) {
	trace_util_0.Count(_distsql_00000, 125)
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:   newBaseExecutor(e.ctx, e.schema, stringutil.MemoizeStr(func() string { trace_util_0.Count(_distsql_00000, 128); return e.id.String() + "_tableReader" })),
		table:          e.table,
		dagPB:          e.tableRequest,
		streaming:      e.tableStreaming,
		feedback:       statistics.NewQueryFeedback(0, nil, 0, false),
		corColInFilter: e.corColInTblSide,
		plans:          e.tblPlans,
	}
	trace_util_0.Count(_distsql_00000, 126)
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 129)
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	trace_util_0.Count(_distsql_00000, 127)
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	trace_util_0.Count(_distsql_00000, 130)
	if !e.workerStarted || e.finished == nil {
		trace_util_0.Count(_distsql_00000, 134)
		return nil
	}

	trace_util_0.Count(_distsql_00000, 131)
	close(e.finished)
	// Drain the resultCh and discard the result, in case that Next() doesn't fully
	// consume the data, background worker still writing to resultCh and block forever.
	for range e.resultCh {
		trace_util_0.Count(_distsql_00000, 135)
	}
	trace_util_0.Count(_distsql_00000, 132)
	e.idxWorkerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	e.memTracker.Detach()
	e.memTracker = nil
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 136)
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.idxPlans[0].ExplainID().String())
		copStats.SetRowNum(e.feedback.Actual())
	}
	trace_util_0.Count(_distsql_00000, 133)
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_distsql_00000, 137)
	if e.runtimeStats != nil {
		trace_util_0.Count(_distsql_00000, 140)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_distsql_00000, 141)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_distsql_00000, 138)
	if !e.workerStarted {
		trace_util_0.Count(_distsql_00000, 142)
		if err := e.startWorkers(ctx, req.RequiredRows()); err != nil {
			trace_util_0.Count(_distsql_00000, 143)
			return err
		}
	}
	trace_util_0.Count(_distsql_00000, 139)
	req.Reset()
	for {
		trace_util_0.Count(_distsql_00000, 144)
		resultTask, err := e.getResultTask()
		if err != nil {
			trace_util_0.Count(_distsql_00000, 147)
			return err
		}
		trace_util_0.Count(_distsql_00000, 145)
		if resultTask == nil {
			trace_util_0.Count(_distsql_00000, 148)
			return nil
		}
		trace_util_0.Count(_distsql_00000, 146)
		for resultTask.cursor < len(resultTask.rows) {
			trace_util_0.Count(_distsql_00000, 149)
			req.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.IsFull() {
				trace_util_0.Count(_distsql_00000, 150)
				return nil
			}
		}
	}
}

func (e *IndexLookUpExecutor) getResultTask() (*lookupTableTask, error) {
	trace_util_0.Count(_distsql_00000, 151)
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		trace_util_0.Count(_distsql_00000, 156)
		return e.resultCurr, nil
	}
	trace_util_0.Count(_distsql_00000, 152)
	task, ok := <-e.resultCh
	if !ok {
		trace_util_0.Count(_distsql_00000, 157)
		return nil, nil
	}
	trace_util_0.Count(_distsql_00000, 153)
	if err := <-task.doneCh; err != nil {
		trace_util_0.Count(_distsql_00000, 158)
		return nil, err
	}

	// Release the memory usage of last task before we handle a new task.
	trace_util_0.Count(_distsql_00000, 154)
	if e.resultCurr != nil {
		trace_util_0.Count(_distsql_00000, 159)
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	trace_util_0.Count(_distsql_00000, 155)
	e.resultCurr = task
	return e.resultCurr, nil
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	idxLookup *IndexLookUpExecutor
	workCh    chan<- *lookupTableTask
	finished  <-chan struct{}
	resultCh  chan<- *lookupTableTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) (count int64, err error) {
	trace_util_0.Count(_distsql_00000, 160)
	defer func() {
		trace_util_0.Count(_distsql_00000, 162)
		if r := recover(); r != nil {
			trace_util_0.Count(_distsql_00000, 163)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("indexWorker in IndexLookupExecutor panicked", zap.String("stack", string(buf)))
			err4Panic := errors.Errorf("%v", r)
			doneCh := make(chan error, 1)
			doneCh <- err4Panic
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			if err != nil {
				trace_util_0.Count(_distsql_00000, 164)
				err = errors.Trace(err4Panic)
			}
		}
	}()
	trace_util_0.Count(_distsql_00000, 161)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.idxLookup.maxChunkSize)
	for {
		trace_util_0.Count(_distsql_00000, 165)
		handles, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 168)
			doneCh := make(chan error, 1)
			doneCh <- err
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		trace_util_0.Count(_distsql_00000, 166)
		if len(handles) == 0 {
			trace_util_0.Count(_distsql_00000, 169)
			return count, nil
		}
		trace_util_0.Count(_distsql_00000, 167)
		count += int64(len(handles))
		task := w.buildTableTask(handles)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_distsql_00000, 170)
			return count, nil
		case <-w.finished:
			trace_util_0.Count(_distsql_00000, 171)
			return count, nil
		case w.workCh <- task:
			trace_util_0.Count(_distsql_00000, 172)
			w.resultCh <- task
		}
	}
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (handles []int64, err error) {
	trace_util_0.Count(_distsql_00000, 173)
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		trace_util_0.Count(_distsql_00000, 176)
		chk.SetRequiredRows(w.batchSize-len(handles), w.maxChunkSize)
		err = idxResult.Next(ctx, chk)
		if err != nil {
			trace_util_0.Count(_distsql_00000, 179)
			return handles, err
		}
		trace_util_0.Count(_distsql_00000, 177)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_distsql_00000, 180)
			return handles, nil
		}
		trace_util_0.Count(_distsql_00000, 178)
		for i := 0; i < chk.NumRows(); i++ {
			trace_util_0.Count(_distsql_00000, 181)
			handles = append(handles, chk.GetRow(i).GetInt64(0))
		}
	}
	trace_util_0.Count(_distsql_00000, 174)
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		trace_util_0.Count(_distsql_00000, 182)
		w.batchSize = w.maxBatchSize
	}
	trace_util_0.Count(_distsql_00000, 175)
	return handles, nil
}

func (w *indexWorker) buildTableTask(handles []int64) *lookupTableTask {
	trace_util_0.Count(_distsql_00000, 183)
	var indexOrder map[int64]int
	if w.keepOrder {
		trace_util_0.Count(_distsql_00000, 185)
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			trace_util_0.Count(_distsql_00000, 186)
			indexOrder[h] = i
		}
	}
	trace_util_0.Count(_distsql_00000, 184)
	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
	}
	task.doneCh = make(chan error, 1)
	return task
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	idxLookup      *IndexLookUpExecutor
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	keepOrder      bool
	handleIdx      int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// isCheckOp is used to determine whether we need to check the consistency of the index data.
	isCheckOp bool
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *tableWorker) pickAndExecTask(ctx context.Context) {
	trace_util_0.Count(_distsql_00000, 187)
	var task *lookupTableTask
	var ok bool
	defer func() {
		trace_util_0.Count(_distsql_00000, 189)
		if r := recover(); r != nil {
			trace_util_0.Count(_distsql_00000, 190)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("tableWorker in IndexLookUpExecutor panicked", zap.String("stack", string(buf)))
			task.doneCh <- errors.Errorf("%v", r)
		}
	}()
	trace_util_0.Count(_distsql_00000, 188)
	for {
		trace_util_0.Count(_distsql_00000, 191)
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok = <-w.workCh:
			trace_util_0.Count(_distsql_00000, 193)
			if !ok {
				trace_util_0.Count(_distsql_00000, 195)
				return
			}
		case <-w.finished:
			trace_util_0.Count(_distsql_00000, 194)
			return
		}
		trace_util_0.Count(_distsql_00000, 192)
		err := w.executeTask(ctx, task)
		task.doneCh <- err
	}
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	trace_util_0.Count(_distsql_00000, 196)
	tableReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 201)
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	trace_util_0.Count(_distsql_00000, 197)
	defer terror.Call(tableReader.Close)

	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		trace_util_0.Count(_distsql_00000, 202)
		chk := newFirstChunk(tableReader)
		err = tableReader.Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_distsql_00000, 205)
			logutil.Logger(ctx).Error("table reader fetch next chunk failed", zap.Error(err))
			return err
		}
		trace_util_0.Count(_distsql_00000, 203)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_distsql_00000, 206)
			break
		}
		trace_util_0.Count(_distsql_00000, 204)
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_distsql_00000, 207)
			task.rows = append(task.rows, row)
		}
	}

	trace_util_0.Count(_distsql_00000, 198)
	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if w.keepOrder {
		trace_util_0.Count(_distsql_00000, 208)
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			trace_util_0.Count(_distsql_00000, 210)
			handle := task.rows[i].GetInt64(w.handleIdx)
			task.rowIdx = append(task.rowIdx, task.indexOrder[handle])
		}
		trace_util_0.Count(_distsql_00000, 209)
		memUsage = int64(cap(task.rowIdx) * 4)
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		sort.Sort(task)
	}

	trace_util_0.Count(_distsql_00000, 199)
	if handleCnt != len(task.rows) {
		trace_util_0.Count(_distsql_00000, 211)
		if w.isCheckOp {
			trace_util_0.Count(_distsql_00000, 213)
			obtainedHandlesMap := make(map[int64]struct{}, len(task.rows))
			for _, row := range task.rows {
				trace_util_0.Count(_distsql_00000, 215)
				handle := row.GetInt64(w.handleIdx)
				obtainedHandlesMap[handle] = struct{}{}
			}
			trace_util_0.Count(_distsql_00000, 214)
			return errors.Errorf("inconsistent index %s handle count %d isn't equal to value count %d, missing handles %v in a batch",
				w.idxLookup.index.Name.O, handleCnt, len(task.rows), GetLackHandles(task.handles, obtainedHandlesMap))
		}

		trace_util_0.Count(_distsql_00000, 212)
		if len(w.idxLookup.tblPlans) == 1 {
			trace_util_0.Count(_distsql_00000, 216)
			obtainedHandlesMap := make(map[int64]struct{}, len(task.rows))
			for _, row := range task.rows {
				trace_util_0.Count(_distsql_00000, 218)
				handle := row.GetInt64(w.handleIdx)
				obtainedHandlesMap[handle] = struct{}{}
			}

			trace_util_0.Count(_distsql_00000, 217)
			logutil.Logger(ctx).Error("inconsistent index handles", zap.String("index", w.idxLookup.index.Name.O),
				zap.Int("index_cnt", handleCnt), zap.Int("table_cnt", len(task.rows)),
				zap.Int64s("missing_handles", GetLackHandles(task.handles, obtainedHandlesMap)),
				zap.Int64s("total_handles", task.handles))

			// table scan in double read can never has conditions according to convertToIndexScan.
			// if this table scan has no condition, the number of rows it returns must equal to the length of handles.
			return errors.Errorf("inconsistent index %s handle count %d isn't equal to value count %d",
				w.idxLookup.index.Name.O, handleCnt, len(task.rows))
		}
	}

	trace_util_0.Count(_distsql_00000, 200)
	return nil
}

// GetLackHandles gets the handles in expectedHandles but not in obtainedHandlesMap.
func GetLackHandles(expectedHandles []int64, obtainedHandlesMap map[int64]struct{}) []int64 {
	trace_util_0.Count(_distsql_00000, 219)
	diffCnt := len(expectedHandles) - len(obtainedHandlesMap)
	diffHandles := make([]int64, 0, diffCnt)
	var cnt int
	for _, handle := range expectedHandles {
		trace_util_0.Count(_distsql_00000, 221)
		isExist := false
		if _, ok := obtainedHandlesMap[handle]; ok {
			trace_util_0.Count(_distsql_00000, 223)
			delete(obtainedHandlesMap, handle)
			isExist = true
		}
		trace_util_0.Count(_distsql_00000, 222)
		if !isExist {
			trace_util_0.Count(_distsql_00000, 224)
			diffHandles = append(diffHandles, handle)
			cnt++
			if cnt == diffCnt {
				trace_util_0.Count(_distsql_00000, 225)
				break
			}
		}
	}

	trace_util_0.Count(_distsql_00000, 220)
	return diffHandles
}

func getPhysicalPlanIDs(plans []plannercore.PhysicalPlan) []fmt.Stringer {
	trace_util_0.Count(_distsql_00000, 226)
	planIDs := make([]fmt.Stringer, 0, len(plans))
	for _, p := range plans {
		trace_util_0.Count(_distsql_00000, 228)
		planIDs = append(planIDs, p.ExplainID())
	}
	trace_util_0.Count(_distsql_00000, 227)
	return planIDs
}

var _distsql_00000 = "executor/distsql.go"
