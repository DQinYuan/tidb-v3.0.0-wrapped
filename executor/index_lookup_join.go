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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var _ Executor = &IndexLookUpJoin{}

// IndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner row hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer row by look up the inner rows hash map in the task.
type IndexLookUpJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task       *lookUpJoinTask
	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner      joiner
	isOuterJoin bool

	requiredRows int64

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

	// lastColHelper store the information for last col if there's complicated filter like col > x_col and col < x_col + 100.
	lastColHelper *plannercore.ColWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage.
}

type outerCtx struct {
	rowTypes []*types.FieldType
	keyCols  []int
	filter   expression.CNFExprs
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
}

type lookUpJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult       *chunk.List
	encodedLookUpKeys *chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Row

	doneCh   chan error
	cursor   int
	hasMatch bool
	hasNull  bool

	memTracker *memory.Tracker // track memory usage.
}

type outerWorker struct {
	outerCtx

	lookup *IndexLookUpJoin

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *lookUpJoinTask
	outerCtx    outerCtx
	ctx         sessionctx.Context
	executorChk *chunk.Chunk

	indexRanges           []*ranger.Range
	nextColCompareFilters *plannercore.ColWithCmpFuncManager
	keyOff2IdxOff         []int
}

// Open implements the Executor interface.
func (e *IndexLookUpJoin) Open(ctx context.Context) error {
	trace_util_0.Count(_index_lookup_join_00000, 0)
	// Be careful, very dirty hack in this line!!!
	// IndexLookUpJoin need to rebuild executor (the dataReaderBuilder) during
	// executing. However `executor.Next()` is lazy evaluation when the RecordSet
	// result is drained.
	// Lazy evaluation means the saved session context may change during executor's
	// building and its running.
	// A specific sequence for example:
	//
	// e := buildExecutor()   // txn at build time
	// recordSet := runStmt(e)
	// session.CommitTxn()    // txn closed
	// recordSet.Next()
	// e.dataReaderBuilder.Build() // txn is used again, which is already closed
	//
	// The trick here is `getStartTS` will cache start ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached start ts to construct DAG.
	_, err := e.innerCtx.readerBuilder.getStartTS()
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 3)
		return err
	}

	trace_util_0.Count(_index_lookup_join_00000, 1)
	err = e.children[0].Open(ctx)
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 4)
		return err
	}
	trace_util_0.Count(_index_lookup_join_00000, 2)
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexLookUpJoin) startWorkers(ctx context.Context) {
	trace_util_0.Count(_index_lookup_join_00000, 5)
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		trace_util_0.Count(_index_lookup_join_00000, 6)
		go e.newInnerWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *IndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *outerWorker {
	trace_util_0.Count(_index_lookup_join_00000, 7)
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
		lookup:           e,
	}
	return ow
}

func (e *IndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	trace_util_0.Count(_index_lookup_join_00000, 8)
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		trace_util_0.Count(_index_lookup_join_00000, 10)
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	trace_util_0.Count(_index_lookup_join_00000, 9)
	iw := &innerWorker{
		innerCtx:              e.innerCtx,
		outerCtx:              e.outerCtx,
		taskCh:                taskCh,
		ctx:                   e.ctx,
		executorChk:           chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:           copiedRanges,
		keyOff2IdxOff:         e.keyOff2IdxOff,
		nextColCompareFilters: e.lastColHelper,
	}
	return iw
}

// Next implements the Executor interface.
func (e *IndexLookUpJoin) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_index_lookup_join_00000, 11)
	if e.runtimeStats != nil {
		trace_util_0.Count(_index_lookup_join_00000, 14)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_index_lookup_join_00000, 15)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_index_lookup_join_00000, 12)
	if e.isOuterJoin {
		trace_util_0.Count(_index_lookup_join_00000, 16)
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	trace_util_0.Count(_index_lookup_join_00000, 13)
	req.Reset()
	e.joinResult.Reset()
	for {
		trace_util_0.Count(_index_lookup_join_00000, 17)
		task, err := e.getFinishedTask(ctx)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 23)
			return err
		}
		trace_util_0.Count(_index_lookup_join_00000, 18)
		if task == nil {
			trace_util_0.Count(_index_lookup_join_00000, 24)
			return nil
		}
		trace_util_0.Count(_index_lookup_join_00000, 19)
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			trace_util_0.Count(_index_lookup_join_00000, 25)
			e.lookUpMatchedInners(task, task.cursor)
			e.innerIter = chunk.NewIterator4Slice(task.matchedInners)
			e.innerIter.Begin()
		}

		trace_util_0.Count(_index_lookup_join_00000, 20)
		outerRow := task.outerResult.GetRow(task.cursor)
		if e.innerIter.Current() != e.innerIter.End() {
			trace_util_0.Count(_index_lookup_join_00000, 26)
			matched, isNull, err := e.joiner.tryToMatch(outerRow, e.innerIter, req.Chunk)
			if err != nil {
				trace_util_0.Count(_index_lookup_join_00000, 28)
				return err
			}
			trace_util_0.Count(_index_lookup_join_00000, 27)
			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull
		}
		trace_util_0.Count(_index_lookup_join_00000, 21)
		if e.innerIter.Current() == e.innerIter.End() {
			trace_util_0.Count(_index_lookup_join_00000, 29)
			if !task.hasMatch {
				trace_util_0.Count(_index_lookup_join_00000, 31)
				e.joiner.onMissMatch(task.hasNull, outerRow, req.Chunk)
			}
			trace_util_0.Count(_index_lookup_join_00000, 30)
			task.cursor++
			task.hasMatch = false
			task.hasNull = false
		}
		trace_util_0.Count(_index_lookup_join_00000, 22)
		if req.IsFull() {
			trace_util_0.Count(_index_lookup_join_00000, 32)
			return nil
		}
	}
}

func (e *IndexLookUpJoin) getFinishedTask(ctx context.Context) (*lookUpJoinTask, error) {
	trace_util_0.Count(_index_lookup_join_00000, 33)
	task := e.task
	if task != nil && task.cursor < task.outerResult.NumRows() {
		trace_util_0.Count(_index_lookup_join_00000, 39)
		return task, nil
	}

	trace_util_0.Count(_index_lookup_join_00000, 34)
	select {
	case task = <-e.resultCh:
		trace_util_0.Count(_index_lookup_join_00000, 40)
	case <-ctx.Done():
		trace_util_0.Count(_index_lookup_join_00000, 41)
		return nil, nil
	}
	trace_util_0.Count(_index_lookup_join_00000, 35)
	if task == nil {
		trace_util_0.Count(_index_lookup_join_00000, 42)
		return nil, nil
	}

	trace_util_0.Count(_index_lookup_join_00000, 36)
	select {
	case err := <-task.doneCh:
		trace_util_0.Count(_index_lookup_join_00000, 43)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 45)
			return nil, err
		}
	case <-ctx.Done():
		trace_util_0.Count(_index_lookup_join_00000, 44)
		return nil, nil
	}

	trace_util_0.Count(_index_lookup_join_00000, 37)
	if e.task != nil {
		trace_util_0.Count(_index_lookup_join_00000, 46)
		e.task.memTracker.Detach()
	}
	trace_util_0.Count(_index_lookup_join_00000, 38)
	e.task = task
	return task, nil
}

func (e *IndexLookUpJoin) lookUpMatchedInners(task *lookUpJoinTask, rowIdx int) {
	trace_util_0.Count(_index_lookup_join_00000, 47)
	outerKey := task.encodedLookUpKeys.GetRow(rowIdx).GetBytes(0)
	e.innerPtrBytes = task.lookupMap.Get(outerKey, e.innerPtrBytes[:0])
	task.matchedInners = task.matchedInners[:0]

	for _, b := range e.innerPtrBytes {
		trace_util_0.Count(_index_lookup_join_00000, 48)
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.innerResult.GetRow(ptr)
		task.matchedInners = append(task.matchedInners, matchedInner)
	}
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	trace_util_0.Count(_index_lookup_join_00000, 49)
	defer func() {
		trace_util_0.Count(_index_lookup_join_00000, 51)
		if r := recover(); r != nil {
			trace_util_0.Count(_index_lookup_join_00000, 53)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("outerWorker panicked", zap.String("stack", string(buf)))
			task := &lookUpJoinTask{doneCh: make(chan error, 1)}
			task.doneCh <- errors.Errorf("%v", r)
			ow.pushToChan(ctx, task, ow.resultCh)
		}
		trace_util_0.Count(_index_lookup_join_00000, 52)
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	trace_util_0.Count(_index_lookup_join_00000, 50)
	for {
		trace_util_0.Count(_index_lookup_join_00000, 54)
		task, err := ow.buildTask(ctx)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 58)
			task.doneCh <- err
			ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		trace_util_0.Count(_index_lookup_join_00000, 55)
		if task == nil {
			trace_util_0.Count(_index_lookup_join_00000, 59)
			return
		}

		trace_util_0.Count(_index_lookup_join_00000, 56)
		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			trace_util_0.Count(_index_lookup_join_00000, 60)
			return
		}

		trace_util_0.Count(_index_lookup_join_00000, 57)
		if finished := ow.pushToChan(ctx, task, ow.resultCh); finished {
			trace_util_0.Count(_index_lookup_join_00000, 61)
			return
		}
	}
}

func (ow *outerWorker) pushToChan(ctx context.Context, task *lookUpJoinTask, dst chan<- *lookUpJoinTask) bool {
	trace_util_0.Count(_index_lookup_join_00000, 62)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_index_lookup_join_00000, 64)
		return true
	case dst <- task:
		trace_util_0.Count(_index_lookup_join_00000, 65)
	}
	trace_util_0.Count(_index_lookup_join_00000, 63)
	return false
}

// buildTask builds a lookUpJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*lookUpJoinTask, error) {
	trace_util_0.Count(_index_lookup_join_00000, 66)
	newFirstChunk(ow.executor)

	task := &lookUpJoinTask{
		doneCh:            make(chan error, 1),
		outerResult:       newFirstChunk(ow.executor),
		encodedLookUpKeys: chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, ow.ctx.GetSessionVars().MaxChunkSize),
		lookupMap:         mvmap.NewMVMap(),
	}
	task.memTracker = memory.NewTracker(stringutil.MemoizeStr(func() string {
		trace_util_0.Count(_index_lookup_join_00000, 72)
		return fmt.Sprintf("lookup join task %p", task)
	}), -1)
	trace_util_0.Count(_index_lookup_join_00000, 67)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()
	if ow.lookup.isOuterJoin {
		trace_util_0.Count(_index_lookup_join_00000, 73) // if is outerJoin, push the requiredRows down
		requiredRows := int(atomic.LoadInt64(&ow.lookup.requiredRows))
		task.outerResult.SetRequiredRows(requiredRows, ow.maxBatchSize)
	} else {
		trace_util_0.Count(_index_lookup_join_00000, 74)
		{
			task.outerResult.SetRequiredRows(ow.batchSize, ow.maxBatchSize)
		}
	}

	trace_util_0.Count(_index_lookup_join_00000, 68)
	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for !task.outerResult.IsFull() {
		trace_util_0.Count(_index_lookup_join_00000, 75)
		err := Next(ctx, ow.executor, chunk.NewRecordBatch(ow.executorChk))
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 78)
			return task, err
		}
		trace_util_0.Count(_index_lookup_join_00000, 76)
		if ow.executorChk.NumRows() == 0 {
			trace_util_0.Count(_index_lookup_join_00000, 79)
			break
		}

		trace_util_0.Count(_index_lookup_join_00000, 77)
		oldMemUsage := task.outerResult.MemoryUsage()
		task.outerResult.Append(ow.executorChk, 0, ow.executorChk.NumRows())
		newMemUsage := task.outerResult.MemoryUsage()
		task.memTracker.Consume(newMemUsage - oldMemUsage)
	}
	trace_util_0.Count(_index_lookup_join_00000, 69)
	if task.outerResult.NumRows() == 0 {
		trace_util_0.Count(_index_lookup_join_00000, 80)
		return nil, nil
	}

	trace_util_0.Count(_index_lookup_join_00000, 70)
	if ow.filter != nil {
		trace_util_0.Count(_index_lookup_join_00000, 81)
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(task.outerResult), outerMatch)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 83)
			return task, err
		}
		trace_util_0.Count(_index_lookup_join_00000, 82)
		task.memTracker.Consume(int64(cap(task.outerMatch)))
	}
	trace_util_0.Count(_index_lookup_join_00000, 71)
	return task, nil
}

func (ow *outerWorker) increaseBatchSize() {
	trace_util_0.Count(_index_lookup_join_00000, 84)
	if ow.batchSize < ow.maxBatchSize {
		trace_util_0.Count(_index_lookup_join_00000, 86)
		ow.batchSize *= 2
	}
	trace_util_0.Count(_index_lookup_join_00000, 85)
	if ow.batchSize > ow.maxBatchSize {
		trace_util_0.Count(_index_lookup_join_00000, 87)
		ow.batchSize = ow.maxBatchSize
	}
}

func (iw *innerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	trace_util_0.Count(_index_lookup_join_00000, 88)
	var task *lookUpJoinTask
	defer func() {
		trace_util_0.Count(_index_lookup_join_00000, 90)
		if r := recover(); r != nil {
			trace_util_0.Count(_index_lookup_join_00000, 92)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("innerWorker panicked", zap.String("stack", string(buf)))
			// "task != nil" is guaranteed when panic happened.
			task.doneCh <- errors.Errorf("%v", r)
		}
		trace_util_0.Count(_index_lookup_join_00000, 91)
		wg.Done()
	}()

	trace_util_0.Count(_index_lookup_join_00000, 89)
	for ok := true; ok; {
		trace_util_0.Count(_index_lookup_join_00000, 93)
		select {
		case task, ok = <-iw.taskCh:
			trace_util_0.Count(_index_lookup_join_00000, 95)
			if !ok {
				trace_util_0.Count(_index_lookup_join_00000, 97)
				return
			}
		case <-ctx.Done():
			trace_util_0.Count(_index_lookup_join_00000, 96)
			return
		}

		trace_util_0.Count(_index_lookup_join_00000, 94)
		err := iw.handleTask(ctx, task)
		task.doneCh <- err
	}
}

type indexJoinLookUpContent struct {
	keys []types.Datum
	row  chunk.Row
}

func (iw *innerWorker) handleTask(ctx context.Context, task *lookUpJoinTask) error {
	trace_util_0.Count(_index_lookup_join_00000, 98)
	lookUpContents, err := iw.constructLookupContent(task)
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 102)
		return err
	}
	trace_util_0.Count(_index_lookup_join_00000, 99)
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	err = iw.fetchInnerResults(ctx, task, lookUpContents)
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 103)
		return err
	}
	trace_util_0.Count(_index_lookup_join_00000, 100)
	err = iw.buildLookUpMap(task)
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 104)
		return err
	}
	trace_util_0.Count(_index_lookup_join_00000, 101)
	return nil
}

func (iw *innerWorker) constructLookupContent(task *lookUpJoinTask) ([]*indexJoinLookUpContent, error) {
	trace_util_0.Count(_index_lookup_join_00000, 105)
	lookUpContents := make([]*indexJoinLookUpContent, 0, task.outerResult.NumRows())
	keyBuf := make([]byte, 0, 64)
	for i := 0; i < task.outerResult.NumRows(); i++ {
		trace_util_0.Count(_index_lookup_join_00000, 107)
		dLookUpKey, err := iw.constructDatumLookupKey(task, i)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 111)
			return nil, err
		}
		trace_util_0.Count(_index_lookup_join_00000, 108)
		if dLookUpKey == nil {
			trace_util_0.Count(_index_lookup_join_00000, 112)
			// Append null to make looUpKeys the same length as outer Result.
			task.encodedLookUpKeys.AppendNull(0)
			continue
		}
		trace_util_0.Count(_index_lookup_join_00000, 109)
		keyBuf = keyBuf[:0]
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dLookUpKey...)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 113)
			return nil, err
		}
		// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
		trace_util_0.Count(_index_lookup_join_00000, 110)
		task.encodedLookUpKeys.AppendBytes(0, keyBuf)
		lookUpContents = append(lookUpContents, &indexJoinLookUpContent{keys: dLookUpKey, row: task.outerResult.GetRow(i)})
	}

	trace_util_0.Count(_index_lookup_join_00000, 106)
	task.memTracker.Consume(task.encodedLookUpKeys.MemoryUsage())
	return lookUpContents, nil
}

func (iw *innerWorker) constructDatumLookupKey(task *lookUpJoinTask, rowIdx int) ([]types.Datum, error) {
	trace_util_0.Count(_index_lookup_join_00000, 114)
	if task.outerMatch != nil && !task.outerMatch[rowIdx] {
		trace_util_0.Count(_index_lookup_join_00000, 117)
		return nil, nil
	}
	trace_util_0.Count(_index_lookup_join_00000, 115)
	outerRow := task.outerResult.GetRow(rowIdx)
	sc := iw.ctx.GetSessionVars().StmtCtx
	keyLen := len(iw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range iw.outerCtx.keyCols {
		trace_util_0.Count(_index_lookup_join_00000, 118)
		outerValue := outerRow.GetDatum(keyCol, iw.outerCtx.rowTypes[keyCol])
		// Join-on-condition can be promised to be equal-condition in
		// IndexNestedLoopJoin, thus the filter will always be false if
		// outerValue is null, and we don't need to lookup it.
		if outerValue.IsNull() {
			trace_util_0.Count(_index_lookup_join_00000, 123)
			return nil, nil
		}
		trace_util_0.Count(_index_lookup_join_00000, 119)
		innerColType := iw.rowTypes[iw.keyCols[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerColType)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 124)
			// If the converted outerValue overflows, we don't need to lookup it.
			if terror.ErrorEqual(err, types.ErrOverflow) {
				trace_util_0.Count(_index_lookup_join_00000, 126)
				return nil, nil
			}
			trace_util_0.Count(_index_lookup_join_00000, 125)
			return nil, err
		}
		trace_util_0.Count(_index_lookup_join_00000, 120)
		cmp, err := outerValue.CompareDatum(sc, &innerValue)
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 127)
			return nil, err
		}
		trace_util_0.Count(_index_lookup_join_00000, 121)
		if cmp != 0 {
			trace_util_0.Count(_index_lookup_join_00000, 128)
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		trace_util_0.Count(_index_lookup_join_00000, 122)
		dLookupKey = append(dLookupKey, innerValue)
	}
	trace_util_0.Count(_index_lookup_join_00000, 116)
	return dLookupKey, nil
}

func (iw *innerWorker) sortAndDedupLookUpContents(lookUpContents []*indexJoinLookUpContent) []*indexJoinLookUpContent {
	trace_util_0.Count(_index_lookup_join_00000, 129)
	if len(lookUpContents) < 2 {
		trace_util_0.Count(_index_lookup_join_00000, 133)
		return lookUpContents
	}
	trace_util_0.Count(_index_lookup_join_00000, 130)
	sc := iw.ctx.GetSessionVars().StmtCtx
	sort.Slice(lookUpContents, func(i, j int) bool {
		trace_util_0.Count(_index_lookup_join_00000, 134)
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[j].keys)
		if cmp != 0 || iw.nextColCompareFilters == nil {
			trace_util_0.Count(_index_lookup_join_00000, 136)
			return cmp < 0
		}
		trace_util_0.Count(_index_lookup_join_00000, 135)
		return iw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[j].row) < 0
	})
	trace_util_0.Count(_index_lookup_join_00000, 131)
	deDupedLookupKeys := lookUpContents[:1]
	for i := 1; i < len(lookUpContents); i++ {
		trace_util_0.Count(_index_lookup_join_00000, 137)
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[i-1].keys)
		if cmp != 0 || (iw.nextColCompareFilters != nil && iw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[i-1].row) != 0) {
			trace_util_0.Count(_index_lookup_join_00000, 138)
			deDupedLookupKeys = append(deDupedLookupKeys, lookUpContents[i])
		}
	}
	trace_util_0.Count(_index_lookup_join_00000, 132)
	return deDupedLookupKeys
}

func compareRow(sc *stmtctx.StatementContext, left, right []types.Datum) int {
	trace_util_0.Count(_index_lookup_join_00000, 139)
	for idx := 0; idx < len(left); idx++ {
		trace_util_0.Count(_index_lookup_join_00000, 141)
		cmp, err := left[idx].CompareDatum(sc, &right[idx])
		// We only compare rows with the same type, no error to return.
		terror.Log(err)
		if cmp > 0 {
			trace_util_0.Count(_index_lookup_join_00000, 142)
			return 1
		} else {
			trace_util_0.Count(_index_lookup_join_00000, 143)
			if cmp < 0 {
				trace_util_0.Count(_index_lookup_join_00000, 144)
				return -1
			}
		}
	}
	trace_util_0.Count(_index_lookup_join_00000, 140)
	return 0
}

func (iw *innerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask, lookUpContent []*indexJoinLookUpContent) error {
	trace_util_0.Count(_index_lookup_join_00000, 145)
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, lookUpContent, iw.indexRanges, iw.keyOff2IdxOff, iw.nextColCompareFilters)
	if err != nil {
		trace_util_0.Count(_index_lookup_join_00000, 148)
		return err
	}
	trace_util_0.Count(_index_lookup_join_00000, 146)
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(retTypes(innerExec), iw.ctx.GetSessionVars().MaxChunkSize, iw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel(innerResultLabel)
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	for {
		trace_util_0.Count(_index_lookup_join_00000, 149)
		err := Next(ctx, innerExec, chunk.NewRecordBatch(iw.executorChk))
		if err != nil {
			trace_util_0.Count(_index_lookup_join_00000, 152)
			return err
		}
		trace_util_0.Count(_index_lookup_join_00000, 150)
		if iw.executorChk.NumRows() == 0 {
			trace_util_0.Count(_index_lookup_join_00000, 153)
			break
		}
		trace_util_0.Count(_index_lookup_join_00000, 151)
		innerResult.Add(iw.executorChk)
		iw.executorChk = newFirstChunk(innerExec)
	}
	trace_util_0.Count(_index_lookup_join_00000, 147)
	task.innerResult = innerResult
	return nil
}

func (iw *innerWorker) buildLookUpMap(task *lookUpJoinTask) error {
	trace_util_0.Count(_index_lookup_join_00000, 154)
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.innerResult.NumChunks(); i++ {
		trace_util_0.Count(_index_lookup_join_00000, 156)
		chk := task.innerResult.GetChunk(i)
		for j := 0; j < chk.NumRows(); j++ {
			trace_util_0.Count(_index_lookup_join_00000, 157)
			innerRow := chk.GetRow(j)
			if iw.hasNullInJoinKey(innerRow) {
				trace_util_0.Count(_index_lookup_join_00000, 160)
				continue
			}

			trace_util_0.Count(_index_lookup_join_00000, 158)
			keyBuf = keyBuf[:0]
			for _, keyCol := range iw.keyCols {
				trace_util_0.Count(_index_lookup_join_00000, 161)
				d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
				var err error
				keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
				if err != nil {
					trace_util_0.Count(_index_lookup_join_00000, 162)
					return err
				}
			}
			trace_util_0.Count(_index_lookup_join_00000, 159)
			rowPtr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			task.lookupMap.Put(keyBuf, valBuf)
		}
	}
	trace_util_0.Count(_index_lookup_join_00000, 155)
	return nil
}

func (iw *innerWorker) hasNullInJoinKey(row chunk.Row) bool {
	trace_util_0.Count(_index_lookup_join_00000, 163)
	for _, ordinal := range iw.keyCols {
		trace_util_0.Count(_index_lookup_join_00000, 165)
		if row.IsNull(ordinal) {
			trace_util_0.Count(_index_lookup_join_00000, 166)
			return true
		}
	}
	trace_util_0.Count(_index_lookup_join_00000, 164)
	return false
}

// Close implements the Executor interface.
func (e *IndexLookUpJoin) Close() error {
	trace_util_0.Count(_index_lookup_join_00000, 167)
	if e.cancelFunc != nil {
		trace_util_0.Count(_index_lookup_join_00000, 169)
		e.cancelFunc()
	}
	trace_util_0.Count(_index_lookup_join_00000, 168)
	e.workerWg.Wait()
	e.memTracker.Detach()
	e.memTracker = nil
	return e.children[0].Close()
}

var _index_lookup_join_00000 = "executor/index_lookup_join.go"
