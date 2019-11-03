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

package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	_ Executor = &HashJoinExec{}
	_ Executor = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerExec   Executor
	innerExec   Executor
	outerFilter expression.CNFExprs
	outerKeys   []*expression.Column
	innerKeys   []*expression.Column

	prepared bool
	// concurrency is the number of partition, build and join workers.
	concurrency     uint
	hashTable       *mvmap.MVMap
	innerFinished   chan error
	hashJoinBuffers []*hashJoinBuffer
	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value
	// closeCh add a lock for closing executor.
	closeCh  chan struct{}
	joinType plannercore.JoinType
	innerIdx int

	isOuterJoin  bool
	requiredRows int64

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	outerKeyColIdx     []int
	innerKeyColIdx     []int
	innerResult        *chunk.List
	outerChkResourceCh chan *outerChkResource
	outerResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult
	hashTableValBufs   [][][]byte

	memTracker *memory.Tracker // track memory usage.
}

// outerChkResource stores the result of the join outer fetch worker,
// `dest` is for Chunk reuse: after join workers process the outer chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the outer fetch worker will put new data into `chk` and write `chk` into dest.
type outerChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type hashJoinBuffer struct {
	data  []types.Datum
	bytes []byte
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	trace_util_0.Count(_join_00000, 0)
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		trace_util_0.Count(_join_00000, 2)
		if e.innerFinished != nil {
			trace_util_0.Count(_join_00000, 8)
			for range e.innerFinished {
				trace_util_0.Count(_join_00000, 9)
			}
		}
		trace_util_0.Count(_join_00000, 3)
		if e.joinResultCh != nil {
			trace_util_0.Count(_join_00000, 10)
			for range e.joinResultCh {
				trace_util_0.Count(_join_00000, 11)
			}
		}
		trace_util_0.Count(_join_00000, 4)
		if e.outerChkResourceCh != nil {
			trace_util_0.Count(_join_00000, 12)
			close(e.outerChkResourceCh)
			for range e.outerChkResourceCh {
				trace_util_0.Count(_join_00000, 13)
			}
		}
		trace_util_0.Count(_join_00000, 5)
		for i := range e.outerResultChs {
			trace_util_0.Count(_join_00000, 14)
			for range e.outerResultChs[i] {
				trace_util_0.Count(_join_00000, 15)
			}
		}
		trace_util_0.Count(_join_00000, 6)
		for i := range e.joinChkResourceCh {
			trace_util_0.Count(_join_00000, 16)
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
				trace_util_0.Count(_join_00000, 17)
			}
		}
		trace_util_0.Count(_join_00000, 7)
		e.outerChkResourceCh = nil
		e.joinChkResourceCh = nil
	}
	trace_util_0.Count(_join_00000, 1)
	e.memTracker.Detach()
	e.memTracker = nil

	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	trace_util_0.Count(_join_00000, 18)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_join_00000, 21)
		return err
	}

	trace_util_0.Count(_join_00000, 19)
	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaHashJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.hashTableValBufs = make([][][]byte, e.concurrency)
	e.hashJoinBuffers = make([]*hashJoinBuffer, 0, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_join_00000, 22)
		buffer := &hashJoinBuffer{
			data:  make([]types.Datum, len(e.outerKeys)),
			bytes: make([]byte, 0, 10000),
		}
		e.hashJoinBuffers = append(e.hashJoinBuffers, buffer)
	}

	trace_util_0.Count(_join_00000, 20)
	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}
	return nil
}

func (e *HashJoinExec) getJoinKeyFromChkRow(isOuterKey bool, row chunk.Row, keyBuf []byte) (hasNull bool, _ []byte, err error) {
	trace_util_0.Count(_join_00000, 23)
	var keyColIdx []int
	var allTypes []*types.FieldType
	if isOuterKey {
		trace_util_0.Count(_join_00000, 26)
		keyColIdx = e.outerKeyColIdx
		allTypes = retTypes(e.outerExec)
	} else {
		trace_util_0.Count(_join_00000, 27)
		{
			keyColIdx = e.innerKeyColIdx
			allTypes = retTypes(e.innerExec)
		}
	}

	trace_util_0.Count(_join_00000, 24)
	for _, i := range keyColIdx {
		trace_util_0.Count(_join_00000, 28)
		if row.IsNull(i) {
			trace_util_0.Count(_join_00000, 29)
			return true, keyBuf, nil
		}
	}
	trace_util_0.Count(_join_00000, 25)
	keyBuf = keyBuf[:0]
	keyBuf, err = codec.HashChunkRow(e.ctx.GetSessionVars().StmtCtx, keyBuf, row, allTypes, keyColIdx)
	return false, keyBuf, err
}

// fetchOuterChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterChunks(ctx context.Context) {
	trace_util_0.Count(_join_00000, 30)
	hasWaitedForInner := false
	for {
		trace_util_0.Count(_join_00000, 31)
		if e.finished.Load().(bool) {
			trace_util_0.Count(_join_00000, 38)
			return
		}

		trace_util_0.Count(_join_00000, 32)
		var outerResource *outerChkResource
		var ok bool
		select {
		case <-e.closeCh:
			trace_util_0.Count(_join_00000, 39)
			return
		case outerResource, ok = <-e.outerChkResourceCh:
			trace_util_0.Count(_join_00000, 40)
			if !ok {
				trace_util_0.Count(_join_00000, 41)
				return
			}
		}
		trace_util_0.Count(_join_00000, 33)
		outerResult := outerResource.chk
		if e.isOuterJoin {
			trace_util_0.Count(_join_00000, 42)
			required := int(atomic.LoadInt64(&e.requiredRows))
			outerResult.SetRequiredRows(required, e.maxChunkSize)
		}
		trace_util_0.Count(_join_00000, 34)
		err := Next(ctx, e.outerExec, chunk.NewRecordBatch(outerResult))
		if err != nil {
			trace_util_0.Count(_join_00000, 43)
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		trace_util_0.Count(_join_00000, 35)
		if !hasWaitedForInner {
			trace_util_0.Count(_join_00000, 44)
			if outerResult.NumRows() == 0 {
				trace_util_0.Count(_join_00000, 47)
				e.finished.Store(true)
				return
			}
			trace_util_0.Count(_join_00000, 45)
			jobFinished, innerErr := e.wait4Inner()
			if innerErr != nil {
				trace_util_0.Count(_join_00000, 48)
				e.joinResultCh <- &hashjoinWorkerResult{
					err: innerErr,
				}
				return
			} else {
				trace_util_0.Count(_join_00000, 49)
				if jobFinished {
					trace_util_0.Count(_join_00000, 50)
					return
				}
			}
			trace_util_0.Count(_join_00000, 46)
			hasWaitedForInner = true
		}

		trace_util_0.Count(_join_00000, 36)
		if outerResult.NumRows() == 0 {
			trace_util_0.Count(_join_00000, 51)
			return
		}

		trace_util_0.Count(_join_00000, 37)
		outerResource.dest <- outerResult
	}
}

func (e *HashJoinExec) wait4Inner() (finished bool, err error) {
	trace_util_0.Count(_join_00000, 52)
	select {
	case <-e.closeCh:
		trace_util_0.Count(_join_00000, 55)
		return true, nil
	case err := <-e.innerFinished:
		trace_util_0.Count(_join_00000, 56)
		if err != nil {
			trace_util_0.Count(_join_00000, 57)
			return false, err
		}
	}
	trace_util_0.Count(_join_00000, 53)
	if e.hashTable.Len() == 0 && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		trace_util_0.Count(_join_00000, 58)
		return true, nil
	}
	trace_util_0.Count(_join_00000, 54)
	return false, nil
}

var innerResultLabel fmt.Stringer = stringutil.StringerStr("innerResult")

// fetchInnerRows fetches all rows from inner executor,
// and append them to e.innerResult.
func (e *HashJoinExec) fetchInnerRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh chan struct{}) {
	trace_util_0.Count(_join_00000, 59)
	defer close(chkCh)
	e.innerResult = chunk.NewList(e.innerExec.base().retFieldTypes, e.initCap, e.maxChunkSize)
	e.innerResult.GetMemTracker().AttachTo(e.memTracker)
	e.innerResult.GetMemTracker().SetLabel(innerResultLabel)
	var err error
	for {
		trace_util_0.Count(_join_00000, 60)
		select {
		case <-doneCh:
			trace_util_0.Count(_join_00000, 61)
			return
		case <-e.closeCh:
			trace_util_0.Count(_join_00000, 62)
			return
		default:
			trace_util_0.Count(_join_00000, 63)
			if e.finished.Load().(bool) {
				trace_util_0.Count(_join_00000, 68)
				return
			}
			trace_util_0.Count(_join_00000, 64)
			chk := chunk.NewChunkWithCapacity(e.children[e.innerIdx].base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
			err = e.innerExec.Next(ctx, chunk.NewRecordBatch(chk))
			if err != nil {
				trace_util_0.Count(_join_00000, 69)
				e.innerFinished <- errors.Trace(err)
				return
			}
			trace_util_0.Count(_join_00000, 65)
			if chk.NumRows() == 0 {
				trace_util_0.Count(_join_00000, 70)
				return
			}
			trace_util_0.Count(_join_00000, 66)
			select {
			case chkCh <- chk:
				trace_util_0.Count(_join_00000, 71)
				break
			case <-e.closeCh:
				trace_util_0.Count(_join_00000, 72)
				return
			}
			trace_util_0.Count(_join_00000, 67)
			e.innerResult.Add(chk)
		}
	}
}

func (e *HashJoinExec) initializeForProbe() {
	trace_util_0.Count(_join_00000, 73)
	// e.outerResultChs is for transmitting the chunks which store the data of
	// outerExec, it'll be written by outer worker goroutine, and read by join
	// workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_join_00000, 77)
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerExec chunks from
	// join workers to outerExec worker.
	trace_util_0.Count(_join_00000, 74)
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_join_00000, 78)
		e.outerChkResourceCh <- &outerChkResource{
			chk:  newFirstChunk(e.outerExec),
			dest: e.outerResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	trace_util_0.Count(_join_00000, 75)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_join_00000, 79)
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	trace_util_0.Count(_join_00000, 76)
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)

	e.outerKeyColIdx = make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		trace_util_0.Count(_join_00000, 80)
		e.outerKeyColIdx[i] = e.outerKeys[i].Index
	}
}

func (e *HashJoinExec) fetchOuterAndProbeHashTable(ctx context.Context) {
	trace_util_0.Count(_join_00000, 81)
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() { trace_util_0.Count(_join_00000, 84); e.fetchOuterChunks(ctx) }, e.handleOuterFetcherPanic)

	// Start e.concurrency join workers to probe hash table and join inner and
	// outer rows.
	trace_util_0.Count(_join_00000, 82)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_join_00000, 85)
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { trace_util_0.Count(_join_00000, 86); e.runJoinWorker(workID) }, e.handleJoinWorkerPanic)
	}
	trace_util_0.Count(_join_00000, 83)
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinExec) handleOuterFetcherPanic(r interface{}) {
	trace_util_0.Count(_join_00000, 87)
	for i := range e.outerResultChs {
		trace_util_0.Count(_join_00000, 90)
		close(e.outerResultChs[i])
	}
	trace_util_0.Count(_join_00000, 88)
	if r != nil {
		trace_util_0.Count(_join_00000, 91)
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	trace_util_0.Count(_join_00000, 89)
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinWorkerPanic(r interface{}) {
	trace_util_0.Count(_join_00000, 92)
	if r != nil {
		trace_util_0.Count(_join_00000, 94)
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	trace_util_0.Count(_join_00000, 93)
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	trace_util_0.Count(_join_00000, 95)
	e.joinWorkerWaitGroup.Wait()
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinWorker(workerID uint) {
	trace_util_0.Count(_join_00000, 96)
	var (
		outerResult *chunk.Chunk
		selected    = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		trace_util_0.Count(_join_00000, 99)
		return
	}

	// Read and filter outerResult, and join the outerResult with the inner rows.
	trace_util_0.Count(_join_00000, 97)
	emptyOuterResult := &outerChkResource{
		dest: e.outerResultChs[workerID],
	}
	for ok := true; ok; {
		trace_util_0.Count(_join_00000, 100)
		if e.finished.Load().(bool) {
			trace_util_0.Count(_join_00000, 105)
			break
		}
		trace_util_0.Count(_join_00000, 101)
		select {
		case <-e.closeCh:
			trace_util_0.Count(_join_00000, 106)
			return
		case outerResult, ok = <-e.outerResultChs[workerID]:
			trace_util_0.Count(_join_00000, 107)
		}
		trace_util_0.Count(_join_00000, 102)
		if !ok {
			trace_util_0.Count(_join_00000, 108)
			break
		}
		trace_util_0.Count(_join_00000, 103)
		ok, joinResult = e.join2Chunk(workerID, outerResult, joinResult, selected)
		if !ok {
			trace_util_0.Count(_join_00000, 109)
			break
		}
		trace_util_0.Count(_join_00000, 104)
		outerResult.Reset()
		emptyOuterResult.chk = outerResult
		e.outerChkResourceCh <- emptyOuterResult
	}
	trace_util_0.Count(_join_00000, 98)
	if joinResult == nil {
		trace_util_0.Count(_join_00000, 110)
		return
	} else {
		trace_util_0.Count(_join_00000, 111)
		if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
			trace_util_0.Count(_join_00000, 112)
			e.joinResultCh <- joinResult
		}
	}
}

func (e *HashJoinExec) joinMatchedOuterRow2Chunk(workerID uint, outerRow chunk.Row,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	trace_util_0.Count(_join_00000, 113)
	buffer := e.hashJoinBuffers[workerID]
	hasNull, joinKey, err := e.getJoinKeyFromChkRow(true, outerRow, buffer.bytes)
	if err != nil {
		trace_util_0.Count(_join_00000, 120)
		joinResult.err = err
		return false, joinResult
	}
	trace_util_0.Count(_join_00000, 114)
	if hasNull {
		trace_util_0.Count(_join_00000, 121)
		e.joiners[workerID].onMissMatch(false, outerRow, joinResult.chk)
		return true, joinResult
	}
	trace_util_0.Count(_join_00000, 115)
	e.hashTableValBufs[workerID] = e.hashTable.Get(joinKey, e.hashTableValBufs[workerID][:0])
	innerPtrs := e.hashTableValBufs[workerID]
	if len(innerPtrs) == 0 {
		trace_util_0.Count(_join_00000, 122)
		e.joiners[workerID].onMissMatch(false, outerRow, joinResult.chk)
		return true, joinResult
	}
	trace_util_0.Count(_join_00000, 116)
	innerRows := make([]chunk.Row, 0, len(innerPtrs))
	for _, b := range innerPtrs {
		trace_util_0.Count(_join_00000, 123)
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := e.innerResult.GetRow(ptr)
		innerRows = append(innerRows, matchedInner)
	}
	trace_util_0.Count(_join_00000, 117)
	iter := chunk.NewIterator4Slice(innerRows)
	hasMatch, hasNull := false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		trace_util_0.Count(_join_00000, 124)
		matched, isNull, err := e.joiners[workerID].tryToMatch(outerRow, iter, joinResult.chk)
		if err != nil {
			trace_util_0.Count(_join_00000, 126)
			joinResult.err = err
			return false, joinResult
		}
		trace_util_0.Count(_join_00000, 125)
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			trace_util_0.Count(_join_00000, 127)
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				trace_util_0.Count(_join_00000, 128)
				return false, joinResult
			}
		}
	}
	trace_util_0.Count(_join_00000, 118)
	if !hasMatch {
		trace_util_0.Count(_join_00000, 129)
		e.joiners[workerID].onMissMatch(hasNull, outerRow, joinResult.chk)
	}
	trace_util_0.Count(_join_00000, 119)
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	trace_util_0.Count(_join_00000, 130)
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		trace_util_0.Count(_join_00000, 132)
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
		trace_util_0.Count(_join_00000, 133)
	}
	trace_util_0.Count(_join_00000, 131)
	return ok, joinResult
}

func (e *HashJoinExec) join2Chunk(workerID uint, outerChk *chunk.Chunk, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	trace_util_0.Count(_join_00000, 134)
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(outerChk), selected)
	if err != nil {
		trace_util_0.Count(_join_00000, 137)
		joinResult.err = err
		return false, joinResult
	}
	trace_util_0.Count(_join_00000, 135)
	for i := range selected {
		trace_util_0.Count(_join_00000, 138)
		if !selected[i] {
			trace_util_0.Count(_join_00000, 140) // process unmatched outer rows
			e.joiners[workerID].onMissMatch(false, outerChk.GetRow(i), joinResult.chk)
		} else {
			trace_util_0.Count(_join_00000, 141)
			{ // process matched outer rows
				ok, joinResult = e.joinMatchedOuterRow2Chunk(workerID, outerChk.GetRow(i), joinResult)
				if !ok {
					trace_util_0.Count(_join_00000, 142)
					return false, joinResult
				}
			}
		}
		trace_util_0.Count(_join_00000, 139)
		if joinResult.chk.IsFull() {
			trace_util_0.Count(_join_00000, 143)
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				trace_util_0.Count(_join_00000, 144)
				return false, joinResult
			}
		}
	}
	trace_util_0.Count(_join_00000, 136)
	return true, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from inner child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	trace_util_0.Count(_join_00000, 145)
	if e.runtimeStats != nil {
		trace_util_0.Count(_join_00000, 152)
		start := time.Now()
		defer func() { trace_util_0.Count(_join_00000, 153); e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	trace_util_0.Count(_join_00000, 146)
	if !e.prepared {
		trace_util_0.Count(_join_00000, 154)
		e.innerFinished = make(chan error, 1)
		go util.WithRecovery(func() { trace_util_0.Count(_join_00000, 156); e.fetchInnerAndBuildHashTable(ctx) }, e.handleFetchInnerAndBuildHashTablePanic)
		trace_util_0.Count(_join_00000, 155)
		e.fetchOuterAndProbeHashTable(ctx)
		e.prepared = true
	}
	trace_util_0.Count(_join_00000, 147)
	if e.isOuterJoin {
		trace_util_0.Count(_join_00000, 157)
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	trace_util_0.Count(_join_00000, 148)
	req.Reset()
	if e.joinResultCh == nil {
		trace_util_0.Count(_join_00000, 158)
		return nil
	}

	trace_util_0.Count(_join_00000, 149)
	result, ok := <-e.joinResultCh
	if !ok {
		trace_util_0.Count(_join_00000, 159)
		return nil
	}
	trace_util_0.Count(_join_00000, 150)
	if result.err != nil {
		trace_util_0.Count(_join_00000, 160)
		e.finished.Store(true)
		return result.err
	}
	trace_util_0.Count(_join_00000, 151)
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinExec) handleFetchInnerAndBuildHashTablePanic(r interface{}) {
	trace_util_0.Count(_join_00000, 161)
	if r != nil {
		trace_util_0.Count(_join_00000, 163)
		e.innerFinished <- errors.Errorf("%v", r)
	}
	trace_util_0.Count(_join_00000, 162)
	close(e.innerFinished)
}

func (e *HashJoinExec) fetchInnerAndBuildHashTable(ctx context.Context) {
	trace_util_0.Count(_join_00000, 164)
	// innerResultCh transfers inner chunk from inner fetch to build hash table.
	innerResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	go util.WithRecovery(func() { trace_util_0.Count(_join_00000, 167); e.fetchInnerRows(ctx, innerResultCh, doneCh) }, nil)

	// TODO: Parallel build hash table. Currently not support because `mvmap` is not thread-safe.
	trace_util_0.Count(_join_00000, 165)
	err := e.buildHashTableForList(innerResultCh)
	if err != nil {
		trace_util_0.Count(_join_00000, 168)
		e.innerFinished <- errors.Trace(err)
		close(doneCh)
	}
	// wait fetchInnerRows be finished.
	trace_util_0.Count(_join_00000, 166)
	for range innerResultCh {
		trace_util_0.Count(_join_00000, 169)
	}
}

// buildHashTableForList builds hash table from `list`.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (e *HashJoinExec) buildHashTableForList(innerResultCh chan *chunk.Chunk) error {
	trace_util_0.Count(_join_00000, 170)
	e.hashTable = mvmap.NewMVMap()
	e.innerKeyColIdx = make([]int, len(e.innerKeys))
	for i := range e.innerKeys {
		trace_util_0.Count(_join_00000, 173)
		e.innerKeyColIdx[i] = e.innerKeys[i].Index
	}
	trace_util_0.Count(_join_00000, 171)
	var (
		hasNull bool
		err     error
		keyBuf  = make([]byte, 0, 64)
		valBuf  = make([]byte, 8)
	)

	chkIdx := uint32(0)
	for chk := range innerResultCh {
		trace_util_0.Count(_join_00000, 174)
		if e.finished.Load().(bool) {
			trace_util_0.Count(_join_00000, 177)
			return nil
		}
		trace_util_0.Count(_join_00000, 175)
		numRows := chk.NumRows()
		for j := 0; j < numRows; j++ {
			trace_util_0.Count(_join_00000, 178)
			hasNull, keyBuf, err = e.getJoinKeyFromChkRow(false, chk.GetRow(j), keyBuf)
			if err != nil {
				trace_util_0.Count(_join_00000, 181)
				return errors.Trace(err)
			}
			trace_util_0.Count(_join_00000, 179)
			if hasNull {
				trace_util_0.Count(_join_00000, 182)
				continue
			}
			trace_util_0.Count(_join_00000, 180)
			rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			e.hashTable.Put(keyBuf, valBuf)
		}
		trace_util_0.Count(_join_00000, 176)
		chkIdx++
	}
	trace_util_0.Count(_join_00000, 172)
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerRows   []chunk.Row
	cursor      int
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs
	outer       bool

	joiner joiner

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	trace_util_0.Count(_join_00000, 183)
	e.innerRows = nil

	e.memTracker.Detach()
	e.memTracker = nil
	return e.outerExec.Close()
}

var innerListLabel fmt.Stringer = stringutil.StringerStr("innerList")

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	trace_util_0.Count(_join_00000, 184)
	err := e.outerExec.Open(ctx)
	if err != nil {
		trace_util_0.Count(_join_00000, 186)
		return err
	}
	trace_util_0.Count(_join_00000, 185)
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = newFirstChunk(e.outerExec)
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaNestedLoopApply)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(innerListLabel)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	return nil
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	trace_util_0.Count(_join_00000, 187)
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		trace_util_0.Count(_join_00000, 188)
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			trace_util_0.Count(_join_00000, 190)
			err := Next(ctx, e.outerExec, chunk.NewRecordBatch(e.outerChunk))
			if err != nil {
				trace_util_0.Count(_join_00000, 194)
				return nil, err
			}
			trace_util_0.Count(_join_00000, 191)
			if e.outerChunk.NumRows() == 0 {
				trace_util_0.Count(_join_00000, 195)
				return nil, nil
			}
			trace_util_0.Count(_join_00000, 192)
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				trace_util_0.Count(_join_00000, 196)
				return nil, err
			}
			trace_util_0.Count(_join_00000, 193)
			e.outerChunkCursor = 0
		}
		trace_util_0.Count(_join_00000, 189)
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			trace_util_0.Count(_join_00000, 197)
			return &outerRow, nil
		} else {
			trace_util_0.Count(_join_00000, 198)
			if e.outer {
				trace_util_0.Count(_join_00000, 199)
				e.joiner.onMissMatch(false, outerRow, chk)
				if chk.IsFull() {
					trace_util_0.Count(_join_00000, 200)
					return nil, nil
				}
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	trace_util_0.Count(_join_00000, 201)
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		trace_util_0.Count(_join_00000, 203)
		return err
	}
	trace_util_0.Count(_join_00000, 202)
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		trace_util_0.Count(_join_00000, 204)
		err := Next(ctx, e.innerExec, chunk.NewRecordBatch(e.innerChunk))
		if err != nil {
			trace_util_0.Count(_join_00000, 208)
			return err
		}
		trace_util_0.Count(_join_00000, 205)
		if e.innerChunk.NumRows() == 0 {
			trace_util_0.Count(_join_00000, 209)
			return nil
		}

		trace_util_0.Count(_join_00000, 206)
		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			trace_util_0.Count(_join_00000, 210)
			return err
		}
		trace_util_0.Count(_join_00000, 207)
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			trace_util_0.Count(_join_00000, 211)
			if e.innerSelected[row.Idx()] {
				trace_util_0.Count(_join_00000, 212)
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	trace_util_0.Count(_join_00000, 213)
	if e.runtimeStats != nil {
		trace_util_0.Count(_join_00000, 215)
		start := time.Now()
		defer func() { trace_util_0.Count(_join_00000, 216); e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	trace_util_0.Count(_join_00000, 214)
	req.Reset()
	for {
		trace_util_0.Count(_join_00000, 217)
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			trace_util_0.Count(_join_00000, 219)
			if e.outerRow != nil && !e.hasMatch {
				trace_util_0.Count(_join_00000, 224)
				e.joiner.onMissMatch(e.hasNull, *e.outerRow, req.Chunk)
			}
			trace_util_0.Count(_join_00000, 220)
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req.Chunk)
			if e.outerRow == nil || err != nil {
				trace_util_0.Count(_join_00000, 225)
				return err
			}
			trace_util_0.Count(_join_00000, 221)
			e.hasMatch = false
			e.hasNull = false

			for _, col := range e.outerSchema {
				trace_util_0.Count(_join_00000, 226)
				*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
			}
			trace_util_0.Count(_join_00000, 222)
			err = e.fetchAllInners(ctx)
			if err != nil {
				trace_util_0.Count(_join_00000, 227)
				return err
			}
			trace_util_0.Count(_join_00000, 223)
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		trace_util_0.Count(_join_00000, 218)
		matched, isNull, err := e.joiner.tryToMatch(*e.outerRow, e.innerIter, req.Chunk)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			trace_util_0.Count(_join_00000, 228)
			return err
		}
	}
}

var _join_00000 = "executor/join.go"
