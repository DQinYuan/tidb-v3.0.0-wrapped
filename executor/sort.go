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
	"container/heap"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var rowChunksLabel fmt.Stringer = stringutil.StringerStr("rowChunks")

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*plannercore.ByItems
	Idx     int
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	rowPtrs []chunk.RowPtr

	memTracker *memory.Tracker
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	trace_util_0.Count(_sort_00000, 0)
	e.memTracker.Detach()
	e.memTracker = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	trace_util_0.Count(_sort_00000, 1)
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		trace_util_0.Count(_sort_00000, 3)
		e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaSort)
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	}
	trace_util_0.Count(_sort_00000, 2)
	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
func (e *SortExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_sort_00000, 4)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_sort_00000, 9)
		span1 := span.Tracer().StartSpan("sort.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_sort_00000, 5)
	if e.runtimeStats != nil {
		trace_util_0.Count(_sort_00000, 10)
		start := time.Now()
		defer func() { trace_util_0.Count(_sort_00000, 11); e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	trace_util_0.Count(_sort_00000, 6)
	req.Reset()
	if !e.fetched {
		trace_util_0.Count(_sort_00000, 12)
		err := e.fetchRowChunks(ctx)
		if err != nil {
			trace_util_0.Count(_sort_00000, 14)
			return err
		}
		trace_util_0.Count(_sort_00000, 13)
		e.initPointers()
		e.initCompareFuncs()
		e.buildKeyColumns()
		sort.Slice(e.rowPtrs, e.keyColumnsLess)
		e.fetched = true
	}
	trace_util_0.Count(_sort_00000, 7)
	for !req.IsFull() && e.Idx < len(e.rowPtrs) {
		trace_util_0.Count(_sort_00000, 15)
		rowPtr := e.rowPtrs[e.Idx]
		req.AppendRow(e.rowChunks.GetRow(rowPtr))
		e.Idx++
	}
	trace_util_0.Count(_sort_00000, 8)
	return nil
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	trace_util_0.Count(_sort_00000, 16)
	fields := retTypes(e)
	e.rowChunks = chunk.NewList(fields, e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for {
		trace_util_0.Count(_sort_00000, 18)
		chk := newFirstChunk(e.children[0])
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_sort_00000, 21)
			return err
		}
		trace_util_0.Count(_sort_00000, 19)
		rowCount := chk.NumRows()
		if rowCount == 0 {
			trace_util_0.Count(_sort_00000, 22)
			break
		}
		trace_util_0.Count(_sort_00000, 20)
		e.rowChunks.Add(chk)
	}
	trace_util_0.Count(_sort_00000, 17)
	return nil
}

func (e *SortExec) initPointers() {
	trace_util_0.Count(_sort_00000, 23)
	e.rowPtrs = make([]chunk.RowPtr, 0, e.rowChunks.Len())
	e.memTracker.Consume(int64(8 * e.rowChunks.Len()))
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		trace_util_0.Count(_sort_00000, 24)
		rowChk := e.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			trace_util_0.Count(_sort_00000, 25)
			e.rowPtrs = append(e.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (e *SortExec) initCompareFuncs() {
	trace_util_0.Count(_sort_00000, 26)
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		trace_util_0.Count(_sort_00000, 27)
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	trace_util_0.Count(_sort_00000, 28)
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		trace_util_0.Count(_sort_00000, 29)
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	trace_util_0.Count(_sort_00000, 30)
	for i, colIdx := range e.keyColumns {
		trace_util_0.Count(_sort_00000, 32)
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			trace_util_0.Count(_sort_00000, 34)
			cmp = -cmp
		}
		trace_util_0.Count(_sort_00000, 33)
		if cmp < 0 {
			trace_util_0.Count(_sort_00000, 35)
			return true
		} else {
			trace_util_0.Count(_sort_00000, 36)
			if cmp > 0 {
				trace_util_0.Count(_sort_00000, 37)
				return false
			}
		}
	}
	trace_util_0.Count(_sort_00000, 31)
	return false
}

// keyColumnsLess is the less function for key columns.
func (e *SortExec) keyColumnsLess(i, j int) bool {
	trace_util_0.Count(_sort_00000, 38)
	rowI := e.rowChunks.GetRow(e.rowPtrs[i])
	rowJ := e.rowChunks.GetRow(e.rowPtrs[j])
	return e.lessRow(rowI, rowJ)
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plannercore.PhysicalLimit
	totalLimit uint64

	chkHeap *topNChunkHeap
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	trace_util_0.Count(_sort_00000, 39)
	rowI := h.rowChunks.GetRow(h.rowPtrs[i])
	rowJ := h.rowChunks.GetRow(h.rowPtrs[j])
	return h.greaterRow(rowI, rowJ)
}

func (h *topNChunkHeap) greaterRow(rowI, rowJ chunk.Row) bool {
	trace_util_0.Count(_sort_00000, 40)
	for i, colIdx := range h.keyColumns {
		trace_util_0.Count(_sort_00000, 42)
		cmpFunc := h.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if h.ByItems[i].Desc {
			trace_util_0.Count(_sort_00000, 44)
			cmp = -cmp
		}
		trace_util_0.Count(_sort_00000, 43)
		if cmp > 0 {
			trace_util_0.Count(_sort_00000, 45)
			return true
		} else {
			trace_util_0.Count(_sort_00000, 46)
			if cmp < 0 {
				trace_util_0.Count(_sort_00000, 47)
				return false
			}
		}
	}
	trace_util_0.Count(_sort_00000, 41)
	return false
}

func (h *topNChunkHeap) Len() int {
	trace_util_0.Count(_sort_00000, 48)
	return len(h.rowPtrs)
}

func (h *topNChunkHeap) Push(x interface{}) {
	trace_util_0.Count(_sort_00000, 49)
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	trace_util_0.Count(_sort_00000, 50)
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	trace_util_0.Count(_sort_00000, 51)
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	trace_util_0.Count(_sort_00000, 52)
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaTopn)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	return e.SortExec.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_sort_00000, 53)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_sort_00000, 59)
		span1 := span.Tracer().StartSpan("topN.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_sort_00000, 54)
	if e.runtimeStats != nil {
		trace_util_0.Count(_sort_00000, 60)
		start := time.Now()
		defer func() { trace_util_0.Count(_sort_00000, 61); e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	trace_util_0.Count(_sort_00000, 55)
	req.Reset()
	if !e.fetched {
		trace_util_0.Count(_sort_00000, 62)
		e.totalLimit = e.limit.Offset + e.limit.Count
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit(ctx)
		if err != nil {
			trace_util_0.Count(_sort_00000, 65)
			return err
		}
		trace_util_0.Count(_sort_00000, 63)
		err = e.executeTopN(ctx)
		if err != nil {
			trace_util_0.Count(_sort_00000, 66)
			return err
		}
		trace_util_0.Count(_sort_00000, 64)
		e.fetched = true
	}
	trace_util_0.Count(_sort_00000, 56)
	if e.Idx >= len(e.rowPtrs) {
		trace_util_0.Count(_sort_00000, 67)
		return nil
	}
	trace_util_0.Count(_sort_00000, 57)
	for !req.IsFull() && e.Idx < len(e.rowPtrs) {
		trace_util_0.Count(_sort_00000, 68)
		row := e.rowChunks.GetRow(e.rowPtrs[e.Idx])
		req.AppendRow(row)
		e.Idx++
	}
	trace_util_0.Count(_sort_00000, 58)
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	trace_util_0.Count(_sort_00000, 69)
	e.chkHeap = &topNChunkHeap{e}
	e.rowChunks = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for uint64(e.rowChunks.Len()) < e.totalLimit {
		trace_util_0.Count(_sort_00000, 71)
		srcChk := newFirstChunk(e.children[0])
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.totalLimit-uint64(e.rowChunks.Len())), e.maxChunkSize)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(srcChk))
		if err != nil {
			trace_util_0.Count(_sort_00000, 74)
			return err
		}
		trace_util_0.Count(_sort_00000, 72)
		if srcChk.NumRows() == 0 {
			trace_util_0.Count(_sort_00000, 75)
			break
		}
		trace_util_0.Count(_sort_00000, 73)
		e.rowChunks.Add(srcChk)
	}
	trace_util_0.Count(_sort_00000, 70)
	e.initPointers()
	e.initCompareFuncs()
	e.buildKeyColumns()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(ctx context.Context) error {
	trace_util_0.Count(_sort_00000, 76)
	heap.Init(e.chkHeap)
	for uint64(len(e.rowPtrs)) > e.totalLimit {
		trace_util_0.Count(_sort_00000, 79)
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	trace_util_0.Count(_sort_00000, 77)
	childRowChk := newFirstChunk(e.children[0])
	for {
		trace_util_0.Count(_sort_00000, 80)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(childRowChk))
		if err != nil {
			trace_util_0.Count(_sort_00000, 84)
			return err
		}
		trace_util_0.Count(_sort_00000, 81)
		if childRowChk.NumRows() == 0 {
			trace_util_0.Count(_sort_00000, 85)
			break
		}
		trace_util_0.Count(_sort_00000, 82)
		err = e.processChildChk(childRowChk)
		if err != nil {
			trace_util_0.Count(_sort_00000, 86)
			return err
		}
		trace_util_0.Count(_sort_00000, 83)
		if e.rowChunks.Len() > len(e.rowPtrs)*topNCompactionFactor {
			trace_util_0.Count(_sort_00000, 87)
			err = e.doCompaction()
			if err != nil {
				trace_util_0.Count(_sort_00000, 88)
				return err
			}
		}
	}
	trace_util_0.Count(_sort_00000, 78)
	sort.Slice(e.rowPtrs, e.keyColumnsLess)
	return nil
}

func (e *TopNExec) processChildChk(childRowChk *chunk.Chunk) error {
	trace_util_0.Count(_sort_00000, 89)
	for i := 0; i < childRowChk.NumRows(); i++ {
		trace_util_0.Count(_sort_00000, 91)
		heapMaxPtr := e.rowPtrs[0]
		var heapMax, next chunk.Row
		heapMax = e.rowChunks.GetRow(heapMaxPtr)
		next = childRowChk.GetRow(i)
		if e.chkHeap.greaterRow(heapMax, next) {
			trace_util_0.Count(_sort_00000, 92)
			// Evict heap max, keep the next row.
			e.rowPtrs[0] = e.rowChunks.AppendRow(childRowChk.GetRow(i))
			heap.Fix(e.chkHeap, 0)
		}
	}
	trace_util_0.Count(_sort_00000, 90)
	return nil
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction() error {
	trace_util_0.Count(_sort_00000, 93)
	newRowChunks := chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	newRowPtrs := make([]chunk.RowPtr, 0, e.rowChunks.Len())
	for _, rowPtr := range e.rowPtrs {
		trace_util_0.Count(_sort_00000, 95)
		newRowPtr := newRowChunks.AppendRow(e.rowChunks.GetRow(rowPtr))
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	trace_util_0.Count(_sort_00000, 94)
	newRowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	e.memTracker.ReplaceChild(e.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	e.rowChunks = newRowChunks

	e.memTracker.Consume(int64(-8 * len(e.rowPtrs)))
	e.memTracker.Consume(int64(8 * len(newRowPtrs)))
	e.rowPtrs = newRowPtrs
	return nil
}

var _sort_00000 = "executor/sort.go"
