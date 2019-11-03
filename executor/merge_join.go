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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	baseExecutor

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []expression.CompareFunc
	joiner       joiner
	isOuterJoin  bool

	prepared bool
	outerIdx int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	innerRows     []chunk.Row
	innerIter4Row chunk.Iterator

	childrenResults []*chunk.Chunk

	memTracker *memory.Tracker
}

type mergeJoinOuterTable struct {
	reader Executor
	filter []expression.Expression
	keys   []*expression.Column

	chk      *chunk.Chunk
	selected []bool

	iter     *chunk.Iterator4Chunk
	row      chunk.Row
	hasMatch bool
	hasNull  bool
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
	reader   Executor
	joinKeys []*expression.Column
	ctx      context.Context

	// for chunk executions
	sameKeyRows    []chunk.Row
	keyCmpFuncs    []chunk.CompareFunc
	firstRow4Key   chunk.Row
	curRow         chunk.Row
	curResult      *chunk.Chunk
	curIter        *chunk.Iterator4Chunk
	curResultInUse bool
	resultQueue    []*chunk.Chunk
	resourceQueue  []*chunk.Chunk

	memTracker *memory.Tracker
}

func (t *mergeJoinInnerTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	trace_util_0.Count(_merge_join_00000, 0)
	if t.reader == nil || ctx == nil {
		trace_util_0.Count(_merge_join_00000, 3)
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	trace_util_0.Count(_merge_join_00000, 1)
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.resultQueue = append(t.resultQueue, chk4Reader)
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.keyCmpFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		trace_util_0.Count(_merge_join_00000, 4)
		t.keyCmpFuncs = append(t.keyCmpFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	trace_util_0.Count(_merge_join_00000, 2)
	return err
}

func (t *mergeJoinInnerTable) rowsWithSameKey() ([]chunk.Row, error) {
	trace_util_0.Count(_merge_join_00000, 5)
	lastResultIdx := len(t.resultQueue) - 1
	t.resourceQueue = append(t.resourceQueue, t.resultQueue[0:lastResultIdx]...)
	t.resultQueue = t.resultQueue[lastResultIdx:]
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		trace_util_0.Count(_merge_join_00000, 7)
		return nil, nil
	}
	trace_util_0.Count(_merge_join_00000, 6)
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		trace_util_0.Count(_merge_join_00000, 8)
		selectedRow, err := t.nextRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			trace_util_0.Count(_merge_join_00000, 10)
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, err
		}
		trace_util_0.Count(_merge_join_00000, 9)
		compareResult := compareChunkRow(t.keyCmpFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			trace_util_0.Count(_merge_join_00000, 11)
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			trace_util_0.Count(_merge_join_00000, 12)
			{
				t.firstRow4Key = selectedRow
				return t.sameKeyRows, nil
			}
		}
	}
}

func (t *mergeJoinInnerTable) nextRow() (chunk.Row, error) {
	trace_util_0.Count(_merge_join_00000, 13)
	for {
		trace_util_0.Count(_merge_join_00000, 14)
		if t.curRow == t.curIter.End() {
			trace_util_0.Count(_merge_join_00000, 16)
			t.reallocReaderResult()
			oldMemUsage := t.curResult.MemoryUsage()
			err := Next(t.ctx, t.reader, chunk.NewRecordBatch(t.curResult))
			// error happens or no more data.
			if err != nil || t.curResult.NumRows() == 0 {
				trace_util_0.Count(_merge_join_00000, 18)
				t.curRow = t.curIter.End()
				return t.curRow, err
			}
			trace_util_0.Count(_merge_join_00000, 17)
			newMemUsage := t.curResult.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curRow = t.curIter.Begin()
		}

		trace_util_0.Count(_merge_join_00000, 15)
		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			trace_util_0.Count(_merge_join_00000, 19)
			return result, nil
		}
	}
}

func (t *mergeJoinInnerTable) hasNullInJoinKey(row chunk.Row) bool {
	trace_util_0.Count(_merge_join_00000, 20)
	for _, col := range t.joinKeys {
		trace_util_0.Count(_merge_join_00000, 22)
		ordinal := col.Index
		if row.IsNull(ordinal) {
			trace_util_0.Count(_merge_join_00000, 23)
			return true
		}
	}
	trace_util_0.Count(_merge_join_00000, 21)
	return false
}

// reallocReaderResult resets "t.curResult" to an empty Chunk to buffer the result of "t.reader".
// It pops a Chunk from "t.resourceQueue" and push it into "t.resultQueue" immediately.
func (t *mergeJoinInnerTable) reallocReaderResult() {
	trace_util_0.Count(_merge_join_00000, 24)
	if !t.curResultInUse {
		trace_util_0.Count(_merge_join_00000, 27)
		// If "t.curResult" is not in use, we can just reuse it.
		t.curResult.Reset()
		return
	}

	// Create a new Chunk and append it to "resourceQueue" if there is no more
	// available chunk in "resourceQueue".
	trace_util_0.Count(_merge_join_00000, 25)
	if len(t.resourceQueue) == 0 {
		trace_util_0.Count(_merge_join_00000, 28)
		newChunk := newFirstChunk(t.reader)
		t.memTracker.Consume(newChunk.MemoryUsage())
		t.resourceQueue = append(t.resourceQueue, newChunk)
	}

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	trace_util_0.Count(_merge_join_00000, 26)
	t.curResult = t.resourceQueue[0]
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.resourceQueue = t.resourceQueue[1:]
	t.resultQueue = append(t.resultQueue, t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	trace_util_0.Count(_merge_join_00000, 29)
	e.memTracker.Detach()
	e.childrenResults = nil
	e.memTracker = nil

	return e.baseExecutor.Close()
}

var innerTableLabel fmt.Stringer = stringutil.StringerStr("innerTable")

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	trace_util_0.Count(_merge_join_00000, 30)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_merge_join_00000, 33)
		return err
	}

	trace_util_0.Count(_merge_join_00000, 31)
	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		trace_util_0.Count(_merge_join_00000, 34)
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}

	trace_util_0.Count(_merge_join_00000, 32)
	e.innerTable.memTracker = memory.NewTracker(innerTableLabel, -1)
	e.innerTable.memTracker.AttachTo(e.memTracker)

	return nil
}

func compareChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	trace_util_0.Count(_merge_join_00000, 35)
	for i := range lhsKey {
		trace_util_0.Count(_merge_join_00000, 37)
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			trace_util_0.Count(_merge_join_00000, 38)
			return cmp
		}
	}
	trace_util_0.Count(_merge_join_00000, 36)
	return 0
}

func (e *MergeJoinExec) prepare(ctx context.Context, requiredRows int) error {
	trace_util_0.Count(_merge_join_00000, 39)
	err := e.innerTable.init(ctx, e.childrenResults[e.outerIdx^1])
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 43)
		return err
	}

	trace_util_0.Count(_merge_join_00000, 40)
	err = e.fetchNextInnerRows()
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 44)
		return err
	}

	// init outer table.
	trace_util_0.Count(_merge_join_00000, 41)
	e.outerTable.chk = e.childrenResults[e.outerIdx]
	e.outerTable.iter = chunk.NewIterator4Chunk(e.outerTable.chk)
	e.outerTable.selected = make([]bool, 0, e.maxChunkSize)

	err = e.fetchNextOuterRows(ctx, requiredRows)
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 45)
		return err
	}

	trace_util_0.Count(_merge_join_00000, 42)
	e.prepared = true
	return nil
}

// Next implements the Executor Next interface.
func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_merge_join_00000, 46)
	if e.runtimeStats != nil {
		trace_util_0.Count(_merge_join_00000, 50)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_merge_join_00000, 51)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_merge_join_00000, 47)
	req.Reset()
	if !e.prepared {
		trace_util_0.Count(_merge_join_00000, 52)
		if err := e.prepare(ctx, req.RequiredRows()); err != nil {
			trace_util_0.Count(_merge_join_00000, 53)
			return err
		}
	}

	trace_util_0.Count(_merge_join_00000, 48)
	for !req.IsFull() {
		trace_util_0.Count(_merge_join_00000, 54)
		hasMore, err := e.joinToChunk(ctx, req.Chunk)
		if err != nil || !hasMore {
			trace_util_0.Count(_merge_join_00000, 55)
			return err
		}
	}
	trace_util_0.Count(_merge_join_00000, 49)
	return nil
}

func (e *MergeJoinExec) joinToChunk(ctx context.Context, chk *chunk.Chunk) (hasMore bool, err error) {
	trace_util_0.Count(_merge_join_00000, 56)
	for {
		trace_util_0.Count(_merge_join_00000, 57)
		if e.outerTable.row == e.outerTable.iter.End() {
			trace_util_0.Count(_merge_join_00000, 64)
			err = e.fetchNextOuterRows(ctx, chk.RequiredRows()-chk.NumRows())
			if err != nil || e.outerTable.chk.NumRows() == 0 {
				trace_util_0.Count(_merge_join_00000, 65)
				return false, err
			}
		}

		trace_util_0.Count(_merge_join_00000, 58)
		cmpResult := -1
		if e.outerTable.selected[e.outerTable.row.Idx()] && len(e.innerRows) > 0 {
			trace_util_0.Count(_merge_join_00000, 66)
			cmpResult, err = e.compare(e.outerTable.row, e.innerIter4Row.Current())
			if err != nil {
				trace_util_0.Count(_merge_join_00000, 67)
				return false, err
			}
		}

		trace_util_0.Count(_merge_join_00000, 59)
		if cmpResult > 0 {
			trace_util_0.Count(_merge_join_00000, 68)
			if err = e.fetchNextInnerRows(); err != nil {
				trace_util_0.Count(_merge_join_00000, 70)
				return false, err
			}
			trace_util_0.Count(_merge_join_00000, 69)
			continue
		}

		trace_util_0.Count(_merge_join_00000, 60)
		if cmpResult < 0 {
			trace_util_0.Count(_merge_join_00000, 71)
			e.joiner.onMissMatch(false, e.outerTable.row, chk)
			if err != nil {
				trace_util_0.Count(_merge_join_00000, 74)
				return false, err
			}

			trace_util_0.Count(_merge_join_00000, 72)
			e.outerTable.row = e.outerTable.iter.Next()
			e.outerTable.hasMatch = false
			e.outerTable.hasNull = false

			if chk.IsFull() {
				trace_util_0.Count(_merge_join_00000, 75)
				return true, nil
			}
			trace_util_0.Count(_merge_join_00000, 73)
			continue
		}

		trace_util_0.Count(_merge_join_00000, 61)
		matched, isNull, err := e.joiner.tryToMatch(e.outerTable.row, e.innerIter4Row, chk)
		if err != nil {
			trace_util_0.Count(_merge_join_00000, 76)
			return false, err
		}
		trace_util_0.Count(_merge_join_00000, 62)
		e.outerTable.hasMatch = e.outerTable.hasMatch || matched
		e.outerTable.hasNull = e.outerTable.hasNull || isNull

		if e.innerIter4Row.Current() == e.innerIter4Row.End() {
			trace_util_0.Count(_merge_join_00000, 77)
			if !e.outerTable.hasMatch {
				trace_util_0.Count(_merge_join_00000, 79)
				e.joiner.onMissMatch(e.outerTable.hasNull, e.outerTable.row, chk)
			}
			trace_util_0.Count(_merge_join_00000, 78)
			e.outerTable.row = e.outerTable.iter.Next()
			e.outerTable.hasMatch = false
			e.outerTable.hasNull = false
			e.innerIter4Row.Begin()
		}

		trace_util_0.Count(_merge_join_00000, 63)
		if chk.IsFull() {
			trace_util_0.Count(_merge_join_00000, 80)
			return true, err
		}
	}
}

func (e *MergeJoinExec) compare(outerRow, innerRow chunk.Row) (int, error) {
	trace_util_0.Count(_merge_join_00000, 81)
	outerJoinKeys := e.outerTable.keys
	innerJoinKeys := e.innerTable.joinKeys
	for i := range outerJoinKeys {
		trace_util_0.Count(_merge_join_00000, 83)
		cmp, _, err := e.compareFuncs[i](e.ctx, outerJoinKeys[i], innerJoinKeys[i], outerRow, innerRow)
		if err != nil {
			trace_util_0.Count(_merge_join_00000, 85)
			return 0, err
		}

		trace_util_0.Count(_merge_join_00000, 84)
		if cmp != 0 {
			trace_util_0.Count(_merge_join_00000, 86)
			return int(cmp), nil
		}
	}
	trace_util_0.Count(_merge_join_00000, 82)
	return 0, nil
}

// fetchNextInnerRows fetches the next join group, within which all the rows
// have the same join key, from the inner table.
func (e *MergeJoinExec) fetchNextInnerRows() (err error) {
	trace_util_0.Count(_merge_join_00000, 87)
	e.innerRows, err = e.innerTable.rowsWithSameKey()
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 89)
		return err
	}
	trace_util_0.Count(_merge_join_00000, 88)
	e.innerIter4Row = chunk.NewIterator4Slice(e.innerRows)
	e.innerIter4Row.Begin()
	return nil
}

// fetchNextOuterRows fetches the next Chunk of outer table. Rows in a Chunk
// may not all belong to the same join key, but are guaranteed to be sorted
// according to the join key.
func (e *MergeJoinExec) fetchNextOuterRows(ctx context.Context, requiredRows int) (err error) {
	trace_util_0.Count(_merge_join_00000, 90)
	// It's hard to calculate selectivity if there is any filter or it's inner join,
	// so we just push the requiredRows down when it's outer join and has no filter.
	if e.isOuterJoin && len(e.outerTable.filter) == 0 {
		trace_util_0.Count(_merge_join_00000, 94)
		e.outerTable.chk.SetRequiredRows(requiredRows, e.maxChunkSize)
	}

	trace_util_0.Count(_merge_join_00000, 91)
	err = Next(ctx, e.outerTable.reader, chunk.NewRecordBatch(e.outerTable.chk))
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 95)
		return err
	}

	trace_util_0.Count(_merge_join_00000, 92)
	e.outerTable.iter.Begin()
	e.outerTable.selected, err = expression.VectorizedFilter(e.ctx, e.outerTable.filter, e.outerTable.iter, e.outerTable.selected)
	if err != nil {
		trace_util_0.Count(_merge_join_00000, 96)
		return err
	}
	trace_util_0.Count(_merge_join_00000, 93)
	e.outerTable.row = e.outerTable.iter.Begin()
	return nil
}

var _merge_join_00000 = "executor/merge_join.go"
