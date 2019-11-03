// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker         *groupChecker
	inputIter            *chunk.Iterator4Chunk
	inputRow             chunk.Row
	groupRows            []chunk.Row
	childResults         []*chunk.Chunk
	executed             bool
	meetNewGroup         bool
	remainingRowsInGroup int
	remainingRowsInChunk int
	numWindowFuncs       int
	processor            windowProcessor
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	trace_util_0.Count(_window_00000, 0)
	e.childResults = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.RecordBatch) error {
	trace_util_0.Count(_window_00000, 1)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_window_00000, 6)
		span1 := span.Tracer().StartSpan("windowExec.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_window_00000, 2)
	if e.runtimeStats != nil {
		trace_util_0.Count(_window_00000, 7)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_window_00000, 8)
			e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows())
		}()
	}
	trace_util_0.Count(_window_00000, 3)
	chk.Reset()
	if e.meetNewGroup && e.remainingRowsInGroup > 0 {
		trace_util_0.Count(_window_00000, 9)
		err := e.appendResult2Chunk(chk.Chunk)
		if err != nil {
			trace_util_0.Count(_window_00000, 10)
			return err
		}
	}
	trace_util_0.Count(_window_00000, 4)
	for !e.executed && (chk.NumRows() == 0 || e.remainingRowsInChunk > 0) {
		trace_util_0.Count(_window_00000, 11)
		err := e.consumeOneGroup(ctx, chk.Chunk)
		if err != nil {
			trace_util_0.Count(_window_00000, 12)
			e.executed = true
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_window_00000, 5)
	return nil
}

func (e *WindowExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_window_00000, 13)
	var err error
	if err = e.fetchChildIfNecessary(ctx, chk); err != nil {
		trace_util_0.Count(_window_00000, 16)
		return errors.Trace(err)
	}
	trace_util_0.Count(_window_00000, 14)
	for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
		trace_util_0.Count(_window_00000, 17)
		e.meetNewGroup, err = e.groupChecker.meetNewGroup(e.inputRow)
		if err != nil {
			trace_util_0.Count(_window_00000, 20)
			return errors.Trace(err)
		}
		trace_util_0.Count(_window_00000, 18)
		if e.meetNewGroup {
			trace_util_0.Count(_window_00000, 21)
			err := e.consumeGroupRows()
			if err != nil {
				trace_util_0.Count(_window_00000, 23)
				return errors.Trace(err)
			}
			trace_util_0.Count(_window_00000, 22)
			err = e.appendResult2Chunk(chk)
			if err != nil {
				trace_util_0.Count(_window_00000, 24)
				return errors.Trace(err)
			}
		}
		trace_util_0.Count(_window_00000, 19)
		e.remainingRowsInGroup++
		e.groupRows = append(e.groupRows, e.inputRow)
		if e.meetNewGroup {
			trace_util_0.Count(_window_00000, 25)
			e.inputRow = e.inputIter.Next()
			return nil
		}
	}
	trace_util_0.Count(_window_00000, 15)
	return nil
}

func (e *WindowExec) consumeGroupRows() (err error) {
	trace_util_0.Count(_window_00000, 26)
	if len(e.groupRows) == 0 {
		trace_util_0.Count(_window_00000, 29)
		return nil
	}
	trace_util_0.Count(_window_00000, 27)
	e.groupRows, err = e.processor.consumeGroupRows(e.ctx, e.groupRows)
	if err != nil {
		trace_util_0.Count(_window_00000, 30)
		return errors.Trace(err)
	}
	trace_util_0.Count(_window_00000, 28)
	return nil
}

func (e *WindowExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) (err error) {
	trace_util_0.Count(_window_00000, 31)
	if e.inputIter != nil && e.inputRow != e.inputIter.End() {
		trace_util_0.Count(_window_00000, 36)
		return nil
	}

	// Before fetching a new batch of input, we should consume the last group rows.
	trace_util_0.Count(_window_00000, 32)
	err = e.consumeGroupRows()
	if err != nil {
		trace_util_0.Count(_window_00000, 37)
		return errors.Trace(err)
	}

	trace_util_0.Count(_window_00000, 33)
	childResult := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], &chunk.RecordBatch{Chunk: childResult})
	if err != nil {
		trace_util_0.Count(_window_00000, 38)
		return errors.Trace(err)
	}
	trace_util_0.Count(_window_00000, 34)
	e.childResults = append(e.childResults, childResult)
	// No more data.
	if childResult.NumRows() == 0 {
		trace_util_0.Count(_window_00000, 39)
		e.executed = true
		err = e.appendResult2Chunk(chk)
		return errors.Trace(err)
	}

	trace_util_0.Count(_window_00000, 35)
	e.inputIter = chunk.NewIterator4Chunk(childResult)
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of the window function to the result chunk.
func (e *WindowExec) appendResult2Chunk(chk *chunk.Chunk) (err error) {
	trace_util_0.Count(_window_00000, 40)
	e.copyChk(chk)
	remained := mathutil.Min(e.remainingRowsInChunk, e.remainingRowsInGroup)
	e.groupRows, err = e.processor.appendResult2Chunk(e.ctx, e.groupRows, chk, remained)
	if err != nil {
		trace_util_0.Count(_window_00000, 43)
		return err
	}
	trace_util_0.Count(_window_00000, 41)
	e.remainingRowsInGroup -= remained
	e.remainingRowsInChunk -= remained
	if e.remainingRowsInGroup == 0 {
		trace_util_0.Count(_window_00000, 44)
		e.processor.resetPartialResult()
		e.groupRows = e.groupRows[:0]
	}
	trace_util_0.Count(_window_00000, 42)
	return nil
}

func (e *WindowExec) copyChk(chk *chunk.Chunk) {
	trace_util_0.Count(_window_00000, 45)
	if len(e.childResults) == 0 || chk.NumRows() > 0 {
		trace_util_0.Count(_window_00000, 47)
		return
	}
	trace_util_0.Count(_window_00000, 46)
	childResult := e.childResults[0]
	e.childResults = e.childResults[1:]
	e.remainingRowsInChunk = childResult.NumRows()
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		trace_util_0.Count(_window_00000, 48)
		chk.MakeRefTo(i, childResult, col.Index)
	}
}

// windowProcessor is the interface for processing different kinds of windows.
type windowProcessor interface {
	// consumeGroupRows updates the result for an window function using the input rows
	// which belong to the same partition.
	consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error)
	// appendResult2Chunk appends the final results to chunk.
	// It is called when there are no more rows in current partition.
	appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error)
	// resetPartialResult resets the partial result to the original state for a specific window function.
	resetPartialResult()
}

type aggWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
}

func (p *aggWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 49)
	for i, windowFunc := range p.windowFuncs {
		trace_util_0.Count(_window_00000, 51)
		err := windowFunc.UpdatePartialResult(ctx, rows, p.partialResults[i])
		if err != nil {
			trace_util_0.Count(_window_00000, 52)
			return nil, err
		}
	}
	trace_util_0.Count(_window_00000, 50)
	rows = rows[:0]
	return rows, nil
}

func (p *aggWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 53)
	for remained > 0 {
		trace_util_0.Count(_window_00000, 55)
		for i, windowFunc := range p.windowFuncs {
			trace_util_0.Count(_window_00000, 57)
			// TODO: We can extend the agg func interface to avoid the `for` loop  here.
			err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				trace_util_0.Count(_window_00000, 58)
				return nil, err
			}
		}
		trace_util_0.Count(_window_00000, 56)
		remained--
	}
	trace_util_0.Count(_window_00000, 54)
	return rows, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	trace_util_0.Count(_window_00000, 59)
	for i, windowFunc := range p.windowFuncs {
		trace_util_0.Count(_window_00000, 60)
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}

type rowFrameWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	start          *core.FrameBound
	end            *core.FrameBound
	curRowIdx      uint64
}

func (p *rowFrameWindowProcessor) getStartOffset(numRows uint64) uint64 {
	trace_util_0.Count(_window_00000, 61)
	if p.start.UnBounded {
		trace_util_0.Count(_window_00000, 64)
		return 0
	}
	trace_util_0.Count(_window_00000, 62)
	switch p.start.Type {
	case ast.Preceding:
		trace_util_0.Count(_window_00000, 65)
		if p.curRowIdx >= p.start.Num {
			trace_util_0.Count(_window_00000, 70)
			return p.curRowIdx - p.start.Num
		}
		trace_util_0.Count(_window_00000, 66)
		return 0
	case ast.Following:
		trace_util_0.Count(_window_00000, 67)
		offset := p.curRowIdx + p.start.Num
		if offset >= numRows {
			trace_util_0.Count(_window_00000, 71)
			return numRows
		}
		trace_util_0.Count(_window_00000, 68)
		return offset
	case ast.CurrentRow:
		trace_util_0.Count(_window_00000, 69)
		return p.curRowIdx
	}
	// It will never reach here.
	trace_util_0.Count(_window_00000, 63)
	return 0
}

func (p *rowFrameWindowProcessor) getEndOffset(numRows uint64) uint64 {
	trace_util_0.Count(_window_00000, 72)
	if p.end.UnBounded {
		trace_util_0.Count(_window_00000, 75)
		return numRows
	}
	trace_util_0.Count(_window_00000, 73)
	switch p.end.Type {
	case ast.Preceding:
		trace_util_0.Count(_window_00000, 76)
		if p.curRowIdx >= p.end.Num {
			trace_util_0.Count(_window_00000, 81)
			return p.curRowIdx - p.end.Num + 1
		}
		trace_util_0.Count(_window_00000, 77)
		return 0
	case ast.Following:
		trace_util_0.Count(_window_00000, 78)
		offset := p.curRowIdx + p.end.Num
		if offset >= numRows {
			trace_util_0.Count(_window_00000, 82)
			return numRows
		}
		trace_util_0.Count(_window_00000, 79)
		return offset + 1
	case ast.CurrentRow:
		trace_util_0.Count(_window_00000, 80)
		return p.curRowIdx + 1
	}
	// It will never reach here.
	trace_util_0.Count(_window_00000, 74)
	return 0
}

func (p *rowFrameWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 83)
	return rows, nil
}

// TODO: We can optimize it using sliding window algorithm.
func (p *rowFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 84)
	numRows := uint64(len(rows))
	for remained > 0 {
		trace_util_0.Count(_window_00000, 86)
		start := p.getStartOffset(numRows)
		end := p.getEndOffset(numRows)
		p.curRowIdx++
		remained--
		if start >= end {
			trace_util_0.Count(_window_00000, 88)
			for i, windowFunc := range p.windowFuncs {
				trace_util_0.Count(_window_00000, 90)
				err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					trace_util_0.Count(_window_00000, 91)
					return nil, err
				}
			}
			trace_util_0.Count(_window_00000, 89)
			continue
		}
		trace_util_0.Count(_window_00000, 87)
		for i, windowFunc := range p.windowFuncs {
			trace_util_0.Count(_window_00000, 92)
			err := windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			if err != nil {
				trace_util_0.Count(_window_00000, 95)
				return nil, err
			}
			trace_util_0.Count(_window_00000, 93)
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				trace_util_0.Count(_window_00000, 96)
				return nil, err
			}
			trace_util_0.Count(_window_00000, 94)
			windowFunc.ResetPartialResult(p.partialResults[i])
		}
	}
	trace_util_0.Count(_window_00000, 85)
	return rows, nil
}

func (p *rowFrameWindowProcessor) resetPartialResult() {
	trace_util_0.Count(_window_00000, 97)
	p.curRowIdx = 0
}

type rangeFrameWindowProcessor struct {
	windowFuncs     []aggfuncs.AggFunc
	partialResults  []aggfuncs.PartialResult
	start           *core.FrameBound
	end             *core.FrameBound
	curRowIdx       uint64
	lastStartOffset uint64
	lastEndOffset   uint64
	orderByCols     []*expression.Column
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64
}

func (p *rangeFrameWindowProcessor) getStartOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	trace_util_0.Count(_window_00000, 98)
	if p.start.UnBounded {
		trace_util_0.Count(_window_00000, 101)
		return 0, nil
	}
	trace_util_0.Count(_window_00000, 99)
	numRows := uint64(len(rows))
	for ; p.lastStartOffset < numRows; p.lastStartOffset++ {
		trace_util_0.Count(_window_00000, 102)
		var res int64
		var err error
		for i := range p.orderByCols {
			trace_util_0.Count(_window_00000, 104)
			res, _, err = p.start.CmpFuncs[i](ctx, p.orderByCols[i], p.start.CalcFuncs[i], rows[p.lastStartOffset], rows[p.curRowIdx])
			if err != nil {
				trace_util_0.Count(_window_00000, 106)
				return 0, err
			}
			trace_util_0.Count(_window_00000, 105)
			if res != 0 {
				trace_util_0.Count(_window_00000, 107)
				break
			}
		}
		// For asc, break when the current value is greater or equal to the calculated result;
		// For desc, break when the current value is less or equal to the calculated result.
		trace_util_0.Count(_window_00000, 103)
		if res != p.expectedCmpResult {
			trace_util_0.Count(_window_00000, 108)
			break
		}
	}
	trace_util_0.Count(_window_00000, 100)
	return p.lastStartOffset, nil
}

func (p *rangeFrameWindowProcessor) getEndOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	trace_util_0.Count(_window_00000, 109)
	numRows := uint64(len(rows))
	if p.end.UnBounded {
		trace_util_0.Count(_window_00000, 112)
		return numRows, nil
	}
	trace_util_0.Count(_window_00000, 110)
	for ; p.lastEndOffset < numRows; p.lastEndOffset++ {
		trace_util_0.Count(_window_00000, 113)
		var res int64
		var err error
		for i := range p.orderByCols {
			trace_util_0.Count(_window_00000, 115)
			res, _, err = p.end.CmpFuncs[i](ctx, p.end.CalcFuncs[i], p.orderByCols[i], rows[p.curRowIdx], rows[p.lastEndOffset])
			if err != nil {
				trace_util_0.Count(_window_00000, 117)
				return 0, err
			}
			trace_util_0.Count(_window_00000, 116)
			if res != 0 {
				trace_util_0.Count(_window_00000, 118)
				break
			}
		}
		// For asc, break when the calculated result is greater than the current value.
		// For desc, break when the calculated result is less than the current value.
		trace_util_0.Count(_window_00000, 114)
		if res == p.expectedCmpResult {
			trace_util_0.Count(_window_00000, 119)
			break
		}
	}
	trace_util_0.Count(_window_00000, 111)
	return p.lastEndOffset, nil
}

func (p *rangeFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 120)
	for remained > 0 {
		trace_util_0.Count(_window_00000, 122)
		start, err := p.getStartOffset(ctx, rows)
		if err != nil {
			trace_util_0.Count(_window_00000, 126)
			return nil, err
		}
		trace_util_0.Count(_window_00000, 123)
		end, err := p.getEndOffset(ctx, rows)
		if err != nil {
			trace_util_0.Count(_window_00000, 127)
			return nil, err
		}
		trace_util_0.Count(_window_00000, 124)
		p.curRowIdx++
		remained--
		if start >= end {
			trace_util_0.Count(_window_00000, 128)
			for i, windowFunc := range p.windowFuncs {
				trace_util_0.Count(_window_00000, 130)
				err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					trace_util_0.Count(_window_00000, 131)
					return nil, err
				}
			}
			trace_util_0.Count(_window_00000, 129)
			continue
		}
		trace_util_0.Count(_window_00000, 125)
		for i, windowFunc := range p.windowFuncs {
			trace_util_0.Count(_window_00000, 132)
			err := windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			if err != nil {
				trace_util_0.Count(_window_00000, 135)
				return nil, err
			}
			trace_util_0.Count(_window_00000, 133)
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				trace_util_0.Count(_window_00000, 136)
				return nil, err
			}
			trace_util_0.Count(_window_00000, 134)
			windowFunc.ResetPartialResult(p.partialResults[i])
		}
	}
	trace_util_0.Count(_window_00000, 121)
	return rows, nil
}

func (p *rangeFrameWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	trace_util_0.Count(_window_00000, 137)
	return rows, nil
}

func (p *rangeFrameWindowProcessor) resetPartialResult() {
	trace_util_0.Count(_window_00000, 138)
	p.curRowIdx = 0
	p.lastStartOffset = 0
	p.lastEndOffset = 0
}

var _window_00000 = "executor/window.go"
