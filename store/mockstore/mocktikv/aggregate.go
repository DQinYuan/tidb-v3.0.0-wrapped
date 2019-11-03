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

package mocktikv

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

var (
	_ executor = &hashAggExec{}
	_ executor = &streamAggExec{}
)

type hashAggExec struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxsMap        aggCtxsMapper
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	groups            map[string]struct{}
	groupKeys         [][]byte
	groupKeyRows      [][][]byte
	executed          bool
	currGroupIdx      int
	count             int64
	execDetail        *execDetail

	src executor
}

func (e *hashAggExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_aggregate_00000, 0)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_aggregate_00000, 2)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_aggregate_00000, 1)
	return append(suffix, e.execDetail)
}

func (e *hashAggExec) SetSrcExec(exec executor) {
	trace_util_0.Count(_aggregate_00000, 3)
	e.src = exec
}

func (e *hashAggExec) GetSrcExec() executor {
	trace_util_0.Count(_aggregate_00000, 4)
	return e.src
}

func (e *hashAggExec) ResetCounts() {
	trace_util_0.Count(_aggregate_00000, 5)
	e.src.ResetCounts()
}

func (e *hashAggExec) Counts() []int64 {
	trace_util_0.Count(_aggregate_00000, 6)
	return e.src.Counts()
}

func (e *hashAggExec) innerNext(ctx context.Context) (bool, error) {
	trace_util_0.Count(_aggregate_00000, 7)
	values, err := e.src.Next(ctx)
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 11)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_aggregate_00000, 8)
	if values == nil {
		trace_util_0.Count(_aggregate_00000, 12)
		return false, nil
	}
	trace_util_0.Count(_aggregate_00000, 9)
	err = e.aggregate(values)
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 13)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_aggregate_00000, 10)
	return true, nil
}

func (e *hashAggExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_aggregate_00000, 14)
	panic("don't not use coprocessor streaming API for hash aggregation!")
}

func (e *hashAggExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_aggregate_00000, 15)
	defer func(begin time.Time) {
		trace_util_0.Count(_aggregate_00000, 21)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_aggregate_00000, 16)
	e.count++
	if e.aggCtxsMap == nil {
		trace_util_0.Count(_aggregate_00000, 22)
		e.aggCtxsMap = make(aggCtxsMapper)
	}
	trace_util_0.Count(_aggregate_00000, 17)
	if !e.executed {
		trace_util_0.Count(_aggregate_00000, 23)
		for {
			trace_util_0.Count(_aggregate_00000, 25)
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 27)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_aggregate_00000, 26)
			if !hasMore {
				trace_util_0.Count(_aggregate_00000, 28)
				break
			}
		}
		trace_util_0.Count(_aggregate_00000, 24)
		e.executed = true
	}

	trace_util_0.Count(_aggregate_00000, 18)
	if e.currGroupIdx >= len(e.groups) {
		trace_util_0.Count(_aggregate_00000, 29)
		return nil, nil
	}
	trace_util_0.Count(_aggregate_00000, 19)
	gk := e.groupKeys[e.currGroupIdx]
	value = make([][]byte, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		trace_util_0.Count(_aggregate_00000, 30)
		partialResults := agg.GetPartialResult(aggCtxs[i])
		for _, result := range partialResults {
			trace_util_0.Count(_aggregate_00000, 31)
			data, err := codec.EncodeValue(e.evalCtx.sc, nil, result)
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 33)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_aggregate_00000, 32)
			value = append(value, data)
		}
	}
	trace_util_0.Count(_aggregate_00000, 20)
	value = append(value, e.groupKeyRows[e.currGroupIdx]...)
	e.currGroupIdx++

	return value, nil
}

func (e *hashAggExec) getGroupKey() ([]byte, [][]byte, error) {
	trace_util_0.Count(_aggregate_00000, 34)
	length := len(e.groupByExprs)
	if length == 0 {
		trace_util_0.Count(_aggregate_00000, 38)
		return nil, nil, nil
	}
	trace_util_0.Count(_aggregate_00000, 35)
	bufLen := 0
	row := make([][]byte, 0, length)
	for _, item := range e.groupByExprs {
		trace_util_0.Count(_aggregate_00000, 39)
		v, err := item.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 42)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 40)
		b, err := codec.EncodeValue(e.evalCtx.sc, nil, v)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 43)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 41)
		bufLen += len(b)
		row = append(row, b)
	}
	trace_util_0.Count(_aggregate_00000, 36)
	buf := make([]byte, 0, bufLen)
	for _, col := range row {
		trace_util_0.Count(_aggregate_00000, 44)
		buf = append(buf, col...)
	}
	trace_util_0.Count(_aggregate_00000, 37)
	return buf, row, nil
}

// aggregate updates aggregate functions with row.
func (e *hashAggExec) aggregate(value [][]byte) error {
	trace_util_0.Count(_aggregate_00000, 45)
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 50)
		return errors.Trace(err)
	}
	// Get group key.
	trace_util_0.Count(_aggregate_00000, 46)
	gk, gbyKeyRow, err := e.getGroupKey()
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 51)
		return errors.Trace(err)
	}
	trace_util_0.Count(_aggregate_00000, 47)
	if _, ok := e.groups[string(gk)]; !ok {
		trace_util_0.Count(_aggregate_00000, 52)
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
		e.groupKeyRows = append(e.groupKeyRows, gbyKeyRow)
	}
	// Update aggregate expressions.
	trace_util_0.Count(_aggregate_00000, 48)
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		trace_util_0.Count(_aggregate_00000, 53)
		err = agg.Update(aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 54)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_aggregate_00000, 49)
	return nil
}

func (e *hashAggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	trace_util_0.Count(_aggregate_00000, 55)
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		trace_util_0.Count(_aggregate_00000, 57)
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			trace_util_0.Count(_aggregate_00000, 59)
			aggCtxs = append(aggCtxs, agg.CreateContext(e.evalCtx.sc))
		}
		trace_util_0.Count(_aggregate_00000, 58)
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	trace_util_0.Count(_aggregate_00000, 56)
	return aggCtxs
}

type streamAggExec struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxs           []*aggregation.AggEvaluateContext
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	tmpGroupByRow     []types.Datum
	currGroupByRow    []types.Datum
	nextGroupByRow    []types.Datum
	currGroupByValues [][]byte
	executed          bool
	hasData           bool
	count             int64
	execDetail        *execDetail

	src executor
}

func (e *streamAggExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_aggregate_00000, 60)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_aggregate_00000, 62)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_aggregate_00000, 61)
	return append(suffix, e.execDetail)
}

func (e *streamAggExec) SetSrcExec(exec executor) {
	trace_util_0.Count(_aggregate_00000, 63)
	e.src = exec
}

func (e *streamAggExec) GetSrcExec() executor {
	trace_util_0.Count(_aggregate_00000, 64)
	return e.src
}

func (e *streamAggExec) ResetCounts() {
	trace_util_0.Count(_aggregate_00000, 65)
	e.src.ResetCounts()
}

func (e *streamAggExec) Counts() []int64 {
	trace_util_0.Count(_aggregate_00000, 66)
	return e.src.Counts()
}

func (e *streamAggExec) getPartialResult() ([][]byte, error) {
	trace_util_0.Count(_aggregate_00000, 67)
	value := make([][]byte, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	for i, agg := range e.aggExprs {
		trace_util_0.Count(_aggregate_00000, 70)
		partialResults := agg.GetPartialResult(e.aggCtxs[i])
		for _, result := range partialResults {
			trace_util_0.Count(_aggregate_00000, 72)
			data, err := codec.EncodeValue(e.evalCtx.sc, nil, result)
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 74)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_aggregate_00000, 73)
			value = append(value, data)
		}
		// Clear the aggregate context.
		trace_util_0.Count(_aggregate_00000, 71)
		e.aggCtxs[i] = agg.CreateContext(e.evalCtx.sc)
	}
	trace_util_0.Count(_aggregate_00000, 68)
	e.currGroupByValues = e.currGroupByValues[:0]
	for _, d := range e.currGroupByRow {
		trace_util_0.Count(_aggregate_00000, 75)
		buf, err := codec.EncodeValue(e.evalCtx.sc, nil, d)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 77)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 76)
		e.currGroupByValues = append(e.currGroupByValues, buf)
	}
	trace_util_0.Count(_aggregate_00000, 69)
	e.currGroupByRow = types.CloneRow(e.nextGroupByRow)
	return append(value, e.currGroupByValues...), nil
}

func (e *streamAggExec) meetNewGroup(row [][]byte) (bool, error) {
	trace_util_0.Count(_aggregate_00000, 78)
	if len(e.groupByExprs) == 0 {
		trace_util_0.Count(_aggregate_00000, 84)
		return false, nil
	}

	trace_util_0.Count(_aggregate_00000, 79)
	e.tmpGroupByRow = e.tmpGroupByRow[:0]
	matched, firstGroup := true, false
	if e.nextGroupByRow == nil {
		trace_util_0.Count(_aggregate_00000, 85)
		matched, firstGroup = false, true
	}
	trace_util_0.Count(_aggregate_00000, 80)
	for i, item := range e.groupByExprs {
		trace_util_0.Count(_aggregate_00000, 86)
		d, err := item.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 89)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 87)
		if matched {
			trace_util_0.Count(_aggregate_00000, 90)
			c, err := d.CompareDatum(e.evalCtx.sc, &e.nextGroupByRow[i])
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 92)
				return false, errors.Trace(err)
			}
			trace_util_0.Count(_aggregate_00000, 91)
			matched = c == 0
		}
		trace_util_0.Count(_aggregate_00000, 88)
		e.tmpGroupByRow = append(e.tmpGroupByRow, d)
	}
	trace_util_0.Count(_aggregate_00000, 81)
	if firstGroup {
		trace_util_0.Count(_aggregate_00000, 93)
		e.currGroupByRow = types.CloneRow(e.tmpGroupByRow)
	}
	trace_util_0.Count(_aggregate_00000, 82)
	if matched {
		trace_util_0.Count(_aggregate_00000, 94)
		return false, nil
	}
	trace_util_0.Count(_aggregate_00000, 83)
	e.nextGroupByRow = e.tmpGroupByRow
	return !firstGroup, nil
}

func (e *streamAggExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_aggregate_00000, 95)
	panic("don't not use coprocessor streaming API for stream aggregation!")
}

func (e *streamAggExec) Next(ctx context.Context) (retRow [][]byte, err error) {
	trace_util_0.Count(_aggregate_00000, 96)
	defer func(begin time.Time) {
		trace_util_0.Count(_aggregate_00000, 99)
		e.execDetail.update(begin, retRow)
	}(time.Now())
	trace_util_0.Count(_aggregate_00000, 97)
	e.count++
	if e.executed {
		trace_util_0.Count(_aggregate_00000, 100)
		return nil, nil
	}

	trace_util_0.Count(_aggregate_00000, 98)
	for {
		trace_util_0.Count(_aggregate_00000, 101)
		values, err := e.src.Next(ctx)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 108)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 102)
		if values == nil {
			trace_util_0.Count(_aggregate_00000, 109)
			e.executed = true
			if !e.hasData && len(e.groupByExprs) > 0 {
				trace_util_0.Count(_aggregate_00000, 111)
				return nil, nil
			}
			trace_util_0.Count(_aggregate_00000, 110)
			return e.getPartialResult()
		}

		trace_util_0.Count(_aggregate_00000, 103)
		e.hasData = true
		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, values, e.row)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 112)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 104)
		newGroup, err := e.meetNewGroup(values)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 113)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_aggregate_00000, 105)
		if newGroup {
			trace_util_0.Count(_aggregate_00000, 114)
			retRow, err = e.getPartialResult()
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 115)
				return nil, errors.Trace(err)
			}
		}
		trace_util_0.Count(_aggregate_00000, 106)
		for i, agg := range e.aggExprs {
			trace_util_0.Count(_aggregate_00000, 116)
			err = agg.Update(e.aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromDatums(e.row).ToRow())
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 117)
				return nil, errors.Trace(err)
			}
		}
		trace_util_0.Count(_aggregate_00000, 107)
		if newGroup {
			trace_util_0.Count(_aggregate_00000, 118)
			return retRow, nil
		}
	}
}

var _aggregate_00000 = "store/mockstore/mocktikv/aggregate.go"
