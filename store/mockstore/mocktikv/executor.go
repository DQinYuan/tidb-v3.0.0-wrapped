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
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ executor = &tableScanExec{}
	_ executor = &indexScanExec{}
	_ executor = &selectionExec{}
	_ executor = &limitExec{}
	_ executor = &topNExec{}
)

type execDetail struct {
	timeProcessed   time.Duration
	numProducedRows int
	numIterations   int
}

func (e *execDetail) update(begin time.Time, row [][]byte) {
	trace_util_0.Count(_executor_00000, 0)
	e.timeProcessed += time.Since(begin)
	e.numIterations++
	if row != nil {
		trace_util_0.Count(_executor_00000, 1)
		e.numProducedRows++
	}
}

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	ResetCounts()
	Counts() []int64
	Next(ctx context.Context) ([][]byte, error)
	// Cursor returns the key gonna to be scanned by the Next() function.
	Cursor() (key []byte, desc bool)
	// ExecDetails returns its and its children's execution details.
	// The order is same as DAGRequest.Executors, which children are in front of parents.
	ExecDetails() []*execDetail
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs         map[int64]int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	start          int
	counts         []int64
	execDetail     *execDetail

	src executor
}

func (e *tableScanExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_executor_00000, 2)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_executor_00000, 4)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_executor_00000, 3)
	return append(suffix, e.execDetail)
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	trace_util_0.Count(_executor_00000, 5)
	e.src = exec
}

func (e *tableScanExec) GetSrcExec() executor {
	trace_util_0.Count(_executor_00000, 6)
	return e.src
}

func (e *tableScanExec) ResetCounts() {
	trace_util_0.Count(_executor_00000, 7)
	if e.counts != nil {
		trace_util_0.Count(_executor_00000, 8)
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *tableScanExec) Counts() []int64 {
	trace_util_0.Count(_executor_00000, 9)
	if e.counts == nil {
		trace_util_0.Count(_executor_00000, 12)
		return nil
	}
	trace_util_0.Count(_executor_00000, 10)
	if e.seekKey == nil {
		trace_util_0.Count(_executor_00000, 13)
		return e.counts[e.start:e.cursor]
	}
	trace_util_0.Count(_executor_00000, 11)
	return e.counts[e.start : e.cursor+1]
}

func (e *tableScanExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_executor_00000, 14)
	if len(e.seekKey) > 0 {
		trace_util_0.Count(_executor_00000, 18)
		return e.seekKey, e.Desc
	}

	trace_util_0.Count(_executor_00000, 15)
	if e.cursor < len(e.kvRanges) {
		trace_util_0.Count(_executor_00000, 19)
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			trace_util_0.Count(_executor_00000, 22)
			return ran.StartKey, e.Desc
		}

		trace_util_0.Count(_executor_00000, 20)
		if e.Desc {
			trace_util_0.Count(_executor_00000, 23)
			return ran.EndKey, e.Desc
		}
		trace_util_0.Count(_executor_00000, 21)
		return ran.StartKey, e.Desc
	}

	trace_util_0.Count(_executor_00000, 16)
	if e.Desc {
		trace_util_0.Count(_executor_00000, 24)
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	trace_util_0.Count(_executor_00000, 17)
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *tableScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_executor_00000, 25)
	defer func(begin time.Time) {
		trace_util_0.Count(_executor_00000, 28)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_executor_00000, 26)
	for e.cursor < len(e.kvRanges) {
		trace_util_0.Count(_executor_00000, 29)
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			trace_util_0.Count(_executor_00000, 34)
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				trace_util_0.Count(_executor_00000, 38)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_executor_00000, 35)
			e.cursor++
			if value == nil {
				trace_util_0.Count(_executor_00000, 39)
				continue
			}
			trace_util_0.Count(_executor_00000, 36)
			if e.counts != nil {
				trace_util_0.Count(_executor_00000, 40)
				e.counts[e.cursor-1]++
			}
			trace_util_0.Count(_executor_00000, 37)
			return value, nil
		}
		trace_util_0.Count(_executor_00000, 30)
		value, err = e.getRowFromRange(ran)
		if err != nil {
			trace_util_0.Count(_executor_00000, 41)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 31)
		if value == nil {
			trace_util_0.Count(_executor_00000, 42)
			e.seekKey = nil
			e.cursor++
			continue
		}
		trace_util_0.Count(_executor_00000, 32)
		if e.counts != nil {
			trace_util_0.Count(_executor_00000, 43)
			e.counts[e.cursor]++
		}
		trace_util_0.Count(_executor_00000, 33)
		return value, nil
	}

	trace_util_0.Count(_executor_00000, 27)
	return nil, nil
}

func (e *tableScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 44)
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel)
	if err != nil {
		trace_util_0.Count(_executor_00000, 49)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 45)
	if len(val) == 0 {
		trace_util_0.Count(_executor_00000, 50)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 46)
	handle, err := tablecodec.DecodeRowKey(ran.StartKey)
	if err != nil {
		trace_util_0.Count(_executor_00000, 51)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 47)
	row, err := getRowData(e.Columns, e.colIDs, handle, val)
	if err != nil {
		trace_util_0.Count(_executor_00000, 52)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 48)
	return row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 53)
	if e.seekKey == nil {
		trace_util_0.Count(_executor_00000, 62)
		if e.Desc {
			trace_util_0.Count(_executor_00000, 63)
			e.seekKey = ran.EndKey
		} else {
			trace_util_0.Count(_executor_00000, 64)
			{
				e.seekKey = ran.StartKey
			}
		}
	}
	trace_util_0.Count(_executor_00000, 54)
	var pairs []Pair
	var pair Pair
	if e.Desc {
		trace_util_0.Count(_executor_00000, 65)
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel)
	} else {
		trace_util_0.Count(_executor_00000, 66)
		{
			pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel)
		}
	}
	trace_util_0.Count(_executor_00000, 55)
	if len(pairs) > 0 {
		trace_util_0.Count(_executor_00000, 67)
		pair = pairs[0]
	}
	trace_util_0.Count(_executor_00000, 56)
	if pair.Err != nil {
		trace_util_0.Count(_executor_00000, 68)
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	trace_util_0.Count(_executor_00000, 57)
	if pair.Key == nil {
		trace_util_0.Count(_executor_00000, 69)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 58)
	if e.Desc {
		trace_util_0.Count(_executor_00000, 70)
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			trace_util_0.Count(_executor_00000, 72)
			return nil, nil
		}
		trace_util_0.Count(_executor_00000, 71)
		e.seekKey = []byte(tablecodec.TruncateToRowKeyLen(kv.Key(pair.Key)))
	} else {
		trace_util_0.Count(_executor_00000, 73)
		{
			if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
				trace_util_0.Count(_executor_00000, 75)
				return nil, nil
			}
			trace_util_0.Count(_executor_00000, 74)
			e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
		}
	}

	trace_util_0.Count(_executor_00000, 59)
	handle, err := tablecodec.DecodeRowKey(pair.Key)
	if err != nil {
		trace_util_0.Count(_executor_00000, 76)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 60)
	row, err := getRowData(e.Columns, e.colIDs, handle, pair.Value)
	if err != nil {
		trace_util_0.Count(_executor_00000, 77)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 61)
	return row, nil
}

const (
	pkColNotExists = iota
	pkColIsSigned
	pkColIsUnsigned
)

type indexScanExec struct {
	*tipb.IndexScan
	colsLen        int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	pkStatus       int
	start          int
	counts         []int64
	execDetail     *execDetail

	src executor
}

func (e *indexScanExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_executor_00000, 78)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_executor_00000, 80)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_executor_00000, 79)
	return append(suffix, e.execDetail)
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	trace_util_0.Count(_executor_00000, 81)
	e.src = exec
}

func (e *indexScanExec) GetSrcExec() executor {
	trace_util_0.Count(_executor_00000, 82)
	return e.src
}

func (e *indexScanExec) ResetCounts() {
	trace_util_0.Count(_executor_00000, 83)
	if e.counts != nil {
		trace_util_0.Count(_executor_00000, 84)
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *indexScanExec) Counts() []int64 {
	trace_util_0.Count(_executor_00000, 85)
	if e.counts == nil {
		trace_util_0.Count(_executor_00000, 88)
		return nil
	}
	trace_util_0.Count(_executor_00000, 86)
	if e.seekKey == nil {
		trace_util_0.Count(_executor_00000, 89)
		return e.counts[e.start:e.cursor]
	}
	trace_util_0.Count(_executor_00000, 87)
	return e.counts[e.start : e.cursor+1]
}

func (e *indexScanExec) isUnique() bool {
	trace_util_0.Count(_executor_00000, 90)
	return e.Unique != nil && *e.Unique
}

func (e *indexScanExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_executor_00000, 91)
	if len(e.seekKey) > 0 {
		trace_util_0.Count(_executor_00000, 95)
		return e.seekKey, e.Desc
	}
	trace_util_0.Count(_executor_00000, 92)
	if e.cursor < len(e.kvRanges) {
		trace_util_0.Count(_executor_00000, 96)
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			trace_util_0.Count(_executor_00000, 99)
			return ran.StartKey, e.Desc
		}
		trace_util_0.Count(_executor_00000, 97)
		if e.Desc {
			trace_util_0.Count(_executor_00000, 100)
			return ran.EndKey, e.Desc
		}
		trace_util_0.Count(_executor_00000, 98)
		return ran.StartKey, e.Desc
	}
	trace_util_0.Count(_executor_00000, 93)
	if e.Desc {
		trace_util_0.Count(_executor_00000, 101)
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	trace_util_0.Count(_executor_00000, 94)
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *indexScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_executor_00000, 102)
	defer func(begin time.Time) {
		trace_util_0.Count(_executor_00000, 105)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_executor_00000, 103)
	for e.cursor < len(e.kvRanges) {
		trace_util_0.Count(_executor_00000, 106)
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			trace_util_0.Count(_executor_00000, 108)
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				trace_util_0.Count(_executor_00000, 111)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_executor_00000, 109)
			e.cursor++
			if value == nil {
				trace_util_0.Count(_executor_00000, 112)
				continue
			}
			trace_util_0.Count(_executor_00000, 110)
			if e.counts != nil {
				trace_util_0.Count(_executor_00000, 113)
				e.counts[e.cursor-1]++
			}
		} else {
			trace_util_0.Count(_executor_00000, 114)
			{
				value, err = e.getRowFromRange(ran)
				if err != nil {
					trace_util_0.Count(_executor_00000, 117)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_executor_00000, 115)
				if value == nil {
					trace_util_0.Count(_executor_00000, 118)
					e.cursor++
					e.seekKey = nil
					continue
				}
				trace_util_0.Count(_executor_00000, 116)
				if e.counts != nil {
					trace_util_0.Count(_executor_00000, 119)
					e.counts[e.cursor]++
				}
			}
		}
		trace_util_0.Count(_executor_00000, 107)
		return value, nil
	}

	trace_util_0.Count(_executor_00000, 104)
	return nil, nil
}

// getRowFromPoint is only used for unique key.
func (e *indexScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 120)
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel)
	if err != nil {
		trace_util_0.Count(_executor_00000, 123)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 121)
	if len(val) == 0 {
		trace_util_0.Count(_executor_00000, 124)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 122)
	return e.decodeIndexKV(Pair{Key: ran.StartKey, Value: val})
}

func (e *indexScanExec) decodeIndexKV(pair Pair) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 125)
	values, b, err := tablecodec.CutIndexKeyNew(pair.Key, e.colsLen)
	if err != nil {
		trace_util_0.Count(_executor_00000, 128)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 126)
	if len(b) > 0 {
		trace_util_0.Count(_executor_00000, 129)
		if e.pkStatus != pkColNotExists {
			trace_util_0.Count(_executor_00000, 130)
			values = append(values, b)
		}
	} else {
		trace_util_0.Count(_executor_00000, 131)
		if e.pkStatus != pkColNotExists {
			trace_util_0.Count(_executor_00000, 132)
			handle, err := decodeHandle(pair.Value)
			if err != nil {
				trace_util_0.Count(_executor_00000, 136)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_executor_00000, 133)
			var handleDatum types.Datum
			if e.pkStatus == pkColIsUnsigned {
				trace_util_0.Count(_executor_00000, 137)
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				trace_util_0.Count(_executor_00000, 138)
				{
					handleDatum = types.NewIntDatum(handle)
				}
			}
			trace_util_0.Count(_executor_00000, 134)
			handleBytes, err := codec.EncodeValue(nil, b, handleDatum)
			if err != nil {
				trace_util_0.Count(_executor_00000, 139)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_executor_00000, 135)
			values = append(values, handleBytes)
		}
	}

	trace_util_0.Count(_executor_00000, 127)
	return values, nil
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 140)
	if e.seekKey == nil {
		trace_util_0.Count(_executor_00000, 147)
		if e.Desc {
			trace_util_0.Count(_executor_00000, 148)
			e.seekKey = ran.EndKey
		} else {
			trace_util_0.Count(_executor_00000, 149)
			{
				e.seekKey = ran.StartKey
			}
		}
	}
	trace_util_0.Count(_executor_00000, 141)
	var pairs []Pair
	var pair Pair
	if e.Desc {
		trace_util_0.Count(_executor_00000, 150)
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel)
	} else {
		trace_util_0.Count(_executor_00000, 151)
		{
			pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel)
		}
	}
	trace_util_0.Count(_executor_00000, 142)
	if len(pairs) > 0 {
		trace_util_0.Count(_executor_00000, 152)
		pair = pairs[0]
	}
	trace_util_0.Count(_executor_00000, 143)
	if pair.Err != nil {
		trace_util_0.Count(_executor_00000, 153)
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	trace_util_0.Count(_executor_00000, 144)
	if pair.Key == nil {
		trace_util_0.Count(_executor_00000, 154)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 145)
	if e.Desc {
		trace_util_0.Count(_executor_00000, 155)
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			trace_util_0.Count(_executor_00000, 157)
			return nil, nil
		}
		trace_util_0.Count(_executor_00000, 156)
		e.seekKey = pair.Key
	} else {
		trace_util_0.Count(_executor_00000, 158)
		{
			if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
				trace_util_0.Count(_executor_00000, 160)
				return nil, nil
			}
			trace_util_0.Count(_executor_00000, 159)
			e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
		}
	}

	trace_util_0.Count(_executor_00000, 146)
	return e.decodeIndexKV(pair)
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	evalCtx           *evalContext
	src               executor
	execDetail        *execDetail
}

func (e *selectionExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_executor_00000, 161)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_executor_00000, 163)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_executor_00000, 162)
	return append(suffix, e.execDetail)
}

func (e *selectionExec) SetSrcExec(exec executor) {
	trace_util_0.Count(_executor_00000, 164)
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	trace_util_0.Count(_executor_00000, 165)
	return e.src
}

func (e *selectionExec) ResetCounts() {
	trace_util_0.Count(_executor_00000, 166)
	e.src.ResetCounts()
}

func (e *selectionExec) Counts() []int64 {
	trace_util_0.Count(_executor_00000, 167)
	return e.src.Counts()
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, row []types.Datum, ctx *stmtctx.StatementContext) (bool, error) {
	trace_util_0.Count(_executor_00000, 168)
	for _, expr := range exprs {
		trace_util_0.Count(_executor_00000, 170)
		data, err := expr.Eval(chunk.MutRowFromDatums(row).ToRow())
		if err != nil {
			trace_util_0.Count(_executor_00000, 174)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 171)
		if data.IsNull() {
			trace_util_0.Count(_executor_00000, 175)
			return false, nil
		}

		trace_util_0.Count(_executor_00000, 172)
		isBool, err := data.ToBool(ctx)
		if err != nil {
			trace_util_0.Count(_executor_00000, 176)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 173)
		if isBool == 0 {
			trace_util_0.Count(_executor_00000, 177)
			return false, nil
		}
	}
	trace_util_0.Count(_executor_00000, 169)
	return true, nil
}

func (e *selectionExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_executor_00000, 178)
	return e.src.Cursor()
}

func (e *selectionExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_executor_00000, 179)
	defer func(begin time.Time) {
		trace_util_0.Count(_executor_00000, 181)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_executor_00000, 180)
	for {
		trace_util_0.Count(_executor_00000, 182)
		value, err = e.src.Next(ctx)
		if err != nil {
			trace_util_0.Count(_executor_00000, 187)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 183)
		if value == nil {
			trace_util_0.Count(_executor_00000, 188)
			return nil, nil
		}

		trace_util_0.Count(_executor_00000, 184)
		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
		if err != nil {
			trace_util_0.Count(_executor_00000, 189)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 185)
		match, err := evalBool(e.conditions, e.row, e.evalCtx.sc)
		if err != nil {
			trace_util_0.Count(_executor_00000, 190)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 186)
		if match {
			trace_util_0.Count(_executor_00000, 191)
			return value, nil
		}
	}
}

type topNExec struct {
	heap              *topNHeap
	evalCtx           *evalContext
	relatedColOffsets []int
	orderByExprs      []expression.Expression
	row               []types.Datum
	cursor            int
	executed          bool
	execDetail        *execDetail

	src executor
}

func (e *topNExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_executor_00000, 192)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_executor_00000, 194)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_executor_00000, 193)
	return append(suffix, e.execDetail)
}

func (e *topNExec) SetSrcExec(src executor) {
	trace_util_0.Count(_executor_00000, 195)
	e.src = src
}

func (e *topNExec) GetSrcExec() executor {
	trace_util_0.Count(_executor_00000, 196)
	return e.src
}

func (e *topNExec) ResetCounts() {
	trace_util_0.Count(_executor_00000, 197)
	e.src.ResetCounts()
}

func (e *topNExec) Counts() []int64 {
	trace_util_0.Count(_executor_00000, 198)
	return e.src.Counts()
}

func (e *topNExec) innerNext(ctx context.Context) (bool, error) {
	trace_util_0.Count(_executor_00000, 199)
	value, err := e.src.Next(ctx)
	if err != nil {
		trace_util_0.Count(_executor_00000, 203)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 200)
	if value == nil {
		trace_util_0.Count(_executor_00000, 204)
		return false, nil
	}
	trace_util_0.Count(_executor_00000, 201)
	err = e.evalTopN(value)
	if err != nil {
		trace_util_0.Count(_executor_00000, 205)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 202)
	return true, nil
}

func (e *topNExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_executor_00000, 206)
	panic("don't not use coprocessor streaming API for topN!")
}

func (e *topNExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_executor_00000, 207)
	defer func(begin time.Time) {
		trace_util_0.Count(_executor_00000, 211)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_executor_00000, 208)
	if !e.executed {
		trace_util_0.Count(_executor_00000, 212)
		for {
			trace_util_0.Count(_executor_00000, 214)
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				trace_util_0.Count(_executor_00000, 216)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_executor_00000, 215)
			if !hasMore {
				trace_util_0.Count(_executor_00000, 217)
				break
			}
		}
		trace_util_0.Count(_executor_00000, 213)
		e.executed = true
	}
	trace_util_0.Count(_executor_00000, 209)
	if e.cursor >= len(e.heap.rows) {
		trace_util_0.Count(_executor_00000, 218)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 210)
	sort.Sort(&e.heap.topNSorter)
	row := e.heap.rows[e.cursor]
	e.cursor++

	return row.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(value [][]byte) error {
	trace_util_0.Count(_executor_00000, 219)
	newRow := &sortRow{
		key: make([]types.Datum, len(value)),
	}
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		trace_util_0.Count(_executor_00000, 223)
		return errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 220)
	for i, expr := range e.orderByExprs {
		trace_util_0.Count(_executor_00000, 224)
		newRow.key[i], err = expr.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			trace_util_0.Count(_executor_00000, 225)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_executor_00000, 221)
	if e.heap.tryToAddRow(newRow) {
		trace_util_0.Count(_executor_00000, 226)
		newRow.data = append(newRow.data, value...)
	}
	trace_util_0.Count(_executor_00000, 222)
	return errors.Trace(e.heap.err)
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor

	execDetail *execDetail
}

func (e *limitExec) ExecDetails() []*execDetail {
	trace_util_0.Count(_executor_00000, 227)
	var suffix []*execDetail
	if e.src != nil {
		trace_util_0.Count(_executor_00000, 229)
		suffix = e.src.ExecDetails()
	}
	trace_util_0.Count(_executor_00000, 228)
	return append(suffix, e.execDetail)
}

func (e *limitExec) SetSrcExec(src executor) {
	trace_util_0.Count(_executor_00000, 230)
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	trace_util_0.Count(_executor_00000, 231)
	return e.src
}

func (e *limitExec) ResetCounts() {
	trace_util_0.Count(_executor_00000, 232)
	e.src.ResetCounts()
}

func (e *limitExec) Counts() []int64 {
	trace_util_0.Count(_executor_00000, 233)
	return e.src.Counts()
}

func (e *limitExec) Cursor() ([]byte, bool) {
	trace_util_0.Count(_executor_00000, 234)
	return e.src.Cursor()
}

func (e *limitExec) Next(ctx context.Context) (value [][]byte, err error) {
	trace_util_0.Count(_executor_00000, 235)
	defer func(begin time.Time) {
		trace_util_0.Count(_executor_00000, 240)
		e.execDetail.update(begin, value)
	}(time.Now())
	trace_util_0.Count(_executor_00000, 236)
	if e.cursor >= e.limit {
		trace_util_0.Count(_executor_00000, 241)
		return nil, nil
	}

	trace_util_0.Count(_executor_00000, 237)
	value, err = e.src.Next(ctx)
	if err != nil {
		trace_util_0.Count(_executor_00000, 242)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 238)
	if value == nil {
		trace_util_0.Count(_executor_00000, 243)
		return nil, nil
	}
	trace_util_0.Count(_executor_00000, 239)
	e.cursor++
	return value, nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	trace_util_0.Count(_executor_00000, 244)
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		trace_util_0.Count(_executor_00000, 246)
		return true
	}
	trace_util_0.Count(_executor_00000, 245)
	return false
}

// getRowData decodes raw byte slice to row data.
func getRowData(columns []*tipb.ColumnInfo, colIDs map[int64]int, handle int64, value []byte) ([][]byte, error) {
	trace_util_0.Count(_executor_00000, 247)
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		trace_util_0.Count(_executor_00000, 251)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_executor_00000, 248)
	if values == nil {
		trace_util_0.Count(_executor_00000, 252)
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	trace_util_0.Count(_executor_00000, 249)
	for _, col := range columns {
		trace_util_0.Count(_executor_00000, 253)
		id := col.GetColumnId()
		offset := colIDs[id]
		if col.GetPkHandle() || id == model.ExtraHandleID {
			trace_util_0.Count(_executor_00000, 258)
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				trace_util_0.Count(_executor_00000, 261)
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				trace_util_0.Count(_executor_00000, 262)
				{
					handleDatum = types.NewIntDatum(handle)
				}
			}
			trace_util_0.Count(_executor_00000, 259)
			handleData, err1 := codec.EncodeValue(nil, nil, handleDatum)
			if err1 != nil {
				trace_util_0.Count(_executor_00000, 263)
				return nil, errors.Trace(err1)
			}
			trace_util_0.Count(_executor_00000, 260)
			values[offset] = handleData
			continue
		}
		trace_util_0.Count(_executor_00000, 254)
		if hasColVal(values, colIDs, id) {
			trace_util_0.Count(_executor_00000, 264)
			continue
		}
		trace_util_0.Count(_executor_00000, 255)
		if len(col.DefaultVal) > 0 {
			trace_util_0.Count(_executor_00000, 265)
			values[offset] = col.DefaultVal
			continue
		}
		trace_util_0.Count(_executor_00000, 256)
		if mysql.HasNotNullFlag(uint(col.GetFlag())) {
			trace_util_0.Count(_executor_00000, 266)
			return nil, errors.Errorf("Miss column %d", id)
		}

		trace_util_0.Count(_executor_00000, 257)
		values[offset] = []byte{codec.NilFlag}
	}

	trace_util_0.Count(_executor_00000, 250)
	return values, nil
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	trace_util_0.Count(_executor_00000, 267)
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		trace_util_0.Count(_executor_00000, 269)
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			trace_util_0.Count(_executor_00000, 271)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_executor_00000, 270)
		exprs = append(exprs, e)
	}
	trace_util_0.Count(_executor_00000, 268)
	return exprs, nil
}

func decodeHandle(data []byte) (int64, error) {
	trace_util_0.Count(_executor_00000, 272)
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

var _executor_00000 = "store/mockstore/mocktikv/executor.go"
