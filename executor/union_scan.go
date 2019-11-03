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
	"sort"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// DirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type DirtyDB struct {
	// tables is a map whose key is tableID.
	tables map[int64]*DirtyTable
}

// GetDirtyTable gets the DirtyTable by id from the DirtyDB.
func (udb *DirtyDB) GetDirtyTable(tid int64) *DirtyTable {
	trace_util_0.Count(_union_scan_00000, 0)
	dt, ok := udb.tables[tid]
	if !ok {
		trace_util_0.Count(_union_scan_00000, 2)
		dt = &DirtyTable{
			tid:         tid,
			addedRows:   make(map[int64]struct{}),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	trace_util_0.Count(_union_scan_00000, 1)
	return dt
}

// DirtyTable stores uncommitted write operation for a transaction.
type DirtyTable struct {
	tid int64
	// addedRows ...
	// the key is handle.
	addedRows   map[int64]struct{}
	deletedRows map[int64]struct{}
	truncated   bool
}

// AddRow adds a row to the DirtyDB.
func (dt *DirtyTable) AddRow(handle int64, row []types.Datum) {
	trace_util_0.Count(_union_scan_00000, 3)
	dt.addedRows[handle] = struct{}{}
}

// DeleteRow deletes a row from the DirtyDB.
func (dt *DirtyTable) DeleteRow(handle int64) {
	trace_util_0.Count(_union_scan_00000, 4)
	delete(dt.addedRows, handle)
	dt.deletedRows[handle] = struct{}{}
}

// TruncateTable truncates a table.
func (dt *DirtyTable) TruncateTable() {
	trace_util_0.Count(_union_scan_00000, 5)
	dt.addedRows = make(map[int64]struct{})
	dt.truncated = true
}

// GetDirtyDB returns the DirtyDB bind to the context.
func GetDirtyDB(ctx sessionctx.Context) *DirtyDB {
	trace_util_0.Count(_union_scan_00000, 6)
	var udb *DirtyDB
	x := ctx.GetSessionVars().TxnCtx.DirtyDB
	if x == nil {
		trace_util_0.Count(_union_scan_00000, 8)
		udb = &DirtyDB{tables: make(map[int64]*DirtyTable)}
		ctx.GetSessionVars().TxnCtx.DirtyDB = udb
	} else {
		trace_util_0.Count(_union_scan_00000, 9)
		{
			udb = x.(*DirtyDB)
		}
	}
	trace_util_0.Count(_union_scan_00000, 7)
	return udb
}

// UnionScanExec merges the rows from dirty table and the rows from distsql request.
type UnionScanExec struct {
	baseExecutor

	dirty *DirtyTable
	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex  []int
	desc       bool
	conditions []expression.Expression
	columns    []*model.ColumnInfo

	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int

	addedRows           [][]types.Datum
	cursor4AddRows      int
	sortErr             error
	snapshotRows        [][]types.Datum
	cursor4SnapshotRows int
	snapshotChunkBuffer *chunk.Chunk
}

// Open implements the Executor Open interface.
func (us *UnionScanExec) Open(ctx context.Context) error {
	trace_util_0.Count(_union_scan_00000, 10)
	if err := us.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_union_scan_00000, 12)
		return err
	}
	trace_util_0.Count(_union_scan_00000, 11)
	us.snapshotChunkBuffer = newFirstChunk(us)
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_union_scan_00000, 13)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_union_scan_00000, 17)
		span1 := span.Tracer().StartSpan("unionScan.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_union_scan_00000, 14)
	if us.runtimeStats != nil {
		trace_util_0.Count(_union_scan_00000, 18)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_union_scan_00000, 19)
			us.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_union_scan_00000, 15)
	req.GrowAndReset(us.maxChunkSize)
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	for i, batchSize := 0, req.Capacity(); i < batchSize; i++ {
		trace_util_0.Count(_union_scan_00000, 20)
		row, err := us.getOneRow(ctx)
		if err != nil {
			trace_util_0.Count(_union_scan_00000, 23)
			return err
		}
		// no more data.
		trace_util_0.Count(_union_scan_00000, 21)
		if row == nil {
			trace_util_0.Count(_union_scan_00000, 24)
			return nil
		}
		trace_util_0.Count(_union_scan_00000, 22)
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	trace_util_0.Count(_union_scan_00000, 16)
	return nil
}

// getOneRow gets one result row from dirty table or child.
func (us *UnionScanExec) getOneRow(ctx context.Context) ([]types.Datum, error) {
	trace_util_0.Count(_union_scan_00000, 25)
	for {
		trace_util_0.Count(_union_scan_00000, 26)
		snapshotRow, err := us.getSnapshotRow(ctx)
		if err != nil {
			trace_util_0.Count(_union_scan_00000, 31)
			return nil, err
		}
		trace_util_0.Count(_union_scan_00000, 27)
		addedRow := us.getAddedRow()
		var row []types.Datum
		var isSnapshotRow bool
		if addedRow == nil {
			trace_util_0.Count(_union_scan_00000, 32)
			row = snapshotRow
			isSnapshotRow = true
		} else {
			trace_util_0.Count(_union_scan_00000, 33)
			if snapshotRow == nil {
				trace_util_0.Count(_union_scan_00000, 34)
				row = addedRow
			} else {
				trace_util_0.Count(_union_scan_00000, 35)
				{
					isSnapshotRow, err = us.shouldPickFirstRow(snapshotRow, addedRow)
					if err != nil {
						trace_util_0.Count(_union_scan_00000, 37)
						return nil, err
					}
					trace_util_0.Count(_union_scan_00000, 36)
					if isSnapshotRow {
						trace_util_0.Count(_union_scan_00000, 38)
						row = snapshotRow
					} else {
						trace_util_0.Count(_union_scan_00000, 39)
						{
							row = addedRow
						}
					}
				}
			}
		}
		trace_util_0.Count(_union_scan_00000, 28)
		if row == nil {
			trace_util_0.Count(_union_scan_00000, 40)
			return nil, nil
		}

		trace_util_0.Count(_union_scan_00000, 29)
		if isSnapshotRow {
			trace_util_0.Count(_union_scan_00000, 41)
			us.cursor4SnapshotRows++
		} else {
			trace_util_0.Count(_union_scan_00000, 42)
			{
				us.cursor4AddRows++
			}
		}
		trace_util_0.Count(_union_scan_00000, 30)
		return row, nil
	}
}

func (us *UnionScanExec) getSnapshotRow(ctx context.Context) ([]types.Datum, error) {
	trace_util_0.Count(_union_scan_00000, 43)
	if us.dirty.truncated {
		trace_util_0.Count(_union_scan_00000, 47)
		return nil, nil
	}
	trace_util_0.Count(_union_scan_00000, 44)
	if us.cursor4SnapshotRows < len(us.snapshotRows) {
		trace_util_0.Count(_union_scan_00000, 48)
		return us.snapshotRows[us.cursor4SnapshotRows], nil
	}
	trace_util_0.Count(_union_scan_00000, 45)
	var err error
	us.cursor4SnapshotRows = 0
	us.snapshotRows = us.snapshotRows[:0]
	for len(us.snapshotRows) == 0 {
		trace_util_0.Count(_union_scan_00000, 49)
		err = Next(ctx, us.children[0], chunk.NewRecordBatch(us.snapshotChunkBuffer))
		if err != nil || us.snapshotChunkBuffer.NumRows() == 0 {
			trace_util_0.Count(_union_scan_00000, 51)
			return nil, err
		}
		trace_util_0.Count(_union_scan_00000, 50)
		iter := chunk.NewIterator4Chunk(us.snapshotChunkBuffer)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_union_scan_00000, 52)
			snapshotHandle := row.GetInt64(us.belowHandleIndex)
			if _, ok := us.dirty.deletedRows[snapshotHandle]; ok {
				trace_util_0.Count(_union_scan_00000, 55)
				continue
			}
			trace_util_0.Count(_union_scan_00000, 53)
			if _, ok := us.dirty.addedRows[snapshotHandle]; ok {
				trace_util_0.Count(_union_scan_00000, 56)
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			trace_util_0.Count(_union_scan_00000, 54)
			us.snapshotRows = append(us.snapshotRows, row.GetDatumRow(retTypes(us.children[0])))
		}
	}
	trace_util_0.Count(_union_scan_00000, 46)
	return us.snapshotRows[0], nil
}

func (us *UnionScanExec) getAddedRow() []types.Datum {
	trace_util_0.Count(_union_scan_00000, 57)
	var addedRow []types.Datum
	if us.cursor4AddRows < len(us.addedRows) {
		trace_util_0.Count(_union_scan_00000, 59)
		addedRow = us.addedRows[us.cursor4AddRows]
	}
	trace_util_0.Count(_union_scan_00000, 58)
	return addedRow
}

// shouldPickFirstRow picks the suitable row in order.
// The value returned is used to determine whether to pick the first input row.
func (us *UnionScanExec) shouldPickFirstRow(a, b []types.Datum) (bool, error) {
	trace_util_0.Count(_union_scan_00000, 60)
	var isFirstRow bool
	addedCmpSrc, err := us.compare(a, b)
	if err != nil {
		trace_util_0.Count(_union_scan_00000, 63)
		return isFirstRow, err
	}
	// Compare result will never be 0.
	trace_util_0.Count(_union_scan_00000, 61)
	if us.desc {
		trace_util_0.Count(_union_scan_00000, 64)
		if addedCmpSrc > 0 {
			trace_util_0.Count(_union_scan_00000, 65)
			isFirstRow = true
		}
	} else {
		trace_util_0.Count(_union_scan_00000, 66)
		{
			if addedCmpSrc < 0 {
				trace_util_0.Count(_union_scan_00000, 67)
				isFirstRow = true
			}
		}
	}
	trace_util_0.Count(_union_scan_00000, 62)
	return isFirstRow, nil
}

func (us *UnionScanExec) compare(a, b []types.Datum) (int, error) {
	trace_util_0.Count(_union_scan_00000, 68)
	sc := us.ctx.GetSessionVars().StmtCtx
	for _, colOff := range us.usedIndex {
		trace_util_0.Count(_union_scan_00000, 71)
		aColumn := a[colOff]
		bColumn := b[colOff]
		cmp, err := aColumn.CompareDatum(sc, &bColumn)
		if err != nil {
			trace_util_0.Count(_union_scan_00000, 73)
			return 0, err
		}
		trace_util_0.Count(_union_scan_00000, 72)
		if cmp != 0 {
			trace_util_0.Count(_union_scan_00000, 74)
			return cmp, nil
		}
	}
	trace_util_0.Count(_union_scan_00000, 69)
	aHandle := a[us.belowHandleIndex].GetInt64()
	bHandle := b[us.belowHandleIndex].GetInt64()
	var cmp int
	if aHandle == bHandle {
		trace_util_0.Count(_union_scan_00000, 75)
		cmp = 0
	} else {
		trace_util_0.Count(_union_scan_00000, 76)
		if aHandle > bHandle {
			trace_util_0.Count(_union_scan_00000, 77)
			cmp = 1
		} else {
			trace_util_0.Count(_union_scan_00000, 78)
			{
				cmp = -1
			}
		}
	}
	trace_util_0.Count(_union_scan_00000, 70)
	return cmp, nil
}

// rowWithColsInTxn gets the row from the transaction buffer.
func (us *UnionScanExec) rowWithColsInTxn(t table.Table, h int64, cols []*table.Column) ([]types.Datum, error) {
	trace_util_0.Count(_union_scan_00000, 79)
	key := t.RecordKey(h)
	txn, err := us.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_union_scan_00000, 83)
		return nil, err
	}
	trace_util_0.Count(_union_scan_00000, 80)
	value, err := txn.GetMemBuffer().Get(key)
	if err != nil {
		trace_util_0.Count(_union_scan_00000, 84)
		return nil, err
	}
	trace_util_0.Count(_union_scan_00000, 81)
	v, _, err := tables.DecodeRawRowData(us.ctx, t.Meta(), h, cols, value)
	if err != nil {
		trace_util_0.Count(_union_scan_00000, 85)
		return nil, err
	}
	trace_util_0.Count(_union_scan_00000, 82)
	return v, nil
}

func (us *UnionScanExec) buildAndSortAddedRows(t table.Table) error {
	trace_util_0.Count(_union_scan_00000, 86)
	us.addedRows = make([][]types.Datum, 0, len(us.dirty.addedRows))
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	cols := t.WritableCols()
	for h := range us.dirty.addedRows {
		trace_util_0.Count(_union_scan_00000, 90)
		newData := make([]types.Datum, 0, us.schema.Len())
		data, err := us.rowWithColsInTxn(t, h, cols)
		if err != nil {
			trace_util_0.Count(_union_scan_00000, 95)
			return err
		}
		trace_util_0.Count(_union_scan_00000, 91)
		for _, col := range us.columns {
			trace_util_0.Count(_union_scan_00000, 96)
			if col.ID == model.ExtraHandleID {
				trace_util_0.Count(_union_scan_00000, 97)
				newData = append(newData, types.NewIntDatum(h))
			} else {
				trace_util_0.Count(_union_scan_00000, 98)
				{
					newData = append(newData, data[col.Offset])
				}
			}
		}
		trace_util_0.Count(_union_scan_00000, 92)
		mutableRow.SetDatums(newData...)
		matched, _, err := expression.EvalBool(us.ctx, us.conditions, mutableRow.ToRow())
		if err != nil {
			trace_util_0.Count(_union_scan_00000, 99)
			return err
		}
		trace_util_0.Count(_union_scan_00000, 93)
		if !matched {
			trace_util_0.Count(_union_scan_00000, 100)
			continue
		}
		trace_util_0.Count(_union_scan_00000, 94)
		us.addedRows = append(us.addedRows, newData)
	}
	trace_util_0.Count(_union_scan_00000, 87)
	if us.desc {
		trace_util_0.Count(_union_scan_00000, 101)
		sort.Sort(sort.Reverse(us))
	} else {
		trace_util_0.Count(_union_scan_00000, 102)
		{
			sort.Sort(us)
		}
	}
	trace_util_0.Count(_union_scan_00000, 88)
	if us.sortErr != nil {
		trace_util_0.Count(_union_scan_00000, 103)
		return errors.Trace(us.sortErr)
	}
	trace_util_0.Count(_union_scan_00000, 89)
	return nil
}

// Len implements sort.Interface interface.
func (us *UnionScanExec) Len() int {
	trace_util_0.Count(_union_scan_00000, 104)
	return len(us.addedRows)
}

// Less implements sort.Interface interface.
func (us *UnionScanExec) Less(i, j int) bool {
	trace_util_0.Count(_union_scan_00000, 105)
	cmp, err := us.compare(us.addedRows[i], us.addedRows[j])
	if err != nil {
		trace_util_0.Count(_union_scan_00000, 107)
		us.sortErr = errors.Trace(err)
		return true
	}
	trace_util_0.Count(_union_scan_00000, 106)
	return cmp < 0
}

// Swap implements sort.Interface interface.
func (us *UnionScanExec) Swap(i, j int) {
	trace_util_0.Count(_union_scan_00000, 108)
	us.addedRows[i], us.addedRows[j] = us.addedRows[j], us.addedRows[i]
}

var _union_scan_00000 = "executor/union_scan.go"
