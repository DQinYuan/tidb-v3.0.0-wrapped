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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// UpdateExec represents a new update executor.
type UpdateExec struct {
	baseExecutor

	SelectExec  Executor
	OrderedList []*expression.Assignment

	// updatedRowKeys is a map for unique (Table, handle) pair.
	// The value is true if the row is changed, or false otherwise
	updatedRowKeys map[int64]map[int64]bool
	tblID2table    map[int64]table.Table

	rows        [][]types.Datum // The rows fetched from TableExec.
	newRowsData [][]types.Datum // The new values to be set.
	fetched     bool
	cursor      int
	matched     uint64 // a counter of matched rows during update
	// columns2Handle stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see executor.cols2Handle
	columns2Handle cols2HandleSlice
	evalBuffer     chunk.MutRow
}

func (e *UpdateExec) exec(schema *expression.Schema) ([]types.Datum, error) {
	trace_util_0.Count(_update_00000, 0)
	assignFlag, err := e.getUpdateColumns(e.ctx, schema.Len())
	if err != nil {
		trace_util_0.Count(_update_00000, 5)
		return nil, err
	}
	trace_util_0.Count(_update_00000, 1)
	if e.cursor >= len(e.rows) {
		trace_util_0.Count(_update_00000, 6)
		return nil, nil
	}
	trace_util_0.Count(_update_00000, 2)
	if e.updatedRowKeys == nil {
		trace_util_0.Count(_update_00000, 7)
		e.updatedRowKeys = make(map[int64]map[int64]bool)
	}
	trace_util_0.Count(_update_00000, 3)
	row := e.rows[e.cursor]
	newData := e.newRowsData[e.cursor]
	for id, cols := range schema.TblID2Handle {
		trace_util_0.Count(_update_00000, 8)
		tbl := e.tblID2table[id]
		if e.updatedRowKeys[id] == nil {
			trace_util_0.Count(_update_00000, 10)
			e.updatedRowKeys[id] = make(map[int64]bool)
		}
		trace_util_0.Count(_update_00000, 9)
		for _, col := range cols {
			trace_util_0.Count(_update_00000, 11)
			offset := getTableOffset(schema, col)
			end := offset + len(tbl.WritableCols())
			handleDatum := row[col.Index]
			if e.canNotUpdate(handleDatum) {
				trace_util_0.Count(_update_00000, 19)
				continue
			}
			trace_util_0.Count(_update_00000, 12)
			handle := row[col.Index].GetInt64()
			oldData := row[offset:end]
			newTableData := newData[offset:end]
			updatable := false
			flags := assignFlag[offset:end]
			for _, flag := range flags {
				trace_util_0.Count(_update_00000, 20)
				if flag {
					trace_util_0.Count(_update_00000, 21)
					updatable = true
					break
				}
			}
			trace_util_0.Count(_update_00000, 13)
			if !updatable {
				trace_util_0.Count(_update_00000, 22)
				// If there's nothing to update, we can just skip current row
				continue
			}
			trace_util_0.Count(_update_00000, 14)
			changed, ok := e.updatedRowKeys[id][handle]
			if !ok {
				trace_util_0.Count(_update_00000, 23)
				// Row is matched for the first time, increment `matched` counter
				e.matched++
			}
			trace_util_0.Count(_update_00000, 15)
			if changed {
				trace_util_0.Count(_update_00000, 24)
				// Each matched row is updated once, even if it matches the conditions multiple times.
				continue
			}

			// Update row
			trace_util_0.Count(_update_00000, 16)
			changed, _, _, err1 := updateRecord(e.ctx, handle, oldData, newTableData, flags, tbl, false)
			if err1 == nil {
				trace_util_0.Count(_update_00000, 25)
				e.updatedRowKeys[id][handle] = changed
				continue
			}

			trace_util_0.Count(_update_00000, 17)
			sc := e.ctx.GetSessionVars().StmtCtx
			if kv.ErrKeyExists.Equal(err1) && sc.DupKeyAsWarning {
				trace_util_0.Count(_update_00000, 26)
				sc.AppendWarning(err1)
				continue
			}
			trace_util_0.Count(_update_00000, 18)
			return nil, err1
		}
	}
	trace_util_0.Count(_update_00000, 4)
	e.cursor++
	return []types.Datum{}, nil
}

// canNotUpdate checks the handle of a record to decide whether that record
// can not be updated. The handle is NULL only when it is the inner side of an
// outer join: the outer row can not match any inner rows, and in this scenario
// the inner handle field is filled with a NULL value.
//
// This fixes: https://github.com/pingcap/tidb/issues/7176.
func (e *UpdateExec) canNotUpdate(handle types.Datum) bool {
	trace_util_0.Count(_update_00000, 27)
	return handle.IsNull()
}

// Next implements the Executor Next interface.
func (e *UpdateExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_update_00000, 28)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_update_00000, 31)
		span1 := span.Tracer().StartSpan("update.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_update_00000, 29)
	req.Reset()
	if !e.fetched {
		trace_util_0.Count(_update_00000, 32)
		err := e.fetchChunkRows(ctx)
		if err != nil {
			trace_util_0.Count(_update_00000, 34)
			return err
		}
		trace_util_0.Count(_update_00000, 33)
		e.fetched = true
		e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(e.rows)))

		for {
			trace_util_0.Count(_update_00000, 35)
			row, err := e.exec(e.children[0].Schema())
			if err != nil {
				trace_util_0.Count(_update_00000, 37)
				return err
			}

			// once "row == nil" there is no more data waiting to be updated,
			// the execution of UpdateExec is finished.
			trace_util_0.Count(_update_00000, 36)
			if row == nil {
				trace_util_0.Count(_update_00000, 38)
				break
			}
		}
	}

	trace_util_0.Count(_update_00000, 30)
	return nil
}

func (e *UpdateExec) fetchChunkRows(ctx context.Context) error {
	trace_util_0.Count(_update_00000, 39)
	fields := retTypes(e.children[0])
	schema := e.children[0].Schema()
	colsInfo := make([]*table.Column, len(fields))
	for id, cols := range schema.TblID2Handle {
		trace_util_0.Count(_update_00000, 42)
		tbl := e.tblID2table[id]
		for _, col := range cols {
			trace_util_0.Count(_update_00000, 43)
			offset := getTableOffset(schema, col)
			for i, c := range tbl.WritableCols() {
				trace_util_0.Count(_update_00000, 44)
				colsInfo[offset+i] = c
			}
		}
	}
	trace_util_0.Count(_update_00000, 40)
	globalRowIdx := 0
	chk := newFirstChunk(e.children[0])
	e.evalBuffer = chunk.MutRowFromTypes(fields)
	for {
		trace_util_0.Count(_update_00000, 45)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_update_00000, 49)
			return err
		}

		trace_util_0.Count(_update_00000, 46)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_update_00000, 50)
			break
		}

		trace_util_0.Count(_update_00000, 47)
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			trace_util_0.Count(_update_00000, 51)
			chunkRow := chk.GetRow(rowIdx)
			datumRow := chunkRow.GetDatumRow(fields)
			newRow, err1 := e.composeNewRow(globalRowIdx, datumRow, colsInfo)
			if err1 != nil {
				trace_util_0.Count(_update_00000, 53)
				return err1
			}
			trace_util_0.Count(_update_00000, 52)
			e.rows = append(e.rows, datumRow)
			e.newRowsData = append(e.newRowsData, newRow)
			globalRowIdx++
		}
		trace_util_0.Count(_update_00000, 48)
		chk = chunk.Renew(chk, e.maxChunkSize)
	}
	trace_util_0.Count(_update_00000, 41)
	return nil
}

func (e *UpdateExec) handleErr(colName model.CIStr, rowIdx int, err error) error {
	trace_util_0.Count(_update_00000, 54)
	if err == nil {
		trace_util_0.Count(_update_00000, 58)
		return nil
	}

	trace_util_0.Count(_update_00000, 55)
	if types.ErrDataTooLong.Equal(err) {
		trace_util_0.Count(_update_00000, 59)
		return resetErrDataTooLong(colName.O, rowIdx+1, err)
	}

	trace_util_0.Count(_update_00000, 56)
	if types.ErrOverflow.Equal(err) {
		trace_util_0.Count(_update_00000, 60)
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(colName.O, rowIdx+1)
	}

	trace_util_0.Count(_update_00000, 57)
	return err
}

func (e *UpdateExec) composeNewRow(rowIdx int, oldRow []types.Datum, cols []*table.Column) ([]types.Datum, error) {
	trace_util_0.Count(_update_00000, 61)
	newRowData := types.CloneRow(oldRow)
	e.evalBuffer.SetDatums(newRowData...)
	for _, assign := range e.OrderedList {
		trace_util_0.Count(_update_00000, 63)
		handleIdx, handleFound := e.columns2Handle.findHandle(int32(assign.Col.Index))
		if handleFound && e.canNotUpdate(oldRow[handleIdx]) {
			trace_util_0.Count(_update_00000, 67)
			continue
		}
		trace_util_0.Count(_update_00000, 64)
		val, err := assign.Expr.Eval(e.evalBuffer.ToRow())
		if err = e.handleErr(assign.Col.ColName, rowIdx, err); err != nil {
			trace_util_0.Count(_update_00000, 68)
			return nil, err
		}

		// info of `_tidb_rowid` column is nil.
		// No need to cast `_tidb_rowid` column value.
		trace_util_0.Count(_update_00000, 65)
		if cols[assign.Col.Index] != nil {
			trace_util_0.Count(_update_00000, 69)
			val, err = table.CastValue(e.ctx, val, cols[assign.Col.Index].ColumnInfo)
			if err = e.handleErr(assign.Col.ColName, rowIdx, err); err != nil {
				trace_util_0.Count(_update_00000, 70)
				return nil, err
			}
		}

		trace_util_0.Count(_update_00000, 66)
		newRowData[assign.Col.Index] = *val.Copy()
		e.evalBuffer.SetDatum(assign.Col.Index, val)
	}
	trace_util_0.Count(_update_00000, 62)
	return newRowData, nil
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	trace_util_0.Count(_update_00000, 71)
	e.setMessage()
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open(ctx context.Context) error {
	trace_util_0.Count(_update_00000, 72)
	return e.SelectExec.Open(ctx)
}

func (e *UpdateExec) getUpdateColumns(ctx sessionctx.Context, schemaLen int) ([]bool, error) {
	trace_util_0.Count(_update_00000, 73)
	assignFlag := make([]bool, schemaLen)
	for _, v := range e.OrderedList {
		trace_util_0.Count(_update_00000, 75)
		if !ctx.GetSessionVars().AllowWriteRowID && v.Col.ColName.L == model.ExtraHandleName.L {
			trace_util_0.Count(_update_00000, 77)
			return nil, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported.")
		}
		trace_util_0.Count(_update_00000, 76)
		idx := v.Col.Index
		assignFlag[idx] = true
	}
	trace_util_0.Count(_update_00000, 74)
	return assignFlag, nil
}

// setMessage sets info message(ERR_UPDATE_INFO) generated by UPDATE statement
func (e *UpdateExec) setMessage() {
	trace_util_0.Count(_update_00000, 78)
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numMatched := e.matched
	numChanged := stmtCtx.UpdatedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUpdateInfo], numMatched, numChanged, numWarnings)
	stmtCtx.SetMessage(msg)
}

var _update_00000 = "executor/update.go"
