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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	SelectExec Executor

	Tables       []*ast.TableName
	IsMultiTable bool
	tblID2Table  map[int64]table.Table
	// tblMap is the table map value is an array which contains table aliases.
	// Table ID may not be unique for deleting multiple tables, for statements like
	// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
	// by its alias instead of ID.
	tblMap map[int64][]*ast.TableName
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_delete_00000, 0)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_delete_00000, 3)
		span1 := span.Tracer().StartSpan("delete.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_delete_00000, 1)
	req.Reset()
	if e.IsMultiTable {
		trace_util_0.Count(_delete_00000, 4)
		return e.deleteMultiTablesByChunk(ctx)
	}
	trace_util_0.Count(_delete_00000, 2)
	return e.deleteSingleTableByChunk(ctx)
}

// matchingDeletingTable checks whether this column is from the table which is in the deleting list.
func (e *DeleteExec) matchingDeletingTable(tableID int64, col *expression.Column) bool {
	trace_util_0.Count(_delete_00000, 5)
	names, ok := e.tblMap[tableID]
	if !ok {
		trace_util_0.Count(_delete_00000, 8)
		return false
	}
	trace_util_0.Count(_delete_00000, 6)
	for _, n := range names {
		trace_util_0.Count(_delete_00000, 9)
		if (col.DBName.L == "" || col.DBName.L == n.Schema.L) && col.TblName.L == n.Name.L {
			trace_util_0.Count(_delete_00000, 10)
			return true
		}
	}
	trace_util_0.Count(_delete_00000, 7)
	return false
}

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleCol *expression.Column, row []types.Datum) error {
	trace_util_0.Count(_delete_00000, 11)
	end := len(row)
	if handleIsExtra(handleCol) {
		trace_util_0.Count(_delete_00000, 14)
		end--
	}
	trace_util_0.Count(_delete_00000, 12)
	handle := row[handleCol.Index].GetInt64()
	err := e.removeRow(e.ctx, tbl, handle, row[:end])
	if err != nil {
		trace_util_0.Count(_delete_00000, 15)
		return err
	}

	trace_util_0.Count(_delete_00000, 13)
	return nil
}

func (e *DeleteExec) deleteSingleTableByChunk(ctx context.Context) error {
	trace_util_0.Count(_delete_00000, 16)
	var (
		id        int64
		tbl       table.Table
		handleCol *expression.Column
		rowCount  int
	)
	for i, t := range e.tblID2Table {
		trace_util_0.Count(_delete_00000, 19)
		id, tbl = i, t
		handleCol = e.children[0].Schema().TblID2Handle[id][0]
		break
	}

	// If tidb_batch_delete is ON and not in a transaction, we could use BatchDelete mode.
	trace_util_0.Count(_delete_00000, 17)
	batchDelete := e.ctx.GetSessionVars().BatchDelete && !e.ctx.GetSessionVars().InTxn()
	batchDMLSize := e.ctx.GetSessionVars().DMLBatchSize
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	for {
		trace_util_0.Count(_delete_00000, 20)
		iter := chunk.NewIterator4Chunk(chk)

		err := Next(ctx, e.children[0], chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_delete_00000, 24)
			return err
		}
		trace_util_0.Count(_delete_00000, 21)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_delete_00000, 25)
			break
		}

		trace_util_0.Count(_delete_00000, 22)
		for chunkRow := iter.Begin(); chunkRow != iter.End(); chunkRow = iter.Next() {
			trace_util_0.Count(_delete_00000, 26)
			if batchDelete && rowCount >= batchDMLSize {
				trace_util_0.Count(_delete_00000, 29)
				if err = e.ctx.StmtCommit(); err != nil {
					trace_util_0.Count(_delete_00000, 32)
					return err
				}
				trace_util_0.Count(_delete_00000, 30)
				if err = e.ctx.NewTxn(ctx); err != nil {
					trace_util_0.Count(_delete_00000, 33)
					// We should return a special error for batch insert.
					return ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
				}
				trace_util_0.Count(_delete_00000, 31)
				rowCount = 0
			}

			trace_util_0.Count(_delete_00000, 27)
			datumRow := chunkRow.GetDatumRow(fields)
			err = e.deleteOneRow(tbl, handleCol, datumRow)
			if err != nil {
				trace_util_0.Count(_delete_00000, 34)
				return err
			}
			trace_util_0.Count(_delete_00000, 28)
			rowCount++
		}
		trace_util_0.Count(_delete_00000, 23)
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	trace_util_0.Count(_delete_00000, 18)
	return nil
}

func (e *DeleteExec) initialMultiTableTblMap() {
	trace_util_0.Count(_delete_00000, 35)
	e.tblMap = make(map[int64][]*ast.TableName, len(e.Tables))
	for _, t := range e.Tables {
		trace_util_0.Count(_delete_00000, 36)
		e.tblMap[t.TableInfo.ID] = append(e.tblMap[t.TableInfo.ID], t)
	}
}

func (e *DeleteExec) getColPosInfos(schema *expression.Schema) []tblColPosInfo {
	trace_util_0.Count(_delete_00000, 37)
	var colPosInfos []tblColPosInfo
	// Extract the columns' position information of this table in the delete's schema, together with the table id
	// and its handle's position in the schema.
	for id, cols := range schema.TblID2Handle {
		trace_util_0.Count(_delete_00000, 39)
		tbl := e.tblID2Table[id]
		for _, col := range cols {
			trace_util_0.Count(_delete_00000, 40)
			if !e.matchingDeletingTable(id, col) {
				trace_util_0.Count(_delete_00000, 42)
				continue
			}
			trace_util_0.Count(_delete_00000, 41)
			offset := getTableOffset(schema, col)
			end := offset + len(tbl.Cols())
			colPosInfos = append(colPosInfos, tblColPosInfo{tblID: id, colBeginIndex: offset, colEndIndex: end, handleIndex: col.Index})
		}
	}
	trace_util_0.Count(_delete_00000, 38)
	return colPosInfos
}

func (e *DeleteExec) composeTblRowMap(tblRowMap tableRowMapType, colPosInfos []tblColPosInfo, joinedRow []types.Datum) {
	trace_util_0.Count(_delete_00000, 43)
	// iterate all the joined tables, and got the copresonding rows in joinedRow.
	for _, info := range colPosInfos {
		trace_util_0.Count(_delete_00000, 44)
		if tblRowMap[info.tblID] == nil {
			trace_util_0.Count(_delete_00000, 46)
			tblRowMap[info.tblID] = make(map[int64][]types.Datum)
		}
		trace_util_0.Count(_delete_00000, 45)
		handle := joinedRow[info.handleIndex].GetInt64()
		// tblRowMap[info.tblID][handle] hold the row datas binding to this table and this handle.
		tblRowMap[info.tblID][handle] = joinedRow[info.colBeginIndex:info.colEndIndex]
	}
}

func (e *DeleteExec) deleteMultiTablesByChunk(ctx context.Context) error {
	trace_util_0.Count(_delete_00000, 47)
	if len(e.Tables) == 0 {
		trace_util_0.Count(_delete_00000, 50)
		return nil
	}

	trace_util_0.Count(_delete_00000, 48)
	e.initialMultiTableTblMap()
	colPosInfos := e.getColPosInfos(e.children[0].Schema())
	tblRowMap := make(tableRowMapType)
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	for {
		trace_util_0.Count(_delete_00000, 51)
		iter := chunk.NewIterator4Chunk(chk)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_delete_00000, 55)
			return err
		}
		trace_util_0.Count(_delete_00000, 52)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_delete_00000, 56)
			break
		}

		trace_util_0.Count(_delete_00000, 53)
		for joinedChunkRow := iter.Begin(); joinedChunkRow != iter.End(); joinedChunkRow = iter.Next() {
			trace_util_0.Count(_delete_00000, 57)
			joinedDatumRow := joinedChunkRow.GetDatumRow(fields)
			e.composeTblRowMap(tblRowMap, colPosInfos, joinedDatumRow)
		}
		trace_util_0.Count(_delete_00000, 54)
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	trace_util_0.Count(_delete_00000, 49)
	return e.removeRowsInTblRowMap(tblRowMap)
}

func (e *DeleteExec) removeRowsInTblRowMap(tblRowMap tableRowMapType) error {
	trace_util_0.Count(_delete_00000, 58)
	for id, rowMap := range tblRowMap {
		trace_util_0.Count(_delete_00000, 60)
		for handle, data := range rowMap {
			trace_util_0.Count(_delete_00000, 61)
			err := e.removeRow(e.ctx, e.tblID2Table[id], handle, data)
			if err != nil {
				trace_util_0.Count(_delete_00000, 62)
				return err
			}
		}
	}

	trace_util_0.Count(_delete_00000, 59)
	return nil
}

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h int64, data []types.Datum) error {
	trace_util_0.Count(_delete_00000, 63)
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		trace_util_0.Count(_delete_00000, 65)
		return err
	}
	trace_util_0.Count(_delete_00000, 64)
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	trace_util_0.Count(_delete_00000, 66)
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	trace_util_0.Count(_delete_00000, 67)
	return e.SelectExec.Open(ctx)
}

type tblColPosInfo struct {
	tblID         int64
	colBeginIndex int
	colEndIndex   int
	handleIndex   int
}

// tableRowMapType is a map for unique (Table, Row) pair. key is the tableID.
// the key in map[int64]Row is the joined table handle, which represent a unique reference row.
// the value in map[int64]Row is the deleting row.
type tableRowMapType map[int64]map[int64][]types.Datum

var _delete_00000 = "executor/delete.go"
