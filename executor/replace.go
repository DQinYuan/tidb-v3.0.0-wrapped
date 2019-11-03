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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	trace_util_0.Count(_replace_00000, 0)
	e.setMessage()
	if e.SelectExec != nil {
		trace_util_0.Count(_replace_00000, 2)
		return e.SelectExec.Close()
	}
	trace_util_0.Count(_replace_00000, 1)
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open(ctx context.Context) error {
	trace_util_0.Count(_replace_00000, 3)
	if e.SelectExec != nil {
		trace_util_0.Count(_replace_00000, 5)
		return e.SelectExec.Open(ctx)
	}
	trace_util_0.Count(_replace_00000, 4)
	e.initEvalBuffer()
	return nil
}

// removeRow removes the duplicate row and cleanup its keys in the key-value map,
// but if the to-be-removed row equals to the to-be-added row, no remove or add things to do.
func (e *ReplaceExec) removeRow(handle int64, r toBeCheckedRow) (bool, error) {
	trace_util_0.Count(_replace_00000, 6)
	newRow := r.row
	oldRow, err := e.batchChecker.getOldRow(e.ctx, r.t, handle, e.GenExprs)
	if err != nil {
		trace_util_0.Count(_replace_00000, 12)
		logutil.Logger(context.Background()).Error("get old row failed when replace", zap.Int64("handle", handle), zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)))
		return false, err
	}
	trace_util_0.Count(_replace_00000, 7)
	rowUnchanged, err := types.EqualDatums(e.ctx.GetSessionVars().StmtCtx, oldRow, newRow)
	if err != nil {
		trace_util_0.Count(_replace_00000, 13)
		return false, err
	}
	trace_util_0.Count(_replace_00000, 8)
	if rowUnchanged {
		trace_util_0.Count(_replace_00000, 14)
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
		return true, nil
	}

	trace_util_0.Count(_replace_00000, 9)
	err = r.t.RemoveRecord(e.ctx, handle, oldRow)
	if err != nil {
		trace_util_0.Count(_replace_00000, 15)
		return false, err
	}
	trace_util_0.Count(_replace_00000, 10)
	e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)

	// Cleanup keys map, because the record was removed.
	err = e.deleteDupKeys(e.ctx, r.t, [][]types.Datum{oldRow})
	if err != nil {
		trace_util_0.Count(_replace_00000, 16)
		return false, err
	}
	trace_util_0.Count(_replace_00000, 11)
	return false, nil
}

// replaceRow removes all duplicate rows for one row, then inserts it.
func (e *ReplaceExec) replaceRow(r toBeCheckedRow) error {
	trace_util_0.Count(_replace_00000, 17)
	if r.handleKey != nil {
		trace_util_0.Count(_replace_00000, 21)
		if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
			trace_util_0.Count(_replace_00000, 22)
			handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
			if err != nil {
				trace_util_0.Count(_replace_00000, 25)
				return err
			}
			trace_util_0.Count(_replace_00000, 23)
			rowUnchanged, err := e.removeRow(handle, r)
			if err != nil {
				trace_util_0.Count(_replace_00000, 26)
				return err
			}
			trace_util_0.Count(_replace_00000, 24)
			if rowUnchanged {
				trace_util_0.Count(_replace_00000, 27)
				return nil
			}
		}
	}

	// Keep on removing duplicated rows.
	trace_util_0.Count(_replace_00000, 18)
	for {
		trace_util_0.Count(_replace_00000, 28)
		rowUnchanged, foundDupKey, err := e.removeIndexRow(r)
		if err != nil {
			trace_util_0.Count(_replace_00000, 32)
			return err
		}
		trace_util_0.Count(_replace_00000, 29)
		if rowUnchanged {
			trace_util_0.Count(_replace_00000, 33)
			return nil
		}
		trace_util_0.Count(_replace_00000, 30)
		if foundDupKey {
			trace_util_0.Count(_replace_00000, 34)
			continue
		}
		trace_util_0.Count(_replace_00000, 31)
		break
	}

	// No duplicated rows now, insert the row.
	trace_util_0.Count(_replace_00000, 19)
	newHandle, err := e.addRecord(r.row)
	if err != nil {
		trace_util_0.Count(_replace_00000, 35)
		return err
	}
	trace_util_0.Count(_replace_00000, 20)
	e.fillBackKeys(r.t, r, newHandle)
	return nil
}

// removeIndexRow removes the row which has a duplicated key.
// the return values:
//     1. bool: true when the row is unchanged. This means no need to remove, and then add the row.
//     2. bool: true when found the duplicated key. This only means that duplicated key was found,
//              and the row was removed.
//     3. error: the error.
func (e *ReplaceExec) removeIndexRow(r toBeCheckedRow) (bool, bool, error) {
	trace_util_0.Count(_replace_00000, 36)
	for _, uk := range r.uniqueKeys {
		trace_util_0.Count(_replace_00000, 38)
		if val, found := e.dupKVs[string(uk.newKV.key)]; found {
			trace_util_0.Count(_replace_00000, 39)
			handle, err := tables.DecodeHandle(val)
			if err != nil {
				trace_util_0.Count(_replace_00000, 42)
				return false, found, err
			}
			trace_util_0.Count(_replace_00000, 40)
			rowUnchanged, err := e.removeRow(handle, r)
			if err != nil {
				trace_util_0.Count(_replace_00000, 43)
				return false, found, err
			}
			trace_util_0.Count(_replace_00000, 41)
			return rowUnchanged, found, nil
		}
	}
	trace_util_0.Count(_replace_00000, 37)
	return false, false, nil
}

func (e *ReplaceExec) exec(ctx context.Context, newRows [][]types.Datum) error {
	trace_util_0.Count(_replace_00000, 44)
	/*
	 * MySQL uses the following algorithm for REPLACE (and LOAD DATA ... REPLACE):
	 *  1. Try to insert the new row into the table
	 *  2. While the insertion fails because a duplicate-key error occurs for a primary key or unique index:
	 *  3. Delete from the table the conflicting row that has the duplicate key value
	 *  4. Try again to insert the new row into the table
	 * See http://dev.mysql.com/doc/refman/5.7/en/replace.html
	 *
	 * For REPLACE statements, the affected-rows value is 2 if the new row replaced an old row,
	 * because in this case, one row was inserted after the duplicate was deleted.
	 * See http://dev.mysql.com/doc/refman/5.7/en/mysql-affected-rows.html
	 */
	err := e.batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		trace_util_0.Count(_replace_00000, 48)
		return err
	}

	// Batch get the to-be-replaced rows in storage.
	trace_util_0.Count(_replace_00000, 45)
	err = e.initDupOldRowValue(e.ctx, e.Table, newRows)
	if err != nil {
		trace_util_0.Count(_replace_00000, 49)
		return err
	}
	trace_util_0.Count(_replace_00000, 46)
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(newRows)))
	for _, r := range e.toBeCheckedRows {
		trace_util_0.Count(_replace_00000, 50)
		err = e.replaceRow(r)
		if err != nil {
			trace_util_0.Count(_replace_00000, 51)
			return err
		}
	}
	trace_util_0.Count(_replace_00000, 47)
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_replace_00000, 52)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_replace_00000, 55)
		span1 := span.Tracer().StartSpan("replace.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_replace_00000, 53)
	req.Reset()
	if len(e.children) > 0 && e.children[0] != nil {
		trace_util_0.Count(_replace_00000, 56)
		return e.insertRowsFromSelect(ctx, e.exec)
	}
	trace_util_0.Count(_replace_00000, 54)
	return e.insertRows(ctx, e.exec)
}

// setMessage sets info message(ERR_INSERT_INFO) generated by REPLACE statement
func (e *ReplaceExec) setMessage() {
	trace_util_0.Count(_replace_00000, 57)
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	if e.SelectExec != nil || numRecords > 1 {
		trace_util_0.Count(_replace_00000, 58)
		numWarnings := stmtCtx.WarningCount()
		numDuplicates := stmtCtx.AffectedRows() - numRecords
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}

var _replace_00000 = "executor/replace.go"
