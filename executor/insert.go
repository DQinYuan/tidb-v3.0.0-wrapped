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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues
	OnDuplicate []*expression.Assignment
	Priority    mysql.PriorityEnum
}

func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) error {
	trace_util_0.Count(_insert_00000, 0)
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()
	ignoreErr := sessVars.StmtCtx.DupKeyAsWarning

	if !sessVars.LightningMode {
		trace_util_0.Count(_insert_00000, 3)
		txn, err := e.ctx.Txn(true)
		if err != nil {
			trace_util_0.Count(_insert_00000, 5)
			return err
		}
		trace_util_0.Count(_insert_00000, 4)
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	}

	trace_util_0.Count(_insert_00000, 1)
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(rows)))
	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
	// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
	// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 {
		trace_util_0.Count(_insert_00000, 6)
		err := e.batchUpdateDupRows(rows)
		if err != nil {
			trace_util_0.Count(_insert_00000, 7)
			return err
		}
	} else {
		trace_util_0.Count(_insert_00000, 8)
		if ignoreErr {
			trace_util_0.Count(_insert_00000, 9)
			err := e.batchCheckAndInsert(rows, e.addRecord)
			if err != nil {
				trace_util_0.Count(_insert_00000, 10)
				return err
			}
		} else {
			trace_util_0.Count(_insert_00000, 11)
			{
				for _, row := range rows {
					trace_util_0.Count(_insert_00000, 12)
					if _, err := e.addRecord(row); err != nil {
						trace_util_0.Count(_insert_00000, 13)
						return err
					}
				}
			}
		}
	}
	trace_util_0.Count(_insert_00000, 2)
	return nil
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(newRows [][]types.Datum) error {
	trace_util_0.Count(_insert_00000, 14)
	err := e.batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		trace_util_0.Count(_insert_00000, 18)
		return err
	}

	// Batch get the to-be-updated rows in storage.
	trace_util_0.Count(_insert_00000, 15)
	err = e.initDupOldRowValue(e.ctx, e.Table, newRows)
	if err != nil {
		trace_util_0.Count(_insert_00000, 19)
		return err
	}

	trace_util_0.Count(_insert_00000, 16)
	for i, r := range e.toBeCheckedRows {
		trace_util_0.Count(_insert_00000, 20)
		if r.handleKey != nil {
			trace_util_0.Count(_insert_00000, 23)
			if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
				trace_util_0.Count(_insert_00000, 24)
				handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
				if err != nil {
					trace_util_0.Count(_insert_00000, 27)
					return err
				}
				trace_util_0.Count(_insert_00000, 25)
				err = e.updateDupRow(r, handle, e.OnDuplicate)
				if err != nil {
					trace_util_0.Count(_insert_00000, 28)
					return err
				}
				trace_util_0.Count(_insert_00000, 26)
				continue
			}
		}
		trace_util_0.Count(_insert_00000, 21)
		for _, uk := range r.uniqueKeys {
			trace_util_0.Count(_insert_00000, 29)
			if val, found := e.dupKVs[string(uk.newKV.key)]; found {
				trace_util_0.Count(_insert_00000, 30)
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					trace_util_0.Count(_insert_00000, 33)
					return err
				}
				trace_util_0.Count(_insert_00000, 31)
				err = e.updateDupRow(r, handle, e.OnDuplicate)
				if err != nil {
					trace_util_0.Count(_insert_00000, 34)
					return err
				}
				trace_util_0.Count(_insert_00000, 32)
				newRows[i] = nil
				break
			}
		}
		// If row was checked with no duplicate keys,
		// we should do insert the row,
		// and key-values should be filled back to dupOldRowValues for the further row check,
		// due to there may be duplicate keys inside the insert statement.
		trace_util_0.Count(_insert_00000, 22)
		if newRows[i] != nil {
			trace_util_0.Count(_insert_00000, 35)
			newHandle, err := e.addRecord(newRows[i])
			if err != nil {
				trace_util_0.Count(_insert_00000, 37)
				return err
			}
			trace_util_0.Count(_insert_00000, 36)
			e.fillBackKeys(e.Table, r, newHandle)
		}
	}
	trace_util_0.Count(_insert_00000, 17)
	return nil
}

// Next implements the Executor Next interface.
func (e *InsertExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_insert_00000, 38)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_insert_00000, 41)
		span1 := span.Tracer().StartSpan("insert.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_insert_00000, 39)
	req.Reset()
	if len(e.children) > 0 && e.children[0] != nil {
		trace_util_0.Count(_insert_00000, 42)
		return e.insertRowsFromSelect(ctx, e.exec)
	}
	trace_util_0.Count(_insert_00000, 40)
	return e.insertRows(ctx, e.exec)
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	trace_util_0.Count(_insert_00000, 43)
	e.ctx.GetSessionVars().CurrInsertValues = chunk.Row{}
	e.setMessage()
	if e.SelectExec != nil {
		trace_util_0.Count(_insert_00000, 45)
		return e.SelectExec.Close()
	}
	trace_util_0.Count(_insert_00000, 44)
	return nil
}

// Open implements the Executor Open interface.
func (e *InsertExec) Open(ctx context.Context) error {
	trace_util_0.Count(_insert_00000, 46)
	if e.SelectExec != nil {
		trace_util_0.Count(_insert_00000, 48)
		return e.SelectExec.Open(ctx)
	}
	trace_util_0.Count(_insert_00000, 47)
	e.initEvalBuffer()
	return nil
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(row toBeCheckedRow, handle int64, onDuplicate []*expression.Assignment) error {
	trace_util_0.Count(_insert_00000, 49)
	oldRow, err := e.getOldRow(e.ctx, e.Table, handle, e.GenExprs)
	if err != nil {
		trace_util_0.Count(_insert_00000, 53)
		logutil.Logger(context.Background()).Error("get old row failed when insert on dup", zap.Int64("handle", handle), zap.String("toBeInsertedRow", types.DatumsToStrNoErr(row.row)))
		return err
	}
	// Do update row.
	trace_util_0.Count(_insert_00000, 50)
	updatedRow, handleChanged, newHandle, err := e.doDupRowUpdate(handle, oldRow, row.row, onDuplicate)
	if e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning && kv.ErrKeyExists.Equal(err) {
		trace_util_0.Count(_insert_00000, 54)
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}
	trace_util_0.Count(_insert_00000, 51)
	if err != nil {
		trace_util_0.Count(_insert_00000, 55)
		return err
	}
	trace_util_0.Count(_insert_00000, 52)
	return e.updateDupKeyValues(handle, newHandle, handleChanged, oldRow, updatedRow)
}

// doDupRowUpdate updates the duplicate row.
func (e *InsertExec) doDupRowUpdate(handle int64, oldRow []types.Datum, newRow []types.Datum,
	cols []*expression.Assignment) ([]types.Datum, bool, int64, error) {
	trace_util_0.Count(_insert_00000, 56)
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(newRow).ToRow()

	// NOTE: In order to execute the expression inside the column assignment,
	// we have to put the value of "oldRow" before "newRow" in "row4Update" to
	// be consistent with "Schema4OnDuplicate" in the "Insert" PhysicalPlan.
	row4Update := make([]types.Datum, 0, len(oldRow)+len(newRow))
	row4Update = append(row4Update, oldRow...)
	row4Update = append(row4Update, newRow...)

	// Update old row when the key is duplicated.
	for _, col := range cols {
		trace_util_0.Count(_insert_00000, 59)
		val, err1 := col.Expr.Eval(chunk.MutRowFromDatums(row4Update).ToRow())
		if err1 != nil {
			trace_util_0.Count(_insert_00000, 61)
			return nil, false, 0, err1
		}
		trace_util_0.Count(_insert_00000, 60)
		row4Update[col.Col.Index] = val
		assignFlag[col.Col.Index] = true
	}

	trace_util_0.Count(_insert_00000, 57)
	newData := row4Update[:len(oldRow)]
	_, handleChanged, newHandle, err := updateRecord(e.ctx, handle, oldRow, newData, assignFlag, e.Table, true)
	if err != nil {
		trace_util_0.Count(_insert_00000, 62)
		return nil, false, 0, err
	}
	trace_util_0.Count(_insert_00000, 58)
	return newData, handleChanged, newHandle, nil
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (e *InsertExec) updateDupKeyValues(oldHandle int64, newHandle int64,
	handleChanged bool, oldRow []types.Datum, updatedRow []types.Datum) error {
	trace_util_0.Count(_insert_00000, 63)
	// There is only one row per update.
	fillBackKeysInRows, err := e.getKeysNeedCheck(e.ctx, e.Table, [][]types.Datum{updatedRow})
	if err != nil {
		trace_util_0.Count(_insert_00000, 67)
		return err
	}
	// Delete old keys and fill back new key-values of the updated row.
	trace_util_0.Count(_insert_00000, 64)
	err = e.deleteDupKeys(e.ctx, e.Table, [][]types.Datum{oldRow})
	if err != nil {
		trace_util_0.Count(_insert_00000, 68)
		return err
	}

	trace_util_0.Count(_insert_00000, 65)
	if handleChanged {
		trace_util_0.Count(_insert_00000, 69)
		delete(e.dupOldRowValues, string(e.Table.RecordKey(oldHandle)))
		e.fillBackKeys(e.Table, fillBackKeysInRows[0], newHandle)
	} else {
		trace_util_0.Count(_insert_00000, 70)
		{
			e.fillBackKeys(e.Table, fillBackKeysInRows[0], oldHandle)
		}
	}
	trace_util_0.Count(_insert_00000, 66)
	return nil
}

// setMessage sets info message(ERR_INSERT_INFO) generated by INSERT statement
func (e *InsertExec) setMessage() {
	trace_util_0.Count(_insert_00000, 71)
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	if e.SelectExec != nil || numRecords > 1 {
		trace_util_0.Count(_insert_00000, 72)
		numWarnings := stmtCtx.WarningCount()
		var numDuplicates uint64
		if stmtCtx.DupKeyAsWarning {
			trace_util_0.Count(_insert_00000, 74)
			// if ignoreErr
			numDuplicates = numRecords - stmtCtx.CopiedRows()
		} else {
			trace_util_0.Count(_insert_00000, 75)
			{
				if e.ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
					trace_util_0.Count(_insert_00000, 76)
					numDuplicates = stmtCtx.TouchedRows()
				} else {
					trace_util_0.Count(_insert_00000, 77)
					{
						numDuplicates = stmtCtx.UpdatedRows()
					}
				}
			}
		}
		trace_util_0.Count(_insert_00000, 73)
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}

var _insert_00000 = "executor/insert.go"
