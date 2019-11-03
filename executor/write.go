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
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadDataExec{}
)

// updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which columns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
// The return values:
//     1. changed (bool) : does the update really change the row values. e.g. update set i = 1 where i = 1;
//     2. handleChanged (bool) : is the handle changed after the update.
//     3. newHandle (int64) : if handleChanged == true, the newHandle means the new handle after update.
//     4. err (error) : error in the update.
func updateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, modified []bool, t table.Table,
	onDup bool) (bool, bool, int64, error) {
	trace_util_0.Count(_write_00000, 0)
	sc := ctx.GetSessionVars().StmtCtx
	changed, handleChanged := false, false
	// onUpdateSpecified is for "UPDATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	onUpdateSpecified := make(map[int]bool)
	var newHandle int64

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.

	// 1. Cast modified values.
	for i, col := range t.Cols() {
		trace_util_0.Count(_write_00000, 7)
		if modified[i] {
			trace_util_0.Count(_write_00000, 8)
			// Cast changed fields with respective columns.
			v, err := table.CastValue(ctx, newData[i], col.ToInfo())
			if err != nil {
				trace_util_0.Count(_write_00000, 10)
				return false, false, 0, err
			}
			trace_util_0.Count(_write_00000, 9)
			newData[i] = v
		}
	}

	// 2. Handle the bad null error.
	trace_util_0.Count(_write_00000, 1)
	for i, col := range t.Cols() {
		trace_util_0.Count(_write_00000, 11)
		var err error
		if newData[i], err = col.HandleBadNull(newData[i], sc); err != nil {
			trace_util_0.Count(_write_00000, 12)
			return false, false, 0, err
		}
	}

	// 3. Compare datum, then handle some flags.
	trace_util_0.Count(_write_00000, 2)
	for i, col := range t.Cols() {
		trace_util_0.Count(_write_00000, 13)
		cmp, err := newData[i].CompareDatum(sc, &oldData[i])
		if err != nil {
			trace_util_0.Count(_write_00000, 15)
			return false, false, 0, err
		}
		trace_util_0.Count(_write_00000, 14)
		if cmp != 0 {
			trace_util_0.Count(_write_00000, 16)
			changed = true
			modified[i] = true
			// Rebase auto increment id if the field is changed.
			if mysql.HasAutoIncrementFlag(col.Flag) {
				trace_util_0.Count(_write_00000, 18)
				if err = t.RebaseAutoID(ctx, newData[i].GetInt64(), true); err != nil {
					trace_util_0.Count(_write_00000, 19)
					return false, false, 0, err
				}
			}
			trace_util_0.Count(_write_00000, 17)
			if col.IsPKHandleColumn(t.Meta()) {
				trace_util_0.Count(_write_00000, 20)
				handleChanged = true
				newHandle = newData[i].GetInt64()
			}
		} else {
			trace_util_0.Count(_write_00000, 21)
			{
				if mysql.HasOnUpdateNowFlag(col.Flag) && modified[i] {
					trace_util_0.Count(_write_00000, 23)
					// It's for "UPDATE t SET ts = ts" and ts is a timestamp.
					onUpdateSpecified[i] = true
				}
				trace_util_0.Count(_write_00000, 22)
				modified[i] = false
			}
		}
	}

	trace_util_0.Count(_write_00000, 3)
	sc.AddTouchedRows(1)
	// If no changes, nothing to do, return directly.
	if !changed {
		trace_util_0.Count(_write_00000, 24)
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			trace_util_0.Count(_write_00000, 26)
			sc.AddAffectedRows(1)
		}
		trace_util_0.Count(_write_00000, 25)
		return false, false, 0, nil
	}

	// 4. Fill values into on-update-now fields, only if they are really changed.
	trace_util_0.Count(_write_00000, 4)
	for i, col := range t.Cols() {
		trace_util_0.Count(_write_00000, 27)
		if mysql.HasOnUpdateNowFlag(col.Flag) && !modified[i] && !onUpdateSpecified[i] {
			trace_util_0.Count(_write_00000, 28)
			if v, err := expression.GetTimeValue(ctx, strings.ToUpper(ast.CurrentTimestamp), col.Tp, col.Decimal); err == nil {
				trace_util_0.Count(_write_00000, 29)
				newData[i] = v
				modified[i] = true
			} else {
				trace_util_0.Count(_write_00000, 30)
				{
					return false, false, 0, err
				}
			}
		}
	}

	// 5. If handle changed, remove the old then add the new record, otherwise update the record.
	trace_util_0.Count(_write_00000, 5)
	var err error
	if handleChanged {
		trace_util_0.Count(_write_00000, 31)
		if sc.DupKeyAsWarning {
			trace_util_0.Count(_write_00000, 35)
			// For `UPDATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UPDATE`
			// If the new handle exists, this will avoid to remove the record.
			err = tables.CheckHandleExists(ctx, t, newHandle, newData)
			if err != nil {
				trace_util_0.Count(_write_00000, 36)
				return false, handleChanged, newHandle, err
			}
		}
		trace_util_0.Count(_write_00000, 32)
		if err = t.RemoveRecord(ctx, h, oldData); err != nil {
			trace_util_0.Count(_write_00000, 37)
			return false, false, 0, err
		}
		// the `affectedRows` is increased when adding new record.
		trace_util_0.Count(_write_00000, 33)
		newHandle, err = t.AddRecord(ctx, newData,
			&table.AddRecordOpt{CreateIdxOpt: table.CreateIdxOpt{SkipHandleCheck: sc.DupKeyAsWarning}, IsUpdate: true})
		if err != nil {
			trace_util_0.Count(_write_00000, 38)
			return false, false, 0, err
		}
		trace_util_0.Count(_write_00000, 34)
		if onDup {
			trace_util_0.Count(_write_00000, 39)
			sc.AddAffectedRows(1)
		}
	} else {
		trace_util_0.Count(_write_00000, 40)
		{
			// Update record to new value and update index.
			if err = t.UpdateRecord(ctx, h, oldData, newData, modified); err != nil {
				trace_util_0.Count(_write_00000, 42)
				return false, false, 0, err
			}
			trace_util_0.Count(_write_00000, 41)
			if onDup {
				trace_util_0.Count(_write_00000, 43)
				sc.AddAffectedRows(2)
			} else {
				trace_util_0.Count(_write_00000, 44)
				{
					sc.AddAffectedRows(1)
				}
			}
		}
	}
	trace_util_0.Count(_write_00000, 6)
	sc.AddUpdatedRows(1)
	sc.AddCopiedRows(1)

	return true, handleChanged, newHandle, nil
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	trace_util_0.Count(_write_00000, 45)
	newErr := types.ErrDataTooLong.GenWithStack("Data too long for column '%v' at row %v", colName, rowIdx)
	logutil.Logger(context.Background()).Error("data too long for column", zap.String("colName", colName), zap.Int("rowIndex", rowIdx))
	return newErr
}

func getTableOffset(schema *expression.Schema, handleCol *expression.Column) int {
	trace_util_0.Count(_write_00000, 46)
	for i, col := range schema.Columns {
		trace_util_0.Count(_write_00000, 48)
		if col.DBName.L == handleCol.DBName.L && col.TblName.L == handleCol.TblName.L {
			trace_util_0.Count(_write_00000, 49)
			return i
		}
	}
	trace_util_0.Count(_write_00000, 47)
	panic("Couldn't get column information when do update/delete")
}

var _write_00000 = "executor/write.go"