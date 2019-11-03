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

package util

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	deleteRangesTable         = `gc_delete_range`
	doneDeleteRangesTable     = `gc_delete_range_done`
	loadDeleteRangeSQL        = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%s WHERE ts < %v`
	recordDoneDeletedRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	completeDeleteRangeSQL    = `DELETE FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	updateDeleteRangeSQL      = `UPDATE mysql.gc_delete_range SET start_key = "%s" WHERE job_id = %d AND element_id = %d AND start_key = "%s"`
	deleteDoneRecordSQL       = `DELETE FROM mysql.gc_delete_range_done WHERE job_id = %d AND element_id = %d`
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	JobID, ElementID int64
	StartKey, EndKey []byte
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() ([]byte, []byte) {
	trace_util_0.Count(_util_00000, 0)
	return t.StartKey, t.EndKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	trace_util_0.Count(_util_00000, 1)
	return loadDeleteRangesFromTable(ctx, deleteRangesTable, safePoint)
}

// LoadDoneDeleteRanges loads deleted ranges from gc_delete_range_done table.
func LoadDoneDeleteRanges(ctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	trace_util_0.Count(_util_00000, 2)
	return loadDeleteRangesFromTable(ctx, doneDeleteRangesTable, safePoint)
}

func loadDeleteRangesFromTable(ctx sessionctx.Context, table string, safePoint uint64) (ranges []DelRangeTask, _ error) {
	trace_util_0.Count(_util_00000, 3)
	sql := fmt.Sprintf(loadDeleteRangeSQL, table, safePoint)
	rss, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rss) > 0 {
		trace_util_0.Count(_util_00000, 7)
		defer terror.Call(rss[0].Close)
	}
	trace_util_0.Count(_util_00000, 4)
	if err != nil {
		trace_util_0.Count(_util_00000, 8)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_util_00000, 5)
	rs := rss[0]
	req := rs.NewRecordBatch()
	it := chunk.NewIterator4Chunk(req.Chunk)
	for {
		trace_util_0.Count(_util_00000, 9)
		err = rs.Next(context.TODO(), req)
		if err != nil {
			trace_util_0.Count(_util_00000, 12)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_util_00000, 10)
		if req.NumRows() == 0 {
			trace_util_0.Count(_util_00000, 13)
			break
		}

		trace_util_0.Count(_util_00000, 11)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			trace_util_0.Count(_util_00000, 14)
			startKey, err := hex.DecodeString(row.GetString(2))
			if err != nil {
				trace_util_0.Count(_util_00000, 17)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_util_00000, 15)
			endKey, err := hex.DecodeString(row.GetString(3))
			if err != nil {
				trace_util_0.Count(_util_00000, 18)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_util_00000, 16)
			ranges = append(ranges, DelRangeTask{
				JobID:     row.GetInt64(0),
				ElementID: row.GetInt64(1),
				StartKey:  startKey,
				EndKey:    endKey,
			})
		}
	}
	trace_util_0.Count(_util_00000, 6)
	return ranges, nil
}

// CompleteDeleteRange moves a record from gc_delete_range table to gc_delete_range_done table.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx sessionctx.Context, dr DelRangeTask) error {
	trace_util_0.Count(_util_00000, 19)
	sql := fmt.Sprintf(recordDoneDeletedRangeSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if err != nil {
		trace_util_0.Count(_util_00000, 21)
		return errors.Trace(err)
	}

	trace_util_0.Count(_util_00000, 20)
	return RemoveFromGCDeleteRange(ctx, dr.JobID, dr.ElementID)
}

// RemoveFromGCDeleteRange is exported for ddl pkg to use.
func RemoveFromGCDeleteRange(ctx sessionctx.Context, jobID, elementID int64) error {
	trace_util_0.Count(_util_00000, 22)
	sql := fmt.Sprintf(completeDeleteRangeSQL, jobID, elementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	return errors.Trace(err)
}

// DeleteDoneRecord removes a record from gc_delete_range_done table.
func DeleteDoneRecord(ctx sessionctx.Context, dr DelRangeTask) error {
	trace_util_0.Count(_util_00000, 23)
	sql := fmt.Sprintf(deleteDoneRecordSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	return errors.Trace(err)
}

// UpdateDeleteRange is only for emulator.
func UpdateDeleteRange(ctx sessionctx.Context, dr DelRangeTask, newStartKey, oldStartKey kv.Key) error {
	trace_util_0.Count(_util_00000, 24)
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	sql := fmt.Sprintf(updateDeleteRangeSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	return errors.Trace(err)
}

// LoadDDLReorgVars loads ddl reorg variable from mysql.global_variables.
func LoadDDLReorgVars(ctx sessionctx.Context) error {
	trace_util_0.Count(_util_00000, 25)
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize})
}

// LoadDDLVars loads ddl variable from mysql.global_variables.
func LoadDDLVars(ctx sessionctx.Context) error {
	trace_util_0.Count(_util_00000, 26)
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLErrorCountLimit})
}

const loadGlobalVarsSQL = "select HIGH_PRIORITY variable_name, variable_value from mysql.global_variables where variable_name in (%s)"

// LoadGlobalVars loads global variable from mysql.global_variables.
func LoadGlobalVars(ctx sessionctx.Context, varNames []string) error {
	trace_util_0.Count(_util_00000, 27)
	if sctx, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		trace_util_0.Count(_util_00000, 29)
		nameList := ""
		for i, name := range varNames {
			trace_util_0.Count(_util_00000, 32)
			if i > 0 {
				trace_util_0.Count(_util_00000, 34)
				nameList += ", "
			}
			trace_util_0.Count(_util_00000, 33)
			nameList += fmt.Sprintf("'%s'", name)
		}
		trace_util_0.Count(_util_00000, 30)
		sql := fmt.Sprintf(loadGlobalVarsSQL, nameList)
		rows, _, err := sctx.ExecRestrictedSQL(ctx, sql)
		if err != nil {
			trace_util_0.Count(_util_00000, 35)
			return errors.Trace(err)
		}
		trace_util_0.Count(_util_00000, 31)
		for _, row := range rows {
			trace_util_0.Count(_util_00000, 36)
			varName := row.GetString(0)
			varValue := row.GetString(1)
			variable.SetLocalSystemVar(varName, varValue)
		}
	}
	trace_util_0.Count(_util_00000, 28)
	return nil
}

var _util_00000 = "ddl/util/util.go"
