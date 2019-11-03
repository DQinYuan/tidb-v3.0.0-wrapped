// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// adjustColumnInfoInAddColumn is used to set the correct position of column info when adding column.
// 1. The added column was append at the end of tblInfo.Columns, due to ddl state was not public then.
//    It should be moved to the correct position when the ddl state to be changed to public.
// 2. The offset of column should also to be set to the right value.
func adjustColumnInfoInAddColumn(tblInfo *model.TableInfo, offset int) {
	trace_util_0.Count(_column_00000, 0)
	oldCols := tblInfo.Columns
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[len(oldCols)-1])
	newCols = append(newCols, oldCols[offset:len(oldCols)-1]...)
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(newCols); i++ {
		trace_util_0.Count(_column_00000, 3)
		offsetChanged[newCols[i].Offset] = i
		newCols[i].Offset = i
	}
	trace_util_0.Count(_column_00000, 1)
	newCols[offset].Offset = offset
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_column_00000, 4)
		for _, col := range idx.Columns {
			trace_util_0.Count(_column_00000, 5)
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				trace_util_0.Count(_column_00000, 6)
				col.Offset = newOffset
			}
		}
	}
	trace_util_0.Count(_column_00000, 2)
	tblInfo.Columns = newCols
}

// adjustColumnInfoInDropColumn is used to set the correct position of column info when dropping column.
// 1. The offset of column should to be set to the last of the columns.
// 2. The dropped column is moved to the end of tblInfo.Columns, due to it was not public any more.
func adjustColumnInfoInDropColumn(tblInfo *model.TableInfo, offset int) {
	trace_util_0.Count(_column_00000, 7)
	oldCols := tblInfo.Columns
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(oldCols); i++ {
		trace_util_0.Count(_column_00000, 10)
		offsetChanged[oldCols[i].Offset] = i - 1
		oldCols[i].Offset = i - 1
	}
	trace_util_0.Count(_column_00000, 8)
	oldCols[offset].Offset = len(oldCols) - 1
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_column_00000, 11)
		for _, col := range idx.Columns {
			trace_util_0.Count(_column_00000, 12)
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				trace_util_0.Count(_column_00000, 13)
				col.Offset = newOffset
			}
		}
	}
	trace_util_0.Count(_column_00000, 9)
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[offset+1:]...)
	newCols = append(newCols, oldCols[offset])
	tblInfo.Columns = newCols
}

func createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ast.ColumnPosition) (*model.ColumnInfo, int, error) {
	trace_util_0.Count(_column_00000, 14)
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if pos.Tp == ast.ColumnPositionFirst {
		trace_util_0.Count(_column_00000, 16)
		position = 0
	} else {
		trace_util_0.Count(_column_00000, 17)
		if pos.Tp == ast.ColumnPositionAfter {
			trace_util_0.Count(_column_00000, 18)
			c := model.FindColumnInfo(cols, pos.RelativeColumn.Name.L)
			if c == nil {
				trace_util_0.Count(_column_00000, 20)
				return nil, 0, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
			}

			// Insert position is after the mentioned column.
			trace_util_0.Count(_column_00000, 19)
			position = c.Offset + 1
		}
	}
	trace_util_0.Count(_column_00000, 15)
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right position in "Columns" when it state change to public.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols...)
	newCols = append(newCols, colInfo)

	tblInfo.Columns = newCols
	return colInfo, position, nil
}

func checkAddColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, *model.ColumnInfo, *ast.ColumnPosition, int, error) {
	trace_util_0.Count(_column_00000, 21)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_column_00000, 25)
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 22)
	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		trace_util_0.Count(_column_00000, 26)
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 23)
	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		trace_util_0.Count(_column_00000, 27)
		if columnInfo.State == model.StatePublic {
			trace_util_0.Count(_column_00000, 28)
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			return nil, nil, nil, nil, 0, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
		}
	}
	trace_util_0.Count(_column_00000, 24)
	return tblInfo, columnInfo, col, pos, offset, nil
}

func onAddColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_column_00000, 29)
	// Handle the rolling back job.
	if job.IsRollingback() {
		trace_util_0.Count(_column_00000, 35)
		ver, err = onDropColumn(t, job)
		if err != nil {
			trace_util_0.Count(_column_00000, 37)
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_column_00000, 36)
		return ver, nil
	}

	trace_util_0.Count(_column_00000, 30)
	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		trace_util_0.Count(_column_00000, 38)
		if val.(bool) {
			trace_util_0.Count(_column_00000, 39)
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	trace_util_0.Count(_column_00000, 31)
	tblInfo, columnInfo, col, pos, offset, err := checkAddColumn(t, job)
	if err != nil {
		trace_util_0.Count(_column_00000, 40)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 32)
	if columnInfo == nil {
		trace_util_0.Count(_column_00000, 41)
		columnInfo, offset, err = createColumnInfo(tblInfo, col, pos)
		if err != nil {
			trace_util_0.Count(_column_00000, 44)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_column_00000, 42)
		logutil.Logger(ddlLogCtx).Info("[ddl] run add column job", zap.String("job", job.String()), zap.Reflect("columnInfo", *columnInfo), zap.Int("offset", offset))
		// Set offset arg to job.
		if offset != 0 {
			trace_util_0.Count(_column_00000, 45)
			job.Args = []interface{}{columnInfo, pos, offset}
		}
		trace_util_0.Count(_column_00000, 43)
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			trace_util_0.Count(_column_00000, 46)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	trace_util_0.Count(_column_00000, 33)
	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		trace_util_0.Count(_column_00000, 47)
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateDeleteOnly:
		trace_util_0.Count(_column_00000, 48)
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteOnly:
		trace_util_0.Count(_column_00000, 49)
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteReorganization:
		trace_util_0.Count(_column_00000, 50)
		// reorganization -> public
		// Adjust table column offset.
		adjustColumnInfoInAddColumn(tblInfo, offset)
		columnInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			trace_util_0.Count(_column_00000, 53)
			return ver, errors.Trace(err)
		}

		// Finish this job.
		trace_util_0.Count(_column_00000, 51)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfo: columnInfo})
	default:
		trace_util_0.Count(_column_00000, 52)
		err = ErrInvalidColumnState.GenWithStack("invalid column state %v", columnInfo.State)
	}

	trace_util_0.Count(_column_00000, 34)
	return ver, errors.Trace(err)
}

func onDropColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 54)
	tblInfo, colInfo, err := checkDropColumn(t, job)
	if err != nil {
		trace_util_0.Count(_column_00000, 57)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 55)
	originalState := colInfo.State
	switch colInfo.State {
	case model.StatePublic:
		trace_util_0.Count(_column_00000, 58)
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly
		// Set this column's offset to the last and reset all following columns' offsets.
		adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
		// When the dropping column has not-null flag and it hasn't the default value, we can backfill the column value like "add column".
		// NOTE: If the state of StateWriteOnly can be rollbacked, we'd better reconsider the original default value.
		// And we need consider the column without not-null flag.
		if colInfo.OriginDefaultValue == nil && mysql.HasNotNullFlag(colInfo.Flag) {
			trace_util_0.Count(_column_00000, 65)
			// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
			// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
			// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
			// But currently will be ok, because we can't cancel the drop column job when the job is running,
			// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
			// More info about this problem, see PR#9115.
			colInfo.OriginDefaultValue, err = generateOriginDefaultValue(colInfo)
			if err != nil {
				trace_util_0.Count(_column_00000, 66)
				return ver, errors.Trace(err)
			}
		}
		trace_util_0.Count(_column_00000, 59)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfo.State)
	case model.StateWriteOnly:
		trace_util_0.Count(_column_00000, 60)
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteOnly:
		trace_util_0.Count(_column_00000, 61)
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		colInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteReorganization:
		trace_util_0.Count(_column_00000, 62)
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			trace_util_0.Count(_column_00000, 67)
			return ver, errors.Trace(err)
		}

		// Finish this job.
		trace_util_0.Count(_column_00000, 63)
		if job.IsRollingback() {
			trace_util_0.Count(_column_00000, 68)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			trace_util_0.Count(_column_00000, 69)
			{
				job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			}
		}
	default:
		trace_util_0.Count(_column_00000, 64)
		err = ErrInvalidTableState.GenWithStack("invalid table state %v", tblInfo.State)
	}
	trace_util_0.Count(_column_00000, 56)
	return ver, errors.Trace(err)
}

func checkDropColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, error) {
	trace_util_0.Count(_column_00000, 70)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_column_00000, 75)
		return nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 71)
	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		trace_util_0.Count(_column_00000, 76)
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 72)
	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		trace_util_0.Count(_column_00000, 77)
		job.State = model.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	trace_util_0.Count(_column_00000, 73)
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		trace_util_0.Count(_column_00000, 78)
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 74)
	return tblInfo, colInfo, nil
}

func onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 79)
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		trace_util_0.Count(_column_00000, 81)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 80)
	return updateColumn(t, job, newCol, &newCol.Name)
}

func (w *worker) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 82)
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	pos := &ast.ColumnPosition{}
	var modifyColumnTp byte
	err := job.DecodeArgs(newCol, oldColName, pos, &modifyColumnTp)
	if err != nil {
		trace_util_0.Count(_column_00000, 84)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 83)
	return w.doModifyColumn(t, job, newCol, oldColName, pos, modifyColumnTp)
}

// doModifyColumn updates the column information and reorders all columns.
func (w *worker) doModifyColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr, pos *ast.ColumnPosition, modifyColumnTp byte) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 85)
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		trace_util_0.Count(_column_00000, 97)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 86)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_column_00000, 98)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 87)
	oldCol := model.FindColumnInfo(tblInfo.Columns, oldName.L)
	if job.IsRollingback() {
		trace_util_0.Count(_column_00000, 99)
		ver, err = rollbackModifyColumnJob(t, tblInfo, job, oldCol, modifyColumnTp)
		if err != nil {
			trace_util_0.Count(_column_00000, 101)
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_column_00000, 100)
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		return ver, nil
	}

	trace_util_0.Count(_column_00000, 88)
	if oldCol == nil || oldCol.State != model.StatePublic {
		trace_util_0.Count(_column_00000, 102)
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
	}
	// If we want to rename the column name, we need to check whether it already exists.
	trace_util_0.Count(_column_00000, 89)
	if newCol.Name.L != oldName.L {
		trace_util_0.Count(_column_00000, 103)
		c := model.FindColumnInfo(tblInfo.Columns, newCol.Name.L)
		if c != nil {
			trace_util_0.Count(_column_00000, 104)
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnExists.GenWithStackByArgs(newCol.Name)
		}
	}

	trace_util_0.Count(_column_00000, 90)
	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		trace_util_0.Count(_column_00000, 105)
		if val.(bool) {
			trace_util_0.Count(_column_00000, 106)
			if newCol.State != model.StatePublic {
				trace_util_0.Count(_column_00000, 107)
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	trace_util_0.Count(_column_00000, 91)
	if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(newCol.Flag) && !mysql.HasPreventNullInsertFlag(oldCol.Flag) {
		trace_util_0.Count(_column_00000, 108)
		// Introduce the `mysql.HasPreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		ver, err = modifyColumnFromNull2NotNull(w, t, dbInfo, tblInfo, job, oldCol, newCol)
		return ver, errors.Trace(err)
	}

	// We need the latest column's offset and state. This information can be obtained from the store.
	trace_util_0.Count(_column_00000, 92)
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset
	if pos.Tp == ast.ColumnPositionAfter {
		trace_util_0.Count(_column_00000, 109)
		if oldName.L == pos.RelativeColumn.Name.L {
			trace_util_0.Count(_column_00000, 112)
			// `alter table tableName modify column b int after b` will return ver,ErrColumnNotExists.
			// Modified the type definition of 'null' to 'not null' before this, so rollback the job when an error occurs.
			job.State = model.JobStateRollingback
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
		}

		trace_util_0.Count(_column_00000, 110)
		relative := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if relative == nil || relative.State != model.StatePublic {
			trace_util_0.Count(_column_00000, 113)
			job.State = model.JobStateRollingback
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		trace_util_0.Count(_column_00000, 111)
		if relative.Offset < oldPos {
			trace_util_0.Count(_column_00000, 114)
			newPos = relative.Offset + 1
		} else {
			trace_util_0.Count(_column_00000, 115)
			{
				newPos = relative.Offset
			}
		}
	} else {
		trace_util_0.Count(_column_00000, 116)
		if pos.Tp == ast.ColumnPositionFirst {
			trace_util_0.Count(_column_00000, 117)
			newPos = 0
		}
	}

	trace_util_0.Count(_column_00000, 93)
	columnChanged := make(map[string]*model.ColumnInfo)
	columnChanged[oldName.L] = newCol

	if newPos == oldPos {
		trace_util_0.Count(_column_00000, 118)
		tblInfo.Columns[newPos] = newCol
	} else {
		trace_util_0.Count(_column_00000, 119)
		{
			cols := tblInfo.Columns

			// Reorder columns in place.
			if newPos < oldPos {
				trace_util_0.Count(_column_00000, 121)
				copy(cols[newPos+1:], cols[newPos:oldPos])
			} else {
				trace_util_0.Count(_column_00000, 122)
				{
					copy(cols[oldPos:], cols[oldPos+1:newPos+1])
				}
			}
			trace_util_0.Count(_column_00000, 120)
			cols[newPos] = newCol

			for i, col := range tblInfo.Columns {
				trace_util_0.Count(_column_00000, 123)
				if col.Offset != i {
					trace_util_0.Count(_column_00000, 124)
					columnChanged[col.Name.L] = col
					col.Offset = i
				}
			}
		}
	}

	// Change offset and name in indices.
	trace_util_0.Count(_column_00000, 94)
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_column_00000, 125)
		for _, c := range idx.Columns {
			trace_util_0.Count(_column_00000, 126)
			if newCol, ok := columnChanged[c.Name.L]; ok {
				trace_util_0.Count(_column_00000, 127)
				c.Name = newCol.Name
				c.Offset = newCol.Offset
			}
		}
	}

	trace_util_0.Count(_column_00000, 95)
	ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_column_00000, 128)
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 96)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

// checkForNullValue ensure there are no null values of the column of this table.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with mysql.
func checkForNullValue(ctx sessionctx.Context, isDataTruncated bool, schema, table, oldCol, newCol model.CIStr) error {
	trace_util_0.Count(_column_00000, 129)
	sql := fmt.Sprintf("select count(*) from `%s`.`%s` where `%s` is null limit 1;", schema.L, table.L, oldCol.L)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_column_00000, 132)
		return errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 130)
	rowCount := rows[0].GetInt64(0)
	if rowCount != 0 {
		trace_util_0.Count(_column_00000, 133)
		if isDataTruncated {
			trace_util_0.Count(_column_00000, 135)
			return errInvalidUseOfNull
		}
		trace_util_0.Count(_column_00000, 134)
		return ErrWarnDataTruncated.GenWithStackByArgs(newCol.L, rowCount)
	}
	trace_util_0.Count(_column_00000, 131)
	return nil
}

func updateColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 136)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_column_00000, 140)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 137)
	oldCol := model.FindColumnInfo(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		trace_util_0.Count(_column_00000, 141)
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newCol.Name, tblInfo.Name)
	}
	trace_util_0.Count(_column_00000, 138)
	*oldCol = *newCol

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_column_00000, 142)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_column_00000, 139)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func isColumnWithIndex(colName string, indices []*model.IndexInfo) bool {
	trace_util_0.Count(_column_00000, 143)
	for _, indexInfo := range indices {
		trace_util_0.Count(_column_00000, 145)
		for _, col := range indexInfo.Columns {
			trace_util_0.Count(_column_00000, 146)
			if col.Name.L == colName {
				trace_util_0.Count(_column_00000, 147)
				return true
			}
		}
	}
	trace_util_0.Count(_column_00000, 144)
	return false
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	trace_util_0.Count(_column_00000, 148)
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

func checkAddColumnTooManyColumns(colNum int) error {
	trace_util_0.Count(_column_00000, 149)
	if uint32(colNum) > atomic.LoadUint32(&TableColumnCountLimit) {
		trace_util_0.Count(_column_00000, 151)
		return errTooManyFields
	}
	trace_util_0.Count(_column_00000, 150)
	return nil
}

// rollbackModifyColumnJob rollbacks the job when an error occurs.
func rollbackModifyColumnJob(t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, modifyColumnTp byte) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 152)
	var err error
	if modifyColumnTp == mysql.TypeNull {
		trace_util_0.Count(_column_00000, 154)
		// field NotNullFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.NotNullFlag
		// field PreventNullInsertFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.PreventNullInsertFlag
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			trace_util_0.Count(_column_00000, 155)
			return ver, errors.Trace(err)
		}
	}
	trace_util_0.Count(_column_00000, 153)
	return ver, nil
}

// modifyColumnFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
func modifyColumnFromNull2NotNull(w *worker, t *meta.Meta, dbInfo *model.DBInfo, tblInfo *model.TableInfo, job *model.Job, oldCol, newCol *model.ColumnInfo) (ver int64, _ error) {
	trace_util_0.Count(_column_00000, 156)
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_column_00000, 159)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_column_00000, 157)
	defer w.sessPool.put(ctx)

	// If there is a null value inserted, it cannot be modified and needs to be rollback.
	err = checkForNullValue(ctx, oldCol.Tp == newCol.Tp, dbInfo.Name, tblInfo.Name, oldCol.Name, newCol.Name)
	if err != nil {
		trace_util_0.Count(_column_00000, 160)
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	// Prevent this field from inserting null values.
	trace_util_0.Count(_column_00000, 158)
	tblInfo.Columns[oldCol.Offset].Flag |= mysql.PreventNullInsertFlag
	ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
	return ver, errors.Trace(err)
}

func generateOriginDefaultValue(col *model.ColumnInfo) (interface{}, error) {
	trace_util_0.Count(_column_00000, 161)
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 164)
		zeroVal := table.GetZeroValue(col)
		odValue, err = zeroVal.ToString()
		if err != nil {
			trace_util_0.Count(_column_00000, 165)
			return nil, errors.Trace(err)
		}
	}

	trace_util_0.Count(_column_00000, 162)
	if odValue == strings.ToUpper(ast.CurrentTimestamp) {
		trace_util_0.Count(_column_00000, 166)
		if col.Tp == mysql.TypeTimestamp {
			trace_util_0.Count(_column_00000, 167)
			odValue = time.Now().UTC().Format(types.TimeFormat)
		} else {
			trace_util_0.Count(_column_00000, 168)
			if col.Tp == mysql.TypeDatetime {
				trace_util_0.Count(_column_00000, 169)
				odValue = time.Now().Format(types.TimeFormat)
			}
		}
	}
	trace_util_0.Count(_column_00000, 163)
	return odValue, nil
}

func findColumnInIndexCols(c *expression.Column, cols []*ast.IndexColName) bool {
	trace_util_0.Count(_column_00000, 170)
	for _, c1 := range cols {
		trace_util_0.Count(_column_00000, 172)
		if c.ColName.L == c1.Column.Name.L {
			trace_util_0.Count(_column_00000, 173)
			return true
		}
	}
	trace_util_0.Count(_column_00000, 171)
	return false
}

func getColumnInfoByName(tbInfo *model.TableInfo, column string) *model.ColumnInfo {
	trace_util_0.Count(_column_00000, 174)
	for _, colInfo := range tbInfo.Cols() {
		trace_util_0.Count(_column_00000, 176)
		if colInfo.Name.L == column {
			trace_util_0.Count(_column_00000, 177)
			return colInfo
		}
	}
	trace_util_0.Count(_column_00000, 175)
	return nil
}

var _column_00000 = "ddl/column.go"
