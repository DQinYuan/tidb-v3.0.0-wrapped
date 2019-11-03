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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func convertAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (int64, error) {
	trace_util_0.Count(_rollingback_00000, 0)
	job.State = model.JobStateRollingback
	// the second args will be used in onDropIndex.
	job.Args = []interface{}{indexInfo.Name, getPartitionIDs(tblInfo)}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	originalState := indexInfo.State
	indexInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err1 != nil {
		trace_util_0.Count(_rollingback_00000, 3)
		return ver, errors.Trace(err1)
	}

	trace_util_0.Count(_rollingback_00000, 1)
	if kv.ErrKeyExists.Equal(err) {
		trace_util_0.Count(_rollingback_00000, 4)
		return ver, kv.ErrKeyExists.GenWithStack("Duplicate for key %s", indexInfo.Name.O)
	}

	trace_util_0.Count(_rollingback_00000, 2)
	return ver, errors.Trace(err)
}

// convertNotStartAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotStartAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, occuredErr error) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 5)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 9)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_rollingback_00000, 6)
	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*ast.IndexColName
		indexOption *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames, &indexOption)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 10)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_rollingback_00000, 7)
	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		trace_util_0.Count(_rollingback_00000, 11)
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	trace_util_0.Count(_rollingback_00000, 8)
	return convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, occuredErr)
}

func rollingbackAddColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 12)
	job.State = model.JobStateRollingback
	tblInfo, columnInfo, col, _, _, err := checkAddColumn(t, job)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 16)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 13)
	if columnInfo == nil {
		trace_util_0.Count(_rollingback_00000, 17)
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	trace_util_0.Count(_rollingback_00000, 14)
	originalState := columnInfo.State
	columnInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly

	job.Args = []interface{}{col.Name}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 18)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 15)
	return ver, errCancelledDDLJob
}

func rollingbackDropColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 19)
	tblInfo, colInfo, err := checkDropColumn(t, job)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 22)
		return ver, errors.Trace(err)
	}

	// StatePublic means when the job is not running yet.
	trace_util_0.Count(_rollingback_00000, 20)
	if colInfo.State == model.StatePublic {
		trace_util_0.Count(_rollingback_00000, 23)
		job.State = model.JobStateCancelled
		job.FinishTableJob(model.JobStateRollbackDone, model.StatePublic, ver, tblInfo)
		return ver, errCancelledDDLJob
	}
	// In the state of drop column `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop column.
	trace_util_0.Count(_rollingback_00000, 21)
	job.State = model.JobStateRunning
	return ver, nil
}

func rollingbackDropIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 24)
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 28)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_rollingback_00000, 25)
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
		trace_util_0.Count(_rollingback_00000, 29)
		// We can not rollback now, so just continue to drop index.
		// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
		job.State = model.JobStateRunning
		return ver, nil
	case model.StatePublic, model.StateWriteOnly:
		trace_util_0.Count(_rollingback_00000, 30)
		job.State = model.JobStateRollbackDone
		indexInfo.State = model.StatePublic
	default:
		trace_util_0.Count(_rollingback_00000, 31)
		return ver, ErrInvalidIndexState.GenWithStack("invalid index state %v", indexInfo.State)
	}

	trace_util_0.Count(_rollingback_00000, 26)
	job.SchemaState = indexInfo.State
	job.Args = []interface{}{indexInfo.Name}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 32)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 27)
	job.FinishTableJob(model.JobStateRollbackDone, model.StatePublic, ver, tblInfo)
	return ver, errCancelledDDLJob
}

func rollingbackAddindex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 33)
	// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
	if job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
		trace_util_0.Count(_rollingback_00000, 35)
		// add index workers are started. need to ask them to exit.
		logutil.Logger(w.logCtx).Info("[ddl] run the cancelling DDL job", zap.String("job", job.String()))
		w.reorgCtx.notifyReorgCancel()
		ver, err = w.onCreateIndex(d, t, job)
	} else {
		trace_util_0.Count(_rollingback_00000, 36)
		{
			// add index workers are not started, remove the indexInfo in tableInfo.
			ver, err = convertNotStartAddIdxJob2RollbackJob(t, job, errCancelledDDLJob)
		}
	}
	trace_util_0.Count(_rollingback_00000, 34)
	return
}

func rollingbackAddTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 37)
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 39)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 38)
	return cancelOnlyNotHandledJob(job)
}

func rollingbackDropTableOrView(t *meta.Meta, job *model.Job) error {
	trace_util_0.Count(_rollingback_00000, 40)
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 43)
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
	trace_util_0.Count(_rollingback_00000, 41)
	if tblInfo.State == model.StatePublic {
		trace_util_0.Count(_rollingback_00000, 44)
		job.State = model.JobStateCancelled
		return errCancelledDDLJob
	}
	trace_util_0.Count(_rollingback_00000, 42)
	job.State = model.JobStateRunning
	return nil
}

func rollingbackDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 45)
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 47)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 46)
	return cancelOnlyNotHandledJob(job)
}

func rollingbackDropSchema(t *meta.Meta, job *model.Job) error {
	trace_util_0.Count(_rollingback_00000, 48)
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 51)
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
	trace_util_0.Count(_rollingback_00000, 49)
	if dbInfo.State == model.StatePublic {
		trace_util_0.Count(_rollingback_00000, 52)
		job.State = model.JobStateCancelled
		return errCancelledDDLJob
	}
	trace_util_0.Count(_rollingback_00000, 50)
	job.State = model.JobStateRunning
	return nil
}

func rollingbackRenameIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 53)
	tblInfo, from, _, err := checkRenameIndex(t, job)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 56)
		return ver, errors.Trace(err)
	}
	// Here rename index is done in a transaction, if the job is not completed, it can be canceled.
	trace_util_0.Count(_rollingback_00000, 54)
	idx := tblInfo.FindIndexByName(from.L)
	if idx.State == model.StatePublic {
		trace_util_0.Count(_rollingback_00000, 57)
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	trace_util_0.Count(_rollingback_00000, 55)
	job.State = model.JobStateRunning
	return ver, errors.Trace(err)
}

func cancelOnlyNotHandledJob(job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 58)
	// We can only cancel the not handled job.
	if job.SchemaState == model.StateNone {
		trace_util_0.Count(_rollingback_00000, 60)
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	trace_util_0.Count(_rollingback_00000, 59)
	job.State = model.JobStateRunning

	return ver, nil
}

func rollingbackTruncateTable(t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 61)
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 63)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_rollingback_00000, 62)
	return cancelOnlyNotHandledJob(job)
}

func convertJob2RollbackJob(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_rollingback_00000, 64)
	switch job.Type {
	case model.ActionAddColumn:
		trace_util_0.Count(_rollingback_00000, 67)
		ver, err = rollingbackAddColumn(t, job)
	case model.ActionAddIndex:
		trace_util_0.Count(_rollingback_00000, 68)
		ver, err = rollingbackAddindex(w, d, t, job)
	case model.ActionAddTablePartition:
		trace_util_0.Count(_rollingback_00000, 69)
		ver, err = rollingbackAddTablePartition(t, job)
	case model.ActionDropColumn:
		trace_util_0.Count(_rollingback_00000, 70)
		ver, err = rollingbackDropColumn(t, job)
	case model.ActionDropIndex:
		trace_util_0.Count(_rollingback_00000, 71)
		ver, err = rollingbackDropIndex(t, job)
	case model.ActionDropTable, model.ActionDropView:
		trace_util_0.Count(_rollingback_00000, 72)
		err = rollingbackDropTableOrView(t, job)
	case model.ActionDropTablePartition:
		trace_util_0.Count(_rollingback_00000, 73)
		ver, err = rollingbackDropTablePartition(t, job)
	case model.ActionDropSchema:
		trace_util_0.Count(_rollingback_00000, 74)
		err = rollingbackDropSchema(t, job)
	case model.ActionRenameIndex:
		trace_util_0.Count(_rollingback_00000, 75)
		ver, err = rollingbackRenameIndex(t, job)
	case model.ActionTruncateTable:
		trace_util_0.Count(_rollingback_00000, 76)
		ver, err = rollingbackTruncateTable(t, job)
	case model.ActionRebaseAutoID, model.ActionShardRowID,
		model.ActionModifyColumn, model.ActionAddForeignKey,
		model.ActionDropForeignKey, model.ActionRenameTable,
		model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
		model.ActionModifySchemaCharsetAndCollate:
		trace_util_0.Count(_rollingback_00000, 77)
		ver, err = cancelOnlyNotHandledJob(job)
	default:
		trace_util_0.Count(_rollingback_00000, 78)
		job.State = model.JobStateCancelled
		err = errCancelledDDLJob
	}

	trace_util_0.Count(_rollingback_00000, 65)
	if err != nil {
		trace_util_0.Count(_rollingback_00000, 79)
		if job.State != model.JobStateRollingback && job.State != model.JobStateCancelled {
			trace_util_0.Count(_rollingback_00000, 81)
			logutil.Logger(w.logCtx).Error("[ddl] run DDL job failed", zap.String("job", job.String()), zap.Error(err))
		} else {
			trace_util_0.Count(_rollingback_00000, 82)
			{
				logutil.Logger(w.logCtx).Info("[ddl] the DDL job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
			}
		}

		trace_util_0.Count(_rollingback_00000, 80)
		job.Error = toTError(err)
		job.ErrorCount++
	}
	trace_util_0.Count(_rollingback_00000, 66)
	return
}

var _rollingback_00000 = "ddl/rollingback.go"
