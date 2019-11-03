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

package admin

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// DDLInfo is for DDL information.
type DDLInfo struct {
	SchemaVer   int64
	ReorgHandle int64        // It's only used for DDL information.
	Jobs        []*model.Job // It's the currently running jobs.
}

// GetDDLInfo returns DDL information.
func GetDDLInfo(txn kv.Transaction) (*DDLInfo, error) {
	trace_util_0.Count(_admin_00000, 0)
	var err error
	info := &DDLInfo{}
	t := meta.NewMeta(txn)

	info.Jobs = make([]*model.Job, 0, 2)
	job, err := t.GetDDLJobByIdx(0)
	if err != nil {
		trace_util_0.Count(_admin_00000, 8)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 1)
	if job != nil {
		trace_util_0.Count(_admin_00000, 9)
		info.Jobs = append(info.Jobs, job)
	}
	trace_util_0.Count(_admin_00000, 2)
	addIdxJob, err := t.GetDDLJobByIdx(0, meta.AddIndexJobListKey)
	if err != nil {
		trace_util_0.Count(_admin_00000, 10)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 3)
	if addIdxJob != nil {
		trace_util_0.Count(_admin_00000, 11)
		info.Jobs = append(info.Jobs, addIdxJob)
	}

	trace_util_0.Count(_admin_00000, 4)
	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		trace_util_0.Count(_admin_00000, 12)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 5)
	if addIdxJob == nil {
		trace_util_0.Count(_admin_00000, 13)
		return info, nil
	}

	trace_util_0.Count(_admin_00000, 6)
	info.ReorgHandle, _, _, err = t.GetDDLReorgHandle(addIdxJob)
	if err != nil {
		trace_util_0.Count(_admin_00000, 14)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 7)
	return info, nil
}

// IsJobRollbackable checks whether the job can be rollback.
func IsJobRollbackable(job *model.Job) bool {
	trace_util_0.Count(_admin_00000, 15)
	switch job.Type {
	case model.ActionDropIndex:
		trace_util_0.Count(_admin_00000, 17)
		// We can't cancel if index current state is in StateDeleteOnly or StateDeleteReorganization, otherwise will cause inconsistent between record and index.
		if job.SchemaState == model.StateDeleteOnly ||
			job.SchemaState == model.StateDeleteReorganization {
			trace_util_0.Count(_admin_00000, 20)
			return false
		}
	case model.ActionDropSchema, model.ActionDropTable:
		trace_util_0.Count(_admin_00000, 18)
		// To simplify the rollback logic, cannot be canceled in the following states.
		if job.SchemaState == model.StateWriteOnly ||
			job.SchemaState == model.StateDeleteOnly {
			trace_util_0.Count(_admin_00000, 21)
			return false
		}
	case model.ActionDropColumn, model.ActionModifyColumn,
		model.ActionDropTablePartition, model.ActionAddTablePartition,
		model.ActionRebaseAutoID, model.ActionShardRowID,
		model.ActionTruncateTable, model.ActionAddForeignKey,
		model.ActionDropForeignKey, model.ActionRenameTable,
		model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
		model.ActionModifySchemaCharsetAndCollate:
		trace_util_0.Count(_admin_00000, 19)
		return job.SchemaState == model.StateNone
	}
	trace_util_0.Count(_admin_00000, 16)
	return true
}

// CancelJobs cancels the DDL jobs.
func CancelJobs(txn kv.Transaction, ids []int64) ([]error, error) {
	trace_util_0.Count(_admin_00000, 22)
	if len(ids) == 0 {
		trace_util_0.Count(_admin_00000, 26)
		return nil, nil
	}

	trace_util_0.Count(_admin_00000, 23)
	jobs, err := GetDDLJobs(txn)
	if err != nil {
		trace_util_0.Count(_admin_00000, 27)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 24)
	errs := make([]error, len(ids))
	t := meta.NewMeta(txn)
	for i, id := range ids {
		trace_util_0.Count(_admin_00000, 28)
		found := false
		for j, job := range jobs {
			trace_util_0.Count(_admin_00000, 30)
			if id != job.ID {
				trace_util_0.Count(_admin_00000, 37)
				logutil.Logger(context.Background()).Debug("the job that needs to be canceled isn't equal to current job",
					zap.Int64("need to canceled job ID", id),
					zap.Int64("current job ID", job.ID))
				continue
			}
			trace_util_0.Count(_admin_00000, 31)
			found = true
			// These states can't be cancelled.
			if job.IsDone() || job.IsSynced() {
				trace_util_0.Count(_admin_00000, 38)
				errs[i] = ErrCancelFinishedDDLJob.GenWithStackByArgs(id)
				continue
			}
			// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
			trace_util_0.Count(_admin_00000, 32)
			if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
				trace_util_0.Count(_admin_00000, 39)
				continue
			}
			trace_util_0.Count(_admin_00000, 33)
			if !IsJobRollbackable(job) {
				trace_util_0.Count(_admin_00000, 40)
				errs[i] = ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
				continue
			}

			trace_util_0.Count(_admin_00000, 34)
			job.State = model.JobStateCancelling
			// Make sure RawArgs isn't overwritten.
			err := job.DecodeArgs(job.RawArgs)
			if err != nil {
				trace_util_0.Count(_admin_00000, 41)
				errs[i] = errors.Trace(err)
				continue
			}
			trace_util_0.Count(_admin_00000, 35)
			if job.Type == model.ActionAddIndex {
				trace_util_0.Count(_admin_00000, 42)
				err = t.UpdateDDLJob(int64(j), job, true, meta.AddIndexJobListKey)
			} else {
				trace_util_0.Count(_admin_00000, 43)
				{
					err = t.UpdateDDLJob(int64(j), job, true)
				}
			}
			trace_util_0.Count(_admin_00000, 36)
			if err != nil {
				trace_util_0.Count(_admin_00000, 44)
				errs[i] = errors.Trace(err)
			}
		}
		trace_util_0.Count(_admin_00000, 29)
		if !found {
			trace_util_0.Count(_admin_00000, 45)
			errs[i] = ErrDDLJobNotFound.GenWithStackByArgs(id)
		}
	}
	trace_util_0.Count(_admin_00000, 25)
	return errs, nil
}

func getDDLJobsInQueue(t *meta.Meta, jobListKey meta.JobListKeyType) ([]*model.Job, error) {
	trace_util_0.Count(_admin_00000, 46)
	cnt, err := t.DDLJobQueueLen(jobListKey)
	if err != nil {
		trace_util_0.Count(_admin_00000, 49)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 47)
	jobs := make([]*model.Job, cnt)
	for i := range jobs {
		trace_util_0.Count(_admin_00000, 50)
		jobs[i], err = t.GetDDLJobByIdx(int64(i), jobListKey)
		if err != nil {
			trace_util_0.Count(_admin_00000, 51)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_admin_00000, 48)
	return jobs, nil
}

// GetDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetDDLJobs(txn kv.Transaction) ([]*model.Job, error) {
	trace_util_0.Count(_admin_00000, 52)
	t := meta.NewMeta(txn)
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		trace_util_0.Count(_admin_00000, 55)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 53)
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		trace_util_0.Count(_admin_00000, 56)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 54)
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

type jobArray []*model.Job

func (v jobArray) Len() int {
	trace_util_0.Count(_admin_00000, 57)
	return len(v)
}

func (v jobArray) Less(i, j int) bool {
	trace_util_0.Count(_admin_00000, 58)
	return v[i].ID < v[j].ID
}

func (v jobArray) Swap(i, j int) {
	trace_util_0.Count(_admin_00000, 59)
	v[i], v[j] = v[j], v[i]
}

// MaxHistoryJobs is exported for testing.
const MaxHistoryJobs = 10

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

// GetHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetHistoryDDLJobs(txn kv.Transaction, maxNumJobs int) ([]*model.Job, error) {
	trace_util_0.Count(_admin_00000, 60)
	t := meta.NewMeta(txn)
	jobs, err := t.GetLastNHistoryDDLJobs(maxNumJobs)
	if err != nil {
		trace_util_0.Count(_admin_00000, 64)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 61)
	jobsLen := len(jobs)
	if jobsLen > maxNumJobs {
		trace_util_0.Count(_admin_00000, 65)
		start := jobsLen - maxNumJobs
		jobs = jobs[start:]
	}
	trace_util_0.Count(_admin_00000, 62)
	jobsLen = len(jobs)
	ret := make([]*model.Job, 0, jobsLen)
	for i := jobsLen - 1; i >= 0; i-- {
		trace_util_0.Count(_admin_00000, 66)
		ret = append(ret, jobs[i])
	}
	trace_util_0.Count(_admin_00000, 63)
	return ret, nil
}

func nextIndexVals(data []types.Datum) []types.Datum {
	trace_util_0.Count(_admin_00000, 67)
	// Add 0x0 to the end of data.
	return append(data, types.Datum{})
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle int64
	Values []types.Datum
}

func getCount(ctx sessionctx.Context, sql string) (int64, error) {
	trace_util_0.Count(_admin_00000, 68)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(ctx, sql)
	if err != nil {
		trace_util_0.Count(_admin_00000, 71)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 69)
	if len(rows) != 1 {
		trace_util_0.Count(_admin_00000, 72)
		return 0, errors.Errorf("can not get count, sql %s result rows %d", sql, len(rows))
	}
	trace_util_0.Count(_admin_00000, 70)
	return rows[0].GetInt64(0), nil
}

// CheckIndicesCount compares indices count with table count.
// It returns nil if the count from the index is equal to the count from the table columns,
// otherwise it returns an error with a different information.
func CheckIndicesCount(ctx sessionctx.Context, dbName, tableName string, indices []string) error {
	trace_util_0.Count(_admin_00000, 73)
	// Add `` for some names like `table name`.
	sql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)
	tblCnt, err := getCount(ctx, sql)
	if err != nil {
		trace_util_0.Count(_admin_00000, 76)
		return errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 74)
	for _, idx := range indices {
		trace_util_0.Count(_admin_00000, 77)
		sql = fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX(`%s`)", dbName, tableName, idx)
		idxCnt, err := getCount(ctx, sql)
		if err != nil {
			trace_util_0.Count(_admin_00000, 79)
			return errors.Trace(err)
		}
		trace_util_0.Count(_admin_00000, 78)
		if tblCnt != idxCnt {
			trace_util_0.Count(_admin_00000, 80)
			return errors.Errorf("table count %d != index(%s) count %d", tblCnt, idx, idxCnt)
		}
	}

	trace_util_0.Count(_admin_00000, 75)
	return nil
}

// ScanIndexData scans the index handles and values in a limited number, according to the index information.
// It returns data and the next startVals until it doesn't have data, then returns data is nil and
// the next startVals is the values which can't get data. If startVals = nil and limit = -1,
// it returns the index data of the whole.
func ScanIndexData(sc *stmtctx.StatementContext, txn kv.Transaction, kvIndex table.Index, startVals []types.Datum, limit int64) (
	[]*RecordData, []types.Datum, error) {
	trace_util_0.Count(_admin_00000, 81)
	it, _, err := kvIndex.Seek(sc, txn, startVals)
	if err != nil {
		trace_util_0.Count(_admin_00000, 85)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 82)
	defer it.Close()

	var idxRows []*RecordData
	var curVals []types.Datum
	for limit != 0 {
		trace_util_0.Count(_admin_00000, 86)
		val, h, err1 := it.Next()
		if terror.ErrorEqual(err1, io.EOF) {
			trace_util_0.Count(_admin_00000, 88)
			return idxRows, nextIndexVals(curVals), nil
		} else {
			trace_util_0.Count(_admin_00000, 89)
			if err1 != nil {
				trace_util_0.Count(_admin_00000, 90)
				return nil, nil, errors.Trace(err1)
			}
		}
		trace_util_0.Count(_admin_00000, 87)
		idxRows = append(idxRows, &RecordData{Handle: h, Values: val})
		limit--
		curVals = val
	}

	trace_util_0.Count(_admin_00000, 83)
	nextVals, _, err := it.Next()
	if terror.ErrorEqual(err, io.EOF) {
		trace_util_0.Count(_admin_00000, 91)
		return idxRows, nextIndexVals(curVals), nil
	} else {
		trace_util_0.Count(_admin_00000, 92)
		if err != nil {
			trace_util_0.Count(_admin_00000, 93)
			return nil, nil, errors.Trace(err)
		}
	}

	trace_util_0.Count(_admin_00000, 84)
	return idxRows, nextVals, nil
}

// CompareIndexData compares index data one by one.
// It returns nil if the data from the index is equal to the data from the table columns,
// otherwise it returns an error with a different set of records.
// genExprs is use to calculate the virtual generate column.
func CompareIndexData(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index, genExprs map[model.TableColumnID]expression.Expression) error {
	trace_util_0.Count(_admin_00000, 94)
	err := checkIndexAndRecord(sessCtx, txn, t, idx, genExprs)
	if err != nil {
		trace_util_0.Count(_admin_00000, 96)
		return errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 95)
	return CheckRecordAndIndex(sessCtx, txn, t, idx, genExprs)
}

func getIndexFieldTypes(t table.Table, idx table.Index) ([]*types.FieldType, error) {
	trace_util_0.Count(_admin_00000, 97)
	idxColumns := idx.Meta().Columns
	tblColumns := t.Meta().Columns
	fieldTypes := make([]*types.FieldType, 0, len(idxColumns))
	for _, col := range idxColumns {
		trace_util_0.Count(_admin_00000, 99)
		colInfo := model.FindColumnInfo(tblColumns, col.Name.L)
		if colInfo == nil {
			trace_util_0.Count(_admin_00000, 101)
			return nil, errors.Errorf("index col:%v not found in table:%v", col.Name.String(), t.Meta().Name.String())
		}

		trace_util_0.Count(_admin_00000, 100)
		fieldTypes = append(fieldTypes, &colInfo.FieldType)
	}
	trace_util_0.Count(_admin_00000, 98)
	return fieldTypes, nil
}

// adjustDatumKind treats KindString as KindBytes.
func adjustDatumKind(vals1, vals2 []types.Datum) {
	trace_util_0.Count(_admin_00000, 102)
	if len(vals1) != len(vals2) {
		trace_util_0.Count(_admin_00000, 104)
		return
	}

	trace_util_0.Count(_admin_00000, 103)
	for i, val1 := range vals1 {
		trace_util_0.Count(_admin_00000, 105)
		val2 := vals2[i]
		if val1.Kind() != val2.Kind() {
			trace_util_0.Count(_admin_00000, 106)
			if (val1.Kind() == types.KindBytes || val1.Kind() == types.KindString) &&
				(val2.Kind() == types.KindBytes || val2.Kind() == types.KindString) {
				trace_util_0.Count(_admin_00000, 107)
				vals1[i].SetBytes(val1.GetBytes())
				vals2[i].SetBytes(val2.GetBytes())
			}
		}
	}
}

func checkIndexAndRecord(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index, genExprs map[model.TableColumnID]expression.Expression) error {
	trace_util_0.Count(_admin_00000, 108)
	it, err := idx.SeekFirst(txn)
	if err != nil {
		trace_util_0.Count(_admin_00000, 113)
		return errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 109)
	defer it.Close()

	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		trace_util_0.Count(_admin_00000, 114)
		cols[i] = t.Cols()[col.Offset]
	}

	trace_util_0.Count(_admin_00000, 110)
	fieldTypes, err := getIndexFieldTypes(t, idx)
	if err != nil {
		trace_util_0.Count(_admin_00000, 115)
		return errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 111)
	rowDecoder := makeRowDecoder(t, cols, genExprs)
	sc := sessCtx.GetSessionVars().StmtCtx
	for {
		trace_util_0.Count(_admin_00000, 116)
		vals1, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			trace_util_0.Count(_admin_00000, 121)
			break
		} else {
			trace_util_0.Count(_admin_00000, 122)
			if err != nil {
				trace_util_0.Count(_admin_00000, 123)
				return errors.Trace(err)
			}
		}

		trace_util_0.Count(_admin_00000, 117)
		vals1, err = tablecodec.UnflattenDatums(vals1, fieldTypes, sessCtx.GetSessionVars().Location())
		if err != nil {
			trace_util_0.Count(_admin_00000, 124)
			return errors.Trace(err)
		}
		trace_util_0.Count(_admin_00000, 118)
		vals2, err := rowWithCols(sessCtx, txn, t, h, cols, rowDecoder)
		vals2 = tables.TruncateIndexValuesIfNeeded(t.Meta(), idx.Meta(), vals2)
		if kv.ErrNotExist.Equal(err) {
			trace_util_0.Count(_admin_00000, 125)
			record := &RecordData{Handle: h, Values: vals1}
			err = ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", record, nil)
		}
		trace_util_0.Count(_admin_00000, 119)
		if err != nil {
			trace_util_0.Count(_admin_00000, 126)
			return errors.Trace(err)
		}
		trace_util_0.Count(_admin_00000, 120)
		adjustDatumKind(vals1, vals2)
		if !compareDatumSlice(sc, vals1, vals2) {
			trace_util_0.Count(_admin_00000, 127)
			record1 := &RecordData{Handle: h, Values: vals1}
			record2 := &RecordData{Handle: h, Values: vals2}
			return ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", record1, record2)
		}
	}

	trace_util_0.Count(_admin_00000, 112)
	return nil
}

func compareDatumSlice(sc *stmtctx.StatementContext, val1s, val2s []types.Datum) bool {
	trace_util_0.Count(_admin_00000, 128)
	if len(val1s) != len(val2s) {
		trace_util_0.Count(_admin_00000, 131)
		return false
	}
	trace_util_0.Count(_admin_00000, 129)
	for i, v := range val1s {
		trace_util_0.Count(_admin_00000, 132)
		res, err := v.CompareDatum(sc, &val2s[i])
		if err != nil || res != 0 {
			trace_util_0.Count(_admin_00000, 133)
			return false
		}
	}
	trace_util_0.Count(_admin_00000, 130)
	return true
}

// CheckRecordAndIndex is exported for testing.
func CheckRecordAndIndex(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index, genExprs map[model.TableColumnID]expression.Expression) error {
	trace_util_0.Count(_admin_00000, 134)
	sc := sessCtx.GetSessionVars().StmtCtx
	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		trace_util_0.Count(_admin_00000, 138)
		cols[i] = t.Cols()[col.Offset]
	}

	trace_util_0.Count(_admin_00000, 135)
	startKey := t.RecordKey(0)
	filterFunc := func(h1 int64, vals1 []types.Datum, cols []*table.Column) (bool, error) {
		trace_util_0.Count(_admin_00000, 139)
		for i, val := range vals1 {
			trace_util_0.Count(_admin_00000, 144)
			col := cols[i]
			if val.IsNull() {
				trace_util_0.Count(_admin_00000, 145)
				if mysql.HasNotNullFlag(col.Flag) && col.ToInfo().OriginDefaultValue == nil {
					trace_util_0.Count(_admin_00000, 148)
					return false, errors.Errorf("Column %v define as not null, but can't find the value where handle is %v", col.Name, h1)
				}
				// NULL value is regarded as its default value.
				trace_util_0.Count(_admin_00000, 146)
				colDefVal, err := table.GetColOriginDefaultValue(sessCtx, col.ToInfo())
				if err != nil {
					trace_util_0.Count(_admin_00000, 149)
					return false, errors.Trace(err)
				}
				trace_util_0.Count(_admin_00000, 147)
				vals1[i] = colDefVal
			}
		}
		trace_util_0.Count(_admin_00000, 140)
		isExist, h2, err := idx.Exist(sc, txn, vals1, h1)
		if kv.ErrKeyExists.Equal(err) {
			trace_util_0.Count(_admin_00000, 150)
			record1 := &RecordData{Handle: h1, Values: vals1}
			record2 := &RecordData{Handle: h2, Values: vals1}
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", record2, record1)
		}
		trace_util_0.Count(_admin_00000, 141)
		if err != nil {
			trace_util_0.Count(_admin_00000, 151)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_admin_00000, 142)
		if !isExist {
			trace_util_0.Count(_admin_00000, 152)
			record := &RecordData{Handle: h1, Values: vals1}
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", nil, record)
		}

		trace_util_0.Count(_admin_00000, 143)
		return true, nil
	}
	trace_util_0.Count(_admin_00000, 136)
	err := iterRecords(sessCtx, txn, t, startKey, cols, filterFunc, genExprs)

	if err != nil {
		trace_util_0.Count(_admin_00000, 153)
		return errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 137)
	return nil
}

func scanTableData(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, cols []*table.Column, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	trace_util_0.Count(_admin_00000, 154)
	var records []*RecordData

	startKey := t.RecordKey(startHandle)
	filterFunc := func(h int64, d []types.Datum, cols []*table.Column) (bool, error) {
		trace_util_0.Count(_admin_00000, 158)
		if limit != 0 {
			trace_util_0.Count(_admin_00000, 160)
			r := &RecordData{
				Handle: h,
				Values: d,
			}
			records = append(records, r)
			limit--
			return true, nil
		}
		trace_util_0.Count(_admin_00000, 159)
		return false, nil
	}
	trace_util_0.Count(_admin_00000, 155)
	err := iterRecords(sessCtx, retriever, t, startKey, cols, filterFunc, nil)
	if err != nil {
		trace_util_0.Count(_admin_00000, 161)
		return nil, 0, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 156)
	if len(records) == 0 {
		trace_util_0.Count(_admin_00000, 162)
		return records, startHandle, nil
	}

	trace_util_0.Count(_admin_00000, 157)
	nextHandle := records[len(records)-1].Handle + 1

	return records, nextHandle, nil
}

// ScanTableRecord scans table row handles and column values in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data. If startHandle = 0 and limit = -1,
// it returns the table data of the whole.
func ScanTableRecord(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	trace_util_0.Count(_admin_00000, 163)
	return scanTableData(sessCtx, retriever, t, t.Cols(), startHandle, limit)
}

// ScanSnapshotTableRecord scans the ver version of the table data in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data. If startHandle = 0 and limit = -1,
// it returns the table data of the whole.
func ScanSnapshotTableRecord(sessCtx sessionctx.Context, store kv.Storage, ver kv.Version, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	trace_util_0.Count(_admin_00000, 164)
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		trace_util_0.Count(_admin_00000, 166)
		return nil, 0, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 165)
	records, nextHandle, err := ScanTableRecord(sessCtx, snap, t, startHandle, limit)

	return records, nextHandle, errors.Trace(err)
}

// CompareTableRecord compares data and the corresponding table data one by one.
// It returns nil if data is equal to the data that scans from table, otherwise
// it returns an error with a different set of records. If exact is false, only compares handle.
func CompareTableRecord(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, data []*RecordData, exact bool) error {
	trace_util_0.Count(_admin_00000, 167)
	m := make(map[int64][]types.Datum, len(data))
	for _, r := range data {
		trace_util_0.Count(_admin_00000, 172)
		if _, ok := m[r.Handle]; ok {
			trace_util_0.Count(_admin_00000, 174)
			return errRepeatHandle.GenWithStack("handle:%d is repeated in data", r.Handle)
		}
		trace_util_0.Count(_admin_00000, 173)
		m[r.Handle] = r.Values
	}

	trace_util_0.Count(_admin_00000, 168)
	startKey := t.RecordKey(0)
	sc := sessCtx.GetSessionVars().StmtCtx
	filterFunc := func(h int64, vals []types.Datum, cols []*table.Column) (bool, error) {
		trace_util_0.Count(_admin_00000, 175)
		vals2, ok := m[h]
		if !ok {
			trace_util_0.Count(_admin_00000, 179)
			record := &RecordData{Handle: h, Values: vals}
			return false, ErrDataInConsistent.GenWithStack("data:%#v != record:%#v", nil, record)
		}
		trace_util_0.Count(_admin_00000, 176)
		if !exact {
			trace_util_0.Count(_admin_00000, 180)
			delete(m, h)
			return true, nil
		}

		trace_util_0.Count(_admin_00000, 177)
		if !compareDatumSlice(sc, vals, vals2) {
			trace_util_0.Count(_admin_00000, 181)
			record1 := &RecordData{Handle: h, Values: vals2}
			record2 := &RecordData{Handle: h, Values: vals}
			return false, ErrDataInConsistent.GenWithStack("data:%#v != record:%#v", record1, record2)
		}

		trace_util_0.Count(_admin_00000, 178)
		delete(m, h)

		return true, nil
	}
	trace_util_0.Count(_admin_00000, 169)
	err := iterRecords(sessCtx, txn, t, startKey, t.Cols(), filterFunc, nil)
	if err != nil {
		trace_util_0.Count(_admin_00000, 182)
		return errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 170)
	for h, vals := range m {
		trace_util_0.Count(_admin_00000, 183)
		record := &RecordData{Handle: h, Values: vals}
		return ErrDataInConsistent.GenWithStack("data:%#v != record:%#v", record, nil)
	}

	trace_util_0.Count(_admin_00000, 171)
	return nil
}

func makeRowDecoder(t table.Table, decodeCol []*table.Column, genExpr map[model.TableColumnID]expression.Expression) *decoder.RowDecoder {
	trace_util_0.Count(_admin_00000, 184)
	cols := t.Cols()
	tblInfo := t.Meta()
	decodeColsMap := make(map[int64]decoder.Column, len(decodeCol))
	for _, v := range decodeCol {
		trace_util_0.Count(_admin_00000, 186)
		col := cols[v.Offset]
		tpExpr := decoder.Column{
			Col: col,
		}
		if col.IsGenerated() && !col.GeneratedStored {
			trace_util_0.Count(_admin_00000, 188)
			for _, c := range cols {
				trace_util_0.Count(_admin_00000, 190)
				if _, ok := col.Dependences[c.Name.L]; ok {
					trace_util_0.Count(_admin_00000, 191)
					decodeColsMap[c.ID] = decoder.Column{
						Col: c,
					}
				}
			}
			trace_util_0.Count(_admin_00000, 189)
			tpExpr.GenExpr = genExpr[model.TableColumnID{TableID: tblInfo.ID, ColumnID: col.ID}]
		}
		trace_util_0.Count(_admin_00000, 187)
		decodeColsMap[col.ID] = tpExpr
	}
	trace_util_0.Count(_admin_00000, 185)
	return decoder.NewRowDecoder(t, decodeColsMap)
}

// genExprs use to calculate generated column value.
func rowWithCols(sessCtx sessionctx.Context, txn kv.Retriever, t table.Table, h int64, cols []*table.Column, rowDecoder *decoder.RowDecoder) ([]types.Datum, error) {
	trace_util_0.Count(_admin_00000, 192)
	key := t.RecordKey(h)
	value, err := txn.Get(key)
	if err != nil {
		trace_util_0.Count(_admin_00000, 197)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 193)
	v := make([]types.Datum, len(cols))
	for i, col := range cols {
		trace_util_0.Count(_admin_00000, 198)
		if col == nil {
			trace_util_0.Count(_admin_00000, 201)
			continue
		}
		trace_util_0.Count(_admin_00000, 199)
		if col.State != model.StatePublic {
			trace_util_0.Count(_admin_00000, 202)
			return nil, errInvalidColumnState.GenWithStack("Cannot use none public column - %v", cols)
		}
		trace_util_0.Count(_admin_00000, 200)
		if col.IsPKHandleColumn(t.Meta()) {
			trace_util_0.Count(_admin_00000, 203)
			if mysql.HasUnsignedFlag(col.Flag) {
				trace_util_0.Count(_admin_00000, 205)
				v[i].SetUint64(uint64(h))
			} else {
				trace_util_0.Count(_admin_00000, 206)
				{
					v[i].SetInt64(h)
				}
			}
			trace_util_0.Count(_admin_00000, 204)
			continue
		}
	}

	trace_util_0.Count(_admin_00000, 194)
	rowMap, err := rowDecoder.DecodeAndEvalRowWithMap(sessCtx, h, value, sessCtx.GetSessionVars().Location(), time.UTC, nil)
	if err != nil {
		trace_util_0.Count(_admin_00000, 207)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_admin_00000, 195)
	for i, col := range cols {
		trace_util_0.Count(_admin_00000, 208)
		if col == nil {
			trace_util_0.Count(_admin_00000, 213)
			continue
		}
		trace_util_0.Count(_admin_00000, 209)
		if col.State != model.StatePublic {
			trace_util_0.Count(_admin_00000, 214)
			// TODO: check this
			return nil, errInvalidColumnState.GenWithStack("Cannot use none public column - %v", cols)
		}
		trace_util_0.Count(_admin_00000, 210)
		if col.IsPKHandleColumn(t.Meta()) {
			trace_util_0.Count(_admin_00000, 215)
			continue
		}
		trace_util_0.Count(_admin_00000, 211)
		ri, ok := rowMap[col.ID]
		if !ok {
			trace_util_0.Count(_admin_00000, 216)
			if mysql.HasNotNullFlag(col.Flag) && col.ToInfo().OriginDefaultValue == nil {
				trace_util_0.Count(_admin_00000, 219)
				return nil, errors.Errorf("Column %v define as not null, but can't find the value where handle is %v", col.Name, h)
			}
			// NULL value is regarded as its default value.
			trace_util_0.Count(_admin_00000, 217)
			colDefVal, err := table.GetColOriginDefaultValue(sessCtx, col.ToInfo())
			if err != nil {
				trace_util_0.Count(_admin_00000, 220)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_admin_00000, 218)
			v[i] = colDefVal
			continue
		}
		trace_util_0.Count(_admin_00000, 212)
		v[i] = ri
	}
	trace_util_0.Count(_admin_00000, 196)
	return v, nil
}

// genExprs use to calculate generated column value.
func iterRecords(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc, genExprs map[model.TableColumnID]expression.Expression) error {
	trace_util_0.Count(_admin_00000, 221)
	prefix := t.RecordPrefix()
	keyUpperBound := prefix.PrefixNext()

	it, err := retriever.Iter(startKey, keyUpperBound)
	if err != nil {
		trace_util_0.Count(_admin_00000, 225)
		return errors.Trace(err)
	}
	trace_util_0.Count(_admin_00000, 222)
	defer it.Close()

	if !it.Valid() {
		trace_util_0.Count(_admin_00000, 226)
		return nil
	}

	trace_util_0.Count(_admin_00000, 223)
	logutil.Logger(context.Background()).Debug("record",
		zap.Binary("startKey", startKey),
		zap.Binary("key", it.Key()),
		zap.Binary("value", it.Value()))
	rowDecoder := makeRowDecoder(t, cols, genExprs)
	for it.Valid() && it.Key().HasPrefix(prefix) {
		trace_util_0.Count(_admin_00000, 227)
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			trace_util_0.Count(_admin_00000, 232)
			return errors.Trace(err)
		}

		trace_util_0.Count(_admin_00000, 228)
		rowMap, err := rowDecoder.DecodeAndEvalRowWithMap(sessCtx, handle, it.Value(), sessCtx.GetSessionVars().Location(), time.UTC, nil)
		if err != nil {
			trace_util_0.Count(_admin_00000, 233)
			return errors.Trace(err)
		}
		trace_util_0.Count(_admin_00000, 229)
		data := make([]types.Datum, 0, len(cols))
		for _, col := range cols {
			trace_util_0.Count(_admin_00000, 234)
			if col.IsPKHandleColumn(t.Meta()) {
				trace_util_0.Count(_admin_00000, 235)
				if mysql.HasUnsignedFlag(col.Flag) {
					trace_util_0.Count(_admin_00000, 236)
					data = append(data, types.NewUintDatum(uint64(handle)))
				} else {
					trace_util_0.Count(_admin_00000, 237)
					{
						data = append(data, types.NewIntDatum(handle))
					}
				}
			} else {
				trace_util_0.Count(_admin_00000, 238)
				{
					data = append(data, rowMap[col.ID])
				}
			}
		}
		trace_util_0.Count(_admin_00000, 230)
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			trace_util_0.Count(_admin_00000, 239)
			return errors.Trace(err)
		}

		trace_util_0.Count(_admin_00000, 231)
		rk := t.RecordKey(handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			trace_util_0.Count(_admin_00000, 240)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_admin_00000, 224)
	return nil
}

// admin error codes.
const (
	codeDataNotEqual       terror.ErrCode = 1
	codeRepeatHandle                      = 2
	codeInvalidColumnState                = 3
	codeDDLJobNotFound                    = 4
	codeCancelFinishedJob                 = 5
	codeCannotCancelDDLJob                = 6
)

var (
	// ErrDataInConsistent indicate that meets inconsistent data.
	ErrDataInConsistent   = terror.ClassAdmin.New(codeDataNotEqual, "data isn't equal")
	errRepeatHandle       = terror.ClassAdmin.New(codeRepeatHandle, "handle is repeated")
	errInvalidColumnState = terror.ClassAdmin.New(codeInvalidColumnState, "invalid column state")
	// ErrDDLJobNotFound indicates the job id was not found.
	ErrDDLJobNotFound = terror.ClassAdmin.New(codeDDLJobNotFound, "DDL Job:%v not found")
	// ErrCancelFinishedDDLJob returns when cancel a finished ddl job.
	ErrCancelFinishedDDLJob = terror.ClassAdmin.New(codeCancelFinishedJob, "This job:%v is finished, so can't be cancelled")
	// ErrCannotCancelDDLJob returns when cancel a almost finished ddl job, because cancel in now may cause data inconsistency.
	ErrCannotCancelDDLJob = terror.ClassAdmin.New(codeCannotCancelDDLJob, "This job:%v is almost finished, can't be cancelled now")
)

var _admin_00000 = "util/admin/admin.go"
