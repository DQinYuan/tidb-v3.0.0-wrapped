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
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const maxPrefixLength = 3072
const maxCommentLength = 1024

func buildIndexColumns(columns []*model.ColumnInfo, idxColNames []*ast.IndexColName) ([]*model.IndexColumn, error) {
	trace_util_0.Count(_index_00000, 0)
	// Build offsets.
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))

	// The sum of length of all index columns.
	sumLength := 0

	for _, ic := range idxColNames {
		trace_util_0.Count(_index_00000, 2)
		col := model.FindColumnInfo(columns, ic.Column.Name.O)
		if col == nil {
			trace_util_0.Count(_index_00000, 12)
			return nil, errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Column.Name)
		}

		trace_util_0.Count(_index_00000, 3)
		if col.Flen == 0 {
			trace_util_0.Count(_index_00000, 13)
			return nil, errors.Trace(errWrongKeyColumn.GenWithStackByArgs(ic.Column.Name))
		}

		// JSON column cannot index.
		trace_util_0.Count(_index_00000, 4)
		if col.FieldType.Tp == mysql.TypeJSON {
			trace_util_0.Count(_index_00000, 14)
			return nil, errors.Trace(errJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
		}

		// Length must be specified for BLOB and TEXT column indexes.
		trace_util_0.Count(_index_00000, 5)
		if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
			trace_util_0.Count(_index_00000, 15)
			return nil, errors.Trace(errBlobKeyWithoutLength)
		}

		// Length can only be specified for specifiable types.
		trace_util_0.Count(_index_00000, 6)
		if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
			trace_util_0.Count(_index_00000, 16)
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Key length must be shorter or equal to the column length.
		trace_util_0.Count(_index_00000, 7)
		if ic.Length != types.UnspecifiedLength &&
			types.IsTypeChar(col.FieldType.Tp) && col.Flen < ic.Length {
			trace_util_0.Count(_index_00000, 17)
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Specified length must be shorter than the max length for prefix.
		trace_util_0.Count(_index_00000, 8)
		if ic.Length > maxPrefixLength {
			trace_util_0.Count(_index_00000, 18)
			return nil, errors.Trace(errTooLongKey)
		}

		// Take care of the sum of length of all index columns.
		trace_util_0.Count(_index_00000, 9)
		if ic.Length != types.UnspecifiedLength {
			trace_util_0.Count(_index_00000, 19)
			sumLength += ic.Length
		} else {
			trace_util_0.Count(_index_00000, 20)
			{
				// Specified data types.
				if col.Flen != types.UnspecifiedLength {
					trace_util_0.Count(_index_00000, 21)
					// Special case for the bit type.
					if col.FieldType.Tp == mysql.TypeBit {
						trace_util_0.Count(_index_00000, 22)
						sumLength += (col.Flen + 7) >> 3
					} else {
						trace_util_0.Count(_index_00000, 23)
						{
							sumLength += col.Flen
						}
					}
				} else {
					trace_util_0.Count(_index_00000, 24)
					{
						if length, ok := mysql.DefaultLengthOfMysqlTypes[col.FieldType.Tp]; ok {
							trace_util_0.Count(_index_00000, 26)
							sumLength += length
						} else {
							trace_util_0.Count(_index_00000, 27)
							{
								return nil, errUnknownTypeLength.GenWithStackByArgs(col.FieldType.Tp)
							}
						}

						// Special case for time fraction.
						trace_util_0.Count(_index_00000, 25)
						if types.IsTypeFractionable(col.FieldType.Tp) &&
							col.FieldType.Decimal != types.UnspecifiedLength {
							trace_util_0.Count(_index_00000, 28)
							if length, ok := mysql.DefaultLengthOfTimeFraction[col.FieldType.Decimal]; ok {
								trace_util_0.Count(_index_00000, 29)
								sumLength += length
							} else {
								trace_util_0.Count(_index_00000, 30)
								{
									return nil, errUnknownFractionLength.GenWithStackByArgs(col.FieldType.Tp, col.FieldType.Decimal)
								}
							}
						}
					}
				}
			}
		}

		// The sum of all lengths must be shorter than the max length for prefix.
		trace_util_0.Count(_index_00000, 10)
		if sumLength > maxPrefixLength {
			trace_util_0.Count(_index_00000, 31)
			return nil, errors.Trace(errTooLongKey)
		}

		trace_util_0.Count(_index_00000, 11)
		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}

	trace_util_0.Count(_index_00000, 1)
	return idxColumns, nil
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, idxColNames []*ast.IndexColName, state model.SchemaState) (*model.IndexInfo, error) {
	trace_util_0.Count(_index_00000, 32)
	idxColumns, err := buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		trace_util_0.Count(_index_00000, 34)
		return nil, errors.Trace(err)
	}

	// Create index info.
	trace_util_0.Count(_index_00000, 33)
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	trace_util_0.Count(_index_00000, 35)
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		trace_util_0.Count(_index_00000, 36)
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		trace_util_0.Count(_index_00000, 37)
		{
			tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
		}
	}
}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	trace_util_0.Count(_index_00000, 38)
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		trace_util_0.Count(_index_00000, 40)
		tblInfo.Columns[col.Offset].Flag &= ^mysql.UniqueKeyFlag
	} else {
		trace_util_0.Count(_index_00000, 41)
		{
			tblInfo.Columns[col.Offset].Flag &= ^mysql.MultipleKeyFlag
		}
	}

	// other index may still cover this col
	trace_util_0.Count(_index_00000, 39)
	for _, index := range tblInfo.Indices {
		trace_util_0.Count(_index_00000, 42)
		if index.Name.L == indexInfo.Name.L {
			trace_util_0.Count(_index_00000, 45)
			continue
		}

		trace_util_0.Count(_index_00000, 43)
		if index.Columns[0].Name.L != col.Name.L {
			trace_util_0.Count(_index_00000, 46)
			continue
		}

		trace_util_0.Count(_index_00000, 44)
		addIndexColumnFlag(tblInfo, index)
	}
}

func validateRenameIndex(from, to model.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	trace_util_0.Count(_index_00000, 47)
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		trace_util_0.Count(_index_00000, 51)
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	trace_util_0.Count(_index_00000, 48)
	if from.O == to.O {
		trace_util_0.Count(_index_00000, 52)
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	trace_util_0.Count(_index_00000, 49)
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		trace_util_0.Count(_index_00000, 53)
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	trace_util_0.Count(_index_00000, 50)
	return false, nil
}

func onRenameIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_index_00000, 54)
	tblInfo, from, to, err := checkRenameIndex(t, job)
	if err != nil {
		trace_util_0.Count(_index_00000, 57)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 55)
	idx := tblInfo.FindIndexByName(from.L)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(t, job, tblInfo, true); err != nil {
		trace_util_0.Count(_index_00000, 58)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 56)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onCreateIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_index_00000, 59)
	// Handle the rolling back job.
	if job.IsRollingback() {
		trace_util_0.Count(_index_00000, 66)
		ver, err = onDropIndex(t, job)
		if err != nil {
			trace_util_0.Count(_index_00000, 68)
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_index_00000, 67)
		return ver, nil
	}

	// Handle normal job.
	trace_util_0.Count(_index_00000, 60)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_index_00000, 69)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 61)
	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*ast.IndexColName
		indexOption *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames, &indexOption)
	if err != nil {
		trace_util_0.Count(_index_00000, 70)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 62)
	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		trace_util_0.Count(_index_00000, 71)
		job.State = model.JobStateCancelled
		return ver, ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	trace_util_0.Count(_index_00000, 63)
	if indexInfo == nil {
		trace_util_0.Count(_index_00000, 72)
		indexInfo, err = buildIndexInfo(tblInfo, indexName, idxColNames, model.StateNone)
		if err != nil {
			trace_util_0.Count(_index_00000, 75)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_index_00000, 73)
		if indexOption != nil {
			trace_util_0.Count(_index_00000, 76)
			indexInfo.Comment = indexOption.Comment
			if indexOption.Tp == model.IndexTypeInvalid {
				trace_util_0.Count(_index_00000, 77)
				// Use btree as default index type.
				indexInfo.Tp = model.IndexTypeBtree
			} else {
				trace_util_0.Count(_index_00000, 78)
				{
					indexInfo.Tp = indexOption.Tp
				}
			}
		} else {
			trace_util_0.Count(_index_00000, 79)
			{
				// Use btree as default index type.
				indexInfo.Tp = model.IndexTypeBtree
			}
		}
		trace_util_0.Count(_index_00000, 74)
		indexInfo.Primary = false
		indexInfo.Unique = unique
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
		logutil.Logger(ddlLogCtx).Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	}
	trace_util_0.Count(_index_00000, 64)
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		trace_util_0.Count(_index_00000, 80)
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		trace_util_0.Count(_index_00000, 81)
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		trace_util_0.Count(_index_00000, 82)
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteReorganization:
		trace_util_0.Count(_index_00000, 83)
		// reorganization -> public
		var tbl table.Table
		tbl, err = getTable(d.store, schemaID, tblInfo)
		if err != nil {
			trace_util_0.Count(_index_00000, 90)
			return ver, errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 84)
		var reorgInfo *reorgInfo
		reorgInfo, err = getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			trace_util_0.Count(_index_00000, 91)
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 85)
		err = w.runReorgJob(t, reorgInfo, d.lease, func() (addIndexErr error) {
			trace_util_0.Count(_index_00000, 92)
			defer func() {
				trace_util_0.Count(_index_00000, 94)
				r := recover()
				if r != nil {
					trace_util_0.Count(_index_00000, 95)
					buf := util.GetStack()
					logutil.Logger(ddlLogCtx).Error("[ddl] add table index panic", zap.Any("panic", r), zap.String("stack", string(buf)))
					metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
					addIndexErr = errCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tblInfo.Name, indexInfo.Name)
				}
			}()
			trace_util_0.Count(_index_00000, 93)
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		trace_util_0.Count(_index_00000, 86)
		if err != nil {
			trace_util_0.Count(_index_00000, 96)
			if errWaitReorgTimeout.Equal(err) {
				trace_util_0.Count(_index_00000, 99)
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			trace_util_0.Count(_index_00000, 97)
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) || errCantDecodeIndex.Equal(err) {
				trace_util_0.Count(_index_00000, 100)
				logutil.Logger(ddlLogCtx).Warn("[ddl] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			trace_util_0.Count(_index_00000, 98)
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		trace_util_0.Count(_index_00000, 87)
		w.reorgCtx.cleanNotifyReorgCancel()

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			trace_util_0.Count(_index_00000, 101)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_index_00000, 88)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		trace_util_0.Count(_index_00000, 89)
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", tblInfo.State)
	}

	trace_util_0.Count(_index_00000, 65)
	return ver, errors.Trace(err)
}

func onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_index_00000, 102)
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		trace_util_0.Count(_index_00000, 105)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 103)
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StatePublic:
		trace_util_0.Count(_index_00000, 106)
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		trace_util_0.Count(_index_00000, 107)
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		trace_util_0.Count(_index_00000, 108)
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteReorganization:
		trace_util_0.Count(_index_00000, 109)
		// reorganization -> absent
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			trace_util_0.Count(_index_00000, 113)
			if idx.Name.L != indexInfo.Name.L {
				trace_util_0.Count(_index_00000, 114)
				newIndices = append(newIndices, idx)
			}
		}
		trace_util_0.Count(_index_00000, 110)
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			trace_util_0.Count(_index_00000, 115)
			return ver, errors.Trace(err)
		}

		// Finish this job.
		trace_util_0.Count(_index_00000, 111)
		if job.IsRollingback() {
			trace_util_0.Count(_index_00000, 116)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			job.Args[0] = indexInfo.ID
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			trace_util_0.Count(_index_00000, 117)
			{
				job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
				job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
			}
		}
	default:
		trace_util_0.Count(_index_00000, 112)
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", indexInfo.State)
	}
	trace_util_0.Count(_index_00000, 104)
	return ver, errors.Trace(err)
}

func checkDropIndex(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.IndexInfo, error) {
	trace_util_0.Count(_index_00000, 118)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_index_00000, 122)
		return nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 119)
	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		trace_util_0.Count(_index_00000, 123)
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 120)
	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		trace_util_0.Count(_index_00000, 124)
		job.State = model.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}
	trace_util_0.Count(_index_00000, 121)
	return tblInfo, indexInfo, nil
}

func checkRenameIndex(t *meta.Meta, job *model.Job) (*model.TableInfo, model.CIStr, model.CIStr, error) {
	trace_util_0.Count(_index_00000, 125)
	var from, to model.CIStr
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_index_00000, 130)
		return nil, from, to, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 126)
	if err := job.DecodeArgs(&from, &to); err != nil {
		trace_util_0.Count(_index_00000, 131)
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in ddl_api.go
	trace_util_0.Count(_index_00000, 127)
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		trace_util_0.Count(_index_00000, 132)
		return nil, from, to, nil
	}
	trace_util_0.Count(_index_00000, 128)
	if err != nil {
		trace_util_0.Count(_index_00000, 133)
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 129)
	return tblInfo, from, to, errors.Trace(err)
}

const (
	// DefaultTaskHandleCnt is default batch size of adding indices.
	DefaultTaskHandleCnt = 128
)

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgIndexTask
	resultCh  chan *addIndexResult
	index     table.Index
	table     table.Table
	closed    bool
	priority  int

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	rowDecoder         *decoder.RowDecoder
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

type reorgIndexTask struct {
	physicalTableID int64
	startHandle     int64
	endHandle       int64
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

func (r *reorgIndexTask) String() string {
	trace_util_0.Count(_index_00000, 134)
	rightParenthesis := ")"
	if r.endIncluded {
		trace_util_0.Count(_index_00000, 136)
		rightParenthesis = "]"
	}
	trace_util_0.Count(_index_00000, 135)
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + strconv.FormatInt(r.startHandle, 10) + "," + strconv.FormatInt(r.endHandle, 10) + rightParenthesis
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle int64
	err        error
}

// addIndexTaskContext is the context of the batch adding indices.
// After finishing the batch adding indices, result in addIndexTaskContext will be merged into addIndexResult.
type addIndexTaskContext struct {
	nextHandle int64
	done       bool
	addedCount int
	scanCount  int
}

// mergeAddIndexCtxToResult merge partial result in taskCtx into result.
func mergeAddIndexCtxToResult(taskCtx *addIndexTaskContext, result *addIndexResult) {
	trace_util_0.Count(_index_00000, 137)
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column) *addIndexWorker {
	trace_util_0.Count(_index_00000, 138)
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, decodeColMap)
	return &addIndexWorker{
		id:          id,
		ddlWorker:   worker,
		batchCnt:    int(variable.GetDDLReorgBatchSize()),
		sessCtx:     sessCtx,
		taskCh:      make(chan *reorgIndexTask, 1),
		resultCh:    make(chan *addIndexResult, 1),
		index:       index,
		table:       t,
		rowDecoder:  rowDecoder,
		priority:    kv.PriorityLow,
		defaultVals: make([]types.Datum, len(t.Cols())),
		rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
	}
}

func (w *addIndexWorker) close() {
	trace_util_0.Count(_index_00000, 139)
	if !w.closed {
		trace_util_0.Count(_index_00000, 140)
		w.closed = true
		close(w.taskCh)
	}
}

// getIndexRecord gets index columns values from raw binary value row.
func (w *addIndexWorker) getIndexRecord(handle int64, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	trace_util_0.Count(_index_00000, 141)
	t := w.table
	cols := t.Cols()
	idxInfo := w.index.Meta()
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		trace_util_0.Count(_index_00000, 144)
		return nil, errors.Trace(errCantDecodeIndex.GenWithStackByArgs(err))
	}
	trace_util_0.Count(_index_00000, 142)
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		trace_util_0.Count(_index_00000, 145)
		col := cols[v.Offset]
		if col.IsPKHandleColumn(t.Meta()) {
			trace_util_0.Count(_index_00000, 150)
			if mysql.HasUnsignedFlag(col.Flag) {
				trace_util_0.Count(_index_00000, 152)
				idxVal[j].SetUint64(uint64(handle))
			} else {
				trace_util_0.Count(_index_00000, 153)
				{
					idxVal[j].SetInt64(handle)
				}
			}
			trace_util_0.Count(_index_00000, 151)
			continue
		}
		trace_util_0.Count(_index_00000, 146)
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			trace_util_0.Count(_index_00000, 154)
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
			continue
		}
		trace_util_0.Count(_index_00000, 147)
		idxColumnVal, err = tables.GetColDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			trace_util_0.Count(_index_00000, 155)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 148)
		if idxColumnVal.Kind() == types.KindMysqlTime {
			trace_util_0.Count(_index_00000, 156)
			t := idxColumnVal.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp && sysZone != time.UTC {
				trace_util_0.Count(_index_00000, 157)
				err := t.ConvertTimeZone(sysZone, time.UTC)
				if err != nil {
					trace_util_0.Count(_index_00000, 159)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_index_00000, 158)
				idxColumnVal.SetMysqlTime(t)
			}
		}
		trace_util_0.Count(_index_00000, 149)
		idxVal[j] = idxColumnVal
	}
	// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
	// the generated value, so we need to clear up the reusing map.
	trace_util_0.Count(_index_00000, 143)
	w.cleanRowMap()
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal}
	return idxRecord, nil
}

func (w *addIndexWorker) cleanRowMap() {
	trace_util_0.Count(_index_00000, 160)
	for id := range w.rowMap {
		trace_util_0.Count(_index_00000, 161)
		delete(w.rowMap, id)
	}
}

// getNextHandle gets next handle of entry that we are going to process.
func (w *addIndexWorker) getNextHandle(taskRange reorgIndexTask, taskDone bool) (nextHandle int64) {
	trace_util_0.Count(_index_00000, 162)
	if !taskDone {
		trace_util_0.Count(_index_00000, 165)
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return w.idxRecords[len(w.idxRecords)-1].handle + 1
	}

	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	trace_util_0.Count(_index_00000, 163)
	if taskRange.endHandle == math.MaxInt64 || !taskRange.endIncluded {
		trace_util_0.Count(_index_00000, 166)
		return taskRange.endHandle
	}

	trace_util_0.Count(_index_00000, 164)
	return taskRange.endHandle + 1
}

// fetchRowColVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgIndexTask) ([]*indexRecord, int64, bool, error) {
	trace_util_0.Count(_index_00000, 167)
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle int64, recordKey kv.Key, rawRow []byte) (bool, error) {
			trace_util_0.Count(_index_00000, 170)
			oprEndTime := time.Now()
			w.logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				trace_util_0.Count(_index_00000, 175)
				taskDone = handle >= taskRange.endHandle
			} else {
				trace_util_0.Count(_index_00000, 176)
				{
					taskDone = handle > taskRange.endHandle
				}
			}

			trace_util_0.Count(_index_00000, 171)
			if taskDone || len(w.idxRecords) >= w.batchCnt {
				trace_util_0.Count(_index_00000, 177)
				return false, nil
			}

			trace_util_0.Count(_index_00000, 172)
			idxRecord, err1 := w.getIndexRecord(handle, recordKey, rawRow)
			if err1 != nil {
				trace_util_0.Count(_index_00000, 178)
				return false, errors.Trace(err1)
			}

			trace_util_0.Count(_index_00000, 173)
			w.idxRecords = append(w.idxRecords, idxRecord)
			if handle == taskRange.endHandle {
				trace_util_0.Count(_index_00000, 179)
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle
				taskDone = true
				return false, nil
			}
			trace_util_0.Count(_index_00000, 174)
			return true, nil
		})

	trace_util_0.Count(_index_00000, 168)
	if len(w.idxRecords) == 0 {
		trace_util_0.Count(_index_00000, 180)
		taskDone = true
	}

	trace_util_0.Count(_index_00000, 169)
	logutil.Logger(ddlLogCtx).Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextHandle(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	trace_util_0.Count(_index_00000, 181)
	if threshold == 0 {
		trace_util_0.Count(_index_00000, 183)
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	trace_util_0.Count(_index_00000, 182)
	if elapsed >= time.Duration(threshold)*time.Millisecond {
		trace_util_0.Count(_index_00000, 184)
		logutil.Logger(ddlLogCtx).Info("[ddl] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
	}
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	trace_util_0.Count(_index_00000, 185)
	if len(w.idxKeyBufs) < batchCount {
		trace_util_0.Count(_index_00000, 187)
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	trace_util_0.Count(_index_00000, 186)
	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
}

func (w *addIndexWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	trace_util_0.Count(_index_00000, 188)
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		trace_util_0.Count(_index_00000, 193)
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	trace_util_0.Count(_index_00000, 189)
	w.initBatchCheckBufs(len(idxRecords))
	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		trace_util_0.Count(_index_00000, 194)
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, record.vals, record.handle, w.idxKeyBufs[i])
		if err != nil {
			trace_util_0.Count(_index_00000, 196)
			return errors.Trace(err)
		}
		// save the buffer to reduce memory allocations.
		trace_util_0.Count(_index_00000, 195)
		w.idxKeyBufs[i] = idxKey

		w.batchCheckKeys = append(w.batchCheckKeys, idxKey)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	trace_util_0.Count(_index_00000, 190)
	batchVals, err := txn.BatchGet(w.batchCheckKeys)
	if err != nil {
		trace_util_0.Count(_index_00000, 197)
		return errors.Trace(err)
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	trace_util_0.Count(_index_00000, 191)
	for i, key := range w.batchCheckKeys {
		trace_util_0.Count(_index_00000, 198)
		if val, found := batchVals[string(key)]; found {
			trace_util_0.Count(_index_00000, 199)
			if w.distinctCheckFlags[i] {
				trace_util_0.Count(_index_00000, 201)
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					trace_util_0.Count(_index_00000, 203)
					return errors.Trace(err1)
				}

				trace_util_0.Count(_index_00000, 202)
				if handle != idxRecords[i].handle {
					trace_util_0.Count(_index_00000, 204)
					return errors.Trace(kv.ErrKeyExists)
				}
			}
			trace_util_0.Count(_index_00000, 200)
			idxRecords[i].skip = true
		} else {
			trace_util_0.Count(_index_00000, 205)
			{
				// The keys in w.batchCheckKeys also maybe duplicate,
				// so we need to backfill the not found key into `batchVals` map.
				if w.distinctCheckFlags[i] {
					trace_util_0.Count(_index_00000, 206)
					batchVals[string(key)] = tables.EncodeHandle(idxRecords[i].handle)
				}
			}
		}
	}
	// Constrains is already checked.
	trace_util_0.Count(_index_00000, 192)
	stmtCtx.BatchCheck = true
	return nil
}

// backfillIndexInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// backfillIndexInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (taskCtx addIndexTaskContext, errInTxn error) {
	trace_util_0.Count(_index_00000, 207)
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		trace_util_0.Count(_index_00000, 210)
		if val.(bool) {
			trace_util_0.Count(_index_00000, 211)
			panic("panic test")
		}
	})

	trace_util_0.Count(_index_00000, 208)
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		trace_util_0.Count(_index_00000, 212)
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		idxRecords, nextHandle, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			trace_util_0.Count(_index_00000, 216)
			return errors.Trace(err)
		}
		trace_util_0.Count(_index_00000, 213)
		taskCtx.nextHandle = nextHandle
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			trace_util_0.Count(_index_00000, 217)
			return errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 214)
		for _, idxRecord := range idxRecords {
			trace_util_0.Count(_index_00000, 218)
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				trace_util_0.Count(_index_00000, 222)
				continue
			}

			// Lock the row key to notify us that someone delete or update the row,
			// then we should not backfill the index of it, otherwise the adding index is redundant.
			trace_util_0.Count(_index_00000, 219)
			err := txn.LockKeys(context.Background(), 0, idxRecord.key)
			if err != nil {
				trace_util_0.Count(_index_00000, 223)
				return errors.Trace(err)
			}

			// Create the index.
			trace_util_0.Count(_index_00000, 220)
			handle, err := w.index.Create(w.sessCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				trace_util_0.Count(_index_00000, 224)
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle == handle {
					trace_util_0.Count(_index_00000, 226)
					// Index already exists, skip it.
					continue
				}

				trace_util_0.Count(_index_00000, 225)
				return errors.Trace(err)
			}
			trace_util_0.Count(_index_00000, 221)
			taskCtx.addedCount++
		}

		trace_util_0.Count(_index_00000, 215)
		return nil
	})
	trace_util_0.Count(_index_00000, 209)
	w.logSlowOperations(time.Since(oprStartTime), "backfillIndexInTxn", 3000)

	return
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *addIndexWorker) handleBackfillTask(d *ddlCtx, task *reorgIndexTask) *addIndexResult {
	trace_util_0.Count(_index_00000, 227)
	handleRange := *task
	result := &addIndexResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		trace_util_0.Count(_index_00000, 229)
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in w.backfillIndexInTxn we will never cancel the job.
		// Because reorgIndexTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := w.ddlWorker.isReorgRunnable(d)
		if err != nil {
			trace_util_0.Count(_index_00000, 233)
			result.err = err
			return result
		}

		trace_util_0.Count(_index_00000, 230)
		taskCtx, err := w.backfillIndexInTxn(handleRange)
		if err != nil {
			trace_util_0.Count(_index_00000, 234)
			result.err = err
			return result
		}

		trace_util_0.Count(_index_00000, 231)
		mergeAddIndexCtxToResult(&taskCtx, result)
		w.ddlWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			trace_util_0.Count(_index_00000, 235)
			lastLogCount = result.scanCount
			logutil.Logger(ddlLogCtx).Info("[ddl] add index worker back fill index", zap.Int("workerID", w.id), zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount), zap.Int64("nextHandle", taskCtx.nextHandle), zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		trace_util_0.Count(_index_00000, 232)
		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			trace_util_0.Count(_index_00000, 236)
			break
		}
	}
	trace_util_0.Count(_index_00000, 228)
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()), zap.Int("addedCount", result.addedCount), zap.Int("scanCount", result.scanCount), zap.Int64("nextHandle", result.nextHandle), zap.String("takeTime", time.Since(startTime).String()))
	return result
}

func (w *addIndexWorker) run(d *ddlCtx) {
	trace_util_0.Count(_index_00000, 237)
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker start", zap.Int("workerID", w.id))
	defer func() {
		trace_util_0.Count(_index_00000, 240)
		r := recover()
		if r != nil {
			trace_util_0.Count(_index_00000, 242)
			buf := util.GetStack()
			logutil.Logger(ddlLogCtx).Error("[ddl] add index worker panic", zap.Any("panic", r), zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		}
		trace_util_0.Count(_index_00000, 241)
		w.resultCh <- &addIndexResult{err: errReorgPanic}
	}()
	trace_util_0.Count(_index_00000, 238)
	for {
		trace_util_0.Count(_index_00000, 243)
		task, more := <-w.taskCh
		if !more {
			trace_util_0.Count(_index_00000, 246)
			break
		}

		trace_util_0.Count(_index_00000, 244)
		logutil.Logger(ddlLogCtx).Debug("[ddl] add index worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockAddIndexErr", func() {
			trace_util_0.Count(_index_00000, 247)
			if w.id == 0 {
				trace_util_0.Count(_index_00000, 248)
				result := &addIndexResult{addedCount: 0, nextHandle: 0, err: errors.Errorf("mock add index error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})

		// Dynamic change batch size.
		trace_util_0.Count(_index_00000, 245)
		w.batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task)
		w.resultCh <- result
	}
	trace_util_0.Count(_index_00000, 239)
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker exit", zap.Int("workerID", w.id))
}

func makeupDecodeColMap(sessCtx sessionctx.Context, t table.Table, indexInfo *model.IndexInfo) (map[int64]decoder.Column, error) {
	trace_util_0.Count(_index_00000, 249)
	cols := t.Cols()
	decodeColMap := make(map[int64]decoder.Column, len(indexInfo.Columns))
	for _, v := range indexInfo.Columns {
		trace_util_0.Count(_index_00000, 251)
		col := cols[v.Offset]
		tpExpr := decoder.Column{
			Col: col,
		}
		if col.IsGenerated() && !col.GeneratedStored {
			trace_util_0.Count(_index_00000, 253)
			for _, c := range cols {
				trace_util_0.Count(_index_00000, 256)
				if _, ok := col.Dependences[c.Name.L]; ok {
					trace_util_0.Count(_index_00000, 257)
					decodeColMap[c.ID] = decoder.Column{
						Col: c,
					}
				}
			}
			trace_util_0.Count(_index_00000, 254)
			e, err := expression.ParseSimpleExprCastWithTableInfo(sessCtx, col.GeneratedExprString, t.Meta(), &col.FieldType)
			if err != nil {
				trace_util_0.Count(_index_00000, 258)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_index_00000, 255)
			tpExpr.GenExpr = e
		}
		trace_util_0.Count(_index_00000, 252)
		decodeColMap[col.ID] = tpExpr
	}
	trace_util_0.Count(_index_00000, 250)
	return decodeColMap, nil
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle int64) ([]kv.KeyRange, error) {
	trace_util_0.Count(_index_00000, 259)
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle).Next()

	logutil.Logger(ddlLogCtx).Info("[ddl] split table range from PD", zap.Int64("physicalTableID", t.GetPhysicalID()), zap.Int64("startHandle", startHandle), zap.Int64("endHandle", endHandle))
	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		trace_util_0.Count(_index_00000, 263)
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	trace_util_0.Count(_index_00000, 260)
	maxSleep := 10000 // ms
	bo := tikv.NewBackoffer(context.Background(), maxSleep)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		trace_util_0.Count(_index_00000, 264)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 261)
	if len(ranges) == 0 {
		trace_util_0.Count(_index_00000, 265)
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	trace_util_0.Count(_index_00000, 262)
	return ranges, nil
}

func decodeHandleRange(keyRange kv.KeyRange) (int64, int64, error) {
	trace_util_0.Count(_index_00000, 266)
	_, startHandle, err := tablecodec.DecodeRecordKey(keyRange.StartKey)
	if err != nil {
		trace_util_0.Count(_index_00000, 269)
		return 0, 0, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 267)
	_, endHandle, err := tablecodec.DecodeRecordKey(keyRange.EndKey)
	if err != nil {
		trace_util_0.Count(_index_00000, 270)
		return 0, 0, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 268)
	return startHandle, endHandle, nil
}

func closeAddIndexWorkers(workers []*addIndexWorker) {
	trace_util_0.Count(_index_00000, 271)
	for _, worker := range workers {
		trace_util_0.Count(_index_00000, 272)
		worker.close()
	}
}

func (w *worker) waitTaskResults(workers []*addIndexWorker, taskCnt int, totalAddedCount *int64, startHandle int64) (int64, int64, error) {
	trace_util_0.Count(_index_00000, 273)
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		trace_util_0.Count(_index_00000, 275)
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			trace_util_0.Count(_index_00000, 278)
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		trace_util_0.Count(_index_00000, 276)
		if result.err != nil {
			trace_util_0.Count(_index_00000, 279)
			logutil.Logger(ddlLogCtx).Warn("[ddl] add index worker return error", zap.Int("workerID", worker.id), zap.Error(result.err))
		}

		trace_util_0.Count(_index_00000, 277)
		if firstErr == nil {
			trace_util_0.Count(_index_00000, 280)
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	trace_util_0.Count(_index_00000, 274)
	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*addIndexWorker, batchTasks []*reorgIndexTask) error {
	trace_util_0.Count(_index_00000, 281)
	for i, task := range batchTasks {
		trace_util_0.Count(_index_00000, 285)
		workers[i].taskCh <- task
	}

	trace_util_0.Count(_index_00000, 282)
	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime)
	if err == nil {
		trace_util_0.Count(_index_00000, 286)
		err = w.isReorgRunnable(reorgInfo.d)
	}

	trace_util_0.Count(_index_00000, 283)
	if err != nil {
		trace_util_0.Count(_index_00000, 287)
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(reorgInfo.d.store, true, func(txn kv.Transaction) error {
			trace_util_0.Count(_index_00000, 289)
			return errors.Trace(reorgInfo.UpdateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		trace_util_0.Count(_index_00000, 288)
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.Logger(ddlLogCtx).Warn("[ddl] add index worker handle batch tasks failed", zap.Int64("totalAddedCount", *totalAddedCount), zap.Int64("startHandle", startHandle), zap.Int64("nextHandle", nextHandle),
			zap.Int64("batchAddedCount", taskAddedCount), zap.String("taskFailedError", err.Error()), zap.String("takeTime", elapsedTime.String()), zap.NamedError("updateHandleError", err1))
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	trace_util_0.Count(_index_00000, 284)
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker handle batch tasks successful", zap.Int64("totalAddedCount", *totalAddedCount), zap.Int64("startHandle", startHandle),
		zap.Int64("nextHandle", nextHandle), zap.Int64("batchAddedCount", taskAddedCount), zap.String("takeTime", elapsedTime.String()))
	return nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(t table.Table, workers []*addIndexWorker, reorgInfo *reorgInfo, totalAddedCount *int64, kvRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	trace_util_0.Count(_index_00000, 290)
	batchTasks := make([]*reorgIndexTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg indices tasks.
	for _, keyRange := range kvRanges {
		trace_util_0.Count(_index_00000, 295)
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			trace_util_0.Count(_index_00000, 298)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 296)
		endKey := t.RecordKey(endHandle)
		endIncluded := false
		if endKey.Cmp(keyRange.EndKey) < 0 {
			trace_util_0.Count(_index_00000, 299)
			endIncluded = true
		}
		trace_util_0.Count(_index_00000, 297)
		task := &reorgIndexTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			trace_util_0.Count(_index_00000, 300)
			break
		}
	}

	trace_util_0.Count(_index_00000, 291)
	if len(batchTasks) == 0 {
		trace_util_0.Count(_index_00000, 301)
		return nil, nil
	}

	// Wait tasks finish.
	trace_util_0.Count(_index_00000, 292)
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
	if err != nil {
		trace_util_0.Count(_index_00000, 302)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_index_00000, 293)
	if len(batchTasks) < len(kvRanges) {
		trace_util_0.Count(_index_00000, 303)
		// there are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	trace_util_0.Count(_index_00000, 294)
	return nil, nil
}

var (
	// TestCheckWorkerNumCh use for test adjust add index worker.
	TestCheckWorkerNumCh = make(chan struct{})
	// TestCheckWorkerNumber use for test adjust add index worker.
	TestCheckWorkerNumber = int32(16)
)

func loadDDLReorgVars(w *worker) error {
	trace_util_0.Count(_index_00000, 304)
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_index_00000, 306)
		return errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 305)
	defer w.sessPool.put(ctx)
	return ddlutil.LoadDDLReorgVars(ctx)
}

// addPhysicalTableIndex handles the add index reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to add index in reorganization state?
// Concurrently process the defaultTaskHandleCnt tasks. Each task deals with a handle range of the index record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split table key range from PD regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	trace_util_0.Count(_index_00000, 307)
	job := reorgInfo.Job
	logutil.Logger(ddlLogCtx).Info("[ddl] start to add table index", zap.String("job", job.String()), zap.String("reorgInfo", reorgInfo.String()))
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, t, indexInfo)
	if err != nil {
		trace_util_0.Count(_index_00000, 311)
		return errors.Trace(err)
	}

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	trace_util_0.Count(_index_00000, 308)
	workerCnt := variable.GetDDLReorgWorkerCounter()
	idxWorkers := make([]*addIndexWorker, 0, workerCnt)
	defer func() {
		trace_util_0.Count(_index_00000, 312)
		closeAddIndexWorkers(idxWorkers)
	}()

	trace_util_0.Count(_index_00000, 309)
	for {
		trace_util_0.Count(_index_00000, 313)
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			trace_util_0.Count(_index_00000, 322)
			return errors.Trace(err)
		}

		// For dynamic adjust add index worker number.
		trace_util_0.Count(_index_00000, 314)
		if err := loadDDLReorgVars(w); err != nil {
			trace_util_0.Count(_index_00000, 323)
			logutil.Logger(ddlLogCtx).Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
		}
		trace_util_0.Count(_index_00000, 315)
		workerCnt = variable.GetDDLReorgWorkerCounter()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			trace_util_0.Count(_index_00000, 324)
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		trace_util_0.Count(_index_00000, 316)
		for i := len(idxWorkers); i < int(workerCnt); i++ {
			trace_util_0.Count(_index_00000, 325)
			sessCtx := newContext(reorgInfo.d.store)
			idxWorker := newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeColMap)
			idxWorker.priority = job.Priority
			idxWorkers = append(idxWorkers, idxWorker)
			go idxWorkers[i].run(reorgInfo.d)
		}
		// Shrink the worker size.
		trace_util_0.Count(_index_00000, 317)
		if len(idxWorkers) > int(workerCnt) {
			trace_util_0.Count(_index_00000, 326)
			workers := idxWorkers[workerCnt:]
			idxWorkers = idxWorkers[:workerCnt]
			closeAddIndexWorkers(workers)
		}

		trace_util_0.Count(_index_00000, 318)
		failpoint.Inject("checkIndexWorkerNum", func(val failpoint.Value) {
			trace_util_0.Count(_index_00000, 327)
			if val.(bool) {
				trace_util_0.Count(_index_00000, 328)
				num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
				if num != 0 {
					trace_util_0.Count(_index_00000, 329)
					if num > len(kvRanges) {
						trace_util_0.Count(_index_00000, 331)
						if len(idxWorkers) != len(kvRanges) {
							trace_util_0.Count(_index_00000, 332)
							failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
						}
					} else {
						trace_util_0.Count(_index_00000, 333)
						if num != len(idxWorkers) {
							trace_util_0.Count(_index_00000, 334)
							failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
						}
					}
					trace_util_0.Count(_index_00000, 330)
					TestCheckWorkerNumCh <- struct{}{}
				}
			}
		})

		trace_util_0.Count(_index_00000, 319)
		logutil.Logger(ddlLogCtx).Info("[ddl] start add index workers to reorg index", zap.Int("workerCnt", len(idxWorkers)), zap.Int("regionCnt", len(kvRanges)), zap.Int64("startHandle", startHandle), zap.Int64("endHandle", endHandle))
		remains, err := w.sendRangeTaskToWorkers(t, idxWorkers, reorgInfo, &totalAddedCount, kvRanges)
		if err != nil {
			trace_util_0.Count(_index_00000, 335)
			return errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 320)
		if len(remains) == 0 {
			trace_util_0.Count(_index_00000, 336)
			break
		}
		trace_util_0.Count(_index_00000, 321)
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			trace_util_0.Count(_index_00000, 337)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_index_00000, 310)
	return nil
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	trace_util_0.Count(_index_00000, 338)
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		trace_util_0.Count(_index_00000, 340)
		var finish bool
		for !finish {
			trace_util_0.Count(_index_00000, 341)
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				trace_util_0.Count(_index_00000, 344)
				return errCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			trace_util_0.Count(_index_00000, 342)
			err = w.addPhysicalTableIndex(p, idx, reorgInfo)
			if err != nil {
				trace_util_0.Count(_index_00000, 345)
				break
			}
			trace_util_0.Count(_index_00000, 343)
			finish, err = w.updateReorgInfo(tbl, reorgInfo)
			if err != nil {
				trace_util_0.Count(_index_00000, 346)
				return errors.Trace(err)
			}
		}
	} else {
		trace_util_0.Count(_index_00000, 347)
		{
			err = w.addPhysicalTableIndex(t.(table.PhysicalTable), idx, reorgInfo)
		}
	}
	trace_util_0.Count(_index_00000, 339)
	return errors.Trace(err)
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfo(t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	trace_util_0.Count(_index_00000, 348)
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_index_00000, 354)
		return true, nil
	}

	trace_util_0.Count(_index_00000, 349)
	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		trace_util_0.Count(_index_00000, 355)
		// Fatal error, should not run here.
		logutil.Logger(ddlLogCtx).Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 350)
	if pid == 0 {
		trace_util_0.Count(_index_00000, 356)
		// Next partition does not exist, all the job done.
		return true, nil
	}

	trace_util_0.Count(_index_00000, 351)
	start, end, err := getTableRange(reorg.d, t.GetPartition(pid), reorg.Job.SnapshotVer, reorg.Job.Priority)
	if err != nil {
		trace_util_0.Count(_index_00000, 357)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 352)
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = kv.RunInNewTxn(reorg.d.store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_index_00000, 358)
		return errors.Trace(reorg.UpdateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID))
	})
	trace_util_0.Count(_index_00000, 353)
	logutil.Logger(ddlLogCtx).Info("[ddl] job update reorgInfo", zap.Int64("jobID", reorg.Job.ID), zap.Int64("partitionTableID", pid), zap.Int64("startHandle", start), zap.Int64("endHandle", end), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	trace_util_0.Count(_index_00000, 359)
	for i, def := range defs {
		trace_util_0.Count(_index_00000, 361)
		if currentPartition == def.ID {
			trace_util_0.Count(_index_00000, 362)
			if i == len(defs)-1 {
				trace_util_0.Count(_index_00000, 364)
				return 0, nil
			}
			trace_util_0.Count(_index_00000, 363)
			return defs[i+1].ID, nil
		}
	}
	trace_util_0.Count(_index_00000, 360)
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func allocateIndexID(tblInfo *model.TableInfo) int64 {
	trace_util_0.Count(_index_00000, 365)
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h int64, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startHandle int64, endHandle int64, endIncluded bool, fn recordIterFunc) error {
	trace_util_0.Count(_index_00000, 366)
	ver := kv.Version{Ver: version}

	snap, err := store.GetSnapshot(ver)
	snap.SetPriority(priority)
	if err != nil {
		trace_util_0.Count(_index_00000, 371)
		return errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 367)
	firstKey := t.RecordKey(startHandle)

	// Calculate the exclusive upper bound
	var upperBound kv.Key
	if endIncluded {
		trace_util_0.Count(_index_00000, 372)
		if endHandle == math.MaxInt64 {
			trace_util_0.Count(_index_00000, 373)
			upperBound = t.RecordKey(endHandle).PrefixNext()
		} else {
			trace_util_0.Count(_index_00000, 374)
			{
				// PrefixNext is time costing. Try to avoid it if possible.
				upperBound = t.RecordKey(endHandle + 1)
			}
		}
	} else {
		trace_util_0.Count(_index_00000, 375)
		{
			upperBound = t.RecordKey(endHandle)
		}
	}

	trace_util_0.Count(_index_00000, 368)
	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		trace_util_0.Count(_index_00000, 376)
		return errors.Trace(err)
	}
	trace_util_0.Count(_index_00000, 369)
	defer it.Close()

	for it.Valid() {
		trace_util_0.Count(_index_00000, 377)
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			trace_util_0.Count(_index_00000, 381)
			break
		}

		trace_util_0.Count(_index_00000, 378)
		var handle int64
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			trace_util_0.Count(_index_00000, 382)
			return errors.Trace(err)
		}
		trace_util_0.Count(_index_00000, 379)
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			trace_util_0.Count(_index_00000, 383)
			return errors.Trace(err)
		}

		trace_util_0.Count(_index_00000, 380)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			trace_util_0.Count(_index_00000, 384)
			if kv.ErrNotExist.Equal(err) {
				trace_util_0.Count(_index_00000, 386)
				break
			}
			trace_util_0.Count(_index_00000, 385)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_index_00000, 370)
	return nil
}

var _index_00000 = "ddl/index.go"
