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
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func onCreateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 0)
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		trace_util_0.Count(_table_00000, 5)
		if val.(bool) {
			trace_util_0.Count(_table_00000, 6)
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	trace_util_0.Count(_table_00000, 1)
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		trace_util_0.Count(_table_00000, 7)
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 2)
	tbInfo.State = model.StateNone
	err := checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		trace_util_0.Count(_table_00000, 8)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			trace_util_0.Count(_table_00000, 10)
			job.State = model.JobStateCancelled
		}
		trace_util_0.Count(_table_00000, 9)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 3)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_table_00000, 11)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 4)
	switch tbInfo.State {
	case model.StateNone:
		trace_util_0.Count(_table_00000, 12)
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			trace_util_0.Count(_table_00000, 15)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_table_00000, 13)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateTable, TableInfo: tbInfo})
		return ver, nil
	default:
		trace_util_0.Count(_table_00000, 14)
		return ver, ErrInvalidTableState.GenWithStack("invalid table state %v", tbInfo.State)
	}
}

func createTableOrViewWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	trace_util_0.Count(_table_00000, 16)
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 18)
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 17)
	return t.CreateTableOrView(schemaID, tbInfo)
}

func onCreateView(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 19)
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	var orReplace bool
	var oldTbInfoID int64
	if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
		trace_util_0.Count(_table_00000, 23)
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 20)
	tbInfo.State = model.StateNone
	err := checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		trace_util_0.Count(_table_00000, 24)
		if infoschema.ErrDatabaseNotExists.Equal(err) {
			trace_util_0.Count(_table_00000, 25)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		} else {
			trace_util_0.Count(_table_00000, 26)
			if infoschema.ErrTableExists.Equal(err) {
				trace_util_0.Count(_table_00000, 27)
				if !orReplace {
					trace_util_0.Count(_table_00000, 28)
					job.State = model.JobStateCancelled
					return ver, errors.Trace(err)
				}
			} else {
				trace_util_0.Count(_table_00000, 29)
				{
					return ver, errors.Trace(err)
				}
			}
		}
	}
	trace_util_0.Count(_table_00000, 21)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_table_00000, 30)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 22)
	switch tbInfo.State {
	case model.StateNone:
		trace_util_0.Count(_table_00000, 31)
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		if oldTbInfoID > 0 && orReplace {
			trace_util_0.Count(_table_00000, 35)
			err = t.DropTableOrView(schemaID, oldTbInfoID, true)
			if err != nil {
				trace_util_0.Count(_table_00000, 36)
				return ver, errors.Trace(err)
			}
		}
		trace_util_0.Count(_table_00000, 32)
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			trace_util_0.Count(_table_00000, 37)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_table_00000, 33)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateView, TableInfo: tbInfo})
		return ver, nil
	default:
		trace_util_0.Count(_table_00000, 34)
		return ver, ErrInvalidTableState.GenWithStack("invalid view state %v", tbInfo.State)
	}
}

func onDropTableOrView(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 38)
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 41)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 39)
	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		trace_util_0.Count(_table_00000, 42)
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateWriteOnly:
		trace_util_0.Count(_table_00000, 43)
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateDeleteOnly:
		trace_util_0.Count(_table_00000, 44)
		tblInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			trace_util_0.Count(_table_00000, 48)
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_table_00000, 45)
		if err = t.DropTableOrView(job.SchemaID, job.TableID, true); err != nil {
			trace_util_0.Count(_table_00000, 49)
			break
		}
		// Finish this job.
		trace_util_0.Count(_table_00000, 46)
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(job.TableID)
		job.Args = append(job.Args, startKey, getPartitionIDs(tblInfo))
	default:
		trace_util_0.Count(_table_00000, 47)
		err = ErrInvalidTableState.GenWithStack("invalid table state %v", tblInfo.State)
	}

	trace_util_0.Count(_table_00000, 40)
	return ver, errors.Trace(err)
}

const (
	recoverTableCheckFlagNone int64 = iota
	recoverTableCheckFlagEnableGC
	recoverTableCheckFlagDisableGC
)

func (w *worker) onRecoverTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_table_00000, 50)
	schemaID := job.SchemaID
	tblInfo := &model.TableInfo{}
	var autoID, dropJobID, recoverTableCheckFlag int64
	var snapshotTS uint64
	if err = job.DecodeArgs(tblInfo, &autoID, &dropJobID, &snapshotTS, &recoverTableCheckFlag); err != nil {
		trace_util_0.Count(_table_00000, 55)
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// check GC and safe point
	trace_util_0.Count(_table_00000, 51)
	gcEnable, err := checkGCEnable(w)
	if err != nil {
		trace_util_0.Count(_table_00000, 56)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 52)
	err = checkTableNotExists(d, t, schemaID, tblInfo.Name.L)
	if err != nil {
		trace_util_0.Count(_table_00000, 57)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			trace_util_0.Count(_table_00000, 59)
			job.State = model.JobStateCancelled
		}
		trace_util_0.Count(_table_00000, 58)
		return ver, errors.Trace(err)
	}

	// Recover table divide into 2 steps:
	// 1. Check GC enable status, to decided whether enable GC after recover table.
	//     a. Why not disable GC before put the job to DDL job queue?
	//        Think about concurrency problem. If a recover job-1 is doing and already disabled GC,
	//        then, another recover table job-2 check GC enable will get disable before into the job queue.
	//        then, after recover table job-2 finished, the GC will be disabled.
	//     b. Why split into 2 steps? 1 step also can finish this job: check GC -> disable GC -> recover table -> finish job.
	//        What if the transaction commit failed? then, the job will retry, but the GC already disabled when first running.
	//        So, after this job retry succeed, the GC will be disabled.
	// 2. Do recover table job.
	//     a. Check whether GC enabled, if enabled, disable GC first.
	//     b. Check GC safe point. If drop table time if after safe point time, then can do recover.
	//        otherwise, can't recover table, because the records of the table may already delete by gc.
	//     c. Remove GC task of the table from gc_delete_range table.
	//     d. Create table and rebase table auto ID.
	//     e. Finish.
	trace_util_0.Count(_table_00000, 53)
	switch tblInfo.State {
	case model.StateNone:
		trace_util_0.Count(_table_00000, 60)
		// none -> write only
		// check GC enable and update flag.
		if gcEnable {
			trace_util_0.Count(_table_00000, 70)
			job.Args[len(job.Args)-1] = recoverTableCheckFlagEnableGC
		} else {
			trace_util_0.Count(_table_00000, 71)
			{
				job.Args[len(job.Args)-1] = recoverTableCheckFlagDisableGC
			}
		}

		trace_util_0.Count(_table_00000, 61)
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, false)
		if err != nil {
			trace_util_0.Count(_table_00000, 72)
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		trace_util_0.Count(_table_00000, 62)
		// write only -> public
		// do recover table.
		if gcEnable {
			trace_util_0.Count(_table_00000, 73)
			err = disableGC(w)
			if err != nil {
				trace_util_0.Count(_table_00000, 74)
				job.State = model.JobStateCancelled
				return ver, errors.Errorf("disable gc failed, try again later. err: %v", err)
			}
		}
		// check GC safe point
		trace_util_0.Count(_table_00000, 63)
		err = checkSafePoint(w, snapshotTS)
		if err != nil {
			trace_util_0.Count(_table_00000, 75)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Remove dropped table DDL job from gc_delete_range table.
		trace_util_0.Count(_table_00000, 64)
		err = w.delRangeManager.removeFromGCDeleteRange(dropJobID, tblInfo.ID)
		if err != nil {
			trace_util_0.Count(_table_00000, 76)
			return ver, errors.Trace(err)
		}

		trace_util_0.Count(_table_00000, 65)
		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = t.StartTS
		err = t.CreateTableAndSetAutoID(schemaID, tblInfo, autoID)
		if err != nil {
			trace_util_0.Count(_table_00000, 77)
			return ver, errors.Trace(err)
		}

		trace_util_0.Count(_table_00000, 66)
		failpoint.Inject("mockRecoverTableCommitErr", func(val failpoint.Value) {
			trace_util_0.Count(_table_00000, 78)
			if val.(bool) && atomic.CompareAndSwapUint32(&mockRecoverTableCommitErrOnce, 0, 1) {
				trace_util_0.Count(_table_00000, 79)
				kv.MockCommitErrorEnable()
			}
		})

		trace_util_0.Count(_table_00000, 67)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			trace_util_0.Count(_table_00000, 80)
			return ver, errors.Trace(err)
		}

		// Finish this job.
		trace_util_0.Count(_table_00000, 68)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		trace_util_0.Count(_table_00000, 69)
		return ver, ErrInvalidTableState.GenWithStack("invalid recover table state %v", tblInfo.State)
	}
	trace_util_0.Count(_table_00000, 54)
	return ver, nil
}

// mockRecoverTableCommitErrOnce uses to make sure
// `mockRecoverTableCommitErr` only mock error once.
var mockRecoverTableCommitErrOnce uint32 = 0

func enableGC(w *worker) error {
	trace_util_0.Count(_table_00000, 81)
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_table_00000, 83)
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 82)
	defer w.sessPool.put(ctx)

	return gcutil.EnableGC(ctx)
}

func disableGC(w *worker) error {
	trace_util_0.Count(_table_00000, 84)
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_table_00000, 86)
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 85)
	defer w.sessPool.put(ctx)

	return gcutil.DisableGC(ctx)
}

func checkGCEnable(w *worker) (enable bool, err error) {
	trace_util_0.Count(_table_00000, 87)
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_table_00000, 89)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 88)
	defer w.sessPool.put(ctx)

	return gcutil.CheckGCEnable(ctx)
}

func checkSafePoint(w *worker, snapshotTS uint64) error {
	trace_util_0.Count(_table_00000, 90)
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_table_00000, 92)
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 91)
	defer w.sessPool.put(ctx)

	return gcutil.ValidateSnapshot(ctx, snapshotTS)
}

type splitableStore interface {
	SplitRegion(splitKey kv.Key) error
	SplitRegionAndScatter(splitKey kv.Key) (uint64, error)
	WaitScatterRegionFinish(regionID uint64) error
}

func splitPartitionTableRegion(store kv.Storage, pi *model.PartitionInfo) {
	trace_util_0.Count(_table_00000, 93)
	// Max partition count is 4096, should we sample and just choose some of the partition to split?
	for _, def := range pi.Definitions {
		trace_util_0.Count(_table_00000, 94)
		splitTableRegion(store, def.ID)
	}
}

func splitTableRegion(store kv.Storage, tableID int64) {
	trace_util_0.Count(_table_00000, 95)
	s, ok := store.(splitableStore)
	if !ok {
		trace_util_0.Count(_table_00000, 97)
		return
	}
	trace_util_0.Count(_table_00000, 96)
	tableStartKey := tablecodec.GenTablePrefix(tableID)
	if err := s.SplitRegion(tableStartKey); err != nil {
		trace_util_0.Count(_table_00000, 98)
		// It will be automatically split by TiKV later.
		logutil.Logger(ddlLogCtx).Warn("[ddl] split table region failed", zap.Error(err))
	}
}

func preSplitTableRegion(store kv.Storage, tblInfo *model.TableInfo, waitTableSplitFinish bool) {
	trace_util_0.Count(_table_00000, 99)
	s, ok := store.(splitableStore)
	if !ok {
		trace_util_0.Count(_table_00000, 104)
		return
	}
	trace_util_0.Count(_table_00000, 100)
	regionIDs := make([]uint64, 0, 1<<(tblInfo.PreSplitRegions-1)+len(tblInfo.Indices))

	// Example:
	// ShardRowIDBits = 5
	// PreSplitRegions = 3
	//
	// then will pre-split 2^(3-1) = 4 regions.
	//
	// in this code:
	// max   = 1 << (tblInfo.ShardRowIDBits - 1) = 1 << (5-1) = 16
	// step := int64(1 << (tblInfo.ShardRowIDBits - tblInfo.PreSplitRegions)) = 1 << (5-3) = 4;
	//
	// then split regionID is below:
	// 4  << 59 = 2305843009213693952
	// 8  << 59 = 4611686018427387904
	// 12 << 59 = 6917529027641081856
	//
	// The 4 pre-split regions range is below:
	// 0                   ~ 2305843009213693952
	// 2305843009213693952 ~ 4611686018427387904
	// 4611686018427387904 ~ 6917529027641081856
	// 6917529027641081856 ~ 9223372036854775807 ( (1 << 63) - 1 )
	//
	// And the max _tidb_rowid is 9223372036854775807, it won't be negative number.

	// Split table region.
	step := int64(1 << (tblInfo.ShardRowIDBits - tblInfo.PreSplitRegions))
	// The highest bit is the symbol bit,and alloc _tidb_rowid will always be positive number.
	// So we only need to split the region for the positive number.
	max := int64(1 << (tblInfo.ShardRowIDBits - 1))
	for p := int64(step); p < max; p += step {
		trace_util_0.Count(_table_00000, 105)
		recordID := p << (64 - tblInfo.ShardRowIDBits)
		recordPrefix := tablecodec.GenTableRecordPrefix(tblInfo.ID)
		key := tablecodec.EncodeRecordKey(recordPrefix, recordID)
		regionID, err := s.SplitRegionAndScatter(key)
		if err != nil {
			trace_util_0.Count(_table_00000, 106)
			logutil.Logger(ddlLogCtx).Warn("[ddl] pre split table region failed", zap.Int64("recordID", recordID), zap.Error(err))
		} else {
			trace_util_0.Count(_table_00000, 107)
			{
				regionIDs = append(regionIDs, regionID)
			}
		}
	}

	// Split index region.
	trace_util_0.Count(_table_00000, 101)
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_table_00000, 108)
		indexPrefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idx.ID)
		regionID, err := s.SplitRegionAndScatter(indexPrefix)
		if err != nil {
			trace_util_0.Count(_table_00000, 109)
			logutil.Logger(ddlLogCtx).Warn("[ddl] pre split table index region failed", zap.String("index", idx.Name.L), zap.Error(err))
		} else {
			trace_util_0.Count(_table_00000, 110)
			{
				regionIDs = append(regionIDs, regionID)
			}
		}
	}
	trace_util_0.Count(_table_00000, 102)
	if !waitTableSplitFinish {
		trace_util_0.Count(_table_00000, 111)
		return
	}
	trace_util_0.Count(_table_00000, 103)
	for _, regionID := range regionIDs {
		trace_util_0.Count(_table_00000, 112)
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			trace_util_0.Count(_table_00000, 113)
			logutil.Logger(ddlLogCtx).Warn("[ddl] wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
}

func getTable(store kv.Storage, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	trace_util_0.Count(_table_00000, 114)
	alloc := autoid.NewAllocator(store, tblInfo.GetDBID(schemaID), tblInfo.IsAutoIncColUnsigned())
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	return tbl, errors.Trace(err)
}

func getTableInfoAndCancelFaultJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	trace_util_0.Count(_table_00000, 115)
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 118)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 116)
	if tblInfo.State != model.StatePublic {
		trace_util_0.Count(_table_00000, 119)
		job.State = model.JobStateCancelled
		return nil, ErrInvalidTableState.GenWithStack("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	trace_util_0.Count(_table_00000, 117)
	return tblInfo, nil
}

func checkTableExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	trace_util_0.Count(_table_00000, 120)
	tableID := job.TableID
	// Check this table's database.
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		trace_util_0.Count(_table_00000, 123)
		if meta.ErrDBNotExists.Equal(err) {
			trace_util_0.Count(_table_00000, 125)
			job.State = model.JobStateCancelled
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		trace_util_0.Count(_table_00000, 124)
		return nil, errors.Trace(err)
	}

	// Check the table.
	trace_util_0.Count(_table_00000, 121)
	if tblInfo == nil {
		trace_util_0.Count(_table_00000, 126)
		job.State = model.JobStateCancelled
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}
	trace_util_0.Count(_table_00000, 122)
	return tblInfo, nil
}

// onTruncateTable delete old table meta, and creates a new table identical to old table except for table ID.
// As all the old data is encoded with old table ID, it can not be accessed any more.
// A background job will be created to delete old data.
func onTruncateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 127)
	schemaID := job.SchemaID
	tableID := job.TableID
	var newTableID int64
	err := job.DecodeArgs(&newTableID)
	if err != nil {
		trace_util_0.Count(_table_00000, 135)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 128)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 136)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 129)
	err = t.DropTableOrView(schemaID, tblInfo.ID, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 137)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 130)
	failpoint.Inject("truncateTableErr", func(val failpoint.Value) {
		trace_util_0.Count(_table_00000, 138)
		if val.(bool) {
			trace_util_0.Count(_table_00000, 139)
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after dropping table"))
		}
	})

	trace_util_0.Count(_table_00000, 131)
	var oldPartitionIDs []int64
	if tblInfo.GetPartitionInfo() != nil {
		trace_util_0.Count(_table_00000, 140)
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		err = truncateTableByReassignPartitionIDs(t, tblInfo)
		if err != nil {
			trace_util_0.Count(_table_00000, 141)
			return ver, errors.Trace(err)
		}
	}

	trace_util_0.Count(_table_00000, 132)
	tblInfo.ID = newTableID
	err = t.CreateTableOrView(schemaID, tblInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 142)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 133)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_table_00000, 143)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 134)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionTruncateTable, TableInfo: tblInfo})
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []interface{}{startKey, oldPartitionIDs}
	return ver, nil
}

func onRebaseAutoID(store kv.Storage, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 144)
	schemaID := job.SchemaID
	var newBase int64
	err := job.DecodeArgs(&newBase)
	if err != nil {
		trace_util_0.Count(_table_00000, 150)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 145)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 151)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// No need to check `newBase` again, because `RebaseAutoID` will do this check.
	trace_util_0.Count(_table_00000, 146)
	tblInfo.AutoIncID = newBase
	tbl, err := getTable(store, schemaID, tblInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 152)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	trace_util_0.Count(_table_00000, 147)
	err = tbl.RebaseAutoID(nil, tblInfo.AutoIncID-1, false)
	if err != nil {
		trace_util_0.Count(_table_00000, 153)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 148)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 154)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 149)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 155)
	var shardRowIDBits uint64
	err := job.DecodeArgs(&shardRowIDBits)
	if err != nil {
		trace_util_0.Count(_table_00000, 160)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 156)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 161)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 157)
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		trace_util_0.Count(_table_00000, 162)
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		trace_util_0.Count(_table_00000, 163)
		{
			tbl, err := getTable(d.store, job.SchemaID, tblInfo)
			if err != nil {
				trace_util_0.Count(_table_00000, 166)
				return ver, errors.Trace(err)
			}
			trace_util_0.Count(_table_00000, 164)
			err = verifyNoOverflowShardBits(w.sessPool, tbl, shardRowIDBits)
			if err != nil {
				trace_util_0.Count(_table_00000, 167)
				job.State = model.JobStateCancelled
				return ver, err
			}
			trace_util_0.Count(_table_00000, 165)
			tblInfo.ShardRowIDBits = shardRowIDBits
			// MaxShardRowIDBits use to check the overflow of auto ID.
			tblInfo.MaxShardRowIDBits = shardRowIDBits
		}
	}
	trace_util_0.Count(_table_00000, 158)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 168)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 159)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *sessionPool, tbl table.Table, shardRowIDBits uint64) error {
	trace_util_0.Count(_table_00000, 169)
	ctx, err := s.get()
	if err != nil {
		trace_util_0.Count(_table_00000, 173)
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 170)
	defer s.put(ctx)

	// Check next global max auto ID first.
	autoIncID, err := tbl.Allocator(ctx).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		trace_util_0.Count(_table_00000, 174)
		return errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 171)
	if tables.OverflowShardBits(autoIncID, shardRowIDBits) {
		trace_util_0.Count(_table_00000, 175)
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID %v overflow", shardRowIDBits, autoIncID)
	}
	trace_util_0.Count(_table_00000, 172)
	return nil
}

func onRenameTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 176)
	var oldSchemaID int64
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName); err != nil {
		trace_util_0.Count(_table_00000, 186)
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 177)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, oldSchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 187)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 178)
	newSchemaID := job.SchemaID
	err = checkTableNotExists(d, t, newSchemaID, tableName.L)
	if err != nil {
		trace_util_0.Count(_table_00000, 188)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			trace_util_0.Count(_table_00000, 190)
			job.State = model.JobStateCancelled
		}
		trace_util_0.Count(_table_00000, 189)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 179)
	var baseID int64
	shouldDelAutoID := false
	if newSchemaID != oldSchemaID {
		trace_util_0.Count(_table_00000, 191)
		shouldDelAutoID = true
		baseID, err = t.GetAutoTableID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			trace_util_0.Count(_table_00000, 193)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// It's compatible with old version.
		// TODO: Remove it.
		trace_util_0.Count(_table_00000, 192)
		tblInfo.OldSchemaID = 0
	}

	trace_util_0.Count(_table_00000, 180)
	err = t.DropTableOrView(oldSchemaID, tblInfo.ID, shouldDelAutoID)
	if err != nil {
		trace_util_0.Count(_table_00000, 194)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 181)
	failpoint.Inject("renameTableErr", func(val failpoint.Value) {
		trace_util_0.Count(_table_00000, 195)
		if val.(bool) {
			trace_util_0.Count(_table_00000, 196)
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after renaming table"))
		}
	})

	trace_util_0.Count(_table_00000, 182)
	tblInfo.Name = tableName
	err = t.CreateTableOrView(newSchemaID, tblInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 197)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// Update the table's auto-increment ID.
	trace_util_0.Count(_table_00000, 183)
	if newSchemaID != oldSchemaID {
		trace_util_0.Count(_table_00000, 198)
		_, err = t.GenAutoTableID(newSchemaID, tblInfo.ID, baseID)
		if err != nil {
			trace_util_0.Count(_table_00000, 199)
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	trace_util_0.Count(_table_00000, 184)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_table_00000, 200)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 185)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableComment(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 201)
	var comment string
	if err := job.DecodeArgs(&comment); err != nil {
		trace_util_0.Count(_table_00000, 205)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 202)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 206)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 203)
	tblInfo.Comment = comment
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 207)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 204)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 208)
	var toCharset, toCollate string
	if err := job.DecodeArgs(&toCharset, &toCollate); err != nil {
		trace_util_0.Count(_table_00000, 215)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 209)
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		trace_util_0.Count(_table_00000, 216)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 210)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 217)
		return ver, errors.Trace(err)
	}

	// double check.
	trace_util_0.Count(_table_00000, 211)
	_, err = checkAlterTableCharset(tblInfo, dbInfo, toCharset, toCollate)
	if err != nil {
		trace_util_0.Count(_table_00000, 218)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 212)
	tblInfo.Charset = toCharset
	tblInfo.Collate = toCollate
	// update column charset.
	for _, col := range tblInfo.Columns {
		trace_util_0.Count(_table_00000, 219)
		if field_types.HasCharset(&col.FieldType) {
			trace_util_0.Count(_table_00000, 220)
			col.Charset = toCharset
			col.Collate = toCollate
		} else {
			trace_util_0.Count(_table_00000, 221)
			{
				col.Charset = charset.CharsetBin
				col.Collate = charset.CharsetBin
			}
		}
	}

	trace_util_0.Count(_table_00000, 213)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 222)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 214)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, tableName string) error {
	trace_util_0.Count(_table_00000, 223)
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil {
		trace_util_0.Count(_table_00000, 227)
		return checkTableNotExistsFromStore(t, schemaID, tableName)
	}
	// Try to use memory schema info to check first.
	trace_util_0.Count(_table_00000, 224)
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		trace_util_0.Count(_table_00000, 228)
		return err
	}
	trace_util_0.Count(_table_00000, 225)
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		trace_util_0.Count(_table_00000, 229)
		return checkTableNotExistsFromInfoSchema(is, schemaID, tableName)
	}

	trace_util_0.Count(_table_00000, 226)
	return checkTableNotExistsFromStore(t, schemaID, tableName)
}

func checkTableNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, tableName string) error {
	trace_util_0.Count(_table_00000, 230)
	// Check this table's database.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		trace_util_0.Count(_table_00000, 233)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	trace_util_0.Count(_table_00000, 231)
	if is.TableExists(schema.Name, model.NewCIStr(tableName)) {
		trace_util_0.Count(_table_00000, 234)
		return infoschema.ErrTableExists.GenWithStackByArgs(tableName)
	}
	trace_util_0.Count(_table_00000, 232)
	return nil
}

func checkTableNotExistsFromStore(t *meta.Meta, schemaID int64, tableName string) error {
	trace_util_0.Count(_table_00000, 235)
	// Check this table's database.
	tables, err := t.ListTables(schemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 238)
		if meta.ErrDBNotExists.Equal(err) {
			trace_util_0.Count(_table_00000, 240)
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		trace_util_0.Count(_table_00000, 239)
		return errors.Trace(err)
	}

	// Check the table.
	trace_util_0.Count(_table_00000, 236)
	for _, tbl := range tables {
		trace_util_0.Count(_table_00000, 241)
		if tbl.Name.L == tableName {
			trace_util_0.Count(_table_00000, 242)
			return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
		}
	}

	trace_util_0.Count(_table_00000, 237)
	return nil
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	trace_util_0.Count(_table_00000, 243)
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 245)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 244)
	return updateVersionAndTableInfo(t, job, tblInfo, shouldUpdateVer)

}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	trace_util_0.Count(_table_00000, 246)
	if shouldUpdateVer {
		trace_util_0.Count(_table_00000, 249)
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			trace_util_0.Count(_table_00000, 250)
			return 0, errors.Trace(err)
		}
	}

	trace_util_0.Count(_table_00000, 247)
	if tblInfo.State == model.StatePublic {
		trace_util_0.Count(_table_00000, 251)
		tblInfo.UpdateTS = t.StartTS
	}
	trace_util_0.Count(_table_00000, 248)
	return ver, t.UpdateTable(job.SchemaID, tblInfo)
}

// TODO: It may have the issue when two clients concurrently add partitions to a table.
func onAddTablePartition(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_table_00000, 252)
	partInfo := &model.PartitionInfo{}
	err := job.DecodeArgs(&partInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 259)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 253)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_table_00000, 260)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_table_00000, 254)
	err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
	if err != nil {
		trace_util_0.Count(_table_00000, 261)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 255)
	err = checkAddPartitionValue(tblInfo, partInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 262)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 256)
	err = checkPartitionNameUnique(tblInfo, partInfo)
	if err != nil {
		trace_util_0.Count(_table_00000, 263)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_table_00000, 257)
	updatePartitionInfo(partInfo, tblInfo)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_table_00000, 264)
		return ver, errors.Trace(err)
	}
	// Finish this job.
	trace_util_0.Count(_table_00000, 258)
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, errors.Trace(err)
}

func updatePartitionInfo(partitionInfo *model.PartitionInfo, tblInfo *model.TableInfo) {
	trace_util_0.Count(_table_00000, 265)
	parInfo := &model.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, partitionInfo.Definitions
	parInfo.Definitions = make([]model.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	trace_util_0.Count(_table_00000, 266)
	if meta.Partition.Type == model.PartitionTypeRange && len(meta.Partition.Columns) == 0 {
		trace_util_0.Count(_table_00000, 268)
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			trace_util_0.Count(_table_00000, 271)
			return errors.Trace(ErrPartitionMaxvalue)
		}

		trace_util_0.Count(_table_00000, 269)
		currentRangeValue, err := strconv.Atoi(rangeValue)
		if err != nil {
			trace_util_0.Count(_table_00000, 272)
			return errors.Trace(err)
		}

		trace_util_0.Count(_table_00000, 270)
		for i := 0; i < len(newDefs); i++ {
			trace_util_0.Count(_table_00000, 273)
			ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
			if ifMaxvalue && i == len(newDefs)-1 {
				trace_util_0.Count(_table_00000, 277)
				return nil
			} else {
				trace_util_0.Count(_table_00000, 278)
				if ifMaxvalue && i != len(newDefs)-1 {
					trace_util_0.Count(_table_00000, 279)
					return errors.Trace(ErrPartitionMaxvalue)
				}
			}

			trace_util_0.Count(_table_00000, 274)
			nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
			if err != nil {
				trace_util_0.Count(_table_00000, 280)
				return errors.Trace(err)
			}
			trace_util_0.Count(_table_00000, 275)
			if nextRangeValue <= currentRangeValue {
				trace_util_0.Count(_table_00000, 281)
				return errors.Trace(ErrRangeNotIncreasing)
			}
			trace_util_0.Count(_table_00000, 276)
			currentRangeValue = nextRangeValue
		}
	}
	trace_util_0.Count(_table_00000, 267)
	return nil
}

var _table_00000 = "ddl/table.go"
