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
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// reorgCtx is for reorganization.
type reorgCtx struct {
	// doneCh is used to notify.
	// If the reorganization job is done, we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	doneCh chan error
	// rowCount is used to simulate a job's row count.
	rowCount int64
	// notifyCancelReorgJob is used to notify the backfilling goroutine if the DDL job is cancelled.
	// 0: job is not canceled.
	// 1: job is canceled.
	notifyCancelReorgJob int32
	// doneHandle is used to simulate the handle that has been processed.
	doneHandle int64
}

// newContext gets a context. It is only used for adding column in reorganization state.
func newContext(store kv.Storage) sessionctx.Context {
	trace_util_0.Count(_reorg_00000, 0)
	c := mock.NewContext()
	c.Store = store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	c.GetSessionVars().StmtCtx.TimeZone = time.UTC
	return c
}

const defaultWaitReorgTimeout = 10 * time.Second

// ReorgWaitTimeout is the timeout that wait ddl in write reorganization stage.
var ReorgWaitTimeout = 5 * time.Second

func (rc *reorgCtx) notifyReorgCancel() {
	trace_util_0.Count(_reorg_00000, 1)
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 1)
}

func (rc *reorgCtx) cleanNotifyReorgCancel() {
	trace_util_0.Count(_reorg_00000, 2)
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 0)
}

func (rc *reorgCtx) isReorgCanceled() bool {
	trace_util_0.Count(_reorg_00000, 3)
	return atomic.LoadInt32(&rc.notifyCancelReorgJob) == 1
}

func (rc *reorgCtx) setRowCount(count int64) {
	trace_util_0.Count(_reorg_00000, 4)
	atomic.StoreInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) setNextHandle(doneHandle int64) {
	trace_util_0.Count(_reorg_00000, 5)
	atomic.StoreInt64(&rc.doneHandle, doneHandle)
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	trace_util_0.Count(_reorg_00000, 6)
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndHandle() (int64, int64) {
	trace_util_0.Count(_reorg_00000, 7)
	row := atomic.LoadInt64(&rc.rowCount)
	handle := atomic.LoadInt64(&rc.doneHandle)
	return row, handle
}

func (rc *reorgCtx) clean() {
	trace_util_0.Count(_reorg_00000, 8)
	rc.setRowCount(0)
	rc.setNextHandle(0)
	rc.doneCh = nil
}

func (w *worker) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, lease time.Duration, f func() error) error {
	trace_util_0.Count(_reorg_00000, 9)
	job := reorgInfo.Job
	if w.reorgCtx.doneCh == nil {
		trace_util_0.Count(_reorg_00000, 12)
		// start a reorganization job
		w.wg.Add(1)
		w.reorgCtx.doneCh = make(chan error, 1)
		// initial reorgCtx
		w.reorgCtx.setRowCount(job.GetRowCount())
		w.reorgCtx.setNextHandle(reorgInfo.StartHandle)
		go func() {
			trace_util_0.Count(_reorg_00000, 13)
			defer w.wg.Done()
			w.reorgCtx.doneCh <- f()
		}()
	}

	trace_util_0.Count(_reorg_00000, 10)
	waitTimeout := defaultWaitReorgTimeout
	// if lease is 0, we are using a local storage,
	// and we can wait the reorganization to be done here.
	// if lease > 0, we don't need to wait here because
	// we should update some job's progress context and try checking again,
	// so we use a very little timeout here.
	if lease > 0 {
		trace_util_0.Count(_reorg_00000, 14)
		waitTimeout = ReorgWaitTimeout
	}

	// wait reorganization job done or timeout
	trace_util_0.Count(_reorg_00000, 11)
	select {
	case err := <-w.reorgCtx.doneCh:
		trace_util_0.Count(_reorg_00000, 15)
		rowCount, _ := w.reorgCtx.getRowCountAndHandle()
		logutil.Logger(ddlLogCtx).Info("[ddl] run reorg job done", zap.Int64("handled rows", rowCount))
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		w.reorgCtx.clean()
		return errors.Trace(err)
	case <-w.quitCh:
		trace_util_0.Count(_reorg_00000, 16)
		logutil.Logger(ddlLogCtx).Info("[ddl] run reorg job quit")
		w.reorgCtx.setNextHandle(0)
		w.reorgCtx.setRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		trace_util_0.Count(_reorg_00000, 17)
		rowCount, doneHandle := w.reorgCtx.getRowCountAndHandle()
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		// Update a reorgInfo's handle.
		err := t.UpdateDDLReorgStartHandle(job, doneHandle)
		logutil.Logger(ddlLogCtx).Info("[ddl] run reorg job wait timeout", zap.Duration("waitTime", waitTimeout),
			zap.Int64("totalAddedRowCount", rowCount), zap.Int64("doneHandle", doneHandle), zap.Error(err))
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func (w *worker) isReorgRunnable(d *ddlCtx) error {
	trace_util_0.Count(_reorg_00000, 18)
	if isChanClosed(w.quitCh) {
		trace_util_0.Count(_reorg_00000, 22)
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.GenWithStack("worker is closed")
	}

	trace_util_0.Count(_reorg_00000, 19)
	if w.reorgCtx.isReorgCanceled() {
		trace_util_0.Count(_reorg_00000, 23)
		// Job is cancelled. So it can't be done.
		return errCancelledDDLJob
	}

	trace_util_0.Count(_reorg_00000, 20)
	if !d.isOwner() {
		trace_util_0.Count(_reorg_00000, 24)
		// If it's not the owner, we will try later, so here just returns an error.
		logutil.Logger(ddlLogCtx).Info("[ddl] DDL worker is not the DDL owner", zap.String("ID", d.uuid))
		return errors.Trace(errNotOwner)
	}
	trace_util_0.Count(_reorg_00000, 21)
	return nil
}

type reorgInfo struct {
	*model.Job

	// StartHandle is the first handle of the adding indices table.
	StartHandle int64
	// EndHandle is the last handle of the adding indices table.
	EndHandle int64
	d         *ddlCtx
	first     bool
	// PhysicalTableID is used for partitioned table.
	// DDL reorganize for a partitioned table will handle partitions one by one,
	// PhysicalTableID is used to trace the current partition we are handling.
	// If the table is not partitioned, PhysicalTableID would be TableID.
	PhysicalTableID int64
}

func (r *reorgInfo) String() string {
	trace_util_0.Count(_reorg_00000, 25)
	return "StartHandle:" + strconv.FormatInt(r.StartHandle, 10) + "," +
		"EndHandle:" + strconv.FormatInt(r.EndHandle, 10) + "," +
		"first:" + strconv.FormatBool(r.first) + "," +
		"PhysicalTableID:" + strconv.FormatInt(r.PhysicalTableID, 10)
}

func constructDescTableScanPB(physicalTableID int64, pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	trace_util_0.Count(_reorg_00000, 26)
	tblScan := &tipb.TableScan{
		TableId: physicalTableID,
		Columns: pbColumnInfos,
		Desc:    true,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func constructLimitPB(count uint64) *tipb.Executor {
	trace_util_0.Count(_reorg_00000, 27)
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func buildDescTableScanDAG(startTS uint64, tbl table.PhysicalTable, columns []*model.ColumnInfo, limit uint64) (*tipb.DAGRequest, error) {
	trace_util_0.Count(_reorg_00000, 28)
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = startTS
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	dagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range columns {
		trace_util_0.Count(_reorg_00000, 30)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	trace_util_0.Count(_reorg_00000, 29)
	dagReq.Flags |= model.FlagInSelectStmt

	pbColumnInfos := model.ColumnsToProto(columns, tbl.Meta().PKIsHandle)
	tblScanExec := constructDescTableScanPB(tbl.GetPhysicalID(), pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)
	dagReq.Executors = append(dagReq.Executors, constructLimitPB(limit))
	return dagReq, nil
}

func getColumnsTypes(columns []*model.ColumnInfo) []*types.FieldType {
	trace_util_0.Count(_reorg_00000, 31)
	colTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		trace_util_0.Count(_reorg_00000, 33)
		colTypes = append(colTypes, &col.FieldType)
	}
	trace_util_0.Count(_reorg_00000, 32)
	return colTypes
}

// buildDescTableScan builds a desc table scan upon tblInfo.
func (d *ddlCtx) buildDescTableScan(ctx context.Context, startTS uint64, tbl table.PhysicalTable, columns []*model.ColumnInfo, limit uint64) (distsql.SelectResult, error) {
	trace_util_0.Count(_reorg_00000, 34)
	dagPB, err := buildDescTableScanDAG(startTS, tbl, columns, limit)
	if err != nil {
		trace_util_0.Count(_reorg_00000, 38)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_reorg_00000, 35)
	ranges := ranger.FullIntRange(false)
	var builder distsql.RequestBuilder
	builder.SetTableRanges(tbl.GetPhysicalID(), ranges, nil).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetConcurrency(1).SetDesc(true)

	builder.Request.NotFillCache = true
	builder.Request.Priority = kv.PriorityLow

	kvReq, err := builder.Build()
	if err != nil {
		trace_util_0.Count(_reorg_00000, 39)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_reorg_00000, 36)
	sctx := newContext(d.store)
	result, err := distsql.Select(ctx, sctx, kvReq, getColumnsTypes(columns), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		trace_util_0.Count(_reorg_00000, 40)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_reorg_00000, 37)
	result.Fetch(ctx)
	return result, nil
}

// GetTableMaxRowID gets the last row id of the table partition.
func (d *ddlCtx) GetTableMaxRowID(startTS uint64, tbl table.PhysicalTable) (maxRowID int64, emptyTable bool, err error) {
	trace_util_0.Count(_reorg_00000, 41)
	maxRowID = int64(math.MaxInt64)
	var columns []*model.ColumnInfo
	if tbl.Meta().PKIsHandle {
		trace_util_0.Count(_reorg_00000, 46)
		for _, col := range tbl.Meta().Columns {
			trace_util_0.Count(_reorg_00000, 47)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_reorg_00000, 48)
				columns = []*model.ColumnInfo{col}
				break
			}
		}
	} else {
		trace_util_0.Count(_reorg_00000, 49)
		{
			columns = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
		}
	}

	trace_util_0.Count(_reorg_00000, 42)
	ctx := context.Background()
	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the table.
	result, err := d.buildDescTableScan(ctx, startTS, tbl, columns, 1)
	if err != nil {
		trace_util_0.Count(_reorg_00000, 50)
		return maxRowID, false, errors.Trace(err)
	}
	trace_util_0.Count(_reorg_00000, 43)
	defer terror.Call(result.Close)

	chk := chunk.New(getColumnsTypes(columns), 1, 1)
	err = result.Next(ctx, chk)
	if err != nil {
		trace_util_0.Count(_reorg_00000, 51)
		return maxRowID, false, errors.Trace(err)
	}

	trace_util_0.Count(_reorg_00000, 44)
	if chk.NumRows() == 0 {
		trace_util_0.Count(_reorg_00000, 52)
		// empty table
		return maxRowID, true, nil
	}
	trace_util_0.Count(_reorg_00000, 45)
	row := chk.GetRow(0)
	maxRowID = row.GetInt64(0)
	return maxRowID, false, nil
}

// getTableRange gets the start and end handle of a table (or partition).
func getTableRange(d *ddlCtx, tbl table.PhysicalTable, snapshotVer uint64, priority int) (startHandle, endHandle int64, err error) {
	trace_util_0.Count(_reorg_00000, 53)
	startHandle = math.MinInt64
	endHandle = math.MaxInt64
	// Get the start handle of this partition.
	err = iterateSnapshotRows(d.store, priority, tbl, snapshotVer, math.MinInt64, math.MaxInt64, true,
		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
			trace_util_0.Count(_reorg_00000, 58)
			startHandle = h
			return false, nil
		})
	trace_util_0.Count(_reorg_00000, 54)
	if err != nil {
		trace_util_0.Count(_reorg_00000, 59)
		return 0, 0, errors.Trace(err)
	}
	trace_util_0.Count(_reorg_00000, 55)
	var emptyTable bool
	// Get the end handle of this partition.
	endHandle, emptyTable, err = d.GetTableMaxRowID(snapshotVer, tbl)
	if err != nil {
		trace_util_0.Count(_reorg_00000, 60)
		return 0, 0, errors.Trace(err)
	}
	trace_util_0.Count(_reorg_00000, 56)
	if endHandle < startHandle || emptyTable {
		trace_util_0.Count(_reorg_00000, 61)
		logutil.Logger(ddlLogCtx).Info("[ddl] get table range, endHandle < startHandle", zap.String("table", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("partitionID", tbl.GetPhysicalID()), zap.Int64("endHandle", endHandle), zap.Int64("startHandle", startHandle))
		endHandle = startHandle
	}
	trace_util_0.Count(_reorg_00000, 57)
	return
}

func getReorgInfo(d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table) (*reorgInfo, error) {
	trace_util_0.Count(_reorg_00000, 62)
	var (
		err   error
		start int64
		end   int64
		pid   int64
		info  reorgInfo
	)

	if job.SnapshotVer == 0 {
		trace_util_0.Count(_reorg_00000, 64)
		info.first = true
		// get the current version for reorganization if we don't have
		var ver kv.Version
		ver, err = d.store.CurrentVersion()
		if err != nil {
			trace_util_0.Count(_reorg_00000, 70)
			return nil, errors.Trace(err)
		} else {
			trace_util_0.Count(_reorg_00000, 71)
			if ver.Ver <= 0 {
				trace_util_0.Count(_reorg_00000, 72)
				return nil, errInvalidStoreVer.GenWithStack("invalid storage current version %d", ver.Ver)
			}
		}
		trace_util_0.Count(_reorg_00000, 65)
		tblInfo := tbl.Meta()
		pid = tblInfo.ID
		var tb table.PhysicalTable
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			trace_util_0.Count(_reorg_00000, 73)
			pid = pi.Definitions[0].ID
			tb = tbl.(table.PartitionedTable).GetPartition(pid)
		} else {
			trace_util_0.Count(_reorg_00000, 74)
			{
				tb = tbl.(table.PhysicalTable)
			}
		}
		trace_util_0.Count(_reorg_00000, 66)
		start, end, err = getTableRange(d, tb, ver.Ver, job.Priority)
		if err != nil {
			trace_util_0.Count(_reorg_00000, 75)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_reorg_00000, 67)
		logutil.Logger(ddlLogCtx).Info("[ddl] job get table range", zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid), zap.Int64("startHandle", start), zap.Int64("endHandle", end))

		failpoint.Inject("errorUpdateReorgHandle", func() (*reorgInfo, error) {
			trace_util_0.Count(_reorg_00000, 76)
			return &info, errors.New("occur an error when update reorg handle")
		})
		trace_util_0.Count(_reorg_00000, 68)
		err = t.UpdateDDLReorgHandle(job, start, end, pid)
		if err != nil {
			trace_util_0.Count(_reorg_00000, 77)
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		trace_util_0.Count(_reorg_00000, 69)
		job.SnapshotVer = ver.Ver
	} else {
		trace_util_0.Count(_reorg_00000, 78)
		{
			start, end, pid, err = t.GetDDLReorgHandle(job)
			if err != nil {
				trace_util_0.Count(_reorg_00000, 79)
				return nil, errors.Trace(err)
			}
		}
	}
	trace_util_0.Count(_reorg_00000, 63)
	info.Job = job
	info.d = d
	info.StartHandle = start
	info.EndHandle = end
	info.PhysicalTableID = pid

	return &info, errors.Trace(err)
}

func (r *reorgInfo) UpdateReorgMeta(txn kv.Transaction, startHandle, endHandle, physicalTableID int64) error {
	trace_util_0.Count(_reorg_00000, 80)
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, startHandle, endHandle, physicalTableID))
}

var _reorg_00000 = "ddl/reorg.go"
