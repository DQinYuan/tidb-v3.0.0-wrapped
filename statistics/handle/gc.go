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

package handle

import (
	"context"
	"fmt"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/sqlexec"
)

// GCStats will garbage collect the useless stats info. For dropped tables, we will first update their version so that
// other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) error {
	trace_util_0.Count(_gc_00000, 0)
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.MaxInt64(int64(h.Lease()), int64(ddlLease))
	offset := DurationToTS(10 * time.Duration(lease))
	if h.LastUpdateVersion() < offset {
		trace_util_0.Count(_gc_00000, 4)
		return nil
	}
	trace_util_0.Count(_gc_00000, 1)
	sql := fmt.Sprintf("select table_id from mysql.stats_meta where version < %d", h.LastUpdateVersion()-offset)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_gc_00000, 5)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 2)
	for _, row := range rows {
		trace_util_0.Count(_gc_00000, 6)
		if err := h.gcTableStats(is, row.GetInt64(0)); err != nil {
			trace_util_0.Count(_gc_00000, 7)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_gc_00000, 3)
	return nil
}

func (h *Handle) gcTableStats(is infoschema.InfoSchema, physicalID int64) error {
	trace_util_0.Count(_gc_00000, 8)
	sql := fmt.Sprintf("select is_index, hist_id from mysql.stats_histograms where table_id = %d", physicalID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_gc_00000, 13)
		return errors.Trace(err)
	}
	// The table has already been deleted in stats and acknowledged to all tidb,
	// we can safely remove the meta info now.
	trace_util_0.Count(_gc_00000, 9)
	if len(rows) == 0 {
		trace_util_0.Count(_gc_00000, 14)
		sql := fmt.Sprintf("delete from mysql.stats_meta where table_id = %d", physicalID)
		_, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 10)
	h.mu.Lock()
	tbl, ok := h.getTableByPhysicalID(is, physicalID)
	h.mu.Unlock()
	if !ok {
		trace_util_0.Count(_gc_00000, 15)
		return errors.Trace(h.DeleteTableStatsFromKV(physicalID))
	}
	trace_util_0.Count(_gc_00000, 11)
	tblInfo := tbl.Meta()
	for _, row := range rows {
		trace_util_0.Count(_gc_00000, 16)
		isIndex, histID := row.GetInt64(0), row.GetInt64(1)
		find := false
		if isIndex == 1 {
			trace_util_0.Count(_gc_00000, 18)
			for _, idx := range tblInfo.Indices {
				trace_util_0.Count(_gc_00000, 19)
				if idx.ID == histID {
					trace_util_0.Count(_gc_00000, 20)
					find = true
					break
				}
			}
		} else {
			trace_util_0.Count(_gc_00000, 21)
			{
				for _, col := range tblInfo.Columns {
					trace_util_0.Count(_gc_00000, 22)
					if col.ID == histID {
						trace_util_0.Count(_gc_00000, 23)
						find = true
						break
					}
				}
			}
		}
		trace_util_0.Count(_gc_00000, 17)
		if !find {
			trace_util_0.Count(_gc_00000, 24)
			if err := h.deleteHistStatsFromKV(physicalID, histID, int(isIndex)); err != nil {
				trace_util_0.Count(_gc_00000, 25)
				return errors.Trace(err)
			}
		}
	}
	trace_util_0.Count(_gc_00000, 12)
	return nil
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	trace_util_0.Count(_gc_00000, 26)
	h.mu.Lock()
	defer h.mu.Unlock()

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		trace_util_0.Count(_gc_00000, 33)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 27)
	defer func() {
		trace_util_0.Count(_gc_00000, 34)
		err = finishTransaction(context.Background(), exec, err)
	}()
	trace_util_0.Count(_gc_00000, 28)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_gc_00000, 35)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 29)
	startTS := txn.StartTS()
	// First of all, we update the version. If this table doesn't exist, it won't have any problem. Because we cannot delete anything.
	_, err = exec.Execute(context.Background(), fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", startTS, physicalID))
	if err != nil {
		trace_util_0.Count(_gc_00000, 36)
		return
	}
	// delete histogram meta
	trace_util_0.Count(_gc_00000, 30)
	_, err = exec.Execute(context.Background(), fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	if err != nil {
		trace_util_0.Count(_gc_00000, 37)
		return
	}
	// delete top n data
	trace_util_0.Count(_gc_00000, 31)
	_, err = exec.Execute(context.Background(), fmt.Sprintf("delete from mysql.stats_top_n where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	if err != nil {
		trace_util_0.Count(_gc_00000, 38)
		return
	}
	// delete all buckets
	trace_util_0.Count(_gc_00000, 32)
	_, err = exec.Execute(context.Background(), fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	return
}

// DeleteTableStatsFromKV deletes table statistics from kv.
func (h *Handle) DeleteTableStatsFromKV(physicalID int64) (err error) {
	trace_util_0.Count(_gc_00000, 39)
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		trace_util_0.Count(_gc_00000, 45)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 40)
	defer func() {
		trace_util_0.Count(_gc_00000, 46)
		err = finishTransaction(context.Background(), exec, err)
	}()
	trace_util_0.Count(_gc_00000, 41)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_gc_00000, 47)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gc_00000, 42)
	startTS := txn.StartTS()
	// We only update the version so that other tidb will know that this table is deleted.
	sql := fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", startTS, physicalID)
	_, err = exec.Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_gc_00000, 48)
		return
	}
	trace_util_0.Count(_gc_00000, 43)
	_, err = exec.Execute(context.Background(), fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d", physicalID))
	if err != nil {
		trace_util_0.Count(_gc_00000, 49)
		return
	}
	trace_util_0.Count(_gc_00000, 44)
	_, err = exec.Execute(context.Background(), fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d", physicalID))
	return
}

var _gc_00000 = "statistics/handle/gc.go"
