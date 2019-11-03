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

package handle

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

// HandleDDLEvent begins to process a ddl task.
func (h *Handle) HandleDDLEvent(t *util.Event) error {
	trace_util_0.Count(_ddl_00000, 0)
	switch t.Tp {
	case model.ActionCreateTable, model.ActionTruncateTable:
		trace_util_0.Count(_ddl_00000, 2)
		ids := getPhysicalIDs(t.TableInfo)
		for _, id := range ids {
			trace_util_0.Count(_ddl_00000, 4)
			if err := h.insertTableStats2KV(t.TableInfo, id); err != nil {
				trace_util_0.Count(_ddl_00000, 5)
				return err
			}
		}
	case model.ActionAddColumn:
		trace_util_0.Count(_ddl_00000, 3)
		ids := getPhysicalIDs(t.TableInfo)
		for _, id := range ids {
			trace_util_0.Count(_ddl_00000, 6)
			if err := h.insertColStats2KV(id, t.ColumnInfo); err != nil {
				trace_util_0.Count(_ddl_00000, 7)
				return err
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 1)
	return nil
}

func getPhysicalIDs(tblInfo *model.TableInfo) []int64 {
	trace_util_0.Count(_ddl_00000, 8)
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_ddl_00000, 11)
		return []int64{tblInfo.ID}
	}
	trace_util_0.Count(_ddl_00000, 9)
	ids := make([]int64, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		trace_util_0.Count(_ddl_00000, 12)
		ids = append(ids, def.ID)
	}
	trace_util_0.Count(_ddl_00000, 10)
	return ids
}

// DDLEventCh returns ddl events channel in handle.
func (h *Handle) DDLEventCh() chan *util.Event {
	trace_util_0.Count(_ddl_00000, 13)
	return h.ddlEventCh
}

// insertTableStats2KV inserts a record standing for a new table to stats_meta and inserts some records standing for the
// new columns and indices which belong to this table.
func (h *Handle) insertTableStats2KV(info *model.TableInfo, physicalID int64) (err error) {
	trace_util_0.Count(_ddl_00000, 14)
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		trace_util_0.Count(_ddl_00000, 21)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_00000, 15)
	defer func() {
		trace_util_0.Count(_ddl_00000, 22)
		err = finishTransaction(context.Background(), exec, err)
	}()
	trace_util_0.Count(_ddl_00000, 16)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 23)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_00000, 17)
	startTS := txn.StartTS()
	_, err = exec.Execute(context.Background(), fmt.Sprintf("insert into mysql.stats_meta (version, table_id) values(%d, %d)", startTS, physicalID))
	if err != nil {
		trace_util_0.Count(_ddl_00000, 24)
		return
	}
	trace_util_0.Count(_ddl_00000, 18)
	for _, col := range info.Columns {
		trace_util_0.Count(_ddl_00000, 25)
		_, err = exec.Execute(context.Background(), fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%d, 0, %d, 0, %d)", physicalID, col.ID, startTS))
		if err != nil {
			trace_util_0.Count(_ddl_00000, 26)
			return
		}
	}
	trace_util_0.Count(_ddl_00000, 19)
	for _, idx := range info.Indices {
		trace_util_0.Count(_ddl_00000, 27)
		_, err = exec.Execute(context.Background(), fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%d, 1, %d, 0, %d)", physicalID, idx.ID, startTS))
		if err != nil {
			trace_util_0.Count(_ddl_00000, 28)
			return
		}
	}
	trace_util_0.Count(_ddl_00000, 20)
	return
}

// insertColStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also updates version.
func (h *Handle) insertColStats2KV(physicalID int64, colInfo *model.ColumnInfo) (err error) {
	trace_util_0.Count(_ddl_00000, 29)
	h.mu.Lock()
	defer h.mu.Unlock()

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		trace_util_0.Count(_ddl_00000, 35)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_00000, 30)
	defer func() {
		trace_util_0.Count(_ddl_00000, 36)
		err = finishTransaction(context.Background(), exec, err)
	}()
	trace_util_0.Count(_ddl_00000, 31)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 37)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_00000, 32)
	startTS := txn.StartTS()
	// First of all, we update the version.
	_, err = exec.Execute(context.Background(), fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", startTS, physicalID))
	if err != nil {
		trace_util_0.Count(_ddl_00000, 38)
		return
	}
	trace_util_0.Count(_ddl_00000, 33)
	ctx := context.TODO()
	// If we didn't update anything by last SQL, it means the stats of this table does not exist.
	if h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
		trace_util_0.Count(_ddl_00000, 39)
		// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
		var rs []sqlexec.RecordSet
		rs, err = exec.Execute(ctx, fmt.Sprintf("select count from mysql.stats_meta where table_id = %d", physicalID))
		if len(rs) > 0 {
			trace_util_0.Count(_ddl_00000, 44)
			defer terror.Call(rs[0].Close)
		}
		trace_util_0.Count(_ddl_00000, 40)
		if err != nil {
			trace_util_0.Count(_ddl_00000, 45)
			return
		}
		trace_util_0.Count(_ddl_00000, 41)
		req := rs[0].NewRecordBatch()
		err = rs[0].Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_ddl_00000, 46)
			return
		}
		trace_util_0.Count(_ddl_00000, 42)
		count := req.GetRow(0).GetInt64(0)
		value := types.NewDatum(colInfo.OriginDefaultValue)
		value, err = value.ConvertTo(h.mu.ctx.GetSessionVars().StmtCtx, &colInfo.FieldType)
		if err != nil {
			trace_util_0.Count(_ddl_00000, 47)
			return
		}
		trace_util_0.Count(_ddl_00000, 43)
		if value.IsNull() {
			trace_util_0.Count(_ddl_00000, 48)
			// If the adding column has default value null, all the existing rows have null value on the newly added column.
			_, err = exec.Execute(ctx, fmt.Sprintf("insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, null_count) values (%d, %d, 0, %d, 0, %d)", startTS, physicalID, colInfo.ID, count))
			if err != nil {
				trace_util_0.Count(_ddl_00000, 49)
				return
			}
		} else {
			trace_util_0.Count(_ddl_00000, 50)
			{
				// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
				_, err = exec.Execute(ctx, fmt.Sprintf("insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, tot_col_size) values (%d, %d, 0, %d, 1, %d)", startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count))
				if err != nil {
					trace_util_0.Count(_ddl_00000, 53)
					return
				}
				trace_util_0.Count(_ddl_00000, 51)
				value, err = value.ConvertTo(h.mu.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
				if err != nil {
					trace_util_0.Count(_ddl_00000, 54)
					return
				}
				// There must be only one bucket for this new column and the value is the default value.
				trace_util_0.Count(_ddl_00000, 52)
				_, err = exec.Execute(ctx, fmt.Sprintf("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound) values (%d, 0, %d, 0, %d, %d, X'%X', X'%X')", physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes()))
				if err != nil {
					trace_util_0.Count(_ddl_00000, 55)
					return
				}
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 34)
	return
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.SQLExecutor, err error) error {
	trace_util_0.Count(_ddl_00000, 56)
	if err == nil {
		trace_util_0.Count(_ddl_00000, 58)
		_, err = exec.Execute(ctx, "commit")
	} else {
		trace_util_0.Count(_ddl_00000, 59)
		{
			_, err1 := exec.Execute(ctx, "rollback")
			terror.Log(errors.Trace(err1))
		}
	}
	trace_util_0.Count(_ddl_00000, 57)
	return errors.Trace(err)
}

var _ddl_00000 = "statistics/handle/ddl.go"
