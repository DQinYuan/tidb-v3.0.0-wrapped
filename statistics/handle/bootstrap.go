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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

func (h *Handle) initStatsMeta4Chunk(is infoschema.InfoSchema, tables StatsCache, iter *chunk.Iterator4Chunk) {
	trace_util_0.Count(_bootstrap_00000, 0)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		trace_util_0.Count(_bootstrap_00000, 1)
		physicalID := row.GetInt64(1)
		table, ok := h.getTableByPhysicalID(is, physicalID)
		if !ok {
			trace_util_0.Count(_bootstrap_00000, 3)
			logutil.Logger(context.Background()).Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			continue
		}
		trace_util_0.Count(_bootstrap_00000, 2)
		tableInfo := table.Meta()
		newHistColl := statistics.HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Count:          row.GetInt64(3),
			ModifyCount:    row.GetInt64(2),
			Columns:        make(map[int64]*statistics.Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*statistics.Index, len(tableInfo.Indices)),
		}
		tbl := &statistics.Table{
			HistColl: newHistColl,
			Version:  row.GetUint64(0),
			Name:     getFullTableName(is, tableInfo),
		}
		tables[physicalID] = tbl
	}
}

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (StatsCache, error) {
	trace_util_0.Count(_bootstrap_00000, 4)
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select HIGH_PRIORITY version, table_id, modify_count, count from mysql.stats_meta"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		trace_util_0.Count(_bootstrap_00000, 8)
		defer terror.Call(rc[0].Close)
	}
	trace_util_0.Count(_bootstrap_00000, 5)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 9)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 6)
	tables := StatsCache{}
	req := rc[0].NewRecordBatch()
	iter := chunk.NewIterator4Chunk(req.Chunk)
	for {
		trace_util_0.Count(_bootstrap_00000, 10)
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 13)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_bootstrap_00000, 11)
		if req.NumRows() == 0 {
			trace_util_0.Count(_bootstrap_00000, 14)
			break
		}
		trace_util_0.Count(_bootstrap_00000, 12)
		h.initStatsMeta4Chunk(is, tables, iter)
	}
	trace_util_0.Count(_bootstrap_00000, 7)
	return tables, nil
}

func (h *Handle) initStatsHistograms4Chunk(is infoschema.InfoSchema, tables StatsCache, iter *chunk.Iterator4Chunk) {
	trace_util_0.Count(_bootstrap_00000, 15)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		trace_util_0.Count(_bootstrap_00000, 16)
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			trace_util_0.Count(_bootstrap_00000, 18)
			continue
		}
		trace_util_0.Count(_bootstrap_00000, 17)
		id, ndv, nullCount, version, totColSize := row.GetInt64(2), row.GetInt64(3), row.GetInt64(5), row.GetUint64(4), row.GetInt64(7)
		tbl, _ := h.getTableByPhysicalID(is, table.PhysicalID)
		if row.GetInt64(1) > 0 {
			trace_util_0.Count(_bootstrap_00000, 19)
			var idxInfo *model.IndexInfo
			for _, idx := range tbl.Meta().Indices {
				trace_util_0.Count(_bootstrap_00000, 23)
				if idx.ID == id {
					trace_util_0.Count(_bootstrap_00000, 24)
					idxInfo = idx
					break
				}
			}
			trace_util_0.Count(_bootstrap_00000, 20)
			if idxInfo == nil {
				trace_util_0.Count(_bootstrap_00000, 25)
				continue
			}
			trace_util_0.Count(_bootstrap_00000, 21)
			cms, err := statistics.LoadCMSketchWithTopN(h.restrictedExec, row.GetInt64(0), row.GetInt64(1), row.GetInt64(2), row.GetBytes(6))
			if err != nil {
				trace_util_0.Count(_bootstrap_00000, 26)
				cms = nil
				terror.Log(errors.Trace(err))
			}
			trace_util_0.Count(_bootstrap_00000, 22)
			hist := statistics.NewHistogram(id, ndv, nullCount, version, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)
			table.Indices[hist.ID] = &statistics.Index{Histogram: *hist, CMSketch: cms, Info: idxInfo, StatsVer: row.GetInt64(8), Flag: row.GetInt64(10), LastAnalyzePos: row.GetDatum(11, types.NewFieldType(mysql.TypeBlob))}
		} else {
			trace_util_0.Count(_bootstrap_00000, 27)
			{
				var colInfo *model.ColumnInfo
				for _, col := range tbl.Meta().Columns {
					trace_util_0.Count(_bootstrap_00000, 30)
					if col.ID == id {
						trace_util_0.Count(_bootstrap_00000, 31)
						colInfo = col
						break
					}
				}
				trace_util_0.Count(_bootstrap_00000, 28)
				if colInfo == nil {
					trace_util_0.Count(_bootstrap_00000, 32)
					continue
				}
				trace_util_0.Count(_bootstrap_00000, 29)
				hist := statistics.NewHistogram(id, ndv, nullCount, version, &colInfo.FieldType, 0, totColSize)
				hist.Correlation = row.GetFloat64(9)
				table.Columns[hist.ID] = &statistics.Column{
					Histogram:      *hist,
					PhysicalID:     table.PhysicalID,
					Info:           colInfo,
					Count:          nullCount,
					IsHandle:       tbl.Meta().PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
					Flag:           row.GetInt64(10),
					LastAnalyzePos: row.GetDatum(11, types.NewFieldType(mysql.TypeBlob)),
				}
			}
		}
	}
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, tables StatsCache) error {
	trace_util_0.Count(_bootstrap_00000, 33)
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		trace_util_0.Count(_bootstrap_00000, 37)
		defer terror.Call(rc[0].Close)
	}
	trace_util_0.Count(_bootstrap_00000, 34)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 38)
		return errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 35)
	req := rc[0].NewRecordBatch()
	iter := chunk.NewIterator4Chunk(req.Chunk)
	for {
		trace_util_0.Count(_bootstrap_00000, 39)
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 42)
			return errors.Trace(err)
		}
		trace_util_0.Count(_bootstrap_00000, 40)
		if req.NumRows() == 0 {
			trace_util_0.Count(_bootstrap_00000, 43)
			break
		}
		trace_util_0.Count(_bootstrap_00000, 41)
		h.initStatsHistograms4Chunk(is, tables, iter)
	}
	trace_util_0.Count(_bootstrap_00000, 36)
	return nil
}

func initStatsBuckets4Chunk(ctx sessionctx.Context, tables StatsCache, iter *chunk.Iterator4Chunk) {
	trace_util_0.Count(_bootstrap_00000, 44)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		trace_util_0.Count(_bootstrap_00000, 45)
		tableID, isIndex, histID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
		table, ok := tables[tableID]
		if !ok {
			trace_util_0.Count(_bootstrap_00000, 48)
			continue
		}
		trace_util_0.Count(_bootstrap_00000, 46)
		var lower, upper types.Datum
		var hist *statistics.Histogram
		if isIndex > 0 {
			trace_util_0.Count(_bootstrap_00000, 49)
			index, ok := table.Indices[histID]
			if !ok {
				trace_util_0.Count(_bootstrap_00000, 51)
				continue
			}
			trace_util_0.Count(_bootstrap_00000, 50)
			hist = &index.Histogram
			lower, upper = types.NewBytesDatum(row.GetBytes(5)), types.NewBytesDatum(row.GetBytes(6))
		} else {
			trace_util_0.Count(_bootstrap_00000, 52)
			{
				column, ok := table.Columns[histID]
				if !ok {
					trace_util_0.Count(_bootstrap_00000, 56)
					continue
				}
				trace_util_0.Count(_bootstrap_00000, 53)
				column.Count += row.GetInt64(3)
				if !mysql.HasPriKeyFlag(column.Info.Flag) {
					trace_util_0.Count(_bootstrap_00000, 57)
					continue
				}
				trace_util_0.Count(_bootstrap_00000, 54)
				hist = &column.Histogram
				d := types.NewBytesDatum(row.GetBytes(5))
				var err error
				lower, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
				if err != nil {
					trace_util_0.Count(_bootstrap_00000, 58)
					logutil.Logger(context.Background()).Debug("decode bucket lower bound failed", zap.Error(err))
					delete(table.Columns, histID)
					continue
				}
				trace_util_0.Count(_bootstrap_00000, 55)
				d = types.NewBytesDatum(row.GetBytes(6))
				upper, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
				if err != nil {
					trace_util_0.Count(_bootstrap_00000, 59)
					logutil.Logger(context.Background()).Debug("decode bucket upper bound failed", zap.Error(err))
					delete(table.Columns, histID)
					continue
				}
			}
		}
		trace_util_0.Count(_bootstrap_00000, 47)
		hist.AppendBucket(&lower, &upper, row.GetInt64(3), row.GetInt64(4))
	}
}

func (h *Handle) initStatsBuckets(tables StatsCache) error {
	trace_util_0.Count(_bootstrap_00000, 60)
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets order by table_id, is_index, hist_id, bucket_id"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		trace_util_0.Count(_bootstrap_00000, 65)
		defer terror.Call(rc[0].Close)
	}
	trace_util_0.Count(_bootstrap_00000, 61)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 66)
		return errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 62)
	req := rc[0].NewRecordBatch()
	iter := chunk.NewIterator4Chunk(req.Chunk)
	for {
		trace_util_0.Count(_bootstrap_00000, 67)
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 70)
			return errors.Trace(err)
		}
		trace_util_0.Count(_bootstrap_00000, 68)
		if req.NumRows() == 0 {
			trace_util_0.Count(_bootstrap_00000, 71)
			break
		}
		trace_util_0.Count(_bootstrap_00000, 69)
		initStatsBuckets4Chunk(h.mu.ctx, tables, iter)
	}
	trace_util_0.Count(_bootstrap_00000, 63)
	for _, table := range tables {
		trace_util_0.Count(_bootstrap_00000, 72)
		if h.mu.lastVersion < table.Version {
			trace_util_0.Count(_bootstrap_00000, 75)
			h.mu.lastVersion = table.Version
		}
		trace_util_0.Count(_bootstrap_00000, 73)
		for _, idx := range table.Indices {
			trace_util_0.Count(_bootstrap_00000, 76)
			for i := 1; i < idx.Len(); i++ {
				trace_util_0.Count(_bootstrap_00000, 78)
				idx.Buckets[i].Count += idx.Buckets[i-1].Count
			}
			trace_util_0.Count(_bootstrap_00000, 77)
			idx.PreCalculateScalar()
		}
		trace_util_0.Count(_bootstrap_00000, 74)
		for _, col := range table.Columns {
			trace_util_0.Count(_bootstrap_00000, 79)
			for i := 1; i < col.Len(); i++ {
				trace_util_0.Count(_bootstrap_00000, 81)
				col.Buckets[i].Count += col.Buckets[i-1].Count
			}
			trace_util_0.Count(_bootstrap_00000, 80)
			col.PreCalculateScalar()
		}
	}
	trace_util_0.Count(_bootstrap_00000, 64)
	return nil
}

// InitStats will init the stats cache using full load strategy.
func (h *Handle) InitStats(is infoschema.InfoSchema) error {
	trace_util_0.Count(_bootstrap_00000, 82)
	tables, err := h.initStatsMeta(is)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 86)
		return errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 83)
	err = h.initStatsHistograms(is, tables)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 87)
		return errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 84)
	err = h.initStatsBuckets(tables)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 88)
		return errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 85)
	h.StatsCache.Store(tables)
	return nil
}

func getFullTableName(is infoschema.InfoSchema, tblInfo *model.TableInfo) string {
	trace_util_0.Count(_bootstrap_00000, 89)
	for _, schema := range is.AllSchemas() {
		trace_util_0.Count(_bootstrap_00000, 91)
		if t, err := is.TableByName(schema.Name, tblInfo.Name); err == nil {
			trace_util_0.Count(_bootstrap_00000, 92)
			if t.Meta().ID == tblInfo.ID {
				trace_util_0.Count(_bootstrap_00000, 93)
				return schema.Name.O + "." + tblInfo.Name.O
			}
		}
	}
	trace_util_0.Count(_bootstrap_00000, 90)
	return fmt.Sprintf("%d", tblInfo.ID)
}

var _bootstrap_00000 = "statistics/handle/bootstrap.go"
