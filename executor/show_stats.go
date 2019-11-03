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

package executor

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

func (e *ShowExec) fetchShowStatsMeta() error {
	trace_util_0.Count(_show_stats_00000, 0)
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		trace_util_0.Count(_show_stats_00000, 2)
		for _, tbl := range db.Tables {
			trace_util_0.Count(_show_stats_00000, 3)
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				trace_util_0.Count(_show_stats_00000, 4)
				e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				trace_util_0.Count(_show_stats_00000, 5)
				{
					for _, def := range pi.Definitions {
						trace_util_0.Count(_show_stats_00000, 6)
						e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			}
		}
	}
	trace_util_0.Count(_show_stats_00000, 1)
	return nil
}

func (e *ShowExec) appendTableForStatsMeta(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	trace_util_0.Count(_show_stats_00000, 7)
	if statsTbl.Pseudo {
		trace_util_0.Count(_show_stats_00000, 9)
		return
	}
	trace_util_0.Count(_show_stats_00000, 8)
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		e.versionToTime(statsTbl.Version),
		statsTbl.ModifyCount,
		statsTbl.Count,
	})
}

func (e *ShowExec) fetchShowStatsHistogram() error {
	trace_util_0.Count(_show_stats_00000, 10)
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		trace_util_0.Count(_show_stats_00000, 12)
		for _, tbl := range db.Tables {
			trace_util_0.Count(_show_stats_00000, 13)
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				trace_util_0.Count(_show_stats_00000, 14)
				e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				trace_util_0.Count(_show_stats_00000, 15)
				{
					for _, def := range pi.Definitions {
						trace_util_0.Count(_show_stats_00000, 16)
						e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			}
		}
	}
	trace_util_0.Count(_show_stats_00000, 11)
	return nil
}

func (e *ShowExec) appendTableForStatsHistograms(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	trace_util_0.Count(_show_stats_00000, 17)
	if statsTbl.Pseudo {
		trace_util_0.Count(_show_stats_00000, 20)
		return
	}
	trace_util_0.Count(_show_stats_00000, 18)
	for _, col := range statsTbl.Columns {
		trace_util_0.Count(_show_stats_00000, 21)
		// Pass a nil StatementContext to avoid column stats being marked as needed.
		if col.IsInvalid(nil, false) {
			trace_util_0.Count(_show_stats_00000, 23)
			continue
		}
		trace_util_0.Count(_show_stats_00000, 22)
		e.histogramToRow(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, col.AvgColSize(statsTbl.Count))
	}
	trace_util_0.Count(_show_stats_00000, 19)
	for _, idx := range statsTbl.Indices {
		trace_util_0.Count(_show_stats_00000, 24)
		e.histogramToRow(dbName, tblName, partitionName, idx.Info.Name.O, 1, idx.Histogram, 0)
	}
}

func (e *ShowExec) histogramToRow(dbName, tblName, partitionName, colName string, isIndex int, hist statistics.Histogram, avgColSize float64) {
	trace_util_0.Count(_show_stats_00000, 25)
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		colName,
		isIndex,
		e.versionToTime(hist.LastUpdateVersion),
		hist.NDV,
		hist.NullCount,
		avgColSize,
		hist.Correlation,
	})
}

func (e *ShowExec) versionToTime(version uint64) types.Time {
	trace_util_0.Count(_show_stats_00000, 26)
	t := time.Unix(0, oracle.ExtractPhysical(version)*int64(time.Millisecond))
	return types.Time{Time: types.FromGoTime(t), Type: mysql.TypeDatetime}
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	trace_util_0.Count(_show_stats_00000, 27)
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		trace_util_0.Count(_show_stats_00000, 29)
		for _, tbl := range db.Tables {
			trace_util_0.Count(_show_stats_00000, 30)
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				trace_util_0.Count(_show_stats_00000, 31)
				if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl)); err != nil {
					trace_util_0.Count(_show_stats_00000, 32)
					return err
				}
			} else {
				trace_util_0.Count(_show_stats_00000, 33)
				{
					for _, def := range pi.Definitions {
						trace_util_0.Count(_show_stats_00000, 34)
						if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
							trace_util_0.Count(_show_stats_00000, 35)
							return err
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_show_stats_00000, 28)
	return nil
}

func (e *ShowExec) appendTableForStatsBuckets(dbName, tblName, partitionName string, statsTbl *statistics.Table) error {
	trace_util_0.Count(_show_stats_00000, 36)
	if statsTbl.Pseudo {
		trace_util_0.Count(_show_stats_00000, 40)
		return nil
	}
	trace_util_0.Count(_show_stats_00000, 37)
	for _, col := range statsTbl.Columns {
		trace_util_0.Count(_show_stats_00000, 41)
		err := e.bucketsToRows(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram)
		if err != nil {
			trace_util_0.Count(_show_stats_00000, 42)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_show_stats_00000, 38)
	for _, idx := range statsTbl.Indices {
		trace_util_0.Count(_show_stats_00000, 43)
		err := e.bucketsToRows(dbName, tblName, partitionName, idx.Info.Name.O, len(idx.Info.Columns), idx.Histogram)
		if err != nil {
			trace_util_0.Count(_show_stats_00000, 44)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_show_stats_00000, 39)
	return nil
}

// bucketsToRows converts histogram buckets to rows. If the histogram is built from index, then numOfCols equals to number
// of index columns, else numOfCols is 0.
func (e *ShowExec) bucketsToRows(dbName, tblName, partitionName, colName string, numOfCols int, hist statistics.Histogram) error {
	trace_util_0.Count(_show_stats_00000, 45)
	isIndex := 0
	if numOfCols > 0 {
		trace_util_0.Count(_show_stats_00000, 48)
		isIndex = 1
	}
	trace_util_0.Count(_show_stats_00000, 46)
	for i := 0; i < hist.Len(); i++ {
		trace_util_0.Count(_show_stats_00000, 49)
		lowerBoundStr, err := statistics.ValueToString(hist.GetLower(i), numOfCols)
		if err != nil {
			trace_util_0.Count(_show_stats_00000, 52)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_stats_00000, 50)
		upperBoundStr, err := statistics.ValueToString(hist.GetUpper(i), numOfCols)
		if err != nil {
			trace_util_0.Count(_show_stats_00000, 53)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_stats_00000, 51)
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partitionName,
			colName,
			isIndex,
			i,
			hist.Buckets[i].Count,
			hist.Buckets[i].Repeat,
			lowerBoundStr,
			upperBoundStr,
		})
	}
	trace_util_0.Count(_show_stats_00000, 47)
	return nil
}

func (e *ShowExec) fetchShowStatsHealthy() {
	trace_util_0.Count(_show_stats_00000, 54)
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		trace_util_0.Count(_show_stats_00000, 55)
		for _, tbl := range db.Tables {
			trace_util_0.Count(_show_stats_00000, 56)
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				trace_util_0.Count(_show_stats_00000, 57)
				e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, "", h.GetTableStats(tbl))
			} else {
				trace_util_0.Count(_show_stats_00000, 58)
				{
					for _, def := range pi.Definitions {
						trace_util_0.Count(_show_stats_00000, 59)
						e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			}
		}
	}
}

func (e *ShowExec) appendTableForStatsHealthy(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	trace_util_0.Count(_show_stats_00000, 60)
	if statsTbl.Pseudo {
		trace_util_0.Count(_show_stats_00000, 63)
		return
	}
	trace_util_0.Count(_show_stats_00000, 61)
	var healthy int64
	if statsTbl.ModifyCount < statsTbl.Count {
		trace_util_0.Count(_show_stats_00000, 64)
		healthy = int64((1.0 - float64(statsTbl.ModifyCount)/float64(statsTbl.Count)) * 100.0)
	} else {
		trace_util_0.Count(_show_stats_00000, 65)
		if statsTbl.ModifyCount == 0 {
			trace_util_0.Count(_show_stats_00000, 66)
			healthy = 100
		}
	}
	trace_util_0.Count(_show_stats_00000, 62)
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		healthy,
	})
}

func (e *ShowExec) fetchShowAnalyzeStatus() {
	trace_util_0.Count(_show_stats_00000, 67)
	rows := infoschema.DataForAnalyzeStatus()
	for _, row := range rows {
		trace_util_0.Count(_show_stats_00000, 68)
		for i, val := range row {
			trace_util_0.Count(_show_stats_00000, 69)
			e.result.AppendDatum(i, &val)
		}
	}
}

var _show_stats_00000 = "executor/show_stats.go"
