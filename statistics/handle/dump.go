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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"table_name"`
	Columns      map[string]*jsonColumn `json:"columns"`
	Indices      map[string]*jsonColumn `json:"indices"`
	Count        int64                  `json:"count"`
	ModifyCount  int64                  `json:"modify_count"`
	Partitions   map[string]*JSONTable  `json:"partitions"`
}

type jsonColumn struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	NullCount         int64           `json:"null_count"`
	TotColSize        int64           `json:"tot_col_size"`
	LastUpdateVersion uint64          `json:"last_update_version"`
	Correlation       float64         `json:"correlation"`
}

func dumpJSONCol(hist *statistics.Histogram, CMSketch *statistics.CMSketch) *jsonColumn {
	trace_util_0.Count(_dump_00000, 0)
	jsonCol := &jsonColumn{
		Histogram:         statistics.HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotColSize:        hist.TotColSize,
		LastUpdateVersion: hist.LastUpdateVersion,
		Correlation:       hist.Correlation,
	}
	if CMSketch != nil {
		trace_util_0.Count(_dump_00000, 2)
		jsonCol.CMSketch = statistics.CMSketchToProto(CMSketch)
	}
	trace_util_0.Count(_dump_00000, 1)
	return jsonCol
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo, historyStatsExec sqlexec.RestrictedSQLExecutor) (*JSONTable, error) {
	trace_util_0.Count(_dump_00000, 3)
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_dump_00000, 6)
		return h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID, historyStatsExec)
	}
	trace_util_0.Count(_dump_00000, 4)
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		trace_util_0.Count(_dump_00000, 7)
		tbl, err := h.tableStatsToJSON(dbName, tableInfo, def.ID, historyStatsExec)
		if err != nil {
			trace_util_0.Count(_dump_00000, 10)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_dump_00000, 8)
		if tbl == nil {
			trace_util_0.Count(_dump_00000, 11)
			continue
		}
		trace_util_0.Count(_dump_00000, 9)
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	trace_util_0.Count(_dump_00000, 5)
	return jsonTbl, nil
}

func (h *Handle) tableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, historyStatsExec sqlexec.RestrictedSQLExecutor) (*JSONTable, error) {
	trace_util_0.Count(_dump_00000, 12)
	tbl, err := h.tableStatsFromStorage(tableInfo, physicalID, true, historyStatsExec)
	if err != nil || tbl == nil {
		trace_util_0.Count(_dump_00000, 17)
		return nil, err
	}
	trace_util_0.Count(_dump_00000, 13)
	tbl.Version, tbl.ModifyCount, tbl.Count, err = h.statsMetaByTableIDFromStorage(physicalID, historyStatsExec)
	if err != nil {
		trace_util_0.Count(_dump_00000, 18)
		return nil, err
	}
	trace_util_0.Count(_dump_00000, 14)
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*jsonColumn, len(tbl.Columns)),
		Indices:      make(map[string]*jsonColumn, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
	}

	for _, col := range tbl.Columns {
		trace_util_0.Count(_dump_00000, 19)
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		hist, err := col.ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			trace_util_0.Count(_dump_00000, 21)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_dump_00000, 20)
		jsonTbl.Columns[col.Info.Name.L] = dumpJSONCol(hist, col.CMSketch)
	}

	trace_util_0.Count(_dump_00000, 15)
	for _, idx := range tbl.Indices {
		trace_util_0.Count(_dump_00000, 22)
		jsonTbl.Indices[idx.Info.Name.L] = dumpJSONCol(&idx.Histogram, idx.CMSketch)
	}
	trace_util_0.Count(_dump_00000, 16)
	return jsonTbl, nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSON(is infoschema.InfoSchema, jsonTbl *JSONTable) error {
	trace_util_0.Count(_dump_00000, 23)
	table, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		trace_util_0.Count(_dump_00000, 26)
		return errors.Trace(err)
	}
	trace_util_0.Count(_dump_00000, 24)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_dump_00000, 27)
		err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			trace_util_0.Count(_dump_00000, 28)
			return errors.Trace(err)
		}
	} else {
		trace_util_0.Count(_dump_00000, 29)
		{
			if jsonTbl.Partitions == nil {
				trace_util_0.Count(_dump_00000, 31)
				return errors.New("No partition statistics")
			}
			trace_util_0.Count(_dump_00000, 30)
			for _, def := range pi.Definitions {
				trace_util_0.Count(_dump_00000, 32)
				tbl := jsonTbl.Partitions[def.Name.L]
				if tbl == nil {
					trace_util_0.Count(_dump_00000, 34)
					continue
				}
				trace_util_0.Count(_dump_00000, 33)
				err := h.loadStatsFromJSON(tableInfo, def.ID, tbl)
				if err != nil {
					trace_util_0.Count(_dump_00000, 35)
					return errors.Trace(err)
				}
			}
		}
	}
	trace_util_0.Count(_dump_00000, 25)
	return errors.Trace(h.Update(is))
}

func (h *Handle) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) error {
	trace_util_0.Count(_dump_00000, 36)
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		trace_util_0.Count(_dump_00000, 41)
		return errors.Trace(err)
	}

	trace_util_0.Count(_dump_00000, 37)
	for _, col := range tbl.Columns {
		trace_util_0.Count(_dump_00000, 42)
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 0, &col.Histogram, col.CMSketch, 1)
		if err != nil {
			trace_util_0.Count(_dump_00000, 43)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_dump_00000, 38)
	for _, idx := range tbl.Indices {
		trace_util_0.Count(_dump_00000, 44)
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 1, &idx.Histogram, idx.CMSketch, 1)
		if err != nil {
			trace_util_0.Count(_dump_00000, 45)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_dump_00000, 39)
	err = h.SaveMetaToStorage(tbl.PhysicalID, tbl.Count, tbl.ModifyCount)
	if err != nil {
		trace_util_0.Count(_dump_00000, 46)
		return errors.Trace(err)
	}
	trace_util_0.Count(_dump_00000, 40)
	return nil
}

// TableStatsFromJSON loads statistic from JSONTable and return the Table of statistic.
func TableStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) (*statistics.Table, error) {
	trace_util_0.Count(_dump_00000, 47)
	newHistColl := statistics.HistColl{
		PhysicalID:     physicalID,
		HavePhysicalID: true,
		Count:          jsonTbl.Count,
		ModifyCount:    jsonTbl.ModifyCount,
		Columns:        make(map[int64]*statistics.Column, len(jsonTbl.Columns)),
		Indices:        make(map[int64]*statistics.Index, len(jsonTbl.Indices)),
	}
	tbl := &statistics.Table{
		HistColl: newHistColl,
	}
	for id, jsonIdx := range jsonTbl.Indices {
		trace_util_0.Count(_dump_00000, 50)
		for _, idxInfo := range tableInfo.Indices {
			trace_util_0.Count(_dump_00000, 51)
			if idxInfo.Name.L != id {
				trace_util_0.Count(_dump_00000, 53)
				continue
			}
			trace_util_0.Count(_dump_00000, 52)
			hist := statistics.HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.Correlation = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUpdateVersion, jsonIdx.Correlation
			idx := &statistics.Index{
				Histogram: *hist,
				CMSketch:  statistics.CMSketchFromProto(jsonIdx.CMSketch),
				Info:      idxInfo,
			}
			tbl.Indices[idx.ID] = idx
		}
	}

	trace_util_0.Count(_dump_00000, 48)
	for id, jsonCol := range jsonTbl.Columns {
		trace_util_0.Count(_dump_00000, 54)
		for _, colInfo := range tableInfo.Columns {
			trace_util_0.Count(_dump_00000, 55)
			if colInfo.Name.L != id {
				trace_util_0.Count(_dump_00000, 58)
				continue
			}
			trace_util_0.Count(_dump_00000, 56)
			hist := statistics.HistogramFromProto(jsonCol.Histogram)
			count := int64(hist.TotalRowCount())
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			hist, err := hist.ConvertTo(sc, &colInfo.FieldType)
			if err != nil {
				trace_util_0.Count(_dump_00000, 59)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_dump_00000, 57)
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.TotColSize, hist.Correlation = colInfo.ID, jsonCol.NullCount, jsonCol.LastUpdateVersion, jsonCol.TotColSize, jsonCol.Correlation
			col := &statistics.Column{
				PhysicalID: physicalID,
				Histogram:  *hist,
				CMSketch:   statistics.CMSketchFromProto(jsonCol.CMSketch),
				Info:       colInfo,
				Count:      count,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
			}
			tbl.Columns[col.ID] = col
		}
	}
	trace_util_0.Count(_dump_00000, 49)
	return tbl, nil
}

var _dump_00000 = "statistics/handle/dump.go"
