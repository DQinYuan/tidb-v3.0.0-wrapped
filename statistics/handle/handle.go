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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// StatsCache caches the tables in memory for Handle.
type StatsCache map[int64]*statistics.Table

// Handle can update stats info periodically.
type Handle struct {
	mu struct {
		sync.Mutex
		ctx sessionctx.Context
		// lastVersion is the latest update version before last lease.
		lastVersion uint64
		// rateMap contains the error rate delta from feedback.
		rateMap errorRateDeltaMap
		// pid2tid is the map from partition ID to table ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schema when `pid2tid` is built.
		schemaVersion int64
	}

	restrictedExec sqlexec.RestrictedSQLExecutor

	StatsCache atomic.Value
	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *util.Event
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap tableDeltaMap
	// feedback is used to store query feedback info.
	feedback []*statistics.QueryFeedback

	lease atomic2.Duration
}

// Clear the StatsCache, only for test.
func (h *Handle) Clear() {
	trace_util_0.Count(_handle_00000, 0)
	h.mu.Lock()
	h.StatsCache.Store(StatsCache{})
	h.mu.lastVersion = 0
	for len(h.ddlEventCh) > 0 {
		trace_util_0.Count(_handle_00000, 2)
		<-h.ddlEventCh
	}
	trace_util_0.Count(_handle_00000, 1)
	h.feedback = h.feedback[:0]
	h.mu.ctx.GetSessionVars().InitChunkSize = 1
	h.mu.ctx.GetSessionVars().MaxChunkSize = 32
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap = make(tableDeltaMap)
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

// MaxQueryFeedbackCount is the max number of feedback that cache in memory.
var MaxQueryFeedbackCount = atomic2.NewInt64(1 << 10)

// NewHandle creates a Handle for update stats.
func NewHandle(ctx sessionctx.Context, lease time.Duration) *Handle {
	trace_util_0.Count(_handle_00000, 3)
	handle := &Handle{
		ddlEventCh: make(chan *util.Event, 100),
		listHead:   &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:  make(tableDeltaMap),
		feedback:   make([]*statistics.QueryFeedback, 0, MaxQueryFeedbackCount.Load()),
	}
	handle.lease.Store(lease)
	// It is safe to use it concurrently because the exec won't touch the ctx.
	if exec, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		trace_util_0.Count(_handle_00000, 5)
		handle.restrictedExec = exec
	}
	trace_util_0.Count(_handle_00000, 4)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.StatsCache.Store(StatsCache{})
	return handle
}

// Lease returns the stats lease.
func (h *Handle) Lease() time.Duration {
	trace_util_0.Count(_handle_00000, 6)
	return h.lease.Load()
}

// SetLease sets the stats lease.
func (h *Handle) SetLease(lease time.Duration) {
	trace_util_0.Count(_handle_00000, 7)
	h.lease.Store(lease)
}

// GetQueryFeedback gets the query feedback. It is only use in test.
func (h *Handle) GetQueryFeedback() []*statistics.QueryFeedback {
	trace_util_0.Count(_handle_00000, 8)
	defer func() {
		trace_util_0.Count(_handle_00000, 10)
		h.feedback = h.feedback[:0]
	}()
	trace_util_0.Count(_handle_00000, 9)
	return h.feedback
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	trace_util_0.Count(_handle_00000, 11)
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	trace_util_0.Count(_handle_00000, 12)
	lastVersion := h.LastUpdateVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if lastVersion >= offset {
		trace_util_0.Count(_handle_00000, 16)
		lastVersion = lastVersion - offset
	} else {
		trace_util_0.Count(_handle_00000, 17)
		{
			lastVersion = 0
		}
	}
	trace_util_0.Count(_handle_00000, 13)
	sql := fmt.Sprintf("SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %d order by version", lastVersion)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_handle_00000, 18)
		return errors.Trace(err)
	}

	trace_util_0.Count(_handle_00000, 14)
	tables := make([]*statistics.Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		trace_util_0.Count(_handle_00000, 19)
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		lastVersion = version
		h.mu.Lock()
		table, ok := h.getTableByPhysicalID(is, physicalID)
		h.mu.Unlock()
		if !ok {
			trace_util_0.Count(_handle_00000, 23)
			logutil.Logger(context.Background()).Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		trace_util_0.Count(_handle_00000, 20)
		tableInfo := table.Meta()
		tbl, err := h.tableStatsFromStorage(tableInfo, physicalID, false, nil)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			trace_util_0.Count(_handle_00000, 24)
			logutil.Logger(context.Background()).Debug("error occurred when read table stats", zap.String("table", tableInfo.Name.O), zap.Error(err))
			continue
		}
		trace_util_0.Count(_handle_00000, 21)
		if tbl == nil {
			trace_util_0.Count(_handle_00000, 25)
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		trace_util_0.Count(_handle_00000, 22)
		tbl.Version = version
		tbl.Count = count
		tbl.ModifyCount = modifyCount
		tbl.Name = getFullTableName(is, tableInfo)
		tables = append(tables, tbl)
	}
	trace_util_0.Count(_handle_00000, 15)
	h.mu.Lock()
	h.mu.lastVersion = lastVersion
	h.UpdateTableStats(tables, deletedTableIDs)
	h.mu.Unlock()
	return nil
}

func (h *Handle) getTableByPhysicalID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	trace_util_0.Count(_handle_00000, 26)
	if is.SchemaMetaVersion() != h.mu.schemaVersion {
		trace_util_0.Count(_handle_00000, 29)
		h.mu.schemaVersion = is.SchemaMetaVersion()
		h.mu.pid2tid = buildPartitionID2TableID(is)
	}
	trace_util_0.Count(_handle_00000, 27)
	if id, ok := h.mu.pid2tid[physicalID]; ok {
		trace_util_0.Count(_handle_00000, 30)
		return is.TableByID(id)
	}
	trace_util_0.Count(_handle_00000, 28)
	return is.TableByID(physicalID)
}

func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	trace_util_0.Count(_handle_00000, 31)
	mapper := make(map[int64]int64)
	for _, db := range is.AllSchemas() {
		trace_util_0.Count(_handle_00000, 33)
		tbls := db.Tables
		for _, tbl := range tbls {
			trace_util_0.Count(_handle_00000, 34)
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				trace_util_0.Count(_handle_00000, 36)
				continue
			}
			trace_util_0.Count(_handle_00000, 35)
			for _, def := range pi.Definitions {
				trace_util_0.Count(_handle_00000, 37)
				mapper[def.ID] = tbl.ID
			}
		}
	}
	trace_util_0.Count(_handle_00000, 32)
	return mapper
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	trace_util_0.Count(_handle_00000, 38)
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	trace_util_0.Count(_handle_00000, 39)
	tbl, ok := h.StatsCache.Load().(StatsCache)[pid]
	if !ok {
		trace_util_0.Count(_handle_00000, 41)
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.UpdateTableStats([]*statistics.Table{tbl}, nil)
		return tbl
	}
	trace_util_0.Count(_handle_00000, 40)
	return tbl
}

func (h *Handle) copyFromOldCache() StatsCache {
	trace_util_0.Count(_handle_00000, 42)
	newCache := StatsCache{}
	oldCache := h.StatsCache.Load().(StatsCache)
	for k, v := range oldCache {
		trace_util_0.Count(_handle_00000, 44)
		newCache[k] = v
	}
	trace_util_0.Count(_handle_00000, 43)
	return newCache
}

// UpdateTableStats updates the statistics table cache using copy on write.
func (h *Handle) UpdateTableStats(tables []*statistics.Table, deletedIDs []int64) {
	trace_util_0.Count(_handle_00000, 45)
	newCache := h.copyFromOldCache()
	for _, tbl := range tables {
		trace_util_0.Count(_handle_00000, 48)
		id := tbl.PhysicalID
		newCache[id] = tbl
	}
	trace_util_0.Count(_handle_00000, 46)
	for _, id := range deletedIDs {
		trace_util_0.Count(_handle_00000, 49)
		delete(newCache, id)
	}
	trace_util_0.Count(_handle_00000, 47)
	h.StatsCache.Store(newCache)
}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() error {
	trace_util_0.Count(_handle_00000, 50)
	cols := statistics.HistogramNeededColumns.AllCols()
	for _, col := range cols {
		trace_util_0.Count(_handle_00000, 52)
		tbl, ok := h.StatsCache.Load().(StatsCache)[col.TableID]
		if !ok {
			trace_util_0.Count(_handle_00000, 57)
			continue
		}
		trace_util_0.Count(_handle_00000, 53)
		tbl = tbl.Copy()
		c, ok := tbl.Columns[col.ColumnID]
		if !ok || c.Len() > 0 {
			trace_util_0.Count(_handle_00000, 58)
			statistics.HistogramNeededColumns.Delete(col)
			continue
		}
		trace_util_0.Count(_handle_00000, 54)
		hg, err := h.histogramFromStorage(col.TableID, c.ID, &c.Info.FieldType, c.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation, nil)
		if err != nil {
			trace_util_0.Count(_handle_00000, 59)
			return errors.Trace(err)
		}
		trace_util_0.Count(_handle_00000, 55)
		cms, err := h.cmSketchFromStorage(col.TableID, 0, col.ColumnID, nil)
		if err != nil {
			trace_util_0.Count(_handle_00000, 60)
			return errors.Trace(err)
		}
		trace_util_0.Count(_handle_00000, 56)
		tbl.Columns[c.ID] = &statistics.Column{
			PhysicalID: col.TableID,
			Histogram:  *hg,
			Info:       c.Info,
			CMSketch:   cms,
			Count:      int64(hg.TotalRowCount()),
			IsHandle:   c.IsHandle,
		}
		h.UpdateTableStats([]*statistics.Table{tbl}, nil)
		statistics.HistogramNeededColumns.Delete(col)
	}
	trace_util_0.Count(_handle_00000, 51)
	return nil
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	trace_util_0.Count(_handle_00000, 61)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.lastVersion
}

// SetLastUpdateVersion sets the last update version.
func (h *Handle) SetLastUpdateVersion(version uint64) {
	trace_util_0.Count(_handle_00000, 62)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.lastVersion = version
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	trace_util_0.Count(_handle_00000, 63)
	for len(h.ddlEventCh) > 0 {
		trace_util_0.Count(_handle_00000, 66)
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			trace_util_0.Count(_handle_00000, 67)
			logutil.Logger(context.Background()).Debug("[stats] handle ddl event fail", zap.Error(err))
		}
	}
	trace_util_0.Count(_handle_00000, 64)
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		trace_util_0.Count(_handle_00000, 68)
		logutil.Logger(context.Background()).Debug("[stats] dump stats delta fail", zap.Error(err))
	}
	trace_util_0.Count(_handle_00000, 65)
	if err := h.DumpStatsFeedbackToKV(); err != nil {
		trace_util_0.Count(_handle_00000, 69)
		logutil.Logger(context.Background()).Debug("[stats] dump stats feedback fail", zap.Error(err))
	}
}

func (h *Handle) cmSketchFromStorage(tblID int64, isIndex, histID int64, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.CMSketch, err error) {
	trace_util_0.Count(_handle_00000, 70)
	selSQL := fmt.Sprintf("select cm_sketch from mysql.stats_histograms where table_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	var rows []chunk.Row
	if historyStatsExec != nil {
		trace_util_0.Count(_handle_00000, 74)
		rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(nil, selSQL)
	} else {
		trace_util_0.Count(_handle_00000, 75)
		{
			rows, _, err = h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
		}
	}
	trace_util_0.Count(_handle_00000, 71)
	if err != nil {
		trace_util_0.Count(_handle_00000, 76)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 72)
	if len(rows) == 0 {
		trace_util_0.Count(_handle_00000, 77)
		return nil, nil
	}
	trace_util_0.Count(_handle_00000, 73)
	return statistics.LoadCMSketchWithTopN(h.restrictedExec, tblID, isIndex, histID, rows[0].GetBytes(0))
}

func (h *Handle) indexStatsFromStorage(row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, historyStatsExec sqlexec.RestrictedSQLExecutor) error {
	trace_util_0.Count(_handle_00000, 78)
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	idx := table.Indices[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	if statistics.IsAnalyzed(flag) {
		trace_util_0.Count(_handle_00000, 82)
		h.mu.Lock()
		h.mu.rateMap.clear(table.PhysicalID, histID, true)
		h.mu.Unlock()
	} else {
		trace_util_0.Count(_handle_00000, 83)
		if idx != nil {
			trace_util_0.Count(_handle_00000, 84)
			errorRate = idx.ErrorRate
		}
	}
	trace_util_0.Count(_handle_00000, 79)
	for _, idxInfo := range tableInfo.Indices {
		trace_util_0.Count(_handle_00000, 85)
		if histID != idxInfo.ID {
			trace_util_0.Count(_handle_00000, 88)
			continue
		}
		trace_util_0.Count(_handle_00000, 86)
		if idx == nil || idx.LastUpdateVersion < histVer {
			trace_util_0.Count(_handle_00000, 89)
			hg, err := h.histogramFromStorage(table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0, historyStatsExec)
			if err != nil {
				trace_util_0.Count(_handle_00000, 92)
				return errors.Trace(err)
			}
			trace_util_0.Count(_handle_00000, 90)
			cms, err := h.cmSketchFromStorage(table.PhysicalID, 1, idxInfo.ID, historyStatsExec)
			if err != nil {
				trace_util_0.Count(_handle_00000, 93)
				return errors.Trace(err)
			}
			trace_util_0.Count(_handle_00000, 91)
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, StatsVer: row.GetInt64(7), Flag: flag, LastAnalyzePos: row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))}
		}
		trace_util_0.Count(_handle_00000, 87)
		break
	}
	trace_util_0.Count(_handle_00000, 80)
	if idx != nil {
		trace_util_0.Count(_handle_00000, 94)
		table.Indices[histID] = idx
	} else {
		trace_util_0.Count(_handle_00000, 95)
		{
			logutil.Logger(context.Background()).Debug("we cannot find index id in table info. It may be deleted.", zap.Int64("indexID", histID), zap.String("table", tableInfo.Name.O))
		}
	}
	trace_util_0.Count(_handle_00000, 81)
	return nil
}

func (h *Handle) columnStatsFromStorage(row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool, historyStatsExec sqlexec.RestrictedSQLExecutor) error {
	trace_util_0.Count(_handle_00000, 96)
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	correlation := row.GetFloat64(9)
	col := table.Columns[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	if statistics.IsAnalyzed(flag) {
		trace_util_0.Count(_handle_00000, 100)
		h.mu.Lock()
		h.mu.rateMap.clear(table.PhysicalID, histID, false)
		h.mu.Unlock()
	} else {
		trace_util_0.Count(_handle_00000, 101)
		if col != nil {
			trace_util_0.Count(_handle_00000, 102)
			errorRate = col.ErrorRate
		}
	}
	trace_util_0.Count(_handle_00000, 97)
	for _, colInfo := range tableInfo.Columns {
		trace_util_0.Count(_handle_00000, 103)
		if histID != colInfo.ID {
			trace_util_0.Count(_handle_00000, 108)
			continue
		}
		trace_util_0.Count(_handle_00000, 104)
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag)
		// We will not load buckets if:
		// 1. Lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has buckets before, and:
		// 4. loadAll is false.
		notNeedLoad := h.Lease() > 0 &&
			!isHandle &&
			(col == nil || col.Len() == 0 && col.LastUpdateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			trace_util_0.Count(_handle_00000, 109)
			count, err := h.columnCountFromStorage(table.PhysicalID, histID)
			if err != nil {
				trace_util_0.Count(_handle_00000, 111)
				return errors.Trace(err)
			}
			trace_util_0.Count(_handle_00000, 110)
			col = &statistics.Column{
				PhysicalID:     table.PhysicalID,
				Histogram:      *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:           colInfo,
				Count:          count + nullCount,
				ErrorRate:      errorRate,
				IsHandle:       tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:           flag,
				LastAnalyzePos: row.GetDatum(10, types.NewFieldType(mysql.TypeBlob)),
			}
			col.Histogram.Correlation = correlation
			break
		}
		trace_util_0.Count(_handle_00000, 105)
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			trace_util_0.Count(_handle_00000, 112)
			hg, err := h.histogramFromStorage(table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation, historyStatsExec)
			if err != nil {
				trace_util_0.Count(_handle_00000, 115)
				return errors.Trace(err)
			}
			trace_util_0.Count(_handle_00000, 113)
			cms, err := h.cmSketchFromStorage(table.PhysicalID, 0, colInfo.ID, historyStatsExec)
			if err != nil {
				trace_util_0.Count(_handle_00000, 116)
				return errors.Trace(err)
			}
			trace_util_0.Count(_handle_00000, 114)
			col = &statistics.Column{
				PhysicalID:     table.PhysicalID,
				Histogram:      *hg,
				Info:           colInfo,
				CMSketch:       cms,
				Count:          int64(hg.TotalRowCount()),
				ErrorRate:      errorRate,
				IsHandle:       tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:           flag,
				LastAnalyzePos: row.GetDatum(10, types.NewFieldType(mysql.TypeBlob)),
			}
			break
		}
		trace_util_0.Count(_handle_00000, 106)
		if col.TotColSize != totColSize {
			trace_util_0.Count(_handle_00000, 117)
			newCol := *col
			newCol.TotColSize = totColSize
			col = &newCol
		}
		trace_util_0.Count(_handle_00000, 107)
		break
	}
	trace_util_0.Count(_handle_00000, 98)
	if col != nil {
		trace_util_0.Count(_handle_00000, 118)
		table.Columns[col.ID] = col
	} else {
		trace_util_0.Count(_handle_00000, 119)
		{
			// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
			// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
			// avoid error.
			logutil.Logger(context.Background()).Debug("we cannot find column in table info now. It may be deleted", zap.Int64("colID", histID), zap.String("table", tableInfo.Name.O))
		}
	}
	trace_util_0.Count(_handle_00000, 99)
	return nil
}

// tableStatsFromStorage loads table stats info from storage.
func (h *Handle) tableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.Table, err error) {
	trace_util_0.Count(_handle_00000, 120)
	table, ok := h.StatsCache.Load().(StatsCache)[physicalID]
	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if !ok || historyStatsExec != nil {
		trace_util_0.Count(_handle_00000, 126)
		histColl := statistics.HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Columns:        make(map[int64]*statistics.Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*statistics.Index, len(tableInfo.Indices)),
		}
		table = &statistics.Table{
			HistColl: histColl,
		}
	} else {
		trace_util_0.Count(_handle_00000, 127)
		{
			// We copy it before writing to avoid race.
			table = table.Copy()
		}
	}
	trace_util_0.Count(_handle_00000, 121)
	table.Pseudo = false
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %d", physicalID)
	var rows []chunk.Row
	if historyStatsExec != nil {
		trace_util_0.Count(_handle_00000, 128)
		rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(nil, selSQL)
	} else {
		trace_util_0.Count(_handle_00000, 129)
		{
			rows, _, err = h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
		}
	}
	trace_util_0.Count(_handle_00000, 122)
	if err != nil {
		trace_util_0.Count(_handle_00000, 130)
		return nil, err
	}
	// Check deleted table.
	trace_util_0.Count(_handle_00000, 123)
	if len(rows) == 0 {
		trace_util_0.Count(_handle_00000, 131)
		return nil, nil
	}
	trace_util_0.Count(_handle_00000, 124)
	for _, row := range rows {
		trace_util_0.Count(_handle_00000, 132)
		if row.GetInt64(1) > 0 {
			trace_util_0.Count(_handle_00000, 133)
			if err := h.indexStatsFromStorage(row, table, tableInfo, historyStatsExec); err != nil {
				trace_util_0.Count(_handle_00000, 134)
				return nil, errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_handle_00000, 135)
			{
				if err := h.columnStatsFromStorage(row, table, tableInfo, loadAll, historyStatsExec); err != nil {
					trace_util_0.Count(_handle_00000, 136)
					return nil, errors.Trace(err)
				}
			}
		}
	}
	trace_util_0.Count(_handle_00000, 125)
	return table, nil
}

// SaveStatsToStorage saves the stats to storage.
func (h *Handle) SaveStatsToStorage(tableID int64, count int64, isIndex int, hg *statistics.Histogram, cms *statistics.CMSketch, isAnalyzed int64) (err error) {
	trace_util_0.Count(_handle_00000, 137)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		trace_util_0.Count(_handle_00000, 151)
		return errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 138)
	defer func() {
		trace_util_0.Count(_handle_00000, 152)
		err = finishTransaction(context.Background(), exec, err)
	}()
	trace_util_0.Count(_handle_00000, 139)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_handle_00000, 153)
		return errors.Trace(err)
	}

	trace_util_0.Count(_handle_00000, 140)
	version := txn.StartTS()
	var sql string
	// If the count is less than 0, then we do not want to update the modify count and count.
	if count >= 0 {
		trace_util_0.Count(_handle_00000, 154)
		sql = fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, tableID, count)
	} else {
		trace_util_0.Count(_handle_00000, 155)
		{
			sql = fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d", version, tableID)
		}
	}
	trace_util_0.Count(_handle_00000, 141)
	_, err = exec.Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_handle_00000, 156)
		return
	}
	trace_util_0.Count(_handle_00000, 142)
	data, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		trace_util_0.Count(_handle_00000, 157)
		return
	}
	// Delete outdated data
	trace_util_0.Count(_handle_00000, 143)
	deleteOutdatedTopNSQL := fmt.Sprintf("delete from mysql.stats_top_n where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID)
	_, err = exec.Execute(ctx, deleteOutdatedTopNSQL)
	if err != nil {
		trace_util_0.Count(_handle_00000, 158)
		return
	}
	trace_util_0.Count(_handle_00000, 144)
	for _, meta := range cms.TopN() {
		trace_util_0.Count(_handle_00000, 159)
		insertSQL := fmt.Sprintf("insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values (%d, %d, %d, X'%X', %d)", tableID, isIndex, hg.ID, meta.Data, meta.Count)
		_, err = exec.Execute(ctx, insertSQL)
		if err != nil {
			trace_util_0.Count(_handle_00000, 160)
			return
		}
	}
	trace_util_0.Count(_handle_00000, 145)
	flag := 0
	if isAnalyzed == 1 {
		trace_util_0.Count(_handle_00000, 161)
		flag = statistics.AnalyzeFlag
	}
	trace_util_0.Count(_handle_00000, 146)
	replaceSQL := fmt.Sprintf("replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%d, %d, %d, %d, %d, %d, X'%X', %d, %d, %d, %f)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, data, hg.TotColSize, statistics.CurStatsVersion, flag, hg.Correlation)
	_, err = exec.Execute(ctx, replaceSQL)
	if err != nil {
		trace_util_0.Count(_handle_00000, 162)
		return
	}
	trace_util_0.Count(_handle_00000, 147)
	deleteSQL := fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID)
	_, err = exec.Execute(ctx, deleteSQL)
	if err != nil {
		trace_util_0.Count(_handle_00000, 163)
		return
	}
	trace_util_0.Count(_handle_00000, 148)
	sc := h.mu.ctx.GetSessionVars().StmtCtx
	var lastAnalyzePos []byte
	for i := range hg.Buckets {
		trace_util_0.Count(_handle_00000, 164)
		count := hg.Buckets[i].Count
		if i > 0 {
			trace_util_0.Count(_handle_00000, 169)
			count -= hg.Buckets[i-1].Count
		}
		trace_util_0.Count(_handle_00000, 165)
		var upperBound types.Datum
		upperBound, err = hg.GetUpper(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			trace_util_0.Count(_handle_00000, 170)
			return
		}
		trace_util_0.Count(_handle_00000, 166)
		if i == len(hg.Buckets)-1 {
			trace_util_0.Count(_handle_00000, 171)
			lastAnalyzePos = upperBound.GetBytes()
		}
		trace_util_0.Count(_handle_00000, 167)
		var lowerBound types.Datum
		lowerBound, err = hg.GetLower(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			trace_util_0.Count(_handle_00000, 172)
			return
		}
		trace_util_0.Count(_handle_00000, 168)
		insertSQL := fmt.Sprintf("insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", tableID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes())
		_, err = exec.Execute(ctx, insertSQL)
		if err != nil {
			trace_util_0.Count(_handle_00000, 173)
			return
		}
	}
	trace_util_0.Count(_handle_00000, 149)
	if isAnalyzed == 1 && len(lastAnalyzePos) > 0 {
		trace_util_0.Count(_handle_00000, 174)
		sql = fmt.Sprintf("update mysql.stats_histograms set last_analyze_pos = X'%X' where table_id = %d and is_index = %d and hist_id = %d", lastAnalyzePos, tableID, isIndex, hg.ID)
		_, err = exec.Execute(ctx, sql)
		if err != nil {
			trace_util_0.Count(_handle_00000, 175)
			return
		}
	}
	trace_util_0.Count(_handle_00000, 150)
	return
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(tableID, count, modifyCount int64) (err error) {
	trace_util_0.Count(_handle_00000, 176)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		trace_util_0.Count(_handle_00000, 180)
		return errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 177)
	defer func() {
		trace_util_0.Count(_handle_00000, 181)
		err = finishTransaction(ctx, exec, err)
	}()
	trace_util_0.Count(_handle_00000, 178)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_handle_00000, 182)
		return errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 179)
	var sql string
	version := txn.StartTS()
	sql = fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count, modify_count) values (%d, %d, %d, %d)", version, tableID, count, modifyCount)
	_, err = exec.Execute(ctx, sql)
	return
}

func (h *Handle) histogramFromStorage(tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.Histogram, err error) {
	trace_util_0.Count(_handle_00000, 183)
	selSQL := fmt.Sprintf("select count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d order by bucket_id", tableID, isIndex, colID)
	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	if historyStatsExec != nil {
		trace_util_0.Count(_handle_00000, 187)
		rows, fields, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(nil, selSQL)
	} else {
		trace_util_0.Count(_handle_00000, 188)
		{
			rows, fields, err = h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
		}
	}
	trace_util_0.Count(_handle_00000, 184)
	if err != nil {
		trace_util_0.Count(_handle_00000, 189)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 185)
	bucketSize := len(rows)
	hg := statistics.NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totColSize)
	hg.Correlation = corr
	totalCount := int64(0)
	for i := 0; i < bucketSize; i++ {
		trace_util_0.Count(_handle_00000, 190)
		count := rows[i].GetInt64(0)
		repeats := rows[i].GetInt64(1)
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			trace_util_0.Count(_handle_00000, 192)
			lowerBound = rows[i].GetDatum(2, &fields[2].Column.FieldType)
			upperBound = rows[i].GetDatum(3, &fields[3].Column.FieldType)
		} else {
			trace_util_0.Count(_handle_00000, 193)
			{
				sc := &stmtctx.StatementContext{TimeZone: time.UTC}
				d := rows[i].GetDatum(2, &fields[2].Column.FieldType)
				lowerBound, err = d.ConvertTo(sc, tp)
				if err != nil {
					trace_util_0.Count(_handle_00000, 195)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_handle_00000, 194)
				d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
				upperBound, err = d.ConvertTo(sc, tp)
				if err != nil {
					trace_util_0.Count(_handle_00000, 196)
					return nil, errors.Trace(err)
				}
			}
		}
		trace_util_0.Count(_handle_00000, 191)
		totalCount += count
		hg.AppendBucket(&lowerBound, &upperBound, totalCount, repeats)
	}
	trace_util_0.Count(_handle_00000, 186)
	hg.PreCalculateScalar()
	return hg, nil
}

func (h *Handle) columnCountFromStorage(tableID, colID int64) (int64, error) {
	trace_util_0.Count(_handle_00000, 197)
	selSQL := fmt.Sprintf("select sum(count) from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, 0, colID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
	if err != nil {
		trace_util_0.Count(_handle_00000, 200)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_handle_00000, 198)
	if rows[0].IsNull(0) {
		trace_util_0.Count(_handle_00000, 201)
		return 0, nil
	}
	trace_util_0.Count(_handle_00000, 199)
	return rows[0].GetMyDecimal(0).ToInt()
}

func (h *Handle) statsMetaByTableIDFromStorage(tableID int64, historyStatsExec sqlexec.RestrictedSQLExecutor) (version uint64, modifyCount, count int64, err error) {
	trace_util_0.Count(_handle_00000, 202)
	selSQL := fmt.Sprintf("SELECT version, modify_count, count from mysql.stats_meta where table_id = %d order by version", tableID)
	var rows []chunk.Row
	if historyStatsExec == nil {
		trace_util_0.Count(_handle_00000, 205)
		rows, _, err = h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
	} else {
		trace_util_0.Count(_handle_00000, 206)
		{
			rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(nil, selSQL)
		}
	}
	trace_util_0.Count(_handle_00000, 203)
	if err != nil || len(rows) == 0 {
		trace_util_0.Count(_handle_00000, 207)
		return
	}
	trace_util_0.Count(_handle_00000, 204)
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}

var _handle_00000 = "statistics/handle/handle.go"
