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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	trace_util_0.Count(_update_00000, 0)
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.ColSize == nil {
		trace_util_0.Count(_update_00000, 3)
		item.ColSize = make(map[int64]int64)
	}
	trace_util_0.Count(_update_00000, 1)
	if colSize != nil {
		trace_util_0.Count(_update_00000, 4)
		for key, val := range *colSize {
			trace_util_0.Count(_update_00000, 5)
			item.ColSize[key] += val
		}
	}
	trace_util_0.Count(_update_00000, 2)
	m[id] = item
}

type errorRateDelta struct {
	PkID         int64
	PkErrorRate  *statistics.ErrorRate
	IdxErrorRate map[int64]*statistics.ErrorRate
}

type errorRateDeltaMap map[int64]errorRateDelta

func (m errorRateDeltaMap) update(tableID int64, histID int64, rate float64, isIndex bool) {
	trace_util_0.Count(_update_00000, 6)
	item := m[tableID]
	if isIndex {
		trace_util_0.Count(_update_00000, 8)
		if item.IdxErrorRate == nil {
			trace_util_0.Count(_update_00000, 11)
			item.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
		}
		trace_util_0.Count(_update_00000, 9)
		if item.IdxErrorRate[histID] == nil {
			trace_util_0.Count(_update_00000, 12)
			item.IdxErrorRate[histID] = &statistics.ErrorRate{}
		}
		trace_util_0.Count(_update_00000, 10)
		item.IdxErrorRate[histID].Update(rate)
	} else {
		trace_util_0.Count(_update_00000, 13)
		{
			if item.PkErrorRate == nil {
				trace_util_0.Count(_update_00000, 15)
				item.PkID = histID
				item.PkErrorRate = &statistics.ErrorRate{}
			}
			trace_util_0.Count(_update_00000, 14)
			item.PkErrorRate.Update(rate)
		}
	}
	trace_util_0.Count(_update_00000, 7)
	m[tableID] = item
}

func (m errorRateDeltaMap) merge(deltaMap errorRateDeltaMap) {
	trace_util_0.Count(_update_00000, 16)
	for tableID, item := range deltaMap {
		trace_util_0.Count(_update_00000, 17)
		tbl := m[tableID]
		for histID, errorRate := range item.IdxErrorRate {
			trace_util_0.Count(_update_00000, 20)
			if tbl.IdxErrorRate == nil {
				trace_util_0.Count(_update_00000, 23)
				tbl.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
			}
			trace_util_0.Count(_update_00000, 21)
			if tbl.IdxErrorRate[histID] == nil {
				trace_util_0.Count(_update_00000, 24)
				tbl.IdxErrorRate[histID] = &statistics.ErrorRate{}
			}
			trace_util_0.Count(_update_00000, 22)
			tbl.IdxErrorRate[histID].Merge(errorRate)
		}
		trace_util_0.Count(_update_00000, 18)
		if item.PkErrorRate != nil {
			trace_util_0.Count(_update_00000, 25)
			if tbl.PkErrorRate == nil {
				trace_util_0.Count(_update_00000, 27)
				tbl.PkID = item.PkID
				tbl.PkErrorRate = &statistics.ErrorRate{}
			}
			trace_util_0.Count(_update_00000, 26)
			tbl.PkErrorRate.Merge(item.PkErrorRate)
		}
		trace_util_0.Count(_update_00000, 19)
		m[tableID] = tbl
	}
}

func (m errorRateDeltaMap) clear(tableID int64, histID int64, isIndex bool) {
	trace_util_0.Count(_update_00000, 28)
	item := m[tableID]
	if isIndex {
		trace_util_0.Count(_update_00000, 30)
		delete(item.IdxErrorRate, histID)
	} else {
		trace_util_0.Count(_update_00000, 31)
		{
			item.PkErrorRate = nil
		}
	}
	trace_util_0.Count(_update_00000, 29)
	m[tableID] = item
}

func (h *Handle) merge(s *SessionStatsCollector, rateMap errorRateDeltaMap) {
	trace_util_0.Count(_update_00000, 32)
	for id, item := range s.mapper {
		trace_util_0.Count(_update_00000, 34)
		h.globalMap.update(id, item.Delta, item.Count, &item.ColSize)
	}
	trace_util_0.Count(_update_00000, 33)
	s.mapper = make(tableDeltaMap)
	rateMap.merge(s.rateMap)
	s.rateMap = make(errorRateDeltaMap)
	h.feedback = mergeQueryFeedback(h.feedback, s.feedback)
	s.feedback = s.feedback[:0]
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper   tableDeltaMap
	feedback []*statistics.QueryFeedback
	rateMap  errorRateDeltaMap
	next     *SessionStatsCollector
	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsCollector) Delete() {
	trace_util_0.Count(_update_00000, 35)
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	trace_util_0.Count(_update_00000, 36)
	s.Lock()
	defer s.Unlock()
	s.mapper.update(id, delta, count, colSize)
}

func mergeQueryFeedback(lq []*statistics.QueryFeedback, rq []*statistics.QueryFeedback) []*statistics.QueryFeedback {
	trace_util_0.Count(_update_00000, 37)
	for _, q := range rq {
		trace_util_0.Count(_update_00000, 39)
		if len(lq) >= int(MaxQueryFeedbackCount.Load()) {
			trace_util_0.Count(_update_00000, 41)
			break
		}
		trace_util_0.Count(_update_00000, 40)
		lq = append(lq, q)
	}
	trace_util_0.Count(_update_00000, 38)
	return lq
}

var (
	// MinLogScanCount is the minimum scan count for a feedback to be logged.
	MinLogScanCount = int64(1000)
	// MinLogErrorRate is the minimum error rate for a feedback to be logged.
	MinLogErrorRate = 0.5
)

// StoreQueryFeedback will merges the feedback into stats collector.
func (s *SessionStatsCollector) StoreQueryFeedback(feedback interface{}, h *Handle) error {
	trace_util_0.Count(_update_00000, 42)
	q := feedback.(*statistics.QueryFeedback)
	// TODO: If the error rate is small or actual scan count is small, we do not need to store the feed back.
	if !q.Valid || q.Hist == nil {
		trace_util_0.Count(_update_00000, 47)
		return nil
	}
	trace_util_0.Count(_update_00000, 43)
	err := h.RecalculateExpectCount(q)
	if err != nil {
		trace_util_0.Count(_update_00000, 48)
		return errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 44)
	rate := q.CalcErrorRate()
	if rate >= MinLogErrorRate && (q.Actual() >= MinLogScanCount || q.Expected >= MinLogScanCount) {
		trace_util_0.Count(_update_00000, 49)
		metrics.SignificantFeedbackCounter.Inc()
		if log.GetLevel() == zap.DebugLevel {
			trace_util_0.Count(_update_00000, 50)
			h.logDetailedInfo(q)
		}
	}
	trace_util_0.Count(_update_00000, 45)
	metrics.StatsInaccuracyRate.Observe(rate)
	s.Lock()
	defer s.Unlock()
	isIndex := q.Tp == statistics.IndexType
	s.rateMap.update(q.PhysicalID, q.Hist.ID, rate, isIndex)
	if len(s.feedback) < int(MaxQueryFeedbackCount.Load()) {
		trace_util_0.Count(_update_00000, 51)
		s.feedback = append(s.feedback, q)
	}
	trace_util_0.Count(_update_00000, 46)
	return nil
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	trace_util_0.Count(_update_00000, 52)
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper:  make(tableDeltaMap),
		rateMap: make(errorRateDeltaMap),
		next:    h.listHead.next,
	}
	h.listHead.next = newCollector
	return newCollector
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta returns true when only updates a small portion of the table and the time since last update
// do not exceed one hour.
func needDumpStatsDelta(h *Handle, id int64, item variable.TableDelta, currentTime time.Time) bool {
	trace_util_0.Count(_update_00000, 53)
	if item.InitTime.IsZero() {
		trace_util_0.Count(_update_00000, 58)
		item.InitTime = currentTime
	}
	trace_util_0.Count(_update_00000, 54)
	tbl, ok := h.StatsCache.Load().(StatsCache)[id]
	if !ok {
		trace_util_0.Count(_update_00000, 59)
		// No need to dump if the stats is invalid.
		return false
	}
	trace_util_0.Count(_update_00000, 55)
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		trace_util_0.Count(_update_00000, 60)
		// Dump the stats to kv at least once an hour.
		return true
	}
	trace_util_0.Count(_update_00000, 56)
	if tbl.Count == 0 || float64(item.Count)/float64(tbl.Count) > DumpStatsDeltaRatio {
		trace_util_0.Count(_update_00000, 61)
		// Dump the stats when there are many modifications.
		return true
	}
	trace_util_0.Count(_update_00000, 57)
	return false
}

const (
	// DumpAll indicates dump all the delta info in to kv
	DumpAll = true
	// DumpDelta indicates dump part of the delta info in to kv.
	DumpDelta = false
)

// sweepList will loop over the list, merge each session's local stats into handle
// and remove closed session's collector.
func (h *Handle) sweepList() {
	trace_util_0.Count(_update_00000, 62)
	prev := h.listHead
	prev.Lock()
	errorRateMap := make(errorRateDeltaMap)
	for curr := prev.next; curr != nil; curr = curr.next {
		trace_util_0.Count(_update_00000, 64)
		curr.Lock()
		// Merge the session stats into handle and error rate map.
		h.merge(curr, errorRateMap)
		if curr.deleted {
			trace_util_0.Count(_update_00000, 65)
			prev.next = curr.next
			// Since the session is already closed, we can safely unlock it here.
			curr.Unlock()
		} else {
			trace_util_0.Count(_update_00000, 66)
			{
				// Unlock the previous lock, so we only holds at most two session's lock at the same time.
				prev.Unlock()
				prev = curr
			}
		}
	}
	trace_util_0.Count(_update_00000, 63)
	prev.Unlock()
	h.mu.Lock()
	h.mu.rateMap.merge(errorRateMap)
	h.mu.Unlock()
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the `dumpAll` is false, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(dumpMode bool) error {
	trace_util_0.Count(_update_00000, 67)
	h.sweepList()
	currentTime := time.Now()
	for id, item := range h.globalMap {
		trace_util_0.Count(_update_00000, 69)
		if dumpMode == DumpDelta && !needDumpStatsDelta(h, id, item, currentTime) {
			trace_util_0.Count(_update_00000, 74)
			continue
		}
		trace_util_0.Count(_update_00000, 70)
		updated, err := h.dumpTableStatCountToKV(id, item)
		if err != nil {
			trace_util_0.Count(_update_00000, 75)
			return errors.Trace(err)
		}
		trace_util_0.Count(_update_00000, 71)
		if updated {
			trace_util_0.Count(_update_00000, 76)
			h.globalMap.update(id, -item.Delta, -item.Count, nil)
		}
		trace_util_0.Count(_update_00000, 72)
		if err = h.dumpTableStatColSizeToKV(id, item); err != nil {
			trace_util_0.Count(_update_00000, 77)
			return errors.Trace(err)
		}
		trace_util_0.Count(_update_00000, 73)
		if updated {
			trace_util_0.Count(_update_00000, 78)
			delete(h.globalMap, id)
		} else {
			trace_util_0.Count(_update_00000, 79)
			{
				m := h.globalMap[id]
				m.ColSize = nil
				h.globalMap[id] = m
			}
		}
	}
	trace_util_0.Count(_update_00000, 68)
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (updated bool, err error) {
	trace_util_0.Count(_update_00000, 80)
	if delta.Count == 0 {
		trace_util_0.Count(_update_00000, 87)
		return true, nil
	}
	trace_util_0.Count(_update_00000, 81)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		trace_util_0.Count(_update_00000, 88)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 82)
	defer func() {
		trace_util_0.Count(_update_00000, 89)
		err = finishTransaction(context.Background(), exec, err)
	}()

	trace_util_0.Count(_update_00000, 83)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_update_00000, 90)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 84)
	startTS := txn.StartTS()
	var sql string
	if delta.Delta < 0 {
		trace_util_0.Count(_update_00000, 91)
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count - %d, modify_count = modify_count + %d where table_id = %d and count >= %d", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		trace_util_0.Count(_update_00000, 92)
		{
			sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", startTS, delta.Delta, delta.Count, id)
		}
	}
	trace_util_0.Count(_update_00000, 85)
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_update_00000, 93)
		return
	}
	trace_util_0.Count(_update_00000, 86)
	updated = h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	return
}

func (h *Handle) dumpTableStatColSizeToKV(id int64, delta variable.TableDelta) error {
	trace_util_0.Count(_update_00000, 94)
	if len(delta.ColSize) == 0 {
		trace_util_0.Count(_update_00000, 98)
		return nil
	}
	trace_util_0.Count(_update_00000, 95)
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		trace_util_0.Count(_update_00000, 99)
		if deltaColSize == 0 {
			trace_util_0.Count(_update_00000, 101)
			continue
		}
		trace_util_0.Count(_update_00000, 100)
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	trace_util_0.Count(_update_00000, 96)
	if len(values) == 0 {
		trace_util_0.Count(_update_00000, 102)
		return nil
	}
	trace_util_0.Count(_update_00000, 97)
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	return errors.Trace(err)
}

// DumpStatsFeedbackToKV dumps the stats feedback to KV.
func (h *Handle) DumpStatsFeedbackToKV() error {
	trace_util_0.Count(_update_00000, 103)
	var err error
	var successCount int
	for _, fb := range h.feedback {
		trace_util_0.Count(_update_00000, 105)
		if fb.Tp == statistics.PkType {
			trace_util_0.Count(_update_00000, 108)
			err = h.DumpFeedbackToKV(fb)
		} else {
			trace_util_0.Count(_update_00000, 109)
			{
				t, ok := h.StatsCache.Load().(StatsCache)[fb.PhysicalID]
				if ok {
					trace_util_0.Count(_update_00000, 110)
					err = h.DumpFeedbackForIndex(fb, t)
				}
			}
		}
		trace_util_0.Count(_update_00000, 106)
		if err != nil {
			trace_util_0.Count(_update_00000, 111)
			break
		}
		trace_util_0.Count(_update_00000, 107)
		successCount++
	}
	trace_util_0.Count(_update_00000, 104)
	h.feedback = h.feedback[successCount:]
	return errors.Trace(err)
}

// DumpFeedbackToKV dumps the given feedback to physical kv layer.
func (h *Handle) DumpFeedbackToKV(fb *statistics.QueryFeedback) error {
	trace_util_0.Count(_update_00000, 112)
	vals, err := statistics.EncodeFeedback(fb)
	if err != nil {
		trace_util_0.Count(_update_00000, 116)
		logutil.Logger(context.Background()).Debug("error occurred when encoding feedback", zap.Error(err))
		return nil
	}
	trace_util_0.Count(_update_00000, 113)
	var isIndex int64
	if fb.Tp == statistics.IndexType {
		trace_util_0.Count(_update_00000, 117)
		isIndex = 1
	}
	trace_util_0.Count(_update_00000, 114)
	sql := fmt.Sprintf("insert into mysql.stats_feedback (table_id, hist_id, is_index, feedback) values "+
		"(%d, %d, %d, X'%X')", fb.PhysicalID, fb.Hist.ID, isIndex, vals)
	h.mu.Lock()
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	h.mu.Unlock()
	if err != nil {
		trace_util_0.Count(_update_00000, 118)
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
	} else {
		trace_util_0.Count(_update_00000, 119)
		{
			metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
		}
	}
	trace_util_0.Count(_update_00000, 115)
	return errors.Trace(err)
}

// UpdateStatsByLocalFeedback will update statistics by the local feedback.
// Currently, we dump the feedback with the period of 10 minutes, which means
// it takes 10 minutes for a feedback to take effect. However, we can use the
// feedback locally on this tidb-server, so it could be used more timely.
func (h *Handle) UpdateStatsByLocalFeedback(is infoschema.InfoSchema) {
	trace_util_0.Count(_update_00000, 120)
	h.sweepList()
	for _, fb := range h.feedback {
		trace_util_0.Count(_update_00000, 121)
		h.mu.Lock()
		table, ok := h.getTableByPhysicalID(is, fb.PhysicalID)
		h.mu.Unlock()
		if !ok {
			trace_util_0.Count(_update_00000, 124)
			continue
		}
		trace_util_0.Count(_update_00000, 122)
		tblStats := h.GetPartitionStats(table.Meta(), fb.PhysicalID)
		newTblStats := tblStats.Copy()
		if fb.Tp == statistics.IndexType {
			trace_util_0.Count(_update_00000, 125)
			idx, ok := tblStats.Indices[fb.Hist.ID]
			if !ok || idx.Histogram.Len() == 0 {
				trace_util_0.Count(_update_00000, 127)
				continue
			}
			trace_util_0.Count(_update_00000, 126)
			newIdx := *idx
			eqFB, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
			newIdx.CMSketch = statistics.UpdateCMSketch(idx.CMSketch, eqFB)
			newIdx.Histogram = *statistics.UpdateHistogram(&idx.Histogram, &statistics.QueryFeedback{Feedback: ranFB})
			newIdx.Histogram.PreCalculateScalar()
			newIdx.Flag = statistics.ResetAnalyzeFlag(newIdx.Flag)
			newTblStats.Indices[fb.Hist.ID] = &newIdx
		} else {
			trace_util_0.Count(_update_00000, 128)
			{
				col, ok := tblStats.Columns[fb.Hist.ID]
				if !ok || col.Histogram.Len() == 0 {
					trace_util_0.Count(_update_00000, 130)
					continue
				}
				trace_util_0.Count(_update_00000, 129)
				newCol := *col
				// only use the range query to update primary key
				_, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
				newFB := &statistics.QueryFeedback{Feedback: ranFB}
				newFB = newFB.DecodeIntValues()
				newCol.Histogram = *statistics.UpdateHistogram(&col.Histogram, newFB)
				newCol.Flag = statistics.ResetAnalyzeFlag(newCol.Flag)
				newTblStats.Columns[fb.Hist.ID] = &newCol
			}
		}
		trace_util_0.Count(_update_00000, 123)
		h.UpdateTableStats([]*statistics.Table{newTblStats}, nil)
	}
}

// UpdateErrorRate updates the error rate of columns from h.rateMap to cache.
func (h *Handle) UpdateErrorRate(is infoschema.InfoSchema) {
	trace_util_0.Count(_update_00000, 131)
	h.mu.Lock()
	tbls := make([]*statistics.Table, 0, len(h.mu.rateMap))
	for id, item := range h.mu.rateMap {
		trace_util_0.Count(_update_00000, 133)
		table, ok := h.getTableByPhysicalID(is, id)
		if !ok {
			trace_util_0.Count(_update_00000, 137)
			continue
		}
		trace_util_0.Count(_update_00000, 134)
		tbl := h.GetPartitionStats(table.Meta(), id).Copy()
		if item.PkErrorRate != nil && tbl.Columns[item.PkID] != nil {
			trace_util_0.Count(_update_00000, 138)
			col := *tbl.Columns[item.PkID]
			col.ErrorRate.Merge(item.PkErrorRate)
			tbl.Columns[item.PkID] = &col
		}
		trace_util_0.Count(_update_00000, 135)
		for key, val := range item.IdxErrorRate {
			trace_util_0.Count(_update_00000, 139)
			if tbl.Indices[key] == nil {
				trace_util_0.Count(_update_00000, 141)
				continue
			}
			trace_util_0.Count(_update_00000, 140)
			idx := *tbl.Indices[key]
			idx.ErrorRate.Merge(val)
			tbl.Indices[key] = &idx
		}
		trace_util_0.Count(_update_00000, 136)
		tbls = append(tbls, tbl)
		delete(h.mu.rateMap, id)
	}
	trace_util_0.Count(_update_00000, 132)
	h.mu.Unlock()
	h.UpdateTableStats(tbls, nil)
}

// HandleUpdateStats update the stats using feedback.
func (h *Handle) HandleUpdateStats(is infoschema.InfoSchema) error {
	trace_util_0.Count(_update_00000, 142)
	sql := "select table_id, hist_id, is_index, feedback from mysql.stats_feedback order by table_id, hist_id, is_index"
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	if len(rows) == 0 || err != nil {
		trace_util_0.Count(_update_00000, 146)
		return errors.Trace(err)
	}

	trace_util_0.Count(_update_00000, 143)
	var groupedRows [][]chunk.Row
	preIdx := 0
	tableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	for i := 1; i < len(rows); i++ {
		trace_util_0.Count(_update_00000, 147)
		row := rows[i]
		if row.GetInt64(0) != tableID || row.GetInt64(1) != histID || row.GetInt64(2) != isIndex {
			trace_util_0.Count(_update_00000, 148)
			groupedRows = append(groupedRows, rows[preIdx:i])
			tableID, histID, isIndex = row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
			preIdx = i
		}
	}
	trace_util_0.Count(_update_00000, 144)
	groupedRows = append(groupedRows, rows[preIdx:])

	for _, rows := range groupedRows {
		trace_util_0.Count(_update_00000, 149)
		if err := h.handleSingleHistogramUpdate(is, rows); err != nil {
			trace_util_0.Count(_update_00000, 150)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_update_00000, 145)
	return nil
}

// handleSingleHistogramUpdate updates the Histogram and CM Sketch using these feedbacks. All the feedbacks for
// the same index or column are gathered in `rows`.
func (h *Handle) handleSingleHistogramUpdate(is infoschema.InfoSchema, rows []chunk.Row) (err error) {
	trace_util_0.Count(_update_00000, 151)
	physicalTableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	defer func() {
		trace_util_0.Count(_update_00000, 158)
		if err == nil {
			trace_util_0.Count(_update_00000, 159)
			err = errors.Trace(h.deleteOutdatedFeedback(physicalTableID, histID, isIndex))
		}
	}()
	trace_util_0.Count(_update_00000, 152)
	h.mu.Lock()
	table, ok := h.getTableByPhysicalID(is, physicalTableID)
	h.mu.Unlock()
	// The table has been deleted.
	if !ok {
		trace_util_0.Count(_update_00000, 160)
		return nil
	}
	trace_util_0.Count(_update_00000, 153)
	var tbl *statistics.Table
	if table.Meta().GetPartitionInfo() != nil {
		trace_util_0.Count(_update_00000, 161)
		tbl = h.GetPartitionStats(table.Meta(), physicalTableID)
	} else {
		trace_util_0.Count(_update_00000, 162)
		{
			tbl = h.GetTableStats(table.Meta())
		}
	}
	trace_util_0.Count(_update_00000, 154)
	var cms *statistics.CMSketch
	var hist *statistics.Histogram
	if isIndex == 1 {
		trace_util_0.Count(_update_00000, 163)
		idx, ok := tbl.Indices[histID]
		if ok && idx.Histogram.Len() > 0 {
			trace_util_0.Count(_update_00000, 164)
			idxHist := idx.Histogram
			hist = &idxHist
			cms = idx.CMSketch.Copy()
		}
	} else {
		trace_util_0.Count(_update_00000, 165)
		{
			col, ok := tbl.Columns[histID]
			if ok && col.Histogram.Len() > 0 {
				trace_util_0.Count(_update_00000, 166)
				colHist := col.Histogram
				hist = &colHist
			}
		}
	}
	// The column or index has been deleted.
	trace_util_0.Count(_update_00000, 155)
	if hist == nil {
		trace_util_0.Count(_update_00000, 167)
		return nil
	}
	trace_util_0.Count(_update_00000, 156)
	q := &statistics.QueryFeedback{}
	for _, row := range rows {
		trace_util_0.Count(_update_00000, 168)
		err1 := statistics.DecodeFeedback(row.GetBytes(3), q, cms, hist.Tp)
		if err1 != nil {
			trace_util_0.Count(_update_00000, 169)
			logutil.Logger(context.Background()).Debug("decode feedback failed", zap.Error(err))
		}
	}
	trace_util_0.Count(_update_00000, 157)
	err = h.dumpStatsUpdateToKV(physicalTableID, isIndex, q, hist, cms)
	return errors.Trace(err)
}

func (h *Handle) deleteOutdatedFeedback(tableID, histID, isIndex int64) error {
	trace_util_0.Count(_update_00000, 170)
	h.mu.Lock()
	h.mu.ctx.GetSessionVars().BatchDelete = true
	sql := fmt.Sprintf("delete from mysql.stats_feedback where table_id = %d and hist_id = %d and is_index = %d", tableID, histID, isIndex)
	_, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	h.mu.ctx.GetSessionVars().BatchDelete = false
	h.mu.Unlock()
	return errors.Trace(err)
}

func (h *Handle) dumpStatsUpdateToKV(tableID, isIndex int64, q *statistics.QueryFeedback, hist *statistics.Histogram, cms *statistics.CMSketch) error {
	trace_util_0.Count(_update_00000, 171)
	hist = statistics.UpdateHistogram(hist, q)
	err := h.SaveStatsToStorage(tableID, -1, int(isIndex), hist, cms, 0)
	metrics.UpdateStatsCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
	return errors.Trace(err)
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// AutoAnalyzeMinCnt means if the count of table is less than this value, we needn't do auto analyze.
var AutoAnalyzeMinCnt int64 = 1000

// TableAnalyzed checks if the table is analyzed.
func TableAnalyzed(tbl *statistics.Table) bool {
	trace_util_0.Count(_update_00000, 172)
	for _, col := range tbl.Columns {
		trace_util_0.Count(_update_00000, 175)
		if col.Count > 0 {
			trace_util_0.Count(_update_00000, 176)
			return true
		}
	}
	trace_util_0.Count(_update_00000, 173)
	for _, idx := range tbl.Indices {
		trace_util_0.Count(_update_00000, 177)
		if idx.Histogram.Len() > 0 {
			trace_util_0.Count(_update_00000, 178)
			return true
		}
	}
	trace_util_0.Count(_update_00000, 174)
	return false
}

// withinTimePeriod tests whether `now` is between `start` and `end`.
func withinTimePeriod(start, end, now time.Time) bool {
	trace_util_0.Count(_update_00000, 179)
	// Converts to UTC and only keeps the hour and minute info.
	start, end, now = start.UTC(), end.UTC(), now.UTC()
	start = time.Date(0, 0, 0, start.Hour(), start.Minute(), 0, 0, time.UTC)
	end = time.Date(0, 0, 0, end.Hour(), end.Minute(), 0, 0, time.UTC)
	now = time.Date(0, 0, 0, now.Hour(), now.Minute(), 0, 0, time.UTC)
	// for cases like from 00:00 to 06:00
	if end.Sub(start) >= 0 {
		trace_util_0.Count(_update_00000, 181)
		return now.Sub(start) >= 0 && now.Sub(end) <= 0
	}
	// for cases like from 22:00 to 06:00
	trace_util_0.Count(_update_00000, 180)
	return now.Sub(end) <= 0 || now.Sub(start) >= 0
}

// NeedAnalyzeTable checks if we need to analyze the table:
// 1. If the table has never been analyzed, we need to analyze it when it has
//    not been modified for a while.
// 2. If the table had been analyzed before, we need to analyze it when
//    "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//    between `start` and `end`.
func NeedAnalyzeTable(tbl *statistics.Table, limit time.Duration, autoAnalyzeRatio float64, start, end, now time.Time) (bool, string) {
	trace_util_0.Count(_update_00000, 182)
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		trace_util_0.Count(_update_00000, 186)
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		dur := time.Since(t)
		return dur >= limit, fmt.Sprintf("table unanalyzed, time since last updated %vs", dur)
	}
	// Auto analyze is disabled.
	trace_util_0.Count(_update_00000, 183)
	if autoAnalyzeRatio == 0 {
		trace_util_0.Count(_update_00000, 187)
		return false, ""
	}
	// No need to analyze it.
	trace_util_0.Count(_update_00000, 184)
	if float64(tbl.ModifyCount)/float64(tbl.Count) <= autoAnalyzeRatio {
		trace_util_0.Count(_update_00000, 188)
		return false, ""
	}
	// Tests if current time is within the time period.
	trace_util_0.Count(_update_00000, 185)
	return withinTimePeriod(start, end, now), fmt.Sprintf("too many modifications(%v/%v)", tbl.ModifyCount, tbl.Count)
}

const (
	minAutoAnalyzeRatio = 0.3
)

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	trace_util_0.Count(_update_00000, 189)
	sql := fmt.Sprintf("select variable_name, variable_value from mysql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.TiDBAutoAnalyzeRatio, variable.TiDBAutoAnalyzeStartTime, variable.TiDBAutoAnalyzeEndTime)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_update_00000, 192)
		return map[string]string{}
	}
	trace_util_0.Count(_update_00000, 190)
	parameters := make(map[string]string)
	for _, row := range rows {
		trace_util_0.Count(_update_00000, 193)
		parameters[row.GetString(0)] = row.GetString(1)
	}
	trace_util_0.Count(_update_00000, 191)
	return parameters
}

func parseAutoAnalyzeRatio(ratio string) float64 {
	trace_util_0.Count(_update_00000, 194)
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		trace_util_0.Count(_update_00000, 197)
		return variable.DefAutoAnalyzeRatio
	}
	trace_util_0.Count(_update_00000, 195)
	if autoAnalyzeRatio > 0 {
		trace_util_0.Count(_update_00000, 198)
		autoAnalyzeRatio = math.Max(autoAnalyzeRatio, minAutoAnalyzeRatio)
	}
	trace_util_0.Count(_update_00000, 196)
	return autoAnalyzeRatio
}

func parseAnalyzePeriod(start, end string) (time.Time, time.Time, error) {
	trace_util_0.Count(_update_00000, 199)
	if start == "" {
		trace_util_0.Count(_update_00000, 204)
		start = variable.DefAutoAnalyzeStartTime
	}
	trace_util_0.Count(_update_00000, 200)
	if end == "" {
		trace_util_0.Count(_update_00000, 205)
		end = variable.DefAutoAnalyzeEndTime
	}
	trace_util_0.Count(_update_00000, 201)
	s, err := time.ParseInLocation(variable.AnalyzeFullTimeFormat, start, time.UTC)
	if err != nil {
		trace_util_0.Count(_update_00000, 206)
		return s, s, errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 202)
	e, err := time.ParseInLocation(variable.AnalyzeFullTimeFormat, end, time.UTC)
	if err != nil {
		trace_util_0.Count(_update_00000, 207)
		return s, e, errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 203)
	return s, e, nil
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) {
	trace_util_0.Count(_update_00000, 208)
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		trace_util_0.Count(_update_00000, 211)
		logutil.Logger(context.Background()).Error("[stats] parse auto analyze period failed", zap.Error(err))
		return
	}
	trace_util_0.Count(_update_00000, 209)
	for _, db := range dbs {
		trace_util_0.Count(_update_00000, 212)
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			trace_util_0.Count(_update_00000, 213)
			tblInfo := tbl.Meta()
			pi := tblInfo.GetPartitionInfo()
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if pi == nil {
				trace_util_0.Count(_update_00000, 215)
				statsTbl := h.GetTableStats(tblInfo)
				sql := fmt.Sprintf("analyze table %s", tblName)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					trace_util_0.Count(_update_00000, 217)
					return
				}
				trace_util_0.Count(_update_00000, 216)
				continue
			}
			trace_util_0.Count(_update_00000, 214)
			for _, def := range pi.Definitions {
				trace_util_0.Count(_update_00000, 218)
				sql := fmt.Sprintf("analyze table %s partition `%s`", tblName, def.Name.O)
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					trace_util_0.Count(_update_00000, 220)
					return
				}
				trace_util_0.Count(_update_00000, 219)
				continue
			}
		}
	}
	trace_util_0.Count(_update_00000, 210)
	return
}

func (h *Handle) autoAnalyzeTable(tblInfo *model.TableInfo, statsTbl *statistics.Table, start, end time.Time, ratio float64, sql string) bool {
	trace_util_0.Count(_update_00000, 221)
	if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
		trace_util_0.Count(_update_00000, 225)
		return false
	}
	trace_util_0.Count(_update_00000, 222)
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio, start, end, time.Now()); needAnalyze {
		trace_util_0.Count(_update_00000, 226)
		logutil.Logger(context.Background()).Info("[stats] auto analyze triggered", zap.String("sql", sql), zap.String("reason", reason))
		h.execAutoAnalyze(sql)
		return true
	}
	trace_util_0.Count(_update_00000, 223)
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_update_00000, 227)
		if idx.State != model.StatePublic {
			trace_util_0.Count(_update_00000, 229)
			continue
		}
		trace_util_0.Count(_update_00000, 228)
		if _, ok := statsTbl.Indices[idx.ID]; !ok {
			trace_util_0.Count(_update_00000, 230)
			sql = fmt.Sprintf("%s index `%s`", sql, idx.Name.O)
			logutil.Logger(context.Background()).Info("[stats] auto analyze for unanalyzed", zap.String("sql", sql))
			h.execAutoAnalyze(sql)
			return true
		}
	}
	trace_util_0.Count(_update_00000, 224)
	return false
}

func (h *Handle) execAutoAnalyze(sql string) {
	trace_util_0.Count(_update_00000, 231)
	startTime := time.Now()
	_, _, err := h.restrictedExec.ExecRestrictedSQL(nil, sql)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		trace_util_0.Count(_update_00000, 232)
		logutil.Logger(context.Background()).Error("[stats] auto analyze failed", zap.String("sql", sql), zap.Duration("cost_time", dur), zap.Error(err))
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		trace_util_0.Count(_update_00000, 233)
		{
			metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
		}
	}
}

// formatBuckets formats bucket from lowBkt to highBkt.
func formatBuckets(hg *statistics.Histogram, lowBkt, highBkt, idxCols int) string {
	trace_util_0.Count(_update_00000, 234)
	if lowBkt == highBkt {
		trace_util_0.Count(_update_00000, 237)
		return hg.BucketToString(lowBkt, idxCols)
	}
	trace_util_0.Count(_update_00000, 235)
	if lowBkt+1 == highBkt {
		trace_util_0.Count(_update_00000, 238)
		return fmt.Sprintf("%s, %s", hg.BucketToString(lowBkt, 0), hg.BucketToString(highBkt, 0))
	}
	// do not care the middle buckets
	trace_util_0.Count(_update_00000, 236)
	return fmt.Sprintf("%s, (%d buckets, total count %d), %s", hg.BucketToString(lowBkt, 0),
		highBkt-lowBkt-1, hg.Buckets[highBkt-1].Count-hg.Buckets[lowBkt].Count, hg.BucketToString(highBkt, 0))
}

func colRangeToStr(c *statistics.Column, ran *ranger.Range, actual int64, factor float64) string {
	trace_util_0.Count(_update_00000, 239)
	lowCount, lowBkt := c.LessRowCountWithBktIdx(ran.LowVal[0])
	highCount, highBkt := c.LessRowCountWithBktIdx(ran.HighVal[0])
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, buckets: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&c.Histogram, lowBkt, highBkt, 0))
}

func logForIndexRange(idx *statistics.Index, ran *ranger.Range, actual int64, factor float64) string {
	trace_util_0.Count(_update_00000, 240)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	lb, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		trace_util_0.Count(_update_00000, 244)
		return ""
	}
	trace_util_0.Count(_update_00000, 241)
	rb, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		trace_util_0.Count(_update_00000, 245)
		return ""
	}
	trace_util_0.Count(_update_00000, 242)
	if idx.CMSketch != nil && bytes.Compare(kv.Key(lb).PrefixNext(), rb) >= 0 {
		trace_util_0.Count(_update_00000, 246)
		str, err := types.DatumsToString(ran.LowVal, true)
		if err != nil {
			trace_util_0.Count(_update_00000, 248)
			return ""
		}
		trace_util_0.Count(_update_00000, 247)
		return fmt.Sprintf("value: %s, actual: %d, expected: %d", str, actual, int64(float64(idx.QueryBytes(lb))*factor))
	}
	trace_util_0.Count(_update_00000, 243)
	l, r := types.NewBytesDatum(lb), types.NewBytesDatum(rb)
	lowCount, lowBkt := idx.LessRowCountWithBktIdx(l)
	highCount, highBkt := idx.LessRowCountWithBktIdx(r)
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, histogram: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&idx.Histogram, lowBkt, highBkt, len(idx.Info.Columns)))
}

func logForIndex(prefix string, t *statistics.Table, idx *statistics.Index, ranges []*ranger.Range, actual []int64, factor float64) {
	trace_util_0.Count(_update_00000, 249)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		trace_util_0.Count(_update_00000, 251)
		for i, ran := range ranges {
			trace_util_0.Count(_update_00000, 253)
			logutil.Logger(context.Background()).Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
		}
		trace_util_0.Count(_update_00000, 252)
		return
	}
	trace_util_0.Count(_update_00000, 250)
	for i, ran := range ranges {
		trace_util_0.Count(_update_00000, 254)
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			trace_util_0.Count(_update_00000, 258)
			logutil.Logger(context.Background()).Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
			continue
		}
		trace_util_0.Count(_update_00000, 255)
		equalityString, err := types.DatumsToString(ran.LowVal[:rangePosition], true)
		if err != nil {
			trace_util_0.Count(_update_00000, 259)
			continue
		}
		trace_util_0.Count(_update_00000, 256)
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			trace_util_0.Count(_update_00000, 260)
			continue
		}
		trace_util_0.Count(_update_00000, 257)
		equalityCount := idx.CMSketch.QueryBytes(bytes)
		rang := ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		// prefer index stats over column stats
		if idxHist := t.IndexStartWithColumn(colName); idxHist != nil && idxHist.Histogram.Len() > 0 {
			trace_util_0.Count(_update_00000, 261)
			rangeString := logForIndexRange(idxHist, &rang, -1, factor)
			logutil.Logger(context.Background()).Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
				zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
				zap.String("range", rangeString))
		} else {
			trace_util_0.Count(_update_00000, 262)
			if colHist := t.ColumnByName(colName); colHist != nil && colHist.Histogram.Len() > 0 {
				trace_util_0.Count(_update_00000, 263)
				err = convertRangeType(&rang, colHist.Tp, time.UTC)
				if err == nil {
					trace_util_0.Count(_update_00000, 264)
					rangeString := colRangeToStr(colHist, &rang, -1, factor)
					logutil.Logger(context.Background()).Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
						zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
						zap.String("range", rangeString))
				}
			} else {
				trace_util_0.Count(_update_00000, 265)
				{
					count, err := statistics.GetPseudoRowCountByColumnRanges(sc, float64(t.Count), []*ranger.Range{&rang}, 0)
					if err == nil {
						trace_util_0.Count(_update_00000, 266)
						logutil.Logger(context.Background()).Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
							zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
							zap.Stringer("range", &rang), zap.Float64("pseudo count", math.Round(count)))
					}
				}
			}
		}
	}
}

func (h *Handle) logDetailedInfo(q *statistics.QueryFeedback) {
	trace_util_0.Count(_update_00000, 267)
	t, ok := h.StatsCache.Load().(StatsCache)[q.PhysicalID]
	if !ok {
		trace_util_0.Count(_update_00000, 271)
		return
	}
	trace_util_0.Count(_update_00000, 268)
	isIndex := q.Hist.IsIndexHist()
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		trace_util_0.Count(_update_00000, 272)
		logutil.Logger(context.Background()).Debug("decode to ranges failed", zap.Error(err))
		return
	}
	trace_util_0.Count(_update_00000, 269)
	actual := make([]int64, 0, len(q.Feedback))
	for _, fb := range q.Feedback {
		trace_util_0.Count(_update_00000, 273)
		actual = append(actual, fb.Count)
	}
	trace_util_0.Count(_update_00000, 270)
	logPrefix := fmt.Sprintf("[stats-feedback] %s", t.Name)
	if isIndex {
		trace_util_0.Count(_update_00000, 274)
		idx := t.Indices[q.Hist.ID]
		if idx == nil || idx.Histogram.Len() == 0 {
			trace_util_0.Count(_update_00000, 276)
			return
		}
		trace_util_0.Count(_update_00000, 275)
		logForIndex(logPrefix, t, idx, ranges, actual, idx.GetIncreaseFactor(t.Count))
	} else {
		trace_util_0.Count(_update_00000, 277)
		{
			c := t.Columns[q.Hist.ID]
			if c == nil || c.Histogram.Len() == 0 {
				trace_util_0.Count(_update_00000, 279)
				return
			}
			trace_util_0.Count(_update_00000, 278)
			logForPK(logPrefix, c, ranges, actual, c.GetIncreaseFactor(t.Count))
		}
	}
}

func logForPK(prefix string, c *statistics.Column, ranges []*ranger.Range, actual []int64, factor float64) {
	trace_util_0.Count(_update_00000, 280)
	for i, ran := range ranges {
		trace_util_0.Count(_update_00000, 281)
		if ran.LowVal[0].GetInt64()+1 >= ran.HighVal[0].GetInt64() {
			trace_util_0.Count(_update_00000, 283)
			continue
		}
		trace_util_0.Count(_update_00000, 282)
		logutil.Logger(context.Background()).Debug(prefix, zap.String("column", c.Info.Name.O), zap.String("rangeStr", colRangeToStr(c, ran, actual[i], factor)))
	}
}

// RecalculateExpectCount recalculates the expect row count if the origin row count is estimated by pseudo.
func (h *Handle) RecalculateExpectCount(q *statistics.QueryFeedback) error {
	trace_util_0.Count(_update_00000, 284)
	t, ok := h.StatsCache.Load().(StatsCache)[q.PhysicalID]
	if !ok {
		trace_util_0.Count(_update_00000, 292)
		return nil
	}
	trace_util_0.Count(_update_00000, 285)
	tablePseudo := t.Pseudo || t.IsOutdated()
	if !tablePseudo {
		trace_util_0.Count(_update_00000, 293)
		return nil
	}
	trace_util_0.Count(_update_00000, 286)
	isIndex := q.Hist.Tp.Tp == mysql.TypeBlob
	id := q.Hist.ID
	if isIndex && (t.Indices[id] == nil || !t.Indices[id].NotAccurate()) {
		trace_util_0.Count(_update_00000, 294)
		return nil
	}
	trace_util_0.Count(_update_00000, 287)
	if !isIndex && (t.Columns[id] == nil || !t.Columns[id].NotAccurate()) {
		trace_util_0.Count(_update_00000, 295)
		return nil
	}

	trace_util_0.Count(_update_00000, 288)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		trace_util_0.Count(_update_00000, 296)
		return errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 289)
	expected := 0.0
	if isIndex {
		trace_util_0.Count(_update_00000, 297)
		idx := t.Indices[id]
		expected, err = idx.GetRowCount(sc, ranges, t.ModifyCount)
		expected *= idx.GetIncreaseFactor(t.Count)
	} else {
		trace_util_0.Count(_update_00000, 298)
		{
			c := t.Columns[id]
			expected, err = c.GetColumnRowCount(sc, ranges, t.ModifyCount)
			expected *= c.GetIncreaseFactor(t.Count)
		}
	}
	trace_util_0.Count(_update_00000, 290)
	if err != nil {
		trace_util_0.Count(_update_00000, 299)
		return errors.Trace(err)
	}
	trace_util_0.Count(_update_00000, 291)
	q.Expected = int64(expected)
	return nil
}

func (h *Handle) dumpRangeFeedback(sc *stmtctx.StatementContext, ran *ranger.Range, rangeCount float64, q *statistics.QueryFeedback) error {
	trace_util_0.Count(_update_00000, 300)
	lowIsNull := ran.LowVal[0].IsNull()
	if q.Tp == statistics.IndexType {
		trace_util_0.Count(_update_00000, 306)
		lower, err := codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			trace_util_0.Count(_update_00000, 309)
			return errors.Trace(err)
		}
		trace_util_0.Count(_update_00000, 307)
		upper, err := codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			trace_util_0.Count(_update_00000, 310)
			return errors.Trace(err)
		}
		trace_util_0.Count(_update_00000, 308)
		ran.LowVal[0].SetBytes(lower)
		ran.HighVal[0].SetBytes(upper)
	} else {
		trace_util_0.Count(_update_00000, 311)
		{
			if !statistics.SupportColumnType(q.Hist.Tp) {
				trace_util_0.Count(_update_00000, 314)
				return nil
			}
			trace_util_0.Count(_update_00000, 312)
			if ran.LowVal[0].Kind() == types.KindMinNotNull {
				trace_util_0.Count(_update_00000, 315)
				ran.LowVal[0] = statistics.GetMinValue(q.Hist.Tp)
			}
			trace_util_0.Count(_update_00000, 313)
			if ran.HighVal[0].Kind() == types.KindMaxValue {
				trace_util_0.Count(_update_00000, 316)
				ran.HighVal[0] = statistics.GetMaxValue(q.Hist.Tp)
			}
		}
	}
	trace_util_0.Count(_update_00000, 301)
	ranges, ok := q.Hist.SplitRange(sc, []*ranger.Range{ran}, q.Tp == statistics.IndexType)
	if !ok {
		trace_util_0.Count(_update_00000, 317)
		logutil.Logger(context.Background()).Debug("type of histogram and ranges mismatch")
		return nil
	}
	trace_util_0.Count(_update_00000, 302)
	counts := make([]float64, 0, len(ranges))
	sum := 0.0
	for i, r := range ranges {
		trace_util_0.Count(_update_00000, 318)
		// Though after `SplitRange`, we may have ranges like `[l, r]`, we still use
		// `betweenRowCount` to compute the estimation since the ranges of feedback are all in `[l, r)`
		// form, that is to say, we ignore the exclusiveness of ranges from `SplitRange` and just use
		// its result of boundary values.
		count := q.Hist.BetweenRowCount(r.LowVal[0], r.HighVal[0])
		// We have to include `NullCount` of histogram for [l, r) cases where l is null because `betweenRowCount`
		// does not include null values of lower bound.
		if i == 0 && lowIsNull {
			trace_util_0.Count(_update_00000, 320)
			count += float64(q.Hist.NullCount)
		}
		trace_util_0.Count(_update_00000, 319)
		sum += count
		counts = append(counts, count)
	}
	trace_util_0.Count(_update_00000, 303)
	if sum <= 1 {
		trace_util_0.Count(_update_00000, 321)
		return nil
	}
	// We assume that each part contributes the same error rate.
	trace_util_0.Count(_update_00000, 304)
	adjustFactor := rangeCount / sum
	for i, r := range ranges {
		trace_util_0.Count(_update_00000, 322)
		q.Feedback = append(q.Feedback, statistics.Feedback{Lower: &r.LowVal[0], Upper: &r.HighVal[0], Count: int64(counts[i] * adjustFactor)})
	}
	trace_util_0.Count(_update_00000, 305)
	return errors.Trace(h.DumpFeedbackToKV(q))
}

func convertRangeType(ran *ranger.Range, ft *types.FieldType, loc *time.Location) error {
	trace_util_0.Count(_update_00000, 323)
	err := statistics.ConvertDatumsType(ran.LowVal, ft, loc)
	if err != nil {
		trace_util_0.Count(_update_00000, 325)
		return err
	}
	trace_util_0.Count(_update_00000, 324)
	return statistics.ConvertDatumsType(ran.HighVal, ft, loc)
}

// DumpFeedbackForIndex dumps the feedback for index.
// For queries that contains both equality and range query, we will split them and Update accordingly.
func (h *Handle) DumpFeedbackForIndex(q *statistics.QueryFeedback, t *statistics.Table) error {
	trace_util_0.Count(_update_00000, 326)
	idx, ok := t.Indices[q.Hist.ID]
	if !ok {
		trace_util_0.Count(_update_00000, 331)
		return nil
	}
	trace_util_0.Count(_update_00000, 327)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		trace_util_0.Count(_update_00000, 332)
		return h.DumpFeedbackToKV(q)
	}
	trace_util_0.Count(_update_00000, 328)
	ranges, err := q.DecodeToRanges(true)
	if err != nil {
		trace_util_0.Count(_update_00000, 333)
		logutil.Logger(context.Background()).Debug("decode feedback ranges fail", zap.Error(err))
		return nil
	}
	trace_util_0.Count(_update_00000, 329)
	for i, ran := range ranges {
		trace_util_0.Count(_update_00000, 334)
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			trace_util_0.Count(_update_00000, 339)
			continue
		}

		trace_util_0.Count(_update_00000, 335)
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			trace_util_0.Count(_update_00000, 340)
			logutil.Logger(context.Background()).Debug("encode keys fail", zap.Error(err))
			continue
		}
		trace_util_0.Count(_update_00000, 336)
		equalityCount := float64(idx.CMSketch.QueryBytes(bytes)) * idx.GetIncreaseFactor(t.Count)
		rang := &ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		var rangeCount float64
		rangeFB := &statistics.QueryFeedback{PhysicalID: q.PhysicalID}
		// prefer index stats over column stats
		if idx := t.IndexStartWithColumn(colName); idx != nil && idx.Histogram.Len() != 0 {
			trace_util_0.Count(_update_00000, 341)
			rangeCount, err = t.GetRowCountByIndexRanges(sc, idx.ID, []*ranger.Range{rang})
			rangeFB.Tp, rangeFB.Hist = statistics.IndexType, &idx.Histogram
		} else {
			trace_util_0.Count(_update_00000, 342)
			if col := t.ColumnByName(colName); col != nil && col.Histogram.Len() != 0 {
				trace_util_0.Count(_update_00000, 343)
				err = convertRangeType(rang, col.Tp, time.UTC)
				if err == nil {
					trace_util_0.Count(_update_00000, 344)
					rangeCount, err = t.GetRowCountByColumnRanges(sc, col.ID, []*ranger.Range{rang})
					rangeFB.Tp, rangeFB.Hist = statistics.ColType, &col.Histogram
				}
			} else {
				trace_util_0.Count(_update_00000, 345)
				{
					continue
				}
			}
		}
		trace_util_0.Count(_update_00000, 337)
		if err != nil {
			trace_util_0.Count(_update_00000, 346)
			logutil.Logger(context.Background()).Debug("get row count by ranges fail", zap.Error(err))
			continue
		}

		trace_util_0.Count(_update_00000, 338)
		equalityCount, rangeCount = getNewCountForIndex(equalityCount, rangeCount, float64(t.Count), float64(q.Feedback[i].Count))
		value := types.NewBytesDatum(bytes)
		q.Feedback[i] = statistics.Feedback{Lower: &value, Upper: &value, Count: int64(equalityCount)}
		err = h.dumpRangeFeedback(sc, rang, rangeCount, rangeFB)
		if err != nil {
			trace_util_0.Count(_update_00000, 347)
			logutil.Logger(context.Background()).Debug("dump range feedback fail", zap.Error(err))
			continue
		}
	}
	trace_util_0.Count(_update_00000, 330)
	return errors.Trace(h.DumpFeedbackToKV(q))
}

// minAdjustFactor is the minimum adjust factor of each index feedback.
// We use it to avoid adjusting too much when the assumption of independence failed.
const minAdjustFactor = 0.7

// getNewCountForIndex adjust the estimated `eqCount` and `rangeCount` according to the real count.
// We assumes that `eqCount` and `rangeCount` contribute the same error rate.
func getNewCountForIndex(eqCount, rangeCount, totalCount, realCount float64) (float64, float64) {
	trace_util_0.Count(_update_00000, 348)
	estimate := (eqCount / totalCount) * (rangeCount / totalCount) * totalCount
	if estimate <= 1 {
		trace_util_0.Count(_update_00000, 350)
		return eqCount, rangeCount
	}
	trace_util_0.Count(_update_00000, 349)
	adjustFactor := math.Sqrt(realCount / estimate)
	adjustFactor = math.Max(adjustFactor, minAdjustFactor)
	return eqCount * adjustFactor, rangeCount * adjustFactor
}

var _update_00000 = "statistics/handle/update.go"
