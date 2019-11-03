// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     util.SessionManager
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	trace_util_0.Count(_expensivequery_00000, 0)
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	trace_util_0.Count(_expensivequery_00000, 1)
	eqh.sm = sm
	return eqh
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	trace_util_0.Count(_expensivequery_00000, 2)
	threshold := atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
	curInterval := time.Second * time.Duration(threshold)
	ticker := time.NewTicker(curInterval / 2)
	for {
		trace_util_0.Count(_expensivequery_00000, 3)
		select {
		case <-ticker.C:
			trace_util_0.Count(_expensivequery_00000, 4)
			if log.GetLevel() > zapcore.WarnLevel {
				trace_util_0.Count(_expensivequery_00000, 8)
				continue
			}
			trace_util_0.Count(_expensivequery_00000, 5)
			processInfo := eqh.sm.ShowProcessList()
			for _, info := range processInfo {
				trace_util_0.Count(_expensivequery_00000, 9)
				if len(info.Info) == 0 || info.ExceedExpensiveTimeThresh {
					trace_util_0.Count(_expensivequery_00000, 11)
					continue
				}
				trace_util_0.Count(_expensivequery_00000, 10)
				if costTime := time.Since(info.Time); costTime >= curInterval {
					trace_util_0.Count(_expensivequery_00000, 12)
					logExpensiveQuery(costTime, info)
					info.ExceedExpensiveTimeThresh = true
				}
			}
			trace_util_0.Count(_expensivequery_00000, 6)
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
			if newInterval := time.Second * time.Duration(threshold); curInterval != newInterval {
				trace_util_0.Count(_expensivequery_00000, 13)
				curInterval = newInterval
				ticker.Stop()
				ticker = time.NewTicker(curInterval / 2)
			}
		case <-eqh.exitCh:
			trace_util_0.Count(_expensivequery_00000, 7)
			return
		}
	}
}

// LogOnQueryExceedMemQuota prints a log when memory usage of connID is out of memory quota.
func (eqh *Handle) LogOnQueryExceedMemQuota(connID uint64) {
	trace_util_0.Count(_expensivequery_00000, 14)
	if log.GetLevel() > zapcore.WarnLevel {
		trace_util_0.Count(_expensivequery_00000, 17)
		return
	}
	trace_util_0.Count(_expensivequery_00000, 15)
	info, ok := eqh.sm.GetProcessInfo(connID)
	if !ok {
		trace_util_0.Count(_expensivequery_00000, 18)
		return
	}
	trace_util_0.Count(_expensivequery_00000, 16)
	logExpensiveQuery(time.Since(info.Time), info)
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *util.ProcessInfo) {
	trace_util_0.Count(_expensivequery_00000, 19)
	logFields := make([]zap.Field, 0, 20)
	logFields = append(logFields, zap.String("cost_time", strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64)+"s"))
	execDetail := info.StmtCtx.GetExecDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := info.StmtCtx.CopTasksDetails(); copTaskInfo != nil {
		trace_util_0.Count(_expensivequery_00000, 29)
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	trace_util_0.Count(_expensivequery_00000, 20)
	if statsInfo := info.StatsInfo(info.Plan); len(statsInfo) > 0 {
		trace_util_0.Count(_expensivequery_00000, 30)
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfo {
			trace_util_0.Count(_expensivequery_00000, 32)
			if v == 0 {
				trace_util_0.Count(_expensivequery_00000, 34)
				vStr = "pseudo"
			} else {
				trace_util_0.Count(_expensivequery_00000, 35)
				{
					vStr = strconv.FormatUint(v, 10)
				}
			}
			trace_util_0.Count(_expensivequery_00000, 33)
			if firstComma {
				trace_util_0.Count(_expensivequery_00000, 36)
				buf.WriteString("," + k + ":" + vStr)
			} else {
				trace_util_0.Count(_expensivequery_00000, 37)
				{
					buf.WriteString(k + ":" + vStr)
					firstComma = true
				}
			}
		}
		trace_util_0.Count(_expensivequery_00000, 31)
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	trace_util_0.Count(_expensivequery_00000, 21)
	if info.ID != 0 {
		trace_util_0.Count(_expensivequery_00000, 38)
		logFields = append(logFields, zap.Uint64("conn_id", info.ID))
	}
	trace_util_0.Count(_expensivequery_00000, 22)
	if len(info.User) > 0 {
		trace_util_0.Count(_expensivequery_00000, 39)
		logFields = append(logFields, zap.String("user", info.User))
	}
	trace_util_0.Count(_expensivequery_00000, 23)
	if len(info.DB) > 0 {
		trace_util_0.Count(_expensivequery_00000, 40)
		logFields = append(logFields, zap.String("database", info.DB))
	}
	trace_util_0.Count(_expensivequery_00000, 24)
	var tableIDs, indexIDs string
	if len(info.StmtCtx.TableIDs) > 0 {
		trace_util_0.Count(_expensivequery_00000, 41)
		tableIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	trace_util_0.Count(_expensivequery_00000, 25)
	if len(info.StmtCtx.IndexIDs) > 0 {
		trace_util_0.Count(_expensivequery_00000, 42)
		indexIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("index_ids", indexIDs))
	}
	trace_util_0.Count(_expensivequery_00000, 26)
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.StmtCtx.MemTracker; memTracker != nil {
		trace_util_0.Count(_expensivequery_00000, 43)
		logFields = append(logFields, zap.String("mem_max", memTracker.BytesToString(memTracker.MaxConsumed())))
	}

	trace_util_0.Count(_expensivequery_00000, 27)
	const logSQLLen = 1024 * 8
	sql := info.Info
	if len(sql) > logSQLLen {
		trace_util_0.Count(_expensivequery_00000, 44)
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	trace_util_0.Count(_expensivequery_00000, 28)
	logFields = append(logFields, zap.String("sql", sql))

	logutil.Logger(context.Background()).Warn("expensive_query", logFields...)
}

var _expensivequery_00000 = "util/expensivequery/expensivequery.go"
