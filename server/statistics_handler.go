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

package server

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// StatsHandler is the handler for dumping statistics.
type StatsHandler struct {
	do *domain.Domain
}

func (s *Server) newStatsHandler() *StatsHandler {
	trace_util_0.Count(_statistics_handler_00000, 0)
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		trace_util_0.Count(_statistics_handler_00000, 3)
		panic("Illegal driver")
	}

	trace_util_0.Count(_statistics_handler_00000, 1)
	do, err := session.GetDomain(store.store)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 4)
		panic("Failed to get domain")
	}
	trace_util_0.Count(_statistics_handler_00000, 2)
	return &StatsHandler{do}
}

func (sh StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_statistics_handler_00000, 5)
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)

	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(params[pDBName]), model.NewCIStr(params[pTableName]))
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 6)
		writeError(w, err)
	} else {
		trace_util_0.Count(_statistics_handler_00000, 7)
		{
			js, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta(), nil)
			if err != nil {
				trace_util_0.Count(_statistics_handler_00000, 8)
				writeError(w, err)
			} else {
				trace_util_0.Count(_statistics_handler_00000, 9)
				{
					writeData(w, js)
				}
			}
		}
	}
}

// StatsHistoryHandler is the handler for dumping statistics.
type StatsHistoryHandler struct {
	do *domain.Domain
}

func (s *Server) newStatsHistoryHandler() *StatsHistoryHandler {
	trace_util_0.Count(_statistics_handler_00000, 10)
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		trace_util_0.Count(_statistics_handler_00000, 13)
		panic("Illegal driver")
	}

	trace_util_0.Count(_statistics_handler_00000, 11)
	do, err := session.GetDomain(store.store)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 14)
		panic("Failed to get domain")
	}
	trace_util_0.Count(_statistics_handler_00000, 12)
	return &StatsHistoryHandler{do}
}

func (sh StatsHistoryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_statistics_handler_00000, 15)
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)
	se, err := session.CreateSession(sh.do.Store())
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 22)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_statistics_handler_00000, 16)
	se.GetSessionVars().StmtCtx.TimeZone = time.Local
	t, err := types.ParseTime(se.GetSessionVars().StmtCtx, params[pSnapshot], mysql.TypeTimestamp, 6)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 23)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_statistics_handler_00000, 17)
	t1, err := t.Time.GoTime(time.Local)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 24)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_statistics_handler_00000, 18)
	snapshot := variable.GoTimeToTS(t1)
	err = gcutil.ValidateSnapshot(se, snapshot)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 25)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_statistics_handler_00000, 19)
	is, err := sh.do.GetSnapshotInfoSchema(snapshot)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 26)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_statistics_handler_00000, 20)
	h := sh.do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(params[pDBName]), model.NewCIStr(params[pTableName]))
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 27)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_statistics_handler_00000, 21)
	se.GetSessionVars().SnapshotInfoschema, se.GetSessionVars().SnapshotTS = is, snapshot
	historyStatsExec := se.(sqlexec.RestrictedSQLExecutor)
	js, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta(), historyStatsExec)
	if err != nil {
		trace_util_0.Count(_statistics_handler_00000, 28)
		writeError(w, err)
	} else {
		trace_util_0.Count(_statistics_handler_00000, 29)
		{
			writeData(w, js)
		}
	}
}

var _statistics_handler_00000 = "server/statistics_handler.go"
