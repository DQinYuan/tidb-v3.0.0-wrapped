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

package server

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tiancaiamao/appdash/traceapp"
	"go.uber.org/zap"
	static "sourcegraph.com/sourcegraph/appdash-data"
)

const defaultStatusPort = 10080

func (s *Server) startStatusHTTP() {
	trace_util_0.Count(_http_status_00000, 0)
	go s.startHTTPServer()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	trace_util_0.Count(_http_status_00000, 1)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	terror.Log(err)
}

func sleepWithCtx(ctx context.Context, d time.Duration) {
	trace_util_0.Count(_http_status_00000, 2)
	select {
	case <-time.After(d):
		trace_util_0.Count(_http_status_00000, 3)
	case <-ctx.Done():
		trace_util_0.Count(_http_status_00000, 4)
	}
}

func (s *Server) startHTTPServer() {
	trace_util_0.Count(_http_status_00000, 5)
	router := mux.NewRouter()

	router.HandleFunc("/status", s.handleStatus).Name("Status")
	// HTTP path for prometheus.
	router.Handle("/metrics", prometheus.Handler()).Name("Metrics")

	// HTTP path for dump statistics.
	router.Handle("/stats/dump/{db}/{table}", s.newStatsHandler()).Name("StatsDump")
	router.Handle("/stats/dump/{db}/{table}/{snapshot}", s.newStatsHistoryHandler()).Name("StatsHistoryDump")

	router.Handle("/settings", settingsHandler{}).Name("Settings")
	router.Handle("/reload-config", configReloadHandler{}).Name("ConfigReload")
	router.Handle("/binlog/recover", binlogRecover{}).Name("BinlogRecover")

	tikvHandlerTool := s.newTikvHandlerTool()
	router.Handle("/schema", schemaHandler{tikvHandlerTool}).Name("Schema")
	router.Handle("/schema/{db}", schemaHandler{tikvHandlerTool})
	router.Handle("/schema/{db}/{table}", schemaHandler{tikvHandlerTool})
	router.Handle("/tables/{colID}/{colTp}/{colFlag}/{colLen}", valueHandler{})
	router.Handle("/ddl/history", ddlHistoryJobHandler{tikvHandlerTool}).Name("DDL_History")
	router.Handle("/ddl/owner/resign", ddlResignOwnerHandler{tikvHandlerTool.Store.(kv.Storage)}).Name("DDL_Owner_Resign")

	// HTTP path for get server info.
	router.Handle("/info", serverInfoHandler{tikvHandlerTool}).Name("Info")
	router.Handle("/info/all", allServerInfoHandler{tikvHandlerTool}).Name("InfoALL")
	// HTTP path for get db and table info that is related to the tableID.
	router.Handle("/db-table/{tableID}", dbTableHandler{tikvHandlerTool})

	if s.cfg.Store == "tikv" {
		trace_util_0.Count(_http_status_00000, 14)
		// HTTP path for tikv.
		router.Handle("/tables/{db}/{table}/regions", tableHandler{tikvHandlerTool, opTableRegions})
		router.Handle("/tables/{db}/{table}/scatter", tableHandler{tikvHandlerTool, opTableScatter})
		router.Handle("/tables/{db}/{table}/stop-scatter", tableHandler{tikvHandlerTool, opStopTableScatter})
		router.Handle("/tables/{db}/{table}/disk-usage", tableHandler{tikvHandlerTool, opTableDiskUsage})
		router.Handle("/regions/meta", regionHandler{tikvHandlerTool}).Name("RegionsMeta")
		router.Handle("/regions/hot", regionHandler{tikvHandlerTool}).Name("RegionHot")
		router.Handle("/regions/{regionID}", regionHandler{tikvHandlerTool})
		router.Handle("/mvcc/key/{db}/{table}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByKey})
		router.Handle("/mvcc/txn/{startTS}/{db}/{table}", mvccTxnHandler{tikvHandlerTool, opMvccGetByTxn})
		router.Handle("/mvcc/hex/{hexKey}", mvccTxnHandler{tikvHandlerTool, opMvccGetByHex})
		router.Handle("/mvcc/index/{db}/{table}/{index}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByIdx})
	}
	trace_util_0.Count(_http_status_00000, 6)
	addr := fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		trace_util_0.Count(_http_status_00000, 15)
		addr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, defaultStatusPort)
	}

	// HTTP path for web UI.
	trace_util_0.Count(_http_status_00000, 7)
	if host, port, err := net.SplitHostPort(addr); err == nil {
		trace_util_0.Count(_http_status_00000, 16)
		if host == "" {
			trace_util_0.Count(_http_status_00000, 19)
			host = "localhost"
		}
		trace_util_0.Count(_http_status_00000, 17)
		baseURL := &url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s:%s", host, port),
		}
		router.HandleFunc("/web/trace", traceapp.HandleTiDB).Name("Trace Viewer")
		sr := router.PathPrefix("/web/trace/").Subrouter()
		if _, err := traceapp.New(traceapp.NewRouter(sr), baseURL); err != nil {
			trace_util_0.Count(_http_status_00000, 20)
			logutil.Logger(context.Background()).Error("new failed", zap.Error(err))
		}
		trace_util_0.Count(_http_status_00000, 18)
		router.PathPrefix("/static/").Handler(http.StripPrefix("/static", http.FileServer(static.Data)))
	}

	trace_util_0.Count(_http_status_00000, 8)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	serverMux.HandleFunc("/debug/zip", func(w http.ResponseWriter, r *http.Request) {
		trace_util_0.Count(_http_status_00000, 21)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="tidb_debug"`+time.Now().Format("20060102150405")+".zip"))

		// dump goroutine/heap/mutex
		items := []struct {
			name   string
			gc     int
			debug  int
			second int
		}{
			{name: "goroutine", debug: 2},
			{name: "heap", gc: 1},
			{name: "mutex"},
		}
		zw := zip.NewWriter(w)
		for _, item := range items {
			trace_util_0.Count(_http_status_00000, 29)
			p := rpprof.Lookup(item.name)
			if p == nil {
				trace_util_0.Count(_http_status_00000, 33)
				serveError(w, http.StatusNotFound, "Unknown profile")
				return
			}
			trace_util_0.Count(_http_status_00000, 30)
			if item.gc > 0 {
				trace_util_0.Count(_http_status_00000, 34)
				runtime.GC()
			}
			trace_util_0.Count(_http_status_00000, 31)
			fw, err := zw.Create(item.name)
			if err != nil {
				trace_util_0.Count(_http_status_00000, 35)
				serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", item.name, err))
				return
			}
			trace_util_0.Count(_http_status_00000, 32)
			err = p.WriteTo(fw, item.debug)
			terror.Log(err)
		}

		// dump profile
		trace_util_0.Count(_http_status_00000, 22)
		fw, err := zw.Create("profile")
		if err != nil {
			trace_util_0.Count(_http_status_00000, 36)
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "profile", err))
			return
		}
		trace_util_0.Count(_http_status_00000, 23)
		if err := rpprof.StartCPUProfile(fw); err != nil {
			trace_util_0.Count(_http_status_00000, 37)
			serveError(w, http.StatusInternalServerError,
				fmt.Sprintf("Could not enable CPU profiling: %s", err))
			return
		}
		trace_util_0.Count(_http_status_00000, 24)
		sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
		if sec <= 0 || err != nil {
			trace_util_0.Count(_http_status_00000, 38)
			sec = 10
		}
		trace_util_0.Count(_http_status_00000, 25)
		sleepWithCtx(r.Context(), time.Duration(sec)*time.Second)
		rpprof.StopCPUProfile()

		// dump config
		fw, err = zw.Create("config")
		if err != nil {
			trace_util_0.Count(_http_status_00000, 39)
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "config", err))
			return
		}
		trace_util_0.Count(_http_status_00000, 26)
		js, err := json.MarshalIndent(config.GetGlobalConfig(), "", " ")
		if err != nil {
			trace_util_0.Count(_http_status_00000, 40)
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("get config info fail%v", err))
			return
		}
		trace_util_0.Count(_http_status_00000, 27)
		_, err = fw.Write(js)
		terror.Log(err)

		// dump version
		fw, err = zw.Create("version")
		if err != nil {
			trace_util_0.Count(_http_status_00000, 41)
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "version", err))
			return
		}
		trace_util_0.Count(_http_status_00000, 28)
		_, err = fw.Write([]byte(printer.GetTiDBInfo()))
		terror.Log(err)

		err = zw.Close()
		terror.Log(err)
	})
	trace_util_0.Count(_http_status_00000, 9)
	fetcher := sqlInfoFetcher{store: tikvHandlerTool.Store}
	serverMux.HandleFunc("/debug/sub-optimal-plan", fetcher.zipInfoForSQL)

	var (
		httpRouterPage bytes.Buffer
		pathTemplate   string
		err            error
	)
	httpRouterPage.WriteString("<html><head><title>TiDB Status and Metrics Report</title></head><body><h1>TiDB Status and Metrics Report</h1><table>")
	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		trace_util_0.Count(_http_status_00000, 42)
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			trace_util_0.Count(_http_status_00000, 45)
			logutil.Logger(context.Background()).Error("get HTTP router path failed", zap.Error(err))
		}
		trace_util_0.Count(_http_status_00000, 43)
		name := route.GetName()
		// If the name attribute is not set, GetName returns "".
		// "traceapp.xxx" are introduced by the traceapp package and are also ignored.
		if name != "" && !strings.HasPrefix(name, "traceapp") && err == nil {
			trace_util_0.Count(_http_status_00000, 46)
			httpRouterPage.WriteString("<tr><td><a href='" + pathTemplate + "'>" + name + "</a><td></tr>")
		}
		trace_util_0.Count(_http_status_00000, 44)
		return nil
	})
	trace_util_0.Count(_http_status_00000, 10)
	if err != nil {
		trace_util_0.Count(_http_status_00000, 47)
		logutil.Logger(context.Background()).Error("generate root failed", zap.Error(err))
	}
	trace_util_0.Count(_http_status_00000, 11)
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		trace_util_0.Count(_http_status_00000, 48)
		_, err = responseWriter.Write([]byte(httpRouterPage.String()))
		if err != nil {
			trace_util_0.Count(_http_status_00000, 49)
			logutil.Logger(context.Background()).Error("write HTTP index page failed", zap.Error(err))
		}
	})

	trace_util_0.Count(_http_status_00000, 12)
	logutil.Logger(context.Background()).Info("for status and metrics report", zap.String("listening on addr", addr))
	s.statusServer = &http.Server{Addr: addr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}

	if len(s.cfg.Security.ClusterSSLCA) != 0 {
		trace_util_0.Count(_http_status_00000, 50)
		err = s.statusServer.ListenAndServeTLS(s.cfg.Security.ClusterSSLCert, s.cfg.Security.ClusterSSLKey)
	} else {
		trace_util_0.Count(_http_status_00000, 51)
		{
			err = s.statusServer.ListenAndServe()
		}
	}

	trace_util_0.Count(_http_status_00000, 13)
	if err != nil {
		trace_util_0.Count(_http_status_00000, 52)
		logutil.Logger(context.Background()).Info("listen failed", zap.Error(err))
	}
}

// status of TiDB.
type status struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_status_00000, 53)
	w.Header().Set("Content-Type", "application/json")

	st := status{
		Connections: s.ConnectionCount(),
		Version:     mysql.ServerVersion,
		GitHash:     printer.TiDBGitHash,
	}
	js, err := json.Marshal(st)
	if err != nil {
		trace_util_0.Count(_http_status_00000, 54)
		w.WriteHeader(http.StatusInternalServerError)
		logutil.Logger(context.Background()).Error("encode json failed", zap.Error(err))
	} else {
		trace_util_0.Count(_http_status_00000, 55)
		{
			_, err = w.Write(js)
			terror.Log(errors.Trace(err))
		}
	}
}

var _http_status_00000 = "server/http_status.go"
