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

package server

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
)

type sqlInfoFetcher struct {
	store tikv.Storage
	do    *domain.Domain
	s     session.Session
}

type tableNamePair struct {
	DBName    string
	TableName string
}

type tableNameExtractor struct {
	curDB string
	names map[tableNamePair]struct{}
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_sql_info_fetcher_00000, 0)
	if _, ok := in.(*ast.TableName); ok {
		trace_util_0.Count(_sql_info_fetcher_00000, 2)
		return in, true
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 1)
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_sql_info_fetcher_00000, 3)
	if t, ok := in.(*ast.TableName); ok {
		trace_util_0.Count(_sql_info_fetcher_00000, 5)
		tp := tableNamePair{DBName: t.Schema.L, TableName: t.Name.L}
		if tp.DBName == "" {
			trace_util_0.Count(_sql_info_fetcher_00000, 7)
			tp.DBName = tne.curDB
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 6)
		if _, ok := tne.names[tp]; !ok {
			trace_util_0.Count(_sql_info_fetcher_00000, 8)
			tne.names[tp] = struct{}{}
		}
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 4)
	return in, true
}

func (sh *sqlInfoFetcher) zipInfoForSQL(w http.ResponseWriter, r *http.Request) {
	trace_util_0.Count(_sql_info_fetcher_00000, 9)
	var err error
	sh.s, err = session.CreateSession(sh.store)
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 20)
		serveError(w, http.StatusInternalServerError, fmt.Sprintf("create session failed, err: %v", err))
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 10)
	defer sh.s.Close()
	sh.do = domain.GetDomain(sh.s)
	reqCtx := r.Context()
	sql := r.FormValue("sql")
	pprofTimeString := r.FormValue("pprof_time")
	timeoutString := r.FormValue("timeout")
	curDB := strings.ToLower(r.FormValue("current_db"))
	if curDB != "" {
		trace_util_0.Count(_sql_info_fetcher_00000, 21)
		_, err = sh.s.Execute(reqCtx, "use %v"+curDB)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 22)
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("use database %v failed, err: %v", curDB, err))
			return
		}
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 11)
	var (
		pprofTime int
		timeout   int
	)
	if pprofTimeString != "" {
		trace_util_0.Count(_sql_info_fetcher_00000, 23)
		pprofTime, err = strconv.Atoi(pprofTimeString)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 24)
			serveError(w, http.StatusBadRequest, "invalid value for pprof_time, please input a int value larger than 5")
			return
		}
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 12)
	if pprofTimeString != "" && pprofTime < 5 {
		trace_util_0.Count(_sql_info_fetcher_00000, 25)
		serveError(w, http.StatusBadRequest, "pprof time is too short, please input a int value larger than 5")
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 13)
	if timeoutString != "" {
		trace_util_0.Count(_sql_info_fetcher_00000, 26)
		timeout, err = strconv.Atoi(timeoutString)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 27)
			serveError(w, http.StatusBadRequest, "invalid value for timeout")
			return
		}
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 14)
	if timeout < pprofTime {
		trace_util_0.Count(_sql_info_fetcher_00000, 28)
		timeout = pprofTime
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 15)
	pairs, err := sh.extractTableNames(sql, curDB)
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 29)
		serveError(w, http.StatusBadRequest, fmt.Sprintf("invalid SQL text, err: %v", err))
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 16)
	zw := zip.NewWriter(w)
	defer func() {
		trace_util_0.Count(_sql_info_fetcher_00000, 30)
		terror.Log(zw.Close())
	}()
	trace_util_0.Count(_sql_info_fetcher_00000, 17)
	for pair := range pairs {
		trace_util_0.Count(_sql_info_fetcher_00000, 31)
		jsonTbl, err := sh.getStatsForTable(pair)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 35)
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			terror.Log(err)
			continue
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 32)
		statsFw, err := zw.Create(fmt.Sprintf("%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 36)
			terror.Log(err)
			continue
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 33)
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 37)
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			terror.Log(err)
			continue
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 34)
		_, err = statsFw.Write(data)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 38)
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			terror.Log(err)
			continue
		}
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 18)
	for pair := range pairs {
		trace_util_0.Count(_sql_info_fetcher_00000, 39)
		err = sh.getShowCreateTable(pair, zw)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 40)
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.schema.err.txt", pair.DBName, pair.TableName), err)
			terror.Log(err)
			return
		}
	}
	// If we don't catch profile. We just get a explain result.
	trace_util_0.Count(_sql_info_fetcher_00000, 19)
	if pprofTime == 0 {
		trace_util_0.Count(_sql_info_fetcher_00000, 41)
		recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(reqCtx, fmt.Sprintf("explain %s", sql))
		if len(recordSets) > 0 {
			trace_util_0.Count(_sql_info_fetcher_00000, 46)
			defer terror.Call(recordSets[0].Close)
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 42)
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 47)
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			terror.Log(err)
			return
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 43)
		sRows, err := testkit.ResultSetToStringSlice(reqCtx, sh.s, recordSets[0])
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 48)
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			terror.Log(err)
			return
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 44)
		fw, err := zw.Create("explain.txt")
		if err != nil {
			trace_util_0.Count(_sql_info_fetcher_00000, 49)
			terror.Log(err)
			return
		}
		trace_util_0.Count(_sql_info_fetcher_00000, 45)
		for _, row := range sRows {
			trace_util_0.Count(_sql_info_fetcher_00000, 50)
			fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
		}
	} else {
		trace_util_0.Count(_sql_info_fetcher_00000, 51)
		{
			// Otherwise we catch a profile and run `EXPLAIN ANALYZE` result.
			ctx, cancelFunc := context.WithCancel(reqCtx)
			timer := time.NewTimer(time.Second * time.Duration(timeout))
			resultChan := make(chan *explainAnalyzeResult)
			go sh.getExplainAnalyze(ctx, sql, resultChan)
			errChan := make(chan error)
			go sh.catchCPUProfile(reqCtx, pprofTime, zw, errChan)
			select {
			case result := <-resultChan:
				trace_util_0.Count(_sql_info_fetcher_00000, 53)
				timer.Stop()
				cancelFunc()
				if result.err != nil {
					trace_util_0.Count(_sql_info_fetcher_00000, 58)
					err = sh.writeErrFile(zw, "explain_analyze.err.txt", result.err)
					terror.Log(err)
					return
				}
				trace_util_0.Count(_sql_info_fetcher_00000, 54)
				if len(result.rows) == 0 {
					trace_util_0.Count(_sql_info_fetcher_00000, 59)
					break
				}
				trace_util_0.Count(_sql_info_fetcher_00000, 55)
				fw, err := zw.Create("explain_analyze.txt")
				if err != nil {
					trace_util_0.Count(_sql_info_fetcher_00000, 60)
					terror.Log(err)
					break
				}
				trace_util_0.Count(_sql_info_fetcher_00000, 56)
				for _, row := range result.rows {
					trace_util_0.Count(_sql_info_fetcher_00000, 61)
					fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
				}
			case <-timer.C:
				trace_util_0.Count(_sql_info_fetcher_00000, 57)
				cancelFunc()
			}
			trace_util_0.Count(_sql_info_fetcher_00000, 52)
			err = <-errChan
			if err != nil {
				trace_util_0.Count(_sql_info_fetcher_00000, 62)
				err = sh.writeErrFile(zw, "profile.err.txt", err)
				terror.Log(err)
				return
			}
		}
	}
}

func (sh *sqlInfoFetcher) writeErrFile(zw *zip.Writer, name string, err error) error {
	trace_util_0.Count(_sql_info_fetcher_00000, 63)
	fw, err1 := zw.Create(name)
	if err1 != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 65)
		return err1
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 64)
	fmt.Fprintf(fw, "error: %v", err)
	return nil
}

type explainAnalyzeResult struct {
	rows [][]string
	err  error
}

func (sh *sqlInfoFetcher) getExplainAnalyze(ctx context.Context, sql string, resultChan chan<- *explainAnalyzeResult) {
	trace_util_0.Count(_sql_info_fetcher_00000, 66)
	recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(ctx, fmt.Sprintf("explain analyze %s", sql))
	if len(recordSets) > 0 {
		trace_util_0.Count(_sql_info_fetcher_00000, 70)
		defer terror.Call(recordSets[0].Close)
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 67)
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 71)
		resultChan <- &explainAnalyzeResult{err: err}
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 68)
	rows, err := testkit.ResultSetToStringSlice(ctx, sh.s, recordSets[0])
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 72)
		terror.Log(err)
		rows = nil
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 69)
	resultChan <- &explainAnalyzeResult{rows: rows}
}

func (sh *sqlInfoFetcher) catchCPUProfile(ctx context.Context, sec int, zw *zip.Writer, errChan chan<- error) {
	trace_util_0.Count(_sql_info_fetcher_00000, 73)
	// dump profile
	fw, err := zw.Create("profile")
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 76)
		errChan <- err
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 74)
	if err := pprof.StartCPUProfile(fw); err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 77)
		errChan <- err
		return
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 75)
	sleepWithCtx(ctx, time.Duration(sec)*time.Second)
	pprof.StopCPUProfile()
	errChan <- nil
}

func (sh *sqlInfoFetcher) getStatsForTable(pair tableNamePair) (*handle.JSONTable, error) {
	trace_util_0.Count(_sql_info_fetcher_00000, 78)
	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 80)
		return nil, err
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 79)
	js, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil)
	return js, err
}

func (sh *sqlInfoFetcher) getShowCreateTable(pair tableNamePair, zw *zip.Writer) error {
	trace_util_0.Count(_sql_info_fetcher_00000, 81)
	recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if len(recordSets) > 0 {
		trace_util_0.Count(_sql_info_fetcher_00000, 87)
		defer terror.Call(recordSets[0].Close)
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 82)
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 88)
		return err
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 83)
	sRows, err := testkit.ResultSetToStringSlice(context.Background(), sh.s, recordSets[0])
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 89)
		terror.Log(err)
		return nil
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 84)
	fw, err := zw.Create(fmt.Sprintf("%v.%v.schema.txt", pair.DBName, pair.TableName))
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 90)
		terror.Log(err)
		return nil
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 85)
	for _, row := range sRows {
		trace_util_0.Count(_sql_info_fetcher_00000, 91)
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 86)
	return nil
}

func (sh *sqlInfoFetcher) extractTableNames(sql, curDB string) (map[tableNamePair]struct{}, error) {
	trace_util_0.Count(_sql_info_fetcher_00000, 92)
	p := parser.New()
	charset, collation := sh.s.GetSessionVars().GetCharsetInfo()
	stmts, _, err := p.Parse(sql, charset, collation)
	if err != nil {
		trace_util_0.Count(_sql_info_fetcher_00000, 95)
		return nil, err
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 93)
	if len(stmts) > 1 {
		trace_util_0.Count(_sql_info_fetcher_00000, 96)
		return nil, errors.Errorf("Only 1 statement is allowed")
	}
	trace_util_0.Count(_sql_info_fetcher_00000, 94)
	extractor := &tableNameExtractor{
		curDB: curDB,
		names: make(map[tableNamePair]struct{}),
	}
	stmts[0].Accept(extractor)
	return extractor.names, nil
}

var _sql_info_fetcher_00000 = "server/sql_info_fetcher.go"
