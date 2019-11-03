// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	addr      = flag.String("addr", "127.0.0.1:2379", "pd address")
	tableName = flag.String("table", "benchdb", "name of the table")
	batchSize = flag.Int("batch", 100, "number of statements in a transaction, used for insert and update-random only")
	blobSize  = flag.Int("blob", 1000, "size of the blob column in the row")
	logLevel  = flag.String("L", "warn", "log level")
	runJobs   = flag.String("run", strings.Join([]string{
		"create",
		"truncate",
		"insert:0_10000",
		"update-random:0_10000:100000",
		"select:0_10000:10",
		"update-range:5000_5100:1000",
		"select:0_10000:10",
		"gc",
		"select:0_10000:10",
	}, "|"), "jobs to run")
	sslCA   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	sslCert = flag.String("cert", "", "path of file that contains X509 certificate in PEM format.")
	sslKey  = flag.String("key", "", "path of file that contains X509 key in PEM format.")
)

func main() {
	trace_util_0.Count(_main_00000, 0)
	flag.Parse()
	flag.PrintDefaults()
	err := logutil.InitZapLogger(logutil.NewLogConfig(*logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	terror.MustNil(err)
	err = store.Register("tikv", tikv.Driver{})
	terror.MustNil(err)
	ut := newBenchDB()
	works := strings.Split(*runJobs, "|")
	for _, v := range works {
		trace_util_0.Count(_main_00000, 1)
		work := strings.ToLower(strings.TrimSpace(v))
		name, spec := ut.mustParseWork(work)
		switch name {
		case "create":
			trace_util_0.Count(_main_00000, 2)
			ut.createTable()
		case "truncate":
			trace_util_0.Count(_main_00000, 3)
			ut.truncateTable()
		case "insert":
			trace_util_0.Count(_main_00000, 4)
			ut.insertRows(spec)
		case "update-random", "update_random":
			trace_util_0.Count(_main_00000, 5)
			ut.updateRandomRows(spec)
		case "update-range", "update_range":
			trace_util_0.Count(_main_00000, 6)
			ut.updateRangeRows(spec)
		case "select":
			trace_util_0.Count(_main_00000, 7)
			ut.selectRows(spec)
		case "query":
			trace_util_0.Count(_main_00000, 8)
			ut.query(spec)
		default:
			trace_util_0.Count(_main_00000, 9)
			cLog("Unknown job ", v)
			return
		}
	}
}

type benchDB struct {
	store   tikv.Storage
	session session.Session
}

func newBenchDB() *benchDB {
	trace_util_0.Count(_main_00000, 10)
	// Create TiKV store and disable GC as we will trigger GC manually.
	store, err := store.New("tikv://" + *addr + "?disableGC=true")
	terror.MustNil(err)
	_, err = session.BootstrapSession(store)
	terror.MustNil(err)
	se, err := session.CreateSession(store)
	terror.MustNil(err)
	_, err = se.Execute(context.Background(), "use test")
	terror.MustNil(err)

	return &benchDB{
		store:   store.(tikv.Storage),
		session: se,
	}
}

func (ut *benchDB) mustExec(sql string) {
	trace_util_0.Count(_main_00000, 11)
	rss, err := ut.session.Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_main_00000, 13)
		log.Fatal(err.Error())
	}
	trace_util_0.Count(_main_00000, 12)
	if len(rss) > 0 {
		trace_util_0.Count(_main_00000, 14)
		ctx := context.Background()
		rs := rss[0]
		req := rs.NewRecordBatch()
		for {
			trace_util_0.Count(_main_00000, 15)
			err := rs.Next(ctx, req)
			if err != nil {
				trace_util_0.Count(_main_00000, 17)
				log.Fatal(err.Error())
			}
			trace_util_0.Count(_main_00000, 16)
			if req.NumRows() == 0 {
				trace_util_0.Count(_main_00000, 18)
				break
			}
		}
	}
}

func (ut *benchDB) mustParseWork(work string) (name string, spec string) {
	trace_util_0.Count(_main_00000, 19)
	strs := strings.Split(work, ":")
	if len(strs) == 1 {
		trace_util_0.Count(_main_00000, 21)
		return strs[0], ""
	}
	trace_util_0.Count(_main_00000, 20)
	return strs[0], strings.Join(strs[1:], ":")
}

func (ut *benchDB) mustParseInt(s string) int {
	trace_util_0.Count(_main_00000, 22)
	i, err := strconv.Atoi(s)
	if err != nil {
		trace_util_0.Count(_main_00000, 24)
		log.Fatal(err.Error())
	}
	trace_util_0.Count(_main_00000, 23)
	return i
}

func (ut *benchDB) mustParseRange(s string) (start, end int) {
	trace_util_0.Count(_main_00000, 25)
	strs := strings.Split(s, "_")
	if len(strs) != 2 {
		trace_util_0.Count(_main_00000, 28)
		log.Fatal("parse range failed", zap.String("invalid range", s))
	}
	trace_util_0.Count(_main_00000, 26)
	startStr, endStr := strs[0], strs[1]
	start = ut.mustParseInt(startStr)
	end = ut.mustParseInt(endStr)
	if start < 0 || end < start {
		trace_util_0.Count(_main_00000, 29)
		log.Fatal("parse range failed", zap.String("invalid range", s))
	}
	trace_util_0.Count(_main_00000, 27)
	return
}

func (ut *benchDB) mustParseSpec(s string) (start, end, count int) {
	trace_util_0.Count(_main_00000, 30)
	strs := strings.Split(s, ":")
	start, end = ut.mustParseRange(strs[0])
	if len(strs) == 1 {
		trace_util_0.Count(_main_00000, 32)
		count = 1
		return
	}
	trace_util_0.Count(_main_00000, 31)
	count = ut.mustParseInt(strs[1])
	return
}

func (ut *benchDB) createTable() {
	trace_util_0.Count(_main_00000, 33)
	cLog("create table")
	createSQL := "CREATE TABLE IF NOT EXISTS " + *tableName + ` (
  id bigint(20) NOT NULL,
  name varchar(32) NOT NULL,
  exp bigint(20) NOT NULL DEFAULT '0',
  data blob,
  PRIMARY KEY (id),
  UNIQUE KEY name (name)
)`
	ut.mustExec(createSQL)
}

func (ut *benchDB) truncateTable() {
	trace_util_0.Count(_main_00000, 34)
	cLog("truncate table")
	ut.mustExec("truncate table " + *tableName)
}

func (ut *benchDB) runCountTimes(name string, count int, f func()) {
	trace_util_0.Count(_main_00000, 35)
	var (
		sum, first, last time.Duration
		min              = time.Minute
		max              = time.Nanosecond
	)
	cLogf("%s started", name)
	for i := 0; i < count; i++ {
		trace_util_0.Count(_main_00000, 37)
		before := time.Now()
		f()
		dur := time.Since(before)
		if first == 0 {
			trace_util_0.Count(_main_00000, 41)
			first = dur
		}
		trace_util_0.Count(_main_00000, 38)
		last = dur
		if dur < min {
			trace_util_0.Count(_main_00000, 42)
			min = dur
		}
		trace_util_0.Count(_main_00000, 39)
		if dur > max {
			trace_util_0.Count(_main_00000, 43)
			max = dur
		}
		trace_util_0.Count(_main_00000, 40)
		sum += dur
	}
	trace_util_0.Count(_main_00000, 36)
	cLogf("%s done, avg %s, count %d, sum %s, first %s, last %s, max %s, min %s\n\n",
		name, sum/time.Duration(count), count, sum, first, last, max, min)
}

func (ut *benchDB) insertRows(spec string) {
	trace_util_0.Count(_main_00000, 44)
	start, end, _ := ut.mustParseSpec(spec)
	loopCount := (end - start + *batchSize - 1) / *batchSize
	id := start
	ut.runCountTimes("insert", loopCount, func() {
		trace_util_0.Count(_main_00000, 45)
		ut.mustExec("begin")
		buf := make([]byte, *blobSize/2)
		for i := 0; i < *batchSize; i++ {
			trace_util_0.Count(_main_00000, 47)
			if id == end {
				trace_util_0.Count(_main_00000, 49)
				break
			}
			trace_util_0.Count(_main_00000, 48)
			rand.Read(buf)
			insetQuery := fmt.Sprintf("insert %s (id, name, data) values (%d, '%d', '%x')",
				*tableName, id, id, buf)
			ut.mustExec(insetQuery)
			id++
		}
		trace_util_0.Count(_main_00000, 46)
		ut.mustExec("commit")
	})
}

func (ut *benchDB) updateRandomRows(spec string) {
	trace_util_0.Count(_main_00000, 50)
	start, end, totalCount := ut.mustParseSpec(spec)
	loopCount := (totalCount + *batchSize - 1) / *batchSize
	var runCount = 0
	ut.runCountTimes("update-random", loopCount, func() {
		trace_util_0.Count(_main_00000, 51)
		ut.mustExec("begin")
		for i := 0; i < *batchSize; i++ {
			trace_util_0.Count(_main_00000, 53)
			if runCount == totalCount {
				trace_util_0.Count(_main_00000, 55)
				break
			}
			trace_util_0.Count(_main_00000, 54)
			id := rand.Intn(end-start) + start
			updateQuery := fmt.Sprintf("update %s set exp = exp + 1 where id = %d", *tableName, id)
			ut.mustExec(updateQuery)
			runCount++
		}
		trace_util_0.Count(_main_00000, 52)
		ut.mustExec("commit")
	})
}

func (ut *benchDB) updateRangeRows(spec string) {
	trace_util_0.Count(_main_00000, 56)
	start, end, count := ut.mustParseSpec(spec)
	ut.runCountTimes("update-range", count, func() {
		trace_util_0.Count(_main_00000, 57)
		ut.mustExec("begin")
		updateQuery := fmt.Sprintf("update %s set exp = exp + 1 where id >= %d and id < %d", *tableName, start, end)
		ut.mustExec(updateQuery)
		ut.mustExec("commit")
	})
}

func (ut *benchDB) selectRows(spec string) {
	trace_util_0.Count(_main_00000, 58)
	start, end, count := ut.mustParseSpec(spec)
	ut.runCountTimes("select", count, func() {
		trace_util_0.Count(_main_00000, 59)
		selectQuery := fmt.Sprintf("select * from %s where id >= %d and id < %d", *tableName, start, end)
		ut.mustExec(selectQuery)
	})
}

func (ut *benchDB) query(spec string) {
	trace_util_0.Count(_main_00000, 60)
	strs := strings.Split(spec, ":")
	sql := strs[0]
	count, err := strconv.Atoi(strs[1])
	terror.MustNil(err)
	ut.runCountTimes("query", count, func() {
		trace_util_0.Count(_main_00000, 61)
		ut.mustExec(sql)
	})
}

func cLogf(format string, args ...interface{}) {
	trace_util_0.Count(_main_00000, 62)
	str := fmt.Sprintf(format, args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}

func cLog(args ...interface{}) {
	trace_util_0.Count(_main_00000, 63)
	str := fmt.Sprint(args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}

var _main_00000 = "cmd/benchdb/main.go"
