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
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/zap"
)

func addJobs(jobCount int, jobChan chan struct{}) {
	trace_util_0.Count(_job_00000, 0)
	for i := 0; i < jobCount; i++ {
		trace_util_0.Count(_job_00000, 2)
		jobChan <- struct{}{}
	}

	trace_util_0.Count(_job_00000, 1)
	close(jobChan)
}

func doInsert(table *table, db *sql.DB, count int) {
	trace_util_0.Count(_job_00000, 3)
	sqls, err := genRowDatas(table, count)
	if err != nil {
		trace_util_0.Count(_job_00000, 7)
		log.Fatal("generate data failed", zap.Error(err))
	}

	trace_util_0.Count(_job_00000, 4)
	txn, err := db.Begin()
	if err != nil {
		trace_util_0.Count(_job_00000, 8)
		log.Fatal("begin failed", zap.Error(err))
	}

	trace_util_0.Count(_job_00000, 5)
	for _, sql := range sqls {
		trace_util_0.Count(_job_00000, 9)
		_, err = txn.Exec(sql)
		if err != nil {
			trace_util_0.Count(_job_00000, 10)
			log.Fatal("exec failed", zap.Error(err))
		}
	}

	trace_util_0.Count(_job_00000, 6)
	err = txn.Commit()
	if err != nil {
		trace_util_0.Count(_job_00000, 11)
		log.Fatal("commit failed", zap.Error(err))
	}
}

func doJob(table *table, db *sql.DB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	trace_util_0.Count(_job_00000, 12)
	count := 0
	for range jobChan {
		trace_util_0.Count(_job_00000, 15)
		count++
		if count == batch {
			trace_util_0.Count(_job_00000, 16)
			doInsert(table, db, count)
			count = 0
		}
	}

	trace_util_0.Count(_job_00000, 13)
	if count > 0 {
		trace_util_0.Count(_job_00000, 17)
		doInsert(table, db, count)
	}

	trace_util_0.Count(_job_00000, 14)
	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	trace_util_0.Count(_job_00000, 18)
	for i := 0; i < workerCount; i++ {
		trace_util_0.Count(_job_00000, 21)
		<-doneChan
	}

	trace_util_0.Count(_job_00000, 19)
	close(doneChan)

	now := time.Now()
	seconds := now.Unix() - start.Unix()

	tps := int64(-1)
	if seconds > 0 {
		trace_util_0.Count(_job_00000, 22)
		tps = int64(jobCount) / seconds
	}

	trace_util_0.Count(_job_00000, 20)
	fmt.Printf("[importer]total %d cases, cost %d seconds, tps %d, start %s, now %s\n", jobCount, seconds, tps, start, now)
}

func doProcess(table *table, dbs []*sql.DB, jobCount int, workerCount int, batch int) {
	trace_util_0.Count(_job_00000, 23)
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	start := time.Now()
	go addJobs(jobCount, jobChan)

	for _, col := range table.columns {
		trace_util_0.Count(_job_00000, 26)
		if col.incremental {
			trace_util_0.Count(_job_00000, 27)
			workerCount = 1
			break
		}
	}
	trace_util_0.Count(_job_00000, 24)
	for i := 0; i < workerCount; i++ {
		trace_util_0.Count(_job_00000, 28)
		go doJob(table, dbs[i], batch, jobChan, doneChan)
	}

	trace_util_0.Count(_job_00000, 25)
	doWait(doneChan, start, jobCount, workerCount)
}

var _job_00000 = "cmd/importer/job.go"
