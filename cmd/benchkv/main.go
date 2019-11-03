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
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	store     kv.Storage
	dataCnt   = flag.Int("N", 1000000, "data num")
	workerCnt = flag.Int("C", 400, "concurrent num")
	pdAddr    = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
	valueSize = flag.Int("V", 5, "value size in byte")
	sslCA     = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	sslCert   = flag.String("cert", "", "path of file that contains X509 certificate in PEM format.")
	sslKey    = flag.String("key", "", "path of file that contains X509 key in PEM format.")

	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "total",
			Help:      "Counter of txns.",
		}, []string{"type"})

	txnRolledbackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "failed_total",
			Help:      "Counter of rolled back txns.",
		}, []string{"type"})

	txnDurations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "durations_histogram_seconds",
			Help:      "Txn latency distributions.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})
)

// Init initializes information.
func Init() {
	trace_util_0.Count(_main_00000, 0)
	driver := tikv.Driver{}
	var err error
	store, err = driver.Open(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	terror.MustNil(err)

	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(txnRolledbackCounter)
	prometheus.MustRegister(txnDurations)
	http.Handle("/metrics", prometheus.Handler())

	go func() {
		trace_util_0.Count(_main_00000, 1)
		err1 := http.ListenAndServe(":9191", nil)
		terror.Log(errors.Trace(err1))
	}()
}

// batchRW makes sure conflict free.
func batchRW(value []byte) {
	trace_util_0.Count(_main_00000, 2)
	wg := sync.WaitGroup{}
	base := *dataCnt / *workerCnt
	wg.Add(*workerCnt)
	for i := 0; i < *workerCnt; i++ {
		trace_util_0.Count(_main_00000, 4)
		go func(i int) {
			trace_util_0.Count(_main_00000, 5)
			defer wg.Done()
			for j := 0; j < base; j++ {
				trace_util_0.Count(_main_00000, 6)
				txnCounter.WithLabelValues("txn").Inc()
				start := time.Now()
				k := base*i + j
				txn, err := store.Begin()
				if err != nil {
					trace_util_0.Count(_main_00000, 9)
					log.Fatal(err.Error())
				}
				trace_util_0.Count(_main_00000, 7)
				key := fmt.Sprintf("key_%d", k)
				err = txn.Set([]byte(key), value)
				terror.Log(errors.Trace(err))
				err = txn.Commit(context.Background())
				if err != nil {
					trace_util_0.Count(_main_00000, 10)
					txnRolledbackCounter.WithLabelValues("txn").Inc()
					terror.Call(txn.Rollback)
				}

				trace_util_0.Count(_main_00000, 8)
				txnDurations.WithLabelValues("txn").Observe(time.Since(start).Seconds())
			}
		}(i)
	}
	trace_util_0.Count(_main_00000, 3)
	wg.Wait()
}

func main() {
	trace_util_0.Count(_main_00000, 11)
	flag.Parse()
	log.SetLevel(zap.ErrorLevel)
	Init()

	value := make([]byte, *valueSize)
	t := time.Now()
	batchRW(value)
	resp, err := http.Get("http://localhost:9191/metrics")
	terror.MustNil(err)

	defer terror.Call(resp.Body.Close)
	text, err1 := ioutil.ReadAll(resp.Body)
	terror.Log(errors.Trace(err1))

	fmt.Println(string(text))

	fmt.Printf("\nelapse:%v, total %v\n", time.Since(t), *dataCnt)
}

var _main_00000 = "cmd/benchkv/main.go"
