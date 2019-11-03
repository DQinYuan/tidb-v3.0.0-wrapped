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
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/zap"
)

var (
	dataCnt   = flag.Int("N", 1000000, "data num")
	workerCnt = flag.Int("C", 100, "concurrent num")
	pdAddr    = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
	valueSize = flag.Int("V", 5, "value size in byte")
	sslCA     = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	sslCert   = flag.String("cert", "", "path of file that contains X509 certificate in PEM format.")
	sslKey    = flag.String("key", "", "path of file that contains X509 key in PEM format.")
)

// batchRawPut blinds put bench.
func batchRawPut(value []byte) {
	trace_util_0.Count(_main_00000, 0)
	cli, err := tikv.NewRawKVClient(strings.Split(*pdAddr, ","), config.Security{
		ClusterSSLCA:   *sslCA,
		ClusterSSLCert: *sslCert,
		ClusterSSLKey:  *sslKey,
	})
	if err != nil {
		trace_util_0.Count(_main_00000, 3)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 1)
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
				k := base*i + j
				key := fmt.Sprintf("key_%d", k)
				err = cli.Put([]byte(key), value)
				if err != nil {
					trace_util_0.Count(_main_00000, 7)
					log.Fatal("put failed", zap.Error(err))
				}
			}
		}(i)
	}
	trace_util_0.Count(_main_00000, 2)
	wg.Wait()
}

func main() {
	trace_util_0.Count(_main_00000, 8)
	flag.Parse()
	log.SetLevel(zap.WarnLevel)
	go func() {
		trace_util_0.Count(_main_00000, 10)
		err := http.ListenAndServe(":9191", nil)
		terror.Log(errors.Trace(err))
	}()

	trace_util_0.Count(_main_00000, 9)
	value := make([]byte, *valueSize)
	t := time.Now()
	batchRawPut(value)

	fmt.Printf("\nelapse:%v, total %v\n", time.Since(t), *dataCnt)
}

var _main_00000 = "cmd/benchraw/main.go"
