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
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/zap"
)

func main() {
	trace_util_0.Count(_main_00000, 0)
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
		trace_util_0.Count(_main_00000, 8)
	case flag.ErrHelp:
		trace_util_0.Count(_main_00000, 9)
		os.Exit(0)
	default:
		trace_util_0.Count(_main_00000, 10)
		log.Error("parse cmd flags", zap.Error(err))
		os.Exit(2)
	}

	trace_util_0.Count(_main_00000, 1)
	table := newTable()
	err = parseTableSQL(table, cfg.DDLCfg.TableSQL)
	if err != nil {
		trace_util_0.Count(_main_00000, 11)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 2)
	err = parseIndexSQL(table, cfg.DDLCfg.IndexSQL)
	if err != nil {
		trace_util_0.Count(_main_00000, 12)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 3)
	dbs, err := createDBs(cfg.DBCfg, cfg.SysCfg.WorkerCount)
	if err != nil {
		trace_util_0.Count(_main_00000, 13)
		log.Fatal(err.Error())
	}
	trace_util_0.Count(_main_00000, 4)
	defer closeDBs(dbs)

	if len(cfg.StatsCfg.Path) > 0 {
		trace_util_0.Count(_main_00000, 14)
		statsInfo, err1 := loadStats(table.tblInfo, cfg.StatsCfg.Path)
		if err1 != nil {
			trace_util_0.Count(_main_00000, 17)
			log.Fatal(err1.Error())
		}
		trace_util_0.Count(_main_00000, 15)
		for _, idxInfo := range table.tblInfo.Indices {
			trace_util_0.Count(_main_00000, 18)
			offset := idxInfo.Columns[0].Offset
			if hist, ok := statsInfo.Indices[idxInfo.ID]; ok && len(hist.Buckets) > 0 {
				trace_util_0.Count(_main_00000, 19)
				table.columns[offset].hist = &histogram{
					Histogram: hist.Histogram,
					index:     hist.Info,
				}
			}
		}
		trace_util_0.Count(_main_00000, 16)
		for i, colInfo := range table.tblInfo.Columns {
			trace_util_0.Count(_main_00000, 20)
			if hist, ok := statsInfo.Columns[colInfo.ID]; ok && table.columns[i].hist == nil && len(hist.Buckets) > 0 {
				trace_util_0.Count(_main_00000, 21)
				table.columns[i].hist = &histogram{
					Histogram: hist.Histogram,
				}
			}
		}
	}

	trace_util_0.Count(_main_00000, 5)
	err = execSQL(dbs[0], cfg.DDLCfg.TableSQL)
	if err != nil {
		trace_util_0.Count(_main_00000, 22)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 6)
	err = execSQL(dbs[0], cfg.DDLCfg.IndexSQL)
	if err != nil {
		trace_util_0.Count(_main_00000, 23)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 7)
	doProcess(table, dbs, cfg.SysCfg.JobCount, cfg.SysCfg.WorkerCount, cfg.SysCfg.Batch)
}

var _main_00000 = "cmd/importer/main.go"
