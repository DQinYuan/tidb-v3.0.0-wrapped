// Copyright 2015 PingCAP, Inc.
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

package printer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Version information.
var (
	TiDBBuildTS   = "None"
	TiDBGitHash   = "None"
	TiDBGitBranch = "None"
	GoVersion     = "None"
	// TiKVMinVersion is the minimum version of TiKV that can be compatible with the current TiDB.
	TiKVMinVersion = "2.1.0-alpha.1-ff3dd160846b7d1aed9079c389fc188f7f5ea13e"
)

// PrintTiDBInfo prints the TiDB version information.
func PrintTiDBInfo() {
	trace_util_0.Count(_printer_00000, 0)
	logutil.Logger(context.Background()).Info("Welcome to TiDB.",
		zap.String("Release Version", mysql.TiDBReleaseVersion),
		zap.String("Git Commit Hash", TiDBGitHash),
		zap.String("Git Branch", TiDBGitBranch),
		zap.String("UTC Build Time", TiDBBuildTS),
		zap.String("GoVersion", GoVersion),
		zap.Bool("Race Enabled", israce.RaceEnabled),
		zap.Bool("Check Table Before Drop", config.CheckTableBeforeDrop),
		zap.String("TiKV Min Version", TiKVMinVersion))
	configJSON, err := json.Marshal(config.GetGlobalConfig())
	if err != nil {
		trace_util_0.Count(_printer_00000, 2)
		panic(err)
	}
	trace_util_0.Count(_printer_00000, 1)
	logutil.Logger(context.Background()).Info("loaded config", zap.ByteString("config", configJSON))
}

// GetTiDBInfo returns the git hash and build time of this tidb-server binary.
func GetTiDBInfo() string {
	trace_util_0.Count(_printer_00000, 3)
	return fmt.Sprintf("Release Version: %s\n"+
		"Git Commit Hash: %s\n"+
		"Git Branch: %s\n"+
		"UTC Build Time: %s\n"+
		"GoVersion: %s\n"+
		"Race Enabled: %v\n"+
		"TiKV Min Version: %s\n"+
		"Check Table Before Drop: %v",
		mysql.TiDBReleaseVersion,
		TiDBGitHash,
		TiDBGitBranch,
		TiDBBuildTS,
		GoVersion,
		israce.RaceEnabled,
		TiKVMinVersion,
		config.CheckTableBeforeDrop)
}

// checkValidity checks whether cols and every data have the same length.
func checkValidity(cols []string, datas [][]string) bool {
	trace_util_0.Count(_printer_00000, 4)
	colLen := len(cols)
	if len(datas) == 0 || colLen == 0 {
		trace_util_0.Count(_printer_00000, 7)
		return false
	}

	trace_util_0.Count(_printer_00000, 5)
	for _, data := range datas {
		trace_util_0.Count(_printer_00000, 8)
		if colLen != len(data) {
			trace_util_0.Count(_printer_00000, 9)
			return false
		}
	}

	trace_util_0.Count(_printer_00000, 6)
	return true
}

func getMaxColLen(cols []string, datas [][]string) []int {
	trace_util_0.Count(_printer_00000, 10)
	maxColLen := make([]int, len(cols))
	for i, col := range cols {
		trace_util_0.Count(_printer_00000, 13)
		maxColLen[i] = len(col)
	}

	trace_util_0.Count(_printer_00000, 11)
	for _, data := range datas {
		trace_util_0.Count(_printer_00000, 14)
		for i, v := range data {
			trace_util_0.Count(_printer_00000, 15)
			if len(v) > maxColLen[i] {
				trace_util_0.Count(_printer_00000, 16)
				maxColLen[i] = len(v)
			}
		}
	}

	trace_util_0.Count(_printer_00000, 12)
	return maxColLen
}

func getPrintDivLine(maxColLen []int) []byte {
	trace_util_0.Count(_printer_00000, 17)
	var value = make([]byte, 0)
	for _, v := range maxColLen {
		trace_util_0.Count(_printer_00000, 19)
		value = append(value, '+')
		value = append(value, bytes.Repeat([]byte{'-'}, v+2)...)
	}
	trace_util_0.Count(_printer_00000, 18)
	value = append(value, '+')
	value = append(value, '\n')
	return value
}

func getPrintCol(cols []string, maxColLen []int) []byte {
	trace_util_0.Count(_printer_00000, 20)
	var value = make([]byte, 0)
	for i, v := range cols {
		trace_util_0.Count(_printer_00000, 22)
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxColLen[i]+1-len(v))...)
	}
	trace_util_0.Count(_printer_00000, 21)
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintRow(data []string, maxColLen []int) []byte {
	trace_util_0.Count(_printer_00000, 23)
	var value = make([]byte, 0)
	for i, v := range data {
		trace_util_0.Count(_printer_00000, 25)
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxColLen[i]+1-len(v))...)
	}
	trace_util_0.Count(_printer_00000, 24)
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintRows(datas [][]string, maxColLen []int) []byte {
	trace_util_0.Count(_printer_00000, 26)
	var value = make([]byte, 0)
	for _, data := range datas {
		trace_util_0.Count(_printer_00000, 28)
		value = append(value, getPrintRow(data, maxColLen)...)
	}
	trace_util_0.Count(_printer_00000, 27)
	return value
}

// GetPrintResult gets a result with a formatted string.
func GetPrintResult(cols []string, datas [][]string) (string, bool) {
	trace_util_0.Count(_printer_00000, 29)
	if !checkValidity(cols, datas) {
		trace_util_0.Count(_printer_00000, 31)
		return "", false
	}

	trace_util_0.Count(_printer_00000, 30)
	var value = make([]byte, 0)
	maxColLen := getMaxColLen(cols, datas)

	value = append(value, getPrintDivLine(maxColLen)...)
	value = append(value, getPrintCol(cols, maxColLen)...)
	value = append(value, getPrintDivLine(maxColLen)...)
	value = append(value, getPrintRows(datas, maxColLen)...)
	value = append(value, getPrintDivLine(maxColLen)...)
	return string(value), true
}

var _printer_00000 = "util/printer/printer.go"
