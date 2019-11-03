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

package infoschema

import (
	"bufio"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var slowQueryCols = []columnInfo{
	{variable.SlowLogTimeStr, mysql.TypeTimestamp, 26, 0, nil, nil},
	{variable.SlowLogTxnStartTSStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogUserStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogHostStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogConnIDStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogQueryTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.ProcessTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.WaitTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.BackoffTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.RequestCountStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.TotalKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.ProcessKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogDBStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogIndexIDsStr, mysql.TypeVarchar, 100, 0, nil, nil},
	{variable.SlowLogIsInternalStr, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogDigestStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogStatsInfoStr, mysql.TypeVarchar, 512, 0, nil, nil},
	{variable.SlowLogCopProcAvg, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcP90, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcMax, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcAddr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogCopWaitAvg, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitP90, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitMax, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitAddr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogMemMax, mysql.TypeLonglong, 20, 0, nil, nil},
	{variable.SlowLogQuerySQLStr, mysql.TypeVarchar, 4096, 0, nil, nil},
}

func dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	trace_util_0.Count(_slow_log_00000, 0)
	return parseSlowLogFile(ctx.GetSessionVars().Location(), ctx.GetSessionVars().SlowQueryFile)
}

// parseSlowLogFile uses to parse slow log file.
// TODO: Support parse multiple log-files.
func parseSlowLogFile(tz *time.Location, filePath string) ([][]types.Datum, error) {
	trace_util_0.Count(_slow_log_00000, 1)
	file, err := os.Open(filePath)
	if err != nil {
		trace_util_0.Count(_slow_log_00000, 4)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_slow_log_00000, 2)
	defer func() {
		trace_util_0.Count(_slow_log_00000, 5)
		if err = file.Close(); err != nil {
			trace_util_0.Count(_slow_log_00000, 6)
			logutil.Logger(context.Background()).Error("close slow log file failed.", zap.String("file", filePath), zap.Error(err))
		}
	}()
	trace_util_0.Count(_slow_log_00000, 3)
	return ParseSlowLog(tz, bufio.NewReader(file))
}

// ParseSlowLog exports for testing.
// TODO: optimize for parse huge log-file.
func ParseSlowLog(tz *time.Location, reader *bufio.Reader) ([][]types.Datum, error) {
	trace_util_0.Count(_slow_log_00000, 7)
	var rows [][]types.Datum
	startFlag := false
	var st *slowQueryTuple
	for {
		trace_util_0.Count(_slow_log_00000, 8)
		lineByte, err := getOneLine(reader)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 11)
			if err == io.EOF {
				trace_util_0.Count(_slow_log_00000, 13)
				return rows, nil
			}
			trace_util_0.Count(_slow_log_00000, 12)
			return rows, err
		}
		trace_util_0.Count(_slow_log_00000, 9)
		line := string(hack.String(lineByte))
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			trace_util_0.Count(_slow_log_00000, 14)
			st = &slowQueryTuple{}
			err = st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):])
			if err != nil {
				trace_util_0.Count(_slow_log_00000, 16)
				return rows, err
			}
			trace_util_0.Count(_slow_log_00000, 15)
			startFlag = true
			continue
		}

		trace_util_0.Count(_slow_log_00000, 10)
		if startFlag {
			trace_util_0.Count(_slow_log_00000, 17)
			// Parse slow log field.
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				trace_util_0.Count(_slow_log_00000, 18)
				line = line[len(variable.SlowLogRowPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					trace_util_0.Count(_slow_log_00000, 19)
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						trace_util_0.Count(_slow_log_00000, 21)
						field = field[:len(field)-1]
					}
					trace_util_0.Count(_slow_log_00000, 20)
					err = st.setFieldValue(tz, field, fieldValues[i+1])
					if err != nil {
						trace_util_0.Count(_slow_log_00000, 22)
						return rows, err
					}
				}
			} else {
				trace_util_0.Count(_slow_log_00000, 23)
				if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
					trace_util_0.Count(_slow_log_00000, 24)
					// Get the sql string, and mark the start flag to false.
					err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)))
					if err != nil {
						trace_util_0.Count(_slow_log_00000, 26)
						return rows, err
					}
					trace_util_0.Count(_slow_log_00000, 25)
					rows = append(rows, st.convertToDatumRow())
					startFlag = false
				} else {
					trace_util_0.Count(_slow_log_00000, 27)
					{
						startFlag = false
					}
				}
			}
		}
	}
}

func getOneLine(reader *bufio.Reader) ([]byte, error) {
	trace_util_0.Count(_slow_log_00000, 28)
	lineByte, isPrefix, err := reader.ReadLine()
	if err != nil {
		trace_util_0.Count(_slow_log_00000, 31)
		return lineByte, err
	}
	trace_util_0.Count(_slow_log_00000, 29)
	var tempLine []byte
	for isPrefix {
		trace_util_0.Count(_slow_log_00000, 32)
		tempLine, isPrefix, err = reader.ReadLine()
		lineByte = append(lineByte, tempLine...)

		// Use the max value of max_allowed_packet to check the single line length.
		if len(lineByte) > int(variable.MaxOfMaxAllowedPacket) {
			trace_util_0.Count(_slow_log_00000, 34)
			return lineByte, errors.Errorf("single line length exceeds limit: %v", variable.MaxOfMaxAllowedPacket)
		}
		trace_util_0.Count(_slow_log_00000, 33)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 35)
			return lineByte, err
		}
	}
	trace_util_0.Count(_slow_log_00000, 30)
	return lineByte, err
}

type slowQueryTuple struct {
	time              time.Time
	txnStartTs        uint64
	user              string
	host              string
	connID            uint64
	queryTime         float64
	processTime       float64
	waitTime          float64
	backOffTime       float64
	requestCount      uint64
	totalKeys         uint64
	processKeys       uint64
	db                string
	indexIDs          string
	isInternal        bool
	digest            string
	statsInfo         string
	avgProcessTime    float64
	p90ProcessTime    float64
	maxProcessTime    float64
	maxProcessAddress string
	avgWaitTime       float64
	p90WaitTime       float64
	maxWaitTime       float64
	maxWaitAddress    string
	memMax            int64
	sql               string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string) error {
	trace_util_0.Count(_slow_log_00000, 36)
	value = stringutil.Copy(value)
	switch field {
	case variable.SlowLogTimeStr:
		trace_util_0.Count(_slow_log_00000, 38)
		t, err := ParseTime(value)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 83)
			return err
		}
		trace_util_0.Count(_slow_log_00000, 39)
		if t.Location() != tz {
			trace_util_0.Count(_slow_log_00000, 84)
			t = t.In(tz)
		}
		trace_util_0.Count(_slow_log_00000, 40)
		st.time = t
	case variable.SlowLogTxnStartTSStr:
		trace_util_0.Count(_slow_log_00000, 41)
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 85)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 42)
		st.txnStartTs = num
	case variable.SlowLogUserStr:
		trace_util_0.Count(_slow_log_00000, 43)
		fields := strings.SplitN(value, "@", 2)
		if len(field) > 0 {
			trace_util_0.Count(_slow_log_00000, 86)
			st.user = fields[0]
		}
		trace_util_0.Count(_slow_log_00000, 44)
		if len(field) > 1 {
			trace_util_0.Count(_slow_log_00000, 87)
			st.host = fields[1]
		}
	case variable.SlowLogConnIDStr:
		trace_util_0.Count(_slow_log_00000, 45)
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 88)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 46)
		st.connID = num
	case variable.SlowLogQueryTimeStr:
		trace_util_0.Count(_slow_log_00000, 47)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 89)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 48)
		st.queryTime = num
	case execdetails.ProcessTimeStr:
		trace_util_0.Count(_slow_log_00000, 49)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 90)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 50)
		st.processTime = num
	case execdetails.WaitTimeStr:
		trace_util_0.Count(_slow_log_00000, 51)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 91)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 52)
		st.waitTime = num
	case execdetails.BackoffTimeStr:
		trace_util_0.Count(_slow_log_00000, 53)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 92)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 54)
		st.backOffTime = num
	case execdetails.RequestCountStr:
		trace_util_0.Count(_slow_log_00000, 55)
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 93)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 56)
		st.requestCount = num
	case execdetails.TotalKeysStr:
		trace_util_0.Count(_slow_log_00000, 57)
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 94)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 58)
		st.totalKeys = num
	case execdetails.ProcessKeysStr:
		trace_util_0.Count(_slow_log_00000, 59)
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 95)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 60)
		st.processKeys = num
	case variable.SlowLogDBStr:
		trace_util_0.Count(_slow_log_00000, 61)
		st.db = value
	case variable.SlowLogIndexIDsStr:
		trace_util_0.Count(_slow_log_00000, 62)
		st.indexIDs = value
	case variable.SlowLogIsInternalStr:
		trace_util_0.Count(_slow_log_00000, 63)
		st.isInternal = value == "true"
	case variable.SlowLogDigestStr:
		trace_util_0.Count(_slow_log_00000, 64)
		st.digest = value
	case variable.SlowLogStatsInfoStr:
		trace_util_0.Count(_slow_log_00000, 65)
		st.statsInfo = value
	case variable.SlowLogCopProcAvg:
		trace_util_0.Count(_slow_log_00000, 66)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 96)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 67)
		st.avgProcessTime = num
	case variable.SlowLogCopProcP90:
		trace_util_0.Count(_slow_log_00000, 68)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 97)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 69)
		st.p90ProcessTime = num
	case variable.SlowLogCopProcMax:
		trace_util_0.Count(_slow_log_00000, 70)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 98)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 71)
		st.maxProcessTime = num
	case variable.SlowLogCopProcAddr:
		trace_util_0.Count(_slow_log_00000, 72)
		st.maxProcessAddress = value
	case variable.SlowLogCopWaitAvg:
		trace_util_0.Count(_slow_log_00000, 73)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 99)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 74)
		st.avgWaitTime = num
	case variable.SlowLogCopWaitP90:
		trace_util_0.Count(_slow_log_00000, 75)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 100)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 76)
		st.p90WaitTime = num
	case variable.SlowLogCopWaitMax:
		trace_util_0.Count(_slow_log_00000, 77)
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 101)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 78)
		st.maxWaitTime = num
	case variable.SlowLogCopWaitAddr:
		trace_util_0.Count(_slow_log_00000, 79)
		st.maxWaitAddress = value
	case variable.SlowLogMemMax:
		trace_util_0.Count(_slow_log_00000, 80)
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 102)
			return errors.AddStack(err)
		}
		trace_util_0.Count(_slow_log_00000, 81)
		st.memMax = num
	case variable.SlowLogQuerySQLStr:
		trace_util_0.Count(_slow_log_00000, 82)
		st.sql = value
	}
	trace_util_0.Count(_slow_log_00000, 37)
	return nil
}

func (st *slowQueryTuple) convertToDatumRow() []types.Datum {
	trace_util_0.Count(_slow_log_00000, 103)
	record := make([]types.Datum, 0, len(slowQueryCols))
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(st.time),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))
	record = append(record, types.NewUintDatum(st.txnStartTs))
	record = append(record, types.NewStringDatum(st.user))
	record = append(record, types.NewStringDatum(st.host))
	record = append(record, types.NewUintDatum(st.connID))
	record = append(record, types.NewFloat64Datum(st.queryTime))
	record = append(record, types.NewFloat64Datum(st.processTime))
	record = append(record, types.NewFloat64Datum(st.waitTime))
	record = append(record, types.NewFloat64Datum(st.backOffTime))
	record = append(record, types.NewUintDatum(st.requestCount))
	record = append(record, types.NewUintDatum(st.totalKeys))
	record = append(record, types.NewUintDatum(st.processKeys))
	record = append(record, types.NewStringDatum(st.db))
	record = append(record, types.NewStringDatum(st.indexIDs))
	record = append(record, types.NewDatum(st.isInternal))
	record = append(record, types.NewStringDatum(st.digest))
	record = append(record, types.NewStringDatum(st.statsInfo))
	record = append(record, types.NewFloat64Datum(st.avgProcessTime))
	record = append(record, types.NewFloat64Datum(st.p90ProcessTime))
	record = append(record, types.NewFloat64Datum(st.maxProcessTime))
	record = append(record, types.NewStringDatum(st.maxProcessAddress))
	record = append(record, types.NewFloat64Datum(st.avgWaitTime))
	record = append(record, types.NewFloat64Datum(st.p90WaitTime))
	record = append(record, types.NewFloat64Datum(st.maxWaitTime))
	record = append(record, types.NewStringDatum(st.maxWaitAddress))
	record = append(record, types.NewIntDatum(st.memMax))
	record = append(record, types.NewStringDatum(st.sql))
	return record
}

// ParseTime exports for testing.
func ParseTime(s string) (time.Time, error) {
	trace_util_0.Count(_slow_log_00000, 104)
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		trace_util_0.Count(_slow_log_00000, 106)
		// This is for compatibility.
		t, err = time.Parse(logutil.OldSlowLogTimeFormat, s)
		if err != nil {
			trace_util_0.Count(_slow_log_00000, 107)
			err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
		}
	}
	trace_util_0.Count(_slow_log_00000, 105)
	return t, err
}

var _slow_log_00000 = "infoschema/slow_log.go"
