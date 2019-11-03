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

package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	stats "github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

func loadStats(tblInfo *model.TableInfo, path string) (*stats.Table, error) {
	trace_util_0.Count(_stats_00000, 0)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		trace_util_0.Count(_stats_00000, 3)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_stats_00000, 1)
	jsTable := &handle.JSONTable{}
	err = json.Unmarshal(data, jsTable)
	if err != nil {
		trace_util_0.Count(_stats_00000, 4)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_stats_00000, 2)
	return handle.TableStatsFromJSON(tblInfo, tblInfo.ID, jsTable)
}

type histogram struct {
	stats.Histogram

	index  *model.IndexInfo
	avgLen int
}

// When the randCnt falls in the middle of bucket, we return the idx of lower bound which is an even number.
// When the randCnt falls in the end of bucket, we return the upper bound which is odd.
func (h *histogram) getRandomBoundIdx() int {
	trace_util_0.Count(_stats_00000, 5)
	cnt := h.Buckets[len(h.Buckets)-1].Count
	randCnt := randInt64(0, cnt)
	for i, bkt := range h.Buckets {
		trace_util_0.Count(_stats_00000, 7)
		if bkt.Count >= randCnt {
			trace_util_0.Count(_stats_00000, 8)
			if bkt.Count-bkt.Repeat > randCnt {
				trace_util_0.Count(_stats_00000, 10)
				return 2 * i
			}
			trace_util_0.Count(_stats_00000, 9)
			return 2*i + 1
		}
	}
	trace_util_0.Count(_stats_00000, 6)
	return 0
}

func (h *histogram) decodeInt(row *chunk.Row) int64 {
	trace_util_0.Count(_stats_00000, 11)
	if h.index == nil {
		trace_util_0.Count(_stats_00000, 14)
		return row.GetInt64(0)
	}
	trace_util_0.Count(_stats_00000, 12)
	data := row.GetBytes(0)
	_, result, err := codec.DecodeInt(data)
	if err != nil {
		trace_util_0.Count(_stats_00000, 15)
		log.Fatal(err.Error())
	}
	trace_util_0.Count(_stats_00000, 13)
	return result
}

func (h *histogram) randInt() int64 {
	trace_util_0.Count(_stats_00000, 16)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 18)
		lower := h.Bounds.GetRow(idx).GetInt64(0)
		upper := h.Bounds.GetRow(idx + 1).GetInt64(0)
		return randInt64(lower, upper)
	}
	trace_util_0.Count(_stats_00000, 17)
	return h.Bounds.GetRow(idx).GetInt64(0)
}

func (h *histogram) randFloat64() float64 {
	trace_util_0.Count(_stats_00000, 19)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 21)
		lower := h.Bounds.GetRow(idx).GetFloat64(0)
		upper := h.Bounds.GetRow(idx + 1).GetFloat64(0)
		rd := rand.Float64()
		return lower + rd*(upper-lower)
	}
	trace_util_0.Count(_stats_00000, 20)
	return h.Bounds.GetRow(idx).GetFloat64(0)
}

func (h *histogram) randFloat32() float32 {
	trace_util_0.Count(_stats_00000, 22)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 24)
		lower := h.Bounds.GetRow(idx).GetFloat32(0)
		upper := h.Bounds.GetRow(idx + 1).GetFloat32(0)
		rd := rand.Float32()
		return lower + rd*(upper-lower)
	}
	trace_util_0.Count(_stats_00000, 23)
	return h.Bounds.GetRow(idx).GetFloat32(0)
}

func (h *histogram) randDecimal() *types.MyDecimal {
	trace_util_0.Count(_stats_00000, 25)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 27)
		lower := h.Bounds.GetRow(idx).GetMyDecimal(0)
		upper := h.Bounds.GetRow(idx + 1).GetMyDecimal(0)
		rd := rand.Float64()
		l, err := lower.ToFloat64()
		if err != nil {
			trace_util_0.Count(_stats_00000, 31)
			log.Fatal(err.Error())
		}
		trace_util_0.Count(_stats_00000, 28)
		r, err := upper.ToFloat64()
		if err != nil {
			trace_util_0.Count(_stats_00000, 32)
			log.Fatal(err.Error())
		}
		trace_util_0.Count(_stats_00000, 29)
		dec := &types.MyDecimal{}
		err = dec.FromFloat64(l + rd*(r-l))
		if err != nil {
			trace_util_0.Count(_stats_00000, 33)
			log.Fatal(err.Error())
		}
		trace_util_0.Count(_stats_00000, 30)
		return dec
	}
	trace_util_0.Count(_stats_00000, 26)
	return h.Bounds.GetRow(idx).GetMyDecimal(0)
}

func getValidPrefix(lower, upper string) string {
	trace_util_0.Count(_stats_00000, 34)
	for i := range lower {
		trace_util_0.Count(_stats_00000, 36)
		if i >= len(upper) {
			trace_util_0.Count(_stats_00000, 38)
			log.Fatal("lower is larger than upper", zap.String("lower", lower), zap.String("upper", upper))
		}
		trace_util_0.Count(_stats_00000, 37)
		if lower[i] != upper[i] {
			trace_util_0.Count(_stats_00000, 39)
			randCh := uint8(rand.Intn(int(upper[i]-lower[i]))) + lower[i]
			newBytes := make([]byte, i, i+1)
			copy(newBytes, lower[:i])
			newBytes = append(newBytes, byte(randCh))
			return string(newBytes)
		}
	}
	trace_util_0.Count(_stats_00000, 35)
	return lower
}

func (h *histogram) getAvgLen(maxLen int) int {
	trace_util_0.Count(_stats_00000, 40)
	l := h.Bounds.NumRows()
	totalLen := 0
	for i := 0; i < l; i++ {
		trace_util_0.Count(_stats_00000, 44)
		totalLen += len(h.Bounds.GetRow(i).GetString(0))
	}
	trace_util_0.Count(_stats_00000, 41)
	avg := totalLen / l
	if avg > maxLen {
		trace_util_0.Count(_stats_00000, 45)
		avg = maxLen
	}
	trace_util_0.Count(_stats_00000, 42)
	if avg == 0 {
		trace_util_0.Count(_stats_00000, 46)
		avg = 1
	}
	trace_util_0.Count(_stats_00000, 43)
	return avg
}

func (h *histogram) randString() string {
	trace_util_0.Count(_stats_00000, 47)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 49)
		lower := h.Bounds.GetRow(idx).GetString(0)
		upper := h.Bounds.GetRow(idx + 1).GetString(0)
		prefix := getValidPrefix(lower, upper)
		restLen := h.avgLen - len(prefix)
		if restLen > 0 {
			trace_util_0.Count(_stats_00000, 51)
			prefix = prefix + randString(restLen)
		}
		trace_util_0.Count(_stats_00000, 50)
		return prefix
	}
	trace_util_0.Count(_stats_00000, 48)
	return h.Bounds.GetRow(idx).GetString(0)
}

// randDate randoms a bucket and random a date between upper and lower bound.
func (h *histogram) randDate(unit string, mysqlFmt string, dateFmt string) string {
	trace_util_0.Count(_stats_00000, 52)
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		trace_util_0.Count(_stats_00000, 55)
		lower := h.Bounds.GetRow(idx).GetTime(0)
		upper := h.Bounds.GetRow(idx + 1).GetTime(0)
		diff := types.TimestampDiff(unit, lower, upper)
		if diff == 0 {
			trace_util_0.Count(_stats_00000, 58)
			str, err := lower.DateFormat(mysqlFmt)
			if err != nil {
				trace_util_0.Count(_stats_00000, 60)
				log.Fatal(err.Error())
			}
			trace_util_0.Count(_stats_00000, 59)
			return str
		}
		trace_util_0.Count(_stats_00000, 56)
		delta := randInt(0, int(diff)-1)
		l, err := lower.Time.GoTime(time.Local)
		if err != nil {
			trace_util_0.Count(_stats_00000, 61)
			log.Fatal(err.Error())
		}
		trace_util_0.Count(_stats_00000, 57)
		l = l.AddDate(0, 0, delta)
		return l.Format(dateFmt)
	}
	trace_util_0.Count(_stats_00000, 53)
	str, err := h.Bounds.GetRow(idx).GetTime(0).DateFormat(mysqlFmt)
	if err != nil {
		trace_util_0.Count(_stats_00000, 62)
		log.Fatal(err.Error())
	}
	trace_util_0.Count(_stats_00000, 54)
	return str
}

var _stats_00000 = "cmd/importer/stats.go"
