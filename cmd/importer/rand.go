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
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/zap"
)

const (
	alphabet       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	yearFormat     = "2006"
	dateFormat     = "2006-01-02"
	timeFormat     = "15:04:05"
	dateTimeFormat = "2006-01-02 15:04:05"

	// Used by randString
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func init() {
	trace_util_0.Count(_rand_00000, 0)
	rand.Seed(time.Now().UnixNano())
}

func randInt(min int, max int) int {
	trace_util_0.Count(_rand_00000, 1)
	return min + rand.Intn(max-min+1)
}

func randInt64(min int64, max int64) int64 {
	trace_util_0.Count(_rand_00000, 2)
	return min + rand.Int63n(max-min+1)
}

// reference: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randString(n int) string {
	trace_util_0.Count(_rand_00000, 3)
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		trace_util_0.Count(_rand_00000, 5)
		if remain == 0 {
			trace_util_0.Count(_rand_00000, 8)
			cache, remain = rand.Int63(), letterIdxMax
		}
		trace_util_0.Count(_rand_00000, 6)
		if idx := int(cache & letterIdxMask); idx < len(alphabet) {
			trace_util_0.Count(_rand_00000, 9)
			b[i] = alphabet[idx]
			i--
		}
		trace_util_0.Count(_rand_00000, 7)
		cache >>= letterIdxBits
		remain--
	}

	trace_util_0.Count(_rand_00000, 4)
	return string(b)
}

func randDate(col *column) string {
	trace_util_0.Count(_rand_00000, 10)
	if col.hist != nil {
		trace_util_0.Count(_rand_00000, 16)
		return col.hist.randDate("DAY", "%Y-%m-%d", dateFormat)
	}

	trace_util_0.Count(_rand_00000, 11)
	min, max := col.min, col.max
	if len(min) == 0 {
		trace_util_0.Count(_rand_00000, 17)
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}

	trace_util_0.Count(_rand_00000, 12)
	minTime, err := time.Parse(dateFormat, min)
	if err != nil {
		trace_util_0.Count(_rand_00000, 18)
		log.Warn("parse min date failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 13)
	if len(max) == 0 {
		trace_util_0.Count(_rand_00000, 19)
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour)
		return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	}

	trace_util_0.Count(_rand_00000, 14)
	maxTime, err := time.Parse(dateFormat, max)
	if err != nil {
		trace_util_0.Count(_rand_00000, 20)
		log.Warn("parse max date failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 15)
	days := int(maxTime.Sub(minTime).Hours() / 24)
	t := minTime.Add(time.Duration(randInt(0, days)) * 24 * time.Hour)
	return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
}

func randTime(col *column) string {
	trace_util_0.Count(_rand_00000, 21)
	if col.hist != nil {
		trace_util_0.Count(_rand_00000, 26)
		return col.hist.randDate("SECOND", "%H:%i:%s", timeFormat)
	}
	trace_util_0.Count(_rand_00000, 22)
	min, max := col.min, col.max
	if len(min) == 0 || len(max) == 0 {
		trace_util_0.Count(_rand_00000, 27)
		hour := randInt(0, 23)
		min := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%02d:%02d:%02d", hour, min, sec)
	}

	trace_util_0.Count(_rand_00000, 23)
	minTime, err := time.Parse(timeFormat, min)
	if err != nil {
		trace_util_0.Count(_rand_00000, 28)
		log.Warn("parse min time failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 24)
	maxTime, err := time.Parse(timeFormat, max)
	if err != nil {
		trace_util_0.Count(_rand_00000, 29)
		log.Warn("parse max time failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 25)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

func randTimestamp(col *column) string {
	trace_util_0.Count(_rand_00000, 30)
	if col.hist != nil {
		trace_util_0.Count(_rand_00000, 36)
		return col.hist.randDate("SECOND", "%Y-%m-%d %H:%i:%s", dateTimeFormat)
	}
	trace_util_0.Count(_rand_00000, 31)
	min, max := col.min, col.max
	if len(min) == 0 {
		trace_util_0.Count(_rand_00000, 37)
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		hour := randInt(0, 23)
		min := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)
	}

	trace_util_0.Count(_rand_00000, 32)
	minTime, err := time.Parse(dateTimeFormat, min)
	if err != nil {
		trace_util_0.Count(_rand_00000, 38)
		log.Warn("parse min timestamp failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 33)
	if len(max) == 0 {
		trace_util_0.Count(_rand_00000, 39)
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	trace_util_0.Count(_rand_00000, 34)
	maxTime, err := time.Parse(dateTimeFormat, max)
	if err != nil {
		trace_util_0.Count(_rand_00000, 40)
		log.Warn("parse max timestamp failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 35)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func randYear(col *column) string {
	trace_util_0.Count(_rand_00000, 41)
	if col.hist != nil {
		trace_util_0.Count(_rand_00000, 46)
		return col.hist.randDate("YEAR", "%Y", yearFormat)
	}
	trace_util_0.Count(_rand_00000, 42)
	min, max := col.min, col.max
	if len(min) == 0 || len(max) == 0 {
		trace_util_0.Count(_rand_00000, 47)
		return fmt.Sprintf("%04d", time.Now().Year()-randInt(0, 10))
	}

	trace_util_0.Count(_rand_00000, 43)
	minTime, err := time.Parse(yearFormat, min)
	if err != nil {
		trace_util_0.Count(_rand_00000, 48)
		log.Warn("parse min year failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 44)
	maxTime, err := time.Parse(yearFormat, max)
	if err != nil {
		trace_util_0.Count(_rand_00000, 49)
		log.Warn("parse max year failed", zap.Error(err))
	}
	trace_util_0.Count(_rand_00000, 45)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d", t.Year())
}

var _rand_00000 = "cmd/importer/rand.go"
