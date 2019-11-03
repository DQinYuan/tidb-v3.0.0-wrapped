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
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/trace_util_0"
)

type datum struct {
	sync.Mutex

	intValue    int64
	minIntValue int64
	maxIntValue int64
	timeValue   time.Time
	remains     uint64
	repeats     uint64
	probability uint32
	step        int64

	init     bool
	useRange bool
}

func newDatum() *datum {
	trace_util_0.Count(_data_00000, 0)
	return &datum{step: 1, repeats: 1, remains: 1, probability: 100}
}

func (d *datum) setInitInt64Value(min int64, max int64) {
	trace_util_0.Count(_data_00000, 1)
	d.Lock()
	defer d.Unlock()

	if d.init {
		trace_util_0.Count(_data_00000, 4)
		return
	}

	trace_util_0.Count(_data_00000, 2)
	d.minIntValue = min
	d.maxIntValue = max
	d.useRange = true
	if d.step < 0 {
		trace_util_0.Count(_data_00000, 5)
		d.intValue = (min + max) / 2
	}

	trace_util_0.Count(_data_00000, 3)
	d.init = true
}

func (d *datum) updateRemains() {
	trace_util_0.Count(_data_00000, 6)
	if uint32(rand.Int31n(100))+1 <= 100-d.probability {
		trace_util_0.Count(_data_00000, 7)
		d.remains -= uint64(rand.Int63n(int64(d.remains))) + 1
	} else {
		trace_util_0.Count(_data_00000, 8)
		{
			d.remains--
		}
	}
}

func (d *datum) nextInt64() int64 {
	trace_util_0.Count(_data_00000, 9)
	d.Lock()
	defer d.Unlock()

	if d.remains <= 0 {
		trace_util_0.Count(_data_00000, 12)
		d.intValue += d.step
		d.remains = d.repeats
	}
	trace_util_0.Count(_data_00000, 10)
	if d.useRange {
		trace_util_0.Count(_data_00000, 13)
		d.intValue = mathutil.MinInt64(d.intValue, d.maxIntValue)
		d.intValue = mathutil.MaxInt64(d.intValue, d.minIntValue)
	}
	trace_util_0.Count(_data_00000, 11)
	d.updateRemains()
	return d.intValue
}

func (d *datum) nextFloat64() float64 {
	trace_util_0.Count(_data_00000, 14)
	data := d.nextInt64()
	return float64(data)
}

func (d *datum) nextString(n int) string {
	trace_util_0.Count(_data_00000, 15)
	data := d.nextInt64()

	var value []byte
	for ; ; n-- {
		trace_util_0.Count(_data_00000, 18)
		if n == 0 {
			trace_util_0.Count(_data_00000, 20)
			break
		}

		trace_util_0.Count(_data_00000, 19)
		idx := data % int64(len(alphabet))
		data = data / int64(len(alphabet))

		value = append(value, alphabet[idx])

		if data == 0 {
			trace_util_0.Count(_data_00000, 21)
			break
		}
	}

	trace_util_0.Count(_data_00000, 16)
	for i, j := 0, len(value)-1; i < j; i, j = i+1, j-1 {
		trace_util_0.Count(_data_00000, 22)
		value[i], value[j] = value[j], value[i]
	}

	trace_util_0.Count(_data_00000, 17)
	return string(value)
}

func (d *datum) nextTime() string {
	trace_util_0.Count(_data_00000, 23)
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		trace_util_0.Count(_data_00000, 26)
		d.timeValue = time.Now()
	}
	trace_util_0.Count(_data_00000, 24)
	if d.remains <= 0 {
		trace_util_0.Count(_data_00000, 27)
		d.timeValue = d.timeValue.Add(time.Duration(d.step) * time.Second)
		d.remains = d.repeats
	}
	trace_util_0.Count(_data_00000, 25)
	d.updateRemains()
	return fmt.Sprintf("%02d:%02d:%02d", d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextDate() string {
	trace_util_0.Count(_data_00000, 28)
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		trace_util_0.Count(_data_00000, 31)
		d.timeValue = time.Now()
	}
	trace_util_0.Count(_data_00000, 29)
	if d.remains <= 0 {
		trace_util_0.Count(_data_00000, 32)
		d.timeValue = d.timeValue.AddDate(0, 0, int(d.step))
		d.remains = d.repeats
	}
	trace_util_0.Count(_data_00000, 30)
	d.updateRemains()
	return fmt.Sprintf("%04d-%02d-%02d", d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day())
}

func (d *datum) nextTimestamp() string {
	trace_util_0.Count(_data_00000, 33)
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		trace_util_0.Count(_data_00000, 36)
		d.timeValue = time.Now()
	}
	trace_util_0.Count(_data_00000, 34)
	if d.remains <= 0 {
		trace_util_0.Count(_data_00000, 37)
		d.timeValue = d.timeValue.Add(time.Duration(d.step) * time.Second)
		d.remains = d.repeats
	}
	trace_util_0.Count(_data_00000, 35)
	d.updateRemains()
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day(),
		d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextYear() string {
	trace_util_0.Count(_data_00000, 38)
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		trace_util_0.Count(_data_00000, 41)
		d.timeValue = time.Now()
	}
	trace_util_0.Count(_data_00000, 39)
	if d.remains <= 0 {
		trace_util_0.Count(_data_00000, 42)
		d.timeValue = d.timeValue.AddDate(int(d.step), 0, 0)
		d.remains = d.repeats
	}
	trace_util_0.Count(_data_00000, 40)
	d.updateRemains()
	return fmt.Sprintf("%04d", d.timeValue.Year())
}

var _data_00000 = "cmd/importer/data.go"
