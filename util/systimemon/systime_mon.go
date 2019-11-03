// Copyright 2017 PingCAP, Inc.
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

package systimemon

import (
	"context"
	"time"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(now func() time.Time, systimeErrHandler func(), successCallback func()) {
	trace_util_0.Count(_systime_mon_00000, 0)
	logutil.Logger(context.Background()).Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	tickCount := 0
	for {
		trace_util_0.Count(_systime_mon_00000, 1)
		last := now().UnixNano()
		<-tick.C
		if now().UnixNano() < last {
			trace_util_0.Count(_systime_mon_00000, 3)
			logutil.Logger(context.Background()).Error("system time jump backward", zap.Int64("last", last))
			systimeErrHandler()
		}
		// call sucessCallback per second.
		trace_util_0.Count(_systime_mon_00000, 2)
		tickCount++
		if tickCount >= 10 {
			trace_util_0.Count(_systime_mon_00000, 4)
			tickCount = 0
			successCallback()
		}
	}
}

var _systime_mon_00000 = "util/systimemon/systime_mon.go"
