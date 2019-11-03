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

package execdetails

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// CommitDetailCtxKey presents CommitDetail info key in context.
const CommitDetailCtxKey = "commitDetail"

// ExecDetails contains execution detail information.
type ExecDetails struct {
	CalleeAddress string
	ProcessTime   time.Duration
	WaitTime      time.Duration
	BackoffTime   time.Duration
	RequestCount  int
	TotalKeys     int64
	ProcessedKeys int64
	CommitDetail  *CommitDetails
}

// CommitDetails contains commit detail information.
type CommitDetails struct {
	GetCommitTsTime   time.Duration
	PrewriteTime      time.Duration
	CommitTime        time.Duration
	LocalLatchTime    time.Duration
	TotalBackoffTime  time.Duration
	ResolveLockTime   int64
	WriteKeys         int
	WriteSize         int
	PrewriteRegionNum int32
	TxnRetry          int
}

const (
	// ProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	ProcessTimeStr = "Process_time"
	// WaitTimeStr means the time of all coprocessor wait.
	WaitTimeStr = "Wait_time"
	// BackoffTimeStr means the time of all back-off.
	BackoffTimeStr = "Backoff_time"
	// RequestCountStr means the request count.
	RequestCountStr = "Request_count"
	// TotalKeysStr means the total scan keys.
	TotalKeysStr = "Total_keys"
	// ProcessKeysStr means the total processed keys.
	ProcessKeysStr = "Process_keys"
)

// String implements the fmt.Stringer interface.
func (d ExecDetails) String() string {
	trace_util_0.Count(_execdetails_00000, 0)
	parts := make([]string, 0, 6)
	if d.ProcessTime > 0 {
		trace_util_0.Count(_execdetails_00000, 8)
		parts = append(parts, ProcessTimeStr+": "+strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64))
	}
	trace_util_0.Count(_execdetails_00000, 1)
	if d.WaitTime > 0 {
		trace_util_0.Count(_execdetails_00000, 9)
		parts = append(parts, WaitTimeStr+": "+strconv.FormatFloat(d.WaitTime.Seconds(), 'f', -1, 64))
	}
	trace_util_0.Count(_execdetails_00000, 2)
	if d.BackoffTime > 0 {
		trace_util_0.Count(_execdetails_00000, 10)
		parts = append(parts, BackoffTimeStr+": "+strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64))
	}
	trace_util_0.Count(_execdetails_00000, 3)
	if d.RequestCount > 0 {
		trace_util_0.Count(_execdetails_00000, 11)
		parts = append(parts, RequestCountStr+": "+strconv.FormatInt(int64(d.RequestCount), 10))
	}
	trace_util_0.Count(_execdetails_00000, 4)
	if d.TotalKeys > 0 {
		trace_util_0.Count(_execdetails_00000, 12)
		parts = append(parts, TotalKeysStr+": "+strconv.FormatInt(d.TotalKeys, 10))
	}
	trace_util_0.Count(_execdetails_00000, 5)
	if d.ProcessedKeys > 0 {
		trace_util_0.Count(_execdetails_00000, 13)
		parts = append(parts, ProcessKeysStr+": "+strconv.FormatInt(d.ProcessedKeys, 10))
	}
	trace_util_0.Count(_execdetails_00000, 6)
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		trace_util_0.Count(_execdetails_00000, 14)
		if commitDetails.PrewriteTime > 0 {
			trace_util_0.Count(_execdetails_00000, 24)
			parts = append(parts, fmt.Sprintf("Prewrite_time: %v", commitDetails.PrewriteTime.Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 15)
		if commitDetails.CommitTime > 0 {
			trace_util_0.Count(_execdetails_00000, 25)
			parts = append(parts, fmt.Sprintf("Commit_time: %v", commitDetails.CommitTime.Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 16)
		if commitDetails.GetCommitTsTime > 0 {
			trace_util_0.Count(_execdetails_00000, 26)
			parts = append(parts, fmt.Sprintf("Get_commit_ts_time: %v", commitDetails.GetCommitTsTime.Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 17)
		if commitDetails.TotalBackoffTime > 0 {
			trace_util_0.Count(_execdetails_00000, 27)
			parts = append(parts, fmt.Sprintf("Total_backoff_time: %v", commitDetails.TotalBackoffTime.Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 18)
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		if resolveLockTime > 0 {
			trace_util_0.Count(_execdetails_00000, 28)
			parts = append(parts, fmt.Sprintf("Resolve_lock_time: %v", time.Duration(resolveLockTime).Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 19)
		if commitDetails.LocalLatchTime > 0 {
			trace_util_0.Count(_execdetails_00000, 29)
			parts = append(parts, fmt.Sprintf("Local_latch_wait_time: %v", commitDetails.LocalLatchTime.Seconds()))
		}
		trace_util_0.Count(_execdetails_00000, 20)
		if commitDetails.WriteKeys > 0 {
			trace_util_0.Count(_execdetails_00000, 30)
			parts = append(parts, fmt.Sprintf("Write_keys: %d", commitDetails.WriteKeys))
		}
		trace_util_0.Count(_execdetails_00000, 21)
		if commitDetails.WriteSize > 0 {
			trace_util_0.Count(_execdetails_00000, 31)
			parts = append(parts, fmt.Sprintf("Write_size: %d", commitDetails.WriteSize))
		}
		trace_util_0.Count(_execdetails_00000, 22)
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			trace_util_0.Count(_execdetails_00000, 32)
			parts = append(parts, fmt.Sprintf("Prewrite_region: %d", prewriteRegionNum))
		}
		trace_util_0.Count(_execdetails_00000, 23)
		if commitDetails.TxnRetry > 0 {
			trace_util_0.Count(_execdetails_00000, 33)
			parts = append(parts, fmt.Sprintf("Txn_retry: %d", commitDetails.TxnRetry))
		}
	}
	trace_util_0.Count(_execdetails_00000, 7)
	return strings.Join(parts, " ")
}

// ToZapFields wraps the ExecDetails as zap.Fields.
func (d ExecDetails) ToZapFields() (fields []zap.Field) {
	trace_util_0.Count(_execdetails_00000, 34)
	fields = make([]zap.Field, 0, 16)
	if d.ProcessTime > 0 {
		trace_util_0.Count(_execdetails_00000, 42)
		fields = append(fields, zap.String(strings.ToLower(ProcessTimeStr), strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	}
	trace_util_0.Count(_execdetails_00000, 35)
	if d.WaitTime > 0 {
		trace_util_0.Count(_execdetails_00000, 43)
		fields = append(fields, zap.String(strings.ToLower(WaitTimeStr), strconv.FormatFloat(d.ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	}
	trace_util_0.Count(_execdetails_00000, 36)
	if d.BackoffTime > 0 {
		trace_util_0.Count(_execdetails_00000, 44)
		fields = append(fields, zap.String(strings.ToLower(BackoffTimeStr), strconv.FormatFloat(d.BackoffTime.Seconds(), 'f', -1, 64)+"s"))
	}
	trace_util_0.Count(_execdetails_00000, 37)
	if d.RequestCount > 0 {
		trace_util_0.Count(_execdetails_00000, 45)
		fields = append(fields, zap.String(strings.ToLower(RequestCountStr), strconv.FormatInt(int64(d.RequestCount), 10)))
	}
	trace_util_0.Count(_execdetails_00000, 38)
	if d.TotalKeys > 0 {
		trace_util_0.Count(_execdetails_00000, 46)
		fields = append(fields, zap.String(strings.ToLower(TotalKeysStr), strconv.FormatInt(d.TotalKeys, 10)))
	}
	trace_util_0.Count(_execdetails_00000, 39)
	if d.ProcessedKeys > 0 {
		trace_util_0.Count(_execdetails_00000, 47)
		fields = append(fields, zap.String(strings.ToLower(ProcessKeysStr), strconv.FormatInt(d.ProcessedKeys, 10)))
	}
	trace_util_0.Count(_execdetails_00000, 40)
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		trace_util_0.Count(_execdetails_00000, 48)
		if commitDetails.PrewriteTime > 0 {
			trace_util_0.Count(_execdetails_00000, 58)
			fields = append(fields, zap.String("prewrite_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.PrewriteTime.Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 49)
		if commitDetails.CommitTime > 0 {
			trace_util_0.Count(_execdetails_00000, 59)
			fields = append(fields, zap.String("commit_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.CommitTime.Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 50)
		if commitDetails.GetCommitTsTime > 0 {
			trace_util_0.Count(_execdetails_00000, 60)
			fields = append(fields, zap.String("get_commit_ts_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.GetCommitTsTime.Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 51)
		if commitDetails.TotalBackoffTime > 0 {
			trace_util_0.Count(_execdetails_00000, 61)
			fields = append(fields, zap.String("total_backoff_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.TotalBackoffTime.Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 52)
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		if resolveLockTime > 0 {
			trace_util_0.Count(_execdetails_00000, 62)
			fields = append(fields, zap.String("resolve_lock_time", fmt.Sprintf("%v", strconv.FormatFloat(time.Duration(resolveLockTime).Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 53)
		if commitDetails.LocalLatchTime > 0 {
			trace_util_0.Count(_execdetails_00000, 63)
			fields = append(fields, zap.String("local_latch_wait_time", fmt.Sprintf("%v", strconv.FormatFloat(commitDetails.LocalLatchTime.Seconds(), 'f', -1, 64)+"s")))
		}
		trace_util_0.Count(_execdetails_00000, 54)
		if commitDetails.WriteKeys > 0 {
			trace_util_0.Count(_execdetails_00000, 64)
			fields = append(fields, zap.Int("write_keys", commitDetails.WriteKeys))
		}
		trace_util_0.Count(_execdetails_00000, 55)
		if commitDetails.WriteSize > 0 {
			trace_util_0.Count(_execdetails_00000, 65)
			fields = append(fields, zap.Int("write_size", commitDetails.WriteSize))
		}
		trace_util_0.Count(_execdetails_00000, 56)
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			trace_util_0.Count(_execdetails_00000, 66)
			fields = append(fields, zap.Int32("prewrite_region", prewriteRegionNum))
		}
		trace_util_0.Count(_execdetails_00000, 57)
		if commitDetails.TxnRetry > 0 {
			trace_util_0.Count(_execdetails_00000, 67)
			fields = append(fields, zap.Int("txn_retry", commitDetails.TxnRetry))
		}
	}
	trace_util_0.Count(_execdetails_00000, 41)
	return fields
}

// CopRuntimeStats collects cop tasks' execution info.
type CopRuntimeStats struct {
	sync.Mutex

	// stats stores the runtime statistics of coprocessor tasks.
	// The key of the map is the tikv-server address. Because a tikv-server can
	// have many region leaders, several coprocessor tasks can be sent to the
	// same tikv-server instance. We have to use a list to maintain all tasks
	// executed on each instance.
	stats map[string][]*RuntimeStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (crs *CopRuntimeStats) RecordOneCopTask(address string, summary *tipb.ExecutorExecutionSummary) {
	trace_util_0.Count(_execdetails_00000, 68)
	crs.Lock()
	defer crs.Unlock()
	crs.stats[address] = append(crs.stats[address],
		&RuntimeStats{int32(*summary.NumIterations), int64(*summary.TimeProcessedNs), int64(*summary.NumProducedRows)})
}

func (crs *CopRuntimeStats) String() string {
	trace_util_0.Count(_execdetails_00000, 69)
	if len(crs.stats) == 0 {
		trace_util_0.Count(_execdetails_00000, 74)
		return ""
	}

	trace_util_0.Count(_execdetails_00000, 70)
	var totalRows, totalTasks int64
	var totalIters int32
	procTimes := make([]time.Duration, 0, 32)
	for _, instanceStats := range crs.stats {
		trace_util_0.Count(_execdetails_00000, 75)
		for _, stat := range instanceStats {
			trace_util_0.Count(_execdetails_00000, 76)
			procTimes = append(procTimes, time.Duration(stat.consume)*time.Nanosecond)
			totalRows += stat.rows
			totalIters += stat.loop
			totalTasks++
		}
	}

	trace_util_0.Count(_execdetails_00000, 71)
	if totalTasks == 1 {
		trace_util_0.Count(_execdetails_00000, 77)
		return fmt.Sprintf("time:%v, loops:%d, rows:%d", procTimes[0], totalIters, totalRows)
	}

	trace_util_0.Count(_execdetails_00000, 72)
	n := len(procTimes)
	sort.Slice(procTimes, func(i, j int) bool { trace_util_0.Count(_execdetails_00000, 78); return procTimes[i] < procTimes[j] })
	trace_util_0.Count(_execdetails_00000, 73)
	return fmt.Sprintf("proc max:%v, min:%v, p80:%v, p95:%v, rows:%v, iters:%v, tasks:%v",
		procTimes[n-1], procTimes[0], procTimes[n*4/5], procTimes[n*19/20], totalRows, totalIters, totalTasks)
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu        sync.Mutex
	rootStats map[string]*RuntimeStats
	copStats  map[string]*CopRuntimeStats
}

// RuntimeStats collects one executor's execution info.
type RuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
}

// NewRuntimeStatsColl creates new executor collector.
func NewRuntimeStatsColl() *RuntimeStatsColl {
	trace_util_0.Count(_execdetails_00000, 79)
	return &RuntimeStatsColl{rootStats: make(map[string]*RuntimeStats),
		copStats: make(map[string]*CopRuntimeStats)}
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID string) *RuntimeStats {
	trace_util_0.Count(_execdetails_00000, 80)
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		trace_util_0.Count(_execdetails_00000, 82)
		runtimeStats = &RuntimeStats{}
		e.rootStats[planID] = runtimeStats
	}
	trace_util_0.Count(_execdetails_00000, 81)
	return runtimeStats
}

// GetCopStats gets the CopRuntimeStats specified by planID.
func (e *RuntimeStatsColl) GetCopStats(planID string) *CopRuntimeStats {
	trace_util_0.Count(_execdetails_00000, 83)
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		trace_util_0.Count(_execdetails_00000, 85)
		copStats = &CopRuntimeStats{stats: make(map[string][]*RuntimeStats)}
		e.copStats[planID] = copStats
	}
	trace_util_0.Count(_execdetails_00000, 84)
	return copStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordOneCopTask(planID, address string, summary *tipb.ExecutorExecutionSummary) {
	trace_util_0.Count(_execdetails_00000, 86)
	copStats := e.GetCopStats(planID)
	copStats.RecordOneCopTask(address, summary)
}

// ExistsRootStats checks if the planID exists in the rootStats collection.
func (e *RuntimeStatsColl) ExistsRootStats(planID string) bool {
	trace_util_0.Count(_execdetails_00000, 87)
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.rootStats[planID]
	return exists
}

// ExistsCopStats checks if the planID exists in the copStats collection.
func (e *RuntimeStatsColl) ExistsCopStats(planID string) bool {
	trace_util_0.Count(_execdetails_00000, 88)
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.copStats[planID]
	return exists
}

// Record records executor's execution.
func (e *RuntimeStats) Record(d time.Duration, rowNum int) {
	trace_util_0.Count(_execdetails_00000, 89)
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

// SetRowNum sets the row num.
func (e *RuntimeStats) SetRowNum(rowNum int64) {
	trace_util_0.Count(_execdetails_00000, 90)
	atomic.StoreInt64(&e.rows, rowNum)
}

func (e *RuntimeStats) String() string {
	trace_util_0.Count(_execdetails_00000, 91)
	if e == nil {
		trace_util_0.Count(_execdetails_00000, 93)
		return ""
	}
	trace_util_0.Count(_execdetails_00000, 92)
	return fmt.Sprintf("time:%v, loops:%d, rows:%d", time.Duration(e.consume), e.loop, e.rows)
}

var _execdetails_00000 = "util/execdetails/execdetails.go"
