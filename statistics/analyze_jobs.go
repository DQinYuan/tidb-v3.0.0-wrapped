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

package statistics

import (
	"github.com/pingcap/tidb/trace_util_0"
	"sort"
	"sync"
	"time"
)

type analyzeJobs struct {
	sync.Mutex
	jobs    map[*AnalyzeJob]struct{}
	history []*AnalyzeJob
}

var analyzeStatus = analyzeJobs{jobs: make(map[*AnalyzeJob]struct{}), history: make([]*AnalyzeJob, 0, numMaxHistoryJobs)}

// AnalyzeJob is used to represent the status of one analyze job.
type AnalyzeJob struct {
	sync.Mutex
	DBName        string
	TableName     string
	PartitionName string
	JobInfo       string
	RowCount      int64
	StartTime     time.Time
	State         string
	updateTime    time.Time
}

const (
	pending  = "pending"
	running  = "running"
	finished = "finished"
	failed   = "failed"
)

// AddNewAnalyzeJob adds new analyze job.
func AddNewAnalyzeJob(job *AnalyzeJob) {
	trace_util_0.Count(_analyze_jobs_00000, 0)
	job.updateTime = time.Now()
	job.State = pending
	analyzeStatus.Lock()
	analyzeStatus.jobs[job] = struct{}{}
	analyzeStatus.Unlock()
}

const numMaxHistoryJobs = 20

// MoveToHistory moves the analyze job to history.
func MoveToHistory(job *AnalyzeJob) {
	trace_util_0.Count(_analyze_jobs_00000, 1)
	analyzeStatus.Lock()
	delete(analyzeStatus.jobs, job)
	analyzeStatus.history = append(analyzeStatus.history, job)
	numJobs := len(analyzeStatus.history)
	if numJobs > numMaxHistoryJobs {
		trace_util_0.Count(_analyze_jobs_00000, 3)
		analyzeStatus.history = analyzeStatus.history[numJobs-numMaxHistoryJobs:]
	}
	trace_util_0.Count(_analyze_jobs_00000, 2)
	analyzeStatus.Unlock()
}

// ClearHistoryJobs clears all history jobs.
func ClearHistoryJobs() {
	trace_util_0.Count(_analyze_jobs_00000, 4)
	analyzeStatus.Lock()
	analyzeStatus.history = analyzeStatus.history[:0]
	analyzeStatus.Unlock()
}

// GetAllAnalyzeJobs gets all analyze jobs.
func GetAllAnalyzeJobs() []*AnalyzeJob {
	trace_util_0.Count(_analyze_jobs_00000, 5)
	analyzeStatus.Lock()
	jobs := make([]*AnalyzeJob, 0, len(analyzeStatus.jobs)+len(analyzeStatus.history))
	for job := range analyzeStatus.jobs {
		trace_util_0.Count(_analyze_jobs_00000, 8)
		jobs = append(jobs, job)
	}
	trace_util_0.Count(_analyze_jobs_00000, 6)
	jobs = append(jobs, analyzeStatus.history...)
	analyzeStatus.Unlock()
	sort.Slice(jobs, func(i int, j int) bool {
		trace_util_0.Count(_analyze_jobs_00000, 9)
		return jobs[i].updateTime.Before(jobs[j].updateTime)
	})
	trace_util_0.Count(_analyze_jobs_00000, 7)
	return jobs
}

// Start marks status of the analyze job as running and update the start time.
func (job *AnalyzeJob) Start() {
	trace_util_0.Count(_analyze_jobs_00000, 10)
	now := time.Now()
	job.Mutex.Lock()
	job.State = running
	job.StartTime = now
	job.updateTime = now
	job.Mutex.Unlock()
}

// Update updates the row count of analyze job.
func (job *AnalyzeJob) Update(rowCount int64) {
	trace_util_0.Count(_analyze_jobs_00000, 11)
	now := time.Now()
	job.Mutex.Lock()
	job.RowCount += rowCount
	job.updateTime = now
	job.Mutex.Unlock()
}

// Finish update the status of analyze job to finished or failed according to `meetError`.
func (job *AnalyzeJob) Finish(meetError bool) {
	trace_util_0.Count(_analyze_jobs_00000, 12)
	now := time.Now()
	job.Mutex.Lock()
	if meetError {
		trace_util_0.Count(_analyze_jobs_00000, 14)
		job.State = failed
	} else {
		trace_util_0.Count(_analyze_jobs_00000, 15)
		{
			job.State = finished
		}
	}
	trace_util_0.Count(_analyze_jobs_00000, 13)
	job.updateTime = now
	job.Mutex.Unlock()
}

var _analyze_jobs_00000 = "statistics/analyze_jobs.go"
