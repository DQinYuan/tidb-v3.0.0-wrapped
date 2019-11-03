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

package ddl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
	RunWorker = true
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = int32(0)
)

type workerType byte

const (
	// generalWorker is the worker who handles all DDL statements except “add index”.
	generalWorker workerType = 0
	// addIdxWorker is the worker who handles the operation of adding indexes.
	addIdxWorker workerType = 1
	// waitDependencyJobInterval is the interval when the dependency job doesn't be done.
	waitDependencyJobInterval = 200 * time.Millisecond
	// noneDependencyJob means a job has no dependency-job.
	noneDependencyJob = 0
)

// worker is used for handling DDL jobs.
// Now we have two kinds of workers.
type worker struct {
	id       int32
	tp       workerType
	ddlJobCh chan struct{}
	quitCh   chan struct{}
	wg       sync.WaitGroup

	sessPool        *sessionPool // sessPool is used to new sessions to execute SQL in ddl package.
	reorgCtx        *reorgCtx    // reorgCtx is used for reorganization.
	delRangeManager delRangeManager
	logCtx          context.Context
}

func newWorker(tp workerType, store kv.Storage, sessPool *sessionPool, delRangeMgr delRangeManager) *worker {
	trace_util_0.Count(_ddl_worker_00000, 0)
	worker := &worker{
		id:              atomic.AddInt32(&ddlWorkerID, 1),
		tp:              tp,
		ddlJobCh:        make(chan struct{}, 1),
		quitCh:          make(chan struct{}),
		reorgCtx:        &reorgCtx{notifyCancelReorgJob: 0},
		sessPool:        sessPool,
		delRangeManager: delRangeMgr,
	}

	worker.logCtx = logutil.WithKeyValue(context.Background(), "worker", worker.String())
	return worker
}

func (w *worker) typeStr() string {
	trace_util_0.Count(_ddl_worker_00000, 1)
	var str string
	switch w.tp {
	case generalWorker:
		trace_util_0.Count(_ddl_worker_00000, 3)
		str = "general"
	case addIdxWorker:
		trace_util_0.Count(_ddl_worker_00000, 4)
		str = model.AddIndexStr
	default:
		trace_util_0.Count(_ddl_worker_00000, 5)
		str = "unknow"
	}
	trace_util_0.Count(_ddl_worker_00000, 2)
	return str
}

func (w *worker) String() string {
	trace_util_0.Count(_ddl_worker_00000, 6)
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

func (w *worker) close() {
	trace_util_0.Count(_ddl_worker_00000, 7)
	startTime := time.Now()
	close(w.quitCh)
	w.wg.Wait()
	logutil.Logger(w.logCtx).Info("[ddl] DDL worker closed", zap.Duration("take time", time.Since(startTime)))
}

// start is used for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (w *worker) start(d *ddlCtx) {
	trace_util_0.Count(_ddl_worker_00000, 8)
	logutil.Logger(w.logCtx).Info("[ddl] start DDL worker")
	defer w.wg.Done()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		trace_util_0.Count(_ddl_worker_00000, 9)
		select {
		case <-ticker.C:
			trace_util_0.Count(_ddl_worker_00000, 11)
			logutil.Logger(w.logCtx).Debug("[ddl] wait to check DDL status again", zap.Duration("interval", checkTime))
		case <-w.ddlJobCh:
			trace_util_0.Count(_ddl_worker_00000, 12)
		case <-w.quitCh:
			trace_util_0.Count(_ddl_worker_00000, 13)
			return
		}

		trace_util_0.Count(_ddl_worker_00000, 10)
		err := w.handleDDLJobQueue(d)
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 14)
			logutil.Logger(w.logCtx).Error("[ddl] handle DDL job failed", zap.Error(err))
		}
	}
}

func asyncNotify(ch chan struct{}) {
	trace_util_0.Count(_ddl_worker_00000, 15)
	select {
	case ch <- struct{}{}:
		trace_util_0.Count(_ddl_worker_00000, 16)
	default:
		trace_util_0.Count(_ddl_worker_00000, 17)
	}
}

// buildJobDependence sets the curjob's dependency-ID.
// The dependency-job's ID must less than the current job's ID, and we need the largest one in the list.
func buildJobDependence(t *meta.Meta, curJob *model.Job) error {
	trace_util_0.Count(_ddl_worker_00000, 18)
	// Jobs in the same queue are ordered. If we want to find a job's dependency-job, we need to look for
	// it from the other queue. So if the job is "ActionAddIndex" job, we need find its dependency-job from DefaultJobList.
	var jobs []*model.Job
	var err error
	switch curJob.Type {
	case model.ActionAddIndex:
		trace_util_0.Count(_ddl_worker_00000, 22)
		jobs, err = t.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
	default:
		trace_util_0.Count(_ddl_worker_00000, 23)
		jobs, err = t.GetAllDDLJobsInQueue(meta.AddIndexJobListKey)
	}
	trace_util_0.Count(_ddl_worker_00000, 19)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 24)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_worker_00000, 20)
	for _, job := range jobs {
		trace_util_0.Count(_ddl_worker_00000, 25)
		if curJob.ID < job.ID {
			trace_util_0.Count(_ddl_worker_00000, 28)
			continue
		}
		trace_util_0.Count(_ddl_worker_00000, 26)
		isDependent, err := curJob.IsDependentOn(job)
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 29)
			return errors.Trace(err)
		}
		trace_util_0.Count(_ddl_worker_00000, 27)
		if isDependent {
			trace_util_0.Count(_ddl_worker_00000, 30)
			logutil.Logger(ddlLogCtx).Info("[ddl] current DDL job depends on other job", zap.String("currentJob", curJob.String()), zap.String("dependentJob", job.String()))
			curJob.DependencyID = job.ID
			break
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 21)
	return nil
}

// addDDLJob gets a global job ID and puts the DDL job in the DDL queue.
func (d *ddl) addDDLJob(ctx sessionctx.Context, job *model.Job) error {
	trace_util_0.Count(_ddl_worker_00000, 31)
	startTime := time.Now()
	job.Version = currentVersion
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_ddl_worker_00000, 33)
		t := newMetaWithQueueTp(txn, job.Type.String())
		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 36)
			return errors.Trace(err)
		}
		trace_util_0.Count(_ddl_worker_00000, 34)
		job.StartTS = txn.StartTS()
		if err = buildJobDependence(t, job); err != nil {
			trace_util_0.Count(_ddl_worker_00000, 37)
			return errors.Trace(err)
		}
		trace_util_0.Count(_ddl_worker_00000, 35)
		err = t.EnQueueDDLJob(job)

		return errors.Trace(err)
	})
	trace_util_0.Count(_ddl_worker_00000, 32)
	metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// getHistoryDDLJob gets a DDL job with job's ID from history queue.
func (d *ddl) getHistoryDDLJob(id int64) (*model.Job, error) {
	trace_util_0.Count(_ddl_worker_00000, 38)
	var job *model.Job

	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		trace_util_0.Count(_ddl_worker_00000, 40)
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	trace_util_0.Count(_ddl_worker_00000, 39)
	return job, errors.Trace(err)
}

// getFirstDDLJob gets the first DDL job form DDL queue.
func (w *worker) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	trace_util_0.Count(_ddl_worker_00000, 41)
	job, err := t.GetDDLJobByIdx(0)
	return job, errors.Trace(err)
}

// handleUpdateJobError handles the too large DDL job.
func (w *worker) handleUpdateJobError(t *meta.Meta, job *model.Job, err error) error {
	trace_util_0.Count(_ddl_worker_00000, 42)
	if err == nil {
		trace_util_0.Count(_ddl_worker_00000, 45)
		return nil
	}
	trace_util_0.Count(_ddl_worker_00000, 43)
	if kv.ErrEntryTooLarge.Equal(err) {
		trace_util_0.Count(_ddl_worker_00000, 46)
		logutil.Logger(w.logCtx).Warn("[ddl] update DDL job failed", zap.String("job", job.String()), zap.Error(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = w.finishDDLJob(t, job)
	}
	trace_util_0.Count(_ddl_worker_00000, 44)
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (w *worker) updateDDLJob(t *meta.Meta, job *model.Job, meetErr bool) error {
	trace_util_0.Count(_ddl_worker_00000, 47)
	updateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		trace_util_0.Count(_ddl_worker_00000, 49)
		logutil.Logger(w.logCtx).Info("[ddl] meet error before update DDL job, shouldn't update raw args", zap.String("job", job.String()))
		updateRawArgs = false
	}
	trace_util_0.Count(_ddl_worker_00000, 48)
	return errors.Trace(t.UpdateDDLJob(0, job, updateRawArgs))
}

func (w *worker) deleteRange(job *model.Job) error {
	trace_util_0.Count(_ddl_worker_00000, 50)
	var err error
	if job.Version <= currentVersion {
		trace_util_0.Count(_ddl_worker_00000, 52)
		err = w.delRangeManager.addDelRangeJob(job)
	} else {
		trace_util_0.Count(_ddl_worker_00000, 53)
		{
			err = errInvalidJobVersion.GenWithStackByArgs(job.Version, currentVersion)
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 51)
	return errors.Trace(err)
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	trace_util_0.Count(_ddl_worker_00000, 54)
	startTime := time.Now()
	defer func() {
		trace_util_0.Count(_ddl_worker_00000, 60)
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	trace_util_0.Count(_ddl_worker_00000, 55)
	if !job.IsCancelled() {
		trace_util_0.Count(_ddl_worker_00000, 61)
		switch job.Type {
		case model.ActionAddIndex:
			trace_util_0.Count(_ddl_worker_00000, 62)
			if job.State != model.JobStateRollbackDone {
				trace_util_0.Count(_ddl_worker_00000, 65)
				break
			}

			// After rolling back an AddIndex operation, we need to use delete-range to delete the half-done index data.
			trace_util_0.Count(_ddl_worker_00000, 63)
			err = w.deleteRange(job)
		case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropTablePartition, model.ActionTruncateTablePartition:
			trace_util_0.Count(_ddl_worker_00000, 64)
			err = w.deleteRange(job)
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 56)
	switch job.Type {
	case model.ActionRecoverTable:
		trace_util_0.Count(_ddl_worker_00000, 66)
		err = finishRecoverTable(w, t, job)
	}
	trace_util_0.Count(_ddl_worker_00000, 57)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 67)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_worker_00000, 58)
	_, err = t.DeQueueDDLJob()
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 68)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_worker_00000, 59)
	job.BinlogInfo.FinishedTS = t.StartTS
	logutil.Logger(w.logCtx).Info("[ddl] finish DDL job", zap.String("job", job.String()))
	err = t.AddHistoryDDLJob(job)
	return errors.Trace(err)
}

func finishRecoverTable(w *worker, t *meta.Meta, job *model.Job) error {
	trace_util_0.Count(_ddl_worker_00000, 69)
	tbInfo := &model.TableInfo{}
	var autoID, dropJobID, recoverTableCheckFlag int64
	var snapshotTS uint64
	err := job.DecodeArgs(tbInfo, &autoID, &dropJobID, &snapshotTS, &recoverTableCheckFlag)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 72)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_worker_00000, 70)
	if recoverTableCheckFlag == recoverTableCheckFlagEnableGC {
		trace_util_0.Count(_ddl_worker_00000, 73)
		err = enableGC(w)
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 74)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 71)
	return nil
}

func isDependencyJobDone(t *meta.Meta, job *model.Job) (bool, error) {
	trace_util_0.Count(_ddl_worker_00000, 75)
	if job.DependencyID == noneDependencyJob {
		trace_util_0.Count(_ddl_worker_00000, 79)
		return true, nil
	}

	trace_util_0.Count(_ddl_worker_00000, 76)
	historyJob, err := t.GetHistoryDDLJob(job.DependencyID)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 80)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_worker_00000, 77)
	if historyJob == nil {
		trace_util_0.Count(_ddl_worker_00000, 81)
		return false, nil
	}
	trace_util_0.Count(_ddl_worker_00000, 78)
	logutil.Logger(ddlLogCtx).Info("[ddl] current DDL job dependent job is finished", zap.String("currentJob", job.String()), zap.Int64("dependentJobID", job.DependencyID))
	job.DependencyID = noneDependencyJob
	return true, nil
}

func newMetaWithQueueTp(txn kv.Transaction, tp string) *meta.Meta {
	trace_util_0.Count(_ddl_worker_00000, 82)
	if tp == model.AddIndexStr {
		trace_util_0.Count(_ddl_worker_00000, 84)
		return meta.NewMeta(txn, meta.AddIndexJobListKey)
	}
	trace_util_0.Count(_ddl_worker_00000, 83)
	return meta.NewMeta(txn)
}

// handleDDLJobQueue handles DDL jobs in DDL Job queue.
func (w *worker) handleDDLJobQueue(d *ddlCtx) error {
	trace_util_0.Count(_ddl_worker_00000, 85)
	once := true
	waitDependencyJobCnt := 0
	for {
		trace_util_0.Count(_ddl_worker_00000, 86)
		if isChanClosed(w.quitCh) {
			trace_util_0.Count(_ddl_worker_00000, 91)
			return nil
		}

		trace_util_0.Count(_ddl_worker_00000, 87)
		var (
			job       *model.Job
			schemaVer int64
			runJobErr error
		)
		waitTime := 2 * d.lease
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			trace_util_0.Count(_ddl_worker_00000, 92)
			// We are not owner, return and retry checking later.
			if !d.isOwner() {
				trace_util_0.Count(_ddl_worker_00000, 101)
				return nil
			}

			trace_util_0.Count(_ddl_worker_00000, 93)
			var err error
			t := newMetaWithQueueTp(txn, w.typeStr())
			// We become the owner. Get the first job and run it.
			job, err = w.getFirstDDLJob(t)
			if job == nil || err != nil {
				trace_util_0.Count(_ddl_worker_00000, 102)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_worker_00000, 94)
			if isDone, err1 := isDependencyJobDone(t, job); err1 != nil || !isDone {
				trace_util_0.Count(_ddl_worker_00000, 103)
				return errors.Trace(err1)
			}

			trace_util_0.Count(_ddl_worker_00000, 95)
			if once {
				trace_util_0.Count(_ddl_worker_00000, 104)
				w.waitSchemaSynced(d, job, waitTime)
				once = false
				return nil
			}

			trace_util_0.Count(_ddl_worker_00000, 96)
			if job.IsDone() || job.IsRollbackDone() {
				trace_util_0.Count(_ddl_worker_00000, 105)
				if !job.IsRollbackDone() {
					trace_util_0.Count(_ddl_worker_00000, 107)
					job.State = model.JobStateSynced
				}
				trace_util_0.Count(_ddl_worker_00000, 106)
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}

			trace_util_0.Count(_ddl_worker_00000, 97)
			d.mu.RLock()
			d.mu.hook.OnJobRunBefore(job)
			d.mu.RUnlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			schemaVer, runJobErr = w.runDDLJob(d, t, job)
			if job.IsCancelled() {
				trace_util_0.Count(_ddl_worker_00000, 108)
				txn.Reset()
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_worker_00000, 98)
			err = w.updateDDLJob(t, job, runJobErr != nil)
			if err = w.handleUpdateJobError(t, job, err); err != nil {
				trace_util_0.Count(_ddl_worker_00000, 109)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_worker_00000, 99)
			if job.IsDone() || job.IsRollbackDone() {
				trace_util_0.Count(_ddl_worker_00000, 110)
				binloginfo.SetDDLBinlog(d.binlogCli, txn, job.ID, job.Query)
			}
			trace_util_0.Count(_ddl_worker_00000, 100)
			return nil
		})

		trace_util_0.Count(_ddl_worker_00000, 88)
		if runJobErr != nil {
			trace_util_0.Count(_ddl_worker_00000, 111)
			// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
			// which may act like a deadlock.
			logutil.Logger(w.logCtx).Info("[ddl] run DDL job error, sleeps a while then retries it.", zap.Duration("waitTime", WaitTimeWhenErrorOccured), zap.Error(runJobErr))
			time.Sleep(WaitTimeWhenErrorOccured)
		}

		trace_util_0.Count(_ddl_worker_00000, 89)
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 112)
			return errors.Trace(err)
		} else {
			trace_util_0.Count(_ddl_worker_00000, 113)
			if job == nil {
				trace_util_0.Count(_ddl_worker_00000, 114)
				// No job now, return and retry getting later.
				return nil
			}
		}
		trace_util_0.Count(_ddl_worker_00000, 90)
		w.waitDependencyJobFinished(job, &waitDependencyJobCnt)

		d.mu.RLock()
		d.mu.hook.OnJobUpdated(job)
		d.mu.RUnlock()

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		w.waitSchemaChanged(nil, d, waitTime, schemaVer, job)
		if job.IsSynced() || job.IsCancelled() {
			trace_util_0.Count(_ddl_worker_00000, 115)
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

// waitDependencyJobFinished waits for the dependency-job to be finished.
// If the dependency job isn't finished yet, we'd better wait a moment.
func (w *worker) waitDependencyJobFinished(job *model.Job, cnt *int) {
	trace_util_0.Count(_ddl_worker_00000, 116)
	if job.DependencyID != noneDependencyJob {
		trace_util_0.Count(_ddl_worker_00000, 117)
		intervalCnt := int(3 * time.Second / waitDependencyJobInterval)
		if *cnt%intervalCnt == 0 {
			trace_util_0.Count(_ddl_worker_00000, 119)
			logutil.Logger(w.logCtx).Info("[ddl] DDL job need to wait dependent job, sleeps a while, then retries it.",
				zap.Int64("jobID", job.ID),
				zap.Int64("dependentJobID", job.DependencyID),
				zap.Duration("waitTime", waitDependencyJobInterval))
		}
		trace_util_0.Count(_ddl_worker_00000, 118)
		time.Sleep(waitDependencyJobInterval)
		*cnt++
	} else {
		trace_util_0.Count(_ddl_worker_00000, 120)
		{
			*cnt = 0
		}
	}
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	trace_util_0.Count(_ddl_worker_00000, 121)
	if t == 0 || t > max {
		trace_util_0.Count(_ddl_worker_00000, 123)
		return max
	}
	trace_util_0.Count(_ddl_worker_00000, 122)
	return t
}

// runDDLJob runs a DDL job. It returns the current schema version in this transaction and the error.
func (w *worker) runDDLJob(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	trace_util_0.Count(_ddl_worker_00000, 124)
	logutil.Logger(w.logCtx).Info("[ddl] run DDL job", zap.String("job", job.String()))
	timeStart := time.Now()
	defer func() {
		trace_util_0.Count(_ddl_worker_00000, 131)
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	trace_util_0.Count(_ddl_worker_00000, 125)
	if job.IsFinished() {
		trace_util_0.Count(_ddl_worker_00000, 132)
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	trace_util_0.Count(_ddl_worker_00000, 126)
	if job.IsCancelling() {
		trace_util_0.Count(_ddl_worker_00000, 133)
		return convertJob2RollbackJob(w, d, t, job)
	}

	trace_util_0.Count(_ddl_worker_00000, 127)
	if !job.IsRollingback() && !job.IsCancelling() {
		trace_util_0.Count(_ddl_worker_00000, 134)
		job.State = model.JobStateRunning
	}

	trace_util_0.Count(_ddl_worker_00000, 128)
	switch job.Type {
	case model.ActionCreateSchema:
		trace_util_0.Count(_ddl_worker_00000, 135)
		ver, err = onCreateSchema(d, t, job)
	case model.ActionModifySchemaCharsetAndCollate:
		trace_util_0.Count(_ddl_worker_00000, 136)
		ver, err = onModifySchemaCharsetAndCollate(t, job)
	case model.ActionDropSchema:
		trace_util_0.Count(_ddl_worker_00000, 137)
		ver, err = onDropSchema(t, job)
	case model.ActionCreateTable:
		trace_util_0.Count(_ddl_worker_00000, 138)
		ver, err = onCreateTable(d, t, job)
	case model.ActionCreateView:
		trace_util_0.Count(_ddl_worker_00000, 139)
		ver, err = onCreateView(d, t, job)
	case model.ActionDropTable, model.ActionDropView:
		trace_util_0.Count(_ddl_worker_00000, 140)
		ver, err = onDropTableOrView(t, job)
	case model.ActionDropTablePartition:
		trace_util_0.Count(_ddl_worker_00000, 141)
		ver, err = onDropTablePartition(t, job)
	case model.ActionTruncateTablePartition:
		trace_util_0.Count(_ddl_worker_00000, 142)
		ver, err = onTruncateTablePartition(t, job)
	case model.ActionAddColumn:
		trace_util_0.Count(_ddl_worker_00000, 143)
		ver, err = onAddColumn(d, t, job)
	case model.ActionDropColumn:
		trace_util_0.Count(_ddl_worker_00000, 144)
		ver, err = onDropColumn(t, job)
	case model.ActionModifyColumn:
		trace_util_0.Count(_ddl_worker_00000, 145)
		ver, err = w.onModifyColumn(t, job)
	case model.ActionSetDefaultValue:
		trace_util_0.Count(_ddl_worker_00000, 146)
		ver, err = onSetDefaultValue(t, job)
	case model.ActionAddIndex:
		trace_util_0.Count(_ddl_worker_00000, 147)
		ver, err = w.onCreateIndex(d, t, job)
	case model.ActionDropIndex:
		trace_util_0.Count(_ddl_worker_00000, 148)
		ver, err = onDropIndex(t, job)
	case model.ActionRenameIndex:
		trace_util_0.Count(_ddl_worker_00000, 149)
		ver, err = onRenameIndex(t, job)
	case model.ActionAddForeignKey:
		trace_util_0.Count(_ddl_worker_00000, 150)
		ver, err = onCreateForeignKey(t, job)
	case model.ActionDropForeignKey:
		trace_util_0.Count(_ddl_worker_00000, 151)
		ver, err = onDropForeignKey(t, job)
	case model.ActionTruncateTable:
		trace_util_0.Count(_ddl_worker_00000, 152)
		ver, err = onTruncateTable(d, t, job)
	case model.ActionRebaseAutoID:
		trace_util_0.Count(_ddl_worker_00000, 153)
		ver, err = onRebaseAutoID(d.store, t, job)
	case model.ActionRenameTable:
		trace_util_0.Count(_ddl_worker_00000, 154)
		ver, err = onRenameTable(d, t, job)
	case model.ActionShardRowID:
		trace_util_0.Count(_ddl_worker_00000, 155)
		ver, err = w.onShardRowID(d, t, job)
	case model.ActionModifyTableComment:
		trace_util_0.Count(_ddl_worker_00000, 156)
		ver, err = onModifyTableComment(t, job)
	case model.ActionAddTablePartition:
		trace_util_0.Count(_ddl_worker_00000, 157)
		ver, err = onAddTablePartition(t, job)
	case model.ActionModifyTableCharsetAndCollate:
		trace_util_0.Count(_ddl_worker_00000, 158)
		ver, err = onModifyTableCharsetAndCollate(t, job)
	case model.ActionRecoverTable:
		trace_util_0.Count(_ddl_worker_00000, 159)
		ver, err = w.onRecoverTable(d, t, job)
	default:
		trace_util_0.Count(_ddl_worker_00000, 160)
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = errInvalidDDLJob.GenWithStack("invalid ddl job type: %v", job.Type)
	}

	// Save errors in job, so that others can know errors happened.
	trace_util_0.Count(_ddl_worker_00000, 129)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 161)
		job.Error = toTError(err)
		job.ErrorCount++

		// If job is cancelled, we shouldn't return an error and shouldn't load DDL variables.
		if job.State == model.JobStateCancelled {
			trace_util_0.Count(_ddl_worker_00000, 164)
			logutil.Logger(w.logCtx).Info("[ddl] DDL job is cancelled normally", zap.Error(err))
			return ver, nil
		}
		trace_util_0.Count(_ddl_worker_00000, 162)
		logutil.Logger(w.logCtx).Error("[ddl] run DDL job error", zap.Error(err))

		// Load global ddl variables.
		if err1 := loadDDLVars(w); err1 != nil {
			trace_util_0.Count(_ddl_worker_00000, 165)
			logutil.Logger(w.logCtx).Error("[ddl] load DDL global variable failed", zap.Error(err1))
		}
		// Check error limit to avoid falling into an infinite loop.
		trace_util_0.Count(_ddl_worker_00000, 163)
		if job.ErrorCount > variable.GetDDLErrorCountLimit() && job.State == model.JobStateRunning && admin.IsJobRollbackable(job) {
			trace_util_0.Count(_ddl_worker_00000, 166)
			logutil.Logger(w.logCtx).Warn("[ddl] DDL job error count exceed the limit, cancelling it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", variable.GetDDLErrorCountLimit()))
			job.State = model.JobStateCancelling
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 130)
	return
}

func loadDDLVars(w *worker) error {
	trace_util_0.Count(_ddl_worker_00000, 167)
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 169)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_worker_00000, 168)
	defer w.sessPool.put(ctx)
	return util.LoadDDLVars(ctx)
}

func toTError(err error) *terror.Error {
	trace_util_0.Count(_ddl_worker_00000, 170)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		trace_util_0.Count(_ddl_worker_00000, 172)
		return tErr
	}

	// TODO: Add the error code.
	trace_util_0.Count(_ddl_worker_00000, 171)
	return terror.ClassDDL.New(terror.CodeUnknown, err.Error())
}

// waitSchemaChanged waits for the completion of updating all servers' schema. In order to make sure that happens,
// we wait 2 * lease time.
func (w *worker) waitSchemaChanged(ctx context.Context, d *ddlCtx, waitTime time.Duration, latestSchemaVersion int64, job *model.Job) {
	trace_util_0.Count(_ddl_worker_00000, 173)
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		trace_util_0.Count(_ddl_worker_00000, 181)
		return
	}
	trace_util_0.Count(_ddl_worker_00000, 174)
	if waitTime == 0 {
		trace_util_0.Count(_ddl_worker_00000, 182)
		return
	}

	trace_util_0.Count(_ddl_worker_00000, 175)
	timeStart := time.Now()
	var err error
	defer func() {
		trace_util_0.Count(_ddl_worker_00000, 183)
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	trace_util_0.Count(_ddl_worker_00000, 176)
	if latestSchemaVersion == 0 {
		trace_util_0.Count(_ddl_worker_00000, 184)
		logutil.Logger(w.logCtx).Info("[ddl] schema version doesn't change")
		return
	}

	trace_util_0.Count(_ddl_worker_00000, 177)
	if ctx == nil {
		trace_util_0.Count(_ddl_worker_00000, 185)
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(context.Background(), waitTime)
		defer cancelFunc()
	}
	trace_util_0.Count(_ddl_worker_00000, 178)
	err = d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 186)
		logutil.Logger(w.logCtx).Info("[ddl] update latest schema version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			trace_util_0.Count(_ddl_worker_00000, 187)
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when context is timeout(2 * lease) or all TiDB schemas are synced.
	trace_util_0.Count(_ddl_worker_00000, 179)
	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 188)
		logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version to deadline", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			trace_util_0.Count(_ddl_worker_00000, 190)
			return
		}
		trace_util_0.Count(_ddl_worker_00000, 189)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_ddl_worker_00000, 191)
			return
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 180)
	logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version changed",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func (w *worker) waitSchemaSynced(d *ddlCtx, job *model.Job, waitTime time.Duration) {
	trace_util_0.Count(_ddl_worker_00000, 192)
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		trace_util_0.Count(_ddl_worker_00000, 195)
		return
	}
	// TODO: Make ctx exits when the d is close.
	trace_util_0.Count(_ddl_worker_00000, 193)
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitTime)
	defer cancelFunc()

	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 196)
		logutil.Logger(w.logCtx).Warn("[ddl] get global version failed", zap.Error(err))
		return
	}
	trace_util_0.Count(_ddl_worker_00000, 194)
	w.waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func updateSchemaVersion(t *meta.Meta, job *model.Job) (int64, error) {
	trace_util_0.Count(_ddl_worker_00000, 197)
	schemaVersion, err := t.GenSchemaVersion()
	if err != nil {
		trace_util_0.Count(_ddl_worker_00000, 200)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_worker_00000, 198)
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	if job.Type == model.ActionTruncateTable {
		trace_util_0.Count(_ddl_worker_00000, 201)
		// Truncate table has two table ID, should be handled differently.
		err = job.DecodeArgs(&diff.TableID)
		if err != nil {
			trace_util_0.Count(_ddl_worker_00000, 203)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_ddl_worker_00000, 202)
		diff.OldTableID = job.TableID
	} else {
		trace_util_0.Count(_ddl_worker_00000, 204)
		if job.Type == model.ActionRenameTable {
			trace_util_0.Count(_ddl_worker_00000, 205)
			err = job.DecodeArgs(&diff.OldSchemaID)
			if err != nil {
				trace_util_0.Count(_ddl_worker_00000, 207)
				return 0, errors.Trace(err)
			}
			trace_util_0.Count(_ddl_worker_00000, 206)
			diff.TableID = job.TableID
		} else {
			trace_util_0.Count(_ddl_worker_00000, 208)
			{
				diff.TableID = job.TableID
			}
		}
	}
	trace_util_0.Count(_ddl_worker_00000, 199)
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func isChanClosed(quitCh chan struct{}) bool {
	trace_util_0.Count(_ddl_worker_00000, 209)
	select {
	case <-quitCh:
		trace_util_0.Count(_ddl_worker_00000, 210)
		return true
	default:
		trace_util_0.Count(_ddl_worker_00000, 211)
		return false
	}
}

var _ddl_worker_00000 = "ddl/ddl_worker.go"
