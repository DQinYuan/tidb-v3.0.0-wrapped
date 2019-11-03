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

package ddl

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	insertDeleteRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`

	delBatchSize = 65536
	delBackLog   = 128
)

// enableEmulatorGC means whether to enable emulator GC. The default is enable.
// In some unit tests, we want to stop emulator GC, then wen can set enableEmulatorGC to 0.
var emulatorGCEnable = int32(1)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(job *model.Job) error
	// removeFromGCDeleteRange removes the deleting table job from gc_delete_range table by jobID and tableID.
	// It's use for recover the table that was mistakenly deleted.
	removeFromGCDeleteRange(jobID, tableID int64) error
	start()
	clear()
}

type delRange struct {
	store        kv.Storage
	sessPool     *sessionPool
	storeSupport bool
	emulatorCh   chan struct{}
	keys         []kv.Key
	quitCh       chan struct{}

	wait sync.WaitGroup // wait is only used when storeSupport is false.
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(store kv.Storage, sessPool *sessionPool) delRangeManager {
	trace_util_0.Count(_delete_range_00000, 0)
	dr := &delRange{
		store:        store,
		sessPool:     sessPool,
		storeSupport: store.SupportDeleteRange(),
		quitCh:       make(chan struct{}),
	}
	if !dr.storeSupport {
		trace_util_0.Count(_delete_range_00000, 2)
		dr.emulatorCh = make(chan struct{}, delBackLog)
		dr.keys = make([]kv.Key, 0, delBatchSize)
	}
	trace_util_0.Count(_delete_range_00000, 1)
	return dr
}

// addDelRangeJob implements delRangeManager interface.
func (dr *delRange) addDelRangeJob(job *model.Job) error {
	trace_util_0.Count(_delete_range_00000, 3)
	ctx, err := dr.sessPool.get()
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 7)
		return errors.Trace(err)
	}
	trace_util_0.Count(_delete_range_00000, 4)
	defer dr.sessPool.put(ctx)

	err = insertJobIntoDeleteRangeTable(ctx, job)
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 8)
		return errors.Trace(err)
	}
	trace_util_0.Count(_delete_range_00000, 5)
	if !dr.storeSupport {
		trace_util_0.Count(_delete_range_00000, 9)
		dr.emulatorCh <- struct{}{}
	}
	trace_util_0.Count(_delete_range_00000, 6)
	logutil.Logger(ddlLogCtx).Info("[ddl] add job into delete-range table", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()))
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *delRange) removeFromGCDeleteRange(jobID, tableID int64) error {
	trace_util_0.Count(_delete_range_00000, 10)
	ctx, err := dr.sessPool.get()
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 12)
		return errors.Trace(err)
	}
	trace_util_0.Count(_delete_range_00000, 11)
	defer dr.sessPool.put(ctx)
	err = util.RemoveFromGCDeleteRange(ctx, jobID, tableID)
	return errors.Trace(err)
}

// start implements delRangeManager interface.
func (dr *delRange) start() {
	trace_util_0.Count(_delete_range_00000, 13)
	if !dr.storeSupport {
		trace_util_0.Count(_delete_range_00000, 14)
		dr.wait.Add(1)
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	trace_util_0.Count(_delete_range_00000, 15)
	logutil.Logger(ddlLogCtx).Info("[ddl] closing delRange")
	close(dr.quitCh)
	dr.wait.Wait()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range table and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	trace_util_0.Count(_delete_range_00000, 16)
	defer dr.wait.Done()
	logutil.Logger(ddlLogCtx).Info("[ddl] start delRange emulator")
	for {
		trace_util_0.Count(_delete_range_00000, 17)
		select {
		case <-dr.emulatorCh:
			trace_util_0.Count(_delete_range_00000, 19)
		case <-dr.quitCh:
			trace_util_0.Count(_delete_range_00000, 20)
			return
		}
		trace_util_0.Count(_delete_range_00000, 18)
		if IsEmulatorGCEnable() {
			trace_util_0.Count(_delete_range_00000, 21)
			err := dr.doDelRangeWork()
			terror.Log(errors.Trace(err))
		}
	}
}

// EmulatorGCEnable enables emulator gc. It exports for testing.
func EmulatorGCEnable() {
	trace_util_0.Count(_delete_range_00000, 22)
	atomic.StoreInt32(&emulatorGCEnable, 1)
}

// EmulatorGCDisable disables emulator gc. It exports for testing.
func EmulatorGCDisable() {
	trace_util_0.Count(_delete_range_00000, 23)
	atomic.StoreInt32(&emulatorGCEnable, 0)
}

// IsEmulatorGCEnable indicates whether emulator GC enabled. It exports for testing.
func IsEmulatorGCEnable() bool {
	trace_util_0.Count(_delete_range_00000, 24)
	return atomic.LoadInt32(&emulatorGCEnable) == 1
}

func (dr *delRange) doDelRangeWork() error {
	trace_util_0.Count(_delete_range_00000, 25)
	ctx, err := dr.sessPool.get()
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 29)
		logutil.Logger(ddlLogCtx).Error("[ddl] delRange emulator get session failed", zap.Error(err))
		return errors.Trace(err)
	}
	trace_util_0.Count(_delete_range_00000, 26)
	defer dr.sessPool.put(ctx)

	ranges, err := util.LoadDeleteRanges(ctx, math.MaxInt64)
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 30)
		logutil.Logger(ddlLogCtx).Error("[ddl] delRange emulator load tasks failed", zap.Error(err))
		return errors.Trace(err)
	}

	trace_util_0.Count(_delete_range_00000, 27)
	for _, r := range ranges {
		trace_util_0.Count(_delete_range_00000, 31)
		if err := dr.doTask(ctx, r); err != nil {
			trace_util_0.Count(_delete_range_00000, 32)
			logutil.Logger(ddlLogCtx).Error("[ddl] delRange emulator do task failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_delete_range_00000, 28)
	return nil
}

func (dr *delRange) doTask(ctx sessionctx.Context, r util.DelRangeTask) error {
	trace_util_0.Count(_delete_range_00000, 33)
	var oldStartKey, newStartKey kv.Key
	oldStartKey = r.StartKey
	for {
		trace_util_0.Count(_delete_range_00000, 35)
		finish := true
		dr.keys = dr.keys[:0]
		err := kv.RunInNewTxn(dr.store, false, func(txn kv.Transaction) error {
			trace_util_0.Count(_delete_range_00000, 40)
			iter, err := txn.Iter(oldStartKey, r.EndKey)
			if err != nil {
				trace_util_0.Count(_delete_range_00000, 44)
				return errors.Trace(err)
			}
			trace_util_0.Count(_delete_range_00000, 41)
			defer iter.Close()

			for i := 0; i < delBatchSize; i++ {
				trace_util_0.Count(_delete_range_00000, 45)
				if !iter.Valid() {
					trace_util_0.Count(_delete_range_00000, 47)
					break
				}
				trace_util_0.Count(_delete_range_00000, 46)
				finish = false
				dr.keys = append(dr.keys, iter.Key().Clone())
				newStartKey = iter.Key().Next()

				if err := iter.Next(); err != nil {
					trace_util_0.Count(_delete_range_00000, 48)
					return errors.Trace(err)
				}
			}

			trace_util_0.Count(_delete_range_00000, 42)
			for _, key := range dr.keys {
				trace_util_0.Count(_delete_range_00000, 49)
				err := txn.Delete(key)
				if err != nil && !kv.ErrNotExist.Equal(err) {
					trace_util_0.Count(_delete_range_00000, 50)
					return errors.Trace(err)
				}
			}
			trace_util_0.Count(_delete_range_00000, 43)
			return nil
		})
		trace_util_0.Count(_delete_range_00000, 36)
		if err != nil {
			trace_util_0.Count(_delete_range_00000, 51)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 37)
		if finish {
			trace_util_0.Count(_delete_range_00000, 52)
			if err := util.CompleteDeleteRange(ctx, r); err != nil {
				trace_util_0.Count(_delete_range_00000, 54)
				logutil.Logger(ddlLogCtx).Error("[ddl] delRange emulator complete task failed", zap.Error(err))
				return errors.Trace(err)
			}
			trace_util_0.Count(_delete_range_00000, 53)
			logutil.Logger(ddlLogCtx).Info("[ddl] delRange emulator complete task", zap.Int64("jobID", r.JobID), zap.Int64("elementID", r.ElementID))
			break
		}
		trace_util_0.Count(_delete_range_00000, 38)
		if err := util.UpdateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
			trace_util_0.Count(_delete_range_00000, 55)
			logutil.Logger(ddlLogCtx).Error("[ddl] delRange emulator update task failed", zap.Error(err))
		}
		trace_util_0.Count(_delete_range_00000, 39)
		oldStartKey = newStartKey
	}
	trace_util_0.Count(_delete_range_00000, 34)
	return nil
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx sessionctx.Context, job *model.Job) error {
	trace_util_0.Count(_delete_range_00000, 56)
	now, err := getNowTSO(ctx)
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 59)
		return errors.Trace(err)
	}

	trace_util_0.Count(_delete_range_00000, 57)
	s := ctx.(sqlexec.SQLExecutor)
	switch job.Type {
	case model.ActionDropSchema:
		trace_util_0.Count(_delete_range_00000, 60)
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			trace_util_0.Count(_delete_range_00000, 71)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 61)
		for _, tableID := range tableIDs {
			trace_util_0.Count(_delete_range_00000, 72)
			startKey := tablecodec.EncodeTablePrefix(tableID)
			endKey := tablecodec.EncodeTablePrefix(tableID + 1)
			if err := doInsert(s, job.ID, tableID, startKey, endKey, now); err != nil {
				trace_util_0.Count(_delete_range_00000, 73)
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		trace_util_0.Count(_delete_range_00000, 62)
		tableID := job.TableID
		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		var startKey kv.Key
		var physicalTableIDs []int64
		if err := job.DecodeArgs(startKey, &physicalTableIDs); err != nil {
			trace_util_0.Count(_delete_range_00000, 74)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 63)
		if len(physicalTableIDs) > 0 {
			trace_util_0.Count(_delete_range_00000, 75)
			for _, pid := range physicalTableIDs {
				trace_util_0.Count(_delete_range_00000, 77)
				startKey = tablecodec.EncodeTablePrefix(pid)
				endKey := tablecodec.EncodeTablePrefix(pid + 1)
				if err := doInsert(s, job.ID, pid, startKey, endKey, now); err != nil {
					trace_util_0.Count(_delete_range_00000, 78)
					return errors.Trace(err)
				}
			}
			trace_util_0.Count(_delete_range_00000, 76)
			return nil
		}
		trace_util_0.Count(_delete_range_00000, 64)
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, now)
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		trace_util_0.Count(_delete_range_00000, 65)
		var physicalTableID int64
		if err := job.DecodeArgs(&physicalTableID); err != nil {
			trace_util_0.Count(_delete_range_00000, 79)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 66)
		startKey := tablecodec.EncodeTablePrefix(physicalTableID)
		endKey := tablecodec.EncodeTablePrefix(physicalTableID + 1)
		return doInsert(s, job.ID, physicalTableID, startKey, endKey, now)
	// ActionAddIndex needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex:
		trace_util_0.Count(_delete_range_00000, 67)
		tableID := job.TableID
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &partitionIDs); err != nil {
			trace_util_0.Count(_delete_range_00000, 80)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 68)
		if len(partitionIDs) > 0 {
			trace_util_0.Count(_delete_range_00000, 81)
			for _, pid := range partitionIDs {
				trace_util_0.Count(_delete_range_00000, 82)
				startKey := tablecodec.EncodeTableIndexPrefix(pid, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(pid, indexID+1)
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					trace_util_0.Count(_delete_range_00000, 83)
					return errors.Trace(err)
				}
			}
		} else {
			trace_util_0.Count(_delete_range_00000, 84)
			{
				startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
				return doInsert(s, job.ID, indexID, startKey, endKey, now)
			}
		}
	case model.ActionDropIndex:
		trace_util_0.Count(_delete_range_00000, 69)
		tableID := job.TableID
		var indexName interface{}
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &indexID, &partitionIDs); err != nil {
			trace_util_0.Count(_delete_range_00000, 85)
			return errors.Trace(err)
		}
		trace_util_0.Count(_delete_range_00000, 70)
		if len(partitionIDs) > 0 {
			trace_util_0.Count(_delete_range_00000, 86)
			for _, pid := range partitionIDs {
				trace_util_0.Count(_delete_range_00000, 87)
				startKey := tablecodec.EncodeTableIndexPrefix(pid, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(pid, indexID+1)
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					trace_util_0.Count(_delete_range_00000, 88)
					return errors.Trace(err)
				}
			}
		} else {
			trace_util_0.Count(_delete_range_00000, 89)
			{
				startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
				return doInsert(s, job.ID, indexID, startKey, endKey, now)
			}
		}
	}
	trace_util_0.Count(_delete_range_00000, 58)
	return nil
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts uint64) error {
	trace_util_0.Count(_delete_range_00000, 90)
	logutil.Logger(ddlLogCtx).Info("[ddl] insert into delete-range table", zap.Int64("jobID", jobID), zap.Int64("elementID", elementID))
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(context.Background(), sql)
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in TSO.
func getNowTSO(ctx sessionctx.Context) (uint64, error) {
	trace_util_0.Count(_delete_range_00000, 91)
	currVer, err := ctx.GetStore().CurrentVersion()
	if err != nil {
		trace_util_0.Count(_delete_range_00000, 93)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_delete_range_00000, 92)
	return currVer.Ver, nil
}

var _delete_range_00000 = "ddl/delete_range.go"
