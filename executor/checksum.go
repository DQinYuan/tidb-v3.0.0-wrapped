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

package executor

import (
	"context"
	"strconv"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ Executor = &ChecksumTableExec{}

// ChecksumTableExec represents ChecksumTable executor.
type ChecksumTableExec struct {
	baseExecutor

	tables map[int64]*checksumContext
	done   bool
}

// Open implements the Executor Open interface.
func (e *ChecksumTableExec) Open(ctx context.Context) error {
	trace_util_0.Count(_checksum_00000, 0)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_checksum_00000, 8)
		return err
	}

	trace_util_0.Count(_checksum_00000, 1)
	concurrency, err := getChecksumTableConcurrency(e.ctx)
	if err != nil {
		trace_util_0.Count(_checksum_00000, 9)
		return err
	}

	trace_util_0.Count(_checksum_00000, 2)
	tasks, err := e.buildTasks()
	if err != nil {
		trace_util_0.Count(_checksum_00000, 10)
		return err
	}

	trace_util_0.Count(_checksum_00000, 3)
	taskCh := make(chan *checksumTask, len(tasks))
	resultCh := make(chan *checksumResult, len(tasks))
	for i := 0; i < concurrency; i++ {
		trace_util_0.Count(_checksum_00000, 11)
		go e.checksumWorker(taskCh, resultCh)
	}

	trace_util_0.Count(_checksum_00000, 4)
	for _, task := range tasks {
		trace_util_0.Count(_checksum_00000, 12)
		taskCh <- task
	}
	trace_util_0.Count(_checksum_00000, 5)
	close(taskCh)

	for i := 0; i < len(tasks); i++ {
		trace_util_0.Count(_checksum_00000, 13)
		result := <-resultCh
		if result.Error != nil {
			trace_util_0.Count(_checksum_00000, 15)
			err = result.Error
			logutil.Logger(ctx).Error("checksum failed", zap.Error(err))
			continue
		}
		trace_util_0.Count(_checksum_00000, 14)
		e.handleResult(result)
	}
	trace_util_0.Count(_checksum_00000, 6)
	if err != nil {
		trace_util_0.Count(_checksum_00000, 16)
		return err
	}

	trace_util_0.Count(_checksum_00000, 7)
	return nil
}

// Next implements the Executor Next interface.
func (e *ChecksumTableExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_checksum_00000, 17)
	req.Reset()
	if e.done {
		trace_util_0.Count(_checksum_00000, 20)
		return nil
	}
	trace_util_0.Count(_checksum_00000, 18)
	for _, t := range e.tables {
		trace_util_0.Count(_checksum_00000, 21)
		req.AppendString(0, t.DBInfo.Name.O)
		req.AppendString(1, t.TableInfo.Name.O)
		req.AppendUint64(2, t.Response.Checksum)
		req.AppendUint64(3, t.Response.TotalKvs)
		req.AppendUint64(4, t.Response.TotalBytes)
	}
	trace_util_0.Count(_checksum_00000, 19)
	e.done = true
	return nil
}

func (e *ChecksumTableExec) buildTasks() ([]*checksumTask, error) {
	trace_util_0.Count(_checksum_00000, 22)
	var tasks []*checksumTask
	for id, t := range e.tables {
		trace_util_0.Count(_checksum_00000, 24)
		reqs, err := t.BuildRequests(e.ctx)
		if err != nil {
			trace_util_0.Count(_checksum_00000, 26)
			return nil, err
		}
		trace_util_0.Count(_checksum_00000, 25)
		for _, req := range reqs {
			trace_util_0.Count(_checksum_00000, 27)
			tasks = append(tasks, &checksumTask{id, req})
		}
	}
	trace_util_0.Count(_checksum_00000, 23)
	return tasks, nil
}

func (e *ChecksumTableExec) handleResult(result *checksumResult) {
	trace_util_0.Count(_checksum_00000, 28)
	table := e.tables[result.TableID]
	table.HandleResponse(result.Response)
}

func (e *ChecksumTableExec) checksumWorker(taskCh <-chan *checksumTask, resultCh chan<- *checksumResult) {
	trace_util_0.Count(_checksum_00000, 29)
	for task := range taskCh {
		trace_util_0.Count(_checksum_00000, 30)
		result := &checksumResult{TableID: task.TableID}
		result.Response, result.Error = e.handleChecksumRequest(task.Request)
		resultCh <- result
	}
}

func (e *ChecksumTableExec) handleChecksumRequest(req *kv.Request) (resp *tipb.ChecksumResponse, err error) {
	trace_util_0.Count(_checksum_00000, 31)
	ctx := context.TODO()
	res, err := distsql.Checksum(ctx, e.ctx.GetClient(), req, e.ctx.GetSessionVars().KVVars)
	if err != nil {
		trace_util_0.Count(_checksum_00000, 35)
		return nil, err
	}
	trace_util_0.Count(_checksum_00000, 32)
	res.Fetch(ctx)
	defer func() {
		trace_util_0.Count(_checksum_00000, 36)
		if err1 := res.Close(); err1 != nil {
			trace_util_0.Count(_checksum_00000, 37)
			err = err1
		}
	}()

	trace_util_0.Count(_checksum_00000, 33)
	resp = &tipb.ChecksumResponse{}

	for {
		trace_util_0.Count(_checksum_00000, 38)
		data, err := res.NextRaw(ctx)
		if err != nil {
			trace_util_0.Count(_checksum_00000, 42)
			return nil, err
		}
		trace_util_0.Count(_checksum_00000, 39)
		if data == nil {
			trace_util_0.Count(_checksum_00000, 43)
			break
		}
		trace_util_0.Count(_checksum_00000, 40)
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			trace_util_0.Count(_checksum_00000, 44)
			return nil, err
		}
		trace_util_0.Count(_checksum_00000, 41)
		updateChecksumResponse(resp, checksum)
	}

	trace_util_0.Count(_checksum_00000, 34)
	return resp, nil
}

type checksumTask struct {
	TableID int64
	Request *kv.Request
}

type checksumResult struct {
	Error    error
	TableID  int64
	Response *tipb.ChecksumResponse
}

type checksumContext struct {
	DBInfo    *model.DBInfo
	TableInfo *model.TableInfo
	StartTs   uint64
	Response  *tipb.ChecksumResponse
}

func newChecksumContext(db *model.DBInfo, table *model.TableInfo, startTs uint64) *checksumContext {
	trace_util_0.Count(_checksum_00000, 45)
	return &checksumContext{
		DBInfo:    db,
		TableInfo: table,
		StartTs:   startTs,
		Response:  &tipb.ChecksumResponse{},
	}
}

func (c *checksumContext) BuildRequests(ctx sessionctx.Context) ([]*kv.Request, error) {
	trace_util_0.Count(_checksum_00000, 46)
	reqs := make([]*kv.Request, 0, len(c.TableInfo.Indices)+1)
	req, err := c.buildTableRequest(ctx)
	if err != nil {
		trace_util_0.Count(_checksum_00000, 49)
		return nil, err
	}
	trace_util_0.Count(_checksum_00000, 47)
	reqs = append(reqs, req)
	for _, indexInfo := range c.TableInfo.Indices {
		trace_util_0.Count(_checksum_00000, 50)
		if indexInfo.State != model.StatePublic {
			trace_util_0.Count(_checksum_00000, 53)
			continue
		}
		trace_util_0.Count(_checksum_00000, 51)
		req, err = c.buildIndexRequest(ctx, indexInfo)
		if err != nil {
			trace_util_0.Count(_checksum_00000, 54)
			return nil, err
		}
		trace_util_0.Count(_checksum_00000, 52)
		reqs = append(reqs, req)
	}

	trace_util_0.Count(_checksum_00000, 48)
	return reqs, nil
}

func (c *checksumContext) buildTableRequest(ctx sessionctx.Context) (*kv.Request, error) {
	trace_util_0.Count(_checksum_00000, 55)
	checksum := &tipb.ChecksumRequest{
		StartTs:   c.StartTs,
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullIntRange(false)

	var builder distsql.RequestBuilder
	return builder.SetTableRanges(c.TableInfo.ID, ranges, nil).
		SetChecksumRequest(checksum).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency).
		Build()
}

func (c *checksumContext) buildIndexRequest(ctx sessionctx.Context, indexInfo *model.IndexInfo) (*kv.Request, error) {
	trace_util_0.Count(_checksum_00000, 56)
	checksum := &tipb.ChecksumRequest{
		StartTs:   c.StartTs,
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(ctx.GetSessionVars().StmtCtx, c.TableInfo.ID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency).
		Build()
}

func (c *checksumContext) HandleResponse(update *tipb.ChecksumResponse) {
	trace_util_0.Count(_checksum_00000, 57)
	updateChecksumResponse(c.Response, update)
}

func getChecksumTableConcurrency(ctx sessionctx.Context) (int, error) {
	trace_util_0.Count(_checksum_00000, 58)
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionSystemVar(sessionVars, variable.TiDBChecksumTableConcurrency)
	if err != nil {
		trace_util_0.Count(_checksum_00000, 60)
		return 0, err
	}
	trace_util_0.Count(_checksum_00000, 59)
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	trace_util_0.Count(_checksum_00000, 61)
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}

var _checksum_00000 = "executor/checksum.go"
