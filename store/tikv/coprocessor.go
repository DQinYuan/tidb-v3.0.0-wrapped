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

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var tikvTxnRegionsNumHistogramWithCoprocessor = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("coprocessor")

// CopClient is coprocessor client.
type CopClient struct {
	store *tikvStore
}

// IsRequestTypeSupported checks whether reqType is supported.
func (c *CopClient) IsRequestTypeSupported(reqType, subType int64) bool {
	trace_util_0.Count(_coprocessor_00000, 0)
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		trace_util_0.Count(_coprocessor_00000, 2)
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			trace_util_0.Count(_coprocessor_00000, 5)
			return true
		default:
			trace_util_0.Count(_coprocessor_00000, 6)
			return c.supportExpr(tipb.ExprType(subType))
		}
	case kv.ReqTypeDAG:
		trace_util_0.Count(_coprocessor_00000, 3)
		return c.supportExpr(tipb.ExprType(subType))
	case kv.ReqTypeAnalyze:
		trace_util_0.Count(_coprocessor_00000, 4)
		return true
	}
	trace_util_0.Count(_coprocessor_00000, 1)
	return false
}

func (c *CopClient) supportExpr(exprType tipb.ExprType) bool {
	trace_util_0.Count(_coprocessor_00000, 7)
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef:
		trace_util_0.Count(_coprocessor_00000, 8)
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr:
		trace_util_0.Count(_coprocessor_00000, 9)
		return true
	case kv.ReqSubTypeDesc:
		trace_util_0.Count(_coprocessor_00000, 10)
		return true
	case kv.ReqSubTypeSignature:
		trace_util_0.Count(_coprocessor_00000, 11)
		return true
	default:
		trace_util_0.Count(_coprocessor_00000, 12)
		return false
	}
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request, vars *kv.Variables) kv.Response {
	trace_util_0.Count(_coprocessor_00000, 13)
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackoffer(ctx, copBuildTaskMaxBackoff).WithVars(vars)
	tasks, err := buildCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, req.Desc, req.Streaming)
	if err != nil {
		trace_util_0.Count(_coprocessor_00000, 18)
		return copErrorResponse{err}
	}
	trace_util_0.Count(_coprocessor_00000, 14)
	it := &copIterator{
		store:       c.store,
		req:         req,
		concurrency: req.Concurrency,
		finishCh:    make(chan struct{}),
		vars:        vars,
		memTracker:  req.MemTracker,
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		trace_util_0.Count(_coprocessor_00000, 19)
		it.concurrency = len(tasks)
	}
	trace_util_0.Count(_coprocessor_00000, 15)
	if it.concurrency < 1 {
		trace_util_0.Count(_coprocessor_00000, 20)
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}
	trace_util_0.Count(_coprocessor_00000, 16)
	if !it.req.KeepOrder {
		trace_util_0.Count(_coprocessor_00000, 21)
		it.respChan = make(chan *copResponse, it.concurrency)
	}
	trace_util_0.Count(_coprocessor_00000, 17)
	it.open(ctx)
	return it
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region RegionVerID
	ranges *copRanges

	respChan  chan *copResponse
	storeAddr string
	cmdType   tikvrpc.CmdType
}

func (r *copTask) String() string {
	trace_util_0.Count(_coprocessor_00000, 22)
	return fmt.Sprintf("region(%d %d %d) ranges(%d) store(%s)",
		r.region.id, r.region.confVer, r.region.ver, r.ranges.len(), r.storeAddr)
}

// copRanges is like []kv.KeyRange, but may has extra elements at head/tail.
// It's for avoiding alloc big slice during build copTask.
type copRanges struct {
	first *kv.KeyRange
	mid   []kv.KeyRange
	last  *kv.KeyRange
}

func (r *copRanges) String() string {
	trace_util_0.Count(_coprocessor_00000, 23)
	var s string
	r.do(func(ran *kv.KeyRange) {
		trace_util_0.Count(_coprocessor_00000, 25)
		s += fmt.Sprintf("[%q, %q]", ran.StartKey, ran.EndKey)
	})
	trace_util_0.Count(_coprocessor_00000, 24)
	return s
}

func (r *copRanges) len() int {
	trace_util_0.Count(_coprocessor_00000, 26)
	var l int
	if r.first != nil {
		trace_util_0.Count(_coprocessor_00000, 29)
		l++
	}
	trace_util_0.Count(_coprocessor_00000, 27)
	l += len(r.mid)
	if r.last != nil {
		trace_util_0.Count(_coprocessor_00000, 30)
		l++
	}
	trace_util_0.Count(_coprocessor_00000, 28)
	return l
}

func (r *copRanges) at(i int) kv.KeyRange {
	trace_util_0.Count(_coprocessor_00000, 31)
	if r.first != nil {
		trace_util_0.Count(_coprocessor_00000, 34)
		if i == 0 {
			trace_util_0.Count(_coprocessor_00000, 36)
			return *r.first
		}
		trace_util_0.Count(_coprocessor_00000, 35)
		i--
	}
	trace_util_0.Count(_coprocessor_00000, 32)
	if i < len(r.mid) {
		trace_util_0.Count(_coprocessor_00000, 37)
		return r.mid[i]
	}
	trace_util_0.Count(_coprocessor_00000, 33)
	return *r.last
}

func (r *copRanges) slice(from, to int) *copRanges {
	trace_util_0.Count(_coprocessor_00000, 38)
	var ran copRanges
	if r.first != nil {
		trace_util_0.Count(_coprocessor_00000, 41)
		if from == 0 && to > 0 {
			trace_util_0.Count(_coprocessor_00000, 44)
			ran.first = r.first
		}
		trace_util_0.Count(_coprocessor_00000, 42)
		if from > 0 {
			trace_util_0.Count(_coprocessor_00000, 45)
			from--
		}
		trace_util_0.Count(_coprocessor_00000, 43)
		if to > 0 {
			trace_util_0.Count(_coprocessor_00000, 46)
			to--
		}
	}
	trace_util_0.Count(_coprocessor_00000, 39)
	if to <= len(r.mid) {
		trace_util_0.Count(_coprocessor_00000, 47)
		ran.mid = r.mid[from:to]
	} else {
		trace_util_0.Count(_coprocessor_00000, 48)
		{
			if from <= len(r.mid) {
				trace_util_0.Count(_coprocessor_00000, 50)
				ran.mid = r.mid[from:]
			}
			trace_util_0.Count(_coprocessor_00000, 49)
			if from < to {
				trace_util_0.Count(_coprocessor_00000, 51)
				ran.last = r.last
			}
		}
	}
	trace_util_0.Count(_coprocessor_00000, 40)
	return &ran
}

func (r *copRanges) do(f func(ran *kv.KeyRange)) {
	trace_util_0.Count(_coprocessor_00000, 52)
	if r.first != nil {
		trace_util_0.Count(_coprocessor_00000, 55)
		f(r.first)
	}
	trace_util_0.Count(_coprocessor_00000, 53)
	for _, ran := range r.mid {
		trace_util_0.Count(_coprocessor_00000, 56)
		f(&ran)
	}
	trace_util_0.Count(_coprocessor_00000, 54)
	if r.last != nil {
		trace_util_0.Count(_coprocessor_00000, 57)
		f(r.last)
	}
}

func (r *copRanges) toPBRanges() []*coprocessor.KeyRange {
	trace_util_0.Count(_coprocessor_00000, 58)
	ranges := make([]*coprocessor.KeyRange, 0, r.len())
	r.do(func(ran *kv.KeyRange) {
		trace_util_0.Count(_coprocessor_00000, 60)
		ranges = append(ranges, &coprocessor.KeyRange{
			Start: ran.StartKey,
			End:   ran.EndKey,
		})
	})
	trace_util_0.Count(_coprocessor_00000, 59)
	return ranges
}

// split ranges into (left, right) by key.
func (r *copRanges) split(key []byte) (*copRanges, *copRanges) {
	trace_util_0.Count(_coprocessor_00000, 61)
	n := sort.Search(r.len(), func(i int) bool {
		trace_util_0.Count(_coprocessor_00000, 64)
		cur := r.at(i)
		return len(cur.EndKey) == 0 || bytes.Compare(cur.EndKey, key) > 0
	})
	// If a range p contains the key, it will split to 2 parts.
	trace_util_0.Count(_coprocessor_00000, 62)
	if n < r.len() {
		trace_util_0.Count(_coprocessor_00000, 65)
		p := r.at(n)
		if bytes.Compare(key, p.StartKey) > 0 {
			trace_util_0.Count(_coprocessor_00000, 66)
			left := r.slice(0, n)
			left.last = &kv.KeyRange{StartKey: p.StartKey, EndKey: key}
			right := r.slice(n+1, r.len())
			right.first = &kv.KeyRange{StartKey: key, EndKey: p.EndKey}
			return left, right
		}
	}
	trace_util_0.Count(_coprocessor_00000, 63)
	return r.slice(0, n), r.slice(n, r.len())
}

// rangesPerTask limits the length of the ranges slice sent in one copTask.
const rangesPerTask = 25000

func buildCopTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, desc bool, streaming bool) ([]*copTask, error) {
	trace_util_0.Count(_coprocessor_00000, 67)
	start := time.Now()
	rangesLen := ranges.len()
	cmdType := tikvrpc.CmdCop
	if streaming {
		trace_util_0.Count(_coprocessor_00000, 73)
		cmdType = tikvrpc.CmdCopStream
	}

	trace_util_0.Count(_coprocessor_00000, 68)
	var tasks []*copTask
	appendTask := func(region RegionVerID, ranges *copRanges) {
		trace_util_0.Count(_coprocessor_00000, 74)
		// TiKV will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := ranges.len()
		for i := 0; i < rLen; {
			trace_util_0.Count(_coprocessor_00000, 75)
			nextI := mathutil.Min(i+rangesPerTask, rLen)
			tasks = append(tasks, &copTask{
				region:   region,
				ranges:   ranges.slice(i, nextI),
				respChan: make(chan *copResponse, 1),
				cmdType:  cmdType,
			})
			i = nextI
		}
	}

	trace_util_0.Count(_coprocessor_00000, 69)
	err := splitRanges(bo, cache, ranges, appendTask)
	if err != nil {
		trace_util_0.Count(_coprocessor_00000, 76)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_coprocessor_00000, 70)
	if desc {
		trace_util_0.Count(_coprocessor_00000, 77)
		reverseTasks(tasks)
	}
	trace_util_0.Count(_coprocessor_00000, 71)
	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		trace_util_0.Count(_coprocessor_00000, 78)
		logutil.Logger(context.Background()).Warn("buildCopTasks takes too much time",
			zap.Duration("elapsed", elapsed),
			zap.Int("range len", rangesLen),
			zap.Int("task len", len(tasks)))
	}
	trace_util_0.Count(_coprocessor_00000, 72)
	tikvTxnRegionsNumHistogramWithCoprocessor.Observe(float64(len(tasks)))
	return tasks, nil
}

func splitRanges(bo *Backoffer, cache *RegionCache, ranges *copRanges, fn func(region RegionVerID, ranges *copRanges)) error {
	trace_util_0.Count(_coprocessor_00000, 79)
	for ranges.len() > 0 {
		trace_util_0.Count(_coprocessor_00000, 81)
		loc, err := cache.LocateKey(bo, ranges.at(0).StartKey)
		if err != nil {
			trace_util_0.Count(_coprocessor_00000, 85)
			return errors.Trace(err)
		}

		// Iterate to the first range that is not complete in the region.
		trace_util_0.Count(_coprocessor_00000, 82)
		var i int
		for ; i < ranges.len(); i++ {
			trace_util_0.Count(_coprocessor_00000, 86)
			r := ranges.at(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				trace_util_0.Count(_coprocessor_00000, 87)
				break
			}
		}
		// All rest ranges belong to the same region.
		trace_util_0.Count(_coprocessor_00000, 83)
		if i == ranges.len() {
			trace_util_0.Count(_coprocessor_00000, 88)
			fn(loc.Region, ranges)
			break
		}

		trace_util_0.Count(_coprocessor_00000, 84)
		r := ranges.at(i)
		if loc.Contains(r.StartKey) {
			trace_util_0.Count(_coprocessor_00000, 89)
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			fn(loc.Region, taskRanges)

			ranges = ranges.slice(i+1, ranges.len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			trace_util_0.Count(_coprocessor_00000, 90)
			{
				// rs[i] is not in the region.
				taskRanges := ranges.slice(0, i)
				fn(loc.Region, taskRanges)
				ranges = ranges.slice(i, ranges.len())
			}
		}
	}

	trace_util_0.Count(_coprocessor_00000, 80)
	return nil
}

// SplitRegionRanges get the split ranges from pd region.
func SplitRegionRanges(bo *Backoffer, cache *RegionCache, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	trace_util_0.Count(_coprocessor_00000, 91)
	ranges := copRanges{mid: keyRanges}

	var ret []kv.KeyRange
	appendRange := func(region RegionVerID, ranges *copRanges) {
		trace_util_0.Count(_coprocessor_00000, 94)
		for i := 0; i < ranges.len(); i++ {
			trace_util_0.Count(_coprocessor_00000, 95)
			ret = append(ret, ranges.at(i))
		}
	}

	trace_util_0.Count(_coprocessor_00000, 92)
	err := splitRanges(bo, cache, &ranges, appendRange)
	if err != nil {
		trace_util_0.Count(_coprocessor_00000, 96)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_coprocessor_00000, 93)
	return ret, nil
}

func reverseTasks(tasks []*copTask) {
	trace_util_0.Count(_coprocessor_00000, 97)
	for i := 0; i < len(tasks)/2; i++ {
		trace_util_0.Count(_coprocessor_00000, 98)
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type copIterator struct {
	store       *tikvStore
	req         *kv.Request
	concurrency int
	finishCh    chan struct{}
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*copTask
	curr  int

	// Otherwise, results are stored in respChan.
	respChan chan *copResponse
	wg       sync.WaitGroup

	vars *kv.Variables

	memTracker *memory.Tracker
}

// copIteratorWorker receives tasks from copIteratorTaskSender, handles tasks and sends the copResponse to respChan.
type copIteratorWorker struct {
	taskCh   <-chan *copTask
	wg       *sync.WaitGroup
	store    *tikvStore
	req      *kv.Request
	respChan chan<- *copResponse
	finishCh <-chan struct{}
	vars     *kv.Variables

	memTracker *memory.Tracker
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh   chan<- *copTask
	wg       *sync.WaitGroup
	tasks    []*copTask
	finishCh <-chan struct{}
	respChan chan<- *copResponse
}

type copResponse struct {
	pbResp   *coprocessor.Response
	detail   *execdetails.ExecDetails
	startKey kv.Key
	err      error
	respSize int64
}

const (
	sizeofExecDetails   = int(unsafe.Sizeof(execdetails.ExecDetails{}))
	sizeofCommitDetails = int(unsafe.Sizeof(execdetails.CommitDetails{}))
)

// GetData implements the kv.ResultSubset GetData interface.
func (rs *copResponse) GetData() []byte {
	trace_util_0.Count(_coprocessor_00000, 99)
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *copResponse) GetStartKey() kv.Key {
	trace_util_0.Count(_coprocessor_00000, 100)
	return rs.startKey
}

func (rs *copResponse) GetExecDetails() *execdetails.ExecDetails {
	trace_util_0.Count(_coprocessor_00000, 101)
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *copResponse) MemSize() int64 {
	trace_util_0.Count(_coprocessor_00000, 102)
	if rs.respSize != 0 {
		trace_util_0.Count(_coprocessor_00000, 106)
		return rs.respSize
	}

	// ignore rs.err
	trace_util_0.Count(_coprocessor_00000, 103)
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		trace_util_0.Count(_coprocessor_00000, 107)
		rs.respSize += int64(sizeofExecDetails)
		if rs.detail.CommitDetail != nil {
			trace_util_0.Count(_coprocessor_00000, 108)
			rs.respSize += int64(sizeofCommitDetails)
		}
	}
	trace_util_0.Count(_coprocessor_00000, 104)
	if rs.pbResp != nil {
		trace_util_0.Count(_coprocessor_00000, 109)
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	trace_util_0.Count(_coprocessor_00000, 105)
	return rs.respSize
}

const minLogCopTaskTime = 300 * time.Millisecond

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	trace_util_0.Count(_coprocessor_00000, 110)
	defer worker.wg.Done()
	for task := range worker.taskCh {
		trace_util_0.Count(_coprocessor_00000, 111)
		respCh := worker.respChan
		if respCh == nil {
			trace_util_0.Count(_coprocessor_00000, 114)
			respCh = task.respChan
		}

		trace_util_0.Count(_coprocessor_00000, 112)
		bo := NewBackoffer(ctx, copNextMaxBackoff).WithVars(worker.vars)
		worker.handleTask(bo, task, respCh)
		if bo.totalSleep > 0 {
			trace_util_0.Count(_coprocessor_00000, 115)
			metrics.TiKVBackoffHistogram.Observe(float64(bo.totalSleep) / 1000)
		}
		trace_util_0.Count(_coprocessor_00000, 113)
		close(task.respChan)
		select {
		case <-worker.finishCh:
			trace_util_0.Count(_coprocessor_00000, 116)
			return
		default:
			trace_util_0.Count(_coprocessor_00000, 117)
		}
	}
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context) {
	trace_util_0.Count(_coprocessor_00000, 118)
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency)
	// Start it.concurrency number of workers to handle cop requests.
	for i := 0; i < it.concurrency; i++ {
		trace_util_0.Count(_coprocessor_00000, 120)
		worker := &copIteratorWorker{
			taskCh:   taskCh,
			wg:       &it.wg,
			store:    it.store,
			req:      it.req,
			respChan: it.respChan,
			finishCh: it.finishCh,
			vars:     it.vars,

			memTracker: it.memTracker,
		}
		go worker.run(ctx)
	}
	trace_util_0.Count(_coprocessor_00000, 119)
	taskSender := &copIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
	}
	taskSender.respChan = it.respChan
	go taskSender.run()
}

func (sender *copIteratorTaskSender) run() {
	trace_util_0.Count(_coprocessor_00000, 121)
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		trace_util_0.Count(_coprocessor_00000, 123)
		exit := sender.sendToTaskCh(t)
		if exit {
			trace_util_0.Count(_coprocessor_00000, 124)
			break
		}
	}
	trace_util_0.Count(_coprocessor_00000, 122)
	close(sender.taskCh)

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		trace_util_0.Count(_coprocessor_00000, 125)
		close(sender.respChan)
	}
}

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan *copResponse) (resp *copResponse, ok bool, exit bool) {
	trace_util_0.Count(_coprocessor_00000, 126)
	select {
	case resp, ok = <-respCh:
		trace_util_0.Count(_coprocessor_00000, 128)
		if it.memTracker != nil && resp != nil {
			trace_util_0.Count(_coprocessor_00000, 132)
			it.memTracker.Consume(-int64(resp.MemSize()))
		}
	case <-it.finishCh:
		trace_util_0.Count(_coprocessor_00000, 129)
		exit = true
	case <-ctx.Done():
		trace_util_0.Count(_coprocessor_00000, 130)
		// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
		if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
			trace_util_0.Count(_coprocessor_00000, 133)
			close(it.finishCh)
		}
		trace_util_0.Count(_coprocessor_00000, 131)
		exit = true
	}
	trace_util_0.Count(_coprocessor_00000, 127)
	return
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask) (exit bool) {
	trace_util_0.Count(_coprocessor_00000, 134)
	select {
	case sender.taskCh <- t:
		trace_util_0.Count(_coprocessor_00000, 136)
	case <-sender.finishCh:
		trace_util_0.Count(_coprocessor_00000, 137)
		exit = true
	}
	trace_util_0.Count(_coprocessor_00000, 135)
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp *copResponse, respCh chan<- *copResponse, checkOOM bool) (exit bool) {
	trace_util_0.Count(_coprocessor_00000, 138)
	if worker.memTracker != nil && checkOOM {
		trace_util_0.Count(_coprocessor_00000, 141)
		worker.memTracker.Consume(int64(resp.MemSize()))
	}
	trace_util_0.Count(_coprocessor_00000, 139)
	select {
	case respCh <- resp:
		trace_util_0.Count(_coprocessor_00000, 142)
	case <-worker.finishCh:
		trace_util_0.Count(_coprocessor_00000, 143)
		exit = true
	}
	trace_util_0.Count(_coprocessor_00000, 140)
	return
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	trace_util_0.Count(_coprocessor_00000, 144)
	var (
		resp   *copResponse
		ok     bool
		closed bool
	)
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.
	if it.respChan != nil {
		trace_util_0.Count(_coprocessor_00000, 148)
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			trace_util_0.Count(_coprocessor_00000, 149)
			return nil, nil
		}
	} else {
		trace_util_0.Count(_coprocessor_00000, 150)
		{
			for {
				trace_util_0.Count(_coprocessor_00000, 151)
				if it.curr >= len(it.tasks) {
					trace_util_0.Count(_coprocessor_00000, 155)
					// Resp will be nil if iterator is finishCh.
					return nil, nil
				}
				trace_util_0.Count(_coprocessor_00000, 152)
				task := it.tasks[it.curr]
				resp, ok, closed = it.recvFromRespCh(ctx, task.respChan)
				if closed {
					trace_util_0.Count(_coprocessor_00000, 156)
					// Close() is already called, so Next() is invalid.
					return nil, nil
				}
				trace_util_0.Count(_coprocessor_00000, 153)
				if ok {
					trace_util_0.Count(_coprocessor_00000, 157)
					break
				}
				// Switch to next task.
				trace_util_0.Count(_coprocessor_00000, 154)
				it.tasks[it.curr] = nil
				it.curr++
			}
		}
	}

	trace_util_0.Count(_coprocessor_00000, 145)
	if resp.err != nil {
		trace_util_0.Count(_coprocessor_00000, 158)
		return nil, errors.Trace(resp.err)
	}

	trace_util_0.Count(_coprocessor_00000, 146)
	err := it.store.CheckVisibility(it.req.StartTs)
	if err != nil {
		trace_util_0.Count(_coprocessor_00000, 159)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_coprocessor_00000, 147)
	return resp, nil
}

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(bo *Backoffer, task *copTask, respCh chan<- *copResponse) {
	trace_util_0.Count(_coprocessor_00000, 160)
	defer func() {
		trace_util_0.Count(_coprocessor_00000, 162)
		r := recover()
		if r != nil {
			trace_util_0.Count(_coprocessor_00000, 163)
			logutil.Logger(context.Background()).Error("copIteratorWork meet panic",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
			resp := &copResponse{err: errors.Errorf("%v", r)}
			// if panic has happened, set checkOOM to false to avoid another panic.
			worker.sendToRespCh(resp, task.respChan, false)
		}
	}()
	trace_util_0.Count(_coprocessor_00000, 161)
	remainTasks := []*copTask{task}
	for len(remainTasks) > 0 {
		trace_util_0.Count(_coprocessor_00000, 164)
		tasks, err := worker.handleTaskOnce(bo, remainTasks[0], respCh)
		if err != nil {
			trace_util_0.Count(_coprocessor_00000, 166)
			resp := &copResponse{err: errors.Trace(err)}
			worker.sendToRespCh(resp, respCh, true)
			return
		}
		trace_util_0.Count(_coprocessor_00000, 165)
		if len(tasks) > 0 {
			trace_util_0.Count(_coprocessor_00000, 167)
			remainTasks = append(tasks, remainTasks[1:]...)
		} else {
			trace_util_0.Count(_coprocessor_00000, 168)
			{
				remainTasks = remainTasks[1:]
			}
		}
	}
}

// handleTaskOnce handles single copTask, successful results are send to channel.
// If error happened, returns error. If region split or meet lock, returns the remain tasks.
func (worker *copIteratorWorker) handleTaskOnce(bo *Backoffer, task *copTask, ch chan<- *copResponse) ([]*copTask, error) {
	trace_util_0.Count(_coprocessor_00000, 169)
	failpoint.Inject("handleTaskOnceError", func(val failpoint.Value) {
		trace_util_0.Count(_coprocessor_00000, 174)
		if val.(bool) {
			trace_util_0.Count(_coprocessor_00000, 175)
			failpoint.Return(nil, errors.New("mock handleTaskOnce error"))
		}
	})

	trace_util_0.Count(_coprocessor_00000, 170)
	sender := NewRegionRequestSender(worker.store.regionCache, worker.store.client)
	req := &tikvrpc.Request{
		Type: task.cmdType,
		Cop: &coprocessor.Request{
			Tp:     worker.req.Tp,
			Data:   worker.req.Data,
			Ranges: task.ranges.toPBRanges(),
		},
		Context: kvrpcpb.Context{
			IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
			Priority:       kvPriorityToCommandPri(worker.req.Priority),
			NotFillCache:   worker.req.NotFillCache,
			HandleTime:     true,
			ScanDetail:     true,
		},
	}
	startTime := time.Now()
	resp, rpcCtx, err := sender.SendReqCtx(bo, req, task.region, ReadTimeoutMedium)
	if err != nil {
		trace_util_0.Count(_coprocessor_00000, 176)
		return nil, errors.Trace(err)
	}
	// Set task.storeAddr field so its task.String() method have the store address information.
	trace_util_0.Count(_coprocessor_00000, 171)
	task.storeAddr = sender.storeAddr
	costTime := time.Since(startTime)
	if costTime > minLogCopTaskTime {
		trace_util_0.Count(_coprocessor_00000, 177)
		worker.logTimeCopTask(costTime, task, bo, resp)
	}
	trace_util_0.Count(_coprocessor_00000, 172)
	metrics.TiKVCoprocessorHistogram.Observe(costTime.Seconds())

	if task.cmdType == tikvrpc.CmdCopStream {
		trace_util_0.Count(_coprocessor_00000, 178)
		return worker.handleCopStreamResult(bo, rpcCtx, resp.CopStream, task, ch)
	}

	// Handles the response for non-streaming copTask.
	trace_util_0.Count(_coprocessor_00000, 173)
	return worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: resp.Cop}, task, ch, nil)
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
	minLogKVWaitTime    = 200
)

func (worker *copIteratorWorker) logTimeCopTask(costTime time.Duration, task *copTask, bo *Backoffer, resp *tikvrpc.Response) {
	trace_util_0.Count(_coprocessor_00000, 179)
	logStr := fmt.Sprintf("[TIME_COP_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.id, task.storeAddr)
	if bo.totalSleep > minLogBackoffTime {
		trace_util_0.Count(_coprocessor_00000, 183)
		backoffTypes := strings.Replace(fmt.Sprintf("%v", bo.types), " ", ",", -1)
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.totalSleep, backoffTypes)
	}
	trace_util_0.Count(_coprocessor_00000, 180)
	var detail *kvrpcpb.ExecDetails
	if resp.Cop != nil {
		trace_util_0.Count(_coprocessor_00000, 184)
		detail = resp.Cop.ExecDetails
	} else {
		trace_util_0.Count(_coprocessor_00000, 185)
		if resp.CopStream != nil && resp.CopStream.Response != nil {
			trace_util_0.Count(_coprocessor_00000, 186)
			// streaming request returns io.EOF, so the first resp.CopStream.Response maybe nil.
			detail = resp.CopStream.ExecDetails
		}
	}

	trace_util_0.Count(_coprocessor_00000, 181)
	if detail != nil && detail.HandleTime != nil {
		trace_util_0.Count(_coprocessor_00000, 187)
		processMs := detail.HandleTime.ProcessMs
		waitMs := detail.HandleTime.WaitMs
		if processMs > minLogKVProcessTime {
			trace_util_0.Count(_coprocessor_00000, 189)
			logStr += fmt.Sprintf(" kv_process_ms:%d", processMs)
			if detail.ScanDetail != nil {
				trace_util_0.Count(_coprocessor_00000, 190)
				logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
				logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
				logStr = appendScanDetail(logStr, "lock", detail.ScanDetail.Lock)
			}
		}
		trace_util_0.Count(_coprocessor_00000, 188)
		if waitMs > minLogKVWaitTime {
			trace_util_0.Count(_coprocessor_00000, 191)
			logStr += fmt.Sprintf(" kv_wait_ms:%d", waitMs)
			if processMs <= minLogKVProcessTime {
				trace_util_0.Count(_coprocessor_00000, 192)
				logStr = strings.Replace(logStr, "TIME_COP_PROCESS", "TIME_COP_WAIT", 1)
			}
		}
	}
	trace_util_0.Count(_coprocessor_00000, 182)
	logutil.Logger(context.Background()).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	trace_util_0.Count(_coprocessor_00000, 193)
	if scanInfo != nil {
		trace_util_0.Count(_coprocessor_00000, 195)
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	trace_util_0.Count(_coprocessor_00000, 194)
	return logStr
}

func (worker *copIteratorWorker) handleCopStreamResult(bo *Backoffer, rpcCtx *RPCContext, stream *tikvrpc.CopStreamResponse, task *copTask, ch chan<- *copResponse) ([]*copTask, error) {
	trace_util_0.Count(_coprocessor_00000, 196)
	defer stream.Close()
	var resp *coprocessor.Response
	var lastRange *coprocessor.KeyRange
	resp = stream.Response
	if resp == nil {
		trace_util_0.Count(_coprocessor_00000, 198)
		// streaming request returns io.EOF, so the first Response is nil.
		return nil, nil
	}
	trace_util_0.Count(_coprocessor_00000, 197)
	for {
		trace_util_0.Count(_coprocessor_00000, 199)
		remainedTasks, err := worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: resp}, task, ch, lastRange)
		if err != nil || len(remainedTasks) != 0 {
			trace_util_0.Count(_coprocessor_00000, 202)
			return remainedTasks, errors.Trace(err)
		}
		trace_util_0.Count(_coprocessor_00000, 200)
		resp, err = stream.Recv()
		if err != nil {
			trace_util_0.Count(_coprocessor_00000, 203)
			if errors.Cause(err) == io.EOF {
				trace_util_0.Count(_coprocessor_00000, 206)
				return nil, nil
			}

			trace_util_0.Count(_coprocessor_00000, 204)
			if err1 := bo.Backoff(boTiKVRPC, errors.Errorf("recv stream response error: %v, task: %s", err, task)); err1 != nil {
				trace_util_0.Count(_coprocessor_00000, 207)
				return nil, errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			trace_util_0.Count(_coprocessor_00000, 205)
			logutil.Logger(context.Background()).Info("stream recv timeout", zap.Error(err))
			return worker.buildCopTasksFromRemain(bo, lastRange, task)
		}
		trace_util_0.Count(_coprocessor_00000, 201)
		lastRange = resp.Range
	}
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling streaming coprocessor response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, rpcCtx *RPCContext, resp *copResponse, task *copTask, ch chan<- *copResponse, lastRange *coprocessor.KeyRange) ([]*copTask, error) {
	trace_util_0.Count(_coprocessor_00000, 208)
	if regionErr := resp.pbResp.GetRegionError(); regionErr != nil {
		trace_util_0.Count(_coprocessor_00000, 216)
		if err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String())); err != nil {
			trace_util_0.Count(_coprocessor_00000, 218)
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		trace_util_0.Count(_coprocessor_00000, 217)
		return buildCopTasks(bo, worker.store.regionCache, task.ranges, worker.req.Desc, worker.req.Streaming)
	}
	trace_util_0.Count(_coprocessor_00000, 209)
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		trace_util_0.Count(_coprocessor_00000, 219)
		logutil.Logger(context.Background()).Debug("coprocessor encounters",
			zap.Stringer("lock", lockErr))
		msBeforeExpired, err1 := worker.store.lockResolver.ResolveLocks(bo, []*Lock{NewLock(lockErr)})
		if err1 != nil {
			trace_util_0.Count(_coprocessor_00000, 222)
			return nil, errors.Trace(err1)
		}
		trace_util_0.Count(_coprocessor_00000, 220)
		if msBeforeExpired > 0 {
			trace_util_0.Count(_coprocessor_00000, 223)
			if err := bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
				trace_util_0.Count(_coprocessor_00000, 224)
				return nil, errors.Trace(err)
			}
		}
		trace_util_0.Count(_coprocessor_00000, 221)
		return worker.buildCopTasksFromRemain(bo, lastRange, task)
	}
	trace_util_0.Count(_coprocessor_00000, 210)
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		trace_util_0.Count(_coprocessor_00000, 225)
		err := errors.Errorf("other error: %s", otherErr)
		logutil.Logger(context.Background()).Warn("other error",
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.id),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	// When the request is using streaming API, the `Range` is not nil.
	trace_util_0.Count(_coprocessor_00000, 211)
	if resp.pbResp.Range != nil {
		trace_util_0.Count(_coprocessor_00000, 226)
		resp.startKey = resp.pbResp.Range.Start
	} else {
		trace_util_0.Count(_coprocessor_00000, 227)
		{
			resp.startKey = task.ranges.at(0).StartKey
		}
	}
	trace_util_0.Count(_coprocessor_00000, 212)
	if resp.detail == nil {
		trace_util_0.Count(_coprocessor_00000, 228)
		resp.detail = new(execdetails.ExecDetails)
	}
	trace_util_0.Count(_coprocessor_00000, 213)
	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	if rpcCtx != nil {
		trace_util_0.Count(_coprocessor_00000, 229)
		resp.detail.CalleeAddress = rpcCtx.Addr
	}
	trace_util_0.Count(_coprocessor_00000, 214)
	if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		trace_util_0.Count(_coprocessor_00000, 230)
		if handleTime := pbDetails.HandleTime; handleTime != nil {
			trace_util_0.Count(_coprocessor_00000, 232)
			resp.detail.WaitTime = time.Duration(handleTime.WaitMs) * time.Millisecond
			resp.detail.ProcessTime = time.Duration(handleTime.ProcessMs) * time.Millisecond
		}
		trace_util_0.Count(_coprocessor_00000, 231)
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			trace_util_0.Count(_coprocessor_00000, 233)
			if scanDetail.Write != nil {
				trace_util_0.Count(_coprocessor_00000, 234)
				resp.detail.TotalKeys += scanDetail.Write.Total
				resp.detail.ProcessedKeys += scanDetail.Write.Processed
			}
		}
	}
	trace_util_0.Count(_coprocessor_00000, 215)
	worker.sendToRespCh(resp, ch, true)
	return nil, nil
}

func (worker *copIteratorWorker) buildCopTasksFromRemain(bo *Backoffer, lastRange *coprocessor.KeyRange, task *copTask) ([]*copTask, error) {
	trace_util_0.Count(_coprocessor_00000, 235)
	remainedRanges := task.ranges
	if worker.req.Streaming && lastRange != nil {
		trace_util_0.Count(_coprocessor_00000, 237)
		remainedRanges = worker.calculateRemain(task.ranges, lastRange, worker.req.Desc)
	}
	trace_util_0.Count(_coprocessor_00000, 236)
	return buildCopTasks(bo, worker.store.regionCache, remainedRanges, worker.req.Desc, worker.req.Streaming)
}

// calculateRemain splits the input ranges into two, and take one of them according to desc flag.
// It's used in streaming API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the remain ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the remain ranges should be [r1 --> r2) [r3 --> s2)
func (worker *copIteratorWorker) calculateRemain(ranges *copRanges, split *coprocessor.KeyRange, desc bool) *copRanges {
	trace_util_0.Count(_coprocessor_00000, 238)
	if desc {
		trace_util_0.Count(_coprocessor_00000, 240)
		left, _ := ranges.split(split.End)
		return left
	}
	trace_util_0.Count(_coprocessor_00000, 239)
	_, right := ranges.split(split.Start)
	return right
}

func (it *copIterator) Close() error {
	trace_util_0.Count(_coprocessor_00000, 241)
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		trace_util_0.Count(_coprocessor_00000, 243)
		close(it.finishCh)
	}
	trace_util_0.Count(_coprocessor_00000, 242)
	it.wg.Wait()
	return nil
}

// copErrorResponse returns error when calling Next()
type copErrorResponse struct{ error }

func (it copErrorResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	trace_util_0.Count(_coprocessor_00000, 244)
	return nil, it.error
}

func (it copErrorResponse) Close() error {
	trace_util_0.Count(_coprocessor_00000, 245)
	return nil
}

var _coprocessor_00000 = "store/tikv/coprocessor.go"
