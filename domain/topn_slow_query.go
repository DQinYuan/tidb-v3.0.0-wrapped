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

package domain

import (
	"container/heap"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/execdetails"
)

type slowQueryHeap struct {
	data []*SlowQueryInfo
}

func (h *slowQueryHeap) Len() int { trace_util_0.Count(_topn_slow_query_00000, 0); return len(h.data) }
func (h *slowQueryHeap) Less(i, j int) bool {
	trace_util_0.Count(_topn_slow_query_00000, 1)
	return h.data[i].Duration < h.data[j].Duration
}
func (h *slowQueryHeap) Swap(i, j int) {
	trace_util_0.Count(_topn_slow_query_00000, 2)
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *slowQueryHeap) Push(x interface{}) {
	trace_util_0.Count(_topn_slow_query_00000, 3)
	h.data = append(h.data, x.(*SlowQueryInfo))
}

func (h *slowQueryHeap) Pop() interface{} {
	trace_util_0.Count(_topn_slow_query_00000, 4)
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

func (h *slowQueryHeap) RemoveExpired(now time.Time, period time.Duration) {
	trace_util_0.Count(_topn_slow_query_00000, 5)
	// Remove outdated slow query element.
	idx := 0
	for i := 0; i < len(h.data); i++ {
		trace_util_0.Count(_topn_slow_query_00000, 8)
		outdateTime := h.data[i].Start.Add(period)
		if outdateTime.After(now) {
			trace_util_0.Count(_topn_slow_query_00000, 9)
			h.data[idx] = h.data[i]
			idx++
		}
	}
	trace_util_0.Count(_topn_slow_query_00000, 6)
	if len(h.data) == idx {
		trace_util_0.Count(_topn_slow_query_00000, 10)
		return
	}

	// Rebuild the heap.
	trace_util_0.Count(_topn_slow_query_00000, 7)
	h.data = h.data[:idx]
	heap.Init(h)
}

func (h *slowQueryHeap) Query(count int) []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 11)
	// The sorted array still maintains the heap property.
	sort.Sort(h)

	// The result shoud be in decrease order.
	return takeLastN(h.data, count)
}

type slowQueryQueue struct {
	data []*SlowQueryInfo
	size int
}

func (q *slowQueryQueue) Enqueue(info *SlowQueryInfo) {
	trace_util_0.Count(_topn_slow_query_00000, 12)
	if len(q.data) < q.size {
		trace_util_0.Count(_topn_slow_query_00000, 14)
		q.data = append(q.data, info)
		return
	}

	trace_util_0.Count(_topn_slow_query_00000, 13)
	q.data = append(q.data, info)[1:]
}

func (q *slowQueryQueue) Query(count int) []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 15)
	// Queue is empty.
	if len(q.data) == 0 {
		trace_util_0.Count(_topn_slow_query_00000, 17)
		return nil
	}
	trace_util_0.Count(_topn_slow_query_00000, 16)
	return takeLastN(q.data, count)
}

func takeLastN(data []*SlowQueryInfo, count int) []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 18)
	if count > len(data) {
		trace_util_0.Count(_topn_slow_query_00000, 21)
		count = len(data)
	}
	trace_util_0.Count(_topn_slow_query_00000, 19)
	ret := make([]*SlowQueryInfo, 0, count)
	for i := len(data) - 1; i >= 0 && len(ret) < count; i-- {
		trace_util_0.Count(_topn_slow_query_00000, 22)
		ret = append(ret, data[i])
	}
	trace_util_0.Count(_topn_slow_query_00000, 20)
	return ret
}

// topNSlowQueries maintains two heaps to store recent slow queries: one for user's and one for internal.
// N = 30, period = 7 days by default.
// It also maintains a recent queue, in a FIFO manner.
type topNSlowQueries struct {
	recent   slowQueryQueue
	user     slowQueryHeap
	internal slowQueryHeap
	topN     int
	period   time.Duration
	ch       chan *SlowQueryInfo
	msgCh    chan *showSlowMessage

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newTopNSlowQueries(topN int, period time.Duration, queueSize int) *topNSlowQueries {
	trace_util_0.Count(_topn_slow_query_00000, 23)
	ret := &topNSlowQueries{
		topN:   topN,
		period: period,
		ch:     make(chan *SlowQueryInfo, 1000),
		msgCh:  make(chan *showSlowMessage, 10),
	}
	ret.user.data = make([]*SlowQueryInfo, 0, topN)
	ret.internal.data = make([]*SlowQueryInfo, 0, topN)
	ret.recent.size = queueSize
	ret.recent.data = make([]*SlowQueryInfo, 0, queueSize)
	return ret
}

func (q *topNSlowQueries) Append(info *SlowQueryInfo) {
	trace_util_0.Count(_topn_slow_query_00000, 24)
	// Put into the recent queue.
	q.recent.Enqueue(info)

	var h *slowQueryHeap
	if info.Internal {
		trace_util_0.Count(_topn_slow_query_00000, 27)
		h = &q.internal
	} else {
		trace_util_0.Count(_topn_slow_query_00000, 28)
		{
			h = &q.user
		}
	}

	// Heap is not full.
	trace_util_0.Count(_topn_slow_query_00000, 25)
	if len(h.data) < q.topN {
		trace_util_0.Count(_topn_slow_query_00000, 29)
		heap.Push(h, info)
		return
	}

	// Replace the heap top.
	trace_util_0.Count(_topn_slow_query_00000, 26)
	if info.Duration > h.data[0].Duration {
		trace_util_0.Count(_topn_slow_query_00000, 30)
		heap.Pop(h)
		heap.Push(h, info)
	}
}

func (q *topNSlowQueries) QueryAll() []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 31)
	return q.recent.data
}

func (q *topNSlowQueries) RemoveExpired(now time.Time) {
	trace_util_0.Count(_topn_slow_query_00000, 32)
	q.user.RemoveExpired(now, q.period)
	q.internal.RemoveExpired(now, q.period)
}

type showSlowMessage struct {
	request *ast.ShowSlow
	result  []*SlowQueryInfo
	sync.WaitGroup
}

type queryType int

const (
	queryTypeTop queryType = iota
	queryTypeRecent
)

func (q *topNSlowQueries) QueryRecent(count int) []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 33)
	return q.recent.Query(count)
}

func (q *topNSlowQueries) QueryTop(count int, kind ast.ShowSlowKind) []*SlowQueryInfo {
	trace_util_0.Count(_topn_slow_query_00000, 34)
	var ret []*SlowQueryInfo
	switch kind {
	case ast.ShowSlowKindDefault:
		trace_util_0.Count(_topn_slow_query_00000, 36)
		ret = q.user.Query(count)
	case ast.ShowSlowKindInternal:
		trace_util_0.Count(_topn_slow_query_00000, 37)
		ret = q.internal.Query(count)
	case ast.ShowSlowKindAll:
		trace_util_0.Count(_topn_slow_query_00000, 38)
		tmp := make([]*SlowQueryInfo, 0, len(q.user.data)+len(q.internal.data))
		tmp = append(tmp, q.user.data...)
		tmp = append(tmp, q.internal.data...)
		tmp1 := slowQueryHeap{tmp}
		sort.Sort(&tmp1)
		ret = takeLastN(tmp, count)
	}
	trace_util_0.Count(_topn_slow_query_00000, 35)
	return ret
}

func (q *topNSlowQueries) Close() {
	trace_util_0.Count(_topn_slow_query_00000, 39)
	q.mu.Lock()
	q.mu.closed = true
	q.mu.Unlock()

	close(q.ch)
}

// SlowQueryInfo is a struct to record slow query info.
type SlowQueryInfo struct {
	SQL      string
	Start    time.Time
	Duration time.Duration
	Detail   execdetails.ExecDetails
	Succ     bool
	ConnID   uint64
	TxnTS    uint64
	User     string
	DB       string
	TableIDs string
	IndexIDs string
	Internal bool
	Digest   string
}

var _topn_slow_query_00000 = "domain/topn_slow_query.go"
