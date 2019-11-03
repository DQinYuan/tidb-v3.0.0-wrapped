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

package mocktikv

import (
	"container/heap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

type sortRow struct {
	key  []types.Datum
	data [][]byte
}

// topNSorter implements sort.Interface. When all rows have been processed, the topNSorter will sort the whole data in heap.
type topNSorter struct {
	orderByItems []*tipb.ByItem
	rows         []*sortRow
	err          error
	sc           *stmtctx.StatementContext
}

func (t *topNSorter) Len() int {
	trace_util_0.Count(_topn_00000, 0)
	return len(t.rows)
}

func (t *topNSorter) Swap(i, j int) {
	trace_util_0.Count(_topn_00000, 1)
	t.rows[i], t.rows[j] = t.rows[j], t.rows[i]
}

func (t *topNSorter) Less(i, j int) bool {
	trace_util_0.Count(_topn_00000, 2)
	for index, by := range t.orderByItems {
		trace_util_0.Count(_topn_00000, 4)
		v1 := t.rows[i].key[index]
		v2 := t.rows[j].key[index]

		ret, err := v1.CompareDatum(t.sc, &v2)
		if err != nil {
			trace_util_0.Count(_topn_00000, 7)
			t.err = errors.Trace(err)
			return true
		}

		trace_util_0.Count(_topn_00000, 5)
		if by.Desc {
			trace_util_0.Count(_topn_00000, 8)
			ret = -ret
		}

		trace_util_0.Count(_topn_00000, 6)
		if ret < 0 {
			trace_util_0.Count(_topn_00000, 9)
			return true
		} else {
			trace_util_0.Count(_topn_00000, 10)
			if ret > 0 {
				trace_util_0.Count(_topn_00000, 11)
				return false
			}
		}
	}

	trace_util_0.Count(_topn_00000, 3)
	return false
}

// topNHeap holds the top n elements using heap structure. It implements heap.Interface.
// When we insert a row, topNHeap will check if the row can become one of the top n element or not.
type topNHeap struct {
	topNSorter

	// totalCount is equal to the limit count, which means the max size of heap.
	totalCount int
	// heapSize means the current size of this heap.
	heapSize int
}

func (t *topNHeap) Len() int {
	trace_util_0.Count(_topn_00000, 12)
	return t.heapSize
}

func (t *topNHeap) Push(x interface{}) {
	trace_util_0.Count(_topn_00000, 13)
	t.rows = append(t.rows, x.(*sortRow))
	t.heapSize++
}

func (t *topNHeap) Pop() interface{} {
	trace_util_0.Count(_topn_00000, 14)
	return nil
}

func (t *topNHeap) Less(i, j int) bool {
	trace_util_0.Count(_topn_00000, 15)
	for index, by := range t.orderByItems {
		trace_util_0.Count(_topn_00000, 17)
		v1 := t.rows[i].key[index]
		v2 := t.rows[j].key[index]

		ret, err := v1.CompareDatum(t.sc, &v2)
		if err != nil {
			trace_util_0.Count(_topn_00000, 20)
			t.err = errors.Trace(err)
			return true
		}

		trace_util_0.Count(_topn_00000, 18)
		if by.Desc {
			trace_util_0.Count(_topn_00000, 21)
			ret = -ret
		}

		trace_util_0.Count(_topn_00000, 19)
		if ret > 0 {
			trace_util_0.Count(_topn_00000, 22)
			return true
		} else {
			trace_util_0.Count(_topn_00000, 23)
			if ret < 0 {
				trace_util_0.Count(_topn_00000, 24)
				return false
			}
		}
	}

	trace_util_0.Count(_topn_00000, 16)
	return false
}

// tryToAddRow tries to add a row to heap.
// When this row is not less than any rows in heap, it will never become the top n element.
// Then this function returns false.
func (t *topNHeap) tryToAddRow(row *sortRow) bool {
	trace_util_0.Count(_topn_00000, 25)
	success := false
	if t.heapSize == t.totalCount {
		trace_util_0.Count(_topn_00000, 27)
		t.rows = append(t.rows, row)
		// When this row is less than the top element, it will replace it and adjust the heap structure.
		if t.Less(0, t.heapSize) {
			trace_util_0.Count(_topn_00000, 29)
			t.Swap(0, t.heapSize)
			heap.Fix(t, 0)
			success = true
		}
		trace_util_0.Count(_topn_00000, 28)
		t.rows = t.rows[:t.heapSize]
	} else {
		trace_util_0.Count(_topn_00000, 30)
		{
			heap.Push(t, row)
			success = true
		}
	}
	trace_util_0.Count(_topn_00000, 26)
	return success
}

var _topn_00000 = "store/mockstore/mocktikv/topn.go"
