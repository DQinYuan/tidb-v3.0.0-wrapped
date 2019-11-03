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

package chunk

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// List holds a slice of chunks, use to append rows with max chunk size properly handled.
type List struct {
	fieldTypes    []*types.FieldType
	initChunkSize int
	maxChunkSize  int
	length        int
	chunks        []*Chunk
	freelist      []*Chunk

	memTracker  *memory.Tracker // track memory usage.
	consumedIdx int             // chunk index in "chunks", has been consumed.
}

// RowPtr is used to get a row from a list.
// It is only valid for the list that returns it.
type RowPtr struct {
	ChkIdx uint32
	RowIdx uint32
}

var chunkListLabel fmt.Stringer = stringutil.StringerStr("chunk.List")

// NewList creates a new List with field types, init chunk size and max chunk size.
func NewList(fieldTypes []*types.FieldType, initChunkSize, maxChunkSize int) *List {
	trace_util_0.Count(_list_00000, 0)
	l := &List{
		fieldTypes:    fieldTypes,
		initChunkSize: initChunkSize,
		maxChunkSize:  maxChunkSize,
		memTracker:    memory.NewTracker(chunkListLabel, -1),
		consumedIdx:   -1,
	}
	return l
}

// GetMemTracker returns the memory tracker of this List.
func (l *List) GetMemTracker() *memory.Tracker {
	trace_util_0.Count(_list_00000, 1)
	return l.memTracker
}

// Len returns the length of the List.
func (l *List) Len() int {
	trace_util_0.Count(_list_00000, 2)
	return l.length
}

// NumChunks returns the number of chunks in the List.
func (l *List) NumChunks() int {
	trace_util_0.Count(_list_00000, 3)
	return len(l.chunks)
}

// GetChunk gets the Chunk by ChkIdx.
func (l *List) GetChunk(chkIdx int) *Chunk {
	trace_util_0.Count(_list_00000, 4)
	return l.chunks[chkIdx]
}

// AppendRow appends a row to the List, the row is copied to the List.
func (l *List) AppendRow(row Row) RowPtr {
	trace_util_0.Count(_list_00000, 5)
	chkIdx := len(l.chunks) - 1
	if chkIdx == -1 || l.chunks[chkIdx].NumRows() >= l.chunks[chkIdx].Capacity() || chkIdx == l.consumedIdx {
		trace_util_0.Count(_list_00000, 7)
		newChk := l.allocChunk()
		l.chunks = append(l.chunks, newChk)
		if chkIdx != l.consumedIdx {
			trace_util_0.Count(_list_00000, 9)
			l.memTracker.Consume(l.chunks[chkIdx].MemoryUsage())
			l.consumedIdx = chkIdx
		}
		trace_util_0.Count(_list_00000, 8)
		chkIdx++
	}
	trace_util_0.Count(_list_00000, 6)
	chk := l.chunks[chkIdx]
	rowIdx := chk.NumRows()
	chk.AppendRow(row)
	l.length++
	return RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
}

// Add adds a chunk to the List, the chunk may be modified later by the list.
// Caller must make sure the input chk is not empty and not used any more and has the same field types.
func (l *List) Add(chk *Chunk) {
	trace_util_0.Count(_list_00000, 10)
	// FixMe: we should avoid add a Chunk that chk.NumRows() > list.maxChunkSize.
	if chk.NumRows() == 0 {
		trace_util_0.Count(_list_00000, 13)
		panic("chunk appended to List should have at least 1 row")
	}
	trace_util_0.Count(_list_00000, 11)
	if chkIdx := len(l.chunks) - 1; l.consumedIdx != chkIdx {
		trace_util_0.Count(_list_00000, 14)
		l.memTracker.Consume(l.chunks[chkIdx].MemoryUsage())
		l.consumedIdx = chkIdx
	}
	trace_util_0.Count(_list_00000, 12)
	l.memTracker.Consume(chk.MemoryUsage())
	l.consumedIdx++
	l.chunks = append(l.chunks, chk)
	l.length += chk.NumRows()
	return
}

func (l *List) allocChunk() (chk *Chunk) {
	trace_util_0.Count(_list_00000, 15)
	if len(l.freelist) > 0 {
		trace_util_0.Count(_list_00000, 18)
		lastIdx := len(l.freelist) - 1
		chk = l.freelist[lastIdx]
		l.freelist = l.freelist[:lastIdx]
		l.memTracker.Consume(-chk.MemoryUsage())
		chk.Reset()
		return
	}
	trace_util_0.Count(_list_00000, 16)
	if len(l.chunks) > 0 {
		trace_util_0.Count(_list_00000, 19)
		return Renew(l.chunks[len(l.chunks)-1], l.maxChunkSize)
	}
	trace_util_0.Count(_list_00000, 17)
	return New(l.fieldTypes, l.initChunkSize, l.maxChunkSize)
}

// GetRow gets a Row from the list by RowPtr.
func (l *List) GetRow(ptr RowPtr) Row {
	trace_util_0.Count(_list_00000, 20)
	chk := l.chunks[ptr.ChkIdx]
	return chk.GetRow(int(ptr.RowIdx))
}

// Reset resets the List.
func (l *List) Reset() {
	trace_util_0.Count(_list_00000, 21)
	if lastIdx := len(l.chunks) - 1; lastIdx != l.consumedIdx {
		trace_util_0.Count(_list_00000, 23)
		l.memTracker.Consume(l.chunks[lastIdx].MemoryUsage())
	}
	trace_util_0.Count(_list_00000, 22)
	l.freelist = append(l.freelist, l.chunks...)
	l.chunks = l.chunks[:0]
	l.length = 0
	l.consumedIdx = -1
}

// PreAlloc4Row pre-allocates the storage memory for a Row.
// NOTE:
// 1. The List must be empty or holds no useful data.
// 2. The schema of the Row must be the same with the List.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the List after the pre-allocation.
func (l *List) PreAlloc4Row(row Row) (ptr RowPtr) {
	trace_util_0.Count(_list_00000, 24)
	chkIdx := len(l.chunks) - 1
	if chkIdx == -1 || l.chunks[chkIdx].NumRows() >= l.chunks[chkIdx].Capacity() {
		trace_util_0.Count(_list_00000, 26)
		newChk := l.allocChunk()
		l.chunks = append(l.chunks, newChk)
		if chkIdx != l.consumedIdx {
			trace_util_0.Count(_list_00000, 28)
			l.memTracker.Consume(l.chunks[chkIdx].MemoryUsage())
			l.consumedIdx = chkIdx
		}
		trace_util_0.Count(_list_00000, 27)
		chkIdx++
	}
	trace_util_0.Count(_list_00000, 25)
	chk := l.chunks[chkIdx]
	rowIdx := chk.PreAlloc(row)
	l.length++
	return RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
}

// Insert inserts `row` on the position specified by `ptr`.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (l *List) Insert(ptr RowPtr, row Row) {
	trace_util_0.Count(_list_00000, 29)
	l.chunks[ptr.ChkIdx].Insert(int(ptr.RowIdx), row)
}

// ListWalkFunc is used to walk the list.
// If error is returned, it will stop walking.
type ListWalkFunc = func(row Row) error

// Walk iterate the list and call walkFunc for each row.
func (l *List) Walk(walkFunc ListWalkFunc) error {
	trace_util_0.Count(_list_00000, 30)
	for i := 0; i < len(l.chunks); i++ {
		trace_util_0.Count(_list_00000, 32)
		chk := l.chunks[i]
		for j := 0; j < chk.NumRows(); j++ {
			trace_util_0.Count(_list_00000, 33)
			err := walkFunc(chk.GetRow(j))
			if err != nil {
				trace_util_0.Count(_list_00000, 34)
				return errors.Trace(err)
			}
		}
	}
	trace_util_0.Count(_list_00000, 31)
	return nil
}

var _list_00000 = "util/chunk/list.go"