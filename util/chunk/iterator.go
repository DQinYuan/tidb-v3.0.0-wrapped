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

import "github.com/pingcap/tidb/trace_util_0"

var (
	_ Iterator = (*Iterator4Chunk)(nil)
	_ Iterator = (*iterator4RowPtr)(nil)
	_ Iterator = (*iterator4List)(nil)
	_ Iterator = (*iterator4Slice)(nil)
)

// Iterator is used to iterate a number of rows.
//
// for row := it.Begin(); row != it.End(); row = it.Next() {
//     ...
// }
type Iterator interface {
	// Begin resets the cursor of the iterator and returns the first Row.
	Begin() Row

	// Next returns the next Row.
	Next() Row

	// End returns the invalid end Row.
	End() Row

	// Len returns the length.
	Len() int

	// Current returns the current Row.
	Current() Row

	// ReachEnd reaches the end of iterator.
	ReachEnd()
}

// NewIterator4Slice returns a Iterator for Row slice.
func NewIterator4Slice(rows []Row) Iterator {
	trace_util_0.Count(_iterator_00000, 0)
	return &iterator4Slice{rows: rows}
}

type iterator4Slice struct {
	rows   []Row
	cursor int
}

// Begin implements the Iterator interface.
func (it *iterator4Slice) Begin() Row {
	trace_util_0.Count(_iterator_00000, 1)
	if it.Len() == 0 {
		trace_util_0.Count(_iterator_00000, 3)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 2)
	it.cursor = 1
	return it.rows[0]
}

// Next implements the Iterator interface.
func (it *iterator4Slice) Next() Row {
	trace_util_0.Count(_iterator_00000, 4)
	if len := it.Len(); it.cursor >= len {
		trace_util_0.Count(_iterator_00000, 6)
		it.cursor = len + 1
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 5)
	row := it.rows[it.cursor]
	it.cursor++
	return row
}

// Current implements the Iterator interface.
func (it *iterator4Slice) Current() Row {
	trace_util_0.Count(_iterator_00000, 7)
	if it.cursor == 0 || it.cursor > it.Len() {
		trace_util_0.Count(_iterator_00000, 9)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 8)
	return it.rows[it.cursor-1]
}

// End implements the Iterator interface.
func (it *iterator4Slice) End() Row {
	trace_util_0.Count(_iterator_00000, 10)
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *iterator4Slice) ReachEnd() {
	trace_util_0.Count(_iterator_00000, 11)
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface.
func (it *iterator4Slice) Len() int {
	trace_util_0.Count(_iterator_00000, 12)
	return len(it.rows)
}

// NewIterator4Chunk returns a iterator for Chunk.
func NewIterator4Chunk(chk *Chunk) *Iterator4Chunk {
	trace_util_0.Count(_iterator_00000, 13)
	return &Iterator4Chunk{chk: chk}
}

// Iterator4Chunk is used to iterate rows inside a chunk.
type Iterator4Chunk struct {
	chk     *Chunk
	cursor  int32
	numRows int32
}

// Begin implements the Iterator interface.
func (it *Iterator4Chunk) Begin() Row {
	trace_util_0.Count(_iterator_00000, 14)
	it.numRows = int32(it.chk.NumRows())
	if it.numRows == 0 {
		trace_util_0.Count(_iterator_00000, 16)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 15)
	it.cursor = 1
	return it.chk.GetRow(0)
}

// Next implements the Iterator interface.
func (it *Iterator4Chunk) Next() Row {
	trace_util_0.Count(_iterator_00000, 17)
	if it.cursor >= it.numRows {
		trace_util_0.Count(_iterator_00000, 19)
		it.cursor = it.numRows + 1
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 18)
	row := it.chk.GetRow(int(it.cursor))
	it.cursor++
	return row
}

// Current implements the Iterator interface.
func (it *Iterator4Chunk) Current() Row {
	trace_util_0.Count(_iterator_00000, 20)
	if it.cursor == 0 || int(it.cursor) > it.Len() {
		trace_util_0.Count(_iterator_00000, 22)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 21)
	return it.chk.GetRow(int(it.cursor) - 1)
}

// End implements the Iterator interface.
func (it *Iterator4Chunk) End() Row {
	trace_util_0.Count(_iterator_00000, 23)
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *Iterator4Chunk) ReachEnd() {
	trace_util_0.Count(_iterator_00000, 24)
	it.cursor = int32(it.Len() + 1)
}

// Len implements the Iterator interface
func (it *Iterator4Chunk) Len() int {
	trace_util_0.Count(_iterator_00000, 25)
	return it.chk.NumRows()
}

// NewIterator4List returns a Iterator for List.
func NewIterator4List(li *List) Iterator {
	trace_util_0.Count(_iterator_00000, 26)
	return &iterator4List{li: li}
}

type iterator4List struct {
	li        *List
	chkCursor int
	rowCursor int
}

// Begin implements the Iterator interface.
func (it *iterator4List) Begin() Row {
	trace_util_0.Count(_iterator_00000, 27)
	if it.li.NumChunks() == 0 {
		trace_util_0.Count(_iterator_00000, 30)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 28)
	chk := it.li.GetChunk(0)
	row := chk.GetRow(0)
	if chk.NumRows() == 1 {
		trace_util_0.Count(_iterator_00000, 31)
		it.chkCursor = 1
		it.rowCursor = 0
	} else {
		trace_util_0.Count(_iterator_00000, 32)
		{
			it.chkCursor = 0
			it.rowCursor = 1
		}
	}
	trace_util_0.Count(_iterator_00000, 29)
	return row
}

// Next implements the Iterator interface.
func (it *iterator4List) Next() Row {
	trace_util_0.Count(_iterator_00000, 33)
	if it.chkCursor >= it.li.NumChunks() {
		trace_util_0.Count(_iterator_00000, 36)
		it.chkCursor = it.li.NumChunks() + 1
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 34)
	chk := it.li.GetChunk(it.chkCursor)
	row := chk.GetRow(it.rowCursor)
	it.rowCursor++
	if it.rowCursor == chk.NumRows() {
		trace_util_0.Count(_iterator_00000, 37)
		it.rowCursor = 0
		it.chkCursor++
	}
	trace_util_0.Count(_iterator_00000, 35)
	return row
}

// Current implements the Iterator interface.
func (it *iterator4List) Current() Row {
	trace_util_0.Count(_iterator_00000, 38)
	if (it.chkCursor == 0 && it.rowCursor == 0) || it.chkCursor > it.li.NumChunks() {
		trace_util_0.Count(_iterator_00000, 41)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 39)
	if it.rowCursor == 0 {
		trace_util_0.Count(_iterator_00000, 42)
		curChk := it.li.GetChunk(it.chkCursor - 1)
		return curChk.GetRow(curChk.NumRows() - 1)
	}
	trace_util_0.Count(_iterator_00000, 40)
	curChk := it.li.GetChunk(it.chkCursor)
	return curChk.GetRow(it.rowCursor - 1)
}

// End implements the Iterator interface.
func (it *iterator4List) End() Row {
	trace_util_0.Count(_iterator_00000, 43)
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *iterator4List) ReachEnd() {
	trace_util_0.Count(_iterator_00000, 44)
	it.chkCursor = it.li.NumChunks() + 1
}

// Len implements the Iterator interface.
func (it *iterator4List) Len() int {
	trace_util_0.Count(_iterator_00000, 45)
	return it.li.Len()
}

// NewIterator4RowPtr returns a Iterator for RowPtrs.
func NewIterator4RowPtr(li *List, ptrs []RowPtr) Iterator {
	trace_util_0.Count(_iterator_00000, 46)
	return &iterator4RowPtr{li: li, ptrs: ptrs}
}

type iterator4RowPtr struct {
	li     *List
	ptrs   []RowPtr
	cursor int
}

// Begin implements the Iterator interface.
func (it *iterator4RowPtr) Begin() Row {
	trace_util_0.Count(_iterator_00000, 47)
	if it.Len() == 0 {
		trace_util_0.Count(_iterator_00000, 49)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 48)
	it.cursor = 1
	return it.li.GetRow(it.ptrs[0])
}

// Next implements the Iterator interface.
func (it *iterator4RowPtr) Next() Row {
	trace_util_0.Count(_iterator_00000, 50)
	if len := it.Len(); it.cursor >= len {
		trace_util_0.Count(_iterator_00000, 52)
		it.cursor = len + 1
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 51)
	row := it.li.GetRow(it.ptrs[it.cursor])
	it.cursor++
	return row
}

// Current implements the Iterator interface.
func (it *iterator4RowPtr) Current() Row {
	trace_util_0.Count(_iterator_00000, 53)
	if it.cursor == 0 || it.cursor > it.Len() {
		trace_util_0.Count(_iterator_00000, 55)
		return it.End()
	}
	trace_util_0.Count(_iterator_00000, 54)
	return it.li.GetRow(it.ptrs[it.cursor-1])
}

// End implements the Iterator interface.
func (it *iterator4RowPtr) End() Row {
	trace_util_0.Count(_iterator_00000, 56)
	return Row{}
}

// ReachEnd implements the Iterator interface.
func (it *iterator4RowPtr) ReachEnd() {
	trace_util_0.Count(_iterator_00000, 57)
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface.
func (it *iterator4RowPtr) Len() int {
	trace_util_0.Count(_iterator_00000, 58)
	return len(it.ptrs)
}

var _iterator_00000 = "util/chunk/iterator.go"
