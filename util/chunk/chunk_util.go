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

package chunk

import

// CopySelectedJoinRows copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
//
// NOTE: All the outer rows in the source Chunk should be the same.
"github.com/pingcap/tidb/trace_util_0"

func CopySelectedJoinRows(src *Chunk, innerColOffset, outerColOffset int, selected []bool, dst *Chunk) bool {
	trace_util_0.Count(_chunk_util_00000, 0)
	if src.NumRows() == 0 {
		trace_util_0.Count(_chunk_util_00000, 2)
		return false
	}

	trace_util_0.Count(_chunk_util_00000, 1)
	numSelected := copySelectedInnerRows(innerColOffset, outerColOffset, src, selected, dst)
	copyOuterRows(innerColOffset, outerColOffset, src, numSelected, dst)
	dst.numVirtualRows += numSelected
	return numSelected > 0
}

// copySelectedInnerRows copies the selected inner rows from the source Chunk
// to the destination Chunk.
// return the number of rows which is selected.
func copySelectedInnerRows(innerColOffset, outerColOffset int, src *Chunk, selected []bool, dst *Chunk) int {
	trace_util_0.Count(_chunk_util_00000, 3)
	oldLen := dst.columns[innerColOffset].length
	var srcCols []*column
	if innerColOffset == 0 {
		trace_util_0.Count(_chunk_util_00000, 6)
		srcCols = src.columns[:outerColOffset]
	} else {
		trace_util_0.Count(_chunk_util_00000, 7)
		{
			srcCols = src.columns[innerColOffset:]
		}
	}
	trace_util_0.Count(_chunk_util_00000, 4)
	for j, srcCol := range srcCols {
		trace_util_0.Count(_chunk_util_00000, 8)
		dstCol := dst.columns[innerColOffset+j]
		if srcCol.isFixed() {
			trace_util_0.Count(_chunk_util_00000, 9)
			for i := 0; i < len(selected); i++ {
				trace_util_0.Count(_chunk_util_00000, 10)
				if !selected[i] {
					trace_util_0.Count(_chunk_util_00000, 12)
					continue
				}
				trace_util_0.Count(_chunk_util_00000, 11)
				dstCol.appendNullBitmap(!srcCol.isNull(i))
				dstCol.length++

				elemLen := len(srcCol.elemBuf)
				offset := i * elemLen
				dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
			}
		} else {
			trace_util_0.Count(_chunk_util_00000, 13)
			{
				for i := 0; i < len(selected); i++ {
					trace_util_0.Count(_chunk_util_00000, 14)
					if !selected[i] {
						trace_util_0.Count(_chunk_util_00000, 16)
						continue
					}
					trace_util_0.Count(_chunk_util_00000, 15)
					dstCol.appendNullBitmap(!srcCol.isNull(i))
					dstCol.length++

					start, end := srcCol.offsets[i], srcCol.offsets[i+1]
					dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
					dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
				}
			}
		}
	}
	trace_util_0.Count(_chunk_util_00000, 5)
	return dst.columns[innerColOffset].length - oldLen
}

// copyOuterRows copies the continuous 'numRows' outer rows in the source Chunk
// to the destination Chunk.
func copyOuterRows(innerColOffset, outerColOffset int, src *Chunk, numRows int, dst *Chunk) {
	trace_util_0.Count(_chunk_util_00000, 17)
	if numRows <= 0 {
		trace_util_0.Count(_chunk_util_00000, 20)
		return
	}
	trace_util_0.Count(_chunk_util_00000, 18)
	row := src.GetRow(0)
	var srcCols []*column
	if innerColOffset == 0 {
		trace_util_0.Count(_chunk_util_00000, 21)
		srcCols = src.columns[outerColOffset:]
	} else {
		trace_util_0.Count(_chunk_util_00000, 22)
		{
			srcCols = src.columns[:innerColOffset]
		}
	}
	trace_util_0.Count(_chunk_util_00000, 19)
	for i, srcCol := range srcCols {
		trace_util_0.Count(_chunk_util_00000, 23)
		dstCol := dst.columns[outerColOffset+i]
		dstCol.appendMultiSameNullBitmap(!srcCol.isNull(row.idx), numRows)
		dstCol.length += numRows
		if srcCol.isFixed() {
			trace_util_0.Count(_chunk_util_00000, 24)
			elemLen := len(srcCol.elemBuf)
			start := row.idx * elemLen
			end := start + numRows*elemLen
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
		} else {
			trace_util_0.Count(_chunk_util_00000, 25)
			{
				start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+numRows]
				dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
				offsets := dstCol.offsets
				elemLen := srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx]
				for j := 0; j < numRows; j++ {
					trace_util_0.Count(_chunk_util_00000, 27)
					offsets = append(offsets, int64(offsets[len(offsets)-1]+elemLen))
				}
				trace_util_0.Count(_chunk_util_00000, 26)
				dstCol.offsets = offsets
			}
		}
	}
}

var _chunk_util_00000 = "util/chunk/chunk_util.go"
