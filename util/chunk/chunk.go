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
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	columns []*column
	// numVirtualRows indicates the number of virtual rows, which have zero column.
	// It is used only when this Chunk doesn't hold any data, i.e. "len(columns)==0".
	numVirtualRows int
	// capacity indicates the max number of rows this chunk can hold.
	// TODO: replace all usages of capacity to requiredRows and remove this field
	capacity int

	// requiredRows indicates how many rows the parent executor want.
	requiredRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, cap int) *Chunk {
	trace_util_0.Count(_chunk_00000, 0)
	return New(fields, cap, cap) //FIXME: in following PR.
}

// New creates a new chunk.
//  cap: the limit for the max number of rows.
//  maxChunkSize: the max limit for the number of rows.
func New(fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	trace_util_0.Count(_chunk_00000, 1)
	chk := new(Chunk)
	chk.columns = make([]*column, 0, len(fields))
	chk.capacity = mathutil.Min(cap, maxChunkSize)
	for _, f := range fields {
		trace_util_0.Count(_chunk_00000, 3)
		elemLen := getFixedLen(f)
		if elemLen == varElemLen {
			trace_util_0.Count(_chunk_00000, 4)
			chk.columns = append(chk.columns, newVarLenColumn(chk.capacity, nil))
		} else {
			trace_util_0.Count(_chunk_00000, 5)
			{
				chk.columns = append(chk.columns, newFixedLenColumn(elemLen, chk.capacity))
			}
		}
	}
	trace_util_0.Count(_chunk_00000, 2)
	chk.numVirtualRows = 0

	// set the default value of requiredRows to maxChunkSize to let chk.IsFull() behave
	// like how we judge whether a chunk is full now, then the statement
	// "chk.NumRows() < maxChunkSize"
	// is equal to
	// "!chk.IsFull()".
	chk.requiredRows = maxChunkSize
	return chk
}

// Renew creates a new Chunk based on an existing Chunk. The newly created Chunk
// has the same data schema with the old Chunk. The capacity of the new Chunk
// might be doubled based on the capacity of the old Chunk and the maxChunkSize.
//  chk: old chunk(often used in previous call).
//  maxChunkSize: the limit for the max number of rows.
func Renew(chk *Chunk, maxChunkSize int) *Chunk {
	trace_util_0.Count(_chunk_00000, 6)
	newChk := new(Chunk)
	if chk.columns == nil {
		trace_util_0.Count(_chunk_00000, 8)
		return newChk
	}
	trace_util_0.Count(_chunk_00000, 7)
	newCap := reCalcCapacity(chk, maxChunkSize)
	newChk.columns = renewColumns(chk.columns, newCap)
	newChk.numVirtualRows = 0
	newChk.capacity = newCap
	newChk.requiredRows = maxChunkSize
	return newChk
}

// renewColumns creates the columns of a Chunk. The capacity of the newly
// created columns is equal to cap.
func renewColumns(oldCol []*column, cap int) []*column {
	trace_util_0.Count(_chunk_00000, 9)
	columns := make([]*column, 0, len(oldCol))
	for _, col := range oldCol {
		trace_util_0.Count(_chunk_00000, 11)
		if col.isFixed() {
			trace_util_0.Count(_chunk_00000, 12)
			columns = append(columns, newFixedLenColumn(len(col.elemBuf), cap))
		} else {
			trace_util_0.Count(_chunk_00000, 13)
			{
				columns = append(columns, newVarLenColumn(cap, col))
			}
		}
	}
	trace_util_0.Count(_chunk_00000, 10)
	return columns
}

// MemoryUsage returns the total memory usage of a Chunk in B.
// We ignore the size of column.length and column.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	trace_util_0.Count(_chunk_00000, 14)
	for _, col := range c.columns {
		trace_util_0.Count(_chunk_00000, 16)
		curColMemUsage := int64(unsafe.Sizeof(*col)) + int64(cap(col.nullBitmap)) + int64(cap(col.offsets)*4) + int64(cap(col.data)) + int64(cap(col.elemBuf))
		sum += curColMemUsage
	}
	trace_util_0.Count(_chunk_00000, 15)
	return
}

// newFixedLenColumn creates a fixed length column with elemLen and initial data capacity.
func newFixedLenColumn(elemLen, cap int) *column {
	trace_util_0.Count(_chunk_00000, 17)
	return &column{
		elemBuf:    make([]byte, elemLen),
		data:       make([]byte, 0, cap*elemLen),
		nullBitmap: make([]byte, 0, cap>>3),
	}
}

// newVarLenColumn creates a variable length column with initial data capacity.
func newVarLenColumn(cap int, old *column) *column {
	trace_util_0.Count(_chunk_00000, 18)
	estimatedElemLen := 8
	// For varLenColumn (e.g. varchar), the accurate length of an element is unknown.
	// Therefore, in the first executor.Next we use an experience value -- 8 (so it may make runtime.growslice)
	// but in the following Next call we estimate the length as AVG x 1.125 elemLen of the previous call.
	if old != nil && old.length != 0 {
		trace_util_0.Count(_chunk_00000, 20)
		estimatedElemLen = (len(old.data) + len(old.data)/8) / old.length
	}
	trace_util_0.Count(_chunk_00000, 19)
	return &column{
		offsets:    make([]int64, 1, cap+1),
		data:       make([]byte, 0, cap*estimatedElemLen),
		nullBitmap: make([]byte, 0, cap>>3),
	}
}

// RequiredRows returns how many rows is considered full.
func (c *Chunk) RequiredRows() int {
	trace_util_0.Count(_chunk_00000, 21)
	return c.requiredRows
}

// SetRequiredRows sets the number of required rows.
func (c *Chunk) SetRequiredRows(requiredRows, maxChunkSize int) *Chunk {
	trace_util_0.Count(_chunk_00000, 22)
	if requiredRows <= 0 || requiredRows > maxChunkSize {
		trace_util_0.Count(_chunk_00000, 24)
		requiredRows = maxChunkSize
	}
	trace_util_0.Count(_chunk_00000, 23)
	c.requiredRows = requiredRows
	return c
}

// IsFull returns if this chunk is considered full.
func (c *Chunk) IsFull() bool {
	trace_util_0.Count(_chunk_00000, 25)
	return c.NumRows() >= c.requiredRows
}

// MakeRef makes column in "dstColIdx" reference to column in "srcColIdx".
func (c *Chunk) MakeRef(srcColIdx, dstColIdx int) {
	trace_util_0.Count(_chunk_00000, 26)
	c.columns[dstColIdx] = c.columns[srcColIdx]
}

// MakeRefTo copies columns `src.columns[srcColIdx]` to `c.columns[dstColIdx]`.
func (c *Chunk) MakeRefTo(dstColIdx int, src *Chunk, srcColIdx int) {
	trace_util_0.Count(_chunk_00000, 27)
	c.columns[dstColIdx] = src.columns[srcColIdx]
}

// SwapColumn swaps column "c.columns[colIdx]" with column
// "other.columns[otherIdx]". If there exists columns refer to the column to be
// swapped, we need to re-build the reference.
func (c *Chunk) SwapColumn(colIdx int, other *Chunk, otherIdx int) {
	trace_util_0.Count(_chunk_00000, 28)
	// Find the leftmost column of the reference which is the actual column to
	// be swapped.
	for i := 0; i < colIdx; i++ {
		trace_util_0.Count(_chunk_00000, 34)
		if c.columns[i] == c.columns[colIdx] {
			trace_util_0.Count(_chunk_00000, 35)
			colIdx = i
		}
	}
	trace_util_0.Count(_chunk_00000, 29)
	for i := 0; i < otherIdx; i++ {
		trace_util_0.Count(_chunk_00000, 36)
		if other.columns[i] == other.columns[otherIdx] {
			trace_util_0.Count(_chunk_00000, 37)
			otherIdx = i
		}
	}

	// Find the columns which refer to the actual column to be swapped.
	trace_util_0.Count(_chunk_00000, 30)
	refColsIdx := make([]int, 0, len(c.columns)-colIdx)
	for i := colIdx; i < len(c.columns); i++ {
		trace_util_0.Count(_chunk_00000, 38)
		if c.columns[i] == c.columns[colIdx] {
			trace_util_0.Count(_chunk_00000, 39)
			refColsIdx = append(refColsIdx, i)
		}
	}
	trace_util_0.Count(_chunk_00000, 31)
	refColsIdx4Other := make([]int, 0, len(other.columns)-otherIdx)
	for i := otherIdx; i < len(other.columns); i++ {
		trace_util_0.Count(_chunk_00000, 40)
		if other.columns[i] == other.columns[otherIdx] {
			trace_util_0.Count(_chunk_00000, 41)
			refColsIdx4Other = append(refColsIdx4Other, i)
		}
	}

	// Swap columns from two chunks.
	trace_util_0.Count(_chunk_00000, 32)
	c.columns[colIdx], other.columns[otherIdx] = other.columns[otherIdx], c.columns[colIdx]

	// Rebuild the reference.
	for _, i := range refColsIdx {
		trace_util_0.Count(_chunk_00000, 42)
		c.MakeRef(colIdx, i)
	}
	trace_util_0.Count(_chunk_00000, 33)
	for _, i := range refColsIdx4Other {
		trace_util_0.Count(_chunk_00000, 43)
		other.MakeRef(otherIdx, i)
	}
}

// SwapColumns swaps columns with another Chunk.
func (c *Chunk) SwapColumns(other *Chunk) {
	trace_util_0.Count(_chunk_00000, 44)
	c.columns, other.columns = other.columns, c.columns
	c.numVirtualRows, other.numVirtualRows = other.numVirtualRows, c.numVirtualRows
}

// SetNumVirtualRows sets the virtual row number for a Chunk.
// It should only be used when there exists no column in the Chunk.
func (c *Chunk) SetNumVirtualRows(numVirtualRows int) {
	trace_util_0.Count(_chunk_00000, 45)
	c.numVirtualRows = numVirtualRows
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset() {
	trace_util_0.Count(_chunk_00000, 46)
	if c.columns == nil {
		trace_util_0.Count(_chunk_00000, 49)
		return
	}
	trace_util_0.Count(_chunk_00000, 47)
	for _, col := range c.columns {
		trace_util_0.Count(_chunk_00000, 50)
		col.reset()
	}
	trace_util_0.Count(_chunk_00000, 48)
	c.numVirtualRows = 0
}

// CopyConstruct creates a new chunk and copies this chunk's data into it.
func (c *Chunk) CopyConstruct() *Chunk {
	trace_util_0.Count(_chunk_00000, 51)
	newChk := &Chunk{numVirtualRows: c.numVirtualRows, capacity: c.capacity, columns: make([]*column, len(c.columns))}
	for i := range c.columns {
		trace_util_0.Count(_chunk_00000, 53)
		newChk.columns[i] = c.columns[i].copyConstruct()
	}
	trace_util_0.Count(_chunk_00000, 52)
	return newChk
}

// GrowAndReset resets the Chunk and doubles the capacity of the Chunk.
// The doubled capacity should not be larger than maxChunkSize.
// TODO: this method will be used in following PR.
func (c *Chunk) GrowAndReset(maxChunkSize int) {
	trace_util_0.Count(_chunk_00000, 54)
	if c.columns == nil {
		trace_util_0.Count(_chunk_00000, 57)
		return
	}
	trace_util_0.Count(_chunk_00000, 55)
	newCap := reCalcCapacity(c, maxChunkSize)
	if newCap <= c.capacity {
		trace_util_0.Count(_chunk_00000, 58)
		c.Reset()
		return
	}
	trace_util_0.Count(_chunk_00000, 56)
	c.capacity = newCap
	c.columns = renewColumns(c.columns, newCap)
	c.numVirtualRows = 0
	c.requiredRows = maxChunkSize
}

// reCalcCapacity calculates the capacity for another Chunk based on the current
// Chunk. The new capacity is doubled only when the current Chunk is full.
func reCalcCapacity(c *Chunk, maxChunkSize int) int {
	trace_util_0.Count(_chunk_00000, 59)
	if c.NumRows() < c.capacity {
		trace_util_0.Count(_chunk_00000, 61)
		return c.capacity
	}
	trace_util_0.Count(_chunk_00000, 60)
	return mathutil.Min(c.capacity*2, maxChunkSize)
}

// Capacity returns the capacity of the Chunk.
func (c *Chunk) Capacity() int {
	trace_util_0.Count(_chunk_00000, 62)
	return c.capacity
}

// NumCols returns the number of columns in the chunk.
func (c *Chunk) NumCols() int {
	trace_util_0.Count(_chunk_00000, 63)
	return len(c.columns)
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	trace_util_0.Count(_chunk_00000, 64)
	if c.NumCols() == 0 {
		trace_util_0.Count(_chunk_00000, 66)
		return c.numVirtualRows
	}
	trace_util_0.Count(_chunk_00000, 65)
	return c.columns[0].length
}

// GetRow gets the Row in the chunk with the row index.
func (c *Chunk) GetRow(idx int) Row {
	trace_util_0.Count(_chunk_00000, 67)
	return Row{c: c, idx: idx}
}

// AppendRow appends a row to the chunk.
func (c *Chunk) AppendRow(row Row) {
	trace_util_0.Count(_chunk_00000, 68)
	c.AppendPartialRow(0, row)
	c.numVirtualRows++
}

// AppendPartialRow appends a row to the chunk.
func (c *Chunk) AppendPartialRow(colIdx int, row Row) {
	trace_util_0.Count(_chunk_00000, 69)
	for i, rowCol := range row.c.columns {
		trace_util_0.Count(_chunk_00000, 70)
		chkCol := c.columns[colIdx+i]
		chkCol.appendNullBitmap(!rowCol.isNull(row.idx))
		if rowCol.isFixed() {
			trace_util_0.Count(_chunk_00000, 72)
			elemLen := len(rowCol.elemBuf)
			offset := row.idx * elemLen
			chkCol.data = append(chkCol.data, rowCol.data[offset:offset+elemLen]...)
		} else {
			trace_util_0.Count(_chunk_00000, 73)
			{
				start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+1]
				chkCol.data = append(chkCol.data, rowCol.data[start:end]...)
				chkCol.offsets = append(chkCol.offsets, int64(len(chkCol.data)))
			}
		}
		trace_util_0.Count(_chunk_00000, 71)
		chkCol.length++
	}
}

// PreAlloc pre-allocates the memory space in a Chunk to store the Row.
// NOTE:
// 1. The Chunk must be empty or holds no useful data.
// 2. The schema of the Row must be the same with the Chunk.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the Chunk after the pre-allocation.
// 4. We set the null bitmap here instead of in the Insert() function because
//    when the Insert() function is called parallelly, the data race on a byte
//    can not be avoided although the manipulated bits are different inside a
//    byte.
func (c *Chunk) PreAlloc(row Row) (rowIdx uint32) {
	trace_util_0.Count(_chunk_00000, 74)
	rowIdx = uint32(c.NumRows())
	for i, srcCol := range row.c.columns {
		trace_util_0.Count(_chunk_00000, 76)
		dstCol := c.columns[i]
		dstCol.appendNullBitmap(!srcCol.isNull(row.idx))
		elemLen := len(srcCol.elemBuf)
		if !srcCol.isFixed() {
			trace_util_0.Count(_chunk_00000, 80)
			elemLen = int(srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx])
			dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)+elemLen))
		}
		trace_util_0.Count(_chunk_00000, 77)
		dstCol.length++
		needCap := len(dstCol.data) + elemLen
		if needCap <= cap(dstCol.data) {
			trace_util_0.Count(_chunk_00000, 81)
			(*reflect.SliceHeader)(unsafe.Pointer(&dstCol.data)).Len = len(dstCol.data) + elemLen
			continue
		}
		// Grow the capacity according to golang.growslice.
		// Implementation differences with golang:
		// 1. We double the capacity when `dstCol.data < 1024*elemLen bytes` but
		// not `1024 bytes`.
		// 2. We expand the capacity to 1.5*originCap rather than 1.25*originCap
		// during the slow-increasing phase.
		trace_util_0.Count(_chunk_00000, 78)
		newCap := cap(dstCol.data)
		doubleCap := newCap << 1
		if needCap > doubleCap {
			trace_util_0.Count(_chunk_00000, 82)
			newCap = needCap
		} else {
			trace_util_0.Count(_chunk_00000, 83)
			{
				avgElemLen := elemLen
				if !srcCol.isFixed() {
					trace_util_0.Count(_chunk_00000, 85)
					avgElemLen = len(dstCol.data) / len(dstCol.offsets)
				}
				// slowIncThreshold indicates the threshold exceeding which the
				// dstCol.data capacity increase fold decreases from 2 to 1.5.
				trace_util_0.Count(_chunk_00000, 84)
				slowIncThreshold := 1024 * avgElemLen
				if len(dstCol.data) < slowIncThreshold {
					trace_util_0.Count(_chunk_00000, 86)
					newCap = doubleCap
				} else {
					trace_util_0.Count(_chunk_00000, 87)
					{
						for 0 < newCap && newCap < needCap {
							trace_util_0.Count(_chunk_00000, 89)
							newCap += newCap / 2
						}
						trace_util_0.Count(_chunk_00000, 88)
						if newCap <= 0 {
							trace_util_0.Count(_chunk_00000, 90)
							newCap = needCap
						}
					}
				}
			}
		}
		trace_util_0.Count(_chunk_00000, 79)
		dstCol.data = make([]byte, len(dstCol.data)+elemLen, newCap)
	}
	trace_util_0.Count(_chunk_00000, 75)
	return
}

// Insert inserts `row` on the position specified by `rowIdx`.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (c *Chunk) Insert(rowIdx int, row Row) {
	trace_util_0.Count(_chunk_00000, 91)
	for i, srcCol := range row.c.columns {
		trace_util_0.Count(_chunk_00000, 92)
		if row.IsNull(i) {
			trace_util_0.Count(_chunk_00000, 95)
			continue
		}
		trace_util_0.Count(_chunk_00000, 93)
		dstCol := c.columns[i]
		var srcStart, srcEnd, destStart, destEnd int
		if srcCol.isFixed() {
			trace_util_0.Count(_chunk_00000, 96)
			srcElemLen, destElemLen := len(srcCol.elemBuf), len(dstCol.elemBuf)
			srcStart, destStart = row.idx*srcElemLen, rowIdx*destElemLen
			srcEnd, destEnd = srcStart+srcElemLen, destStart+destElemLen
		} else {
			trace_util_0.Count(_chunk_00000, 97)
			{
				srcStart, srcEnd = int(srcCol.offsets[row.idx]), int(srcCol.offsets[row.idx+1])
				destStart, destEnd = int(dstCol.offsets[rowIdx]), int(dstCol.offsets[rowIdx+1])
			}
		}
		trace_util_0.Count(_chunk_00000, 94)
		copy(dstCol.data[destStart:destEnd], srcCol.data[srcStart:srcEnd])
	}
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	trace_util_0.Count(_chunk_00000, 98)
	for colID, src := range other.columns {
		trace_util_0.Count(_chunk_00000, 100)
		dst := c.columns[colID]
		if src.isFixed() {
			trace_util_0.Count(_chunk_00000, 102)
			elemLen := len(src.elemBuf)
			dst.data = append(dst.data, src.data[begin*elemLen:end*elemLen]...)
		} else {
			trace_util_0.Count(_chunk_00000, 103)
			{
				beginOffset, endOffset := src.offsets[begin], src.offsets[end]
				dst.data = append(dst.data, src.data[beginOffset:endOffset]...)
				for i := begin; i < end; i++ {
					trace_util_0.Count(_chunk_00000, 104)
					dst.offsets = append(dst.offsets, dst.offsets[len(dst.offsets)-1]+src.offsets[i+1]-src.offsets[i])
				}
			}
		}
		trace_util_0.Count(_chunk_00000, 101)
		for i := begin; i < end; i++ {
			trace_util_0.Count(_chunk_00000, 105)
			dst.appendNullBitmap(!src.isNull(i))
			dst.length++
		}
	}
	trace_util_0.Count(_chunk_00000, 99)
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	trace_util_0.Count(_chunk_00000, 106)
	for _, col := range c.columns {
		trace_util_0.Count(_chunk_00000, 108)
		if col.isFixed() {
			trace_util_0.Count(_chunk_00000, 111)
			elemLen := len(col.elemBuf)
			col.data = col.data[:numRows*elemLen]
		} else {
			trace_util_0.Count(_chunk_00000, 112)
			{
				col.data = col.data[:col.offsets[numRows]]
				col.offsets = col.offsets[:numRows+1]
			}
		}
		trace_util_0.Count(_chunk_00000, 109)
		for i := numRows; i < col.length; i++ {
			trace_util_0.Count(_chunk_00000, 113)
			if col.isNull(i) {
				trace_util_0.Count(_chunk_00000, 114)
				col.nullCount--
			}
		}
		trace_util_0.Count(_chunk_00000, 110)
		col.length = numRows
		bitmapLen := (col.length + 7) / 8
		col.nullBitmap = col.nullBitmap[:bitmapLen]
		if col.length%8 != 0 {
			trace_util_0.Count(_chunk_00000, 115)
			// When we append null, we simply increment the nullCount,
			// so we need to clear the unused bits in the last bitmap byte.
			lastByte := col.nullBitmap[bitmapLen-1]
			unusedBitsLen := 8 - uint(col.length%8)
			lastByte <<= unusedBitsLen
			lastByte >>= unusedBitsLen
			col.nullBitmap[bitmapLen-1] = lastByte
		}
	}
	trace_util_0.Count(_chunk_00000, 107)
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(colIdx int) {
	trace_util_0.Count(_chunk_00000, 116)
	c.columns[colIdx].appendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(colIdx int, i int64) {
	trace_util_0.Count(_chunk_00000, 117)
	c.columns[colIdx].appendInt64(i)
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(colIdx int, u uint64) {
	trace_util_0.Count(_chunk_00000, 118)
	c.columns[colIdx].appendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(colIdx int, f float32) {
	trace_util_0.Count(_chunk_00000, 119)
	c.columns[colIdx].appendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(colIdx int, f float64) {
	trace_util_0.Count(_chunk_00000, 120)
	c.columns[colIdx].appendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(colIdx int, str string) {
	trace_util_0.Count(_chunk_00000, 121)
	c.columns[colIdx].appendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(colIdx int, b []byte) {
	trace_util_0.Count(_chunk_00000, 122)
	c.columns[colIdx].appendBytes(b)
}

// AppendTime appends a Time value to the chunk.
// TODO: change the time structure so it can be directly written to memory.
func (c *Chunk) AppendTime(colIdx int, t types.Time) {
	trace_util_0.Count(_chunk_00000, 123)
	c.columns[colIdx].appendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(colIdx int, dur types.Duration) {
	trace_util_0.Count(_chunk_00000, 124)
	c.columns[colIdx].appendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(colIdx int, dec *types.MyDecimal) {
	trace_util_0.Count(_chunk_00000, 125)
	c.columns[colIdx].appendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(colIdx int, enum types.Enum) {
	trace_util_0.Count(_chunk_00000, 126)
	c.columns[colIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(colIdx int, set types.Set) {
	trace_util_0.Count(_chunk_00000, 127)
	c.columns[colIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(colIdx int, j json.BinaryJSON) {
	trace_util_0.Count(_chunk_00000, 128)
	c.columns[colIdx].appendJSON(j)
}

// AppendDatum appends a datum into the chunk.
func (c *Chunk) AppendDatum(colIdx int, d *types.Datum) {
	trace_util_0.Count(_chunk_00000, 129)
	switch d.Kind() {
	case types.KindNull:
		trace_util_0.Count(_chunk_00000, 130)
		c.AppendNull(colIdx)
	case types.KindInt64:
		trace_util_0.Count(_chunk_00000, 131)
		c.AppendInt64(colIdx, d.GetInt64())
	case types.KindUint64:
		trace_util_0.Count(_chunk_00000, 132)
		c.AppendUint64(colIdx, d.GetUint64())
	case types.KindFloat32:
		trace_util_0.Count(_chunk_00000, 133)
		c.AppendFloat32(colIdx, d.GetFloat32())
	case types.KindFloat64:
		trace_util_0.Count(_chunk_00000, 134)
		c.AppendFloat64(colIdx, d.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindRaw, types.KindMysqlBit:
		trace_util_0.Count(_chunk_00000, 135)
		c.AppendBytes(colIdx, d.GetBytes())
	case types.KindMysqlDecimal:
		trace_util_0.Count(_chunk_00000, 136)
		c.AppendMyDecimal(colIdx, d.GetMysqlDecimal())
	case types.KindMysqlDuration:
		trace_util_0.Count(_chunk_00000, 137)
		c.AppendDuration(colIdx, d.GetMysqlDuration())
	case types.KindMysqlEnum:
		trace_util_0.Count(_chunk_00000, 138)
		c.AppendEnum(colIdx, d.GetMysqlEnum())
	case types.KindMysqlSet:
		trace_util_0.Count(_chunk_00000, 139)
		c.AppendSet(colIdx, d.GetMysqlSet())
	case types.KindMysqlTime:
		trace_util_0.Count(_chunk_00000, 140)
		c.AppendTime(colIdx, d.GetMysqlTime())
	case types.KindMysqlJSON:
		trace_util_0.Count(_chunk_00000, 141)
		c.AppendJSON(colIdx, d.GetMysqlJSON())
	}
}

func writeTime(buf []byte, t types.Time) {
	trace_util_0.Count(_chunk_00000, 142)
	binary.BigEndian.PutUint16(buf, uint16(t.Time.Year()))
	buf[2] = uint8(t.Time.Month())
	buf[3] = uint8(t.Time.Day())
	buf[4] = uint8(t.Time.Hour())
	buf[5] = uint8(t.Time.Minute())
	buf[6] = uint8(t.Time.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Time.Microsecond()))
	buf[12] = t.Type
	buf[13] = uint8(t.Fsp)
}

func readTime(buf []byte) types.Time {
	trace_util_0.Count(_chunk_00000, 143)
	year := int(binary.BigEndian.Uint16(buf))
	month := int(buf[2])
	day := int(buf[3])
	hour := int(buf[4])
	minute := int(buf[5])
	second := int(buf[6])
	microseconds := int(binary.BigEndian.Uint32(buf[8:]))
	tp := buf[12]
	fsp := int(buf[13])
	return types.Time{
		Time: types.FromDate(year, month, day, hour, minute, second, microseconds),
		Type: tp,
		Fsp:  fsp,
	}
}

var _chunk_00000 = "util/chunk/chunk.go"
