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

import (
	"unsafe"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (c *column) appendDuration(dur types.Duration) {
	trace_util_0.Count(_column_00000, 0)
	c.appendInt64(int64(dur.Duration))
}

func (c *column) appendMyDecimal(dec *types.MyDecimal) {
	trace_util_0.Count(_column_00000, 1)
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *column) appendNameValue(name string, val uint64) {
	trace_util_0.Count(_column_00000, 2)
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

func (c *column) appendJSON(j json.BinaryJSON) {
	trace_util_0.Count(_column_00000, 3)
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

type column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

func (c *column) isFixed() bool {
	trace_util_0.Count(_column_00000, 4)
	return c.elemBuf != nil
}

func (c *column) reset() {
	trace_util_0.Count(_column_00000, 5)
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		trace_util_0.Count(_column_00000, 7)
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	trace_util_0.Count(_column_00000, 6)
	c.data = c.data[:0]
}

func (c *column) isNull(rowIdx int) bool {
	trace_util_0.Count(_column_00000, 8)
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *column) copyConstruct() *column {
	trace_util_0.Count(_column_00000, 9)
	newCol := &column{length: c.length, nullCount: c.nullCount}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}

func (c *column) appendNullBitmap(notNull bool) {
	trace_util_0.Count(_column_00000, 10)
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		trace_util_0.Count(_column_00000, 12)
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	trace_util_0.Count(_column_00000, 11)
	if notNull {
		trace_util_0.Count(_column_00000, 13)
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	} else {
		trace_util_0.Count(_column_00000, 14)
		{
			c.nullCount++
		}
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *column) appendMultiSameNullBitmap(notNull bool, num int) {
	trace_util_0.Count(_column_00000, 15)
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		trace_util_0.Count(_column_00000, 19)
		b = 0xff
	}
	trace_util_0.Count(_column_00000, 16)
	for i := 0; i < numNewBytes; i++ {
		trace_util_0.Count(_column_00000, 20)
		c.nullBitmap = append(c.nullBitmap, b)
	}
	trace_util_0.Count(_column_00000, 17)
	if !notNull {
		trace_util_0.Count(_column_00000, 21)
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	trace_util_0.Count(_column_00000, 18)
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

func (c *column) appendNull() {
	trace_util_0.Count(_column_00000, 22)
	c.appendNullBitmap(false)
	if c.isFixed() {
		trace_util_0.Count(_column_00000, 24)
		c.data = append(c.data, c.elemBuf...)
	} else {
		trace_util_0.Count(_column_00000, 25)
		{
			c.offsets = append(c.offsets, c.offsets[c.length])
		}
	}
	trace_util_0.Count(_column_00000, 23)
	c.length++
}

func (c *column) finishAppendFixed() {
	trace_util_0.Count(_column_00000, 26)
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

func (c *column) appendInt64(i int64) {
	trace_util_0.Count(_column_00000, 27)
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

func (c *column) appendUint64(u uint64) {
	trace_util_0.Count(_column_00000, 28)
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

func (c *column) appendFloat32(f float32) {
	trace_util_0.Count(_column_00000, 29)
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) appendFloat64(f float64) {
	trace_util_0.Count(_column_00000, 30)
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) finishAppendVar() {
	trace_util_0.Count(_column_00000, 31)
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

func (c *column) appendString(str string) {
	trace_util_0.Count(_column_00000, 32)
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

func (c *column) appendBytes(b []byte) {
	trace_util_0.Count(_column_00000, 33)
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *column) appendTime(t types.Time) {
	trace_util_0.Count(_column_00000, 34)
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}

var _column_00000 = "util/chunk/column.go"
