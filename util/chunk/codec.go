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
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// Codec is used to:
// 1. encode a Chunk to a byte slice.
// 2. decode a Chunk from a byte slice.
type Codec struct {
	// colTypes is used to check whether a column is fixed sized and what the
	// fixed size for every element.
	// NOTE: It's only used for decoding.
	colTypes []*types.FieldType
}

// NewCodec creates a new Codec object for encode or decode a Chunk.
func NewCodec(colTypes []*types.FieldType) *Codec {
	trace_util_0.Count(_codec_00000, 0)
	return &Codec{colTypes}
}

// Encode encodes a Chunk to a byte slice.
func (c *Codec) Encode(chk *Chunk) []byte {
	trace_util_0.Count(_codec_00000, 1)
	buffer := make([]byte, 0, chk.MemoryUsage())
	for _, col := range chk.columns {
		trace_util_0.Count(_codec_00000, 3)
		buffer = c.encodeColumn(buffer, col)
	}
	trace_util_0.Count(_codec_00000, 2)
	return buffer
}

func (c *Codec) encodeColumn(buffer []byte, col *column) []byte {
	trace_util_0.Count(_codec_00000, 4)
	var lenBuffer [4]byte
	// encode length.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(col.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(col.nullCount))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullBitmap.
	if col.nullCount > 0 {
		trace_util_0.Count(_codec_00000, 7)
		numNullBitmapBytes := (col.length + 7) / 8
		buffer = append(buffer, col.nullBitmap[:numNullBitmapBytes]...)
	}

	// encode offsets.
	trace_util_0.Count(_codec_00000, 5)
	if !col.isFixed() {
		trace_util_0.Count(_codec_00000, 8)
		numOffsetBytes := (col.length + 1) * 8
		offsetBytes := c.i64SliceToBytes(col.offsets)
		buffer = append(buffer, offsetBytes[:numOffsetBytes]...)
	}

	// encode data.
	trace_util_0.Count(_codec_00000, 6)
	buffer = append(buffer, col.data...)
	return buffer
}

func (c *Codec) i64SliceToBytes(i64s []int64) (b []byte) {
	trace_util_0.Count(_codec_00000, 9)
	if len(i64s) == 0 {
		trace_util_0.Count(_codec_00000, 11)
		return nil
	}
	trace_util_0.Count(_codec_00000, 10)
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(i64s) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&i64s[0]))
	return b
}

// Decode decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) Decode(buffer []byte) (*Chunk, []byte) {
	trace_util_0.Count(_codec_00000, 12)
	chk := &Chunk{}
	for ordinal := 0; len(buffer) > 0; ordinal++ {
		trace_util_0.Count(_codec_00000, 14)
		col := &column{}
		buffer = c.decodeColumn(buffer, col, ordinal)
		chk.columns = append(chk.columns, col)
	}
	trace_util_0.Count(_codec_00000, 13)
	return chk, buffer
}

// DecodeToChunk decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) DecodeToChunk(buffer []byte, chk *Chunk) (remained []byte) {
	trace_util_0.Count(_codec_00000, 15)
	for i := 0; i < len(chk.columns); i++ {
		trace_util_0.Count(_codec_00000, 17)
		buffer = c.decodeColumn(buffer, chk.columns[i], i)
	}
	trace_util_0.Count(_codec_00000, 16)
	return buffer
}

func (c *Codec) decodeColumn(buffer []byte, col *column, ordinal int) (remained []byte) {
	trace_util_0.Count(_codec_00000, 18)
	// decode length.
	col.length = int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullCount.
	col.nullCount = int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullBitmap.
	if col.nullCount > 0 {
		trace_util_0.Count(_codec_00000, 21)
		numNullBitmapBytes := (col.length + 7) / 8
		col.nullBitmap = append(col.nullBitmap[:0], buffer[:numNullBitmapBytes]...)
		buffer = buffer[numNullBitmapBytes:]
	} else {
		trace_util_0.Count(_codec_00000, 22)
		{
			c.setAllNotNull(col)
		}
	}

	// decode offsets.
	trace_util_0.Count(_codec_00000, 19)
	numFixedBytes := getFixedLen(c.colTypes[ordinal])
	numDataBytes := int64(numFixedBytes * col.length)
	if numFixedBytes == -1 {
		trace_util_0.Count(_codec_00000, 23)
		numOffsetBytes := (col.length + 1) * 8
		col.offsets = append(col.offsets[:0], c.bytesToI64Slice(buffer[:numOffsetBytes])...)
		buffer = buffer[numOffsetBytes:]
		numDataBytes = col.offsets[col.length]
	} else {
		trace_util_0.Count(_codec_00000, 24)
		if cap(col.elemBuf) < numFixedBytes {
			trace_util_0.Count(_codec_00000, 25)
			col.elemBuf = make([]byte, numFixedBytes)
		}
	}

	// decode data.
	trace_util_0.Count(_codec_00000, 20)
	col.data = append(col.data[:0], buffer[:numDataBytes]...)
	return buffer[numDataBytes:]
}

var allNotNullBitmap [128]byte

func (c *Codec) setAllNotNull(col *column) {
	trace_util_0.Count(_codec_00000, 26)
	numNullBitmapBytes := (col.length + 7) / 8
	col.nullBitmap = col.nullBitmap[:0]
	for i := 0; i < numNullBitmapBytes; {
		trace_util_0.Count(_codec_00000, 27)
		numAppendBytes := mathutil.Min(numNullBitmapBytes-i, cap(allNotNullBitmap))
		col.nullBitmap = append(col.nullBitmap, allNotNullBitmap[:numAppendBytes]...)
		i += numAppendBytes
	}
}

func (c *Codec) bytesToI64Slice(b []byte) (i64s []int64) {
	trace_util_0.Count(_codec_00000, 28)
	if len(b) == 0 {
		trace_util_0.Count(_codec_00000, 30)
		return nil
	}
	trace_util_0.Count(_codec_00000, 29)
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&i64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return i64s
}

// varElemLen indicates this column is a variable length column.
const varElemLen = -1

func getFixedLen(colType *types.FieldType) int {
	trace_util_0.Count(_codec_00000, 31)
	switch colType.Tp {
	case mysql.TypeFloat:
		trace_util_0.Count(_codec_00000, 32)
		return 4
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeYear, mysql.TypeDuration:
		trace_util_0.Count(_codec_00000, 33)
		return 8
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_codec_00000, 34)
		return 16
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_codec_00000, 35)
		return types.MyDecimalStructSize
	default:
		trace_util_0.Count(_codec_00000, 36)
		return varElemLen
	}
}

func init() {
	trace_util_0.Count(_codec_00000, 37)
	for i := 0; i < 128; i++ {
		trace_util_0.Count(_codec_00000, 38)
		allNotNullBitmap[i] = 0xFF
	}
}

var _codec_00000 = "util/chunk/codec.go"
