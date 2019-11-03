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
	"math"
	"unsafe"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

// MutRow represents a mutable Row.
// The underlying columns only contains one row and not exposed to the user.
type MutRow Row

// ToRow converts the MutRow to Row, so it can be used to read data.
func (mr MutRow) ToRow() Row {
	trace_util_0.Count(_mutrow_00000, 0)
	return Row(mr)
}

// Len returns the number of columns.
func (mr MutRow) Len() int {
	trace_util_0.Count(_mutrow_00000, 1)
	return len(mr.c.columns)
}

// MutRowFromValues creates a MutRow from a interface slice.
func MutRowFromValues(vals ...interface{}) MutRow {
	trace_util_0.Count(_mutrow_00000, 2)
	c := &Chunk{columns: make([]*column, 0, len(vals))}
	for _, val := range vals {
		trace_util_0.Count(_mutrow_00000, 4)
		col := makeMutRowColumn(val)
		c.columns = append(c.columns, col)
	}
	trace_util_0.Count(_mutrow_00000, 3)
	return MutRow{c: c}
}

// MutRowFromDatums creates a MutRow from a datum slice.
func MutRowFromDatums(datums []types.Datum) MutRow {
	trace_util_0.Count(_mutrow_00000, 5)
	c := &Chunk{columns: make([]*column, 0, len(datums))}
	for _, d := range datums {
		trace_util_0.Count(_mutrow_00000, 7)
		col := makeMutRowColumn(d.GetValue())
		c.columns = append(c.columns, col)
	}
	trace_util_0.Count(_mutrow_00000, 6)
	return MutRow{c: c, idx: 0}
}

// MutRowFromTypes creates a MutRow from a FieldType slice, each column is initialized to zero value.
func MutRowFromTypes(types []*types.FieldType) MutRow {
	trace_util_0.Count(_mutrow_00000, 8)
	c := &Chunk{columns: make([]*column, 0, len(types))}
	for _, tp := range types {
		trace_util_0.Count(_mutrow_00000, 10)
		col := makeMutRowColumn(zeroValForType(tp))
		c.columns = append(c.columns, col)
	}
	trace_util_0.Count(_mutrow_00000, 9)
	return MutRow{c: c, idx: 0}
}

func zeroValForType(tp *types.FieldType) interface{} {
	trace_util_0.Count(_mutrow_00000, 11)
	switch tp.Tp {
	case mysql.TypeFloat:
		trace_util_0.Count(_mutrow_00000, 12)
		return float32(0)
	case mysql.TypeDouble:
		trace_util_0.Count(_mutrow_00000, 13)
		return float64(0)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		trace_util_0.Count(_mutrow_00000, 14)
		if mysql.HasUnsignedFlag(tp.Flag) {
			trace_util_0.Count(_mutrow_00000, 28)
			return uint64(0)
		}
		trace_util_0.Count(_mutrow_00000, 15)
		return int64(0)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		trace_util_0.Count(_mutrow_00000, 16)
		return ""
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_mutrow_00000, 17)
		return []byte{}
	case mysql.TypeDuration:
		trace_util_0.Count(_mutrow_00000, 18)
		return types.ZeroDuration
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_mutrow_00000, 19)
		return types.NewDecFromInt(0)
	case mysql.TypeDate:
		trace_util_0.Count(_mutrow_00000, 20)
		return types.ZeroDate
	case mysql.TypeDatetime:
		trace_util_0.Count(_mutrow_00000, 21)
		return types.ZeroDatetime
	case mysql.TypeTimestamp:
		trace_util_0.Count(_mutrow_00000, 22)
		return types.ZeroTimestamp
	case mysql.TypeBit:
		trace_util_0.Count(_mutrow_00000, 23)
		return types.BinaryLiteral{}
	case mysql.TypeSet:
		trace_util_0.Count(_mutrow_00000, 24)
		return types.Set{}
	case mysql.TypeEnum:
		trace_util_0.Count(_mutrow_00000, 25)
		return types.Enum{}
	case mysql.TypeJSON:
		trace_util_0.Count(_mutrow_00000, 26)
		return json.CreateBinary(nil)
	default:
		trace_util_0.Count(_mutrow_00000, 27)
		return nil
	}
}

func makeMutRowColumn(in interface{}) *column {
	trace_util_0.Count(_mutrow_00000, 29)
	switch x := in.(type) {
	case nil:
		trace_util_0.Count(_mutrow_00000, 30)
		col := makeMutRowUint64Column(uint64(0))
		col.nullBitmap[0] = 0
		return col
	case int:
		trace_util_0.Count(_mutrow_00000, 31)
		return makeMutRowUint64Column(uint64(x))
	case int64:
		trace_util_0.Count(_mutrow_00000, 32)
		return makeMutRowUint64Column(uint64(x))
	case uint64:
		trace_util_0.Count(_mutrow_00000, 33)
		return makeMutRowUint64Column(x)
	case float64:
		trace_util_0.Count(_mutrow_00000, 34)
		return makeMutRowUint64Column(math.Float64bits(x))
	case float32:
		trace_util_0.Count(_mutrow_00000, 35)
		col := newMutRowFixedLenColumn(4)
		*(*uint32)(unsafe.Pointer(&col.data[0])) = math.Float32bits(x)
		return col
	case string:
		trace_util_0.Count(_mutrow_00000, 36)
		return makeMutRowBytesColumn(hack.Slice(x))
	case []byte:
		trace_util_0.Count(_mutrow_00000, 37)
		return makeMutRowBytesColumn(x)
	case types.BinaryLiteral:
		trace_util_0.Count(_mutrow_00000, 38)
		return makeMutRowBytesColumn(x)
	case *types.MyDecimal:
		trace_util_0.Count(_mutrow_00000, 39)
		col := newMutRowFixedLenColumn(types.MyDecimalStructSize)
		*(*types.MyDecimal)(unsafe.Pointer(&col.data[0])) = *x
		return col
	case types.Time:
		trace_util_0.Count(_mutrow_00000, 40)
		col := newMutRowFixedLenColumn(16)
		writeTime(col.data, x)
		return col
	case json.BinaryJSON:
		trace_util_0.Count(_mutrow_00000, 41)
		col := newMutRowVarLenColumn(len(x.Value) + 1)
		col.data[0] = x.TypeCode
		copy(col.data[1:], x.Value)
		return col
	case types.Duration:
		trace_util_0.Count(_mutrow_00000, 42)
		col := newMutRowFixedLenColumn(8)
		*(*int64)(unsafe.Pointer(&col.data[0])) = int64(x.Duration)
		return col
	case types.Enum:
		trace_util_0.Count(_mutrow_00000, 43)
		col := newMutRowVarLenColumn(len(x.Name) + 8)
		*(*uint64)(unsafe.Pointer(&col.data[0])) = x.Value
		copy(col.data[8:], x.Name)
		return col
	case types.Set:
		trace_util_0.Count(_mutrow_00000, 44)
		col := newMutRowVarLenColumn(len(x.Name) + 8)
		*(*uint64)(unsafe.Pointer(&col.data[0])) = x.Value
		copy(col.data[8:], x.Name)
		return col
	default:
		trace_util_0.Count(_mutrow_00000, 45)
		return nil
	}
}

func newMutRowFixedLenColumn(elemSize int) *column {
	trace_util_0.Count(_mutrow_00000, 46)
	buf := make([]byte, elemSize+1)
	col := &column{
		length:     1,
		elemBuf:    buf[:elemSize],
		data:       buf[:elemSize],
		nullBitmap: buf[elemSize:],
	}
	col.nullBitmap[0] = 1
	return col
}

func newMutRowVarLenColumn(valSize int) *column {
	trace_util_0.Count(_mutrow_00000, 47)
	buf := make([]byte, valSize+1)
	col := &column{
		length:     1,
		offsets:    []int64{0, int64(valSize)},
		data:       buf[:valSize],
		nullBitmap: buf[valSize:],
	}
	col.nullBitmap[0] = 1
	return col
}

func makeMutRowUint64Column(val uint64) *column {
	trace_util_0.Count(_mutrow_00000, 48)
	col := newMutRowFixedLenColumn(8)
	*(*uint64)(unsafe.Pointer(&col.data[0])) = val
	return col
}

func makeMutRowBytesColumn(bin []byte) *column {
	trace_util_0.Count(_mutrow_00000, 49)
	col := newMutRowVarLenColumn(len(bin))
	copy(col.data, bin)
	col.nullBitmap[0] = 1
	return col
}

// SetRow sets the MutRow with Row.
func (mr MutRow) SetRow(row Row) {
	trace_util_0.Count(_mutrow_00000, 50)
	for colIdx, rCol := range row.c.columns {
		trace_util_0.Count(_mutrow_00000, 51)
		mrCol := mr.c.columns[colIdx]
		if rCol.isNull(row.idx) {
			trace_util_0.Count(_mutrow_00000, 54)
			mrCol.nullBitmap[0] = 0
			continue
		}
		trace_util_0.Count(_mutrow_00000, 52)
		elemLen := len(rCol.elemBuf)
		if elemLen > 0 {
			trace_util_0.Count(_mutrow_00000, 55)
			copy(mrCol.data, rCol.data[row.idx*elemLen:(row.idx+1)*elemLen])
		} else {
			trace_util_0.Count(_mutrow_00000, 56)
			{
				setMutRowBytes(mrCol, rCol.data[rCol.offsets[row.idx]:rCol.offsets[row.idx+1]])
			}
		}
		trace_util_0.Count(_mutrow_00000, 53)
		mrCol.nullBitmap[0] = 1
	}
}

// SetValues sets the MutRow with values.
func (mr MutRow) SetValues(vals ...interface{}) {
	trace_util_0.Count(_mutrow_00000, 57)
	for i, v := range vals {
		trace_util_0.Count(_mutrow_00000, 58)
		mr.SetValue(i, v)
	}
}

// SetValue sets the MutRow with colIdx and value.
func (mr MutRow) SetValue(colIdx int, val interface{}) {
	trace_util_0.Count(_mutrow_00000, 59)
	col := mr.c.columns[colIdx]
	if val == nil {
		trace_util_0.Count(_mutrow_00000, 62)
		col.nullBitmap[0] = 0
		return
	}
	trace_util_0.Count(_mutrow_00000, 60)
	switch x := val.(type) {
	case int:
		trace_util_0.Count(_mutrow_00000, 63)
		binary.LittleEndian.PutUint64(col.data, uint64(x))
	case int64:
		trace_util_0.Count(_mutrow_00000, 64)
		binary.LittleEndian.PutUint64(col.data, uint64(x))
	case uint64:
		trace_util_0.Count(_mutrow_00000, 65)
		binary.LittleEndian.PutUint64(col.data, x)
	case float64:
		trace_util_0.Count(_mutrow_00000, 66)
		binary.LittleEndian.PutUint64(col.data, math.Float64bits(x))
	case float32:
		trace_util_0.Count(_mutrow_00000, 67)
		binary.LittleEndian.PutUint32(col.data, math.Float32bits(x))
	case string:
		trace_util_0.Count(_mutrow_00000, 68)
		setMutRowBytes(col, hack.Slice(x))
	case []byte:
		trace_util_0.Count(_mutrow_00000, 69)
		setMutRowBytes(col, x)
	case types.BinaryLiteral:
		trace_util_0.Count(_mutrow_00000, 70)
		setMutRowBytes(col, x)
	case types.Duration:
		trace_util_0.Count(_mutrow_00000, 71)
		*(*int64)(unsafe.Pointer(&col.data[0])) = int64(x.Duration)
	case *types.MyDecimal:
		trace_util_0.Count(_mutrow_00000, 72)
		*(*types.MyDecimal)(unsafe.Pointer(&col.data[0])) = *x
	case types.Time:
		trace_util_0.Count(_mutrow_00000, 73)
		writeTime(col.data, x)
	case types.Enum:
		trace_util_0.Count(_mutrow_00000, 74)
		setMutRowNameValue(col, x.Name, x.Value)
	case types.Set:
		trace_util_0.Count(_mutrow_00000, 75)
		setMutRowNameValue(col, x.Name, x.Value)
	case json.BinaryJSON:
		trace_util_0.Count(_mutrow_00000, 76)
		setMutRowJSON(col, x)
	}
	trace_util_0.Count(_mutrow_00000, 61)
	col.nullBitmap[0] = 1
}

// SetDatums sets the MutRow with datum slice.
func (mr MutRow) SetDatums(datums ...types.Datum) {
	trace_util_0.Count(_mutrow_00000, 77)
	for i, d := range datums {
		trace_util_0.Count(_mutrow_00000, 78)
		mr.SetDatum(i, d)
	}
}

// SetDatum sets the MutRow with colIdx and datum.
func (mr MutRow) SetDatum(colIdx int, d types.Datum) {
	trace_util_0.Count(_mutrow_00000, 79)
	col := mr.c.columns[colIdx]
	if d.IsNull() {
		trace_util_0.Count(_mutrow_00000, 82)
		col.nullBitmap[0] = 0
		return
	}
	trace_util_0.Count(_mutrow_00000, 80)
	switch d.Kind() {
	case types.KindInt64, types.KindUint64, types.KindFloat64:
		trace_util_0.Count(_mutrow_00000, 83)
		binary.LittleEndian.PutUint64(mr.c.columns[colIdx].data, d.GetUint64())
	case types.KindFloat32:
		trace_util_0.Count(_mutrow_00000, 84)
		binary.LittleEndian.PutUint32(mr.c.columns[colIdx].data, math.Float32bits(d.GetFloat32()))
	case types.KindString, types.KindBytes, types.KindBinaryLiteral:
		trace_util_0.Count(_mutrow_00000, 85)
		setMutRowBytes(col, d.GetBytes())
	case types.KindMysqlTime:
		trace_util_0.Count(_mutrow_00000, 86)
		writeTime(col.data, d.GetMysqlTime())
	case types.KindMysqlDuration:
		trace_util_0.Count(_mutrow_00000, 87)
		*(*int64)(unsafe.Pointer(&col.data[0])) = int64(d.GetMysqlDuration().Duration)
	case types.KindMysqlDecimal:
		trace_util_0.Count(_mutrow_00000, 88)
		*(*types.MyDecimal)(unsafe.Pointer(&col.data[0])) = *d.GetMysqlDecimal()
	case types.KindMysqlJSON:
		trace_util_0.Count(_mutrow_00000, 89)
		setMutRowJSON(col, d.GetMysqlJSON())
	case types.KindMysqlEnum:
		trace_util_0.Count(_mutrow_00000, 90)
		e := d.GetMysqlEnum()
		setMutRowNameValue(col, e.Name, e.Value)
	case types.KindMysqlSet:
		trace_util_0.Count(_mutrow_00000, 91)
		s := d.GetMysqlSet()
		setMutRowNameValue(col, s.Name, s.Value)
	default:
		trace_util_0.Count(_mutrow_00000, 92)
		mr.c.columns[colIdx] = makeMutRowColumn(d.GetValue())
	}
	trace_util_0.Count(_mutrow_00000, 81)
	col.nullBitmap[0] = 1
}

func setMutRowBytes(col *column, bin []byte) {
	trace_util_0.Count(_mutrow_00000, 93)
	if len(col.data) >= len(bin) {
		trace_util_0.Count(_mutrow_00000, 95)
		col.data = col.data[:len(bin)]
	} else {
		trace_util_0.Count(_mutrow_00000, 96)
		{
			buf := make([]byte, len(bin)+1)
			col.data = buf[:len(bin)]
			col.nullBitmap = buf[len(bin):]
		}
	}
	trace_util_0.Count(_mutrow_00000, 94)
	copy(col.data, bin)
	col.offsets[1] = int64(len(bin))
}

func setMutRowNameValue(col *column, name string, val uint64) {
	trace_util_0.Count(_mutrow_00000, 97)
	dataLen := len(name) + 8
	if len(col.data) >= dataLen {
		trace_util_0.Count(_mutrow_00000, 99)
		col.data = col.data[:dataLen]
	} else {
		trace_util_0.Count(_mutrow_00000, 100)
		{
			buf := make([]byte, dataLen+1)
			col.data = buf[:dataLen]
			col.nullBitmap = buf[dataLen:]
		}
	}
	trace_util_0.Count(_mutrow_00000, 98)
	binary.LittleEndian.PutUint64(col.data, val)
	copy(col.data[8:], name)
	col.offsets[1] = int64(dataLen)
}

func setMutRowJSON(col *column, j json.BinaryJSON) {
	trace_util_0.Count(_mutrow_00000, 101)
	dataLen := len(j.Value) + 1
	if len(col.data) >= dataLen {
		trace_util_0.Count(_mutrow_00000, 103)
		col.data = col.data[:dataLen]
	} else {
		trace_util_0.Count(_mutrow_00000, 104)
		{
			// In MutRow, there always exists 1 data in every column,
			// we should allocate one more byte for null bitmap.
			buf := make([]byte, dataLen+1)
			col.data = buf[:dataLen]
			col.nullBitmap = buf[dataLen:]
		}
	}
	trace_util_0.Count(_mutrow_00000, 102)
	col.data[0] = j.TypeCode
	copy(col.data[1:], j.Value)
	col.offsets[1] = int64(dataLen)
}

// ShallowCopyPartialRow shallow copies the data of `row` to MutRow.
func (mr MutRow) ShallowCopyPartialRow(colIdx int, row Row) {
	trace_util_0.Count(_mutrow_00000, 105)
	for i, srcCol := range row.c.columns {
		trace_util_0.Count(_mutrow_00000, 106)
		dstCol := mr.c.columns[colIdx+i]
		if !srcCol.isNull(row.idx) {
			trace_util_0.Count(_mutrow_00000, 108)
			// MutRow only contains one row, so we can directly set the whole byte.
			dstCol.nullBitmap[0] = 1
		} else {
			trace_util_0.Count(_mutrow_00000, 109)
			{
				dstCol.nullBitmap[0] = 0
			}
		}

		trace_util_0.Count(_mutrow_00000, 107)
		if srcCol.isFixed() {
			trace_util_0.Count(_mutrow_00000, 110)
			elemLen := len(srcCol.elemBuf)
			offset := row.idx * elemLen
			dstCol.data = srcCol.data[offset : offset+elemLen]
		} else {
			trace_util_0.Count(_mutrow_00000, 111)
			{
				start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+1]
				dstCol.data = srcCol.data[start:end]
				dstCol.offsets[1] = int64(len(dstCol.data))
			}
		}
	}
}

var _mutrow_00000 = "util/chunk/mutrow.go"
