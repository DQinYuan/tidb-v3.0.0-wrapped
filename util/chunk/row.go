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
	"time"
	"unsafe"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

// Row represents a row of data, can be used to access values.
type Row struct {
	c   *Chunk
	idx int
}

// IsEmpty returns true if the Row is empty.
func (r Row) IsEmpty() bool {
	trace_util_0.Count(_row_00000, 0)
	return r == Row{}
}

// Idx returns the row index of Chunk.
func (r Row) Idx() int {
	trace_util_0.Count(_row_00000, 1)
	return r.idx
}

// Len returns the number of values in the row.
func (r Row) Len() int {
	trace_util_0.Count(_row_00000, 2)
	return r.c.NumCols()
}

// GetInt64 returns the int64 value with the colIdx.
func (r Row) GetInt64(colIdx int) int64 {
	trace_util_0.Count(_row_00000, 3)
	col := r.c.columns[colIdx]
	return *(*int64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetUint64 returns the uint64 value with the colIdx.
func (r Row) GetUint64(colIdx int) uint64 {
	trace_util_0.Count(_row_00000, 4)
	col := r.c.columns[colIdx]
	return *(*uint64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetFloat32 returns the float32 value with the colIdx.
func (r Row) GetFloat32(colIdx int) float32 {
	trace_util_0.Count(_row_00000, 5)
	col := r.c.columns[colIdx]
	return *(*float32)(unsafe.Pointer(&col.data[r.idx*4]))
}

// GetFloat64 returns the float64 value with the colIdx.
func (r Row) GetFloat64(colIdx int) float64 {
	trace_util_0.Count(_row_00000, 6)
	col := r.c.columns[colIdx]
	return *(*float64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetString returns the string value with the colIdx.
func (r Row) GetString(colIdx int) string {
	trace_util_0.Count(_row_00000, 7)
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	str := string(hack.String(col.data[start:end]))
	return str
}

// GetBytes returns the bytes value with the colIdx.
func (r Row) GetBytes(colIdx int) []byte {
	trace_util_0.Count(_row_00000, 8)
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	return col.data[start:end]
}

// GetTime returns the Time value with the colIdx.
// TODO: use Time structure directly.
func (r Row) GetTime(colIdx int) types.Time {
	trace_util_0.Count(_row_00000, 9)
	col := r.c.columns[colIdx]
	return readTime(col.data[r.idx*16:])
}

// GetDuration returns the Duration value with the colIdx.
func (r Row) GetDuration(colIdx int, fillFsp int) types.Duration {
	trace_util_0.Count(_row_00000, 10)
	col := r.c.columns[colIdx]
	dur := *(*int64)(unsafe.Pointer(&col.data[r.idx*8]))
	return types.Duration{Duration: time.Duration(dur), Fsp: fillFsp}
}

func (r Row) getNameValue(colIdx int) (string, uint64) {
	trace_util_0.Count(_row_00000, 11)
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	if start == end {
		trace_util_0.Count(_row_00000, 13)
		return "", 0
	}
	trace_util_0.Count(_row_00000, 12)
	val := *(*uint64)(unsafe.Pointer(&col.data[start]))
	name := string(hack.String(col.data[start+8 : end]))
	return name, val
}

// GetEnum returns the Enum value with the colIdx.
func (r Row) GetEnum(colIdx int) types.Enum {
	trace_util_0.Count(_row_00000, 14)
	name, val := r.getNameValue(colIdx)
	return types.Enum{Name: name, Value: val}
}

// GetSet returns the Set value with the colIdx.
func (r Row) GetSet(colIdx int) types.Set {
	trace_util_0.Count(_row_00000, 15)
	name, val := r.getNameValue(colIdx)
	return types.Set{Name: name, Value: val}
}

// GetMyDecimal returns the MyDecimal value with the colIdx.
func (r Row) GetMyDecimal(colIdx int) *types.MyDecimal {
	trace_util_0.Count(_row_00000, 16)
	col := r.c.columns[colIdx]
	return (*types.MyDecimal)(unsafe.Pointer(&col.data[r.idx*types.MyDecimalStructSize]))
}

// GetJSON returns the JSON value with the colIdx.
func (r Row) GetJSON(colIdx int) json.BinaryJSON {
	trace_util_0.Count(_row_00000, 17)
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	return json.BinaryJSON{TypeCode: col.data[start], Value: col.data[start+1 : end]}
}

// GetDatumRow converts chunk.Row to types.DatumRow.
// Keep in mind that GetDatumRow has a reference to r.c, which is a chunk,
// this function works only if the underlying chunk is valid or unchanged.
func (r Row) GetDatumRow(fields []*types.FieldType) []types.Datum {
	trace_util_0.Count(_row_00000, 18)
	datumRow := make([]types.Datum, 0, r.c.NumCols())
	for colIdx := 0; colIdx < r.c.NumCols(); colIdx++ {
		trace_util_0.Count(_row_00000, 20)
		datum := r.GetDatum(colIdx, fields[colIdx])
		datumRow = append(datumRow, datum)
	}
	trace_util_0.Count(_row_00000, 19)
	return datumRow
}

// GetDatum implements the chunk.Row interface.
func (r Row) GetDatum(colIdx int, tp *types.FieldType) types.Datum {
	trace_util_0.Count(_row_00000, 21)
	var d types.Datum
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_row_00000, 23)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 35)
			if mysql.HasUnsignedFlag(tp.Flag) {
				trace_util_0.Count(_row_00000, 36)
				d.SetUint64(r.GetUint64(colIdx))
			} else {
				trace_util_0.Count(_row_00000, 37)
				{
					d.SetInt64(r.GetInt64(colIdx))
				}
			}
		}
	case mysql.TypeYear:
		trace_util_0.Count(_row_00000, 24)
		// FIXBUG: because insert type of TypeYear is definite int64, so we regardless of the unsigned flag.
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 38)
			d.SetInt64(r.GetInt64(colIdx))
		}
	case mysql.TypeFloat:
		trace_util_0.Count(_row_00000, 25)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 39)
			d.SetFloat32(r.GetFloat32(colIdx))
		}
	case mysql.TypeDouble:
		trace_util_0.Count(_row_00000, 26)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 40)
			d.SetFloat64(r.GetFloat64(colIdx))
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_row_00000, 27)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 41)
			d.SetBytes(r.GetBytes(colIdx))
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_row_00000, 28)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 42)
			d.SetMysqlTime(r.GetTime(colIdx))
		}
	case mysql.TypeDuration:
		trace_util_0.Count(_row_00000, 29)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 43)
			duration := r.GetDuration(colIdx, tp.Decimal)
			d.SetMysqlDuration(duration)
		}
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_row_00000, 30)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 44)
			d.SetMysqlDecimal(r.GetMyDecimal(colIdx))
			d.SetLength(tp.Flen)
			// If tp.Decimal is unspecified(-1), we should set it to the real
			// fraction length of the decimal value, if not, the d.Frac will
			// be set to MAX_UINT16 which will cause unexpected BadNumber error
			// when encoding.
			if tp.Decimal == types.UnspecifiedLength {
				trace_util_0.Count(_row_00000, 45)
				d.SetFrac(d.Frac())
			} else {
				trace_util_0.Count(_row_00000, 46)
				{
					d.SetFrac(tp.Decimal)
				}
			}
		}
	case mysql.TypeEnum:
		trace_util_0.Count(_row_00000, 31)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 47)
			d.SetMysqlEnum(r.GetEnum(colIdx))
		}
	case mysql.TypeSet:
		trace_util_0.Count(_row_00000, 32)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 48)
			d.SetMysqlSet(r.GetSet(colIdx))
		}
	case mysql.TypeBit:
		trace_util_0.Count(_row_00000, 33)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 49)
			d.SetMysqlBit(r.GetBytes(colIdx))
		}
	case mysql.TypeJSON:
		trace_util_0.Count(_row_00000, 34)
		if !r.IsNull(colIdx) {
			trace_util_0.Count(_row_00000, 50)
			d.SetMysqlJSON(r.GetJSON(colIdx))
		}
	}
	trace_util_0.Count(_row_00000, 22)
	return d
}

// IsNull returns if the datum in the chunk.Row is null.
func (r Row) IsNull(colIdx int) bool {
	trace_util_0.Count(_row_00000, 51)
	return r.c.columns[colIdx].isNull(r.idx)
}

var _row_00000 = "util/chunk/row.go"
