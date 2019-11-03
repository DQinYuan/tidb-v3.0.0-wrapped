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
	"sort"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// CompareFunc is a function to compare the two values in Row, the two columns must have the same type.
type CompareFunc = func(l Row, lCol int, r Row, rCol int) int

// GetCompareFunc gets a compare function for the field type.
func GetCompareFunc(tp *types.FieldType) CompareFunc {
	trace_util_0.Count(_compare_00000, 0)
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		trace_util_0.Count(_compare_00000, 2)
		if mysql.HasUnsignedFlag(tp.Flag) {
			trace_util_0.Count(_compare_00000, 13)
			return cmpUint64
		}
		trace_util_0.Count(_compare_00000, 3)
		return cmpInt64
	case mysql.TypeFloat:
		trace_util_0.Count(_compare_00000, 4)
		return cmpFloat32
	case mysql.TypeDouble:
		trace_util_0.Count(_compare_00000, 5)
		return cmpFloat64
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_compare_00000, 6)
		return cmpString
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_compare_00000, 7)
		return cmpTime
	case mysql.TypeDuration:
		trace_util_0.Count(_compare_00000, 8)
		return cmpDuration
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_compare_00000, 9)
		return cmpMyDecimal
	case mysql.TypeSet, mysql.TypeEnum:
		trace_util_0.Count(_compare_00000, 10)
		return cmpNameValue
	case mysql.TypeBit:
		trace_util_0.Count(_compare_00000, 11)
		return cmpBit
	case mysql.TypeJSON:
		trace_util_0.Count(_compare_00000, 12)
		return cmpJSON
	}
	trace_util_0.Count(_compare_00000, 1)
	return nil
}

func cmpNull(lNull, rNull bool) int {
	trace_util_0.Count(_compare_00000, 14)
	if lNull && rNull {
		trace_util_0.Count(_compare_00000, 17)
		return 0
	}
	trace_util_0.Count(_compare_00000, 15)
	if lNull {
		trace_util_0.Count(_compare_00000, 18)
		return -1
	}
	trace_util_0.Count(_compare_00000, 16)
	return 1
}

func cmpInt64(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 19)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 21)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 20)
	return types.CompareInt64(l.GetInt64(lCol), r.GetInt64(rCol))
}

func cmpUint64(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 22)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 24)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 23)
	return types.CompareUint64(l.GetUint64(lCol), r.GetUint64(rCol))
}

func cmpString(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 25)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 27)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 26)
	return types.CompareString(l.GetString(lCol), r.GetString(rCol))
}

func cmpFloat32(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 28)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 30)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 29)
	return types.CompareFloat64(float64(l.GetFloat32(lCol)), float64(r.GetFloat32(rCol)))
}

func cmpFloat64(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 31)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 33)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 32)
	return types.CompareFloat64(l.GetFloat64(lCol), r.GetFloat64(rCol))
}

func cmpMyDecimal(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 34)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 36)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 35)
	lDec, rDec := l.GetMyDecimal(lCol), r.GetMyDecimal(rCol)
	return lDec.Compare(rDec)
}

func cmpTime(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 37)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 39)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 38)
	lTime, rTime := l.GetTime(lCol), r.GetTime(rCol)
	return lTime.Compare(rTime)
}

func cmpDuration(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 40)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 42)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 41)
	lDur, rDur := l.GetDuration(lCol, 0).Duration, r.GetDuration(rCol, 0).Duration
	return types.CompareInt64(int64(lDur), int64(rDur))
}

func cmpNameValue(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 43)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 45)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 44)
	_, lVal := l.getNameValue(lCol)
	_, rVal := r.getNameValue(rCol)
	return types.CompareUint64(lVal, rVal)
}

func cmpBit(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 46)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 48)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 47)
	lBit := types.BinaryLiteral(l.GetBytes(lCol))
	rBit := types.BinaryLiteral(r.GetBytes(rCol))
	return lBit.Compare(rBit)
}

func cmpJSON(l Row, lCol int, r Row, rCol int) int {
	trace_util_0.Count(_compare_00000, 49)
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		trace_util_0.Count(_compare_00000, 51)
		return cmpNull(lNull, rNull)
	}
	trace_util_0.Count(_compare_00000, 50)
	lJ, rJ := l.GetJSON(lCol), r.GetJSON(rCol)
	return json.CompareBinary(lJ, rJ)
}

// Compare compares the value with ad.
func Compare(row Row, colIdx int, ad *types.Datum) int {
	trace_util_0.Count(_compare_00000, 52)
	switch ad.Kind() {
	case types.KindNull:
		trace_util_0.Count(_compare_00000, 53)
		if row.IsNull(colIdx) {
			trace_util_0.Count(_compare_00000, 70)
			return 0
		}
		trace_util_0.Count(_compare_00000, 54)
		return 1
	case types.KindMinNotNull:
		trace_util_0.Count(_compare_00000, 55)
		if row.IsNull(colIdx) {
			trace_util_0.Count(_compare_00000, 71)
			return -1
		}
		trace_util_0.Count(_compare_00000, 56)
		return 1
	case types.KindMaxValue:
		trace_util_0.Count(_compare_00000, 57)
		return -1
	case types.KindInt64:
		trace_util_0.Count(_compare_00000, 58)
		return types.CompareInt64(row.GetInt64(colIdx), ad.GetInt64())
	case types.KindUint64:
		trace_util_0.Count(_compare_00000, 59)
		return types.CompareUint64(row.GetUint64(colIdx), ad.GetUint64())
	case types.KindFloat32:
		trace_util_0.Count(_compare_00000, 60)
		return types.CompareFloat64(float64(row.GetFloat32(colIdx)), float64(ad.GetFloat32()))
	case types.KindFloat64:
		trace_util_0.Count(_compare_00000, 61)
		return types.CompareFloat64(row.GetFloat64(colIdx), ad.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindMysqlBit:
		trace_util_0.Count(_compare_00000, 62)
		return types.CompareString(row.GetString(colIdx), ad.GetString())
	case types.KindMysqlDecimal:
		trace_util_0.Count(_compare_00000, 63)
		l, r := row.GetMyDecimal(colIdx), ad.GetMysqlDecimal()
		return l.Compare(r)
	case types.KindMysqlDuration:
		trace_util_0.Count(_compare_00000, 64)
		l, r := row.GetDuration(colIdx, 0).Duration, ad.GetMysqlDuration().Duration
		return types.CompareInt64(int64(l), int64(r))
	case types.KindMysqlEnum:
		trace_util_0.Count(_compare_00000, 65)
		l, r := row.GetEnum(colIdx).Value, ad.GetMysqlEnum().Value
		return types.CompareUint64(l, r)
	case types.KindMysqlSet:
		trace_util_0.Count(_compare_00000, 66)
		l, r := row.GetSet(colIdx).Value, ad.GetMysqlSet().Value
		return types.CompareUint64(l, r)
	case types.KindMysqlJSON:
		trace_util_0.Count(_compare_00000, 67)
		l, r := row.GetJSON(colIdx), ad.GetMysqlJSON()
		return json.CompareBinary(l, r)
	case types.KindMysqlTime:
		trace_util_0.Count(_compare_00000, 68)
		l, r := row.GetTime(colIdx), ad.GetMysqlTime()
		return l.Compare(r)
	default:
		trace_util_0.Count(_compare_00000, 69)
		return 0
	}
}

// LowerBound searches on the non-decreasing column colIdx,
// returns the smallest index i such that the value at row i is not less than `d`.
func (c *Chunk) LowerBound(colIdx int, d *types.Datum) (index int, match bool) {
	trace_util_0.Count(_compare_00000, 72)
	index = sort.Search(c.NumRows(), func(i int) bool {
		trace_util_0.Count(_compare_00000, 74)
		cmp := Compare(c.GetRow(i), colIdx, d)
		if cmp == 0 {
			trace_util_0.Count(_compare_00000, 76)
			match = true
		}
		trace_util_0.Count(_compare_00000, 75)
		return cmp >= 0
	})
	trace_util_0.Count(_compare_00000, 73)
	return
}

// UpperBound searches on the non-decreasing column colIdx,
// returns the smallest index i such that the value at row i is larger than `d`.
func (c *Chunk) UpperBound(colIdx int, d *types.Datum) int {
	trace_util_0.Count(_compare_00000, 77)
	return sort.Search(c.NumRows(), func(i int) bool {
		trace_util_0.Count(_compare_00000, 78)
		return Compare(c.GetRow(i), colIdx, d) > 0
	})
}

var _compare_00000 = "util/chunk/compare.go"
