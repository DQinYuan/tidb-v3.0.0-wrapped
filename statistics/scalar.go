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

package statistics

import (
	"encoding/binary"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// calcFraction is used to calculate the fraction of the interval [lower, upper] that lies within the [lower, value]
// using the continuous-value assumption.
func calcFraction(lower, upper, value float64) float64 {
	trace_util_0.Count(_scalar_00000, 0)
	if upper <= lower {
		trace_util_0.Count(_scalar_00000, 5)
		return 0.5
	}
	trace_util_0.Count(_scalar_00000, 1)
	if value <= lower {
		trace_util_0.Count(_scalar_00000, 6)
		return 0
	}
	trace_util_0.Count(_scalar_00000, 2)
	if value >= upper {
		trace_util_0.Count(_scalar_00000, 7)
		return 1
	}
	trace_util_0.Count(_scalar_00000, 3)
	frac := (value - lower) / (upper - lower)
	if math.IsNaN(frac) || math.IsInf(frac, 0) || frac < 0 || frac > 1 {
		trace_util_0.Count(_scalar_00000, 8)
		return 0.5
	}
	trace_util_0.Count(_scalar_00000, 4)
	return frac
}

func convertDatumToScalar(value *types.Datum, commonPfxLen int) float64 {
	trace_util_0.Count(_scalar_00000, 9)
	switch value.Kind() {
	case types.KindMysqlDecimal:
		trace_util_0.Count(_scalar_00000, 10)
		scalar, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			trace_util_0.Count(_scalar_00000, 17)
			return 0
		}
		trace_util_0.Count(_scalar_00000, 11)
		return scalar
	case types.KindMysqlTime:
		trace_util_0.Count(_scalar_00000, 12)
		valueTime := value.GetMysqlTime()
		var minTime types.Time
		switch valueTime.Type {
		case mysql.TypeDate:
			trace_util_0.Count(_scalar_00000, 18)
			minTime = types.Time{
				Time: types.MinDatetime,
				Type: mysql.TypeDate,
				Fsp:  types.DefaultFsp,
			}
		case mysql.TypeDatetime:
			trace_util_0.Count(_scalar_00000, 19)
			minTime = types.Time{
				Time: types.MinDatetime,
				Type: mysql.TypeDatetime,
				Fsp:  types.DefaultFsp,
			}
		case mysql.TypeTimestamp:
			trace_util_0.Count(_scalar_00000, 20)
			minTime = types.MinTimestamp
		}
		trace_util_0.Count(_scalar_00000, 13)
		sc := &stmtctx.StatementContext{TimeZone: types.BoundTimezone}
		return float64(valueTime.Sub(sc, &minTime).Duration)
	case types.KindString, types.KindBytes:
		trace_util_0.Count(_scalar_00000, 14)
		bytes := value.GetBytes()
		if len(bytes) <= commonPfxLen {
			trace_util_0.Count(_scalar_00000, 21)
			return 0
		}
		trace_util_0.Count(_scalar_00000, 15)
		return convertBytesToScalar(bytes[commonPfxLen:])
	default:
		trace_util_0.Count(_scalar_00000, 16)
		// do not know how to convert
		return 0
	}
}

// PreCalculateScalar converts the lower and upper to scalar. When the datum type is KindString or KindBytes, we also
// calculate their common prefix length, because when a value falls between lower and upper, the common prefix
// of lower and upper equals to the common prefix of the lower, upper and the value. For some simple types like `Int64`,
// we do not convert it because we can directly infer the scalar value.
func (hg *Histogram) PreCalculateScalar() {
	trace_util_0.Count(_scalar_00000, 22)
	len := hg.Len()
	if len == 0 {
		trace_util_0.Count(_scalar_00000, 24)
		return
	}
	trace_util_0.Count(_scalar_00000, 23)
	switch hg.GetLower(0).Kind() {
	case types.KindMysqlDecimal, types.KindMysqlTime:
		trace_util_0.Count(_scalar_00000, 25)
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			trace_util_0.Count(_scalar_00000, 27)
			hg.scalars[i] = scalar{
				lower: convertDatumToScalar(hg.GetLower(i), 0),
				upper: convertDatumToScalar(hg.GetUpper(i), 0),
			}
		}
	case types.KindBytes, types.KindString:
		trace_util_0.Count(_scalar_00000, 26)
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			trace_util_0.Count(_scalar_00000, 28)
			lower, upper := hg.GetLower(i), hg.GetUpper(i)
			common := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
			hg.scalars[i] = scalar{
				commonPfxLen: common,
				lower:        convertDatumToScalar(lower, common),
				upper:        convertDatumToScalar(upper, common),
			}
		}
	}
}

func (hg *Histogram) calcFraction(index int, value *types.Datum) float64 {
	trace_util_0.Count(_scalar_00000, 29)
	lower, upper := hg.Bounds.GetRow(2*index), hg.Bounds.GetRow(2*index+1)
	switch value.Kind() {
	case types.KindFloat32:
		trace_util_0.Count(_scalar_00000, 31)
		return calcFraction(float64(lower.GetFloat32(0)), float64(upper.GetFloat32(0)), float64(value.GetFloat32()))
	case types.KindFloat64:
		trace_util_0.Count(_scalar_00000, 32)
		return calcFraction(lower.GetFloat64(0), upper.GetFloat64(0), value.GetFloat64())
	case types.KindInt64:
		trace_util_0.Count(_scalar_00000, 33)
		return calcFraction(float64(lower.GetInt64(0)), float64(upper.GetInt64(0)), float64(value.GetInt64()))
	case types.KindUint64:
		trace_util_0.Count(_scalar_00000, 34)
		return calcFraction(float64(lower.GetUint64(0)), float64(upper.GetUint64(0)), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		trace_util_0.Count(_scalar_00000, 35)
		return calcFraction(float64(lower.GetDuration(0, 0).Duration), float64(upper.GetDuration(0, 0).Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		trace_util_0.Count(_scalar_00000, 36)
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		trace_util_0.Count(_scalar_00000, 37)
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, hg.scalars[index].commonPfxLen))
	}
	trace_util_0.Count(_scalar_00000, 30)
	return 0.5
}

func commonPrefixLength(lower, upper []byte) int {
	trace_util_0.Count(_scalar_00000, 38)
	minLen := len(lower)
	if minLen > len(upper) {
		trace_util_0.Count(_scalar_00000, 41)
		minLen = len(upper)
	}
	trace_util_0.Count(_scalar_00000, 39)
	for i := 0; i < minLen; i++ {
		trace_util_0.Count(_scalar_00000, 42)
		if lower[i] != upper[i] {
			trace_util_0.Count(_scalar_00000, 43)
			return i
		}
	}
	trace_util_0.Count(_scalar_00000, 40)
	return minLen
}

func convertBytesToScalar(value []byte) float64 {
	trace_util_0.Count(_scalar_00000, 44)
	// Bytes type is viewed as a base-256 value, so we only consider at most 8 bytes.
	var buf [8]byte
	copy(buf[:], value)
	return float64(binary.BigEndian.Uint64(buf[:]))
}

func calcFraction4Datums(lower, upper, value *types.Datum) float64 {
	trace_util_0.Count(_scalar_00000, 45)
	switch value.Kind() {
	case types.KindFloat32:
		trace_util_0.Count(_scalar_00000, 47)
		return calcFraction(float64(lower.GetFloat32()), float64(upper.GetFloat32()), float64(value.GetFloat32()))
	case types.KindFloat64:
		trace_util_0.Count(_scalar_00000, 48)
		return calcFraction(lower.GetFloat64(), upper.GetFloat64(), value.GetFloat64())
	case types.KindInt64:
		trace_util_0.Count(_scalar_00000, 49)
		return calcFraction(float64(lower.GetInt64()), float64(upper.GetInt64()), float64(value.GetInt64()))
	case types.KindUint64:
		trace_util_0.Count(_scalar_00000, 50)
		return calcFraction(float64(lower.GetUint64()), float64(upper.GetUint64()), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		trace_util_0.Count(_scalar_00000, 51)
		return calcFraction(float64(lower.GetMysqlDuration().Duration), float64(upper.GetMysqlDuration().Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		trace_util_0.Count(_scalar_00000, 52)
		return calcFraction(convertDatumToScalar(lower, 0), convertDatumToScalar(upper, 0), convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		trace_util_0.Count(_scalar_00000, 53)
		commonPfxLen := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
		return calcFraction(convertDatumToScalar(lower, commonPfxLen), convertDatumToScalar(upper, commonPfxLen), convertDatumToScalar(value, commonPfxLen))
	}
	trace_util_0.Count(_scalar_00000, 46)
	return 0.5
}

var _scalar_00000 = "statistics/scalar.go"
