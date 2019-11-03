// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Kind constants.
const (
	KindNull          byte = 0
	KindInt64         byte = 1
	KindUint64        byte = 2
	KindFloat32       byte = 3
	KindFloat64       byte = 4
	KindString        byte = 5
	KindBytes         byte = 6
	KindBinaryLiteral byte = 7 // Used for BIT / HEX literals.
	KindMysqlDecimal  byte = 8
	KindMysqlDuration byte = 9
	KindMysqlEnum     byte = 10
	KindMysqlBit      byte = 11 // Used for BIT table column values.
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindInterface     byte = 14
	KindMinNotNull    byte = 15
	KindMaxValue      byte = 16
	KindRaw           byte = 17
	KindMysqlJSON     byte = 18
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte        // datum kind.
	collation uint8       // collation can hold uint8 values.
	decimal   uint16      // decimal can hold uint16 values.
	length    uint32      // length can hold uint32 values.
	i         int64       // i can hold int64 uint64 float64 values.
	b         []byte      // b can hold string or []byte values.
	x         interface{} // x hold all other types.
}

// Copy deep copies a Datum.
func (d *Datum) Copy() *Datum {
	trace_util_0.Count(_datum_00000, 0)
	ret := *d
	if d.b != nil {
		trace_util_0.Count(_datum_00000, 3)
		ret.b = make([]byte, len(d.b))
		copy(ret.b, d.b)
	}
	trace_util_0.Count(_datum_00000, 1)
	switch ret.Kind() {
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 4)
		d := *d.GetMysqlDecimal()
		ret.SetMysqlDecimal(&d)
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 5)
		ret.SetMysqlTime(d.GetMysqlTime())
	}
	trace_util_0.Count(_datum_00000, 2)
	return &ret
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	trace_util_0.Count(_datum_00000, 6)
	return d.k
}

// Collation gets the collation of the datum.
func (d *Datum) Collation() byte {
	trace_util_0.Count(_datum_00000, 7)
	return d.collation
}

// SetCollation sets the collation of the datum.
func (d *Datum) SetCollation(collation byte) {
	trace_util_0.Count(_datum_00000, 8)
	d.collation = collation
}

// Frac gets the frac of the datum.
func (d *Datum) Frac() int {
	trace_util_0.Count(_datum_00000, 9)
	return int(d.decimal)
}

// SetFrac sets the frac of the datum.
func (d *Datum) SetFrac(frac int) {
	trace_util_0.Count(_datum_00000, 10)
	d.decimal = uint16(frac)
}

// Length gets the length of the datum.
func (d *Datum) Length() int {
	trace_util_0.Count(_datum_00000, 11)
	return int(d.length)
}

// SetLength sets the length of the datum.
func (d *Datum) SetLength(l int) {
	trace_util_0.Count(_datum_00000, 12)
	d.length = uint32(l)
}

// IsNull checks if datum is null.
func (d *Datum) IsNull() bool {
	trace_util_0.Count(_datum_00000, 13)
	return d.k == KindNull
}

// GetInt64 gets int64 value.
func (d *Datum) GetInt64() int64 {
	trace_util_0.Count(_datum_00000, 14)
	return d.i
}

// SetInt64 sets int64 value.
func (d *Datum) SetInt64(i int64) {
	trace_util_0.Count(_datum_00000, 15)
	d.k = KindInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Datum) GetUint64() uint64 {
	trace_util_0.Count(_datum_00000, 16)
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Datum) SetUint64(i uint64) {
	trace_util_0.Count(_datum_00000, 17)
	d.k = KindUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Datum) GetFloat64() float64 {
	trace_util_0.Count(_datum_00000, 18)
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Datum) SetFloat64(f float64) {
	trace_util_0.Count(_datum_00000, 19)
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Datum) GetFloat32() float32 {
	trace_util_0.Count(_datum_00000, 20)
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetFloat32 sets float32 value.
func (d *Datum) SetFloat32(f float32) {
	trace_util_0.Count(_datum_00000, 21)
	d.k = KindFloat32
	d.i = int64(math.Float64bits(float64(f)))
}

// GetString gets string value.
func (d *Datum) GetString() string {
	trace_util_0.Count(_datum_00000, 22)
	return string(hack.String(d.b))
}

// SetString sets string value.
func (d *Datum) SetString(s string) {
	trace_util_0.Count(_datum_00000, 23)
	d.k = KindString
	sink(s)
	d.b = hack.Slice(s)
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
	trace_util_0.Count(_datum_00000, 24)
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	trace_util_0.Count(_datum_00000, 25)
	return d.b
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	trace_util_0.Count(_datum_00000, 26)
	d.k = KindBytes
	d.b = b
}

// SetBytesAsString sets bytes value to datum as string type.
func (d *Datum) SetBytesAsString(b []byte) {
	trace_util_0.Count(_datum_00000, 27)
	d.k = KindString
	d.b = b
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() interface{} {
	trace_util_0.Count(_datum_00000, 28)
	return d.x
}

// SetInterface sets interface to datum.
func (d *Datum) SetInterface(x interface{}) {
	trace_util_0.Count(_datum_00000, 29)
	d.k = KindInterface
	d.x = x
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	trace_util_0.Count(_datum_00000, 30)
	d.k = KindNull
	d.x = nil
}

// SetMinNotNull sets datum to minNotNull value.
func (d *Datum) SetMinNotNull() {
	trace_util_0.Count(_datum_00000, 31)
	d.k = KindMinNotNull
	d.x = nil
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	trace_util_0.Count(_datum_00000, 32)
	return d.b
}

// GetMysqlBit gets MysqlBit value
func (d *Datum) GetMysqlBit() BinaryLiteral {
	trace_util_0.Count(_datum_00000, 33)
	return d.GetBinaryLiteral()
}

// SetBinaryLiteral sets Bit value
func (d *Datum) SetBinaryLiteral(b BinaryLiteral) {
	trace_util_0.Count(_datum_00000, 34)
	d.k = KindBinaryLiteral
	d.b = b
}

// SetMysqlBit sets MysqlBit value
func (d *Datum) SetMysqlBit(b BinaryLiteral) {
	trace_util_0.Count(_datum_00000, 35)
	d.k = KindMysqlBit
	d.b = b
}

// GetMysqlDecimal gets Decimal value
func (d *Datum) GetMysqlDecimal() *MyDecimal {
	trace_util_0.Count(_datum_00000, 36)
	return d.x.(*MyDecimal)
}

// SetMysqlDecimal sets Decimal value
func (d *Datum) SetMysqlDecimal(b *MyDecimal) {
	trace_util_0.Count(_datum_00000, 37)
	d.k = KindMysqlDecimal
	d.x = b
}

// GetMysqlDuration gets Duration value
func (d *Datum) GetMysqlDuration() Duration {
	trace_util_0.Count(_datum_00000, 38)
	return Duration{Duration: time.Duration(d.i), Fsp: int(d.decimal)}
}

// SetMysqlDuration sets Duration value
func (d *Datum) SetMysqlDuration(b Duration) {
	trace_util_0.Count(_datum_00000, 39)
	d.k = KindMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// GetMysqlEnum gets Enum value
func (d *Datum) GetMysqlEnum() Enum {
	trace_util_0.Count(_datum_00000, 40)
	str := string(hack.String(d.b))
	return Enum{Value: uint64(d.i), Name: str}
}

// SetMysqlEnum sets Enum value
func (d *Datum) SetMysqlEnum(b Enum) {
	trace_util_0.Count(_datum_00000, 41)
	d.k = KindMysqlEnum
	d.i = int64(b.Value)
	sink(b.Name)
	d.b = hack.Slice(b.Name)
}

// GetMysqlSet gets Set value
func (d *Datum) GetMysqlSet() Set {
	trace_util_0.Count(_datum_00000, 42)
	str := string(hack.String(d.b))
	return Set{Value: uint64(d.i), Name: str}
}

// SetMysqlSet sets Set value
func (d *Datum) SetMysqlSet(b Set) {
	trace_util_0.Count(_datum_00000, 43)
	d.k = KindMysqlSet
	d.i = int64(b.Value)
	sink(b.Name)
	d.b = hack.Slice(b.Name)
}

// GetMysqlJSON gets json.BinaryJSON value
func (d *Datum) GetMysqlJSON() json.BinaryJSON {
	trace_util_0.Count(_datum_00000, 44)
	return json.BinaryJSON{TypeCode: byte(d.i), Value: d.b}
}

// SetMysqlJSON sets json.BinaryJSON value
func (d *Datum) SetMysqlJSON(b json.BinaryJSON) {
	trace_util_0.Count(_datum_00000, 45)
	d.k = KindMysqlJSON
	d.i = int64(b.TypeCode)
	d.b = b.Value
}

// GetMysqlTime gets types.Time value
func (d *Datum) GetMysqlTime() Time {
	trace_util_0.Count(_datum_00000, 46)
	return d.x.(Time)
}

// SetMysqlTime sets types.Time value
func (d *Datum) SetMysqlTime(b Time) {
	trace_util_0.Count(_datum_00000, 47)
	d.k = KindMysqlTime
	d.x = b
}

// SetRaw sets raw value.
func (d *Datum) SetRaw(b []byte) {
	trace_util_0.Count(_datum_00000, 48)
	d.k = KindRaw
	d.b = b
}

// GetRaw gets raw value.
func (d *Datum) GetRaw() []byte {
	trace_util_0.Count(_datum_00000, 49)
	return d.b
}

// SetAutoID set the auto increment ID according to its int flag.
func (d *Datum) SetAutoID(id int64, flag uint) {
	trace_util_0.Count(_datum_00000, 50)
	if mysql.HasUnsignedFlag(flag) {
		trace_util_0.Count(_datum_00000, 51)
		d.SetUint64(uint64(id))
	} else {
		trace_util_0.Count(_datum_00000, 52)
		{
			d.SetInt64(id)
		}
	}
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() interface{} {
	trace_util_0.Count(_datum_00000, 53)
	switch d.k {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 54)
		return d.GetInt64()
	case KindUint64:
		trace_util_0.Count(_datum_00000, 55)
		return d.GetUint64()
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 56)
		return d.GetFloat32()
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 57)
		return d.GetFloat64()
	case KindString:
		trace_util_0.Count(_datum_00000, 58)
		return d.GetString()
	case KindBytes:
		trace_util_0.Count(_datum_00000, 59)
		return d.GetBytes()
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 60)
		return d.GetMysqlDecimal()
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 61)
		return d.GetMysqlDuration()
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 62)
		return d.GetMysqlEnum()
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 63)
		return d.GetBinaryLiteral()
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 64)
		return d.GetMysqlSet()
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 65)
		return d.GetMysqlJSON()
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 66)
		return d.GetMysqlTime()
	default:
		trace_util_0.Count(_datum_00000, 67)
		return d.GetInterface()
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val interface{}) {
	trace_util_0.Count(_datum_00000, 68)
	switch x := val.(type) {
	case nil:
		trace_util_0.Count(_datum_00000, 69)
		d.SetNull()
	case bool:
		trace_util_0.Count(_datum_00000, 70)
		if x {
			trace_util_0.Count(_datum_00000, 88)
			d.SetInt64(1)
		} else {
			trace_util_0.Count(_datum_00000, 89)
			{
				d.SetInt64(0)
			}
		}
	case int:
		trace_util_0.Count(_datum_00000, 71)
		d.SetInt64(int64(x))
	case int64:
		trace_util_0.Count(_datum_00000, 72)
		d.SetInt64(x)
	case uint64:
		trace_util_0.Count(_datum_00000, 73)
		d.SetUint64(x)
	case float32:
		trace_util_0.Count(_datum_00000, 74)
		d.SetFloat32(x)
	case float64:
		trace_util_0.Count(_datum_00000, 75)
		d.SetFloat64(x)
	case string:
		trace_util_0.Count(_datum_00000, 76)
		d.SetString(x)
	case []byte:
		trace_util_0.Count(_datum_00000, 77)
		d.SetBytes(x)
	case *MyDecimal:
		trace_util_0.Count(_datum_00000, 78)
		d.SetMysqlDecimal(x)
	case Duration:
		trace_util_0.Count(_datum_00000, 79)
		d.SetMysqlDuration(x)
	case Enum:
		trace_util_0.Count(_datum_00000, 80)
		d.SetMysqlEnum(x)
	case BinaryLiteral:
		trace_util_0.Count(_datum_00000, 81)
		d.SetBinaryLiteral(x)
	case BitLiteral:
		trace_util_0.Count(_datum_00000, 82) // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		trace_util_0.Count(_datum_00000, 83)
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		trace_util_0.Count(_datum_00000, 84)
		d.SetMysqlSet(x)
	case json.BinaryJSON:
		trace_util_0.Count(_datum_00000, 85)
		d.SetMysqlJSON(x)
	case Time:
		trace_util_0.Count(_datum_00000, 86)
		d.SetMysqlTime(x)
	default:
		trace_util_0.Count(_datum_00000, 87)
		d.SetInterface(x)
	}
}

// CompareDatum compares datum to another datum.
// TODO: return error properly.
func (d *Datum) CompareDatum(sc *stmtctx.StatementContext, ad *Datum) (int, error) {
	trace_util_0.Count(_datum_00000, 90)
	if d.k == KindMysqlJSON && ad.k != KindMysqlJSON {
		trace_util_0.Count(_datum_00000, 92)
		cmp, err := ad.CompareDatum(sc, d)
		return cmp * -1, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 91)
	switch ad.k {
	case KindNull:
		trace_util_0.Count(_datum_00000, 93)
		if d.k == KindNull {
			trace_util_0.Count(_datum_00000, 112)
			return 0, nil
		}
		trace_util_0.Count(_datum_00000, 94)
		return 1, nil
	case KindMinNotNull:
		trace_util_0.Count(_datum_00000, 95)
		if d.k == KindNull {
			trace_util_0.Count(_datum_00000, 113)
			return -1, nil
		} else {
			trace_util_0.Count(_datum_00000, 114)
			if d.k == KindMinNotNull {
				trace_util_0.Count(_datum_00000, 115)
				return 0, nil
			}
		}
		trace_util_0.Count(_datum_00000, 96)
		return 1, nil
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 97)
		if d.k == KindMaxValue {
			trace_util_0.Count(_datum_00000, 116)
			return 0, nil
		}
		trace_util_0.Count(_datum_00000, 98)
		return -1, nil
	case KindInt64:
		trace_util_0.Count(_datum_00000, 99)
		return d.compareInt64(sc, ad.GetInt64())
	case KindUint64:
		trace_util_0.Count(_datum_00000, 100)
		return d.compareUint64(sc, ad.GetUint64())
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 101)
		return d.compareFloat64(sc, ad.GetFloat64())
	case KindString:
		trace_util_0.Count(_datum_00000, 102)
		return d.compareString(sc, ad.GetString())
	case KindBytes:
		trace_util_0.Count(_datum_00000, 103)
		return d.compareBytes(sc, ad.GetBytes())
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 104)
		return d.compareMysqlDecimal(sc, ad.GetMysqlDecimal())
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 105)
		return d.compareMysqlDuration(sc, ad.GetMysqlDuration())
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 106)
		return d.compareMysqlEnum(sc, ad.GetMysqlEnum())
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 107)
		return d.compareBinaryLiteral(sc, ad.GetBinaryLiteral())
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 108)
		return d.compareMysqlSet(sc, ad.GetMysqlSet())
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 109)
		return d.compareMysqlJSON(sc, ad.GetMysqlJSON())
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 110)
		return d.compareMysqlTime(sc, ad.GetMysqlTime())
	default:
		trace_util_0.Count(_datum_00000, 111)
		return 0, nil
	}
}

func (d *Datum) compareInt64(sc *stmtctx.StatementContext, i int64) (int, error) {
	trace_util_0.Count(_datum_00000, 117)
	switch d.k {
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 118)
		return 1, nil
	case KindInt64:
		trace_util_0.Count(_datum_00000, 119)
		return CompareInt64(d.i, i), nil
	case KindUint64:
		trace_util_0.Count(_datum_00000, 120)
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			trace_util_0.Count(_datum_00000, 123)
			return 1, nil
		}
		trace_util_0.Count(_datum_00000, 121)
		return CompareInt64(d.i, i), nil
	default:
		trace_util_0.Count(_datum_00000, 122)
		return d.compareFloat64(sc, float64(i))
	}
}

func (d *Datum) compareUint64(sc *stmtctx.StatementContext, u uint64) (int, error) {
	trace_util_0.Count(_datum_00000, 124)
	switch d.k {
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 125)
		return 1, nil
	case KindInt64:
		trace_util_0.Count(_datum_00000, 126)
		if d.i < 0 || u > math.MaxInt64 {
			trace_util_0.Count(_datum_00000, 130)
			return -1, nil
		}
		trace_util_0.Count(_datum_00000, 127)
		return CompareInt64(d.i, int64(u)), nil
	case KindUint64:
		trace_util_0.Count(_datum_00000, 128)
		return CompareUint64(d.GetUint64(), u), nil
	default:
		trace_util_0.Count(_datum_00000, 129)
		return d.compareFloat64(sc, float64(u))
	}
}

func (d *Datum) compareFloat64(sc *stmtctx.StatementContext, f float64) (int, error) {
	trace_util_0.Count(_datum_00000, 131)
	switch d.k {
	case KindNull, KindMinNotNull:
		trace_util_0.Count(_datum_00000, 132)
		return -1, nil
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 133)
		return 1, nil
	case KindInt64:
		trace_util_0.Count(_datum_00000, 134)
		return CompareFloat64(float64(d.i), f), nil
	case KindUint64:
		trace_util_0.Count(_datum_00000, 135)
		return CompareFloat64(float64(d.GetUint64()), f), nil
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 136)
		return CompareFloat64(d.GetFloat64(), f), nil
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 137)
		fVal, err := StrToFloat(sc, d.GetString())
		return CompareFloat64(fVal, f), errors.Trace(err)
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 138)
		fVal, err := d.GetMysqlDecimal().ToFloat64()
		return CompareFloat64(fVal, f), errors.Trace(err)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 139)
		fVal := d.GetMysqlDuration().Seconds()
		return CompareFloat64(fVal, f), nil
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 140)
		fVal := d.GetMysqlEnum().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 141)
		val, err := d.GetBinaryLiteral().ToInt(sc)
		fVal := float64(val)
		return CompareFloat64(fVal, f), errors.Trace(err)
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 142)
		fVal := d.GetMysqlSet().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 143)
		fVal, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return CompareFloat64(fVal, f), errors.Trace(err)
	default:
		trace_util_0.Count(_datum_00000, 144)
		return -1, nil
	}
}

func (d *Datum) compareString(sc *stmtctx.StatementContext, s string) (int, error) {
	trace_util_0.Count(_datum_00000, 145)
	switch d.k {
	case KindNull, KindMinNotNull:
		trace_util_0.Count(_datum_00000, 146)
		return -1, nil
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 147)
		return 1, nil
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 148)
		return CompareString(d.GetString(), s), nil
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 149)
		dec := new(MyDecimal)
		err := sc.HandleTruncate(dec.FromString(hack.Slice(s)))
		return d.GetMysqlDecimal().Compare(dec), errors.Trace(err)
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 150)
		dt, err := ParseDatetime(sc, s)
		return d.GetMysqlTime().Compare(dt), errors.Trace(err)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 151)
		dur, err := ParseDuration(sc, s, MaxFsp)
		return d.GetMysqlDuration().Compare(dur), errors.Trace(err)
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 152)
		return CompareString(d.GetMysqlSet().String(), s), nil
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 153)
		return CompareString(d.GetMysqlEnum().String(), s), nil
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 154)
		return CompareString(d.GetBinaryLiteral().ToString(), s), nil
	default:
		trace_util_0.Count(_datum_00000, 155)
		fVal, err := StrToFloat(sc, s)
		if err != nil {
			trace_util_0.Count(_datum_00000, 157)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 156)
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Datum) compareBytes(sc *stmtctx.StatementContext, b []byte) (int, error) {
	trace_util_0.Count(_datum_00000, 158)
	str := string(hack.String(b))
	return d.compareString(sc, str)
}

func (d *Datum) compareMysqlDecimal(sc *stmtctx.StatementContext, dec *MyDecimal) (int, error) {
	trace_util_0.Count(_datum_00000, 159)
	switch d.k {
	case KindNull, KindMinNotNull:
		trace_util_0.Count(_datum_00000, 160)
		return -1, nil
	case KindMaxValue:
		trace_util_0.Count(_datum_00000, 161)
		return 1, nil
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 162)
		return d.GetMysqlDecimal().Compare(dec), nil
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 163)
		dDec := new(MyDecimal)
		err := sc.HandleTruncate(dDec.FromString(d.GetBytes()))
		return dDec.Compare(dec), errors.Trace(err)
	default:
		trace_util_0.Count(_datum_00000, 164)
		dVal, err := d.ConvertTo(sc, NewFieldType(mysql.TypeNewDecimal))
		if err != nil {
			trace_util_0.Count(_datum_00000, 166)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 165)
		return dVal.GetMysqlDecimal().Compare(dec), nil
	}
}

func (d *Datum) compareMysqlDuration(sc *stmtctx.StatementContext, dur Duration) (int, error) {
	trace_util_0.Count(_datum_00000, 167)
	switch d.k {
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 168)
		return d.GetMysqlDuration().Compare(dur), nil
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 169)
		dDur, err := ParseDuration(sc, d.GetString(), MaxFsp)
		return dDur.Compare(dur), errors.Trace(err)
	default:
		trace_util_0.Count(_datum_00000, 170)
		return d.compareFloat64(sc, dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(sc *stmtctx.StatementContext, enum Enum) (int, error) {
	trace_util_0.Count(_datum_00000, 171)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 172)
		return CompareString(d.GetString(), enum.String()), nil
	default:
		trace_util_0.Count(_datum_00000, 173)
		return d.compareFloat64(sc, enum.ToNumber())
	}
}

func (d *Datum) compareBinaryLiteral(sc *stmtctx.StatementContext, b BinaryLiteral) (int, error) {
	trace_util_0.Count(_datum_00000, 174)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 175)
		return CompareString(d.GetString(), b.ToString()), nil
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 176)
		return CompareString(d.GetBinaryLiteral().ToString(), b.ToString()), nil
	default:
		trace_util_0.Count(_datum_00000, 177)
		val, err := b.ToInt(sc)
		if err != nil {
			trace_util_0.Count(_datum_00000, 179)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 178)
		result, err := d.compareFloat64(sc, float64(val))
		return result, errors.Trace(err)
	}
}

func (d *Datum) compareMysqlSet(sc *stmtctx.StatementContext, set Set) (int, error) {
	trace_util_0.Count(_datum_00000, 180)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 181)
		return CompareString(d.GetString(), set.String()), nil
	default:
		trace_util_0.Count(_datum_00000, 182)
		return d.compareFloat64(sc, set.ToNumber())
	}
}

func (d *Datum) compareMysqlJSON(sc *stmtctx.StatementContext, target json.BinaryJSON) (int, error) {
	trace_util_0.Count(_datum_00000, 183)
	origin, err := d.ToMysqlJSON()
	if err != nil {
		trace_util_0.Count(_datum_00000, 185)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 184)
	return json.CompareBinary(origin, target), nil
}

func (d *Datum) compareMysqlTime(sc *stmtctx.StatementContext, time Time) (int, error) {
	trace_util_0.Count(_datum_00000, 186)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 187)
		dt, err := ParseDatetime(sc, d.GetString())
		return dt.Compare(time), errors.Trace(err)
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 188)
		return d.GetMysqlTime().Compare(time), nil
	default:
		trace_util_0.Count(_datum_00000, 189)
		fVal, err := time.ToNumber().ToFloat64()
		if err != nil {
			trace_util_0.Count(_datum_00000, 191)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 190)
		return d.compareFloat64(sc, fVal)
	}
}

// ConvertTo converts a datum to the target field type.
func (d *Datum) ConvertTo(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 192)
	if d.k == KindNull {
		trace_util_0.Count(_datum_00000, 194)
		return Datum{}, nil
	}
	trace_util_0.Count(_datum_00000, 193)
	switch target.Tp { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_datum_00000, 195)
		unsigned := mysql.HasUnsignedFlag(target.Flag)
		if unsigned {
			trace_util_0.Count(_datum_00000, 210)
			return d.convertToUint(sc, target)
		}
		trace_util_0.Count(_datum_00000, 196)
		return d.convertToInt(sc, target)
	case mysql.TypeFloat, mysql.TypeDouble:
		trace_util_0.Count(_datum_00000, 197)
		return d.convertToFloat(sc, target)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		trace_util_0.Count(_datum_00000, 198)
		return d.convertToString(sc, target)
	case mysql.TypeTimestamp:
		trace_util_0.Count(_datum_00000, 199)
		return d.convertToMysqlTimestamp(sc, target)
	case mysql.TypeDatetime, mysql.TypeDate:
		trace_util_0.Count(_datum_00000, 200)
		return d.convertToMysqlTime(sc, target)
	case mysql.TypeDuration:
		trace_util_0.Count(_datum_00000, 201)
		return d.convertToMysqlDuration(sc, target)
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_datum_00000, 202)
		return d.convertToMysqlDecimal(sc, target)
	case mysql.TypeYear:
		trace_util_0.Count(_datum_00000, 203)
		return d.convertToMysqlYear(sc, target)
	case mysql.TypeEnum:
		trace_util_0.Count(_datum_00000, 204)
		return d.convertToMysqlEnum(sc, target)
	case mysql.TypeBit:
		trace_util_0.Count(_datum_00000, 205)
		return d.convertToMysqlBit(sc, target)
	case mysql.TypeSet:
		trace_util_0.Count(_datum_00000, 206)
		return d.convertToMysqlSet(sc, target)
	case mysql.TypeJSON:
		trace_util_0.Count(_datum_00000, 207)
		return d.convertToMysqlJSON(sc, target)
	case mysql.TypeNull:
		trace_util_0.Count(_datum_00000, 208)
		return Datum{}, nil
	default:
		trace_util_0.Count(_datum_00000, 209)
		panic("should never happen")
	}
}

func (d *Datum) convertToFloat(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 211)
	var (
		f   float64
		ret Datum
		err error
	)
	switch d.k {
	case KindNull:
		trace_util_0.Count(_datum_00000, 215)
		return ret, nil
	case KindInt64:
		trace_util_0.Count(_datum_00000, 216)
		f = float64(d.GetInt64())
	case KindUint64:
		trace_util_0.Count(_datum_00000, 217)
		f = float64(d.GetUint64())
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 218)
		f = d.GetFloat64()
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 219)
		f, err = StrToFloat(sc, d.GetString())
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 220)
		f, err = d.GetMysqlTime().ToNumber().ToFloat64()
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 221)
		f, err = d.GetMysqlDuration().ToNumber().ToFloat64()
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 222)
		f, err = d.GetMysqlDecimal().ToFloat64()
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 223)
		f = d.GetMysqlSet().ToNumber()
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 224)
		f = d.GetMysqlEnum().ToNumber()
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 225)
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		f, err = float64(val), err1
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 226)
		f, err = ConvertJSONToFloat(sc, d.GetMysqlJSON())
	default:
		trace_util_0.Count(_datum_00000, 227)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 212)
	var err1 error
	f, err1 = ProduceFloatWithSpecifiedTp(f, target, sc)
	if err == nil && err1 != nil {
		trace_util_0.Count(_datum_00000, 228)
		err = err1
	}
	trace_util_0.Count(_datum_00000, 213)
	if target.Tp == mysql.TypeFloat {
		trace_util_0.Count(_datum_00000, 229)
		ret.SetFloat32(float32(f))
	} else {
		trace_util_0.Count(_datum_00000, 230)
		{
			ret.SetFloat64(f)
		}
	}
	trace_util_0.Count(_datum_00000, 214)
	return ret, errors.Trace(err)
}

// ProduceFloatWithSpecifiedTp produces a new float64 according to `flen` and `decimal`.
func ProduceFloatWithSpecifiedTp(f float64, target *FieldType, sc *stmtctx.StatementContext) (_ float64, err error) {
	trace_util_0.Count(_datum_00000, 231)
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
		trace_util_0.Count(_datum_00000, 234)
		f, err = TruncateFloat(f, target.Flen, target.Decimal)
		if err = sc.HandleOverflow(err, err); err != nil {
			trace_util_0.Count(_datum_00000, 235)
			return f, errors.Trace(err)
		}
	}
	trace_util_0.Count(_datum_00000, 232)
	if mysql.HasUnsignedFlag(target.Flag) && f < 0 {
		trace_util_0.Count(_datum_00000, 236)
		return 0, overflow(f, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 233)
	return f, nil
}

func (d *Datum) convertToString(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 237)
	var ret Datum
	var s string
	switch d.k {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 240)
		s = strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		trace_util_0.Count(_datum_00000, 241)
		s = strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 242)
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 243)
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 244)
		s = d.GetString()
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 245)
		s = d.GetMysqlTime().String()
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 246)
		s = d.GetMysqlDuration().String()
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 247)
		s = d.GetMysqlDecimal().String()
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 248)
		s = d.GetMysqlEnum().String()
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 249)
		s = d.GetMysqlSet().String()
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 250)
		s = d.GetBinaryLiteral().ToString()
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 251)
		s = d.GetMysqlJSON().String()
	default:
		trace_util_0.Count(_datum_00000, 252)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 238)
	s, err := ProduceStrWithSpecifiedTp(s, target, sc, true)
	ret.SetString(s)
	if target.Charset == charset.CharsetBin {
		trace_util_0.Count(_datum_00000, 253)
		ret.k = KindBytes
	}
	trace_util_0.Count(_datum_00000, 239)
	return ret, errors.Trace(err)
}

// ProduceStrWithSpecifiedTp produces a new string according to `flen` and `chs`. Param `padZero` indicates
// whether we should pad `\0` for `binary(flen)` type.
func ProduceStrWithSpecifiedTp(s string, tp *FieldType, sc *stmtctx.StatementContext, padZero bool) (_ string, err error) {
	trace_util_0.Count(_datum_00000, 254)
	flen, chs := tp.Flen, tp.Charset
	if flen >= 0 {
		trace_util_0.Count(_datum_00000, 256)
		// Flen is the rune length, not binary length, for UTF8 charset, we need to calculate the
		// rune count and truncate to Flen runes if it is too long.
		if chs == charset.CharsetUTF8 || chs == charset.CharsetUTF8MB4 {
			trace_util_0.Count(_datum_00000, 257)
			characterLen := utf8.RuneCountInString(s)
			if characterLen > flen {
				trace_util_0.Count(_datum_00000, 258)
				// 1. If len(s) is 0 and flen is 0, truncateLen will be 0, don't truncate s.
				//    CREATE TABLE t (a char(0));
				//    INSERT INTO t VALUES (``);
				// 2. If len(s) is 10 and flen is 0, truncateLen will be 0 too, but we still need to truncate s.
				//    SELECT 1, CAST(1234 AS CHAR(0));
				// So truncateLen is not a suitable variable to determine to do truncate or not.
				var runeCount int
				var truncateLen int
				for i := range s {
					trace_util_0.Count(_datum_00000, 260)
					if runeCount == flen {
						trace_util_0.Count(_datum_00000, 262)
						truncateLen = i
						break
					}
					trace_util_0.Count(_datum_00000, 261)
					runeCount++
				}
				trace_util_0.Count(_datum_00000, 259)
				err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, characterLen)
				s = truncateStr(s, truncateLen)
			}
		} else {
			trace_util_0.Count(_datum_00000, 263)
			if len(s) > flen {
				trace_util_0.Count(_datum_00000, 264)
				err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, len(s))
				s = truncateStr(s, flen)
			} else {
				trace_util_0.Count(_datum_00000, 265)
				if tp.Tp == mysql.TypeString && IsBinaryStr(tp) && len(s) < flen && padZero {
					trace_util_0.Count(_datum_00000, 266)
					padding := make([]byte, flen-len(s))
					s = string(append([]byte(s), padding...))
				}
			}
		}
	}
	trace_util_0.Count(_datum_00000, 255)
	return s, errors.Trace(sc.HandleTruncate(err))
}

func (d *Datum) convertToInt(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 267)
	i64, err := d.toSignedInteger(sc, target.Tp)
	return NewIntDatum(i64), errors.Trace(err)
}

func (d *Datum) convertToUint(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 268)
	tp := target.Tp
	upperBound := IntergerUnsignedUpperBound(tp)
	var (
		val uint64
		err error
		ret Datum
	)
	switch d.k {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 271)
		val, err = ConvertIntToUint(sc, d.GetInt64(), upperBound, tp)
	case KindUint64:
		trace_util_0.Count(_datum_00000, 272)
		val, err = ConvertUintToUint(d.GetUint64(), upperBound, tp)
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 273)
		val, err = ConvertFloatToUint(sc, d.GetFloat64(), upperBound, tp)
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 274)
		uval, err1 := StrToUint(sc, d.GetString())
		if err1 != nil && ErrOverflow.Equal(err1) && !sc.ShouldIgnoreOverflowError() {
			trace_util_0.Count(_datum_00000, 286)
			return ret, errors.Trace(err1)
		}
		trace_util_0.Count(_datum_00000, 275)
		val, err = ConvertUintToUint(uval, upperBound, tp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 287)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 276)
		err = err1
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 277)
		dec := d.GetMysqlTime().ToNumber()
		err = dec.Round(dec, 0, ModeHalfEven)
		ival, err1 := dec.ToInt()
		if err == nil {
			trace_util_0.Count(_datum_00000, 288)
			err = err1
		}
		trace_util_0.Count(_datum_00000, 278)
		val, err1 = ConvertIntToUint(sc, ival, upperBound, tp)
		if err == nil {
			trace_util_0.Count(_datum_00000, 289)
			err = err1
		}
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 279)
		dec := d.GetMysqlDuration().ToNumber()
		err = dec.Round(dec, 0, ModeHalfEven)
		ival, err1 := dec.ToInt()
		if err1 == nil {
			trace_util_0.Count(_datum_00000, 290)
			val, err = ConvertIntToUint(sc, ival, upperBound, tp)
		}
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 280)
		val, err = ConvertDecimalToUint(sc, d.GetMysqlDecimal(), upperBound, tp)
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 281)
		val, err = ConvertFloatToUint(sc, d.GetMysqlEnum().ToNumber(), upperBound, tp)
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 282)
		val, err = ConvertFloatToUint(sc, d.GetMysqlSet().ToNumber(), upperBound, tp)
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 283)
		val, err = d.GetBinaryLiteral().ToInt(sc)
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 284)
		var i64 int64
		i64, err = ConvertJSONToInt(sc, d.GetMysqlJSON(), true)
		val = uint64(i64)
	default:
		trace_util_0.Count(_datum_00000, 285)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 269)
	ret.SetUint64(val)
	if err != nil {
		trace_util_0.Count(_datum_00000, 291)
		return ret, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 270)
	return ret, nil
}

func (d *Datum) convertToMysqlTimestamp(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 292)
	var (
		ret Datum
		t   Time
		err error
	)
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		trace_util_0.Count(_datum_00000, 296)
		fsp = target.Decimal
	}
	trace_util_0.Count(_datum_00000, 293)
	switch d.k {
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 297)
		t = d.GetMysqlTime()
		t, err = t.RoundFrac(sc, fsp)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 298)
		t, err = d.GetMysqlDuration().ConvertToTime(sc, mysql.TypeTimestamp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 304)
			ret.SetValue(t)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 299)
		t, err = t.RoundFrac(sc, fsp)
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 300)
		t, err = ParseTime(sc, d.GetString(), mysql.TypeTimestamp, fsp)
	case KindInt64:
		trace_util_0.Count(_datum_00000, 301)
		t, err = ParseTimeFromNum(sc, d.GetInt64(), mysql.TypeTimestamp, fsp)
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 302)
		t, err = ParseTimeFromFloatString(sc, d.GetMysqlDecimal().String(), mysql.TypeTimestamp, fsp)
	default:
		trace_util_0.Count(_datum_00000, 303)
		return invalidConv(d, mysql.TypeTimestamp)
	}
	trace_util_0.Count(_datum_00000, 294)
	t.Type = mysql.TypeTimestamp
	ret.SetMysqlTime(t)
	if err != nil {
		trace_util_0.Count(_datum_00000, 305)
		return ret, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 295)
	return ret, nil
}

func (d *Datum) convertToMysqlTime(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 306)
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		trace_util_0.Count(_datum_00000, 311)
		fsp = target.Decimal
	}
	trace_util_0.Count(_datum_00000, 307)
	var (
		ret Datum
		t   Time
		err error
	)
	switch d.k {
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 312)
		t, err = d.GetMysqlTime().Convert(sc, tp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 320)
			ret.SetValue(t)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 313)
		t, err = t.RoundFrac(sc, fsp)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 314)
		t, err = d.GetMysqlDuration().ConvertToTime(sc, tp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 321)
			ret.SetValue(t)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 315)
		t, err = t.RoundFrac(sc, fsp)
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 316)
		t, err = ParseTimeFromFloatString(sc, d.GetMysqlDecimal().String(), tp, fsp)
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 317)
		t, err = ParseTime(sc, d.GetString(), tp, fsp)
	case KindInt64:
		trace_util_0.Count(_datum_00000, 318)
		t, err = ParseTimeFromNum(sc, d.GetInt64(), tp, fsp)
	default:
		trace_util_0.Count(_datum_00000, 319)
		return invalidConv(d, tp)
	}
	trace_util_0.Count(_datum_00000, 308)
	if tp == mysql.TypeDate {
		trace_util_0.Count(_datum_00000, 322)
		// Truncate hh:mm:ss part if the type is Date.
		t.Time = FromDate(t.Time.Year(), t.Time.Month(), t.Time.Day(), 0, 0, 0, 0)
	}
	trace_util_0.Count(_datum_00000, 309)
	ret.SetValue(t)
	if err != nil {
		trace_util_0.Count(_datum_00000, 323)
		return ret, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 310)
	return ret, nil
}

func (d *Datum) convertToMysqlDuration(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 324)
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		trace_util_0.Count(_datum_00000, 327)
		fsp = target.Decimal
	}
	trace_util_0.Count(_datum_00000, 325)
	var ret Datum
	switch d.k {
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 328)
		dur, err := d.GetMysqlTime().ConvertToDuration()
		if err != nil {
			trace_util_0.Count(_datum_00000, 338)
			ret.SetValue(dur)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 329)
		dur, err = dur.RoundFrac(fsp)
		ret.SetValue(dur)
		if err != nil {
			trace_util_0.Count(_datum_00000, 339)
			return ret, errors.Trace(err)
		}
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 330)
		dur, err := d.GetMysqlDuration().RoundFrac(fsp)
		ret.SetValue(dur)
		if err != nil {
			trace_util_0.Count(_datum_00000, 340)
			return ret, errors.Trace(err)
		}
	case KindInt64, KindFloat32, KindFloat64, KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 331)
		// TODO: We need a ParseDurationFromNum to avoid the cost of converting a num to string.
		timeStr, err := d.ToString()
		if err != nil {
			trace_util_0.Count(_datum_00000, 341)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 332)
		timeNum, err := d.ToInt64(sc)
		if err != nil {
			trace_util_0.Count(_datum_00000, 342)
			return ret, errors.Trace(err)
		}
		// For huge numbers(>'0001-00-00 00-00-00') try full DATETIME in ParseDuration.
		trace_util_0.Count(_datum_00000, 333)
		if timeNum > MaxDuration && timeNum < 10000000000 {
			trace_util_0.Count(_datum_00000, 343)
			// mysql return max in no strict sql mode.
			ret.SetValue(Duration{Duration: MaxTime, Fsp: 0})
			return ret, ErrInvalidTimeFormat.GenWithStack("Incorrect time value: '%s'", timeStr)
		}
		trace_util_0.Count(_datum_00000, 334)
		if timeNum < -MaxDuration {
			trace_util_0.Count(_datum_00000, 344)
			return ret, ErrInvalidTimeFormat.GenWithStack("Incorrect time value: '%s'", timeStr)
		}
		trace_util_0.Count(_datum_00000, 335)
		t, err := ParseDuration(sc, timeStr, fsp)
		ret.SetValue(t)
		if err != nil {
			trace_util_0.Count(_datum_00000, 345)
			return ret, errors.Trace(err)
		}
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 336)
		t, err := ParseDuration(sc, d.GetString(), fsp)
		ret.SetValue(t)
		if err != nil {
			trace_util_0.Count(_datum_00000, 346)
			return ret, errors.Trace(err)
		}
	default:
		trace_util_0.Count(_datum_00000, 337)
		return invalidConv(d, tp)
	}
	trace_util_0.Count(_datum_00000, 326)
	return ret, nil
}

func (d *Datum) convertToMysqlDecimal(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 347)
	var ret Datum
	ret.SetLength(target.Flen)
	ret.SetFrac(target.Decimal)
	var dec = &MyDecimal{}
	var err error
	switch d.k {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 351)
		dec.FromInt(d.GetInt64())
	case KindUint64:
		trace_util_0.Count(_datum_00000, 352)
		dec.FromUint(d.GetUint64())
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 353)
		err = dec.FromFloat64(d.GetFloat64())
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 354)
		err = dec.FromString(d.GetBytes())
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 355)
		*dec = *d.GetMysqlDecimal()
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 356)
		dec = d.GetMysqlTime().ToNumber()
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 357)
		dec = d.GetMysqlDuration().ToNumber()
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 358)
		err = dec.FromFloat64(d.GetMysqlEnum().ToNumber())
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 359)
		err = dec.FromFloat64(d.GetMysqlSet().ToNumber())
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 360)
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		err = err1
		dec.FromUint(val)
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 361)
		f, err1 := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		if err1 != nil {
			trace_util_0.Count(_datum_00000, 364)
			return ret, errors.Trace(err1)
		}
		trace_util_0.Count(_datum_00000, 362)
		err = dec.FromFloat64(f)
	default:
		trace_util_0.Count(_datum_00000, 363)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 348)
	var err1 error
	dec, err1 = ProduceDecWithSpecifiedTp(dec, target, sc)
	if err == nil && err1 != nil {
		trace_util_0.Count(_datum_00000, 365)
		err = err1
	}
	trace_util_0.Count(_datum_00000, 349)
	if dec.negative && mysql.HasUnsignedFlag(target.Flag) {
		trace_util_0.Count(_datum_00000, 366)
		*dec = zeroMyDecimal
		if err == nil {
			trace_util_0.Count(_datum_00000, 367)
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", target.Flen, target.Decimal))
		}
	}
	trace_util_0.Count(_datum_00000, 350)
	ret.SetValue(dec)
	return ret, err
}

// ProduceDecWithSpecifiedTp produces a new decimal according to `flen` and `decimal`.
func ProduceDecWithSpecifiedTp(dec *MyDecimal, tp *FieldType, sc *stmtctx.StatementContext) (_ *MyDecimal, err error) {
	trace_util_0.Count(_datum_00000, 368)
	flen, decimal := tp.Flen, tp.Decimal
	if flen != UnspecifiedLength && decimal != UnspecifiedLength {
		trace_util_0.Count(_datum_00000, 372)
		if flen < decimal {
			trace_util_0.Count(_datum_00000, 374)
			return nil, ErrMBiggerThanD.GenWithStackByArgs("")
		}
		trace_util_0.Count(_datum_00000, 373)
		prec, frac := dec.PrecisionAndFrac()
		if !dec.IsZero() && prec-frac > flen-decimal {
			trace_util_0.Count(_datum_00000, 375)
			dec = NewMaxOrMinDec(dec.IsNegative(), flen, decimal)
			// select (cast 111 as decimal(1)) causes a warning in MySQL.
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", flen, decimal))
		} else {
			trace_util_0.Count(_datum_00000, 376)
			if frac != decimal {
				trace_util_0.Count(_datum_00000, 377)
				old := *dec
				err = dec.Round(dec, decimal, ModeHalfEven)
				if err != nil {
					trace_util_0.Count(_datum_00000, 379)
					return nil, err
				}
				trace_util_0.Count(_datum_00000, 378)
				if !dec.IsZero() && frac > decimal && dec.Compare(&old) != 0 {
					trace_util_0.Count(_datum_00000, 380)
					if sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt {
						trace_util_0.Count(_datum_00000, 381)
						// fix https://github.com/pingcap/tidb/issues/3895
						// fix https://github.com/pingcap/tidb/issues/5532
						sc.AppendWarning(ErrTruncated)
						err = nil
					} else {
						trace_util_0.Count(_datum_00000, 382)
						{
							err = sc.HandleTruncate(ErrTruncated)
						}
					}
				}
			}
		}
	}

	trace_util_0.Count(_datum_00000, 369)
	if ErrOverflow.Equal(err) {
		trace_util_0.Count(_datum_00000, 383)
		// TODO: warnErr need to be ErrWarnDataOutOfRange
		err = sc.HandleOverflow(err, err)
	}
	trace_util_0.Count(_datum_00000, 370)
	unsigned := mysql.HasUnsignedFlag(tp.Flag)
	if unsigned && dec.IsNegative() {
		trace_util_0.Count(_datum_00000, 384)
		dec = dec.FromUint(0)
	}
	trace_util_0.Count(_datum_00000, 371)
	return dec, err
}

func (d *Datum) convertToMysqlYear(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 385)
	var (
		ret    Datum
		y      int64
		err    error
		adjust bool
	)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 388)
		s := d.GetString()
		y, err = StrToInt(sc, s)
		if err != nil {
			trace_util_0.Count(_datum_00000, 394)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 389)
		if len(s) != 4 && len(s) > 0 && s[0:1] == "0" {
			trace_util_0.Count(_datum_00000, 395)
			adjust = true
		}
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 390)
		y = int64(d.GetMysqlTime().Time.Year())
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 391)
		y = int64(time.Now().Year())
	default:
		trace_util_0.Count(_datum_00000, 392)
		ret, err = d.convertToInt(sc, NewFieldType(mysql.TypeLonglong))
		if err != nil {
			trace_util_0.Count(_datum_00000, 396)
			return invalidConv(d, target.Tp)
		}
		trace_util_0.Count(_datum_00000, 393)
		y = ret.GetInt64()
	}
	trace_util_0.Count(_datum_00000, 386)
	y, err = AdjustYear(y, adjust)
	if err != nil {
		trace_util_0.Count(_datum_00000, 397)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 387)
	ret.SetInt64(y)
	return ret, nil
}

func (d *Datum) convertToMysqlBit(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 398)
	var ret Datum
	var uintValue uint64
	var err error
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 401)
		uintValue, err = BinaryLiteral(d.b).ToInt(sc)
	default:
		trace_util_0.Count(_datum_00000, 402)
		uintDatum, err1 := d.convertToUint(sc, target)
		uintValue, err = uintDatum.GetUint64(), err1
	}
	trace_util_0.Count(_datum_00000, 399)
	if target.Flen < 64 && uintValue >= 1<<(uint64(target.Flen)) {
		trace_util_0.Count(_datum_00000, 403)
		return Datum{}, errors.Trace(ErrOverflow.GenWithStackByArgs("BIT", fmt.Sprintf("(%d)", target.Flen)))
	}
	trace_util_0.Count(_datum_00000, 400)
	byteSize := (target.Flen + 7) >> 3
	ret.SetMysqlBit(NewBinaryLiteralFromUint(uintValue, byteSize))
	return ret, errors.Trace(err)
}

func (d *Datum) convertToMysqlEnum(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 404)
	var (
		ret Datum
		e   Enum
		err error
	)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 407)
		e, err = ParseEnumName(target.Elems, d.GetString())
	default:
		trace_util_0.Count(_datum_00000, 408)
		var uintDatum Datum
		uintDatum, err = d.convertToUint(sc, target)
		if err != nil {
			trace_util_0.Count(_datum_00000, 410)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 409)
		e, err = ParseEnumValue(target.Elems, uintDatum.GetUint64())
	}
	trace_util_0.Count(_datum_00000, 405)
	if err != nil {
		trace_util_0.Count(_datum_00000, 411)
		logutil.Logger(context.Background()).Error("convert to MySQL enum failed", zap.Error(err))
		err = errors.Trace(ErrTruncated)
	}
	trace_util_0.Count(_datum_00000, 406)
	ret.SetValue(e)
	return ret, err
}

func (d *Datum) convertToMysqlSet(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	trace_util_0.Count(_datum_00000, 412)
	var (
		ret Datum
		s   Set
		err error
	)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 415)
		s, err = ParseSetName(target.Elems, d.GetString())
	default:
		trace_util_0.Count(_datum_00000, 416)
		var uintDatum Datum
		uintDatum, err = d.convertToUint(sc, target)
		if err != nil {
			trace_util_0.Count(_datum_00000, 418)
			return ret, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 417)
		s, err = ParseSetValue(target.Elems, uintDatum.GetUint64())
	}

	trace_util_0.Count(_datum_00000, 413)
	if err != nil {
		trace_util_0.Count(_datum_00000, 419)
		return invalidConv(d, target.Tp)
	}
	trace_util_0.Count(_datum_00000, 414)
	ret.SetValue(s)
	return ret, nil
}

func (d *Datum) convertToMysqlJSON(sc *stmtctx.StatementContext, target *FieldType) (ret Datum, err error) {
	trace_util_0.Count(_datum_00000, 420)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 422)
		var j json.BinaryJSON
		if j, err = json.ParseBinaryFromString(d.GetString()); err == nil {
			trace_util_0.Count(_datum_00000, 429)
			ret.SetMysqlJSON(j)
		}
	case KindInt64:
		trace_util_0.Count(_datum_00000, 423)
		i64 := d.GetInt64()
		ret.SetMysqlJSON(json.CreateBinary(i64))
	case KindUint64:
		trace_util_0.Count(_datum_00000, 424)
		u64 := d.GetUint64()
		ret.SetMysqlJSON(json.CreateBinary(u64))
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 425)
		f64 := d.GetFloat64()
		ret.SetMysqlJSON(json.CreateBinary(f64))
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 426)
		var f64 float64
		if f64, err = d.GetMysqlDecimal().ToFloat64(); err == nil {
			trace_util_0.Count(_datum_00000, 430)
			ret.SetMysqlJSON(json.CreateBinary(f64))
		}
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 427)
		ret = *d
	default:
		trace_util_0.Count(_datum_00000, 428)
		var s string
		if s, err = d.ToString(); err == nil {
			trace_util_0.Count(_datum_00000, 431)
			// TODO: fix precision of MysqlTime. For example,
			// On MySQL 5.7 CAST(NOW() AS JSON) -> "2011-11-11 11:11:11.111111",
			// But now we can only return "2011-11-11 11:11:11".
			ret.SetMysqlJSON(json.CreateBinary(s))
		}
	}
	trace_util_0.Count(_datum_00000, 421)
	return ret, errors.Trace(err)
}

// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Datum) ToBool(sc *stmtctx.StatementContext) (int64, error) {
	trace_util_0.Count(_datum_00000, 432)
	var err error
	isZero := false
	switch d.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 436)
		isZero = d.GetInt64() == 0
	case KindUint64:
		trace_util_0.Count(_datum_00000, 437)
		isZero = d.GetUint64() == 0
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 438)
		isZero = RoundFloat(d.GetFloat64()) == 0
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 439)
		isZero = RoundFloat(d.GetFloat64()) == 0
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 440)
		iVal, err1 := StrToInt(sc, d.GetString())
		isZero, err = iVal == 0, err1
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 441)
		isZero = d.GetMysqlTime().IsZero()
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 442)
		isZero = d.GetMysqlDuration().Duration == 0
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 443)
		v, err1 := d.GetMysqlDecimal().ToFloat64()
		isZero, err = RoundFloat(v) == 0, err1
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 444)
		isZero = d.GetMysqlEnum().ToNumber() == 0
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 445)
		isZero = d.GetMysqlSet().ToNumber() == 0
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 446)
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		isZero, err = val == 0, err1
	default:
		trace_util_0.Count(_datum_00000, 447)
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	trace_util_0.Count(_datum_00000, 433)
	var ret int64
	if isZero {
		trace_util_0.Count(_datum_00000, 448)
		ret = 0
	} else {
		trace_util_0.Count(_datum_00000, 449)
		{
			ret = 1
		}
	}
	trace_util_0.Count(_datum_00000, 434)
	if err != nil {
		trace_util_0.Count(_datum_00000, 450)
		return ret, errors.Trace(err)
	}
	trace_util_0.Count(_datum_00000, 435)
	return ret, nil
}

// ConvertDatumToDecimal converts datum to decimal.
func ConvertDatumToDecimal(sc *stmtctx.StatementContext, d Datum) (*MyDecimal, error) {
	trace_util_0.Count(_datum_00000, 451)
	dec := new(MyDecimal)
	var err error
	switch d.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 453)
		dec.FromInt(d.GetInt64())
	case KindUint64:
		trace_util_0.Count(_datum_00000, 454)
		dec.FromUint(d.GetUint64())
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 455)
		err = dec.FromFloat64(float64(d.GetFloat32()))
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 456)
		err = dec.FromFloat64(d.GetFloat64())
	case KindString:
		trace_util_0.Count(_datum_00000, 457)
		err = sc.HandleTruncate(dec.FromString(d.GetBytes()))
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 458)
		*dec = *d.GetMysqlDecimal()
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 459)
		dec.FromUint(d.GetMysqlEnum().Value)
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 460)
		dec.FromUint(d.GetMysqlSet().Value)
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 461)
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		dec.FromUint(val)
		err = err1
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 462)
		f, err1 := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		if err1 != nil {
			trace_util_0.Count(_datum_00000, 465)
			return nil, errors.Trace(err1)
		}
		trace_util_0.Count(_datum_00000, 463)
		err = dec.FromFloat64(f)
	default:
		trace_util_0.Count(_datum_00000, 464)
		err = fmt.Errorf("can't convert %v to decimal", d.GetValue())
	}
	trace_util_0.Count(_datum_00000, 452)
	return dec, errors.Trace(err)
}

// ToDecimal converts to a decimal.
func (d *Datum) ToDecimal(sc *stmtctx.StatementContext) (*MyDecimal, error) {
	trace_util_0.Count(_datum_00000, 466)
	switch d.Kind() {
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 467)
		return d.GetMysqlTime().ToNumber(), nil
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 468)
		return d.GetMysqlDuration().ToNumber(), nil
	default:
		trace_util_0.Count(_datum_00000, 469)
		return ConvertDatumToDecimal(sc, *d)
	}
}

// ToInt64 converts to a int64.
func (d *Datum) ToInt64(sc *stmtctx.StatementContext) (int64, error) {
	trace_util_0.Count(_datum_00000, 470)
	return d.toSignedInteger(sc, mysql.TypeLonglong)
}

func (d *Datum) toSignedInteger(sc *stmtctx.StatementContext, tp byte) (int64, error) {
	trace_util_0.Count(_datum_00000, 471)
	lowerBound := IntergerSignedLowerBound(tp)
	upperBound := IntergerSignedUpperBound(tp)
	switch d.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 472)
		return ConvertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case KindUint64:
		trace_util_0.Count(_datum_00000, 473)
		return ConvertUintToInt(d.GetUint64(), upperBound, tp)
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 474)
		return ConvertFloatToInt(float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 475)
		return ConvertFloatToInt(d.GetFloat64(), lowerBound, upperBound, tp)
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 476)
		iVal, err := StrToInt(sc, d.GetString())
		iVal, err2 := ConvertIntToInt(iVal, lowerBound, upperBound, tp)
		if err == nil {
			trace_util_0.Count(_datum_00000, 492)
			err = err2
		}
		trace_util_0.Count(_datum_00000, 477)
		return iVal, errors.Trace(err)
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 478)
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		// 2011-11-10 11:59:59.999999 -> 20111110120000
		t, err := d.GetMysqlTime().RoundFrac(sc, DefaultFsp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 493)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 479)
		ival, err := t.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			trace_util_0.Count(_datum_00000, 494)
			err = err2
		}
		trace_util_0.Count(_datum_00000, 480)
		return ival, errors.Trace(err)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 481)
		// 11:11:11.999999 -> 111112
		// 11:59:59.999999 -> 120000
		dur, err := d.GetMysqlDuration().RoundFrac(DefaultFsp)
		if err != nil {
			trace_util_0.Count(_datum_00000, 495)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 482)
		ival, err := dur.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			trace_util_0.Count(_datum_00000, 496)
			err = err2
		}
		trace_util_0.Count(_datum_00000, 483)
		return ival, errors.Trace(err)
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 484)
		var to MyDecimal
		err := d.GetMysqlDecimal().Round(&to, 0, ModeHalfEven)
		ival, err1 := to.ToInt()
		if err == nil {
			trace_util_0.Count(_datum_00000, 497)
			err = err1
		}
		trace_util_0.Count(_datum_00000, 485)
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			trace_util_0.Count(_datum_00000, 498)
			err = err2
		}
		trace_util_0.Count(_datum_00000, 486)
		return ival, errors.Trace(err)
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 487)
		fval := d.GetMysqlEnum().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 488)
		fval := d.GetMysqlSet().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 489)
		return ConvertJSONToInt(sc, d.GetMysqlJSON(), false)
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 490)
		val, err := d.GetBinaryLiteral().ToInt(sc)
		return int64(val), errors.Trace(err)
	default:
		trace_util_0.Count(_datum_00000, 491)
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Datum) ToFloat64(sc *stmtctx.StatementContext) (float64, error) {
	trace_util_0.Count(_datum_00000, 499)
	switch d.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 500)
		return float64(d.GetInt64()), nil
	case KindUint64:
		trace_util_0.Count(_datum_00000, 501)
		return float64(d.GetUint64()), nil
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 502)
		return float64(d.GetFloat32()), nil
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 503)
		return d.GetFloat64(), nil
	case KindString:
		trace_util_0.Count(_datum_00000, 504)
		return StrToFloat(sc, d.GetString())
	case KindBytes:
		trace_util_0.Count(_datum_00000, 505)
		return StrToFloat(sc, string(d.GetBytes()))
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 506)
		f, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 507)
		f, err := d.GetMysqlDuration().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 508)
		f, err := d.GetMysqlDecimal().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 509)
		return d.GetMysqlEnum().ToNumber(), nil
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 510)
		return d.GetMysqlSet().ToNumber(), nil
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 511)
		val, err := d.GetBinaryLiteral().ToInt(sc)
		return float64(val), errors.Trace(err)
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 512)
		f, err := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		return f, errors.Trace(err)
	default:
		trace_util_0.Count(_datum_00000, 513)
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", d.GetValue(), d.GetValue())
	}
}

// ToString gets the string representation of the datum.
func (d *Datum) ToString() (string, error) {
	trace_util_0.Count(_datum_00000, 514)
	switch d.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_00000, 515)
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case KindUint64:
		trace_util_0.Count(_datum_00000, 516)
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 517)
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 518)
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case KindString:
		trace_util_0.Count(_datum_00000, 519)
		return d.GetString(), nil
	case KindBytes:
		trace_util_0.Count(_datum_00000, 520)
		return d.GetString(), nil
	case KindMysqlTime:
		trace_util_0.Count(_datum_00000, 521)
		return d.GetMysqlTime().String(), nil
	case KindMysqlDuration:
		trace_util_0.Count(_datum_00000, 522)
		return d.GetMysqlDuration().String(), nil
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 523)
		return d.GetMysqlDecimal().String(), nil
	case KindMysqlEnum:
		trace_util_0.Count(_datum_00000, 524)
		return d.GetMysqlEnum().String(), nil
	case KindMysqlSet:
		trace_util_0.Count(_datum_00000, 525)
		return d.GetMysqlSet().String(), nil
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 526)
		return d.GetMysqlJSON().String(), nil
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 527)
		return d.GetBinaryLiteral().ToString(), nil
	default:
		trace_util_0.Count(_datum_00000, 528)
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// ToBytes gets the bytes representation of the datum.
func (d *Datum) ToBytes() ([]byte, error) {
	trace_util_0.Count(_datum_00000, 529)
	switch d.k {
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 530)
		return d.GetBytes(), nil
	default:
		trace_util_0.Count(_datum_00000, 531)
		str, err := d.ToString()
		if err != nil {
			trace_util_0.Count(_datum_00000, 533)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 532)
		return []byte(str), nil
	}
}

// ToMysqlJSON is similar to convertToMysqlJSON, except the
// latter parses from string, but the former uses it as primitive.
func (d *Datum) ToMysqlJSON() (j json.BinaryJSON, err error) {
	trace_util_0.Count(_datum_00000, 534)
	var in interface{}
	switch d.Kind() {
	case KindMysqlJSON:
		trace_util_0.Count(_datum_00000, 537)
		j = d.GetMysqlJSON()
		return
	case KindInt64:
		trace_util_0.Count(_datum_00000, 538)
		in = d.GetInt64()
	case KindUint64:
		trace_util_0.Count(_datum_00000, 539)
		in = d.GetUint64()
	case KindFloat32, KindFloat64:
		trace_util_0.Count(_datum_00000, 540)
		in = d.GetFloat64()
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 541)
		in, err = d.GetMysqlDecimal().ToFloat64()
	case KindString, KindBytes:
		trace_util_0.Count(_datum_00000, 542)
		in = d.GetString()
	case KindBinaryLiteral, KindMysqlBit:
		trace_util_0.Count(_datum_00000, 543)
		in = d.GetBinaryLiteral().ToString()
	case KindNull:
		trace_util_0.Count(_datum_00000, 544)
		in = nil
	default:
		trace_util_0.Count(_datum_00000, 545)
		in, err = d.ToString()
	}
	trace_util_0.Count(_datum_00000, 535)
	if err != nil {
		trace_util_0.Count(_datum_00000, 546)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_datum_00000, 536)
	j = json.CreateBinary(in)
	return
}

func invalidConv(d *Datum, tp byte) (Datum, error) {
	trace_util_0.Count(_datum_00000, 547)
	return Datum{}, errors.Errorf("cannot convert datum from %s to type %s.", KindStr(d.Kind()), TypeStr(tp))
}

func (d *Datum) convergeType(hasUint, hasDecimal, hasFloat *bool) (x Datum) {
	trace_util_0.Count(_datum_00000, 548)
	x = *d
	switch d.Kind() {
	case KindUint64:
		trace_util_0.Count(_datum_00000, 550)
		*hasUint = true
	case KindFloat32:
		trace_util_0.Count(_datum_00000, 551)
		f := d.GetFloat32()
		x.SetFloat64(float64(f))
		*hasFloat = true
	case KindFloat64:
		trace_util_0.Count(_datum_00000, 552)
		*hasFloat = true
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_00000, 553)
		*hasDecimal = true
	}
	trace_util_0.Count(_datum_00000, 549)
	return x
}

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in interface{}) (d Datum) {
	trace_util_0.Count(_datum_00000, 554)
	switch x := in.(type) {
	case []interface{}:
		trace_util_0.Count(_datum_00000, 556)
		d.SetValue(MakeDatums(x...))
	default:
		trace_util_0.Count(_datum_00000, 557)
		d.SetValue(in)
	}
	trace_util_0.Count(_datum_00000, 555)
	return d
}

// NewIntDatum creates a new Datum from an int64 value.
func NewIntDatum(i int64) (d Datum) {
	trace_util_0.Count(_datum_00000, 558)
	d.SetInt64(i)
	return d
}

// NewUintDatum creates a new Datum from an uint64 value.
func NewUintDatum(i uint64) (d Datum) {
	trace_util_0.Count(_datum_00000, 559)
	d.SetUint64(i)
	return d
}

// NewBytesDatum creates a new Datum from a byte slice.
func NewBytesDatum(b []byte) (d Datum) {
	trace_util_0.Count(_datum_00000, 560)
	d.SetBytes(b)
	return d
}

// NewStringDatum creates a new Datum from a string.
func NewStringDatum(s string) (d Datum) {
	trace_util_0.Count(_datum_00000, 561)
	d.SetString(s)
	return d
}

// NewFloat64Datum creates a new Datum from a float64 value.
func NewFloat64Datum(f float64) (d Datum) {
	trace_util_0.Count(_datum_00000, 562)
	d.SetFloat64(f)
	return d
}

// NewFloat32Datum creates a new Datum from a float32 value.
func NewFloat32Datum(f float32) (d Datum) {
	trace_util_0.Count(_datum_00000, 563)
	d.SetFloat32(f)
	return d
}

// NewDurationDatum creates a new Datum from a Duration value.
func NewDurationDatum(dur Duration) (d Datum) {
	trace_util_0.Count(_datum_00000, 564)
	d.SetMysqlDuration(dur)
	return d
}

// NewTimeDatum creates a new Time from a Time value.
func NewTimeDatum(t Time) (d Datum) {
	trace_util_0.Count(_datum_00000, 565)
	d.SetMysqlTime(t)
	return d
}

// NewDecimalDatum creates a new Datum form a MyDecimal value.
func NewDecimalDatum(dec *MyDecimal) (d Datum) {
	trace_util_0.Count(_datum_00000, 566)
	d.SetMysqlDecimal(dec)
	return d
}

// NewBinaryLiteralDatum creates a new BinaryLiteral Datum for a BinaryLiteral value.
func NewBinaryLiteralDatum(b BinaryLiteral) (d Datum) {
	trace_util_0.Count(_datum_00000, 567)
	d.SetBinaryLiteral(b)
	return d
}

// NewMysqlBitDatum creates a new MysqlBit Datum for a BinaryLiteral value.
func NewMysqlBitDatum(b BinaryLiteral) (d Datum) {
	trace_util_0.Count(_datum_00000, 568)
	d.SetMysqlBit(b)
	return d
}

// NewMysqlEnumDatum creates a new MysqlEnum Datum for a Enum value.
func NewMysqlEnumDatum(e Enum) (d Datum) {
	trace_util_0.Count(_datum_00000, 569)
	d.SetMysqlEnum(e)
	return d
}

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...interface{}) []Datum {
	trace_util_0.Count(_datum_00000, 570)
	datums := make([]Datum, len(args))
	for i, v := range args {
		trace_util_0.Count(_datum_00000, 572)
		datums[i] = NewDatum(v)
	}
	trace_util_0.Count(_datum_00000, 571)
	return datums
}

// MinNotNullDatum returns a datum represents minimum not null value.
func MinNotNullDatum() Datum {
	trace_util_0.Count(_datum_00000, 573)
	return Datum{k: KindMinNotNull}
}

// MaxValueDatum returns a datum represents max value.
func MaxValueDatum() Datum {
	trace_util_0.Count(_datum_00000, 574)
	return Datum{k: KindMaxValue}
}

// EqualDatums compare if a and b contains the same datum values.
func EqualDatums(sc *stmtctx.StatementContext, a []Datum, b []Datum) (bool, error) {
	trace_util_0.Count(_datum_00000, 575)
	if len(a) != len(b) {
		trace_util_0.Count(_datum_00000, 580)
		return false, nil
	}
	trace_util_0.Count(_datum_00000, 576)
	if a == nil && b == nil {
		trace_util_0.Count(_datum_00000, 581)
		return true, nil
	}
	trace_util_0.Count(_datum_00000, 577)
	if a == nil || b == nil {
		trace_util_0.Count(_datum_00000, 582)
		return false, nil
	}
	trace_util_0.Count(_datum_00000, 578)
	for i, ai := range a {
		trace_util_0.Count(_datum_00000, 583)
		v, err := ai.CompareDatum(sc, &b[i])
		if err != nil {
			trace_util_0.Count(_datum_00000, 585)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 584)
		if v != 0 {
			trace_util_0.Count(_datum_00000, 586)
			return false, nil
		}
	}
	trace_util_0.Count(_datum_00000, 579)
	return true, nil
}

// SortDatums sorts a slice of datum.
func SortDatums(sc *stmtctx.StatementContext, datums []Datum) error {
	trace_util_0.Count(_datum_00000, 587)
	sorter := datumsSorter{datums: datums, sc: sc}
	sort.Sort(&sorter)
	return sorter.err
}

type datumsSorter struct {
	datums []Datum
	sc     *stmtctx.StatementContext
	err    error
}

func (ds *datumsSorter) Len() int {
	trace_util_0.Count(_datum_00000, 588)
	return len(ds.datums)
}

func (ds *datumsSorter) Less(i, j int) bool {
	trace_util_0.Count(_datum_00000, 589)
	cmp, err := ds.datums[i].CompareDatum(ds.sc, &ds.datums[j])
	if err != nil {
		trace_util_0.Count(_datum_00000, 591)
		ds.err = errors.Trace(err)
		return true
	}
	trace_util_0.Count(_datum_00000, 590)
	return cmp < 0
}

func (ds *datumsSorter) Swap(i, j int) {
	trace_util_0.Count(_datum_00000, 592)
	ds.datums[i], ds.datums[j] = ds.datums[j], ds.datums[i]
}

func handleTruncateError(sc *stmtctx.StatementContext) error {
	trace_util_0.Count(_datum_00000, 593)
	if sc.IgnoreTruncate {
		trace_util_0.Count(_datum_00000, 596)
		return nil
	}
	trace_util_0.Count(_datum_00000, 594)
	if !sc.TruncateAsWarning {
		trace_util_0.Count(_datum_00000, 597)
		return ErrTruncated
	}
	trace_util_0.Count(_datum_00000, 595)
	sc.AppendWarning(ErrTruncated)
	return nil
}

// DatumsToString converts several datums to formatted string.
func DatumsToString(datums []Datum, handleSpecialValue bool) (string, error) {
	trace_util_0.Count(_datum_00000, 598)
	strs := make([]string, 0, len(datums))
	for _, datum := range datums {
		trace_util_0.Count(_datum_00000, 601)
		if handleSpecialValue {
			trace_util_0.Count(_datum_00000, 604)
			switch datum.Kind() {
			case KindNull:
				trace_util_0.Count(_datum_00000, 605)
				strs = append(strs, "NULL")
				continue
			case KindMinNotNull:
				trace_util_0.Count(_datum_00000, 606)
				strs = append(strs, "-inf")
				continue
			case KindMaxValue:
				trace_util_0.Count(_datum_00000, 607)
				strs = append(strs, "+inf")
				continue
			}
		}
		trace_util_0.Count(_datum_00000, 602)
		str, err := datum.ToString()
		if err != nil {
			trace_util_0.Count(_datum_00000, 608)
			return "", errors.Trace(err)
		}
		trace_util_0.Count(_datum_00000, 603)
		strs = append(strs, str)
	}
	trace_util_0.Count(_datum_00000, 599)
	size := len(datums)
	if size > 1 {
		trace_util_0.Count(_datum_00000, 609)
		strs[0] = "(" + strs[0]
		strs[size-1] = strs[size-1] + ")"
	}
	trace_util_0.Count(_datum_00000, 600)
	return strings.Join(strs, ", "), nil
}

// DatumsToStrNoErr converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
func DatumsToStrNoErr(datums []Datum) string {
	trace_util_0.Count(_datum_00000, 610)
	str, err := DatumsToString(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// CloneDatum returns a new copy of the datum.
// TODO: Abandon this function.
func CloneDatum(datum Datum) Datum {
	trace_util_0.Count(_datum_00000, 611)
	return *datum.Copy()
}

// CloneRow deep copies a Datum slice.
func CloneRow(dr []Datum) []Datum {
	trace_util_0.Count(_datum_00000, 612)
	c := make([]Datum, len(dr))
	for i, d := range dr {
		trace_util_0.Count(_datum_00000, 614)
		c[i] = *d.Copy()
	}
	trace_util_0.Count(_datum_00000, 613)
	return c
}

var _datum_00000 = "types/datum.go"
