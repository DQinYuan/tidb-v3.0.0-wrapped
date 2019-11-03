// Copyright 2015 PingCAP, Inc.
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

package codec

import (
	"encoding/binary"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	bytesFlag        byte = 1
	compactBytesFlag byte = 2
	intFlag          byte = 3
	uintFlag         byte = 4
	floatFlag        byte = 5
	decimalFlag      byte = 6
	durationFlag     byte = 7
	varintFlag       byte = 8
	uvarintFlag      byte = 9
	jsonFlag         byte = 10
	maxFlag          byte = 250
)

// encode will encode a datum and append it to a byte slice. If comparable is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(sc *stmtctx.StatementContext, b []byte, vals []types.Datum, comparable bool, hash bool) (_ []byte, err error) {
	trace_util_0.Count(_codec_00000, 0)
	for i, length := 0, len(vals); i < length; i++ {
		trace_util_0.Count(_codec_00000, 2)
		switch vals[i].Kind() {
		case types.KindInt64:
			trace_util_0.Count(_codec_00000, 3)
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable)
		case types.KindUint64:
			trace_util_0.Count(_codec_00000, 4)
			if hash {
				trace_util_0.Count(_codec_00000, 18)
				integer := vals[i].GetInt64()
				if integer < 0 {
					trace_util_0.Count(_codec_00000, 19)
					b = encodeUnsignedInt(b, uint64(integer), comparable)
				} else {
					trace_util_0.Count(_codec_00000, 20)
					{
						b = encodeSignedInt(b, integer, comparable)
					}
				}
			} else {
				trace_util_0.Count(_codec_00000, 21)
				{
					b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable)
				}
			}
		case types.KindFloat32, types.KindFloat64:
			trace_util_0.Count(_codec_00000, 5)
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.KindString, types.KindBytes:
			trace_util_0.Count(_codec_00000, 6)
			b = encodeBytes(b, vals[i].GetBytes(), comparable)
		case types.KindMysqlTime:
			trace_util_0.Count(_codec_00000, 7)
			b = append(b, uintFlag)
			b, err = EncodeMySQLTime(sc, vals[i], mysql.TypeUnspecified, b)
			if err != nil {
				trace_util_0.Count(_codec_00000, 22)
				return nil, err
			}
		case types.KindMysqlDuration:
			trace_util_0.Count(_codec_00000, 8)
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(vals[i].GetMysqlDuration().Duration))
		case types.KindMysqlDecimal:
			trace_util_0.Count(_codec_00000, 9)
			b = append(b, decimalFlag)
			if hash {
				trace_util_0.Count(_codec_00000, 23)
				// If hash is true, we only consider the original value of this decimal and ignore it's precision.
				dec := vals[i].GetMysqlDecimal()
				var bin []byte
				bin, err = dec.ToHashKey()
				if err != nil {
					trace_util_0.Count(_codec_00000, 25)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_codec_00000, 24)
				b = append(b, bin...)
			} else {
				trace_util_0.Count(_codec_00000, 26)
				{
					b, err = EncodeDecimal(b, vals[i].GetMysqlDecimal(), vals[i].Length(), vals[i].Frac())
					if terror.ErrorEqual(err, types.ErrTruncated) {
						trace_util_0.Count(_codec_00000, 27)
						err = sc.HandleTruncate(err)
					} else {
						trace_util_0.Count(_codec_00000, 28)
						if terror.ErrorEqual(err, types.ErrOverflow) {
							trace_util_0.Count(_codec_00000, 29)
							err = sc.HandleOverflow(err, err)
						}
					}
				}
			}
		case types.KindMysqlEnum:
			trace_util_0.Count(_codec_00000, 10)
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlEnum().ToNumber()), comparable)
		case types.KindMysqlSet:
			trace_util_0.Count(_codec_00000, 11)
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlSet().ToNumber()), comparable)
		case types.KindMysqlBit, types.KindBinaryLiteral:
			trace_util_0.Count(_codec_00000, 12)
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			var val uint64
			val, err = vals[i].GetBinaryLiteral().ToInt(sc)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable)
		case types.KindMysqlJSON:
			trace_util_0.Count(_codec_00000, 13)
			b = append(b, jsonFlag)
			j := vals[i].GetMysqlJSON()
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		case types.KindNull:
			trace_util_0.Count(_codec_00000, 14)
			b = append(b, NilFlag)
		case types.KindMinNotNull:
			trace_util_0.Count(_codec_00000, 15)
			b = append(b, bytesFlag)
		case types.KindMaxValue:
			trace_util_0.Count(_codec_00000, 16)
			b = append(b, maxFlag)
		default:
			trace_util_0.Count(_codec_00000, 17)
			return nil, errors.Errorf("unsupport encode type %d", vals[i].Kind())
		}
	}

	trace_util_0.Count(_codec_00000, 1)
	return b, errors.Trace(err)
}

// EncodeMySQLTime encodes datum of `KindMysqlTime` to []byte.
func EncodeMySQLTime(sc *stmtctx.StatementContext, d types.Datum, tp byte, b []byte) (_ []byte, err error) {
	trace_util_0.Count(_codec_00000, 30)
	t := d.GetMysqlTime()
	// Encoding timestamp need to consider timezone. If it's not in UTC, transform to UTC first.
	// This is compatible with `PBToExpr > convertTime`, and coprocessor assumes the passed timestamp is in UTC as well.
	if tp == mysql.TypeUnspecified {
		trace_util_0.Count(_codec_00000, 34)
		tp = t.Type
	}
	trace_util_0.Count(_codec_00000, 31)
	if tp == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
		trace_util_0.Count(_codec_00000, 35)
		err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
		if err != nil {
			trace_util_0.Count(_codec_00000, 36)
			return nil, err
		}
	}
	trace_util_0.Count(_codec_00000, 32)
	var v uint64
	v, err = t.ToPackedUint()
	if err != nil {
		trace_util_0.Count(_codec_00000, 37)
		return nil, err
	}
	trace_util_0.Count(_codec_00000, 33)
	b = EncodeUint(b, v)
	return b, nil
}

func encodeBytes(b []byte, v []byte, comparable bool) []byte {
	trace_util_0.Count(_codec_00000, 38)
	if comparable {
		trace_util_0.Count(_codec_00000, 40)
		b = append(b, bytesFlag)
		b = EncodeBytes(b, v)
	} else {
		trace_util_0.Count(_codec_00000, 41)
		{
			b = append(b, compactBytesFlag)
			b = EncodeCompactBytes(b, v)
		}
	}
	trace_util_0.Count(_codec_00000, 39)
	return b
}

func encodeSignedInt(b []byte, v int64, comparable bool) []byte {
	trace_util_0.Count(_codec_00000, 42)
	if comparable {
		trace_util_0.Count(_codec_00000, 44)
		b = append(b, intFlag)
		b = EncodeInt(b, v)
	} else {
		trace_util_0.Count(_codec_00000, 45)
		{
			b = append(b, varintFlag)
			b = EncodeVarint(b, v)
		}
	}
	trace_util_0.Count(_codec_00000, 43)
	return b
}

func encodeUnsignedInt(b []byte, v uint64, comparable bool) []byte {
	trace_util_0.Count(_codec_00000, 46)
	if comparable {
		trace_util_0.Count(_codec_00000, 48)
		b = append(b, uintFlag)
		b = EncodeUint(b, v)
	} else {
		trace_util_0.Count(_codec_00000, 49)
		{
			b = append(b, uvarintFlag)
			b = EncodeUvarint(b, v)
		}
	}
	trace_util_0.Count(_codec_00000, 47)
	return b
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For Decimal type, datum must set datum's length and frac.
func EncodeKey(sc *stmtctx.StatementContext, b []byte, v ...types.Datum) ([]byte, error) {
	trace_util_0.Count(_codec_00000, 50)
	return encode(sc, b, v, true, false)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(sc *stmtctx.StatementContext, b []byte, v ...types.Datum) ([]byte, error) {
	trace_util_0.Count(_codec_00000, 51)
	return encode(sc, b, v, false, false)
}

func encodeHashChunkRow(sc *stmtctx.StatementContext, b []byte, row chunk.Row, allTypes []*types.FieldType, colIdx []int) (_ []byte, err error) {
	trace_util_0.Count(_codec_00000, 52)
	const comparable = false
	for _, i := range colIdx {
		trace_util_0.Count(_codec_00000, 54)
		if row.IsNull(i) {
			trace_util_0.Count(_codec_00000, 56)
			b = append(b, NilFlag)
			continue
		}
		trace_util_0.Count(_codec_00000, 55)
		switch allTypes[i].Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			trace_util_0.Count(_codec_00000, 57)
			if !mysql.HasUnsignedFlag(allTypes[i].Flag) {
				trace_util_0.Count(_codec_00000, 73)
				b = encodeSignedInt(b, row.GetInt64(i), comparable)
				break
			}
			// encode unsigned integers.
			trace_util_0.Count(_codec_00000, 58)
			integer := row.GetInt64(i)
			if integer < 0 {
				trace_util_0.Count(_codec_00000, 74)
				b = encodeUnsignedInt(b, uint64(integer), comparable)
			} else {
				trace_util_0.Count(_codec_00000, 75)
				{
					b = encodeSignedInt(b, integer, comparable)
				}
			}
		case mysql.TypeFloat:
			trace_util_0.Count(_codec_00000, 59)
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(row.GetFloat32(i)))
		case mysql.TypeDouble:
			trace_util_0.Count(_codec_00000, 60)
			b = append(b, floatFlag)
			b = EncodeFloat(b, row.GetFloat64(i))
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			trace_util_0.Count(_codec_00000, 61)
			b = encodeBytes(b, row.GetBytes(i), comparable)
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_codec_00000, 62)
			b = append(b, uintFlag)
			t := row.GetTime(i)
			// Encoding timestamp need to consider timezone.
			// If it's not in UTC, transform to UTC first.
			if t.Type == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
				trace_util_0.Count(_codec_00000, 76)
				err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
				if err != nil {
					trace_util_0.Count(_codec_00000, 77)
					return nil, errors.Trace(err)
				}
			}
			trace_util_0.Count(_codec_00000, 63)
			var v uint64
			v, err = t.ToPackedUint()
			if err != nil {
				trace_util_0.Count(_codec_00000, 78)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_codec_00000, 64)
			b = EncodeUint(b, v)
		case mysql.TypeDuration:
			trace_util_0.Count(_codec_00000, 65)
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(row.GetDuration(i, 0).Duration))
		case mysql.TypeNewDecimal:
			trace_util_0.Count(_codec_00000, 66)
			b = append(b, decimalFlag)
			// If hash is true, we only consider the original value of this decimal and ignore it's precision.
			dec := row.GetMyDecimal(i)
			bin, err := dec.ToHashKey()
			if err != nil {
				trace_util_0.Count(_codec_00000, 79)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_codec_00000, 67)
			b = append(b, bin...)
		case mysql.TypeEnum:
			trace_util_0.Count(_codec_00000, 68)
			b = encodeUnsignedInt(b, uint64(row.GetEnum(i).ToNumber()), comparable)
		case mysql.TypeSet:
			trace_util_0.Count(_codec_00000, 69)
			b = encodeUnsignedInt(b, uint64(row.GetSet(i).ToNumber()), comparable)
		case mysql.TypeBit:
			trace_util_0.Count(_codec_00000, 70)
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			var val uint64
			val, err = types.BinaryLiteral(row.GetBytes(i)).ToInt(sc)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable)
		case mysql.TypeJSON:
			trace_util_0.Count(_codec_00000, 71)
			b = append(b, jsonFlag)
			j := row.GetJSON(i)
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		default:
			trace_util_0.Count(_codec_00000, 72)
			return nil, errors.Errorf("unsupport column type for encode %d", allTypes[i].Tp)
		}
	}
	trace_util_0.Count(_codec_00000, 53)
	return b, errors.Trace(err)
}

// HashValues appends the encoded values to byte slice b, returning the appended
// slice. If two datums are equal, they will generate the same bytes.
func HashValues(sc *stmtctx.StatementContext, b []byte, v ...types.Datum) ([]byte, error) {
	trace_util_0.Count(_codec_00000, 80)
	return encode(sc, b, v, false, true)
}

// HashChunkRow appends the encoded values to byte slice "b", returning the appended slice.
// If two rows are equal, it will generate the same bytes.
func HashChunkRow(sc *stmtctx.StatementContext, b []byte, row chunk.Row, allTypes []*types.FieldType, colIdx []int) ([]byte, error) {
	trace_util_0.Count(_codec_00000, 81)
	return encodeHashChunkRow(sc, b, row, allTypes, colIdx)
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
// size is the size of decoded datum slice.
func Decode(b []byte, size int) ([]types.Datum, error) {
	trace_util_0.Count(_codec_00000, 82)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 85)
		return nil, errors.New("invalid encoded key")
	}

	trace_util_0.Count(_codec_00000, 83)
	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	for len(b) > 0 {
		trace_util_0.Count(_codec_00000, 86)
		var d types.Datum
		b, d, err = DecodeOne(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 88)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_codec_00000, 87)
		values = append(values, d)
	}

	trace_util_0.Count(_codec_00000, 84)
	return values, nil
}

// DecodeRange decodes the range values from a byte slice that generated by EncodeKey.
// It handles some special values like `MinNotNull` and `MaxValueDatum`.
func DecodeRange(b []byte, size int) ([]types.Datum, error) {
	trace_util_0.Count(_codec_00000, 89)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 93)
		return nil, errors.New("invalid encoded key: length of key is zero")
	}

	trace_util_0.Count(_codec_00000, 90)
	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	for len(b) > 1 {
		trace_util_0.Count(_codec_00000, 94)
		var d types.Datum
		b, d, err = DecodeOne(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 96)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 95)
		values = append(values, d)
	}

	trace_util_0.Count(_codec_00000, 91)
	if len(b) == 1 {
		trace_util_0.Count(_codec_00000, 97)
		switch b[0] {
		case NilFlag:
			trace_util_0.Count(_codec_00000, 98)
			values = append(values, types.Datum{})
		case bytesFlag:
			trace_util_0.Count(_codec_00000, 99)
			values = append(values, types.MinNotNullDatum())
		// `maxFlag + 1` for PrefixNext
		case maxFlag, maxFlag + 1:
			trace_util_0.Count(_codec_00000, 100)
			values = append(values, types.MaxValueDatum())
		default:
			trace_util_0.Count(_codec_00000, 101)
			return nil, errors.Errorf("invalid encoded key flag %v", b[0])
		}
	}
	trace_util_0.Count(_codec_00000, 92)
	return values, nil
}

// DecodeOne decodes on datum from a byte slice generated with EncodeKey or EncodeValue.
func DecodeOne(b []byte) (remain []byte, d types.Datum, err error) {
	trace_util_0.Count(_codec_00000, 102)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 106)
		return nil, d, errors.New("invalid encoded key")
	}
	trace_util_0.Count(_codec_00000, 103)
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		trace_util_0.Count(_codec_00000, 107)
		var v int64
		b, v, err = DecodeInt(b)
		d.SetInt64(v)
	case uintFlag:
		trace_util_0.Count(_codec_00000, 108)
		var v uint64
		b, v, err = DecodeUint(b)
		d.SetUint64(v)
	case varintFlag:
		trace_util_0.Count(_codec_00000, 109)
		var v int64
		b, v, err = DecodeVarint(b)
		d.SetInt64(v)
	case uvarintFlag:
		trace_util_0.Count(_codec_00000, 110)
		var v uint64
		b, v, err = DecodeUvarint(b)
		d.SetUint64(v)
	case floatFlag:
		trace_util_0.Count(_codec_00000, 111)
		var v float64
		b, v, err = DecodeFloat(b)
		d.SetFloat64(v)
	case bytesFlag:
		trace_util_0.Count(_codec_00000, 112)
		var v []byte
		b, v, err = DecodeBytes(b, nil)
		d.SetBytes(v)
	case compactBytesFlag:
		trace_util_0.Count(_codec_00000, 113)
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		trace_util_0.Count(_codec_00000, 114)
		var (
			dec             *types.MyDecimal
			precision, frac int
		)
		b, dec, precision, frac, err = DecodeDecimal(b)
		if err == nil {
			trace_util_0.Count(_codec_00000, 120)
			d.SetMysqlDecimal(dec)
			d.SetLength(precision)
			d.SetFrac(frac)
		}
	case durationFlag:
		trace_util_0.Count(_codec_00000, 115)
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			trace_util_0.Count(_codec_00000, 121)
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetValue(v)
		}
	case jsonFlag:
		trace_util_0.Count(_codec_00000, 116)
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 122)
			return b, d, err
		}
		trace_util_0.Count(_codec_00000, 117)
		j := json.BinaryJSON{TypeCode: b[0], Value: b[1:size]}
		d.SetMysqlJSON(j)
		b = b[size:]
	case NilFlag:
		trace_util_0.Count(_codec_00000, 118)
	default:
		trace_util_0.Count(_codec_00000, 119)
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	trace_util_0.Count(_codec_00000, 104)
	if err != nil {
		trace_util_0.Count(_codec_00000, 123)
		return b, d, errors.Trace(err)
	}
	trace_util_0.Count(_codec_00000, 105)
	return b, d, nil
}

// CutOne cuts the first encoded value from b.
// It will return the first encoded item and the remains as byte slice.
func CutOne(b []byte) (data []byte, remain []byte, err error) {
	trace_util_0.Count(_codec_00000, 124)
	l, err := peek(b)
	if err != nil {
		trace_util_0.Count(_codec_00000, 126)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_codec_00000, 125)
	return b[:l], b[l:], nil
}

// CutColumnID cuts the column ID from b.
// It will return the remains as byte slice and column ID
func CutColumnID(b []byte) (remain []byte, n int64, err error) {
	trace_util_0.Count(_codec_00000, 127)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 129)
		return nil, 0, errors.New("invalid encoded key")
	}
	// skip the flag
	trace_util_0.Count(_codec_00000, 128)
	b = b[1:]
	return DecodeVarint(b)
}

// SetRawValues set raw datum values from a row data.
func SetRawValues(data []byte, values []types.Datum) error {
	trace_util_0.Count(_codec_00000, 130)
	for i := 0; i < len(values); i++ {
		trace_util_0.Count(_codec_00000, 132)
		l, err := peek(data)
		if err != nil {
			trace_util_0.Count(_codec_00000, 134)
			return errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 133)
		values[i].SetRaw(data[:l:l])
		data = data[l:]
	}
	trace_util_0.Count(_codec_00000, 131)
	return nil
}

// peek peeks the first encoded value from b and returns its length.
func peek(b []byte) (length int, err error) {
	trace_util_0.Count(_codec_00000, 135)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 139)
		return 0, errors.New("invalid encoded key")
	}
	trace_util_0.Count(_codec_00000, 136)
	flag := b[0]
	length++
	b = b[1:]
	var l int
	switch flag {
	case NilFlag:
		trace_util_0.Count(_codec_00000, 140)
	case intFlag, uintFlag, floatFlag, durationFlag:
		trace_util_0.Count(_codec_00000, 141)
		// Those types are stored in 8 bytes.
		l = 8
	case bytesFlag:
		trace_util_0.Count(_codec_00000, 142)
		l, err = peekBytes(b)
	case compactBytesFlag:
		trace_util_0.Count(_codec_00000, 143)
		l, err = peekCompactBytes(b)
	case decimalFlag:
		trace_util_0.Count(_codec_00000, 144)
		l, err = types.DecimalPeak(b)
	case varintFlag:
		trace_util_0.Count(_codec_00000, 145)
		l, err = peekVarint(b)
	case uvarintFlag:
		trace_util_0.Count(_codec_00000, 146)
		l, err = peekUvarint(b)
	case jsonFlag:
		trace_util_0.Count(_codec_00000, 147)
		l, err = json.PeekBytesAsJSON(b)
	default:
		trace_util_0.Count(_codec_00000, 148)
		return 0, errors.Errorf("invalid encoded key flag %v", flag)
	}
	trace_util_0.Count(_codec_00000, 137)
	if err != nil {
		trace_util_0.Count(_codec_00000, 149)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_codec_00000, 138)
	length += l
	return
}

func peekBytes(b []byte) (int, error) {
	trace_util_0.Count(_codec_00000, 150)
	offset := 0
	for {
		trace_util_0.Count(_codec_00000, 152)
		if len(b) < offset+encGroupSize+1 {
			trace_util_0.Count(_codec_00000, 154)
			return 0, errors.New("insufficient bytes to decode value")
		}
		// The byte slice is encoded into many groups.
		// For each group, there are 8 bytes for data and 1 byte for marker.
		trace_util_0.Count(_codec_00000, 153)
		marker := b[offset+encGroupSize]
		padCount := encMarker - marker
		offset += encGroupSize + 1
		// When padCount is not zero, it means we get the end of the byte slice.
		if padCount != 0 {
			trace_util_0.Count(_codec_00000, 155)
			break
		}
	}
	trace_util_0.Count(_codec_00000, 151)
	return offset, nil
}

func peekCompactBytes(b []byte) (int, error) {
	trace_util_0.Count(_codec_00000, 156)
	// Get length.
	v, n := binary.Varint(b)
	vi := int(v)
	if n < 0 {
		trace_util_0.Count(_codec_00000, 159)
		return 0, errors.New("value larger than 64 bits")
	} else {
		trace_util_0.Count(_codec_00000, 160)
		if n == 0 {
			trace_util_0.Count(_codec_00000, 161)
			return 0, errors.New("insufficient bytes to decode value")
		}
	}
	trace_util_0.Count(_codec_00000, 157)
	if len(b) < vi+n {
		trace_util_0.Count(_codec_00000, 162)
		return 0, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	trace_util_0.Count(_codec_00000, 158)
	return n + vi, nil
}

func peekVarint(b []byte) (int, error) {
	trace_util_0.Count(_codec_00000, 163)
	_, n := binary.Varint(b)
	if n < 0 {
		trace_util_0.Count(_codec_00000, 165)
		return 0, errors.New("value larger than 64 bits")
	}
	trace_util_0.Count(_codec_00000, 164)
	return n, nil
}

func peekUvarint(b []byte) (int, error) {
	trace_util_0.Count(_codec_00000, 166)
	_, n := binary.Uvarint(b)
	if n < 0 {
		trace_util_0.Count(_codec_00000, 168)
		return 0, errors.New("value larger than 64 bits")
	}
	trace_util_0.Count(_codec_00000, 167)
	return n, nil
}

// Decoder is used to decode value to chunk.
type Decoder struct {
	chk      *chunk.Chunk
	timezone *time.Location

	// buf is only used for DecodeBytes to avoid the cost of makeslice.
	buf []byte
}

// NewDecoder creates a Decoder.
func NewDecoder(chk *chunk.Chunk, timezone *time.Location) *Decoder {
	trace_util_0.Count(_codec_00000, 169)
	return &Decoder{
		chk:      chk,
		timezone: timezone,
	}
}

// DecodeOne decodes one value to chunk and returns the remained bytes.
func (decoder *Decoder) DecodeOne(b []byte, colIdx int, ft *types.FieldType) (remain []byte, err error) {
	trace_util_0.Count(_codec_00000, 170)
	if len(b) < 1 {
		trace_util_0.Count(_codec_00000, 174)
		return nil, errors.New("invalid encoded key")
	}
	trace_util_0.Count(_codec_00000, 171)
	chk := decoder.chk
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		trace_util_0.Count(_codec_00000, 175)
		var v int64
		b, v, err = DecodeInt(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 197)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 176)
		appendIntToChunk(v, chk, colIdx, ft)
	case uintFlag:
		trace_util_0.Count(_codec_00000, 177)
		var v uint64
		b, v, err = DecodeUint(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 198)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 178)
		err = appendUintToChunk(v, chk, colIdx, ft, decoder.timezone)
	case varintFlag:
		trace_util_0.Count(_codec_00000, 179)
		var v int64
		b, v, err = DecodeVarint(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 199)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 180)
		appendIntToChunk(v, chk, colIdx, ft)
	case uvarintFlag:
		trace_util_0.Count(_codec_00000, 181)
		var v uint64
		b, v, err = DecodeUvarint(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 200)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 182)
		err = appendUintToChunk(v, chk, colIdx, ft, decoder.timezone)
	case floatFlag:
		trace_util_0.Count(_codec_00000, 183)
		var v float64
		b, v, err = DecodeFloat(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 201)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 184)
		appendFloatToChunk(v, chk, colIdx, ft)
	case bytesFlag:
		trace_util_0.Count(_codec_00000, 185)
		b, decoder.buf, err = DecodeBytes(b, decoder.buf)
		if err != nil {
			trace_util_0.Count(_codec_00000, 202)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 186)
		chk.AppendBytes(colIdx, decoder.buf)
	case compactBytesFlag:
		trace_util_0.Count(_codec_00000, 187)
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 203)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 188)
		chk.AppendBytes(colIdx, v)
	case decimalFlag:
		trace_util_0.Count(_codec_00000, 189)
		var dec *types.MyDecimal
		b, dec, _, _, err = DecodeDecimal(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 204)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 190)
		chk.AppendMyDecimal(colIdx, dec)
	case durationFlag:
		trace_util_0.Count(_codec_00000, 191)
		var r int64
		b, r, err = DecodeInt(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 205)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 192)
		v := types.Duration{Duration: time.Duration(r), Fsp: ft.Decimal}
		chk.AppendDuration(colIdx, v)
	case jsonFlag:
		trace_util_0.Count(_codec_00000, 193)
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			trace_util_0.Count(_codec_00000, 206)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 194)
		chk.AppendJSON(colIdx, json.BinaryJSON{TypeCode: b[0], Value: b[1:size]})
		b = b[size:]
	case NilFlag:
		trace_util_0.Count(_codec_00000, 195)
		chk.AppendNull(colIdx)
	default:
		trace_util_0.Count(_codec_00000, 196)
		return nil, errors.Errorf("invalid encoded key flag %v", flag)
	}
	trace_util_0.Count(_codec_00000, 172)
	if err != nil {
		trace_util_0.Count(_codec_00000, 207)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_codec_00000, 173)
	return b, nil
}

func appendIntToChunk(val int64, chk *chunk.Chunk, colIdx int, ft *types.FieldType) {
	trace_util_0.Count(_codec_00000, 208)
	switch ft.Tp {
	case mysql.TypeDuration:
		trace_util_0.Count(_codec_00000, 209)
		v := types.Duration{Duration: time.Duration(val), Fsp: ft.Decimal}
		chk.AppendDuration(colIdx, v)
	default:
		trace_util_0.Count(_codec_00000, 210)
		chk.AppendInt64(colIdx, val)
	}
}

func appendUintToChunk(val uint64, chk *chunk.Chunk, colIdx int, ft *types.FieldType, loc *time.Location) error {
	trace_util_0.Count(_codec_00000, 211)
	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_codec_00000, 213)
		var t types.Time
		t.Type = ft.Tp
		t.Fsp = ft.Decimal
		var err error
		err = t.FromPackedUint(val)
		if err != nil {
			trace_util_0.Count(_codec_00000, 222)
			return errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 214)
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			trace_util_0.Count(_codec_00000, 223)
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				trace_util_0.Count(_codec_00000, 224)
				return errors.Trace(err)
			}
		}
		trace_util_0.Count(_codec_00000, 215)
		chk.AppendTime(colIdx, t)
	case mysql.TypeEnum:
		trace_util_0.Count(_codec_00000, 216)
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, val)
		if err != nil {
			trace_util_0.Count(_codec_00000, 225)
			enum = types.Enum{}
		}
		trace_util_0.Count(_codec_00000, 217)
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		trace_util_0.Count(_codec_00000, 218)
		set, err := types.ParseSetValue(ft.Elems, val)
		if err != nil {
			trace_util_0.Count(_codec_00000, 226)
			return errors.Trace(err)
		}
		trace_util_0.Count(_codec_00000, 219)
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		trace_util_0.Count(_codec_00000, 220)
		byteSize := (ft.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(val, byteSize))
	default:
		trace_util_0.Count(_codec_00000, 221)
		chk.AppendUint64(colIdx, val)
	}
	trace_util_0.Count(_codec_00000, 212)
	return nil
}

func appendFloatToChunk(val float64, chk *chunk.Chunk, colIdx int, ft *types.FieldType) {
	trace_util_0.Count(_codec_00000, 227)
	if ft.Tp == mysql.TypeFloat {
		trace_util_0.Count(_codec_00000, 228)
		chk.AppendFloat32(colIdx, float32(val))
	} else {
		trace_util_0.Count(_codec_00000, 229)
		{
			chk.AppendFloat64(colIdx, val)
		}
	}
}

var _codec_00000 = "util/codec/codec.go"
