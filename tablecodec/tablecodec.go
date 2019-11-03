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

package tablecodec

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

var (
	errInvalidKey         = terror.ClassXEval.New(codeInvalidKey, "invalid key")
	errInvalidRecordKey   = terror.ClassXEval.New(codeInvalidRecordKey, "invalid record key")
	errInvalidIndexKey    = terror.ClassXEval.New(codeInvalidIndexKey, "invalid index key")
	errInvalidColumnCount = terror.ClassXEval.New(codeInvalidColumnCount, "invalid column count")
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

const (
	idLen                 = 8
	prefixLen             = 1 + idLen /*tableID*/ + 2
	recordRowKeyLen       = prefixLen + idLen /*handle*/
	tablePrefixLength     = 1
	recordPrefixSepLength = 2
)

// TableSplitKeyLen is the length of key 't{table_id}' which is used for table split.
const TableSplitKeyLen = 1 + idLen

// TablePrefix returns table's prefix 't'.
func TablePrefix() []byte {
	trace_util_0.Count(_tablecodec_00000, 0)
	return tablePrefix
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 1)
	buf := make([]byte, 0, recordRowKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 2)
	buf := make([]byte, 0, recordRowKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = codec.EncodeInt(buf, handle)
	return buf
}

// CutRowKeyPrefix cuts the row key prefix.
func CutRowKeyPrefix(key kv.Key) []byte {
	trace_util_0.Count(_tablecodec_00000, 3)
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 4)
	buf := make([]byte, 0, len(recordPrefix)+idLen)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)
	return buf
}

func hasTablePrefix(key kv.Key) bool {
	trace_util_0.Count(_tablecodec_00000, 5)
	return key[0] == tablePrefix[0]
}

func hasRecordPrefixSep(key kv.Key) bool {
	trace_util_0.Count(_tablecodec_00000, 6)
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle int64, err error) {
	trace_util_0.Count(_tablecodec_00000, 7)
	if len(key) <= prefixLen {
		trace_util_0.Count(_tablecodec_00000, 13)
		return 0, 0, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}

	trace_util_0.Count(_tablecodec_00000, 8)
	k := key
	if !hasTablePrefix(key) {
		trace_util_0.Count(_tablecodec_00000, 14)
		return 0, 0, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	trace_util_0.Count(_tablecodec_00000, 9)
	key = key[tablePrefixLength:]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 15)
		return 0, 0, errors.Trace(err)
	}

	trace_util_0.Count(_tablecodec_00000, 10)
	if !hasRecordPrefixSep(key) {
		trace_util_0.Count(_tablecodec_00000, 16)
		return 0, 0, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	trace_util_0.Count(_tablecodec_00000, 11)
	key = key[recordPrefixSepLength:]
	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 17)
		return 0, 0, errors.Trace(err)
	}
	trace_util_0.Count(_tablecodec_00000, 12)
	return
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
	trace_util_0.Count(_tablecodec_00000, 18)
	k := key

	tableID, indexID, isRecord, err := DecodeKeyHead(key)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 22)
		return 0, 0, nil, errors.Trace(err)
	}
	trace_util_0.Count(_tablecodec_00000, 19)
	if isRecord {
		trace_util_0.Count(_tablecodec_00000, 23)
		return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
	}
	trace_util_0.Count(_tablecodec_00000, 20)
	key = key[prefixLen+idLen:]

	for len(key) > 0 {
		trace_util_0.Count(_tablecodec_00000, 24)
		// FIXME: Without the schema information, we can only decode the raw kind of
		// the column. For instance, MysqlTime is internally saved as uint64.
		remain, d, e := codec.DecodeOne(key)
		if e != nil {
			trace_util_0.Count(_tablecodec_00000, 27)
			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e)
		}
		trace_util_0.Count(_tablecodec_00000, 25)
		str, e1 := d.ToString()
		if e1 != nil {
			trace_util_0.Count(_tablecodec_00000, 28)
			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e1)
		}
		trace_util_0.Count(_tablecodec_00000, 26)
		indexValues = append(indexValues, str)
		key = remain
	}
	trace_util_0.Count(_tablecodec_00000, 21)
	return
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	trace_util_0.Count(_tablecodec_00000, 29)
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		trace_util_0.Count(_tablecodec_00000, 35)
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	trace_util_0.Count(_tablecodec_00000, 30)
	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 36)
		err = errors.Trace(err)
		return
	}

	trace_util_0.Count(_tablecodec_00000, 31)
	if key.HasPrefix(recordPrefixSep) {
		trace_util_0.Count(_tablecodec_00000, 37)
		isRecordKey = true
		return
	}
	trace_util_0.Count(_tablecodec_00000, 32)
	if !key.HasPrefix(indexPrefixSep) {
		trace_util_0.Count(_tablecodec_00000, 38)
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	trace_util_0.Count(_tablecodec_00000, 33)
	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 39)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_tablecodec_00000, 34)
	return
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	trace_util_0.Count(_tablecodec_00000, 40)
	if !key.HasPrefix(tablePrefix) {
		trace_util_0.Count(_tablecodec_00000, 42)
		return 0
	}
	trace_util_0.Count(_tablecodec_00000, 41)
	key = key[len(tablePrefix):]
	_, tableID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (int64, error) {
	trace_util_0.Count(_tablecodec_00000, 43)
	if len(key) != recordRowKeyLen || !hasTablePrefix(key) || !hasRecordPrefixSep(key[prefixLen-2:]) {
		trace_util_0.Count(_tablecodec_00000, 45)
		return 0, errInvalidKey.GenWithStack("invalid key - %q", key)
	}
	trace_util_0.Count(_tablecodec_00000, 44)
	u := binary.BigEndian.Uint64(key[prefixLen:])
	return codec.DecodeCmpUintToInt(u), nil
}

// EncodeValue encodes a go value to bytes.
func EncodeValue(sc *stmtctx.StatementContext, raw types.Datum) ([]byte, error) {
	trace_util_0.Count(_tablecodec_00000, 46)
	var v types.Datum
	err := flatten(sc, raw, &v)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 48)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_tablecodec_00000, 47)
	b, err := codec.EncodeValue(sc, nil, v)
	return b, errors.Trace(err)
}

// EncodeRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
func EncodeRow(sc *stmtctx.StatementContext, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum) ([]byte, error) {
	trace_util_0.Count(_tablecodec_00000, 49)
	if len(row) != len(colIDs) {
		trace_util_0.Count(_tablecodec_00000, 54)
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	trace_util_0.Count(_tablecodec_00000, 50)
	valBuf = valBuf[:0]
	if values == nil {
		trace_util_0.Count(_tablecodec_00000, 55)
		values = make([]types.Datum, len(row)*2)
	}
	trace_util_0.Count(_tablecodec_00000, 51)
	for i, c := range row {
		trace_util_0.Count(_tablecodec_00000, 56)
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(sc, c, &values[2*i+1])
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 57)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_tablecodec_00000, 52)
	if len(values) == 0 {
		trace_util_0.Count(_tablecodec_00000, 58)
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	trace_util_0.Count(_tablecodec_00000, 53)
	return codec.EncodeValue(sc, valBuf, values...)
}

func flatten(sc *stmtctx.StatementContext, data types.Datum, ret *types.Datum) error {
	trace_util_0.Count(_tablecodec_00000, 59)
	switch data.Kind() {
	case types.KindMysqlTime:
		trace_util_0.Count(_tablecodec_00000, 60)
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
			trace_util_0.Count(_tablecodec_00000, 68)
			err := t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				trace_util_0.Count(_tablecodec_00000, 69)
				return errors.Trace(err)
			}
		}
		trace_util_0.Count(_tablecodec_00000, 61)
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.KindMysqlDuration:
		trace_util_0.Count(_tablecodec_00000, 62)
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		trace_util_0.Count(_tablecodec_00000, 63)
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		trace_util_0.Count(_tablecodec_00000, 64)
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		trace_util_0.Count(_tablecodec_00000, 65)
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 70)
			return errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 66)
		ret.SetUint64(val)
		return nil
	default:
		trace_util_0.Count(_tablecodec_00000, 67)
		*ret = data
		return nil
	}
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	trace_util_0.Count(_tablecodec_00000, 71)
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 74)
		return types.Datum{}, errors.Trace(err)
	}
	trace_util_0.Count(_tablecodec_00000, 72)
	colDatum, err := unflatten(d, ft, loc)
	if err != nil {
		trace_util_0.Count(_tablecodec_00000, 75)
		return types.Datum{}, errors.Trace(err)
	}
	trace_util_0.Count(_tablecodec_00000, 73)
	return colDatum, nil
}

// DecodeRowWithMap decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	trace_util_0.Count(_tablecodec_00000, 76)
	if row == nil {
		trace_util_0.Count(_tablecodec_00000, 81)
		row = make(map[int64]types.Datum, len(cols))
	}
	trace_util_0.Count(_tablecodec_00000, 77)
	if b == nil {
		trace_util_0.Count(_tablecodec_00000, 82)
		return row, nil
	}
	trace_util_0.Count(_tablecodec_00000, 78)
	if len(b) == 1 && b[0] == codec.NilFlag {
		trace_util_0.Count(_tablecodec_00000, 83)
		return row, nil
	}
	trace_util_0.Count(_tablecodec_00000, 79)
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		trace_util_0.Count(_tablecodec_00000, 84)
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 88)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 85)
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 89)
			return nil, errors.Trace(err)
		}
		// Get col value.
		trace_util_0.Count(_tablecodec_00000, 86)
		data, b, err = codec.CutOne(b)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 90)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 87)
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			trace_util_0.Count(_tablecodec_00000, 91)
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				trace_util_0.Count(_tablecodec_00000, 94)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_tablecodec_00000, 92)
			v, err = unflatten(v, ft, loc)
			if err != nil {
				trace_util_0.Count(_tablecodec_00000, 95)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_tablecodec_00000, 93)
			row[id] = v
			cnt++
			if cnt == len(cols) {
				trace_util_0.Count(_tablecodec_00000, 96)
				// Get enough data.
				break
			}
		}
	}
	trace_util_0.Count(_tablecodec_00000, 80)
	return row, nil
}

// DecodeRow decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRow(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, error) {
	trace_util_0.Count(_tablecodec_00000, 97)
	return DecodeRowWithMap(b, cols, loc, nil)
}

// CutRowNew cuts encoded row into byte slices and return columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	trace_util_0.Count(_tablecodec_00000, 98)
	if data == nil {
		trace_util_0.Count(_tablecodec_00000, 102)
		return nil, nil
	}
	trace_util_0.Count(_tablecodec_00000, 99)
	if len(data) == 1 && data[0] == codec.NilFlag {
		trace_util_0.Count(_tablecodec_00000, 103)
		return nil, nil
	}

	trace_util_0.Count(_tablecodec_00000, 100)
	var (
		cnt int
		b   []byte
		err error
		cid int64
	)
	row := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		trace_util_0.Count(_tablecodec_00000, 104)
		// Get col id.
		data, cid, err = codec.CutColumnID(data)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 107)
			return nil, errors.Trace(err)
		}

		// Get col value.
		trace_util_0.Count(_tablecodec_00000, 105)
		b, data, err = codec.CutOne(data)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 108)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_tablecodec_00000, 106)
		offset, ok := colIDs[cid]
		if ok {
			trace_util_0.Count(_tablecodec_00000, 109)
			row[offset] = b
			cnt++
		}
	}
	trace_util_0.Count(_tablecodec_00000, 101)
	return row, nil
}

// UnflattenDatums converts raw datums to column datums.
func UnflattenDatums(datums []types.Datum, fts []*types.FieldType, loc *time.Location) ([]types.Datum, error) {
	trace_util_0.Count(_tablecodec_00000, 110)
	for i, datum := range datums {
		trace_util_0.Count(_tablecodec_00000, 112)
		ft := fts[i]
		uDatum, err := unflatten(datum, ft, loc)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 114)
			return datums, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 113)
		datums[i] = uDatum
	}
	trace_util_0.Count(_tablecodec_00000, 111)
	return datums, nil
}

// unflatten converts a raw datum to a column datum.
func unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	trace_util_0.Count(_tablecodec_00000, 115)
	if datum.IsNull() {
		trace_util_0.Count(_tablecodec_00000, 118)
		return datum, nil
	}
	trace_util_0.Count(_tablecodec_00000, 116)
	switch ft.Tp {
	case mysql.TypeFloat:
		trace_util_0.Count(_tablecodec_00000, 119)
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar,
		mysql.TypeString:
		trace_util_0.Count(_tablecodec_00000, 120)
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_tablecodec_00000, 121)
		var t types.Time
		t.Type = ft.Tp
		t.Fsp = ft.Decimal
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 130)
			return datum, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 122)
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			trace_util_0.Count(_tablecodec_00000, 131)
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				trace_util_0.Count(_tablecodec_00000, 132)
				return datum, errors.Trace(err)
			}
		}
		trace_util_0.Count(_tablecodec_00000, 123)
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration:
		trace_util_0.Count(_tablecodec_00000, 124) //duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: ft.Decimal}
		datum.SetValue(dur)
		return datum, nil
	case mysql.TypeEnum:
		trace_util_0.Count(_tablecodec_00000, 125)
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, datum.GetUint64())
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 133)
			enum = types.Enum{}
		}
		trace_util_0.Count(_tablecodec_00000, 126)
		datum.SetValue(enum)
		return datum, nil
	case mysql.TypeSet:
		trace_util_0.Count(_tablecodec_00000, 127)
		set, err := types.ParseSetValue(ft.Elems, datum.GetUint64())
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 134)
			return datum, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 128)
		datum.SetValue(set)
		return datum, nil
	case mysql.TypeBit:
		trace_util_0.Count(_tablecodec_00000, 129)
		val := datum.GetUint64()
		byteSize := (ft.Flen + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	trace_util_0.Count(_tablecodec_00000, 117)
	return datum, nil
}

// EncodeIndexSeekKey encodes an index value to kv.Key.
func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 135)
	key := make([]byte, 0, prefixLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	trace_util_0.Count(_tablecodec_00000, 136)
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte)
	for _, id := range colIDs {
		trace_util_0.Count(_tablecodec_00000, 138)
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 140)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 139)
		values[id] = val
	}
	trace_util_0.Count(_tablecodec_00000, 137)
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key kv.Key) []byte {
	trace_util_0.Count(_tablecodec_00000, 141)
	return key[prefixLen+idLen:]
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	trace_util_0.Count(_tablecodec_00000, 142)
	b = key[prefixLen+idLen:]
	values = make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		trace_util_0.Count(_tablecodec_00000, 144)
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			trace_util_0.Count(_tablecodec_00000, 146)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_tablecodec_00000, 145)
		values = append(values, val)
	}
	trace_util_0.Count(_tablecodec_00000, 143)
	return
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 147)
	key := make([]byte, 0, prefixLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes table prefix with table ID.
func EncodeTablePrefix(tableID int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 148)
	var key kv.Key
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	trace_util_0.Count(_tablecodec_00000, 149)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	trace_util_0.Count(_tablecodec_00000, 150)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// ReplaceRecordKeyTableID replace the tableID in the recordKey buf.
func ReplaceRecordKeyTableID(buf []byte, tableID int64) []byte {
	trace_util_0.Count(_tablecodec_00000, 151)
	if len(buf) < len(tablePrefix)+8 {
		trace_util_0.Count(_tablecodec_00000, 153)
		return buf
	}

	trace_util_0.Count(_tablecodec_00000, 152)
	u := codec.EncodeIntToCmpUint(tableID)
	binary.BigEndian.PutUint64(buf[len(tablePrefix):], u)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 154)
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 155)
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// GenTablePrefix composes table record and index prefix: "t[tableID]".
func GenTablePrefix(tableID int64) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 156)
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	return buf
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	trace_util_0.Count(_tablecodec_00000, 157)
	if len(key) > recordRowKeyLen {
		trace_util_0.Count(_tablecodec_00000, 159)
		return key[:recordRowKeyLen]
	}
	trace_util_0.Count(_tablecodec_00000, 158)
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	trace_util_0.Count(_tablecodec_00000, 160)
	startKey = EncodeRowKeyWithHandle(tableID, math.MinInt64)
	endKey = EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	trace_util_0.Count(_tablecodec_00000, 161)
	startKey = EncodeIndexSeekKey(tableID, indexID, nil)
	endKey = EncodeIndexSeekKey(tableID, indexID, []byte{255})
	return
}

const (
	codeInvalidRecordKey   = 4
	codeInvalidColumnCount = 5
	codeInvalidKey         = 6
	codeInvalidIndexKey    = 7
)

var _tablecodec_00000 = "tablecodec/tablecodec.go"
