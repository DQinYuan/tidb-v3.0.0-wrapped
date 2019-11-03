// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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

package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
)

func parseNullTermString(b []byte) (str []byte, remain []byte) {
	trace_util_0.Count(_util_00000, 0)
	off := bytes.IndexByte(b, 0)
	if off == -1 {
		trace_util_0.Count(_util_00000, 2)
		return nil, b
	}
	trace_util_0.Count(_util_00000, 1)
	return b[:off], b[off+1:]
}

func parseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	trace_util_0.Count(_util_00000, 3)
	switch b[0] {
	// 251: NULL
	case 0xfb:
		trace_util_0.Count(_util_00000, 5)
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		trace_util_0.Count(_util_00000, 6)
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		trace_util_0.Count(_util_00000, 7)
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		trace_util_0.Count(_util_00000, 8)
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// 0-250: value of first byte
	trace_util_0.Count(_util_00000, 4)
	num = uint64(b[0])
	n = 1
	return
}

func dumpLengthEncodedInt(buffer []byte, n uint64) []byte {
	trace_util_0.Count(_util_00000, 9)
	switch {
	case n <= 250:
		trace_util_0.Count(_util_00000, 11)
		return append(buffer, tinyIntCache[n]...)

	case n <= 0xffff:
		trace_util_0.Count(_util_00000, 12)
		return append(buffer, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		trace_util_0.Count(_util_00000, 13)
		return append(buffer, 0xfd, byte(n), byte(n>>8), byte(n>>16))

	case n <= 0xffffffffffffffff:
		trace_util_0.Count(_util_00000, 14)
		return append(buffer, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}

	trace_util_0.Count(_util_00000, 10)
	return buffer
}

func parseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	trace_util_0.Count(_util_00000, 15)
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		trace_util_0.Count(_util_00000, 18)
		return nil, isNull, n, nil
	}

	trace_util_0.Count(_util_00000, 16)
	n += int(num)

	// Check data length
	if len(b) >= n {
		trace_util_0.Count(_util_00000, 19)
		return b[n-int(num) : n], false, n, nil
	}

	trace_util_0.Count(_util_00000, 17)
	return nil, false, n, io.EOF
}

func dumpLengthEncodedString(buffer []byte, bytes []byte) []byte {
	trace_util_0.Count(_util_00000, 20)
	buffer = dumpLengthEncodedInt(buffer, uint64(len(bytes)))
	buffer = append(buffer, bytes...)
	return buffer
}

func dumpUint16(buffer []byte, n uint16) []byte {
	trace_util_0.Count(_util_00000, 21)
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	return buffer
}

func dumpUint32(buffer []byte, n uint32) []byte {
	trace_util_0.Count(_util_00000, 22)
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	return buffer
}

func dumpUint64(buffer []byte, n uint64) []byte {
	trace_util_0.Count(_util_00000, 23)
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	buffer = append(buffer, byte(n>>32))
	buffer = append(buffer, byte(n>>40))
	buffer = append(buffer, byte(n>>48))
	buffer = append(buffer, byte(n>>56))
	return buffer
}

var tinyIntCache [251][]byte

func init() {
	trace_util_0.Count(_util_00000, 24)
	for i := 0; i < len(tinyIntCache); i++ {
		trace_util_0.Count(_util_00000, 25)
		tinyIntCache[i] = []byte{byte(i)}
	}
}

func dumpBinaryTime(dur time.Duration) (data []byte) {
	trace_util_0.Count(_util_00000, 26)
	if dur == 0 {
		trace_util_0.Count(_util_00000, 30)
		data = tinyIntCache[0]
		return
	}
	trace_util_0.Count(_util_00000, 27)
	data = make([]byte, 13)
	data[0] = 12
	if dur < 0 {
		trace_util_0.Count(_util_00000, 31)
		data[1] = 1
		dur = -dur
	}
	trace_util_0.Count(_util_00000, 28)
	days := dur / (24 * time.Hour)
	dur -= days * 24 * time.Hour
	data[2] = byte(days)
	hours := dur / time.Hour
	dur -= hours * time.Hour
	data[6] = byte(hours)
	minutes := dur / time.Minute
	dur -= minutes * time.Minute
	data[7] = byte(minutes)
	seconds := dur / time.Second
	dur -= seconds * time.Second
	data[8] = byte(seconds)
	if dur == 0 {
		trace_util_0.Count(_util_00000, 32)
		data[0] = 8
		return data[:9]
	}
	trace_util_0.Count(_util_00000, 29)
	binary.LittleEndian.PutUint32(data[9:13], uint32(dur/time.Microsecond))
	return
}

func dumpBinaryDateTime(data []byte, t types.Time, loc *time.Location) ([]byte, error) {
	trace_util_0.Count(_util_00000, 33)
	if t.Type == mysql.TypeTimestamp && loc != nil {
		trace_util_0.Count(_util_00000, 36)
		// TODO: Consider time_zone variable.
		t1, err := t.Time.GoTime(time.Local)
		if err != nil {
			trace_util_0.Count(_util_00000, 38)
			return nil, errors.Errorf("FATAL: convert timestamp %v go time return error!", t.Time)
		}
		trace_util_0.Count(_util_00000, 37)
		t.Time = types.FromGoTime(t1.In(loc))
	}

	trace_util_0.Count(_util_00000, 34)
	year, mon, day := t.Time.Year(), t.Time.Month(), t.Time.Day()
	switch t.Type {
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		trace_util_0.Count(_util_00000, 39)
		data = append(data, 11)
		data = dumpUint16(data, uint16(year))
		data = append(data, byte(mon), byte(day), byte(t.Time.Hour()), byte(t.Time.Minute()), byte(t.Time.Second()))
		data = dumpUint32(data, uint32(t.Time.Microsecond()))
	case mysql.TypeDate:
		trace_util_0.Count(_util_00000, 40)
		data = append(data, 4)
		data = dumpUint16(data, uint16(year)) //year
		data = append(data, byte(mon), byte(day))
	}
	trace_util_0.Count(_util_00000, 35)
	return data, nil
}

func dumpBinaryRow(buffer []byte, columns []*ColumnInfo, row chunk.Row) ([]byte, error) {
	trace_util_0.Count(_util_00000, 41)
	buffer = append(buffer, mysql.OKHeader)
	nullBitmapOff := len(buffer)
	numBytes4Null := (len(columns) + 7 + 2) / 8
	for i := 0; i < numBytes4Null; i++ {
		trace_util_0.Count(_util_00000, 44)
		buffer = append(buffer, 0)
	}
	trace_util_0.Count(_util_00000, 42)
	for i := range columns {
		trace_util_0.Count(_util_00000, 45)
		if row.IsNull(i) {
			trace_util_0.Count(_util_00000, 47)
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			buffer[nullBitmapOff+bytePos] |= 1 << bitPos
			continue
		}
		trace_util_0.Count(_util_00000, 46)
		switch columns[i].Type {
		case mysql.TypeTiny:
			trace_util_0.Count(_util_00000, 48)
			buffer = append(buffer, byte(row.GetInt64(i)))
		case mysql.TypeShort, mysql.TypeYear:
			trace_util_0.Count(_util_00000, 49)
			buffer = dumpUint16(buffer, uint16(row.GetInt64(i)))
		case mysql.TypeInt24, mysql.TypeLong:
			trace_util_0.Count(_util_00000, 50)
			buffer = dumpUint32(buffer, uint32(row.GetInt64(i)))
		case mysql.TypeLonglong:
			trace_util_0.Count(_util_00000, 51)
			buffer = dumpUint64(buffer, row.GetUint64(i))
		case mysql.TypeFloat:
			trace_util_0.Count(_util_00000, 52)
			buffer = dumpUint32(buffer, math.Float32bits(row.GetFloat32(i)))
		case mysql.TypeDouble:
			trace_util_0.Count(_util_00000, 53)
			buffer = dumpUint64(buffer, math.Float64bits(row.GetFloat64(i)))
		case mysql.TypeNewDecimal:
			trace_util_0.Count(_util_00000, 54)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			trace_util_0.Count(_util_00000, 55)
			buffer = dumpLengthEncodedString(buffer, row.GetBytes(i))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_util_00000, 56)
			var err error
			buffer, err = dumpBinaryDateTime(buffer, row.GetTime(i), nil)
			if err != nil {
				trace_util_0.Count(_util_00000, 62)
				return buffer, err
			}
		case mysql.TypeDuration:
			trace_util_0.Count(_util_00000, 57)
			buffer = append(buffer, dumpBinaryTime(row.GetDuration(i, 0).Duration)...)
		case mysql.TypeEnum:
			trace_util_0.Count(_util_00000, 58)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetEnum(i).String()))
		case mysql.TypeSet:
			trace_util_0.Count(_util_00000, 59)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetSet(i).String()))
		case mysql.TypeJSON:
			trace_util_0.Count(_util_00000, 60)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetJSON(i).String()))
		default:
			trace_util_0.Count(_util_00000, 61)
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	trace_util_0.Count(_util_00000, 43)
	return buffer, nil
}

func dumpTextRow(buffer []byte, columns []*ColumnInfo, row chunk.Row) ([]byte, error) {
	trace_util_0.Count(_util_00000, 63)
	tmp := make([]byte, 0, 20)
	for i, col := range columns {
		trace_util_0.Count(_util_00000, 65)
		if row.IsNull(i) {
			trace_util_0.Count(_util_00000, 67)
			buffer = append(buffer, 0xfb)
			continue
		}
		trace_util_0.Count(_util_00000, 66)
		switch col.Type {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
			trace_util_0.Count(_util_00000, 68)
			tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeYear:
			trace_util_0.Count(_util_00000, 69)
			year := row.GetInt64(i)
			tmp = tmp[:0]
			if year == 0 {
				trace_util_0.Count(_util_00000, 85)
				tmp = append(tmp, '0', '0', '0', '0')
			} else {
				trace_util_0.Count(_util_00000, 86)
				{
					tmp = strconv.AppendInt(tmp, year, 10)
				}
			}
			trace_util_0.Count(_util_00000, 70)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeLonglong:
			trace_util_0.Count(_util_00000, 71)
			if mysql.HasUnsignedFlag(uint(columns[i].Flag)) {
				trace_util_0.Count(_util_00000, 87)
				tmp = strconv.AppendUint(tmp[:0], row.GetUint64(i), 10)
			} else {
				trace_util_0.Count(_util_00000, 88)
				{
					tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
				}
			}
			trace_util_0.Count(_util_00000, 72)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeFloat:
			trace_util_0.Count(_util_00000, 73)
			prec := -1
			if columns[i].Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec {
				trace_util_0.Count(_util_00000, 89)
				prec = int(col.Decimal)
			}
			trace_util_0.Count(_util_00000, 74)
			tmp = appendFormatFloat(tmp[:0], float64(row.GetFloat32(i)), prec, 32)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeDouble:
			trace_util_0.Count(_util_00000, 75)
			prec := types.UnspecifiedLength
			if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec {
				trace_util_0.Count(_util_00000, 90)
				prec = int(col.Decimal)
			}
			trace_util_0.Count(_util_00000, 76)
			tmp = appendFormatFloat(tmp[:0], row.GetFloat64(i), prec, 64)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeNewDecimal:
			trace_util_0.Count(_util_00000, 77)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			trace_util_0.Count(_util_00000, 78)
			buffer = dumpLengthEncodedString(buffer, row.GetBytes(i))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			trace_util_0.Count(_util_00000, 79)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetTime(i).String()))
		case mysql.TypeDuration:
			trace_util_0.Count(_util_00000, 80)
			dur := row.GetDuration(i, int(col.Decimal))
			buffer = dumpLengthEncodedString(buffer, hack.Slice(dur.String()))
		case mysql.TypeEnum:
			trace_util_0.Count(_util_00000, 81)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetEnum(i).String()))
		case mysql.TypeSet:
			trace_util_0.Count(_util_00000, 82)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetSet(i).String()))
		case mysql.TypeJSON:
			trace_util_0.Count(_util_00000, 83)
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetJSON(i).String()))
		default:
			trace_util_0.Count(_util_00000, 84)
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	trace_util_0.Count(_util_00000, 64)
	return buffer, nil
}

func lengthEncodedIntSize(n uint64) int {
	trace_util_0.Count(_util_00000, 91)
	switch {
	case n <= 250:
		trace_util_0.Count(_util_00000, 93)
		return 1

	case n <= 0xffff:
		trace_util_0.Count(_util_00000, 94)
		return 3

	case n <= 0xffffff:
		trace_util_0.Count(_util_00000, 95)
		return 4
	}

	trace_util_0.Count(_util_00000, 92)
	return 9
}

const (
	expFormatBig   = 1e15
	expFormatSmall = 1e-15
)

func appendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	trace_util_0.Count(_util_00000, 96)
	absVal := math.Abs(fVal)
	var out []byte
	if prec == types.UnspecifiedLength && (absVal >= expFormatBig || (absVal != 0 && absVal < expFormatSmall)) {
		trace_util_0.Count(_util_00000, 98)
		out = strconv.AppendFloat(in, fVal, 'e', prec, bitSize)
		valStr := out[len(in):]
		// remove the '+' from the string for compatibility.
		plusPos := bytes.IndexByte(valStr, '+')
		if plusPos > 0 {
			trace_util_0.Count(_util_00000, 99)
			plusPosInOut := len(in) + plusPos
			out = append(out[:plusPosInOut], out[plusPosInOut+1:]...)
		}
	} else {
		trace_util_0.Count(_util_00000, 100)
		{
			out = strconv.AppendFloat(in, fVal, 'f', prec, bitSize)
		}
	}
	trace_util_0.Count(_util_00000, 97)
	return out
}

// CorsHandler adds Cors Header if `cors` config is set.
type CorsHandler struct {
	handler http.Handler
	cfg     *config.Config
}

func (h CorsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_util_00000, 101)
	if h.cfg.Cors != "" {
		trace_util_0.Count(_util_00000, 103)
		w.Header().Set("Access-Control-Allow-Origin", h.cfg.Cors)
		w.Header().Set("Access-Control-Allow-Methods", "GET")
	}
	trace_util_0.Count(_util_00000, 102)
	h.handler.ServeHTTP(w, req)
}

var _util_00000 = "server/util.go"
