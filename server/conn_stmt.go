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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
)

func (cc *clientConn) handleStmtPrepare(sql string) error {
	trace_util_0.Count(_conn_stmt_00000, 0)
	stmt, columns, params, err := cc.ctx.Prepare(sql)
	if err != nil {
		trace_util_0.Count(_conn_stmt_00000, 5)
		return err
	}
	trace_util_0.Count(_conn_stmt_00000, 1)
	data := make([]byte, 4, 128)

	//status ok
	data = append(data, 0)
	//stmt id
	data = dumpUint32(data, uint32(stmt.ID()))
	//number columns
	data = dumpUint16(data, uint16(len(columns)))
	//number params
	data = dumpUint16(data, uint16(len(params)))
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0) //TODO support warning count

	if err := cc.writePacket(data); err != nil {
		trace_util_0.Count(_conn_stmt_00000, 6)
		return err
	}

	trace_util_0.Count(_conn_stmt_00000, 2)
	if len(params) > 0 {
		trace_util_0.Count(_conn_stmt_00000, 7)
		for i := 0; i < len(params); i++ {
			trace_util_0.Count(_conn_stmt_00000, 9)
			data = data[0:4]
			data = params[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				trace_util_0.Count(_conn_stmt_00000, 10)
				return err
			}
		}

		trace_util_0.Count(_conn_stmt_00000, 8)
		if err := cc.writeEOF(0); err != nil {
			trace_util_0.Count(_conn_stmt_00000, 11)
			return err
		}
	}

	trace_util_0.Count(_conn_stmt_00000, 3)
	if len(columns) > 0 {
		trace_util_0.Count(_conn_stmt_00000, 12)
		for i := 0; i < len(columns); i++ {
			trace_util_0.Count(_conn_stmt_00000, 14)
			data = data[0:4]
			data = columns[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				trace_util_0.Count(_conn_stmt_00000, 15)
				return err
			}
		}

		trace_util_0.Count(_conn_stmt_00000, 13)
		if err := cc.writeEOF(0); err != nil {
			trace_util_0.Count(_conn_stmt_00000, 16)
			return err
		}

	}
	trace_util_0.Count(_conn_stmt_00000, 4)
	return cc.flush()
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 17)
	if len(data) < 9 {
		trace_util_0.Count(_conn_stmt_00000, 25)
		return mysql.ErrMalformPacket
	}
	trace_util_0.Count(_conn_stmt_00000, 18)
	pos := 0
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		trace_util_0.Count(_conn_stmt_00000, 26)
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_execute")
	}

	trace_util_0.Count(_conn_stmt_00000, 19)
	flag := data[pos]
	pos++
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
	// The client indicates that it wants to use cursor by setting this flag.
	// 0x00 CURSOR_TYPE_NO_CURSOR
	// 0x01 CURSOR_TYPE_READ_ONLY
	// 0x02 CURSOR_TYPE_FOR_UPDATE
	// 0x04 CURSOR_TYPE_SCROLLABLE
	// Now we only support forward-only, read-only cursor.
	var useCursor bool
	switch flag {
	case 0:
		trace_util_0.Count(_conn_stmt_00000, 27)
		useCursor = false
	case 1:
		trace_util_0.Count(_conn_stmt_00000, 28)
		useCursor = true
	default:
		trace_util_0.Count(_conn_stmt_00000, 29)
		return mysql.NewErrf(mysql.ErrUnknown, "unsupported flag %d", flag)
	}

	// skip iteration-count, always 1
	trace_util_0.Count(_conn_stmt_00000, 20)
	pos += 4

	var (
		nullBitmaps []byte
		paramTypes  []byte
		paramValues []byte
	)
	numParams := stmt.NumParams()
	args := make([]interface{}, numParams)
	if numParams > 0 {
		trace_util_0.Count(_conn_stmt_00000, 30)
		nullBitmapLen := (numParams + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			trace_util_0.Count(_conn_stmt_00000, 33)
			return mysql.ErrMalformPacket
		}
		trace_util_0.Count(_conn_stmt_00000, 31)
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		// new param bound flag
		if data[pos] == 1 {
			trace_util_0.Count(_conn_stmt_00000, 34)
			pos++
			if len(data) < (pos + (numParams << 1)) {
				trace_util_0.Count(_conn_stmt_00000, 36)
				return mysql.ErrMalformPacket
			}

			trace_util_0.Count(_conn_stmt_00000, 35)
			paramTypes = data[pos : pos+(numParams<<1)]
			pos += numParams << 1
			paramValues = data[pos:]
			// Just the first StmtExecute packet contain parameters type,
			// we need save it for further use.
			stmt.SetParamsType(paramTypes)
		} else {
			trace_util_0.Count(_conn_stmt_00000, 37)
			{
				paramValues = data[pos+1:]
			}
		}

		trace_util_0.Count(_conn_stmt_00000, 32)
		err = parseStmtArgs(args, stmt.BoundParams(), nullBitmaps, stmt.GetParamsType(), paramValues)
		stmt.Reset()
		if err != nil {
			trace_util_0.Count(_conn_stmt_00000, 38)
			return errors.Annotatef(err, "%s", cc.preparedStmt2String(stmtID))
		}
	}
	trace_util_0.Count(_conn_stmt_00000, 21)
	rs, err := stmt.Execute(ctx, args...)
	if err != nil {
		trace_util_0.Count(_conn_stmt_00000, 39)
		return errors.Annotatef(err, "%s", cc.preparedStmt2String(stmtID))
	}
	trace_util_0.Count(_conn_stmt_00000, 22)
	if rs == nil {
		trace_util_0.Count(_conn_stmt_00000, 40)
		return cc.writeOK()
	}

	// if the client wants to use cursor
	// we should hold the ResultSet in PreparedStatement for next stmt_fetch, and only send back ColumnInfo.
	// Tell the client cursor exists in server by setting proper serverStatus.
	trace_util_0.Count(_conn_stmt_00000, 23)
	if useCursor {
		trace_util_0.Count(_conn_stmt_00000, 41)
		stmt.StoreResultSet(rs)
		err = cc.writeColumnInfo(rs.Columns(), mysql.ServerStatusCursorExists)
		if err != nil {
			trace_util_0.Count(_conn_stmt_00000, 43)
			return err
		}
		// explicitly flush columnInfo to client.
		trace_util_0.Count(_conn_stmt_00000, 42)
		return cc.flush()
	}
	trace_util_0.Count(_conn_stmt_00000, 24)
	return cc.writeResultset(ctx, rs, true, 0, 0)
}

// maxFetchSize constants
const (
	maxFetchSize = 1024
)

func (cc *clientConn) handleStmtFetch(ctx context.Context, data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 44)

	stmtID, fetchSize, err := parseStmtFetchCmd(data)
	if err != nil {
		trace_util_0.Count(_conn_stmt_00000, 49)
		return err
	}

	trace_util_0.Count(_conn_stmt_00000, 45)
	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		trace_util_0.Count(_conn_stmt_00000, 50)
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch")
	}
	trace_util_0.Count(_conn_stmt_00000, 46)
	sql := ""
	if prepared, ok := cc.ctx.GetStatement(int(stmtID)).(*TiDBStatement); ok {
		trace_util_0.Count(_conn_stmt_00000, 51)
		sql = prepared.sql
	}
	trace_util_0.Count(_conn_stmt_00000, 47)
	cc.ctx.SetProcessInfo(sql, time.Now(), mysql.ComStmtExecute)
	rs := stmt.GetResultSet()
	if rs == nil {
		trace_util_0.Count(_conn_stmt_00000, 52)
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch_rs")
	}

	trace_util_0.Count(_conn_stmt_00000, 48)
	return cc.writeResultset(ctx, rs, true, mysql.ServerStatusCursorExists, int(fetchSize))
}

func parseStmtFetchCmd(data []byte) (uint32, uint32, error) {
	trace_util_0.Count(_conn_stmt_00000, 53)
	if len(data) != 8 {
		trace_util_0.Count(_conn_stmt_00000, 56)
		return 0, 0, mysql.ErrMalformPacket
	}
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
	trace_util_0.Count(_conn_stmt_00000, 54)
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	fetchSize := binary.LittleEndian.Uint32(data[4:8])
	if fetchSize > maxFetchSize {
		trace_util_0.Count(_conn_stmt_00000, 57)
		fetchSize = maxFetchSize
	}
	trace_util_0.Count(_conn_stmt_00000, 55)
	return stmtID, fetchSize, nil
}

func parseStmtArgs(args []interface{}, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 58)
	pos := 0
	var v []byte
	var n int
	var isNull bool

	for i := 0; i < len(args); i++ {
		trace_util_0.Count(_conn_stmt_00000, 60)
		// if params had received via ComStmtSendLongData, use them directly.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
		// see clientConn#handleStmtSendLongData
		if boundParams[i] != nil {
			trace_util_0.Count(_conn_stmt_00000, 64)
			args[i] = boundParams[i]
			continue
		}

		// check nullBitMap to determine the NULL arguments.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
		// notice: some client(e.g. mariadb) will set nullBitMap even if data had be sent via ComStmtSendLongData,
		// so this check need place after boundParam's check.
		trace_util_0.Count(_conn_stmt_00000, 61)
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			trace_util_0.Count(_conn_stmt_00000, 65)
			args[i] = nil
			continue
		}

		trace_util_0.Count(_conn_stmt_00000, 62)
		if (i<<1)+1 >= len(paramTypes) {
			trace_util_0.Count(_conn_stmt_00000, 66)
			return mysql.ErrMalformPacket
		}

		trace_util_0.Count(_conn_stmt_00000, 63)
		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.TypeNull:
			trace_util_0.Count(_conn_stmt_00000, 67)
			args[i] = nil
			continue

		case mysql.TypeTiny:
			trace_util_0.Count(_conn_stmt_00000, 68)
			if len(paramValues) < (pos + 1) {
				trace_util_0.Count(_conn_stmt_00000, 95)
				err = mysql.ErrMalformPacket
				return
			}

			trace_util_0.Count(_conn_stmt_00000, 69)
			if isUnsigned {
				trace_util_0.Count(_conn_stmt_00000, 96)
				args[i] = uint8(paramValues[pos])
			} else {
				trace_util_0.Count(_conn_stmt_00000, 97)
				{
					args[i] = int8(paramValues[pos])
				}
			}

			trace_util_0.Count(_conn_stmt_00000, 70)
			pos++
			continue

		case mysql.TypeShort, mysql.TypeYear:
			trace_util_0.Count(_conn_stmt_00000, 71)
			if len(paramValues) < (pos + 2) {
				trace_util_0.Count(_conn_stmt_00000, 98)
				err = mysql.ErrMalformPacket
				return
			}
			trace_util_0.Count(_conn_stmt_00000, 72)
			valU16 := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
			if isUnsigned {
				trace_util_0.Count(_conn_stmt_00000, 99)
				args[i] = valU16
			} else {
				trace_util_0.Count(_conn_stmt_00000, 100)
				{
					args[i] = int16(valU16)
				}
			}
			trace_util_0.Count(_conn_stmt_00000, 73)
			pos += 2
			continue

		case mysql.TypeInt24, mysql.TypeLong:
			trace_util_0.Count(_conn_stmt_00000, 74)
			if len(paramValues) < (pos + 4) {
				trace_util_0.Count(_conn_stmt_00000, 101)
				err = mysql.ErrMalformPacket
				return
			}
			trace_util_0.Count(_conn_stmt_00000, 75)
			valU32 := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
			if isUnsigned {
				trace_util_0.Count(_conn_stmt_00000, 102)
				args[i] = valU32
			} else {
				trace_util_0.Count(_conn_stmt_00000, 103)
				{
					args[i] = int32(valU32)
				}
			}
			trace_util_0.Count(_conn_stmt_00000, 76)
			pos += 4
			continue

		case mysql.TypeLonglong:
			trace_util_0.Count(_conn_stmt_00000, 77)
			if len(paramValues) < (pos + 8) {
				trace_util_0.Count(_conn_stmt_00000, 104)
				err = mysql.ErrMalformPacket
				return
			}
			trace_util_0.Count(_conn_stmt_00000, 78)
			valU64 := binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			if isUnsigned {
				trace_util_0.Count(_conn_stmt_00000, 105)
				args[i] = valU64
			} else {
				trace_util_0.Count(_conn_stmt_00000, 106)
				{
					args[i] = int64(valU64)
				}
			}
			trace_util_0.Count(_conn_stmt_00000, 79)
			pos += 8
			continue

		case mysql.TypeFloat:
			trace_util_0.Count(_conn_stmt_00000, 80)
			if len(paramValues) < (pos + 4) {
				trace_util_0.Count(_conn_stmt_00000, 107)
				err = mysql.ErrMalformPacket
				return
			}

			trace_util_0.Count(_conn_stmt_00000, 81)
			args[i] = math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			pos += 4
			continue

		case mysql.TypeDouble:
			trace_util_0.Count(_conn_stmt_00000, 82)
			if len(paramValues) < (pos + 8) {
				trace_util_0.Count(_conn_stmt_00000, 108)
				err = mysql.ErrMalformPacket
				return
			}

			trace_util_0.Count(_conn_stmt_00000, 83)
			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime:
			trace_util_0.Count(_conn_stmt_00000, 84)
			if len(paramValues) < (pos + 1) {
				trace_util_0.Count(_conn_stmt_00000, 109)
				err = mysql.ErrMalformPacket
				return
			}
			// See https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
			// for more details.
			trace_util_0.Count(_conn_stmt_00000, 85)
			length := uint8(paramValues[pos])
			pos++
			switch length {
			case 0:
				trace_util_0.Count(_conn_stmt_00000, 110)
				args[i] = types.ZeroDatetimeStr
			case 4:
				trace_util_0.Count(_conn_stmt_00000, 111)
				pos, args[i] = parseBinaryDate(pos, paramValues)
			case 7:
				trace_util_0.Count(_conn_stmt_00000, 112)
				pos, args[i] = parseBinaryDateTime(pos, paramValues)
			case 11:
				trace_util_0.Count(_conn_stmt_00000, 113)
				pos, args[i] = parseBinaryTimestamp(pos, paramValues)
			default:
				trace_util_0.Count(_conn_stmt_00000, 114)
				err = mysql.ErrMalformPacket
				return
			}
			trace_util_0.Count(_conn_stmt_00000, 86)
			continue

		case mysql.TypeDuration:
			trace_util_0.Count(_conn_stmt_00000, 87)
			if len(paramValues) < (pos + 1) {
				trace_util_0.Count(_conn_stmt_00000, 115)
				err = mysql.ErrMalformPacket
				return
			}
			// See https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
			// for more details.
			trace_util_0.Count(_conn_stmt_00000, 88)
			length := uint8(paramValues[pos])
			pos++
			switch length {
			case 0:
				trace_util_0.Count(_conn_stmt_00000, 116)
				args[i] = "0"
			case 8:
				trace_util_0.Count(_conn_stmt_00000, 117)
				isNegative := uint8(paramValues[pos])
				if isNegative > 1 {
					trace_util_0.Count(_conn_stmt_00000, 122)
					err = mysql.ErrMalformPacket
					return
				}
				trace_util_0.Count(_conn_stmt_00000, 118)
				pos++
				pos, args[i] = parseBinaryDuration(pos, paramValues, isNegative)
			case 12:
				trace_util_0.Count(_conn_stmt_00000, 119)
				isNegative := uint8(paramValues[pos])
				if isNegative > 1 {
					trace_util_0.Count(_conn_stmt_00000, 123)
					err = mysql.ErrMalformPacket
					return
				}
				trace_util_0.Count(_conn_stmt_00000, 120)
				pos++
				pos, args[i] = parseBinaryDurationWithMS(pos, paramValues, isNegative)
			default:
				trace_util_0.Count(_conn_stmt_00000, 121)
				err = mysql.ErrMalformPacket
				return
			}
			trace_util_0.Count(_conn_stmt_00000, 89)
			continue

		case mysql.TypeUnspecified, mysql.TypeNewDecimal, mysql.TypeVarchar,
			mysql.TypeBit, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			trace_util_0.Count(_conn_stmt_00000, 90)
			if len(paramValues) < (pos + 1) {
				trace_util_0.Count(_conn_stmt_00000, 124)
				err = mysql.ErrMalformPacket
				return
			}

			trace_util_0.Count(_conn_stmt_00000, 91)
			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				trace_util_0.Count(_conn_stmt_00000, 125)
				return
			}

			trace_util_0.Count(_conn_stmt_00000, 92)
			if !isNull {
				trace_util_0.Count(_conn_stmt_00000, 126)
				args[i] = string(hack.String(v))
			} else {
				trace_util_0.Count(_conn_stmt_00000, 127)
				{
					args[i] = nil
				}
			}
			trace_util_0.Count(_conn_stmt_00000, 93)
			continue
		default:
			trace_util_0.Count(_conn_stmt_00000, 94)
			err = errUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}
	}
	trace_util_0.Count(_conn_stmt_00000, 59)
	return
}

func parseBinaryDate(pos int, paramValues []byte) (int, string) {
	trace_util_0.Count(_conn_stmt_00000, 128)
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := uint8(paramValues[pos])
	pos++
	day := uint8(paramValues[pos])
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func parseBinaryDateTime(pos int, paramValues []byte) (int, string) {
	trace_util_0.Count(_conn_stmt_00000, 129)
	pos, date := parseBinaryDate(pos, paramValues)
	hour := uint8(paramValues[pos])
	pos++
	minute := uint8(paramValues[pos])
	pos++
	second := uint8(paramValues[pos])
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func parseBinaryTimestamp(pos int, paramValues []byte) (int, string) {
	trace_util_0.Count(_conn_stmt_00000, 130)
	pos, dateTime := parseBinaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func parseBinaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
	trace_util_0.Count(_conn_stmt_00000, 131)
	sign := ""
	if isNegative == 1 {
		trace_util_0.Count(_conn_stmt_00000, 133)
		sign = "-"
	}
	trace_util_0.Count(_conn_stmt_00000, 132)
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := uint8(paramValues[pos])
	pos++
	minutes := uint8(paramValues[pos])
	pos++
	seconds := uint8(paramValues[pos])
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func parseBinaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	trace_util_0.Count(_conn_stmt_00000, 134)
	pos, dur := parseBinaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

func (cc *clientConn) handleStmtClose(data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 135)
	if len(data) < 4 {
		trace_util_0.Count(_conn_stmt_00000, 138)
		return
	}

	trace_util_0.Count(_conn_stmt_00000, 136)
	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt != nil {
		trace_util_0.Count(_conn_stmt_00000, 139)
		return stmt.Close()
	}
	trace_util_0.Count(_conn_stmt_00000, 137)
	return
}

func (cc *clientConn) handleStmtSendLongData(data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 140)
	if len(data) < 6 {
		trace_util_0.Count(_conn_stmt_00000, 143)
		return mysql.ErrMalformPacket
	}

	trace_util_0.Count(_conn_stmt_00000, 141)
	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))

	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		trace_util_0.Count(_conn_stmt_00000, 144)
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_send_longdata")
	}

	trace_util_0.Count(_conn_stmt_00000, 142)
	paramID := int(binary.LittleEndian.Uint16(data[4:6]))
	return stmt.AppendParam(paramID, data[6:])
}

func (cc *clientConn) handleStmtReset(data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 145)
	if len(data) < 4 {
		trace_util_0.Count(_conn_stmt_00000, 148)
		return mysql.ErrMalformPacket
	}

	trace_util_0.Count(_conn_stmt_00000, 146)
	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		trace_util_0.Count(_conn_stmt_00000, 149)
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_reset")
	}
	trace_util_0.Count(_conn_stmt_00000, 147)
	stmt.Reset()
	return cc.writeOK()
}

// handleSetOption refer to https://dev.mysql.com/doc/internals/en/com-set-option.html
func (cc *clientConn) handleSetOption(data []byte) (err error) {
	trace_util_0.Count(_conn_stmt_00000, 150)
	if len(data) < 2 {
		trace_util_0.Count(_conn_stmt_00000, 154)
		return mysql.ErrMalformPacket
	}

	trace_util_0.Count(_conn_stmt_00000, 151)
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		trace_util_0.Count(_conn_stmt_00000, 155)
		cc.capability |= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	case 1:
		trace_util_0.Count(_conn_stmt_00000, 156)
		cc.capability &^= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	default:
		trace_util_0.Count(_conn_stmt_00000, 157)
		return mysql.ErrMalformPacket
	}
	trace_util_0.Count(_conn_stmt_00000, 152)
	if err = cc.writeEOF(0); err != nil {
		trace_util_0.Count(_conn_stmt_00000, 158)
		return err
	}

	trace_util_0.Count(_conn_stmt_00000, 153)
	return cc.flush()
}

func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	trace_util_0.Count(_conn_stmt_00000, 159)
	sv := cc.ctx.GetSessionVars()
	if prepared, ok := sv.PreparedStmts[stmtID]; ok {
		trace_util_0.Count(_conn_stmt_00000, 161)
		return prepared.Stmt.Text() + sv.GetExecuteArgumentsInfo()
	}
	trace_util_0.Count(_conn_stmt_00000, 160)
	return fmt.Sprintf("prepared statement not found, ID: %d", stmtID)
}

var _conn_stmt_00000 = "server/conn_stmt.go"
