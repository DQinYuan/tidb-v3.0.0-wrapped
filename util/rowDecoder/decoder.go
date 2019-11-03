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

package decoder

import (
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Column contains the info and generated expr of column.
type Column struct {
	Col     *table.Column
	GenExpr expression.Expression
}

// RowDecoder decodes a byte slice into datums and eval the generated column value.
type RowDecoder struct {
	tbl           table.Table
	mutRow        chunk.MutRow
	columns       map[int64]Column
	colTypes      map[int64]*types.FieldType
	haveGenColumn bool
	defaultVals   []types.Datum
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, decodeColMap map[int64]Column) *RowDecoder {
	trace_util_0.Count(_decoder_00000, 0)
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	haveGenCol := false
	for id, col := range decodeColMap {
		trace_util_0.Count(_decoder_00000, 4)
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
		if col.GenExpr != nil {
			trace_util_0.Count(_decoder_00000, 5)
			haveGenCol = true
		}
	}
	trace_util_0.Count(_decoder_00000, 1)
	if !haveGenCol {
		trace_util_0.Count(_decoder_00000, 6)
		return &RowDecoder{
			colTypes: colFieldMap,
		}
	}

	trace_util_0.Count(_decoder_00000, 2)
	cols := tbl.Cols()
	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_decoder_00000, 7)
		tps[col.Offset] = &col.FieldType
	}
	trace_util_0.Count(_decoder_00000, 3)
	return &RowDecoder{
		tbl:           tbl,
		mutRow:        chunk.MutRowFromTypes(tps),
		columns:       decodeColMap,
		colTypes:      colFieldMap,
		haveGenColumn: haveGenCol,
		defaultVals:   make([]types.Datum, len(cols)),
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, handle int64, b []byte, decodeLoc, sysLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	trace_util_0.Count(_decoder_00000, 8)
	row, err := tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	if err != nil {
		trace_util_0.Count(_decoder_00000, 13)
		return nil, err
	}
	trace_util_0.Count(_decoder_00000, 9)
	if !rd.haveGenColumn {
		trace_util_0.Count(_decoder_00000, 14)
		return row, nil
	}

	trace_util_0.Count(_decoder_00000, 10)
	for _, dCol := range rd.columns {
		trace_util_0.Count(_decoder_00000, 15)
		colInfo := dCol.Col.ColumnInfo
		val, ok := row[colInfo.ID]
		if ok || dCol.GenExpr != nil {
			trace_util_0.Count(_decoder_00000, 18)
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
			continue
		}

		// Get the default value of the column in the generated column expression.
		trace_util_0.Count(_decoder_00000, 16)
		if dCol.Col.IsPKHandleColumn(rd.tbl.Meta()) {
			trace_util_0.Count(_decoder_00000, 19)
			if mysql.HasUnsignedFlag(colInfo.Flag) {
				trace_util_0.Count(_decoder_00000, 20)
				val.SetUint64(uint64(handle))
			} else {
				trace_util_0.Count(_decoder_00000, 21)
				{
					val.SetInt64(handle)
				}
			}
		} else {
			trace_util_0.Count(_decoder_00000, 22)
			{
				val, err = tables.GetColDefaultValue(ctx, dCol.Col, rd.defaultVals)
				if err != nil {
					trace_util_0.Count(_decoder_00000, 23)
					return nil, err
				}
			}
		}
		trace_util_0.Count(_decoder_00000, 17)
		rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
	}
	trace_util_0.Count(_decoder_00000, 11)
	for id, col := range rd.columns {
		trace_util_0.Count(_decoder_00000, 24)
		if col.GenExpr == nil {
			trace_util_0.Count(_decoder_00000, 29)
			continue
		}
		// Eval the column value
		trace_util_0.Count(_decoder_00000, 25)
		val, err := col.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			trace_util_0.Count(_decoder_00000, 30)
			return nil, err
		}
		trace_util_0.Count(_decoder_00000, 26)
		val, err = table.CastValue(ctx, val, col.Col.ColumnInfo)
		if err != nil {
			trace_util_0.Count(_decoder_00000, 31)
			return nil, err
		}

		trace_util_0.Count(_decoder_00000, 27)
		if val.Kind() == types.KindMysqlTime && sysLoc != time.UTC {
			trace_util_0.Count(_decoder_00000, 32)
			t := val.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp {
				trace_util_0.Count(_decoder_00000, 33)
				err := t.ConvertTimeZone(sysLoc, time.UTC)
				if err != nil {
					trace_util_0.Count(_decoder_00000, 35)
					return nil, err
				}
				trace_util_0.Count(_decoder_00000, 34)
				val.SetMysqlTime(t)
			}
		}
		trace_util_0.Count(_decoder_00000, 28)
		row[id] = val
	}
	trace_util_0.Count(_decoder_00000, 12)
	return row, nil
}

var _decoder_00000 = "util/rowDecoder/decoder.go"
