// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package tables

import (
	"context"
	"encoding/binary"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	binlog "github.com/pingcap/tipb/go-binlog"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

// tableCommon is shared by both Table and partition.
type tableCommon struct {
	tableID int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID int64
	Columns         []*table.Column
	publicColumns   []*table.Column
	writableColumns []*table.Column
	writableIndices []table.Index
	indices         []table.Index
	meta            *model.TableInfo
	alloc           autoid.Allocator

	// recordPrefix and indexPrefix are generated using physicalTableID.
	recordPrefix kv.Key
	indexPrefix  kv.Key
}

// Table implements table.Table interface.
type Table struct {
	tableCommon
}

var _ table.Table = &Table{}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tblInfo *model.TableInfo) table.Table {
	trace_util_0.Count(_tables_00000, 0)
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		trace_util_0.Count(_tables_00000, 4)
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	trace_util_0.Count(_tables_00000, 1)
	var t Table
	initTableCommon(&t.tableCommon, tblInfo, tblInfo.ID, columns, nil)
	if tblInfo.GetPartitionInfo() == nil {
		trace_util_0.Count(_tables_00000, 5)
		if err := initTableIndices(&t.tableCommon); err != nil {
			trace_util_0.Count(_tables_00000, 7)
			return nil
		}
		trace_util_0.Count(_tables_00000, 6)
		return &t
	}

	trace_util_0.Count(_tables_00000, 2)
	ret, err := newPartitionedTable(&t, tblInfo)
	if err != nil {
		trace_util_0.Count(_tables_00000, 8)
		return nil
	}
	trace_util_0.Count(_tables_00000, 3)
	return ret
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error) {
	trace_util_0.Count(_tables_00000, 9)
	if tblInfo.State == model.StateNone {
		trace_util_0.Count(_tables_00000, 13)
		return nil, table.ErrTableStateCantNone.GenWithStack("table %s can't be in none state", tblInfo.Name)
	}

	trace_util_0.Count(_tables_00000, 10)
	colsLen := len(tblInfo.Columns)
	columns := make([]*table.Column, 0, colsLen)
	for i, colInfo := range tblInfo.Columns {
		trace_util_0.Count(_tables_00000, 14)
		if colInfo.State == model.StateNone {
			trace_util_0.Count(_tables_00000, 18)
			return nil, table.ErrColumnStateCantNone.GenWithStack("column %s can't be in none state", colInfo.Name)
		}

		// Print some information when the column's offset isn't equal to i.
		trace_util_0.Count(_tables_00000, 15)
		if colInfo.Offset != i {
			trace_util_0.Count(_tables_00000, 19)
			logutil.Logger(context.Background()).Error("wrong table schema", zap.Any("table", tblInfo), zap.Any("column", colInfo), zap.Int("index", i), zap.Int("offset", colInfo.Offset), zap.Int("columnNumber", colsLen))
		}

		trace_util_0.Count(_tables_00000, 16)
		col := table.ToColumn(colInfo)
		if col.IsGenerated() {
			trace_util_0.Count(_tables_00000, 20)
			expr, err := parseExpression(colInfo.GeneratedExprString)
			if err != nil {
				trace_util_0.Count(_tables_00000, 23)
				return nil, err
			}
			trace_util_0.Count(_tables_00000, 21)
			expr, err = simpleResolveName(expr, tblInfo)
			if err != nil {
				trace_util_0.Count(_tables_00000, 24)
				return nil, err
			}
			trace_util_0.Count(_tables_00000, 22)
			col.GeneratedExpr = expr
		}
		trace_util_0.Count(_tables_00000, 17)
		columns = append(columns, col)
	}

	trace_util_0.Count(_tables_00000, 11)
	var t Table
	initTableCommon(&t.tableCommon, tblInfo, tblInfo.ID, columns, alloc)
	if tblInfo.GetPartitionInfo() == nil {
		trace_util_0.Count(_tables_00000, 25)
		if err := initTableIndices(&t.tableCommon); err != nil {
			trace_util_0.Count(_tables_00000, 27)
			return nil, err
		}
		trace_util_0.Count(_tables_00000, 26)
		return &t, nil
	}

	trace_util_0.Count(_tables_00000, 12)
	return newPartitionedTable(&t, tblInfo)
}

// initTableCommon initializes a tableCommon struct.
func initTableCommon(t *tableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, alloc autoid.Allocator) {
	trace_util_0.Count(_tables_00000, 28)
	t.tableID = tblInfo.ID
	t.physicalTableID = physicalTableID
	t.alloc = alloc
	t.meta = tblInfo
	t.Columns = cols
	t.publicColumns = t.Cols()
	t.writableColumns = t.WritableCols()
	t.writableIndices = t.WritableIndices()
	t.recordPrefix = tablecodec.GenTableRecordPrefix(physicalTableID)
	t.indexPrefix = tablecodec.GenTableIndexPrefix(physicalTableID)
}

// initTableIndices initializes the indices of the tableCommon.
func initTableIndices(t *tableCommon) error {
	trace_util_0.Count(_tables_00000, 29)
	tblInfo := t.meta
	for _, idxInfo := range tblInfo.Indices {
		trace_util_0.Count(_tables_00000, 31)
		if idxInfo.State == model.StateNone {
			trace_util_0.Count(_tables_00000, 33)
			return table.ErrIndexStateCantNone.GenWithStack("index %s can't be in none state", idxInfo.Name)
		}

		// Use partition ID for index, because tableCommon may be table or partition.
		trace_util_0.Count(_tables_00000, 32)
		idx := NewIndex(t.physicalTableID, tblInfo, idxInfo)
		t.indices = append(t.indices, idx)
	}
	trace_util_0.Count(_tables_00000, 30)
	return nil
}

func initTableCommonWithIndices(t *tableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, alloc autoid.Allocator) error {
	trace_util_0.Count(_tables_00000, 34)
	initTableCommon(t, tblInfo, physicalTableID, cols, alloc)
	return initTableIndices(t)
}

// Indices implements table.Table Indices interface.
func (t *tableCommon) Indices() []table.Index {
	trace_util_0.Count(_tables_00000, 35)
	return t.indices
}

// WritableIndices implements table.Table WritableIndices interface.
func (t *tableCommon) WritableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 36)
	if len(t.writableIndices) > 0 {
		trace_util_0.Count(_tables_00000, 39)
		return t.writableIndices
	}
	trace_util_0.Count(_tables_00000, 37)
	writable := make([]table.Index, 0, len(t.indices))
	for _, index := range t.indices {
		trace_util_0.Count(_tables_00000, 40)
		s := index.Meta().State
		if s != model.StateDeleteOnly && s != model.StateDeleteReorganization {
			trace_util_0.Count(_tables_00000, 41)
			writable = append(writable, index)
		}
	}
	trace_util_0.Count(_tables_00000, 38)
	return writable
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (t *tableCommon) DeletableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 42)
	// All indices are deletable because we don't need to check StateNone.
	return t.indices
}

// Meta implements table.Table Meta interface.
func (t *tableCommon) Meta() *model.TableInfo {
	trace_util_0.Count(_tables_00000, 43)
	return t.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (t *Table) GetPhysicalID() int64 {
	trace_util_0.Count(_tables_00000, 44)
	return t.physicalTableID
}

// Cols implements table.Table Cols interface.
func (t *tableCommon) Cols() []*table.Column {
	trace_util_0.Count(_tables_00000, 45)
	if len(t.publicColumns) > 0 {
		trace_util_0.Count(_tables_00000, 48)
		return t.publicColumns
	}
	trace_util_0.Count(_tables_00000, 46)
	publicColumns := make([]*table.Column, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		trace_util_0.Count(_tables_00000, 49)
		if col.State != model.StatePublic {
			trace_util_0.Count(_tables_00000, 51)
			continue
		}
		trace_util_0.Count(_tables_00000, 50)
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			trace_util_0.Count(_tables_00000, 52)
			maxOffset = col.Offset
		}
	}
	trace_util_0.Count(_tables_00000, 47)
	return publicColumns[0 : maxOffset+1]
}

// WritableCols implements table WritableCols interface.
func (t *tableCommon) WritableCols() []*table.Column {
	trace_util_0.Count(_tables_00000, 53)
	if len(t.writableColumns) > 0 {
		trace_util_0.Count(_tables_00000, 56)
		return t.writableColumns
	}
	trace_util_0.Count(_tables_00000, 54)
	writableColumns := make([]*table.Column, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		trace_util_0.Count(_tables_00000, 57)
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			trace_util_0.Count(_tables_00000, 59)
			continue
		}
		trace_util_0.Count(_tables_00000, 58)
		writableColumns[col.Offset] = col
		if maxOffset < col.Offset {
			trace_util_0.Count(_tables_00000, 60)
			maxOffset = col.Offset
		}
	}
	trace_util_0.Count(_tables_00000, 55)
	return writableColumns[0 : maxOffset+1]
}

// RecordPrefix implements table.Table interface.
func (t *tableCommon) RecordPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 61)
	return t.recordPrefix
}

// IndexPrefix implements table.Table interface.
func (t *tableCommon) IndexPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 62)
	return t.indexPrefix
}

// RecordKey implements table.Table interface.
func (t *tableCommon) RecordKey(h int64) kv.Key {
	trace_util_0.Count(_tables_00000, 63)
	return tablecodec.EncodeRecordKey(t.recordPrefix, h)
}

// FirstKey implements table.Table interface.
func (t *tableCommon) FirstKey() kv.Key {
	trace_util_0.Count(_tables_00000, 64)
	return t.RecordKey(math.MinInt64)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *tableCommon) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	trace_util_0.Count(_tables_00000, 65)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 75)
		return err
	}

	// TODO: reuse bs, like AddRecord does.
	trace_util_0.Count(_tables_00000, 66)
	bs := kv.NewBufferStore(txn, kv.DefaultTxnMembufCap)

	// rebuild index
	err = t.rebuildIndices(ctx, bs, h, touched, oldData, newData)
	if err != nil {
		trace_util_0.Count(_tables_00000, 76)
		return err
	}
	trace_util_0.Count(_tables_00000, 67)
	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	var colIDs, binlogColIDs []int64
	var row, binlogOldRow, binlogNewRow []types.Datum
	colIDs = make([]int64, 0, numColsCap)
	row = make([]types.Datum, 0, numColsCap)
	if shouldWriteBinlog(ctx) {
		trace_util_0.Count(_tables_00000, 77)
		binlogColIDs = make([]int64, 0, numColsCap)
		binlogOldRow = make([]types.Datum, 0, numColsCap)
		binlogNewRow = make([]types.Datum, 0, numColsCap)
	}

	trace_util_0.Count(_tables_00000, 68)
	for _, col := range t.WritableCols() {
		trace_util_0.Count(_tables_00000, 78)
		var value types.Datum
		if col.State != model.StatePublic {
			trace_util_0.Count(_tables_00000, 81)
			// If col is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the orignal data(it's changed by other TiDBs.) or the orignal default value.
			// TODO: Use newData directly.
			value = oldData[col.Offset]
		} else {
			trace_util_0.Count(_tables_00000, 82)
			{
				value = newData[col.Offset]
			}
		}
		trace_util_0.Count(_tables_00000, 79)
		if !t.canSkip(col, value) {
			trace_util_0.Count(_tables_00000, 83)
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
		trace_util_0.Count(_tables_00000, 80)
		if shouldWriteBinlog(ctx) && !t.canSkipUpdateBinlog(col, value) {
			trace_util_0.Count(_tables_00000, 84)
			binlogColIDs = append(binlogColIDs, col.ID)
			binlogOldRow = append(binlogOldRow, oldData[col.Offset])
			binlogNewRow = append(binlogNewRow, value)
		}
	}

	trace_util_0.Count(_tables_00000, 69)
	key := t.RecordKey(h)
	value, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_tables_00000, 85)
		return err
	}
	trace_util_0.Count(_tables_00000, 70)
	if err = bs.Set(key, value); err != nil {
		trace_util_0.Count(_tables_00000, 86)
		return err
	}
	trace_util_0.Count(_tables_00000, 71)
	if err = bs.SaveTo(txn); err != nil {
		trace_util_0.Count(_tables_00000, 87)
		return err
	}
	trace_util_0.Count(_tables_00000, 72)
	ctx.StmtAddDirtyTableOP(table.DirtyTableDeleteRow, t.physicalTableID, h, nil)
	ctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, h, newData)
	if shouldWriteBinlog(ctx) {
		trace_util_0.Count(_tables_00000, 88)
		if !t.meta.PKIsHandle {
			trace_util_0.Count(_tables_00000, 90)
			binlogColIDs = append(binlogColIDs, model.ExtraHandleID)
			binlogOldRow = append(binlogOldRow, types.NewIntDatum(h))
			binlogNewRow = append(binlogNewRow, types.NewIntDatum(h))
		}
		trace_util_0.Count(_tables_00000, 89)
		err = t.addUpdateBinlog(ctx, binlogOldRow, binlogNewRow, binlogColIDs)
		if err != nil {
			trace_util_0.Count(_tables_00000, 91)
			return err
		}
	}
	trace_util_0.Count(_tables_00000, 73)
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		trace_util_0.Count(_tables_00000, 92)
		val := int64(len(newData[id].GetBytes()) - len(oldData[id].GetBytes()))
		if val != 0 {
			trace_util_0.Count(_tables_00000, 93)
			colSize[col.ID] = val
		}
	}
	trace_util_0.Count(_tables_00000, 74)
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.physicalTableID, 0, 1, colSize)
	return nil
}

func (t *tableCommon) rebuildIndices(ctx sessionctx.Context, rm kv.RetrieverMutator, h int64, touched []bool, oldData []types.Datum, newData []types.Datum) error {
	trace_util_0.Count(_tables_00000, 94)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 98)
		return err
	}
	trace_util_0.Count(_tables_00000, 95)
	for _, idx := range t.DeletableIndices() {
		trace_util_0.Count(_tables_00000, 99)
		for _, ic := range idx.Meta().Columns {
			trace_util_0.Count(_tables_00000, 100)
			if !touched[ic.Offset] {
				trace_util_0.Count(_tables_00000, 104)
				continue
			}
			trace_util_0.Count(_tables_00000, 101)
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				trace_util_0.Count(_tables_00000, 105)
				return err
			}
			trace_util_0.Count(_tables_00000, 102)
			if err = t.removeRowIndex(ctx.GetSessionVars().StmtCtx, rm, h, oldVs, idx, txn); err != nil {
				trace_util_0.Count(_tables_00000, 106)
				return err
			}
			trace_util_0.Count(_tables_00000, 103)
			break
		}
	}
	trace_util_0.Count(_tables_00000, 96)
	for _, idx := range t.WritableIndices() {
		trace_util_0.Count(_tables_00000, 107)
		for _, ic := range idx.Meta().Columns {
			trace_util_0.Count(_tables_00000, 108)
			if !touched[ic.Offset] {
				trace_util_0.Count(_tables_00000, 112)
				continue
			}
			trace_util_0.Count(_tables_00000, 109)
			newVs, err := idx.FetchValues(newData, nil)
			if err != nil {
				trace_util_0.Count(_tables_00000, 113)
				return err
			}
			trace_util_0.Count(_tables_00000, 110)
			if err := t.buildIndexForRow(ctx, rm, h, newVs, idx); err != nil {
				trace_util_0.Count(_tables_00000, 114)
				return err
			}
			trace_util_0.Count(_tables_00000, 111)
			break
		}
	}
	trace_util_0.Count(_tables_00000, 97)
	return nil
}

// adjustRowValuesBuf adjust writeBufs.AddRowValues length, AddRowValues stores the inserting values that is used
// by tablecodec.EncodeRow, the encoded row format is `id1, colval, id2, colval`, so the correct length is rowLen * 2. If
// the inserting row has null value, AddRecord will skip it, so the rowLen will be different, so we need to adjust it.
func adjustRowValuesBuf(writeBufs *variable.WriteStmtBufs, rowLen int) {
	trace_util_0.Count(_tables_00000, 115)
	adjustLen := rowLen * 2
	if writeBufs.AddRowValues == nil || cap(writeBufs.AddRowValues) < adjustLen {
		trace_util_0.Count(_tables_00000, 117)
		writeBufs.AddRowValues = make([]types.Datum, adjustLen)
	}
	trace_util_0.Count(_tables_00000, 116)
	writeBufs.AddRowValues = writeBufs.AddRowValues[:adjustLen]
}

// getRollbackableMemStore get a rollbackable BufferStore, when we are importing data,
// Just add the kv to transaction's membuf directly.
func (t *tableCommon) getRollbackableMemStore(ctx sessionctx.Context) (kv.RetrieverMutator, error) {
	trace_util_0.Count(_tables_00000, 118)
	if ctx.GetSessionVars().LightningMode {
		trace_util_0.Count(_tables_00000, 121)
		return ctx.Txn(true)
	}

	trace_util_0.Count(_tables_00000, 119)
	bs := ctx.GetSessionVars().GetWriteStmtBufs().BufStore
	if bs == nil {
		trace_util_0.Count(_tables_00000, 122)
		txn, err := ctx.Txn(true)
		if err != nil {
			trace_util_0.Count(_tables_00000, 124)
			return nil, err
		}
		trace_util_0.Count(_tables_00000, 123)
		bs = kv.NewBufferStore(txn, kv.DefaultTxnMembufCap)
	} else {
		trace_util_0.Count(_tables_00000, 125)
		{
			bs.Reset()
		}
	}
	trace_util_0.Count(_tables_00000, 120)
	return bs, nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *tableCommon) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	trace_util_0.Count(_tables_00000, 126)
	opt := &table.AddRecordOpt{}
	if len(opts) != 0 {
		trace_util_0.Count(_tables_00000, 139)
		opt = opts[0]
	}
	trace_util_0.Count(_tables_00000, 127)
	var hasRecordID bool
	cols := t.Cols()
	// opt.IsUpdate is a flag for update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
	if len(r) > len(cols) && !opt.IsUpdate {
		trace_util_0.Count(_tables_00000, 140)
		// The last value is _tidb_rowid.
		recordID = r[len(r)-1].GetInt64()
		hasRecordID = true
	} else {
		trace_util_0.Count(_tables_00000, 141)
		{
			for _, col := range cols {
				trace_util_0.Count(_tables_00000, 142)
				if col.IsPKHandleColumn(t.meta) {
					trace_util_0.Count(_tables_00000, 143)
					recordID = r[col.Offset].GetInt64()
					hasRecordID = true
					break
				}
			}
		}
	}
	trace_util_0.Count(_tables_00000, 128)
	if !hasRecordID {
		trace_util_0.Count(_tables_00000, 144)
		recordID, err = t.AllocHandle(ctx)
		if err != nil {
			trace_util_0.Count(_tables_00000, 145)
			return 0, err
		}
	}

	trace_util_0.Count(_tables_00000, 129)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 146)
		return 0, err
	}

	trace_util_0.Count(_tables_00000, 130)
	sessVars := ctx.GetSessionVars()

	rm, err := t.getRollbackableMemStore(ctx)
	// Insert new entries into indices.
	h, err := t.addIndices(ctx, recordID, r, rm, &opt.CreateIdxOpt)
	if err != nil {
		trace_util_0.Count(_tables_00000, 147)
		return h, err
	}

	trace_util_0.Count(_tables_00000, 131)
	var colIDs, binlogColIDs []int64
	var row, binlogRow []types.Datum
	colIDs = make([]int64, 0, len(r))
	row = make([]types.Datum, 0, len(r))

	for _, col := range t.WritableCols() {
		trace_util_0.Count(_tables_00000, 148)
		var value types.Datum
		// Update call `AddRecord` will already handle the write only column default value.
		// Only insert should add default value for write only column.
		if col.State != model.StatePublic && !opt.IsUpdate {
			trace_util_0.Count(_tables_00000, 150)
			// If col is in write only or write reorganization state, we must add it with its default value.
			value, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
			if err != nil {
				trace_util_0.Count(_tables_00000, 152)
				return 0, err
			}
			// add value to `r` for dirty db in transaction.
			// Otherwise when update will panic cause by get value of column in write only state from dirty db.
			trace_util_0.Count(_tables_00000, 151)
			if col.Offset < len(r) {
				trace_util_0.Count(_tables_00000, 153)
				r[col.Offset] = value
			} else {
				trace_util_0.Count(_tables_00000, 154)
				{
					r = append(r, value)
				}
			}
		} else {
			trace_util_0.Count(_tables_00000, 155)
			{
				value = r[col.Offset]
			}
		}
		trace_util_0.Count(_tables_00000, 149)
		if !t.canSkip(col, value) {
			trace_util_0.Count(_tables_00000, 156)
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
	}
	trace_util_0.Count(_tables_00000, 132)
	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(row))
	key := t.RecordKey(recordID)
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues)
	if err != nil {
		trace_util_0.Count(_tables_00000, 157)
		return 0, err
	}
	trace_util_0.Count(_tables_00000, 133)
	value := writeBufs.RowValBuf
	if err = txn.Set(key, value); err != nil {
		trace_util_0.Count(_tables_00000, 158)
		return 0, err
	}
	trace_util_0.Count(_tables_00000, 134)
	txn.SetAssertion(key, kv.None)

	if !sessVars.LightningMode {
		trace_util_0.Count(_tables_00000, 159)
		if err = rm.(*kv.BufferStore).SaveTo(txn); err != nil {
			trace_util_0.Count(_tables_00000, 160)
			return 0, err
		}
	}

	trace_util_0.Count(_tables_00000, 135)
	if !ctx.GetSessionVars().LightningMode {
		trace_util_0.Count(_tables_00000, 161)
		ctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, recordID, r)
	}
	trace_util_0.Count(_tables_00000, 136)
	if shouldWriteBinlog(ctx) {
		trace_util_0.Count(_tables_00000, 162)
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		err = t.addInsertBinlog(ctx, recordID, binlogRow, binlogColIDs)
		if err != nil {
			trace_util_0.Count(_tables_00000, 163)
			return 0, err
		}
	}
	trace_util_0.Count(_tables_00000, 137)
	sessVars.StmtCtx.AddAffectedRows(1)
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		trace_util_0.Count(_tables_00000, 164)
		val := int64(len(r[id].GetBytes()))
		if val != 0 {
			trace_util_0.Count(_tables_00000, 165)
			colSize[col.ID] = val
		}
	}
	trace_util_0.Count(_tables_00000, 138)
	sessVars.TxnCtx.UpdateDeltaForTable(t.physicalTableID, 1, 1, colSize)
	return recordID, nil
}

// genIndexKeyStr generates index content string representation.
func (t *tableCommon) genIndexKeyStr(colVals []types.Datum) (string, error) {
	trace_util_0.Count(_tables_00000, 166)
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		trace_util_0.Count(_tables_00000, 168)
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			trace_util_0.Count(_tables_00000, 170)
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				trace_util_0.Count(_tables_00000, 171)
				return "", err
			}
		}
		trace_util_0.Count(_tables_00000, 169)
		strVals = append(strVals, cvs)
	}
	trace_util_0.Count(_tables_00000, 167)
	return strings.Join(strVals, "-"), nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *tableCommon) addIndices(ctx sessionctx.Context, recordID int64, r []types.Datum, rm kv.RetrieverMutator,
	opt *table.CreateIdxOpt) (int64, error) {
	trace_util_0.Count(_tables_00000, 172)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 176)
		return 0, err
	}
	// Clean up lazy check error environment
	trace_util_0.Count(_tables_00000, 173)
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	skipCheck := ctx.GetSessionVars().LightningMode || ctx.GetSessionVars().StmtCtx.BatchCheck
	if t.meta.PKIsHandle && !skipCheck && !opt.SkipHandleCheck {
		trace_util_0.Count(_tables_00000, 177)
		if err := CheckHandleExists(ctx, t, recordID, nil); err != nil {
			trace_util_0.Count(_tables_00000, 178)
			return recordID, err
		}
	}

	trace_util_0.Count(_tables_00000, 174)
	writeBufs := ctx.GetSessionVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	for _, v := range t.WritableIndices() {
		trace_util_0.Count(_tables_00000, 179)
		var err2 error
		indexVals, err2 = v.FetchValues(r, indexVals)
		if err2 != nil {
			trace_util_0.Count(_tables_00000, 183)
			return 0, err2
		}
		trace_util_0.Count(_tables_00000, 180)
		var dupKeyErr error
		if !skipCheck && v.Meta().Unique {
			trace_util_0.Count(_tables_00000, 184)
			entryKey, err1 := t.genIndexKeyStr(indexVals)
			if err1 != nil {
				trace_util_0.Count(_tables_00000, 186)
				return 0, err1
			}
			trace_util_0.Count(_tables_00000, 185)
			dupKeyErr = kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'", entryKey, v.Meta().Name)
			txn.SetOption(kv.PresumeKeyNotExistsError, dupKeyErr)
		}
		trace_util_0.Count(_tables_00000, 181)
		if dupHandle, err := v.Create(ctx, rm, indexVals, recordID, opt); err != nil {
			trace_util_0.Count(_tables_00000, 187)
			if kv.ErrKeyExists.Equal(err) {
				trace_util_0.Count(_tables_00000, 189)
				return dupHandle, dupKeyErr
			}
			trace_util_0.Count(_tables_00000, 188)
			return 0, err
		}
		trace_util_0.Count(_tables_00000, 182)
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}
	// save the buffer, multi rows insert can use it.
	trace_util_0.Count(_tables_00000, 175)
	writeBufs.IndexValsBuf = indexVals
	return 0, nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *tableCommon) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 190)
	// Get raw row data from kv.
	key := t.RecordKey(h)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 194)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 191)
	value, err := txn.Get(key)
	if err != nil {
		trace_util_0.Count(_tables_00000, 195)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 192)
	v, _, err := DecodeRawRowData(ctx, t.Meta(), h, cols, value)
	if err != nil {
		trace_util_0.Count(_tables_00000, 196)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 193)
	return v, nil
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h int64, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 197)
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		trace_util_0.Count(_tables_00000, 201)
		if col == nil {
			trace_util_0.Count(_tables_00000, 204)
			continue
		}
		trace_util_0.Count(_tables_00000, 202)
		if col.IsPKHandleColumn(meta) {
			trace_util_0.Count(_tables_00000, 205)
			if mysql.HasUnsignedFlag(col.Flag) {
				trace_util_0.Count(_tables_00000, 207)
				v[i].SetUint64(uint64(h))
			} else {
				trace_util_0.Count(_tables_00000, 208)
				{
					v[i].SetInt64(h)
				}
			}
			trace_util_0.Count(_tables_00000, 206)
			continue
		}
		trace_util_0.Count(_tables_00000, 203)
		colTps[col.ID] = &col.FieldType
	}
	trace_util_0.Count(_tables_00000, 198)
	rowMap, err := tablecodec.DecodeRow(value, colTps, ctx.GetSessionVars().Location())
	if err != nil {
		trace_util_0.Count(_tables_00000, 209)
		return nil, rowMap, err
	}
	trace_util_0.Count(_tables_00000, 199)
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		trace_util_0.Count(_tables_00000, 210)
		if col == nil {
			trace_util_0.Count(_tables_00000, 214)
			continue
		}
		trace_util_0.Count(_tables_00000, 211)
		if col.IsPKHandleColumn(meta) {
			trace_util_0.Count(_tables_00000, 215)
			continue
		}
		trace_util_0.Count(_tables_00000, 212)
		ri, ok := rowMap[col.ID]
		if ok {
			trace_util_0.Count(_tables_00000, 216)
			v[i] = ri
			continue
		}
		trace_util_0.Count(_tables_00000, 213)
		v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		if err != nil {
			trace_util_0.Count(_tables_00000, 217)
			return nil, rowMap, err
		}
	}
	trace_util_0.Count(_tables_00000, 200)
	return v, rowMap, nil
}

// Row implements table.Table Row interface.
func (t *tableCommon) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 218)
	r, err := t.RowWithCols(ctx, h, t.Cols())
	if err != nil {
		trace_util_0.Count(_tables_00000, 220)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 219)
	return r, nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *tableCommon) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	trace_util_0.Count(_tables_00000, 221)
	err := t.removeRowData(ctx, h)
	if err != nil {
		trace_util_0.Count(_tables_00000, 226)
		return err
	}
	trace_util_0.Count(_tables_00000, 222)
	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		trace_util_0.Count(_tables_00000, 227)
		return err
	}

	trace_util_0.Count(_tables_00000, 223)
	ctx.StmtAddDirtyTableOP(table.DirtyTableDeleteRow, t.physicalTableID, h, nil)
	if shouldWriteBinlog(ctx) {
		trace_util_0.Count(_tables_00000, 228)
		cols := t.Cols()
		colIDs := make([]int64, 0, len(cols)+1)
		for _, col := range cols {
			trace_util_0.Count(_tables_00000, 231)
			colIDs = append(colIDs, col.ID)
		}
		trace_util_0.Count(_tables_00000, 229)
		var binlogRow []types.Datum
		if !t.meta.PKIsHandle {
			trace_util_0.Count(_tables_00000, 232)
			colIDs = append(colIDs, model.ExtraHandleID)
			binlogRow = make([]types.Datum, 0, len(r)+1)
			binlogRow = append(binlogRow, r...)
			binlogRow = append(binlogRow, types.NewIntDatum(h))
		} else {
			trace_util_0.Count(_tables_00000, 233)
			{
				binlogRow = r
			}
		}
		trace_util_0.Count(_tables_00000, 230)
		err = t.addDeleteBinlog(ctx, binlogRow, colIDs)
	}
	trace_util_0.Count(_tables_00000, 224)
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		trace_util_0.Count(_tables_00000, 234)
		val := -int64(len(r[id].GetBytes()))
		if val != 0 {
			trace_util_0.Count(_tables_00000, 235)
			colSize[col.ID] = val
		}
	}
	trace_util_0.Count(_tables_00000, 225)
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.physicalTableID, -1, 1, colSize)
	return err
}

func (t *tableCommon) addInsertBinlog(ctx sessionctx.Context, h int64, row []types.Datum, colIDs []int64) error {
	trace_util_0.Count(_tables_00000, 236)
	mutation := t.getMutation(ctx)
	pk, err := codec.EncodeValue(ctx.GetSessionVars().StmtCtx, nil, types.NewIntDatum(h))
	if err != nil {
		trace_util_0.Count(_tables_00000, 239)
		return err
	}
	trace_util_0.Count(_tables_00000, 237)
	value, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_tables_00000, 240)
		return err
	}
	trace_util_0.Count(_tables_00000, 238)
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *tableCommon) addUpdateBinlog(ctx sessionctx.Context, oldRow, newRow []types.Datum, colIDs []int64) error {
	trace_util_0.Count(_tables_00000, 241)
	old, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, oldRow, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_tables_00000, 244)
		return err
	}
	trace_util_0.Count(_tables_00000, 242)
	newVal, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, newRow, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_tables_00000, 245)
		return err
	}
	trace_util_0.Count(_tables_00000, 243)
	bin := append(old, newVal...)
	mutation := t.getMutation(ctx)
	mutation.UpdatedRows = append(mutation.UpdatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Update)
	return nil
}

func (t *tableCommon) addDeleteBinlog(ctx sessionctx.Context, r []types.Datum, colIDs []int64) error {
	trace_util_0.Count(_tables_00000, 246)
	data, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, r, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_tables_00000, 248)
		return err
	}
	trace_util_0.Count(_tables_00000, 247)
	mutation := t.getMutation(ctx)
	mutation.DeletedRows = append(mutation.DeletedRows, data)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_DeleteRow)
	return nil
}

func (t *tableCommon) removeRowData(ctx sessionctx.Context, h int64) error {
	trace_util_0.Count(_tables_00000, 249)
	// Remove row data.
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 252)
		return err
	}

	trace_util_0.Count(_tables_00000, 250)
	key := t.RecordKey(h)
	txn.SetAssertion(key, kv.Exist)
	err = txn.Delete([]byte(key))
	if err != nil {
		trace_util_0.Count(_tables_00000, 253)
		return err
	}
	trace_util_0.Count(_tables_00000, 251)
	return nil
}

// removeRowIndices removes all the indices of a row.
func (t *tableCommon) removeRowIndices(ctx sessionctx.Context, h int64, rec []types.Datum) error {
	trace_util_0.Count(_tables_00000, 254)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 257)
		return err
	}
	trace_util_0.Count(_tables_00000, 255)
	for _, v := range t.DeletableIndices() {
		trace_util_0.Count(_tables_00000, 258)
		vals, err := v.FetchValues(rec, nil)
		if err != nil {
			trace_util_0.Count(_tables_00000, 260)
			logutil.Logger(context.Background()).Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.Int64("handle", h), zap.Any("record", rec), zap.Error(err))
			return err
		}
		trace_util_0.Count(_tables_00000, 259)
		if err = v.Delete(ctx.GetSessionVars().StmtCtx, txn, vals, h, txn); err != nil {
			trace_util_0.Count(_tables_00000, 261)
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				trace_util_0.Count(_tables_00000, 263)
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.Logger(context.Background()).Debug("row index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.Int64("handle", h))
				continue
			}
			trace_util_0.Count(_tables_00000, 262)
			return err
		}
	}
	trace_util_0.Count(_tables_00000, 256)
	return nil
}

// removeRowIndex implements table.Table RemoveRowIndex interface.能
func (t *tableCommon) removeRowIndex(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index, txn kv.Transaction) error {
	trace_util_0.Count(_tables_00000, 264)
	return idx.Delete(sc, rm, vals, h, txn)
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *tableCommon) buildIndexForRow(ctx sessionctx.Context, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index) error {
	trace_util_0.Count(_tables_00000, 265)
	if _, err := idx.Create(ctx, rm, vals, h); err != nil {
		trace_util_0.Count(_tables_00000, 267)
		if kv.ErrKeyExists.Equal(err) {
			trace_util_0.Count(_tables_00000, 269)
			// Make error message consistent with MySQL.
			entryKey, err1 := t.genIndexKeyStr(vals)
			if err1 != nil {
				trace_util_0.Count(_tables_00000, 271)
				// if genIndexKeyStr failed, return the original error.
				return err
			}

			trace_util_0.Count(_tables_00000, 270)
			return kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'", entryKey, idx.Meta().Name)
		}
		trace_util_0.Count(_tables_00000, 268)
		return err
	}
	trace_util_0.Count(_tables_00000, 266)
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *tableCommon) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	trace_util_0.Count(_tables_00000, 272)
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 278)
		return err
	}

	trace_util_0.Count(_tables_00000, 273)
	it, err := txn.Iter(startKey, prefix.PrefixNext())
	if err != nil {
		trace_util_0.Count(_tables_00000, 279)
		return err
	}
	trace_util_0.Count(_tables_00000, 274)
	defer it.Close()

	if !it.Valid() {
		trace_util_0.Count(_tables_00000, 280)
		return nil
	}

	trace_util_0.Count(_tables_00000, 275)
	logutil.Logger(context.Background()).Debug("iterate records", zap.ByteString("startKey", startKey), zap.ByteString("key", it.Key()), zap.ByteString("value", it.Value()))

	colMap := make(map[int64]*types.FieldType)
	for _, col := range cols {
		trace_util_0.Count(_tables_00000, 281)
		colMap[col.ID] = &col.FieldType
	}
	trace_util_0.Count(_tables_00000, 276)
	defaultVals := make([]types.Datum, len(cols))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		trace_util_0.Count(_tables_00000, 282)
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			trace_util_0.Count(_tables_00000, 287)
			return err
		}
		trace_util_0.Count(_tables_00000, 283)
		rowMap, err := tablecodec.DecodeRow(it.Value(), colMap, ctx.GetSessionVars().Location())
		if err != nil {
			trace_util_0.Count(_tables_00000, 288)
			return err
		}
		trace_util_0.Count(_tables_00000, 284)
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			trace_util_0.Count(_tables_00000, 289)
			if col.IsPKHandleColumn(t.meta) {
				trace_util_0.Count(_tables_00000, 292)
				if mysql.HasUnsignedFlag(col.Flag) {
					trace_util_0.Count(_tables_00000, 294)
					data[col.Offset].SetUint64(uint64(handle))
				} else {
					trace_util_0.Count(_tables_00000, 295)
					{
						data[col.Offset].SetInt64(handle)
					}
				}
				trace_util_0.Count(_tables_00000, 293)
				continue
			}
			trace_util_0.Count(_tables_00000, 290)
			if _, ok := rowMap[col.ID]; ok {
				trace_util_0.Count(_tables_00000, 296)
				data[col.Offset] = rowMap[col.ID]
				continue
			}
			trace_util_0.Count(_tables_00000, 291)
			data[col.Offset], err = GetColDefaultValue(ctx, col, defaultVals)
			if err != nil {
				trace_util_0.Count(_tables_00000, 297)
				return err
			}
		}
		trace_util_0.Count(_tables_00000, 285)
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			trace_util_0.Count(_tables_00000, 298)
			return err
		}

		trace_util_0.Count(_tables_00000, 286)
		rk := t.RecordKey(handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			trace_util_0.Count(_tables_00000, 299)
			return err
		}
	}

	trace_util_0.Count(_tables_00000, 277)
	return nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx sessionctx.Context, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 300)
	if col.OriginDefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
		trace_util_0.Count(_tables_00000, 304)
		return colVal, errors.New("Miss column")
	}
	trace_util_0.Count(_tables_00000, 301)
	if col.State != model.StatePublic {
		trace_util_0.Count(_tables_00000, 305)
		return colVal, nil
	}
	trace_util_0.Count(_tables_00000, 302)
	if defaultVals[col.Offset].IsNull() {
		trace_util_0.Count(_tables_00000, 306)
		colVal, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
		if err != nil {
			trace_util_0.Count(_tables_00000, 308)
			return colVal, err
		}
		trace_util_0.Count(_tables_00000, 307)
		defaultVals[col.Offset] = colVal
	} else {
		trace_util_0.Count(_tables_00000, 309)
		{
			colVal = defaultVals[col.Offset]
		}
	}

	trace_util_0.Count(_tables_00000, 303)
	return colVal, nil
}

// AllocAutoIncrementValue implements table.Table AllocAutoIncrementValue interface.
func (t *tableCommon) AllocAutoIncrementValue(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 310)
	return t.Allocator(ctx).Alloc(t.tableID)
}

// AllocHandle implements table.Table AllocHandle interface.
func (t *tableCommon) AllocHandle(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 311)
	rowID, err := t.Allocator(ctx).Alloc(t.tableID)
	if err != nil {
		trace_util_0.Count(_tables_00000, 314)
		return 0, err
	}
	trace_util_0.Count(_tables_00000, 312)
	if t.meta.ShardRowIDBits > 0 {
		trace_util_0.Count(_tables_00000, 315)
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(rowID, t.meta.MaxShardRowIDBits) {
			trace_util_0.Count(_tables_00000, 318)
			// If overflow, the rowID may be duplicated. For examples,
			// t.meta.ShardRowIDBits = 4
			// rowID = 0010111111111111111111111111111111111111111111111111111111111111
			// shard = 01000000000000000000000000000000000000000000000000000000000000000
			// will be duplicated with:
			// rowID = 0100111111111111111111111111111111111111111111111111111111111111
			// shard = 0010000000000000000000000000000000000000000000000000000000000000
			return 0, autoid.ErrAutoincReadFailed
		}
		trace_util_0.Count(_tables_00000, 316)
		txnCtx := ctx.GetSessionVars().TxnCtx
		if txnCtx.Shard == nil {
			trace_util_0.Count(_tables_00000, 319)
			shard := t.calcShard(txnCtx.StartTS)
			txnCtx.Shard = &shard
		}
		trace_util_0.Count(_tables_00000, 317)
		rowID |= *txnCtx.Shard
	}
	trace_util_0.Count(_tables_00000, 313)
	return rowID, nil
}

// OverflowShardBits checks whether the rowID overflow `1<<(64-shardRowIDBits-1) -1`.
func OverflowShardBits(rowID int64, shardRowIDBits uint64) bool {
	trace_util_0.Count(_tables_00000, 320)
	mask := (1<<shardRowIDBits - 1) << (64 - shardRowIDBits - 1)
	return rowID&int64(mask) > 0
}

func (t *tableCommon) calcShard(startTS uint64) int64 {
	trace_util_0.Count(_tables_00000, 321)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], startTS)
	hashVal := int64(murmur3.Sum32(buf[:]))
	return (hashVal & (1<<t.meta.ShardRowIDBits - 1)) << (64 - t.meta.ShardRowIDBits - 1)
}

// Allocator implements table.Table Allocator interface.
func (t *tableCommon) Allocator(ctx sessionctx.Context) autoid.Allocator {
	trace_util_0.Count(_tables_00000, 322)
	if ctx != nil {
		trace_util_0.Count(_tables_00000, 324)
		sessAlloc := ctx.GetSessionVars().IDAllocator
		if sessAlloc != nil {
			trace_util_0.Count(_tables_00000, 325)
			return sessAlloc
		}
	}
	trace_util_0.Count(_tables_00000, 323)
	return t.alloc
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (t *tableCommon) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	trace_util_0.Count(_tables_00000, 326)
	return t.Allocator(ctx).Rebase(t.tableID, newBase, isSetStep)
}

// Seek implements table.Table Seek interface.
func (t *tableCommon) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	trace_util_0.Count(_tables_00000, 327)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 332)
		return 0, false, err
	}
	trace_util_0.Count(_tables_00000, 328)
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.physicalTableID, h)
	iter, err := txn.Iter(seekKey, t.RecordPrefix().PrefixNext())
	if err != nil {
		trace_util_0.Count(_tables_00000, 333)
		return 0, false, err
	}
	trace_util_0.Count(_tables_00000, 329)
	if !iter.Valid() || !iter.Key().HasPrefix(t.RecordPrefix()) {
		trace_util_0.Count(_tables_00000, 334)
		// No more records in the table, skip to the end.
		return 0, false, nil
	}
	trace_util_0.Count(_tables_00000, 330)
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		trace_util_0.Count(_tables_00000, 335)
		return 0, false, err
	}
	trace_util_0.Count(_tables_00000, 331)
	return handle, true, nil
}

// Type implements table.Table Type interface.
func (t *tableCommon) Type() table.Type {
	trace_util_0.Count(_tables_00000, 336)
	return table.NormalTable
}

func shouldWriteBinlog(ctx sessionctx.Context) bool {
	trace_util_0.Count(_tables_00000, 337)
	if ctx.GetSessionVars().BinlogClient == nil {
		trace_util_0.Count(_tables_00000, 339)
		return false
	}
	trace_util_0.Count(_tables_00000, 338)
	return !ctx.GetSessionVars().InRestrictedSQL
}

func (t *tableCommon) getMutation(ctx sessionctx.Context) *binlog.TableMutation {
	trace_util_0.Count(_tables_00000, 340)
	return ctx.StmtGetMutation(t.tableID)
}

func (t *tableCommon) canSkip(col *table.Column, value types.Datum) bool {
	trace_util_0.Count(_tables_00000, 341)
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that;
// 3. the column is virtual generated.
func CanSkip(info *model.TableInfo, col *table.Column, value types.Datum) bool {
	trace_util_0.Count(_tables_00000, 342)
	if col.IsPKHandleColumn(info) {
		trace_util_0.Count(_tables_00000, 346)
		return true
	}
	trace_util_0.Count(_tables_00000, 343)
	if col.GetDefaultValue() == nil && value.IsNull() {
		trace_util_0.Count(_tables_00000, 347)
		return true
	}
	trace_util_0.Count(_tables_00000, 344)
	if col.IsGenerated() && !col.GeneratedStored {
		trace_util_0.Count(_tables_00000, 348)
		return true
	}
	trace_util_0.Count(_tables_00000, 345)
	return false
}

// canSkipUpdateBinlog checks whether the column can be skipped or not.
func (t *tableCommon) canSkipUpdateBinlog(col *table.Column, value types.Datum) bool {
	trace_util_0.Count(_tables_00000, 349)
	if col.IsGenerated() && !col.GeneratedStored {
		trace_util_0.Count(_tables_00000, 351)
		return true
	}
	trace_util_0.Count(_tables_00000, 350)
	return false
}

var (
	recordPrefixSep = []byte("_r")
)

// FindIndexByColName implements table.Table FindIndexByColName interface.
func FindIndexByColName(t table.Table, name string) table.Index {
	trace_util_0.Count(_tables_00000, 352)
	for _, idx := range t.Indices() {
		trace_util_0.Count(_tables_00000, 354)
		// only public index can be read.
		if idx.Meta().State != model.StatePublic {
			trace_util_0.Count(_tables_00000, 356)
			continue
		}

		trace_util_0.Count(_tables_00000, 355)
		if len(idx.Meta().Columns) == 1 && strings.EqualFold(idx.Meta().Columns[0].Name.L, name) {
			trace_util_0.Count(_tables_00000, 357)
			return idx
		}
	}
	trace_util_0.Count(_tables_00000, 353)
	return nil
}

// CheckHandleExists check whether recordID key exists. if not exists, return nil,
// otherwise return kv.ErrKeyExists error.
func CheckHandleExists(ctx sessionctx.Context, t table.Table, recordID int64, data []types.Datum) error {
	trace_util_0.Count(_tables_00000, 358)
	if pt, ok := t.(*partitionedTable); ok {
		trace_util_0.Count(_tables_00000, 362)
		info := t.Meta().GetPartitionInfo()
		pid, err := pt.locatePartition(ctx, info, data)
		if err != nil {
			trace_util_0.Count(_tables_00000, 364)
			return err
		}
		trace_util_0.Count(_tables_00000, 363)
		t = pt.GetPartition(pid)
	}
	trace_util_0.Count(_tables_00000, 359)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_tables_00000, 365)
		return err
	}
	// Check key exists.
	trace_util_0.Count(_tables_00000, 360)
	recordKey := t.RecordKey(recordID)
	e := kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", recordID)
	txn.SetOption(kv.PresumeKeyNotExistsError, e)
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	_, err = txn.Get(recordKey)
	if err == nil {
		trace_util_0.Count(_tables_00000, 366)
		return e
	} else {
		trace_util_0.Count(_tables_00000, 367)
		if !kv.ErrNotExist.Equal(err) {
			trace_util_0.Count(_tables_00000, 368)
			return err
		}
	}
	trace_util_0.Count(_tables_00000, 361)
	return nil
}

func init() {
	trace_util_0.Count(_tables_00000, 369)
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
}

var _tables_00000 = "table/tables/tables.go"
