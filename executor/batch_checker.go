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

package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type keyValue struct {
	key   kv.Key
	value []byte
}

type keyValueWithDupInfo struct {
	newKV  keyValue
	dupErr error
}

type toBeCheckedRow struct {
	row        []types.Datum
	rowValue   []byte
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
	// t is the table or partition this row belongs to.
	t table.Table
}

type batchChecker struct {
	// toBeCheckedRows is used for duplicate key update
	toBeCheckedRows []toBeCheckedRow
	dupKVs          map[string][]byte
	dupOldRowValues map[string][]byte
}

// batchGetOldValues gets the values of storage in batch.
func (b *batchChecker) batchGetOldValues(ctx sessionctx.Context, batchKeys []kv.Key) error {
	trace_util_0.Count(_batch_checker_00000, 0)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 4)
		return err
	}
	trace_util_0.Count(_batch_checker_00000, 1)
	values, err := txn.BatchGet(batchKeys)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 5)
		return err
	}
	trace_util_0.Count(_batch_checker_00000, 2)
	for k, v := range values {
		trace_util_0.Count(_batch_checker_00000, 6)
		b.dupOldRowValues[k] = v
	}
	trace_util_0.Count(_batch_checker_00000, 3)
	return nil
}

// encodeNewRow encodes a new row to value.
func (b *batchChecker) encodeNewRow(ctx sessionctx.Context, t table.Table, row []types.Datum) ([]byte, error) {
	trace_util_0.Count(_batch_checker_00000, 7)
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make([]types.Datum, 0, len(row))
	for _, col := range t.Cols() {
		trace_util_0.Count(_batch_checker_00000, 10)
		if !tables.CanSkip(t.Meta(), col, row[col.Offset]) {
			trace_util_0.Count(_batch_checker_00000, 11)
			colIDs = append(colIDs, col.ID)
			skimmedRow = append(skimmedRow, row[col.Offset])
		}
	}
	trace_util_0.Count(_batch_checker_00000, 8)
	newRowValue, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, skimmedRow, colIDs, nil, nil)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 12)
		return nil, err
	}
	trace_util_0.Count(_batch_checker_00000, 9)
	return newRowValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func (b *batchChecker) getKeysNeedCheck(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([]toBeCheckedRow, error) {
	trace_util_0.Count(_batch_checker_00000, 13)
	nUnique := 0
	for _, v := range t.WritableIndices() {
		trace_util_0.Count(_batch_checker_00000, 17)
		if v.Meta().Unique {
			trace_util_0.Count(_batch_checker_00000, 18)
			nUnique++
		}
	}
	trace_util_0.Count(_batch_checker_00000, 14)
	toBeCheckRows := make([]toBeCheckedRow, 0, len(rows))

	var handleCol *table.Column
	// Get handle column if PK is handle.
	if t.Meta().PKIsHandle {
		trace_util_0.Count(_batch_checker_00000, 19)
		for _, col := range t.Cols() {
			trace_util_0.Count(_batch_checker_00000, 20)
			if col.IsPKHandleColumn(t.Meta()) {
				trace_util_0.Count(_batch_checker_00000, 21)
				handleCol = col
				break
			}
		}
	}

	trace_util_0.Count(_batch_checker_00000, 15)
	var err error
	for _, row := range rows {
		trace_util_0.Count(_batch_checker_00000, 22)
		toBeCheckRows, err = b.getKeysNeedCheckOneRow(ctx, t, row, nUnique, handleCol, toBeCheckRows)
		if err != nil {
			trace_util_0.Count(_batch_checker_00000, 23)
			return nil, err
		}
	}
	trace_util_0.Count(_batch_checker_00000, 16)
	return toBeCheckRows, nil
}

func (b *batchChecker) getKeysNeedCheckOneRow(ctx sessionctx.Context, t table.Table, row []types.Datum, nUnique int, handleCol *table.Column, result []toBeCheckedRow) ([]toBeCheckedRow, error) {
	trace_util_0.Count(_batch_checker_00000, 24)
	var err error
	if p, ok := t.(table.PartitionedTable); ok {
		trace_util_0.Count(_batch_checker_00000, 29)
		t, err = p.GetPartitionByRow(ctx, row)
		if err != nil {
			trace_util_0.Count(_batch_checker_00000, 30)
			return nil, err
		}
	}

	trace_util_0.Count(_batch_checker_00000, 25)
	var handleKey *keyValueWithDupInfo
	uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
	newRowValue, err := b.encodeNewRow(ctx, t, row)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 31)
		return nil, err
	}
	// Append record keys and errors.
	trace_util_0.Count(_batch_checker_00000, 26)
	if handleCol != nil {
		trace_util_0.Count(_batch_checker_00000, 32)
		handle := row[handleCol.Offset].GetInt64()
		handleKey = &keyValueWithDupInfo{
			newKV: keyValue{
				key:   t.RecordKey(handle),
				value: newRowValue,
			},
			dupErr: kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", handle),
		}
	}

	// append unique keys and errors
	trace_util_0.Count(_batch_checker_00000, 27)
	for _, v := range t.WritableIndices() {
		trace_util_0.Count(_batch_checker_00000, 33)
		if !v.Meta().Unique {
			trace_util_0.Count(_batch_checker_00000, 39)
			continue
		}
		trace_util_0.Count(_batch_checker_00000, 34)
		colVals, err1 := v.FetchValues(row, nil)
		if err1 != nil {
			trace_util_0.Count(_batch_checker_00000, 40)
			return nil, err1
		}
		// Pass handle = 0 to GenIndexKey,
		// due to we only care about distinct key.
		trace_util_0.Count(_batch_checker_00000, 35)
		key, distinct, err1 := v.GenIndexKey(ctx.GetSessionVars().StmtCtx,
			colVals, 0, nil)
		if err1 != nil {
			trace_util_0.Count(_batch_checker_00000, 41)
			return nil, err1
		}
		// Skip the non-distinct keys.
		trace_util_0.Count(_batch_checker_00000, 36)
		if !distinct {
			trace_util_0.Count(_batch_checker_00000, 42)
			continue
		}
		trace_util_0.Count(_batch_checker_00000, 37)
		colValStr, err1 := types.DatumsToString(colVals, false)
		if err1 != nil {
			trace_util_0.Count(_batch_checker_00000, 43)
			return nil, err1
		}
		trace_util_0.Count(_batch_checker_00000, 38)
		uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
			newKV: keyValue{
				key: key,
			},
			dupErr: kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'",
				colValStr, v.Meta().Name),
		})
	}
	trace_util_0.Count(_batch_checker_00000, 28)
	result = append(result, toBeCheckedRow{
		row:        row,
		rowValue:   newRowValue,
		handleKey:  handleKey,
		uniqueKeys: uniqueKeys,
		t:          t,
	})
	return result, nil
}

// batchGetInsertKeys uses batch-get to fetch all key-value pairs to be checked for ignore or duplicate key update.
func (b *batchChecker) batchGetInsertKeys(ctx sessionctx.Context, t table.Table, newRows [][]types.Datum) (err error) {
	trace_util_0.Count(_batch_checker_00000, 44)
	// Get keys need to be checked.
	b.toBeCheckedRows, err = b.getKeysNeedCheck(ctx, t, newRows)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 49)
		return err
	}

	// Batch get values.
	trace_util_0.Count(_batch_checker_00000, 45)
	nKeys := 0
	for _, r := range b.toBeCheckedRows {
		trace_util_0.Count(_batch_checker_00000, 50)
		if r.handleKey != nil {
			trace_util_0.Count(_batch_checker_00000, 52)
			nKeys++
		}
		trace_util_0.Count(_batch_checker_00000, 51)
		nKeys += len(r.uniqueKeys)
	}
	trace_util_0.Count(_batch_checker_00000, 46)
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, r := range b.toBeCheckedRows {
		trace_util_0.Count(_batch_checker_00000, 53)
		if r.handleKey != nil {
			trace_util_0.Count(_batch_checker_00000, 55)
			batchKeys = append(batchKeys, r.handleKey.newKV.key)
		}
		trace_util_0.Count(_batch_checker_00000, 54)
		for _, k := range r.uniqueKeys {
			trace_util_0.Count(_batch_checker_00000, 56)
			batchKeys = append(batchKeys, k.newKV.key)
		}
	}
	trace_util_0.Count(_batch_checker_00000, 47)
	txn, err := ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 57)
		return err
	}
	trace_util_0.Count(_batch_checker_00000, 48)
	b.dupKVs, err = txn.BatchGet(batchKeys)
	return err
}

func (b *batchChecker) initDupOldRowFromHandleKey() {
	trace_util_0.Count(_batch_checker_00000, 58)
	for _, r := range b.toBeCheckedRows {
		trace_util_0.Count(_batch_checker_00000, 59)
		if r.handleKey == nil {
			trace_util_0.Count(_batch_checker_00000, 61)
			continue
		}
		trace_util_0.Count(_batch_checker_00000, 60)
		k := r.handleKey.newKV.key
		if val, found := b.dupKVs[string(k)]; found {
			trace_util_0.Count(_batch_checker_00000, 62)
			b.dupOldRowValues[string(k)] = val
		}
	}
}

func (b *batchChecker) initDupOldRowFromUniqueKey(ctx sessionctx.Context, newRows [][]types.Datum) error {
	trace_util_0.Count(_batch_checker_00000, 63)
	batchKeys := make([]kv.Key, 0, len(newRows))
	for _, r := range b.toBeCheckedRows {
		trace_util_0.Count(_batch_checker_00000, 65)
		for _, uk := range r.uniqueKeys {
			trace_util_0.Count(_batch_checker_00000, 66)
			if val, found := b.dupKVs[string(uk.newKV.key)]; found {
				trace_util_0.Count(_batch_checker_00000, 67)
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					trace_util_0.Count(_batch_checker_00000, 69)
					return err
				}
				trace_util_0.Count(_batch_checker_00000, 68)
				batchKeys = append(batchKeys, r.t.RecordKey(handle))
			}
		}
	}
	trace_util_0.Count(_batch_checker_00000, 64)
	return b.batchGetOldValues(ctx, batchKeys)
}

// initDupOldRowValue initializes dupOldRowValues which contain the to-be-updated rows from storage.
func (b *batchChecker) initDupOldRowValue(ctx sessionctx.Context, t table.Table, newRows [][]types.Datum) error {
	trace_util_0.Count(_batch_checker_00000, 70)
	b.dupOldRowValues = make(map[string][]byte, len(newRows))
	b.initDupOldRowFromHandleKey()
	return b.initDupOldRowFromUniqueKey(ctx, newRows)
}

// fillBackKeys fills the updated key-value pair to the dupKeyValues for further check.
func (b *batchChecker) fillBackKeys(t table.Table, row toBeCheckedRow, handle int64) {
	trace_util_0.Count(_batch_checker_00000, 71)
	if row.rowValue != nil {
		trace_util_0.Count(_batch_checker_00000, 74)
		b.dupOldRowValues[string(t.RecordKey(handle))] = row.rowValue
	}
	trace_util_0.Count(_batch_checker_00000, 72)
	if row.handleKey != nil {
		trace_util_0.Count(_batch_checker_00000, 75)
		b.dupKVs[string(row.handleKey.newKV.key)] = row.handleKey.newKV.value
	}
	trace_util_0.Count(_batch_checker_00000, 73)
	for _, uk := range row.uniqueKeys {
		trace_util_0.Count(_batch_checker_00000, 76)
		b.dupKVs[string(uk.newKV.key)] = tables.EncodeHandle(handle)
	}
}

// deleteDupKeys picks primary/unique key-value pairs from rows and remove them from the dupKVs
func (b *batchChecker) deleteDupKeys(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) error {
	trace_util_0.Count(_batch_checker_00000, 77)
	cleanupRows, err := b.getKeysNeedCheck(ctx, t, rows)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 80)
		return err
	}
	trace_util_0.Count(_batch_checker_00000, 78)
	for _, row := range cleanupRows {
		trace_util_0.Count(_batch_checker_00000, 81)
		if row.handleKey != nil {
			trace_util_0.Count(_batch_checker_00000, 83)
			delete(b.dupKVs, string(row.handleKey.newKV.key))
		}
		trace_util_0.Count(_batch_checker_00000, 82)
		for _, uk := range row.uniqueKeys {
			trace_util_0.Count(_batch_checker_00000, 84)
			delete(b.dupKVs, string(uk.newKV.key))
		}
	}
	trace_util_0.Count(_batch_checker_00000, 79)
	return nil
}

// getOldRow gets the table record row from storage for batch check.
// t could be a normal table or a partition, but it must not be a PartitionedTable.
func (b *batchChecker) getOldRow(ctx sessionctx.Context, t table.Table, handle int64,
	genExprs []expression.Expression) ([]types.Datum, error) {
	trace_util_0.Count(_batch_checker_00000, 85)
	oldValue, ok := b.dupOldRowValues[string(t.RecordKey(handle))]
	if !ok {
		trace_util_0.Count(_batch_checker_00000, 89)
		return nil, errors.NotFoundf("can not be duplicated row, due to old row not found. handle %d", handle)
	}
	trace_util_0.Count(_batch_checker_00000, 86)
	cols := t.WritableCols()
	oldRow, oldRowMap, err := tables.DecodeRawRowData(ctx, t.Meta(), handle, cols, oldValue)
	if err != nil {
		trace_util_0.Count(_batch_checker_00000, 90)
		return nil, err
	}
	// Fill write-only and write-reorg columns with originDefaultValue if not found in oldValue.
	trace_util_0.Count(_batch_checker_00000, 87)
	gIdx := 0
	for _, col := range cols {
		trace_util_0.Count(_batch_checker_00000, 91)
		if col.State != model.StatePublic && oldRow[col.Offset].IsNull() {
			trace_util_0.Count(_batch_checker_00000, 93)
			_, found := oldRowMap[col.ID]
			if !found {
				trace_util_0.Count(_batch_checker_00000, 94)
				oldRow[col.Offset], err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
				if err != nil {
					trace_util_0.Count(_batch_checker_00000, 95)
					return nil, err
				}
			}
		}
		trace_util_0.Count(_batch_checker_00000, 92)
		if col.IsGenerated() {
			trace_util_0.Count(_batch_checker_00000, 96)
			// only the virtual column needs fill back.
			if !col.GeneratedStored {
				trace_util_0.Count(_batch_checker_00000, 98)
				val, err := genExprs[gIdx].Eval(chunk.MutRowFromDatums(oldRow).ToRow())
				if err != nil {
					trace_util_0.Count(_batch_checker_00000, 100)
					return nil, err
				}
				trace_util_0.Count(_batch_checker_00000, 99)
				oldRow[col.Offset], err = table.CastValue(ctx, val, col.ToInfo())
				if err != nil {
					trace_util_0.Count(_batch_checker_00000, 101)
					return nil, err
				}
			}
			trace_util_0.Count(_batch_checker_00000, 97)
			gIdx++
		}
	}
	trace_util_0.Count(_batch_checker_00000, 88)
	return oldRow, nil
}

var _batch_checker_00000 = "executor/batch_checker.go"
