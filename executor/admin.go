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
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Executor = &CheckIndexRangeExec{}
	_ Executor = &RecoverIndexExec{}
	_ Executor = &CleanupIndexExec{}
)

// CheckIndexRangeExec outputs the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	baseExecutor

	table    *model.TableInfo
	index    *model.IndexInfo
	is       infoschema.InfoSchema
	startKey []types.Datum

	handleRanges []ast.HandleRange
	srcChunk     *chunk.Chunk

	result distsql.SelectResult
	cols   []*model.ColumnInfo
}

// Next implements the Executor Next interface.
func (e *CheckIndexRangeExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_admin_00000, 0)
	req.Reset()
	handleIdx := e.schema.Len() - 1
	for {
		trace_util_0.Count(_admin_00000, 1)
		err := e.result.Next(ctx, e.srcChunk)
		if err != nil {
			trace_util_0.Count(_admin_00000, 5)
			return err
		}
		trace_util_0.Count(_admin_00000, 2)
		if e.srcChunk.NumRows() == 0 {
			trace_util_0.Count(_admin_00000, 6)
			return nil
		}
		trace_util_0.Count(_admin_00000, 3)
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_admin_00000, 7)
			handle := row.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				trace_util_0.Count(_admin_00000, 8)
				if handle >= hr.Begin && handle < hr.End {
					trace_util_0.Count(_admin_00000, 9)
					req.AppendRow(row)
					break
				}
			}
		}
		trace_util_0.Count(_admin_00000, 4)
		if req.NumRows() > 0 {
			trace_util_0.Count(_admin_00000, 10)
			return nil
		}
	}
}

// Open implements the Executor Open interface.
func (e *CheckIndexRangeExec) Open(ctx context.Context) error {
	trace_util_0.Count(_admin_00000, 11)
	tCols := e.table.Cols()
	for _, ic := range e.index.Columns {
		trace_util_0.Count(_admin_00000, 16)
		col := tCols[ic.Offset]
		e.cols = append(e.cols, col)
	}

	trace_util_0.Count(_admin_00000, 12)
	colTypeForHandle := e.schema.Columns[len(e.cols)].RetType
	e.cols = append(e.cols, &model.ColumnInfo{
		ID:        model.ExtraHandleID,
		Name:      model.ExtraHandleName,
		FieldType: *colTypeForHandle,
	})

	e.srcChunk = newFirstChunk(e)
	dagPB, err := e.buildDAGPB()
	if err != nil {
		trace_util_0.Count(_admin_00000, 17)
		return err
	}
	trace_util_0.Count(_admin_00000, 13)
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.table.ID, e.index.ID, ranger.FullRange()).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		trace_util_0.Count(_admin_00000, 18)
		return err
	}

	trace_util_0.Count(_admin_00000, 14)
	e.result, err = distsql.Select(ctx, e.ctx, kvReq, e.retFieldTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		trace_util_0.Count(_admin_00000, 19)
		return err
	}
	trace_util_0.Count(_admin_00000, 15)
	e.result.Fetch(ctx)
	return nil
}

func (e *CheckIndexRangeExec) buildDAGPB() (*tipb.DAGRequest, error) {
	trace_util_0.Count(_admin_00000, 20)
	dagReq := &tipb.DAGRequest{}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_admin_00000, 24)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 21)
	dagReq.StartTs = txn.StartTS()
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for i := range e.schema.Columns {
		trace_util_0.Count(_admin_00000, 25)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	trace_util_0.Count(_admin_00000, 22)
	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)

	err = plannercore.SetPBColumnsDefaultValue(e.ctx, dagReq.Executors[0].IdxScan.Columns, e.cols)
	if err != nil {
		trace_util_0.Count(_admin_00000, 26)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 23)
	return dagReq, nil
}

func (e *CheckIndexRangeExec) constructIndexScanPB() *tipb.Executor {
	trace_util_0.Count(_admin_00000, 27)
	idxExec := &tipb.IndexScan{
		TableId: e.table.ID,
		IndexId: e.index.ID,
		Columns: model.ColumnsToProto(e.cols, e.table.PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements the Executor Close interface.
func (e *CheckIndexRangeExec) Close() error {
	trace_util_0.Count(_admin_00000, 28)
	return nil
}

// RecoverIndexExec represents a recover index executor.
// It is built from "admin recover index" statement, is used to backfill
// corrupted index.
type RecoverIndexExec struct {
	baseExecutor

	done bool

	index     table.Index
	table     table.Table
	batchSize int

	columns       []*model.ColumnInfo
	colFieldTypes []*types.FieldType
	srcChunk      *chunk.Chunk

	// below buf is used to reduce allocations.
	recoverRows []recoverRows
	idxValsBufs [][]types.Datum
	idxKeyBufs  [][]byte
	batchKeys   []kv.Key
}

func (e *RecoverIndexExec) columnsTypes() []*types.FieldType {
	trace_util_0.Count(_admin_00000, 29)
	if e.colFieldTypes != nil {
		trace_util_0.Count(_admin_00000, 32)
		return e.colFieldTypes
	}

	trace_util_0.Count(_admin_00000, 30)
	e.colFieldTypes = make([]*types.FieldType, 0, len(e.columns))
	for _, col := range e.columns {
		trace_util_0.Count(_admin_00000, 33)
		e.colFieldTypes = append(e.colFieldTypes, &col.FieldType)
	}
	trace_util_0.Count(_admin_00000, 31)
	return e.colFieldTypes
}

// Open implements the Executor Open interface.
func (e *RecoverIndexExec) Open(ctx context.Context) error {
	trace_util_0.Count(_admin_00000, 34)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_admin_00000, 36)
		return err
	}

	trace_util_0.Count(_admin_00000, 35)
	e.srcChunk = chunk.New(e.columnsTypes(), e.initCap, e.maxChunkSize)
	e.batchSize = 2048
	e.recoverRows = make([]recoverRows, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	e.idxKeyBufs = make([][]byte, e.batchSize)
	return nil
}

func (e *RecoverIndexExec) constructTableScanPB(pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	trace_util_0.Count(_admin_00000, 37)
	tblScan := &tipb.TableScan{
		TableId: e.table.Meta().ID,
		Columns: pbColumnInfos,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func (e *RecoverIndexExec) constructLimitPB(count uint64) *tipb.Executor {
	trace_util_0.Count(_admin_00000, 38)
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func (e *RecoverIndexExec) buildDAGPB(txn kv.Transaction, limitCnt uint64) (*tipb.DAGRequest, error) {
	trace_util_0.Count(_admin_00000, 39)
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = txn.StartTS()
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for i := range e.columns {
		trace_util_0.Count(_admin_00000, 42)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	trace_util_0.Count(_admin_00000, 40)
	tblInfo := e.table.Meta()
	pbColumnInfos := model.ColumnsToProto(e.columns, tblInfo.PKIsHandle)
	err := plannercore.SetPBColumnsDefaultValue(e.ctx, pbColumnInfos, e.columns)
	if err != nil {
		trace_util_0.Count(_admin_00000, 43)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 41)
	tblScanExec := e.constructTableScanPB(pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)

	limitExec := e.constructLimitPB(limitCnt)
	dagReq.Executors = append(dagReq.Executors, limitExec)

	return dagReq, nil
}

func (e *RecoverIndexExec) buildTableScan(ctx context.Context, txn kv.Transaction, t table.Table, startHandle int64, limitCnt uint64) (distsql.SelectResult, error) {
	trace_util_0.Count(_admin_00000, 44)
	dagPB, err := e.buildDAGPB(txn, limitCnt)
	if err != nil {
		trace_util_0.Count(_admin_00000, 48)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 45)
	tblInfo := e.table.Meta()
	ranges := []*ranger.Range{{LowVal: []types.Datum{types.NewIntDatum(startHandle)}, HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)}}}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(tblInfo.ID, ranges, nil).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		trace_util_0.Count(_admin_00000, 49)
		return nil, err
	}

	// Actually, with limitCnt, the match datas maybe only in one region, so let the concurrency to be 1,
	// avoid unnecessary region scan.
	trace_util_0.Count(_admin_00000, 46)
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.columnsTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		trace_util_0.Count(_admin_00000, 50)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 47)
	result.Fetch(ctx)
	return result, nil
}

type backfillResult struct {
	nextHandle   int64
	addedCount   int64
	scanRowCount int64
}

func (e *RecoverIndexExec) backfillIndex(ctx context.Context) (int64, int64, error) {
	trace_util_0.Count(_admin_00000, 51)
	var (
		nextHandle    = int64(math.MinInt64)
		totalAddedCnt = int64(0)
		totalScanCnt  = int64(0)
		lastLogCnt    = int64(0)
		result        backfillResult
	)
	for {
		trace_util_0.Count(_admin_00000, 53)
		errInTxn := kv.RunInNewTxn(e.ctx.GetStore(), true, func(txn kv.Transaction) error {
			trace_util_0.Count(_admin_00000, 58)
			var err error
			result, err = e.backfillIndexInTxn(ctx, txn, nextHandle)
			return err
		})
		trace_util_0.Count(_admin_00000, 54)
		if errInTxn != nil {
			trace_util_0.Count(_admin_00000, 59)
			return totalAddedCnt, totalScanCnt, errInTxn
		}
		trace_util_0.Count(_admin_00000, 55)
		totalAddedCnt += result.addedCount
		totalScanCnt += result.scanRowCount
		if totalScanCnt-lastLogCnt >= 50000 {
			trace_util_0.Count(_admin_00000, 60)
			lastLogCnt = totalScanCnt
			logutil.Logger(ctx).Info("recover index", zap.String("table", e.table.Meta().Name.O),
				zap.String("index", e.index.Meta().Name.O), zap.Int64("totalAddedCnt", totalAddedCnt),
				zap.Int64("totalScanCnt", totalScanCnt), zap.Int64("nextHandle", result.nextHandle))
		}

		// no more rows
		trace_util_0.Count(_admin_00000, 56)
		if result.scanRowCount == 0 {
			trace_util_0.Count(_admin_00000, 61)
			break
		}
		trace_util_0.Count(_admin_00000, 57)
		nextHandle = result.nextHandle
	}
	trace_util_0.Count(_admin_00000, 52)
	return totalAddedCnt, totalScanCnt, nil
}

type recoverRows struct {
	handle  int64
	idxVals []types.Datum
	skip    bool
}

func (e *RecoverIndexExec) fetchRecoverRows(ctx context.Context, srcResult distsql.SelectResult, result *backfillResult) ([]recoverRows, error) {
	trace_util_0.Count(_admin_00000, 62)
	e.recoverRows = e.recoverRows[:0]
	handleIdx := len(e.columns) - 1
	result.scanRowCount = 0

	for {
		trace_util_0.Count(_admin_00000, 64)
		err := srcResult.Next(ctx, e.srcChunk)
		if err != nil {
			trace_util_0.Count(_admin_00000, 67)
			return nil, err
		}

		trace_util_0.Count(_admin_00000, 65)
		if e.srcChunk.NumRows() == 0 {
			trace_util_0.Count(_admin_00000, 68)
			break
		}
		trace_util_0.Count(_admin_00000, 66)
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_admin_00000, 69)
			if result.scanRowCount >= int64(e.batchSize) {
				trace_util_0.Count(_admin_00000, 71)
				return e.recoverRows, nil
			}
			trace_util_0.Count(_admin_00000, 70)
			handle := row.GetInt64(handleIdx)
			idxVals := extractIdxVals(row, e.idxValsBufs[result.scanRowCount], e.colFieldTypes)
			e.idxValsBufs[result.scanRowCount] = idxVals
			e.recoverRows = append(e.recoverRows, recoverRows{handle: handle, idxVals: idxVals, skip: false})
			result.scanRowCount++
			result.nextHandle = handle + 1
		}
	}

	trace_util_0.Count(_admin_00000, 63)
	return e.recoverRows, nil
}

func (e *RecoverIndexExec) batchMarkDup(txn kv.Transaction, rows []recoverRows) error {
	trace_util_0.Count(_admin_00000, 72)
	if len(rows) == 0 {
		trace_util_0.Count(_admin_00000, 77)
		return nil
	}
	trace_util_0.Count(_admin_00000, 73)
	e.batchKeys = e.batchKeys[:0]
	sc := e.ctx.GetSessionVars().StmtCtx
	distinctFlags := make([]bool, len(rows))
	for i, row := range rows {
		trace_util_0.Count(_admin_00000, 78)
		idxKey, distinct, err := e.index.GenIndexKey(sc, row.idxVals, row.handle, e.idxKeyBufs[i])
		if err != nil {
			trace_util_0.Count(_admin_00000, 80)
			return err
		}
		trace_util_0.Count(_admin_00000, 79)
		e.idxKeyBufs[i] = idxKey

		e.batchKeys = append(e.batchKeys, idxKey)
		distinctFlags[i] = distinct
	}

	trace_util_0.Count(_admin_00000, 74)
	values, err := txn.BatchGet(e.batchKeys)
	if err != nil {
		trace_util_0.Count(_admin_00000, 81)
		return err
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it and skip it.
	// 3. non-unique-key is duplicate, skip it.
	trace_util_0.Count(_admin_00000, 75)
	for i, key := range e.batchKeys {
		trace_util_0.Count(_admin_00000, 82)
		if val, found := values[string(key)]; found {
			trace_util_0.Count(_admin_00000, 83)
			if distinctFlags[i] {
				trace_util_0.Count(_admin_00000, 85)
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					trace_util_0.Count(_admin_00000, 87)
					return err1
				}

				trace_util_0.Count(_admin_00000, 86)
				if handle != rows[i].handle {
					trace_util_0.Count(_admin_00000, 88)
					logutil.Logger(context.Background()).Warn("recover index: the constraint of unique index is broken, handle in index is not equal to handle in table",
						zap.String("index", e.index.Meta().Name.O), zap.ByteString("indexKey", key),
						zap.Int64("handleInTable", rows[i].handle), zap.Int64("handleInIndex", handle))
				}
			}
			trace_util_0.Count(_admin_00000, 84)
			rows[i].skip = true
		}
	}
	trace_util_0.Count(_admin_00000, 76)
	return nil
}

func (e *RecoverIndexExec) backfillIndexInTxn(ctx context.Context, txn kv.Transaction, startHandle int64) (result backfillResult, err error) {
	trace_util_0.Count(_admin_00000, 89)
	result.nextHandle = startHandle
	srcResult, err := e.buildTableScan(ctx, txn, e.table, startHandle, uint64(e.batchSize))
	if err != nil {
		trace_util_0.Count(_admin_00000, 94)
		return result, err
	}
	trace_util_0.Count(_admin_00000, 90)
	defer terror.Call(srcResult.Close)

	rows, err := e.fetchRecoverRows(ctx, srcResult, &result)
	if err != nil {
		trace_util_0.Count(_admin_00000, 95)
		return result, err
	}

	trace_util_0.Count(_admin_00000, 91)
	err = e.batchMarkDup(txn, rows)
	if err != nil {
		trace_util_0.Count(_admin_00000, 96)
		return result, err
	}

	// Constrains is already checked.
	trace_util_0.Count(_admin_00000, 92)
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	for _, row := range rows {
		trace_util_0.Count(_admin_00000, 97)
		if row.skip {
			trace_util_0.Count(_admin_00000, 101)
			continue
		}

		trace_util_0.Count(_admin_00000, 98)
		recordKey := e.table.RecordKey(row.handle)
		err := txn.LockKeys(ctx, 0, recordKey)
		if err != nil {
			trace_util_0.Count(_admin_00000, 102)
			return result, err
		}

		trace_util_0.Count(_admin_00000, 99)
		_, err = e.index.Create(e.ctx, txn, row.idxVals, row.handle)
		if err != nil {
			trace_util_0.Count(_admin_00000, 103)
			return result, err
		}
		trace_util_0.Count(_admin_00000, 100)
		result.addedCount++
	}
	trace_util_0.Count(_admin_00000, 93)
	return result, nil
}

// Next implements the Executor Next interface.
func (e *RecoverIndexExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_admin_00000, 104)
	req.Reset()
	if e.done {
		trace_util_0.Count(_admin_00000, 107)
		return nil
	}

	trace_util_0.Count(_admin_00000, 105)
	totalAddedCnt, totalScanCnt, err := e.backfillIndex(ctx)
	if err != nil {
		trace_util_0.Count(_admin_00000, 108)
		return err
	}

	trace_util_0.Count(_admin_00000, 106)
	req.AppendInt64(0, totalAddedCnt)
	req.AppendInt64(1, totalScanCnt)
	e.done = true
	return nil
}

// CleanupIndexExec represents a cleanup index executor.
// It is built from "admin cleanup index" statement, is used to delete
// dangling index data.
type CleanupIndexExec struct {
	baseExecutor

	done      bool
	removeCnt uint64

	index table.Index
	table table.Table

	idxCols          []*model.ColumnInfo
	idxColFieldTypes []*types.FieldType
	idxChunk         *chunk.Chunk

	idxValues   map[int64][][]types.Datum
	batchSize   uint64
	batchKeys   []kv.Key
	idxValsBufs [][]types.Datum
	lastIdxKey  []byte
	scanRowCnt  uint64
}

func (e *CleanupIndexExec) getIdxColTypes() []*types.FieldType {
	trace_util_0.Count(_admin_00000, 109)
	if e.idxColFieldTypes != nil {
		trace_util_0.Count(_admin_00000, 112)
		return e.idxColFieldTypes
	}
	trace_util_0.Count(_admin_00000, 110)
	e.idxColFieldTypes = make([]*types.FieldType, 0, len(e.idxCols))
	for _, col := range e.idxCols {
		trace_util_0.Count(_admin_00000, 113)
		e.idxColFieldTypes = append(e.idxColFieldTypes, &col.FieldType)
	}
	trace_util_0.Count(_admin_00000, 111)
	return e.idxColFieldTypes
}

func (e *CleanupIndexExec) batchGetRecord(txn kv.Transaction) (map[string][]byte, error) {
	trace_util_0.Count(_admin_00000, 114)
	for handle := range e.idxValues {
		trace_util_0.Count(_admin_00000, 117)
		e.batchKeys = append(e.batchKeys, e.table.RecordKey(handle))
	}
	trace_util_0.Count(_admin_00000, 115)
	values, err := txn.BatchGet(e.batchKeys)
	if err != nil {
		trace_util_0.Count(_admin_00000, 118)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 116)
	return values, nil
}

func (e *CleanupIndexExec) deleteDanglingIdx(txn kv.Transaction, values map[string][]byte) error {
	trace_util_0.Count(_admin_00000, 119)
	for _, k := range e.batchKeys {
		trace_util_0.Count(_admin_00000, 121)
		if _, found := values[string(k)]; !found {
			trace_util_0.Count(_admin_00000, 122)
			_, handle, err := tablecodec.DecodeRecordKey(k)
			if err != nil {
				trace_util_0.Count(_admin_00000, 124)
				return err
			}
			trace_util_0.Count(_admin_00000, 123)
			for _, idxVals := range e.idxValues[handle] {
				trace_util_0.Count(_admin_00000, 125)
				if err := e.index.Delete(e.ctx.GetSessionVars().StmtCtx, txn, idxVals, handle, nil); err != nil {
					trace_util_0.Count(_admin_00000, 127)
					return err
				}
				trace_util_0.Count(_admin_00000, 126)
				e.removeCnt++
				if e.removeCnt%e.batchSize == 0 {
					trace_util_0.Count(_admin_00000, 128)
					logutil.Logger(context.Background()).Info("clean up dangling index", zap.String("table", e.table.Meta().Name.String()),
						zap.String("index", e.index.Meta().Name.String()), zap.Uint64("count", e.removeCnt))
				}
			}
		}
	}
	trace_util_0.Count(_admin_00000, 120)
	return nil
}

func extractIdxVals(row chunk.Row, idxVals []types.Datum,
	fieldTypes []*types.FieldType) []types.Datum {
	trace_util_0.Count(_admin_00000, 129)
	if idxVals == nil {
		trace_util_0.Count(_admin_00000, 132)
		idxVals = make([]types.Datum, 0, row.Len()-1)
	} else {
		trace_util_0.Count(_admin_00000, 133)
		{
			idxVals = idxVals[:0]
		}
	}

	trace_util_0.Count(_admin_00000, 130)
	for i := 0; i < row.Len()-1; i++ {
		trace_util_0.Count(_admin_00000, 134)
		colVal := row.GetDatum(i, fieldTypes[i])
		idxVals = append(idxVals, *colVal.Copy())
	}
	trace_util_0.Count(_admin_00000, 131)
	return idxVals
}

func (e *CleanupIndexExec) fetchIndex(ctx context.Context, txn kv.Transaction) error {
	trace_util_0.Count(_admin_00000, 135)
	result, err := e.buildIndexScan(ctx, txn)
	if err != nil {
		trace_util_0.Count(_admin_00000, 137)
		return err
	}
	trace_util_0.Count(_admin_00000, 136)
	defer terror.Call(result.Close)

	sc := e.ctx.GetSessionVars().StmtCtx
	for {
		trace_util_0.Count(_admin_00000, 138)
		err := result.Next(ctx, e.idxChunk)
		if err != nil {
			trace_util_0.Count(_admin_00000, 141)
			return err
		}
		trace_util_0.Count(_admin_00000, 139)
		if e.idxChunk.NumRows() == 0 {
			trace_util_0.Count(_admin_00000, 142)
			return nil
		}
		trace_util_0.Count(_admin_00000, 140)
		iter := chunk.NewIterator4Chunk(e.idxChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_admin_00000, 143)
			handle := row.GetInt64(len(e.idxCols) - 1)
			idxVals := extractIdxVals(row, e.idxValsBufs[e.scanRowCnt], e.idxColFieldTypes)
			e.idxValsBufs[e.scanRowCnt] = idxVals
			e.idxValues[handle] = append(e.idxValues[handle], idxVals)
			idxKey, _, err := e.index.GenIndexKey(sc, idxVals, handle, nil)
			if err != nil {
				trace_util_0.Count(_admin_00000, 145)
				return err
			}
			trace_util_0.Count(_admin_00000, 144)
			e.scanRowCnt++
			e.lastIdxKey = idxKey
			if e.scanRowCnt >= e.batchSize {
				trace_util_0.Count(_admin_00000, 146)
				return nil
			}
		}
	}
}

// Next implements the Executor Next interface.
func (e *CleanupIndexExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_admin_00000, 147)
	req.Reset()
	if e.done {
		trace_util_0.Count(_admin_00000, 150)
		return nil
	}
	trace_util_0.Count(_admin_00000, 148)
	for {
		trace_util_0.Count(_admin_00000, 151)
		errInTxn := kv.RunInNewTxn(e.ctx.GetStore(), true, func(txn kv.Transaction) error {
			trace_util_0.Count(_admin_00000, 155)
			err := e.fetchIndex(ctx, txn)
			if err != nil {
				trace_util_0.Count(_admin_00000, 159)
				return err
			}
			trace_util_0.Count(_admin_00000, 156)
			values, err := e.batchGetRecord(txn)
			if err != nil {
				trace_util_0.Count(_admin_00000, 160)
				return err
			}
			trace_util_0.Count(_admin_00000, 157)
			err = e.deleteDanglingIdx(txn, values)
			if err != nil {
				trace_util_0.Count(_admin_00000, 161)
				return err
			}
			trace_util_0.Count(_admin_00000, 158)
			return nil
		})
		trace_util_0.Count(_admin_00000, 152)
		if errInTxn != nil {
			trace_util_0.Count(_admin_00000, 162)
			return errInTxn
		}
		trace_util_0.Count(_admin_00000, 153)
		if e.scanRowCnt == 0 {
			trace_util_0.Count(_admin_00000, 163)
			break
		}
		trace_util_0.Count(_admin_00000, 154)
		e.scanRowCnt = 0
		e.batchKeys = e.batchKeys[:0]
		for k := range e.idxValues {
			trace_util_0.Count(_admin_00000, 164)
			delete(e.idxValues, k)
		}
	}
	trace_util_0.Count(_admin_00000, 149)
	e.done = true
	req.AppendUint64(0, e.removeCnt)
	return nil
}

func (e *CleanupIndexExec) buildIndexScan(ctx context.Context, txn kv.Transaction) (distsql.SelectResult, error) {
	trace_util_0.Count(_admin_00000, 165)
	dagPB, err := e.buildIdxDAGPB(txn)
	if err != nil {
		trace_util_0.Count(_admin_00000, 169)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 166)
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder distsql.RequestBuilder
	ranges := ranger.FullRange()
	kvReq, err := builder.SetIndexRanges(sc, e.table.Meta().ID, e.index.Meta().ID, ranges).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		trace_util_0.Count(_admin_00000, 170)
		return nil, err
	}

	trace_util_0.Count(_admin_00000, 167)
	kvReq.KeyRanges[0].StartKey = kv.Key(e.lastIdxKey).PrefixNext()
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.getIdxColTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		trace_util_0.Count(_admin_00000, 171)
		return nil, err
	}
	trace_util_0.Count(_admin_00000, 168)
	result.Fetch(ctx)
	return result, nil
}

// Open implements the Executor Open interface.
func (e *CleanupIndexExec) Open(ctx context.Context) error {
	trace_util_0.Count(_admin_00000, 172)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_admin_00000, 175)
		return err
	}
	trace_util_0.Count(_admin_00000, 173)
	e.idxChunk = chunk.New(e.getIdxColTypes(), e.initCap, e.maxChunkSize)
	e.idxValues = make(map[int64][][]types.Datum, e.batchSize)
	e.batchKeys = make([]kv.Key, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	sc := e.ctx.GetSessionVars().StmtCtx
	idxKey, _, err := e.index.GenIndexKey(sc, []types.Datum{{}}, math.MinInt64, nil)
	if err != nil {
		trace_util_0.Count(_admin_00000, 176)
		return err
	}
	trace_util_0.Count(_admin_00000, 174)
	e.lastIdxKey = idxKey
	return nil
}

func (e *CleanupIndexExec) buildIdxDAGPB(txn kv.Transaction) (*tipb.DAGRequest, error) {
	trace_util_0.Count(_admin_00000, 177)
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = txn.StartTS()
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for i := range e.idxCols {
		trace_util_0.Count(_admin_00000, 180)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	trace_util_0.Count(_admin_00000, 178)
	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)
	err := plannercore.SetPBColumnsDefaultValue(e.ctx, dagReq.Executors[0].IdxScan.Columns, e.idxCols)
	if err != nil {
		trace_util_0.Count(_admin_00000, 181)
		return nil, err
	}

	trace_util_0.Count(_admin_00000, 179)
	limitExec := e.constructLimitPB()
	dagReq.Executors = append(dagReq.Executors, limitExec)

	return dagReq, nil
}

func (e *CleanupIndexExec) constructIndexScanPB() *tipb.Executor {
	trace_util_0.Count(_admin_00000, 182)
	idxExec := &tipb.IndexScan{
		TableId: e.table.Meta().ID,
		IndexId: e.index.Meta().ID,
		Columns: model.ColumnsToProto(e.idxCols, e.table.Meta().PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

func (e *CleanupIndexExec) constructLimitPB() *tipb.Executor {
	trace_util_0.Count(_admin_00000, 183)
	limitExec := &tipb.Limit{
		Limit: e.batchSize,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

// Close implements the Executor Close interface.
func (e *CleanupIndexExec) Close() error {
	trace_util_0.Count(_admin_00000, 184)
	return nil
}

var _admin_00000 = "executor/admin.go"
