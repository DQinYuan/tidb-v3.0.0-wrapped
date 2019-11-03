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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	trace_util_0.Count(_point_get_00000, 0)
	startTS, err := b.getStartTS()
	if err != nil {
		trace_util_0.Count(_point_get_00000, 2)
		b.err = err
		return nil
	}
	trace_util_0.Count(_point_get_00000, 1)
	e := &PointGetExecutor{
		baseExecutor: newBaseExecutor(b.ctx, p.Schema(), p.ExplainID()),
		tblInfo:      p.TblInfo,
		idxInfo:      p.IndexInfo,
		idxVals:      p.IndexValues,
		handle:       p.Handle,
		startTS:      startTS,
	}
	e.base().initCap = 1
	e.base().maxChunkSize = 1
	return e
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	baseExecutor

	tblInfo  *model.TableInfo
	handle   int64
	idxInfo  *model.IndexInfo
	idxVals  []types.Datum
	startTS  uint64
	snapshot kv.Snapshot
	done     bool
}

// Open implements the Executor interface.
func (e *PointGetExecutor) Open(context.Context) error {
	trace_util_0.Count(_point_get_00000, 3)
	return nil
}

// Close implements the Executor interface.
func (e *PointGetExecutor) Close() error {
	trace_util_0.Count(_point_get_00000, 4)
	return nil
}

// Next implements the Executor interface.
func (e *PointGetExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_point_get_00000, 5)
	req.Reset()
	if e.done {
		trace_util_0.Count(_point_get_00000, 11)
		return nil
	}
	trace_util_0.Count(_point_get_00000, 6)
	e.done = true
	var err error
	e.snapshot, err = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: e.startTS})
	if err != nil {
		trace_util_0.Count(_point_get_00000, 12)
		return err
	}
	trace_util_0.Count(_point_get_00000, 7)
	if e.idxInfo != nil {
		trace_util_0.Count(_point_get_00000, 13)
		idxKey, err1 := e.encodeIndexKey()
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			trace_util_0.Count(_point_get_00000, 18)
			return err1
		}

		trace_util_0.Count(_point_get_00000, 14)
		handleVal, err1 := e.get(idxKey)
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			trace_util_0.Count(_point_get_00000, 19)
			return err1
		}
		trace_util_0.Count(_point_get_00000, 15)
		if len(handleVal) == 0 {
			trace_util_0.Count(_point_get_00000, 20)
			return nil
		}
		trace_util_0.Count(_point_get_00000, 16)
		e.handle, err1 = tables.DecodeHandle(handleVal)
		if err1 != nil {
			trace_util_0.Count(_point_get_00000, 21)
			return err1
		}

		// The injection is used to simulate following scenario:
		// 1. Session A create a point get query but pause before second time `GET` kv from backend
		// 2. Session B create an UPDATE query to update the record that will be obtained in step 1
		// 3. Then point get retrieve data from backend after step 2 finished
		// 4. Check the result
		trace_util_0.Count(_point_get_00000, 17)
		failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step1", func() {
			trace_util_0.Count(_point_get_00000, 22)
			if ch, ok := ctx.Value("pointGetRepeatableReadTest").(chan struct{}); ok {
				trace_util_0.Count(_point_get_00000, 24)
				// Make `UPDATE` continue
				close(ch)
			}
			// Wait `UPDATE` finished
			trace_util_0.Count(_point_get_00000, 23)
			failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step2", nil)
		})
	}

	trace_util_0.Count(_point_get_00000, 8)
	key := tablecodec.EncodeRowKeyWithHandle(e.tblInfo.ID, e.handle)
	val, err := e.get(key)
	if err != nil && !kv.ErrNotExist.Equal(err) {
		trace_util_0.Count(_point_get_00000, 25)
		return err
	}
	trace_util_0.Count(_point_get_00000, 9)
	if len(val) == 0 {
		trace_util_0.Count(_point_get_00000, 26)
		if e.idxInfo != nil {
			trace_util_0.Count(_point_get_00000, 28)
			return kv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in table",
				e.idxInfo.Name.O, e.handle)
		}
		trace_util_0.Count(_point_get_00000, 27)
		return nil
	}
	trace_util_0.Count(_point_get_00000, 10)
	return e.decodeRowValToChunk(val, req.Chunk)
}

func (e *PointGetExecutor) encodeIndexKey() (_ []byte, err error) {
	trace_util_0.Count(_point_get_00000, 29)
	sc := e.ctx.GetSessionVars().StmtCtx
	for i := range e.idxVals {
		trace_util_0.Count(_point_get_00000, 32)
		colInfo := e.tblInfo.Columns[e.idxInfo.Columns[i].Offset]
		if colInfo.Tp == mysql.TypeString || colInfo.Tp == mysql.TypeVarString || colInfo.Tp == mysql.TypeVarchar {
			trace_util_0.Count(_point_get_00000, 34)
			e.idxVals[i], err = ranger.HandlePadCharToFullLength(sc, &colInfo.FieldType, e.idxVals[i])
		} else {
			trace_util_0.Count(_point_get_00000, 35)
			{
				e.idxVals[i], err = table.CastValue(e.ctx, e.idxVals[i], colInfo)
			}
		}
		trace_util_0.Count(_point_get_00000, 33)
		if err != nil {
			trace_util_0.Count(_point_get_00000, 36)
			return nil, err
		}
	}

	trace_util_0.Count(_point_get_00000, 30)
	encodedIdxVals, err := codec.EncodeKey(sc, nil, e.idxVals...)
	if err != nil {
		trace_util_0.Count(_point_get_00000, 37)
		return nil, err
	}
	trace_util_0.Count(_point_get_00000, 31)
	return tablecodec.EncodeIndexSeekKey(e.tblInfo.ID, e.idxInfo.ID, encodedIdxVals), nil
}

func (e *PointGetExecutor) get(key kv.Key) (val []byte, err error) {
	trace_util_0.Count(_point_get_00000, 38)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_point_get_00000, 41)
		return nil, err
	}
	trace_util_0.Count(_point_get_00000, 39)
	if txn != nil && txn.Valid() && !txn.IsReadOnly() {
		trace_util_0.Count(_point_get_00000, 42)
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot may be
		// different for pessimistic transaction.
		val, err = txn.GetMemBuffer().Get(key)
		if err == nil {
			trace_util_0.Count(_point_get_00000, 44)
			return val, err
		}
		trace_util_0.Count(_point_get_00000, 43)
		if !kv.IsErrNotFound(err) {
			trace_util_0.Count(_point_get_00000, 45)
			return nil, err
		}
		// fallthrough to snapshot get.
	}
	trace_util_0.Count(_point_get_00000, 40)
	return e.snapshot.Get(key)
}

func (e *PointGetExecutor) decodeRowValToChunk(rowVal []byte, chk *chunk.Chunk) error {
	trace_util_0.Count(_point_get_00000, 46)
	//  One column could be filled for multi-times in the schema. e.g. select b, b, c, c from t where a = 1.
	// We need to set the positions in the schema for the same column.
	colID2DecodedPos := make(map[int64]int, e.schema.Len())
	decodedPos2SchemaPos := make([][]int, 0, e.schema.Len())
	for schemaPos, col := range e.schema.Columns {
		trace_util_0.Count(_point_get_00000, 51)
		if decodedPos, ok := colID2DecodedPos[col.ID]; !ok {
			trace_util_0.Count(_point_get_00000, 52)
			colID2DecodedPos[col.ID] = len(colID2DecodedPos)
			decodedPos2SchemaPos = append(decodedPos2SchemaPos, []int{schemaPos})
		} else {
			trace_util_0.Count(_point_get_00000, 53)
			{
				decodedPos2SchemaPos[decodedPos] = append(decodedPos2SchemaPos[decodedPos], schemaPos)
			}
		}
	}
	trace_util_0.Count(_point_get_00000, 47)
	decodedVals, err := tablecodec.CutRowNew(rowVal, colID2DecodedPos)
	if err != nil {
		trace_util_0.Count(_point_get_00000, 54)
		return err
	}
	trace_util_0.Count(_point_get_00000, 48)
	if decodedVals == nil {
		trace_util_0.Count(_point_get_00000, 55)
		decodedVals = make([][]byte, len(colID2DecodedPos))
	}
	trace_util_0.Count(_point_get_00000, 49)
	decoder := codec.NewDecoder(chk, e.ctx.GetSessionVars().Location())
	for id, decodedPos := range colID2DecodedPos {
		trace_util_0.Count(_point_get_00000, 56)
		schemaPoses := decodedPos2SchemaPos[decodedPos]
		firstPos := schemaPoses[0]
		if e.tblInfo.PKIsHandle && mysql.HasPriKeyFlag(e.schema.Columns[firstPos].RetType.Flag) {
			trace_util_0.Count(_point_get_00000, 61)
			chk.AppendInt64(firstPos, e.handle)
			// Fill other positions.
			for i := 1; i < len(schemaPoses); i++ {
				trace_util_0.Count(_point_get_00000, 63)
				chk.MakeRef(firstPos, schemaPoses[i])
			}
			trace_util_0.Count(_point_get_00000, 62)
			continue
		}
		// ExtraHandleID is added when building plan, we can make sure that there's only one column's ID is this.
		trace_util_0.Count(_point_get_00000, 57)
		if id == model.ExtraHandleID {
			trace_util_0.Count(_point_get_00000, 64)
			chk.AppendInt64(firstPos, e.handle)
			continue
		}
		trace_util_0.Count(_point_get_00000, 58)
		if len(decodedVals[decodedPos]) == 0 {
			trace_util_0.Count(_point_get_00000, 65)
			// This branch only entered for updating and deleting. It won't have one column in multiple positions.
			colInfo := getColInfoByID(e.tblInfo, id)
			d, err1 := table.GetColOriginDefaultValue(e.ctx, colInfo)
			if err1 != nil {
				trace_util_0.Count(_point_get_00000, 67)
				return err1
			}
			trace_util_0.Count(_point_get_00000, 66)
			chk.AppendDatum(firstPos, &d)
			continue
		}
		trace_util_0.Count(_point_get_00000, 59)
		_, err = decoder.DecodeOne(decodedVals[decodedPos], firstPos, e.schema.Columns[firstPos].RetType)
		if err != nil {
			trace_util_0.Count(_point_get_00000, 68)
			return err
		}
		// Fill other positions.
		trace_util_0.Count(_point_get_00000, 60)
		for i := 1; i < len(schemaPoses); i++ {
			trace_util_0.Count(_point_get_00000, 69)
			chk.MakeRef(firstPos, schemaPoses[i])
		}
	}
	trace_util_0.Count(_point_get_00000, 50)
	return nil
}

func getColInfoByID(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
	trace_util_0.Count(_point_get_00000, 70)
	for _, col := range tbl.Columns {
		trace_util_0.Count(_point_get_00000, 72)
		if col.ID == colID {
			trace_util_0.Count(_point_get_00000, 73)
			return col
		}
	}
	trace_util_0.Count(_point_get_00000, 71)
	return nil
}

var _point_get_00000 = "executor/point_get.go"
