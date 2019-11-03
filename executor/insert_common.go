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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertValues is the data to insert.
type InsertValues struct {
	baseExecutor
	batchChecker

	rowCount       uint64
	maxRowsInBatch uint64
	lastInsertID   uint64
	hasRefCols     bool
	hasExtraHandle bool

	SelectExec Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]expression.Expression
	SetList []*expression.Assignment

	GenColumns []*ast.ColumnName
	GenExprs   []expression.Expression

	insertColumns []*table.Column

	// colDefaultVals is used to store casted default value.
	// Because not every insert statement needs colDefaultVals, so we will init the buffer lazily.
	colDefaultVals  []defaultVal
	evalBuffer      chunk.MutRow
	evalBufferTypes []*types.FieldType
}

type defaultVal struct {
	val types.Datum
	// valid indicates whether the val is evaluated. We evaluate the default value lazily.
	valid bool
}

// initInsertColumns sets the explicitly specified columns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) initInsertColumns() error {
	trace_util_0.Count(_insert_common_00000, 0)
	var cols []*table.Column
	var err error

	tableCols := e.Table.Cols()

	if len(e.SetList) > 0 {
		trace_util_0.Count(_insert_common_00000, 4)
		// Process `set` type column.
		columns := make([]string, 0, len(e.SetList))
		for _, v := range e.SetList {
			trace_util_0.Count(_insert_common_00000, 8)
			columns = append(columns, v.Col.ColName.O)
		}
		trace_util_0.Count(_insert_common_00000, 5)
		for _, v := range e.GenColumns {
			trace_util_0.Count(_insert_common_00000, 9)
			columns = append(columns, v.Name.O)
		}
		trace_util_0.Count(_insert_common_00000, 6)
		cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 10)
			return errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}
		trace_util_0.Count(_insert_common_00000, 7)
		if len(cols) == 0 {
			trace_util_0.Count(_insert_common_00000, 11)
			return errors.Errorf("INSERT INTO %s: empty column", e.Table.Meta().Name.O)
		}
	} else {
		trace_util_0.Count(_insert_common_00000, 12)
		if len(e.Columns) > 0 {
			trace_util_0.Count(_insert_common_00000, 13)
			// Process `name` type column.
			columns := make([]string, 0, len(e.Columns))
			for _, v := range e.Columns {
				trace_util_0.Count(_insert_common_00000, 16)
				columns = append(columns, v.Name.O)
			}
			trace_util_0.Count(_insert_common_00000, 14)
			for _, v := range e.GenColumns {
				trace_util_0.Count(_insert_common_00000, 17)
				columns = append(columns, v.Name.O)
			}
			trace_util_0.Count(_insert_common_00000, 15)
			cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
			if err != nil {
				trace_util_0.Count(_insert_common_00000, 18)
				return errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
			}
		} else {
			trace_util_0.Count(_insert_common_00000, 19)
			{
				// If e.Columns are empty, use all columns instead.
				cols = tableCols
			}
		}
	}
	trace_util_0.Count(_insert_common_00000, 1)
	for _, col := range cols {
		trace_util_0.Count(_insert_common_00000, 20)
		if col.Name.L == model.ExtraHandleName.L {
			trace_util_0.Count(_insert_common_00000, 21)
			if !e.ctx.GetSessionVars().AllowWriteRowID {
				trace_util_0.Count(_insert_common_00000, 23)
				return errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported.")
			}
			trace_util_0.Count(_insert_common_00000, 22)
			e.hasExtraHandle = true
			break
		}
	}

	// Check column whether is specified only once.
	trace_util_0.Count(_insert_common_00000, 2)
	err = table.CheckOnce(cols)
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 24)
		return err
	}
	trace_util_0.Count(_insert_common_00000, 3)
	e.insertColumns = cols
	return nil
}

func (e *InsertValues) initEvalBuffer() {
	trace_util_0.Count(_insert_common_00000, 25)
	numCols := len(e.Table.Cols())
	if e.hasExtraHandle {
		trace_util_0.Count(_insert_common_00000, 29)
		numCols++
	}
	trace_util_0.Count(_insert_common_00000, 26)
	e.evalBufferTypes = make([]*types.FieldType, numCols)
	for i, col := range e.Table.Cols() {
		trace_util_0.Count(_insert_common_00000, 30)
		e.evalBufferTypes[i] = &col.FieldType
	}
	trace_util_0.Count(_insert_common_00000, 27)
	if e.hasExtraHandle {
		trace_util_0.Count(_insert_common_00000, 31)
		e.evalBufferTypes[len(e.evalBufferTypes)-1] = types.NewFieldType(mysql.TypeLonglong)
	}
	trace_util_0.Count(_insert_common_00000, 28)
	e.evalBuffer = chunk.MutRowFromTypes(e.evalBufferTypes)
}

func (e *InsertValues) lazilyInitColDefaultValBuf() (ok bool) {
	trace_util_0.Count(_insert_common_00000, 32)
	if e.colDefaultVals != nil {
		trace_util_0.Count(_insert_common_00000, 35)
		return true
	}

	// only if values count of insert statement is more than one, use colDefaultVals to store
	// casted default values has benefits.
	trace_util_0.Count(_insert_common_00000, 33)
	if len(e.Lists) > 1 {
		trace_util_0.Count(_insert_common_00000, 36)
		e.colDefaultVals = make([]defaultVal, len(e.Table.Cols()))
		return true
	}

	trace_util_0.Count(_insert_common_00000, 34)
	return false
}

func (e *InsertValues) processSetList() error {
	trace_util_0.Count(_insert_common_00000, 37)
	if len(e.SetList) > 0 {
		trace_util_0.Count(_insert_common_00000, 39)
		if len(e.Lists) > 0 {
			trace_util_0.Count(_insert_common_00000, 42)
			return errors.Errorf("INSERT INTO %s: set type should not use values", e.Table)
		}
		trace_util_0.Count(_insert_common_00000, 40)
		l := make([]expression.Expression, 0, len(e.SetList))
		for _, v := range e.SetList {
			trace_util_0.Count(_insert_common_00000, 43)
			l = append(l, v.Expr)
		}
		trace_util_0.Count(_insert_common_00000, 41)
		e.Lists = append(e.Lists, l)
	}
	trace_util_0.Count(_insert_common_00000, 38)
	return nil
}

// insertRows processes `insert|replace into values ()` or `insert|replace into set x=y`
func (e *InsertValues) insertRows(ctx context.Context, exec func(ctx context.Context, rows [][]types.Datum) error) (err error) {
	trace_util_0.Count(_insert_common_00000, 44)
	// For `insert|replace into set x=y`, process the set list here.
	if err = e.processSetList(); err != nil {
		trace_util_0.Count(_insert_common_00000, 47)
		return err
	}
	trace_util_0.Count(_insert_common_00000, 45)
	sessVars := e.ctx.GetSessionVars()
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize

	rows := make([][]types.Datum, 0, len(e.Lists))
	for i, list := range e.Lists {
		trace_util_0.Count(_insert_common_00000, 48)
		e.rowCount++
		row, err := e.evalRow(list, i)
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 50)
			return err
		}
		trace_util_0.Count(_insert_common_00000, 49)
		rows = append(rows, row)
		if batchInsert && e.rowCount%uint64(batchSize) == 0 {
			trace_util_0.Count(_insert_common_00000, 51)
			if err = exec(ctx, rows); err != nil {
				trace_util_0.Count(_insert_common_00000, 53)
				return err
			}
			trace_util_0.Count(_insert_common_00000, 52)
			rows = rows[:0]
			if err = e.doBatchInsert(ctx); err != nil {
				trace_util_0.Count(_insert_common_00000, 54)
				return err
			}
		}
	}
	trace_util_0.Count(_insert_common_00000, 46)
	return exec(ctx, rows)
}

func (e *InsertValues) handleErr(col *table.Column, val *types.Datum, rowIdx int, err error) error {
	trace_util_0.Count(_insert_common_00000, 55)
	if err == nil {
		trace_util_0.Count(_insert_common_00000, 60)
		return nil
	}

	trace_util_0.Count(_insert_common_00000, 56)
	if types.ErrDataTooLong.Equal(err) {
		trace_util_0.Count(_insert_common_00000, 61)
		return resetErrDataTooLong(col.Name.O, rowIdx+1, err)
	}

	trace_util_0.Count(_insert_common_00000, 57)
	if types.ErrOverflow.Equal(err) {
		trace_util_0.Count(_insert_common_00000, 62)
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(col.Name.O, rowIdx+1)
	}
	trace_util_0.Count(_insert_common_00000, 58)
	if types.ErrTruncated.Equal(err) {
		trace_util_0.Count(_insert_common_00000, 63)
		valStr, err1 := val.ToString()
		if err1 != nil {
			trace_util_0.Count(_insert_common_00000, 65)
			logutil.Logger(context.Background()).Warn("truncate error", zap.Error(err1))
		}
		trace_util_0.Count(_insert_common_00000, 64)
		return table.ErrTruncatedWrongValueForField.GenWithStackByArgs(types.TypeStr(col.Tp), valStr, col.Name.O, rowIdx+1)
	}
	trace_util_0.Count(_insert_common_00000, 59)
	return e.filterErr(err)
}

// evalRow evaluates a to-be-inserted row. The value of the column may base on another column,
// so we use setValueForRefColumn to fill the empty row some default values when needFillDefaultValues is true.
func (e *InsertValues) evalRow(list []expression.Expression, rowIdx int) ([]types.Datum, error) {
	trace_util_0.Count(_insert_common_00000, 66)
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		trace_util_0.Count(_insert_common_00000, 70)
		rowLen++
	}
	trace_util_0.Count(_insert_common_00000, 67)
	row := make([]types.Datum, rowLen)
	hasValue := make([]bool, rowLen)

	// For statements like `insert into t set a = b + 1`.
	if e.hasRefCols {
		trace_util_0.Count(_insert_common_00000, 71)
		if err := e.setValueForRefColumn(row, hasValue); err != nil {
			trace_util_0.Count(_insert_common_00000, 72)
			return nil, err
		}
	}

	trace_util_0.Count(_insert_common_00000, 68)
	e.evalBuffer.SetDatums(row...)
	for i, expr := range list {
		trace_util_0.Count(_insert_common_00000, 73)
		val, err := expr.Eval(e.evalBuffer.ToRow())
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			trace_util_0.Count(_insert_common_00000, 76)
			return nil, err
		}
		trace_util_0.Count(_insert_common_00000, 74)
		val1, err := table.CastValue(e.ctx, val, e.insertColumns[i].ToInfo())
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			trace_util_0.Count(_insert_common_00000, 77)
			return nil, err
		}

		trace_util_0.Count(_insert_common_00000, 75)
		offset := e.insertColumns[i].Offset
		row[offset], hasValue[offset] = *val1.Copy(), true
		e.evalBuffer.SetDatum(offset, val1)
	}

	trace_util_0.Count(_insert_common_00000, 69)
	return e.fillRow(row, hasValue)
}

// setValueForRefColumn set some default values for the row to eval the row value with other columns,
// it follows these rules:
//     1. for nullable and no default value column, use NULL.
//     2. for nullable and have default value column, use it's default value.
//     3. for not null column, use zero value even in strict mode.
//     4. for auto_increment column, use zero value.
//     5. for generated column, use NULL.
func (e *InsertValues) setValueForRefColumn(row []types.Datum, hasValue []bool) error {
	trace_util_0.Count(_insert_common_00000, 78)
	for i, c := range e.Table.Cols() {
		trace_util_0.Count(_insert_common_00000, 80)
		d, err := e.getColDefaultValue(i, c)
		if err == nil {
			trace_util_0.Count(_insert_common_00000, 81)
			row[i] = d
			if !mysql.HasAutoIncrementFlag(c.Flag) {
				trace_util_0.Count(_insert_common_00000, 82)
				// It is an interesting behavior in MySQL.
				// If the value of auto ID is not explicit, MySQL use 0 value for auto ID when it is
				// evaluated by another column, but it should be used once only.
				// When we fill it as an auto ID column, it should be set as it used to be.
				// So just keep `hasValue` false for auto ID, and the others set true.
				hasValue[c.Offset] = true
			}
		} else {
			trace_util_0.Count(_insert_common_00000, 83)
			if table.ErrNoDefaultValue.Equal(err) {
				trace_util_0.Count(_insert_common_00000, 84)
				row[i] = table.GetZeroValue(c.ToInfo())
				hasValue[c.Offset] = false
			} else {
				trace_util_0.Count(_insert_common_00000, 85)
				if e.filterErr(err) != nil {
					trace_util_0.Count(_insert_common_00000, 86)
					return err
				}
			}
		}
	}
	trace_util_0.Count(_insert_common_00000, 79)
	return nil
}

func (e *InsertValues) insertRowsFromSelect(ctx context.Context, exec func(ctx context.Context, rows [][]types.Datum) error) error {
	trace_util_0.Count(_insert_common_00000, 87)
	// process `insert|replace into ... select ... from ...`
	selectExec := e.children[0]
	fields := retTypes(selectExec)
	chk := newFirstChunk(selectExec)
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Datum, 0, chk.Capacity())

	sessVars := e.ctx.GetSessionVars()
	if !sessVars.StrictSQLMode {
		trace_util_0.Count(_insert_common_00000, 90)
		// If StrictSQLMode is disabled and it is a insert-select statement, it also handle BadNullAsWarning.
		sessVars.StmtCtx.BadNullAsWarning = true
	}
	trace_util_0.Count(_insert_common_00000, 88)
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize

	for {
		trace_util_0.Count(_insert_common_00000, 91)
		err := selectExec.Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 94)
			return err
		}
		trace_util_0.Count(_insert_common_00000, 92)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_insert_common_00000, 95)
			break
		}

		trace_util_0.Count(_insert_common_00000, 93)
		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			trace_util_0.Count(_insert_common_00000, 96)
			innerRow := types.CloneRow(innerChunkRow.GetDatumRow(fields))
			e.rowCount++
			row, err := e.getRow(innerRow)
			if err != nil {
				trace_util_0.Count(_insert_common_00000, 98)
				return err
			}
			trace_util_0.Count(_insert_common_00000, 97)
			rows = append(rows, row)
			if batchInsert && e.rowCount%uint64(batchSize) == 0 {
				trace_util_0.Count(_insert_common_00000, 99)
				if err = exec(ctx, rows); err != nil {
					trace_util_0.Count(_insert_common_00000, 101)
					return err
				}
				trace_util_0.Count(_insert_common_00000, 100)
				rows = rows[:0]
				if err = e.doBatchInsert(ctx); err != nil {
					trace_util_0.Count(_insert_common_00000, 102)
					return err
				}
			}
		}
	}
	trace_util_0.Count(_insert_common_00000, 89)
	return exec(ctx, rows)
}

func (e *InsertValues) doBatchInsert(ctx context.Context) error {
	trace_util_0.Count(_insert_common_00000, 103)
	sessVars := e.ctx.GetSessionVars()
	if err := e.ctx.StmtCommit(); err != nil {
		trace_util_0.Count(_insert_common_00000, 107)
		return err
	}
	trace_util_0.Count(_insert_common_00000, 104)
	if err := e.ctx.NewTxn(ctx); err != nil {
		trace_util_0.Count(_insert_common_00000, 108)
		// We should return a special error for batch insert.
		return ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	trace_util_0.Count(_insert_common_00000, 105)
	if !sessVars.LightningMode {
		trace_util_0.Count(_insert_common_00000, 109)
		txn, err := e.ctx.Txn(true)
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 111)
			return err
		}
		trace_util_0.Count(_insert_common_00000, 110)
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	}
	trace_util_0.Count(_insert_common_00000, 106)
	return nil
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getRow(vals []types.Datum) ([]types.Datum, error) {
	trace_util_0.Count(_insert_common_00000, 112)
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		trace_util_0.Count(_insert_common_00000, 114)
		casted, err := table.CastValue(e.ctx, v, e.insertColumns[i].ToInfo())
		if e.filterErr(err) != nil {
			trace_util_0.Count(_insert_common_00000, 116)
			return nil, err
		}

		trace_util_0.Count(_insert_common_00000, 115)
		offset := e.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	trace_util_0.Count(_insert_common_00000, 113)
	return e.fillRow(row, hasValue)
}

func (e *InsertValues) filterErr(err error) error {
	trace_util_0.Count(_insert_common_00000, 117)
	if err == nil {
		trace_util_0.Count(_insert_common_00000, 120)
		return nil
	}
	trace_util_0.Count(_insert_common_00000, 118)
	if !e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning {
		trace_util_0.Count(_insert_common_00000, 121)
		return err
	}
	// TODO: should not filter all types of errors here.
	trace_util_0.Count(_insert_common_00000, 119)
	e.handleWarning(err)
	return nil
}

// getColDefaultValue gets the column default value.
func (e *InsertValues) getColDefaultValue(idx int, col *table.Column) (d types.Datum, err error) {
	trace_util_0.Count(_insert_common_00000, 122)
	if e.colDefaultVals != nil && e.colDefaultVals[idx].valid {
		trace_util_0.Count(_insert_common_00000, 126)
		return e.colDefaultVals[idx].val, nil
	}

	trace_util_0.Count(_insert_common_00000, 123)
	defaultVal, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 127)
		return types.Datum{}, err
	}
	trace_util_0.Count(_insert_common_00000, 124)
	if initialized := e.lazilyInitColDefaultValBuf(); initialized {
		trace_util_0.Count(_insert_common_00000, 128)
		e.colDefaultVals[idx].val = defaultVal
		e.colDefaultVals[idx].valid = true
	}

	trace_util_0.Count(_insert_common_00000, 125)
	return defaultVal, nil
}

// fillColValue fills the column value if it is not set in the insert statement.
func (e *InsertValues) fillColValue(datum types.Datum, idx int, column *table.Column, hasValue bool) (types.Datum,
	error) {
	trace_util_0.Count(_insert_common_00000, 129)
	if mysql.HasAutoIncrementFlag(column.Flag) {
		trace_util_0.Count(_insert_common_00000, 132)
		d, err := e.adjustAutoIncrementDatum(datum, hasValue, column)
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 134)
			return types.Datum{}, err
		}
		trace_util_0.Count(_insert_common_00000, 133)
		return d, nil
	}
	trace_util_0.Count(_insert_common_00000, 130)
	if !hasValue {
		trace_util_0.Count(_insert_common_00000, 135)
		d, err := e.getColDefaultValue(idx, column)
		if e.filterErr(err) != nil {
			trace_util_0.Count(_insert_common_00000, 137)
			return types.Datum{}, err
		}
		trace_util_0.Count(_insert_common_00000, 136)
		return d, nil
	}
	trace_util_0.Count(_insert_common_00000, 131)
	return datum, nil
}

// fillRow fills generated columns, auto_increment column and empty column.
// For NOT NULL column, it will return error or use zero value based on sql_mode.
func (e *InsertValues) fillRow(row []types.Datum, hasValue []bool) ([]types.Datum, error) {
	trace_util_0.Count(_insert_common_00000, 138)
	gIdx := 0
	for i, c := range e.Table.Cols() {
		trace_util_0.Count(_insert_common_00000, 140)
		var err error
		// Get the default value for all no value columns, the auto increment column is different from the others.
		row[i], err = e.fillColValue(row[i], i, c, hasValue[i])
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 143)
			return nil, err
		}

		// Evaluate the generated columns.
		trace_util_0.Count(_insert_common_00000, 141)
		if c.IsGenerated() {
			trace_util_0.Count(_insert_common_00000, 144)
			var val types.Datum
			val, err = e.GenExprs[gIdx].Eval(chunk.MutRowFromDatums(row).ToRow())
			gIdx++
			if e.filterErr(err) != nil {
				trace_util_0.Count(_insert_common_00000, 146)
				return nil, err
			}
			trace_util_0.Count(_insert_common_00000, 145)
			row[i], err = table.CastValue(e.ctx, val, c.ToInfo())
			if err != nil {
				trace_util_0.Count(_insert_common_00000, 147)
				return nil, err
			}
		}

		// Handle the bad null error.
		trace_util_0.Count(_insert_common_00000, 142)
		if row[i], err = c.HandleBadNull(row[i], e.ctx.GetSessionVars().StmtCtx); err != nil {
			trace_util_0.Count(_insert_common_00000, 148)
			return nil, err
		}
	}
	trace_util_0.Count(_insert_common_00000, 139)
	return row, nil
}

func (e *InsertValues) adjustAutoIncrementDatum(d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	trace_util_0.Count(_insert_common_00000, 149)
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		trace_util_0.Count(_insert_common_00000, 156)
		id, err := retryInfo.GetCurrAutoIncrementID()
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 158)
			return types.Datum{}, err
		}
		trace_util_0.Count(_insert_common_00000, 157)
		d.SetAutoID(id, c.Flag)
		return d, nil
	}

	trace_util_0.Count(_insert_common_00000, 150)
	var err error
	var recordID int64
	if !hasValue {
		trace_util_0.Count(_insert_common_00000, 159)
		d.SetNull()
	}
	trace_util_0.Count(_insert_common_00000, 151)
	if !d.IsNull() {
		trace_util_0.Count(_insert_common_00000, 160)
		sc := e.ctx.GetSessionVars().StmtCtx
		datum, err1 := d.ConvertTo(sc, &c.FieldType)
		if e.filterErr(err1) != nil {
			trace_util_0.Count(_insert_common_00000, 162)
			return types.Datum{}, err1
		}
		trace_util_0.Count(_insert_common_00000, 161)
		recordID = datum.GetInt64()
	}
	// Use the value if it's not null and not 0.
	trace_util_0.Count(_insert_common_00000, 152)
	if recordID != 0 {
		trace_util_0.Count(_insert_common_00000, 163)
		err = e.Table.RebaseAutoID(e.ctx, recordID, true)
		if err != nil {
			trace_util_0.Count(_insert_common_00000, 165)
			return types.Datum{}, err
		}
		trace_util_0.Count(_insert_common_00000, 164)
		e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		d.SetAutoID(recordID, c.Flag)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	trace_util_0.Count(_insert_common_00000, 153)
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		trace_util_0.Count(_insert_common_00000, 166)
		recordID, err = e.Table.AllocAutoIncrementValue(e.ctx)
		if e.filterErr(err) != nil {
			trace_util_0.Count(_insert_common_00000, 168)
			return types.Datum{}, err
		}
		// It's compatible with mysql. So it sets last insert id to the first row.
		trace_util_0.Count(_insert_common_00000, 167)
		if e.rowCount == 1 {
			trace_util_0.Count(_insert_common_00000, 169)
			e.lastInsertID = uint64(recordID)
		}
	}

	trace_util_0.Count(_insert_common_00000, 154)
	d.SetAutoID(recordID, c.Flag)
	retryInfo.AddAutoIncrementID(recordID)

	// the value of d is adjusted by auto ID, so we need to cast it again.
	casted, err := table.CastValue(e.ctx, d, c.ToInfo())
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 170)
		return types.Datum{}, err
	}
	trace_util_0.Count(_insert_common_00000, 155)
	return casted, nil
}

func (e *InsertValues) handleWarning(err error) {
	trace_util_0.Count(_insert_common_00000, 171)
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(rows [][]types.Datum, addRecord func(row []types.Datum) (int64, error)) error {
	trace_util_0.Count(_insert_common_00000, 172)
	// all the rows will be checked, so it is safe to set BatchCheck = true
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	err := e.batchGetInsertKeys(e.ctx, e.Table, rows)
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 175)
		return err
	}
	// append warnings and get no duplicated error rows
	trace_util_0.Count(_insert_common_00000, 173)
	for i, r := range e.toBeCheckedRows {
		trace_util_0.Count(_insert_common_00000, 176)
		if r.handleKey != nil {
			trace_util_0.Count(_insert_common_00000, 179)
			if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
				trace_util_0.Count(_insert_common_00000, 180)
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
				continue
			}
		}
		trace_util_0.Count(_insert_common_00000, 177)
		for _, uk := range r.uniqueKeys {
			trace_util_0.Count(_insert_common_00000, 181)
			if _, found := e.dupKVs[string(uk.newKV.key)]; found {
				trace_util_0.Count(_insert_common_00000, 182)
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
				break
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		trace_util_0.Count(_insert_common_00000, 178)
		if rows[i] != nil {
			trace_util_0.Count(_insert_common_00000, 183)
			e.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
			_, err = addRecord(rows[i])
			if err != nil {
				trace_util_0.Count(_insert_common_00000, 186)
				return err
			}
			trace_util_0.Count(_insert_common_00000, 184)
			if r.handleKey != nil {
				trace_util_0.Count(_insert_common_00000, 187)
				e.dupKVs[string(r.handleKey.newKV.key)] = r.handleKey.newKV.value
			}
			trace_util_0.Count(_insert_common_00000, 185)
			for _, uk := range r.uniqueKeys {
				trace_util_0.Count(_insert_common_00000, 188)
				e.dupKVs[string(uk.newKV.key)] = []byte{}
			}
		}
	}
	trace_util_0.Count(_insert_common_00000, 174)
	return nil
}

func (e *InsertValues) addRecord(row []types.Datum) (int64, error) {
	trace_util_0.Count(_insert_common_00000, 189)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 194)
		return 0, err
	}
	trace_util_0.Count(_insert_common_00000, 190)
	if !e.ctx.GetSessionVars().ConstraintCheckInPlace {
		trace_util_0.Count(_insert_common_00000, 195)
		txn.SetOption(kv.PresumeKeyNotExists, nil)
	}
	trace_util_0.Count(_insert_common_00000, 191)
	h, err := e.Table.AddRecord(e.ctx, row)
	txn.DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		trace_util_0.Count(_insert_common_00000, 196)
		return 0, err
	}
	trace_util_0.Count(_insert_common_00000, 192)
	if e.lastInsertID != 0 {
		trace_util_0.Count(_insert_common_00000, 197)
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	trace_util_0.Count(_insert_common_00000, 193)
	return h, nil
}

var _insert_common_00000 = "executor/insert_common.go"
