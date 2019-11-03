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

package expression

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// CorrelatedColumn stands for a column in a correlated sub query.
type CorrelatedColumn struct {
	Column

	Data *types.Datum
}

// Clone implements Expression interface.
func (col *CorrelatedColumn) Clone() Expression {
	trace_util_0.Count(_column_00000, 0)
	return col
}

// Eval implements Expression interface.
func (col *CorrelatedColumn) Eval(row chunk.Row) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 1)
	return *col.Data, nil
}

// EvalInt returns int representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_column_00000, 2)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 5)
		return 0, true, nil
	}
	trace_util_0.Count(_column_00000, 3)
	if col.GetType().Hybrid() {
		trace_util_0.Count(_column_00000, 6)
		res, err := col.Data.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	trace_util_0.Count(_column_00000, 4)
	return col.Data.GetInt64(), false, nil
}

// EvalReal returns real representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_column_00000, 7)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 9)
		return 0, true, nil
	}
	trace_util_0.Count(_column_00000, 8)
	return col.Data.GetFloat64(), false, nil
}

// EvalString returns string representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_column_00000, 10)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 13)
		return "", true, nil
	}
	trace_util_0.Count(_column_00000, 11)
	res, err := col.Data.ToString()
	resLen := len([]rune(res))
	if resLen < col.RetType.Flen && ctx.GetSessionVars().StmtCtx.PadCharToFullLength {
		trace_util_0.Count(_column_00000, 14)
		res = res + strings.Repeat(" ", col.RetType.Flen-resLen)
	}
	trace_util_0.Count(_column_00000, 12)
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_column_00000, 15)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 17)
		return nil, true, nil
	}
	trace_util_0.Count(_column_00000, 16)
	return col.Data.GetMysqlDecimal(), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_column_00000, 18)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 20)
		return types.Time{}, true, nil
	}
	trace_util_0.Count(_column_00000, 19)
	return col.Data.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_column_00000, 21)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 23)
		return types.Duration{}, true, nil
	}
	trace_util_0.Count(_column_00000, 22)
	return col.Data.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalJSON(ctx sessionctx.Context, row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_column_00000, 24)
	if col.Data.IsNull() {
		trace_util_0.Count(_column_00000, 26)
		return json.BinaryJSON{}, true, nil
	}
	trace_util_0.Count(_column_00000, 25)
	return col.Data.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (col *CorrelatedColumn) Equal(ctx sessionctx.Context, expr Expression) bool {
	trace_util_0.Count(_column_00000, 27)
	if cc, ok := expr.(*CorrelatedColumn); ok {
		trace_util_0.Count(_column_00000, 29)
		return col.Column.Equal(ctx, &cc.Column)
	}
	trace_util_0.Count(_column_00000, 28)
	return false
}

// IsCorrelated implements Expression interface.
func (col *CorrelatedColumn) IsCorrelated() bool {
	trace_util_0.Count(_column_00000, 30)
	return true
}

// ConstItem implements Expression interface.
func (col *CorrelatedColumn) ConstItem() bool {
	trace_util_0.Count(_column_00000, 31)
	return true
}

// Decorrelate implements Expression interface.
func (col *CorrelatedColumn) Decorrelate(schema *Schema) Expression {
	trace_util_0.Count(_column_00000, 32)
	if !schema.Contains(&col.Column) {
		trace_util_0.Count(_column_00000, 34)
		return col
	}
	trace_util_0.Count(_column_00000, 33)
	return &col.Column
}

// ResolveIndices implements Expression interface.
func (col *CorrelatedColumn) ResolveIndices(_ *Schema) (Expression, error) {
	trace_util_0.Count(_column_00000, 35)
	return col, nil
}

func (col *CorrelatedColumn) resolveIndices(_ *Schema) error {
	trace_util_0.Count(_column_00000, 36)
	return nil
}

// Column represents a column.
type Column struct {
	OrigColName model.CIStr
	ColName     model.CIStr
	DBName      model.CIStr
	OrigTblName model.CIStr
	TblName     model.CIStr
	RetType     *types.FieldType
	// ID is used to specify whether this column is ExtraHandleColumn or to access histogram.
	// We'll try to remove it in the future.
	ID int64
	// UniqueID is the unique id of this column.
	UniqueID int64
	// IsReferenced means if this column is referenced to an Aggregation column, or a Subquery column,
	// or an argument column of function IfNull.
	// If so, this column's name will be the plain sql text.
	IsReferenced bool

	// Index is used for execution, to tell the column's position in the given row.
	Index int

	hashcode []byte

	// InOperand indicates whether this column is the inner operand of column equal condition converted
	// from `[not] in (subq)`.
	InOperand bool
}

// Equal implements Expression interface.
func (col *Column) Equal(_ sessionctx.Context, expr Expression) bool {
	trace_util_0.Count(_column_00000, 37)
	if newCol, ok := expr.(*Column); ok {
		trace_util_0.Count(_column_00000, 39)
		return newCol.UniqueID == col.UniqueID
	}
	trace_util_0.Count(_column_00000, 38)
	return false
}

// String implements Stringer interface.
func (col *Column) String() string {
	trace_util_0.Count(_column_00000, 40)
	result := col.ColName.L
	if col.TblName.L != "" {
		trace_util_0.Count(_column_00000, 43)
		result = col.TblName.L + "." + result
	}
	trace_util_0.Count(_column_00000, 41)
	if col.DBName.L != "" {
		trace_util_0.Count(_column_00000, 44)
		result = col.DBName.L + "." + result
	}
	trace_util_0.Count(_column_00000, 42)
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (col *Column) MarshalJSON() ([]byte, error) {
	trace_util_0.Count(_column_00000, 45)
	return []byte(fmt.Sprintf("\"%s\"", col)), nil
}

// GetType implements Expression interface.
func (col *Column) GetType() *types.FieldType {
	trace_util_0.Count(_column_00000, 46)
	return col.RetType
}

// Eval implements Expression interface.
func (col *Column) Eval(row chunk.Row) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 47)
	return row.GetDatum(col.Index, col.RetType), nil
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_column_00000, 48)
	if col.GetType().Hybrid() {
		trace_util_0.Count(_column_00000, 51)
		val := row.GetDatum(col.Index, col.RetType)
		if val.IsNull() {
			trace_util_0.Count(_column_00000, 53)
			return 0, true, nil
		}
		trace_util_0.Count(_column_00000, 52)
		res, err := val.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	trace_util_0.Count(_column_00000, 49)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 54)
		return 0, true, nil
	}
	trace_util_0.Count(_column_00000, 50)
	return row.GetInt64(col.Index), false, nil
}

// EvalReal returns real representation of Column.
func (col *Column) EvalReal(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_column_00000, 55)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 58)
		return 0, true, nil
	}
	trace_util_0.Count(_column_00000, 56)
	if col.GetType().Tp == mysql.TypeFloat {
		trace_util_0.Count(_column_00000, 59)
		return float64(row.GetFloat32(col.Index)), false, nil
	}
	trace_util_0.Count(_column_00000, 57)
	return row.GetFloat64(col.Index), false, nil
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	trace_util_0.Count(_column_00000, 60)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 64)
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	trace_util_0.Count(_column_00000, 61)
	if col.GetType().Hybrid() {
		trace_util_0.Count(_column_00000, 65)
		val := row.GetDatum(col.Index, col.RetType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	trace_util_0.Count(_column_00000, 62)
	val := row.GetString(col.Index)
	if ctx.GetSessionVars().StmtCtx.PadCharToFullLength && col.GetType().Tp == mysql.TypeString {
		trace_util_0.Count(_column_00000, 66)
		valLen := len([]rune(val))
		if valLen < col.RetType.Flen {
			trace_util_0.Count(_column_00000, 67)
			val = val + strings.Repeat(" ", col.RetType.Flen-valLen)
		}
	}
	trace_util_0.Count(_column_00000, 63)
	return val, false, nil
}

// EvalDecimal returns decimal representation of Column.
func (col *Column) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_column_00000, 68)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 70)
		return nil, true, nil
	}
	trace_util_0.Count(_column_00000, 69)
	return row.GetMyDecimal(col.Index), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Column.
func (col *Column) EvalTime(ctx sessionctx.Context, row chunk.Row) (types.Time, bool, error) {
	trace_util_0.Count(_column_00000, 71)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 73)
		return types.Time{}, true, nil
	}
	trace_util_0.Count(_column_00000, 72)
	return row.GetTime(col.Index), false, nil
}

// EvalDuration returns Duration representation of Column.
func (col *Column) EvalDuration(ctx sessionctx.Context, row chunk.Row) (types.Duration, bool, error) {
	trace_util_0.Count(_column_00000, 74)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 76)
		return types.Duration{}, true, nil
	}
	trace_util_0.Count(_column_00000, 75)
	duration := row.GetDuration(col.Index, col.RetType.Decimal)
	return duration, false, nil
}

// EvalJSON returns JSON representation of Column.
func (col *Column) EvalJSON(ctx sessionctx.Context, row chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_column_00000, 77)
	if row.IsNull(col.Index) {
		trace_util_0.Count(_column_00000, 79)
		return json.BinaryJSON{}, true, nil
	}
	trace_util_0.Count(_column_00000, 78)
	return row.GetJSON(col.Index), false, nil
}

// Clone implements Expression interface.
func (col *Column) Clone() Expression {
	trace_util_0.Count(_column_00000, 80)
	newCol := *col
	return &newCol
}

// IsCorrelated implements Expression interface.
func (col *Column) IsCorrelated() bool {
	trace_util_0.Count(_column_00000, 81)
	return false
}

// ConstItem implements Expression interface.
func (col *Column) ConstItem() bool {
	trace_util_0.Count(_column_00000, 82)
	return false
}

// Decorrelate implements Expression interface.
func (col *Column) Decorrelate(_ *Schema) Expression {
	trace_util_0.Count(_column_00000, 83)
	return col
}

// HashCode implements Expression interface.
func (col *Column) HashCode(_ *stmtctx.StatementContext) []byte {
	trace_util_0.Count(_column_00000, 84)
	if len(col.hashcode) != 0 {
		trace_util_0.Count(_column_00000, 86)
		return col.hashcode
	}
	trace_util_0.Count(_column_00000, 85)
	col.hashcode = make([]byte, 0, 9)
	col.hashcode = append(col.hashcode, columnFlag)
	col.hashcode = codec.EncodeInt(col.hashcode, int64(col.UniqueID))
	return col.hashcode
}

// ResolveIndices implements Expression interface.
func (col *Column) ResolveIndices(schema *Schema) (Expression, error) {
	trace_util_0.Count(_column_00000, 87)
	newCol := col.Clone()
	err := newCol.resolveIndices(schema)
	return newCol, err
}

func (col *Column) resolveIndices(schema *Schema) error {
	trace_util_0.Count(_column_00000, 88)
	col.Index = schema.ColumnIndex(col)
	if col.Index == -1 {
		trace_util_0.Count(_column_00000, 90)
		return errors.Errorf("Can't find column %s in schema %s", col, schema)
	}
	trace_util_0.Count(_column_00000, 89)
	return nil
}

// Column2Exprs will transfer column slice to expression slice.
func Column2Exprs(cols []*Column) []Expression {
	trace_util_0.Count(_column_00000, 91)
	result := make([]Expression, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_column_00000, 93)
		result = append(result, col)
	}
	trace_util_0.Count(_column_00000, 92)
	return result
}

// ColInfo2Col finds the corresponding column of the ColumnInfo in a column slice.
func ColInfo2Col(cols []*Column, col *model.ColumnInfo) *Column {
	trace_util_0.Count(_column_00000, 94)
	for _, c := range cols {
		trace_util_0.Count(_column_00000, 96)
		if c.ColName.L == col.Name.L {
			trace_util_0.Count(_column_00000, 97)
			return c
		}
	}
	trace_util_0.Count(_column_00000, 95)
	return nil
}

// indexCol2Col finds the corresponding column of the IndexColumn in a column slice.
func indexCol2Col(cols []*Column, col *model.IndexColumn) *Column {
	trace_util_0.Count(_column_00000, 98)
	for _, c := range cols {
		trace_util_0.Count(_column_00000, 100)
		if c.ColName.L == col.Name.L {
			trace_util_0.Count(_column_00000, 101)
			return c
		}
	}
	trace_util_0.Count(_column_00000, 99)
	return nil
}

// IndexInfo2Cols gets the corresponding []*Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *Column,
// the return value will be only the 1st corresponding *Column and its length.
func IndexInfo2Cols(cols []*Column, index *model.IndexInfo) ([]*Column, []int) {
	trace_util_0.Count(_column_00000, 102)
	retCols := make([]*Column, 0, len(index.Columns))
	lengths := make([]int, 0, len(index.Columns))
	for _, c := range index.Columns {
		trace_util_0.Count(_column_00000, 104)
		col := indexCol2Col(cols, c)
		if col == nil {
			trace_util_0.Count(_column_00000, 106)
			return retCols, lengths
		}
		trace_util_0.Count(_column_00000, 105)
		retCols = append(retCols, col)
		if c.Length != types.UnspecifiedLength && c.Length == col.RetType.Flen {
			trace_util_0.Count(_column_00000, 107)
			lengths = append(lengths, types.UnspecifiedLength)
		} else {
			trace_util_0.Count(_column_00000, 108)
			{
				lengths = append(lengths, c.Length)
			}
		}
	}
	trace_util_0.Count(_column_00000, 103)
	return retCols, lengths
}

// FindPrefixOfIndex will find columns in index by checking the unique id.
// So it will return at once no matching column is found.
func FindPrefixOfIndex(cols []*Column, idxColIDs []int64) []*Column {
	trace_util_0.Count(_column_00000, 109)
	retCols := make([]*Column, 0, len(idxColIDs))
idLoop:
	for _, id := range idxColIDs {
		trace_util_0.Count(_column_00000, 111)
		for _, col := range cols {
			trace_util_0.Count(_column_00000, 113)
			if col.UniqueID == id {
				trace_util_0.Count(_column_00000, 114)
				retCols = append(retCols, col)
				continue idLoop
			}
		}
		// If no matching column is found, just return.
		trace_util_0.Count(_column_00000, 112)
		return retCols
	}
	trace_util_0.Count(_column_00000, 110)
	return retCols
}

var _column_00000 = "expression/column.go"
