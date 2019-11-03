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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/trace_util_0"
)

// KeyInfo stores the columns of one unique key or primary key.
type KeyInfo []*Column

// Clone copies the entire UniqueKey.
func (ki KeyInfo) Clone() KeyInfo {
	trace_util_0.Count(_schema_00000, 0)
	result := make([]*Column, 0, len(ki))
	for _, col := range ki {
		trace_util_0.Count(_schema_00000, 2)
		result = append(result, col.Clone().(*Column))
	}
	trace_util_0.Count(_schema_00000, 1)
	return result
}

// Schema stands for the row schema and unique key information get from input.
type Schema struct {
	Columns []*Column
	Keys    []KeyInfo
	// TblID2Handle stores the tables' handle column information if we need handle in execution phase.
	TblID2Handle map[int64][]*Column
}

// String implements fmt.Stringer interface.
func (s *Schema) String() string {
	trace_util_0.Count(_schema_00000, 3)
	colStrs := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		trace_util_0.Count(_schema_00000, 6)
		colStrs = append(colStrs, col.String())
	}
	trace_util_0.Count(_schema_00000, 4)
	ukStrs := make([]string, 0, len(s.Keys))
	for _, key := range s.Keys {
		trace_util_0.Count(_schema_00000, 7)
		ukColStrs := make([]string, 0, len(key))
		for _, col := range key {
			trace_util_0.Count(_schema_00000, 9)
			ukColStrs = append(ukColStrs, col.String())
		}
		trace_util_0.Count(_schema_00000, 8)
		ukStrs = append(ukStrs, "["+strings.Join(ukColStrs, ",")+"]")
	}
	trace_util_0.Count(_schema_00000, 5)
	return "Column: [" + strings.Join(colStrs, ",") + "] Unique key: [" + strings.Join(ukStrs, ",") + "]"
}

// Clone copies the total schema.
func (s *Schema) Clone() *Schema {
	trace_util_0.Count(_schema_00000, 10)
	cols := make([]*Column, 0, s.Len())
	keys := make([]KeyInfo, 0, len(s.Keys))
	for _, col := range s.Columns {
		trace_util_0.Count(_schema_00000, 14)
		cols = append(cols, col.Clone().(*Column))
	}
	trace_util_0.Count(_schema_00000, 11)
	for _, key := range s.Keys {
		trace_util_0.Count(_schema_00000, 15)
		keys = append(keys, key.Clone())
	}
	trace_util_0.Count(_schema_00000, 12)
	schema := NewSchema(cols...)
	schema.SetUniqueKeys(keys)
	for id, cols := range s.TblID2Handle {
		trace_util_0.Count(_schema_00000, 16)
		schema.TblID2Handle[id] = make([]*Column, 0, len(cols))
		for _, col := range cols {
			trace_util_0.Count(_schema_00000, 17)
			var inColumns = false
			for i, colInColumns := range s.Columns {
				trace_util_0.Count(_schema_00000, 19)
				if col == colInColumns {
					trace_util_0.Count(_schema_00000, 20)
					schema.TblID2Handle[id] = append(schema.TblID2Handle[id], schema.Columns[i])
					inColumns = true
					break
				}
			}
			trace_util_0.Count(_schema_00000, 18)
			if !inColumns {
				trace_util_0.Count(_schema_00000, 21)
				schema.TblID2Handle[id] = append(schema.TblID2Handle[id], col.Clone().(*Column))
			}
		}
	}
	trace_util_0.Count(_schema_00000, 13)
	return schema
}

// ExprFromSchema checks if all columns of this expression are from the same schema.
func ExprFromSchema(expr Expression, schema *Schema) bool {
	trace_util_0.Count(_schema_00000, 22)
	switch v := expr.(type) {
	case *Column:
		trace_util_0.Count(_schema_00000, 24)
		return schema.Contains(v)
	case *ScalarFunction:
		trace_util_0.Count(_schema_00000, 25)
		for _, arg := range v.GetArgs() {
			trace_util_0.Count(_schema_00000, 28)
			if !ExprFromSchema(arg, schema) {
				trace_util_0.Count(_schema_00000, 29)
				return false
			}
		}
		trace_util_0.Count(_schema_00000, 26)
		return true
	case *CorrelatedColumn, *Constant:
		trace_util_0.Count(_schema_00000, 27)
		return true
	}
	trace_util_0.Count(_schema_00000, 23)
	return false
}

// FindColumn finds an Column from schema for a ast.ColumnName. It compares the db/table/column names.
// If there are more than one result, it will raise ambiguous error.
func (s *Schema) FindColumn(astCol *ast.ColumnName) (*Column, error) {
	trace_util_0.Count(_schema_00000, 30)
	col, _, err := s.FindColumnAndIndex(astCol)
	return col, err
}

// FindColumnAndIndex finds an Column and its index from schema for a ast.ColumnName.
// It compares the db/table/column names. If there are more than one result, raise ambiguous error.
func (s *Schema) FindColumnAndIndex(astCol *ast.ColumnName) (*Column, int, error) {
	trace_util_0.Count(_schema_00000, 31)
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s.Columns {
		trace_util_0.Count(_schema_00000, 34)
		if (dbName.L == "" || dbName.L == col.DBName.L) &&
			(tblName.L == "" || tblName.L == col.TblName.L) &&
			(colName.L == col.ColName.L) {
			trace_util_0.Count(_schema_00000, 35)
			if idx == -1 {
				trace_util_0.Count(_schema_00000, 36)
				idx = i
			} else {
				trace_util_0.Count(_schema_00000, 37)
				{
					// For query like:
					// create table t1(a int); create table t2(d int);
					// select 1 from t1, t2 where 1 = (select d from t2 where a > 1) and d = 1;
					// we will get an Apply operator whose schema is [test.t1.a, test.t2.d, test.t2.d],
					// we check whether the column of the schema comes from a subquery to avoid
					// causing the ambiguous error when resolve the column `d` in the Selection.
					if !col.IsReferenced {
						trace_util_0.Count(_schema_00000, 38)
						return nil, -1, errors.Errorf("Column %s is ambiguous", col.String())
					}
				}
			}
		}
	}
	trace_util_0.Count(_schema_00000, 32)
	if idx == -1 {
		trace_util_0.Count(_schema_00000, 39)
		return nil, idx, nil
	}
	trace_util_0.Count(_schema_00000, 33)
	return s.Columns[idx], idx, nil
}

// RetrieveColumn retrieves column in expression from the columns in schema.
func (s *Schema) RetrieveColumn(col *Column) *Column {
	trace_util_0.Count(_schema_00000, 40)
	index := s.ColumnIndex(col)
	if index != -1 {
		trace_util_0.Count(_schema_00000, 42)
		return s.Columns[index]
	}
	trace_util_0.Count(_schema_00000, 41)
	return nil
}

// IsUniqueKey checks if this column is a unique key.
func (s *Schema) IsUniqueKey(col *Column) bool {
	trace_util_0.Count(_schema_00000, 43)
	for _, key := range s.Keys {
		trace_util_0.Count(_schema_00000, 45)
		if len(key) == 1 && key[0].Equal(nil, col) {
			trace_util_0.Count(_schema_00000, 46)
			return true
		}
	}
	trace_util_0.Count(_schema_00000, 44)
	return false
}

// ColumnIndex finds the index for a column.
func (s *Schema) ColumnIndex(col *Column) int {
	trace_util_0.Count(_schema_00000, 47)
	for i, c := range s.Columns {
		trace_util_0.Count(_schema_00000, 49)
		if c.UniqueID == col.UniqueID {
			trace_util_0.Count(_schema_00000, 50)
			return i
		}
	}
	trace_util_0.Count(_schema_00000, 48)
	return -1
}

// FindColumnByName finds a column by its name.
func (s *Schema) FindColumnByName(name string) *Column {
	trace_util_0.Count(_schema_00000, 51)
	for _, col := range s.Columns {
		trace_util_0.Count(_schema_00000, 53)
		if col.ColName.L == name {
			trace_util_0.Count(_schema_00000, 54)
			return col
		}
	}
	trace_util_0.Count(_schema_00000, 52)
	return nil
}

// Contains checks if the schema contains the column.
func (s *Schema) Contains(col *Column) bool {
	trace_util_0.Count(_schema_00000, 55)
	return s.ColumnIndex(col) != -1
}

// Len returns the number of columns in schema.
func (s *Schema) Len() int {
	trace_util_0.Count(_schema_00000, 56)
	return len(s.Columns)
}

// Append append new column to the columns stored in schema.
func (s *Schema) Append(col ...*Column) {
	trace_util_0.Count(_schema_00000, 57)
	s.Columns = append(s.Columns, col...)
}

// SetUniqueKeys will set the value of Schema.Keys.
func (s *Schema) SetUniqueKeys(keys []KeyInfo) {
	trace_util_0.Count(_schema_00000, 58)
	s.Keys = keys
}

// ColumnsIndices will return a slice which contains the position of each column in schema.
// If there is one column that doesn't match, nil will be returned.
func (s *Schema) ColumnsIndices(cols []*Column) (ret []int) {
	trace_util_0.Count(_schema_00000, 59)
	ret = make([]int, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_schema_00000, 61)
		pos := s.ColumnIndex(col)
		if pos != -1 {
			trace_util_0.Count(_schema_00000, 62)
			ret = append(ret, pos)
		} else {
			trace_util_0.Count(_schema_00000, 63)
			{
				return nil
			}
		}
	}
	trace_util_0.Count(_schema_00000, 60)
	return
}

// ColumnsByIndices returns columns by multiple offsets.
// Callers should guarantee that all the offsets provided should be valid, which means offset should:
// 1. not smaller than 0, and
// 2. not exceed len(s.Columns)
func (s *Schema) ColumnsByIndices(offsets []int) []*Column {
	trace_util_0.Count(_schema_00000, 64)
	cols := make([]*Column, 0, len(offsets))
	for _, offset := range offsets {
		trace_util_0.Count(_schema_00000, 66)
		cols = append(cols, s.Columns[offset])
	}
	trace_util_0.Count(_schema_00000, 65)
	return cols
}

// MergeSchema will merge two schema into one schema. We shouldn't need to consider unique keys.
// That will be processed in build_key_info.go.
func MergeSchema(lSchema, rSchema *Schema) *Schema {
	trace_util_0.Count(_schema_00000, 67)
	if lSchema == nil && rSchema == nil {
		trace_util_0.Count(_schema_00000, 72)
		return nil
	}
	trace_util_0.Count(_schema_00000, 68)
	if lSchema == nil {
		trace_util_0.Count(_schema_00000, 73)
		return rSchema.Clone()
	}
	trace_util_0.Count(_schema_00000, 69)
	if rSchema == nil {
		trace_util_0.Count(_schema_00000, 74)
		return lSchema.Clone()
	}
	trace_util_0.Count(_schema_00000, 70)
	tmpL := lSchema.Clone()
	tmpR := rSchema.Clone()
	ret := NewSchema(append(tmpL.Columns, tmpR.Columns...)...)
	ret.TblID2Handle = tmpL.TblID2Handle
	for id, cols := range tmpR.TblID2Handle {
		trace_util_0.Count(_schema_00000, 75)
		if _, ok := ret.TblID2Handle[id]; ok {
			trace_util_0.Count(_schema_00000, 76)
			ret.TblID2Handle[id] = append(ret.TblID2Handle[id], cols...)
		} else {
			trace_util_0.Count(_schema_00000, 77)
			{
				ret.TblID2Handle[id] = cols
			}
		}
	}
	trace_util_0.Count(_schema_00000, 71)
	return ret
}

// NewSchema returns a schema made by its parameter.
func NewSchema(cols ...*Column) *Schema {
	trace_util_0.Count(_schema_00000, 78)
	return &Schema{Columns: cols, TblID2Handle: make(map[int64][]*Column)}
}

var _schema_00000 = "expression/schema.go"
