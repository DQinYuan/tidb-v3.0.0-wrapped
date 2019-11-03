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

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	_ "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	log "github.com/sirupsen/logrus"
)

type column struct {
	idx         int
	name        string
	data        *datum
	tp          *types.FieldType
	comment     string
	min         string
	max         string
	incremental bool
	set         []string

	table *table

	hist *histogram
}

func (col *column) String() string {
	trace_util_0.Count(_parser_00000, 0)
	if col == nil {
		trace_util_0.Count(_parser_00000, 2)
		return "<nil>"
	}

	trace_util_0.Count(_parser_00000, 1)
	return fmt.Sprintf("[column]idx: %d, name: %s, tp: %v, min: %s, max: %s, step: %d, set: %v\n",
		col.idx, col.name, col.tp, col.min, col.max, col.data.step, col.set)
}

func (col *column) parseRule(kvs []string, uniq bool) {
	trace_util_0.Count(_parser_00000, 3)
	if len(kvs) != 2 {
		trace_util_0.Count(_parser_00000, 5)
		return
	}

	trace_util_0.Count(_parser_00000, 4)
	key := strings.TrimSpace(kvs[0])
	value := strings.TrimSpace(kvs[1])
	if key == "range" {
		trace_util_0.Count(_parser_00000, 6)
		fields := strings.Split(value, ",")
		if len(fields) == 1 {
			trace_util_0.Count(_parser_00000, 7)
			col.min = strings.TrimSpace(fields[0])
		} else {
			trace_util_0.Count(_parser_00000, 8)
			if len(fields) == 2 {
				trace_util_0.Count(_parser_00000, 9)
				col.min = strings.TrimSpace(fields[0])
				col.max = strings.TrimSpace(fields[1])
			}
		}
	} else {
		trace_util_0.Count(_parser_00000, 10)
		if key == "step" {
			trace_util_0.Count(_parser_00000, 11)
			var err error
			col.data.step, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				trace_util_0.Count(_parser_00000, 12)
				log.Fatal(err)
			}
		} else {
			trace_util_0.Count(_parser_00000, 13)
			if key == "set" {
				trace_util_0.Count(_parser_00000, 14)
				fields := strings.Split(value, ",")
				for _, field := range fields {
					trace_util_0.Count(_parser_00000, 15)
					col.set = append(col.set, strings.TrimSpace(field))
				}
			} else {
				trace_util_0.Count(_parser_00000, 16)
				if key == "incremental" {
					trace_util_0.Count(_parser_00000, 17)
					var err error
					col.incremental, err = strconv.ParseBool(value)
					if err != nil {
						trace_util_0.Count(_parser_00000, 18)
						log.Fatal(err)
					}
				} else {
					trace_util_0.Count(_parser_00000, 19)
					if key == "repeats" {
						trace_util_0.Count(_parser_00000, 20)
						repeats, err := strconv.ParseUint(value, 10, 64)
						if err != nil {
							trace_util_0.Count(_parser_00000, 23)
							log.Fatal(err)
						}
						trace_util_0.Count(_parser_00000, 21)
						if uniq && repeats > 1 {
							trace_util_0.Count(_parser_00000, 24)
							log.Fatal("cannot repeat more than 1 times on unique columns")
						}
						trace_util_0.Count(_parser_00000, 22)
						col.data.repeats = repeats
						col.data.remains = repeats
					} else {
						trace_util_0.Count(_parser_00000, 25)
						if key == "probability" {
							trace_util_0.Count(_parser_00000, 26)
							prob, err := strconv.ParseUint(value, 10, 32)
							if err != nil {
								trace_util_0.Count(_parser_00000, 29)
								log.Fatal(err)
							}
							trace_util_0.Count(_parser_00000, 27)
							if prob > 100 || prob == 0 {
								trace_util_0.Count(_parser_00000, 30)
								log.Fatal("probability must be in (0, 100]")
							}
							trace_util_0.Count(_parser_00000, 28)
							col.data.probability = uint32(prob)
						}
					}
				}
			}
		}
	}
}

// parse the data rules.
// rules like `a int unique comment '[[range=1,10;step=1]]'`,
// then we will get value from 1,2...10
func (col *column) parseColumnComment(uniq bool) {
	trace_util_0.Count(_parser_00000, 31)
	comment := strings.TrimSpace(col.comment)
	start := strings.Index(comment, "[[")
	end := strings.Index(comment, "]]")
	var content string
	if start < end {
		trace_util_0.Count(_parser_00000, 33)
		content = comment[start+2 : end]
	}

	trace_util_0.Count(_parser_00000, 32)
	fields := strings.Split(content, ";")
	for _, field := range fields {
		trace_util_0.Count(_parser_00000, 34)
		field = strings.TrimSpace(field)
		kvs := strings.Split(field, "=")
		col.parseRule(kvs, uniq)
	}
}

func (col *column) parseColumn(cd *ast.ColumnDef) {
	trace_util_0.Count(_parser_00000, 35)
	col.name = cd.Name.Name.L
	col.tp = cd.Tp
	col.parseColumnOptions(cd.Options)
	_, uniq := col.table.uniqIndices[col.name]
	col.parseColumnComment(uniq)
	col.table.columns = append(col.table.columns, col)
}

func (col *column) parseColumnOptions(ops []*ast.ColumnOption) {
	trace_util_0.Count(_parser_00000, 36)
	for _, op := range ops {
		trace_util_0.Count(_parser_00000, 37)
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey, ast.ColumnOptionAutoIncrement:
			trace_util_0.Count(_parser_00000, 38)
			col.table.uniqIndices[col.name] = col
		case ast.ColumnOptionComment:
			trace_util_0.Count(_parser_00000, 39)
			col.comment = op.Expr.(ast.ValueExpr).GetDatumString()
		}
	}
}

type table struct {
	name        string
	columns     []*column
	columnList  string
	indices     map[string]*column
	uniqIndices map[string]*column
	tblInfo     *model.TableInfo
}

func (t *table) printColumns() string {
	trace_util_0.Count(_parser_00000, 40)
	ret := ""
	for _, col := range t.columns {
		trace_util_0.Count(_parser_00000, 42)
		ret += fmt.Sprintf("%v", col)
	}

	trace_util_0.Count(_parser_00000, 41)
	return ret
}

func (t *table) String() string {
	trace_util_0.Count(_parser_00000, 43)
	if t == nil {
		trace_util_0.Count(_parser_00000, 47)
		return "<nil>"
	}

	trace_util_0.Count(_parser_00000, 44)
	ret := fmt.Sprintf("[table]name: %s\n", t.name)
	ret += fmt.Sprintf("[table]columns:\n")
	ret += t.printColumns()

	ret += fmt.Sprintf("[table]column list: %s\n", t.columnList)

	ret += fmt.Sprintf("[table]indices:\n")
	for k, v := range t.indices {
		trace_util_0.Count(_parser_00000, 48)
		ret += fmt.Sprintf("key->%s, value->%v", k, v)
	}

	trace_util_0.Count(_parser_00000, 45)
	ret += fmt.Sprintf("[table]unique indices:\n")
	for k, v := range t.uniqIndices {
		trace_util_0.Count(_parser_00000, 49)
		ret += fmt.Sprintf("key->%s, value->%v", k, v)
	}

	trace_util_0.Count(_parser_00000, 46)
	return ret
}

func newTable() *table {
	trace_util_0.Count(_parser_00000, 50)
	return &table{
		indices:     make(map[string]*column),
		uniqIndices: make(map[string]*column),
	}
}

func (t *table) findCol(cols []*column, name string) *column {
	trace_util_0.Count(_parser_00000, 51)
	for _, col := range cols {
		trace_util_0.Count(_parser_00000, 53)
		if col.name == name {
			trace_util_0.Count(_parser_00000, 54)
			return col
		}
	}
	trace_util_0.Count(_parser_00000, 52)
	return nil
}

func (t *table) parseTableConstraint(cons *ast.Constraint) {
	trace_util_0.Count(_parser_00000, 55)
	switch cons.Tp {
	case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintUniq,
		ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		trace_util_0.Count(_parser_00000, 56)
		for _, indexCol := range cons.Keys {
			trace_util_0.Count(_parser_00000, 58)
			name := indexCol.Column.Name.L
			t.uniqIndices[name] = t.findCol(t.columns, name)
		}
	case ast.ConstraintIndex:
		trace_util_0.Count(_parser_00000, 57)
		for _, indexCol := range cons.Keys {
			trace_util_0.Count(_parser_00000, 59)
			name := indexCol.Column.Name.L
			t.indices[name] = t.findCol(t.columns, name)
		}
	}
}

func (t *table) buildColumnList() {
	trace_util_0.Count(_parser_00000, 60)
	columns := make([]string, 0, len(t.columns))
	for _, column := range t.columns {
		trace_util_0.Count(_parser_00000, 62)
		columns = append(columns, column.name)
	}

	trace_util_0.Count(_parser_00000, 61)
	t.columnList = strings.Join(columns, ",")
}

func parseTable(t *table, stmt *ast.CreateTableStmt) error {
	trace_util_0.Count(_parser_00000, 63)
	t.name = stmt.Table.Name.L
	t.columns = make([]*column, 0, len(stmt.Cols))

	mockTbl, err := ddl.MockTableInfo(mock.NewContext(), stmt, 1)
	if err != nil {
		trace_util_0.Count(_parser_00000, 67)
		return errors.Trace(err)
	}
	trace_util_0.Count(_parser_00000, 64)
	t.tblInfo = mockTbl

	for i, col := range stmt.Cols {
		trace_util_0.Count(_parser_00000, 68)
		column := &column{idx: i + 1, table: t, data: newDatum()}
		column.parseColumn(col)
	}

	trace_util_0.Count(_parser_00000, 65)
	for _, cons := range stmt.Constraints {
		trace_util_0.Count(_parser_00000, 69)
		t.parseTableConstraint(cons)
	}

	trace_util_0.Count(_parser_00000, 66)
	t.buildColumnList()

	return nil
}

func parseTableSQL(table *table, sql string) error {
	trace_util_0.Count(_parser_00000, 70)
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		trace_util_0.Count(_parser_00000, 73)
		return errors.Trace(err)
	}

	trace_util_0.Count(_parser_00000, 71)
	switch node := stmt.(type) {
	case *ast.CreateTableStmt:
		trace_util_0.Count(_parser_00000, 74)
		err = parseTable(table, node)
	default:
		trace_util_0.Count(_parser_00000, 75)
		err = errors.Errorf("invalid statement - %v", stmt.Text())
	}

	trace_util_0.Count(_parser_00000, 72)
	return errors.Trace(err)
}

func parseIndex(table *table, stmt *ast.CreateIndexStmt) error {
	trace_util_0.Count(_parser_00000, 76)
	if table.name != stmt.Table.Name.L {
		trace_util_0.Count(_parser_00000, 79)
		return errors.Errorf("mismatch table name for create index - %s : %s", table.name, stmt.Table.Name.L)
	}

	trace_util_0.Count(_parser_00000, 77)
	for _, indexCol := range stmt.IndexColNames {
		trace_util_0.Count(_parser_00000, 80)
		name := indexCol.Column.Name.L
		if stmt.Unique {
			trace_util_0.Count(_parser_00000, 81)
			table.uniqIndices[name] = table.findCol(table.columns, name)
		} else {
			trace_util_0.Count(_parser_00000, 82)
			{
				table.indices[name] = table.findCol(table.columns, name)
			}
		}
	}

	trace_util_0.Count(_parser_00000, 78)
	return nil
}

func parseIndexSQL(table *table, sql string) error {
	trace_util_0.Count(_parser_00000, 83)
	if len(sql) == 0 {
		trace_util_0.Count(_parser_00000, 87)
		return nil
	}

	trace_util_0.Count(_parser_00000, 84)
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		trace_util_0.Count(_parser_00000, 88)
		return errors.Trace(err)
	}

	trace_util_0.Count(_parser_00000, 85)
	switch node := stmt.(type) {
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_parser_00000, 89)
		err = parseIndex(table, node)
	default:
		trace_util_0.Count(_parser_00000, 90)
		err = errors.Errorf("invalid statement - %v", stmt.Text())
	}

	trace_util_0.Count(_parser_00000, 86)
	return errors.Trace(err)
}

var _parser_00000 = "cmd/importer/parser.go"
