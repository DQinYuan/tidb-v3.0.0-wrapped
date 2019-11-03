// Copyright 2017 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
)

// columnGenerationInDDL is a struct for validating generated columns in DDL.
type columnGenerationInDDL struct {
	position    int
	generated   bool
	dependences map[string]struct{}
}

// verifyColumnGeneration is for CREATE TABLE, because we need verify all columns in the table.
func verifyColumnGeneration(colName2Generation map[string]columnGenerationInDDL, colName string) error {
	trace_util_0.Count(_generated_column_00000, 0)
	attribute := colName2Generation[colName]
	if attribute.generated {
		trace_util_0.Count(_generated_column_00000, 2)
		for depCol := range attribute.dependences {
			trace_util_0.Count(_generated_column_00000, 3)
			if attr, ok := colName2Generation[depCol]; ok {
				trace_util_0.Count(_generated_column_00000, 4)
				if attr.generated && attribute.position <= attr.position {
					trace_util_0.Count(_generated_column_00000, 5)
					// A generated column definition can refer to other
					// generated columns occurring earilier in the table.
					err := errGeneratedColumnNonPrior.GenWithStackByArgs()
					return errors.Trace(err)
				}
			} else {
				trace_util_0.Count(_generated_column_00000, 6)
				{
					err := ErrBadField.GenWithStackByArgs(depCol, "generated column function")
					return errors.Trace(err)
				}
			}
		}
	}
	trace_util_0.Count(_generated_column_00000, 1)
	return nil
}

// checkDependedColExist ensure all depended columns exist.
//
// NOTE: this will MODIFY parameter `dependCols`.
func checkDependedColExist(dependCols map[string]struct{}, cols []*table.Column) error {
	trace_util_0.Count(_generated_column_00000, 7)
	for _, col := range cols {
		trace_util_0.Count(_generated_column_00000, 10)
		delete(dependCols, col.Name.L)
	}
	trace_util_0.Count(_generated_column_00000, 8)
	if len(dependCols) != 0 {
		trace_util_0.Count(_generated_column_00000, 11)
		for arbitraryCol := range dependCols {
			trace_util_0.Count(_generated_column_00000, 12)
			return ErrBadField.GenWithStackByArgs(arbitraryCol, "generated column function")
		}
	}
	trace_util_0.Count(_generated_column_00000, 9)
	return nil
}

// findDependedColumnNames returns a set of string, which indicates
// the names of the columns that are depended by colDef.
func findDependedColumnNames(colDef *ast.ColumnDef) (generated bool, colsMap map[string]struct{}) {
	trace_util_0.Count(_generated_column_00000, 13)
	colsMap = make(map[string]struct{})
	for _, option := range colDef.Options {
		trace_util_0.Count(_generated_column_00000, 15)
		if option.Tp == ast.ColumnOptionGenerated {
			trace_util_0.Count(_generated_column_00000, 16)
			generated = true
			colNames := findColumnNamesInExpr(option.Expr)
			for _, depCol := range colNames {
				trace_util_0.Count(_generated_column_00000, 18)
				colsMap[depCol.Name.L] = struct{}{}
			}
			trace_util_0.Count(_generated_column_00000, 17)
			break
		}
	}
	trace_util_0.Count(_generated_column_00000, 14)
	return
}

// findColumnNamesInExpr returns a slice of ast.ColumnName which is referred in expr.
func findColumnNamesInExpr(expr ast.ExprNode) []*ast.ColumnName {
	trace_util_0.Count(_generated_column_00000, 19)
	var c generatedColumnChecker
	expr.Accept(&c)
	return c.cols
}

type generatedColumnChecker struct {
	cols []*ast.ColumnName
}

func (c *generatedColumnChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	trace_util_0.Count(_generated_column_00000, 20)
	return inNode, false
}

func (c *generatedColumnChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	trace_util_0.Count(_generated_column_00000, 21)
	switch x := inNode.(type) {
	case *ast.ColumnName:
		trace_util_0.Count(_generated_column_00000, 23)
		c.cols = append(c.cols, x)
	}
	trace_util_0.Count(_generated_column_00000, 22)
	return inNode, true
}

// checkModifyGeneratedColumn checks the modification between
// old and new is valid or not by such rules:
//  1. the modification can't change stored status;
//  2. if the new is generated, check its refer rules.
//  3. check if the modified expr contains non-deterministic functions
func checkModifyGeneratedColumn(originCols []*table.Column, oldCol, newCol *table.Column) error {
	trace_util_0.Count(_generated_column_00000, 24)
	// rule 1.
	var stored = [2]bool{false, false}
	var cols = [2]*table.Column{oldCol, newCol}
	for i, col := range cols {
		trace_util_0.Count(_generated_column_00000, 30)
		if !col.IsGenerated() || col.GeneratedStored {
			trace_util_0.Count(_generated_column_00000, 31)
			stored[i] = true
		}
	}
	trace_util_0.Count(_generated_column_00000, 25)
	if stored[0] != stored[1] {
		trace_util_0.Count(_generated_column_00000, 32)
		return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Changing the STORED status")
	}
	// rule 2.
	trace_util_0.Count(_generated_column_00000, 26)
	var colName2Generation = make(map[string]columnGenerationInDDL, len(originCols))
	for i, column := range originCols {
		trace_util_0.Count(_generated_column_00000, 33)
		// We can compare the pointers simply.
		if column == oldCol {
			trace_util_0.Count(_generated_column_00000, 34)
			colName2Generation[newCol.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   newCol.IsGenerated(),
				dependences: newCol.Dependences,
			}
		} else {
			trace_util_0.Count(_generated_column_00000, 35)
			if !column.IsGenerated() {
				trace_util_0.Count(_generated_column_00000, 36)
				colName2Generation[column.Name.L] = columnGenerationInDDL{
					position:  i,
					generated: false,
				}
			} else {
				trace_util_0.Count(_generated_column_00000, 37)
				{
					colName2Generation[column.Name.L] = columnGenerationInDDL{
						position:    i,
						generated:   true,
						dependences: column.Dependences,
					}
				}
			}
		}
	}
	// We always need test all columns, even if it's not changed
	// because other can depend on it so its name can't be changed.
	trace_util_0.Count(_generated_column_00000, 27)
	for _, column := range originCols {
		trace_util_0.Count(_generated_column_00000, 38)
		var colName string
		if column == oldCol {
			trace_util_0.Count(_generated_column_00000, 40)
			colName = newCol.Name.L
		} else {
			trace_util_0.Count(_generated_column_00000, 41)
			{
				colName = column.Name.L
			}
		}
		trace_util_0.Count(_generated_column_00000, 39)
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			trace_util_0.Count(_generated_column_00000, 42)
			return errors.Trace(err)
		}
	}

	// rule 3
	trace_util_0.Count(_generated_column_00000, 28)
	if newCol.IsGenerated() {
		trace_util_0.Count(_generated_column_00000, 43)
		if err := checkIllegalFn4GeneratedColumn(newCol.Name.L, newCol.GeneratedExpr); err != nil {
			trace_util_0.Count(_generated_column_00000, 44)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_generated_column_00000, 29)
	return nil
}

type illegalFunctionChecker struct {
	found bool
}

func (c *illegalFunctionChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	trace_util_0.Count(_generated_column_00000, 45)
	switch node := inNode.(type) {
	case *ast.FuncCallExpr:
		trace_util_0.Count(_generated_column_00000, 47)
		if _, found := expression.IllegalFunctions4GeneratedColumns[node.FnName.L]; found {
			trace_util_0.Count(_generated_column_00000, 48)
			c.found = true
			return inNode, true
		}
	}
	trace_util_0.Count(_generated_column_00000, 46)
	return inNode, false
}

func (c *illegalFunctionChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	trace_util_0.Count(_generated_column_00000, 49)
	return inNode, true
}

func checkIllegalFn4GeneratedColumn(colName string, expr ast.ExprNode) error {
	trace_util_0.Count(_generated_column_00000, 50)
	if expr == nil {
		trace_util_0.Count(_generated_column_00000, 53)
		return nil
	}
	trace_util_0.Count(_generated_column_00000, 51)
	var c illegalFunctionChecker
	expr.Accept(&c)
	if c.found {
		trace_util_0.Count(_generated_column_00000, 54)
		return ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs(colName)
	}
	trace_util_0.Count(_generated_column_00000, 52)
	return nil
}

// checkAutoIncrementRef checks if an generated column depends on an auto-increment column and raises an error if so.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
func checkAutoIncrementRef(name string, dependencies map[string]struct{}, tbInfo *model.TableInfo) error {
	trace_util_0.Count(_generated_column_00000, 55)
	exists, autoIncrementColumn := hasAutoIncrementColumn(tbInfo)
	if exists {
		trace_util_0.Count(_generated_column_00000, 57)
		if _, found := dependencies[autoIncrementColumn]; found {
			trace_util_0.Count(_generated_column_00000, 58)
			return ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(name)
		}
	}
	trace_util_0.Count(_generated_column_00000, 56)
	return nil
}

var _generated_column_00000 = "ddl/generated_column.go"
