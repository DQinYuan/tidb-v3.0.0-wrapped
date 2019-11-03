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

package tables

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
)

// nameResolver is the visitor to resolve table name and column name.
// it combines TableInfo and ColumnInfo to a generation expression.
type nameResolver struct {
	tableInfo *model.TableInfo
	err       error
}

// Enter implements ast.Visitor interface.
func (nr *nameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_gen_expr_00000, 0)
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (nr *nameResolver) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	trace_util_0.Count(_gen_expr_00000, 1)
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		trace_util_0.Count(_gen_expr_00000, 3)
		for _, col := range nr.tableInfo.Columns {
			trace_util_0.Count(_gen_expr_00000, 5)
			if col.Name.L == v.Name.Name.L {
				trace_util_0.Count(_gen_expr_00000, 6)
				v.Refer = &ast.ResultField{
					Column: col,
					Table:  nr.tableInfo,
				}
				return inNode, true
			}
		}
		trace_util_0.Count(_gen_expr_00000, 4)
		nr.err = errors.Errorf("can't find column %s in %s", v.Name.Name.O, nr.tableInfo.Name.O)
		return inNode, false
	}
	trace_util_0.Count(_gen_expr_00000, 2)
	return inNode, true
}

// ParseExpression parses an ExprNode from a string.
// When TiDB loads infoschema from TiKV, `GeneratedExprString`
// of `ColumnInfo` is a string field, so we need to parse
// it into ast.ExprNode. This function is for that.
func parseExpression(expr string) (node ast.ExprNode, err error) {
	trace_util_0.Count(_gen_expr_00000, 7)
	expr = fmt.Sprintf("select %s", expr)
	charset, collation := charset.GetDefaultCharsetAndCollate()
	stmts, _, err := parser.New().Parse(expr, charset, collation)
	if err == nil {
		trace_util_0.Count(_gen_expr_00000, 9)
		node = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	trace_util_0.Count(_gen_expr_00000, 8)
	return node, util.SyntaxError(err)
}

// SimpleResolveName resolves all column names in the expression node.
func simpleResolveName(node ast.ExprNode, tblInfo *model.TableInfo) (ast.ExprNode, error) {
	trace_util_0.Count(_gen_expr_00000, 10)
	nr := nameResolver{tblInfo, nil}
	if _, ok := node.Accept(&nr); !ok {
		trace_util_0.Count(_gen_expr_00000, 12)
		return nil, errors.Trace(nr.err)
	}
	trace_util_0.Count(_gen_expr_00000, 11)
	return node, nil
}

var _gen_expr_00000 = "table/tables/gen_expr.go"
