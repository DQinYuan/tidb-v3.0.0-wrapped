// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/trace_util_0"

	// BindHint will add hints for originStmt according to hintedStmt' hints.
)

func BindHint(originStmt, hintedStmt ast.StmtNode) ast.StmtNode {
	trace_util_0.Count(_bind_00000, 0)
	switch x := originStmt.(type) {
	case *ast.SelectStmt:
		trace_util_0.Count(_bind_00000, 1)
		return selectBind(x, hintedStmt.(*ast.SelectStmt))
	default:
		trace_util_0.Count(_bind_00000, 2)
		return originStmt
	}
}

func selectBind(originalNode, hintedNode *ast.SelectStmt) *ast.SelectStmt {
	trace_util_0.Count(_bind_00000, 3)
	if hintedNode.TableHints != nil {
		trace_util_0.Count(_bind_00000, 10)
		originalNode.TableHints = hintedNode.TableHints
	}
	trace_util_0.Count(_bind_00000, 4)
	if originalNode.From != nil {
		trace_util_0.Count(_bind_00000, 11)
		originalNode.From.TableRefs = resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs).(*ast.Join)
	}
	trace_util_0.Count(_bind_00000, 5)
	if originalNode.Where != nil {
		trace_util_0.Count(_bind_00000, 12)
		originalNode.Where = exprBind(originalNode.Where, hintedNode.Where).(ast.ExprNode)
	}

	trace_util_0.Count(_bind_00000, 6)
	if originalNode.Having != nil {
		trace_util_0.Count(_bind_00000, 13)
		originalNode.Having.Expr = exprBind(originalNode.Having.Expr, hintedNode.Having.Expr)
	}

	trace_util_0.Count(_bind_00000, 7)
	if originalNode.OrderBy != nil {
		trace_util_0.Count(_bind_00000, 14)
		originalNode.OrderBy = orderByBind(originalNode.OrderBy, hintedNode.OrderBy)
	}

	trace_util_0.Count(_bind_00000, 8)
	if originalNode.Fields != nil {
		trace_util_0.Count(_bind_00000, 15)
		origFields := originalNode.Fields.Fields
		hintFields := hintedNode.Fields.Fields
		for idx := range origFields {
			trace_util_0.Count(_bind_00000, 16)
			origFields[idx].Expr = exprBind(origFields[idx].Expr, hintFields[idx].Expr)
		}
	}
	trace_util_0.Count(_bind_00000, 9)
	return originalNode
}

func orderByBind(originalNode, hintedNode *ast.OrderByClause) *ast.OrderByClause {
	trace_util_0.Count(_bind_00000, 17)
	for idx := 0; idx < len(originalNode.Items); idx++ {
		trace_util_0.Count(_bind_00000, 19)
		originalNode.Items[idx].Expr = exprBind(originalNode.Items[idx].Expr, hintedNode.Items[idx].Expr)
	}
	trace_util_0.Count(_bind_00000, 18)
	return originalNode
}

func exprBind(originalNode, hintedNode ast.ExprNode) ast.ExprNode {
	trace_util_0.Count(_bind_00000, 20)
	switch v := originalNode.(type) {
	case *ast.SubqueryExpr:
		trace_util_0.Count(_bind_00000, 22)
		if v.Query != nil {
			trace_util_0.Count(_bind_00000, 37)
			v.Query = resultSetNodeBind(v.Query, hintedNode.(*ast.SubqueryExpr).Query)
		}
	case *ast.ExistsSubqueryExpr:
		trace_util_0.Count(_bind_00000, 23)
		if v.Sel != nil {
			trace_util_0.Count(_bind_00000, 38)
			v.Sel.(*ast.SubqueryExpr).Query = resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedNode.(*ast.ExistsSubqueryExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	case *ast.PatternInExpr:
		trace_util_0.Count(_bind_00000, 24)
		if v.Sel != nil {
			trace_util_0.Count(_bind_00000, 39)
			v.Sel.(*ast.SubqueryExpr).Query = resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedNode.(*ast.PatternInExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	case *ast.BinaryOperationExpr:
		trace_util_0.Count(_bind_00000, 25)
		if v.L != nil {
			trace_util_0.Count(_bind_00000, 40)
			v.L = exprBind(v.L, hintedNode.(*ast.BinaryOperationExpr).L)
		}
		trace_util_0.Count(_bind_00000, 26)
		if v.R != nil {
			trace_util_0.Count(_bind_00000, 41)
			v.R = exprBind(v.R, hintedNode.(*ast.BinaryOperationExpr).R)
		}
	case *ast.IsNullExpr:
		trace_util_0.Count(_bind_00000, 27)
		if v.Expr != nil {
			trace_util_0.Count(_bind_00000, 42)
			v.Expr = exprBind(v.Expr, hintedNode.(*ast.IsNullExpr).Expr)
		}
	case *ast.IsTruthExpr:
		trace_util_0.Count(_bind_00000, 28)
		if v.Expr != nil {
			trace_util_0.Count(_bind_00000, 43)
			v.Expr = exprBind(v.Expr, hintedNode.(*ast.IsTruthExpr).Expr)
		}
	case *ast.PatternLikeExpr:
		trace_util_0.Count(_bind_00000, 29)
		if v.Pattern != nil {
			trace_util_0.Count(_bind_00000, 44)
			v.Pattern = exprBind(v.Pattern, hintedNode.(*ast.PatternLikeExpr).Pattern)
		}
	case *ast.CompareSubqueryExpr:
		trace_util_0.Count(_bind_00000, 30)
		if v.L != nil {
			trace_util_0.Count(_bind_00000, 45)
			v.L = exprBind(v.L, hintedNode.(*ast.CompareSubqueryExpr).L)
		}
		trace_util_0.Count(_bind_00000, 31)
		if v.R != nil {
			trace_util_0.Count(_bind_00000, 46)
			v.R = exprBind(v.R, hintedNode.(*ast.CompareSubqueryExpr).R)
		}
	case *ast.BetweenExpr:
		trace_util_0.Count(_bind_00000, 32)
		if v.Left != nil {
			trace_util_0.Count(_bind_00000, 47)
			v.Left = exprBind(v.Left, hintedNode.(*ast.BetweenExpr).Left)
		}
		trace_util_0.Count(_bind_00000, 33)
		if v.Right != nil {
			trace_util_0.Count(_bind_00000, 48)
			v.Right = exprBind(v.Right, hintedNode.(*ast.BetweenExpr).Right)
		}
	case *ast.UnaryOperationExpr:
		trace_util_0.Count(_bind_00000, 34)
		if v.V != nil {
			trace_util_0.Count(_bind_00000, 49)
			v.V = exprBind(v.V, hintedNode.(*ast.UnaryOperationExpr).V)
		}
	case *ast.CaseExpr:
		trace_util_0.Count(_bind_00000, 35)
		if v.Value != nil {
			trace_util_0.Count(_bind_00000, 50)
			v.Value = exprBind(v.Value, hintedNode.(*ast.CaseExpr).Value)
		}
		trace_util_0.Count(_bind_00000, 36)
		if v.ElseClause != nil {
			trace_util_0.Count(_bind_00000, 51)
			v.ElseClause = exprBind(v.ElseClause, hintedNode.(*ast.CaseExpr).ElseClause)
		}
	}
	trace_util_0.Count(_bind_00000, 21)
	return originalNode
}

func resultSetNodeBind(originalNode, hintedNode ast.ResultSetNode) ast.ResultSetNode {
	trace_util_0.Count(_bind_00000, 52)
	switch x := originalNode.(type) {
	case *ast.Join:
		trace_util_0.Count(_bind_00000, 53)
		return joinBind(x, hintedNode.(*ast.Join))
	case *ast.TableSource:
		trace_util_0.Count(_bind_00000, 54)
		ts, _ := hintedNode.(*ast.TableSource)
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			trace_util_0.Count(_bind_00000, 59)
			x.Source = selectBind(v, ts.Source.(*ast.SelectStmt))
		case *ast.UnionStmt:
			trace_util_0.Count(_bind_00000, 60)
			x.Source = unionSelectBind(v, hintedNode.(*ast.TableSource).Source.(*ast.UnionStmt))
		case *ast.TableName:
			trace_util_0.Count(_bind_00000, 61)
			x.Source.(*ast.TableName).IndexHints = ts.Source.(*ast.TableName).IndexHints
		}
		trace_util_0.Count(_bind_00000, 55)
		return x
	case *ast.SelectStmt:
		trace_util_0.Count(_bind_00000, 56)
		return selectBind(x, hintedNode.(*ast.SelectStmt))
	case *ast.UnionStmt:
		trace_util_0.Count(_bind_00000, 57)
		return unionSelectBind(x, hintedNode.(*ast.UnionStmt))
	default:
		trace_util_0.Count(_bind_00000, 58)
		return x
	}
}

func joinBind(originalNode, hintedNode *ast.Join) *ast.Join {
	trace_util_0.Count(_bind_00000, 62)
	if originalNode.Left != nil {
		trace_util_0.Count(_bind_00000, 65)
		originalNode.Left = resultSetNodeBind(originalNode.Left, hintedNode.Left)
	}

	trace_util_0.Count(_bind_00000, 63)
	if hintedNode.Right != nil {
		trace_util_0.Count(_bind_00000, 66)
		originalNode.Right = resultSetNodeBind(originalNode.Right, hintedNode.Right)
	}

	trace_util_0.Count(_bind_00000, 64)
	return originalNode
}

func unionSelectBind(originalNode, hintedNode *ast.UnionStmt) ast.ResultSetNode {
	trace_util_0.Count(_bind_00000, 67)
	selects := originalNode.SelectList.Selects
	for i := len(selects) - 1; i >= 0; i-- {
		trace_util_0.Count(_bind_00000, 69)
		originalNode.SelectList.Selects[i] = selectBind(selects[i], hintedNode.SelectList.Selects[i])
	}

	trace_util_0.Count(_bind_00000, 68)
	return originalNode
}

var _bind_00000 = "bindinfo/bind.go"
