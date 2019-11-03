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

package core

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types/parser_driver"
)

// Cacheable checks whether the input ast is cacheable.
func Cacheable(node ast.Node) bool {
	trace_util_0.Count(_cacheable_checker_00000, 0)
	_, isSelect := node.(*ast.SelectStmt)
	_, isUpdate := node.(*ast.UpdateStmt)
	_, isInsert := node.(*ast.InsertStmt)
	_, isDelete := node.(*ast.DeleteStmt)
	if !(isSelect || isUpdate || isInsert || isDelete) {
		trace_util_0.Count(_cacheable_checker_00000, 2)
		return false
	}
	trace_util_0.Count(_cacheable_checker_00000, 1)
	checker := cacheableChecker{
		cacheable: true,
	}
	node.Accept(&checker)
	return checker.cacheable
}

// cacheableChecker checks whether a query's plan can be cached, querys that:
//	 1. have ExistsSubqueryExpr, or
//	 2. have VariableExpr
// will not be cached currently.
// NOTE: we can add more rules in the future.
type cacheableChecker struct {
	cacheable bool
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	trace_util_0.Count(_cacheable_checker_00000, 3)
	switch node := in.(type) {
	case *ast.VariableExpr, *ast.ExistsSubqueryExpr, *ast.SubqueryExpr:
		trace_util_0.Count(_cacheable_checker_00000, 5)
		checker.cacheable = false
		return in, true
	case *ast.FuncCallExpr:
		trace_util_0.Count(_cacheable_checker_00000, 6)
		if _, found := expression.UnCacheableFunctions[node.FnName.L]; found {
			trace_util_0.Count(_cacheable_checker_00000, 11)
			checker.cacheable = false
			return in, true
		}
	case *ast.OrderByClause:
		trace_util_0.Count(_cacheable_checker_00000, 7)
		for _, item := range node.Items {
			trace_util_0.Count(_cacheable_checker_00000, 12)
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				trace_util_0.Count(_cacheable_checker_00000, 13)
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.GroupByClause:
		trace_util_0.Count(_cacheable_checker_00000, 8)
		for _, item := range node.Items {
			trace_util_0.Count(_cacheable_checker_00000, 14)
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				trace_util_0.Count(_cacheable_checker_00000, 15)
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.Limit:
		trace_util_0.Count(_cacheable_checker_00000, 9)
		if node.Count != nil {
			trace_util_0.Count(_cacheable_checker_00000, 16)
			if _, isParamMarker := node.Count.(*driver.ParamMarkerExpr); isParamMarker {
				trace_util_0.Count(_cacheable_checker_00000, 17)
				checker.cacheable = false
				return in, true
			}
		}
		trace_util_0.Count(_cacheable_checker_00000, 10)
		if node.Offset != nil {
			trace_util_0.Count(_cacheable_checker_00000, 18)
			if _, isParamMarker := node.Offset.(*driver.ParamMarkerExpr); isParamMarker {
				trace_util_0.Count(_cacheable_checker_00000, 19)
				checker.cacheable = false
				return in, true
			}
		}
	}
	trace_util_0.Count(_cacheable_checker_00000, 4)
	return in, false
}

// Leave implements Visitor interface.
func (checker *cacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	trace_util_0.Count(_cacheable_checker_00000, 20)
	return in, checker.cacheable
}

var _cacheable_checker_00000 = "planner/core/cacheable_checker.go"
