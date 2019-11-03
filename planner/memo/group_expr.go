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

package memo

import (
	"fmt"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
)

// GroupExpr is used to store all the logically equivalent expressions which
// have the same root operator. Different from a normal expression, the
// Children of a Group expression are expression Groups, not expressions.
// Another property of Group expression is that the child Group references will
// never be changed once the Group expression is created.
type GroupExpr struct {
	ExprNode plannercore.LogicalPlan
	Children []*Group
	Explored bool

	selfFingerprint string
}

// NewGroupExpr creates a GroupExpr based on a logical plan node.
func NewGroupExpr(node plannercore.LogicalPlan) *GroupExpr {
	trace_util_0.Count(_group_expr_00000, 0)
	return &GroupExpr{
		ExprNode: node,
		Children: nil,
		Explored: false,
	}
}

// FingerPrint gets the unique fingerprint of the Group expression.
func (e *GroupExpr) FingerPrint() string {
	trace_util_0.Count(_group_expr_00000, 1)
	if e.selfFingerprint == "" {
		trace_util_0.Count(_group_expr_00000, 3)
		e.selfFingerprint = fmt.Sprintf("%v", e.ExprNode.ID())
		for i := range e.Children {
			trace_util_0.Count(_group_expr_00000, 4)
			e.selfFingerprint += e.Children[i].FingerPrint()
		}
	}
	trace_util_0.Count(_group_expr_00000, 2)
	return e.selfFingerprint
}

var _group_expr_00000 = "planner/memo/group_expr.go"
