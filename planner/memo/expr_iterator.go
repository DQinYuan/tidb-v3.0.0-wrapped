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
	"container/list"
	"github.com/pingcap/tidb/trace_util_0"
)

// ExprIter enumerates all the equivalent expressions in the Group according to
// the expression pattern.
type ExprIter struct {
	// Group and Element solely identify a Group expression.
	*Group
	*list.Element

	// matched indicates whether the current Group expression binded by the
	// iterator matches the pattern after the creation or iteration.
	matched bool

	// Operand is the node of the pattern tree. The Operand type of the Group
	// expression must be matched with it.
	Operand

	// Children is used to iterate the child expressions.
	Children []*ExprIter
}

// Next returns the next Group expression matches the pattern.
func (iter *ExprIter) Next() (found bool) {
	trace_util_0.Count(_expr_iterator_00000, 0)
	defer func() {
		trace_util_0.Count(_expr_iterator_00000, 5)
		iter.matched = found
	}()

	// Iterate child firstly.
	trace_util_0.Count(_expr_iterator_00000, 1)
	for i := len(iter.Children) - 1; i >= 0; i-- {
		trace_util_0.Count(_expr_iterator_00000, 6)
		if !iter.Children[i].Next() {
			trace_util_0.Count(_expr_iterator_00000, 9)
			continue
		}

		trace_util_0.Count(_expr_iterator_00000, 7)
		for j := i + 1; j < len(iter.Children); j++ {
			trace_util_0.Count(_expr_iterator_00000, 10)
			iter.Children[j].Reset()
		}
		trace_util_0.Count(_expr_iterator_00000, 8)
		return true
	}

	// It's root node.
	trace_util_0.Count(_expr_iterator_00000, 2)
	if iter.Group == nil {
		trace_util_0.Count(_expr_iterator_00000, 11)
		return false
	}

	// Otherwise, iterate itself to find more matched equivalent expressions.
	trace_util_0.Count(_expr_iterator_00000, 3)
	for elem := iter.Element.Next(); elem != nil; elem = elem.Next() {
		trace_util_0.Count(_expr_iterator_00000, 12)
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.ExprNode)

		if !iter.Operand.Match(exprOperand) {
			trace_util_0.Count(_expr_iterator_00000, 16)
			// All the Equivalents which have the same Operand are continuously
			// stored in the list. Once the current equivalent can not Match
			// the Operand, the rest can not, either.
			return false
		}

		trace_util_0.Count(_expr_iterator_00000, 13)
		if len(iter.Children) != len(expr.Children) {
			trace_util_0.Count(_expr_iterator_00000, 17)
			continue
		}

		trace_util_0.Count(_expr_iterator_00000, 14)
		allMatched := true
		for i := range iter.Children {
			trace_util_0.Count(_expr_iterator_00000, 18)
			iter.Children[i].Group = expr.Children[i]
			if !iter.Children[i].Reset() {
				trace_util_0.Count(_expr_iterator_00000, 19)
				allMatched = false
				break
			}
		}

		trace_util_0.Count(_expr_iterator_00000, 15)
		if allMatched {
			trace_util_0.Count(_expr_iterator_00000, 20)
			iter.Element = elem
			return true
		}
	}
	trace_util_0.Count(_expr_iterator_00000, 4)
	return false
}

// Matched returns whether the iterator founds a Group expression matches the
// pattern.
func (iter *ExprIter) Matched() bool {
	trace_util_0.Count(_expr_iterator_00000, 21)
	return iter.matched
}

// Reset resets the iterator to the first matched Group expression.
func (iter *ExprIter) Reset() (findMatch bool) {
	trace_util_0.Count(_expr_iterator_00000, 22)
	defer func() { trace_util_0.Count(_expr_iterator_00000, 25); iter.matched = findMatch }()

	trace_util_0.Count(_expr_iterator_00000, 23)
	for elem := iter.Group.GetFirstElem(iter.Operand); elem != nil; elem = elem.Next() {
		trace_util_0.Count(_expr_iterator_00000, 26)
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.ExprNode)
		if !iter.Operand.Match(exprOperand) {
			trace_util_0.Count(_expr_iterator_00000, 30)
			break
		}

		trace_util_0.Count(_expr_iterator_00000, 27)
		if len(expr.Children) != len(iter.Children) {
			trace_util_0.Count(_expr_iterator_00000, 31)
			continue
		}

		trace_util_0.Count(_expr_iterator_00000, 28)
		allMatched := true
		for i := range iter.Children {
			trace_util_0.Count(_expr_iterator_00000, 32)
			iter.Children[i].Group = expr.Children[i]
			if !iter.Children[i].Reset() {
				trace_util_0.Count(_expr_iterator_00000, 33)
				allMatched = false
				break
			}
		}
		trace_util_0.Count(_expr_iterator_00000, 29)
		if allMatched {
			trace_util_0.Count(_expr_iterator_00000, 34)
			iter.Element = elem
			return true
		}
	}
	trace_util_0.Count(_expr_iterator_00000, 24)
	return false
}

// NewExprIterFromGroupElem creates the iterator on the Group Element.
func NewExprIterFromGroupElem(elem *list.Element, p *Pattern) *ExprIter {
	trace_util_0.Count(_expr_iterator_00000, 35)
	expr := elem.Value.(*GroupExpr)
	if !p.Operand.Match(GetOperand(expr.ExprNode)) {
		trace_util_0.Count(_expr_iterator_00000, 38)
		return nil
	}
	trace_util_0.Count(_expr_iterator_00000, 36)
	iter := newExprIterFromGroupExpr(expr, p)
	if iter != nil {
		trace_util_0.Count(_expr_iterator_00000, 39)
		iter.Element = elem
	}
	trace_util_0.Count(_expr_iterator_00000, 37)
	return iter
}

// newExprIterFromGroupExpr creates the iterator on the Group expression.
func newExprIterFromGroupExpr(expr *GroupExpr, p *Pattern) *ExprIter {
	trace_util_0.Count(_expr_iterator_00000, 40)
	if len(p.Children) != len(expr.Children) {
		trace_util_0.Count(_expr_iterator_00000, 43)
		return nil
	}

	trace_util_0.Count(_expr_iterator_00000, 41)
	iter := &ExprIter{Operand: p.Operand, matched: true}
	for i := range p.Children {
		trace_util_0.Count(_expr_iterator_00000, 44)
		childIter := newExprIterFromGroup(expr.Children[i], p.Children[i])
		if childIter == nil {
			trace_util_0.Count(_expr_iterator_00000, 46)
			return nil
		}
		trace_util_0.Count(_expr_iterator_00000, 45)
		iter.Children = append(iter.Children, childIter)
	}
	trace_util_0.Count(_expr_iterator_00000, 42)
	return iter
}

// newExprIterFromGroup creates the iterator on the Group.
func newExprIterFromGroup(g *Group, p *Pattern) *ExprIter {
	trace_util_0.Count(_expr_iterator_00000, 47)
	for elem := g.GetFirstElem(p.Operand); elem != nil; elem = elem.Next() {
		trace_util_0.Count(_expr_iterator_00000, 49)
		expr := elem.Value.(*GroupExpr)
		if !p.Operand.Match(GetOperand(expr.ExprNode)) {
			trace_util_0.Count(_expr_iterator_00000, 51)
			return nil
		}
		trace_util_0.Count(_expr_iterator_00000, 50)
		iter := newExprIterFromGroupExpr(expr, p)
		if iter != nil {
			trace_util_0.Count(_expr_iterator_00000, 52)
			iter.Group, iter.Element = g, elem
			return iter
		}
	}
	trace_util_0.Count(_expr_iterator_00000, 48)
	return nil
}

var _expr_iterator_00000 = "planner/memo/expr_iterator.go"
