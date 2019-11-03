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
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

// pushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type pushDownTopNOptimizer struct {
}

func (s *pushDownTopNOptimizer) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_topn_push_down_00000, 0)
	return p.pushDownTopN(nil), nil
}

func (s *baseLogicalPlan) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 1)
	p := s.self
	for i, child := range p.Children() {
		trace_util_0.Count(_rule_topn_push_down_00000, 4)
		p.Children()[i] = child.pushDownTopN(nil)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 2)
	if topN != nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 5)
		return topN.setChild(p)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 3)
	return p
}

// setChild set p as topn's child.
func (lt *LogicalTopN) setChild(p LogicalPlan) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 6)
	// Remove this TopN if its child is a TableDual.
	dual, isDual := p.(*LogicalTableDual)
	if isDual {
		trace_util_0.Count(_rule_topn_push_down_00000, 9)
		numDualRows := uint64(dual.RowCount)
		if numDualRows < lt.Offset {
			trace_util_0.Count(_rule_topn_push_down_00000, 11)
			dual.RowCount = 0
			return dual
		}
		trace_util_0.Count(_rule_topn_push_down_00000, 10)
		dual.RowCount = int(mathutil.MinUint64(numDualRows-lt.Offset, lt.Count))
		return dual
	}

	trace_util_0.Count(_rule_topn_push_down_00000, 7)
	if lt.isLimit() {
		trace_util_0.Count(_rule_topn_push_down_00000, 12)
		limit := LogicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.Init(lt.ctx)
		limit.SetChildren(p)
		return limit
	}
	// Then lt must be topN.
	trace_util_0.Count(_rule_topn_push_down_00000, 8)
	lt.SetChildren(p)
	return lt
}

func (ls *LogicalSort) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 13)
	if topN == nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 15)
		return ls.baseLogicalPlan.pushDownTopN(nil)
	} else {
		trace_util_0.Count(_rule_topn_push_down_00000, 16)
		if topN.isLimit() {
			trace_util_0.Count(_rule_topn_push_down_00000, 17)
			topN.ByItems = ls.ByItems
			return ls.children[0].pushDownTopN(topN)
		}
	}
	// If a TopN is pushed down, this sort is useless.
	trace_util_0.Count(_rule_topn_push_down_00000, 14)
	return ls.children[0].pushDownTopN(topN)
}

func (p *LogicalLimit) convertToTopN() *LogicalTopN {
	trace_util_0.Count(_rule_topn_push_down_00000, 18)
	return LogicalTopN{Offset: p.Offset, Count: p.Count}.Init(p.ctx)
}

func (p *LogicalLimit) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 19)
	child := p.children[0].pushDownTopN(p.convertToTopN())
	if topN != nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 21)
		return topN.setChild(child)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 20)
	return child
}

func (p *LogicalUnionAll) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 22)
	for i, child := range p.children {
		trace_util_0.Count(_rule_topn_push_down_00000, 25)
		var newTopN *LogicalTopN
		if topN != nil {
			trace_util_0.Count(_rule_topn_push_down_00000, 27)
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset}.Init(p.ctx)
			for _, by := range topN.ByItems {
				trace_util_0.Count(_rule_topn_push_down_00000, 28)
				newTopN.ByItems = append(newTopN.ByItems, &ByItems{by.Expr, by.Desc})
			}
		}
		trace_util_0.Count(_rule_topn_push_down_00000, 26)
		p.children[i] = child.pushDownTopN(newTopN)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 23)
	if topN != nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 29)
		return topN.setChild(p)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 24)
	return p
}

func (p *LogicalProjection) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 30)
	if topN != nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 32)
		for _, by := range topN.ByItems {
			trace_util_0.Count(_rule_topn_push_down_00000, 34)
			by.Expr = expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs)
		}

		// remove meaningless constant sort items.
		trace_util_0.Count(_rule_topn_push_down_00000, 33)
		for i := len(topN.ByItems) - 1; i >= 0; i-- {
			trace_util_0.Count(_rule_topn_push_down_00000, 35)
			switch topN.ByItems[i].Expr.(type) {
			case *expression.Constant, *expression.CorrelatedColumn:
				trace_util_0.Count(_rule_topn_push_down_00000, 36)
				topN.ByItems = append(topN.ByItems[:i], topN.ByItems[i+1:]...)
			}
		}
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 31)
	p.children[0] = p.children[0].pushDownTopN(topN)
	return p
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 37)
	if topN == nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 41)
		return p.children[idx].pushDownTopN(nil)
	}

	trace_util_0.Count(_rule_topn_push_down_00000, 38)
	for _, by := range topN.ByItems {
		trace_util_0.Count(_rule_topn_push_down_00000, 42)
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			trace_util_0.Count(_rule_topn_push_down_00000, 43)
			if p.children[1-idx].Schema().Contains(col) {
				trace_util_0.Count(_rule_topn_push_down_00000, 44)
				return p.children[idx].pushDownTopN(nil)
			}
		}
	}

	trace_util_0.Count(_rule_topn_push_down_00000, 39)
	newTopN := LogicalTopN{
		Count:   topN.Count + topN.Offset,
		ByItems: make([]*ByItems, len(topN.ByItems)),
	}.Init(topN.ctx)
	for i := range topN.ByItems {
		trace_util_0.Count(_rule_topn_push_down_00000, 45)
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 40)
	return p.children[idx].pushDownTopN(newTopN)
}

func (p *LogicalJoin) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	trace_util_0.Count(_rule_topn_push_down_00000, 46)
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		trace_util_0.Count(_rule_topn_push_down_00000, 49)
		p.children[0] = p.pushDownTopNToChild(topN, 0)
		p.children[1] = p.children[1].pushDownTopN(nil)
	case RightOuterJoin:
		trace_util_0.Count(_rule_topn_push_down_00000, 50)
		p.children[1] = p.pushDownTopNToChild(topN, 1)
		p.children[0] = p.children[0].pushDownTopN(nil)
	default:
		trace_util_0.Count(_rule_topn_push_down_00000, 51)
		return p.baseLogicalPlan.pushDownTopN(topN)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	trace_util_0.Count(_rule_topn_push_down_00000, 47)
	if topN != nil {
		trace_util_0.Count(_rule_topn_push_down_00000, 52)
		return topN.setChild(p.self)
	}
	trace_util_0.Count(_rule_topn_push_down_00000, 48)
	return p.self
}

var _rule_topn_push_down_00000 = "planner/core/rule_topn_push_down.go"
