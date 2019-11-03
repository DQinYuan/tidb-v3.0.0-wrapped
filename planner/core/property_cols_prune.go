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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

func (ds *DataSource) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 0)
	result := make([][]*expression.Column, 0, len(ds.possibleAccessPaths))

	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_property_cols_prune_00000, 2)
		if path.isTablePath {
			trace_util_0.Count(_property_cols_prune_00000, 4)
			col := ds.getPKIsHandleCol()
			if col != nil {
				trace_util_0.Count(_property_cols_prune_00000, 6)
				result = append(result, []*expression.Column{col})
			}
			trace_util_0.Count(_property_cols_prune_00000, 5)
			continue
		}
		trace_util_0.Count(_property_cols_prune_00000, 3)
		if len(path.idxCols) > 0 {
			trace_util_0.Count(_property_cols_prune_00000, 7)
			result = append(result, path.idxCols)
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 1)
	return result
}

func (p *LogicalSelection) preparePossibleProperties() (result [][]*expression.Column) {
	trace_util_0.Count(_property_cols_prune_00000, 8)
	return p.children[0].preparePossibleProperties()
}

func (p *LogicalSort) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 9)
	p.children[0].preparePossibleProperties()
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		trace_util_0.Count(_property_cols_prune_00000, 11)
		return nil
	}
	trace_util_0.Count(_property_cols_prune_00000, 10)
	return [][]*expression.Column{propCols}
}

func (p *LogicalTopN) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 12)
	p.children[0].preparePossibleProperties()
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		trace_util_0.Count(_property_cols_prune_00000, 14)
		return nil
	}
	trace_util_0.Count(_property_cols_prune_00000, 13)
	return [][]*expression.Column{propCols}
}

func getPossiblePropertyFromByItems(items []*ByItems) []*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 15)
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		trace_util_0.Count(_property_cols_prune_00000, 17)
		if col, ok := item.Expr.(*expression.Column); ok {
			trace_util_0.Count(_property_cols_prune_00000, 18)
			cols = append(cols, col)
		} else {
			trace_util_0.Count(_property_cols_prune_00000, 19)
			{
				break
			}
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 16)
	return cols
}

func (p *baseLogicalPlan) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 20)
	for _, ch := range p.children {
		trace_util_0.Count(_property_cols_prune_00000, 22)
		ch.preparePossibleProperties()
	}
	trace_util_0.Count(_property_cols_prune_00000, 21)
	return nil
}

func (p *LogicalProjection) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 23)
	childProperties := p.children[0].preparePossibleProperties()
	oldCols := make([]*expression.Column, 0, p.schema.Len())
	newCols := make([]*expression.Column, 0, p.schema.Len())
	for i, expr := range p.Exprs {
		trace_util_0.Count(_property_cols_prune_00000, 26)
		if col, ok := expr.(*expression.Column); ok {
			trace_util_0.Count(_property_cols_prune_00000, 27)
			newCols = append(newCols, p.schema.Columns[i])
			oldCols = append(oldCols, col)
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 24)
	tmpSchema := expression.NewSchema(oldCols...)
	for i := len(childProperties) - 1; i >= 0; i-- {
		trace_util_0.Count(_property_cols_prune_00000, 28)
		for j, col := range childProperties[i] {
			trace_util_0.Count(_property_cols_prune_00000, 30)
			pos := tmpSchema.ColumnIndex(col)
			if pos >= 0 {
				trace_util_0.Count(_property_cols_prune_00000, 31)
				childProperties[i][j] = newCols[pos]
			} else {
				trace_util_0.Count(_property_cols_prune_00000, 32)
				{
					childProperties[i] = childProperties[i][:j]
					break
				}
			}
		}
		trace_util_0.Count(_property_cols_prune_00000, 29)
		if len(childProperties[i]) == 0 {
			trace_util_0.Count(_property_cols_prune_00000, 33)
			childProperties = append(childProperties[:i], childProperties[i+1:]...)
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 25)
	return childProperties
}

func (p *LogicalJoin) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 34)
	leftProperties := p.children[0].preparePossibleProperties()
	rightProperties := p.children[1].preparePossibleProperties()
	// TODO: We should consider properties propagation.
	p.leftProperties = leftProperties
	p.rightProperties = rightProperties
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin {
		trace_util_0.Count(_property_cols_prune_00000, 38)
		rightProperties = nil
	} else {
		trace_util_0.Count(_property_cols_prune_00000, 39)
		if p.JoinType == RightOuterJoin {
			trace_util_0.Count(_property_cols_prune_00000, 40)
			leftProperties = nil
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 35)
	resultProperties := make([][]*expression.Column, len(leftProperties)+len(rightProperties))
	for i, cols := range leftProperties {
		trace_util_0.Count(_property_cols_prune_00000, 41)
		resultProperties[i] = make([]*expression.Column, len(cols))
		copy(resultProperties[i], cols)
	}
	trace_util_0.Count(_property_cols_prune_00000, 36)
	leftLen := len(leftProperties)
	for i, cols := range rightProperties {
		trace_util_0.Count(_property_cols_prune_00000, 42)
		resultProperties[leftLen+i] = make([]*expression.Column, len(cols))
		copy(resultProperties[leftLen+i], cols)
	}
	trace_util_0.Count(_property_cols_prune_00000, 37)
	return resultProperties
}

func (la *LogicalAggregation) preparePossibleProperties() [][]*expression.Column {
	trace_util_0.Count(_property_cols_prune_00000, 43)
	childProps := la.children[0].preparePossibleProperties()
	// If there's no group-by item, the stream aggregation could have no order property. So we can add an empty property
	// when its group-by item is empty.
	if len(la.GroupByItems) == 0 {
		trace_util_0.Count(_property_cols_prune_00000, 45)
		la.possibleProperties = [][]*expression.Column{nil}
	} else {
		trace_util_0.Count(_property_cols_prune_00000, 46)
		{
			la.possibleProperties = childProps
		}
	}
	trace_util_0.Count(_property_cols_prune_00000, 44)
	return nil
}

var _property_cols_prune_00000 = "planner/core/property_cols_prune.go"
