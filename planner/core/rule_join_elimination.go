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

package core

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

type outerJoinEliminator struct {
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left table and the join key of right table(the inner table) is a unique
//    key of the right table. the left outer join can be eliminated.
// 2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//    If the parent only use the columns from left table with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin, aggCols []*expression.Column, parentSchema *expression.Schema) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 0)
	var innerChildIdx int
	switch p.JoinType {
	case LeftOuterJoin:
		trace_util_0.Count(_rule_join_elimination_00000, 6)
		innerChildIdx = 1
	case RightOuterJoin:
		trace_util_0.Count(_rule_join_elimination_00000, 7)
		innerChildIdx = 0
	default:
		trace_util_0.Count(_rule_join_elimination_00000, 8)
		return p, nil
	}

	trace_util_0.Count(_rule_join_elimination_00000, 1)
	outerPlan := p.children[1^innerChildIdx]
	innerPlan := p.children[innerChildIdx]
	// outer join elimination with duplicate agnostic aggregate functions
	matched, err := o.isAggColsAllFromOuterTable(outerPlan, aggCols)
	if err != nil || matched {
		trace_util_0.Count(_rule_join_elimination_00000, 9)
		return outerPlan, err
	}
	// outer join elimination without duplicate agnostic aggregate functions
	trace_util_0.Count(_rule_join_elimination_00000, 2)
	matched, err = o.isParentColsAllFromOuterTable(outerPlan, parentSchema)
	if err != nil || !matched {
		trace_util_0.Count(_rule_join_elimination_00000, 10)
		return p, err
	}
	trace_util_0.Count(_rule_join_elimination_00000, 3)
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
	if err != nil || contain {
		trace_util_0.Count(_rule_join_elimination_00000, 11)
		return outerPlan, err
	}
	trace_util_0.Count(_rule_join_elimination_00000, 4)
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys)
	if err != nil || contain {
		trace_util_0.Count(_rule_join_elimination_00000, 12)
		return outerPlan, err
	}

	trace_util_0.Count(_rule_join_elimination_00000, 5)
	return p, nil
}

// extract join keys as a schema for inner child of a outer join
func (o *outerJoinEliminator) extractInnerJoinKeys(join *LogicalJoin, innerChildIdx int) *expression.Schema {
	trace_util_0.Count(_rule_join_elimination_00000, 13)
	joinKeys := make([]*expression.Column, 0, len(join.EqualConditions))
	for _, eqCond := range join.EqualConditions {
		trace_util_0.Count(_rule_join_elimination_00000, 15)
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.Column))
	}
	trace_util_0.Count(_rule_join_elimination_00000, 14)
	return expression.NewSchema(joinKeys...)
}

func (o *outerJoinEliminator) isAggColsAllFromOuterTable(outerPlan LogicalPlan, aggCols []*expression.Column) (bool, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 16)
	if len(aggCols) == 0 {
		trace_util_0.Count(_rule_join_elimination_00000, 19)
		return false, nil
	}
	trace_util_0.Count(_rule_join_elimination_00000, 17)
	for _, col := range aggCols {
		trace_util_0.Count(_rule_join_elimination_00000, 20)
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		c, err := outerPlan.Schema().FindColumn(columnName)
		if err != nil || c == nil {
			trace_util_0.Count(_rule_join_elimination_00000, 21)
			return false, err
		}
	}
	trace_util_0.Count(_rule_join_elimination_00000, 18)
	return true, nil
}

// check whether schema cols of join's parent plan are all from outer join table
func (o *outerJoinEliminator) isParentColsAllFromOuterTable(outerPlan LogicalPlan, parentSchema *expression.Schema) (bool, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 22)
	if parentSchema == nil {
		trace_util_0.Count(_rule_join_elimination_00000, 25)
		return false, nil
	}
	trace_util_0.Count(_rule_join_elimination_00000, 23)
	for _, col := range parentSchema.Columns {
		trace_util_0.Count(_rule_join_elimination_00000, 26)
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		c, err := outerPlan.Schema().FindColumn(columnName)
		if err != nil || c == nil {
			trace_util_0.Count(_rule_join_elimination_00000, 27)
			return false, err
		}
	}
	trace_util_0.Count(_rule_join_elimination_00000, 24)
	return true, nil
}

// check whether one of unique keys sets is contained by inner join keys
func (o *outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 28)
	for _, keyInfo := range innerPlan.Schema().Keys {
		trace_util_0.Count(_rule_join_elimination_00000, 30)
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			trace_util_0.Count(_rule_join_elimination_00000, 32)
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			c, err := joinKeys.FindColumn(columnName)
			if err != nil {
				trace_util_0.Count(_rule_join_elimination_00000, 34)
				return false, err
			}
			trace_util_0.Count(_rule_join_elimination_00000, 33)
			if c == nil {
				trace_util_0.Count(_rule_join_elimination_00000, 35)
				joinKeysContainKeyInfo = false
				break
			}
		}
		trace_util_0.Count(_rule_join_elimination_00000, 31)
		if joinKeysContainKeyInfo {
			trace_util_0.Count(_rule_join_elimination_00000, 36)
			return true, nil
		}
	}
	trace_util_0.Count(_rule_join_elimination_00000, 29)
	return false, nil
}

// check whether one of index sets is contained by inner join index
func (o *outerJoinEliminator) isInnerJoinKeysContainIndex(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 37)
	ds, ok := innerPlan.(*DataSource)
	if !ok {
		trace_util_0.Count(_rule_join_elimination_00000, 40)
		return false, nil
	}
	trace_util_0.Count(_rule_join_elimination_00000, 38)
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_rule_join_elimination_00000, 41)
		if path.isTablePath {
			trace_util_0.Count(_rule_join_elimination_00000, 45)
			continue
		}
		trace_util_0.Count(_rule_join_elimination_00000, 42)
		idx := path.index
		if !idx.Unique {
			trace_util_0.Count(_rule_join_elimination_00000, 46)
			continue
		}
		trace_util_0.Count(_rule_join_elimination_00000, 43)
		joinKeysContainIndex := true
		for _, idxCol := range idx.Columns {
			trace_util_0.Count(_rule_join_elimination_00000, 47)
			columnName := &ast.ColumnName{Schema: ds.DBName, Table: ds.tableInfo.Name, Name: idxCol.Name}
			c, err := joinKeys.FindColumn(columnName)
			if err != nil {
				trace_util_0.Count(_rule_join_elimination_00000, 49)
				return false, err
			}
			trace_util_0.Count(_rule_join_elimination_00000, 48)
			if c == nil {
				trace_util_0.Count(_rule_join_elimination_00000, 50)
				joinKeysContainIndex = false
				break
			}
		}
		trace_util_0.Count(_rule_join_elimination_00000, 44)
		if joinKeysContainIndex {
			trace_util_0.Count(_rule_join_elimination_00000, 51)
			return true, nil
		}
	}
	trace_util_0.Count(_rule_join_elimination_00000, 39)
	return false, nil
}

// Check whether a LogicalPlan is a LogicalAggregation and its all aggregate functions is duplicate agnostic.
// Also, check all the args are expression.Column.
func (o *outerJoinEliminator) isDuplicateAgnosticAgg(p LogicalPlan) (_ bool, cols []*expression.Column) {
	trace_util_0.Count(_rule_join_elimination_00000, 52)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		trace_util_0.Count(_rule_join_elimination_00000, 55)
		return false, nil
	}
	trace_util_0.Count(_rule_join_elimination_00000, 53)
	cols = agg.groupByCols
	for _, aggDesc := range agg.AggFuncs {
		trace_util_0.Count(_rule_join_elimination_00000, 56)
		if !aggDesc.HasDistinct &&
			aggDesc.Name != ast.AggFuncFirstRow &&
			aggDesc.Name != ast.AggFuncMax &&
			aggDesc.Name != ast.AggFuncMin {
			trace_util_0.Count(_rule_join_elimination_00000, 58)
			return false, nil
		}
		trace_util_0.Count(_rule_join_elimination_00000, 57)
		for _, expr := range aggDesc.Args {
			trace_util_0.Count(_rule_join_elimination_00000, 59)
			if col, ok := expr.(*expression.Column); ok {
				trace_util_0.Count(_rule_join_elimination_00000, 60)
				cols = append(cols, col)
			} else {
				trace_util_0.Count(_rule_join_elimination_00000, 61)
				{
					return false, nil
				}
			}
		}
	}
	trace_util_0.Count(_rule_join_elimination_00000, 54)
	return true, cols
}

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, aggCols []*expression.Column, parentSchema *expression.Schema) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 62)
	// check the duplicate agnostic aggregate functions
	if ok, newCols := o.isDuplicateAgnosticAgg(p); ok {
		trace_util_0.Count(_rule_join_elimination_00000, 66)
		aggCols = newCols
	}

	trace_util_0.Count(_rule_join_elimination_00000, 63)
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_join_elimination_00000, 67)
		newChild, err := o.doOptimize(child, aggCols, p.Schema())
		if err != nil {
			trace_util_0.Count(_rule_join_elimination_00000, 69)
			return nil, err
		}
		trace_util_0.Count(_rule_join_elimination_00000, 68)
		newChildren = append(newChildren, newChild)
	}
	trace_util_0.Count(_rule_join_elimination_00000, 64)
	p.SetChildren(newChildren...)
	join, isJoin := p.(*LogicalJoin)
	if !isJoin {
		trace_util_0.Count(_rule_join_elimination_00000, 70)
		return p, nil
	}
	trace_util_0.Count(_rule_join_elimination_00000, 65)
	return o.tryToEliminateOuterJoin(join, aggCols, parentSchema)
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_join_elimination_00000, 71)
	return o.doOptimize(p, nil, nil)
}

var _rule_join_elimination_00000 = "planner/core/rule_join_elimination.go"
