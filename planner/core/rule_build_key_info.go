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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

type buildKeySolver struct{}

func (s *buildKeySolver) optimize(lp LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_build_key_info_00000, 0)
	lp.buildKeyInfo()
	return lp, nil
}

func (la *LogicalAggregation) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 1)
	la.schema.Keys = nil
	la.baseLogicalPlan.buildKeyInfo()
	for _, key := range la.Children()[0].Schema().Keys {
		trace_util_0.Count(_rule_build_key_info_00000, 4)
		indices := la.schema.ColumnsIndices(key)
		if indices == nil {
			trace_util_0.Count(_rule_build_key_info_00000, 7)
			continue
		}
		trace_util_0.Count(_rule_build_key_info_00000, 5)
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			trace_util_0.Count(_rule_build_key_info_00000, 8)
			newKey = append(newKey, la.schema.Columns[i])
		}
		trace_util_0.Count(_rule_build_key_info_00000, 6)
		la.schema.Keys = append(la.schema.Keys, newKey)
	}
	trace_util_0.Count(_rule_build_key_info_00000, 2)
	if len(la.groupByCols) == len(la.GroupByItems) && len(la.GroupByItems) > 0 {
		trace_util_0.Count(_rule_build_key_info_00000, 9)
		indices := la.schema.ColumnsIndices(la.groupByCols)
		if indices != nil {
			trace_util_0.Count(_rule_build_key_info_00000, 10)
			newKey := make([]*expression.Column, 0, len(indices))
			for _, i := range indices {
				trace_util_0.Count(_rule_build_key_info_00000, 12)
				newKey = append(newKey, la.schema.Columns[i])
			}
			trace_util_0.Count(_rule_build_key_info_00000, 11)
			la.schema.Keys = append(la.schema.Keys, newKey)
		}
	}
	trace_util_0.Count(_rule_build_key_info_00000, 3)
	if len(la.GroupByItems) == 0 {
		trace_util_0.Count(_rule_build_key_info_00000, 13)
		la.maxOneRow = true
	}
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one row.
// This function will check it.
func (p *LogicalSelection) checkMaxOneRowCond(unique expression.Expression, constOrCorCol expression.Expression) bool {
	trace_util_0.Count(_rule_build_key_info_00000, 14)
	col, ok := unique.(*expression.Column)
	if !ok {
		trace_util_0.Count(_rule_build_key_info_00000, 18)
		return false
	}
	trace_util_0.Count(_rule_build_key_info_00000, 15)
	if !p.children[0].Schema().IsUniqueKey(col) {
		trace_util_0.Count(_rule_build_key_info_00000, 19)
		return false
	}
	trace_util_0.Count(_rule_build_key_info_00000, 16)
	_, okCon := constOrCorCol.(*expression.Constant)
	if okCon {
		trace_util_0.Count(_rule_build_key_info_00000, 20)
		return true
	}
	trace_util_0.Count(_rule_build_key_info_00000, 17)
	_, okCorCol := constOrCorCol.(*expression.CorrelatedColumn)
	return okCorCol
}

func (p *LogicalSelection) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 21)
	p.baseLogicalPlan.buildKeyInfo()
	for _, cond := range p.Conditions {
		trace_util_0.Count(_rule_build_key_info_00000, 22)
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			trace_util_0.Count(_rule_build_key_info_00000, 23)
			if p.checkMaxOneRowCond(sf.GetArgs()[0], sf.GetArgs()[1]) || p.checkMaxOneRowCond(sf.GetArgs()[1], sf.GetArgs()[0]) {
				trace_util_0.Count(_rule_build_key_info_00000, 24)
				p.maxOneRow = true
				break
			}
		}
	}
}

func (p *LogicalLimit) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 25)
	p.baseLogicalPlan.buildKeyInfo()
	if p.Count == 1 {
		trace_util_0.Count(_rule_build_key_info_00000, 26)
		p.maxOneRow = true
	}
}

// A bijection exists between columns of a projection's schema and this projection's Exprs.
// Sometimes we need a schema made by expr of Exprs to convert a column in child's schema to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs() *expression.Schema {
	trace_util_0.Count(_rule_build_key_info_00000, 27)
	schema := expression.NewSchema(make([]*expression.Column, 0, p.schema.Len())...)
	for _, expr := range p.Exprs {
		trace_util_0.Count(_rule_build_key_info_00000, 29)
		if col, isCol := expr.(*expression.Column); isCol {
			trace_util_0.Count(_rule_build_key_info_00000, 30)
			schema.Append(col)
		} else {
			trace_util_0.Count(_rule_build_key_info_00000, 31)
			{
				// If the expression is not a column, we add a column to occupy the position.
				schema.Append(&expression.Column{
					UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  expr.GetType(),
				})
			}
		}
	}
	trace_util_0.Count(_rule_build_key_info_00000, 28)
	return schema
}

func (p *LogicalProjection) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 32)
	p.schema.Keys = nil
	p.baseLogicalPlan.buildKeyInfo()
	schema := p.buildSchemaByExprs()
	for _, key := range p.Children()[0].Schema().Keys {
		trace_util_0.Count(_rule_build_key_info_00000, 33)
		indices := schema.ColumnsIndices(key)
		if indices == nil {
			trace_util_0.Count(_rule_build_key_info_00000, 36)
			continue
		}
		trace_util_0.Count(_rule_build_key_info_00000, 34)
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			trace_util_0.Count(_rule_build_key_info_00000, 37)
			newKey = append(newKey, p.schema.Columns[i])
		}
		trace_util_0.Count(_rule_build_key_info_00000, 35)
		p.schema.Keys = append(p.schema.Keys, newKey)
	}
}

func (p *LogicalJoin) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 38)
	p.schema.Keys = nil
	p.baseLogicalPlan.buildKeyInfo()
	p.maxOneRow = p.children[0].MaxOneRow() && p.children[1].MaxOneRow()
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin:
		trace_util_0.Count(_rule_build_key_info_00000, 39)
		p.schema.Keys = p.children[0].Schema().Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		trace_util_0.Count(_rule_build_key_info_00000, 40)
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			trace_util_0.Count(_rule_build_key_info_00000, 44)
			return
		}
		trace_util_0.Count(_rule_build_key_info_00000, 41)
		lOk := false
		rOk := false
		// Such as 'select * from t1 join t2 where t1.a = t2.a and t1.b = t2.b'.
		// If one sides (a, b) is a unique key, then the unique key information is remained.
		// But we don't consider this situation currently.
		// Only key made by one column is considered now.
		for _, expr := range p.EqualConditions {
			trace_util_0.Count(_rule_build_key_info_00000, 45)
			ln := expr.GetArgs()[0].(*expression.Column)
			rn := expr.GetArgs()[1].(*expression.Column)
			for _, key := range p.children[0].Schema().Keys {
				trace_util_0.Count(_rule_build_key_info_00000, 47)
				if len(key) == 1 && key[0].Equal(p.ctx, ln) {
					trace_util_0.Count(_rule_build_key_info_00000, 48)
					lOk = true
					break
				}
			}
			trace_util_0.Count(_rule_build_key_info_00000, 46)
			for _, key := range p.children[1].Schema().Keys {
				trace_util_0.Count(_rule_build_key_info_00000, 49)
				if len(key) == 1 && key[0].Equal(p.ctx, rn) {
					trace_util_0.Count(_rule_build_key_info_00000, 50)
					rOk = true
					break
				}
			}
		}
		// For inner join, if one side of one equal condition is unique key,
		// another side's unique key information will all be reserved.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		trace_util_0.Count(_rule_build_key_info_00000, 42)
		if lOk && p.JoinType != LeftOuterJoin {
			trace_util_0.Count(_rule_build_key_info_00000, 51)
			p.schema.Keys = append(p.schema.Keys, p.children[1].Schema().Keys...)
		}
		trace_util_0.Count(_rule_build_key_info_00000, 43)
		if rOk && p.JoinType != RightOuterJoin {
			trace_util_0.Count(_rule_build_key_info_00000, 52)
			p.schema.Keys = append(p.schema.Keys, p.children[0].Schema().Keys...)
		}
	}
}

func (ds *DataSource) buildKeyInfo() {
	trace_util_0.Count(_rule_build_key_info_00000, 53)
	ds.schema.Keys = nil
	ds.baseLogicalPlan.buildKeyInfo()
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_rule_build_key_info_00000, 55)
		if path.isTablePath {
			trace_util_0.Count(_rule_build_key_info_00000, 59)
			continue
		}
		trace_util_0.Count(_rule_build_key_info_00000, 56)
		idx := path.index
		if !idx.Unique {
			trace_util_0.Count(_rule_build_key_info_00000, 60)
			continue
		}
		trace_util_0.Count(_rule_build_key_info_00000, 57)
		newKey := make([]*expression.Column, 0, len(idx.Columns))
		ok := true
		for _, idxCol := range idx.Columns {
			trace_util_0.Count(_rule_build_key_info_00000, 61)
			// The columns of this index should all occur in column schema.
			// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
			find := false
			for i, col := range ds.schema.Columns {
				trace_util_0.Count(_rule_build_key_info_00000, 63)
				if idxCol.Name.L == col.ColName.L {
					trace_util_0.Count(_rule_build_key_info_00000, 64)
					if !mysql.HasNotNullFlag(ds.Columns[i].Flag) {
						trace_util_0.Count(_rule_build_key_info_00000, 66)
						break
					}
					trace_util_0.Count(_rule_build_key_info_00000, 65)
					newKey = append(newKey, ds.schema.Columns[i])
					find = true
					break
				}
			}
			trace_util_0.Count(_rule_build_key_info_00000, 62)
			if !find {
				trace_util_0.Count(_rule_build_key_info_00000, 67)
				ok = false
				break
			}
		}
		trace_util_0.Count(_rule_build_key_info_00000, 58)
		if ok {
			trace_util_0.Count(_rule_build_key_info_00000, 68)
			ds.schema.Keys = append(ds.schema.Keys, newKey)
		}
	}
	trace_util_0.Count(_rule_build_key_info_00000, 54)
	if ds.tableInfo.PKIsHandle {
		trace_util_0.Count(_rule_build_key_info_00000, 69)
		for i, col := range ds.Columns {
			trace_util_0.Count(_rule_build_key_info_00000, 70)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_rule_build_key_info_00000, 71)
				ds.schema.Keys = append(ds.schema.Keys, []*expression.Column{ds.schema.Columns[i]})
				break
			}
		}
	}
}

var _rule_build_key_info_00000 = "planner/core/rule_build_key_info.go"
