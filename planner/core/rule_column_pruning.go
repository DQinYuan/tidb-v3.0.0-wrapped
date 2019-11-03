// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/trace_util_0"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(lp LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_column_pruning_00000, 0)
	err := lp.PruneColumns(lp.Schema().Columns)
	return lp, err
}

func getUsedList(usedCols []*expression.Column, schema *expression.Schema) ([]bool, error) {
	trace_util_0.Count(_rule_column_pruning_00000, 1)
	failpoint.Inject("enableGetUsedListErr", func(val failpoint.Value) {
		trace_util_0.Count(_rule_column_pruning_00000, 4)
		if val.(bool) {
			trace_util_0.Count(_rule_column_pruning_00000, 5)
			failpoint.Return(nil, errors.New("getUsedList failed, triggered by gofail enableGetUsedListErr"))
		}
	})

	trace_util_0.Count(_rule_column_pruning_00000, 2)
	used := make([]bool, schema.Len())
	for _, col := range usedCols {
		trace_util_0.Count(_rule_column_pruning_00000, 6)
		idx := schema.ColumnIndex(col)
		if idx == -1 {
			trace_util_0.Count(_rule_column_pruning_00000, 8)
			return nil, errors.Errorf("Can't find column %s from schema %s.", col, schema)
		}
		trace_util_0.Count(_rule_column_pruning_00000, 7)
		used[idx] = true
	}
	trace_util_0.Count(_rule_column_pruning_00000, 3)
	return used, nil
}

// exprHasSetVar checks if the expression has SetVar function.
func exprHasSetVar(expr expression.Expression) bool {
	trace_util_0.Count(_rule_column_pruning_00000, 9)
	scalaFunc, isScalaFunc := expr.(*expression.ScalarFunction)
	if !isScalaFunc {
		trace_util_0.Count(_rule_column_pruning_00000, 13)
		return false
	}
	trace_util_0.Count(_rule_column_pruning_00000, 10)
	if scalaFunc.FuncName.L == ast.SetVar {
		trace_util_0.Count(_rule_column_pruning_00000, 14)
		return true
	}
	trace_util_0.Count(_rule_column_pruning_00000, 11)
	for _, arg := range scalaFunc.GetArgs() {
		trace_util_0.Count(_rule_column_pruning_00000, 15)
		if exprHasSetVar(arg) {
			trace_util_0.Count(_rule_column_pruning_00000, 16)
			return true
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 12)
	return false
}

// PruneColumns implements LogicalPlan interface.
// If any expression has SetVar functions, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 17)
	child := p.children[0]
	used, err := getUsedList(parentUsedCols, p.schema)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 21)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 18)
	for i := len(used) - 1; i >= 0; i-- {
		trace_util_0.Count(_rule_column_pruning_00000, 22)
		if !used[i] && !exprHasSetVar(p.Exprs[i]) {
			trace_util_0.Count(_rule_column_pruning_00000, 23)
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	// Prune TblID2Handle since that handle column may be pruned.
	trace_util_0.Count(_rule_column_pruning_00000, 19)
	for k, cols := range p.schema.TblID2Handle {
		trace_util_0.Count(_rule_column_pruning_00000, 24)
		if p.schema.ColumnIndex(cols[0]) == -1 {
			trace_util_0.Count(_rule_column_pruning_00000, 25)
			delete(p.schema.TblID2Handle, k)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 20)
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 26)
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	return child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 27)
	child := la.children[0]
	used, err := getUsedList(parentUsedCols, la.Schema())
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 32)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 28)
	for i := len(used) - 1; i >= 0; i-- {
		trace_util_0.Count(_rule_column_pruning_00000, 33)
		if !used[i] {
			trace_util_0.Count(_rule_column_pruning_00000, 34)
			la.schema.Columns = append(la.schema.Columns[:i], la.schema.Columns[i+1:]...)
			la.AggFuncs = append(la.AggFuncs[:i], la.AggFuncs[i+1:]...)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 29)
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range la.AggFuncs {
		trace_util_0.Count(_rule_column_pruning_00000, 35)
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.Args, nil)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 30)
	if len(la.GroupByItems) > 0 {
		trace_util_0.Count(_rule_column_pruning_00000, 36)
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			trace_util_0.Count(_rule_column_pruning_00000, 38)
			cols := expression.ExtractColumns(la.GroupByItems[i])
			if len(cols) == 0 {
				trace_util_0.Count(_rule_column_pruning_00000, 39)
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				trace_util_0.Count(_rule_column_pruning_00000, 40)
				{
					selfUsedCols = append(selfUsedCols, cols...)
				}
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		trace_util_0.Count(_rule_column_pruning_00000, 37)
		if len(la.GroupByItems) == 0 {
			trace_util_0.Count(_rule_column_pruning_00000, 41)
			la.GroupByItems = []expression.Expression{expression.One}
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 31)
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 42)
	child := ls.children[0]
	for i := len(ls.ByItems) - 1; i >= 0; i-- {
		trace_util_0.Count(_rule_column_pruning_00000, 44)
		cols := expression.ExtractColumns(ls.ByItems[i].Expr)
		if len(cols) == 0 {
			trace_util_0.Count(_rule_column_pruning_00000, 45)
			if !ls.ByItems[i].Expr.ConstItem() {
				trace_util_0.Count(_rule_column_pruning_00000, 47)
				continue
			}
			trace_util_0.Count(_rule_column_pruning_00000, 46)
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else {
			trace_util_0.Count(_rule_column_pruning_00000, 48)
			if ls.ByItems[i].Expr.GetType().Tp == mysql.TypeNull {
				trace_util_0.Count(_rule_column_pruning_00000, 49)
				ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
			} else {
				trace_util_0.Count(_rule_column_pruning_00000, 50)
				{
					parentUsedCols = append(parentUsedCols, cols...)
				}
			}
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 43)
	return child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 51)
	used, err := getUsedList(parentUsedCols, p.schema)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 56)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 52)
	hasBeenUsed := false
	for i := range used {
		trace_util_0.Count(_rule_column_pruning_00000, 57)
		hasBeenUsed = hasBeenUsed || used[i]
	}
	trace_util_0.Count(_rule_column_pruning_00000, 53)
	if !hasBeenUsed {
		trace_util_0.Count(_rule_column_pruning_00000, 58)
		parentUsedCols = make([]*expression.Column, len(p.schema.Columns))
		copy(parentUsedCols, p.schema.Columns)
	} else {
		trace_util_0.Count(_rule_column_pruning_00000, 59)
		{
			// Issue 10341: p.schema.Columns might contain table name (AsName), but p.Children()0].Schema().Columns does not.
			for i := len(used) - 1; i >= 0; i-- {
				trace_util_0.Count(_rule_column_pruning_00000, 60)
				if !used[i] {
					trace_util_0.Count(_rule_column_pruning_00000, 61)
					p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
				}
			}
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 54)
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_column_pruning_00000, 62)
		err := child.PruneColumns(parentUsedCols)
		if err != nil {
			trace_util_0.Count(_rule_column_pruning_00000, 63)
			return err
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 55)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 64)
	for _, col := range p.Schema().TblID2Handle {
		trace_util_0.Count(_rule_column_pruning_00000, 66)
		parentUsedCols = append(parentUsedCols, col[0])
	}
	trace_util_0.Count(_rule_column_pruning_00000, 65)
	return p.children[0].PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 67)
	used, err := getUsedList(parentUsedCols, ds.schema)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 72)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 68)
	var (
		handleCol     *expression.Column
		handleColInfo *model.ColumnInfo
	)
	for i := len(used) - 1; i >= 0; i-- {
		trace_util_0.Count(_rule_column_pruning_00000, 73)
		if ds.tableInfo.PKIsHandle && mysql.HasPriKeyFlag(ds.Columns[i].Flag) {
			trace_util_0.Count(_rule_column_pruning_00000, 75)
			handleCol = ds.schema.Columns[i]
			handleColInfo = ds.Columns[i]
		}
		trace_util_0.Count(_rule_column_pruning_00000, 74)
		if !used[i] {
			trace_util_0.Count(_rule_column_pruning_00000, 76)
			ds.schema.Columns = append(ds.schema.Columns[:i], ds.schema.Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 69)
	for k, cols := range ds.schema.TblID2Handle {
		trace_util_0.Count(_rule_column_pruning_00000, 77)
		if ds.schema.ColumnIndex(cols[0]) == -1 {
			trace_util_0.Count(_rule_column_pruning_00000, 78)
			delete(ds.schema.TblID2Handle, k)
		}
	}
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	trace_util_0.Count(_rule_column_pruning_00000, 70)
	if ds.schema.Len() == 0 && !infoschema.IsMemoryDB(ds.DBName.L) {
		trace_util_0.Count(_rule_column_pruning_00000, 79)
		if handleCol == nil {
			trace_util_0.Count(_rule_column_pruning_00000, 81)
			handleCol = ds.newExtraHandleSchemaCol()
			handleColInfo = model.NewExtraHandleColInfo()
		}
		trace_util_0.Count(_rule_column_pruning_00000, 80)
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 71)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 82)
	used, err := getUsedList(parentUsedCols, p.Schema())
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 86)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 83)
	for i := len(used) - 1; i >= 0; i-- {
		trace_util_0.Count(_rule_column_pruning_00000, 87)
		if !used[i] {
			trace_util_0.Count(_rule_column_pruning_00000, 88)
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 84)
	for k, cols := range p.schema.TblID2Handle {
		trace_util_0.Count(_rule_column_pruning_00000, 89)
		if p.schema.ColumnIndex(cols[0]) == -1 {
			trace_util_0.Count(_rule_column_pruning_00000, 90)
			delete(p.schema.TblID2Handle, k)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 85)
	return nil
}

func (p *LogicalJoin) extractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	trace_util_0.Count(_rule_column_pruning_00000, 91)
	for _, eqCond := range p.EqualConditions {
		trace_util_0.Count(_rule_column_pruning_00000, 97)
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 92)
	for _, leftCond := range p.LeftConditions {
		trace_util_0.Count(_rule_column_pruning_00000, 98)
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 93)
	for _, rightCond := range p.RightConditions {
		trace_util_0.Count(_rule_column_pruning_00000, 99)
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 94)
	for _, otherCond := range p.OtherConditions {
		trace_util_0.Count(_rule_column_pruning_00000, 100)
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 95)
	lChild := p.children[0]
	rChild := p.children[1]
	for _, col := range parentUsedCols {
		trace_util_0.Count(_rule_column_pruning_00000, 101)
		if lChild.Schema().Contains(col) {
			trace_util_0.Count(_rule_column_pruning_00000, 102)
			leftCols = append(leftCols, col)
		} else {
			trace_util_0.Count(_rule_column_pruning_00000, 103)
			if rChild.Schema().Contains(col) {
				trace_util_0.Count(_rule_column_pruning_00000, 104)
				rightCols = append(rightCols, col)
			}
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 96)
	return leftCols, rightCols
}

func (p *LogicalJoin) mergeSchema() {
	trace_util_0.Count(_rule_column_pruning_00000, 105)
	lChild := p.children[0]
	rChild := p.children[1]
	composedSchema := expression.MergeSchema(lChild.Schema(), rChild.Schema())
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		trace_util_0.Count(_rule_column_pruning_00000, 106)
		p.schema = lChild.Schema().Clone()
	} else {
		trace_util_0.Count(_rule_column_pruning_00000, 107)
		if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
			trace_util_0.Count(_rule_column_pruning_00000, 108)
			joinCol := p.schema.Columns[len(p.schema.Columns)-1]
			p.schema = lChild.Schema().Clone()
			p.schema.Append(joinCol)
		} else {
			trace_util_0.Count(_rule_column_pruning_00000, 109)
			{
				p.schema = composedSchema
			}
		}
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 110)
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	err := p.children[0].PruneColumns(leftCols)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 113)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 111)
	err = p.children[1].PruneColumns(rightCols)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 114)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 112)
	p.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 115)
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)

	err := la.children[1].PruneColumns(rightCols)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 119)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 116)
	la.corCols = extractCorColumnsBySchema(la.children[1], la.children[0].Schema())
	for _, col := range la.corCols {
		trace_util_0.Count(_rule_column_pruning_00000, 120)
		leftCols = append(leftCols, &col.Column)
	}

	trace_util_0.Count(_rule_column_pruning_00000, 117)
	err = la.children[0].PruneColumns(leftCols)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 121)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 118)
	la.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 122)
	if p.Lock != ast.SelectLockForUpdate {
		trace_util_0.Count(_rule_column_pruning_00000, 125)
		return p.baseLogicalPlan.PruneColumns(parentUsedCols)
	}

	trace_util_0.Count(_rule_column_pruning_00000, 123)
	for _, cols := range p.children[0].Schema().TblID2Handle {
		trace_util_0.Count(_rule_column_pruning_00000, 126)
		parentUsedCols = append(parentUsedCols, cols...)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 124)
	return p.children[0].PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column) error {
	trace_util_0.Count(_rule_column_pruning_00000, 127)
	windowColumns := p.GetWindowResultColumns()
	len := 0
	for _, col := range parentUsedCols {
		trace_util_0.Count(_rule_column_pruning_00000, 130)
		used := false
		for _, windowColumn := range windowColumns {
			trace_util_0.Count(_rule_column_pruning_00000, 132)
			if windowColumn.Equal(nil, col) {
				trace_util_0.Count(_rule_column_pruning_00000, 133)
				used = true
				break
			}
		}
		trace_util_0.Count(_rule_column_pruning_00000, 131)
		if !used {
			trace_util_0.Count(_rule_column_pruning_00000, 134)
			parentUsedCols[len] = col
			len++
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 128)
	parentUsedCols = parentUsedCols[:len]
	parentUsedCols = p.extractUsedCols(parentUsedCols)
	err := p.children[0].PruneColumns(parentUsedCols)
	if err != nil {
		trace_util_0.Count(_rule_column_pruning_00000, 135)
		return err
	}

	trace_util_0.Count(_rule_column_pruning_00000, 129)
	p.SetSchema(p.children[0].Schema().Clone())
	p.Schema().Append(windowColumns...)
	return nil
}

func (p *LogicalWindow) extractUsedCols(parentUsedCols []*expression.Column) []*expression.Column {
	trace_util_0.Count(_rule_column_pruning_00000, 136)
	for _, desc := range p.WindowFuncDescs {
		trace_util_0.Count(_rule_column_pruning_00000, 140)
		for _, arg := range desc.Args {
			trace_util_0.Count(_rule_column_pruning_00000, 141)
			parentUsedCols = append(parentUsedCols, expression.ExtractColumns(arg)...)
		}
	}
	trace_util_0.Count(_rule_column_pruning_00000, 137)
	for _, by := range p.PartitionBy {
		trace_util_0.Count(_rule_column_pruning_00000, 142)
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 138)
	for _, by := range p.OrderBy {
		trace_util_0.Count(_rule_column_pruning_00000, 143)
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	trace_util_0.Count(_rule_column_pruning_00000, 139)
	return parentUsedCols
}

var _rule_column_pruning_00000 = "planner/core/rule_column_pruning.go"
