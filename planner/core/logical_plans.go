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
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

var (
	_ LogicalPlan = &LogicalJoin{}
	_ LogicalPlan = &LogicalAggregation{}
	_ LogicalPlan = &LogicalProjection{}
	_ LogicalPlan = &LogicalSelection{}
	_ LogicalPlan = &LogicalApply{}
	_ LogicalPlan = &LogicalMaxOneRow{}
	_ LogicalPlan = &LogicalTableDual{}
	_ LogicalPlan = &DataSource{}
	_ LogicalPlan = &LogicalUnionAll{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalLock{}
	_ LogicalPlan = &LogicalLimit{}
	_ LogicalPlan = &LogicalWindow{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if row a in table A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if row a in table A does not match any row in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

// IsOuterJoin returns if this joiner is a outer joiner
func (tp JoinType) IsOuterJoin() bool {
	trace_util_0.Count(_logical_plans_00000, 0)
	return tp == LeftOuterJoin || tp == RightOuterJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

func (tp JoinType) String() string {
	trace_util_0.Count(_logical_plans_00000, 1)
	switch tp {
	case InnerJoin:
		trace_util_0.Count(_logical_plans_00000, 3)
		return "inner join"
	case LeftOuterJoin:
		trace_util_0.Count(_logical_plans_00000, 4)
		return "left outer join"
	case RightOuterJoin:
		trace_util_0.Count(_logical_plans_00000, 5)
		return "right outer join"
	case SemiJoin:
		trace_util_0.Count(_logical_plans_00000, 6)
		return "semi join"
	case AntiSemiJoin:
		trace_util_0.Count(_logical_plans_00000, 7)
		return "anti semi join"
	case LeftOuterSemiJoin:
		trace_util_0.Count(_logical_plans_00000, 8)
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		trace_util_0.Count(_logical_plans_00000, 9)
		return "anti left outer semi join"
	}
	trace_util_0.Count(_logical_plans_00000, 2)
	return "unsupported join type"
}

const (
	preferLeftAsIndexInner = 1 << iota
	preferRightAsIndexInner
	preferHashJoin
	preferMergeJoin
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	logicalSchemaProducer

	JoinType      JoinType
	reordered     bool
	cartesianJoin bool
	StraightJoin  bool

	// hintInfo stores the join algorithm hint information specified by client.
	hintInfo       *tableHintInfo
	preferJoinType uint

	EqualConditions []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	LeftJoinKeys    []*expression.Column
	RightJoinKeys   []*expression.Column
	leftProperties  [][]*expression.Column
	rightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// redundantSchema contains columns which are eliminated in join.
	// For select * from a join b using (c); a.c will in output schema, and b.c will in redundantSchema.
	redundantSchema *expression.Schema
}

func (p *LogicalJoin) columnSubstitute(schema *expression.Schema, exprs []expression.Expression) {
	trace_util_0.Count(_logical_plans_00000, 10)
	for i, cond := range p.LeftConditions {
		trace_util_0.Count(_logical_plans_00000, 14)
		p.LeftConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	trace_util_0.Count(_logical_plans_00000, 11)
	for i, cond := range p.RightConditions {
		trace_util_0.Count(_logical_plans_00000, 15)
		p.RightConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	trace_util_0.Count(_logical_plans_00000, 12)
	for i, cond := range p.OtherConditions {
		trace_util_0.Count(_logical_plans_00000, 16)
		p.OtherConditions[i] = expression.ColumnSubstitute(cond, schema, exprs)
	}

	trace_util_0.Count(_logical_plans_00000, 13)
	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		trace_util_0.Count(_logical_plans_00000, 17)
		newCond := expression.ColumnSubstitute(p.EqualConditions[i], schema, exprs).(*expression.ScalarFunction)

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.children[0].Schema()) {
			trace_util_0.Count(_logical_plans_00000, 21)
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		trace_util_0.Count(_logical_plans_00000, 18)
		if expression.ExprFromSchema(newCond, p.children[1].Schema()) {
			trace_util_0.Count(_logical_plans_00000, 22)
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		trace_util_0.Count(_logical_plans_00000, 19)
		_, lhsIsCol := newCond.GetArgs()[0].(*expression.Column)
		_, rhsIsCol := newCond.GetArgs()[1].(*expression.Column)

		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !(lhsIsCol && rhsIsCol) {
			trace_util_0.Count(_logical_plans_00000, 23)
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		trace_util_0.Count(_logical_plans_00000, 20)
		p.EqualConditions[i] = newCond
	}
}

func (p *LogicalJoin) attachOnConds(onConds []expression.Expression) {
	trace_util_0.Count(_logical_plans_00000, 24)
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

func (p *LogicalJoin) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 25)
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, fun := range p.EqualConditions {
		trace_util_0.Count(_logical_plans_00000, 30)
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	trace_util_0.Count(_logical_plans_00000, 26)
	for _, fun := range p.LeftConditions {
		trace_util_0.Count(_logical_plans_00000, 31)
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	trace_util_0.Count(_logical_plans_00000, 27)
	for _, fun := range p.RightConditions {
		trace_util_0.Count(_logical_plans_00000, 32)
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	trace_util_0.Count(_logical_plans_00000, 28)
	for _, fun := range p.OtherConditions {
		trace_util_0.Count(_logical_plans_00000, 33)
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	trace_util_0.Count(_logical_plans_00000, 29)
	return corCols
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []expression.Expression

	// calculateGenCols indicates the projection is for calculating generated columns.
	// In *UPDATE*, we should know this to tell different projections.
	calculateGenCols bool

	// calculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	calculateNoDelay bool

	// avoidColumnRef is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the expressions of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	avoidColumnEvaluator bool
}

func (p *LogicalProjection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 34)
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range p.Exprs {
		trace_util_0.Count(_logical_plans_00000, 36)
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	trace_util_0.Count(_logical_plans_00000, 35)
	return corCols
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	// groupByCols stores the columns that are group-by items.
	groupByCols []*expression.Column

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.
}

func (la *LogicalAggregation) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 37)
	corCols := la.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range la.GroupByItems {
		trace_util_0.Count(_logical_plans_00000, 40)
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	trace_util_0.Count(_logical_plans_00000, 38)
	for _, fun := range la.AggFuncs {
		trace_util_0.Count(_logical_plans_00000, 41)
		for _, arg := range fun.Args {
			trace_util_0.Count(_logical_plans_00000, 42)
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	trace_util_0.Count(_logical_plans_00000, 39)
	return corCols
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

func (p *LogicalSelection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 43)
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, cond := range p.Conditions {
		trace_util_0.Count(_logical_plans_00000, 45)
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	trace_util_0.Count(_logical_plans_00000, 44)
	return corCols
}

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	corCols []*expression.CorrelatedColumn
}

func (la *LogicalApply) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 46)
	corCols := la.LogicalJoin.extractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		trace_util_0.Count(_logical_plans_00000, 48)
		if la.children[0].Schema().Contains(&corCols[i].Column) {
			trace_util_0.Count(_logical_plans_00000, 49)
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	trace_util_0.Count(_logical_plans_00000, 47)
	return corCols
}

// LogicalMaxOneRow checks if a query returns no more than one row.
type LogicalMaxOneRow struct {
	baseLogicalPlan
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	logicalSchemaProducer

	RowCount int
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	baseLogicalPlan

	conditions []expression.Expression
}

// DataSource represents a tablescan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	indexHints []*ast.IndexHint
	table      table.Table
	tableInfo  *model.TableInfo
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr

	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*accessPath

	// The data source may be a partition, rather than a real table.
	isPartition     bool
	physicalTableID int64
	partitionNames  []model.CIStr
}

// accessPath tells how we access one index or just access table.
type accessPath struct {
	index      *model.IndexInfo
	idxCols    []*expression.Column
	idxColLens []int
	ranges     []*ranger.Range
	// countAfterAccess is the row count after we apply range seek and before we use other filter to filter data.
	countAfterAccess float64
	// countAfterIndex is the row count after we apply filters on index and before we apply the table filters.
	countAfterIndex float64
	accessConds     []expression.Expression
	eqCondCount     int
	indexFilters    []expression.Expression
	tableFilters    []expression.Expression
	// isTablePath indicates whether this path is table path.
	isTablePath bool
	// forced means this path is generated by `use/force index()`.
	forced bool
}

// deriveTablePathStats will fulfill the information that the accessPath need.
// And it will check whether the primary key is covered only by point query.
func (ds *DataSource) deriveTablePathStats(path *accessPath) (bool, error) {
	trace_util_0.Count(_logical_plans_00000, 50)
	var err error
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.countAfterAccess = float64(ds.statisticTable.Count)
	path.tableFilters = ds.pushedDownConds
	var pkCol *expression.Column
	columnLen := len(ds.schema.Columns)
	isUnsigned := false
	if ds.tableInfo.PKIsHandle {
		trace_util_0.Count(_logical_plans_00000, 59)
		if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
			trace_util_0.Count(_logical_plans_00000, 60)
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			pkCol = expression.ColInfo2Col(ds.schema.Columns, pkColInfo)
		}
	} else {
		trace_util_0.Count(_logical_plans_00000, 61)
		if columnLen > 0 && ds.schema.Columns[columnLen-1].ID == model.ExtraHandleID {
			trace_util_0.Count(_logical_plans_00000, 62)
			pkCol = ds.schema.Columns[columnLen-1]
		}
	}
	trace_util_0.Count(_logical_plans_00000, 51)
	if pkCol == nil {
		trace_util_0.Count(_logical_plans_00000, 63)
		path.ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	trace_util_0.Count(_logical_plans_00000, 52)
	path.ranges = ranger.FullIntRange(isUnsigned)
	if len(ds.pushedDownConds) == 0 {
		trace_util_0.Count(_logical_plans_00000, 64)
		return false, nil
	}
	trace_util_0.Count(_logical_plans_00000, 53)
	path.accessConds, path.tableFilters = ranger.DetachCondsForColumn(ds.ctx, ds.pushedDownConds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.accessConds) == 0 {
		trace_util_0.Count(_logical_plans_00000, 65)
		for i, filter := range path.tableFilters {
			trace_util_0.Count(_logical_plans_00000, 66)
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				trace_util_0.Count(_logical_plans_00000, 69)
				continue
			}
			trace_util_0.Count(_logical_plans_00000, 67)
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.ctx, pkCol) {
				trace_util_0.Count(_logical_plans_00000, 70)
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					trace_util_0.Count(_logical_plans_00000, 71)
					path.accessConds = append(path.accessConds, filter)
					path.tableFilters = append(path.tableFilters[:i], path.tableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			trace_util_0.Count(_logical_plans_00000, 68)
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.ctx, pkCol) {
				trace_util_0.Count(_logical_plans_00000, 72)
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					trace_util_0.Count(_logical_plans_00000, 73)
					path.accessConds = append(path.accessConds, filter)
					path.tableFilters = append(path.tableFilters[:i], path.tableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	trace_util_0.Count(_logical_plans_00000, 54)
	if corColInAccessConds {
		trace_util_0.Count(_logical_plans_00000, 74)
		path.countAfterAccess = 1
		return true, nil
	}
	trace_util_0.Count(_logical_plans_00000, 55)
	path.ranges, err = ranger.BuildTableRange(path.accessConds, sc, pkCol.RetType)
	if err != nil {
		trace_util_0.Count(_logical_plans_00000, 75)
		return false, err
	}
	trace_util_0.Count(_logical_plans_00000, 56)
	path.countAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkCol.ID, path.ranges)
	// If the `countAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.countAfterAccess < ds.stats.RowCount {
		trace_util_0.Count(_logical_plans_00000, 76)
		path.countAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether the primary key is covered by point query.
	trace_util_0.Count(_logical_plans_00000, 57)
	noIntervalRange := true
	for _, ran := range path.ranges {
		trace_util_0.Count(_logical_plans_00000, 77)
		if !ran.IsPoint(sc) {
			trace_util_0.Count(_logical_plans_00000, 78)
			noIntervalRange = false
			break
		}
	}
	trace_util_0.Count(_logical_plans_00000, 58)
	return noIntervalRange, err
}

// deriveIndexPathStats will fulfill the information that the accessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
func (ds *DataSource) deriveIndexPathStats(path *accessPath) (bool, error) {
	trace_util_0.Count(_logical_plans_00000, 79)
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.ranges = ranger.FullRange()
	path.countAfterAccess = float64(ds.statisticTable.Count)
	path.idxCols, path.idxColLens = expression.IndexInfo2Cols(ds.schema.Columns, path.index)
	eqOrInCount := 0
	if len(path.idxCols) != 0 {
		trace_util_0.Count(_logical_plans_00000, 85)
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, ds.pushedDownConds, path.idxCols, path.idxColLens)
		if err != nil {
			trace_util_0.Count(_logical_plans_00000, 87)
			return false, err
		}
		trace_util_0.Count(_logical_plans_00000, 86)
		path.ranges = res.Ranges
		path.accessConds = res.AccessConds
		path.tableFilters = res.RemainedConds
		path.eqCondCount = res.EqCondCount
		eqOrInCount = res.EqOrInCount
		path.countAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.index.ID, path.ranges)
		if err != nil {
			trace_util_0.Count(_logical_plans_00000, 88)
			return false, err
		}
	} else {
		trace_util_0.Count(_logical_plans_00000, 89)
		{
			path.tableFilters = ds.pushedDownConds
		}
	}
	trace_util_0.Count(_logical_plans_00000, 80)
	if eqOrInCount == len(path.accessConds) {
		trace_util_0.Count(_logical_plans_00000, 90)
		accesses, remained := path.splitCorColAccessCondFromFilters(eqOrInCount)
		path.accessConds = append(path.accessConds, accesses...)
		path.tableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			trace_util_0.Count(_logical_plans_00000, 91)
			path.countAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			trace_util_0.Count(_logical_plans_00000, 92)
			{
				selectivity := path.countAfterAccess / float64(ds.statisticTable.Count)
				for i := range accesses {
					trace_util_0.Count(_logical_plans_00000, 93)
					col := path.idxCols[eqOrInCount+i]
					ndv := ds.getColumnNDV(col.ID)
					ndv *= selectivity
					if ndv < 1 {
						trace_util_0.Count(_logical_plans_00000, 95)
						ndv = 1.0
					}
					trace_util_0.Count(_logical_plans_00000, 94)
					path.countAfterAccess = path.countAfterAccess / ndv
				}
			}
		}
	}
	trace_util_0.Count(_logical_plans_00000, 81)
	path.indexFilters, path.tableFilters = splitIndexFilterConditions(path.tableFilters, path.index.Columns, ds.tableInfo)
	// If the `countAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.countAfterAccess < ds.stats.RowCount {
		trace_util_0.Count(_logical_plans_00000, 96)
		path.countAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	trace_util_0.Count(_logical_plans_00000, 82)
	if path.indexFilters != nil {
		trace_util_0.Count(_logical_plans_00000, 97)
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.indexFilters)
		if err != nil {
			trace_util_0.Count(_logical_plans_00000, 99)
			logutil.Logger(context.Background()).Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		trace_util_0.Count(_logical_plans_00000, 98)
		path.countAfterIndex = math.Max(path.countAfterAccess*selectivity, ds.stats.RowCount)
	}
	// Check whether there's only point query.
	trace_util_0.Count(_logical_plans_00000, 83)
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.ranges {
		trace_util_0.Count(_logical_plans_00000, 100)
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.index.Columns) {
			trace_util_0.Count(_logical_plans_00000, 103)
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		trace_util_0.Count(_logical_plans_00000, 101)
		for i := 0; i < len(path.index.Columns); i++ {
			trace_util_0.Count(_logical_plans_00000, 104)
			if ran.HighVal[i].IsNull() {
				trace_util_0.Count(_logical_plans_00000, 105)
				haveNullVal = true
				break
			}
		}
		trace_util_0.Count(_logical_plans_00000, 102)
		if haveNullVal {
			trace_util_0.Count(_logical_plans_00000, 106)
			break
		}
	}
	trace_util_0.Count(_logical_plans_00000, 84)
	return noIntervalRanges && !haveNullVal, nil
}

func (path *accessPath) splitCorColAccessCondFromFilters(eqOrInCount int) (access, remained []expression.Expression) {
	trace_util_0.Count(_logical_plans_00000, 107)
	access = make([]expression.Expression, len(path.idxCols)-eqOrInCount)
	used := make([]bool, len(path.tableFilters))
	for i := eqOrInCount; i < len(path.idxCols); i++ {
		trace_util_0.Count(_logical_plans_00000, 110)
		matched := false
		for j, filter := range path.tableFilters {
			trace_util_0.Count(_logical_plans_00000, 112)
			if used[j] || !isColEqCorColOrConstant(filter, path.idxCols[i]) {
				trace_util_0.Count(_logical_plans_00000, 115)
				continue
			}
			trace_util_0.Count(_logical_plans_00000, 113)
			matched = true
			access[i-eqOrInCount] = filter
			if path.idxColLens[i] == types.UnspecifiedLength {
				trace_util_0.Count(_logical_plans_00000, 116)
				used[j] = true
			}
			trace_util_0.Count(_logical_plans_00000, 114)
			break
		}
		trace_util_0.Count(_logical_plans_00000, 111)
		if !matched {
			trace_util_0.Count(_logical_plans_00000, 117)
			access = access[:i-eqOrInCount]
			break
		}
	}
	trace_util_0.Count(_logical_plans_00000, 108)
	for i, ok := range used {
		trace_util_0.Count(_logical_plans_00000, 118)
		if !ok {
			trace_util_0.Count(_logical_plans_00000, 119)
			remained = append(remained, path.tableFilters[i])
		}
	}
	trace_util_0.Count(_logical_plans_00000, 109)
	return access, remained
}

// getEqOrInColOffset checks if the expression is a eq function that one side is constant or correlated column
// and another is column.
func isColEqCorColOrConstant(filter expression.Expression, col *expression.Column) bool {
	trace_util_0.Count(_logical_plans_00000, 120)
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		trace_util_0.Count(_logical_plans_00000, 124)
		return false
	}
	trace_util_0.Count(_logical_plans_00000, 121)
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		trace_util_0.Count(_logical_plans_00000, 125)
		if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
			trace_util_0.Count(_logical_plans_00000, 127)
			if col.Equal(nil, c) {
				trace_util_0.Count(_logical_plans_00000, 128)
				return true
			}
		}
		trace_util_0.Count(_logical_plans_00000, 126)
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			trace_util_0.Count(_logical_plans_00000, 129)
			if col.Equal(nil, c) {
				trace_util_0.Count(_logical_plans_00000, 130)
				return true
			}
		}
	}
	trace_util_0.Count(_logical_plans_00000, 122)
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		trace_util_0.Count(_logical_plans_00000, 131)
		if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
			trace_util_0.Count(_logical_plans_00000, 133)
			if col.Equal(nil, c) {
				trace_util_0.Count(_logical_plans_00000, 134)
				return true
			}
		}
		trace_util_0.Count(_logical_plans_00000, 132)
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			trace_util_0.Count(_logical_plans_00000, 135)
			if col.Equal(nil, c) {
				trace_util_0.Count(_logical_plans_00000, 136)
				return true
			}
		}
	}
	trace_util_0.Count(_logical_plans_00000, 123)
	return false
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	trace_util_0.Count(_logical_plans_00000, 137)
	if !ds.tableInfo.PKIsHandle {
		trace_util_0.Count(_logical_plans_00000, 140)
		return nil
	}
	trace_util_0.Count(_logical_plans_00000, 138)
	for i, col := range ds.Columns {
		trace_util_0.Count(_logical_plans_00000, 141)
		if mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_logical_plans_00000, 142)
			return ds.schema.Columns[i]
		}
	}
	trace_util_0.Count(_logical_plans_00000, 139)
	return nil
}

// TableInfo returns the *TableInfo of data source.
func (ds *DataSource) TableInfo() *model.TableInfo {
	trace_util_0.Count(_logical_plans_00000, 143)
	return ds.tableInfo
}

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	logicalSchemaProducer
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	baseLogicalPlan

	ByItems []*ByItems
}

func (ls *LogicalSort) extractCorrelatedCols() []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 144)
	corCols := ls.baseLogicalPlan.extractCorrelatedCols()
	for _, item := range ls.ByItems {
		trace_util_0.Count(_logical_plans_00000, 146)
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	trace_util_0.Count(_logical_plans_00000, 145)
	return corCols
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalPlan

	ByItems []*ByItems
	Offset  uint64
	Count   uint64
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	trace_util_0.Count(_logical_plans_00000, 147)
	return len(lt.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	baseLogicalPlan

	Offset uint64
	Count  uint64
}

// LogicalLock represents a select lock plan.
type LogicalLock struct {
	baseLogicalPlan

	Lock ast.SelectLockType
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []expression.Expression
	// CmpFuncs is used to decide whether one row is included in the current frame.
	CmpFuncs []expression.CompareFunc
}

// LogicalWindow represents a logical window function plan.
type LogicalWindow struct {
	logicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// GetWindowResultColumns returns the columns storing the result of the window function.
func (p *LogicalWindow) GetWindowResultColumns() []*expression.Column {
	trace_util_0.Count(_logical_plans_00000, 148)
	return p.schema.Columns[p.schema.Len()-len(p.WindowFuncDescs):]
}

// extractCorColumnsBySchema only extracts the correlated columns that match the specified schema.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schema is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func extractCorColumnsBySchema(p LogicalPlan, schema *expression.Schema) []*expression.CorrelatedColumn {
	trace_util_0.Count(_logical_plans_00000, 149)
	corCols := p.extractCorrelatedCols()
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		trace_util_0.Count(_logical_plans_00000, 152)
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			trace_util_0.Count(_logical_plans_00000, 153)
			if resultCorCols[idx] == nil {
				trace_util_0.Count(_logical_plans_00000, 155)
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			trace_util_0.Count(_logical_plans_00000, 154)
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	trace_util_0.Count(_logical_plans_00000, 150)
	length := 0
	for _, col := range resultCorCols {
		trace_util_0.Count(_logical_plans_00000, 156)
		if col != nil {
			trace_util_0.Count(_logical_plans_00000, 157)
			resultCorCols[length] = col
			length++
		}
	}
	trace_util_0.Count(_logical_plans_00000, 151)
	return resultCorCols[:length]
}

var _logical_plans_00000 = "planner/core/logical_plans.go"
