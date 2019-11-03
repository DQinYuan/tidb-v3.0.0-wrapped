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
	"context"
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	trace_util_0.Count(_stats_00000, 0)
	return p.stats.RowCount
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalTableDual) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 1)
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for i := range profile.Cardinality {
		trace_util_0.Count(_stats_00000, 3)
		profile.Cardinality[i] = float64(p.RowCount)
	}
	trace_util_0.Count(_stats_00000, 2)
	p.stats = profile
	return p.stats, nil
}

func (p *baseLogicalPlan) recursiveDeriveStats() (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 4)
	if p.stats != nil {
		trace_util_0.Count(_stats_00000, 7)
		return p.stats, nil
	}
	trace_util_0.Count(_stats_00000, 5)
	childStats := make([]*property.StatsInfo, len(p.children))
	for i, child := range p.children {
		trace_util_0.Count(_stats_00000, 8)
		childProfile, err := child.recursiveDeriveStats()
		if err != nil {
			trace_util_0.Count(_stats_00000, 10)
			return nil, err
		}
		trace_util_0.Count(_stats_00000, 9)
		childStats[i] = childProfile
	}
	trace_util_0.Count(_stats_00000, 6)
	return p.self.DeriveStats(childStats)
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 11)
	if len(childStats) == 1 {
		trace_util_0.Count(_stats_00000, 15)
		p.stats = childStats[0]
		return p.stats, nil
	}
	trace_util_0.Count(_stats_00000, 12)
	if len(childStats) > 1 {
		trace_util_0.Count(_stats_00000, 16)
		err := ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, err
	}
	trace_util_0.Count(_stats_00000, 13)
	profile := &property.StatsInfo{
		RowCount:    float64(1),
		Cardinality: make([]float64, p.self.Schema().Len()),
	}
	for i := range profile.Cardinality {
		trace_util_0.Count(_stats_00000, 17)
		profile.Cardinality[i] = float64(1)
	}
	trace_util_0.Count(_stats_00000, 14)
	p.stats = profile
	return profile, nil
}

// getColumnNDV computes estimated NDV of specified column using the original
// histogram of `DataSource` which is retrieved from storage(not the derived one).
func (ds *DataSource) getColumnNDV(colID int64) (ndv float64) {
	trace_util_0.Count(_stats_00000, 18)
	hist, ok := ds.statisticTable.Columns[colID]
	if ok && hist.Count > 0 {
		trace_util_0.Count(_stats_00000, 20)
		factor := float64(ds.statisticTable.Count) / float64(hist.Count)
		ndv = float64(hist.NDV) * factor
	} else {
		trace_util_0.Count(_stats_00000, 21)
		{
			ndv = float64(ds.statisticTable.Count) * distinctFactor
		}
	}
	trace_util_0.Count(_stats_00000, 19)
	return ndv
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs) {
	trace_util_0.Count(_stats_00000, 22)
	tableStats := &property.StatsInfo{
		RowCount:     float64(ds.statisticTable.Count),
		Cardinality:  make([]float64, len(ds.Columns)),
		HistColl:     ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
		StatsVersion: ds.statisticTable.Version,
	}
	if ds.statisticTable.Pseudo {
		trace_util_0.Count(_stats_00000, 26)
		tableStats.StatsVersion = statistics.PseudoVersion
	}
	trace_util_0.Count(_stats_00000, 23)
	for i, col := range ds.Columns {
		trace_util_0.Count(_stats_00000, 27)
		tableStats.Cardinality[i] = ds.getColumnNDV(col.ID)
	}
	trace_util_0.Count(_stats_00000, 24)
	ds.tableStats = tableStats
	selectivity, nodes, err := tableStats.HistColl.Selectivity(ds.ctx, conds)
	if err != nil {
		trace_util_0.Count(_stats_00000, 28)
		logutil.Logger(context.Background()).Debug("an error happened, use the default selectivity", zap.Error(err))
		selectivity = selectionFactor
	}
	trace_util_0.Count(_stats_00000, 25)
	ds.stats = tableStats.Scale(selectivity)
	if ds.ctx.GetSessionVars().OptimizerSelectivityLevel >= 1 {
		trace_util_0.Count(_stats_00000, 29)
		ds.stats.HistColl = ds.stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
	}
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 30)
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		trace_util_0.Count(_stats_00000, 33)
		ds.pushedDownConds[i] = expression.PushDownNot(nil, expr, false)
	}
	trace_util_0.Count(_stats_00000, 31)
	ds.deriveStatsByFilter(ds.pushedDownConds)
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_stats_00000, 34)
		if path.isTablePath {
			trace_util_0.Count(_stats_00000, 37)
			noIntervalRanges, err := ds.deriveTablePathStats(path)
			if err != nil {
				trace_util_0.Count(_stats_00000, 40)
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			trace_util_0.Count(_stats_00000, 38)
			if noIntervalRanges || len(path.ranges) == 0 {
				trace_util_0.Count(_stats_00000, 41)
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			trace_util_0.Count(_stats_00000, 39)
			continue
		}
		trace_util_0.Count(_stats_00000, 35)
		noIntervalRanges, err := ds.deriveIndexPathStats(path)
		if err != nil {
			trace_util_0.Count(_stats_00000, 42)
			return nil, err
		}
		// If we have empty range, or point range on unique index, just remove other possible paths.
		trace_util_0.Count(_stats_00000, 36)
		if (noIntervalRanges && path.index.Unique) || len(path.ranges) == 0 {
			trace_util_0.Count(_stats_00000, 43)
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}
	trace_util_0.Count(_stats_00000, 32)
	return ds.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 44)
	p.stats = childStats[0].Scale(selectionFactor)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 45)
	p.stats = &property.StatsInfo{
		Cardinality: make([]float64, p.Schema().Len()),
	}
	for _, childProfile := range childStats {
		trace_util_0.Count(_stats_00000, 47)
		p.stats.RowCount += childProfile.RowCount
		for i := range p.stats.Cardinality {
			trace_util_0.Count(_stats_00000, 48)
			p.stats.Cardinality[i] += childProfile.Cardinality[i]
		}
	}
	trace_util_0.Count(_stats_00000, 46)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 49)
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(p.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range p.stats.Cardinality {
		trace_util_0.Count(_stats_00000, 51)
		p.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], p.stats.RowCount)
	}
	trace_util_0.Count(_stats_00000, 50)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 52)
	childProfile := childStats[0]
	lt.stats = &property.StatsInfo{
		RowCount:    math.Min(float64(lt.Count), childProfile.RowCount),
		Cardinality: make([]float64, len(childProfile.Cardinality)),
	}
	for i := range lt.stats.Cardinality {
		trace_util_0.Count(_stats_00000, 54)
		lt.stats.Cardinality[i] = math.Min(childProfile.Cardinality[i], lt.stats.RowCount)
	}
	trace_util_0.Count(_stats_00000, 53)
	return lt.stats, nil
}

// getCardinality will return the Cardinality of a couple of columns. We simply return the max one, because we cannot know
// the Cardinality for multi-dimension attributes properly. This is a simple and naive scheme of Cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	trace_util_0.Count(_stats_00000, 55)
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		trace_util_0.Count(_stats_00000, 58)
		logutil.Logger(context.Background()).Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return 0
	}
	trace_util_0.Count(_stats_00000, 56)
	var cardinality = 1.0
	for _, idx := range indices {
		trace_util_0.Count(_stats_00000, 59)
		// It is a very elementary estimation.
		cardinality = math.Max(cardinality, profile.Cardinality[idx])
	}
	trace_util_0.Count(_stats_00000, 57)
	return cardinality
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 60)
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		trace_util_0.Count(_stats_00000, 62)
		cols := expression.ExtractColumns(expr)
		p.stats.Cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	trace_util_0.Count(_stats_00000, 61)
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 63)
	childProfile := childStats[0]
	gbyCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		trace_util_0.Count(_stats_00000, 66)
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	trace_util_0.Count(_stats_00000, 64)
	cardinality := getCardinality(gbyCols, la.children[0].Schema(), childProfile)
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make([]float64, la.schema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for i := range la.stats.Cardinality {
		trace_util_0.Count(_stats_00000, 67)
		la.stats.Cardinality[i] = cardinality
	}
	trace_util_0.Count(_stats_00000, 65)
	la.inputCount = childProfile.RowCount
	return la.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 68)
	leftProfile, rightProfile := childStats[0], childStats[1]
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		trace_util_0.Count(_stats_00000, 74)
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * selectionFactor,
			Cardinality: make([]float64, len(leftProfile.Cardinality)),
		}
		for i := range p.stats.Cardinality {
			trace_util_0.Count(_stats_00000, 76)
			p.stats.Cardinality[i] = leftProfile.Cardinality[i] * selectionFactor
		}
		trace_util_0.Count(_stats_00000, 75)
		return p.stats, nil
	}
	trace_util_0.Count(_stats_00000, 69)
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		trace_util_0.Count(_stats_00000, 77)
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.stats.Cardinality, leftProfile.Cardinality)
		p.stats.Cardinality[len(p.stats.Cardinality)-1] = 2.0
		return p.stats, nil
	}
	trace_util_0.Count(_stats_00000, 70)
	if 0 == len(p.EqualConditions) {
		trace_util_0.Count(_stats_00000, 78)
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * rightProfile.RowCount,
			Cardinality: append(leftProfile.Cardinality, rightProfile.Cardinality...),
		}
		return p.stats, nil
	}
	trace_util_0.Count(_stats_00000, 71)
	leftKeyCardinality := getCardinality(p.LeftJoinKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(p.RightJoinKeys, p.children[1].Schema(), rightProfile)
	count := leftProfile.RowCount * rightProfile.RowCount / math.Max(leftKeyCardinality, rightKeyCardinality)
	if p.JoinType == LeftOuterJoin {
		trace_util_0.Count(_stats_00000, 79)
		count = math.Max(count, leftProfile.RowCount)
	} else {
		trace_util_0.Count(_stats_00000, 80)
		if p.JoinType == RightOuterJoin {
			trace_util_0.Count(_stats_00000, 81)
			count = math.Max(count, rightProfile.RowCount)
		}
	}
	trace_util_0.Count(_stats_00000, 72)
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.Cardinality...)
	cardinality = append(cardinality, rightProfile.Cardinality...)
	for i := range cardinality {
		trace_util_0.Count(_stats_00000, 82)
		cardinality[i] = math.Min(cardinality[i], count)
	}
	trace_util_0.Count(_stats_00000, 73)
	p.stats = &property.StatsInfo{
		RowCount:    count,
		Cardinality: cardinality,
	}
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 83)
	leftProfile := childStats[0]
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make([]float64, la.schema.Len()),
	}
	copy(la.stats.Cardinality, leftProfile.Cardinality)
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		trace_util_0.Count(_stats_00000, 85)
		la.stats.Cardinality[len(la.stats.Cardinality)-1] = 2.0
	} else {
		trace_util_0.Count(_stats_00000, 86)
		{
			for i := la.children[0].Schema().Len(); i < la.schema.Len(); i++ {
				trace_util_0.Count(_stats_00000, 87)
				la.stats.Cardinality[i] = leftProfile.RowCount
			}
		}
	}
	trace_util_0.Count(_stats_00000, 84)
	return la.stats, nil
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(len int) *property.StatsInfo {
	trace_util_0.Count(_stats_00000, 88)
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make([]float64, len),
	}
	for i := 0; i < len; i++ {
		trace_util_0.Count(_stats_00000, 90)
		ret.Cardinality[i] = 1
	}
	trace_util_0.Count(_stats_00000, 89)
	return ret
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 91)
	p.stats = getSingletonStats(p.Schema().Len())
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error) {
	trace_util_0.Count(_stats_00000, 92)
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make([]float64, p.schema.Len()),
	}
	childLen := p.schema.Len() - len(p.WindowFuncDescs)
	for i := 0; i < childLen; i++ {
		trace_util_0.Count(_stats_00000, 95)
		colIdx := p.children[0].Schema().ColumnIndex(p.schema.Columns[i])
		p.stats.Cardinality[i] = childProfile.Cardinality[colIdx]
	}
	trace_util_0.Count(_stats_00000, 93)
	for i := childLen; i < p.schema.Len(); i++ {
		trace_util_0.Count(_stats_00000, 96)
		p.stats.Cardinality[i] = childProfile.RowCount
	}
	trace_util_0.Count(_stats_00000, 94)
	return p.stats, nil
}

var _stats_00000 = "planner/core/stats.go"
