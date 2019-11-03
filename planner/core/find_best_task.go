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
	"math"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"golang.org/x/tools/container/intsets"
)

const (
	netWorkFactor      = 1.5
	netWorkStartFactor = 20.0
	scanFactor         = 2.0
	descScanFactor     = 2 * scanFactor
	memoryFactor       = 5.0
	// 0.5 is the looking up agg context factor.
	hashAggFactor      = 1.2 + 0.5
	selectionFactor    = 0.8
	distinctFactor     = 0.8
	cpuFactor          = 0.9
	distinctAggFactor  = 1.6
	createAggCtxFactor = 6
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType, property.RootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func getPropByOrderByItems(items []*ByItems) (*property.PhysicalProperty, bool) {
	trace_util_0.Count(_find_best_task_00000, 0)
	propItems := make([]property.Item, 0, len(items))
	for _, item := range items {
		trace_util_0.Count(_find_best_task_00000, 2)
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			trace_util_0.Count(_find_best_task_00000, 4)
			return nil, false
		}
		trace_util_0.Count(_find_best_task_00000, 3)
		propItems = append(propItems, property.Item{Col: col, Desc: item.Desc})
	}
	trace_util_0.Count(_find_best_task_00000, 1)
	return &property.PhysicalProperty{Items: propItems}, true
}

func (p *LogicalTableDual) findBestTask(prop *property.PhysicalProperty) (task, error) {
	trace_util_0.Count(_find_best_task_00000, 5)
	if !prop.IsEmpty() {
		trace_util_0.Count(_find_best_task_00000, 7)
		return invalidTask, nil
	}
	trace_util_0.Count(_find_best_task_00000, 6)
	dual := PhysicalTableDual{RowCount: p.RowCount}.Init(p.ctx, p.stats)
	dual.SetSchema(p.schema)
	return &rootTask{p: dual}, nil
}

// findBestTask implements LogicalPlan interface.
func (p *baseLogicalPlan) findBestTask(prop *property.PhysicalProperty) (bestTask task, err error) {
	trace_util_0.Count(_find_best_task_00000, 8)
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		trace_util_0.Count(_find_best_task_00000, 14)
		return nil, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	trace_util_0.Count(_find_best_task_00000, 9)
	bestTask = p.getTask(prop)
	if bestTask != nil {
		trace_util_0.Count(_find_best_task_00000, 15)
		return bestTask, nil
	}

	trace_util_0.Count(_find_best_task_00000, 10)
	if prop.TaskTp != property.RootTaskType {
		trace_util_0.Count(_find_best_task_00000, 16)
		// Currently all plan cannot totally push down.
		p.storeTask(prop, invalidTask)
		return invalidTask, nil
	}

	trace_util_0.Count(_find_best_task_00000, 11)
	bestTask = invalidTask
	childTasks := make([]task, 0, len(p.children))

	// If prop.enforced is true, cols of prop as parameter in exhaustPhysicalPlans should be nil
	// And reset it for enforcing task prop and storing map<prop,task>
	oldPropCols := prop.Items
	if prop.Enforced {
		trace_util_0.Count(_find_best_task_00000, 17)
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		bestTask, err = p.findBestTask(prop)
		if err != nil {
			trace_util_0.Count(_find_best_task_00000, 19)
			return nil, err
		}
		trace_util_0.Count(_find_best_task_00000, 18)
		prop.Enforced = true
		// Next, get the bestTask with enforced prop
		prop.Items = []property.Item{}
	}
	trace_util_0.Count(_find_best_task_00000, 12)
	physicalPlans := p.self.exhaustPhysicalPlans(prop)
	prop.Items = oldPropCols

	for _, pp := range physicalPlans {
		trace_util_0.Count(_find_best_task_00000, 20)
		// find best child tasks firstly.
		childTasks = childTasks[:0]
		for i, child := range p.children {
			trace_util_0.Count(_find_best_task_00000, 24)
			childTask, err := child.findBestTask(pp.GetChildReqProps(i))
			if err != nil {
				trace_util_0.Count(_find_best_task_00000, 27)
				return nil, err
			}
			trace_util_0.Count(_find_best_task_00000, 25)
			if childTask != nil && childTask.invalid() {
				trace_util_0.Count(_find_best_task_00000, 28)
				break
			}
			trace_util_0.Count(_find_best_task_00000, 26)
			childTasks = append(childTasks, childTask)
		}

		// This check makes sure that there is no invalid child task.
		trace_util_0.Count(_find_best_task_00000, 21)
		if len(childTasks) != len(p.children) {
			trace_util_0.Count(_find_best_task_00000, 29)
			continue
		}

		// combine best child tasks with parent physical plan.
		trace_util_0.Count(_find_best_task_00000, 22)
		curTask := pp.attach2Task(childTasks...)

		// enforce curTask property
		if prop.Enforced {
			trace_util_0.Count(_find_best_task_00000, 30)
			curTask = enforceProperty(prop, curTask, p.basePlan.ctx)
		}

		// get the most efficient one.
		trace_util_0.Count(_find_best_task_00000, 23)
		if curTask.cost() < bestTask.cost() {
			trace_util_0.Count(_find_best_task_00000, 31)
			bestTask = curTask
		}
	}

	trace_util_0.Count(_find_best_task_00000, 13)
	p.storeTask(prop, bestTask)
	return bestTask, nil
}

// tryToGetMemTask will check if this table is a mem table. If it is, it will produce a task.
func (ds *DataSource) tryToGetMemTask(prop *property.PhysicalProperty) (task task, err error) {
	trace_util_0.Count(_find_best_task_00000, 32)
	if !prop.IsEmpty() {
		trace_util_0.Count(_find_best_task_00000, 36)
		return nil, nil
	}
	trace_util_0.Count(_find_best_task_00000, 33)
	if !infoschema.IsMemoryDB(ds.DBName.L) {
		trace_util_0.Count(_find_best_task_00000, 37)
		return nil, nil
	}

	trace_util_0.Count(_find_best_task_00000, 34)
	memTable := PhysicalMemTable{
		DBName:      ds.DBName,
		Table:       ds.tableInfo,
		Columns:     ds.Columns,
		TableAsName: ds.TableAsName,
	}.Init(ds.ctx, ds.stats)
	memTable.SetSchema(ds.schema)

	// Stop to push down these conditions.
	var retPlan PhysicalPlan = memTable
	if len(ds.pushedDownConds) > 0 {
		trace_util_0.Count(_find_best_task_00000, 38)
		sel := PhysicalSelection{
			Conditions: ds.pushedDownConds,
		}.Init(ds.ctx, ds.stats)
		sel.SetChildren(memTable)
		retPlan = sel
	}
	trace_util_0.Count(_find_best_task_00000, 35)
	return &rootTask{p: retPlan}, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	trace_util_0.Count(_find_best_task_00000, 39)
	for _, cond := range ds.pushedDownConds {
		trace_util_0.Count(_find_best_task_00000, 41)
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil {
			trace_util_0.Count(_find_best_task_00000, 42)
			result, _, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				trace_util_0.Count(_find_best_task_00000, 44)
				return nil, err
			}
			trace_util_0.Count(_find_best_task_00000, 43)
			if !result {
				trace_util_0.Count(_find_best_task_00000, 45)
				dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats)
				dual.SetSchema(ds.schema)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 40)
	return nil, nil
}

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path         *accessPath
	columnSet    *intsets.Sparse // columnSet is the set of columns that occurred in the access conditions.
	isSingleScan bool
	isMatchProp  bool
}

// compareColumnSet will compares the two set. The last return value is used to indicate
// if they are comparable, it is false when both two sets have columns that do not occur in the other.
// When the second return value is true, the value of first:
// (1) -1 means that `l` is a strict subset of `r`;
// (2) 0 means that `l` equals to `r`;
// (3) 1 means that `l` is a strict superset of `r`.
func compareColumnSet(l, r *intsets.Sparse) (int, bool) {
	trace_util_0.Count(_find_best_task_00000, 46)
	lLen, rLen := l.Len(), r.Len()
	if lLen < rLen {
		trace_util_0.Count(_find_best_task_00000, 49)
		// -1 is meaningful only when l.SubsetOf(r) is true.
		return -1, l.SubsetOf(r)
	}
	trace_util_0.Count(_find_best_task_00000, 47)
	if lLen == rLen {
		trace_util_0.Count(_find_best_task_00000, 50)
		// 0 is meaningful only when l.SubsetOf(r) is true.
		return 0, l.SubsetOf(r)
	}
	// 1 is meaningful only when r.SubsetOf(l) is true.
	trace_util_0.Count(_find_best_task_00000, 48)
	return 1, r.SubsetOf(l)
}

func compareBool(l, r bool) int {
	trace_util_0.Count(_find_best_task_00000, 51)
	if l == r {
		trace_util_0.Count(_find_best_task_00000, 54)
		return 0
	}
	trace_util_0.Count(_find_best_task_00000, 52)
	if l == false {
		trace_util_0.Count(_find_best_task_00000, 55)
		return -1
	}
	trace_util_0.Count(_find_best_task_00000, 53)
	return 1
}

// compareCandidates is the core of skyline pruning. It compares the two candidate paths on three dimensions:
// (1): the set of columns that occurred in the access condition,
// (2): whether or not it matches the physical property
// (3): does it require a double scan.
// If `x` is not worse than `y` at all factors,
// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
func compareCandidates(lhs, rhs *candidatePath) int {
	trace_util_0.Count(_find_best_task_00000, 56)
	setsResult, comparable := compareColumnSet(lhs.columnSet, rhs.columnSet)
	if !comparable {
		trace_util_0.Count(_find_best_task_00000, 60)
		return 0
	}
	trace_util_0.Count(_find_best_task_00000, 57)
	scanResult := compareBool(lhs.isSingleScan, rhs.isSingleScan)
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := setsResult + scanResult + matchResult
	if setsResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		trace_util_0.Count(_find_best_task_00000, 61)
		return 1
	}
	trace_util_0.Count(_find_best_task_00000, 58)
	if setsResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		trace_util_0.Count(_find_best_task_00000, 62)
		return -1
	}
	trace_util_0.Count(_find_best_task_00000, 59)
	return 0
}

func (ds *DataSource) getTableCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	trace_util_0.Count(_find_best_task_00000, 63)
	candidate := &candidatePath{path: path}
	pkCol := ds.getPKIsHandleCol()
	candidate.isMatchProp = len(prop.Items) == 1 && pkCol != nil && prop.Items[0].Col.Equal(nil, pkCol)
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = true
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *accessPath, prop *property.PhysicalProperty) *candidatePath {
	trace_util_0.Count(_find_best_task_00000, 64)
	candidate := &candidatePath{path: path}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.
	if !prop.IsEmpty() && all {
		trace_util_0.Count(_find_best_task_00000, 66)
		for i, col := range path.index.Columns {
			trace_util_0.Count(_find_best_task_00000, 67)
			if col.Name.L == prop.Items[0].Col.ColName.L {
				trace_util_0.Count(_find_best_task_00000, 68)
				candidate.isMatchProp = matchIndicesProp(path.index.Columns[i:], prop.Items)
				break
			} else {
				trace_util_0.Count(_find_best_task_00000, 69)
				if i >= path.eqCondCount {
					trace_util_0.Count(_find_best_task_00000, 70)
					break
				}
			}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 65)
	candidate.columnSet = expression.ExtractColumnSet(path.accessConds)
	candidate.isSingleScan = isCoveringIndex(ds.schema.Columns, path.index.Columns, ds.tableInfo.PKIsHandle)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	trace_util_0.Count(_find_best_task_00000, 71)
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_find_best_task_00000, 73)
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			trace_util_0.Count(_find_best_task_00000, 77)
			return []*candidatePath{{path: path}}
		}
		trace_util_0.Count(_find_best_task_00000, 74)
		var currentCandidate *candidatePath
		if path.isTablePath {
			trace_util_0.Count(_find_best_task_00000, 78)
			currentCandidate = ds.getTableCandidate(path, prop)
		} else {
			trace_util_0.Count(_find_best_task_00000, 79)
			if len(path.accessConds) > 0 || !prop.IsEmpty() || path.forced {
				trace_util_0.Count(_find_best_task_00000, 80)
				// We will use index to generate physical plan if:
				// this path's access cond is not nil or
				// we have prop to match or
				// this index is forced to choose.
				currentCandidate = ds.getIndexCandidate(path, prop)
			} else {
				trace_util_0.Count(_find_best_task_00000, 81)
				{
					continue
				}
			}
		}
		trace_util_0.Count(_find_best_task_00000, 75)
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			trace_util_0.Count(_find_best_task_00000, 82)
			result := compareCandidates(candidates[i], currentCandidate)
			if result == 1 {
				trace_util_0.Count(_find_best_task_00000, 83)
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else {
				trace_util_0.Count(_find_best_task_00000, 84)
				if result == -1 {
					trace_util_0.Count(_find_best_task_00000, 85)
					candidates = append(candidates[:i], candidates[i+1:]...)
				}
			}
		}
		trace_util_0.Count(_find_best_task_00000, 76)
		if !pruned {
			trace_util_0.Count(_find_best_task_00000, 86)
			candidates = append(candidates, currentCandidate)
		}
	}
	trace_util_0.Count(_find_best_task_00000, 72)
	return candidates
}

// findBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) findBestTask(prop *property.PhysicalProperty) (t task, err error) {
	trace_util_0.Count(_find_best_task_00000, 87)
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		trace_util_0.Count(_find_best_task_00000, 95)
		return nil, nil
	}

	trace_util_0.Count(_find_best_task_00000, 88)
	t = ds.getTask(prop)
	if t != nil {
		trace_util_0.Count(_find_best_task_00000, 96)
		return
	}

	// If prop.enforced is true, the prop.cols need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	trace_util_0.Count(_find_best_task_00000, 89)
	oldPropCols := prop.Items
	if prop.Enforced {
		trace_util_0.Count(_find_best_task_00000, 97)
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		t, err = ds.findBestTask(prop)
		if err != nil {
			trace_util_0.Count(_find_best_task_00000, 100)
			return nil, err
		}
		trace_util_0.Count(_find_best_task_00000, 98)
		prop.Enforced = true
		if t != invalidTask {
			trace_util_0.Count(_find_best_task_00000, 101)
			ds.storeTask(prop, t)
			return
		}
		// Next, get the bestTask with enforced prop
		trace_util_0.Count(_find_best_task_00000, 99)
		prop.Items = []property.Item{}
	}
	trace_util_0.Count(_find_best_task_00000, 90)
	defer func() {
		trace_util_0.Count(_find_best_task_00000, 102)
		if err != nil {
			trace_util_0.Count(_find_best_task_00000, 105)
			return
		}
		trace_util_0.Count(_find_best_task_00000, 103)
		if prop.Enforced {
			trace_util_0.Count(_find_best_task_00000, 106)
			prop.Items = oldPropCols
			t = enforceProperty(prop, t, ds.basePlan.ctx)
		}
		trace_util_0.Count(_find_best_task_00000, 104)
		ds.storeTask(prop, t)
	}()

	trace_util_0.Count(_find_best_task_00000, 91)
	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		trace_util_0.Count(_find_best_task_00000, 107)
		return t, err
	}
	trace_util_0.Count(_find_best_task_00000, 92)
	t, err = ds.tryToGetMemTask(prop)
	if err != nil || t != nil {
		trace_util_0.Count(_find_best_task_00000, 108)
		return t, err
	}

	trace_util_0.Count(_find_best_task_00000, 93)
	t = invalidTask

	candidates := ds.skylinePruning(prop)
	for _, candidate := range candidates {
		trace_util_0.Count(_find_best_task_00000, 109)
		path := candidate.path
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.ranges) == 0 && !ds.ctx.GetSessionVars().StmtCtx.UseCache {
			trace_util_0.Count(_find_best_task_00000, 113)
			dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats)
			dual.SetSchema(ds.schema)
			return &rootTask{
				p: dual,
			}, nil
		}
		trace_util_0.Count(_find_best_task_00000, 110)
		if path.isTablePath {
			trace_util_0.Count(_find_best_task_00000, 114)
			tblTask, err := ds.convertToTableScan(prop, candidate)
			if err != nil {
				trace_util_0.Count(_find_best_task_00000, 117)
				return nil, err
			}
			trace_util_0.Count(_find_best_task_00000, 115)
			if tblTask.cost() < t.cost() {
				trace_util_0.Count(_find_best_task_00000, 118)
				t = tblTask
			}
			trace_util_0.Count(_find_best_task_00000, 116)
			continue
		}
		trace_util_0.Count(_find_best_task_00000, 111)
		idxTask, err := ds.convertToIndexScan(prop, candidate)
		if err != nil {
			trace_util_0.Count(_find_best_task_00000, 119)
			return nil, err
		}
		trace_util_0.Count(_find_best_task_00000, 112)
		if idxTask.cost() < t.cost() {
			trace_util_0.Count(_find_best_task_00000, 120)
			t = idxTask
		}
	}
	trace_util_0.Count(_find_best_task_00000, 94)
	return
}

func isCoveringIndex(columns []*expression.Column, indexColumns []*model.IndexColumn, pkIsHandle bool) bool {
	trace_util_0.Count(_find_best_task_00000, 121)
	for _, col := range columns {
		trace_util_0.Count(_find_best_task_00000, 123)
		if pkIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			trace_util_0.Count(_find_best_task_00000, 127)
			continue
		}
		trace_util_0.Count(_find_best_task_00000, 124)
		if col.ID == model.ExtraHandleID {
			trace_util_0.Count(_find_best_task_00000, 128)
			continue
		}
		trace_util_0.Count(_find_best_task_00000, 125)
		isIndexColumn := false
		for _, indexCol := range indexColumns {
			trace_util_0.Count(_find_best_task_00000, 129)
			isFullLen := indexCol.Length == types.UnspecifiedLength || indexCol.Length == col.RetType.Flen
			// We use col.OrigColName instead of col.ColName.
			// Related issue: https://github.com/pingcap/tidb/issues/9636.
			if col.OrigColName.L == indexCol.Name.L && isFullLen {
				trace_util_0.Count(_find_best_task_00000, 130)
				isIndexColumn = true
				break
			}
		}
		trace_util_0.Count(_find_best_task_00000, 126)
		if !isIndexColumn {
			trace_util_0.Count(_find_best_task_00000, 131)
			return false
		}
	}
	trace_util_0.Count(_find_best_task_00000, 122)
	return true
}

// If there is a table reader which needs to keep order, we should append a pk to table scan.
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) {
	trace_util_0.Count(_find_best_task_00000, 132)
	if len(ds.schema.TblID2Handle) > 0 {
		trace_util_0.Count(_find_best_task_00000, 134)
		return
	}
	trace_util_0.Count(_find_best_task_00000, 133)
	pkInfo := model.NewExtraHandleColInfo()
	ts.Columns = append(ts.Columns, pkInfo)
	handleCol := ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.schema.TblID2Handle[ds.tableInfo.ID] = []*expression.Column{handleCol}
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	trace_util_0.Count(_find_best_task_00000, 135)
	if !candidate.isSingleScan {
		trace_util_0.Count(_find_best_task_00000, 144)
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			trace_util_0.Count(_find_best_task_00000, 145)
			return invalidTask, nil
		}
	} else {
		trace_util_0.Count(_find_best_task_00000, 146)
		if prop.TaskTp == property.CopDoubleReadTaskType {
			trace_util_0.Count(_find_best_task_00000, 147)
			// If it's parent requires double read task, return max cost.
			return invalidTask, nil
		}
	}
	trace_util_0.Count(_find_best_task_00000, 136)
	if !prop.IsEmpty() && !candidate.isMatchProp {
		trace_util_0.Count(_find_best_task_00000, 148)
		return invalidTask, nil
	}
	trace_util_0.Count(_find_best_task_00000, 137)
	path := candidate.path
	idx := path.index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		IdxCols:          path.idxCols,
		IdxColLens:       path.idxColLens,
		AccessCondition:  path.accessConds,
		Ranges:           path.ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		trace_util_0.Count(_find_best_task_00000, 149)
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	trace_util_0.Count(_find_best_task_00000, 138)
	rowCount := path.countAfterAccess
	cop := &copTask{indexPlan: is}
	if !candidate.isSingleScan {
		trace_util_0.Count(_find_best_task_00000, 150)
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.Init(ds.ctx)
		ts.SetSchema(ds.schema.Clone())
		cop.tablePlan = ts
	}
	trace_util_0.Count(_find_best_task_00000, 139)
	is.initSchema(ds.id, idx, cop.tablePlan != nil)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (candidate.isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		trace_util_0.Count(_find_best_task_00000, 151)
		selectivity := ds.stats.RowCount / path.countAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	trace_util_0.Count(_find_best_task_00000, 140)
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		trace_util_0.Count(_find_best_task_00000, 152)
		is.stats.StatsVersion = statistics.PseudoVersion
	}

	trace_util_0.Count(_find_best_task_00000, 141)
	cop.cst = rowCount * scanFactor
	task = cop
	if candidate.isMatchProp {
		trace_util_0.Count(_find_best_task_00000, 153)
		if prop.Items[0].Desc {
			trace_util_0.Count(_find_best_task_00000, 156)
			is.Desc = true
			cop.cst = rowCount * descScanFactor
		}
		trace_util_0.Count(_find_best_task_00000, 154)
		if cop.tablePlan != nil {
			trace_util_0.Count(_find_best_task_00000, 157)
			cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.doubleReadNeedProj = true
		}
		trace_util_0.Count(_find_best_task_00000, 155)
		cop.keepOrder = true
		is.KeepOrder = true
	}
	// prop.IsEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of addPushedDownSelection.
	trace_util_0.Count(_find_best_task_00000, 142)
	finalStats := ds.stats.ScaleByExpectCnt(prop.ExpectedCnt)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	if prop.TaskTp == property.RootTaskType {
		trace_util_0.Count(_find_best_task_00000, 158)
		task = finishCopTask(ds.ctx, task)
	} else {
		trace_util_0.Count(_find_best_task_00000, 159)
		if _, ok := task.(*rootTask); ok {
			trace_util_0.Count(_find_best_task_00000, 160)
			return invalidTask, nil
		}
	}
	trace_util_0.Count(_find_best_task_00000, 143)
	return task, nil
}

// TODO: refactor this part, we should not call Clone in fact.
func (is *PhysicalIndexScan) initSchema(id int, idx *model.IndexInfo, isDoubleRead bool) {
	trace_util_0.Count(_find_best_task_00000, 161)
	indexCols := make([]*expression.Column, 0, len(idx.Columns))
	for _, col := range idx.Columns {
		trace_util_0.Count(_find_best_task_00000, 165)
		colFound := is.dataSourceSchema.FindColumnByName(col.Name.L)
		if colFound == nil {
			trace_util_0.Count(_find_best_task_00000, 167)
			colFound = &expression.Column{
				ColName:  col.Name,
				RetType:  &is.Table.Columns[col.Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			}
		} else {
			trace_util_0.Count(_find_best_task_00000, 168)
			{
				colFound = colFound.Clone().(*expression.Column)
			}
		}
		trace_util_0.Count(_find_best_task_00000, 166)
		indexCols = append(indexCols, colFound)
	}
	trace_util_0.Count(_find_best_task_00000, 162)
	setHandle := false
	for _, col := range is.Columns {
		trace_util_0.Count(_find_best_task_00000, 169)
		if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
			trace_util_0.Count(_find_best_task_00000, 170)
			indexCols = append(indexCols, is.dataSourceSchema.FindColumnByName(col.Name.L))
			setHandle = true
			break
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	trace_util_0.Count(_find_best_task_00000, 163)
	if isDoubleRead && !setHandle {
		trace_util_0.Count(_find_best_task_00000, 171)
		indexCols = append(indexCols, &expression.Column{ID: model.ExtraHandleID, ColName: model.ExtraHandleName, UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID()})
	}
	trace_util_0.Count(_find_best_task_00000, 164)
	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, path *accessPath, finalStats *property.StatsInfo) {
	trace_util_0.Count(_find_best_task_00000, 172)
	// Add filter condition to table plan now.
	indexConds, tableConds := path.indexFilters, path.tableFilters
	if indexConds != nil {
		trace_util_0.Count(_find_best_task_00000, 174)
		copTask.cst += copTask.count() * cpuFactor
		var selectivity float64
		if path.countAfterAccess > 0 {
			trace_util_0.Count(_find_best_task_00000, 176)
			selectivity = path.countAfterIndex / path.countAfterAccess
		}
		trace_util_0.Count(_find_best_task_00000, 175)
		count := is.stats.RowCount * selectivity
		stats := &property.StatsInfo{RowCount: count}
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	trace_util_0.Count(_find_best_task_00000, 173)
	if tableConds != nil {
		trace_util_0.Count(_find_best_task_00000, 177)
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * cpuFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.Init(is.ctx, finalStats)
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
	}
}

func matchIndicesProp(idxCols []*model.IndexColumn, propItems []property.Item) bool {
	trace_util_0.Count(_find_best_task_00000, 178)
	if len(idxCols) < len(propItems) {
		trace_util_0.Count(_find_best_task_00000, 181)
		return false
	}
	trace_util_0.Count(_find_best_task_00000, 179)
	for i, item := range propItems {
		trace_util_0.Count(_find_best_task_00000, 182)
		if idxCols[i].Length != types.UnspecifiedLength || item.Col.ColName.L != idxCols[i].Name.L {
			trace_util_0.Count(_find_best_task_00000, 183)
			return false
		}
	}
	trace_util_0.Count(_find_best_task_00000, 180)
	return true
}

func splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*model.IndexColumn,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	trace_util_0.Count(_find_best_task_00000, 184)
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		trace_util_0.Count(_find_best_task_00000, 186)
		if isCoveringIndex(expression.ExtractColumns(cond), indexColumns, table.PKIsHandle) {
			trace_util_0.Count(_find_best_task_00000, 187)
			indexConditions = append(indexConditions, cond)
		} else {
			trace_util_0.Count(_find_best_task_00000, 188)
			{
				tableConditions = append(tableConditions, cond)
			}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 185)
	return indexConditions, tableConditions
}

// getMostCorrColFromExprs checks if column in the condition is correlated enough with handle. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrColFromExprs(exprs []expression.Expression, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	trace_util_0.Count(_find_best_task_00000, 189)
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, exprs, nil)
	if len(cols) == 0 {
		trace_util_0.Count(_find_best_task_00000, 193)
		return nil, 0
	}
	trace_util_0.Count(_find_best_task_00000, 190)
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		trace_util_0.Count(_find_best_task_00000, 194)
		if colSet.Exist(col.UniqueID) {
			trace_util_0.Count(_find_best_task_00000, 197)
			continue
		}
		trace_util_0.Count(_find_best_task_00000, 195)
		colSet.Insert(col.UniqueID)
		hist, ok := histColl.Columns[col.ID]
		if !ok {
			trace_util_0.Count(_find_best_task_00000, 198)
			continue
		}
		trace_util_0.Count(_find_best_task_00000, 196)
		curCorr := math.Abs(hist.Correlation)
		if corrCol == nil || corr < curCorr {
			trace_util_0.Count(_find_best_task_00000, 199)
			corrCol = col
			corr = curCorr
		}
	}
	trace_util_0.Count(_find_best_task_00000, 191)
	if len(colSet) == 1 && corr >= threshold {
		trace_util_0.Count(_find_best_task_00000, 200)
		return corrCol, corr
	}
	trace_util_0.Count(_find_best_task_00000, 192)
	return nil, corr
}

// getColumnRangeCounts estimates row count for each range respectively.
func getColumnRangeCounts(sc *stmtctx.StatementContext, colID int64, ranges []*ranger.Range, histColl *statistics.Table, idxID int64) ([]float64, bool) {
	trace_util_0.Count(_find_best_task_00000, 201)
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		trace_util_0.Count(_find_best_task_00000, 203)
		if idxID >= 0 {
			trace_util_0.Count(_find_best_task_00000, 206)
			idxHist := histColl.Indices[idxID]
			if idxHist == nil || idxHist.IsInvalid(false) {
				trace_util_0.Count(_find_best_task_00000, 208)
				return nil, false
			}
			trace_util_0.Count(_find_best_task_00000, 207)
			count, err = histColl.GetRowCountByIndexRanges(sc, idxID, []*ranger.Range{ran})
		} else {
			trace_util_0.Count(_find_best_task_00000, 209)
			{
				colHist, ok := histColl.Columns[colID]
				if !ok || colHist.IsInvalid(sc, false) {
					trace_util_0.Count(_find_best_task_00000, 211)
					return nil, false
				}
				trace_util_0.Count(_find_best_task_00000, 210)
				count, err = histColl.GetRowCountByColumnRanges(sc, colID, []*ranger.Range{ran})
			}
		}
		trace_util_0.Count(_find_best_task_00000, 204)
		if err != nil {
			trace_util_0.Count(_find_best_task_00000, 212)
			return nil, false
		}
		trace_util_0.Count(_find_best_task_00000, 205)
		rangeCounts[i] = count
	}
	trace_util_0.Count(_find_best_task_00000, 202)
	return rangeCounts, true
}

// convertRangeFromExpectedCnt builds new ranges used to estimate row count we need to scan in table scan before finding specified
// number of tuples which fall into input ranges.
func convertRangeFromExpectedCnt(ranges []*ranger.Range, rangeCounts []float64, expectedCnt float64, desc bool) ([]*ranger.Range, float64, bool) {
	trace_util_0.Count(_find_best_task_00000, 213)
	var i int
	var count float64
	var convertedRanges []*ranger.Range
	if desc {
		trace_util_0.Count(_find_best_task_00000, 215)
		for i = len(ranges) - 1; i >= 0; i-- {
			trace_util_0.Count(_find_best_task_00000, 218)
			if count+rangeCounts[i] >= expectedCnt {
				trace_util_0.Count(_find_best_task_00000, 220)
				break
			}
			trace_util_0.Count(_find_best_task_00000, 219)
			count += rangeCounts[i]
		}
		trace_util_0.Count(_find_best_task_00000, 216)
		if i < 0 {
			trace_util_0.Count(_find_best_task_00000, 221)
			return nil, 0, true
		}
		trace_util_0.Count(_find_best_task_00000, 217)
		convertedRanges = []*ranger.Range{{LowVal: ranges[i].HighVal, HighVal: []types.Datum{types.MaxValueDatum()}, LowExclude: !ranges[i].HighExclude}}
	} else {
		trace_util_0.Count(_find_best_task_00000, 222)
		{
			for i = 0; i < len(ranges); i++ {
				trace_util_0.Count(_find_best_task_00000, 225)
				if count+rangeCounts[i] >= expectedCnt {
					trace_util_0.Count(_find_best_task_00000, 227)
					break
				}
				trace_util_0.Count(_find_best_task_00000, 226)
				count += rangeCounts[i]
			}
			trace_util_0.Count(_find_best_task_00000, 223)
			if i == len(ranges) {
				trace_util_0.Count(_find_best_task_00000, 228)
				return nil, 0, true
			}
			trace_util_0.Count(_find_best_task_00000, 224)
			convertedRanges = []*ranger.Range{{LowVal: []types.Datum{{}}, HighVal: ranges[i].LowVal, HighExclude: !ranges[i].LowExclude}}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 214)
	return convertedRanges, count, false
}

// crossEstimateRowCount estimates row count of table scan using histogram of another column which is in tableFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the row count of table scan should be:
// `1 + row_count(a < 1 or a is null)`
func (ds *DataSource) crossEstimateRowCount(path *accessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	trace_util_0.Count(_find_best_task_00000, 229)
	if ds.statisticTable.Pseudo || len(path.tableFilters) == 0 {
		trace_util_0.Count(_find_best_task_00000, 241)
		return 0, false, 0
	}
	trace_util_0.Count(_find_best_task_00000, 230)
	col, corr := getMostCorrColFromExprs(path.tableFilters, ds.statisticTable, ds.ctx.GetSessionVars().CorrelationThreshold)
	// If table scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole table level.
	if col == nil || len(path.accessConds) > 0 {
		trace_util_0.Count(_find_best_task_00000, 242)
		return 0, false, corr
	}
	trace_util_0.Count(_find_best_task_00000, 231)
	colInfoID := col.ID
	colID := col.UniqueID
	colHist := ds.statisticTable.Columns[colInfoID]
	if colHist.Correlation < 0 {
		trace_util_0.Count(_find_best_task_00000, 243)
		desc = !desc
	}
	trace_util_0.Count(_find_best_task_00000, 232)
	accessConds, remained := ranger.DetachCondsForColumn(ds.ctx, path.tableFilters, col)
	if len(accessConds) == 0 {
		trace_util_0.Count(_find_best_task_00000, 244)
		return 0, false, corr
	}
	trace_util_0.Count(_find_best_task_00000, 233)
	sc := ds.ctx.GetSessionVars().StmtCtx
	ranges, err := ranger.BuildColumnRange(accessConds, sc, col.RetType, types.UnspecifiedLength)
	if len(ranges) == 0 || err != nil {
		trace_util_0.Count(_find_best_task_00000, 245)
		return 0, err == nil, corr
	}
	trace_util_0.Count(_find_best_task_00000, 234)
	idxID, idxExists := ds.stats.HistColl.ColID2IdxID[colID]
	if !idxExists {
		trace_util_0.Count(_find_best_task_00000, 246)
		idxID = -1
	}
	trace_util_0.Count(_find_best_task_00000, 235)
	rangeCounts, ok := getColumnRangeCounts(sc, colInfoID, ranges, ds.statisticTable, idxID)
	if !ok {
		trace_util_0.Count(_find_best_task_00000, 247)
		return 0, false, corr
	}
	trace_util_0.Count(_find_best_task_00000, 236)
	convertedRanges, count, isFull := convertRangeFromExpectedCnt(ranges, rangeCounts, expectedCnt, desc)
	if isFull {
		trace_util_0.Count(_find_best_task_00000, 248)
		return path.countAfterAccess, true, 0
	}
	trace_util_0.Count(_find_best_task_00000, 237)
	var rangeCount float64
	if idxExists {
		trace_util_0.Count(_find_best_task_00000, 249)
		rangeCount, err = ds.statisticTable.GetRowCountByIndexRanges(sc, idxID, convertedRanges)
	} else {
		trace_util_0.Count(_find_best_task_00000, 250)
		{
			rangeCount, err = ds.statisticTable.GetRowCountByColumnRanges(sc, colInfoID, convertedRanges)
		}
	}
	trace_util_0.Count(_find_best_task_00000, 238)
	if err != nil {
		trace_util_0.Count(_find_best_task_00000, 251)
		return 0, false, corr
	}
	trace_util_0.Count(_find_best_task_00000, 239)
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		trace_util_0.Count(_find_best_task_00000, 252)
		scanCount = scanCount / selectionFactor
	}
	trace_util_0.Count(_find_best_task_00000, 240)
	scanCount = math.Min(scanCount, path.countAfterAccess)
	return scanCount, true, 0
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	trace_util_0.Count(_find_best_task_00000, 253)
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopDoubleReadTaskType {
		trace_util_0.Count(_find_best_task_00000, 261)
		return invalidTask, nil
	}
	trace_util_0.Count(_find_best_task_00000, 254)
	if !prop.IsEmpty() && !candidate.isMatchProp {
		trace_util_0.Count(_find_best_task_00000, 262)
		return invalidTask, nil
	}
	trace_util_0.Count(_find_best_task_00000, 255)
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
	}.Init(ds.ctx)
	ts.SetSchema(ds.schema)
	if ts.Table.PKIsHandle {
		trace_util_0.Count(_find_best_task_00000, 263)
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			trace_util_0.Count(_find_best_task_00000, 264)
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				trace_util_0.Count(_find_best_task_00000, 265)
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 256)
	path := candidate.path
	ts.Ranges = path.ranges
	ts.AccessCondition, ts.filterCondition = path.accessConds, path.tableFilters
	rowCount := path.countAfterAccess
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	task = copTask
	// Adjust number of rows we actually need to scan if prop.ExpectedCnt is smaller than the count we calculated.
	if prop.ExpectedCnt < ds.stats.RowCount {
		trace_util_0.Count(_find_best_task_00000, 266)
		count, ok, corr := ds.crossEstimateRowCount(path, prop.ExpectedCnt, candidate.isMatchProp && prop.Items[0].Desc)
		if ok {
			trace_util_0.Count(_find_best_task_00000, 267)
			// TODO: actually, before using this count as the estimated row count of table scan, we need additionally
			// check if count < row_count(first_region | last_region), and use the larger one since we build one copTask
			// for one region now, so even if it is `limit 1`, we have to scan at least one region in table scan.
			// Currently, we can use `tikvrpc.CmdDebugGetRegionProperties` interface as `getSampRegionsRowCount()` does
			// to get the row count in a region, but that result contains MVCC old version rows, so it is not that accurate.
			// Considering that when this scenario happens, the execution time is close between IndexScan and TableScan,
			// we do not add this check temporarily.
			rowCount = count
		} else {
			trace_util_0.Count(_find_best_task_00000, 268)
			if corr < 1 {
				trace_util_0.Count(_find_best_task_00000, 269)
				correlationFactor := math.Pow(1-corr, float64(ds.ctx.GetSessionVars().CorrelationExpFactor))
				selectivity := ds.stats.RowCount / rowCount
				rowCount = math.Min(prop.ExpectedCnt/selectivity/correlationFactor, rowCount)
			}
		}
	}
	trace_util_0.Count(_find_best_task_00000, 257)
	ts.stats = property.NewSimpleStats(rowCount)
	ts.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		trace_util_0.Count(_find_best_task_00000, 270)
		ts.stats.StatsVersion = statistics.PseudoVersion
	}

	trace_util_0.Count(_find_best_task_00000, 258)
	copTask.cst = rowCount * scanFactor
	if candidate.isMatchProp {
		trace_util_0.Count(_find_best_task_00000, 271)
		if prop.Items[0].Desc {
			trace_util_0.Count(_find_best_task_00000, 273)
			ts.Desc = true
			copTask.cst = rowCount * descScanFactor
		}
		trace_util_0.Count(_find_best_task_00000, 272)
		ts.KeepOrder = true
		copTask.keepOrder = true
	}
	trace_util_0.Count(_find_best_task_00000, 259)
	ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	if prop.TaskTp == property.RootTaskType {
		trace_util_0.Count(_find_best_task_00000, 274)
		task = finishCopTask(ds.ctx, task)
	} else {
		trace_util_0.Count(_find_best_task_00000, 275)
		if _, ok := task.(*rootTask); ok {
			trace_util_0.Count(_find_best_task_00000, 276)
			return invalidTask, nil
		}
	}
	trace_util_0.Count(_find_best_task_00000, 260)
	return task, nil
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *property.StatsInfo) {
	trace_util_0.Count(_find_best_task_00000, 277)
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		trace_util_0.Count(_find_best_task_00000, 278)
		copTask.cst += copTask.count() * cpuFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats)
		sel.SetChildren(ts)
		copTask.tablePlan = sel
	}
}

var _find_best_task_00000 = "planner/core/find_best_task.go"
