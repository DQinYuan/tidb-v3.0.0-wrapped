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
	"fmt"
	"math"

	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type task interface {
	count() float64
	addCost(cost float64)
	cost() float64
	copy() task
	plan() PhysicalPlan
	invalid() bool
}

// copTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type copTask struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// In double read case, it may output one more column for handle(row id).
	// We need to prune it, so we add a project do this.
	doubleReadNeedProj bool
}

func (t *copTask) invalid() bool {
	trace_util_0.Count(_task_00000, 0)
	return t.tablePlan == nil && t.indexPlan == nil
}

func (t *rootTask) invalid() bool {
	trace_util_0.Count(_task_00000, 1)
	return t.p == nil
}

func (t *copTask) count() float64 {
	trace_util_0.Count(_task_00000, 2)
	if t.indexPlanFinished {
		trace_util_0.Count(_task_00000, 4)
		return t.tablePlan.statsInfo().RowCount
	}
	trace_util_0.Count(_task_00000, 3)
	return t.indexPlan.statsInfo().RowCount
}

func (t *copTask) addCost(cst float64) {
	trace_util_0.Count(_task_00000, 5)
	t.cst += cst
}

func (t *copTask) cost() float64 {
	trace_util_0.Count(_task_00000, 6)
	return t.cst
}

func (t *copTask) copy() task {
	trace_util_0.Count(_task_00000, 7)
	nt := *t
	return &nt
}

func (t *copTask) plan() PhysicalPlan {
	trace_util_0.Count(_task_00000, 8)
	if t.indexPlanFinished {
		trace_util_0.Count(_task_00000, 10)
		return t.tablePlan
	}
	trace_util_0.Count(_task_00000, 9)
	return t.indexPlan
}

func attachPlan2Task(p PhysicalPlan, t task) task {
	trace_util_0.Count(_task_00000, 11)
	switch v := t.(type) {
	case *copTask:
		trace_util_0.Count(_task_00000, 13)
		if v.indexPlanFinished {
			trace_util_0.Count(_task_00000, 15)
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			trace_util_0.Count(_task_00000, 16)
			{
				p.SetChildren(v.indexPlan)
				v.indexPlan = p
			}
		}
	case *rootTask:
		trace_util_0.Count(_task_00000, 14)
		p.SetChildren(v.p)
		v.p = p
	}
	trace_util_0.Count(_task_00000, 12)
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTask) finishIndexPlan() {
	trace_util_0.Count(_task_00000, 17)
	if !t.indexPlanFinished {
		trace_util_0.Count(_task_00000, 18)
		t.cst += t.count() * netWorkFactor
		t.indexPlanFinished = true
		if t.tablePlan != nil {
			trace_util_0.Count(_task_00000, 19)
			t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
			t.cst += t.count() * scanFactor
		}
	}
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 20)
	t := finishCopTask(p.ctx, tasks[0].copy())
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 21)
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + lTask.count()*rTask.cost(),
	}
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 22)
	outerTask := finishCopTask(p.ctx, tasks[p.OuterIndex].copy())
	if p.OuterIndex == 0 {
		trace_util_0.Count(_task_00000, 24)
		p.SetChildren(outerTask.plan(), p.innerPlan)
	} else {
		trace_util_0.Count(_task_00000, 25)
		{
			p.SetChildren(p.innerPlan, outerTask.plan())
		}
	}
	trace_util_0.Count(_task_00000, 23)
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: outerTask.cost() + p.getCost(outerTask.count()),
	}
}

func (p *PhysicalIndexJoin) getCost(lCnt float64) float64 {
	trace_util_0.Count(_task_00000, 26)
	if lCnt < 1 {
		trace_util_0.Count(_task_00000, 28)
		lCnt = 1
	}
	trace_util_0.Count(_task_00000, 27)
	cst := lCnt * netWorkFactor
	batchSize := p.ctx.GetSessionVars().IndexJoinBatchSize
	cst += lCnt * math.Log2(math.Min(float64(batchSize), lCnt)) * 2
	cst += lCnt / float64(batchSize) * netWorkStartFactor
	return cst
}

func (p *PhysicalHashJoin) getCost(lCnt, rCnt float64) float64 {
	trace_util_0.Count(_task_00000, 29)
	smallTableCnt := lCnt
	if p.InnerChildIdx == 1 {
		trace_util_0.Count(_task_00000, 32)
		smallTableCnt = rCnt
	}
	trace_util_0.Count(_task_00000, 30)
	if smallTableCnt <= 1 {
		trace_util_0.Count(_task_00000, 33)
		smallTableCnt = 1
	}
	trace_util_0.Count(_task_00000, 31)
	return (lCnt + rCnt) * (1 + math.Log2(smallTableCnt)/float64(p.Concurrency))
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 34)
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

func (p *PhysicalMergeJoin) getCost(lCnt, rCnt float64) float64 {
	trace_util_0.Count(_task_00000, 35)
	return lCnt + rCnt
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 36)
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(ctx sessionctx.Context, task task) task {
	trace_util_0.Count(_task_00000, 37)
	t, ok := task.(*copTask)
	if !ok {
		trace_util_0.Count(_task_00000, 41)
		return task
	}
	// FIXME: When it is a double reading. The cost should be more expensive. The right cost should add the
	// `NetWorkStartCost` * (totalCount / perCountIndexRead)
	trace_util_0.Count(_task_00000, 38)
	t.finishIndexPlan()
	if t.tablePlan != nil {
		trace_util_0.Count(_task_00000, 42)
		t.cst += t.count() * netWorkFactor
	}
	trace_util_0.Count(_task_00000, 39)
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		trace_util_0.Count(_task_00000, 43)
		p := PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		if t.doubleReadNeedProj {
			trace_util_0.Count(_task_00000, 44)
			schema := p.IndexPlans[0].(*PhysicalIndexScan).dataSourceSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.p = proj
		} else {
			trace_util_0.Count(_task_00000, 45)
			{
				newTask.p = p
			}
		}
	} else {
		trace_util_0.Count(_task_00000, 46)
		if t.indexPlan != nil {
			trace_util_0.Count(_task_00000, 47)
			p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx)
			p.stats = t.indexPlan.statsInfo()
			newTask.p = p
		} else {
			trace_util_0.Count(_task_00000, 48)
			{
				p := PhysicalTableReader{tablePlan: t.tablePlan}.Init(ctx)
				p.stats = t.tablePlan.statsInfo()
				newTask.p = p
			}
		}
	}
	trace_util_0.Count(_task_00000, 40)
	return newTask
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTask struct {
	p   PhysicalPlan
	cst float64
}

func (t *rootTask) copy() task {
	trace_util_0.Count(_task_00000, 49)
	return &rootTask{
		p:   t.p,
		cst: t.cst,
	}
}

func (t *rootTask) count() float64 {
	trace_util_0.Count(_task_00000, 50)
	return t.p.statsInfo().RowCount
}

func (t *rootTask) addCost(cst float64) {
	trace_util_0.Count(_task_00000, 51)
	t.cst += cst
}

func (t *rootTask) cost() float64 {
	trace_util_0.Count(_task_00000, 52)
	return t.cst
}

func (t *rootTask) plan() PhysicalPlan {
	trace_util_0.Count(_task_00000, 53)
	return t.p
}

func (p *PhysicalLimit) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 54)
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		trace_util_0.Count(_task_00000, 56)
		// If the table/index scans data by order and applies a double read, the limit cannot be pushed to the table side.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
			trace_util_0.Count(_task_00000, 58)
			// When limit be pushed down, it should remove its offset.
			pushedDownLimit := PhysicalLimit{Count: p.Offset + p.Count}.Init(p.ctx, p.stats)
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		}
		trace_util_0.Count(_task_00000, 57)
		t = finishCopTask(p.ctx, cop)
	}
	trace_util_0.Count(_task_00000, 55)
	t = attachPlan2Task(p, t)
	return t
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64) float64 {
	trace_util_0.Count(_task_00000, 59)
	if count < 2.0 {
		trace_util_0.Count(_task_00000, 61)
		count = 2.0
	}
	trace_util_0.Count(_task_00000, 60)
	return count*cpuFactor + count*memoryFactor
}

func (p *PhysicalTopN) getCost(count float64) float64 {
	trace_util_0.Count(_task_00000, 62)
	return count*cpuFactor + float64(p.Count)*memoryFactor
}

// canPushDown checks if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *PhysicalTopN) canPushDown() bool {
	trace_util_0.Count(_task_00000, 63)
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		trace_util_0.Count(_task_00000, 65)
		exprs = append(exprs, item.Expr)
	}
	trace_util_0.Count(_task_00000, 64)
	_, _, remained := expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
	return len(remained) == 0
}

func (p *PhysicalTopN) allColsFromSchema(schema *expression.Schema) bool {
	trace_util_0.Count(_task_00000, 66)
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		trace_util_0.Count(_task_00000, 68)
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	trace_util_0.Count(_task_00000, 67)
	return len(schema.ColumnsIndices(cols)) > 0
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 69)
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 70)
	return tasks[0]
}

func (p *PhysicalTopN) getPushedDownTopN() *PhysicalTopN {
	trace_util_0.Count(_task_00000, 71)
	newByItems := make([]*ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		trace_util_0.Count(_task_00000, 73)
		newByItems = append(newByItems, expr.Clone())
	}
	trace_util_0.Count(_task_00000, 72)
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   p.Offset + p.Count,
	}.Init(p.ctx, p.stats)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 74)
	t := tasks[0].copy()
	// This is a topN plan.
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
		trace_util_0.Count(_task_00000, 76)
		pushedDownTopN := p.getPushedDownTopN()
		// If all columns in topN are from index plan, we can push it to index plan. Or we finish the index plan and
		// push it to table plan.
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			trace_util_0.Count(_task_00000, 78)
			pushedDownTopN.SetChildren(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			trace_util_0.Count(_task_00000, 79)
			{
				// FIXME: When we pushed down a top-N plan to table plan branch in case of double reading. The cost should
				// be more expensive in case of single reading, because we may execute table scan multi times.
				copTask.finishIndexPlan()
				pushedDownTopN.SetChildren(copTask.tablePlan)
				copTask.tablePlan = pushedDownTopN
			}
		}
		trace_util_0.Count(_task_00000, 77)
		copTask.addCost(pushedDownTopN.getCost(t.count()))
	}
	trace_util_0.Count(_task_00000, 75)
	t = finishCopTask(p.ctx, t)
	t = attachPlan2Task(p, t)
	t.addCost(p.getCost(t.count()))
	return t
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 80)
	t := tasks[0].copy()
	switch tp := t.(type) {
	case *copTask:
		trace_util_0.Count(_task_00000, 82)
		// TODO: Support projection push down.
		t = finishCopTask(p.ctx, t)
		t = attachPlan2Task(p, t)
		return t
	case *rootTask:
		trace_util_0.Count(_task_00000, 83)
		return attachPlan2Task(p, tp)
	}
	trace_util_0.Count(_task_00000, 81)
	return nil
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 84)
	newTask := &rootTask{p: p}
	newChildren := make([]PhysicalPlan, 0, len(p.children))
	for _, task := range tasks {
		trace_util_0.Count(_task_00000, 86)
		task = finishCopTask(p.ctx, task)
		newTask.cst += task.cost()
		newChildren = append(newChildren, task.plan())
	}
	trace_util_0.Count(_task_00000, 85)
	p.SetChildren(newChildren...)
	return newTask
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 87)
	t := finishCopTask(sel.ctx, tasks[0].copy())
	t.addCost(t.count() * cpuFactor)
	t = attachPlan2Task(sel, t)
	return t
}

func (p *basePhysicalAgg) newPartialAggregate() (partial, final PhysicalPlan) {
	trace_util_0.Count(_task_00000, 88)
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		trace_util_0.Count(_task_00000, 94)
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			trace_util_0.Count(_task_00000, 95)
			return nil, p.self
		}
	}
	trace_util_0.Count(_task_00000, 89)
	_, _, remained := expression.ExpressionsToPB(sc, p.GroupByItems, client)
	if len(remained) > 0 {
		trace_util_0.Count(_task_00000, 96)
		return nil, p.self
	}

	trace_util_0.Count(_task_00000, 90)
	finalSchema := p.schema
	partialSchema := expression.NewSchema()
	p.schema = partialSchema
	partialAgg := p.self

	// TODO: Refactor the way of constructing aggregation functions.
	partialCursor := 0
	finalAggFuncs := make([]*aggregation.AggFuncDesc, len(p.AggFuncs))
	for i, aggFun := range p.AggFuncs {
		trace_util_0.Count(_task_00000, 97)
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFun.Name
		args := make([]expression.Expression, 0, len(aggFun.Args))
		if aggregation.NeedCount(finalAggFunc.Name) {
			trace_util_0.Count(_task_00000, 100)
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen, ft.Charset, ft.Collate = 21, charset.CharsetBin, charset.CollationBin
			partialSchema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor)),
				RetType:  ft,
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		trace_util_0.Count(_task_00000, 98)
		if aggregation.NeedValue(finalAggFunc.Name) {
			trace_util_0.Count(_task_00000, 101)
			partialSchema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor)),
				RetType:  finalSchema.Columns[i].GetType(),
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		trace_util_0.Count(_task_00000, 99)
		finalAggFunc.Args = args
		finalAggFunc.Mode = aggregation.FinalMode
		finalAggFunc.RetTp = aggFun.RetTp
		finalAggFuncs[i] = finalAggFunc
	}

	// add group by columns
	trace_util_0.Count(_task_00000, 91)
	groupByItems := make([]expression.Expression, 0, len(p.GroupByItems))
	for i, gbyExpr := range p.GroupByItems {
		trace_util_0.Count(_task_00000, 102)
		gbyCol := &expression.Column{
			UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor+i)),
			RetType:  gbyExpr.GetType(),
		}
		partialSchema.Append(gbyCol)
		groupByItems = append(groupByItems, gbyCol)
	}

	// Create physical "final" aggregation.
	trace_util_0.Count(_task_00000, 92)
	if p.tp == TypeStreamAgg {
		trace_util_0.Count(_task_00000, 103)
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalAggFuncs,
			GroupByItems: groupByItems,
		}.initForStream(p.ctx, p.stats)
		finalAgg.schema = finalSchema
		return partialAgg, finalAgg
	}

	trace_util_0.Count(_task_00000, 93)
	finalAgg := basePhysicalAgg{
		AggFuncs:     finalAggFuncs,
		GroupByItems: groupByItems,
	}.initForHash(p.ctx, p.stats)
	finalAgg.schema = finalSchema
	return partialAgg, finalAgg
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 104)
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		trace_util_0.Count(_task_00000, 107)
		partialAgg, finalAgg := p.newPartialAggregate()
		if partialAgg != nil {
			trace_util_0.Count(_task_00000, 109)
			if cop.tablePlan != nil {
				trace_util_0.Count(_task_00000, 110)
				partialAgg.SetChildren(cop.tablePlan)
				cop.tablePlan = partialAgg
			} else {
				trace_util_0.Count(_task_00000, 111)
				{
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
			}
		}
		trace_util_0.Count(_task_00000, 108)
		t = finishCopTask(p.ctx, cop)
		attachPlan2Task(finalAgg, t)
	} else {
		trace_util_0.Count(_task_00000, 112)
		{
			attachPlan2Task(p, t)
		}
	}
	trace_util_0.Count(_task_00000, 105)
	t.addCost(t.count() * cpuFactor)
	if p.hasDistinctFunc() {
		trace_util_0.Count(_task_00000, 113)
		t.addCost(t.count() * cpuFactor * distinctAggFactor)
	}
	trace_util_0.Count(_task_00000, 106)
	return t
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	trace_util_0.Count(_task_00000, 114)
	cardinality := p.statsInfo().RowCount
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		trace_util_0.Count(_task_00000, 117)
		partialAgg, finalAgg := p.newPartialAggregate()
		if partialAgg != nil {
			trace_util_0.Count(_task_00000, 119)
			if cop.tablePlan != nil {
				trace_util_0.Count(_task_00000, 120)
				cop.finishIndexPlan()
				partialAgg.SetChildren(cop.tablePlan)
				cop.tablePlan = partialAgg
			} else {
				trace_util_0.Count(_task_00000, 121)
				{
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
			}
		}
		trace_util_0.Count(_task_00000, 118)
		t = finishCopTask(p.ctx, cop)
		attachPlan2Task(finalAgg, t)
	} else {
		trace_util_0.Count(_task_00000, 122)
		{
			attachPlan2Task(p, t)
		}
	}
	trace_util_0.Count(_task_00000, 115)
	t.addCost(t.count()*cpuFactor*hashAggFactor + cardinality*createAggCtxFactor)
	if p.hasDistinctFunc() {
		trace_util_0.Count(_task_00000, 123)
		t.addCost(t.count() * cpuFactor * distinctAggFactor)
	}
	trace_util_0.Count(_task_00000, 116)
	return t
}

var _task_00000 = "planner/core/task.go"
