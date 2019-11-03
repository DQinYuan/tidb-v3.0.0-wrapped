// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
)

// partitionProcessor rewrites the ast for table partition.
//
// create table t (id int) partition by range (id)
//   (partition p1 values less than (10),
//    partition p2 values less than (20),
//    partition p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
//
// partitionProcessor is here because it's easier to prune partition after predicate push down.
type partitionProcessor struct{}

func (s *partitionProcessor) optimize(lp LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_partition_processor_00000, 0)
	return s.rewriteDataSource(lp)
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_partition_processor_00000, 1)
	// Assert there will not be sel -> sel in the ast.
	switch lp.(type) {
	case *DataSource:
		trace_util_0.Count(_rule_partition_processor_00000, 3)
		return s.prune(lp.(*DataSource))
	case *LogicalUnionScan:
		trace_util_0.Count(_rule_partition_processor_00000, 4)
		us := lp.(*LogicalUnionScan)
		ds := us.Children()[0]
		ds, err := s.prune(ds.(*DataSource))
		if err != nil {
			trace_util_0.Count(_rule_partition_processor_00000, 8)
			return nil, err
		}
		trace_util_0.Count(_rule_partition_processor_00000, 5)
		if ua, ok := ds.(*LogicalUnionAll); ok {
			trace_util_0.Count(_rule_partition_processor_00000, 9)
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				trace_util_0.Count(_rule_partition_processor_00000, 11)
				us := LogicalUnionScan{}.Init(ua.ctx)
				us.SetChildren(child)
				children = append(children, us)
			}
			trace_util_0.Count(_rule_partition_processor_00000, 10)
			ua.SetChildren(children...)
			return ua, nil
		}
		// Only one partition, no union all.
		trace_util_0.Count(_rule_partition_processor_00000, 6)
		us.SetChildren(ds)
		return us, nil
	default:
		trace_util_0.Count(_rule_partition_processor_00000, 7)
		children := lp.Children()
		for i, child := range children {
			trace_util_0.Count(_rule_partition_processor_00000, 12)
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				trace_util_0.Count(_rule_partition_processor_00000, 14)
				return nil, err
			}
			trace_util_0.Count(_rule_partition_processor_00000, 13)
			children[i] = newChild
		}
	}

	trace_util_0.Count(_rule_partition_processor_00000, 2)
	return lp, nil
}

// partitionTable is for those tables which implement partition.
type partitionTable interface {
	PartitionExpr() *tables.PartitionExpr
}

func (s *partitionProcessor) prune(ds *DataSource) (LogicalPlan, error) {
	trace_util_0.Count(_rule_partition_processor_00000, 15)
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_rule_partition_processor_00000, 22)
		return ds, nil
	}

	trace_util_0.Count(_rule_partition_processor_00000, 16)
	var partitionExprs []expression.Expression
	var col *expression.Column
	if table, ok := ds.table.(partitionTable); ok {
		trace_util_0.Count(_rule_partition_processor_00000, 23)
		partitionExprs = table.PartitionExpr().Ranges
		col = table.PartitionExpr().Column
	}
	trace_util_0.Count(_rule_partition_processor_00000, 17)
	if len(partitionExprs) == 0 {
		trace_util_0.Count(_rule_partition_processor_00000, 24)
		return nil, errors.New("partition expression missing")
	}
	trace_util_0.Count(_rule_partition_processor_00000, 18)
	partitionDefs := ds.table.Meta().Partition.Definitions

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to the filter conditions pushed to the DataSource.
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	for i, expr := range partitionExprs {
		trace_util_0.Count(_rule_partition_processor_00000, 25)
		// If the select condition would never be satisified, prune that partition.
		pruned, err := s.canBePruned(ds.context(), col, expr, ds.allConds)
		if err != nil {
			trace_util_0.Count(_rule_partition_processor_00000, 29)
			return nil, err
		}
		trace_util_0.Count(_rule_partition_processor_00000, 26)
		if pruned {
			trace_util_0.Count(_rule_partition_processor_00000, 30)
			continue
		}
		// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
		trace_util_0.Count(_rule_partition_processor_00000, 27)
		if len(ds.partitionNames) != 0 {
			trace_util_0.Count(_rule_partition_processor_00000, 31)
			if !s.findByName(ds.partitionNames, partitionDefs[i].Name.L) {
				trace_util_0.Count(_rule_partition_processor_00000, 32)
				continue
			}
		}

		// Not a deep copy.
		trace_util_0.Count(_rule_partition_processor_00000, 28)
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), TypeTableScan, &newDataSource)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[i].ID
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.context(), ds.table.Meta(), pi.Definitions[i].ID)
		children = append(children, &newDataSource)
	}
	trace_util_0.Count(_rule_partition_processor_00000, 19)
	if len(children) == 0 {
		trace_util_0.Count(_rule_partition_processor_00000, 33)
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.context())
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	trace_util_0.Count(_rule_partition_processor_00000, 20)
	if len(children) == 1 {
		trace_util_0.Count(_rule_partition_processor_00000, 34)
		// No need for the union all.
		return children[0], nil
	}
	trace_util_0.Count(_rule_partition_processor_00000, 21)
	unionAll := LogicalUnionAll{}.Init(ds.context())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema)
	return unionAll, nil
}

var solver = expression.NewPartitionPruneSolver()

// canBePruned checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then canBePrune returns true.
func (s *partitionProcessor) canBePruned(sctx sessionctx.Context, partCol *expression.Column, partExpr expression.Expression, filterExprs []expression.Expression) (bool, error) {
	trace_util_0.Count(_rule_partition_processor_00000, 35)
	conds := make([]expression.Expression, 0, 1+len(filterExprs))
	conds = append(conds, partExpr)
	conds = append(conds, filterExprs...)
	conds = expression.PropagateConstant(sctx, conds)
	conds = solver.Solve(sctx, conds)

	if len(conds) == 1 {
		trace_util_0.Count(_rule_partition_processor_00000, 39)
		// Constant false.
		if con, ok := conds[0].(*expression.Constant); ok && con.DeferredExpr == nil {
			trace_util_0.Count(_rule_partition_processor_00000, 41)
			ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
			if err == nil && ret == false {
				trace_util_0.Count(_rule_partition_processor_00000, 42)
				return true, nil
			}
		}
		// Not a constant false, but this is the only condition, it can't be pruned.
		trace_util_0.Count(_rule_partition_processor_00000, 40)
		return false, nil
	}

	// Calculates the column range to prune.
	trace_util_0.Count(_rule_partition_processor_00000, 36)
	if partCol == nil {
		trace_util_0.Count(_rule_partition_processor_00000, 43)
		// If partition column is nil, we can't calculate range, so we can't prune
		// partition by range.
		return false, nil
	}

	// TODO: Remove prune by calculating range. Current constraint propagate doesn't
	// handle the null condition, while calculate range can prune something like:
	// "select * from t where t is null"
	trace_util_0.Count(_rule_partition_processor_00000, 37)
	accessConds := ranger.ExtractAccessConditionsForColumn(conds, partCol.UniqueID)
	r, err := ranger.BuildColumnRange(accessConds, sctx.GetSessionVars().StmtCtx, partCol.RetType, types.UnspecifiedLength)
	if err != nil {
		trace_util_0.Count(_rule_partition_processor_00000, 44)
		return false, err
	}
	trace_util_0.Count(_rule_partition_processor_00000, 38)
	return len(r) == 0, nil
}

// findByName checks whether object name exists in list.
func (s *partitionProcessor) findByName(partitionNames []model.CIStr, partitionName string) bool {
	trace_util_0.Count(_rule_partition_processor_00000, 45)
	for _, s := range partitionNames {
		trace_util_0.Count(_rule_partition_processor_00000, 47)
		if s.L == partitionName {
			trace_util_0.Count(_rule_partition_processor_00000, 48)
			return true
		}
	}
	trace_util_0.Count(_rule_partition_processor_00000, 46)
	return false
}

var _rule_partition_processor_00000 = "planner/core/rule_partition_processor.go"
