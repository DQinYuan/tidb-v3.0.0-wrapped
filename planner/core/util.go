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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_util_00000, 0)
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_util_00000, 2)
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		trace_util_0.Count(_util_00000, 3)
		return n, true
	}
	trace_util_0.Count(_util_00000, 1)
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_util_00000, 4)
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		trace_util_0.Count(_util_00000, 6)
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	trace_util_0.Count(_util_00000, 5)
	return n, true
}

// WindowFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to WindowFuncExpr and collects WindowFuncExpr.
type WindowFuncExtractor struct {
	// WindowFuncs is the collected WindowFuncExprs.
	windowFuncs []*ast.WindowFuncExpr
}

// Enter implements Visitor interface.
func (a *WindowFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_util_00000, 7)
	switch n.(type) {
	case *ast.SelectStmt, *ast.UnionStmt:
		trace_util_0.Count(_util_00000, 9)
		return n, true
	}
	trace_util_0.Count(_util_00000, 8)
	return n, false
}

// Leave implements Visitor interface.
func (a *WindowFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_util_00000, 10)
	switch v := n.(type) {
	case *ast.WindowFuncExpr:
		trace_util_0.Count(_util_00000, 12)
		a.windowFuncs = append(a.windowFuncs, v)
	}
	trace_util_0.Count(_util_00000, 11)
	return n, true
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	schema *expression.Schema
	baseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	trace_util_0.Count(_util_00000, 13)
	if s.schema == nil {
		trace_util_0.Count(_util_00000, 15)
		s.schema = expression.NewSchema()
	}
	trace_util_0.Count(_util_00000, 14)
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	trace_util_0.Count(_util_00000, 16)
	s.schema = schema
}

// physicalSchemaProducer stores the schema for the physical plans who can produce schema directly.
type physicalSchemaProducer struct {
	schema *expression.Schema
	basePhysicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *physicalSchemaProducer) Schema() *expression.Schema {
	trace_util_0.Count(_util_00000, 17)
	if s.schema == nil {
		trace_util_0.Count(_util_00000, 19)
		s.schema = expression.NewSchema()
	}
	trace_util_0.Count(_util_00000, 18)
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *physicalSchemaProducer) SetSchema(schema *expression.Schema) {
	trace_util_0.Count(_util_00000, 20)
	s.schema = schema
}

// baseSchemaProducer stores the schema for the base plans who can produce schema directly.
type baseSchemaProducer struct {
	schema *expression.Schema
	basePlan
}

// Schema implements the Plan.Schema interface.
func (s *baseSchemaProducer) Schema() *expression.Schema {
	trace_util_0.Count(_util_00000, 21)
	if s.schema == nil {
		trace_util_0.Count(_util_00000, 23)
		s.schema = expression.NewSchema()
	}
	trace_util_0.Count(_util_00000, 22)
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *baseSchemaProducer) SetSchema(schema *expression.Schema) {
	trace_util_0.Count(_util_00000, 24)
	s.schema = schema
}

func buildLogicalJoinSchema(joinType JoinType, join LogicalPlan) *expression.Schema {
	trace_util_0.Count(_util_00000, 25)
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		trace_util_0.Count(_util_00000, 27)
		return join.Children()[0].Schema().Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		trace_util_0.Count(_util_00000, 28)
		newSchema := join.Children()[0].Schema().Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	trace_util_0.Count(_util_00000, 26)
	return expression.MergeSchema(join.Children()[0].Schema(), join.Children()[1].Schema())
}

func buildPhysicalJoinSchema(joinType JoinType, join PhysicalPlan) *expression.Schema {
	trace_util_0.Count(_util_00000, 29)
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		trace_util_0.Count(_util_00000, 31)
		return join.Children()[0].Schema().Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		trace_util_0.Count(_util_00000, 32)
		newSchema := join.Children()[0].Schema().Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	trace_util_0.Count(_util_00000, 30)
	return expression.MergeSchema(join.Children()[0].Schema(), join.Children()[1].Schema())
}

// GetStatsInfo gets the statistics info from a physical plan tree.
func GetStatsInfo(i interface{}) map[string]uint64 {
	trace_util_0.Count(_util_00000, 33)
	p := i.(Plan)
	var physicalPlan PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		trace_util_0.Count(_util_00000, 36)
		physicalPlan = x.SelectPlan
	case *Update:
		trace_util_0.Count(_util_00000, 37)
		physicalPlan = x.SelectPlan
	case *Delete:
		trace_util_0.Count(_util_00000, 38)
		physicalPlan = x.SelectPlan
	case PhysicalPlan:
		trace_util_0.Count(_util_00000, 39)
		physicalPlan = x
	}

	trace_util_0.Count(_util_00000, 34)
	if physicalPlan == nil {
		trace_util_0.Count(_util_00000, 40)
		return nil
	}

	trace_util_0.Count(_util_00000, 35)
	statsInfos := make(map[string]uint64)
	statsInfos = CollectPlanStatsVersion(physicalPlan, statsInfos)
	return statsInfos
}

var _util_00000 = "planner/core/util.go"
