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

package memo

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
)

// Operand is the node of a pattern tree, it represents a logical expression operator.
// Different from logical plan operator which holds the full information about an expression
// operator, Operand only stores the type information.
// An Operand may correspond to a concrete logical plan operator, or it can has special meaning,
// e.g, a placeholder for any logical plan operator.
type Operand int

const (
	// OperandAny is a placeholder for any Operand.
	OperandAny Operand = iota
	// OperandJoin for LogicalJoin.
	OperandJoin
	// OperandAggregation for LogicalAggregation.
	OperandAggregation
	// OperandProjection for LogicalProjection.
	OperandProjection
	// OperandSelection for LogicalSelection.
	OperandSelection
	// OperandApply for LogicalApply.
	OperandApply
	// OperandMaxOneRow for LogicalMaxOneRow.
	OperandMaxOneRow
	// OperandTableDual for LogicalTableDual.
	OperandTableDual
	// OperandDataSource for DataSource.
	OperandDataSource
	// OperandUnionScan for LogicalUnionScan.
	OperandUnionScan
	// OperandUnionAll for LogicalUnionAll.
	OperandUnionAll
	// OperandSort for LogicalSort.
	OperandSort
	// OperandTopN for LogicalTopN.
	OperandTopN
	// OperandLock for LogicalLock.
	OperandLock
	// OperandLimit for LogicalLimit.
	OperandLimit
	// OperandUnsupported is upper bound of defined Operand yet.
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p plannercore.LogicalPlan) Operand {
	trace_util_0.Count(_pattern_00000, 0)
	switch p.(type) {
	case *plannercore.LogicalJoin:
		trace_util_0.Count(_pattern_00000, 1)
		return OperandJoin
	case *plannercore.LogicalAggregation:
		trace_util_0.Count(_pattern_00000, 2)
		return OperandAggregation
	case *plannercore.LogicalProjection:
		trace_util_0.Count(_pattern_00000, 3)
		return OperandProjection
	case *plannercore.LogicalSelection:
		trace_util_0.Count(_pattern_00000, 4)
		return OperandSelection
	case *plannercore.LogicalApply:
		trace_util_0.Count(_pattern_00000, 5)
		return OperandApply
	case *plannercore.LogicalMaxOneRow:
		trace_util_0.Count(_pattern_00000, 6)
		return OperandMaxOneRow
	case *plannercore.LogicalTableDual:
		trace_util_0.Count(_pattern_00000, 7)
		return OperandTableDual
	case *plannercore.DataSource:
		trace_util_0.Count(_pattern_00000, 8)
		return OperandDataSource
	case *plannercore.LogicalUnionScan:
		trace_util_0.Count(_pattern_00000, 9)
		return OperandUnionScan
	case *plannercore.LogicalUnionAll:
		trace_util_0.Count(_pattern_00000, 10)
		return OperandUnionAll
	case *plannercore.LogicalSort:
		trace_util_0.Count(_pattern_00000, 11)
		return OperandSort
	case *plannercore.LogicalTopN:
		trace_util_0.Count(_pattern_00000, 12)
		return OperandTopN
	case *plannercore.LogicalLock:
		trace_util_0.Count(_pattern_00000, 13)
		return OperandLock
	case *plannercore.LogicalLimit:
		trace_util_0.Count(_pattern_00000, 14)
		return OperandLimit
	default:
		trace_util_0.Count(_pattern_00000, 15)
		return OperandUnsupported
	}
}

// Match checks if current Operand matches specified one.
func (o Operand) Match(t Operand) bool {
	trace_util_0.Count(_pattern_00000, 16)
	if o == OperandAny || t == OperandAny {
		trace_util_0.Count(_pattern_00000, 19)
		return true
	}
	trace_util_0.Count(_pattern_00000, 17)
	if o == t {
		trace_util_0.Count(_pattern_00000, 20)
		return true
	}
	trace_util_0.Count(_pattern_00000, 18)
	return false
}

// Pattern defines the Match pattern for a rule.
// It describes a piece of logical expression.
// It's a tree-like structure and each node in the tree is an Operand.
type Pattern struct {
	Operand
	Children []*Pattern
}

// NewPattern creats a pattern node according to the Operand.
func NewPattern(operand Operand) *Pattern {
	trace_util_0.Count(_pattern_00000, 21)
	return &Pattern{Operand: operand}
}

// SetChildren sets the Children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	trace_util_0.Count(_pattern_00000, 22)
	p.Children = children
}

// BuildPattern builds a Pattern from Operand and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, children ...*Pattern) *Pattern {
	trace_util_0.Count(_pattern_00000, 23)
	p := &Pattern{Operand: operand}
	p.Children = children
	return p
}

var _pattern_00000 = "planner/memo/pattern.go"
