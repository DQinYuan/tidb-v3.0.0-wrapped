// Copyright 2015 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/atomic"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const (
	flagPrunColumns uint64 = 1 << iota
	flagBuildKeyInfo
	flagDecorrelate
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagPushDownAgg
	flagPushDownTopN
	flagJoinReOrder
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&aggregationEliminator{},
	&projectionEliminater{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderSolver{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
}

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	trace_util_0.Count(_optimizer_00000, 0)
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := &PlanBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p, err := builder.Build(node)
	if err != nil {
		trace_util_0.Count(_optimizer_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_optimizer_00000, 1)
	return p, nil
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	trace_util_0.Count(_optimizer_00000, 3)
	for _, v := range vs {
		trace_util_0.Count(_optimizer_00000, 5)
		if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			trace_util_0.Count(_optimizer_00000, 6)
			if v.err == nil {
				trace_util_0.Count(_optimizer_00000, 8)
				return ErrPrivilegeCheckFail
			}
			trace_util_0.Count(_optimizer_00000, 7)
			return v.err
		}
	}
	trace_util_0.Count(_optimizer_00000, 4)
	return nil
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(flag uint64, logic LogicalPlan) (PhysicalPlan, error) {
	trace_util_0.Count(_optimizer_00000, 9)
	logic, err := logicalOptimize(flag, logic)
	if err != nil {
		trace_util_0.Count(_optimizer_00000, 13)
		return nil, err
	}
	trace_util_0.Count(_optimizer_00000, 10)
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		trace_util_0.Count(_optimizer_00000, 14)
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	trace_util_0.Count(_optimizer_00000, 11)
	physical, err := physicalOptimize(logic)
	if err != nil {
		trace_util_0.Count(_optimizer_00000, 15)
		return nil, err
	}
	trace_util_0.Count(_optimizer_00000, 12)
	finalPlan := postOptimize(physical)
	return finalPlan, nil
}

func postOptimize(plan PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_optimizer_00000, 16)
	plan = eliminatePhysicalProjection(plan)
	plan = injectExtraProjection(plan)
	return plan
}

func logicalOptimize(flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_optimizer_00000, 17)
	var err error
	for i, rule := range optRuleList {
		trace_util_0.Count(_optimizer_00000, 19)
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 {
			trace_util_0.Count(_optimizer_00000, 21)
			continue
		}
		trace_util_0.Count(_optimizer_00000, 20)
		logic, err = rule.optimize(logic)
		if err != nil {
			trace_util_0.Count(_optimizer_00000, 22)
			return nil, err
		}
	}
	trace_util_0.Count(_optimizer_00000, 18)
	return logic, err
}

func physicalOptimize(logic LogicalPlan) (PhysicalPlan, error) {
	trace_util_0.Count(_optimizer_00000, 23)
	if _, err := logic.recursiveDeriveStats(); err != nil {
		trace_util_0.Count(_optimizer_00000, 27)
		return nil, err
	}

	trace_util_0.Count(_optimizer_00000, 24)
	logic.preparePossibleProperties()

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	t, err := logic.findBestTask(prop)
	if err != nil {
		trace_util_0.Count(_optimizer_00000, 28)
		return nil, err
	}
	trace_util_0.Count(_optimizer_00000, 25)
	if t.invalid() {
		trace_util_0.Count(_optimizer_00000, 29)
		return nil, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	trace_util_0.Count(_optimizer_00000, 26)
	err = t.plan().ResolveIndices()
	return t.plan(), err
}

func existsCartesianProduct(p LogicalPlan) bool {
	trace_util_0.Count(_optimizer_00000, 30)
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		trace_util_0.Count(_optimizer_00000, 33)
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	trace_util_0.Count(_optimizer_00000, 31)
	for _, child := range p.Children() {
		trace_util_0.Count(_optimizer_00000, 34)
		if existsCartesianProduct(child) {
			trace_util_0.Count(_optimizer_00000, 35)
			return true
		}
	}
	trace_util_0.Count(_optimizer_00000, 32)
	return false
}

func init() {
	trace_util_0.Count(_optimizer_00000, 36)
	expression.EvalAstExpr = evalAstExpr
}

var _optimizer_00000 = "planner/core/optimizer.go"
