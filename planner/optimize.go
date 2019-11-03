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

package planner

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, error) {
	trace_util_0.Count(_optimize_00000, 0)
	fp := plannercore.TryFastPlan(ctx, node)
	if fp != nil {
		trace_util_0.Count(_optimize_00000, 7)
		return fp, nil
	}

	// build logical plan
	trace_util_0.Count(_optimize_00000, 1)
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := plannercore.NewPlanBuilder(ctx, is)
	p, err := builder.Build(node)
	if err != nil {
		trace_util_0.Count(_optimize_00000, 8)
		return nil, err
	}

	trace_util_0.Count(_optimize_00000, 2)
	ctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := ctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		trace_util_0.Count(_optimize_00000, 9)
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			trace_util_0.Count(_optimize_00000, 10)
			return nil, err
		}
	}

	// Handle the execute statement.
	trace_util_0.Count(_optimize_00000, 3)
	if execPlan, ok := p.(*plannercore.Execute); ok {
		trace_util_0.Count(_optimize_00000, 11)
		err := execPlan.OptimizePreparedPlan(ctx, is)
		return p, err
	}

	// Handle the non-logical plan statement.
	trace_util_0.Count(_optimize_00000, 4)
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		trace_util_0.Count(_optimize_00000, 12)
		return p, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	trace_util_0.Count(_optimize_00000, 5)
	if ctx.GetSessionVars().EnableCascadesPlanner {
		trace_util_0.Count(_optimize_00000, 13)
		return cascades.FindBestPlan(ctx, logic)
	}
	trace_util_0.Count(_optimize_00000, 6)
	return plannercore.DoOptimize(builder.GetOptFlag(), logic)
}

func init() {
	trace_util_0.Count(_optimize_00000, 14)
	plannercore.OptimizeAstNode = Optimize
}

var _optimize_00000 = "planner/optimize.go"
