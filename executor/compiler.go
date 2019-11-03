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

package executor

import (
	"context"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	stmtNodeCounterUse      = metrics.StmtNodeCounter.WithLabelValues("Use")
	stmtNodeCounterShow     = metrics.StmtNodeCounter.WithLabelValues("Show")
	stmtNodeCounterBegin    = metrics.StmtNodeCounter.WithLabelValues("Begin")
	stmtNodeCounterCommit   = metrics.StmtNodeCounter.WithLabelValues("Commit")
	stmtNodeCounterRollback = metrics.StmtNodeCounter.WithLabelValues("Rollback")
	stmtNodeCounterInsert   = metrics.StmtNodeCounter.WithLabelValues("Insert")
	stmtNodeCounterReplace  = metrics.StmtNodeCounter.WithLabelValues("Replace")
	stmtNodeCounterDelete   = metrics.StmtNodeCounter.WithLabelValues("Delete")
	stmtNodeCounterUpdate   = metrics.StmtNodeCounter.WithLabelValues("Update")
	stmtNodeCounterSelect   = metrics.StmtNodeCounter.WithLabelValues("Select")
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	trace_util_0.Count(_compiler_00000, 0)
	return c.compile(ctx, stmtNode, false)
}

// SkipBindCompile compiles an ast.StmtNode to a physical plan without SQL bind.
func (c *Compiler) SkipBindCompile(ctx context.Context, node ast.StmtNode) (*ExecStmt, error) {
	trace_util_0.Count(_compiler_00000, 1)
	return c.compile(ctx, node, true)
}

func (c *Compiler) compile(ctx context.Context, stmtNode ast.StmtNode, skipBind bool) (*ExecStmt, error) {
	trace_util_0.Count(_compiler_00000, 2)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_compiler_00000, 7)
		span1 := span.Tracer().StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_compiler_00000, 3)
	if !skipBind {
		trace_util_0.Count(_compiler_00000, 8)
		stmtNode = addHint(c.Ctx, stmtNode)
	}

	trace_util_0.Count(_compiler_00000, 4)
	infoSchema := GetInfoSchema(c.Ctx)
	if err := plannercore.Preprocess(c.Ctx, stmtNode, infoSchema); err != nil {
		trace_util_0.Count(_compiler_00000, 9)
		return nil, err
	}

	trace_util_0.Count(_compiler_00000, 5)
	finalPlan, err := planner.Optimize(c.Ctx, stmtNode, infoSchema)
	if err != nil {
		trace_util_0.Count(_compiler_00000, 10)
		return nil, err
	}

	trace_util_0.Count(_compiler_00000, 6)
	CountStmtNode(stmtNode, c.Ctx.GetSessionVars().InRestrictedSQL)
	lowerPriority := needLowerPriority(finalPlan)
	return &ExecStmt{
		InfoSchema:    infoSchema,
		Plan:          finalPlan,
		LowerPriority: lowerPriority,
		Cacheable:     plannercore.Cacheable(stmtNode),
		Text:          stmtNode.Text(),
		StmtNode:      stmtNode,
		Ctx:           c.Ctx,
	}, nil
}

// needLowerPriority checks whether it's needed to lower the execution priority
// of a query.
// If the estimated output row count of any operator in the physical plan tree
// is greater than the specific threshold, we'll set it to lowPriority when
// sending it to the coprocessor.
func needLowerPriority(p plannercore.Plan) bool {
	trace_util_0.Count(_compiler_00000, 11)
	switch x := p.(type) {
	case plannercore.PhysicalPlan:
		trace_util_0.Count(_compiler_00000, 13)
		return isPhysicalPlanNeedLowerPriority(x)
	case *plannercore.Execute:
		trace_util_0.Count(_compiler_00000, 14)
		return needLowerPriority(x.Plan)
	case *plannercore.Insert:
		trace_util_0.Count(_compiler_00000, 15)
		if x.SelectPlan != nil {
			trace_util_0.Count(_compiler_00000, 18)
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.Delete:
		trace_util_0.Count(_compiler_00000, 16)
		if x.SelectPlan != nil {
			trace_util_0.Count(_compiler_00000, 19)
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.Update:
		trace_util_0.Count(_compiler_00000, 17)
		if x.SelectPlan != nil {
			trace_util_0.Count(_compiler_00000, 20)
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	}
	trace_util_0.Count(_compiler_00000, 12)
	return false
}

func isPhysicalPlanNeedLowerPriority(p plannercore.PhysicalPlan) bool {
	trace_util_0.Count(_compiler_00000, 21)
	expensiveThreshold := int64(config.GetGlobalConfig().Log.ExpensiveThreshold)
	if int64(p.StatsCount()) > expensiveThreshold {
		trace_util_0.Count(_compiler_00000, 24)
		return true
	}

	trace_util_0.Count(_compiler_00000, 22)
	for _, child := range p.Children() {
		trace_util_0.Count(_compiler_00000, 25)
		if isPhysicalPlanNeedLowerPriority(child) {
			trace_util_0.Count(_compiler_00000, 26)
			return true
		}
	}

	trace_util_0.Count(_compiler_00000, 23)
	return false
}

// CountStmtNode records the number of statements with the same type.
func CountStmtNode(stmtNode ast.StmtNode, inRestrictedSQL bool) {
	trace_util_0.Count(_compiler_00000, 27)
	if inRestrictedSQL {
		trace_util_0.Count(_compiler_00000, 31)
		return
	}

	trace_util_0.Count(_compiler_00000, 28)
	typeLabel := GetStmtLabel(stmtNode)
	switch typeLabel {
	case "Use":
		trace_util_0.Count(_compiler_00000, 32)
		stmtNodeCounterUse.Inc()
	case "Show":
		trace_util_0.Count(_compiler_00000, 33)
		stmtNodeCounterShow.Inc()
	case "Begin":
		trace_util_0.Count(_compiler_00000, 34)
		stmtNodeCounterBegin.Inc()
	case "Commit":
		trace_util_0.Count(_compiler_00000, 35)
		stmtNodeCounterCommit.Inc()
	case "Rollback":
		trace_util_0.Count(_compiler_00000, 36)
		stmtNodeCounterRollback.Inc()
	case "Insert":
		trace_util_0.Count(_compiler_00000, 37)
		stmtNodeCounterInsert.Inc()
	case "Replace":
		trace_util_0.Count(_compiler_00000, 38)
		stmtNodeCounterReplace.Inc()
	case "Delete":
		trace_util_0.Count(_compiler_00000, 39)
		stmtNodeCounterDelete.Inc()
	case "Update":
		trace_util_0.Count(_compiler_00000, 40)
		stmtNodeCounterUpdate.Inc()
	case "Select":
		trace_util_0.Count(_compiler_00000, 41)
		stmtNodeCounterSelect.Inc()
	default:
		trace_util_0.Count(_compiler_00000, 42)
		metrics.StmtNodeCounter.WithLabelValues(typeLabel).Inc()
	}

	trace_util_0.Count(_compiler_00000, 29)
	if !config.GetGlobalConfig().Status.RecordQPSbyDB {
		trace_util_0.Count(_compiler_00000, 43)
		return
	}

	trace_util_0.Count(_compiler_00000, 30)
	dbLabels := getStmtDbLabel(stmtNode)
	for dbLabel := range dbLabels {
		trace_util_0.Count(_compiler_00000, 44)
		metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel).Inc()
	}
}

func getStmtDbLabel(stmtNode ast.StmtNode) map[string]struct{} {
	trace_util_0.Count(_compiler_00000, 45)
	dbLabelSet := make(map[string]struct{})

	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		trace_util_0.Count(_compiler_00000, 47)
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_compiler_00000, 48)
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateTableStmt:
		trace_util_0.Count(_compiler_00000, 49)
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.InsertStmt:
		trace_util_0.Count(_compiler_00000, 50)
		dbLabels := getDbFromResultNode(x.Table.TableRefs)
		for _, db := range dbLabels {
			trace_util_0.Count(_compiler_00000, 59)
			dbLabelSet[db] = struct{}{}
		}
		trace_util_0.Count(_compiler_00000, 51)
		dbLabels = getDbFromResultNode(x.Select)
		for _, db := range dbLabels {
			trace_util_0.Count(_compiler_00000, 60)
			dbLabelSet[db] = struct{}{}
		}
	case *ast.DropIndexStmt:
		trace_util_0.Count(_compiler_00000, 52)
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.DropTableStmt:
		trace_util_0.Count(_compiler_00000, 53)
		tables := x.Tables
		for _, table := range tables {
			trace_util_0.Count(_compiler_00000, 61)
			dbLabel := table.Schema.O
			if _, ok := dbLabelSet[dbLabel]; !ok {
				trace_util_0.Count(_compiler_00000, 62)
				dbLabelSet[dbLabel] = struct{}{}
			}
		}
	case *ast.SelectStmt:
		trace_util_0.Count(_compiler_00000, 54)
		dbLabels := getDbFromResultNode(x)
		for _, db := range dbLabels {
			trace_util_0.Count(_compiler_00000, 63)
			dbLabelSet[db] = struct{}{}
		}
	case *ast.UpdateStmt:
		trace_util_0.Count(_compiler_00000, 55)
		if x.TableRefs != nil {
			trace_util_0.Count(_compiler_00000, 64)
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				trace_util_0.Count(_compiler_00000, 65)
				dbLabelSet[db] = struct{}{}
			}
		}
	case *ast.DeleteStmt:
		trace_util_0.Count(_compiler_00000, 56)
		if x.TableRefs != nil {
			trace_util_0.Count(_compiler_00000, 66)
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				trace_util_0.Count(_compiler_00000, 67)
				dbLabelSet[db] = struct{}{}
			}
		}
	case *ast.CreateBindingStmt:
		trace_util_0.Count(_compiler_00000, 57)
		if x.OriginSel != nil {
			trace_util_0.Count(_compiler_00000, 68)
			originSelect := x.OriginSel.(*ast.SelectStmt)
			dbLabels := getDbFromResultNode(originSelect.From.TableRefs)
			for _, db := range dbLabels {
				trace_util_0.Count(_compiler_00000, 69)
				dbLabelSet[db] = struct{}{}
			}
		}

		trace_util_0.Count(_compiler_00000, 58)
		if len(dbLabelSet) == 0 && x.HintedSel != nil {
			trace_util_0.Count(_compiler_00000, 70)
			hintedSelect := x.HintedSel.(*ast.SelectStmt)
			dbLabels := getDbFromResultNode(hintedSelect.From.TableRefs)
			for _, db := range dbLabels {
				trace_util_0.Count(_compiler_00000, 71)
				dbLabelSet[db] = struct{}{}
			}
		}
	}

	trace_util_0.Count(_compiler_00000, 46)
	return dbLabelSet
}

func getDbFromResultNode(resultNode ast.ResultSetNode) []string {
	trace_util_0.Count(_compiler_00000, 72) //may have duplicate db name
	var dbLabels []string

	if resultNode == nil {
		trace_util_0.Count(_compiler_00000, 75)
		return dbLabels
	}

	trace_util_0.Count(_compiler_00000, 73)
	switch x := resultNode.(type) {
	case *ast.TableSource:
		trace_util_0.Count(_compiler_00000, 76)
		return getDbFromResultNode(x.Source)
	case *ast.SelectStmt:
		trace_util_0.Count(_compiler_00000, 77)
		if x.From != nil {
			trace_util_0.Count(_compiler_00000, 81)
			return getDbFromResultNode(x.From.TableRefs)
		}
	case *ast.TableName:
		trace_util_0.Count(_compiler_00000, 78)
		dbLabels = append(dbLabels, x.DBInfo.Name.O)
	case *ast.Join:
		trace_util_0.Count(_compiler_00000, 79)
		if x.Left != nil {
			trace_util_0.Count(_compiler_00000, 82)
			dbs := getDbFromResultNode(x.Left)
			if dbs != nil {
				trace_util_0.Count(_compiler_00000, 83)
				for _, db := range dbs {
					trace_util_0.Count(_compiler_00000, 84)
					dbLabels = append(dbLabels, db)
				}
			}
		}

		trace_util_0.Count(_compiler_00000, 80)
		if x.Right != nil {
			trace_util_0.Count(_compiler_00000, 85)
			dbs := getDbFromResultNode(x.Right)
			if dbs != nil {
				trace_util_0.Count(_compiler_00000, 86)
				for _, db := range dbs {
					trace_util_0.Count(_compiler_00000, 87)
					dbLabels = append(dbLabels, db)
				}
			}
		}
	}

	trace_util_0.Count(_compiler_00000, 74)
	return dbLabels
}

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	trace_util_0.Count(_compiler_00000, 88)
	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		trace_util_0.Count(_compiler_00000, 90)
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		trace_util_0.Count(_compiler_00000, 91)
		return "AnalyzeTable"
	case *ast.BeginStmt:
		trace_util_0.Count(_compiler_00000, 92)
		return "Begin"
	case *ast.ChangeStmt:
		trace_util_0.Count(_compiler_00000, 93)
		return "Change"
	case *ast.CommitStmt:
		trace_util_0.Count(_compiler_00000, 94)
		return "Commit"
	case *ast.CreateDatabaseStmt:
		trace_util_0.Count(_compiler_00000, 95)
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_compiler_00000, 96)
		return "CreateIndex"
	case *ast.CreateTableStmt:
		trace_util_0.Count(_compiler_00000, 97)
		return "CreateTable"
	case *ast.CreateViewStmt:
		trace_util_0.Count(_compiler_00000, 98)
		return "CreateView"
	case *ast.CreateUserStmt:
		trace_util_0.Count(_compiler_00000, 99)
		return "CreateUser"
	case *ast.DeleteStmt:
		trace_util_0.Count(_compiler_00000, 100)
		return "Delete"
	case *ast.DropDatabaseStmt:
		trace_util_0.Count(_compiler_00000, 101)
		return "DropDatabase"
	case *ast.DropIndexStmt:
		trace_util_0.Count(_compiler_00000, 102)
		return "DropIndex"
	case *ast.DropTableStmt:
		trace_util_0.Count(_compiler_00000, 103)
		return "DropTable"
	case *ast.ExplainStmt:
		trace_util_0.Count(_compiler_00000, 104)
		return "Explain"
	case *ast.InsertStmt:
		trace_util_0.Count(_compiler_00000, 105)
		if x.IsReplace {
			trace_util_0.Count(_compiler_00000, 121)
			return "Replace"
		}
		trace_util_0.Count(_compiler_00000, 106)
		return "Insert"
	case *ast.LoadDataStmt:
		trace_util_0.Count(_compiler_00000, 107)
		return "LoadData"
	case *ast.RollbackStmt:
		trace_util_0.Count(_compiler_00000, 108)
		return "RollBack"
	case *ast.SelectStmt:
		trace_util_0.Count(_compiler_00000, 109)
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		trace_util_0.Count(_compiler_00000, 110)
		return "Set"
	case *ast.ShowStmt:
		trace_util_0.Count(_compiler_00000, 111)
		return "Show"
	case *ast.TruncateTableStmt:
		trace_util_0.Count(_compiler_00000, 112)
		return "TruncateTable"
	case *ast.UpdateStmt:
		trace_util_0.Count(_compiler_00000, 113)
		return "Update"
	case *ast.GrantStmt:
		trace_util_0.Count(_compiler_00000, 114)
		return "Grant"
	case *ast.RevokeStmt:
		trace_util_0.Count(_compiler_00000, 115)
		return "Revoke"
	case *ast.DeallocateStmt:
		trace_util_0.Count(_compiler_00000, 116)
		return "Deallocate"
	case *ast.ExecuteStmt:
		trace_util_0.Count(_compiler_00000, 117)
		return "Execute"
	case *ast.PrepareStmt:
		trace_util_0.Count(_compiler_00000, 118)
		return "Prepare"
	case *ast.UseStmt:
		trace_util_0.Count(_compiler_00000, 119)
		return "Use"
	case *ast.CreateBindingStmt:
		trace_util_0.Count(_compiler_00000, 120)
		return "CreateBinding"
	}
	trace_util_0.Count(_compiler_00000, 89)
	return "other"
}

// GetInfoSchema gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchema(ctx sessionctx.Context) infoschema.InfoSchema {
	trace_util_0.Count(_compiler_00000, 122)
	sessVar := ctx.GetSessionVars()
	var is infoschema.InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		trace_util_0.Count(_compiler_00000, 124)
		is = snap.(infoschema.InfoSchema)
		logutil.Logger(context.Background()).Info("use snapshot schema", zap.Uint64("conn", sessVar.ConnectionID), zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	} else {
		trace_util_0.Count(_compiler_00000, 125)
		{
			is = sessVar.TxnCtx.InfoSchema.(infoschema.InfoSchema)
		}
	}
	trace_util_0.Count(_compiler_00000, 123)
	return is
}

func addHint(ctx sessionctx.Context, stmtNode ast.StmtNode) ast.StmtNode {
	trace_util_0.Count(_compiler_00000, 126)
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		trace_util_0.Count(_compiler_00000, 128) //when the domain is initializing, the bind will be nil.
		return stmtNode
	}
	trace_util_0.Count(_compiler_00000, 127)
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		trace_util_0.Count(_compiler_00000, 129)
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			trace_util_0.Count(_compiler_00000, 133)
			normalizeExplainSQL := parser.Normalize(x.Text())
			idx := strings.Index(normalizeExplainSQL, "select")
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestHash(normalizeSQL)
			x.Stmt = addHintForSelect(hash, normalizeSQL, ctx, x.Stmt)
		}
		trace_util_0.Count(_compiler_00000, 130)
		return x
	case *ast.SelectStmt:
		trace_util_0.Count(_compiler_00000, 131)
		normalizeSQL, hash := parser.NormalizeDigest(x.Text())
		return addHintForSelect(hash, normalizeSQL, ctx, x)
	default:
		trace_util_0.Count(_compiler_00000, 132)
		return stmtNode
	}
}

func addHintForSelect(hash, normdOrigSQL string, ctx sessionctx.Context, stmt ast.StmtNode) ast.StmtNode {
	trace_util_0.Count(_compiler_00000, 134)
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord != nil {
		trace_util_0.Count(_compiler_00000, 138)
		if bindRecord.Status == bindinfo.Invalid {
			trace_util_0.Count(_compiler_00000, 140)
			return stmt
		}
		trace_util_0.Count(_compiler_00000, 139)
		if bindRecord.Status == bindinfo.Using {
			trace_util_0.Count(_compiler_00000, 141)
			return bindinfo.BindHint(stmt, bindRecord.Ast)
		}
	}
	trace_util_0.Count(_compiler_00000, 135)
	globalHandle := domain.GetDomain(ctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		trace_util_0.Count(_compiler_00000, 142)
		bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	trace_util_0.Count(_compiler_00000, 136)
	if bindRecord != nil {
		trace_util_0.Count(_compiler_00000, 143)
		return bindinfo.BindHint(stmt, bindRecord.Ast)
	}
	trace_util_0.Count(_compiler_00000, 137)
	return stmt
}

var _compiler_00000 = "executor/compiler.go"
