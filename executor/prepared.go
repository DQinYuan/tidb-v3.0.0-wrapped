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
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	_ Executor = &DeallocateExec{}
	_ Executor = &ExecuteExec{}
	_ Executor = &PrepareExec{}
)

type paramMarkerSorter struct {
	markers []ast.ParamMarkerExpr
}

func (p *paramMarkerSorter) Len() int {
	trace_util_0.Count(_prepared_00000, 0)
	return len(p.markers)
}

func (p *paramMarkerSorter) Less(i, j int) bool {
	trace_util_0.Count(_prepared_00000, 1)
	return p.markers[i].(*driver.ParamMarkerExpr).Offset < p.markers[j].(*driver.ParamMarkerExpr).Offset
}

func (p *paramMarkerSorter) Swap(i, j int) {
	trace_util_0.Count(_prepared_00000, 2)
	p.markers[i], p.markers[j] = p.markers[j], p.markers[i]
}

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (e *paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_prepared_00000, 3)
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	trace_util_0.Count(_prepared_00000, 4)
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
		trace_util_0.Count(_prepared_00000, 6)
		e.markers = append(e.markers, x)
	}
	trace_util_0.Count(_prepared_00000, 5)
	return in, true
}

// PrepareExec represents a PREPARE executor.
type PrepareExec struct {
	baseExecutor

	is      infoschema.InfoSchema
	name    string
	sqlText string

	ID         uint32
	ParamCount int
	Fields     []*ast.ResultField
}

var prepareStmtLabel = stringutil.StringerStr("PrepareStmt")

// NewPrepareExec creates a new PrepareExec.
func NewPrepareExec(ctx sessionctx.Context, is infoschema.InfoSchema, sqlTxt string) *PrepareExec {
	trace_util_0.Count(_prepared_00000, 7)
	base := newBaseExecutor(ctx, nil, prepareStmtLabel)
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           is,
		sqlText:      sqlTxt,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_prepared_00000, 8)
	vars := e.ctx.GetSessionVars()
	if e.ID != 0 {
		trace_util_0.Count(_prepared_00000, 23)
		// Must be the case when we retry a prepare.
		// Make sure it is idempotent.
		_, ok := vars.PreparedStmts[e.ID]
		if ok {
			trace_util_0.Count(_prepared_00000, 24)
			return nil
		}
	}
	trace_util_0.Count(_prepared_00000, 9)
	charset, collation := vars.GetCharsetInfo()
	var (
		stmts []ast.StmtNode
		err   error
	)
	if sqlParser, ok := e.ctx.(sqlexec.SQLParser); ok {
		trace_util_0.Count(_prepared_00000, 25)
		stmts, err = sqlParser.ParseSQL(e.sqlText, charset, collation)
	} else {
		trace_util_0.Count(_prepared_00000, 26)
		{
			p := parser.New()
			p.EnableWindowFunc(vars.EnableWindowFunction)
			var warns []error
			stmts, warns, err = p.Parse(e.sqlText, charset, collation)
			for _, warn := range warns {
				trace_util_0.Count(_prepared_00000, 27)
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
			}
		}
	}
	trace_util_0.Count(_prepared_00000, 10)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 28)
		return util.SyntaxError(err)
	}
	trace_util_0.Count(_prepared_00000, 11)
	if len(stmts) != 1 {
		trace_util_0.Count(_prepared_00000, 29)
		return ErrPrepareMulti
	}
	trace_util_0.Count(_prepared_00000, 12)
	stmt := stmts[0]
	err = ResetContextOfStmt(e.ctx, stmt)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 30)
		return err
	}
	trace_util_0.Count(_prepared_00000, 13)
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := stmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		trace_util_0.Count(_prepared_00000, 31)
		return ErrPrepareDDL
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	trace_util_0.Count(_prepared_00000, 14)
	if len(extractor.markers) > math.MaxUint16 {
		trace_util_0.Count(_prepared_00000, 32)
		return ErrPsManyParam
	}

	trace_util_0.Count(_prepared_00000, 15)
	err = plannercore.Preprocess(e.ctx, stmt, e.is, plannercore.InPrepare)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 33)
		return err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	trace_util_0.Count(_prepared_00000, 16)
	sorter := &paramMarkerSorter{markers: extractor.markers}
	sort.Sort(sorter)
	e.ParamCount = len(sorter.markers)
	for i := 0; i < e.ParamCount; i++ {
		trace_util_0.Count(_prepared_00000, 34)
		sorter.markers[i].SetOrder(i)
	}
	trace_util_0.Count(_prepared_00000, 17)
	prepared := &ast.Prepared{
		Stmt:          stmt,
		Params:        sorter.markers,
		SchemaVersion: e.is.SchemaMetaVersion(),
	}
	prepared.UseCache = plannercore.PreparedPlanCacheEnabled() && (vars.LightningMode || plannercore.Cacheable(stmt))

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		trace_util_0.Count(_prepared_00000, 35)
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Datum.SetNull()
		param.InExecute = false
	}
	trace_util_0.Count(_prepared_00000, 18)
	var p plannercore.Plan
	p, err = plannercore.BuildLogicalPlan(e.ctx, stmt, e.is)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 36)
		return err
	}
	trace_util_0.Count(_prepared_00000, 19)
	if _, ok := stmt.(*ast.SelectStmt); ok {
		trace_util_0.Count(_prepared_00000, 37)
		e.Fields = schema2ResultFields(p.Schema(), vars.CurrentDB)
	}
	trace_util_0.Count(_prepared_00000, 20)
	if e.ID == 0 {
		trace_util_0.Count(_prepared_00000, 38)
		e.ID = vars.GetNextPreparedStmtID()
	}
	trace_util_0.Count(_prepared_00000, 21)
	if e.name != "" {
		trace_util_0.Count(_prepared_00000, 39)
		vars.PreparedStmtNameToID[e.name] = e.ID
	}
	trace_util_0.Count(_prepared_00000, 22)
	return vars.AddPreparedStmt(e.ID, prepared)
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	baseExecutor

	is            infoschema.InfoSchema
	name          string
	usingVars     []expression.Expression
	id            uint32
	stmtExec      Executor
	stmt          ast.StmtNode
	plan          plannercore.Plan
	lowerPriority bool
}

// Next implements the Executor Next interface.
func (e *ExecuteExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_prepared_00000, 40)
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build(b *executorBuilder) error {
	trace_util_0.Count(_prepared_00000, 41)
	ok, err := IsPointGetWithPKOrUniqueKeyByAutoCommit(e.ctx, e.plan)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 46)
		return err
	}
	trace_util_0.Count(_prepared_00000, 42)
	if ok {
		trace_util_0.Count(_prepared_00000, 47)
		err = e.ctx.InitTxnWithStartTS(math.MaxUint64)
	}
	trace_util_0.Count(_prepared_00000, 43)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 48)
		return err
	}
	trace_util_0.Count(_prepared_00000, 44)
	stmtExec := b.build(e.plan)
	if b.err != nil {
		trace_util_0.Count(_prepared_00000, 49)
		log.Warn("rebuild plan in EXECUTE statement failed", zap.String("labelName of PREPARE statement", e.name))
		return errors.Trace(b.err)
	}
	trace_util_0.Count(_prepared_00000, 45)
	e.stmtExec = stmtExec
	CountStmtNode(e.stmt, e.ctx.GetSessionVars().InRestrictedSQL)
	e.lowerPriority = needLowerPriority(e.plan)
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	baseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_prepared_00000, 50)
	vars := e.ctx.GetSessionVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		trace_util_0.Count(_prepared_00000, 53)
		return errors.Trace(plannercore.ErrStmtNotFound)
	}
	trace_util_0.Count(_prepared_00000, 51)
	delete(vars.PreparedStmtNameToID, e.Name)
	if plannercore.PreparedPlanCacheEnabled() {
		trace_util_0.Count(_prepared_00000, 54)
		e.ctx.PreparedPlanCache().Delete(plannercore.NewPSTMTPlanCacheKey(
			vars, id, vars.PreparedStmts[id].SchemaVersion,
		))
	}
	trace_util_0.Count(_prepared_00000, 52)
	vars.RemovePreparedStmt(id)
	return nil
}

// CompileExecutePreparedStmt compiles a session Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx sessionctx.Context, ID uint32, args ...interface{}) (sqlexec.Statement, error) {
	trace_util_0.Count(_prepared_00000, 55)
	execStmt := &ast.ExecuteStmt{ExecID: ID}
	if err := ResetContextOfStmt(ctx, execStmt); err != nil {
		trace_util_0.Count(_prepared_00000, 60)
		return nil, err
	}
	trace_util_0.Count(_prepared_00000, 56)
	execStmt.UsingVars = make([]ast.ExprNode, len(args))
	for i, val := range args {
		trace_util_0.Count(_prepared_00000, 61)
		execStmt.UsingVars[i] = ast.NewValueExpr(val)
	}
	trace_util_0.Count(_prepared_00000, 57)
	is := GetInfoSchema(ctx)
	execPlan, err := planner.Optimize(ctx, execStmt, is)
	if err != nil {
		trace_util_0.Count(_prepared_00000, 62)
		return nil, err
	}

	trace_util_0.Count(_prepared_00000, 58)
	stmt := &ExecStmt{
		InfoSchema: is,
		Plan:       execPlan,
		StmtNode:   execStmt,
		Ctx:        ctx,
	}
	if prepared, ok := ctx.GetSessionVars().PreparedStmts[ID]; ok {
		trace_util_0.Count(_prepared_00000, 63)
		stmt.Text = prepared.Stmt.Text()
		ctx.GetSessionVars().StmtCtx.OriginalSQL = stmt.Text
	}
	trace_util_0.Count(_prepared_00000, 59)
	return stmt, nil
}

func getPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (ast.StmtNode, error) {
	trace_util_0.Count(_prepared_00000, 64)
	var ok bool
	execID := stmt.ExecID
	if stmt.Name != "" {
		trace_util_0.Count(_prepared_00000, 67)
		if execID, ok = vars.PreparedStmtNameToID[stmt.Name]; !ok {
			trace_util_0.Count(_prepared_00000, 68)
			return nil, plannercore.ErrStmtNotFound
		}
	}
	trace_util_0.Count(_prepared_00000, 65)
	if prepared, ok := vars.PreparedStmts[execID]; ok {
		trace_util_0.Count(_prepared_00000, 69)
		return prepared.Stmt, nil
	}
	trace_util_0.Count(_prepared_00000, 66)
	return nil, plannercore.ErrStmtNotFound
}

var _prepared_00000 = "executor/prepared.go"
