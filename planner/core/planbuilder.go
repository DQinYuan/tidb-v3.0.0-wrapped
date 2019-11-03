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
	"bytes"
	"fmt"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
)

type visitInfo struct {
	privilege mysql.PrivilegeType
	db        string
	table     string
	column    string
	err       error
}

type tableHintInfo struct {
	indexNestedLoopJoinTables []hintTableInfo
	sortMergeJoinTables       []hintTableInfo
	hashJoinTables            []hintTableInfo
}

type hintTableInfo struct {
	name    model.CIStr
	matched bool
}

func tableNames2HintTableInfo(tableNames []model.CIStr) []hintTableInfo {
	trace_util_0.Count(_planbuilder_00000, 0)
	if len(tableNames) == 0 {
		trace_util_0.Count(_planbuilder_00000, 3)
		return nil
	}
	trace_util_0.Count(_planbuilder_00000, 1)
	hintTables := make([]hintTableInfo, 0, len(tableNames))
	for _, tableName := range tableNames {
		trace_util_0.Count(_planbuilder_00000, 4)
		hintTables = append(hintTables, hintTableInfo{name: tableName})
	}
	trace_util_0.Count(_planbuilder_00000, 2)
	return hintTables
}

func (info *tableHintInfo) ifPreferMergeJoin(tableNames ...*model.CIStr) bool {
	trace_util_0.Count(_planbuilder_00000, 5)
	return info.matchTableName(tableNames, info.sortMergeJoinTables)
}

func (info *tableHintInfo) ifPreferHashJoin(tableNames ...*model.CIStr) bool {
	trace_util_0.Count(_planbuilder_00000, 6)
	return info.matchTableName(tableNames, info.hashJoinTables)
}

func (info *tableHintInfo) ifPreferINLJ(tableNames ...*model.CIStr) bool {
	trace_util_0.Count(_planbuilder_00000, 7)
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables)
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (info *tableHintInfo) matchTableName(tables []*model.CIStr, hintTables []hintTableInfo) bool {
	trace_util_0.Count(_planbuilder_00000, 8)
	hintMatched := false
	for _, tableName := range tables {
		trace_util_0.Count(_planbuilder_00000, 10)
		if tableName == nil {
			trace_util_0.Count(_planbuilder_00000, 12)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 11)
		for i, curEntry := range hintTables {
			trace_util_0.Count(_planbuilder_00000, 13)
			if curEntry.name.L == tableName.L {
				trace_util_0.Count(_planbuilder_00000, 14)
				hintTables[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 9)
	return hintMatched
}

func restore2JoinHint(hintType string, hintTables []hintTableInfo) string {
	trace_util_0.Count(_planbuilder_00000, 15)
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	for i, table := range hintTables {
		trace_util_0.Count(_planbuilder_00000, 17)
		buffer.WriteString(table.name.L)
		if i < len(hintTables)-1 {
			trace_util_0.Count(_planbuilder_00000, 18)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_planbuilder_00000, 16)
	buffer.WriteString(") */")
	return buffer.String()
}

func extractUnmatchedTables(hintTables []hintTableInfo) []string {
	trace_util_0.Count(_planbuilder_00000, 19)
	var tableNames []string
	for _, table := range hintTables {
		trace_util_0.Count(_planbuilder_00000, 21)
		if !table.matched {
			trace_util_0.Count(_planbuilder_00000, 22)
			tableNames = append(tableNames, table.name.O)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 20)
	return tableNames
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	windowClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
	windowClause:        "field list", // For window functions that in field list.
}

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	inUpdateStmt bool
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo     []visitInfo
	tableHintInfo []tableHintInfo
	optFlag       uint64

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	windowSpecs map[string]*ast.WindowSpec
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	trace_util_0.Count(_planbuilder_00000, 23)
	return b.visitInfo
}

// GetDBTableInfo gets the accessed dbs and tables info.
func (b *PlanBuilder) GetDBTableInfo() []stmtctx.TableEntry {
	trace_util_0.Count(_planbuilder_00000, 24)
	var tables []stmtctx.TableEntry
	existsFunc := func(tbls []stmtctx.TableEntry, tbl *stmtctx.TableEntry) bool {
		trace_util_0.Count(_planbuilder_00000, 27)
		for _, t := range tbls {
			trace_util_0.Count(_planbuilder_00000, 29)
			if t == *tbl {
				trace_util_0.Count(_planbuilder_00000, 30)
				return true
			}
		}
		trace_util_0.Count(_planbuilder_00000, 28)
		return false
	}
	trace_util_0.Count(_planbuilder_00000, 25)
	for _, v := range b.visitInfo {
		trace_util_0.Count(_planbuilder_00000, 31)
		tbl := &stmtctx.TableEntry{DB: v.db, Table: v.table}
		if !existsFunc(tables, tbl) {
			trace_util_0.Count(_planbuilder_00000, 32)
			tables = append(tables, *tbl)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 26)
	return tables
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	trace_util_0.Count(_planbuilder_00000, 33)
	return b.optFlag
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	trace_util_0.Count(_planbuilder_00000, 34)
	return &PlanBuilder{
		ctx:       sctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(node ast.Node) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 35)
	b.optFlag = flagPrunColumns
	switch x := node.(type) {
	case *ast.AdminStmt:
		trace_util_0.Count(_planbuilder_00000, 37)
		return b.buildAdmin(x)
	case *ast.DeallocateStmt:
		trace_util_0.Count(_planbuilder_00000, 38)
		return &Deallocate{Name: x.Name}, nil
	case *ast.DeleteStmt:
		trace_util_0.Count(_planbuilder_00000, 39)
		return b.buildDelete(x)
	case *ast.ExecuteStmt:
		trace_util_0.Count(_planbuilder_00000, 40)
		return b.buildExecute(x)
	case *ast.ExplainStmt:
		trace_util_0.Count(_planbuilder_00000, 41)
		return b.buildExplain(x)
	case *ast.ExplainForStmt:
		trace_util_0.Count(_planbuilder_00000, 42)
		return b.buildExplainFor(x)
	case *ast.TraceStmt:
		trace_util_0.Count(_planbuilder_00000, 43)
		return b.buildTrace(x)
	case *ast.InsertStmt:
		trace_util_0.Count(_planbuilder_00000, 44)
		return b.buildInsert(x)
	case *ast.LoadDataStmt:
		trace_util_0.Count(_planbuilder_00000, 45)
		return b.buildLoadData(x)
	case *ast.LoadStatsStmt:
		trace_util_0.Count(_planbuilder_00000, 46)
		return b.buildLoadStats(x), nil
	case *ast.PrepareStmt:
		trace_util_0.Count(_planbuilder_00000, 47)
		return b.buildPrepare(x), nil
	case *ast.SelectStmt:
		trace_util_0.Count(_planbuilder_00000, 48)
		return b.buildSelect(x)
	case *ast.UnionStmt:
		trace_util_0.Count(_planbuilder_00000, 49)
		return b.buildUnion(x)
	case *ast.UpdateStmt:
		trace_util_0.Count(_planbuilder_00000, 50)
		return b.buildUpdate(x)
	case *ast.ShowStmt:
		trace_util_0.Count(_planbuilder_00000, 51)
		return b.buildShow(x)
	case *ast.DoStmt:
		trace_util_0.Count(_planbuilder_00000, 52)
		return b.buildDo(x)
	case *ast.SetStmt:
		trace_util_0.Count(_planbuilder_00000, 53)
		return b.buildSet(x)
	case *ast.AnalyzeTableStmt:
		trace_util_0.Count(_planbuilder_00000, 54)
		return b.buildAnalyze(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt:
		trace_util_0.Count(_planbuilder_00000, 55)
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DDLNode:
		trace_util_0.Count(_planbuilder_00000, 56)
		return b.buildDDL(x)
	case *ast.CreateBindingStmt:
		trace_util_0.Count(_planbuilder_00000, 57)
		return b.buildCreateBindPlan(x)
	case *ast.DropBindingStmt:
		trace_util_0.Count(_planbuilder_00000, 58)
		return b.buildDropBindPlan(x)
	case *ast.ChangeStmt:
		trace_util_0.Count(_planbuilder_00000, 59)
		return b.buildChange(x)
	case *ast.SplitRegionStmt:
		trace_util_0.Count(_planbuilder_00000, 60)
		return b.buildSplitRegion(x)
	}
	trace_util_0.Count(_planbuilder_00000, 36)
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildChange(v *ast.ChangeStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 61)
	exe := &Change{
		ChangeStmt: v,
	}
	return exe, nil
}

func (b *PlanBuilder) buildExecute(v *ast.ExecuteStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 62)
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		trace_util_0.Count(_planbuilder_00000, 64)
		newExpr, _, err := b.rewrite(expr, nil, nil, true)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 66)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 65)
		vars = append(vars, newExpr)
	}
	trace_util_0.Count(_planbuilder_00000, 63)
	exe := &Execute{Name: v.Name, UsingVars: vars, ExecID: v.ExecID}
	return exe, nil
}

func (b *PlanBuilder) buildDo(v *ast.DoStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 67)
	var p LogicalPlan
	dual := LogicalTableDual{RowCount: 1}.Init(b.ctx)
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(v.Exprs))...)
	for _, astExpr := range v.Exprs {
		trace_util_0.Count(_planbuilder_00000, 69)
		expr, np, err := b.rewrite(astExpr, p, nil, true)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 71)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 70)
		p = np
		proj.Exprs = append(proj.Exprs, expr)
		schema.Append(&expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  expr.GetType(),
		})
	}
	trace_util_0.Count(_planbuilder_00000, 68)
	proj.SetChildren(p)
	proj.self = proj
	proj.SetSchema(schema)
	proj.calculateNoDelay = true
	return proj, nil
}

func (b *PlanBuilder) buildSet(v *ast.SetStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 72)
	p := &Set{}
	for _, vars := range v.Variables {
		trace_util_0.Count(_planbuilder_00000, 74)
		if vars.IsGlobal {
			trace_util_0.Count(_planbuilder_00000, 78)
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
		trace_util_0.Count(_planbuilder_00000, 75)
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			trace_util_0.Count(_planbuilder_00000, 79)
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				trace_util_0.Count(_planbuilder_00000, 81)
				// Convert column name expression to string value expression.
				vars.Value = ast.NewValueExpr(cn.Name.Name.O)
			}
			trace_util_0.Count(_planbuilder_00000, 80)
			mockTablePlan := LogicalTableDual{}.Init(b.ctx)
			var err error
			assign.Expr, _, err = b.rewrite(vars.Value, mockTablePlan, nil, true)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 82)
				return nil, err
			}
		} else {
			trace_util_0.Count(_planbuilder_00000, 83)
			{
				assign.IsDefault = true
			}
		}
		trace_util_0.Count(_planbuilder_00000, 76)
		if vars.ExtendValue != nil {
			trace_util_0.Count(_planbuilder_00000, 84)
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.(*driver.ValueExpr).Datum,
				RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			}
		}
		trace_util_0.Count(_planbuilder_00000, 77)
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	trace_util_0.Count(_planbuilder_00000, 73)
	return p, nil
}

func (b *PlanBuilder) buildDropBindPlan(v *ast.DropBindingStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 85)
	p := &SQLBindPlan{
		SQLBindOp:    OpSQLBindDrop,
		NormdOrigSQL: parser.Normalize(v.OriginSel.Text()),
		IsGlobal:     v.GlobalScope,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func (b *PlanBuilder) buildCreateBindPlan(v *ast.CreateBindingStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 86)
	charSet, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	p := &SQLBindPlan{
		SQLBindOp:    OpSQLBindCreate,
		NormdOrigSQL: parser.Normalize(v.OriginSel.Text()),
		BindSQL:      v.HintedSel.Text(),
		IsGlobal:     v.GlobalScope,
		BindStmt:     v.HintedSel,
		Charset:      charSet,
		Collation:    collation,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	trace_util_0.Count(_planbuilder_00000, 87)
	if sel.GroupBy != nil {
		trace_util_0.Count(_planbuilder_00000, 92)
		return true
	}
	trace_util_0.Count(_planbuilder_00000, 88)
	for _, f := range sel.Fields.Fields {
		trace_util_0.Count(_planbuilder_00000, 93)
		if ast.HasAggFlag(f.Expr) {
			trace_util_0.Count(_planbuilder_00000, 94)
			return true
		}
	}
	trace_util_0.Count(_planbuilder_00000, 89)
	if sel.Having != nil {
		trace_util_0.Count(_planbuilder_00000, 95)
		if ast.HasAggFlag(sel.Having.Expr) {
			trace_util_0.Count(_planbuilder_00000, 96)
			return true
		}
	}
	trace_util_0.Count(_planbuilder_00000, 90)
	if sel.OrderBy != nil {
		trace_util_0.Count(_planbuilder_00000, 97)
		for _, item := range sel.OrderBy.Items {
			trace_util_0.Count(_planbuilder_00000, 98)
			if ast.HasAggFlag(item.Expr) {
				trace_util_0.Count(_planbuilder_00000, 99)
				return true
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 91)
	return false
}

func (b *PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
	trace_util_0.Count(_planbuilder_00000, 100)
	for _, f := range sel.Fields.Fields {
		trace_util_0.Count(_planbuilder_00000, 103)
		if ast.HasWindowFlag(f.Expr) {
			trace_util_0.Count(_planbuilder_00000, 104)
			return true
		}
	}
	trace_util_0.Count(_planbuilder_00000, 101)
	if sel.OrderBy != nil {
		trace_util_0.Count(_planbuilder_00000, 105)
		for _, item := range sel.OrderBy.Items {
			trace_util_0.Count(_planbuilder_00000, 106)
			if ast.HasWindowFlag(item.Expr) {
				trace_util_0.Count(_planbuilder_00000, 107)
				return true
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 102)
	return false
}

func getPathByIndexName(paths []*accessPath, idxName model.CIStr, tblInfo *model.TableInfo) *accessPath {
	trace_util_0.Count(_planbuilder_00000, 108)
	var tablePath *accessPath
	for _, path := range paths {
		trace_util_0.Count(_planbuilder_00000, 111)
		if path.isTablePath {
			trace_util_0.Count(_planbuilder_00000, 113)
			tablePath = path
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 112)
		if path.index.Name.L == idxName.L {
			trace_util_0.Count(_planbuilder_00000, 114)
			return path
		}
	}
	trace_util_0.Count(_planbuilder_00000, 109)
	if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
		trace_util_0.Count(_planbuilder_00000, 115)
		return tablePath
	}
	trace_util_0.Count(_planbuilder_00000, 110)
	return nil
}

func isPrimaryIndex(indexName model.CIStr) bool {
	trace_util_0.Count(_planbuilder_00000, 116)
	return indexName.L == "primary"
}

func getPossibleAccessPaths(indexHints []*ast.IndexHint, tblInfo *model.TableInfo) ([]*accessPath, error) {
	trace_util_0.Count(_planbuilder_00000, 117)
	publicPaths := make([]*accessPath, 0, len(tblInfo.Indices)+1)
	publicPaths = append(publicPaths, &accessPath{isTablePath: true})
	for _, index := range tblInfo.Indices {
		trace_util_0.Count(_planbuilder_00000, 122)
		if index.State == model.StatePublic {
			trace_util_0.Count(_planbuilder_00000, 123)
			publicPaths = append(publicPaths, &accessPath{index: index})
		}
	}

	trace_util_0.Count(_planbuilder_00000, 118)
	hasScanHint, hasUseOrForce := false, false
	available := make([]*accessPath, 0, len(publicPaths))
	ignored := make([]*accessPath, 0, len(publicPaths))
	for _, hint := range indexHints {
		trace_util_0.Count(_planbuilder_00000, 124)
		if hint.HintScope != ast.HintForScan {
			trace_util_0.Count(_planbuilder_00000, 126)
			continue
		}

		trace_util_0.Count(_planbuilder_00000, 125)
		hasScanHint = true
		for _, idxName := range hint.IndexNames {
			trace_util_0.Count(_planbuilder_00000, 127)
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				trace_util_0.Count(_planbuilder_00000, 130)
				return nil, ErrKeyDoesNotExist.GenWithStackByArgs(idxName, tblInfo.Name)
			}
			trace_util_0.Count(_planbuilder_00000, 128)
			if hint.HintType == ast.HintIgnore {
				trace_util_0.Count(_planbuilder_00000, 131)
				// Collect all the ignored index hints.
				ignored = append(ignored, path)
				continue
			}
			// Currently we don't distinguish between "FORCE" and "USE" because
			// our cost estimation is not reliable.
			trace_util_0.Count(_planbuilder_00000, 129)
			hasUseOrForce = true
			path.forced = true
			available = append(available, path)
		}
	}

	trace_util_0.Count(_planbuilder_00000, 119)
	if !hasScanHint || !hasUseOrForce {
		trace_util_0.Count(_planbuilder_00000, 132)
		available = publicPaths
	}

	trace_util_0.Count(_planbuilder_00000, 120)
	available = removeIgnoredPaths(available, ignored, tblInfo)

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use table scan.
	if len(available) == 0 {
		trace_util_0.Count(_planbuilder_00000, 133)
		available = append(available, &accessPath{isTablePath: true})
	}
	trace_util_0.Count(_planbuilder_00000, 121)
	return available, nil
}

func removeIgnoredPaths(paths, ignoredPaths []*accessPath, tblInfo *model.TableInfo) []*accessPath {
	trace_util_0.Count(_planbuilder_00000, 134)
	if len(ignoredPaths) == 0 {
		trace_util_0.Count(_planbuilder_00000, 137)
		return paths
	}
	trace_util_0.Count(_planbuilder_00000, 135)
	remainedPaths := make([]*accessPath, 0, len(paths))
	for _, path := range paths {
		trace_util_0.Count(_planbuilder_00000, 138)
		if path.isTablePath || getPathByIndexName(ignoredPaths, path.index.Name, tblInfo) == nil {
			trace_util_0.Count(_planbuilder_00000, 139)
			remainedPaths = append(remainedPaths, path)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 136)
	return remainedPaths
}

func (b *PlanBuilder) buildSelectLock(src LogicalPlan, lock ast.SelectLockType) *LogicalLock {
	trace_util_0.Count(_planbuilder_00000, 140)
	selectLock := LogicalLock{Lock: lock}.Init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock
}

func (b *PlanBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	trace_util_0.Count(_planbuilder_00000, 141)
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		trace_util_0.Count(_planbuilder_00000, 143)
		if v, ok := b.ctx.GetSessionVars().Users[x.SQLVar.Name]; ok {
			trace_util_0.Count(_planbuilder_00000, 144)
			p.SQLText = v
		} else {
			trace_util_0.Count(_planbuilder_00000, 145)
			{
				p.SQLText = "NULL"
			}
		}
	} else {
		trace_util_0.Count(_planbuilder_00000, 146)
		{
			p.SQLText = x.SQLText
		}
	}
	trace_util_0.Count(_planbuilder_00000, 142)
	return p
}

func (b *PlanBuilder) buildCheckIndex(dbName model.CIStr, as *ast.AdminStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 147)
	tblName := as.Tables[0]
	tbl, err := b.is.TableByName(dbName, tblName.Name)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 153)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 148)
	tblInfo := tbl.Meta()

	// get index information
	var idx *model.IndexInfo
	for _, index := range tblInfo.Indices {
		trace_util_0.Count(_planbuilder_00000, 154)
		if index.Name.L == strings.ToLower(as.Index) {
			trace_util_0.Count(_planbuilder_00000, 155)
			idx = index
			break
		}
	}
	trace_util_0.Count(_planbuilder_00000, 149)
	if idx == nil {
		trace_util_0.Count(_planbuilder_00000, 156)
		return nil, errors.Errorf("index %s do not exist", as.Index)
	}
	trace_util_0.Count(_planbuilder_00000, 150)
	if idx.State != model.StatePublic {
		trace_util_0.Count(_planbuilder_00000, 157)
		return nil, errors.Errorf("index %s state %s isn't public", as.Index, idx.State)
	}

	trace_util_0.Count(_planbuilder_00000, 151)
	id := 1
	columns := make([]*model.ColumnInfo, 0, len(idx.Columns))
	schema := expression.NewSchema(make([]*expression.Column, 0, len(idx.Columns))...)
	for _, idxCol := range idx.Columns {
		trace_util_0.Count(_planbuilder_00000, 158)
		for _, col := range tblInfo.Columns {
			trace_util_0.Count(_planbuilder_00000, 159)
			if idxCol.Name.L == col.Name.L {
				trace_util_0.Count(_planbuilder_00000, 160)
				columns = append(columns, col)
				schema.Append(&expression.Column{
					ColName:  col.Name,
					UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  &col.FieldType,
				})
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 152)
	is := PhysicalIndexScan{
		Table:            tblInfo,
		TableAsName:      &tblName.Name,
		DBName:           dbName,
		Columns:          columns,
		Index:            idx,
		dataSourceSchema: schema,
		Ranges:           ranger.FullRange(),
		KeepOrder:        false,
	}.Init(b.ctx)
	is.stats = &property.StatsInfo{}
	cop := &copTask{indexPlan: is}
	// It's double read case.
	ts := PhysicalTableScan{Columns: columns, Table: is.Table}.Init(b.ctx)
	ts.SetSchema(is.dataSourceSchema)
	cop.tablePlan = ts
	is.initSchema(id, idx, true)
	t := finishCopTask(b.ctx, cop)

	rootT := t.(*rootTask)
	return rootT.p, nil
}

func (b *PlanBuilder) buildAdmin(as *ast.AdminStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 161)
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckTable:
		trace_util_0.Count(_planbuilder_00000, 163)
		ret, err = b.buildAdminCheckTable(as)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 179)
			return ret, err
		}
	case ast.AdminCheckIndex:
		trace_util_0.Count(_planbuilder_00000, 164)
		dbName := as.Tables[0].Schema
		readerPlan, err := b.buildCheckIndex(dbName, as)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 180)
			return ret, err
		}

		trace_util_0.Count(_planbuilder_00000, 165)
		ret = &CheckIndex{
			DBName:            dbName.L,
			IdxName:           as.Index,
			IndexLookUpReader: readerPlan.(*PhysicalIndexLookUpReader),
		}
	case ast.AdminRecoverIndex:
		trace_util_0.Count(_planbuilder_00000, 166)
		p := &RecoverIndex{Table: as.Tables[0], IndexName: as.Index}
		p.SetSchema(buildRecoverIndexFields())
		ret = p
	case ast.AdminCleanupIndex:
		trace_util_0.Count(_planbuilder_00000, 167)
		p := &CleanupIndex{Table: as.Tables[0], IndexName: as.Index}
		p.SetSchema(buildCleanupIndexFields())
		ret = p
	case ast.AdminChecksumTable:
		trace_util_0.Count(_planbuilder_00000, 168)
		p := &ChecksumTable{Tables: as.Tables}
		p.SetSchema(buildChecksumTableSchema())
		ret = p
	case ast.AdminShowNextRowID:
		trace_util_0.Count(_planbuilder_00000, 169)
		p := &ShowNextRowID{TableName: as.Tables[0]}
		p.SetSchema(buildShowNextRowID())
		ret = p
	case ast.AdminShowDDL:
		trace_util_0.Count(_planbuilder_00000, 170)
		p := &ShowDDL{}
		p.SetSchema(buildShowDDLFields())
		ret = p
	case ast.AdminShowDDLJobs:
		trace_util_0.Count(_planbuilder_00000, 171)
		p := &ShowDDLJobs{JobNumber: as.JobNumber}
		p.SetSchema(buildShowDDLJobsFields())
		ret = p
	case ast.AdminCancelDDLJobs:
		trace_util_0.Count(_planbuilder_00000, 172)
		p := &CancelDDLJobs{JobIDs: as.JobIDs}
		p.SetSchema(buildCancelDDLJobsFields())
		ret = p
	case ast.AdminCheckIndexRange:
		trace_util_0.Count(_planbuilder_00000, 173)
		schema, err := b.buildCheckIndexSchema(as.Tables[0], as.Index)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 181)
			return nil, err
		}

		trace_util_0.Count(_planbuilder_00000, 174)
		p := &CheckIndexRange{Table: as.Tables[0], IndexName: as.Index, HandleRanges: as.HandleRanges}
		p.SetSchema(schema)
		ret = p
	case ast.AdminShowDDLJobQueries:
		trace_util_0.Count(_planbuilder_00000, 175)
		p := &ShowDDLJobQueries{JobIDs: as.JobIDs}
		p.SetSchema(buildShowDDLJobQueriesFields())
		ret = p
	case ast.AdminShowSlow:
		trace_util_0.Count(_planbuilder_00000, 176)
		p := &ShowSlow{ShowSlow: as.ShowSlow}
		p.SetSchema(buildShowSlowSchema())
		ret = p
	case ast.AdminReloadExprPushdownBlacklist:
		trace_util_0.Count(_planbuilder_00000, 177)
		return &ReloadExprPushdownBlacklist{}, nil
	default:
		trace_util_0.Count(_planbuilder_00000, 178)
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	trace_util_0.Count(_planbuilder_00000, 162)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return ret, nil
}

func (b *PlanBuilder) buildAdminCheckTable(as *ast.AdminStmt) (*CheckTable, error) {
	trace_util_0.Count(_planbuilder_00000, 182)
	p := &CheckTable{Tables: as.Tables}
	p.GenExprs = make(map[model.TableColumnID]expression.Expression, len(p.Tables))

	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	for _, tbl := range p.Tables {
		trace_util_0.Count(_planbuilder_00000, 184)
		tableInfo := tbl.TableInfo
		schema := expression.TableInfo2SchemaWithDBName(b.ctx, tbl.Schema, tableInfo)
		table, ok := b.is.TableByID(tableInfo.ID)
		if !ok {
			trace_util_0.Count(_planbuilder_00000, 186)
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tbl.DBInfo.Name.O, tableInfo.Name.O)
		}

		trace_util_0.Count(_planbuilder_00000, 185)
		mockTablePlan.SetSchema(schema)

		// Calculate generated columns.
		columns := table.Cols()
		for _, column := range columns {
			trace_util_0.Count(_planbuilder_00000, 187)
			if !column.IsGenerated() {
				trace_util_0.Count(_planbuilder_00000, 191)
				continue
			}
			trace_util_0.Count(_planbuilder_00000, 188)
			columnName := &ast.ColumnName{Name: column.Name}
			columnName.SetText(column.Name.O)

			colExpr, _, err := mockTablePlan.findColumn(columnName)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 192)
				return nil, err
			}

			trace_util_0.Count(_planbuilder_00000, 189)
			expr, _, err := b.rewrite(column.GeneratedExpr, mockTablePlan, nil, true)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 193)
				return nil, err
			}
			trace_util_0.Count(_planbuilder_00000, 190)
			expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
			p.GenExprs[model.TableColumnID{TableID: tableInfo.ID, ColumnID: column.ColumnInfo.ID}] = expr
		}
	}
	trace_util_0.Count(_planbuilder_00000, 183)
	return p, nil
}

func (b *PlanBuilder) buildCheckIndexSchema(tn *ast.TableName, indexName string) (*expression.Schema, error) {
	trace_util_0.Count(_planbuilder_00000, 194)
	schema := expression.NewSchema()
	indexName = strings.ToLower(indexName)
	indicesInfo := tn.TableInfo.Indices
	cols := tn.TableInfo.Cols()
	for _, idxInfo := range indicesInfo {
		trace_util_0.Count(_planbuilder_00000, 197)
		if idxInfo.Name.L != indexName {
			trace_util_0.Count(_planbuilder_00000, 200)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 198)
		for _, idxCol := range idxInfo.Columns {
			trace_util_0.Count(_planbuilder_00000, 201)
			col := cols[idxCol.Offset]
			schema.Append(&expression.Column{
				ColName:  idxCol.Name,
				TblName:  tn.Name,
				DBName:   tn.Schema,
				RetType:  &col.FieldType,
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				ID:       col.ID})
		}
		trace_util_0.Count(_planbuilder_00000, 199)
		schema.Append(&expression.Column{
			ColName:  model.NewCIStr("extra_handle"),
			TblName:  tn.Name,
			DBName:   tn.Schema,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       -1,
		})
	}
	trace_util_0.Count(_planbuilder_00000, 195)
	if schema.Len() == 0 {
		trace_util_0.Count(_planbuilder_00000, 202)
		return nil, errors.Errorf("index %s not found", indexName)
	}
	trace_util_0.Count(_planbuilder_00000, 196)
	return schema, nil
}

// getColsInfo returns the info of index columns, normal columns and primary key.
func getColsInfo(tn *ast.TableName) (indicesInfo []*model.IndexInfo, colsInfo []*model.ColumnInfo, pkCol *model.ColumnInfo) {
	trace_util_0.Count(_planbuilder_00000, 203)
	tbl := tn.TableInfo
	for _, col := range tbl.Columns {
		trace_util_0.Count(_planbuilder_00000, 206)
		// The virtual column will not store any data in TiKV, so it should be ignored when collect statistics
		if col.IsGenerated() && !col.GeneratedStored {
			trace_util_0.Count(_planbuilder_00000, 208)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 207)
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_planbuilder_00000, 209)
			pkCol = col
		} else {
			trace_util_0.Count(_planbuilder_00000, 210)
			{
				colsInfo = append(colsInfo, col)
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 204)
	for _, idx := range tn.TableInfo.Indices {
		trace_util_0.Count(_planbuilder_00000, 211)
		if idx.State == model.StatePublic {
			trace_util_0.Count(_planbuilder_00000, 212)
			indicesInfo = append(indicesInfo, idx)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 205)
	return
}

func getPhysicalIDsAndPartitionNames(tblInfo *model.TableInfo, partitionNames []model.CIStr) ([]int64, []string, error) {
	trace_util_0.Count(_planbuilder_00000, 213)
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_planbuilder_00000, 217)
		if len(partitionNames) != 0 {
			trace_util_0.Count(_planbuilder_00000, 219)
			return nil, nil, errors.Trace(ddl.ErrPartitionMgmtOnNonpartitioned)
		}
		trace_util_0.Count(_planbuilder_00000, 218)
		return []int64{tblInfo.ID}, []string{""}, nil
	}
	trace_util_0.Count(_planbuilder_00000, 214)
	if len(partitionNames) == 0 {
		trace_util_0.Count(_planbuilder_00000, 220)
		ids := make([]int64, 0, len(pi.Definitions))
		names := make([]string, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			trace_util_0.Count(_planbuilder_00000, 222)
			ids = append(ids, def.ID)
			names = append(names, def.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 221)
		return ids, names, nil
	}
	trace_util_0.Count(_planbuilder_00000, 215)
	ids := make([]int64, 0, len(partitionNames))
	names := make([]string, 0, len(partitionNames))
	for _, name := range partitionNames {
		trace_util_0.Count(_planbuilder_00000, 223)
		found := false
		for _, def := range pi.Definitions {
			trace_util_0.Count(_planbuilder_00000, 225)
			if def.Name.L == name.L {
				trace_util_0.Count(_planbuilder_00000, 226)
				found = true
				ids = append(ids, def.ID)
				names = append(names, def.Name.O)
				break
			}
		}
		trace_util_0.Count(_planbuilder_00000, 224)
		if !found {
			trace_util_0.Count(_planbuilder_00000, 227)
			return nil, nil, fmt.Errorf("can not found the specified partition name %s in the table definition", name.O)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 216)
	return ids, names, nil
}

func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 228)
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	for _, tbl := range as.TableNames {
		trace_util_0.Count(_planbuilder_00000, 230)
		if tbl.TableInfo.IsView() {
			trace_util_0.Count(_planbuilder_00000, 234)
			return nil, errors.Errorf("analyze %s is not supported now.", tbl.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 231)
		idxInfo, colInfo, pkInfo := getColsInfo(tbl)
		physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tbl.TableInfo, as.PartitionNames)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 235)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 232)
		for _, idx := range idxInfo {
			trace_util_0.Count(_planbuilder_00000, 236)
			for i, id := range physicalIDs {
				trace_util_0.Count(_planbuilder_00000, 237)
				info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{
					IndexInfo:   idx,
					analyzeInfo: info,
					TblInfo:     tbl.TableInfo,
				})
			}
		}
		trace_util_0.Count(_planbuilder_00000, 233)
		if len(colInfo) > 0 || pkInfo != nil {
			trace_util_0.Count(_planbuilder_00000, 238)
			for i, id := range physicalIDs {
				trace_util_0.Count(_planbuilder_00000, 239)
				info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
					PKInfo:      pkInfo,
					ColsInfo:    colInfo,
					analyzeInfo: info,
					TblInfo:     tbl.TableInfo,
				})
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 229)
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 240)
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 243)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 241)
	for _, idxName := range as.IndexNames {
		trace_util_0.Count(_planbuilder_00000, 244)
		if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
			trace_util_0.Count(_planbuilder_00000, 247)
			pkCol := tblInfo.GetPkColInfo()
			for i, id := range physicalIDs {
				trace_util_0.Count(_planbuilder_00000, 249)
				info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info})
			}
			trace_util_0.Count(_planbuilder_00000, 248)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 245)
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			trace_util_0.Count(_planbuilder_00000, 250)
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 246)
		for i, id := range physicalIDs {
			trace_util_0.Count(_planbuilder_00000, 251)
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
		}
	}
	trace_util_0.Count(_planbuilder_00000, 242)
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 252)
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 256)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 253)
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_planbuilder_00000, 257)
		if idx.State == model.StatePublic {
			trace_util_0.Count(_planbuilder_00000, 258)
			for i, id := range physicalIDs {
				trace_util_0.Count(_planbuilder_00000, 259)
				info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 254)
	if tblInfo.PKIsHandle {
		trace_util_0.Count(_planbuilder_00000, 260)
		pkCol := tblInfo.GetPkColInfo()
		for i, id := range physicalIDs {
			trace_util_0.Count(_planbuilder_00000, 261)
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info})
		}
	}
	trace_util_0.Count(_planbuilder_00000, 255)
	return p, nil
}

const (
	defaultMaxNumBuckets = 256
	numBucketsLimit      = 1024
)

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 262)
	// If enable fast analyze, the storage must be tikv.Storage.
	if _, isTikvStorage := b.ctx.GetStore().(tikv.Storage); !isTikvStorage && b.ctx.GetSessionVars().EnableFastAnalyze {
		trace_util_0.Count(_planbuilder_00000, 267)
		return nil, errors.Errorf("Only support fast analyze in tikv storage.")
	}
	trace_util_0.Count(_planbuilder_00000, 263)
	for _, tbl := range as.TableNames {
		trace_util_0.Count(_planbuilder_00000, 268)
		user := b.ctx.GetSessionVars().User
		var insertErr, selectErr error
		if user != nil {
			trace_util_0.Count(_planbuilder_00000, 270)
			insertErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
			selectErr = ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 269)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tbl.Schema.O, tbl.Name.O, "", insertErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, tbl.Schema.O, tbl.Name.O, "", selectErr)
	}
	trace_util_0.Count(_planbuilder_00000, 264)
	if as.MaxNumBuckets == 0 {
		trace_util_0.Count(_planbuilder_00000, 271)
		as.MaxNumBuckets = defaultMaxNumBuckets
	} else {
		trace_util_0.Count(_planbuilder_00000, 272)
		{
			as.MaxNumBuckets = mathutil.MinUint64(as.MaxNumBuckets, numBucketsLimit)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 265)
	if as.IndexFlag {
		trace_util_0.Count(_planbuilder_00000, 273)
		if len(as.IndexNames) == 0 {
			trace_util_0.Count(_planbuilder_00000, 275)
			return b.buildAnalyzeAllIndex(as)
		}
		trace_util_0.Count(_planbuilder_00000, 274)
		return b.buildAnalyzeIndex(as)
	}
	trace_util_0.Count(_planbuilder_00000, 266)
	return b.buildAnalyzeTable(as)
}

func buildShowNextRowID() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 276)
	schema := expression.NewSchema(make([]*expression.Column, 0, 4)...)
	schema.Append(buildColumn("", "DB_NAME", mysql.TypeVarchar, mysql.MaxDatabaseNameLength))
	schema.Append(buildColumn("", "TABLE_NAME", mysql.TypeVarchar, mysql.MaxTableNameLength))
	schema.Append(buildColumn("", "COLUMN_NAME", mysql.TypeVarchar, mysql.MaxColumnNameLength))
	schema.Append(buildColumn("", "NEXT_GLOBAL_ROW_ID", mysql.TypeLonglong, 4))
	return schema
}

func buildShowDDLFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 277)
	schema := expression.NewSchema(make([]*expression.Column, 0, 4)...)
	schema.Append(buildColumn("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "OWNER_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "OWNER_ADDRESS", mysql.TypeVarchar, 32))
	schema.Append(buildColumn("", "RUNNING_JOBS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "SELF_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "QUERY", mysql.TypeVarchar, 256))

	return schema
}

func buildRecoverIndexFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 278)
	schema := expression.NewSchema(make([]*expression.Column, 0, 2)...)
	schema.Append(buildColumn("", "ADDED_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "SCAN_COUNT", mysql.TypeLonglong, 4))
	return schema
}

func buildCleanupIndexFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 279)
	schema := expression.NewSchema(make([]*expression.Column, 0, 1)...)
	schema.Append(buildColumn("", "REMOVED_COUNT", mysql.TypeLonglong, 4))
	return schema
}

func buildShowDDLJobsFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 280)
	schema := expression.NewSchema(make([]*expression.Column, 0, 10)...)
	schema.Append(buildColumn("", "JOB_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "DB_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "TABLE_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "JOB_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_STATE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "TABLE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "ROW_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "START_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "STATE", mysql.TypeVarchar, 64))
	return schema
}

func buildShowDDLJobQueriesFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 281)
	schema := expression.NewSchema(make([]*expression.Column, 0, 1)...)
	schema.Append(buildColumn("", "QUERY", mysql.TypeVarchar, 256))
	return schema
}

func buildShowSlowSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 282)
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	tinySize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	timestampSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTimestamp)
	durationSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDuration)

	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn("", "SQL", mysql.TypeVarchar, 4096))
	schema.Append(buildColumn("", "START", mysql.TypeTimestamp, timestampSize))
	schema.Append(buildColumn("", "DURATION", mysql.TypeDuration, durationSize))
	schema.Append(buildColumn("", "DETAILS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "SUCC", mysql.TypeTiny, tinySize))
	schema.Append(buildColumn("", "CONN_ID", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumn("", "TRANSACTION_TS", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumn("", "USER", mysql.TypeVarchar, 32))
	schema.Append(buildColumn("", "DB", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "TABLE_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "INDEX_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "INTERNAL", mysql.TypeTiny, tinySize))
	schema.Append(buildColumn("", "DIGEST", mysql.TypeVarchar, 64))
	return schema
}

func buildCancelDDLJobsFields() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 283)
	schema := expression.NewSchema(make([]*expression.Column, 0, 2)...)
	schema.Append(buildColumn("", "JOB_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "RESULT", mysql.TypeVarchar, 128))

	return schema
}

func buildColumn(tableName, name string, tp byte, size int) *expression.Column {
	trace_util_0.Count(_planbuilder_00000, 284)
	cs, cl := types.DefaultCharsetForType(tp)
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		trace_util_0.Count(_planbuilder_00000, 286)
		cs = charset.CharsetUTF8MB4
		cl = charset.CollationUTF8MB4
		flag = 0
	}

	trace_util_0.Count(_planbuilder_00000, 285)
	fieldType := &types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    flag,
	}
	return &expression.Column{
		ColName: model.NewCIStr(name),
		TblName: model.NewCIStr(tableName),
		DBName:  model.NewCIStr(infoschema.Name),
		RetType: fieldType,
	}
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	trace_util_0.Count(_planbuilder_00000, 287)
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
		trace_util_0.Count(_planbuilder_00000, 289)
	case *ast.BinaryOperationExpr:
		trace_util_0.Count(_planbuilder_00000, 290)
		if x.Op == opcode.LogicAnd {
			trace_util_0.Count(_planbuilder_00000, 293)
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			trace_util_0.Count(_planbuilder_00000, 294)
			{
				conditions = append(conditions, x)
			}
		}
	case *ast.ParenthesesExpr:
		trace_util_0.Count(_planbuilder_00000, 291)
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		trace_util_0.Count(_planbuilder_00000, 292)
		conditions = append(conditions, where)
	}
	trace_util_0.Count(_planbuilder_00000, 288)
	return conditions
}

func (b *PlanBuilder) buildShow(show *ast.ShowStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 295)
	p := Show{
		Tp:          show.Tp,
		DBName:      show.DBName,
		Table:       show.Table,
		Column:      show.Column,
		Flag:        show.Flag,
		Full:        show.Full,
		User:        show.User,
		Roles:       show.Roles,
		IfNotExists: show.IfNotExists,
		GlobalScope: show.GlobalScope,
	}.Init(b.ctx)
	switch showTp := show.Tp; showTp {
	case ast.ShowProcedureStatus:
		trace_util_0.Count(_planbuilder_00000, 300)
		p.SetSchema(buildShowProcedureSchema())
	case ast.ShowTriggers:
		trace_util_0.Count(_planbuilder_00000, 301)
		p.SetSchema(buildShowTriggerSchema())
	case ast.ShowEvents:
		trace_util_0.Count(_planbuilder_00000, 302)
		p.SetSchema(buildShowEventsSchema())
	case ast.ShowWarnings, ast.ShowErrors:
		trace_util_0.Count(_planbuilder_00000, 303)
		p.SetSchema(buildShowWarningsSchema())
	default:
		trace_util_0.Count(_planbuilder_00000, 304)
		isView := false
		switch showTp {
		case ast.ShowTables, ast.ShowTableStatus:
			trace_util_0.Count(_planbuilder_00000, 306)
			if p.DBName == "" {
				trace_util_0.Count(_planbuilder_00000, 310)
				return nil, ErrNoDB
			}
		case ast.ShowCreateTable:
			trace_util_0.Count(_planbuilder_00000, 307)
			user := b.ctx.GetSessionVars().User
			var err error
			if user != nil {
				trace_util_0.Count(_planbuilder_00000, 311)
				err = ErrTableaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			trace_util_0.Count(_planbuilder_00000, 308)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask, show.Table.Schema.L, show.Table.Name.L, "", err)
			if table, err := b.is.TableByName(show.Table.Schema, show.Table.Name); err == nil {
				trace_util_0.Count(_planbuilder_00000, 312)
				isView = table.Meta().IsView()
			}
		case ast.ShowCreateView:
			trace_util_0.Count(_planbuilder_00000, 309)
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SHOW VIEW")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		}
		trace_util_0.Count(_planbuilder_00000, 305)
		p.SetSchema(buildShowSchema(show, isView))
	}
	trace_util_0.Count(_planbuilder_00000, 296)
	for _, col := range p.schema.Columns {
		trace_util_0.Count(_planbuilder_00000, 313)
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	trace_util_0.Count(_planbuilder_00000, 297)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(p.schema)
	if show.Pattern != nil {
		trace_util_0.Count(_planbuilder_00000, 314)
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.Schema().Columns[0].ColName},
		}
		expr, _, err := b.rewrite(show.Pattern, mockTablePlan, nil, false)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 316)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 315)
		p.Conditions = append(p.Conditions, expr)
	}
	trace_util_0.Count(_planbuilder_00000, 298)
	if show.Where != nil {
		trace_util_0.Count(_planbuilder_00000, 317)
		conds := splitWhere(show.Where)
		for _, cond := range conds {
			trace_util_0.Count(_planbuilder_00000, 319)
			expr, _, err := b.rewrite(cond, mockTablePlan, nil, false)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 321)
				return nil, err
			}
			trace_util_0.Count(_planbuilder_00000, 320)
			p.Conditions = append(p.Conditions, expr)
		}
		trace_util_0.Count(_planbuilder_00000, 318)
		err := p.ResolveIndices()
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 322)
			return nil, err
		}
	}
	trace_util_0.Count(_planbuilder_00000, 299)
	return p, nil
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 323)
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.CreateUserStmt:
		trace_util_0.Count(_planbuilder_00000, 325)
		if raw.IsCreateRole {
			trace_util_0.Count(_planbuilder_00000, 334)
			err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateRolePriv, "", "", "", err)
		} else {
			trace_util_0.Count(_planbuilder_00000, 335)
			{
				err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
			}
		}
	case *ast.DropUserStmt:
		trace_util_0.Count(_planbuilder_00000, 326)
		if raw.IsDropRole {
			trace_util_0.Count(_planbuilder_00000, 336)
			err := ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropRolePriv, "", "", "", err)
		} else {
			trace_util_0.Count(_planbuilder_00000, 337)
			{
				err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
			}
		}
	case *ast.AlterUserStmt, *ast.SetDefaultRoleStmt:
		trace_util_0.Count(_planbuilder_00000, 327)
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		trace_util_0.Count(_planbuilder_00000, 328)
		b.visitInfo = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
	case *ast.GrantRoleStmt:
		trace_util_0.Count(_planbuilder_00000, 329)
		err := ErrSpecificAccessDenied.GenWithStackByArgs("GRANT ROLE")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.GrantPriv, "", "", "", err)
	case *ast.RevokeStmt:
		trace_util_0.Count(_planbuilder_00000, 330)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.RevokeRoleStmt:
		trace_util_0.Count(_planbuilder_00000, 331)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.KillStmt:
		trace_util_0.Count(_planbuilder_00000, 332)
		// If you have the SUPER privilege, you can kill all threads and statements.
		// Otherwise, you can kill only your own threads and statements.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			trace_util_0.Count(_planbuilder_00000, 338)
			if pi, ok := sm.GetProcessInfo(raw.ConnectionID); ok {
				trace_util_0.Count(_planbuilder_00000, 339)
				loginUser := b.ctx.GetSessionVars().User
				if pi.User != loginUser.Username {
					trace_util_0.Count(_planbuilder_00000, 340)
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
				}
			}
		}
	case *ast.UseStmt:
		trace_util_0.Count(_planbuilder_00000, 333)
		if raw.DBName == "" {
			trace_util_0.Count(_planbuilder_00000, 341)
			return nil, ErrNoDB
		}
	}
	trace_util_0.Count(_planbuilder_00000, 324)
	return p, nil
}

func collectVisitInfoFromGrantStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	trace_util_0.Count(_planbuilder_00000, 342)
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	if dbName == "" {
		trace_util_0.Count(_planbuilder_00000, 346)
		dbName = sctx.GetSessionVars().CurrentDB
	}
	trace_util_0.Count(_planbuilder_00000, 343)
	vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)

	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		trace_util_0.Count(_planbuilder_00000, 347)
		if item.Priv == mysql.AllPriv {
			trace_util_0.Count(_planbuilder_00000, 349)
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				trace_util_0.Count(_planbuilder_00000, 351)
				allPrivs = mysql.AllGlobalPrivs
			case ast.GrantLevelDB:
				trace_util_0.Count(_planbuilder_00000, 352)
				allPrivs = mysql.AllDBPrivs
			case ast.GrantLevelTable:
				trace_util_0.Count(_planbuilder_00000, 353)
				allPrivs = mysql.AllTablePrivs
			}
			trace_util_0.Count(_planbuilder_00000, 350)
			break
		}
		trace_util_0.Count(_planbuilder_00000, 348)
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", nil)
	}

	trace_util_0.Count(_planbuilder_00000, 344)
	for _, priv := range allPrivs {
		trace_util_0.Count(_planbuilder_00000, 354)
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", nil)
	}

	trace_util_0.Count(_planbuilder_00000, 345)
	return vi
}

func (b *PlanBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	trace_util_0.Count(_planbuilder_00000, 355)
	value, err := table.GetColDefaultValue(b.ctx, col.ToInfo())
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 357)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 356)
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

func (b *PlanBuilder) findDefaultValue(cols []*table.Column, name *ast.ColumnName) (*expression.Constant, error) {
	trace_util_0.Count(_planbuilder_00000, 358)
	for _, col := range cols {
		trace_util_0.Count(_planbuilder_00000, 360)
		if col.Name.L == name.Name.L {
			trace_util_0.Count(_planbuilder_00000, 361)
			return b.getDefaultValue(col)
		}
	}
	trace_util_0.Count(_planbuilder_00000, 359)
	return nil, ErrUnknownColumn.GenWithStackByArgs(name.Name.O, "field_list")
}

// resolveGeneratedColumns resolves generated columns with their generation
// expressions respectively. onDups indicates which columns are in on-duplicate list.
func (b *PlanBuilder) resolveGeneratedColumns(columns []*table.Column, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedColumns, err error) {
	trace_util_0.Count(_planbuilder_00000, 362)
	for _, column := range columns {
		trace_util_0.Count(_planbuilder_00000, 364)
		if !column.IsGenerated() {
			trace_util_0.Count(_planbuilder_00000, 369)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 365)
		columnName := &ast.ColumnName{Name: column.Name}
		columnName.SetText(column.Name.O)

		colExpr, _, err := mockPlan.findColumn(columnName)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 370)
			return igc, err
		}

		trace_util_0.Count(_planbuilder_00000, 366)
		expr, _, err := b.rewrite(column.GeneratedExpr, mockPlan, nil, true)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 371)
			return igc, err
		}
		trace_util_0.Count(_planbuilder_00000, 367)
		expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())

		igc.Columns = append(igc.Columns, columnName)
		igc.Exprs = append(igc.Exprs, expr)
		if onDups == nil {
			trace_util_0.Count(_planbuilder_00000, 372)
			continue
		}
		trace_util_0.Count(_planbuilder_00000, 368)
		for dep := range column.Dependences {
			trace_util_0.Count(_planbuilder_00000, 373)
			if _, ok := onDups[dep]; ok {
				trace_util_0.Count(_planbuilder_00000, 374)
				assign := &expression.Assignment{Col: colExpr, Expr: expr}
				igc.OnDuplicates = append(igc.OnDuplicates, assign)
				break
			}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 363)
	return igc, nil
}

func (b *PlanBuilder) buildInsert(insert *ast.InsertStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 375)
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 387)
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	trace_util_0.Count(_planbuilder_00000, 376)
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 388)
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	trace_util_0.Count(_planbuilder_00000, 377)
	tableInfo := tn.TableInfo
	if tableInfo.IsView() {
		trace_util_0.Count(_planbuilder_00000, 389)
		err := errors.Errorf("insert into view %s is not supported now.", tableInfo.Name.O)
		if insert.IsReplace {
			trace_util_0.Count(_planbuilder_00000, 391)
			err = errors.Errorf("replace into view %s is not supported now.", tableInfo.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 390)
		return nil, err
	}
	// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	trace_util_0.Count(_planbuilder_00000, 378)
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, tn.Schema, tableInfo)
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 392)
		return nil, errors.Errorf("Can't get table %s.", tableInfo.Name.O)
	}

	trace_util_0.Count(_planbuilder_00000, 379)
	insertPlan := Insert{
		Table:       tableInPlan,
		Columns:     insert.Columns,
		tableSchema: schema,
		IsReplace:   insert.IsReplace,
	}.Init(b.ctx)

	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_planbuilder_00000, 393)
		authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
			b.ctx.GetSessionVars().User.Username, tableInfo.Name.L)
	}

	trace_util_0.Count(_planbuilder_00000, 380)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tn.DBInfo.Name.L,
		tableInfo.Name.L, "", authErr)

	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(insertPlan.tableSchema)

	checkRefColumn := func(n ast.Node) ast.Node {
		trace_util_0.Count(_planbuilder_00000, 394)
		if insertPlan.NeedFillDefaultValue {
			trace_util_0.Count(_planbuilder_00000, 397)
			return n
		}
		trace_util_0.Count(_planbuilder_00000, 395)
		switch n.(type) {
		case *ast.ColumnName, *ast.ColumnNameExpr:
			trace_util_0.Count(_planbuilder_00000, 398)
			insertPlan.NeedFillDefaultValue = true
		}
		trace_util_0.Count(_planbuilder_00000, 396)
		return n
	}

	trace_util_0.Count(_planbuilder_00000, 381)
	if len(insert.Setlist) > 0 {
		trace_util_0.Count(_planbuilder_00000, 399)
		// Branch for `INSERT ... SET ...`.
		err := b.buildSetValuesOfInsert(insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 400)
			return nil, err
		}
	} else {
		trace_util_0.Count(_planbuilder_00000, 401)
		if len(insert.Lists) > 0 {
			trace_util_0.Count(_planbuilder_00000, 402)
			// Branch for `INSERT ... VALUES ...`.
			err := b.buildValuesListOfInsert(insert, insertPlan, mockTablePlan, checkRefColumn)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 403)
				return nil, err
			}
		} else {
			trace_util_0.Count(_planbuilder_00000, 404)
			{
				// Branch for `INSERT ... SELECT ...`.
				err := b.buildSelectPlanOfInsert(insert, insertPlan)
				if err != nil {
					trace_util_0.Count(_planbuilder_00000, 405)
					return nil, err
				}
			}
		}
	}

	trace_util_0.Count(_planbuilder_00000, 382)
	mockTablePlan.SetSchema(insertPlan.Schema4OnDuplicate)
	columnByName := make(map[string]*table.Column, len(insertPlan.Table.Cols()))
	for _, col := range insertPlan.Table.Cols() {
		trace_util_0.Count(_planbuilder_00000, 406)
		columnByName[col.Name.L] = col
	}
	trace_util_0.Count(_planbuilder_00000, 383)
	onDupColSet, dupCols, err := insertPlan.validateOnDup(insert.OnDuplicate, columnByName, tableInfo)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 407)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 384)
	for i, assign := range insert.OnDuplicate {
		trace_util_0.Count(_planbuilder_00000, 408)
		// Construct the function which calculates the assign value of the column.
		expr, err1 := b.rewriteInsertOnDuplicateUpdate(assign.Expr, mockTablePlan, insertPlan)
		if err1 != nil {
			trace_util_0.Count(_planbuilder_00000, 410)
			return nil, err1
		}

		trace_util_0.Count(_planbuilder_00000, 409)
		insertPlan.OnDuplicate = append(insertPlan.OnDuplicate, &expression.Assignment{
			Col:  dupCols[i],
			Expr: expr,
		})
	}

	// Calculate generated columns.
	trace_util_0.Count(_planbuilder_00000, 385)
	mockTablePlan.schema = insertPlan.tableSchema
	insertPlan.GenCols, err = b.resolveGeneratedColumns(insertPlan.Table.Cols(), onDupColSet, mockTablePlan)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 411)
		return nil, err
	}

	trace_util_0.Count(_planbuilder_00000, 386)
	err = insertPlan.ResolveIndices()
	return insertPlan, err
}

func (p *Insert) validateOnDup(onDup []*ast.Assignment, colMap map[string]*table.Column, tblInfo *model.TableInfo) (map[string]struct{}, []*expression.Column, error) {
	trace_util_0.Count(_planbuilder_00000, 412)
	onDupColSet := make(map[string]struct{}, len(onDup))
	dupCols := make([]*expression.Column, 0, len(onDup))
	for _, assign := range onDup {
		trace_util_0.Count(_planbuilder_00000, 414)
		// Check whether the column to be updated exists in the source table.
		col, err := p.tableSchema.FindColumn(assign.Column)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 417)
			return nil, nil, err
		} else {
			trace_util_0.Count(_planbuilder_00000, 418)
			if col == nil {
				trace_util_0.Count(_planbuilder_00000, 419)
				return nil, nil, ErrUnknownColumn.GenWithStackByArgs(assign.Column.OrigColName(), "field list")
			}
		}

		// Check whether the column to be updated is the generated column.
		trace_util_0.Count(_planbuilder_00000, 415)
		column := colMap[assign.Column.Name.L]
		if column.IsGenerated() {
			trace_util_0.Count(_planbuilder_00000, 420)
			return nil, nil, ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 416)
		onDupColSet[column.Name.L] = struct{}{}
		dupCols = append(dupCols, col)
	}
	trace_util_0.Count(_planbuilder_00000, 413)
	return onDupColSet, dupCols, nil
}

func (b *PlanBuilder) getAffectCols(insertStmt *ast.InsertStmt, insertPlan *Insert) (affectedValuesCols []*table.Column, err error) {
	trace_util_0.Count(_planbuilder_00000, 421)
	if len(insertStmt.Columns) > 0 {
		trace_util_0.Count(_planbuilder_00000, 423)
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name (col_name [, col_name] ...) {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name (col_name [, col_name] ...) SELECT ...`.
		colName := make([]string, 0, len(insertStmt.Columns))
		for _, col := range insertStmt.Columns {
			trace_util_0.Count(_planbuilder_00000, 425)
			colName = append(colName, col.Name.O)
		}
		trace_util_0.Count(_planbuilder_00000, 424)
		affectedValuesCols, err = table.FindCols(insertPlan.Table.Cols(), colName, insertPlan.Table.Meta().PKIsHandle)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 426)
			return nil, err
		}

	} else {
		trace_util_0.Count(_planbuilder_00000, 427)
		if len(insertStmt.Setlist) == 0 {
			trace_util_0.Count(_planbuilder_00000, 428)
			// This branch is for the following scenarios:
			// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
			// 2. `INSERT INTO tbl_name SELECT ...`.
			affectedValuesCols = insertPlan.Table.Cols()
		}
	}
	trace_util_0.Count(_planbuilder_00000, 422)
	return affectedValuesCols, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	trace_util_0.Count(_planbuilder_00000, 429)
	tableInfo := insertPlan.Table.Meta()
	colNames := make([]string, 0, len(insert.Setlist))
	exprCols := make([]*expression.Column, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		trace_util_0.Count(_planbuilder_00000, 434)
		exprCol, err := insertPlan.tableSchema.FindColumn(assign.Column)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 437)
			return err
		}
		trace_util_0.Count(_planbuilder_00000, 435)
		if exprCol == nil {
			trace_util_0.Count(_planbuilder_00000, 438)
			return errors.Errorf("Can't find column %s", assign.Column)
		}
		trace_util_0.Count(_planbuilder_00000, 436)
		colNames = append(colNames, assign.Column.Name.L)
		exprCols = append(exprCols, exprCol)
	}

	// Check whether the column to be updated is the generated column.
	trace_util_0.Count(_planbuilder_00000, 430)
	tCols, err := table.FindCols(insertPlan.Table.Cols(), colNames, tableInfo.PKIsHandle)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 439)
		return err
	}
	trace_util_0.Count(_planbuilder_00000, 431)
	for _, tCol := range tCols {
		trace_util_0.Count(_planbuilder_00000, 440)
		if tCol.IsGenerated() {
			trace_util_0.Count(_planbuilder_00000, 441)
			return ErrBadGeneratedColumn.GenWithStackByArgs(tCol.Name.O, tableInfo.Name.O)
		}
	}

	trace_util_0.Count(_planbuilder_00000, 432)
	for i, assign := range insert.Setlist {
		trace_util_0.Count(_planbuilder_00000, 442)
		expr, _, err := b.rewriteWithPreprocess(assign.Expr, mockTablePlan, nil, nil, true, checkRefColumn)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 444)
			return err
		}
		trace_util_0.Count(_planbuilder_00000, 443)
		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			Col:  exprCols[i],
			Expr: expr,
		})
	}
	trace_util_0.Count(_planbuilder_00000, 433)
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildValuesListOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	trace_util_0.Count(_planbuilder_00000, 445)
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 449)
		return err
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	trace_util_0.Count(_planbuilder_00000, 446)
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		trace_util_0.Count(_planbuilder_00000, 450)
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			trace_util_0.Count(_planbuilder_00000, 452)
			return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
		// No generated column is allowed.
		trace_util_0.Count(_planbuilder_00000, 451)
		for _, col := range affectedValuesCols {
			trace_util_0.Count(_planbuilder_00000, 453)
			if col.IsGenerated() {
				trace_util_0.Count(_planbuilder_00000, 454)
				return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
			}
		}
	}

	trace_util_0.Count(_planbuilder_00000, 447)
	totalTableCols := insertPlan.Table.Cols()
	for i, valuesItem := range insert.Lists {
		trace_util_0.Count(_planbuilder_00000, 455)
		// The length of all the value_list should be the same.
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		if i > 0 && len(insert.Lists[i-1]) != len(insert.Lists[i]) {
			trace_util_0.Count(_planbuilder_00000, 458)
			return ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
		}
		trace_util_0.Count(_planbuilder_00000, 456)
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			trace_util_0.Count(_planbuilder_00000, 459)
			var expr expression.Expression
			var err error
			switch x := valueItem.(type) {
			case *ast.DefaultExpr:
				trace_util_0.Count(_planbuilder_00000, 462)
				if x.Name != nil {
					trace_util_0.Count(_planbuilder_00000, 465)
					expr, err = b.findDefaultValue(totalTableCols, x.Name)
				} else {
					trace_util_0.Count(_planbuilder_00000, 466)
					{
						expr, err = b.getDefaultValue(affectedValuesCols[j])
					}
				}
			case *driver.ValueExpr:
				trace_util_0.Count(_planbuilder_00000, 463)
				expr = &expression.Constant{
					Value:   x.Datum,
					RetType: &x.Type,
				}
			default:
				trace_util_0.Count(_planbuilder_00000, 464)
				expr, _, err = b.rewriteWithPreprocess(valueItem, mockTablePlan, nil, nil, true, checkRefColumn)
			}
			trace_util_0.Count(_planbuilder_00000, 460)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 467)
				return err
			}
			trace_util_0.Count(_planbuilder_00000, 461)
			exprList = append(exprList, expr)
		}
		trace_util_0.Count(_planbuilder_00000, 457)
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	trace_util_0.Count(_planbuilder_00000, 448)
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildSelectPlanOfInsert(insert *ast.InsertStmt, insertPlan *Insert) error {
	trace_util_0.Count(_planbuilder_00000, 468)
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 476)
		return err
	}
	trace_util_0.Count(_planbuilder_00000, 469)
	selectPlan, err := b.Build(insert.Select)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 477)
		return err
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	trace_util_0.Count(_planbuilder_00000, 470)
	if selectPlan.Schema().Len() != len(affectedValuesCols) {
		trace_util_0.Count(_planbuilder_00000, 478)
		return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
	}

	// Check to guarantee that there's no generated column.
	// This check should be done after the above one to make its behavior compatible with MySQL.
	// For example, table t has two columns, namely a and b, and b is a generated column.
	// "insert into t (b) select * from t" will raise an error that the column count is not matched.
	// "insert into t select * from t" will raise an error that there's a generated column in the column list.
	// If we do this check before the above one, "insert into t (b) select * from t" will raise an error
	// that there's a generated column in the column list.
	trace_util_0.Count(_planbuilder_00000, 471)
	for _, col := range affectedValuesCols {
		trace_util_0.Count(_planbuilder_00000, 479)
		if col.IsGenerated() {
			trace_util_0.Count(_planbuilder_00000, 480)
			return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
		}
	}

	trace_util_0.Count(_planbuilder_00000, 472)
	insertPlan.SelectPlan, err = DoOptimize(b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 481)
		return err
	}

	// schema4NewRow is the schema for the newly created data record based on
	// the result of the select statement.
	trace_util_0.Count(_planbuilder_00000, 473)
	schema4NewRow := expression.NewSchema(make([]*expression.Column, len(insertPlan.Table.Cols()))...)
	for i, selCol := range insertPlan.SelectPlan.Schema().Columns {
		trace_util_0.Count(_planbuilder_00000, 482)
		ordinal := affectedValuesCols[i].Offset
		schema4NewRow.Columns[ordinal] = &expression.Column{}
		*schema4NewRow.Columns[ordinal] = *selCol

		schema4NewRow.Columns[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.Columns[ordinal].RetType = affectedValuesCols[i].FieldType
	}
	trace_util_0.Count(_planbuilder_00000, 474)
	for i := range schema4NewRow.Columns {
		trace_util_0.Count(_planbuilder_00000, 483)
		if schema4NewRow.Columns[i] == nil {
			trace_util_0.Count(_planbuilder_00000, 484)
			schema4NewRow.Columns[i] = &expression.Column{UniqueID: insertPlan.ctx.GetSessionVars().AllocPlanColumnID()}
		}
	}
	trace_util_0.Count(_planbuilder_00000, 475)
	insertPlan.Schema4OnDuplicate = expression.MergeSchema(insertPlan.tableSchema, schema4NewRow)
	return nil
}

func (b *PlanBuilder) buildLoadData(ld *ast.LoadDataStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 485)
	p := &LoadData{
		IsLocal:     ld.IsLocal,
		OnDuplicate: ld.OnDuplicate,
		Path:        ld.Path,
		Table:       ld.Table,
		Columns:     ld.Columns,
		FieldsInfo:  ld.FieldsInfo,
		LinesInfo:   ld.LinesInfo,
		IgnoreLines: ld.IgnoreLines,
	}
	tableInfo := p.Table.TableInfo
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 488)
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(db, tableInfo.Name.O)
	}
	trace_util_0.Count(_planbuilder_00000, 486)
	schema := expression.TableInfo2Schema(b.ctx, tableInfo)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(schema)

	var err error
	p.GenCols, err = b.resolveGeneratedColumns(tableInPlan.Cols(), nil, mockTablePlan)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 489)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 487)
	return p, nil
}

func (b *PlanBuilder) buildLoadStats(ld *ast.LoadStatsStmt) Plan {
	trace_util_0.Count(_planbuilder_00000, 490)
	p := &LoadStats{Path: ld.Path}
	return p
}

const maxSplitRegionNum = 1000

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 491)
	if len(node.IndexName.L) != 0 {
		trace_util_0.Count(_planbuilder_00000, 493)
		return b.buildSplitIndexRegion(node)
	}
	trace_util_0.Count(_planbuilder_00000, 492)
	return b.buildSplitTableRegion(node)
}

func (b *PlanBuilder) buildSplitIndexRegion(node *ast.SplitRegionStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 494)
	tblInfo := node.Table.TableInfo
	indexInfo := tblInfo.FindIndexByName(node.IndexName.L)
	if indexInfo == nil {
		trace_util_0.Count(_planbuilder_00000, 501)
		return nil, ErrKeyDoesNotExist.GenWithStackByArgs(node.IndexName, tblInfo.Name)
	}
	trace_util_0.Count(_planbuilder_00000, 495)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, node.Table.Schema, tblInfo)
	mockTablePlan.SetSchema(schema)

	p := &SplitRegion{
		TableInfo: tblInfo,
		IndexInfo: indexInfo,
	}
	// Split index regions by user specified value lists.
	if len(node.SplitOpt.ValueLists) > 0 {
		trace_util_0.Count(_planbuilder_00000, 502)
		indexValues := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			trace_util_0.Count(_planbuilder_00000, 504)
			if len(valuesItem) > len(indexInfo.Columns) {
				trace_util_0.Count(_planbuilder_00000, 507)
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			trace_util_0.Count(_planbuilder_00000, 505)
			values, err := b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 508)
				return nil, err
			}
			trace_util_0.Count(_planbuilder_00000, 506)
			indexValues = append(indexValues, values)
		}
		trace_util_0.Count(_planbuilder_00000, 503)
		p.ValueLists = indexValues
		return p, nil
	}

	// Split index regions by lower, upper value.
	trace_util_0.Count(_planbuilder_00000, 496)
	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) ([]types.Datum, error) {
		trace_util_0.Count(_planbuilder_00000, 509)
		if len(valuesItem) == 0 {
			trace_util_0.Count(_planbuilder_00000, 512)
			return nil, errors.Errorf("Split index `%v` region %s value count should more than 0", indexInfo.Name, name)
		}
		trace_util_0.Count(_planbuilder_00000, 510)
		if len(valuesItem) > len(indexInfo.Columns) {
			trace_util_0.Count(_planbuilder_00000, 513)
			return nil, errors.Errorf("Split index `%v` region column count doesn't match value count at %v", indexInfo.Name, name)
		}
		trace_util_0.Count(_planbuilder_00000, 511)
		return b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
	}
	trace_util_0.Count(_planbuilder_00000, 497)
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 514)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 498)
	upperValues, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 515)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 499)
	p.Lower = lowerValues
	p.Upper = upperValues

	if node.SplitOpt.Num > maxSplitRegionNum {
		trace_util_0.Count(_planbuilder_00000, 516)
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else {
		trace_util_0.Count(_planbuilder_00000, 517)
		if node.SplitOpt.Num < 1 {
			trace_util_0.Count(_planbuilder_00000, 518)
			return nil, errors.Errorf("Split index region num should more than 0")
		}
	}
	trace_util_0.Count(_planbuilder_00000, 500)
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) convertValue2ColumnType(valuesItem []ast.ExprNode, mockTablePlan LogicalPlan, indexInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]types.Datum, error) {
	trace_util_0.Count(_planbuilder_00000, 519)
	values := make([]types.Datum, 0, len(valuesItem))
	for j, valueItem := range valuesItem {
		trace_util_0.Count(_planbuilder_00000, 521)
		colOffset := indexInfo.Columns[j].Offset
		value, err := b.convertValue(valueItem, mockTablePlan, tblInfo.Columns[colOffset])
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 523)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 522)
		values = append(values, value)
	}
	trace_util_0.Count(_planbuilder_00000, 520)
	return values, nil
}

func (b *PlanBuilder) convertValue(valueItem ast.ExprNode, mockTablePlan LogicalPlan, col *model.ColumnInfo) (d types.Datum, err error) {
	trace_util_0.Count(_planbuilder_00000, 524)
	var expr expression.Expression
	switch x := valueItem.(type) {
	case *driver.ValueExpr:
		trace_util_0.Count(_planbuilder_00000, 529)
		expr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	default:
		trace_util_0.Count(_planbuilder_00000, 530)
		expr, _, err = b.rewrite(valueItem, mockTablePlan, nil, true)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 531)
			return d, err
		}
	}
	trace_util_0.Count(_planbuilder_00000, 525)
	constant, ok := expr.(*expression.Constant)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 532)
		return d, errors.New("Expect constant values")
	}
	trace_util_0.Count(_planbuilder_00000, 526)
	value, err := constant.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 533)
		return d, err
	}
	trace_util_0.Count(_planbuilder_00000, 527)
	d, err = value.ConvertTo(b.ctx.GetSessionVars().StmtCtx, &col.FieldType)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 534)
		if !types.ErrTruncated.Equal(err) {
			trace_util_0.Count(_planbuilder_00000, 537)
			return d, err
		}
		trace_util_0.Count(_planbuilder_00000, 535)
		valStr, err1 := value.ToString()
		if err1 != nil {
			trace_util_0.Count(_planbuilder_00000, 538)
			return d, err
		}
		trace_util_0.Count(_planbuilder_00000, 536)
		return d, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
	}
	trace_util_0.Count(_planbuilder_00000, 528)
	return d, nil
}

func (b *PlanBuilder) buildSplitTableRegion(node *ast.SplitRegionStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 539)
	tblInfo := node.Table.TableInfo
	var pkCol *model.ColumnInfo
	if tblInfo.PKIsHandle {
		trace_util_0.Count(_planbuilder_00000, 547)
		if col := tblInfo.GetPkColInfo(); col != nil {
			trace_util_0.Count(_planbuilder_00000, 548)
			pkCol = col
		}
	}
	trace_util_0.Count(_planbuilder_00000, 540)
	if pkCol == nil {
		trace_util_0.Count(_planbuilder_00000, 549)
		pkCol = model.NewExtraHandleColInfo()
	}
	trace_util_0.Count(_planbuilder_00000, 541)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, node.Table.Schema, tblInfo)
	mockTablePlan.SetSchema(schema)

	p := &SplitRegion{
		TableInfo: tblInfo,
	}
	if len(node.SplitOpt.ValueLists) > 0 {
		trace_util_0.Count(_planbuilder_00000, 550)
		values := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			trace_util_0.Count(_planbuilder_00000, 552)
			if len(valuesItem) > 1 {
				trace_util_0.Count(_planbuilder_00000, 555)
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			trace_util_0.Count(_planbuilder_00000, 553)
			value, err := b.convertValue(valuesItem[0], mockTablePlan, pkCol)
			if err != nil {
				trace_util_0.Count(_planbuilder_00000, 556)
				return nil, err
			}
			trace_util_0.Count(_planbuilder_00000, 554)
			values = append(values, []types.Datum{value})
		}
		trace_util_0.Count(_planbuilder_00000, 551)
		p.ValueLists = values
		return p, nil
	}

	trace_util_0.Count(_planbuilder_00000, 542)
	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) (types.Datum, error) {
		trace_util_0.Count(_planbuilder_00000, 557)
		if len(valuesItem) != 1 {
			trace_util_0.Count(_planbuilder_00000, 559)
			return types.Datum{}, errors.Errorf("Split table region %s value count should be 1", name)
		}
		trace_util_0.Count(_planbuilder_00000, 558)
		return b.convertValue(valuesItem[0], mockTablePlan, pkCol)
	}
	trace_util_0.Count(_planbuilder_00000, 543)
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 560)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 544)
	upperValue, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 561)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 545)
	p.Lower = []types.Datum{lowerValues}
	p.Upper = []types.Datum{upperValue}

	if node.SplitOpt.Num > maxSplitRegionNum {
		trace_util_0.Count(_planbuilder_00000, 562)
		return nil, errors.Errorf("Split table region num exceeded the limit %v", maxSplitRegionNum)
	} else {
		trace_util_0.Count(_planbuilder_00000, 563)
		if node.SplitOpt.Num < 1 {
			trace_util_0.Count(_planbuilder_00000, 564)
			return nil, errors.Errorf("Split table region num should more than 0")
		}
	}
	trace_util_0.Count(_planbuilder_00000, 546)
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) buildDDL(node ast.DDLNode) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 565)
	var authErr error
	switch v := node.(type) {
	case *ast.AlterDatabaseStmt:
		trace_util_0.Count(_planbuilder_00000, 567)
		if v.AlterDefaultDatabase {
			trace_util_0.Count(_planbuilder_00000, 598)
			v.Name = b.ctx.GetSessionVars().CurrentDB
		}
		trace_util_0.Count(_planbuilder_00000, 568)
		if v.Name == "" {
			trace_util_0.Count(_planbuilder_00000, 599)
			return nil, ErrNoDB
		}
		trace_util_0.Count(_planbuilder_00000, 569)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 600)
			authErr = ErrDBaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Name)
		}
		trace_util_0.Count(_planbuilder_00000, 570)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name, "", "", authErr)
	case *ast.AlterTableStmt:
		trace_util_0.Count(_planbuilder_00000, 571)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 601)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 572)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		for _, spec := range v.Specs {
			trace_util_0.Count(_planbuilder_00000, 602)
			if spec.Tp == ast.AlterTableRenameTable {
				trace_util_0.Count(_planbuilder_00000, 603)
				if b.ctx.GetSessionVars().User != nil {
					trace_util_0.Count(_planbuilder_00000, 607)
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
				}
				trace_util_0.Count(_planbuilder_00000, 604)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
					v.Table.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					trace_util_0.Count(_planbuilder_00000, 608)
					authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, spec.NewTable.Name.L)
				}
				trace_util_0.Count(_planbuilder_00000, 605)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, spec.NewTable.Schema.L,
					spec.NewTable.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					trace_util_0.Count(_planbuilder_00000, 609)
					authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, spec.NewTable.Name.L)
				}
				trace_util_0.Count(_planbuilder_00000, 606)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, spec.NewTable.Schema.L,
					spec.NewTable.Name.L, "", authErr)
			} else {
				trace_util_0.Count(_planbuilder_00000, 610)
				if spec.Tp == ast.AlterTableDropPartition {
					trace_util_0.Count(_planbuilder_00000, 611)
					if b.ctx.GetSessionVars().User != nil {
						trace_util_0.Count(_planbuilder_00000, 613)
						authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
							b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
					}
					trace_util_0.Count(_planbuilder_00000, 612)
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
						v.Table.Name.L, "", authErr)
				}
			}
		}
	case *ast.CreateDatabaseStmt:
		trace_util_0.Count(_planbuilder_00000, 573)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 614)
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		trace_util_0.Count(_planbuilder_00000, 574)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name,
			"", "", authErr)
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_planbuilder_00000, 575)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 615)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 576)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.CreateTableStmt:
		trace_util_0.Count(_planbuilder_00000, 577)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 616)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 578)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		if v.ReferTable != nil {
			trace_util_0.Count(_planbuilder_00000, 617)
			if b.ctx.GetSessionVars().User != nil {
				trace_util_0.Count(_planbuilder_00000, 619)
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, v.ReferTable.Name.L)
			}
			trace_util_0.Count(_planbuilder_00000, 618)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.ReferTable.Schema.L,
				v.ReferTable.Name.L, "", authErr)
		}
	case *ast.CreateViewStmt:
		trace_util_0.Count(_planbuilder_00000, 579)
		plan, err := b.Build(v.Select)
		if err != nil {
			trace_util_0.Count(_planbuilder_00000, 620)
			return nil, err
		}
		trace_util_0.Count(_planbuilder_00000, 580)
		schema := plan.Schema()
		if v.Cols != nil && len(v.Cols) != schema.Len() {
			trace_util_0.Count(_planbuilder_00000, 621)
			return nil, ddl.ErrViewWrongList
		}
		trace_util_0.Count(_planbuilder_00000, 581)
		v.SchemaCols = make([]model.CIStr, schema.Len())
		for i, col := range schema.Columns {
			trace_util_0.Count(_planbuilder_00000, 622)
			v.SchemaCols[i] = col.ColName
		}
		trace_util_0.Count(_planbuilder_00000, 582)
		if _, ok := plan.(LogicalPlan); ok {
			trace_util_0.Count(_planbuilder_00000, 623)
			if b.ctx.GetSessionVars().User != nil {
				trace_util_0.Count(_planbuilder_00000, 625)
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE VIEW", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, v.ViewName.Name.L)
			}
			trace_util_0.Count(_planbuilder_00000, 624)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateViewPriv, v.ViewName.Schema.L,
				v.ViewName.Name.L, "", authErr)
		}
		trace_util_0.Count(_planbuilder_00000, 583)
		if v.Definer.CurrentUser && b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 626)
			v.Definer = b.ctx.GetSessionVars().User
		}
		trace_util_0.Count(_planbuilder_00000, 584)
		if b.ctx.GetSessionVars().User != nil && v.Definer.String() != b.ctx.GetSessionVars().User.String() {
			trace_util_0.Count(_planbuilder_00000, 627)
			err = ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "",
				"", "", err)
		}
	case *ast.DropDatabaseStmt:
		trace_util_0.Count(_planbuilder_00000, 585)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 628)
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		trace_util_0.Count(_planbuilder_00000, 586)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Name,
			"", "", authErr)
	case *ast.DropIndexStmt:
		trace_util_0.Count(_planbuilder_00000, 587)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 629)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEx", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 588)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.DropTableStmt:
		trace_util_0.Count(_planbuilder_00000, 589)
		for _, tableVal := range v.Tables {
			trace_util_0.Count(_planbuilder_00000, 630)
			if b.ctx.GetSessionVars().User != nil {
				trace_util_0.Count(_planbuilder_00000, 632)
				authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, tableVal.Name.L)
			}
			trace_util_0.Count(_planbuilder_00000, 631)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, tableVal.Schema.L,
				tableVal.Name.L, "", authErr)
		}
	case *ast.TruncateTableStmt:
		trace_util_0.Count(_planbuilder_00000, 590)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 633)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 591)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.RenameTableStmt:
		trace_util_0.Count(_planbuilder_00000, 592)
		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 634)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.OldTable.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 593)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.OldTable.Schema.L,
			v.OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 635)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.OldTable.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 594)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.OldTable.Schema.L,
			v.OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 636)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.NewTable.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 595)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.NewTable.Schema.L,
			v.NewTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			trace_util_0.Count(_planbuilder_00000, 637)
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.NewTable.Name.L)
		}
		trace_util_0.Count(_planbuilder_00000, 596)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, v.NewTable.Schema.L,
			v.NewTable.Name.L, "", authErr)
	case *ast.RecoverTableStmt:
		trace_util_0.Count(_planbuilder_00000, 597)
		// Recover table command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	}
	trace_util_0.Count(_planbuilder_00000, 566)
	p := &DDL{Statement: node}
	return p, nil
}

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 638)
	if _, ok := trace.Stmt.(*ast.SelectStmt); !ok && trace.Format == "row" {
		trace_util_0.Count(_planbuilder_00000, 641)
		return nil, errors.New("trace only supports select query when format is row")
	}

	trace_util_0.Count(_planbuilder_00000, 639)
	p := &Trace{StmtNode: trace.Stmt, Format: trace.Format}

	switch trace.Format {
	case "row":
		trace_util_0.Count(_planbuilder_00000, 642)
		retFields := []string{"operation", "duration", "spanID"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		schema.Append(buildColumn("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumn("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumn("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema)
	case "json":
		trace_util_0.Count(_planbuilder_00000, 643)
		retFields := []string{"json"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		schema.Append(buildColumn("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema)
	default:
		trace_util_0.Count(_planbuilder_00000, 644)
		return nil, errors.New("trace format should be one of 'row' or 'json'")
	}
	trace_util_0.Count(_planbuilder_00000, 640)
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan Plan, format string, analyze bool, execStmt ast.StmtNode) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 645)
	pp, ok := targetPlan.(PhysicalPlan)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 648)
		switch x := targetPlan.(type) {
		case *Delete:
			trace_util_0.Count(_planbuilder_00000, 650)
			pp = x.SelectPlan
		case *Update:
			trace_util_0.Count(_planbuilder_00000, 651)
			pp = x.SelectPlan
		case *Insert:
			trace_util_0.Count(_planbuilder_00000, 652)
			if x.SelectPlan != nil {
				trace_util_0.Count(_planbuilder_00000, 653)
				pp = x.SelectPlan
			}
		}
		trace_util_0.Count(_planbuilder_00000, 649)
		if pp == nil {
			trace_util_0.Count(_planbuilder_00000, 654)
			return nil, ErrUnsupportedType.GenWithStackByArgs(targetPlan)
		}
	}

	trace_util_0.Count(_planbuilder_00000, 646)
	p := &Explain{StmtPlan: pp, Analyze: analyze, Format: format, ExecStmt: execStmt, ExecPlan: targetPlan}
	p.ctx = b.ctx
	err := p.prepareSchema()
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 655)
		return nil, err
	}
	trace_util_0.Count(_planbuilder_00000, 647)
	return p, nil
}

// buildExplainFor gets *last* (maybe running or finished) query plan from connection #connection id.
// See https://dev.mysql.com/doc/refman/8.0/en/explain-for-connection.html.
func (b *PlanBuilder) buildExplainFor(explainFor *ast.ExplainForStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 656)
	processInfo, ok := b.ctx.GetSessionManager().GetProcessInfo(explainFor.ConnectionID)
	if !ok {
		trace_util_0.Count(_planbuilder_00000, 660)
		return nil, ErrNoSuchThread.GenWithStackByArgs(explainFor.ConnectionID)
	}
	trace_util_0.Count(_planbuilder_00000, 657)
	if b.ctx.GetSessionVars() != nil && b.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_planbuilder_00000, 661)
		if b.ctx.GetSessionVars().User.Username != processInfo.User {
			trace_util_0.Count(_planbuilder_00000, 662)
			err := ErrAccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username, b.ctx.GetSessionVars().User.Hostname)
			// Different from MySQL's behavior and document.
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
	}

	trace_util_0.Count(_planbuilder_00000, 658)
	targetPlan, ok := processInfo.Plan.(Plan)
	if !ok || targetPlan == nil {
		trace_util_0.Count(_planbuilder_00000, 663)
		return &Explain{Format: explainFor.Format}, nil
	}

	trace_util_0.Count(_planbuilder_00000, 659)
	return b.buildExplainPlan(targetPlan, explainFor.Format, false, nil)
}

func (b *PlanBuilder) buildExplain(explain *ast.ExplainStmt) (Plan, error) {
	trace_util_0.Count(_planbuilder_00000, 664)
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		trace_util_0.Count(_planbuilder_00000, 667)
		return b.buildShow(show)
	}
	trace_util_0.Count(_planbuilder_00000, 665)
	targetPlan, err := OptimizeAstNode(b.ctx, explain.Stmt, b.is)
	if err != nil {
		trace_util_0.Count(_planbuilder_00000, 668)
		return nil, err
	}

	trace_util_0.Count(_planbuilder_00000, 666)
	return b.buildExplainPlan(targetPlan, explain.Format, explain.Analyze, explain.Stmt)
}

func buildShowProcedureSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 669)
	tblName := "ROUTINES"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowTriggerSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 670)
	tblName := "TRIGGERS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowEventsSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 671)
	tblName := "EVENTS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 15)...)
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowWarningsSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 672)
	tblName := "WARNINGS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 3)...)
	schema.Append(buildColumn(tblName, "Level", mysql.TypeVarchar, 64))
	schema.Append(buildColumn(tblName, "Code", mysql.TypeLong, 19))
	schema.Append(buildColumn(tblName, "Message", mysql.TypeVarchar, 64))
	return schema
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool) (schema *expression.Schema) {
	trace_util_0.Count(_planbuilder_00000, 673)
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowEngines:
		trace_util_0.Count(_planbuilder_00000, 676)
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowDatabases:
		trace_util_0.Count(_planbuilder_00000, 677)
		names = []string{"Database"}
	case ast.ShowOpenTables:
		trace_util_0.Count(_planbuilder_00000, 678)
		names = []string{"Database", "Table", "In_use", "Name_locked"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeLong}
	case ast.ShowTables:
		trace_util_0.Count(_planbuilder_00000, 679)
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			trace_util_0.Count(_planbuilder_00000, 705)
			names = append(names, "Table_type")
		}
	case ast.ShowTableStatus:
		trace_util_0.Count(_planbuilder_00000, 680)
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowColumns:
		trace_util_0.Count(_planbuilder_00000, 681)
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowWarnings, ast.ShowErrors:
		trace_util_0.Count(_planbuilder_00000, 682)
		names = []string{"Level", "Code", "Message"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar}
	case ast.ShowCharset:
		trace_util_0.Count(_planbuilder_00000, 683)
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		trace_util_0.Count(_planbuilder_00000, 684)
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		trace_util_0.Count(_planbuilder_00000, 685)
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCreateTable:
		trace_util_0.Count(_planbuilder_00000, 686)
		if !isView {
			trace_util_0.Count(_planbuilder_00000, 706)
			names = []string{"Table", "Create Table"}
		} else {
			trace_util_0.Count(_planbuilder_00000, 707)
			{
				names = []string{"View", "Create View", "character_set_client", "collation_connection"}
			}
		}
	case ast.ShowCreateUser:
		trace_util_0.Count(_planbuilder_00000, 687)
		if s.User != nil {
			trace_util_0.Count(_planbuilder_00000, 708)
			names = []string{fmt.Sprintf("CREATE USER for %s", s.User)}
		}
	case ast.ShowCreateView:
		trace_util_0.Count(_planbuilder_00000, 688)
		names = []string{"View", "Create View", "character_set_client", "collation_connection"}
	case ast.ShowCreateDatabase:
		trace_util_0.Count(_planbuilder_00000, 689)
		names = []string{"Database", "Create Database"}
	case ast.ShowDrainerStatus:
		trace_util_0.Count(_planbuilder_00000, 690)
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowGrants:
		trace_util_0.Count(_planbuilder_00000, 691)
		if s.User != nil {
			trace_util_0.Count(_planbuilder_00000, 709)
			names = []string{fmt.Sprintf("Grants for %s", s.User)}
		} else {
			trace_util_0.Count(_planbuilder_00000, 710)
			{
				// Don't know the name yet, so just say "user"
				names = []string{"Grants for User"}
			}
		}
	case ast.ShowIndex:
		trace_util_0.Count(_planbuilder_00000, 692)
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPlugins:
		trace_util_0.Count(_planbuilder_00000, 693)
		names = []string{"Name", "Status", "Type", "Library", "License", "Version"}
		ftypes = []byte{
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
		}
	case ast.ShowProcessList:
		trace_util_0.Count(_planbuilder_00000, 694)
		names = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	case ast.ShowPumpStatus:
		trace_util_0.Count(_planbuilder_00000, 695)
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowStatsMeta:
		trace_util_0.Count(_planbuilder_00000, 696)
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		trace_util_0.Count(_planbuilder_00000, 697)
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble}
	case ast.ShowStatsBuckets:
		trace_util_0.Count(_planbuilder_00000, 698)
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowStatsHealthy:
		trace_util_0.Count(_planbuilder_00000, 699)
		names = []string{"Db_name", "Table_name", "Partition_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowProfiles:
		trace_util_0.Count(_planbuilder_00000, 700) // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{mysql.TypeLong, mysql.TypeDouble, mysql.TypeVarchar}
	case ast.ShowMasterStatus:
		trace_util_0.Count(_planbuilder_00000, 701)
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPrivileges:
		trace_util_0.Count(_planbuilder_00000, 702)
		names = []string{"Privilege", "Context", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindings:
		trace_util_0.Count(_planbuilder_00000, 703)
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		trace_util_0.Count(_planbuilder_00000, 704)
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time", "State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeDatetime, mysql.TypeVarchar}
	}

	trace_util_0.Count(_planbuilder_00000, 674)
	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	for i := range names {
		trace_util_0.Count(_planbuilder_00000, 711)
		col := &expression.Column{
			ColName: model.NewCIStr(names[i]),
		}
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			trace_util_0.Count(_planbuilder_00000, 713)
			tp = ftypes[i]
		}
		trace_util_0.Count(_planbuilder_00000, 712)
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schema.Append(col)
	}
	trace_util_0.Count(_planbuilder_00000, 675)
	return schema
}

func buildChecksumTableSchema() *expression.Schema {
	trace_util_0.Count(_planbuilder_00000, 714)
	schema := expression.NewSchema(make([]*expression.Column, 0, 5)...)
	schema.Append(buildColumn("", "Db_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn("", "Table_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn("", "Checksum_crc64_xor", mysql.TypeLonglong, 22))
	schema.Append(buildColumn("", "Total_kvs", mysql.TypeLonglong, 22))
	schema.Append(buildColumn("", "Total_bytes", mysql.TypeLonglong, 22))
	return schema
}

var _planbuilder_00000 = "planner/core/planbuilder.go"
