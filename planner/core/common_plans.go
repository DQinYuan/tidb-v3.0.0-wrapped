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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/ranger"
)

var planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowDDLJobs is for showing DDL job list.
type ShowDDLJobs struct {
	baseSchemaProducer

	JobNumber int64
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	Tables []*ast.TableName

	GenExprs map[model.TableColumnID]expression.Expression
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CheckIndex is used for checking index data, built from the 'admin check index' statement.
type CheckIndex struct {
	baseSchemaProducer

	IndexLookUpReader *PhysicalIndexLookUpReader
	DBName            string
	IdxName           string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name      string
	UsingVars []expression.Expression
	ExecID    uint32
	Stmt      ast.StmtNode
	Plan      Plan
}

// OptimizePreparedPlan optimizes the prepared statement.
func (e *Execute) OptimizePreparedPlan(ctx sessionctx.Context, is infoschema.InfoSchema) error {
	trace_util_0.Count(_common_plans_00000, 0)
	vars := ctx.GetSessionVars()
	if e.Name != "" {
		trace_util_0.Count(_common_plans_00000, 7)
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	trace_util_0.Count(_common_plans_00000, 1)
	prepared, ok := vars.PreparedStmts[e.ExecID]
	if !ok {
		trace_util_0.Count(_common_plans_00000, 8)
		return errors.Trace(ErrStmtNotFound)
	}

	trace_util_0.Count(_common_plans_00000, 2)
	if len(prepared.Params) != len(e.UsingVars) {
		trace_util_0.Count(_common_plans_00000, 9)
		return errors.Trace(ErrWrongParamCount)
	}

	trace_util_0.Count(_common_plans_00000, 3)
	for i, usingVar := range e.UsingVars {
		trace_util_0.Count(_common_plans_00000, 10)
		val, err := usingVar.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_common_plans_00000, 12)
			return err
		}
		trace_util_0.Count(_common_plans_00000, 11)
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Datum = val
		param.InExecute = true
		vars.PreparedParams = append(vars.PreparedParams, val)
	}
	trace_util_0.Count(_common_plans_00000, 4)
	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		trace_util_0.Count(_common_plans_00000, 13)
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := Preprocess(ctx, prepared.Stmt, is, InPrepare)
		if err != nil {
			trace_util_0.Count(_common_plans_00000, 15)
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		trace_util_0.Count(_common_plans_00000, 14)
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	trace_util_0.Count(_common_plans_00000, 5)
	p, err := e.getPhysicalPlan(ctx, is, prepared)
	if err != nil {
		trace_util_0.Count(_common_plans_00000, 16)
		return err
	}
	trace_util_0.Count(_common_plans_00000, 6)
	e.Stmt = prepared.Stmt
	e.Plan = p
	return nil
}

func (e *Execute) getPhysicalPlan(ctx sessionctx.Context, is infoschema.InfoSchema, prepared *ast.Prepared) (Plan, error) {
	trace_util_0.Count(_common_plans_00000, 17)
	var cacheKey kvcache.Key
	sessionVars := ctx.GetSessionVars()
	sessionVars.StmtCtx.UseCache = prepared.UseCache
	if prepared.UseCache {
		trace_util_0.Count(_common_plans_00000, 21)
		cacheKey = NewPSTMTPlanCacheKey(sessionVars, e.ExecID, prepared.SchemaVersion)
		if cacheValue, exists := ctx.PreparedPlanCache().Get(cacheKey); exists {
			trace_util_0.Count(_common_plans_00000, 22)
			if metrics.ResettablePlanCacheCounterFortTest {
				trace_util_0.Count(_common_plans_00000, 25)
				metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
			} else {
				trace_util_0.Count(_common_plans_00000, 26)
				{
					planCacheCounter.Inc()
				}
			}
			trace_util_0.Count(_common_plans_00000, 23)
			plan := cacheValue.(*PSTMTPlanCacheValue).Plan
			err := e.rebuildRange(plan)
			if err != nil {
				trace_util_0.Count(_common_plans_00000, 27)
				return nil, err
			}
			trace_util_0.Count(_common_plans_00000, 24)
			return plan, nil
		}
	}
	trace_util_0.Count(_common_plans_00000, 18)
	p, err := OptimizeAstNode(ctx, prepared.Stmt, is)
	if err != nil {
		trace_util_0.Count(_common_plans_00000, 28)
		return nil, err
	}
	trace_util_0.Count(_common_plans_00000, 19)
	_, isTableDual := p.(*PhysicalTableDual)
	if !isTableDual && prepared.UseCache {
		trace_util_0.Count(_common_plans_00000, 29)
		ctx.PreparedPlanCache().Put(cacheKey, NewPSTMTPlanCacheValue(p))
	}
	trace_util_0.Count(_common_plans_00000, 20)
	return p, err
}

func (e *Execute) rebuildRange(p Plan) error {
	trace_util_0.Count(_common_plans_00000, 30)
	sctx := p.context()
	sc := p.context().GetSessionVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalTableReader:
		trace_util_0.Count(_common_plans_00000, 32)
		ts := x.TablePlans[0].(*PhysicalTableScan)
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			trace_util_0.Count(_common_plans_00000, 43)
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				trace_util_0.Count(_common_plans_00000, 44)
				pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			}
		}
		trace_util_0.Count(_common_plans_00000, 33)
		if pkCol != nil {
			trace_util_0.Count(_common_plans_00000, 45)
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sc, pkCol.RetType)
			if err != nil {
				trace_util_0.Count(_common_plans_00000, 46)
				return err
			}
		} else {
			trace_util_0.Count(_common_plans_00000, 47)
			{
				ts.Ranges = ranger.FullIntRange(false)
			}
		}
	case *PhysicalIndexReader:
		trace_util_0.Count(_common_plans_00000, 34)
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			trace_util_0.Count(_common_plans_00000, 48)
			return err
		}
	case *PhysicalIndexLookUpReader:
		trace_util_0.Count(_common_plans_00000, 35)
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			trace_util_0.Count(_common_plans_00000, 49)
			return err
		}
	case *PointGetPlan:
		trace_util_0.Count(_common_plans_00000, 36)
		if x.HandleParam != nil {
			trace_util_0.Count(_common_plans_00000, 50)
			x.Handle, err = x.HandleParam.Datum.ToInt64(sc)
			if err != nil {
				trace_util_0.Count(_common_plans_00000, 52)
				return err
			}
			trace_util_0.Count(_common_plans_00000, 51)
			return nil
		}
		trace_util_0.Count(_common_plans_00000, 37)
		for i, param := range x.IndexValueParams {
			trace_util_0.Count(_common_plans_00000, 53)
			if param != nil {
				trace_util_0.Count(_common_plans_00000, 54)
				x.IndexValues[i] = param.Datum
			}
		}
		trace_util_0.Count(_common_plans_00000, 38)
		return nil
	case PhysicalPlan:
		trace_util_0.Count(_common_plans_00000, 39)
		for _, child := range x.Children() {
			trace_util_0.Count(_common_plans_00000, 55)
			err = e.rebuildRange(child)
			if err != nil {
				trace_util_0.Count(_common_plans_00000, 56)
				return err
			}
		}
	case *Insert:
		trace_util_0.Count(_common_plans_00000, 40)
		if x.SelectPlan != nil {
			trace_util_0.Count(_common_plans_00000, 57)
			return e.rebuildRange(x.SelectPlan)
		}
	case *Update:
		trace_util_0.Count(_common_plans_00000, 41)
		if x.SelectPlan != nil {
			trace_util_0.Count(_common_plans_00000, 58)
			return e.rebuildRange(x.SelectPlan)
		}
	case *Delete:
		trace_util_0.Count(_common_plans_00000, 42)
		if x.SelectPlan != nil {
			trace_util_0.Count(_common_plans_00000, 59)
			return e.rebuildRange(x.SelectPlan)
		}
	}
	trace_util_0.Count(_common_plans_00000, 31)
	return nil
}

func (e *Execute) buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) ([]*ranger.Range, error) {
	trace_util_0.Count(_common_plans_00000, 60)
	idxCols, colLengths := expression.IndexInfo2Cols(is.schema.Columns, is.Index)
	if len(idxCols) == 0 {
		trace_util_0.Count(_common_plans_00000, 63)
		return ranger.FullRange(), nil
	}
	trace_util_0.Count(_common_plans_00000, 61)
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, idxCols, colLengths)
	if err != nil {
		trace_util_0.Count(_common_plans_00000, 64)
		return nil, err
	}
	trace_util_0.Count(_common_plans_00000, 62)
	return res.Ranges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Show represents a show plan.
type Show struct {
	baseSchemaProducer

	Tp          ast.ShowStmtType // Databases/Tables/Columns/....
	DBName      string
	Table       *ast.TableName  // Used for showing columns.
	Column      *ast.ColumnName // Used for `desc table column`.
	Flag        int             // Some flag parsed from sql, such as FULL.
	Full        bool
	User        *auth.UserIdentity   // Used for show grants.
	Roles       []*auth.RoleIdentity // Used for show grants.
	IfNotExists bool                 // Used for `show create database if not exists`

	Conditions []expression.Expression

	GlobalScope bool // Used by show variables
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SQLBindOpType repreents the SQL bind type
type SQLBindOpType int

const (
	// OpSQLBindCreate represents the operation to create a SQL bind.
	OpSQLBindCreate SQLBindOpType = iota
	// OpSQLBindDrop represents the operation to drop a SQL bind.
	OpSQLBindDrop
)

// SQLBindPlan represents a plan for SQL bind.
type SQLBindPlan struct {
	baseSchemaProducer

	SQLBindOp    SQLBindOpType
	NormdOrigSQL string
	BindSQL      string
	IsGlobal     bool
	BindStmt     ast.StmtNode
	Charset      string
	Collation    string
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Table       table.Table
	tableSchema *expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	SetList     []*expression.Assignment

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	GenCols InsertGeneratedColumns

	SelectPlan PhysicalPlan
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	SelectPlan PhysicalPlan
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	Tables       []*ast.TableName
	IsMultiTable bool

	SelectPlan PhysicalPlan
}

// analyzeInfo is used to store the database name, table name and partition name of analyze task.
type analyzeInfo struct {
	DBName        string
	TableName     string
	PartitionName string
	// PhysicalTableID is the id for a partition or a table.
	PhysicalTableID int64
	Incremental     bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	PKInfo   *model.ColumnInfo
	ColsInfo []*model.ColumnInfo
	TblInfo  *model.TableInfo
	analyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	analyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks      []AnalyzeColumnsTask
	IdxTasks      []AnalyzeIndexTask
	MaxNumBuckets uint64
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal     bool
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Table       *ast.TableName
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines uint64

	GenCols InsertGeneratedColumns
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	TableInfo  *model.TableInfo
	IndexInfo  *model.IndexInfo
	Lower      []types.Datum
	Upper      []types.Datum
	Num        int
	ValueLists [][]types.Datum
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	StmtPlan       Plan
	Rows           [][]string
	explainedPlans map[int]bool
	Format         string
	Analyze        bool
	ExecStmt       ast.StmtNode
	ExecPlan       Plan
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	trace_util_0.Count(_common_plans_00000, 65)
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		trace_util_0.Count(_common_plans_00000, 67)
		retFields := []string{"id", "count", "task", "operator info"}
		if e.Analyze {
			trace_util_0.Count(_common_plans_00000, 73)
			retFields = append(retFields, "execution info")
		}
		trace_util_0.Count(_common_plans_00000, 68)
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			trace_util_0.Count(_common_plans_00000, 74)
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		trace_util_0.Count(_common_plans_00000, 69)
		e.SetSchema(schema)
	case ast.ExplainFormatDOT:
		trace_util_0.Count(_common_plans_00000, 70)
		retFields := []string{"dot contents"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			trace_util_0.Count(_common_plans_00000, 75)
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		trace_util_0.Count(_common_plans_00000, 71)
		e.SetSchema(schema)
	default:
		trace_util_0.Count(_common_plans_00000, 72)
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	trace_util_0.Count(_common_plans_00000, 66)
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	trace_util_0.Count(_common_plans_00000, 76)
	if e.StmtPlan == nil {
		trace_util_0.Count(_common_plans_00000, 79)
		return nil
	}
	trace_util_0.Count(_common_plans_00000, 77)
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		trace_util_0.Count(_common_plans_00000, 80)
		e.explainedPlans = map[int]bool{}
		e.explainPlanInRowFormat(e.StmtPlan.(PhysicalPlan), "root", "", true)
	case ast.ExplainFormatDOT:
		trace_util_0.Count(_common_plans_00000, 81)
		e.prepareDotInfo(e.StmtPlan.(PhysicalPlan))
	default:
		trace_util_0.Count(_common_plans_00000, 82)
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	trace_util_0.Count(_common_plans_00000, 78)
	return nil
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p PhysicalPlan, taskType, indent string, isLastChild bool) {
	trace_util_0.Count(_common_plans_00000, 83)
	e.prepareOperatorInfo(p, taskType, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := e.getIndent4Child(indent, isLastChild)
	for i, child := range p.Children() {
		trace_util_0.Count(_common_plans_00000, 85)
		if e.explainedPlans[child.ID()] {
			trace_util_0.Count(_common_plans_00000, 87)
			continue
		}
		trace_util_0.Count(_common_plans_00000, 86)
		e.explainPlanInRowFormat(child.(PhysicalPlan), taskType, childIndent, i == len(p.Children())-1)
	}

	trace_util_0.Count(_common_plans_00000, 84)
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		trace_util_0.Count(_common_plans_00000, 88)
		e.explainPlanInRowFormat(copPlan.tablePlan, "cop", childIndent, true)
	case *PhysicalIndexReader:
		trace_util_0.Count(_common_plans_00000, 89)
		e.explainPlanInRowFormat(copPlan.indexPlan, "cop", childIndent, true)
	case *PhysicalIndexLookUpReader:
		trace_util_0.Count(_common_plans_00000, 90)
		e.explainPlanInRowFormat(copPlan.indexPlan, "cop", childIndent, false)
		e.explainPlanInRowFormat(copPlan.tablePlan, "cop", childIndent, true)
	}
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, task type, operator info, and the estemated row count.
func (e *Explain) prepareOperatorInfo(p PhysicalPlan, taskType string, indent string, isLastChild bool) {
	trace_util_0.Count(_common_plans_00000, 91)
	operatorInfo := p.ExplainInfo()
	count := string(strconv.AppendFloat([]byte{}, p.statsInfo().RowCount, 'f', 2, 64))
	explainID := p.ExplainID().String()
	row := []string{e.prettyIdentifier(explainID, indent, isLastChild), count, taskType, operatorInfo}
	if e.Analyze {
		trace_util_0.Count(_common_plans_00000, 93)
		runtimeStatsColl := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
		// So check copTaskExecDetail first and print the real cop task information if it's not empty.
		if runtimeStatsColl.ExistsCopStats(explainID) {
			trace_util_0.Count(_common_plans_00000, 94)
			row = append(row, runtimeStatsColl.GetCopStats(explainID).String())
		} else {
			trace_util_0.Count(_common_plans_00000, 95)
			if runtimeStatsColl.ExistsRootStats(explainID) {
				trace_util_0.Count(_common_plans_00000, 96)
				row = append(row, runtimeStatsColl.GetRootStats(explainID).String())
			} else {
				trace_util_0.Count(_common_plans_00000, 97)
				{
					row = append(row, "time:0ns, loops:0, rows:0")
				}
			}
		}
	}
	trace_util_0.Count(_common_plans_00000, 92)
	e.Rows = append(e.Rows, row)
}

const (
	// treeBody indicates the current operator sub-tree is not finished, still
	// has child operators to be attached on.
	treeBody = '│'
	// treeMiddleNode indicates this operator is not the last child of the
	// current sub-tree rooted by its parent.
	treeMiddleNode = '├'
	// treeLastNode indicates this operator is the last child of the current
	// sub-tree rooted by its parent.
	treeLastNode = '└'
	// treeGap is used to represent the gap between the branches of the tree.
	treeGap = ' '
	// treeNodeIdentifier is used to replace the treeGap once we need to attach
	// a node to a sub-tree.
	treeNodeIdentifier = '─'
)

func (e *Explain) prettyIdentifier(id, indent string, isLastChild bool) string {
	trace_util_0.Count(_common_plans_00000, 98)
	if len(indent) == 0 {
		trace_util_0.Count(_common_plans_00000, 101)
		return id
	}

	trace_util_0.Count(_common_plans_00000, 99)
	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		trace_util_0.Count(_common_plans_00000, 102)
		if indentBytes[i] != treeBody {
			trace_util_0.Count(_common_plans_00000, 105)
			continue
		}

		// Here we attach a new node to the current sub-tree by changing
		// the closest treeBody to a:
		// 1. treeLastNode, if this operator is the last child.
		// 2. treeMiddleNode, if this operator is not the last child..
		trace_util_0.Count(_common_plans_00000, 103)
		if isLastChild {
			trace_util_0.Count(_common_plans_00000, 106)
			indentBytes[i] = treeLastNode
		} else {
			trace_util_0.Count(_common_plans_00000, 107)
			{
				indentBytes[i] = treeMiddleNode
			}
		}
		trace_util_0.Count(_common_plans_00000, 104)
		break
	}

	// Replace the treeGap between the treeBody and the node to a
	// treeNodeIdentifier.
	trace_util_0.Count(_common_plans_00000, 100)
	indentBytes[len(indentBytes)-1] = treeNodeIdentifier
	return string(indentBytes) + id
}

func (e *Explain) getIndent4Child(indent string, isLastChild bool) string {
	trace_util_0.Count(_common_plans_00000, 108)
	if !isLastChild {
		trace_util_0.Count(_common_plans_00000, 111)
		return string(append([]rune(indent), treeBody, treeGap))
	}

	// If the current node is the last node of the current operator tree, we
	// need to end this sub-tree by changing the closest treeBody to a treeGap.
	trace_util_0.Count(_common_plans_00000, 109)
	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		trace_util_0.Count(_common_plans_00000, 112)
		if indentBytes[i] == treeBody {
			trace_util_0.Count(_common_plans_00000, 113)
			indentBytes[i] = treeGap
			break
		}
	}

	trace_util_0.Count(_common_plans_00000, 110)
	return string(append(indentBytes, treeBody, treeGap))
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	trace_util_0.Count(_common_plans_00000, 114)
	buffer := bytes.NewBufferString("")
	buffer.WriteString(fmt.Sprintf("\ndigraph %s {\n", p.ExplainID()))
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString(fmt.Sprintln("}"))

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	trace_util_0.Count(_common_plans_00000, 115)
	buffer.WriteString(fmt.Sprintf("subgraph cluster%v{\n", p.ID()))
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	buffer.WriteString(fmt.Sprintf("label = \"%s\"\n", taskTp))

	if len(p.Children()) == 0 {
		trace_util_0.Count(_common_plans_00000, 119)
		buffer.WriteString(fmt.Sprintf("\"%s\"\n}\n", p.ExplainID()))
		return
	}

	trace_util_0.Count(_common_plans_00000, 116)
	var copTasks []PhysicalPlan
	var pipelines []string

	for planQueue := []PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		trace_util_0.Count(_common_plans_00000, 120)
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			trace_util_0.Count(_common_plans_00000, 122)
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			trace_util_0.Count(_common_plans_00000, 123)
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			trace_util_0.Count(_common_plans_00000, 124)
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		}
		trace_util_0.Count(_common_plans_00000, 121)
		for _, child := range curPlan.Children() {
			trace_util_0.Count(_common_plans_00000, 125)
			buffer.WriteString(fmt.Sprintf("\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID()))
			planQueue = append(planQueue, child)
		}
	}
	trace_util_0.Count(_common_plans_00000, 117)
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		trace_util_0.Count(_common_plans_00000, 126)
		e.prepareTaskDot(cop.(PhysicalPlan), "cop", buffer)
	}

	trace_util_0.Count(_common_plans_00000, 118)
	for i := range pipelines {
		trace_util_0.Count(_common_plans_00000, 127)
		buffer.WriteString(pipelines[i])
	}
}

var _common_plans_00000 = "planner/core/common_plans.go"
