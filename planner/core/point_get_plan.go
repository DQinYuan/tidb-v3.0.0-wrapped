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

package core

import (
	"bytes"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tipb/go-tipb"
)

// PointGetPlan is a fast plan for simple point get.
// When we detect that the statement has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoid the optimization and coprocessor cost.
type PointGetPlan struct {
	basePlan
	schema           *expression.Schema
	TblInfo          *model.TableInfo
	IndexInfo        *model.IndexInfo
	Handle           int64
	HandleParam      *driver.ParamMarkerExpr
	UnsignedHandle   bool
	IndexValues      []types.Datum
	IndexValueParams []*driver.ParamMarkerExpr
	expr             expression.Expression
	ctx              sessionctx.Context
	IsTableDual      bool
}

type nameValuePair struct {
	colName string
	value   types.Datum
	param   *driver.ParamMarkerExpr
}

// Schema implements the Plan interface.
func (p *PointGetPlan) Schema() *expression.Schema {
	trace_util_0.Count(_point_get_plan_00000, 0)
	return p.schema
}

// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *PointGetPlan) attach2Task(...task) task {
	trace_util_0.Count(_point_get_plan_00000, 1)
	return nil
}

// ToPB converts physical plan to tipb executor.
func (p *PointGetPlan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_point_get_plan_00000, 2)
	return nil, nil
}

// ExplainInfo returns operator information to be explained.
func (p *PointGetPlan) ExplainInfo() string {
	trace_util_0.Count(_point_get_plan_00000, 3)
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.IndexInfo != nil {
		trace_util_0.Count(_point_get_plan_00000, 5)
		fmt.Fprintf(buffer, ", index:")
		for i, col := range p.IndexInfo.Columns {
			trace_util_0.Count(_point_get_plan_00000, 6)
			buffer.WriteString(col.Name.O)
			if i < len(p.IndexInfo.Columns)-1 {
				trace_util_0.Count(_point_get_plan_00000, 7)
				buffer.WriteString(" ")
			}
		}
	} else {
		trace_util_0.Count(_point_get_plan_00000, 8)
		{
			if p.UnsignedHandle {
				trace_util_0.Count(_point_get_plan_00000, 9)
				fmt.Fprintf(buffer, ", handle:%d", uint64(p.Handle))
			} else {
				trace_util_0.Count(_point_get_plan_00000, 10)
				{
					fmt.Fprintf(buffer, ", handle:%d", p.Handle)
				}
			}
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 4)
	return buffer.String()
}

// GetChildReqProps gets the required property by child index.
func (p *PointGetPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	trace_util_0.Count(_point_get_plan_00000, 11)
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) StatsCount() float64 {
	trace_util_0.Count(_point_get_plan_00000, 12)
	return 1
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetPlan) statsInfo() *property.StatsInfo {
	trace_util_0.Count(_point_get_plan_00000, 13)
	if p.stats == nil {
		trace_util_0.Count(_point_get_plan_00000, 15)
		p.stats = &property.StatsInfo{}
	}
	trace_util_0.Count(_point_get_plan_00000, 14)
	p.stats.RowCount = 1
	return p.stats
}

// Children gets all the children.
func (p *PointGetPlan) Children() []PhysicalPlan {
	trace_util_0.Count(_point_get_plan_00000, 16)
	return nil
}

// SetChildren sets the children for the plan.
func (p *PointGetPlan) SetChildren(...PhysicalPlan) { trace_util_0.Count(_point_get_plan_00000, 17) }

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *PointGetPlan) ResolveIndices() error {
	trace_util_0.Count(_point_get_plan_00000, 18)
	return nil
}

// TryFastPlan tries to use the PointGetPlan for the query.
func TryFastPlan(ctx sessionctx.Context, node ast.Node) Plan {
	trace_util_0.Count(_point_get_plan_00000, 19)
	switch x := node.(type) {
	case *ast.SelectStmt:
		trace_util_0.Count(_point_get_plan_00000, 21)
		fp := tryPointGetPlan(ctx, x)
		if fp != nil {
			trace_util_0.Count(_point_get_plan_00000, 24)
			if checkFastPlanPrivilege(ctx, fp, mysql.SelectPriv) != nil {
				trace_util_0.Count(_point_get_plan_00000, 27)
				return nil
			}
			trace_util_0.Count(_point_get_plan_00000, 25)
			if fp.IsTableDual {
				trace_util_0.Count(_point_get_plan_00000, 28)
				tableDual := PhysicalTableDual{}
				tableDual.SetSchema(fp.Schema())
				return tableDual.Init(ctx, &property.StatsInfo{})
			}
			trace_util_0.Count(_point_get_plan_00000, 26)
			return fp
		}
	case *ast.UpdateStmt:
		trace_util_0.Count(_point_get_plan_00000, 22)
		return tryUpdatePointPlan(ctx, x)
	case *ast.DeleteStmt:
		trace_util_0.Count(_point_get_plan_00000, 23)
		return tryDeletePointPlan(ctx, x)
	}
	trace_util_0.Count(_point_get_plan_00000, 20)
	return nil
}

// tryPointGetPlan determine if the SelectStmt can use a PointGetPlan.
// Returns nil if not applicable.
// To use the PointGetPlan the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single table select.
// 3. All the columns must be public and generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetPlan(ctx sessionctx.Context, selStmt *ast.SelectStmt) *PointGetPlan {
	trace_util_0.Count(_point_get_plan_00000, 29)
	if selStmt.Having != nil || selStmt.LockTp != ast.SelectLockNone {
		trace_util_0.Count(_point_get_plan_00000, 38)
		return nil
	} else {
		trace_util_0.Count(_point_get_plan_00000, 39)
		if selStmt.Limit != nil {
			trace_util_0.Count(_point_get_plan_00000, 40)
			count, offset, err := extractLimitCountOffset(ctx, selStmt.Limit)
			if err != nil || count == 0 || offset > 0 {
				trace_util_0.Count(_point_get_plan_00000, 41)
				return nil
			}
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 30)
	tblName := getSingleTableName(selStmt.From)
	if tblName == nil {
		trace_util_0.Count(_point_get_plan_00000, 42)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 31)
	tbl := tblName.TableInfo
	if tbl == nil {
		trace_util_0.Count(_point_get_plan_00000, 43)
		return nil
	}
	// Do not handle partitioned table.
	// Table partition implementation translates LogicalPlan from `DataSource` to
	// `Union -> DataSource` in the logical plan optimization pass, since PointGetPlan
	// bypass the logical plan optimization, it can't support partitioned table.
	trace_util_0.Count(_point_get_plan_00000, 32)
	if tbl.GetPartitionInfo() != nil {
		trace_util_0.Count(_point_get_plan_00000, 44)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 33)
	for _, col := range tbl.Columns {
		trace_util_0.Count(_point_get_plan_00000, 45)
		// Do not handle generated columns.
		if col.IsGenerated() {
			trace_util_0.Count(_point_get_plan_00000, 47)
			return nil
		}
		// Only handle tables that all columns are public.
		trace_util_0.Count(_point_get_plan_00000, 46)
		if col.State != model.StatePublic {
			trace_util_0.Count(_point_get_plan_00000, 48)
			return nil
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 34)
	pairs := make([]nameValuePair, 0, 4)
	pairs = getNameValuePairs(pairs, selStmt.Where)
	if pairs == nil {
		trace_util_0.Count(_point_get_plan_00000, 49)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 35)
	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.Kind() != types.KindNull && len(pairs) == 1 {
		trace_util_0.Count(_point_get_plan_00000, 50)
		schema := buildSchemaFromFields(ctx, tblName.Schema, tbl, selStmt.Fields.Fields)
		if schema == nil {
			trace_util_0.Count(_point_get_plan_00000, 54)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 51)
		p := newPointGetPlan(ctx, schema, tbl)
		intDatum, err := handlePair.value.ConvertTo(ctx.GetSessionVars().StmtCtx, fieldType)
		if err != nil {
			trace_util_0.Count(_point_get_plan_00000, 55)
			if terror.ErrorEqual(types.ErrOverflow, err) {
				trace_util_0.Count(_point_get_plan_00000, 57)
				p.IsTableDual = true
				return p
			}
			trace_util_0.Count(_point_get_plan_00000, 56)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 52)
		cmp, err := intDatum.CompareDatum(ctx.GetSessionVars().StmtCtx, &handlePair.value)
		if err != nil {
			trace_util_0.Count(_point_get_plan_00000, 58)
			return nil
		} else {
			trace_util_0.Count(_point_get_plan_00000, 59)
			if cmp != 0 {
				trace_util_0.Count(_point_get_plan_00000, 60)
				p.IsTableDual = true
				return p
			}
		}
		trace_util_0.Count(_point_get_plan_00000, 53)
		p.Handle = intDatum.GetInt64()
		p.UnsignedHandle = mysql.HasUnsignedFlag(fieldType.Flag)
		p.HandleParam = handlePair.param
		return p
	}

	trace_util_0.Count(_point_get_plan_00000, 36)
	for _, idxInfo := range tbl.Indices {
		trace_util_0.Count(_point_get_plan_00000, 61)
		if !idxInfo.Unique {
			trace_util_0.Count(_point_get_plan_00000, 66)
			continue
		}
		trace_util_0.Count(_point_get_plan_00000, 62)
		if idxInfo.State != model.StatePublic {
			trace_util_0.Count(_point_get_plan_00000, 67)
			continue
		}
		trace_util_0.Count(_point_get_plan_00000, 63)
		idxValues, idxValueParams := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			trace_util_0.Count(_point_get_plan_00000, 68)
			continue
		}
		trace_util_0.Count(_point_get_plan_00000, 64)
		schema := buildSchemaFromFields(ctx, tblName.Schema, tbl, selStmt.Fields.Fields)
		if schema == nil {
			trace_util_0.Count(_point_get_plan_00000, 69)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 65)
		p := newPointGetPlan(ctx, schema, tbl)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexValueParams = idxValueParams
		return p
	}
	trace_util_0.Count(_point_get_plan_00000, 37)
	return nil
}

func newPointGetPlan(ctx sessionctx.Context, schema *expression.Schema, tbl *model.TableInfo) *PointGetPlan {
	trace_util_0.Count(_point_get_plan_00000, 70)
	p := &PointGetPlan{
		basePlan: newBasePlan(ctx, "Point_Get"),
		schema:   schema,
		TblInfo:  tbl,
	}
	return p
}

func checkFastPlanPrivilege(ctx sessionctx.Context, fastPlan *PointGetPlan, checkTypes ...mysql.PrivilegeType) error {
	trace_util_0.Count(_point_get_plan_00000, 71)
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		trace_util_0.Count(_point_get_plan_00000, 74)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 72)
	dbName := ctx.GetSessionVars().CurrentDB
	for _, checkType := range checkTypes {
		trace_util_0.Count(_point_get_plan_00000, 75)
		if !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, fastPlan.TblInfo.Name.L, "", checkType) {
			trace_util_0.Count(_point_get_plan_00000, 76)
			return errors.New("privilege check fail")
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 73)
	return nil
}

func buildSchemaFromFields(ctx sessionctx.Context, dbName model.CIStr, tbl *model.TableInfo, fields []*ast.SelectField) *expression.Schema {
	trace_util_0.Count(_point_get_plan_00000, 77)
	if dbName.L == "" {
		trace_util_0.Count(_point_get_plan_00000, 83)
		dbName = model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	}
	trace_util_0.Count(_point_get_plan_00000, 78)
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	if len(fields) == 1 && fields[0].WildCard != nil {
		trace_util_0.Count(_point_get_plan_00000, 84)
		for _, col := range tbl.Columns {
			trace_util_0.Count(_point_get_plan_00000, 86)
			columns = append(columns, colInfoToColumn(dbName, tbl.Name, col.Name, col, len(columns)))
		}
		trace_util_0.Count(_point_get_plan_00000, 85)
		return expression.NewSchema(columns...)
	}
	trace_util_0.Count(_point_get_plan_00000, 79)
	if len(fields) > 0 {
		trace_util_0.Count(_point_get_plan_00000, 87)
		for _, field := range fields {
			trace_util_0.Count(_point_get_plan_00000, 89)
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				trace_util_0.Count(_point_get_plan_00000, 93)
				return nil
			}
			trace_util_0.Count(_point_get_plan_00000, 90)
			col := findCol(tbl, colNameExpr.Name)
			if col == nil {
				trace_util_0.Count(_point_get_plan_00000, 94)
				return nil
			}
			trace_util_0.Count(_point_get_plan_00000, 91)
			asName := col.Name
			if field.AsName.L != "" {
				trace_util_0.Count(_point_get_plan_00000, 95)
				asName = field.AsName
			}
			trace_util_0.Count(_point_get_plan_00000, 92)
			columns = append(columns, colInfoToColumn(dbName, tbl.Name, asName, col, len(columns)))
		}
		trace_util_0.Count(_point_get_plan_00000, 88)
		return expression.NewSchema(columns...)
	}
	// fields len is 0 for update and delete.
	trace_util_0.Count(_point_get_plan_00000, 80)
	var handleCol *expression.Column
	for _, col := range tbl.Columns {
		trace_util_0.Count(_point_get_plan_00000, 96)
		column := colInfoToColumn(dbName, tbl.Name, col.Name, col, len(columns))
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_point_get_plan_00000, 98)
			handleCol = column
		}
		trace_util_0.Count(_point_get_plan_00000, 97)
		columns = append(columns, column)
	}
	trace_util_0.Count(_point_get_plan_00000, 81)
	if handleCol == nil {
		trace_util_0.Count(_point_get_plan_00000, 99)
		handleCol = colInfoToColumn(dbName, tbl.Name, model.ExtraHandleName, model.NewExtraHandleColInfo(), len(columns))
		columns = append(columns, handleCol)
	}
	trace_util_0.Count(_point_get_plan_00000, 82)
	schema := expression.NewSchema(columns...)
	schema.TblID2Handle = make(map[int64][]*expression.Column)
	schema.TblID2Handle[tbl.ID] = []*expression.Column{handleCol}
	return schema
}

func getSingleTableName(tableRefs *ast.TableRefsClause) *ast.TableName {
	trace_util_0.Count(_point_get_plan_00000, 100)
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		trace_util_0.Count(_point_get_plan_00000, 105)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 101)
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		trace_util_0.Count(_point_get_plan_00000, 106)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 102)
	if tblSrc.AsName.L != "" {
		trace_util_0.Count(_point_get_plan_00000, 107)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 103)
	tblName, ok := tblSrc.Source.(*ast.TableName)
	if !ok {
		trace_util_0.Count(_point_get_plan_00000, 108)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 104)
	return tblName
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(nvPairs []nameValuePair, expr ast.ExprNode) []nameValuePair {
	trace_util_0.Count(_point_get_plan_00000, 109)
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		trace_util_0.Count(_point_get_plan_00000, 112)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 110)
	if binOp.Op == opcode.LogicAnd {
		trace_util_0.Count(_point_get_plan_00000, 113)
		nvPairs = getNameValuePairs(nvPairs, binOp.L)
		if nvPairs == nil {
			trace_util_0.Count(_point_get_plan_00000, 116)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 114)
		nvPairs = getNameValuePairs(nvPairs, binOp.R)
		if nvPairs == nil {
			trace_util_0.Count(_point_get_plan_00000, 117)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 115)
		return nvPairs
	} else {
		trace_util_0.Count(_point_get_plan_00000, 118)
		if binOp.Op == opcode.EQ {
			trace_util_0.Count(_point_get_plan_00000, 119)
			var d types.Datum
			var colName *ast.ColumnNameExpr
			var param *driver.ParamMarkerExpr
			var ok bool
			if colName, ok = binOp.L.(*ast.ColumnNameExpr); ok {
				trace_util_0.Count(_point_get_plan_00000, 122)
				switch x := binOp.R.(type) {
				case *driver.ValueExpr:
					trace_util_0.Count(_point_get_plan_00000, 123)
					d = x.Datum
				case *driver.ParamMarkerExpr:
					trace_util_0.Count(_point_get_plan_00000, 124)
					d = x.Datum
					param = x
				}
			} else {
				trace_util_0.Count(_point_get_plan_00000, 125)
				if colName, ok = binOp.R.(*ast.ColumnNameExpr); ok {
					trace_util_0.Count(_point_get_plan_00000, 126)
					switch x := binOp.L.(type) {
					case *driver.ValueExpr:
						trace_util_0.Count(_point_get_plan_00000, 127)
						d = x.Datum
					case *driver.ParamMarkerExpr:
						trace_util_0.Count(_point_get_plan_00000, 128)
						d = x.Datum
						param = x
					}
				} else {
					trace_util_0.Count(_point_get_plan_00000, 129)
					{
						return nil
					}
				}
			}
			trace_util_0.Count(_point_get_plan_00000, 120)
			if d.IsNull() {
				trace_util_0.Count(_point_get_plan_00000, 130)
				return nil
			}
			trace_util_0.Count(_point_get_plan_00000, 121)
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param})
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 111)
	return nil
}

func findPKHandle(tblInfo *model.TableInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	trace_util_0.Count(_point_get_plan_00000, 131)
	if !tblInfo.PKIsHandle {
		trace_util_0.Count(_point_get_plan_00000, 134)
		return handlePair, nil
	}
	trace_util_0.Count(_point_get_plan_00000, 132)
	for _, col := range tblInfo.Columns {
		trace_util_0.Count(_point_get_plan_00000, 135)
		if mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_point_get_plan_00000, 136)
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				trace_util_0.Count(_point_get_plan_00000, 138)
				return handlePair, nil
			}
			trace_util_0.Count(_point_get_plan_00000, 137)
			return pairs[i], &col.FieldType
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 133)
	return handlePair, nil
}

func getIndexValues(idxInfo *model.IndexInfo, pairs []nameValuePair) ([]types.Datum, []*driver.ParamMarkerExpr) {
	trace_util_0.Count(_point_get_plan_00000, 139)
	idxValues := make([]types.Datum, 0, 4)
	idxValueParams := make([]*driver.ParamMarkerExpr, 0, 4)
	if len(idxInfo.Columns) != len(pairs) {
		trace_util_0.Count(_point_get_plan_00000, 144)
		return nil, nil
	}
	trace_util_0.Count(_point_get_plan_00000, 140)
	if idxInfo.HasPrefixIndex() {
		trace_util_0.Count(_point_get_plan_00000, 145)
		return nil, nil
	}
	trace_util_0.Count(_point_get_plan_00000, 141)
	for _, idxCol := range idxInfo.Columns {
		trace_util_0.Count(_point_get_plan_00000, 146)
		i := findInPairs(idxCol.Name.L, pairs)
		if i == -1 {
			trace_util_0.Count(_point_get_plan_00000, 148)
			return nil, nil
		}
		trace_util_0.Count(_point_get_plan_00000, 147)
		idxValues = append(idxValues, pairs[i].value)
		idxValueParams = append(idxValueParams, pairs[i].param)
	}
	trace_util_0.Count(_point_get_plan_00000, 142)
	if len(idxValues) > 0 {
		trace_util_0.Count(_point_get_plan_00000, 149)
		return idxValues, idxValueParams
	}
	trace_util_0.Count(_point_get_plan_00000, 143)
	return nil, nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	trace_util_0.Count(_point_get_plan_00000, 150)
	for i, pair := range pairs {
		trace_util_0.Count(_point_get_plan_00000, 152)
		if pair.colName == colName {
			trace_util_0.Count(_point_get_plan_00000, 153)
			return i
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 151)
	return -1
}

func tryUpdatePointPlan(ctx sessionctx.Context, updateStmt *ast.UpdateStmt) Plan {
	trace_util_0.Count(_point_get_plan_00000, 154)
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    updateStmt.TableRefs,
		Where:   updateStmt.Where,
		OrderBy: updateStmt.Order,
		Limit:   updateStmt.Limit,
	}
	fastSelect := tryPointGetPlan(ctx, selStmt)
	if fastSelect == nil {
		trace_util_0.Count(_point_get_plan_00000, 159)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 155)
	if checkFastPlanPrivilege(ctx, fastSelect, mysql.SelectPriv, mysql.UpdatePriv) != nil {
		trace_util_0.Count(_point_get_plan_00000, 160)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 156)
	if fastSelect.IsTableDual {
		trace_util_0.Count(_point_get_plan_00000, 161)
		return PhysicalTableDual{}.Init(ctx, &property.StatsInfo{})
	}
	trace_util_0.Count(_point_get_plan_00000, 157)
	orderedList := buildOrderedList(ctx, fastSelect, updateStmt.List)
	if orderedList == nil {
		trace_util_0.Count(_point_get_plan_00000, 162)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 158)
	updatePlan := Update{
		SelectPlan:  fastSelect,
		OrderedList: orderedList,
	}.Init(ctx)
	updatePlan.SetSchema(fastSelect.schema)
	return updatePlan
}

func buildOrderedList(ctx sessionctx.Context, fastSelect *PointGetPlan, list []*ast.Assignment) []*expression.Assignment {
	trace_util_0.Count(_point_get_plan_00000, 163)
	orderedList := make([]*expression.Assignment, 0, len(list))
	for _, assign := range list {
		trace_util_0.Count(_point_get_plan_00000, 165)
		col, err := fastSelect.schema.FindColumn(assign.Column)
		if err != nil {
			trace_util_0.Count(_point_get_plan_00000, 170)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 166)
		if col == nil {
			trace_util_0.Count(_point_get_plan_00000, 171)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 167)
		newAssign := &expression.Assignment{
			Col: col,
		}
		expr, err := expression.RewriteSimpleExprWithSchema(ctx, assign.Expr, fastSelect.schema)
		if err != nil {
			trace_util_0.Count(_point_get_plan_00000, 172)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 168)
		expr = expression.BuildCastFunction(ctx, expr, col.GetType())
		newAssign.Expr, err = expr.ResolveIndices(fastSelect.schema)
		if err != nil {
			trace_util_0.Count(_point_get_plan_00000, 173)
			return nil
		}
		trace_util_0.Count(_point_get_plan_00000, 169)
		orderedList = append(orderedList, newAssign)
	}
	trace_util_0.Count(_point_get_plan_00000, 164)
	return orderedList
}

func tryDeletePointPlan(ctx sessionctx.Context, delStmt *ast.DeleteStmt) Plan {
	trace_util_0.Count(_point_get_plan_00000, 174)
	if delStmt.IsMultiTable {
		trace_util_0.Count(_point_get_plan_00000, 179)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 175)
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delStmt.TableRefs,
		Where:   delStmt.Where,
		OrderBy: delStmt.Order,
		Limit:   delStmt.Limit,
	}
	fastSelect := tryPointGetPlan(ctx, selStmt)
	if fastSelect == nil {
		trace_util_0.Count(_point_get_plan_00000, 180)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 176)
	if checkFastPlanPrivilege(ctx, fastSelect, mysql.SelectPriv, mysql.DeletePriv) != nil {
		trace_util_0.Count(_point_get_plan_00000, 181)
		return nil
	}
	trace_util_0.Count(_point_get_plan_00000, 177)
	if fastSelect.IsTableDual {
		trace_util_0.Count(_point_get_plan_00000, 182)
		return PhysicalTableDual{}.Init(ctx, &property.StatsInfo{})
	}
	trace_util_0.Count(_point_get_plan_00000, 178)
	delPlan := Delete{
		SelectPlan: fastSelect,
	}.Init(ctx)
	delPlan.SetSchema(fastSelect.schema)
	return delPlan
}

func findCol(tbl *model.TableInfo, colName *ast.ColumnName) *model.ColumnInfo {
	trace_util_0.Count(_point_get_plan_00000, 183)
	for _, col := range tbl.Columns {
		trace_util_0.Count(_point_get_plan_00000, 185)
		if col.Name.L == colName.Name.L {
			trace_util_0.Count(_point_get_plan_00000, 186)
			return col
		}
	}
	trace_util_0.Count(_point_get_plan_00000, 184)
	return nil
}

func colInfoToColumn(db model.CIStr, tblName model.CIStr, asName model.CIStr, col *model.ColumnInfo, idx int) *expression.Column {
	trace_util_0.Count(_point_get_plan_00000, 187)
	return &expression.Column{
		ColName:     asName,
		OrigTblName: tblName,
		DBName:      db,
		TblName:     tblName,
		RetType:     &col.FieldType,
		ID:          col.ID,
		UniqueID:    int64(col.Offset),
		Index:       idx,
	}
}

var _point_get_plan_00000 = "planner/core/point_get_plan.go"
