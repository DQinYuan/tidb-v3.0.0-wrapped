// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/sqlexec"
)

var etcdDialTimeout = 5 * time.Second

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp          ast.ShowStmtType // Databases/Tables/Columns/....
	DBName      model.CIStr
	Table       *ast.TableName  // Used for showing columns.
	Column      *ast.ColumnName // Used for `desc table column`.
	Flag        int             // Some flag parsed from sql, such as FULL.
	Full        bool
	User        *auth.UserIdentity   // Used for show grants.
	Roles       []*auth.RoleIdentity // Used for show grants.
	IfNotExists bool                 // Used for `show create database if not exists`

	// GlobalScope is used by show variables
	GlobalScope bool

	is infoschema.InfoSchema

	result *chunk.Chunk
	cursor int
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_show_00000, 0)
	req.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		trace_util_0.Count(_show_00000, 3)
		e.result = newFirstChunk(e)
		err := e.fetchAll()
		if err != nil {
			trace_util_0.Count(_show_00000, 5)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_00000, 4)
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := 0; colIdx < e.Schema().Len(); colIdx++ {
			trace_util_0.Count(_show_00000, 6)
			retType := e.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				trace_util_0.Count(_show_00000, 8)
				continue
			}
			trace_util_0.Count(_show_00000, 7)
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				trace_util_0.Count(_show_00000, 9)
				if valLen := len(row.GetString(colIdx)); retType.Flen < valLen {
					trace_util_0.Count(_show_00000, 10)
					retType.Flen = valLen
				}
			}
		}
	}
	trace_util_0.Count(_show_00000, 1)
	if e.cursor >= e.result.NumRows() {
		trace_util_0.Count(_show_00000, 11)
		return nil
	}
	trace_util_0.Count(_show_00000, 2)
	numCurBatch := mathutil.Min(req.Capacity(), e.result.NumRows()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) fetchAll() error {
	trace_util_0.Count(_show_00000, 12)
	switch e.Tp {
	case ast.ShowCharset:
		trace_util_0.Count(_show_00000, 14)
		return e.fetchShowCharset()
	case ast.ShowCollation:
		trace_util_0.Count(_show_00000, 15)
		return e.fetchShowCollation()
	case ast.ShowColumns:
		trace_util_0.Count(_show_00000, 16)
		return e.fetchShowColumns()
	case ast.ShowCreateTable:
		trace_util_0.Count(_show_00000, 17)
		return e.fetchShowCreateTable()
	case ast.ShowCreateUser:
		trace_util_0.Count(_show_00000, 18)
		return e.fetchShowCreateUser()
	case ast.ShowCreateView:
		trace_util_0.Count(_show_00000, 19)
		return e.fetchShowCreateView()
	case ast.ShowCreateDatabase:
		trace_util_0.Count(_show_00000, 20)
		return e.fetchShowCreateDatabase()
	case ast.ShowDatabases:
		trace_util_0.Count(_show_00000, 21)
		return e.fetchShowDatabases()
	case ast.ShowDrainerStatus:
		trace_util_0.Count(_show_00000, 22)
		return e.fetchShowPumpOrDrainerStatus(node.DrainerNode)
	case ast.ShowEngines:
		trace_util_0.Count(_show_00000, 23)
		return e.fetchShowEngines()
	case ast.ShowGrants:
		trace_util_0.Count(_show_00000, 24)
		return e.fetchShowGrants()
	case ast.ShowIndex:
		trace_util_0.Count(_show_00000, 25)
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		trace_util_0.Count(_show_00000, 26)
		return e.fetchShowProcedureStatus()
	case ast.ShowPumpStatus:
		trace_util_0.Count(_show_00000, 27)
		return e.fetchShowPumpOrDrainerStatus(node.PumpNode)
	case ast.ShowStatus:
		trace_util_0.Count(_show_00000, 28)
		return e.fetchShowStatus()
	case ast.ShowTables:
		trace_util_0.Count(_show_00000, 29)
		return e.fetchShowTables()
	case ast.ShowOpenTables:
		trace_util_0.Count(_show_00000, 30)
		return e.fetchShowOpenTables()
	case ast.ShowTableStatus:
		trace_util_0.Count(_show_00000, 31)
		return e.fetchShowTableStatus()
	case ast.ShowTriggers:
		trace_util_0.Count(_show_00000, 32)
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		trace_util_0.Count(_show_00000, 33)
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		trace_util_0.Count(_show_00000, 34)
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		trace_util_0.Count(_show_00000, 35)
		return e.fetchShowWarnings(true)
	case ast.ShowProcessList:
		trace_util_0.Count(_show_00000, 36)
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		trace_util_0.Count(_show_00000, 37)
		// empty result
	case ast.ShowStatsMeta:
		trace_util_0.Count(_show_00000, 38)
		return e.fetchShowStatsMeta()
	case ast.ShowStatsHistograms:
		trace_util_0.Count(_show_00000, 39)
		return e.fetchShowStatsHistogram()
	case ast.ShowStatsBuckets:
		trace_util_0.Count(_show_00000, 40)
		return e.fetchShowStatsBuckets()
	case ast.ShowStatsHealthy:
		trace_util_0.Count(_show_00000, 41)
		e.fetchShowStatsHealthy()
		return nil
	case ast.ShowPlugins:
		trace_util_0.Count(_show_00000, 42)
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		trace_util_0.Count(_show_00000, 43)
		// empty result
	case ast.ShowMasterStatus:
		trace_util_0.Count(_show_00000, 44)
		return e.fetchShowMasterStatus()
	case ast.ShowPrivileges:
		trace_util_0.Count(_show_00000, 45)
		return e.fetchShowPrivileges()
	case ast.ShowBindings:
		trace_util_0.Count(_show_00000, 46)
		return e.fetchShowBind()
	case ast.ShowAnalyzeStatus:
		trace_util_0.Count(_show_00000, 47)
		e.fetchShowAnalyzeStatus()
		return nil
	}
	trace_util_0.Count(_show_00000, 13)
	return nil
}

func (e *ShowExec) fetchShowBind() error {
	trace_util_0.Count(_show_00000, 48)
	var bindRecords []*bindinfo.BindMeta
	if !e.GlobalScope {
		trace_util_0.Count(_show_00000, 51)
		handle := e.ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
		bindRecords = handle.GetAllBindRecord()
	} else {
		trace_util_0.Count(_show_00000, 52)
		{
			bindRecords = domain.GetDomain(e.ctx).BindHandle().GetAllBindRecord()
		}
	}
	trace_util_0.Count(_show_00000, 49)
	for _, bindData := range bindRecords {
		trace_util_0.Count(_show_00000, 53)
		e.appendRow([]interface{}{
			bindData.OriginalSQL,
			bindData.BindSQL,
			bindData.Db,
			bindData.Status,
			bindData.CreateTime,
			bindData.UpdateTime,
			bindData.Charset,
			bindData.Collation,
		})
	}
	trace_util_0.Count(_show_00000, 50)
	return nil
}

func (e *ShowExec) fetchShowEngines() error {
	trace_util_0.Count(_show_00000, 54)
	sql := `SELECT * FROM information_schema.engines`
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)

	if err != nil {
		trace_util_0.Count(_show_00000, 57)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 55)
	for _, row := range rows {
		trace_util_0.Count(_show_00000, 58)
		e.result.AppendRow(row)
	}
	trace_util_0.Count(_show_00000, 56)
	return nil
}

// moveInfoSchemaToFront moves information_schema to the first, and the others are sorted in the origin ascending order.
func moveInfoSchemaToFront(dbs []string) {
	trace_util_0.Count(_show_00000, 59)
	if len(dbs) > 0 && strings.EqualFold(dbs[0], "INFORMATION_SCHEMA") {
		trace_util_0.Count(_show_00000, 61)
		return
	}

	trace_util_0.Count(_show_00000, 60)
	i := sort.SearchStrings(dbs, "INFORMATION_SCHEMA")
	if i < len(dbs) && strings.EqualFold(dbs[i], "INFORMATION_SCHEMA") {
		trace_util_0.Count(_show_00000, 62)
		copy(dbs[1:i+1], dbs[0:i])
		dbs[0] = "INFORMATION_SCHEMA"
	}
}

func (e *ShowExec) fetchShowDatabases() error {
	trace_util_0.Count(_show_00000, 63)
	dbs := e.is.AllSchemaNames()
	checker := privilege.GetPrivilegeManager(e.ctx)
	sort.Strings(dbs)
	// let information_schema be the first database
	moveInfoSchemaToFront(dbs)
	for _, d := range dbs {
		trace_util_0.Count(_show_00000, 65)
		if checker != nil && !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, d) {
			trace_util_0.Count(_show_00000, 67)
			continue
		}
		trace_util_0.Count(_show_00000, 66)
		e.appendRow([]interface{}{
			d,
		})
	}
	trace_util_0.Count(_show_00000, 64)
	return nil
}

func (e *ShowExec) fetchShowProcessList() error {
	trace_util_0.Count(_show_00000, 68)
	sm := e.ctx.GetSessionManager()
	if sm == nil {
		trace_util_0.Count(_show_00000, 72)
		return nil
	}

	trace_util_0.Count(_show_00000, 69)
	loginUser, activeRoles := e.ctx.GetSessionVars().User, e.ctx.GetSessionVars().ActiveRoles
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(e.ctx); pm != nil {
		trace_util_0.Count(_show_00000, 73)
		if pm.RequestVerification(activeRoles, "", "", "", mysql.ProcessPriv) {
			trace_util_0.Count(_show_00000, 74)
			hasProcessPriv = true
		}
	}

	trace_util_0.Count(_show_00000, 70)
	pl := sm.ShowProcessList()
	for _, pi := range pl {
		trace_util_0.Count(_show_00000, 75)
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && pi.User != loginUser.Username {
			trace_util_0.Count(_show_00000, 77)
			continue
		}
		trace_util_0.Count(_show_00000, 76)
		row := pi.ToRow(e.Full)
		e.appendRow(row)
	}
	trace_util_0.Count(_show_00000, 71)
	return nil
}

func (e *ShowExec) fetchShowOpenTables() error {
	trace_util_0.Count(_show_00000, 78)
	// TiDB has no concept like mysql's "table cache" and "open table"
	// For simplicity, we just return an empty result with the same structure as MySQL's SHOW OPEN TABLES
	return nil
}

func (e *ShowExec) fetchShowTables() error {
	trace_util_0.Count(_show_00000, 79)
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_show_00000, 84)
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O) {
			trace_util_0.Count(_show_00000, 85)
			return e.dbAccessDenied()
		}
	}
	trace_util_0.Count(_show_00000, 80)
	if !e.is.SchemaExists(e.DBName) {
		trace_util_0.Count(_show_00000, 86)
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	// sort for tables
	trace_util_0.Count(_show_00000, 81)
	tableNames := make([]string, 0, len(e.is.SchemaTables(e.DBName)))
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	var tableTypes = make(map[string]string)
	for _, v := range e.is.SchemaTables(e.DBName) {
		trace_util_0.Count(_show_00000, 87)
		// Test with mysql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider column privileges, which also make a table visible.
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, v.Meta().Name.O, "", mysql.AllPrivMask) {
			trace_util_0.Count(_show_00000, 89)
			continue
		}
		trace_util_0.Count(_show_00000, 88)
		tableNames = append(tableNames, v.Meta().Name.O)
		if v.Meta().IsView() {
			trace_util_0.Count(_show_00000, 90)
			tableTypes[v.Meta().Name.O] = "VIEW"
		} else {
			trace_util_0.Count(_show_00000, 91)
			{
				tableTypes[v.Meta().Name.O] = "BASE TABLE"
			}
		}
	}
	trace_util_0.Count(_show_00000, 82)
	sort.Strings(tableNames)
	for _, v := range tableNames {
		trace_util_0.Count(_show_00000, 92)
		if e.Full {
			trace_util_0.Count(_show_00000, 93)
			e.appendRow([]interface{}{v, tableTypes[v]})
		} else {
			trace_util_0.Count(_show_00000, 94)
			{
				e.appendRow([]interface{}{v})
			}
		}
	}
	trace_util_0.Count(_show_00000, 83)
	return nil
}

func (e *ShowExec) fetchShowTableStatus() error {
	trace_util_0.Count(_show_00000, 95)
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_show_00000, 100)
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.O) {
			trace_util_0.Count(_show_00000, 101)
			return e.dbAccessDenied()
		}
	}
	trace_util_0.Count(_show_00000, 96)
	if !e.is.SchemaExists(e.DBName) {
		trace_util_0.Count(_show_00000, 102)
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	trace_util_0.Count(_show_00000, 97)
	sql := fmt.Sprintf(`SELECT
               table_name, engine, version, row_format, table_rows,
               avg_row_length, data_length, max_data_length, index_length,
               data_free, auto_increment, create_time, update_time, check_time,
               table_collation, IFNULL(checksum,''), create_options, table_comment
               FROM information_schema.tables
	       WHERE table_schema='%s' ORDER BY table_name`, e.DBName)

	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)

	if err != nil {
		trace_util_0.Count(_show_00000, 103)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 98)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	for _, row := range rows {
		trace_util_0.Count(_show_00000, 104)
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, row.GetString(0), "", mysql.AllPrivMask) {
			trace_util_0.Count(_show_00000, 106)
			continue
		}
		trace_util_0.Count(_show_00000, 105)
		e.result.AppendRow(row)

	}
	trace_util_0.Count(_show_00000, 99)
	return nil
}

func createOptions(tb *model.TableInfo) string {
	trace_util_0.Count(_show_00000, 107)
	if tb.GetPartitionInfo() != nil {
		trace_util_0.Count(_show_00000, 109)
		return "partitioned"
	}
	trace_util_0.Count(_show_00000, 108)
	return ""
}

func (e *ShowExec) fetchShowColumns() error {
	trace_util_0.Count(_show_00000, 110)
	tb, err := e.getTable()

	if err != nil {
		trace_util_0.Count(_show_00000, 115)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 111)
	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	if checker != nil && e.ctx.GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.AllPrivMask) {
		trace_util_0.Count(_show_00000, 116)
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	trace_util_0.Count(_show_00000, 112)
	cols := tb.Cols()
	if tb.Meta().IsView() {
		trace_util_0.Count(_show_00000, 117)
		// Because view's undertable's column could change or recreate, so view's column type may change overtime.
		// To avoid this situation we need to generate a logical plan and extract current column types from Schema.
		planBuilder := plannercore.NewPlanBuilder(e.ctx, e.is)
		viewLogicalPlan, err := planBuilder.BuildDataSourceFromView(e.DBName, tb.Meta())
		if err != nil {
			trace_util_0.Count(_show_00000, 119)
			return err
		}
		trace_util_0.Count(_show_00000, 118)
		viewSchema := viewLogicalPlan.Schema()
		for _, col := range cols {
			trace_util_0.Count(_show_00000, 120)
			viewColumn := viewSchema.FindColumnByName(col.Name.L)
			if viewColumn != nil {
				trace_util_0.Count(_show_00000, 121)
				col.FieldType = *viewColumn.GetType()
			}
		}
	}
	trace_util_0.Count(_show_00000, 113)
	for _, col := range cols {
		trace_util_0.Count(_show_00000, 122)
		if e.Column != nil && e.Column.Name.L != col.Name.L {
			trace_util_0.Count(_show_00000, 125)
			continue
		}

		trace_util_0.Count(_show_00000, 123)
		desc := table.NewColDesc(col)
		var columnDefault interface{}
		if desc.DefaultValue != nil {
			trace_util_0.Count(_show_00000, 126)
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && strings.ToUpper(defaultValStr) != strings.ToUpper(ast.CurrentTimestamp) {
				trace_util_0.Count(_show_00000, 128)
				timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
				if err != nil {
					trace_util_0.Count(_show_00000, 130)
					return errors.Trace(err)
				}
				trace_util_0.Count(_show_00000, 129)
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			trace_util_0.Count(_show_00000, 127)
			if col.Tp == mysql.TypeBit {
				trace_util_0.Count(_show_00000, 131)
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				trace_util_0.Count(_show_00000, 132)
				{
					columnDefault = defaultValStr
				}
			}
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		trace_util_0.Count(_show_00000, 124)
		if e.Full {
			trace_util_0.Count(_show_00000, 133)
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			trace_util_0.Count(_show_00000, 134)
			{
				e.appendRow([]interface{}{
					desc.Field,
					desc.Type,
					desc.Null,
					desc.Key,
					columnDefault,
					desc.Extra,
				})
			}
		}
	}
	trace_util_0.Count(_show_00000, 114)
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	trace_util_0.Count(_show_00000, 135)
	tb, err := e.getTable()
	if err != nil {
		trace_util_0.Count(_show_00000, 140)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 136)
	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles
	if checker != nil && e.ctx.GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.AllPrivMask) {
		trace_util_0.Count(_show_00000, 141)
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	trace_util_0.Count(_show_00000, 137)
	if tb.Meta().PKIsHandle {
		trace_util_0.Count(_show_00000, 142)
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			trace_util_0.Count(_show_00000, 144)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_show_00000, 145)
				pkCol = col
				break
			}
		}
		trace_util_0.Count(_show_00000, 143)
		e.appendRow([]interface{}{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
		})
	}
	trace_util_0.Count(_show_00000, 138)
	for _, idx := range tb.Indices() {
		trace_util_0.Count(_show_00000, 146)
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			trace_util_0.Count(_show_00000, 148)
			continue
		}
		trace_util_0.Count(_show_00000, 147)
		for i, col := range idxInfo.Columns {
			trace_util_0.Count(_show_00000, 149)
			nonUniq := 1
			if idx.Meta().Unique {
				trace_util_0.Count(_show_00000, 152)
				nonUniq = 0
			}
			trace_util_0.Count(_show_00000, 150)
			var subPart interface{}
			if col.Length != types.UnspecifiedLength {
				trace_util_0.Count(_show_00000, 153)
				subPart = col.Length
			}
			trace_util_0.Count(_show_00000, 151)
			e.appendRow([]interface{}{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				col.Name.O,             // Column_name
				"A",                    // Collation
				0,                      // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				"YES",                  // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
			})
		}
	}
	trace_util_0.Count(_show_00000, 139)
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	trace_util_0.Count(_show_00000, 154)
	descs := charset.GetSupportedCharsets()
	for _, desc := range descs {
		trace_util_0.Count(_show_00000, 156)
		e.appendRow([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultCollation,
			desc.Maxlen,
		})
	}
	trace_util_0.Count(_show_00000, 155)
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	trace_util_0.Count(_show_00000, 157)
	tso := e.ctx.GetSessionVars().TxnCtx.StartTS
	e.appendRow([]interface{}{"tidb-binlog", tso, "", "", ""})
	return nil
}

func (e *ShowExec) fetchShowVariables() (err error) {
	trace_util_0.Count(_show_00000, 158)
	var (
		value         string
		ok            bool
		sessionVars   = e.ctx.GetSessionVars()
		unreachedVars = make([]string, 0, len(variable.SysVars))
	)
	for _, v := range variable.SysVars {
		trace_util_0.Count(_show_00000, 161)
		if !e.GlobalScope {
			trace_util_0.Count(_show_00000, 165)
			// For a session scope variable,
			// 1. try to fetch value from SessionVars.Systems;
			// 2. if this variable is session-only, fetch value from SysVars
			//		otherwise, fetch the value from table `mysql.Global_Variables`.
			value, ok, err = variable.GetSessionOnlySysVars(sessionVars, v.Name)
		} else {
			trace_util_0.Count(_show_00000, 166)
			{
				// If the scope of a system variable is ScopeNone,
				// it's a read-only variable, so we return the default value of it.
				// Otherwise, we have to fetch the values from table `mysql.Global_Variables` for global variable names.
				value, ok, err = variable.GetScopeNoneSystemVar(v.Name)
			}
		}
		trace_util_0.Count(_show_00000, 162)
		if err != nil {
			trace_util_0.Count(_show_00000, 167)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_00000, 163)
		if !ok {
			trace_util_0.Count(_show_00000, 168)
			unreachedVars = append(unreachedVars, v.Name)
			continue
		}
		trace_util_0.Count(_show_00000, 164)
		e.appendRow([]interface{}{v.Name, value})
	}
	trace_util_0.Count(_show_00000, 159)
	if len(unreachedVars) != 0 {
		trace_util_0.Count(_show_00000, 169)
		systemVars, err := sessionVars.GlobalVarsAccessor.GetAllSysVars()
		if err != nil {
			trace_util_0.Count(_show_00000, 171)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_00000, 170)
		for _, varName := range unreachedVars {
			trace_util_0.Count(_show_00000, 172)
			varValue, ok := systemVars[varName]
			if !ok {
				trace_util_0.Count(_show_00000, 174)
				varValue = variable.SysVars[varName].Value
			}
			trace_util_0.Count(_show_00000, 173)
			e.appendRow([]interface{}{varName, varValue})
		}
	}
	trace_util_0.Count(_show_00000, 160)
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	trace_util_0.Count(_show_00000, 175)
	sessionVars := e.ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		trace_util_0.Count(_show_00000, 178)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 176)
	for status, v := range statusVars {
		trace_util_0.Count(_show_00000, 179)
		if e.GlobalScope && v.Scope == variable.ScopeSession {
			trace_util_0.Count(_show_00000, 183)
			continue
		}
		trace_util_0.Count(_show_00000, 180)
		switch v.Value.(type) {
		case []interface{}, nil:
			trace_util_0.Count(_show_00000, 184)
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		trace_util_0.Count(_show_00000, 181)
		value, err := types.ToString(v.Value)
		if err != nil {
			trace_util_0.Count(_show_00000, 185)
			return errors.Trace(err)
		}
		trace_util_0.Count(_show_00000, 182)
		e.appendRow([]interface{}{status, value})
	}
	trace_util_0.Count(_show_00000, 177)
	return nil
}

func getDefaultCollate(charsetName string) string {
	trace_util_0.Count(_show_00000, 186)
	for _, c := range charset.GetSupportedCharsets() {
		trace_util_0.Count(_show_00000, 188)
		if strings.EqualFold(c.Name, charsetName) {
			trace_util_0.Count(_show_00000, 189)
			return c.DefaultCollation
		}
	}
	trace_util_0.Count(_show_00000, 187)
	return ""
}

// escape the identifier for pretty-printing.
// For instance, the identifier "foo `bar`" will become "`foo ``bar```".
// The sqlMode controls whether to escape with backquotes (`) or double quotes
// (`"`) depending on whether mysql.ModeANSIQuotes is enabled.
func escape(cis model.CIStr, sqlMode mysql.SQLMode) string {
	trace_util_0.Count(_show_00000, 190)
	var quote string
	if sqlMode&mysql.ModeANSIQuotes != 0 {
		trace_util_0.Count(_show_00000, 192)
		quote = `"`
	} else {
		trace_util_0.Count(_show_00000, 193)
		{
			quote = "`"
		}
	}
	trace_util_0.Count(_show_00000, 191)
	return quote + strings.Replace(cis.O, quote, quote+quote, -1) + quote
}

func (e *ShowExec) fetchShowCreateTable() error {
	trace_util_0.Count(_show_00000, 194)
	tb, err := e.getTable()
	if err != nil {
		trace_util_0.Count(_show_00000, 209)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 195)
	sqlMode := e.ctx.GetSessionVars().SQLMode

	// TODO: let the result more like MySQL.
	var buf bytes.Buffer
	if tb.Meta().IsView() {
		trace_util_0.Count(_show_00000, 210)
		e.fetchShowCreateTable4View(tb.Meta(), &buf)
		e.appendRow([]interface{}{tb.Meta().Name.O, buf.String(), tb.Meta().Charset, tb.Meta().Collate})
		return nil
	}

	trace_util_0.Count(_show_00000, 196)
	tblCharset := tb.Meta().Charset
	if len(tblCharset) == 0 {
		trace_util_0.Count(_show_00000, 211)
		tblCharset = mysql.DefaultCharset
	}
	trace_util_0.Count(_show_00000, 197)
	tblCollate := tb.Meta().Collate
	// Set default collate if collate is not specified.
	if len(tblCollate) == 0 {
		trace_util_0.Count(_show_00000, 212)
		tblCollate = getDefaultCollate(tblCharset)
	}

	trace_util_0.Count(_show_00000, 198)
	fmt.Fprintf(&buf, "CREATE TABLE %s (\n", escape(tb.Meta().Name, sqlMode))
	var pkCol *table.Column
	var hasAutoIncID bool
	for i, col := range tb.Cols() {
		trace_util_0.Count(_show_00000, 213)
		fmt.Fprintf(&buf, "  %s %s", escape(col.Name, sqlMode), col.GetTypeDesc())
		if col.Charset != "binary" {
			trace_util_0.Count(_show_00000, 219)
			if col.Charset != tblCharset || col.Collate != tblCollate {
				trace_util_0.Count(_show_00000, 220)
				fmt.Fprintf(&buf, " CHARACTER SET %s COLLATE %s", col.Charset, col.Collate)
			}
		}
		trace_util_0.Count(_show_00000, 214)
		if col.IsGenerated() {
			trace_util_0.Count(_show_00000, 221)
			// It's a generated column.
			fmt.Fprintf(&buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				trace_util_0.Count(_show_00000, 222)
				buf.WriteString(" STORED")
			} else {
				trace_util_0.Count(_show_00000, 223)
				{
					buf.WriteString(" VIRTUAL")
				}
			}
		}
		trace_util_0.Count(_show_00000, 215)
		if mysql.HasAutoIncrementFlag(col.Flag) {
			trace_util_0.Count(_show_00000, 224)
			hasAutoIncID = true
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			trace_util_0.Count(_show_00000, 225)
			{
				if mysql.HasNotNullFlag(col.Flag) {
					trace_util_0.Count(_show_00000, 228)
					buf.WriteString(" NOT NULL")
				}
				// default values are not shown for generated columns in MySQL
				trace_util_0.Count(_show_00000, 226)
				if !mysql.HasNoDefaultValueFlag(col.Flag) && !col.IsGenerated() {
					trace_util_0.Count(_show_00000, 229)
					defaultValue := col.GetDefaultValue()
					switch defaultValue {
					case nil:
						trace_util_0.Count(_show_00000, 230)
						if !mysql.HasNotNullFlag(col.Flag) {
							trace_util_0.Count(_show_00000, 234)
							if col.Tp == mysql.TypeTimestamp {
								trace_util_0.Count(_show_00000, 236)
								buf.WriteString(" NULL")
							}
							trace_util_0.Count(_show_00000, 235)
							buf.WriteString(" DEFAULT NULL")
						}
					case "CURRENT_TIMESTAMP":
						trace_util_0.Count(_show_00000, 231)
						buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					default:
						trace_util_0.Count(_show_00000, 232)
						defaultValStr := fmt.Sprintf("%v", defaultValue)
						// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
						if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
							trace_util_0.Count(_show_00000, 237)
							timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
							if err != nil {
								trace_util_0.Count(_show_00000, 239)
								return errors.Trace(err)
							}
							trace_util_0.Count(_show_00000, 238)
							defaultValStr = timeValue.GetMysqlTime().String()
						}

						trace_util_0.Count(_show_00000, 233)
						if col.Tp == mysql.TypeBit {
							trace_util_0.Count(_show_00000, 240)
							defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
							fmt.Fprintf(&buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
						} else {
							trace_util_0.Count(_show_00000, 241)
							{
								fmt.Fprintf(&buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
							}
						}
					}
				}
				trace_util_0.Count(_show_00000, 227)
				if mysql.HasOnUpdateNowFlag(col.Flag) {
					trace_util_0.Count(_show_00000, 242)
					buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				}
			}
		}
		trace_util_0.Count(_show_00000, 216)
		if len(col.Comment) > 0 {
			trace_util_0.Count(_show_00000, 243)
			fmt.Fprintf(&buf, " COMMENT '%s'", format.OutputFormat(col.Comment))
		}
		trace_util_0.Count(_show_00000, 217)
		if i != len(tb.Cols())-1 {
			trace_util_0.Count(_show_00000, 244)
			buf.WriteString(",\n")
		}
		trace_util_0.Count(_show_00000, 218)
		if tb.Meta().PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			trace_util_0.Count(_show_00000, 245)
			pkCol = col
		}
	}

	trace_util_0.Count(_show_00000, 199)
	if pkCol != nil {
		trace_util_0.Count(_show_00000, 246)
		// If PKIsHanle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(&buf, "  PRIMARY KEY (%s)", escape(pkCol.Name, sqlMode))
	}

	trace_util_0.Count(_show_00000, 200)
	publicIndices := make([]table.Index, 0, len(tb.Indices()))
	for _, idx := range tb.Indices() {
		trace_util_0.Count(_show_00000, 247)
		if idx.Meta().State == model.StatePublic {
			trace_util_0.Count(_show_00000, 248)
			publicIndices = append(publicIndices, idx)
		}
	}
	trace_util_0.Count(_show_00000, 201)
	if len(publicIndices) > 0 {
		trace_util_0.Count(_show_00000, 249)
		buf.WriteString(",\n")
	}

	trace_util_0.Count(_show_00000, 202)
	for i, idx := range publicIndices {
		trace_util_0.Count(_show_00000, 250)
		idxInfo := idx.Meta()
		if idxInfo.Primary {
			trace_util_0.Count(_show_00000, 253)
			buf.WriteString("  PRIMARY KEY ")
		} else {
			trace_util_0.Count(_show_00000, 254)
			if idxInfo.Unique {
				trace_util_0.Count(_show_00000, 255)
				fmt.Fprintf(&buf, "  UNIQUE KEY %s ", escape(idxInfo.Name, sqlMode))
			} else {
				trace_util_0.Count(_show_00000, 256)
				{
					fmt.Fprintf(&buf, "  KEY %s ", escape(idxInfo.Name, sqlMode))
				}
			}
		}

		trace_util_0.Count(_show_00000, 251)
		cols := make([]string, 0, len(idxInfo.Columns))
		for _, c := range idxInfo.Columns {
			trace_util_0.Count(_show_00000, 257)
			colInfo := escape(c.Name, sqlMode)
			if c.Length != types.UnspecifiedLength {
				trace_util_0.Count(_show_00000, 259)
				colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
			}
			trace_util_0.Count(_show_00000, 258)
			cols = append(cols, colInfo)
		}
		trace_util_0.Count(_show_00000, 252)
		fmt.Fprintf(&buf, "(%s)", strings.Join(cols, ","))
		if i != len(publicIndices)-1 {
			trace_util_0.Count(_show_00000, 260)
			buf.WriteString(",\n")
		}
	}

	trace_util_0.Count(_show_00000, 203)
	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// Because we only support case sensitive utf8_bin collate, we need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 {
		trace_util_0.Count(_show_00000, 261)
		// If we can not find default collate for the given charset,
		// do not show the collate part.
		fmt.Fprintf(&buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		trace_util_0.Count(_show_00000, 262)
		{
			fmt.Fprintf(&buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
		}
	}

	// Displayed if the compression typed is set.
	trace_util_0.Count(_show_00000, 204)
	if len(tb.Meta().Compression) != 0 {
		trace_util_0.Count(_show_00000, 263)
		fmt.Fprintf(&buf, " COMPRESSION='%s'", tb.Meta().Compression)
	}

	trace_util_0.Count(_show_00000, 205)
	if hasAutoIncID {
		trace_util_0.Count(_show_00000, 264)
		autoIncID, err := tb.Allocator(e.ctx).NextGlobalAutoID(tb.Meta().ID)
		if err != nil {
			trace_util_0.Count(_show_00000, 266)
			return errors.Trace(err)
		}
		// It's campatible with MySQL.
		trace_util_0.Count(_show_00000, 265)
		if autoIncID > 1 {
			trace_util_0.Count(_show_00000, 267)
			fmt.Fprintf(&buf, " AUTO_INCREMENT=%d", autoIncID)
		}
	}

	trace_util_0.Count(_show_00000, 206)
	if tb.Meta().ShardRowIDBits > 0 {
		trace_util_0.Count(_show_00000, 268)
		fmt.Fprintf(&buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", tb.Meta().ShardRowIDBits)
		if tb.Meta().PreSplitRegions > 0 {
			trace_util_0.Count(_show_00000, 270)
			fmt.Fprintf(&buf, "PRE_SPLIT_REGIONS=%d ", tb.Meta().PreSplitRegions)
		}
		trace_util_0.Count(_show_00000, 269)
		buf.WriteString("*/")
	}

	trace_util_0.Count(_show_00000, 207)
	if len(tb.Meta().Comment) > 0 {
		trace_util_0.Count(_show_00000, 271)
		fmt.Fprintf(&buf, " COMMENT='%s'", format.OutputFormat(tb.Meta().Comment))
	}
	// add partition info here.
	trace_util_0.Count(_show_00000, 208)
	appendPartitionInfo(tb.Meta().Partition, &buf)

	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCreateView() error {
	trace_util_0.Count(_show_00000, 272)
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		trace_util_0.Count(_show_00000, 276)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	trace_util_0.Count(_show_00000, 273)
	tb, err := e.getTable()
	if err != nil {
		trace_util_0.Count(_show_00000, 277)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 274)
	if !tb.Meta().IsView() {
		trace_util_0.Count(_show_00000, 278)
		return ErrWrongObject.GenWithStackByArgs(db.Name.O, tb.Meta().Name.O, "VIEW")
	}

	trace_util_0.Count(_show_00000, 275)
	var buf bytes.Buffer
	e.fetchShowCreateTable4View(tb.Meta(), &buf)
	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String(), tb.Meta().Charset, tb.Meta().Collate})
	return nil
}

func (e *ShowExec) fetchShowCreateTable4View(tb *model.TableInfo, buf *bytes.Buffer) {
	trace_util_0.Count(_show_00000, 279)
	sqlMode := e.ctx.GetSessionVars().SQLMode

	fmt.Fprintf(buf, "CREATE ALGORITHM=%s ", tb.View.Algorithm.String())
	fmt.Fprintf(buf, "DEFINER=%s@%s ", escape(model.NewCIStr(tb.View.Definer.Username), sqlMode), escape(model.NewCIStr(tb.View.Definer.Hostname), sqlMode))
	fmt.Fprintf(buf, "SQL SECURITY %s ", tb.View.Security.String())
	fmt.Fprintf(buf, "VIEW %s (", escape(tb.Name, sqlMode))
	for i, col := range tb.Columns {
		trace_util_0.Count(_show_00000, 281)
		fmt.Fprintf(buf, "%s", escape(col.Name, sqlMode))
		if i < len(tb.Columns)-1 {
			trace_util_0.Count(_show_00000, 282)
			fmt.Fprintf(buf, ", ")
		}
	}
	trace_util_0.Count(_show_00000, 280)
	fmt.Fprintf(buf, ") AS %s", tb.View.SelectStmt)
}

func appendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer) {
	trace_util_0.Count(_show_00000, 283)
	if partitionInfo == nil {
		trace_util_0.Count(_show_00000, 288)
		return
	}
	trace_util_0.Count(_show_00000, 284)
	if partitionInfo.Type == model.PartitionTypeHash {
		trace_util_0.Count(_show_00000, 289)
		fmt.Fprintf(buf, "\nPARTITION BY HASH( %s )", partitionInfo.Expr)
		fmt.Fprintf(buf, "\nPARTITIONS %d", partitionInfo.Num)
		return
	}
	// this if statement takes care of range columns case
	trace_util_0.Count(_show_00000, 285)
	if partitionInfo.Columns != nil && partitionInfo.Type == model.PartitionTypeRange {
		trace_util_0.Count(_show_00000, 290)
		buf.WriteString("\nPARTITION BY RANGE COLUMNS(")
		for i, col := range partitionInfo.Columns {
			trace_util_0.Count(_show_00000, 292)
			buf.WriteString(col.L)
			if i < len(partitionInfo.Columns)-1 {
				trace_util_0.Count(_show_00000, 293)
				buf.WriteString(",")
			}
		}
		trace_util_0.Count(_show_00000, 291)
		buf.WriteString(") (\n")
	} else {
		trace_util_0.Count(_show_00000, 294)
		{
			fmt.Fprintf(buf, "\nPARTITION BY %s ( %s ) (\n", partitionInfo.Type.String(), partitionInfo.Expr)
		}
	}
	trace_util_0.Count(_show_00000, 286)
	for i, def := range partitionInfo.Definitions {
		trace_util_0.Count(_show_00000, 295)
		lessThans := strings.Join(def.LessThan, ",")
		fmt.Fprintf(buf, "  PARTITION %s VALUES LESS THAN (%s)", def.Name, lessThans)
		if i < len(partitionInfo.Definitions)-1 {
			trace_util_0.Count(_show_00000, 296)
			buf.WriteString(",\n")
		} else {
			trace_util_0.Count(_show_00000, 297)
			{
				buf.WriteString("\n")
			}
		}
	}
	trace_util_0.Count(_show_00000, 287)
	buf.WriteString(")")
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	trace_util_0.Count(_show_00000, 298)
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_show_00000, 303)
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.String()) {
			trace_util_0.Count(_show_00000, 304)
			return e.dbAccessDenied()
		}
	}
	trace_util_0.Count(_show_00000, 299)
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		trace_util_0.Count(_show_00000, 305)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	trace_util_0.Count(_show_00000, 300)
	sqlMode := e.ctx.GetSessionVars().SQLMode

	var buf bytes.Buffer
	var ifNotExists string
	if e.IfNotExists {
		trace_util_0.Count(_show_00000, 306)
		ifNotExists = "/*!32312 IF NOT EXISTS*/ "
	}
	trace_util_0.Count(_show_00000, 301)
	fmt.Fprintf(&buf, "CREATE DATABASE %s%s", ifNotExists, escape(db.Name, sqlMode))
	if s := db.Charset; len(s) > 0 {
		trace_util_0.Count(_show_00000, 307)
		fmt.Fprintf(&buf, " /*!40100 DEFAULT CHARACTER SET %s */", s)
	}

	trace_util_0.Count(_show_00000, 302)
	e.appendRow([]interface{}{db.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCollation() error {
	trace_util_0.Count(_show_00000, 308)
	collations := charset.GetSupportedCollations()
	for _, v := range collations {
		trace_util_0.Count(_show_00000, 310)
		isDefault := ""
		if v.IsDefault {
			trace_util_0.Count(_show_00000, 312)
			isDefault = "Yes"
		}
		trace_util_0.Count(_show_00000, 311)
		e.appendRow([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	trace_util_0.Count(_show_00000, 309)
	return nil
}

// fetchShowCreateUser composes show create create user result.
func (e *ShowExec) fetchShowCreateUser() error {
	trace_util_0.Count(_show_00000, 313)
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		trace_util_0.Count(_show_00000, 317)
		return errors.New("miss privilege checker")
	}
	trace_util_0.Count(_show_00000, 314)
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`,
		mysql.SystemDB, mysql.UserTable, e.User.Username, e.User.Hostname)
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	if err != nil {
		trace_util_0.Count(_show_00000, 318)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 315)
	if len(rows) == 0 {
		trace_util_0.Count(_show_00000, 319)
		return ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER",
			fmt.Sprintf("'%s'@'%s'", e.User.Username, e.User.Hostname))
	}
	trace_util_0.Count(_show_00000, 316)
	showStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH 'mysql_native_password' AS '%s' %s",
		e.User.Username, e.User.Hostname, checker.GetEncodedPassword(e.User.Username, e.User.Hostname),
		"REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK")
	e.appendRow([]interface{}{showStr})
	return nil
}

func (e *ShowExec) fetchShowGrants() error {
	trace_util_0.Count(_show_00000, 320)
	// Get checker
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		trace_util_0.Count(_show_00000, 325)
		return errors.New("miss privilege checker")
	}
	trace_util_0.Count(_show_00000, 321)
	for _, r := range e.Roles {
		trace_util_0.Count(_show_00000, 326)
		if r.Hostname == "" {
			trace_util_0.Count(_show_00000, 328)
			r.Hostname = "%"
		}
		trace_util_0.Count(_show_00000, 327)
		if !checker.FindEdge(e.ctx, r, e.User) {
			trace_util_0.Count(_show_00000, 329)
			return ErrRoleNotGranted.GenWithStackByArgs(r.String(), e.User.String())
		}
	}
	trace_util_0.Count(_show_00000, 322)
	gs, err := checker.ShowGrants(e.ctx, e.User, e.Roles)
	if err != nil {
		trace_util_0.Count(_show_00000, 330)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 323)
	for _, g := range gs {
		trace_util_0.Count(_show_00000, 331)
		e.appendRow([]interface{}{g})
	}
	trace_util_0.Count(_show_00000, 324)
	return nil
}

func (e *ShowExec) fetchShowPrivileges() error {
	trace_util_0.Count(_show_00000, 332)
	e.appendRow([]interface{}{"Alter", "Tables", "To alter the table"})
	e.appendRow([]interface{}{"Alter", "Tables", "To alter the table"})
	e.appendRow([]interface{}{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendRow([]interface{}{"Create", "Databases,Tables,Indexes", "To create new databases and tables"})
	e.appendRow([]interface{}{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendRow([]interface{}{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendRow([]interface{}{"Create view", "Tables", "To create new views"})
	e.appendRow([]interface{}{"Create user", "Server Admin", "To create new users"})
	e.appendRow([]interface{}{"Delete", "Tables", "To delete existing rows"})
	e.appendRow([]interface{}{"Drop", "Databases,Tables", "To drop databases, tables, and views"})
	e.appendRow([]interface{}{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendRow([]interface{}{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendRow([]interface{}{"File", "File access on server", "To read and write files on the server"})
	e.appendRow([]interface{}{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendRow([]interface{}{"Index", "Tables", "To create or drop indexes"})
	e.appendRow([]interface{}{"Insert", "Tables", "To insert data into tables"})
	e.appendRow([]interface{}{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendRow([]interface{}{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendRow([]interface{}{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendRow([]interface{}{"References", "Databases,Tables", "To have references on tables"})
	e.appendRow([]interface{}{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"})
	e.appendRow([]interface{}{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendRow([]interface{}{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendRow([]interface{}{"Select", "Tables", "To retrieve rows from table"})
	e.appendRow([]interface{}{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendRow([]interface{}{"Show view", "Tables", "To see views with SHOW CREATE VIEW"})
	e.appendRow([]interface{}{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendRow([]interface{}{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendRow([]interface{}{"Trigger", "Tables", "To use triggers"})
	e.appendRow([]interface{}{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"})
	e.appendRow([]interface{}{"Update", "Tables", "To update existing rows"})
	e.appendRow([]interface{}{"Usage", "Server Admin", "No privileges - allow connect only"})
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	trace_util_0.Count(_show_00000, 333)
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	trace_util_0.Count(_show_00000, 334)
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	trace_util_0.Count(_show_00000, 335)
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		trace_util_0.Count(_show_00000, 337)
		for _, p := range ps {
			trace_util_0.Count(_show_00000, 338)
			e.appendRow([]interface{}{p.Name, p.State.String(), p.Kind.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	trace_util_0.Count(_show_00000, 336)
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	trace_util_0.Count(_show_00000, 339)
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, w := range warns {
		trace_util_0.Count(_show_00000, 341)
		if errOnly && w.Level != stmtctx.WarnLevelError {
			trace_util_0.Count(_show_00000, 343)
			continue
		}
		trace_util_0.Count(_show_00000, 342)
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			trace_util_0.Count(_show_00000, 344)
			sqlErr := x.ToSQLError()
			e.appendRow([]interface{}{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			trace_util_0.Count(_show_00000, 345)
			e.appendRow([]interface{}{w.Level, int64(mysql.ErrUnknown), warn.Error()})
		}
	}
	trace_util_0.Count(_show_00000, 340)
	return nil
}

// fetchShowPumpOrDrainerStatus gets status of all pumps or drainers and fill them into e.rows.
func (e *ShowExec) fetchShowPumpOrDrainerStatus(kind string) error {
	trace_util_0.Count(_show_00000, 346)
	registry, err := createRegistry(config.GetGlobalConfig().Path)
	if err != nil {
		trace_util_0.Count(_show_00000, 351)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 347)
	nodes, _, err := registry.Nodes(context.Background(), node.NodePrefix[kind])
	if err != nil {
		trace_util_0.Count(_show_00000, 352)
		return errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 348)
	err = registry.Close()
	if err != nil {
		trace_util_0.Count(_show_00000, 353)
		return errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 349)
	for _, n := range nodes {
		trace_util_0.Count(_show_00000, 354)
		e.appendRow([]interface{}{n.NodeID, n.Addr, n.State, n.MaxCommitTS, utils.TSOToRoughTime(n.UpdateTS).Format(types.TimeFormat)})
	}

	trace_util_0.Count(_show_00000, 350)
	return nil
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	trace_util_0.Count(_show_00000, 355)
	ectdEndpoints, err := utils.ParseHostPortAddr(urls)
	if err != nil {
		trace_util_0.Count(_show_00000, 358)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_show_00000, 356)
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, node.DefaultRootPath, nil)
	if err != nil {
		trace_util_0.Count(_show_00000, 359)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_show_00000, 357)
	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	trace_util_0.Count(_show_00000, 360)
	if e.Table == nil {
		trace_util_0.Count(_show_00000, 363)
		return nil, errors.New("table not found")
	}
	trace_util_0.Count(_show_00000, 361)
	tb, ok := e.is.TableByID(e.Table.TableInfo.ID)
	if !ok {
		trace_util_0.Count(_show_00000, 364)
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	trace_util_0.Count(_show_00000, 362)
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	trace_util_0.Count(_show_00000, 365)
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		trace_util_0.Count(_show_00000, 367)
		u = user.AuthUsername
		h = user.AuthHostname
	}
	trace_util_0.Count(_show_00000, 366)
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}

func (e *ShowExec) tableAccessDenied(access string, table string) error {
	trace_util_0.Count(_show_00000, 368)
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		trace_util_0.Count(_show_00000, 370)
		u = user.AuthUsername
		h = user.AuthHostname
	}
	trace_util_0.Count(_show_00000, 369)
	return ErrTableaccessDenied.GenWithStackByArgs(access, u, h, table)
}

func (e *ShowExec) appendRow(row []interface{}) {
	trace_util_0.Count(_show_00000, 371)
	for i, col := range row {
		trace_util_0.Count(_show_00000, 372)
		if col == nil {
			trace_util_0.Count(_show_00000, 374)
			e.result.AppendNull(i)
			continue
		}
		trace_util_0.Count(_show_00000, 373)
		switch x := col.(type) {
		case nil:
			trace_util_0.Count(_show_00000, 375)
			e.result.AppendNull(i)
		case int:
			trace_util_0.Count(_show_00000, 376)
			e.result.AppendInt64(i, int64(x))
		case int64:
			trace_util_0.Count(_show_00000, 377)
			e.result.AppendInt64(i, x)
		case uint64:
			trace_util_0.Count(_show_00000, 378)
			e.result.AppendUint64(i, x)
		case float64:
			trace_util_0.Count(_show_00000, 379)
			e.result.AppendFloat64(i, x)
		case float32:
			trace_util_0.Count(_show_00000, 380)
			e.result.AppendFloat32(i, x)
		case string:
			trace_util_0.Count(_show_00000, 381)
			e.result.AppendString(i, x)
		case []byte:
			trace_util_0.Count(_show_00000, 382)
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			trace_util_0.Count(_show_00000, 383)
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			trace_util_0.Count(_show_00000, 384)
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			trace_util_0.Count(_show_00000, 385)
			e.result.AppendTime(i, x)
		case json.BinaryJSON:
			trace_util_0.Count(_show_00000, 386)
			e.result.AppendJSON(i, x)
		case types.Duration:
			trace_util_0.Count(_show_00000, 387)
			e.result.AppendDuration(i, x)
		case types.Enum:
			trace_util_0.Count(_show_00000, 388)
			e.result.AppendEnum(i, x)
		case types.Set:
			trace_util_0.Count(_show_00000, 389)
			e.result.AppendSet(i, x)
		default:
			trace_util_0.Count(_show_00000, 390)
			e.result.AppendNull(i)
		}
	}
}

var _show_00000 = "executor/show.go"
