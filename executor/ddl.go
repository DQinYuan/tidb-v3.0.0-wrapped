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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor

	stmt ast.StmtNode
	is   infoschema.InfoSchema
	done bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	trace_util_0.Count(_ddl_00000, 0)
	if e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue {
		trace_util_0.Count(_ddl_00000, 4)
		return err
	}

	// Before the DDL job is ready, it encouters an error that may be due to the outdated schema information.
	// After the DDL job is ready, the ErrInfoSchemaChanged error won't happen because we are getting the schema directly from storage.
	// So we needn't to consider this condition.
	// Here we distinguish the ErrInfoSchemaChanged error from other errors.
	trace_util_0.Count(_ddl_00000, 1)
	dom := domain.GetDomain(e.ctx)
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
	if err1 != nil {
		trace_util_0.Count(_ddl_00000, 5)
		logutil.Logger(context.Background()).Error("active txn failed", zap.Error(err))
		return err1
	}
	trace_util_0.Count(_ddl_00000, 2)
	schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		trace_util_0.Count(_ddl_00000, 6)
		return errors.Trace(schemaInfoErr)
	}
	trace_util_0.Count(_ddl_00000, 3)
	return err
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	trace_util_0.Count(_ddl_00000, 7)
	if e.done {
		trace_util_0.Count(_ddl_00000, 13)
		return nil
	}
	trace_util_0.Count(_ddl_00000, 8)
	e.done = true

	// For each DDL, we should commit the previous transaction and create a new transaction.
	if err = e.ctx.NewTxn(ctx); err != nil {
		trace_util_0.Count(_ddl_00000, 14)
		return err
	}
	trace_util_0.Count(_ddl_00000, 9)
	defer func() { trace_util_0.Count(_ddl_00000, 15); e.ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = false }()

	trace_util_0.Count(_ddl_00000, 10)
	switch x := e.stmt.(type) {
	case *ast.AlterDatabaseStmt:
		trace_util_0.Count(_ddl_00000, 16)
		err = e.executeAlterDatabase(x)
	case *ast.AlterTableStmt:
		trace_util_0.Count(_ddl_00000, 17)
		err = e.executeAlterTable(x)
	case *ast.CreateIndexStmt:
		trace_util_0.Count(_ddl_00000, 18)
		err = e.executeCreateIndex(x)
	case *ast.CreateDatabaseStmt:
		trace_util_0.Count(_ddl_00000, 19)
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		trace_util_0.Count(_ddl_00000, 20)
		err = e.executeCreateTable(x)
	case *ast.CreateViewStmt:
		trace_util_0.Count(_ddl_00000, 21)
		err = e.executeCreateView(x)
	case *ast.DropIndexStmt:
		trace_util_0.Count(_ddl_00000, 22)
		err = e.executeDropIndex(x)
	case *ast.DropDatabaseStmt:
		trace_util_0.Count(_ddl_00000, 23)
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		trace_util_0.Count(_ddl_00000, 24)
		err = e.executeDropTableOrView(x)
	case *ast.RecoverTableStmt:
		trace_util_0.Count(_ddl_00000, 25)
		err = e.executeRecoverTable(x)
	case *ast.RenameTableStmt:
		trace_util_0.Count(_ddl_00000, 26)
		err = e.executeRenameTable(x)
	case *ast.TruncateTableStmt:
		trace_util_0.Count(_ddl_00000, 27)
		err = e.executeTruncateTable(x)
	}
	trace_util_0.Count(_ddl_00000, 11)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 28)
		return e.toErr(err)
	}

	trace_util_0.Count(_ddl_00000, 12)
	dom := domain.GetDomain(e.ctx)
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	trace_util_0.Count(_ddl_00000, 29)
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().TruncateTable(e.ctx, ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	trace_util_0.Count(_ddl_00000, 30)
	if len(s.TableToTables) != 1 {
		trace_util_0.Count(_ddl_00000, 32)
		// Now we only allow one schema changing at the same time.
		return errors.Errorf("can't run multi schema change")
	}
	trace_util_0.Count(_ddl_00000, 31)
	oldIdent := ast.Ident{Schema: s.OldTable.Schema, Name: s.OldTable.Name}
	newIdent := ast.Ident{Schema: s.NewTable.Schema, Name: s.NewTable.Name}
	isAlterTable := false
	err := domain.GetDomain(e.ctx).DDL().RenameTable(e.ctx, oldIdent, newIdent, isAlterTable)
	return err
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	trace_util_0.Count(_ddl_00000, 33)
	var opt *ast.CharsetOpt
	if len(s.Options) != 0 {
		trace_util_0.Count(_ddl_00000, 36)
		opt = &ast.CharsetOpt{}
		for _, val := range s.Options {
			trace_util_0.Count(_ddl_00000, 37)
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				trace_util_0.Count(_ddl_00000, 38)
				opt.Chs = val.Value
			case ast.DatabaseOptionCollate:
				trace_util_0.Count(_ddl_00000, 39)
				opt.Col = val.Value
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 34)
	err := domain.GetDomain(e.ctx).DDL().CreateSchema(e.ctx, model.NewCIStr(s.Name), opt)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 40)
		if infoschema.ErrDatabaseExists.Equal(err) && s.IfNotExists {
			trace_util_0.Count(_ddl_00000, 41)
			err = nil
		}
	}
	trace_util_0.Count(_ddl_00000, 35)
	return err
}

func (e *DDLExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	trace_util_0.Count(_ddl_00000, 42)
	err := domain.GetDomain(e.ctx).DDL().AlterSchema(e.ctx, s)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	trace_util_0.Count(_ddl_00000, 43)
	err := domain.GetDomain(e.ctx).DDL().CreateTable(e.ctx, s)
	return err
}

func (e *DDLExec) executeCreateView(s *ast.CreateViewStmt) error {
	trace_util_0.Count(_ddl_00000, 44)
	err := domain.GetDomain(e.ctx).DDL().CreateView(e.ctx, s)
	return err
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	trace_util_0.Count(_ddl_00000, 45)
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, ident, s.Unique, model.NewCIStr(s.IndexName), s.IndexColNames, s.IndexOption)
	return err
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	trace_util_0.Count(_ddl_00000, 46)
	dbName := model.NewCIStr(s.Name)

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		trace_util_0.Count(_ddl_00000, 50)
		return errors.New("Drop 'mysql' database is forbidden")
	}

	trace_util_0.Count(_ddl_00000, 47)
	err := domain.GetDomain(e.ctx).DDL().DropSchema(e.ctx, dbName)
	if infoschema.ErrDatabaseNotExists.Equal(err) {
		trace_util_0.Count(_ddl_00000, 51)
		if s.IfExists {
			trace_util_0.Count(_ddl_00000, 52)
			err = nil
		} else {
			trace_util_0.Count(_ddl_00000, 53)
			{
				err = infoschema.ErrDatabaseDropExists.GenWithStackByArgs(s.Name)
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 48)
	sessionVars := e.ctx.GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		trace_util_0.Count(_ddl_00000, 54)
		sessionVars.CurrentDB = ""
		err = variable.SetSessionSystemVar(sessionVars, variable.CharsetDatabase, types.NewStringDatum("utf8"))
		if err != nil {
			trace_util_0.Count(_ddl_00000, 56)
			return err
		}
		trace_util_0.Count(_ddl_00000, 55)
		err = variable.SetSessionSystemVar(sessionVars, variable.CollationDatabase, types.NewStringDatum("utf8_unicode_ci"))
		if err != nil {
			trace_util_0.Count(_ddl_00000, 57)
			return err
		}
	}
	trace_util_0.Count(_ddl_00000, 49)
	return err
}

// If one drop those tables by mistake, it's difficult to recover.
// In the worst case, the whole TiDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemTables = map[string]struct{}{
	"tidb":                 {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isSystemTable(schema, table string) bool {
	trace_util_0.Count(_ddl_00000, 58)
	if schema != "mysql" {
		trace_util_0.Count(_ddl_00000, 61)
		return false
	}
	trace_util_0.Count(_ddl_00000, 59)
	if _, ok := systemTables[table]; ok {
		trace_util_0.Count(_ddl_00000, 62)
		return true
	}
	trace_util_0.Count(_ddl_00000, 60)
	return false
}

func (e *DDLExec) executeDropTableOrView(s *ast.DropTableStmt) error {
	trace_util_0.Count(_ddl_00000, 63)
	var notExistTables []string
	for _, tn := range s.Tables {
		trace_util_0.Count(_ddl_00000, 66)
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		_, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			trace_util_0.Count(_ddl_00000, 72)
			// TODO: we should return special error for table not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistTables = append(notExistTables, fullti.String())
			continue
		}
		trace_util_0.Count(_ddl_00000, 67)
		_, err := e.is.TableByName(tn.Schema, tn.Name)
		if err != nil && infoschema.ErrTableNotExists.Equal(err) {
			trace_util_0.Count(_ddl_00000, 73)
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else {
			trace_util_0.Count(_ddl_00000, 74)
			if err != nil {
				trace_util_0.Count(_ddl_00000, 75)
				return err
			}
		}

		// Protect important system table from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		trace_util_0.Count(_ddl_00000, 68)
		if isSystemTable(tn.Schema.L, tn.Name.L) {
			trace_util_0.Count(_ddl_00000, 76)
			return errors.Errorf("Drop tidb system table '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}

		trace_util_0.Count(_ddl_00000, 69)
		if config.CheckTableBeforeDrop {
			trace_util_0.Count(_ddl_00000, 77)
			logutil.Logger(context.Background()).Warn("admin check table before drop",
				zap.String("database", fullti.Schema.O),
				zap.String("table", fullti.Name.O),
			)
			sql := fmt.Sprintf("admin check table `%s`.`%s`", fullti.Schema.O, fullti.Name.O)
			_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
			if err != nil {
				trace_util_0.Count(_ddl_00000, 78)
				return err
			}
		}

		trace_util_0.Count(_ddl_00000, 70)
		if s.IsView {
			trace_util_0.Count(_ddl_00000, 79)
			err = domain.GetDomain(e.ctx).DDL().DropView(e.ctx, fullti)
		} else {
			trace_util_0.Count(_ddl_00000, 80)
			{
				err = domain.GetDomain(e.ctx).DDL().DropTable(e.ctx, fullti)
			}
		}
		trace_util_0.Count(_ddl_00000, 71)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			trace_util_0.Count(_ddl_00000, 81)
			notExistTables = append(notExistTables, fullti.String())
		} else {
			trace_util_0.Count(_ddl_00000, 82)
			if err != nil {
				trace_util_0.Count(_ddl_00000, 83)
				return err
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 64)
	if len(notExistTables) > 0 && !s.IfExists {
		trace_util_0.Count(_ddl_00000, 84)
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	trace_util_0.Count(_ddl_00000, 65)
	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	trace_util_0.Count(_ddl_00000, 85)
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().DropIndex(e.ctx, ti, model.NewCIStr(s.IndexName))
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && s.IfExists {
		trace_util_0.Count(_ddl_00000, 87)
		err = nil
	}
	trace_util_0.Count(_ddl_00000, 86)
	return err
}

func (e *DDLExec) executeAlterTable(s *ast.AlterTableStmt) error {
	trace_util_0.Count(_ddl_00000, 88)
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().AlterTable(e.ctx, ti, s.Specs)
	return err
}

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
func (e *DDLExec) executeRecoverTable(s *ast.RecoverTableStmt) error {
	trace_util_0.Count(_ddl_00000, 89)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 95)
		return err
	}
	trace_util_0.Count(_ddl_00000, 90)
	t := meta.NewMeta(txn)
	dom := domain.GetDomain(e.ctx)
	var job *model.Job
	var tblInfo *model.TableInfo
	if s.JobID != 0 {
		trace_util_0.Count(_ddl_00000, 96)
		job, tblInfo, err = e.getRecoverTableByJobID(s, t, dom)
	} else {
		trace_util_0.Count(_ddl_00000, 97)
		{
			job, tblInfo, err = e.getRecoverTableByTableName(s, t, dom)
		}
	}
	trace_util_0.Count(_ddl_00000, 91)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 98)
		return err
	}
	// Get table original autoID before table drop.
	trace_util_0.Count(_ddl_00000, 92)
	m, err := dom.GetSnapshotMeta(job.StartTS)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 99)
		return err
	}
	trace_util_0.Count(_ddl_00000, 93)
	autoID, err := m.GetAutoTableID(job.SchemaID, job.TableID)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 100)
		return errors.Errorf("recover table_id: %d, get original autoID from snapshot meta err: %s", job.TableID, err.Error())
	}
	// Call DDL RecoverTable
	trace_util_0.Count(_ddl_00000, 94)
	err = domain.GetDomain(e.ctx).DDL().RecoverTable(e.ctx, tblInfo, job.SchemaID, autoID, job.ID, job.StartTS)
	return err
}

func (e *DDLExec) getRecoverTableByJobID(s *ast.RecoverTableStmt, t *meta.Meta, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	trace_util_0.Count(_ddl_00000, 101)
	job, err := t.GetHistoryDDLJob(s.JobID)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 108)
		return nil, nil, err
	}
	trace_util_0.Count(_ddl_00000, 102)
	if job == nil {
		trace_util_0.Count(_ddl_00000, 109)
		return nil, nil, admin.ErrDDLJobNotFound.GenWithStackByArgs(s.JobID)
	}
	trace_util_0.Count(_ddl_00000, 103)
	if job.Type != model.ActionDropTable {
		trace_util_0.Count(_ddl_00000, 110)
		return nil, nil, errors.Errorf("Job %v type is %v, not drop table", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot infoSchema.
	trace_util_0.Count(_ddl_00000, 104)
	err = gcutil.ValidateSnapshot(e.ctx, job.StartTS)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 111)
		return nil, nil, err
	}

	// Get the snapshot infoSchema before drop table.
	trace_util_0.Count(_ddl_00000, 105)
	snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 112)
		return nil, nil, err
	}
	// Get table meta from snapshot infoSchema.
	trace_util_0.Count(_ddl_00000, 106)
	table, ok := snapInfo.TableByID(job.TableID)
	if !ok {
		trace_util_0.Count(_ddl_00000, 113)
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Table ID %d)", job.TableID),
		)
	}
	trace_util_0.Count(_ddl_00000, 107)
	return job, table.Meta(), nil
}

func (e *DDLExec) getRecoverTableByTableName(s *ast.RecoverTableStmt, t *meta.Meta, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	trace_util_0.Count(_ddl_00000, 114)
	jobs, err := t.GetAllHistoryDDLJobs()
	if err != nil {
		trace_util_0.Count(_ddl_00000, 121)
		return nil, nil, err
	}
	trace_util_0.Count(_ddl_00000, 115)
	var job *model.Job
	var tblInfo *model.TableInfo
	gcSafePoint, err := gcutil.GetGCSafePoint(e.ctx)
	if err != nil {
		trace_util_0.Count(_ddl_00000, 122)
		return nil, nil, err
	}
	trace_util_0.Count(_ddl_00000, 116)
	schemaName := s.Table.Schema.L
	if schemaName == "" {
		trace_util_0.Count(_ddl_00000, 123)
		schemaName = e.ctx.GetSessionVars().CurrentDB
	}
	trace_util_0.Count(_ddl_00000, 117)
	if schemaName == "" {
		trace_util_0.Count(_ddl_00000, 124)
		return nil, nil, errors.Trace(core.ErrNoDB)
	}
	// TODO: only search recent `e.JobNum` DDL jobs.
	trace_util_0.Count(_ddl_00000, 118)
	for i := len(jobs) - 1; i > 0; i-- {
		trace_util_0.Count(_ddl_00000, 125)
		job = jobs[i]
		if job.Type != model.ActionDropTable {
			trace_util_0.Count(_ddl_00000, 130)
			continue
		}
		// Check GC safe point for getting snapshot infoSchema.
		trace_util_0.Count(_ddl_00000, 126)
		err = gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			trace_util_0.Count(_ddl_00000, 131)
			return nil, nil, err
		}
		// Get the snapshot infoSchema before drop table.
		trace_util_0.Count(_ddl_00000, 127)
		snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
		if err != nil {
			trace_util_0.Count(_ddl_00000, 132)
			return nil, nil, err
		}
		// Get table meta from snapshot infoSchema.
		trace_util_0.Count(_ddl_00000, 128)
		table, ok := snapInfo.TableByID(job.TableID)
		if !ok {
			trace_util_0.Count(_ddl_00000, 133)
			return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", job.SchemaID),
				fmt.Sprintf("(Table ID %d)", job.TableID),
			)
		}
		trace_util_0.Count(_ddl_00000, 129)
		if table.Meta().Name.L == s.Table.Name.L {
			trace_util_0.Count(_ddl_00000, 134)
			schema, ok := dom.InfoSchema().SchemaByID(job.SchemaID)
			if !ok {
				trace_util_0.Count(_ddl_00000, 136)
				return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", job.SchemaID),
				)
			}
			trace_util_0.Count(_ddl_00000, 135)
			if schema.Name.L == schemaName {
				trace_util_0.Count(_ddl_00000, 137)
				tblInfo = table.Meta()
				break
			}
		}
	}
	trace_util_0.Count(_ddl_00000, 119)
	if tblInfo == nil {
		trace_util_0.Count(_ddl_00000, 138)
		return nil, nil, errors.Errorf("Can't found drop table: %v in ddl history jobs", s.Table.Name)
	}
	trace_util_0.Count(_ddl_00000, 120)
	return job, tblInfo, nil
}

var _ddl_00000 = "executor/ddl.go"
