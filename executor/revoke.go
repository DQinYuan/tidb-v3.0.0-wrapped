// Copyright 2017 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

/***
 * Revoke Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/revoke.html
 ************************************************************************************/
var (
	_ Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  sessionctx.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_revoke_00000, 0)
	if e.done {
		trace_util_0.Count(_revoke_00000, 3)
		return nil
	}
	trace_util_0.Count(_revoke_00000, 1)
	e.done = true

	// Revoke for each user.
	for idx, user := range e.Users {
		trace_util_0.Count(_revoke_00000, 4)
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 8)
			return err
		}
		trace_util_0.Count(_revoke_00000, 5)
		if !exists {
			trace_util_0.Count(_revoke_00000, 9)
			return errors.Errorf("Unknown user: %s", user.User)
		}

		trace_util_0.Count(_revoke_00000, 6)
		if idx == 0 {
			trace_util_0.Count(_revoke_00000, 10)
			// Commit the old transaction, like DDL.
			if err := e.ctx.NewTxn(ctx); err != nil {
				trace_util_0.Count(_revoke_00000, 12)
				return err
			}
			trace_util_0.Count(_revoke_00000, 11)
			defer func() {
				trace_util_0.Count(_revoke_00000, 13)
				e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
			}()
		}
		trace_util_0.Count(_revoke_00000, 7)
		err = e.revokeOneUser(user.User.Username, user.User.Hostname)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 14)
			return err
		}
	}
	trace_util_0.Count(_revoke_00000, 2)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *RevokeExec) revokeOneUser(user, host string) error {
	trace_util_0.Count(_revoke_00000, 15)
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		trace_util_0.Count(_revoke_00000, 19)
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	// If there is no privilege entry in corresponding table, insert a new one.
	// DB scope:		mysql.DB
	// Table scope:		mysql.Tables_priv
	// Column scope:	mysql.Columns_priv
	trace_util_0.Count(_revoke_00000, 16)
	switch e.Level.Level {
	case ast.GrantLevelDB:
		trace_util_0.Count(_revoke_00000, 20)
		ok, err := dbUserExists(e.ctx, user, host, dbName)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 24)
			return err
		}
		trace_util_0.Count(_revoke_00000, 21)
		if !ok {
			trace_util_0.Count(_revoke_00000, 25)
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on database %s", user, host, dbName)
		}
	case ast.GrantLevelTable:
		trace_util_0.Count(_revoke_00000, 22)
		ok, err := tableUserExists(e.ctx, user, host, dbName, e.Level.TableName)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 26)
			return err
		}
		trace_util_0.Count(_revoke_00000, 23)
		if !ok {
			trace_util_0.Count(_revoke_00000, 27)
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on table %s.%s", user, host, dbName, e.Level.TableName)
		}
	}

	trace_util_0.Count(_revoke_00000, 17)
	for _, priv := range e.Privs {
		trace_util_0.Count(_revoke_00000, 28)
		err := e.revokePriv(priv, user, host)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 29)
			return err
		}
	}
	trace_util_0.Count(_revoke_00000, 18)
	return nil
}

func (e *RevokeExec) revokePriv(priv *ast.PrivElem, user, host string) error {
	trace_util_0.Count(_revoke_00000, 30)
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		trace_util_0.Count(_revoke_00000, 32)
		return e.revokeGlobalPriv(priv, user, host)
	case ast.GrantLevelDB:
		trace_util_0.Count(_revoke_00000, 33)
		return e.revokeDBPriv(priv, user, host)
	case ast.GrantLevelTable:
		trace_util_0.Count(_revoke_00000, 34)
		if len(priv.Cols) == 0 {
			trace_util_0.Count(_revoke_00000, 36)
			return e.revokeTablePriv(priv, user, host)
		}
		trace_util_0.Count(_revoke_00000, 35)
		return e.revokeColumnPriv(priv, user, host)
	}
	trace_util_0.Count(_revoke_00000, 31)
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeGlobalPriv(priv *ast.PrivElem, user, host string) error {
	trace_util_0.Count(_revoke_00000, 37)
	asgns, err := composeGlobalPrivUpdate(priv.Priv, "N")
	if err != nil {
		trace_util_0.Count(_revoke_00000, 39)
		return err
	}
	trace_util_0.Count(_revoke_00000, 38)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s'`, mysql.SystemDB, mysql.UserTable, asgns, user, host)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

func (e *RevokeExec) revokeDBPriv(priv *ast.PrivElem, userName, host string) error {
	trace_util_0.Count(_revoke_00000, 40)
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		trace_util_0.Count(_revoke_00000, 43)
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	trace_util_0.Count(_revoke_00000, 41)
	asgns, err := composeDBPrivUpdate(priv.Priv, "N")
	if err != nil {
		trace_util_0.Count(_revoke_00000, 44)
		return err
	}
	trace_util_0.Count(_revoke_00000, 42)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s';`, mysql.SystemDB, mysql.DBTable, asgns, userName, host, dbName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

func (e *RevokeExec) revokeTablePriv(priv *ast.PrivElem, user, host string) error {
	trace_util_0.Count(_revoke_00000, 45)
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		trace_util_0.Count(_revoke_00000, 48)
		return err
	}
	trace_util_0.Count(_revoke_00000, 46)
	asgns, err := composeTablePrivUpdateForRevoke(e.ctx, priv.Priv, user, host, dbName, tbl.Meta().Name.O)
	if err != nil {
		trace_util_0.Count(_revoke_00000, 49)
		return err
	}
	trace_util_0.Count(_revoke_00000, 47)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s';`, mysql.SystemDB, mysql.TablePrivTable, asgns, user, host, dbName, tbl.Meta().Name.O)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

func (e *RevokeExec) revokeColumnPriv(priv *ast.PrivElem, user, host string) error {
	trace_util_0.Count(_revoke_00000, 50)
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		trace_util_0.Count(_revoke_00000, 53)
		return err
	}
	trace_util_0.Count(_revoke_00000, 51)
	for _, c := range priv.Cols {
		trace_util_0.Count(_revoke_00000, 54)
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			trace_util_0.Count(_revoke_00000, 57)
			return errors.Errorf("Unknown column: %s", c)
		}
		trace_util_0.Count(_revoke_00000, 55)
		asgns, err := composeColumnPrivUpdateForRevoke(e.ctx, priv.Priv, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 58)
			return err
		}
		trace_util_0.Count(_revoke_00000, 56)
		sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s' AND Column_name='%s';`, mysql.SystemDB, mysql.ColumnPrivTable, asgns, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			trace_util_0.Count(_revoke_00000, 59)
			return err
		}
	}
	trace_util_0.Count(_revoke_00000, 52)
	return nil
}

var _revoke_00000 = "executor/revoke.go"
