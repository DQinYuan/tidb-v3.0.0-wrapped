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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

/***
 * Grant Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/grant.html
 ************************************************************************************/
var (
	_ Executor = (*GrantExec)(nil)
)

// GrantExec executes GrantStmt.
type GrantExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec
	WithGrant  bool

	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *GrantExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_grant_00000, 0)
	if e.done {
		trace_util_0.Count(_grant_00000, 4)
		return nil
	}
	trace_util_0.Count(_grant_00000, 1)
	e.done = true

	dbName := e.Level.DBName
	if len(dbName) == 0 {
		trace_util_0.Count(_grant_00000, 5)
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	// Grant for each user
	trace_util_0.Count(_grant_00000, 2)
	for idx, user := range e.Users {
		trace_util_0.Count(_grant_00000, 6)
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			trace_util_0.Count(_grant_00000, 12)
			return err
		}
		trace_util_0.Count(_grant_00000, 7)
		if !exists && e.ctx.GetSessionVars().SQLMode.HasNoAutoCreateUserMode() {
			trace_util_0.Count(_grant_00000, 13)
			return ErrCantCreateUserWithGrant
		} else {
			trace_util_0.Count(_grant_00000, 14)
			if !exists {
				trace_util_0.Count(_grant_00000, 15)
				pwd, ok := user.EncodedPassword()
				if !ok {
					trace_util_0.Count(_grant_00000, 17)
					return errors.Trace(ErrPasswordFormat)
				}
				trace_util_0.Count(_grant_00000, 16)
				user := fmt.Sprintf(`('%s', '%s', '%s')`, user.User.Hostname, user.User.Username, pwd)
				sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password) VALUES %s;`, mysql.SystemDB, mysql.UserTable, user)
				_, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
				if err != nil {
					trace_util_0.Count(_grant_00000, 18)
					return err
				}
			}
		}

		// If there is no privilege entry in corresponding table, insert a new one.
		// DB scope:		mysql.DB
		// Table scope:		mysql.Tables_priv
		// Column scope:	mysql.Columns_priv
		trace_util_0.Count(_grant_00000, 8)
		switch e.Level.Level {
		case ast.GrantLevelDB:
			trace_util_0.Count(_grant_00000, 19)
			err := checkAndInitDBPriv(e.ctx, dbName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				trace_util_0.Count(_grant_00000, 21)
				return err
			}
		case ast.GrantLevelTable:
			trace_util_0.Count(_grant_00000, 20)
			err := checkAndInitTablePriv(e.ctx, dbName, e.Level.TableName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				trace_util_0.Count(_grant_00000, 22)
				return err
			}
		}
		trace_util_0.Count(_grant_00000, 9)
		privs := e.Privs
		if e.WithGrant {
			trace_util_0.Count(_grant_00000, 23)
			privs = append(privs, &ast.PrivElem{Priv: mysql.GrantPriv})
		}

		trace_util_0.Count(_grant_00000, 10)
		if idx == 0 {
			trace_util_0.Count(_grant_00000, 24)
			// Commit the old transaction, like DDL.
			if err := e.ctx.NewTxn(ctx); err != nil {
				trace_util_0.Count(_grant_00000, 26)
				return err
			}
			trace_util_0.Count(_grant_00000, 25)
			defer func() {
				trace_util_0.Count(_grant_00000, 27)
				e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
			}()
		}

		// Grant each priv to the user.
		trace_util_0.Count(_grant_00000, 11)
		for _, priv := range privs {
			trace_util_0.Count(_grant_00000, 28)
			if len(priv.Cols) > 0 {
				trace_util_0.Count(_grant_00000, 30)
				// Check column scope privilege entry.
				// TODO: Check validity before insert new entry.
				err := e.checkAndInitColumnPriv(user.User.Username, user.User.Hostname, priv.Cols)
				if err != nil {
					trace_util_0.Count(_grant_00000, 31)
					return err
				}
			}
			trace_util_0.Count(_grant_00000, 29)
			err := e.grantPriv(priv, user)
			if err != nil {
				trace_util_0.Count(_grant_00000, 32)
				return err
			}
		}
	}
	trace_util_0.Count(_grant_00000, 3)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

// checkAndInitDBPriv checks if DB scope privilege entry exists in mysql.DB.
// If unexists, insert a new one.
func checkAndInitDBPriv(ctx sessionctx.Context, dbName string, is infoschema.InfoSchema, user string, host string) error {
	trace_util_0.Count(_grant_00000, 33)
	ok, err := dbUserExists(ctx, user, host, dbName)
	if err != nil {
		trace_util_0.Count(_grant_00000, 36)
		return err
	}
	trace_util_0.Count(_grant_00000, 34)
	if ok {
		trace_util_0.Count(_grant_00000, 37)
		return nil
	}
	// Entry does not exist for user-host-db. Insert a new entry.
	trace_util_0.Count(_grant_00000, 35)
	return initDBPrivEntry(ctx, user, host, dbName)
}

// checkAndInitTablePriv checks if table scope privilege entry exists in mysql.Tables_priv.
// If unexists, insert a new one.
func checkAndInitTablePriv(ctx sessionctx.Context, dbName, tblName string, is infoschema.InfoSchema, user string, host string) error {
	trace_util_0.Count(_grant_00000, 38)
	ok, err := tableUserExists(ctx, user, host, dbName, tblName)
	if err != nil {
		trace_util_0.Count(_grant_00000, 41)
		return err
	}
	trace_util_0.Count(_grant_00000, 39)
	if ok {
		trace_util_0.Count(_grant_00000, 42)
		return nil
	}
	// Entry does not exist for user-host-db-tbl. Insert a new entry.
	trace_util_0.Count(_grant_00000, 40)
	return initTablePrivEntry(ctx, user, host, dbName, tblName)
}

// checkAndInitColumnPriv checks if column scope privilege entry exists in mysql.Columns_priv.
// If unexists, insert a new one.
func (e *GrantExec) checkAndInitColumnPriv(user string, host string, cols []*ast.ColumnName) error {
	trace_util_0.Count(_grant_00000, 43)
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		trace_util_0.Count(_grant_00000, 46)
		return err
	}
	trace_util_0.Count(_grant_00000, 44)
	for _, c := range cols {
		trace_util_0.Count(_grant_00000, 47)
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			trace_util_0.Count(_grant_00000, 51)
			return errors.Errorf("Unknown column: %s", c.Name.O)
		}
		trace_util_0.Count(_grant_00000, 48)
		ok, err := columnPrivEntryExists(e.ctx, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			trace_util_0.Count(_grant_00000, 52)
			return err
		}
		trace_util_0.Count(_grant_00000, 49)
		if ok {
			trace_util_0.Count(_grant_00000, 53)
			continue
		}
		// Entry does not exist for user-host-db-tbl-col. Insert a new entry.
		trace_util_0.Count(_grant_00000, 50)
		err = initColumnPrivEntry(e.ctx, user, host, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			trace_util_0.Count(_grant_00000, 54)
			return err
		}
	}
	trace_util_0.Count(_grant_00000, 45)
	return nil
}

// initDBPrivEntry inserts a new row into mysql.DB with empty privilege.
func initDBPrivEntry(ctx sessionctx.Context, user string, host string, db string) error {
	trace_util_0.Count(_grant_00000, 55)
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB) VALUES ('%s', '%s', '%s')`, mysql.SystemDB, mysql.DBTable, host, user, db)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return err
}

// initTablePrivEntry inserts a new row into mysql.Tables_priv with empty privilege.
func initTablePrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string) error {
	trace_util_0.Count(_grant_00000, 56)
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB, Table_name, Table_priv, Column_priv) VALUES ('%s', '%s', '%s', '%s', '', '')`, mysql.SystemDB, mysql.TablePrivTable, host, user, db, tbl)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return err
}

// initColumnPrivEntry inserts a new row into mysql.Columns_priv with empty privilege.
func initColumnPrivEntry(ctx sessionctx.Context, user string, host string, db string, tbl string, col string) error {
	trace_util_0.Count(_grant_00000, 57)
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, DB, Table_name, Column_name, Column_priv) VALUES ('%s', '%s', '%s', '%s', '%s', '')`, mysql.SystemDB, mysql.ColumnPrivTable, host, user, db, tbl, col)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return err
}

// grantPriv grants priv to user in s.Level scope.
func (e *GrantExec) grantPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	trace_util_0.Count(_grant_00000, 58)
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		trace_util_0.Count(_grant_00000, 59)
		return e.grantGlobalPriv(priv, user)
	case ast.GrantLevelDB:
		trace_util_0.Count(_grant_00000, 60)
		return e.grantDBPriv(priv, user)
	case ast.GrantLevelTable:
		trace_util_0.Count(_grant_00000, 61)
		if len(priv.Cols) == 0 {
			trace_util_0.Count(_grant_00000, 64)
			return e.grantTablePriv(priv, user)
		}
		trace_util_0.Count(_grant_00000, 62)
		return e.grantColumnPriv(priv, user)
	default:
		trace_util_0.Count(_grant_00000, 63)
		return errors.Errorf("Unknown grant level: %#v", e.Level)
	}
}

// grantGlobalPriv manipulates mysql.user table.
func (e *GrantExec) grantGlobalPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	trace_util_0.Count(_grant_00000, 65)
	if priv.Priv == 0 {
		trace_util_0.Count(_grant_00000, 68)
		return nil
	}
	trace_util_0.Count(_grant_00000, 66)
	asgns, err := composeGlobalPrivUpdate(priv.Priv, "Y")
	if err != nil {
		trace_util_0.Count(_grant_00000, 69)
		return err
	}
	trace_util_0.Count(_grant_00000, 67)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s'`, mysql.SystemDB, mysql.UserTable, asgns, user.User.Username, user.User.Hostname)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

// grantDBPriv manipulates mysql.db table.
func (e *GrantExec) grantDBPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	trace_util_0.Count(_grant_00000, 70)
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		trace_util_0.Count(_grant_00000, 73)
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	trace_util_0.Count(_grant_00000, 71)
	asgns, err := composeDBPrivUpdate(priv.Priv, "Y")
	if err != nil {
		trace_util_0.Count(_grant_00000, 74)
		return err
	}
	trace_util_0.Count(_grant_00000, 72)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s';`, mysql.SystemDB, mysql.DBTable, asgns, user.User.Username, user.User.Hostname, dbName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

// grantTablePriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantTablePriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	trace_util_0.Count(_grant_00000, 75)
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		trace_util_0.Count(_grant_00000, 78)
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	trace_util_0.Count(_grant_00000, 76)
	tblName := e.Level.TableName
	asgns, err := composeTablePrivUpdateForGrant(e.ctx, priv.Priv, user.User.Username, user.User.Hostname, dbName, tblName)
	if err != nil {
		trace_util_0.Count(_grant_00000, 79)
		return err
	}
	trace_util_0.Count(_grant_00000, 77)
	sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s';`, mysql.SystemDB, mysql.TablePrivTable, asgns, user.User.Username, user.User.Hostname, dbName, tblName)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	return err
}

// grantColumnPriv manipulates mysql.tables_priv table.
func (e *GrantExec) grantColumnPriv(priv *ast.PrivElem, user *ast.UserSpec) error {
	trace_util_0.Count(_grant_00000, 80)
	dbName, tbl, err := getTargetSchemaAndTable(e.ctx, e.Level.DBName, e.Level.TableName, e.is)
	if err != nil {
		trace_util_0.Count(_grant_00000, 83)
		return err
	}

	trace_util_0.Count(_grant_00000, 81)
	for _, c := range priv.Cols {
		trace_util_0.Count(_grant_00000, 84)
		col := table.FindCol(tbl.Cols(), c.Name.L)
		if col == nil {
			trace_util_0.Count(_grant_00000, 87)
			return errors.Errorf("Unknown column: %s", c)
		}
		trace_util_0.Count(_grant_00000, 85)
		asgns, err := composeColumnPrivUpdateForGrant(e.ctx, priv.Priv, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		if err != nil {
			trace_util_0.Count(_grant_00000, 88)
			return err
		}
		trace_util_0.Count(_grant_00000, 86)
		sql := fmt.Sprintf(`UPDATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s' AND Column_name='%s';`, mysql.SystemDB, mysql.ColumnPrivTable, asgns, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, col.Name.O)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			trace_util_0.Count(_grant_00000, 89)
			return err
		}
	}
	trace_util_0.Count(_grant_00000, 82)
	return nil
}

// composeGlobalPrivUpdate composes update stmt assignment list string for global scope privilege update.
func composeGlobalPrivUpdate(priv mysql.PrivilegeType, value string) (string, error) {
	trace_util_0.Count(_grant_00000, 90)
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 93)
		strs := make([]string, 0, len(mysql.Priv2UserCol))
		for _, v := range mysql.Priv2UserCol {
			trace_util_0.Count(_grant_00000, 95)
			strs = append(strs, fmt.Sprintf(`%s='%s'`, v, value))
		}
		trace_util_0.Count(_grant_00000, 94)
		return strings.Join(strs, ", "), nil
	}
	trace_util_0.Count(_grant_00000, 91)
	col, ok := mysql.Priv2UserCol[priv]
	if !ok {
		trace_util_0.Count(_grant_00000, 96)
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	trace_util_0.Count(_grant_00000, 92)
	return fmt.Sprintf(`%s='%s'`, col, value), nil
}

// composeDBPrivUpdate composes update stmt assignment list for db scope privilege update.
func composeDBPrivUpdate(priv mysql.PrivilegeType, value string) (string, error) {
	trace_util_0.Count(_grant_00000, 97)
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 100)
		strs := make([]string, 0, len(mysql.AllDBPrivs))
		for _, p := range mysql.AllDBPrivs {
			trace_util_0.Count(_grant_00000, 102)
			v, ok := mysql.Priv2UserCol[p]
			if !ok {
				trace_util_0.Count(_grant_00000, 104)
				return "", errors.Errorf("Unknown db privilege %v", priv)
			}
			trace_util_0.Count(_grant_00000, 103)
			strs = append(strs, fmt.Sprintf(`%s='%s'`, v, value))
		}
		trace_util_0.Count(_grant_00000, 101)
		return strings.Join(strs, ", "), nil
	}
	trace_util_0.Count(_grant_00000, 98)
	col, ok := mysql.Priv2UserCol[priv]
	if !ok {
		trace_util_0.Count(_grant_00000, 105)
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	trace_util_0.Count(_grant_00000, 99)
	return fmt.Sprintf(`%s='%s'`, col, value), nil
}

// composeTablePrivUpdateForGrant composes update stmt assignment list for table scope privilege update.
func composeTablePrivUpdateForGrant(ctx sessionctx.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string) (string, error) {
	trace_util_0.Count(_grant_00000, 106)
	var newTablePriv, newColumnPriv string
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 108)
		for _, p := range mysql.AllTablePrivs {
			trace_util_0.Count(_grant_00000, 110)
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				trace_util_0.Count(_grant_00000, 112)
				return "", errors.Errorf("Unknown table privilege %v", p)
			}
			trace_util_0.Count(_grant_00000, 111)
			newTablePriv = addToSet(newTablePriv, v)
		}
		trace_util_0.Count(_grant_00000, 109)
		for _, p := range mysql.AllColumnPrivs {
			trace_util_0.Count(_grant_00000, 113)
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				trace_util_0.Count(_grant_00000, 115)
				return "", errors.Errorf("Unknown column privilege %v", p)
			}
			trace_util_0.Count(_grant_00000, 114)
			newColumnPriv = addToSet(newColumnPriv, v)
		}
	} else {
		trace_util_0.Count(_grant_00000, 116)
		{
			currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
			if err != nil {
				trace_util_0.Count(_grant_00000, 119)
				return "", err
			}
			trace_util_0.Count(_grant_00000, 117)
			p, ok := mysql.Priv2SetStr[priv]
			if !ok {
				trace_util_0.Count(_grant_00000, 120)
				return "", errors.Errorf("Unknown priv: %v", priv)
			}
			trace_util_0.Count(_grant_00000, 118)
			newTablePriv = addToSet(currTablePriv, p)

			for _, cp := range mysql.AllColumnPrivs {
				trace_util_0.Count(_grant_00000, 121)
				if priv == cp {
					trace_util_0.Count(_grant_00000, 122)
					newColumnPriv = addToSet(currColumnPriv, p)
					break
				}
			}
		}
	}
	trace_util_0.Count(_grant_00000, 107)
	return fmt.Sprintf(`Table_priv='%s', Column_priv='%s', Grantor='%s'`, newTablePriv, newColumnPriv, ctx.GetSessionVars().User), nil
}

func composeTablePrivUpdateForRevoke(ctx sessionctx.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string) (string, error) {
	trace_util_0.Count(_grant_00000, 123)
	var newTablePriv, newColumnPriv string
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 125)
		newTablePriv = ""
		newColumnPriv = ""
	} else {
		trace_util_0.Count(_grant_00000, 126)
		{
			currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
			if err != nil {
				trace_util_0.Count(_grant_00000, 129)
				return "", err
			}
			trace_util_0.Count(_grant_00000, 127)
			p, ok := mysql.Priv2SetStr[priv]
			if !ok {
				trace_util_0.Count(_grant_00000, 130)
				return "", errors.Errorf("Unknown priv: %v", priv)
			}
			trace_util_0.Count(_grant_00000, 128)
			newTablePriv = deleteFromSet(currTablePriv, p)

			for _, cp := range mysql.AllColumnPrivs {
				trace_util_0.Count(_grant_00000, 131)
				if priv == cp {
					trace_util_0.Count(_grant_00000, 132)
					newColumnPriv = deleteFromSet(currColumnPriv, p)
					break
				}
			}
		}
	}
	trace_util_0.Count(_grant_00000, 124)
	return fmt.Sprintf(`Table_priv='%s', Column_priv='%s', Grantor='%s'`, newTablePriv, newColumnPriv, ctx.GetSessionVars().User), nil
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert", "Update") returns "Select,Insert,Update".
func addToSet(set string, value string) string {
	trace_util_0.Count(_grant_00000, 133)
	if set == "" {
		trace_util_0.Count(_grant_00000, 135)
		return value
	}
	trace_util_0.Count(_grant_00000, 134)
	return fmt.Sprintf("%s,%s", set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set string, value string) string {
	trace_util_0.Count(_grant_00000, 136)
	sets := strings.Split(set, ",")
	res := make([]string, 0, len(sets))
	for _, v := range sets {
		trace_util_0.Count(_grant_00000, 138)
		if v != value {
			trace_util_0.Count(_grant_00000, 139)
			res = append(res, v)
		}
	}
	trace_util_0.Count(_grant_00000, 137)
	return strings.Join(res, ",")
}

// composeColumnPrivUpdateForGrant composes update stmt assignment list for column scope privilege update.
func composeColumnPrivUpdateForGrant(ctx sessionctx.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) (string, error) {
	trace_util_0.Count(_grant_00000, 140)
	newColumnPriv := ""
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 142)
		for _, p := range mysql.AllColumnPrivs {
			trace_util_0.Count(_grant_00000, 143)
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				trace_util_0.Count(_grant_00000, 145)
				return "", errors.Errorf("Unknown column privilege %v", p)
			}
			trace_util_0.Count(_grant_00000, 144)
			newColumnPriv = addToSet(newColumnPriv, v)
		}
	} else {
		trace_util_0.Count(_grant_00000, 146)
		{
			currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
			if err != nil {
				trace_util_0.Count(_grant_00000, 149)
				return "", err
			}
			trace_util_0.Count(_grant_00000, 147)
			p, ok := mysql.Priv2SetStr[priv]
			if !ok {
				trace_util_0.Count(_grant_00000, 150)
				return "", errors.Errorf("Unknown priv: %v", priv)
			}
			trace_util_0.Count(_grant_00000, 148)
			newColumnPriv = addToSet(currColumnPriv, p)
		}
	}
	trace_util_0.Count(_grant_00000, 141)
	return fmt.Sprintf(`Column_priv='%s'`, newColumnPriv), nil
}

func composeColumnPrivUpdateForRevoke(ctx sessionctx.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) (string, error) {
	trace_util_0.Count(_grant_00000, 151)
	newColumnPriv := ""
	if priv == mysql.AllPriv {
		trace_util_0.Count(_grant_00000, 153)
		newColumnPriv = ""
	} else {
		trace_util_0.Count(_grant_00000, 154)
		{
			currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
			if err != nil {
				trace_util_0.Count(_grant_00000, 157)
				return "", err
			}
			trace_util_0.Count(_grant_00000, 155)
			p, ok := mysql.Priv2SetStr[priv]
			if !ok {
				trace_util_0.Count(_grant_00000, 158)
				return "", errors.Errorf("Unknown priv: %v", priv)
			}
			trace_util_0.Count(_grant_00000, 156)
			newColumnPriv = deleteFromSet(currColumnPriv, p)
		}
	}
	trace_util_0.Count(_grant_00000, 152)
	return fmt.Sprintf(`Column_priv='%s'`, newColumnPriv), nil
}

// recordExists is a helper function to check if the sql returns any row.
func recordExists(ctx sessionctx.Context, sql string) (bool, error) {
	trace_util_0.Count(_grant_00000, 159)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_grant_00000, 161)
		return false, err
	}
	trace_util_0.Count(_grant_00000, 160)
	return len(rows) > 0, nil
}

// dbUserExists checks if there is an entry with key user-host-db in mysql.DB.
func dbUserExists(ctx sessionctx.Context, name string, host string, db string) (bool, error) {
	trace_util_0.Count(_grant_00000, 162)
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND DB='%s';`, mysql.SystemDB, mysql.DBTable, name, host, db)
	return recordExists(ctx, sql)
}

// tableUserExists checks if there is an entry with key user-host-db-tbl in mysql.Tables_priv.
func tableUserExists(ctx sessionctx.Context, name string, host string, db string, tbl string) (bool, error) {
	trace_util_0.Count(_grant_00000, 163)
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s';`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	return recordExists(ctx, sql)
}

// columnPrivEntryExists checks if there is an entry with key user-host-db-tbl-col in mysql.Columns_priv.
func columnPrivEntryExists(ctx sessionctx.Context, name string, host string, db string, tbl string, col string) (bool, error) {
	trace_util_0.Count(_grant_00000, 164)
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s' AND Column_name='%s';`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	return recordExists(ctx, sql)
}

// getTablePriv gets current table scope privilege set from mysql.Tables_priv.
// Return Table_priv and Column_priv.
func getTablePriv(ctx sessionctx.Context, name string, host string, db string, tbl string) (string, string, error) {
	trace_util_0.Count(_grant_00000, 165)
	sql := fmt.Sprintf(`SELECT Table_priv, Column_priv FROM %s.%s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s';`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_grant_00000, 170)
		return "", "", err
	}
	trace_util_0.Count(_grant_00000, 166)
	if len(rows) < 1 {
		trace_util_0.Count(_grant_00000, 171)
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s", name, host, db, tbl)
	}
	trace_util_0.Count(_grant_00000, 167)
	var tPriv, cPriv string
	row := rows[0]
	if fields[0].Column.Tp == mysql.TypeSet {
		trace_util_0.Count(_grant_00000, 172)
		tablePriv := row.GetSet(0)
		tPriv = tablePriv.Name
	}
	trace_util_0.Count(_grant_00000, 168)
	if fields[1].Column.Tp == mysql.TypeSet {
		trace_util_0.Count(_grant_00000, 173)
		columnPriv := row.GetSet(1)
		cPriv = columnPriv.Name
	}
	trace_util_0.Count(_grant_00000, 169)
	return tPriv, cPriv, nil
}

// getColumnPriv gets current column scope privilege set from mysql.Columns_priv.
// Return Column_priv.
func getColumnPriv(ctx sessionctx.Context, name string, host string, db string, tbl string, col string) (string, error) {
	trace_util_0.Count(_grant_00000, 174)
	sql := fmt.Sprintf(`SELECT Column_priv FROM %s.%s WHERE User='%s' AND Host='%s' AND DB='%s' AND Table_name='%s' AND Column_name='%s';`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_grant_00000, 178)
		return "", err
	}
	trace_util_0.Count(_grant_00000, 175)
	if len(rows) < 1 {
		trace_util_0.Count(_grant_00000, 179)
		return "", errors.Errorf("get column privilege fail for %s %s %s %s %s", name, host, db, tbl, col)
	}
	trace_util_0.Count(_grant_00000, 176)
	cPriv := ""
	if fields[0].Column.Tp == mysql.TypeSet {
		trace_util_0.Count(_grant_00000, 180)
		setVal := rows[0].GetSet(0)
		cPriv = setVal.Name
	}
	trace_util_0.Count(_grant_00000, 177)
	return cPriv, nil
}

// getTargetSchemaAndTable finds the schema and table by dbName and tableName.
func getTargetSchemaAndTable(ctx sessionctx.Context, dbName, tableName string, is infoschema.InfoSchema) (string, table.Table, error) {
	trace_util_0.Count(_grant_00000, 181)
	if len(dbName) == 0 {
		trace_util_0.Count(_grant_00000, 184)
		dbName = ctx.GetSessionVars().CurrentDB
		if len(dbName) == 0 {
			trace_util_0.Count(_grant_00000, 185)
			return "", nil, errors.New("miss DB name for grant privilege")
		}
	}
	trace_util_0.Count(_grant_00000, 182)
	name := model.NewCIStr(tableName)
	tbl, err := is.TableByName(model.NewCIStr(dbName), name)
	if err != nil {
		trace_util_0.Count(_grant_00000, 186)
		return "", nil, err
	}
	trace_util_0.Count(_grant_00000, 183)
	return dbName, tbl, nil
}

var _grant_00000 = "executor/grant.go"
