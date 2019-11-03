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
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	baseExecutor

	Statement ast.StmtNode
	done      bool
	is        infoschema.InfoSchema
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	trace_util_0.Count(_simple_00000, 0)
	if e.done {
		trace_util_0.Count(_simple_00000, 4)
		return nil
	}

	trace_util_0.Count(_simple_00000, 1)
	if e.autoNewTxn() {
		trace_util_0.Count(_simple_00000, 5)
		// Commit the old transaction, like DDL.
		if err := e.ctx.NewTxn(ctx); err != nil {
			trace_util_0.Count(_simple_00000, 7)
			return err
		}
		trace_util_0.Count(_simple_00000, 6)
		defer func() {
			trace_util_0.Count(_simple_00000, 8)
			e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
		}()
	}

	trace_util_0.Count(_simple_00000, 2)
	switch x := e.Statement.(type) {
	case *ast.GrantRoleStmt:
		trace_util_0.Count(_simple_00000, 9)
		err = e.executeGrantRole(x)
	case *ast.UseStmt:
		trace_util_0.Count(_simple_00000, 10)
		err = e.executeUse(x)
	case *ast.FlushStmt:
		trace_util_0.Count(_simple_00000, 11)
		err = e.executeFlush(x)
	case *ast.BeginStmt:
		trace_util_0.Count(_simple_00000, 12)
		err = e.executeBegin(ctx, x)
	case *ast.CommitStmt:
		trace_util_0.Count(_simple_00000, 13)
		e.executeCommit(x)
	case *ast.RollbackStmt:
		trace_util_0.Count(_simple_00000, 14)
		err = e.executeRollback(x)
	case *ast.CreateUserStmt:
		trace_util_0.Count(_simple_00000, 15)
		err = e.executeCreateUser(ctx, x)
	case *ast.AlterUserStmt:
		trace_util_0.Count(_simple_00000, 16)
		err = e.executeAlterUser(x)
	case *ast.DropUserStmt:
		trace_util_0.Count(_simple_00000, 17)
		err = e.executeDropUser(x)
	case *ast.SetPwdStmt:
		trace_util_0.Count(_simple_00000, 18)
		err = e.executeSetPwd(x)
	case *ast.KillStmt:
		trace_util_0.Count(_simple_00000, 19)
		err = e.executeKillStmt(x)
	case *ast.BinlogStmt:
		trace_util_0.Count(_simple_00000, 20)
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		trace_util_0.Count(_simple_00000, 21)
		err = e.executeDropStats(x)
	case *ast.SetRoleStmt:
		trace_util_0.Count(_simple_00000, 22)
		err = e.executeSetRole(x)
	case *ast.RevokeRoleStmt:
		trace_util_0.Count(_simple_00000, 23)
		err = e.executeRevokeRole(x)
	case *ast.SetDefaultRoleStmt:
		trace_util_0.Count(_simple_00000, 24)
		err = e.executeSetDefaultRole(x)
	}
	trace_util_0.Count(_simple_00000, 3)
	e.done = true
	return err
}

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	trace_util_0.Count(_simple_00000, 25)
	sqlExecutor := e.ctx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		trace_util_0.Count(_simple_00000, 29)
		return err
	}
	trace_util_0.Count(_simple_00000, 26)
	for _, u := range s.UserList {
		trace_util_0.Count(_simple_00000, 30)
		if u.Hostname == "" {
			trace_util_0.Count(_simple_00000, 32)
			u.Hostname = "%"
		}
		trace_util_0.Count(_simple_00000, 31)
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", u.Username, u.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 33)
			logutil.Logger(context.Background()).Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				trace_util_0.Count(_simple_00000, 35)
				return rollbackErr
			}
			trace_util_0.Count(_simple_00000, 34)
			return err
		}
	}
	trace_util_0.Count(_simple_00000, 27)
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		trace_util_0.Count(_simple_00000, 36)
		return err
	}
	trace_util_0.Count(_simple_00000, 28)
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(s *ast.SetDefaultRoleStmt) error {
	trace_util_0.Count(_simple_00000, 37)
	for _, user := range s.UserList {
		trace_util_0.Count(_simple_00000, 43)
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 45)
			return err
		}
		trace_util_0.Count(_simple_00000, 44)
		if !exists {
			trace_util_0.Count(_simple_00000, 46)
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	trace_util_0.Count(_simple_00000, 38)
	for _, role := range s.RoleList {
		trace_util_0.Count(_simple_00000, 47)
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 49)
			return err
		}
		trace_util_0.Count(_simple_00000, 48)
		if !exists {
			trace_util_0.Count(_simple_00000, 50)
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}
	trace_util_0.Count(_simple_00000, 39)
	sqlExecutor := e.ctx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		trace_util_0.Count(_simple_00000, 51)
		return err
	}
	trace_util_0.Count(_simple_00000, 40)
	for _, user := range s.UserList {
		trace_util_0.Count(_simple_00000, 52)
		if user.Hostname == "" {
			trace_util_0.Count(_simple_00000, 55)
			user.Hostname = "%"
		}
		trace_util_0.Count(_simple_00000, 53)
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 56)
			logutil.Logger(context.Background()).Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				trace_util_0.Count(_simple_00000, 58)
				return rollbackErr
			}
			trace_util_0.Count(_simple_00000, 57)
			return err
		}
		trace_util_0.Count(_simple_00000, 54)
		for _, role := range s.RoleList {
			trace_util_0.Count(_simple_00000, 59)
			sql := fmt.Sprintf("INSERT IGNORE INTO mysql.default_roles values('%s', '%s', '%s', '%s');", user.Hostname, user.Username, role.Hostname, role.Username)
			checker := privilege.GetPrivilegeManager(e.ctx)
			ok := checker.FindEdge(e.ctx, role, user)
			if ok {
				trace_util_0.Count(_simple_00000, 60)
				if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
					trace_util_0.Count(_simple_00000, 61)
					logutil.Logger(context.Background()).Error(fmt.Sprintf("Error occur when executing %s", sql))
					if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
						trace_util_0.Count(_simple_00000, 63)
						return rollbackErr
					}
					trace_util_0.Count(_simple_00000, 62)
					return err
				}
			} else {
				trace_util_0.Count(_simple_00000, 64)
				{
					if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
						trace_util_0.Count(_simple_00000, 66)
						return rollbackErr
					}
					trace_util_0.Count(_simple_00000, 65)
					return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
				}
			}
		}
	}
	trace_util_0.Count(_simple_00000, 41)
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		trace_util_0.Count(_simple_00000, 67)
		return err
	}
	trace_util_0.Count(_simple_00000, 42)
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(s *ast.SetDefaultRoleStmt) error {
	trace_util_0.Count(_simple_00000, 68)
	for _, user := range s.UserList {
		trace_util_0.Count(_simple_00000, 73)
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 75)
			return err
		}
		trace_util_0.Count(_simple_00000, 74)
		if !exists {
			trace_util_0.Count(_simple_00000, 76)
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	trace_util_0.Count(_simple_00000, 69)
	sqlExecutor := e.ctx.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		trace_util_0.Count(_simple_00000, 77)
		return err
	}
	trace_util_0.Count(_simple_00000, 70)
	for _, user := range s.UserList {
		trace_util_0.Count(_simple_00000, 78)
		if user.Hostname == "" {
			trace_util_0.Count(_simple_00000, 81)
			user.Hostname = "%"
		}
		trace_util_0.Count(_simple_00000, 79)
		sql := fmt.Sprintf("DELETE IGNORE FROM mysql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 82)
			logutil.Logger(context.Background()).Error(fmt.Sprintf("Error occur when executing %s", sql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				trace_util_0.Count(_simple_00000, 84)
				return rollbackErr
			}
			trace_util_0.Count(_simple_00000, 83)
			return err
		}
		trace_util_0.Count(_simple_00000, 80)
		sql = fmt.Sprintf("INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) "+
			"SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM mysql.role_edges WHERE TO_HOST='%s' AND TO_USER='%s';", user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 85)
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				trace_util_0.Count(_simple_00000, 87)
				return rollbackErr
			}
			trace_util_0.Count(_simple_00000, 86)
			return err
		}
	}
	trace_util_0.Count(_simple_00000, 71)
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		trace_util_0.Count(_simple_00000, 88)
		return err
	}
	trace_util_0.Count(_simple_00000, 72)
	return nil
}

func (e *SimpleExec) executeSetDefaultRole(s *ast.SetDefaultRoleStmt) (err error) {
	trace_util_0.Count(_simple_00000, 89)
	switch s.SetRoleOpt {
	case ast.SetRoleAll:
		trace_util_0.Count(_simple_00000, 92)
		err = e.setDefaultRoleAll(s)
	case ast.SetRoleNone:
		trace_util_0.Count(_simple_00000, 93)
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		trace_util_0.Count(_simple_00000, 94)
		err = e.setDefaultRoleRegular(s)
	}
	trace_util_0.Count(_simple_00000, 90)
	if err != nil {
		trace_util_0.Count(_simple_00000, 95)
		return
	}
	trace_util_0.Count(_simple_00000, 91)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return
}

func (e *SimpleExec) setRoleRegular(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 96)
	// Deal with SQL like `SET ROLE role1, role2;`
	checkDup := make(map[string]*auth.RoleIdentity, len(s.RoleList))
	// Check whether RoleNameList contain duplicate role name.
	for _, r := range s.RoleList {
		trace_util_0.Count(_simple_00000, 100)
		key := r.String()
		checkDup[key] = r
	}
	trace_util_0.Count(_simple_00000, 97)
	roleList := make([]*auth.RoleIdentity, 0, 10)
	for _, v := range checkDup {
		trace_util_0.Count(_simple_00000, 101)
		roleList = append(roleList, v)
	}

	trace_util_0.Count(_simple_00000, 98)
	checker := privilege.GetPrivilegeManager(e.ctx)
	ok, roleName := checker.ActiveRoles(e.ctx, roleList)
	if !ok {
		trace_util_0.Count(_simple_00000, 102)
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	trace_util_0.Count(_simple_00000, 99)
	return nil
}

func (e *SimpleExec) setRoleAll(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 103)
	// Deal with SQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		trace_util_0.Count(_simple_00000, 105)
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	trace_util_0.Count(_simple_00000, 104)
	return nil
}

func (e *SimpleExec) setRoleAllExcept(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 106)
	// Deal with SQL like `SET ROLE ALL EXCEPT role1, role2;`
	for _, r := range s.RoleList {
		trace_util_0.Count(_simple_00000, 111)
		if r.Hostname == "" {
			trace_util_0.Count(_simple_00000, 112)
			r.Hostname = "%"
		}
	}
	trace_util_0.Count(_simple_00000, 107)
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)

	filter := func(arr []*auth.RoleIdentity, f func(*auth.RoleIdentity) bool) []*auth.RoleIdentity {
		trace_util_0.Count(_simple_00000, 113)
		i, j := 0, 0
		for i = 0; i < len(arr); i++ {
			trace_util_0.Count(_simple_00000, 115)
			if f(arr[i]) {
				trace_util_0.Count(_simple_00000, 116)
				arr[j] = arr[i]
				j++
			}
		}
		trace_util_0.Count(_simple_00000, 114)
		return arr[:j]
	}
	trace_util_0.Count(_simple_00000, 108)
	banned := func(r *auth.RoleIdentity) bool {
		trace_util_0.Count(_simple_00000, 117)
		for _, ban := range s.RoleList {
			trace_util_0.Count(_simple_00000, 119)
			if ban.Hostname == r.Hostname && ban.Username == r.Username {
				trace_util_0.Count(_simple_00000, 120)
				return false
			}
		}
		trace_util_0.Count(_simple_00000, 118)
		return true
	}

	trace_util_0.Count(_simple_00000, 109)
	afterExcept := filter(roles, banned)
	ok, roleName := checker.ActiveRoles(e.ctx, afterExcept)
	if !ok {
		trace_util_0.Count(_simple_00000, 121)
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	trace_util_0.Count(_simple_00000, 110)
	return nil
}

func (e *SimpleExec) setRoleDefault(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 122)
	// Deal with SQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetSessionVars().User.AuthUsername, e.ctx.GetSessionVars().User.AuthHostname
	roles := checker.GetDefaultRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		trace_util_0.Count(_simple_00000, 124)
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	trace_util_0.Count(_simple_00000, 123)
	return nil
}

func (e *SimpleExec) setRoleNone(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 125)
	// Deal with SQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		trace_util_0.Count(_simple_00000, 127)
		u := e.ctx.GetSessionVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	trace_util_0.Count(_simple_00000, 126)
	return nil
}

func (e *SimpleExec) executeSetRole(s *ast.SetRoleStmt) error {
	trace_util_0.Count(_simple_00000, 128)
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		trace_util_0.Count(_simple_00000, 130)
		return e.setRoleRegular(s)
	case ast.SetRoleAll:
		trace_util_0.Count(_simple_00000, 131)
		return e.setRoleAll(s)
	case ast.SetRoleAllExcept:
		trace_util_0.Count(_simple_00000, 132)
		return e.setRoleAllExcept(s)
	case ast.SetRoleNone:
		trace_util_0.Count(_simple_00000, 133)
		return e.setRoleNone(s)
	case ast.SetRoleDefault:
		trace_util_0.Count(_simple_00000, 134)
		return e.setRoleDefault(s)
	}
	trace_util_0.Count(_simple_00000, 129)
	return nil
}

func (e *SimpleExec) dbAccessDenied(dbname string) error {
	trace_util_0.Count(_simple_00000, 135)
	user := e.ctx.GetSessionVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		trace_util_0.Count(_simple_00000, 137)
		u = user.AuthUsername
		h = user.AuthHostname
	}
	trace_util_0.Count(_simple_00000, 136)
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, dbname)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	trace_util_0.Count(_simple_00000, 138)
	dbname := model.NewCIStr(s.DBName)

	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		trace_util_0.Count(_simple_00000, 141)
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, dbname.String()) {
			trace_util_0.Count(_simple_00000, 142)
			return e.dbAccessDenied(dbname.O)
		}
	}

	trace_util_0.Count(_simple_00000, 139)
	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		trace_util_0.Count(_simple_00000, 143)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	trace_util_0.Count(_simple_00000, 140)
	e.ctx.GetSessionVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := e.ctx.GetSessionVars()
	terror.Log(sessionVars.SetSystemVar(variable.CharsetDatabase, dbinfo.Charset))
	terror.Log(sessionVars.SetSystemVar(variable.CollationDatabase, dbinfo.Collate))
	return nil
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	trace_util_0.Count(_simple_00000, 144)
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	if txnCtx.History != nil {
		trace_util_0.Count(_simple_00000, 149)
		err := e.ctx.NewTxn(ctx)
		if err != nil {
			trace_util_0.Count(_simple_00000, 150)
			return err
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	trace_util_0.Count(_simple_00000, 145)
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	// Call ctx.Txn(true) to active pending txn.
	pTxnConf := config.GetGlobalConfig().PessimisticTxn
	if pTxnConf.Enable {
		trace_util_0.Count(_simple_00000, 151)
		txnMode := s.Mode
		if txnMode == "" {
			trace_util_0.Count(_simple_00000, 153)
			txnMode = e.ctx.GetSessionVars().TxnMode
			if txnMode == "" && pTxnConf.Default {
				trace_util_0.Count(_simple_00000, 154)
				txnMode = ast.Pessimistic
			}
		}
		trace_util_0.Count(_simple_00000, 152)
		if txnMode == ast.Pessimistic {
			trace_util_0.Count(_simple_00000, 155)
			e.ctx.GetSessionVars().TxnCtx.IsPessimistic = true
		}
	}
	trace_util_0.Count(_simple_00000, 146)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_simple_00000, 156)
		return err
	}
	trace_util_0.Count(_simple_00000, 147)
	if e.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		trace_util_0.Count(_simple_00000, 157)
		txn.SetOption(kv.Pessimistic, true)
	}
	trace_util_0.Count(_simple_00000, 148)
	return nil
}

func (e *SimpleExec) executeRevokeRole(s *ast.RevokeRoleStmt) error {
	trace_util_0.Count(_simple_00000, 158)
	for _, role := range s.Roles {
		trace_util_0.Count(_simple_00000, 163)
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 165)
			return errors.Trace(err)
		}
		trace_util_0.Count(_simple_00000, 164)
		if !exists {
			trace_util_0.Count(_simple_00000, 166)
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	// begin a transaction to insert role graph edges.
	trace_util_0.Count(_simple_00000, 159)
	if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "begin"); err != nil {
		trace_util_0.Count(_simple_00000, 167)
		return errors.Trace(err)
	}
	trace_util_0.Count(_simple_00000, 160)
	for _, user := range s.Users {
		trace_util_0.Count(_simple_00000, 168)
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 171)
			return errors.Trace(err)
		}
		trace_util_0.Count(_simple_00000, 169)
		if !exists {
			trace_util_0.Count(_simple_00000, 172)
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 174)
				return errors.Trace(err)
			}
			trace_util_0.Count(_simple_00000, 173)
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		trace_util_0.Count(_simple_00000, 170)
		for _, role := range s.Roles {
			trace_util_0.Count(_simple_00000, 175)
			if role.Hostname == "" {
				trace_util_0.Count(_simple_00000, 177)
				role.Hostname = "%"
			}
			trace_util_0.Count(_simple_00000, 176)
			sql := fmt.Sprintf(`DELETE IGNORE FROM %s.%s WHERE FROM_HOST='%s' and FROM_USER='%s' and TO_HOST='%s' and TO_USER='%s'`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
				trace_util_0.Count(_simple_00000, 178)
				if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
					trace_util_0.Count(_simple_00000, 180)
					return errors.Trace(err)
				}
				trace_util_0.Count(_simple_00000, 179)
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}
		}
	}
	trace_util_0.Count(_simple_00000, 161)
	if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "commit"); err != nil {
		trace_util_0.Count(_simple_00000, 181)
		return err
	}
	trace_util_0.Count(_simple_00000, 162)
	err := domain.GetDomain(e.ctx).PrivilegeHandle().Update(e.ctx.(sessionctx.Context))
	return errors.Trace(err)
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	trace_util_0.Count(_simple_00000, 182)
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	trace_util_0.Count(_simple_00000, 183)
	sessVars := e.ctx.GetSessionVars()
	logutil.Logger(context.Background()).Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_simple_00000, 186)
		return err
	}
	trace_util_0.Count(_simple_00000, 184)
	if txn.Valid() {
		trace_util_0.Count(_simple_00000, 187)
		e.ctx.GetSessionVars().TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	trace_util_0.Count(_simple_00000, 185)
	return nil
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) error {
	trace_util_0.Count(_simple_00000, 188)
	users := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		trace_util_0.Count(_simple_00000, 193)
		exists, err1 := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			trace_util_0.Count(_simple_00000, 198)
			return err1
		}
		trace_util_0.Count(_simple_00000, 194)
		if exists {
			trace_util_0.Count(_simple_00000, 199)
			if !s.IfNotExists {
				trace_util_0.Count(_simple_00000, 201)
				return errors.New("Duplicate user")
			}
			trace_util_0.Count(_simple_00000, 200)
			continue
		}
		trace_util_0.Count(_simple_00000, 195)
		pwd, ok := spec.EncodedPassword()
		if !ok {
			trace_util_0.Count(_simple_00000, 202)
			return errors.Trace(ErrPasswordFormat)
		}
		trace_util_0.Count(_simple_00000, 196)
		user := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, pwd)
		if s.IsCreateRole {
			trace_util_0.Count(_simple_00000, 203)
			user = fmt.Sprintf(`('%s', '%s', '%s', 'Y')`, spec.User.Hostname, spec.User.Username, pwd)
		}
		trace_util_0.Count(_simple_00000, 197)
		users = append(users, user)
	}
	trace_util_0.Count(_simple_00000, 189)
	if len(users) == 0 {
		trace_util_0.Count(_simple_00000, 204)
		return nil
	}

	trace_util_0.Count(_simple_00000, 190)
	sql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	if s.IsCreateRole {
		trace_util_0.Count(_simple_00000, 205)
		sql = fmt.Sprintf(`INSERT INTO %s.%s (Host, User, Password, Account_locked) VALUES %s;`, mysql.SystemDB, mysql.UserTable, strings.Join(users, ", "))
	}
	trace_util_0.Count(_simple_00000, 191)
	_, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_simple_00000, 206)
		return err
	}
	trace_util_0.Count(_simple_00000, 192)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeAlterUser(s *ast.AlterUserStmt) error {
	trace_util_0.Count(_simple_00000, 207)
	if s.CurrentAuth != nil {
		trace_util_0.Count(_simple_00000, 211)
		user := e.ctx.GetSessionVars().User
		if user == nil {
			trace_util_0.Count(_simple_00000, 213)
			return errors.New("Session user is empty")
		}
		trace_util_0.Count(_simple_00000, 212)
		spec := &ast.UserSpec{
			User:    user,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	trace_util_0.Count(_simple_00000, 208)
	failedUsers := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		trace_util_0.Count(_simple_00000, 214)
		exists, err := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 218)
			return err
		}
		trace_util_0.Count(_simple_00000, 215)
		if !exists {
			trace_util_0.Count(_simple_00000, 219)
			failedUsers = append(failedUsers, spec.User.String())
			// TODO: Make this error as a warning.
			// if s.IfExists {
			// }
			continue
		}
		trace_util_0.Count(_simple_00000, 216)
		pwd := ""
		if spec.AuthOpt != nil {
			trace_util_0.Count(_simple_00000, 220)
			if spec.AuthOpt.ByAuthString {
				trace_util_0.Count(_simple_00000, 221)
				pwd = auth.EncodePassword(spec.AuthOpt.AuthString)
			} else {
				trace_util_0.Count(_simple_00000, 222)
				{
					pwd = auth.EncodePassword(spec.AuthOpt.HashString)
				}
			}
		}
		trace_util_0.Count(_simple_00000, 217)
		sql := fmt.Sprintf(`UPDATE %s.%s SET Password = '%s' WHERE Host = '%s' and User = '%s';`,
			mysql.SystemDB, mysql.UserTable, pwd, spec.User.Hostname, spec.User.Username)
		_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
		if err != nil {
			trace_util_0.Count(_simple_00000, 223)
			failedUsers = append(failedUsers, spec.User.String())
		}
	}
	trace_util_0.Count(_simple_00000, 209)
	if len(failedUsers) > 0 {
		trace_util_0.Count(_simple_00000, 224)
		// Commit the transaction even if we returns error
		txn, err := e.ctx.Txn(true)
		if err != nil {
			trace_util_0.Count(_simple_00000, 227)
			return err
		}
		trace_util_0.Count(_simple_00000, 225)
		err = txn.Commit(sessionctx.SetCommitCtx(context.Background(), e.ctx))
		if err != nil {
			trace_util_0.Count(_simple_00000, 228)
			return err
		}
		trace_util_0.Count(_simple_00000, 226)
		return ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
	}
	trace_util_0.Count(_simple_00000, 210)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeGrantRole(s *ast.GrantRoleStmt) error {
	trace_util_0.Count(_simple_00000, 229)
	failedUsers := make([]string, 0, len(s.Users))
	for _, role := range s.Roles {
		trace_util_0.Count(_simple_00000, 235)
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 237)
			return err
		}
		trace_util_0.Count(_simple_00000, 236)
		if !exists {
			trace_util_0.Count(_simple_00000, 238)
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", role.String())
		}
	}
	trace_util_0.Count(_simple_00000, 230)
	for _, user := range s.Users {
		trace_util_0.Count(_simple_00000, 239)
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 241)
			return err
		}
		trace_util_0.Count(_simple_00000, 240)
		if !exists {
			trace_util_0.Count(_simple_00000, 242)
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
		}
	}

	// begin a transaction to insert role graph edges.
	trace_util_0.Count(_simple_00000, 231)
	if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "begin"); err != nil {
		trace_util_0.Count(_simple_00000, 243)
		return err
	}

	trace_util_0.Count(_simple_00000, 232)
	for _, user := range s.Users {
		trace_util_0.Count(_simple_00000, 244)
		for _, role := range s.Roles {
			trace_util_0.Count(_simple_00000, 245)
			sql := fmt.Sprintf(`INSERT IGNORE INTO %s.%s (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ('%s','%s','%s','%s')`, mysql.SystemDB, mysql.RoleEdgeTable, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
				trace_util_0.Count(_simple_00000, 246)
				failedUsers = append(failedUsers, user.String())
				logutil.Logger(context.Background()).Error(fmt.Sprintf("Error occur when executing %s", sql))
				if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
					trace_util_0.Count(_simple_00000, 248)
					return err
				}
				trace_util_0.Count(_simple_00000, 247)
				return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	trace_util_0.Count(_simple_00000, 233)
	if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "commit"); err != nil {
		trace_util_0.Count(_simple_00000, 249)
		return err
	}
	trace_util_0.Count(_simple_00000, 234)
	err := domain.GetDomain(e.ctx).PrivilegeHandle().Update(e.ctx.(sessionctx.Context))
	return err
}

func (e *SimpleExec) executeDropUser(s *ast.DropUserStmt) error {
	trace_util_0.Count(_simple_00000, 250)
	failedUsers := make([]string, 0, len(s.UserList))
	notExistUsers := make([]string, 0, len(s.UserList))

	for _, user := range s.UserList {
		trace_util_0.Count(_simple_00000, 254)
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			trace_util_0.Count(_simple_00000, 265)
			return err
		}
		trace_util_0.Count(_simple_00000, 255)
		if !exists {
			trace_util_0.Count(_simple_00000, 266)
			notExistUsers = append(notExistUsers, user.String())
			continue
		}

		// begin a transaction to delete a user.
		trace_util_0.Count(_simple_00000, 256)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "begin"); err != nil {
			trace_util_0.Count(_simple_00000, 267)
			return err
		}
		trace_util_0.Count(_simple_00000, 257)
		sql := fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.UserTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 268)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 270)
				return err
			}
			trace_util_0.Count(_simple_00000, 269)
			continue
		}

		// delete privileges from mysql.db
		trace_util_0.Count(_simple_00000, 258)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.DBTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 271)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 273)
				return err
			}
			trace_util_0.Count(_simple_00000, 272)
			continue
		}

		// delete privileges from mysql.tables_priv
		trace_util_0.Count(_simple_00000, 259)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, mysql.SystemDB, mysql.TablePrivTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 274)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 276)
				return err
			}
			trace_util_0.Count(_simple_00000, 275)
			continue
		}

		// delete relationship from mysql.role_edges
		trace_util_0.Count(_simple_00000, 260)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE TO_HOST = '%s' and TO_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 277)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 279)
				return err
			}
			trace_util_0.Count(_simple_00000, 278)
			continue
		}

		trace_util_0.Count(_simple_00000, 261)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE FROM_HOST = '%s' and FROM_USER = '%s';`, mysql.SystemDB, mysql.RoleEdgeTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 280)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 282)
				return err
			}
			trace_util_0.Count(_simple_00000, 281)
			continue
		}

		// delete relationship from mysql.default_roles
		trace_util_0.Count(_simple_00000, 262)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE DEFAULT_ROLE_HOST = '%s' and DEFAULT_ROLE_USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 283)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 285)
				return err
			}
			trace_util_0.Count(_simple_00000, 284)
			continue
		}

		trace_util_0.Count(_simple_00000, 263)
		sql = fmt.Sprintf(`DELETE FROM %s.%s WHERE HOST = '%s' and USER = '%s';`, mysql.SystemDB, mysql.DefaultRoleTable, user.Hostname, user.Username)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql); err != nil {
			trace_util_0.Count(_simple_00000, 286)
			failedUsers = append(failedUsers, user.String())
			if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "rollback"); err != nil {
				trace_util_0.Count(_simple_00000, 288)
				return err
			}
			trace_util_0.Count(_simple_00000, 287)
			continue
		}

		//TODO: need delete columns_priv once we implement columns_priv functionality.
		trace_util_0.Count(_simple_00000, 264)
		if _, err := e.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "commit"); err != nil {
			trace_util_0.Count(_simple_00000, 289)
			failedUsers = append(failedUsers, user.String())
		}
	}

	trace_util_0.Count(_simple_00000, 251)
	if len(notExistUsers) > 0 {
		trace_util_0.Count(_simple_00000, 290)
		if s.IfExists {
			trace_util_0.Count(_simple_00000, 291)
			for _, user := range notExistUsers {
				trace_util_0.Count(_simple_00000, 292)
				e.ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrUserDropExists.GenWithStackByArgs(user))
			}
		} else {
			trace_util_0.Count(_simple_00000, 293)
			{
				failedUsers = append(failedUsers, notExistUsers...)
			}
		}
	}

	trace_util_0.Count(_simple_00000, 252)
	if len(failedUsers) > 0 {
		trace_util_0.Count(_simple_00000, 294)
		return ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	trace_util_0.Count(_simple_00000, 253)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

func userExists(ctx sessionctx.Context, name string, host string) (bool, error) {
	trace_util_0.Count(_simple_00000, 295)
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`, mysql.SystemDB, mysql.UserTable, name, host)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_simple_00000, 297)
		return false, err
	}
	trace_util_0.Count(_simple_00000, 296)
	return len(rows) > 0, nil
}

func (e *SimpleExec) executeSetPwd(s *ast.SetPwdStmt) error {
	trace_util_0.Count(_simple_00000, 298)
	var u, h string
	if s.User == nil {
		trace_util_0.Count(_simple_00000, 302)
		if e.ctx.GetSessionVars().User == nil {
			trace_util_0.Count(_simple_00000, 304)
			return errors.New("Session error is empty")
		}
		trace_util_0.Count(_simple_00000, 303)
		u = e.ctx.GetSessionVars().User.AuthUsername
		h = e.ctx.GetSessionVars().User.AuthHostname
	} else {
		trace_util_0.Count(_simple_00000, 305)
		{
			checker := privilege.GetPrivilegeManager(e.ctx)
			activeRoles := e.ctx.GetSessionVars().ActiveRoles
			if checker != nil && !checker.RequestVerification(activeRoles, "", "", "", mysql.SuperPriv) {
				trace_util_0.Count(_simple_00000, 307)
				return ErrDBaccessDenied.GenWithStackByArgs(u, h, "mysql")
			}
			trace_util_0.Count(_simple_00000, 306)
			u = s.User.Username
			h = s.User.Hostname
		}
	}
	trace_util_0.Count(_simple_00000, 299)
	exists, err := userExists(e.ctx, u, h)
	if err != nil {
		trace_util_0.Count(_simple_00000, 308)
		return err
	}
	trace_util_0.Count(_simple_00000, 300)
	if !exists {
		trace_util_0.Count(_simple_00000, 309)
		return errors.Trace(ErrPasswordNoMatch)
	}

	// update mysql.user
	trace_util_0.Count(_simple_00000, 301)
	sql := fmt.Sprintf(`UPDATE %s.%s SET password='%s' WHERE User='%s' AND Host='%s';`, mysql.SystemDB, mysql.UserTable, auth.EncodePassword(s.Password), u, h)
	_, _, err = e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeKillStmt(s *ast.KillStmt) error {
	trace_util_0.Count(_simple_00000, 310)
	conf := config.GetGlobalConfig()
	if s.TiDBExtension || conf.CompatibleKillQuery {
		trace_util_0.Count(_simple_00000, 312)
		sm := e.ctx.GetSessionManager()
		if sm == nil {
			trace_util_0.Count(_simple_00000, 314)
			return nil
		}
		trace_util_0.Count(_simple_00000, 313)
		sm.Kill(s.ConnectionID, s.Query)
	} else {
		trace_util_0.Count(_simple_00000, 315)
		{
			err := errors.New("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead")
			e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
	trace_util_0.Count(_simple_00000, 311)
	return nil
}

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	trace_util_0.Count(_simple_00000, 316)
	switch s.Tp {
	case ast.FlushTables:
		trace_util_0.Count(_simple_00000, 318)
		if s.ReadLock {
			trace_util_0.Count(_simple_00000, 322)
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		trace_util_0.Count(_simple_00000, 319)
		dom := domain.GetDomain(e.ctx)
		sysSessionPool := dom.SysSessionPool()
		ctx, err := sysSessionPool.Get()
		if err != nil {
			trace_util_0.Count(_simple_00000, 323)
			return err
		}
		trace_util_0.Count(_simple_00000, 320)
		defer sysSessionPool.Put(ctx)
		err = dom.PrivilegeHandle().Update(ctx.(sessionctx.Context))
		return err
	case ast.FlushTiDBPlugin:
		trace_util_0.Count(_simple_00000, 321)
		dom := domain.GetDomain(e.ctx)
		for _, pluginName := range s.Plugins {
			trace_util_0.Count(_simple_00000, 324)
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				trace_util_0.Count(_simple_00000, 325)
				return err
			}
		}
	}
	trace_util_0.Count(_simple_00000, 317)
	return nil
}

func (e *SimpleExec) executeDropStats(s *ast.DropStatsStmt) error {
	trace_util_0.Count(_simple_00000, 326)
	h := domain.GetDomain(e.ctx).StatsHandle()
	err := h.DeleteTableStatsFromKV(s.Table.TableInfo.ID)
	if err != nil {
		trace_util_0.Count(_simple_00000, 328)
		return err
	}
	trace_util_0.Count(_simple_00000, 327)
	return h.Update(GetInfoSchema(e.ctx))
}

func (e *SimpleExec) autoNewTxn() bool {
	trace_util_0.Count(_simple_00000, 329)
	switch e.Statement.(type) {
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt:
		trace_util_0.Count(_simple_00000, 331)
		return true
	}
	trace_util_0.Count(_simple_00000, 330)
	return false
}

var _simple_00000 = "executor/simple.go"
