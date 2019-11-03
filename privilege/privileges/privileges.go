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

package privileges

import (
	"context"
	"strings"

	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SkipWithGrant causes the server to start without using the privilege system at all.
var SkipWithGrant = false

var _ privilege.Manager = (*UserPrivileges)(nil)

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string
	*Handle
}

// RequestVerification implements the Manager interface.
func (p *UserPrivileges) RequestVerification(activeRoles []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType) bool {
	trace_util_0.Count(_privileges_00000, 0)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 4)
		return true
	}

	trace_util_0.Count(_privileges_00000, 1)
	if p.user == "" && p.host == "" {
		trace_util_0.Count(_privileges_00000, 5)
		return true
	}

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	trace_util_0.Count(_privileges_00000, 2)
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		trace_util_0.Count(_privileges_00000, 6)
		return true
	}

	trace_util_0.Count(_privileges_00000, 3)
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(activeRoles, p.user, p.host, db, table, column, priv)
}

// RequestVerificationWithUser implements the Manager interface.
func (p *UserPrivileges) RequestVerificationWithUser(db, table, column string, priv mysql.PrivilegeType, user *auth.UserIdentity) bool {
	trace_util_0.Count(_privileges_00000, 7)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 11)
		return true
	}

	trace_util_0.Count(_privileges_00000, 8)
	if user == nil {
		trace_util_0.Count(_privileges_00000, 12)
		return false
	}

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.mysql.com/doc/refman/5.7/en/information-schema.html
	trace_util_0.Count(_privileges_00000, 9)
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		trace_util_0.Count(_privileges_00000, 13)
		return true
	}

	trace_util_0.Count(_privileges_00000, 10)
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(nil, user.Username, user.Hostname, db, table, column, priv)
}

// GetEncodedPassword implements the Manager interface.
func (p *UserPrivileges) GetEncodedPassword(user, host string) string {
	trace_util_0.Count(_privileges_00000, 14)
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		trace_util_0.Count(_privileges_00000, 17)
		logutil.Logger(context.Background()).Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return ""
	}
	trace_util_0.Count(_privileges_00000, 15)
	pwd := record.Password
	if len(pwd) != 0 && len(pwd) != mysql.PWDHashLen+1 {
		trace_util_0.Count(_privileges_00000, 18)
		logutil.Logger(context.Background()).Error("user password from system DB not like sha1sum", zap.String("user", user))
		return ""
	}
	trace_util_0.Count(_privileges_00000, 16)
	return pwd
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host string, authentication, salt []byte) (u string, h string, success bool) {
	trace_util_0.Count(_privileges_00000, 19)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 28)
		p.user = user
		p.host = host
		success = true
		return
	}

	trace_util_0.Count(_privileges_00000, 20)
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		trace_util_0.Count(_privileges_00000, 29)
		logutil.Logger(context.Background()).Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return
	}

	trace_util_0.Count(_privileges_00000, 21)
	u = record.User
	h = record.Host

	// Login a locked account is not allowed.
	locked := record.AccountLocked
	if locked {
		trace_util_0.Count(_privileges_00000, 30)
		logutil.Logger(context.Background()).Error("try to login a locked account",
			zap.String("user", user), zap.String("host", host))
		success = false
		return
	}

	trace_util_0.Count(_privileges_00000, 22)
	pwd := record.Password
	if len(pwd) != 0 && len(pwd) != mysql.PWDHashLen+1 {
		trace_util_0.Count(_privileges_00000, 31)
		logutil.Logger(context.Background()).Error("user password from system DB not like sha1sum", zap.String("user", user))
		return
	}

	// empty password
	trace_util_0.Count(_privileges_00000, 23)
	if len(pwd) == 0 && len(authentication) == 0 {
		trace_util_0.Count(_privileges_00000, 32)
		p.user = user
		p.host = host
		success = true
		return
	}

	trace_util_0.Count(_privileges_00000, 24)
	if len(pwd) == 0 || len(authentication) == 0 {
		trace_util_0.Count(_privileges_00000, 33)
		return
	}

	trace_util_0.Count(_privileges_00000, 25)
	hpwd, err := auth.DecodePassword(pwd)
	if err != nil {
		trace_util_0.Count(_privileges_00000, 34)
		logutil.Logger(context.Background()).Error("decode password string failed", zap.Error(err))
		return
	}

	trace_util_0.Count(_privileges_00000, 26)
	if !auth.CheckScrambledPassword(salt, hpwd, authentication) {
		trace_util_0.Count(_privileges_00000, 35)
		return
	}

	trace_util_0.Count(_privileges_00000, 27)
	p.user = user
	p.host = host
	success = true
	return
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(activeRoles []*auth.RoleIdentity, db string) bool {
	trace_util_0.Count(_privileges_00000, 36)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 40)
		return true
	}
	trace_util_0.Count(_privileges_00000, 37)
	mysqlPriv := p.Handle.Get()
	if mysqlPriv.DBIsVisible(p.user, p.host, db) {
		trace_util_0.Count(_privileges_00000, 41)
		return true
	}
	trace_util_0.Count(_privileges_00000, 38)
	allRoles := mysqlPriv.FindAllRole(activeRoles)
	for _, role := range allRoles {
		trace_util_0.Count(_privileges_00000, 42)
		if mysqlPriv.DBIsVisible(role.Username, role.Hostname, db) {
			trace_util_0.Count(_privileges_00000, 43)
			return true
		}
	}
	trace_util_0.Count(_privileges_00000, 39)
	return false
}

// UserPrivilegesTable implements the Manager interface.
func (p *UserPrivileges) UserPrivilegesTable() [][]types.Datum {
	trace_util_0.Count(_privileges_00000, 44)
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.UserPrivilegesTable()
}

// ShowGrants implements privilege.Manager ShowGrants interface.
func (p *UserPrivileges) ShowGrants(ctx sessionctx.Context, user *auth.UserIdentity, roles []*auth.RoleIdentity) (grants []string, err error) {
	trace_util_0.Count(_privileges_00000, 45)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 49)
		return nil, errNonexistingGrant.GenWithStackByArgs("root", "%")
	}
	trace_util_0.Count(_privileges_00000, 46)
	mysqlPrivilege := p.Handle.Get()
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		trace_util_0.Count(_privileges_00000, 50)
		u = user.AuthUsername
		h = user.AuthHostname
	}
	trace_util_0.Count(_privileges_00000, 47)
	grants = mysqlPrivilege.showGrants(u, h, roles)
	if len(grants) == 0 {
		trace_util_0.Count(_privileges_00000, 51)
		err = errNonexistingGrant.GenWithStackByArgs(u, h)
	}

	trace_util_0.Count(_privileges_00000, 48)
	return
}

// ActiveRoles implements privilege.Manager ActiveRoles interface.
func (p *UserPrivileges) ActiveRoles(ctx sessionctx.Context, roleList []*auth.RoleIdentity) (bool, string) {
	trace_util_0.Count(_privileges_00000, 52)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 55)
		return true, ""
	}
	trace_util_0.Count(_privileges_00000, 53)
	mysqlPrivilege := p.Handle.Get()
	u := p.user
	h := p.host
	for _, r := range roleList {
		trace_util_0.Count(_privileges_00000, 56)
		ok := mysqlPrivilege.FindRole(u, h, r)
		if !ok {
			trace_util_0.Count(_privileges_00000, 57)
			logutil.Logger(context.Background()).Error("find role failed", zap.Stringer("role", r))
			return false, r.String()
		}
	}
	trace_util_0.Count(_privileges_00000, 54)
	ctx.GetSessionVars().ActiveRoles = roleList
	return true, ""
}

// FindEdge implements privilege.Manager FindRelationship interface.
func (p *UserPrivileges) FindEdge(ctx sessionctx.Context, role *auth.RoleIdentity, user *auth.UserIdentity) bool {
	trace_util_0.Count(_privileges_00000, 58)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 61)
		return false
	}
	trace_util_0.Count(_privileges_00000, 59)
	mysqlPrivilege := p.Handle.Get()
	ok := mysqlPrivilege.FindRole(user.Username, user.Hostname, role)
	if !ok {
		trace_util_0.Count(_privileges_00000, 62)
		logutil.Logger(context.Background()).Error("find role failed", zap.Stringer("role", role))
		return false
	}
	trace_util_0.Count(_privileges_00000, 60)
	return true
}

// GetDefaultRoles returns all default roles for certain user.
func (p *UserPrivileges) GetDefaultRoles(user, host string) []*auth.RoleIdentity {
	trace_util_0.Count(_privileges_00000, 63)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 65)
		return make([]*auth.RoleIdentity, 0, 10)
	}
	trace_util_0.Count(_privileges_00000, 64)
	mysqlPrivilege := p.Handle.Get()
	ret := mysqlPrivilege.getDefaultRoles(user, host)
	return ret
}

// GetAllRoles return all roles of user.
func (p *UserPrivileges) GetAllRoles(user, host string) []*auth.RoleIdentity {
	trace_util_0.Count(_privileges_00000, 66)
	if SkipWithGrant {
		trace_util_0.Count(_privileges_00000, 68)
		return make([]*auth.RoleIdentity, 0, 10)
	}

	trace_util_0.Count(_privileges_00000, 67)
	mysqlPrivilege := p.Handle.Get()
	return mysqlPrivilege.getAllRoles(user, host)
}

var _privileges_00000 = "privilege/privileges/privileges.go"
