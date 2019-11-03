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

package privileges

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	log "github.com/sirupsen/logrus"
)

var (
	userTablePrivilegeMask = computePrivMask(mysql.AllGlobalPrivs)
	dbTablePrivilegeMask   = computePrivMask(mysql.AllDBPrivs)
	tablePrivMask          = computePrivMask(mysql.AllTablePrivs)
	columnPrivMask         = computePrivMask(mysql.AllColumnPrivs)
)

func computePrivMask(privs []mysql.PrivilegeType) mysql.PrivilegeType {
	trace_util_0.Count(_cache_00000, 0)
	var mask mysql.PrivilegeType
	for _, p := range privs {
		trace_util_0.Count(_cache_00000, 2)
		mask |= p
	}
	trace_util_0.Count(_cache_00000, 1)
	return mask
}

// UserRecord is used to represent a user record in privilege cache.
type UserRecord struct {
	Host          string // max length 60, primary key
	User          string // max length 32, primary key
	Password      string // max length 41
	Privileges    mysql.PrivilegeType
	AccountLocked bool // A role record when this field is true

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type dbRecord struct {
	Host       string
	DB         string
	User       string
	Privileges mysql.PrivilegeType

	// hostPatChars is compiled from Host and DB, cached for pattern match performance.
	hostPatChars []byte
	hostPatTypes []byte

	dbPatChars []byte
	dbPatTypes []byte
}

type tablesPrivRecord struct {
	Host       string
	DB         string
	User       string
	TableName  string
	Grantor    string
	Timestamp  time.Time
	TablePriv  mysql.PrivilegeType
	ColumnPriv mysql.PrivilegeType

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

type columnsPrivRecord struct {
	Host       string
	DB         string
	User       string
	TableName  string
	ColumnName string
	Timestamp  time.Time
	ColumnPriv mysql.PrivilegeType

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

// defaultRoleRecord is used to cache mysql.default_roles
type defaultRoleRecord struct {
	Host            string
	User            string
	DefaultRoleUser string
	DefaultRoleHost string

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte
}

// roleGraphEdgesTable is used to cache relationship between and role.
type roleGraphEdgesTable struct {
	roleList map[string]*auth.RoleIdentity
}

// Find method is used to find role from table
func (g roleGraphEdgesTable) Find(user, host string) bool {
	trace_util_0.Count(_cache_00000, 3)
	if host == "" {
		trace_util_0.Count(_cache_00000, 6)
		host = "%"
	}
	trace_util_0.Count(_cache_00000, 4)
	key := user + "@" + host
	if g.roleList == nil {
		trace_util_0.Count(_cache_00000, 7)
		return false
	}
	trace_util_0.Count(_cache_00000, 5)
	_, ok := g.roleList[key]
	return ok
}

// MySQLPrivilege is the in-memory cache of mysql privilege tables.
type MySQLPrivilege struct {
	User         []UserRecord
	DB           []dbRecord
	TablesPriv   []tablesPrivRecord
	ColumnsPriv  []columnsPrivRecord
	DefaultRoles []defaultRoleRecord
	RoleGraph    map[string]roleGraphEdgesTable
}

// FindAllRole is used to find all roles grant to this user.
func (p *MySQLPrivilege) FindAllRole(activeRoles []*auth.RoleIdentity) []*auth.RoleIdentity {
	trace_util_0.Count(_cache_00000, 8)
	queue, head := make([]*auth.RoleIdentity, 0, len(activeRoles)), 0
	for _, r := range activeRoles {
		trace_util_0.Count(_cache_00000, 11)
		queue = append(queue, r)
	}
	// Using breadth first search to find all roles grant to this user.
	trace_util_0.Count(_cache_00000, 9)
	visited, ret := make(map[string]bool), make([]*auth.RoleIdentity, 0)
	for head < len(queue) {
		trace_util_0.Count(_cache_00000, 12)
		role := queue[head]
		if _, ok := visited[role.String()]; !ok {
			trace_util_0.Count(_cache_00000, 14)
			visited[role.String()] = true
			ret = append(ret, role)
			key := role.Username + "@" + role.Hostname
			if edgeTable, ok := p.RoleGraph[key]; ok {
				trace_util_0.Count(_cache_00000, 15)
				for _, v := range edgeTable.roleList {
					trace_util_0.Count(_cache_00000, 16)
					if _, ok := visited[v.String()]; !ok {
						trace_util_0.Count(_cache_00000, 17)
						queue = append(queue, v)
					}
				}
			}
		}
		trace_util_0.Count(_cache_00000, 13)
		head += 1
	}
	trace_util_0.Count(_cache_00000, 10)
	return ret
}

// FindRole is used to detect whether there is edges between users and roles.
func (p *MySQLPrivilege) FindRole(user string, host string, role *auth.RoleIdentity) bool {
	trace_util_0.Count(_cache_00000, 18)
	rec := p.matchUser(user, host)
	r := p.matchUser(role.Username, role.Hostname)
	if rec != nil && r != nil {
		trace_util_0.Count(_cache_00000, 20)
		key := rec.User + "@" + rec.Host
		return p.RoleGraph[key].Find(role.Username, role.Hostname)
	}
	trace_util_0.Count(_cache_00000, 19)
	return false
}

// LoadAll loads the tables from database to memory.
func (p *MySQLPrivilege) LoadAll(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 21)
	err := p.LoadUserTable(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 28)
		return errors.Trace(err)
	}

	trace_util_0.Count(_cache_00000, 22)
	err = p.LoadDBTable(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 29)
		if !noSuchTable(err) {
			trace_util_0.Count(_cache_00000, 31)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 30)
		log.Warn("mysql.db maybe missing")
	}

	trace_util_0.Count(_cache_00000, 23)
	err = p.LoadTablesPrivTable(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 32)
		if !noSuchTable(err) {
			trace_util_0.Count(_cache_00000, 34)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 33)
		log.Warn("mysql.tables_priv missing")
	}

	trace_util_0.Count(_cache_00000, 24)
	err = p.LoadDefaultRoles(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 35)
		if !noSuchTable(err) {
			trace_util_0.Count(_cache_00000, 37)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 36)
		log.Warn("mysql.default_roles missing")
	}

	trace_util_0.Count(_cache_00000, 25)
	err = p.LoadColumnsPrivTable(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 38)
		if !noSuchTable(err) {
			trace_util_0.Count(_cache_00000, 40)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 39)
		log.Warn("mysql.columns_priv missing")
	}

	trace_util_0.Count(_cache_00000, 26)
	err = p.LoadRoleGraph(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 41)
		if !noSuchTable(err) {
			trace_util_0.Count(_cache_00000, 43)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 42)
		log.Warn("mysql.role_edges missing")
	}
	trace_util_0.Count(_cache_00000, 27)
	return nil
}

func noSuchTable(err error) bool {
	trace_util_0.Count(_cache_00000, 44)
	e1 := errors.Cause(err)
	if e2, ok := e1.(*terror.Error); ok {
		trace_util_0.Count(_cache_00000, 46)
		if e2.Code() == terror.ErrCode(mysql.ErrNoSuchTable) {
			trace_util_0.Count(_cache_00000, 47)
			return true
		}
	}
	trace_util_0.Count(_cache_00000, 45)
	return false
}

// LoadRoleGraph loads the mysql.role_edges table from database.
func (p *MySQLPrivilege) LoadRoleGraph(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 48)
	p.RoleGraph = make(map[string]roleGraphEdgesTable)
	err := p.loadTable(ctx, "select FROM_USER, FROM_HOST, TO_USER, TO_HOST from mysql.role_edges;", p.decodeRoleEdgesTable)
	if err != nil {
		trace_util_0.Count(_cache_00000, 50)
		return errors.Trace(err)
	}
	trace_util_0.Count(_cache_00000, 49)
	return nil
}

// LoadUserTable loads the mysql.user table from database.
func (p *MySQLPrivilege) LoadUserTable(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 51)
	err := p.loadTable(ctx, "select HIGH_PRIORITY Host,User,Password,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,Super_priv,Execute_priv,Create_view_priv,Show_view_priv,Index_priv,Create_user_priv,Trigger_priv,Create_role_priv,Drop_role_priv,account_locked from mysql.user;", p.decodeUserTableRow)
	if err != nil {
		trace_util_0.Count(_cache_00000, 53)
		return errors.Trace(err)
	}
	// See https://dev.mysql.com/doc/refman/8.0/en/connection-access.html
	// When multiple matches are possible, the server must determine which of them to use. It resolves this issue as follows:
	// 1. Whenever the server reads the user table into memory, it sorts the rows.
	// 2. When a client attempts to connect, the server looks through the rows in sorted order.
	// 3. The server uses the first row that matches the client host name and user name.
	// The server uses sorting rules that order rows with the most-specific Host values first.
	trace_util_0.Count(_cache_00000, 52)
	p.SortUserTable()
	return nil
}

type sortedUserRecord []UserRecord

func (s sortedUserRecord) Len() int {
	trace_util_0.Count(_cache_00000, 54)
	return len(s)
}

func (s sortedUserRecord) Less(i, j int) bool {
	trace_util_0.Count(_cache_00000, 55)
	x := s[i]
	y := s[j]

	// Compare two item by user's host first.
	c1 := compareHost(x.Host, y.Host)
	if c1 < 0 {
		trace_util_0.Count(_cache_00000, 58)
		return true
	}
	trace_util_0.Count(_cache_00000, 56)
	if c1 > 0 {
		trace_util_0.Count(_cache_00000, 59)
		return false
	}

	// Then, compare item by user's name value.
	trace_util_0.Count(_cache_00000, 57)
	return x.User < y.User
}

// compareHost compares two host string using some special rules, return value 1, 0, -1 means > = <.
// TODO: Check how MySQL do it exactly, instead of guess its rules.
func compareHost(x, y string) int {
	trace_util_0.Count(_cache_00000, 60)
	// The more-specific, the smaller it is.
	// The pattern '%' means “any host” and is least specific.
	if y == `%` {
		trace_util_0.Count(_cache_00000, 65)
		if x == `%` {
			trace_util_0.Count(_cache_00000, 67)
			return 0
		}
		trace_util_0.Count(_cache_00000, 66)
		return -1
	}

	// The empty string '' also means “any host” but sorts after '%'.
	trace_util_0.Count(_cache_00000, 61)
	if y == "" {
		trace_util_0.Count(_cache_00000, 68)
		if x == "" {
			trace_util_0.Count(_cache_00000, 70)
			return 0
		}
		trace_util_0.Count(_cache_00000, 69)
		return -1
	}

	// One of them end with `%`.
	trace_util_0.Count(_cache_00000, 62)
	xEnd := strings.HasSuffix(x, `%`)
	yEnd := strings.HasSuffix(y, `%`)
	if xEnd || yEnd {
		trace_util_0.Count(_cache_00000, 71)
		switch {
		case !xEnd && yEnd:
			trace_util_0.Count(_cache_00000, 73)
			return -1
		case xEnd && !yEnd:
			trace_util_0.Count(_cache_00000, 74)
			return 1
		case xEnd && yEnd:
			trace_util_0.Count(_cache_00000, 75)
			// 192.168.199.% smaller than 192.168.%
			// A not very accurate comparison, compare them by length.
			if len(x) > len(y) {
				trace_util_0.Count(_cache_00000, 76)
				return -1
			}
		}
		trace_util_0.Count(_cache_00000, 72)
		return 0
	}

	// For other case, the order is nondeterministic.
	trace_util_0.Count(_cache_00000, 63)
	switch x < y {
	case true:
		trace_util_0.Count(_cache_00000, 77)
		return -1
	case false:
		trace_util_0.Count(_cache_00000, 78)
		return 1
	}
	trace_util_0.Count(_cache_00000, 64)
	return 0
}

func (s sortedUserRecord) Swap(i, j int) {
	trace_util_0.Count(_cache_00000, 79)
	s[i], s[j] = s[j], s[i]
}

// SortUserTable sorts p.User in the MySQLPrivilege struct.
func (p MySQLPrivilege) SortUserTable() {
	trace_util_0.Count(_cache_00000, 80)
	sort.Sort(sortedUserRecord(p.User))
}

// LoadDBTable loads the mysql.db table from database.
func (p *MySQLPrivilege) LoadDBTable(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 81)
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,Alter_priv,Execute_priv,Create_view_priv,Show_view_priv from mysql.db order by host, db, user;", p.decodeDBTableRow)
}

// LoadTablesPrivTable loads the mysql.tables_priv table from database.
func (p *MySQLPrivilege) LoadTablesPrivTable(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 82)
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Table_name,Grantor,Timestamp,Table_priv,Column_priv from mysql.tables_priv", p.decodeTablesPrivTableRow)
}

// LoadColumnsPrivTable loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadColumnsPrivTable(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 83)
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,DB,User,Table_name,Column_name,Timestamp,Column_priv from mysql.columns_priv", p.decodeColumnsPrivTableRow)
}

// LoadDefaultRoles loads the mysql.columns_priv table from database.
func (p *MySQLPrivilege) LoadDefaultRoles(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 84)
	return p.loadTable(ctx, "select HOST, USER, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER from mysql.default_roles", p.decodeDefaultRoleTableRow)
}

func (p *MySQLPrivilege) loadTable(sctx sessionctx.Context, sql string,
	decodeTableRow func(chunk.Row, []*ast.ResultField) error) error {
	trace_util_0.Count(_cache_00000, 85)
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_cache_00000, 87)
		return errors.Trace(err)
	}
	trace_util_0.Count(_cache_00000, 86)
	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	req := rs.NewRecordBatch()
	for {
		trace_util_0.Count(_cache_00000, 88)
		err = rs.Next(context.TODO(), req)
		if err != nil {
			trace_util_0.Count(_cache_00000, 92)
			return errors.Trace(err)
		}
		trace_util_0.Count(_cache_00000, 89)
		if req.NumRows() == 0 {
			trace_util_0.Count(_cache_00000, 93)
			return nil
		}
		trace_util_0.Count(_cache_00000, 90)
		it := chunk.NewIterator4Chunk(req.Chunk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			trace_util_0.Count(_cache_00000, 94)
			err = decodeTableRow(row, fs)
			if err != nil {
				trace_util_0.Count(_cache_00000, 95)
				return errors.Trace(err)
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		trace_util_0.Count(_cache_00000, 91)
		req.Chunk = chunk.Renew(req.Chunk, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (p *MySQLPrivilege) decodeUserTableRow(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 96)
	var value UserRecord
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 98)
		switch {
		case f.ColumnAsName.L == "user":
			trace_util_0.Count(_cache_00000, 99)
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			trace_util_0.Count(_cache_00000, 100)
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "password":
			trace_util_0.Count(_cache_00000, 101)
			value.Password = row.GetString(i)
		case f.ColumnAsName.L == "account_locked":
			trace_util_0.Count(_cache_00000, 102)
			if row.GetEnum(i).String() == "Y" {
				trace_util_0.Count(_cache_00000, 106)
				value.AccountLocked = true
			}
		case f.Column.Tp == mysql.TypeEnum:
			trace_util_0.Count(_cache_00000, 103)
			if row.GetEnum(i).String() != "Y" {
				trace_util_0.Count(_cache_00000, 107)
				continue
			}
			trace_util_0.Count(_cache_00000, 104)
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				trace_util_0.Count(_cache_00000, 108)
				return errInvalidPrivilegeType.GenWithStack(f.ColumnAsName.O)
			}
			trace_util_0.Count(_cache_00000, 105)
			value.Privileges |= priv
		}
	}
	trace_util_0.Count(_cache_00000, 97)
	p.User = append(p.User, value)
	return nil
}

func (p *MySQLPrivilege) decodeDBTableRow(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 109)
	var value dbRecord
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 111)
		switch {
		case f.ColumnAsName.L == "user":
			trace_util_0.Count(_cache_00000, 112)
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			trace_util_0.Count(_cache_00000, 113)
			value.Host = row.GetString(i)
			value.hostPatChars, value.hostPatTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			trace_util_0.Count(_cache_00000, 114)
			value.DB = row.GetString(i)
			value.dbPatChars, value.dbPatTypes = stringutil.CompilePattern(strings.ToUpper(value.DB), '\\')
		case f.Column.Tp == mysql.TypeEnum:
			trace_util_0.Count(_cache_00000, 115)
			if row.GetEnum(i).String() != "Y" {
				trace_util_0.Count(_cache_00000, 118)
				continue
			}
			trace_util_0.Count(_cache_00000, 116)
			priv, ok := mysql.Col2PrivType[f.ColumnAsName.O]
			if !ok {
				trace_util_0.Count(_cache_00000, 119)
				return errInvalidPrivilegeType.GenWithStack("Unknown Privilege Type!")
			}
			trace_util_0.Count(_cache_00000, 117)
			value.Privileges |= priv
		}
	}
	trace_util_0.Count(_cache_00000, 110)
	p.DB = append(p.DB, value)
	return nil
}

func (p *MySQLPrivilege) decodeTablesPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 120)
	var value tablesPrivRecord
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 122)
		switch {
		case f.ColumnAsName.L == "user":
			trace_util_0.Count(_cache_00000, 123)
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			trace_util_0.Count(_cache_00000, 124)
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			trace_util_0.Count(_cache_00000, 125)
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			trace_util_0.Count(_cache_00000, 126)
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "table_priv":
			trace_util_0.Count(_cache_00000, 127)
			value.TablePriv = decodeSetToPrivilege(row.GetSet(i))
		case f.ColumnAsName.L == "column_priv":
			trace_util_0.Count(_cache_00000, 128)
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
		}
	}
	trace_util_0.Count(_cache_00000, 121)
	p.TablesPriv = append(p.TablesPriv, value)
	return nil
}

func (p *MySQLPrivilege) decodeRoleEdgesTable(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 129)
	var fromUser, fromHost, toHost, toUser string
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 132)
		switch {
		case f.ColumnAsName.L == "from_host":
			trace_util_0.Count(_cache_00000, 133)
			fromHost = row.GetString(i)
		case f.ColumnAsName.L == "from_user":
			trace_util_0.Count(_cache_00000, 134)
			fromUser = row.GetString(i)
		case f.ColumnAsName.L == "to_host":
			trace_util_0.Count(_cache_00000, 135)
			toHost = row.GetString(i)
		case f.ColumnAsName.L == "to_user":
			trace_util_0.Count(_cache_00000, 136)
			toUser = row.GetString(i)
		}
	}
	trace_util_0.Count(_cache_00000, 130)
	fromKey := fromUser + "@" + fromHost
	toKey := toUser + "@" + toHost
	roleGraph, ok := p.RoleGraph[toKey]
	if !ok {
		trace_util_0.Count(_cache_00000, 137)
		roleGraph = roleGraphEdgesTable{roleList: make(map[string]*auth.RoleIdentity)}
		p.RoleGraph[toKey] = roleGraph
	}
	trace_util_0.Count(_cache_00000, 131)
	roleGraph.roleList[fromKey] = &auth.RoleIdentity{Username: fromUser, Hostname: fromHost}
	return nil
}

func (p *MySQLPrivilege) decodeDefaultRoleTableRow(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 138)
	var value defaultRoleRecord
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 140)
		switch {
		case f.ColumnAsName.L == "host":
			trace_util_0.Count(_cache_00000, 141)
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "user":
			trace_util_0.Count(_cache_00000, 142)
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "default_role_host":
			trace_util_0.Count(_cache_00000, 143)
			value.DefaultRoleHost = row.GetString(i)
		case f.ColumnAsName.L == "default_role_user":
			trace_util_0.Count(_cache_00000, 144)
			value.DefaultRoleUser = row.GetString(i)
		}
	}
	trace_util_0.Count(_cache_00000, 139)
	p.DefaultRoles = append(p.DefaultRoles, value)
	return nil
}

func (p *MySQLPrivilege) decodeColumnsPrivTableRow(row chunk.Row, fs []*ast.ResultField) error {
	trace_util_0.Count(_cache_00000, 145)
	var value columnsPrivRecord
	for i, f := range fs {
		trace_util_0.Count(_cache_00000, 147)
		switch {
		case f.ColumnAsName.L == "user":
			trace_util_0.Count(_cache_00000, 148)
			value.User = row.GetString(i)
		case f.ColumnAsName.L == "host":
			trace_util_0.Count(_cache_00000, 149)
			value.Host = row.GetString(i)
			value.patChars, value.patTypes = stringutil.CompilePattern(value.Host, '\\')
		case f.ColumnAsName.L == "db":
			trace_util_0.Count(_cache_00000, 150)
			value.DB = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			trace_util_0.Count(_cache_00000, 151)
			value.TableName = row.GetString(i)
		case f.ColumnAsName.L == "column_name":
			trace_util_0.Count(_cache_00000, 152)
			value.ColumnName = row.GetString(i)
		case f.ColumnAsName.L == "timestamp":
			trace_util_0.Count(_cache_00000, 153)
			var err error
			value.Timestamp, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				trace_util_0.Count(_cache_00000, 155)
				return errors.Trace(err)
			}
		case f.ColumnAsName.L == "column_priv":
			trace_util_0.Count(_cache_00000, 154)
			value.ColumnPriv = decodeSetToPrivilege(row.GetSet(i))
		}
	}
	trace_util_0.Count(_cache_00000, 146)
	p.ColumnsPriv = append(p.ColumnsPriv, value)
	return nil
}

func decodeSetToPrivilege(s types.Set) mysql.PrivilegeType {
	trace_util_0.Count(_cache_00000, 156)
	var ret mysql.PrivilegeType
	if s.Name == "" {
		trace_util_0.Count(_cache_00000, 159)
		return ret
	}
	trace_util_0.Count(_cache_00000, 157)
	for _, str := range strings.Split(s.Name, ",") {
		trace_util_0.Count(_cache_00000, 160)
		priv, ok := mysql.SetStr2Priv[str]
		if !ok {
			trace_util_0.Count(_cache_00000, 162)
			log.Warn("unsupported privilege type:", str)
			continue
		}
		trace_util_0.Count(_cache_00000, 161)
		ret |= priv
	}
	trace_util_0.Count(_cache_00000, 158)
	return ret
}

func (record *UserRecord) match(user, host string) bool {
	trace_util_0.Count(_cache_00000, 163)
	return record.User == user && patternMatch(host, record.patChars, record.patTypes)
}

func (record *dbRecord) match(user, host, db string) bool {
	trace_util_0.Count(_cache_00000, 164)
	return record.User == user &&
		patternMatch(strings.ToUpper(db), record.dbPatChars, record.dbPatTypes) &&
		patternMatch(host, record.hostPatChars, record.hostPatTypes)
}

func (record *tablesPrivRecord) match(user, host, db, table string) bool {
	trace_util_0.Count(_cache_00000, 165)
	return record.User == user && strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table) && patternMatch(host, record.patChars, record.patTypes)
}

func (record *columnsPrivRecord) match(user, host, db, table, col string) bool {
	trace_util_0.Count(_cache_00000, 166)
	return record.User == user && strings.EqualFold(record.DB, db) &&
		strings.EqualFold(record.TableName, table) &&
		strings.EqualFold(record.ColumnName, col) &&
		patternMatch(host, record.patChars, record.patTypes)
}

func (record *defaultRoleRecord) match(user, host string) bool {
	trace_util_0.Count(_cache_00000, 167)
	return record.User == user && patternMatch(host, record.patChars, record.patTypes)
}

// patternMatch matches "%" the same way as ".*" in regular expression, for example,
// "10.0.%" would match "10.0.1" "10.0.1.118" ...
func patternMatch(str string, patChars, patTypes []byte) bool {
	trace_util_0.Count(_cache_00000, 168)
	return stringutil.DoMatch(str, patChars, patTypes)
}

// connectionVerification verifies the connection have access to TiDB server.
func (p *MySQLPrivilege) connectionVerification(user, host string) *UserRecord {
	trace_util_0.Count(_cache_00000, 169)
	for i := 0; i < len(p.User); i++ {
		trace_util_0.Count(_cache_00000, 171)
		record := &p.User[i]
		if record.match(user, host) {
			trace_util_0.Count(_cache_00000, 172)
			return record
		}
	}
	trace_util_0.Count(_cache_00000, 170)
	return nil
}

func (p *MySQLPrivilege) matchUser(user, host string) *UserRecord {
	trace_util_0.Count(_cache_00000, 173)
	for i := 0; i < len(p.User); i++ {
		trace_util_0.Count(_cache_00000, 175)
		record := &p.User[i]
		if record.match(user, host) {
			trace_util_0.Count(_cache_00000, 176)
			return record
		}
	}
	trace_util_0.Count(_cache_00000, 174)
	return nil
}

func (p *MySQLPrivilege) matchDB(user, host, db string) *dbRecord {
	trace_util_0.Count(_cache_00000, 177)
	for i := 0; i < len(p.DB); i++ {
		trace_util_0.Count(_cache_00000, 179)
		record := &p.DB[i]
		if record.match(user, host, db) {
			trace_util_0.Count(_cache_00000, 180)
			return record
		}
	}
	trace_util_0.Count(_cache_00000, 178)
	return nil
}

func (p *MySQLPrivilege) matchTables(user, host, db, table string) *tablesPrivRecord {
	trace_util_0.Count(_cache_00000, 181)
	for i := 0; i < len(p.TablesPriv); i++ {
		trace_util_0.Count(_cache_00000, 183)
		record := &p.TablesPriv[i]
		if record.match(user, host, db, table) {
			trace_util_0.Count(_cache_00000, 184)
			return record
		}
	}
	trace_util_0.Count(_cache_00000, 182)
	return nil
}

func (p *MySQLPrivilege) matchColumns(user, host, db, table, column string) *columnsPrivRecord {
	trace_util_0.Count(_cache_00000, 185)
	for i := 0; i < len(p.ColumnsPriv); i++ {
		trace_util_0.Count(_cache_00000, 187)
		record := &p.ColumnsPriv[i]
		if record.match(user, host, db, table, column) {
			trace_util_0.Count(_cache_00000, 188)
			return record
		}
	}
	trace_util_0.Count(_cache_00000, 186)
	return nil
}

// RequestVerification checks whether the user have sufficient privileges to do the operation.
func (p *MySQLPrivilege) RequestVerification(activeRoles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType) bool {
	trace_util_0.Count(_cache_00000, 189)
	roleList := p.FindAllRole(activeRoles)
	roleList = append(roleList, &auth.RoleIdentity{Username: user, Hostname: host})

	var userPriv, dbPriv, tablePriv, columnPriv mysql.PrivilegeType
	for _, r := range roleList {
		trace_util_0.Count(_cache_00000, 198)
		userRecord := p.matchUser(r.Username, r.Hostname)
		if userRecord != nil {
			trace_util_0.Count(_cache_00000, 199)
			userPriv |= userRecord.Privileges
		}
	}
	trace_util_0.Count(_cache_00000, 190)
	if userPriv&priv > 0 {
		trace_util_0.Count(_cache_00000, 200)
		return true
	}

	trace_util_0.Count(_cache_00000, 191)
	for _, r := range roleList {
		trace_util_0.Count(_cache_00000, 201)
		dbRecord := p.matchDB(r.Username, r.Hostname, db)
		if dbRecord != nil {
			trace_util_0.Count(_cache_00000, 202)
			dbPriv |= dbRecord.Privileges
		}
	}
	trace_util_0.Count(_cache_00000, 192)
	if dbPriv&priv > 0 {
		trace_util_0.Count(_cache_00000, 203)
		return true
	}

	trace_util_0.Count(_cache_00000, 193)
	for _, r := range roleList {
		trace_util_0.Count(_cache_00000, 204)
		tableRecord := p.matchTables(r.Username, r.Hostname, db, table)
		if tableRecord != nil {
			trace_util_0.Count(_cache_00000, 205)
			tablePriv |= tableRecord.TablePriv
			if column != "" {
				trace_util_0.Count(_cache_00000, 206)
				columnPriv |= tableRecord.ColumnPriv
			}
		}
	}
	trace_util_0.Count(_cache_00000, 194)
	if tablePriv&priv > 0 || columnPriv&priv > 0 {
		trace_util_0.Count(_cache_00000, 207)
		return true
	}

	trace_util_0.Count(_cache_00000, 195)
	columnPriv = 0
	for _, r := range roleList {
		trace_util_0.Count(_cache_00000, 208)
		columnRecord := p.matchColumns(r.Username, r.Hostname, db, table, column)
		if columnRecord != nil {
			trace_util_0.Count(_cache_00000, 209)
			columnPriv |= columnRecord.ColumnPriv
		}
	}
	trace_util_0.Count(_cache_00000, 196)
	if columnPriv&priv > 0 {
		trace_util_0.Count(_cache_00000, 210)
		return true
	}

	trace_util_0.Count(_cache_00000, 197)
	return priv == 0
}

// DBIsVisible checks whether the user can see the db.
func (p *MySQLPrivilege) DBIsVisible(user, host, db string) bool {
	trace_util_0.Count(_cache_00000, 211)
	if record := p.matchUser(user, host); record != nil {
		trace_util_0.Count(_cache_00000, 217)
		if record.Privileges != 0 {
			trace_util_0.Count(_cache_00000, 218)
			return true
		}
	}

	// INFORMATION_SCHEMA is visible to all users.
	trace_util_0.Count(_cache_00000, 212)
	if strings.EqualFold(db, "INFORMATION_SCHEMA") {
		trace_util_0.Count(_cache_00000, 219)
		return true
	}

	trace_util_0.Count(_cache_00000, 213)
	if record := p.matchDB(user, host, db); record != nil {
		trace_util_0.Count(_cache_00000, 220)
		if record.Privileges > 0 {
			trace_util_0.Count(_cache_00000, 221)
			return true
		}
	}

	trace_util_0.Count(_cache_00000, 214)
	for _, record := range p.TablesPriv {
		trace_util_0.Count(_cache_00000, 222)
		if record.User == user &&
			patternMatch(host, record.patChars, record.patTypes) &&
			strings.EqualFold(record.DB, db) {
			trace_util_0.Count(_cache_00000, 223)
			if record.TablePriv != 0 || record.ColumnPriv != 0 {
				trace_util_0.Count(_cache_00000, 224)
				return true
			}
		}
	}

	trace_util_0.Count(_cache_00000, 215)
	for _, record := range p.ColumnsPriv {
		trace_util_0.Count(_cache_00000, 225)
		if record.User == user &&
			patternMatch(host, record.patChars, record.patTypes) &&
			strings.EqualFold(record.DB, db) {
			trace_util_0.Count(_cache_00000, 226)
			if record.ColumnPriv != 0 {
				trace_util_0.Count(_cache_00000, 227)
				return true
			}
		}
	}

	trace_util_0.Count(_cache_00000, 216)
	return false
}

func (p *MySQLPrivilege) showGrants(user, host string, roles []*auth.RoleIdentity) []string {
	trace_util_0.Count(_cache_00000, 228)
	var gs []string
	var hasGlobalGrant bool = false
	// Some privileges may granted from role inheritance.
	// We should find these inheritance relationship.
	allRoles := p.FindAllRole(roles)
	// Show global grants.
	var currentPriv mysql.PrivilegeType
	var g string
	for _, record := range p.User {
		trace_util_0.Count(_cache_00000, 237)
		if record.User == user && record.Host == host {
			trace_util_0.Count(_cache_00000, 238)
			hasGlobalGrant = true
			currentPriv |= record.Privileges
		} else {
			trace_util_0.Count(_cache_00000, 239)
			{
				for _, r := range allRoles {
					trace_util_0.Count(_cache_00000, 240)
					if record.User == r.Username && record.Host == r.Hostname {
						trace_util_0.Count(_cache_00000, 241)
						hasGlobalGrant = true
						currentPriv |= record.Privileges
					}
				}
			}
		}
	}
	trace_util_0.Count(_cache_00000, 229)
	g = userPrivToString(currentPriv)
	if len(g) > 0 {
		trace_util_0.Count(_cache_00000, 242)
		s := fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s'`, g, user, host)
		gs = append(gs, s)
	}

	// This is a mysql convention.
	trace_util_0.Count(_cache_00000, 230)
	if len(gs) == 0 && hasGlobalGrant {
		trace_util_0.Count(_cache_00000, 243)
		s := fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s'", user, host)
		gs = append(gs, s)
	}

	// Show db scope grants.
	trace_util_0.Count(_cache_00000, 231)
	dbPrivTable := make(map[string]mysql.PrivilegeType)
	for _, record := range p.DB {
		trace_util_0.Count(_cache_00000, 244)
		if record.User == user && record.Host == host {
			trace_util_0.Count(_cache_00000, 245)
			if _, ok := dbPrivTable[record.DB]; ok {
				trace_util_0.Count(_cache_00000, 246)
				dbPrivTable[record.DB] |= record.Privileges
			} else {
				trace_util_0.Count(_cache_00000, 247)
				{
					dbPrivTable[record.DB] = record.Privileges
				}
			}
		} else {
			trace_util_0.Count(_cache_00000, 248)
			{
				for _, r := range allRoles {
					trace_util_0.Count(_cache_00000, 249)
					if record.User == r.Username && record.Host == r.Hostname {
						trace_util_0.Count(_cache_00000, 250)
						if _, ok := dbPrivTable[record.DB]; ok {
							trace_util_0.Count(_cache_00000, 251)
							dbPrivTable[record.DB] |= record.Privileges
						} else {
							trace_util_0.Count(_cache_00000, 252)
							{
								dbPrivTable[record.DB] = record.Privileges
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_cache_00000, 232)
	for dbName, priv := range dbPrivTable {
		trace_util_0.Count(_cache_00000, 253)
		g := dbPrivToString(priv)
		if len(g) > 0 {
			trace_util_0.Count(_cache_00000, 254)
			s := fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s'`, g, dbName, user, host)
			gs = append(gs, s)
		}
	}

	// Show table scope grants.
	trace_util_0.Count(_cache_00000, 233)
	tablePrivTable := make(map[string]mysql.PrivilegeType)
	for _, record := range p.TablesPriv {
		trace_util_0.Count(_cache_00000, 255)
		recordKey := record.DB + "." + record.TableName
		if record.User == user && record.Host == host {
			trace_util_0.Count(_cache_00000, 256)
			if _, ok := dbPrivTable[record.DB]; ok {
				trace_util_0.Count(_cache_00000, 257)
				tablePrivTable[recordKey] |= record.TablePriv
			} else {
				trace_util_0.Count(_cache_00000, 258)
				{
					tablePrivTable[recordKey] = record.TablePriv
				}
			}
		} else {
			trace_util_0.Count(_cache_00000, 259)
			{
				for _, r := range allRoles {
					trace_util_0.Count(_cache_00000, 260)
					if record.User == r.Username && record.Host == r.Hostname {
						trace_util_0.Count(_cache_00000, 261)
						if _, ok := dbPrivTable[record.DB]; ok {
							trace_util_0.Count(_cache_00000, 262)
							tablePrivTable[recordKey] |= record.TablePriv
						} else {
							trace_util_0.Count(_cache_00000, 263)
							{
								tablePrivTable[recordKey] = record.TablePriv
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_cache_00000, 234)
	for k, priv := range tablePrivTable {
		trace_util_0.Count(_cache_00000, 264)
		g := tablePrivToString(priv)
		if len(g) > 0 {
			trace_util_0.Count(_cache_00000, 265)
			s := fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, g, k, user, host)
			gs = append(gs, s)
		}
	}

	// Show role grants.
	trace_util_0.Count(_cache_00000, 235)
	graphKey := user + "@" + host
	edgeTable, ok := p.RoleGraph[graphKey]
	g = ""
	if ok {
		trace_util_0.Count(_cache_00000, 266)
		for k := range edgeTable.roleList {
			trace_util_0.Count(_cache_00000, 268)
			role := strings.Split(k, "@")
			roleName, roleHost := role[0], role[1]
			if g != "" {
				trace_util_0.Count(_cache_00000, 270)
				g += ", "
			}
			trace_util_0.Count(_cache_00000, 269)
			g += fmt.Sprintf("'%s'@'%s'", roleName, roleHost)
		}
		trace_util_0.Count(_cache_00000, 267)
		s := fmt.Sprintf(`GRANT %s TO '%s'@'%s'`, g, user, host)
		gs = append(gs, s)
	}
	trace_util_0.Count(_cache_00000, 236)
	return gs
}

func userPrivToString(privs mysql.PrivilegeType) string {
	trace_util_0.Count(_cache_00000, 271)
	if privs == userTablePrivilegeMask {
		trace_util_0.Count(_cache_00000, 273)
		return mysql.AllPrivilegeLiteral
	}
	trace_util_0.Count(_cache_00000, 272)
	return privToString(privs, mysql.AllGlobalPrivs, mysql.Priv2Str)
}

func dbPrivToString(privs mysql.PrivilegeType) string {
	trace_util_0.Count(_cache_00000, 274)
	if privs == dbTablePrivilegeMask {
		trace_util_0.Count(_cache_00000, 276)
		return mysql.AllPrivilegeLiteral
	}
	trace_util_0.Count(_cache_00000, 275)
	return privToString(privs, mysql.AllDBPrivs, mysql.Priv2SetStr)
}

func tablePrivToString(privs mysql.PrivilegeType) string {
	trace_util_0.Count(_cache_00000, 277)
	if privs == tablePrivMask {
		trace_util_0.Count(_cache_00000, 279)
		return mysql.AllPrivilegeLiteral
	}
	trace_util_0.Count(_cache_00000, 278)
	return privToString(privs, mysql.AllTablePrivs, mysql.Priv2Str)
}

func privToString(priv mysql.PrivilegeType, allPrivs []mysql.PrivilegeType, allPrivNames map[mysql.PrivilegeType]string) string {
	trace_util_0.Count(_cache_00000, 280)
	pstrs := make([]string, 0, 20)
	for _, p := range allPrivs {
		trace_util_0.Count(_cache_00000, 282)
		if priv&p == 0 {
			trace_util_0.Count(_cache_00000, 284)
			continue
		}
		trace_util_0.Count(_cache_00000, 283)
		s := allPrivNames[p]
		pstrs = append(pstrs, s)
	}
	trace_util_0.Count(_cache_00000, 281)
	return strings.Join(pstrs, ",")
}

// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGE table.
func (p *MySQLPrivilege) UserPrivilegesTable() [][]types.Datum {
	trace_util_0.Count(_cache_00000, 285)
	var rows [][]types.Datum
	for _, user := range p.User {
		trace_util_0.Count(_cache_00000, 287)
		rows = appendUserPrivilegesTableRow(rows, user)
	}
	trace_util_0.Count(_cache_00000, 286)
	return rows
}

func appendUserPrivilegesTableRow(rows [][]types.Datum, user UserRecord) [][]types.Datum {
	trace_util_0.Count(_cache_00000, 288)
	var isGrantable string
	if user.Privileges&mysql.GrantPriv > 0 {
		trace_util_0.Count(_cache_00000, 291)
		isGrantable = "YES"
	} else {
		trace_util_0.Count(_cache_00000, 292)
		{
			isGrantable = "NO"
		}
	}
	trace_util_0.Count(_cache_00000, 289)
	guarantee := fmt.Sprintf("'%s'@'%s'", user.User, user.Host)

	for _, priv := range mysql.AllGlobalPrivs {
		trace_util_0.Count(_cache_00000, 293)
		if priv == mysql.GrantPriv {
			trace_util_0.Count(_cache_00000, 295)
			continue
		}
		trace_util_0.Count(_cache_00000, 294)
		if user.Privileges&priv > 0 {
			trace_util_0.Count(_cache_00000, 296)
			privilegeType := mysql.Priv2Str[priv]
			// +---------------------------+---------------+-------------------------+--------------+
			// | GRANTEE                   | TABLE_CATALOG | PRIVILEGE_TYPE          | IS_GRANTABLE |
			// +---------------------------+---------------+-------------------------+--------------+
			// | 'root'@'localhost'        | def           | SELECT                  | YES          |
			record := types.MakeDatums(guarantee, "def", privilegeType, isGrantable)
			rows = append(rows, record)
		}
	}
	trace_util_0.Count(_cache_00000, 290)
	return rows
}

func (p *MySQLPrivilege) getDefaultRoles(user, host string) []*auth.RoleIdentity {
	trace_util_0.Count(_cache_00000, 297)
	ret := make([]*auth.RoleIdentity, 0)
	for _, r := range p.DefaultRoles {
		trace_util_0.Count(_cache_00000, 299)
		if r.match(user, host) {
			trace_util_0.Count(_cache_00000, 300)
			ret = append(ret, &auth.RoleIdentity{Username: r.DefaultRoleUser, Hostname: r.DefaultRoleHost})
		}
	}
	trace_util_0.Count(_cache_00000, 298)
	return ret
}

func (p *MySQLPrivilege) getAllRoles(user, host string) []*auth.RoleIdentity {
	trace_util_0.Count(_cache_00000, 301)
	key := user + "@" + host
	edgeTable, ok := p.RoleGraph[key]
	ret := make([]*auth.RoleIdentity, 0, len(edgeTable.roleList))
	if !ok {
		trace_util_0.Count(_cache_00000, 304)
		return nil
	}
	trace_util_0.Count(_cache_00000, 302)
	for _, r := range edgeTable.roleList {
		trace_util_0.Count(_cache_00000, 305)
		ret = append(ret, r)
	}
	trace_util_0.Count(_cache_00000, 303)
	return ret
}

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	priv atomic.Value
}

// NewHandle returns a Handle.
func NewHandle() *Handle {
	trace_util_0.Count(_cache_00000, 306)
	return &Handle{}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *MySQLPrivilege {
	trace_util_0.Count(_cache_00000, 307)
	return h.priv.Load().(*MySQLPrivilege)
}

// Update loads all the privilege info from kv storage.
func (h *Handle) Update(ctx sessionctx.Context) error {
	trace_util_0.Count(_cache_00000, 308)
	var priv MySQLPrivilege
	err := priv.LoadAll(ctx)
	if err != nil {
		trace_util_0.Count(_cache_00000, 310)
		return errors.Trace(err)
	}

	trace_util_0.Count(_cache_00000, 309)
	h.priv.Store(&priv)
	return nil
}

var _cache_00000 = "privilege/privileges/cache.go"
