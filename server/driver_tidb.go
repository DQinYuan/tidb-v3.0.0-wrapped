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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// TiDBDriver implements IDriver.
type TiDBDriver struct {
	store kv.Storage
}

// NewTiDBDriver creates a new TiDBDriver.
func NewTiDBDriver(store kv.Storage) *TiDBDriver {
	trace_util_0.Count(_driver_tidb_00000, 0)
	driver := &TiDBDriver{
		store: store,
	}
	return driver
}

// TiDBContext implements QueryCtx.
type TiDBContext struct {
	session   session.Session
	currentDB string
	stmts     map[int]*TiDBStatement
}

// TiDBStatement implements PreparedStatement.
type TiDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *TiDBContext
	rs          ResultSet
	sql         string
}

// ID implements PreparedStatement ID method.
func (ts *TiDBStatement) ID() int {
	trace_util_0.Count(_driver_tidb_00000, 1)
	return int(ts.id)
}

// Execute implements PreparedStatement Execute method.
func (ts *TiDBStatement) Execute(ctx context.Context, args ...interface{}) (rs ResultSet, err error) {
	trace_util_0.Count(_driver_tidb_00000, 2)
	tidbRecordset, err := ts.ctx.session.ExecutePreparedStmt(ctx, ts.id, args...)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 5)
		return nil, err
	}
	trace_util_0.Count(_driver_tidb_00000, 3)
	if tidbRecordset == nil {
		trace_util_0.Count(_driver_tidb_00000, 6)
		return
	}
	trace_util_0.Count(_driver_tidb_00000, 4)
	rs = &tidbResultSet{
		recordSet: tidbRecordset,
	}
	return
}

// AppendParam implements PreparedStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	trace_util_0.Count(_driver_tidb_00000, 7)
	if paramID >= len(ts.boundParams) {
		trace_util_0.Count(_driver_tidb_00000, 10)
		return mysql.NewErr(mysql.ErrWrongArguments, "stmt_send_longdata")
	}
	// If len(data) is 0, append an empty byte slice to the end to distinguish no data and no parameter.
	trace_util_0.Count(_driver_tidb_00000, 8)
	if len(data) == 0 {
		trace_util_0.Count(_driver_tidb_00000, 11)
		ts.boundParams[paramID] = []byte{}
	} else {
		trace_util_0.Count(_driver_tidb_00000, 12)
		{
			ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
		}
	}
	trace_util_0.Count(_driver_tidb_00000, 9)
	return nil
}

// NumParams implements PreparedStatement NumParams method.
func (ts *TiDBStatement) NumParams() int {
	trace_util_0.Count(_driver_tidb_00000, 13)
	return ts.numParams
}

// BoundParams implements PreparedStatement BoundParams method.
func (ts *TiDBStatement) BoundParams() [][]byte {
	trace_util_0.Count(_driver_tidb_00000, 14)
	return ts.boundParams
}

// SetParamsType implements PreparedStatement SetParamsType method.
func (ts *TiDBStatement) SetParamsType(paramsType []byte) {
	trace_util_0.Count(_driver_tidb_00000, 15)
	ts.paramsType = paramsType
}

// GetParamsType implements PreparedStatement GetParamsType method.
func (ts *TiDBStatement) GetParamsType() []byte {
	trace_util_0.Count(_driver_tidb_00000, 16)
	return ts.paramsType
}

// StoreResultSet stores ResultSet for stmt fetching
func (ts *TiDBStatement) StoreResultSet(rs ResultSet) {
	trace_util_0.Count(_driver_tidb_00000, 17)
	// refer to https://dev.mysql.com/doc/refman/5.7/en/cursor-restrictions.html
	// You can have open only a single cursor per prepared statement.
	// closing previous ResultSet before associating a new ResultSet with this statement
	// if it exists
	if ts.rs != nil {
		trace_util_0.Count(_driver_tidb_00000, 19)
		terror.Call(ts.rs.Close)
	}
	trace_util_0.Count(_driver_tidb_00000, 18)
	ts.rs = rs
}

// GetResultSet gets ResultSet associated this statement
func (ts *TiDBStatement) GetResultSet() ResultSet {
	trace_util_0.Count(_driver_tidb_00000, 20)
	return ts.rs
}

// Reset implements PreparedStatement Reset method.
func (ts *TiDBStatement) Reset() {
	trace_util_0.Count(_driver_tidb_00000, 21)
	for i := range ts.boundParams {
		trace_util_0.Count(_driver_tidb_00000, 23)
		ts.boundParams[i] = nil
	}

	// closing previous ResultSet if it exists
	trace_util_0.Count(_driver_tidb_00000, 22)
	if ts.rs != nil {
		trace_util_0.Count(_driver_tidb_00000, 24)
		terror.Call(ts.rs.Close)
		ts.rs = nil
	}
}

// Close implements PreparedStatement Close method.
func (ts *TiDBStatement) Close() error {
	trace_util_0.Count(_driver_tidb_00000, 25)
	//TODO close at tidb level
	err := ts.ctx.session.DropPreparedStmt(ts.id)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 28)
		return err
	}
	trace_util_0.Count(_driver_tidb_00000, 26)
	delete(ts.ctx.stmts, int(ts.id))

	// close ResultSet associated with this statement
	if ts.rs != nil {
		trace_util_0.Count(_driver_tidb_00000, 29)
		terror.Call(ts.rs.Close)
	}
	trace_util_0.Count(_driver_tidb_00000, 27)
	return nil
}

// OpenCtx implements IDriver.
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (QueryCtx, error) {
	trace_util_0.Count(_driver_tidb_00000, 30)
	se, err := session.CreateSession(qd.store)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 33)
		return nil, err
	}
	trace_util_0.Count(_driver_tidb_00000, 31)
	se.SetTLSState(tlsState)
	err = se.SetCollation(int(collation))
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 34)
		return nil, err
	}
	trace_util_0.Count(_driver_tidb_00000, 32)
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &TiDBContext{
		session:   se,
		currentDB: dbname,
		stmts:     make(map[int]*TiDBStatement),
	}
	return tc, nil
}

// Status implements QueryCtx Status method.
func (tc *TiDBContext) Status() uint16 {
	trace_util_0.Count(_driver_tidb_00000, 35)
	return tc.session.Status()
}

// LastInsertID implements QueryCtx LastInsertID method.
func (tc *TiDBContext) LastInsertID() uint64 {
	trace_util_0.Count(_driver_tidb_00000, 36)
	return tc.session.LastInsertID()
}

// Value implements QueryCtx Value method.
func (tc *TiDBContext) Value(key fmt.Stringer) interface{} {
	trace_util_0.Count(_driver_tidb_00000, 37)
	return tc.session.Value(key)
}

// SetValue implements QueryCtx SetValue method.
func (tc *TiDBContext) SetValue(key fmt.Stringer, value interface{}) {
	trace_util_0.Count(_driver_tidb_00000, 38)
	tc.session.SetValue(key, value)
}

// CommitTxn implements QueryCtx CommitTxn method.
func (tc *TiDBContext) CommitTxn(ctx context.Context) error {
	trace_util_0.Count(_driver_tidb_00000, 39)
	return tc.session.CommitTxn(ctx)
}

// SetProcessInfo implements QueryCtx SetProcessInfo method.
func (tc *TiDBContext) SetProcessInfo(sql string, t time.Time, command byte) {
	trace_util_0.Count(_driver_tidb_00000, 40)
	tc.session.SetProcessInfo(sql, t, command)
}

// RollbackTxn implements QueryCtx RollbackTxn method.
func (tc *TiDBContext) RollbackTxn() {
	trace_util_0.Count(_driver_tidb_00000, 41)
	tc.session.RollbackTxn(context.TODO())
}

// AffectedRows implements QueryCtx AffectedRows method.
func (tc *TiDBContext) AffectedRows() uint64 {
	trace_util_0.Count(_driver_tidb_00000, 42)
	return tc.session.AffectedRows()
}

// LastMessage implements QueryCtx LastMessage method.
func (tc *TiDBContext) LastMessage() string {
	trace_util_0.Count(_driver_tidb_00000, 43)
	return tc.session.LastMessage()
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *TiDBContext) CurrentDB() string {
	trace_util_0.Count(_driver_tidb_00000, 44)
	return tc.currentDB
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	trace_util_0.Count(_driver_tidb_00000, 45)
	return tc.session.GetSessionVars().StmtCtx.WarningCount()
}

// Execute implements QueryCtx Execute method.
func (tc *TiDBContext) Execute(ctx context.Context, sql string) (rs []ResultSet, err error) {
	trace_util_0.Count(_driver_tidb_00000, 46)
	rsList, err := tc.session.Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 50)
		return
	}
	trace_util_0.Count(_driver_tidb_00000, 47)
	if len(rsList) == 0 {
		trace_util_0.Count(_driver_tidb_00000, 51) // result ok
		return
	}
	trace_util_0.Count(_driver_tidb_00000, 48)
	rs = make([]ResultSet, len(rsList))
	for i := 0; i < len(rsList); i++ {
		trace_util_0.Count(_driver_tidb_00000, 52)
		rs[i] = &tidbResultSet{
			recordSet: rsList[i],
		}
	}
	trace_util_0.Count(_driver_tidb_00000, 49)
	return
}

// SetSessionManager implements the QueryCtx interface.
func (tc *TiDBContext) SetSessionManager(sm util.SessionManager) {
	trace_util_0.Count(_driver_tidb_00000, 53)
	tc.session.SetSessionManager(sm)
}

// SetClientCapability implements QueryCtx SetClientCapability method.
func (tc *TiDBContext) SetClientCapability(flags uint32) {
	trace_util_0.Count(_driver_tidb_00000, 54)
	tc.session.SetClientCapability(flags)
}

// Close implements QueryCtx Close method.
func (tc *TiDBContext) Close() error {
	trace_util_0.Count(_driver_tidb_00000, 55)
	// close PreparedStatement associated with this connection
	for _, v := range tc.stmts {
		trace_util_0.Count(_driver_tidb_00000, 57)
		terror.Call(v.Close)
	}

	trace_util_0.Count(_driver_tidb_00000, 56)
	tc.session.Close()
	return nil
}

// Auth implements QueryCtx Auth method.
func (tc *TiDBContext) Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool {
	trace_util_0.Count(_driver_tidb_00000, 58)
	return tc.session.Auth(user, auth, salt)
}

// FieldList implements QueryCtx FieldList method.
func (tc *TiDBContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	trace_util_0.Count(_driver_tidb_00000, 59)
	fields, err := tc.session.FieldList(table)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 62)
		return nil, err
	}
	trace_util_0.Count(_driver_tidb_00000, 60)
	columns = make([]*ColumnInfo, 0, len(fields))
	for _, f := range fields {
		trace_util_0.Count(_driver_tidb_00000, 63)
		columns = append(columns, convertColumnInfo(f))
	}
	trace_util_0.Count(_driver_tidb_00000, 61)
	return columns, nil
}

// GetStatement implements QueryCtx GetStatement method.
func (tc *TiDBContext) GetStatement(stmtID int) PreparedStatement {
	trace_util_0.Count(_driver_tidb_00000, 64)
	tcStmt := tc.stmts[stmtID]
	if tcStmt != nil {
		trace_util_0.Count(_driver_tidb_00000, 66)
		return tcStmt
	}
	trace_util_0.Count(_driver_tidb_00000, 65)
	return nil
}

// Prepare implements QueryCtx Prepare method.
func (tc *TiDBContext) Prepare(sql string) (statement PreparedStatement, columns, params []*ColumnInfo, err error) {
	trace_util_0.Count(_driver_tidb_00000, 67)
	stmtID, paramCount, fields, err := tc.session.PrepareStmt(sql)
	if err != nil {
		trace_util_0.Count(_driver_tidb_00000, 71)
		return
	}
	trace_util_0.Count(_driver_tidb_00000, 68)
	stmt := &TiDBStatement{
		sql:         sql,
		id:          stmtID,
		numParams:   paramCount,
		boundParams: make([][]byte, paramCount),
		ctx:         tc,
	}
	statement = stmt
	columns = make([]*ColumnInfo, len(fields))
	for i := range fields {
		trace_util_0.Count(_driver_tidb_00000, 72)
		columns[i] = convertColumnInfo(fields[i])
	}
	trace_util_0.Count(_driver_tidb_00000, 69)
	params = make([]*ColumnInfo, paramCount)
	for i := range params {
		trace_util_0.Count(_driver_tidb_00000, 73)
		params[i] = &ColumnInfo{
			Type: mysql.TypeBlob,
		}
	}
	trace_util_0.Count(_driver_tidb_00000, 70)
	tc.stmts[int(stmtID)] = stmt
	return
}

// ShowProcess implements QueryCtx ShowProcess method.
func (tc *TiDBContext) ShowProcess() *util.ProcessInfo {
	trace_util_0.Count(_driver_tidb_00000, 74)
	return tc.session.ShowProcess()
}

// SetCommandValue implements QueryCtx SetCommandValue method.
func (tc *TiDBContext) SetCommandValue(command byte) {
	trace_util_0.Count(_driver_tidb_00000, 75)
	tc.session.SetCommandValue(command)
}

// GetSessionVars return SessionVars.
func (tc *TiDBContext) GetSessionVars() *variable.SessionVars {
	trace_util_0.Count(_driver_tidb_00000, 76)
	return tc.session.GetSessionVars()
}

type tidbResultSet struct {
	recordSet sqlexec.RecordSet
	columns   []*ColumnInfo
	rows      []chunk.Row
	closed    int32
}

func (trs *tidbResultSet) NewRecordBatch() *chunk.RecordBatch {
	trace_util_0.Count(_driver_tidb_00000, 77)
	return trs.recordSet.NewRecordBatch()
}

func (trs *tidbResultSet) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_driver_tidb_00000, 78)
	return trs.recordSet.Next(ctx, req)
}

func (trs *tidbResultSet) StoreFetchedRows(rows []chunk.Row) {
	trace_util_0.Count(_driver_tidb_00000, 79)
	trs.rows = rows
}

func (trs *tidbResultSet) GetFetchedRows() []chunk.Row {
	trace_util_0.Count(_driver_tidb_00000, 80)
	if trs.rows == nil {
		trace_util_0.Count(_driver_tidb_00000, 82)
		trs.rows = make([]chunk.Row, 0, 1024)
	}
	trace_util_0.Count(_driver_tidb_00000, 81)
	return trs.rows
}

func (trs *tidbResultSet) Close() error {
	trace_util_0.Count(_driver_tidb_00000, 83)
	if !atomic.CompareAndSwapInt32(&trs.closed, 0, 1) {
		trace_util_0.Count(_driver_tidb_00000, 85)
		return nil
	}
	trace_util_0.Count(_driver_tidb_00000, 84)
	return trs.recordSet.Close()
}

func (trs *tidbResultSet) Columns() []*ColumnInfo {
	trace_util_0.Count(_driver_tidb_00000, 86)
	if trs.columns == nil {
		trace_util_0.Count(_driver_tidb_00000, 88)
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trace_util_0.Count(_driver_tidb_00000, 89)
			trs.columns = append(trs.columns, convertColumnInfo(v))
		}
	}
	trace_util_0.Count(_driver_tidb_00000, 87)
	return trs.columns
}

func convertColumnInfo(fld *ast.ResultField) (ci *ColumnInfo) {
	trace_util_0.Count(_driver_tidb_00000, 90)
	ci = &ColumnInfo{
		Name:    fld.ColumnAsName.O,
		OrgName: fld.Column.Name.O,
		Table:   fld.TableAsName.O,
		Schema:  fld.DBName.O,
		Flag:    uint16(fld.Column.Flag),
		Charset: uint16(mysql.CharsetNameToID(fld.Column.Charset)),
		Type:    fld.Column.Tp,
	}

	if fld.Table != nil {
		trace_util_0.Count(_driver_tidb_00000, 96)
		ci.OrgTable = fld.Table.Name.O
	}
	trace_util_0.Count(_driver_tidb_00000, 91)
	if fld.Column.Flen == types.UnspecifiedLength {
		trace_util_0.Count(_driver_tidb_00000, 97)
		ci.ColumnLength = 0
	} else {
		trace_util_0.Count(_driver_tidb_00000, 98)
		{
			ci.ColumnLength = uint32(fld.Column.Flen)
		}
	}
	trace_util_0.Count(_driver_tidb_00000, 92)
	if fld.Column.Tp == mysql.TypeNewDecimal {
		trace_util_0.Count(_driver_tidb_00000, 99)
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.Decimal > types.DefaultFsp {
			trace_util_0.Count(_driver_tidb_00000, 100)
			// Consider the decimal point.
			ci.ColumnLength++
		}
	} else {
		trace_util_0.Count(_driver_tidb_00000, 101)
		if types.IsString(fld.Column.Tp) {
			trace_util_0.Count(_driver_tidb_00000, 102)
			// Fix issue #4540.
			// The flen is a hint, not a precise value, so most client will not use the value.
			// But we found in rare MySQL client, like Navicat for MySQL(version before 12) will truncate
			// the `show create table` result. To fix this case, we must use a large enough flen to prevent
			// the truncation, in MySQL, it will multiply bytes length by a multiple based on character set.
			// For examples:
			// * latin, the multiple is 1
			// * gb2312, the multiple is 2
			// * Utf-8, the multiple is 3
			// * utf8mb4, the multiple is 4
			// We used to check non-string types to avoid the truncation problem in some MySQL
			// client such as Navicat. Now we only allow string type enter this branch.
			charsetDesc, err := charset.GetCharsetDesc(fld.Column.Charset)
			if err != nil {
				trace_util_0.Count(_driver_tidb_00000, 103)
				ci.ColumnLength = ci.ColumnLength * 4
			} else {
				trace_util_0.Count(_driver_tidb_00000, 104)
				{
					ci.ColumnLength = ci.ColumnLength * uint32(charsetDesc.Maxlen)
				}
			}
		}
	}

	trace_util_0.Count(_driver_tidb_00000, 93)
	if fld.Column.Decimal == types.UnspecifiedLength {
		trace_util_0.Count(_driver_tidb_00000, 105)
		if fld.Column.Tp == mysql.TypeDuration {
			trace_util_0.Count(_driver_tidb_00000, 106)
			ci.Decimal = types.DefaultFsp
		} else {
			trace_util_0.Count(_driver_tidb_00000, 107)
			{
				ci.Decimal = mysql.NotFixedDec
			}
		}
	} else {
		trace_util_0.Count(_driver_tidb_00000, 108)
		{
			ci.Decimal = uint8(fld.Column.Decimal)
		}
	}

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	trace_util_0.Count(_driver_tidb_00000, 94)
	if ci.Type == mysql.TypeVarchar {
		trace_util_0.Count(_driver_tidb_00000, 109)
		ci.Type = mysql.TypeVarString
	}
	trace_util_0.Count(_driver_tidb_00000, 95)
	return
}

var _driver_tidb_00000 = "server/driver_tidb.go"
