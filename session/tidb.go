// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package session

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	trace_util_0.Count(_tidb_00000, 0)
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only domain instance, and the caller doesn't provide store.
	if len(dm.domains) == 1 && store == nil {
		trace_util_0.Count(_tidb_00000, 5)
		for _, r := range dm.domains {
			trace_util_0.Count(_tidb_00000, 6)
			return r, nil
		}
	}

	trace_util_0.Count(_tidb_00000, 1)
	key := store.UUID()
	d = dm.domains[key]
	if d != nil {
		trace_util_0.Count(_tidb_00000, 7)
		return
	}

	trace_util_0.Count(_tidb_00000, 2)
	ddlLease := schemaLease
	statisticLease := statsLease
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		trace_util_0.Count(_tidb_00000, 8)
		logutil.Logger(context.Background()).Info("new domain",
			zap.String("store", store.UUID()),
			zap.Stringer("ddl lease", ddlLease),
			zap.Stringer("stats lease", statisticLease))
		factory := createSessionFunc(store)
		sysFactory := createSessionWithDomainFunc(store)
		d = domain.NewDomain(store, ddlLease, statisticLease, factory)
		err1 = d.Init(ddlLease, sysFactory)
		if err1 != nil {
			trace_util_0.Count(_tidb_00000, 10)
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.Logger(context.Background()).Error("[ddl] init domain failed",
				zap.Error(err1))
		}
		trace_util_0.Count(_tidb_00000, 9)
		return true, err1
	})
	trace_util_0.Count(_tidb_00000, 3)
	if err != nil {
		trace_util_0.Count(_tidb_00000, 11)
		return nil, err
	}
	trace_util_0.Count(_tidb_00000, 4)
	dm.domains[key] = d

	return
}

func (dm *domainMap) Delete(store kv.Storage) {
	trace_util_0.Count(_tidb_00000, 12)
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = 1 * time.Second

	// statsLease is the time for reload stats table.
	statsLease = 3 * time.Second
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	trace_util_0.Count(_tidb_00000, 13)
	schemaLease = lease
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	trace_util_0.Count(_tidb_00000, 14)
	statsLease = lease
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	trace_util_0.Count(_tidb_00000, 15)
	logutil.Logger(context.Background()).Debug("compiling", zap.String("source", src))
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.EnableWindowFunc(ctx.GetSessionVars().EnableWindowFunction)
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, warns, err := p.Parse(src, charset, collation)
	for _, warn := range warns {
		trace_util_0.Count(_tidb_00000, 18)
		ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
	}
	trace_util_0.Count(_tidb_00000, 16)
	if err != nil {
		trace_util_0.Count(_tidb_00000, 19)
		logutil.Logger(context.Background()).Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	trace_util_0.Count(_tidb_00000, 17)
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) (sqlexec.Statement, error) {
	trace_util_0.Count(_tidb_00000, 20)
	compiler := executor.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, err
}

func finishStmt(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars, meetsErr error) error {
	trace_util_0.Count(_tidb_00000, 21)
	if meetsErr != nil {
		trace_util_0.Count(_tidb_00000, 24)
		if !sessVars.InTxn() {
			trace_util_0.Count(_tidb_00000, 26)
			logutil.Logger(context.Background()).Info("rollbackTxn for ddl/autocommit error.")
			se.RollbackTxn(ctx)
		} else {
			trace_util_0.Count(_tidb_00000, 27)
			if se.txn.Valid() && se.txn.IsPessimistic() && executor.ErrDeadlock.Equal(meetsErr) {
				trace_util_0.Count(_tidb_00000, 28)
				logutil.Logger(context.Background()).Info("rollbackTxn for deadlock error", zap.Uint64("txn", se.txn.StartTS()))
				se.RollbackTxn(ctx)
			}
		}
		trace_util_0.Count(_tidb_00000, 25)
		return meetsErr
	}

	trace_util_0.Count(_tidb_00000, 22)
	if !sessVars.InTxn() {
		trace_util_0.Count(_tidb_00000, 29)
		return se.CommitTxn(ctx)
	}

	trace_util_0.Count(_tidb_00000, 23)
	return checkStmtLimit(ctx, sctx, se, sessVars)
}

func checkStmtLimit(ctx context.Context, sctx sessionctx.Context, se *session, sessVars *variable.SessionVars) error {
	trace_util_0.Count(_tidb_00000, 30)
	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	history := GetHistory(sctx)
	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		trace_util_0.Count(_tidb_00000, 32)
		if !sessVars.BatchCommit {
			trace_util_0.Count(_tidb_00000, 34)
			se.RollbackTxn(ctx)
			return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
				history.Count(), sctx.GetSessionVars().IsAutocommit())
		}
		trace_util_0.Count(_tidb_00000, 33)
		err = se.NewTxn(ctx)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the session is out of the transaction.
		se.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
	trace_util_0.Count(_tidb_00000, 31)
	return err
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, sctx sessionctx.Context, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	trace_util_0.Count(_tidb_00000, 35)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_tidb_00000, 41)
		span1 := span.Tracer().StartSpan("session.runStmt", opentracing.ChildOf(span.Context()))
		span1.LogKV("sql", s.OriginText())
		defer span1.Finish()
	}
	trace_util_0.Count(_tidb_00000, 36)
	se := sctx.(*session)
	defer func() {
		trace_util_0.Count(_tidb_00000, 42)
		// If it is not a select statement, we record its slow log here,
		// then it could include the transaction commit time.
		if rs == nil {
			trace_util_0.Count(_tidb_00000, 43)
			s.(*executor.ExecStmt).LogSlowQuery(se.GetSessionVars().TxnCtx.StartTS, err != nil)
		}
	}()

	trace_util_0.Count(_tidb_00000, 37)
	err = se.checkTxnAborted(s)
	if err != nil {
		trace_util_0.Count(_tidb_00000, 44)
		return nil, err
	}
	trace_util_0.Count(_tidb_00000, 38)
	rs, err = s.Exec(ctx)
	sessVars := se.GetSessionVars()
	// All the history should be added here.
	sessVars.TxnCtx.StatementCount++
	if !s.IsReadOnly(sessVars) {
		trace_util_0.Count(_tidb_00000, 45)
		if err == nil && !sessVars.TxnCtx.IsPessimistic {
			trace_util_0.Count(_tidb_00000, 47)
			GetHistory(sctx).Add(0, s, se.sessionVars.StmtCtx)
		}
		trace_util_0.Count(_tidb_00000, 46)
		if txn, err1 := sctx.Txn(false); err1 == nil {
			trace_util_0.Count(_tidb_00000, 48)
			if txn.Valid() {
				trace_util_0.Count(_tidb_00000, 49)
				if err != nil {
					trace_util_0.Count(_tidb_00000, 50)
					sctx.StmtRollback()
				} else {
					trace_util_0.Count(_tidb_00000, 51)
					{
						err = sctx.StmtCommit()
					}
				}
			}
		} else {
			trace_util_0.Count(_tidb_00000, 52)
			{
				logutil.Logger(context.Background()).Error("get txn error", zap.Error(err1))
			}
		}
	}

	trace_util_0.Count(_tidb_00000, 39)
	err = finishStmt(ctx, sctx, se, sessVars, err)
	if se.txn.pending() {
		trace_util_0.Count(_tidb_00000, 53)
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@tidb_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}
	trace_util_0.Count(_tidb_00000, 40)
	return rs, err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx sessionctx.Context) *StmtHistory {
	trace_util_0.Count(_tidb_00000, 54)
	hist, ok := ctx.GetSessionVars().TxnCtx.History.(*StmtHistory)
	if ok {
		trace_util_0.Count(_tidb_00000, 56)
		return hist
	}
	trace_util_0.Count(_tidb_00000, 55)
	hist = new(StmtHistory)
	ctx.GetSessionVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	trace_util_0.Count(_tidb_00000, 57)
	if rs == nil {
		trace_util_0.Count(_tidb_00000, 60)
		return nil, nil
	}
	trace_util_0.Count(_tidb_00000, 58)
	var rows []chunk.Row
	req := rs.NewRecordBatch()
	for {
		trace_util_0.Count(_tidb_00000, 61)
		// Since we collect all the rows, we can not reuse the chunk.
		iter := chunk.NewIterator4Chunk(req.Chunk)

		err := rs.Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_tidb_00000, 65)
			return nil, err
		}
		trace_util_0.Count(_tidb_00000, 62)
		if req.NumRows() == 0 {
			trace_util_0.Count(_tidb_00000, 66)
			break
		}

		trace_util_0.Count(_tidb_00000, 63)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			trace_util_0.Count(_tidb_00000, 67)
			rows = append(rows, row)
		}
		trace_util_0.Count(_tidb_00000, 64)
		req.Chunk = chunk.Renew(req.Chunk, sctx.GetSessionVars().MaxChunkSize)
	}
	trace_util_0.Count(_tidb_00000, 59)
	return rows, nil
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}

func trimSQL(sql string) string {
	trace_util_0.Count(_tidb_00000, 68)
	// Trim space.
	sql = strings.TrimSpace(sql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(sql, "/*") {
		trace_util_0.Count(_tidb_00000, 70)
		i := strings.Index(sql, "*/")
		if i != -1 && i < len(sql)+1 {
			trace_util_0.Count(_tidb_00000, 72)
			sql = sql[i+2:]
			sql = strings.TrimSpace(sql)
			continue
		}
		trace_util_0.Count(_tidb_00000, 71)
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	trace_util_0.Count(_tidb_00000, 69)
	return strings.TrimLeft(sql, "( ")
}

// IsQuery checks if a sql statement is a query statement.
func IsQuery(sql string) bool {
	trace_util_0.Count(_tidb_00000, 73)
	sqlText := strings.ToLower(trimSQL(sql))
	for _, key := range queryStmtTable {
		trace_util_0.Count(_tidb_00000, 75)
		if strings.HasPrefix(sqlText, key) {
			trace_util_0.Count(_tidb_00000, 76)
			return true
		}
	}

	trace_util_0.Count(_tidb_00000, 74)
	return false
}

var (
	errForUpdateCantRetry = terror.ClassSession.New(codeForUpdateCantRetry,
		mysql.MySQLErrName[mysql.ErrForUpdateCantRetry])
)

const (
	codeForUpdateCantRetry terror.ErrCode = mysql.ErrForUpdateCantRetry
)

func init() {
	trace_util_0.Count(_tidb_00000, 77)
	sessionMySQLErrCodes := map[terror.ErrCode]uint16{
		codeForUpdateCantRetry: mysql.ErrForUpdateCantRetry,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSession] = sessionMySQLErrCodes
}

var _tidb_00000 = "session/tidb.go"
