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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

var (
	statementPerTransactionInternalOK    = metrics.StatementPerTransaction.WithLabelValues(metrics.LblInternal, "ok")
	statementPerTransactionInternalError = metrics.StatementPerTransaction.WithLabelValues(metrics.LblInternal, "error")
	statementPerTransactionGeneralOK     = metrics.StatementPerTransaction.WithLabelValues(metrics.LblGeneral, "ok")
	statementPerTransactionGeneralError  = metrics.StatementPerTransaction.WithLabelValues(metrics.LblGeneral, "error")
	transactionDurationInternalOK        = metrics.TransactionDuration.WithLabelValues(metrics.LblInternal, "ok")
	transactionDurationInternalError     = metrics.TransactionDuration.WithLabelValues(metrics.LblInternal, "error")
	transactionDurationGeneralOK         = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, "ok")
	transactionDurationGeneralError      = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, "error")

	transactionCounterInternalOK  = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblOK)
	transactionCounterInternalErr = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblError)
	transactionCounterGeneralOK   = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	transactionCounterGeneralErr  = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)

	transactionRollbackCounterInternal = metrics.TransactionCounter.WithLabelValues(metrics.LblInternal, metrics.LblRollback)
	transactionRollbackCounterGeneral  = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblRollback)

	sessionExecuteRunDurationInternal = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteRunDurationGeneral  = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblGeneral)

	sessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteCompileDurationGeneral  = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	sessionExecuteParseDurationInternal   = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteParseDurationGeneral    = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)
)

// Session context
type Session interface {
	sessionctx.Context
	Status() uint16                                               // Flag of current status, such as autocommit.
	LastInsertID() uint64                                         // LastInsertID is the last inserted auto_increment ID.
	LastMessage() string                                          // LastMessage is the info message that may be generated by last command
	AffectedRows() uint64                                         // Affected rows by latest executed stmt.
	Execute(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	String() string                                               // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param ...interface{}) (sqlexec.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetCommandValue(byte)
	SetProcessInfo(string, time.Time, byte)
	SetTLSState(*tls.ConnectionState)
	SetCollation(coID int) error
	SetSessionManager(util.SessionManager)
	Close()
	Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool
	ShowProcess() *util.ProcessInfo
	// PrePareTxnCtx is exported for test.
	PrepareTxnCtx(context.Context)
	// FieldList returns fields list of a table.
	FieldList(tableName string) (fields []*ast.ResultField, err error)
}

var (
	_ Session = (*session)(nil)
)

type stmtRecord struct {
	stmtID  uint32
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
	params  []interface{}
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(stmtID uint32, st sqlexec.Statement, stmtCtx *stmtctx.StatementContext, params ...interface{}) {
	trace_util_0.Count(_session_00000, 0)
	s := &stmtRecord{
		stmtID:  stmtID,
		st:      st,
		stmtCtx: stmtCtx,
		params:  append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	trace_util_0.Count(_session_00000, 1)
	return len(h.history)
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Value
	txn         TxnState

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	currentPlan plannercore.Plan

	store kv.Storage

	parser *parser.Parser

	preparedPlanCache *kvcache.SimpleLRUCache

	sessionVars    *variable.SessionVars
	sessionManager util.SessionManager

	statsCollector *handle.SessionStatsCollector
	// ddlOwnerChecker is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerChecker owner.DDLOwnerChecker
}

// DDLOwnerChecker returns s.ddlOwnerChecker.
func (s *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	trace_util_0.Count(_session_00000, 2)
	return s.ddlOwnerChecker
}

func (s *session) getMembufCap() int {
	trace_util_0.Count(_session_00000, 3)
	if s.sessionVars.LightningMode {
		trace_util_0.Count(_session_00000, 5)
		return kv.ImportingTxnMembufCap
	}

	trace_util_0.Count(_session_00000, 4)
	return kv.DefaultTxnMembufCap
}

func (s *session) cleanRetryInfo() {
	trace_util_0.Count(_session_00000, 6)
	if s.sessionVars.RetryInfo.Retrying {
		trace_util_0.Count(_session_00000, 10)
		return
	}

	trace_util_0.Count(_session_00000, 7)
	retryInfo := s.sessionVars.RetryInfo
	defer retryInfo.Clean()
	if len(retryInfo.DroppedPreparedStmtIDs) == 0 {
		trace_util_0.Count(_session_00000, 11)
		return
	}

	trace_util_0.Count(_session_00000, 8)
	planCacheEnabled := plannercore.PreparedPlanCacheEnabled()
	var cacheKey kvcache.Key
	if planCacheEnabled {
		trace_util_0.Count(_session_00000, 12)
		firstStmtID := retryInfo.DroppedPreparedStmtIDs[0]
		cacheKey = plannercore.NewPSTMTPlanCacheKey(
			s.sessionVars, firstStmtID, s.sessionVars.PreparedStmts[firstStmtID].SchemaVersion,
		)
	}
	trace_util_0.Count(_session_00000, 9)
	for i, stmtID := range retryInfo.DroppedPreparedStmtIDs {
		trace_util_0.Count(_session_00000, 13)
		if planCacheEnabled {
			trace_util_0.Count(_session_00000, 15)
			if i > 0 {
				trace_util_0.Count(_session_00000, 17)
				plannercore.SetPstmtIDSchemaVersion(cacheKey, stmtID, s.sessionVars.PreparedStmts[stmtID].SchemaVersion)
			}
			trace_util_0.Count(_session_00000, 16)
			s.PreparedPlanCache().Delete(cacheKey)
		}
		trace_util_0.Count(_session_00000, 14)
		s.sessionVars.RemovePreparedStmt(stmtID)
	}
}

func (s *session) Status() uint16 {
	trace_util_0.Count(_session_00000, 18)
	return s.sessionVars.Status
}

func (s *session) LastInsertID() uint64 {
	trace_util_0.Count(_session_00000, 19)
	if s.sessionVars.StmtCtx.LastInsertID > 0 {
		trace_util_0.Count(_session_00000, 21)
		return s.sessionVars.StmtCtx.LastInsertID
	}
	trace_util_0.Count(_session_00000, 20)
	return s.sessionVars.StmtCtx.InsertID
}

func (s *session) LastMessage() string {
	trace_util_0.Count(_session_00000, 22)
	return s.sessionVars.StmtCtx.GetMessage()
}

func (s *session) AffectedRows() uint64 {
	trace_util_0.Count(_session_00000, 23)
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	trace_util_0.Count(_session_00000, 24)
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	trace_util_0.Count(_session_00000, 25)
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	trace_util_0.Count(_session_00000, 26)
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		trace_util_0.Count(_session_00000, 27)
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCommandValue(command byte) {
	trace_util_0.Count(_session_00000, 28)
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *session) GetTLSState() *tls.ConnectionState {
	trace_util_0.Count(_session_00000, 29)
	return s.sessionVars.TLSConnectionState
}

func (s *session) SetCollation(coID int) error {
	trace_util_0.Count(_session_00000, 30)
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		trace_util_0.Count(_session_00000, 33)
		return err
	}
	trace_util_0.Count(_session_00000, 31)
	for _, v := range variable.SetNamesVariables {
		trace_util_0.Count(_session_00000, 34)
		terror.Log(s.sessionVars.SetSystemVar(v, cs))
	}
	trace_util_0.Count(_session_00000, 32)
	terror.Log(s.sessionVars.SetSystemVar(variable.CollationConnection, co))
	return nil
}

func (s *session) PreparedPlanCache() *kvcache.SimpleLRUCache {
	trace_util_0.Count(_session_00000, 35)
	return s.preparedPlanCache
}

func (s *session) SetSessionManager(sm util.SessionManager) {
	trace_util_0.Count(_session_00000, 36)
	s.sessionManager = sm
}

func (s *session) GetSessionManager() util.SessionManager {
	trace_util_0.Count(_session_00000, 37)
	return s.sessionManager
}

func (s *session) StoreQueryFeedback(feedback interface{}) {
	trace_util_0.Count(_session_00000, 38)
	if s.statsCollector != nil {
		trace_util_0.Count(_session_00000, 39)
		do, err := GetDomain(s.store)
		if err != nil {
			trace_util_0.Count(_session_00000, 42)
			logutil.Logger(context.Background()).Debug("domain not found", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		trace_util_0.Count(_session_00000, 40)
		err = s.statsCollector.StoreQueryFeedback(feedback, do.StatsHandle())
		if err != nil {
			trace_util_0.Count(_session_00000, 43)
			logutil.Logger(context.Background()).Debug("store query feedback", zap.Error(err))
			metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
			return
		}
		trace_util_0.Count(_session_00000, 41)
		metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*ast.ResultField, error) {
	trace_util_0.Count(_session_00000, 44)
	is := executor.GetInfoSchema(s)
	dbName := model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := model.NewCIStr(tableName)
	table, err := is.TableByName(dbName, tName)
	if err != nil {
		trace_util_0.Count(_session_00000, 47)
		return nil, err
	}

	trace_util_0.Count(_session_00000, 45)
	cols := table.Cols()
	fields := make([]*ast.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		trace_util_0.Count(_session_00000, 48)
		rf := &ast.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	trace_util_0.Count(_session_00000, 46)
	return fields, nil
}

// mockCommitErrorOnce use to make sure gofail mockCommitError only mock commit error once.
var mockCommitErrorOnce = true

func (s *session) doCommit(ctx context.Context) error {
	trace_util_0.Count(_session_00000, 49)
	if !s.txn.Valid() {
		trace_util_0.Count(_session_00000, 56)
		return nil
	}
	trace_util_0.Count(_session_00000, 50)
	defer func() {
		trace_util_0.Count(_session_00000, 57)
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()
	trace_util_0.Count(_session_00000, 51)
	if s.txn.IsReadOnly() {
		trace_util_0.Count(_session_00000, 58)
		return nil
	}

	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	trace_util_0.Count(_session_00000, 52)
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		trace_util_0.Count(_session_00000, 59)
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			trace_util_0.Count(_session_00000, 60)
			kv.MockCommitErrorDisable()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	trace_util_0.Count(_session_00000, 53)
	if s.sessionVars.BinlogClient != nil {
		trace_util_0.Count(_session_00000, 61)
		prewriteValue := binloginfo.GetPrewriteValue(s, false)
		if prewriteValue != nil {
			trace_util_0.Count(_session_00000, 62)
			prewriteData, err := prewriteValue.Marshal()
			if err != nil {
				trace_util_0.Count(_session_00000, 64)
				return errors.Trace(err)
			}
			trace_util_0.Count(_session_00000, 63)
			info := &binloginfo.BinlogInfo{
				Data: &binlog.Binlog{
					Tp:            binlog.BinlogType_Prewrite,
					PrewriteValue: prewriteData,
				},
				Client: s.sessionVars.BinlogClient,
			}
			s.txn.SetOption(kv.BinlogInfo, info)
		}
	}

	// Get the related table IDs.
	trace_util_0.Count(_session_00000, 54)
	relatedTables := s.GetSessionVars().TxnCtx.TableDeltaMap
	tableIDs := make([]int64, 0, len(relatedTables))
	for id := range relatedTables {
		trace_util_0.Count(_session_00000, 65)
		tableIDs = append(tableIDs, id)
	}
	// Set this option for 2 phase commit to validate schema lease.
	trace_util_0.Count(_session_00000, 55)
	s.txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(domain.GetDomain(s), s.sessionVars.TxnCtx.SchemaVersion, tableIDs))

	return s.txn.Commit(sessionctx.SetCommitCtx(ctx, s))
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	trace_util_0.Count(_session_00000, 66)
	var txnSize int
	var isPessimistic bool
	if s.txn.Valid() {
		trace_util_0.Count(_session_00000, 72)
		txnSize = s.txn.Size()
		isPessimistic = s.txn.IsPessimistic()
	}
	trace_util_0.Count(_session_00000, 67)
	err := s.doCommit(ctx)
	if err != nil {
		trace_util_0.Count(_session_00000, 73)
		commitRetryLimit := s.sessionVars.RetryLimit
		if s.sessionVars.DisableTxnAutoRetry && !s.sessionVars.InRestrictedSQL {
			trace_util_0.Count(_session_00000, 75)
			// Do not retry non-autocommit transactions.
			// For autocommit single statement transactions, the history count is always 1.
			// For explicit transactions, the statement count is more than 1.
			history := GetHistory(s)
			if history.Count() > 1 {
				trace_util_0.Count(_session_00000, 76)
				commitRetryLimit = 0
			}
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		trace_util_0.Count(_session_00000, 74)
		if s.isTxnRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 && !isPessimistic {
			trace_util_0.Count(_session_00000, 77)
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", s.getSQLLabel()),
				zap.Error(err),
				zap.String("txn", s.txn.GoString()))
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit)
			maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, uint(maxRetryCount))
		}
	}
	trace_util_0.Count(_session_00000, 68)
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	s.recordOnTransactionExecution(err, counter, duration)
	s.cleanRetryInfo()

	if isoLevelOneShot := &s.sessionVars.TxnIsolationLevelOneShot; isoLevelOneShot.State != 0 {
		trace_util_0.Count(_session_00000, 78)
		switch isoLevelOneShot.State {
		case 1:
			trace_util_0.Count(_session_00000, 79)
			isoLevelOneShot.State = 2
		case 2:
			trace_util_0.Count(_session_00000, 80)
			isoLevelOneShot.State = 0
			isoLevelOneShot.Value = ""
		}
	}

	trace_util_0.Count(_session_00000, 69)
	if err != nil {
		trace_util_0.Count(_session_00000, 81)
		logutil.Logger(ctx).Warn("commit failed",
			zap.String("finished txn", s.txn.GoString()),
			zap.Error(err))
		return err
	}
	trace_util_0.Count(_session_00000, 70)
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		trace_util_0.Count(_session_00000, 82)
		for id, item := range mapper {
			trace_util_0.Count(_session_00000, 83)
			s.statsCollector.Update(id, item.Delta, item.Count, &item.ColSize)
		}
	}
	trace_util_0.Count(_session_00000, 71)
	return nil
}

func (s *session) CommitTxn(ctx context.Context) error {
	trace_util_0.Count(_session_00000, 84)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_session_00000, 87)
		span1 := span.Tracer().StartSpan("session.CommitTxn", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_session_00000, 85)
	var commitDetail *execdetails.CommitDetails
	ctx = context.WithValue(ctx, execdetails.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		trace_util_0.Count(_session_00000, 88)
		s.sessionVars.StmtCtx.MergeExecDetails(nil, commitDetail)
	}
	trace_util_0.Count(_session_00000, 86)
	s.sessionVars.TxnCtx.Cleanup()
	s.recordTransactionCounter(err)
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	trace_util_0.Count(_session_00000, 89)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_session_00000, 92)
		span1 := span.Tracer().StartSpan("session.RollbackTxn", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_session_00000, 90)
	if s.txn.Valid() {
		trace_util_0.Count(_session_00000, 93)
		terror.Log(s.txn.Rollback())
		if s.isInternal() {
			trace_util_0.Count(_session_00000, 94)
			transactionRollbackCounterInternal.Inc()
		} else {
			trace_util_0.Count(_session_00000, 95)
			{
				transactionRollbackCounterGeneral.Inc()
			}
		}
	}
	trace_util_0.Count(_session_00000, 91)
	s.cleanRetryInfo()
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (s *session) GetClient() kv.Client {
	trace_util_0.Count(_session_00000, 96)
	return s.store.GetClient()
}

func (s *session) String() string {
	trace_util_0.Count(_session_00000, 97)
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	if s.txn.Valid() {
		trace_util_0.Count(_session_00000, 102)
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	trace_util_0.Count(_session_00000, 98)
	if sessVars.SnapshotTS != 0 {
		trace_util_0.Count(_session_00000, 103)
		data["snapshotTS"] = sessVars.SnapshotTS
	}
	trace_util_0.Count(_session_00000, 99)
	if sessVars.StmtCtx.LastInsertID > 0 {
		trace_util_0.Count(_session_00000, 104)
		data["lastInsertID"] = sessVars.StmtCtx.LastInsertID
	}
	trace_util_0.Count(_session_00000, 100)
	if len(sessVars.PreparedStmts) > 0 {
		trace_util_0.Count(_session_00000, 105)
		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
	}
	trace_util_0.Count(_session_00000, 101)
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

const sqlLogMaxLen = 1024

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry bool

func (s *session) getSQLLabel() string {
	trace_util_0.Count(_session_00000, 106)
	if s.sessionVars.InRestrictedSQL {
		trace_util_0.Count(_session_00000, 108)
		return metrics.LblInternal
	}
	trace_util_0.Count(_session_00000, 107)
	return metrics.LblGeneral
}

func (s *session) isInternal() bool {
	trace_util_0.Count(_session_00000, 109)
	return s.sessionVars.InRestrictedSQL
}

func (s *session) isTxnRetryableError(err error) bool {
	trace_util_0.Count(_session_00000, 110)
	if SchemaChangedWithoutRetry {
		trace_util_0.Count(_session_00000, 112)
		return kv.IsTxnRetryableError(err)
	}
	trace_util_0.Count(_session_00000, 111)
	return kv.IsTxnRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	trace_util_0.Count(_session_00000, 113)
	if s.txn.doNotCommit == nil {
		trace_util_0.Count(_session_00000, 117)
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	trace_util_0.Count(_session_00000, 114)
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
		trace_util_0.Count(_session_00000, 118)
		return nil
	}
	trace_util_0.Count(_session_00000, 115)
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.RollbackStmt); ok {
		trace_util_0.Count(_session_00000, 119)
		return nil
	}
	trace_util_0.Count(_session_00000, 116)
	return errors.New("current transaction is aborted, commands ignored until end of transaction block:" + s.txn.doNotCommit.Error())
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	trace_util_0.Count(_session_00000, 120)
	var retryCnt uint
	defer func() {
		trace_util_0.Count(_session_00000, 124)
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		metrics.SessionRetry.Observe(float64(retryCnt + 1))
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
		if err != nil {
			trace_util_0.Count(_session_00000, 126)
			s.RollbackTxn(ctx)
		}
		trace_util_0.Count(_session_00000, 125)
		s.txn.changeToInvalid()
	}()

	trace_util_0.Count(_session_00000, 121)
	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if s.sessionVars.TxnCtx.ForUpdate {
		trace_util_0.Count(_session_00000, 127)
		err = errForUpdateCantRetry.GenWithStackByArgs(connID)
		return err
	}

	trace_util_0.Count(_session_00000, 122)
	nh := GetHistory(s)
	var schemaVersion int64
	sessVars := s.GetSessionVars()
	orgStartTS := sessVars.TxnCtx.StartTS
	label := s.getSQLLabel()
	for {
		trace_util_0.Count(_session_00000, 128)
		s.PrepareTxnCtx(ctx)
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			trace_util_0.Count(_session_00000, 134)
			st := sr.st
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PreparedParams = s.sessionVars.PreparedParams[:0]
			schemaVersion, err = st.RebuildPlan()
			if err != nil {
				trace_util_0.Count(_session_00000, 138)
				return err
			}

			trace_util_0.Count(_session_00000, 135)
			if retryCnt == 0 {
				trace_util_0.Count(_session_00000, 139)
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sqlForLog(st.OriginText())+sessVars.GetExecuteArgumentsInfo()))
			} else {
				trace_util_0.Count(_session_00000, 140)
				{
					logutil.Logger(ctx).Warn("retrying",
						zap.Int64("schemaVersion", schemaVersion),
						zap.Uint("retryCnt", retryCnt),
						zap.Int("queryNum", i))
				}
			}
			trace_util_0.Count(_session_00000, 136)
			_, err = st.Exec(ctx)
			if err != nil {
				trace_util_0.Count(_session_00000, 141)
				s.StmtRollback()
				break
			}
			trace_util_0.Count(_session_00000, 137)
			err = s.StmtCommit()
			if err != nil {
				trace_util_0.Count(_session_00000, 142)
				return err
			}
		}
		trace_util_0.Count(_session_00000, 129)
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		if hook := ctx.Value("preCommitHook"); hook != nil {
			trace_util_0.Count(_session_00000, 143)
			// For testing purpose.
			hook.(func())()
		}
		trace_util_0.Count(_session_00000, 130)
		if err == nil {
			trace_util_0.Count(_session_00000, 144)
			err = s.doCommit(ctx)
			if err == nil {
				trace_util_0.Count(_session_00000, 145)
				break
			}
		}
		trace_util_0.Count(_session_00000, 131)
		if !s.isTxnRetryableError(err) {
			trace_util_0.Count(_session_00000, 146)
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable)
			return err
		}
		trace_util_0.Count(_session_00000, 132)
		retryCnt++
		if retryCnt >= maxCnt {
			trace_util_0.Count(_session_00000, 147)
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax)
			return err
		}
		trace_util_0.Count(_session_00000, 133)
		logutil.Logger(ctx).Warn("sql",
			zap.String("label", label),
			zap.Error(err),
			zap.String("txn", s.txn.GoString()))
		kv.BackOff(retryCnt)
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}
	trace_util_0.Count(_session_00000, 123)
	return err
}

func sqlForLog(sql string) string {
	trace_util_0.Count(_session_00000, 148)
	if len(sql) > sqlLogMaxLen {
		trace_util_0.Count(_session_00000, 150)
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	trace_util_0.Count(_session_00000, 149)
	return executor.QueryReplacer.Replace(sql)
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

func (s *session) sysSessionPool() sessionPool {
	trace_util_0.Count(_session_00000, 151)
	return domain.GetDomain(s).SysSessionPool()
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	trace_util_0.Count(_session_00000, 152)
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		trace_util_0.Count(_session_00000, 154)
		return nil, nil, err
	}
	trace_util_0.Count(_session_00000, 153)
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)
	metrics.SessionRestrictedSQLCounter.Inc()

	return execRestrictedSQL(ctx, se, sql)
}

// ExecRestrictedSQLWithSnapshot implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements with snapshot.
// If current session sets the snapshot timestamp, then execute with this snapshot timestamp.
// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
func (s *session) ExecRestrictedSQLWithSnapshot(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	trace_util_0.Count(_session_00000, 155)
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		trace_util_0.Count(_session_00000, 161)
		return nil, nil, err
	}
	trace_util_0.Count(_session_00000, 156)
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)
	metrics.SessionRestrictedSQLCounter.Inc()
	var snapshot uint64
	txn, err := s.Txn(false)
	if err != nil {
		trace_util_0.Count(_session_00000, 162)
		return nil, nil, err
	}
	trace_util_0.Count(_session_00000, 157)
	if txn.Valid() {
		trace_util_0.Count(_session_00000, 163)
		snapshot = s.txn.StartTS()
	}
	trace_util_0.Count(_session_00000, 158)
	if s.sessionVars.SnapshotTS != 0 {
		trace_util_0.Count(_session_00000, 164)
		snapshot = s.sessionVars.SnapshotTS
	}
	// Set snapshot.
	trace_util_0.Count(_session_00000, 159)
	if snapshot != 0 {
		trace_util_0.Count(_session_00000, 165)
		if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, strconv.FormatUint(snapshot, 10)); err != nil {
			trace_util_0.Count(_session_00000, 167)
			return nil, nil, err
		}
		trace_util_0.Count(_session_00000, 166)
		defer func() {
			trace_util_0.Count(_session_00000, 168)
			if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, ""); err != nil {
				trace_util_0.Count(_session_00000, 169)
				logutil.Logger(context.Background()).Error("set tidbSnapshot error", zap.Error(err))
			}
		}()
	}
	trace_util_0.Count(_session_00000, 160)
	return execRestrictedSQL(ctx, se, sql)
}

func execRestrictedSQL(ctx context.Context, se *session, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	trace_util_0.Count(_session_00000, 170)
	startTime := time.Now()
	recordSets, err := se.Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_session_00000, 173)
		return nil, nil, err
	}

	trace_util_0.Count(_session_00000, 171)
	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		trace_util_0.Count(_session_00000, 174)
		tmp, err := drainRecordSet(ctx, se, rs)
		if err != nil {
			trace_util_0.Count(_session_00000, 177)
			return nil, nil, err
		}
		trace_util_0.Count(_session_00000, 175)
		if err = rs.Close(); err != nil {
			trace_util_0.Count(_session_00000, 178)
			return nil, nil, err
		}

		trace_util_0.Count(_session_00000, 176)
		if i == 0 {
			trace_util_0.Count(_session_00000, 179)
			rows = tmp
			fields = rs.Fields()
		}
	}
	trace_util_0.Count(_session_00000, 172)
	metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal).Observe(time.Since(startTime).Seconds())
	return rows, fields, nil
}

func createSessionFunc(store kv.Storage) pools.Factory {
	trace_util_0.Count(_session_00000, 180)
	return func() (pools.Resource, error) {
		trace_util_0.Count(_session_00000, 181)
		se, err := createSession(store)
		if err != nil {
			trace_util_0.Count(_session_00000, 184)
			return nil, err
		}
		trace_util_0.Count(_session_00000, 182)
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			trace_util_0.Count(_session_00000, 185)
			return nil, err
		}
		trace_util_0.Count(_session_00000, 183)
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func createSessionWithDomainFunc(store kv.Storage) func(*domain.Domain) (pools.Resource, error) {
	trace_util_0.Count(_session_00000, 186)
	return func(dom *domain.Domain) (pools.Resource, error) {
		trace_util_0.Count(_session_00000, 187)
		se, err := createSessionWithDomain(store, dom)
		if err != nil {
			trace_util_0.Count(_session_00000, 190)
			return nil, err
		}
		trace_util_0.Count(_session_00000, 188)
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			trace_util_0.Count(_session_00000, 191)
			return nil, err
		}
		trace_util_0.Count(_session_00000, 189)
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	trace_util_0.Count(_session_00000, 192)
	var rows []chunk.Row
	req := rs.NewRecordBatch()
	for {
		trace_util_0.Count(_session_00000, 193)
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			trace_util_0.Count(_session_00000, 196)
			return rows, err
		}
		trace_util_0.Count(_session_00000, 194)
		iter := chunk.NewIterator4Chunk(req.Chunk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			trace_util_0.Count(_session_00000, 197)
			rows = append(rows, r)
		}
		trace_util_0.Count(_session_00000, 195)
		req.Chunk = chunk.Renew(req.Chunk, se.sessionVars.MaxChunkSize)
	}
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	trace_util_0.Count(_session_00000, 198)
	rows, fields, err := s.ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_session_00000, 202)
		return "", err
	}
	trace_util_0.Count(_session_00000, 199)
	if len(rows) == 0 {
		trace_util_0.Count(_session_00000, 203)
		return "", executor.ErrResultIsEmpty
	}
	trace_util_0.Count(_session_00000, 200)
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		trace_util_0.Count(_session_00000, 204)
		return "", err
	}
	trace_util_0.Count(_session_00000, 201)
	return value, nil
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (s *session) GetAllSysVars() (map[string]string, error) {
	trace_util_0.Count(_session_00000, 205)
	if s.Value(sessionctx.Initing) != nil {
		trace_util_0.Count(_session_00000, 209)
		return nil, nil
	}
	trace_util_0.Count(_session_00000, 206)
	sql := `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM %s.%s;`
	sql = fmt.Sprintf(sql, mysql.SystemDB, mysql.GlobalVariablesTable)
	rows, _, err := s.ExecRestrictedSQL(s, sql)
	if err != nil {
		trace_util_0.Count(_session_00000, 210)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 207)
	ret := make(map[string]string)
	for _, r := range rows {
		trace_util_0.Count(_session_00000, 211)
		k, v := r.GetString(0), r.GetString(1)
		ret[k] = v
	}
	trace_util_0.Count(_session_00000, 208)
	return ret, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	trace_util_0.Count(_session_00000, 212)
	if s.Value(sessionctx.Initing) != nil {
		trace_util_0.Count(_session_00000, 215)
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	trace_util_0.Count(_session_00000, 213)
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		trace_util_0.Count(_session_00000, 216)
		if executor.ErrResultIsEmpty.Equal(err) {
			trace_util_0.Count(_session_00000, 218)
			if sv, ok := variable.SysVars[name]; ok {
				trace_util_0.Count(_session_00000, 220)
				return sv.Value, nil
			}
			trace_util_0.Count(_session_00000, 219)
			return "", variable.UnknownSystemVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_session_00000, 217)
		return "", err
	}
	trace_util_0.Count(_session_00000, 214)
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name, value string) error {
	trace_util_0.Count(_session_00000, 221)
	if name == variable.SQLModeVar {
		trace_util_0.Count(_session_00000, 224)
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			trace_util_0.Count(_session_00000, 225)
			return err
		}
	}
	trace_util_0.Count(_session_00000, 222)
	var sVal string
	var err error
	sVal, err = variable.ValidateSetSystemVar(s.sessionVars, name, value)
	if err != nil {
		trace_util_0.Count(_session_00000, 226)
		return err
	}
	trace_util_0.Count(_session_00000, 223)
	name = strings.ToLower(name)
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name, sVal)
	_, _, err = s.ExecRestrictedSQL(s, sql)
	return err
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, []error, error) {
	trace_util_0.Count(_session_00000, 227)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_session_00000, 229)
		span1 := span.Tracer().StartSpan("session.ParseSQL", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_session_00000, 228)
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	s.parser.EnableWindowFunc(s.sessionVars.EnableWindowFunction)
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) SetProcessInfo(sql string, t time.Time, command byte) {
	trace_util_0.Count(_session_00000, 230)
	pi := util.ProcessInfo{
		ID:            s.sessionVars.ConnectionID,
		DB:            s.sessionVars.CurrentDB,
		Command:       command,
		Plan:          s.currentPlan,
		Time:          t,
		State:         s.Status(),
		Info:          sql,
		CurTxnStartTS: s.sessionVars.TxnCtx.StartTS,
		StmtCtx:       s.sessionVars.StmtCtx,
		StatsInfo:     plannercore.GetStatsInfo,
	}
	if s.sessionVars.User != nil {
		trace_util_0.Count(_session_00000, 232)
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	trace_util_0.Count(_session_00000, 231)
	s.processInfo.Store(&pi)
}

func (s *session) executeStatement(ctx context.Context, connID uint64, stmtNode ast.StmtNode, stmt sqlexec.Statement, recordSets []sqlexec.RecordSet) ([]sqlexec.RecordSet, error) {
	trace_util_0.Count(_session_00000, 233)
	s.SetValue(sessionctx.QueryString, stmt.OriginText())
	if _, ok := stmtNode.(ast.DDLNode); ok {
		trace_util_0.Count(_session_00000, 238)
		s.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		trace_util_0.Count(_session_00000, 239)
		{
			s.ClearValue(sessionctx.LastExecuteDDL)
		}
	}
	trace_util_0.Count(_session_00000, 234)
	logStmt(stmtNode, s.sessionVars)
	startTime := time.Now()
	recordSet, err := runStmt(ctx, s, stmt)
	if err != nil {
		trace_util_0.Count(_session_00000, 240)
		if !kv.ErrKeyExists.Equal(err) {
			trace_util_0.Count(_session_00000, 242)
			logutil.Logger(ctx).Warn("run statement error",
				zap.Int64("schemaVersion", s.sessionVars.TxnCtx.SchemaVersion),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		trace_util_0.Count(_session_00000, 241)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 235)
	if s.isInternal() {
		trace_util_0.Count(_session_00000, 243)
		sessionExecuteRunDurationInternal.Observe(time.Since(startTime).Seconds())
	} else {
		trace_util_0.Count(_session_00000, 244)
		{
			sessionExecuteRunDurationGeneral.Observe(time.Since(startTime).Seconds())
		}
	}

	trace_util_0.Count(_session_00000, 236)
	if recordSet != nil {
		trace_util_0.Count(_session_00000, 245)
		recordSets = append(recordSets, recordSet)
	}
	trace_util_0.Count(_session_00000, 237)
	return recordSets, nil
}

func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	trace_util_0.Count(_session_00000, 246)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_session_00000, 249)
		span1 := span.Tracer().StartSpan("session.Execute", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_session_00000, 247)
	if recordSets, err = s.execute(ctx, sql); err != nil {
		trace_util_0.Count(_session_00000, 250)
		s.sessionVars.StmtCtx.AppendError(err)
	}
	trace_util_0.Count(_session_00000, 248)
	return
}

func (s *session) execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	trace_util_0.Count(_session_00000, 251)
	s.PrepareTxnCtx(ctx)
	connID := s.sessionVars.ConnectionID
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		trace_util_0.Count(_session_00000, 258)
		return nil, err
	}

	trace_util_0.Count(_session_00000, 252)
	charsetInfo, collation := s.sessionVars.GetCharsetInfo()

	// Step1: Compile query string to abstract syntax trees(ASTs).
	startTS := time.Now()
	stmtNodes, warns, err := s.ParseSQL(ctx, sql, charsetInfo, collation)
	if err != nil {
		trace_util_0.Count(_session_00000, 259)
		s.rollbackOnError(ctx)
		logutil.Logger(ctx).Warn("parse sql error",
			zap.Error(err),
			zap.String("sql", sql))
		return nil, util.SyntaxError(err)
	}
	trace_util_0.Count(_session_00000, 253)
	isInternal := s.isInternal()
	if isInternal {
		trace_util_0.Count(_session_00000, 260)
		sessionExecuteParseDurationInternal.Observe(time.Since(startTS).Seconds())
	} else {
		trace_util_0.Count(_session_00000, 261)
		{
			sessionExecuteParseDurationGeneral.Observe(time.Since(startTS).Seconds())
		}
	}

	trace_util_0.Count(_session_00000, 254)
	var tempStmtNodes []ast.StmtNode
	compiler := executor.Compiler{Ctx: s}
	for idx, stmtNode := range stmtNodes {
		trace_util_0.Count(_session_00000, 262)
		s.PrepareTxnCtx(ctx)

		// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		startTS = time.Now()
		// Some executions are done in compile stage, so we reset them before compile.
		if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
			trace_util_0.Count(_session_00000, 266)
			return nil, err
		}
		trace_util_0.Count(_session_00000, 263)
		stmt, err := compiler.Compile(ctx, stmtNode)
		if err != nil {
			trace_util_0.Count(_session_00000, 267)
			if tempStmtNodes == nil {
				trace_util_0.Count(_session_00000, 270)
				tempStmtNodes, warns, err = s.ParseSQL(ctx, sql, charsetInfo, collation)
				if err != nil || warns != nil {
					trace_util_0.Count(_session_00000, 271)
					//just skip errcheck, because parse will not return an error.
				}
			}
			trace_util_0.Count(_session_00000, 268)
			stmtNode = tempStmtNodes[idx]
			stmt, err = compiler.SkipBindCompile(ctx, stmtNode)
			if err != nil {
				trace_util_0.Count(_session_00000, 272)
				s.rollbackOnError(ctx)
				logutil.Logger(ctx).Warn("compile sql error",
					zap.Error(err),
					zap.String("sql", sql))
				return nil, err
			}
			trace_util_0.Count(_session_00000, 269)
			s.handleInvalidBindRecord(ctx, stmtNode)
		}
		trace_util_0.Count(_session_00000, 264)
		if isInternal {
			trace_util_0.Count(_session_00000, 273)
			sessionExecuteCompileDurationInternal.Observe(time.Since(startTS).Seconds())
		} else {
			trace_util_0.Count(_session_00000, 274)
			{
				sessionExecuteCompileDurationGeneral.Observe(time.Since(startTS).Seconds())
			}
		}
		trace_util_0.Count(_session_00000, 265)
		s.currentPlan = stmt.Plan

		// Step3: Execute the physical plan.
		if recordSets, err = s.executeStatement(ctx, connID, stmtNode, stmt, recordSets); err != nil {
			trace_util_0.Count(_session_00000, 275)
			return nil, err
		}
	}

	trace_util_0.Count(_session_00000, 255)
	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(recordSets) > 1 {
		trace_util_0.Count(_session_00000, 276)
		// return the first recordset if client doesn't support ClientMultiResults.
		recordSets = recordSets[:1]
	}

	trace_util_0.Count(_session_00000, 256)
	for _, warn := range warns {
		trace_util_0.Count(_session_00000, 277)
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	trace_util_0.Count(_session_00000, 257)
	return recordSets, nil
}

func (s *session) handleInvalidBindRecord(ctx context.Context, stmtNode ast.StmtNode) {
	trace_util_0.Count(_session_00000, 278)
	var normdOrigSQL, hash string
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		trace_util_0.Count(_session_00000, 282)
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			trace_util_0.Count(_session_00000, 285)
			normalizeExplainSQL := parser.Normalize(x.Text())
			idx := strings.Index(normalizeExplainSQL, "select")
			normdOrigSQL = normalizeExplainSQL[idx:]
			hash = parser.DigestHash(normdOrigSQL)
		default:
			trace_util_0.Count(_session_00000, 286)
			return
		}
	case *ast.SelectStmt:
		trace_util_0.Count(_session_00000, 283)
		normdOrigSQL, hash = parser.NormalizeDigest(x.Text())
	default:
		trace_util_0.Count(_session_00000, 284)
		return
	}
	trace_util_0.Count(_session_00000, 279)
	sessionHandle := s.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindMeta := sessionHandle.GetBindRecord(normdOrigSQL, s.GetSessionVars().CurrentDB)
	if bindMeta != nil {
		trace_util_0.Count(_session_00000, 287)
		bindMeta.Status = bindinfo.Invalid
		return
	}

	trace_util_0.Count(_session_00000, 280)
	globalHandle := domain.GetDomain(s).BindHandle()
	bindMeta = globalHandle.GetBindRecord(hash, normdOrigSQL, s.GetSessionVars().CurrentDB)
	if bindMeta == nil {
		trace_util_0.Count(_session_00000, 288)
		bindMeta = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	trace_util_0.Count(_session_00000, 281)
	if bindMeta != nil {
		trace_util_0.Count(_session_00000, 289)
		record := &bindinfo.BindRecord{
			OriginalSQL: bindMeta.OriginalSQL,
			BindSQL:     bindMeta.BindSQL,
			Db:          s.GetSessionVars().CurrentDB,
			Charset:     bindMeta.Charset,
			Collation:   bindMeta.Collation,
			Status:      bindinfo.Invalid,
		}

		err := sessionHandle.AddBindRecord(record)
		if err != nil {
			trace_util_0.Count(_session_00000, 291)
			logutil.Logger(ctx).Warn("handleInvalidBindRecord failed", zap.Error(err))
		}

		trace_util_0.Count(_session_00000, 290)
		globalHandle := domain.GetDomain(s).BindHandle()
		dropBindRecord := &bindinfo.BindRecord{
			OriginalSQL: bindMeta.OriginalSQL,
			Db:          bindMeta.Db,
		}
		globalHandle.AddDropInvalidBindTask(dropBindRecord)
	}
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	trace_util_0.Count(_session_00000, 292)
	if !s.sessionVars.InTxn() {
		trace_util_0.Count(_session_00000, 293)
		s.RollbackTxn(ctx)
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	trace_util_0.Count(_session_00000, 294)
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		trace_util_0.Count(_session_00000, 299)
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = domain.GetDomain(s).InfoSchema()
	}
	trace_util_0.Count(_session_00000, 295)
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		trace_util_0.Count(_session_00000, 300)
		return
	}

	trace_util_0.Count(_session_00000, 296)
	ctx := context.Background()
	inTxn := s.GetSessionVars().InTxn()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	s.PrepareTxnCtx(ctx)
	prepareExec := executor.NewPrepareExec(s, executor.GetInfoSchema(s), sql)
	err = prepareExec.Next(ctx, nil)
	if err != nil {
		trace_util_0.Count(_session_00000, 301)
		return
	}
	trace_util_0.Count(_session_00000, 297)
	if !inTxn {
		trace_util_0.Count(_session_00000, 302)
		// We could start a transaction to build the prepare executor before, we should rollback it here.
		s.RollbackTxn(ctx)
	}
	trace_util_0.Count(_session_00000, 298)
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

// checkArgs makes sure all the arguments' types are known and can be handled.
// integer types are converted to int64 and uint64, time.Time is converted to types.Time.
// time.Duration is converted to types.Duration, other known types are leaved as it is.
func checkArgs(args ...interface{}) error {
	trace_util_0.Count(_session_00000, 303)
	for i, v := range args {
		trace_util_0.Count(_session_00000, 305)
		switch x := v.(type) {
		case bool:
			trace_util_0.Count(_session_00000, 306)
			if x {
				trace_util_0.Count(_session_00000, 325)
				args[i] = int64(1)
			} else {
				trace_util_0.Count(_session_00000, 326)
				{
					args[i] = int64(0)
				}
			}
		case int8:
			trace_util_0.Count(_session_00000, 307)
			args[i] = int64(x)
		case int16:
			trace_util_0.Count(_session_00000, 308)
			args[i] = int64(x)
		case int32:
			trace_util_0.Count(_session_00000, 309)
			args[i] = int64(x)
		case int:
			trace_util_0.Count(_session_00000, 310)
			args[i] = int64(x)
		case uint8:
			trace_util_0.Count(_session_00000, 311)
			args[i] = uint64(x)
		case uint16:
			trace_util_0.Count(_session_00000, 312)
			args[i] = uint64(x)
		case uint32:
			trace_util_0.Count(_session_00000, 313)
			args[i] = uint64(x)
		case uint:
			trace_util_0.Count(_session_00000, 314)
			args[i] = uint64(x)
		case int64:
			trace_util_0.Count(_session_00000, 315)
		case uint64:
			trace_util_0.Count(_session_00000, 316)
		case float32:
			trace_util_0.Count(_session_00000, 317)
		case float64:
			trace_util_0.Count(_session_00000, 318)
		case string:
			trace_util_0.Count(_session_00000, 319)
		case []byte:
			trace_util_0.Count(_session_00000, 320)
		case time.Duration:
			trace_util_0.Count(_session_00000, 321)
			args[i] = types.Duration{Duration: x}
		case time.Time:
			trace_util_0.Count(_session_00000, 322)
			args[i] = types.Time{Time: types.FromGoTime(x), Type: mysql.TypeDatetime}
		case nil:
			trace_util_0.Count(_session_00000, 323)
		default:
			trace_util_0.Count(_session_00000, 324)
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	trace_util_0.Count(_session_00000, 304)
	return nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, args ...interface{}) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_session_00000, 327)
	err := checkArgs(args...)
	if err != nil {
		trace_util_0.Count(_session_00000, 330)
		return nil, err
	}

	trace_util_0.Count(_session_00000, 328)
	s.PrepareTxnCtx(ctx)
	st, err := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	if err != nil {
		trace_util_0.Count(_session_00000, 331)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 329)
	logQuery(st.OriginText(), s.sessionVars)
	r, err := runStmt(ctx, s, st)
	return r, err
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	trace_util_0.Count(_session_00000, 332)
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		trace_util_0.Count(_session_00000, 334)
		return plannercore.ErrStmtNotFound
	}
	trace_util_0.Count(_session_00000, 333)
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	trace_util_0.Count(_session_00000, 335)
	if s.txn.pending() && active {
		trace_util_0.Count(_session_00000, 337)
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		txnCap := s.getMembufCap()
		if err := s.txn.changePendingToValid(txnCap); err != nil {
			trace_util_0.Count(_session_00000, 340)
			logutil.Logger(context.Background()).Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, err
		}
		trace_util_0.Count(_session_00000, 338)
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if s.sessionVars.TxnCtx.IsPessimistic {
			trace_util_0.Count(_session_00000, 341)
			s.txn.SetOption(kv.Pessimistic, true)
		}
		trace_util_0.Count(_session_00000, 339)
		if !s.sessionVars.IsAutocommit() {
			trace_util_0.Count(_session_00000, 342)
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
	}
	trace_util_0.Count(_session_00000, 336)
	return &s.txn, nil
}

func (s *session) NewTxn(ctx context.Context) error {
	trace_util_0.Count(_session_00000, 343)
	if s.txn.Valid() {
		trace_util_0.Count(_session_00000, 346)
		txnID := s.txn.StartTS()
		err := s.CommitTxn(ctx)
		if err != nil {
			trace_util_0.Count(_session_00000, 348)
			return err
		}
		trace_util_0.Count(_session_00000, 347)
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("NewTxn() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID))
	}

	trace_util_0.Count(_session_00000, 344)
	txn, err := s.store.Begin()
	if err != nil {
		trace_util_0.Count(_session_00000, 349)
		return err
	}
	trace_util_0.Count(_session_00000, 345)
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	trace_util_0.Count(_session_00000, 350)
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) interface{} {
	trace_util_0.Count(_session_00000, 351)
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	trace_util_0.Count(_session_00000, 352)
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

// Close function does some clean work when session end.
func (s *session) Close() {
	trace_util_0.Count(_session_00000, 353)
	if s.statsCollector != nil {
		trace_util_0.Count(_session_00000, 355)
		s.statsCollector.Delete()
	}
	trace_util_0.Count(_session_00000, 354)
	ctx := context.TODO()
	s.RollbackTxn(ctx)
	if s.sessionVars != nil {
		trace_util_0.Count(_session_00000, 356)
		s.sessionVars.WithdrawAllPreparedStmt()
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	trace_util_0.Count(_session_00000, 357)
	return s.sessionVars
}

func (s *session) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
	trace_util_0.Count(_session_00000, 358)
	pm := privilege.GetPrivilegeManager(s)

	// Check IP or localhost.
	var success bool
	user.AuthUsername, user.AuthHostname, success = pm.ConnectionVerification(user.Username, user.Hostname, authentication, salt)
	if success {
		trace_util_0.Count(_session_00000, 361)
		s.sessionVars.User = user
		s.sessionVars.ActiveRoles = pm.GetDefaultRoles(user.AuthUsername, user.AuthHostname)
		return true
	} else {
		trace_util_0.Count(_session_00000, 362)
		if user.Hostname == variable.DefHostname {
			trace_util_0.Count(_session_00000, 363)
			logutil.Logger(context.Background()).Error("user connection verification failed",
				zap.Stringer("user", user))
			return false
		}
	}

	// Check Hostname.
	trace_util_0.Count(_session_00000, 359)
	for _, addr := range getHostByIP(user.Hostname) {
		trace_util_0.Count(_session_00000, 364)
		u, h, success := pm.ConnectionVerification(user.Username, addr, authentication, salt)
		if success {
			trace_util_0.Count(_session_00000, 365)
			s.sessionVars.User = &auth.UserIdentity{
				Username:     user.Username,
				Hostname:     addr,
				AuthUsername: u,
				AuthHostname: h,
			}
			s.sessionVars.ActiveRoles = pm.GetDefaultRoles(u, h)
			return true
		}
	}

	trace_util_0.Count(_session_00000, 360)
	logutil.Logger(context.Background()).Error("user connection verification failed",
		zap.Stringer("user", user))
	return false
}

func getHostByIP(ip string) []string {
	trace_util_0.Count(_session_00000, 366)
	if ip == "127.0.0.1" {
		trace_util_0.Count(_session_00000, 368)
		return []string{variable.DefHostname}
	}
	trace_util_0.Count(_session_00000, 367)
	addrs, err := net.LookupAddr(ip)
	terror.Log(errors.Trace(err))
	return addrs
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (Session, error) {
	trace_util_0.Count(_session_00000, 369)
	s, err := CreateSession(store)
	if err == nil {
		trace_util_0.Count(_session_00000, 371)
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
	}
	trace_util_0.Count(_session_00000, 370)
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	trace_util_0.Count(_session_00000, 372)
	s, err := createSession(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 376)
		return nil, err
	}

	// Add auth here.
	trace_util_0.Count(_session_00000, 373)
	do, err := domap.Get(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 377)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 374)
	pm := &privileges.UserPrivileges{
		Handle: do.PrivilegeHandle(),
	}
	privilege.BindPrivilegeManager(s, pm)

	sessionBindHandle := bindinfo.NewSessionBindHandle(s.parser)
	s.SetValue(bindinfo.SessionBindInfoKeyType, sessionBindHandle)
	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	if do.StatsHandle() != nil && do.StatsUpdating() {
		trace_util_0.Count(_session_00000, 378)
		s.statsCollector = do.StatsHandle().NewSessionStatsCollector()
	}

	trace_util_0.Count(_session_00000, 375)
	return s, nil
}

// loadSystemTZ loads systemTZ from mysql.tidb
func loadSystemTZ(se *session) (string, error) {
	trace_util_0.Count(_session_00000, 379)
	sql := `select variable_value from mysql.tidb where variable_name = 'system_tz'`
	rss, errLoad := se.Execute(context.Background(), sql)
	if errLoad != nil {
		trace_util_0.Count(_session_00000, 383)
		return "", errLoad
	}
	// the record of mysql.tidb under where condition: variable_name = "system_tz" should shall only be one.
	trace_util_0.Count(_session_00000, 380)
	defer func() {
		trace_util_0.Count(_session_00000, 384)
		if err := rss[0].Close(); err != nil {
			trace_util_0.Count(_session_00000, 385)
			logutil.Logger(context.Background()).Error("close result set error", zap.Error(err))
		}
	}()
	trace_util_0.Count(_session_00000, 381)
	req := rss[0].NewRecordBatch()
	if err := rss[0].Next(context.Background(), req); err != nil {
		trace_util_0.Count(_session_00000, 386)
		return "", err
	}
	trace_util_0.Count(_session_00000, 382)
	return req.GetRow(0).GetString(0), nil
}

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	trace_util_0.Count(_session_00000, 387)
	cfg := config.GetGlobalConfig()
	if len(cfg.Plugin.Load) > 0 {
		trace_util_0.Count(_session_00000, 400)
		err := plugin.Load(context.Background(), plugin.Config{
			Plugins:        strings.Split(cfg.Plugin.Load, ","),
			PluginDir:      cfg.Plugin.Dir,
			GlobalSysVar:   &variable.SysVars,
			PluginVarNames: &variable.PluginVarNames,
		})
		if err != nil {
			trace_util_0.Count(_session_00000, 401)
			return nil, err
		}
	}

	trace_util_0.Count(_session_00000, 388)
	initLoadCommonGlobalVarsSQL()

	ver := getStoreBootstrapVersion(store)
	if ver == notBootstrapped {
		trace_util_0.Count(_session_00000, 402)
		runInBootstrapSession(store, bootstrap)
	} else {
		trace_util_0.Count(_session_00000, 403)
		if ver < currentBootstrapVersion {
			trace_util_0.Count(_session_00000, 404)
			runInBootstrapSession(store, upgrade)
		}
	}

	trace_util_0.Count(_session_00000, 389)
	se, err := createSession(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 405)
		return nil, err
	}
	// get system tz from mysql.tidb
	trace_util_0.Count(_session_00000, 390)
	tz, err := loadSystemTZ(se)
	if err != nil {
		trace_util_0.Count(_session_00000, 406)
		return nil, err
	}

	trace_util_0.Count(_session_00000, 391)
	timeutil.SetSystemTZ(tz)
	dom := domain.GetDomain(se)
	dom.InitExpensiveQueryHandle()

	if !config.GetGlobalConfig().Security.SkipGrantTable {
		trace_util_0.Count(_session_00000, 407)
		err = dom.LoadPrivilegeLoop(se)
		if err != nil {
			trace_util_0.Count(_session_00000, 408)
			return nil, err
		}
	}

	trace_util_0.Count(_session_00000, 392)
	if len(cfg.Plugin.Load) > 0 {
		trace_util_0.Count(_session_00000, 409)
		err := plugin.Init(context.Background(), plugin.Config{EtcdClient: dom.GetEtcdClient()})
		if err != nil {
			trace_util_0.Count(_session_00000, 410)
			return nil, err
		}
	}

	trace_util_0.Count(_session_00000, 393)
	err = executor.LoadExprPushdownBlacklist(se)
	if err != nil {
		trace_util_0.Count(_session_00000, 411)
		return nil, err
	}

	trace_util_0.Count(_session_00000, 394)
	se1, err := createSession(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 412)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 395)
	err = dom.UpdateTableStatsLoop(se1)
	if err != nil {
		trace_util_0.Count(_session_00000, 413)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 396)
	se2, err := createSession(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 414)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 397)
	err = dom.LoadBindInfoLoop(se2)
	if err != nil {
		trace_util_0.Count(_session_00000, 415)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 398)
	if raw, ok := store.(tikv.EtcdBackend); ok {
		trace_util_0.Count(_session_00000, 416)
		err = raw.StartGCWorker()
		if err != nil {
			trace_util_0.Count(_session_00000, 417)
			return nil, err
		}
	}

	trace_util_0.Count(_session_00000, 399)
	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	trace_util_0.Count(_session_00000, 418)
	return domap.Get(store)
}

// runInBootstrapSession create a special session for boostrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	trace_util_0.Count(_session_00000, 419)
	s, err := createSession(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 421)
		// Bootstrap fail will cause program exit.
		logutil.Logger(context.Background()).Fatal("createSession error", zap.Error(err))
	}

	trace_util_0.Count(_session_00000, 420)
	s.SetValue(sessionctx.Initing, true)
	bootstrap(s)
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom := domain.GetDomain(s)
	dom.Close()
	domap.Delete(store)
}

func createSession(store kv.Storage) (*session, error) {
	trace_util_0.Count(_session_00000, 422)
	dom, err := domap.Get(store)
	if err != nil {
		trace_util_0.Count(_session_00000, 425)
		return nil, err
	}
	trace_util_0.Count(_session_00000, 423)
	s := &session{
		store:           store,
		parser:          parser.New(),
		sessionVars:     variable.NewSessionVars(),
		ddlOwnerChecker: dom.DDL().OwnerManager(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		trace_util_0.Count(_session_00000, 426)
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity,
			plannercore.PreparedPlanCacheMemoryGuardRatio, plannercore.PreparedPlanCacheMaxMemory.Load())
	}
	trace_util_0.Count(_session_00000, 424)
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpsClient()
	s.txn.init()
	return s, nil
}

// createSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSesion directly.
func createSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	trace_util_0.Count(_session_00000, 427)
	s := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	if plannercore.PreparedPlanCacheEnabled() {
		trace_util_0.Count(_session_00000, 429)
		s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity,
			plannercore.PreparedPlanCacheMemoryGuardRatio, plannercore.PreparedPlanCacheMaxMemory.Load())
	}
	trace_util_0.Count(_session_00000, 428)
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 33
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
	trace_util_0.Count(_session_00000, 430)
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		trace_util_0.Count(_session_00000, 435)
		return currentBootstrapVersion
	}

	trace_util_0.Count(_session_00000, 431)
	var ver int64
	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		trace_util_0.Count(_session_00000, 436)
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})

	trace_util_0.Count(_session_00000, 432)
	if err != nil {
		trace_util_0.Count(_session_00000, 437)
		logutil.Logger(context.Background()).Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	trace_util_0.Count(_session_00000, 433)
	if ver > notBootstrapped {
		trace_util_0.Count(_session_00000, 438)
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	trace_util_0.Count(_session_00000, 434)
	return ver
}

func finishBootstrap(store kv.Storage) {
	trace_util_0.Count(_session_00000, 439)
	storeBootstrappedLock.Lock()
	storeBootstrapped[store.UUID()] = true
	storeBootstrappedLock.Unlock()

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		trace_util_0.Count(_session_00000, 441)
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	trace_util_0.Count(_session_00000, 440)
	if err != nil {
		trace_util_0.Count(_session_00000, 442)
		logutil.Logger(context.Background()).Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

var builtinGlobalVariable = []string{
	variable.AutoCommit,
	variable.SQLModeVar,
	variable.MaxAllowedPacket,
	variable.TimeZone,
	variable.BlockEncryptionMode,
	variable.WaitTimeout,
	variable.InteractiveTimeout,
	variable.MaxPreparedStmtCount,
	/* TiDB specific global variables: */
	variable.TiDBSkipUTF8Check,
	variable.TiDBIndexJoinBatchSize,
	variable.TiDBIndexLookupSize,
	variable.TiDBIndexLookupConcurrency,
	variable.TiDBIndexLookupJoinConcurrency,
	variable.TiDBIndexSerialScanConcurrency,
	variable.TiDBHashJoinConcurrency,
	variable.TiDBProjectionConcurrency,
	variable.TiDBHashAggPartialConcurrency,
	variable.TiDBHashAggFinalConcurrency,
	variable.TiDBBackoffLockFast,
	variable.TiDBBackOffWeight,
	variable.TiDBConstraintCheckInPlace,
	variable.TiDBDDLReorgWorkerCount,
	variable.TiDBDDLReorgBatchSize,
	variable.TiDBDDLErrorCountLimit,
	variable.TiDBOptInSubqToJoinAndAgg,
	variable.TiDBOptCorrelationThreshold,
	variable.TiDBOptCorrelationExpFactor,
	variable.TiDBDistSQLScanConcurrency,
	variable.TiDBInitChunkSize,
	variable.TiDBMaxChunkSize,
	variable.TiDBEnableCascadesPlanner,
	variable.TiDBRetryLimit,
	variable.TiDBDisableTxnAutoRetry,
	variable.TiDBEnableWindowFunction,
	variable.TiDBEnableFastAnalyze,
	variable.TiDBExpensiveQueryTimeThreshold,
}

var (
	loadCommonGlobalVarsSQLOnce sync.Once
	loadCommonGlobalVarsSQL     string
)

func initLoadCommonGlobalVarsSQL() {
	trace_util_0.Count(_session_00000, 443)
	loadCommonGlobalVarsSQLOnce.Do(func() {
		trace_util_0.Count(_session_00000, 444)
		vars := append(make([]string, 0, len(builtinGlobalVariable)+len(variable.PluginVarNames)), builtinGlobalVariable...)
		if len(variable.PluginVarNames) > 0 {
			trace_util_0.Count(_session_00000, 446)
			vars = append(vars, variable.PluginVarNames...)
		}
		trace_util_0.Count(_session_00000, 445)
		loadCommonGlobalVarsSQL = "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" + strings.Join(vars, quoteCommaQuote) + "')"
	})
}

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	trace_util_0.Count(_session_00000, 447)
	initLoadCommonGlobalVarsSQL()
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		trace_util_0.Count(_session_00000, 453)
		return nil
	}
	trace_util_0.Count(_session_00000, 448)
	if s.Value(sessionctx.Initing) != nil {
		trace_util_0.Count(_session_00000, 454)
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}

	trace_util_0.Count(_session_00000, 449)
	var err error
	// Use GlobalVariableCache if TiDB just loaded global variables within 2 second ago.
	// When a lot of connections connect to TiDB simultaneously, it can protect TiKV meta region from overload.
	gvc := domain.GetDomain(s).GetGlobalVarsCache()
	succ, rows, fields := gvc.Get()
	if !succ {
		trace_util_0.Count(_session_00000, 455)
		// Set the variable to true to prevent cyclic recursive call.
		vars.CommonGlobalLoaded = true
		rows, fields, err = s.ExecRestrictedSQL(s, loadCommonGlobalVarsSQL)
		if err != nil {
			trace_util_0.Count(_session_00000, 457)
			vars.CommonGlobalLoaded = false
			logutil.Logger(context.Background()).Error("failed to load common global variables.")
			return err
		}
		trace_util_0.Count(_session_00000, 456)
		gvc.Update(rows, fields)
	}

	trace_util_0.Count(_session_00000, 450)
	for _, row := range rows {
		trace_util_0.Count(_session_00000, 458)
		varName := row.GetString(0)
		varVal := row.GetDatum(1, &fields[1].Column.FieldType)
		if _, ok := vars.GetSystemVar(varName); !ok {
			trace_util_0.Count(_session_00000, 459)
			err = variable.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				trace_util_0.Count(_session_00000, 460)
				return err
			}
		}
	}

	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	trace_util_0.Count(_session_00000, 451)
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		trace_util_0.Count(_session_00000, 461)
		if varVal, ok := vars.GetSystemVar(variable.InteractiveTimeout); ok {
			trace_util_0.Count(_session_00000, 462)
			if err := vars.SetSystemVar(variable.WaitTimeout, varVal); err != nil {
				trace_util_0.Count(_session_00000, 463)
				return err
			}
		}
	}

	trace_util_0.Count(_session_00000, 452)
	vars.CommonGlobalLoaded = true
	return nil
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) {
	trace_util_0.Count(_session_00000, 464)
	if s.txn.validOrPending() {
		trace_util_0.Count(_session_00000, 466)
		return
	}

	trace_util_0.Count(_session_00000, 465)
	txnFuture := s.getTxnFuture(ctx)
	s.txn.changeInvalidToPending(txnFuture)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
	}
	if !s.sessionVars.IsAutocommit() {
		trace_util_0.Count(_session_00000, 467)
		pessTxnConf := config.GetGlobalConfig().PessimisticTxn
		if pessTxnConf.Enable {
			trace_util_0.Count(_session_00000, 468)
			txnMode := s.sessionVars.TxnMode
			if txnMode == "" && pessTxnConf.Default {
				trace_util_0.Count(_session_00000, 470)
				txnMode = ast.Pessimistic
			}
			trace_util_0.Count(_session_00000, 469)
			if txnMode == ast.Pessimistic {
				trace_util_0.Count(_session_00000, 471)
				s.sessionVars.TxnCtx.IsPessimistic = true
			}
		}
	}
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	trace_util_0.Count(_session_00000, 472)
	if err := s.doCommit(ctx); err != nil {
		trace_util_0.Count(_session_00000, 474)
		return err
	}

	trace_util_0.Count(_session_00000, 473)
	return s.NewTxn(ctx)
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	trace_util_0.Count(_session_00000, 475)
	if s.txn.Valid() {
		trace_util_0.Count(_session_00000, 479)
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	trace_util_0.Count(_session_00000, 476)
	txn, err := s.store.BeginWithStartTS(startTS)
	if err != nil {
		trace_util_0.Count(_session_00000, 480)
		return err
	}
	trace_util_0.Count(_session_00000, 477)
	s.txn.changeInvalidToValid(txn)
	s.txn.SetCap(s.getMembufCap())
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		trace_util_0.Count(_session_00000, 481)
		return err
	}
	trace_util_0.Count(_session_00000, 478)
	return nil
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	trace_util_0.Count(_session_00000, 482)
	return s.store
}

func (s *session) ShowProcess() *util.ProcessInfo {
	trace_util_0.Count(_session_00000, 483)
	var pi *util.ProcessInfo
	tmp := s.processInfo.Load()
	if tmp != nil {
		trace_util_0.Count(_session_00000, 485)
		pi = tmp.(*util.ProcessInfo)
	}
	trace_util_0.Count(_session_00000, 484)
	return pi
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(node ast.StmtNode, vars *variable.SessionVars) {
	trace_util_0.Count(_session_00000, 486)
	switch stmt := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt:
		trace_util_0.Count(_session_00000, 487)
		user := vars.User
		schemaVersion := vars.TxnCtx.SchemaVersion
		if ss, ok := node.(ast.SensitiveStmtNode); ok {
			trace_util_0.Count(_session_00000, 489)
			logutil.Logger(context.Background()).Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			trace_util_0.Count(_session_00000, 490)
			{
				logutil.Logger(context.Background()).Info("CRUCIAL OPERATION",
					zap.Uint64("conn", vars.ConnectionID),
					zap.Int64("schemaVersion", schemaVersion),
					zap.String("cur_db", vars.CurrentDB),
					zap.String("sql", stmt.Text()),
					zap.Stringer("user", user))
			}
		}
	default:
		trace_util_0.Count(_session_00000, 488)
		logQuery(node.Text(), vars)
	}
}

func logQuery(query string, vars *variable.SessionVars) {
	trace_util_0.Count(_session_00000, 491)
	if atomic.LoadUint32(&variable.ProcessGeneralLog) != 0 && !vars.InRestrictedSQL {
		trace_util_0.Count(_session_00000, 492)
		query = executor.QueryReplacer.Replace(query)
		logutil.Logger(context.Background()).Info("GENERAL_LOG",
			zap.Uint64("conn", vars.ConnectionID),
			zap.Stringer("user", vars.User),
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.String("current_db", vars.CurrentDB),
			zap.String("sql", query+vars.GetExecuteArgumentsInfo()))
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64) {
	trace_util_0.Count(_session_00000, 493)
	if s.isInternal() {
		trace_util_0.Count(_session_00000, 494)
		if err != nil {
			trace_util_0.Count(_session_00000, 495)
			statementPerTransactionInternalError.Observe(float64(counter))
			transactionDurationInternalError.Observe(duration)
		} else {
			trace_util_0.Count(_session_00000, 496)
			{
				statementPerTransactionInternalOK.Observe(float64(counter))
				transactionDurationInternalOK.Observe(duration)
			}
		}
	} else {
		trace_util_0.Count(_session_00000, 497)
		{
			if err != nil {
				trace_util_0.Count(_session_00000, 498)
				statementPerTransactionGeneralError.Observe(float64(counter))
				transactionDurationGeneralError.Observe(duration)
			} else {
				trace_util_0.Count(_session_00000, 499)
				{
					statementPerTransactionGeneralOK.Observe(float64(counter))
					transactionDurationGeneralOK.Observe(duration)
				}
			}
		}
	}
}

func (s *session) recordTransactionCounter(err error) {
	trace_util_0.Count(_session_00000, 500)
	if s.isInternal() {
		trace_util_0.Count(_session_00000, 501)
		if err != nil {
			trace_util_0.Count(_session_00000, 502)
			transactionCounterInternalErr.Inc()
		} else {
			trace_util_0.Count(_session_00000, 503)
			{
				transactionCounterInternalOK.Inc()
			}
		}
	} else {
		trace_util_0.Count(_session_00000, 504)
		{
			if err != nil {
				trace_util_0.Count(_session_00000, 505)
				transactionCounterGeneralErr.Inc()
			} else {
				trace_util_0.Count(_session_00000, 506)
				{
					transactionCounterGeneralOK.Inc()
				}
			}
		}
	}
}

var _session_00000 = "session/session.go"
