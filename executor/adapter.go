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

package executor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte)
}

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	executor   Executor
	stmt       *ExecStmt
	lastErr    error
	txnStartTS uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	trace_util_0.Count(_adapter_00000, 0)
	if len(a.fields) == 0 {
		trace_util_0.Count(_adapter_00000, 2)
		a.fields = schema2ResultFields(a.executor.Schema(), a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	trace_util_0.Count(_adapter_00000, 1)
	return a.fields
}

func schema2ResultFields(schema *expression.Schema, defaultDB string) (rfs []*ast.ResultField) {
	trace_util_0.Count(_adapter_00000, 3)
	rfs = make([]*ast.ResultField, 0, schema.Len())
	for _, col := range schema.Columns {
		trace_util_0.Count(_adapter_00000, 5)
		dbName := col.DBName.O
		if dbName == "" && col.TblName.L != "" {
			trace_util_0.Count(_adapter_00000, 8)
			dbName = defaultDB
		}
		trace_util_0.Count(_adapter_00000, 6)
		origColName := col.OrigColName
		if origColName.L == "" {
			trace_util_0.Count(_adapter_00000, 9)
			origColName = col.ColName
		}
		trace_util_0.Count(_adapter_00000, 7)
		rf := &ast.ResultField{
			ColumnAsName: col.ColName,
			TableAsName:  col.TblName,
			DBName:       model.NewCIStr(dbName),
			Table:        &model.TableInfo{Name: col.OrigTblName},
			Column: &model.ColumnInfo{
				FieldType: *col.RetType,
				Name:      origColName,
			},
		}
		rfs = append(rfs, rf)
	}
	trace_util_0.Count(_adapter_00000, 4)
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_adapter_00000, 10)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_adapter_00000, 15)
		span1 := span.Tracer().StartSpan("recordSet.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_adapter_00000, 11)
	err := Next(ctx, a.executor, req)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 16)
		a.lastErr = err
		return err
	}
	trace_util_0.Count(_adapter_00000, 12)
	numRows := req.NumRows()
	if numRows == 0 {
		trace_util_0.Count(_adapter_00000, 17)
		if a.stmt != nil {
			trace_util_0.Count(_adapter_00000, 19)
			a.stmt.Ctx.GetSessionVars().LastFoundRows = a.stmt.Ctx.GetSessionVars().StmtCtx.FoundRows()
		}
		trace_util_0.Count(_adapter_00000, 18)
		return nil
	}
	trace_util_0.Count(_adapter_00000, 13)
	if a.stmt != nil {
		trace_util_0.Count(_adapter_00000, 20)
		a.stmt.Ctx.GetSessionVars().StmtCtx.AddFoundRows(uint64(numRows))
	}
	trace_util_0.Count(_adapter_00000, 14)
	return nil
}

// NewRecordBatch create a recordBatch base on top-level executor's newFirstChunk().
func (a *recordSet) NewRecordBatch() *chunk.RecordBatch {
	trace_util_0.Count(_adapter_00000, 21)
	return chunk.NewRecordBatch(newFirstChunk(a.executor))
}

func (a *recordSet) Close() error {
	trace_util_0.Count(_adapter_00000, 22)
	err := a.executor.Close()
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil)
	a.stmt.logAudit()
	return err
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority bool
	// Cacheable represents whether the physical plan can be cached.
	Cacheable bool
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx sessionctx.Context
	// StartTime stands for the starting time when executing the statement.
	StartTime         time.Time
	isPreparedStmt    bool
	isSelectForUpdate bool
	retryCount        uint
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	trace_util_0.Count(_adapter_00000, 23)
	return a.Text
}

// IsPrepared returns true if stmt is a prepare statement.
func (a *ExecStmt) IsPrepared() bool {
	trace_util_0.Count(_adapter_00000, 24)
	return a.isPreparedStmt
}

// IsReadOnly returns true if a statement is read only.
// If current StmtNode is an ExecuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a statement is read only or not.
func (a *ExecStmt) IsReadOnly(vars *variable.SessionVars) bool {
	trace_util_0.Count(_adapter_00000, 25)
	if execStmt, ok := a.StmtNode.(*ast.ExecuteStmt); ok {
		trace_util_0.Count(_adapter_00000, 27)
		s, err := getPreparedStmt(execStmt, vars)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 29)
			logutil.Logger(context.Background()).Error("getPreparedStmt failed", zap.Error(err))
			return false
		}
		trace_util_0.Count(_adapter_00000, 28)
		return ast.IsReadOnly(s)
	}
	trace_util_0.Count(_adapter_00000, 26)
	return ast.IsReadOnly(a.StmtNode)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schema version that 'a' is using.
func (a *ExecStmt) RebuildPlan() (int64, error) {
	trace_util_0.Count(_adapter_00000, 30)
	is := GetInfoSchema(a.Ctx)
	a.InfoSchema = is
	if err := plannercore.Preprocess(a.Ctx, a.StmtNode, is, plannercore.InTxnRetry); err != nil {
		trace_util_0.Count(_adapter_00000, 33)
		return 0, err
	}
	trace_util_0.Count(_adapter_00000, 31)
	p, err := planner.Optimize(a.Ctx, a.StmtNode, is)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 34)
		return 0, err
	}
	trace_util_0.Count(_adapter_00000, 32)
	a.Plan = p
	return is.SchemaMetaVersion(), nil
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	trace_util_0.Count(_adapter_00000, 35)
	a.StartTime = time.Now()
	sctx := a.Ctx
	if _, ok := a.Plan.(*plannercore.Analyze); ok && sctx.GetSessionVars().InRestrictedSQL {
		trace_util_0.Count(_adapter_00000, 44)
		oriStats, _ := sctx.GetSessionVars().GetSystemVar(variable.TiDBBuildStatsConcurrency)
		oriScan := sctx.GetSessionVars().DistSQLScanConcurrency
		oriIndex := sctx.GetSessionVars().IndexSerialScanConcurrency
		oriIso, _ := sctx.GetSessionVars().GetSystemVar(variable.TxnIsolation)
		terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, "1"))
		sctx.GetSessionVars().DistSQLScanConcurrency = 1
		sctx.GetSessionVars().IndexSerialScanConcurrency = 1
		terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted))
		defer func() {
			trace_util_0.Count(_adapter_00000, 45)
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TiDBBuildStatsConcurrency, oriStats))
			sctx.GetSessionVars().DistSQLScanConcurrency = oriScan
			sctx.GetSessionVars().IndexSerialScanConcurrency = oriIndex
			terror.Log(sctx.GetSessionVars().SetSystemVar(variable.TxnIsolation, oriIso))
		}()
	}

	trace_util_0.Count(_adapter_00000, 36)
	e, err := a.buildExecutor()
	if err != nil {
		trace_util_0.Count(_adapter_00000, 46)
		return nil, err
	}

	trace_util_0.Count(_adapter_00000, 37)
	if err = e.Open(ctx); err != nil {
		trace_util_0.Count(_adapter_00000, 47)
		terror.Call(e.Close)
		return nil, err
	}

	trace_util_0.Count(_adapter_00000, 38)
	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		trace_util_0.Count(_adapter_00000, 48)
		pi = raw
		sql := a.OriginText()
		if simple, ok := a.Plan.(*plannercore.Simple); ok && simple.Statement != nil {
			trace_util_0.Count(_adapter_00000, 50)
			if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
				trace_util_0.Count(_adapter_00000, 51)
				// Use SecureText to avoid leak password information.
				sql = ss.SecureText()
			}
		}
		// Update processinfo, ShowProcess() will use it.
		trace_util_0.Count(_adapter_00000, 49)
		pi.SetProcessInfo(sql, time.Now(), cmd)
		a.Ctx.GetSessionVars().StmtCtx.StmtType = GetStmtLabel(a.StmtNode)
	}

	trace_util_0.Count(_adapter_00000, 39)
	isPessimistic := sctx.GetSessionVars().TxnCtx.IsPessimistic

	// Special handle for "select for update statement" in pessimistic transaction.
	if isPessimistic && a.isSelectForUpdate {
		trace_util_0.Count(_adapter_00000, 52)
		return a.handlePessimisticSelectForUpdate(ctx, e)
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	trace_util_0.Count(_adapter_00000, 40)
	if e.Schema().Len() == 0 {
		trace_util_0.Count(_adapter_00000, 53)
		if isPessimistic {
			trace_util_0.Count(_adapter_00000, 55)
			return nil, a.handlePessimisticDML(ctx, e)
		}
		trace_util_0.Count(_adapter_00000, 54)
		return a.handleNoDelayExecutor(ctx, e)
	} else {
		trace_util_0.Count(_adapter_00000, 56)
		if proj, ok := e.(*ProjectionExec); ok && proj.calculateNoDelay {
			trace_util_0.Count(_adapter_00000, 57)
			// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
			// the Projection has two expressions and two columns in the schema, but we should
			// not return the result of the two expressions.
			return a.handleNoDelayExecutor(ctx, e)
		}
	}

	trace_util_0.Count(_adapter_00000, 41)
	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 58)
		return nil, err
	}
	trace_util_0.Count(_adapter_00000, 42)
	if txn.Valid() {
		trace_util_0.Count(_adapter_00000, 59)
		txnStartTS = txn.StartTS()
	}
	trace_util_0.Count(_adapter_00000, 43)
	return &recordSet{
		executor:   e,
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

type chunkRowRecordSet struct {
	rows   []chunk.Row
	idx    int
	fields []*ast.ResultField
	e      Executor
}

func (c *chunkRowRecordSet) Fields() []*ast.ResultField {
	trace_util_0.Count(_adapter_00000, 60)
	return c.fields
}

func (c *chunkRowRecordSet) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_adapter_00000, 61)
	chk := req.Chunk
	chk.Reset()
	for !chk.IsFull() && c.idx < len(c.rows) {
		trace_util_0.Count(_adapter_00000, 63)
		chk.AppendRow(c.rows[c.idx])
		c.idx++
	}
	trace_util_0.Count(_adapter_00000, 62)
	return nil
}

func (c *chunkRowRecordSet) NewRecordBatch() *chunk.RecordBatch {
	trace_util_0.Count(_adapter_00000, 64)
	return chunk.NewRecordBatch(newFirstChunk(c.e))
}

func (c *chunkRowRecordSet) Close() error {
	trace_util_0.Count(_adapter_00000, 65)
	return nil
}

func (a *ExecStmt) handlePessimisticSelectForUpdate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_adapter_00000, 66)
	for {
		trace_util_0.Count(_adapter_00000, 67)
		rs, err := a.runPessimisticSelectForUpdate(ctx, e)
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 69)
			return nil, err
		}
		trace_util_0.Count(_adapter_00000, 68)
		if e == nil {
			trace_util_0.Count(_adapter_00000, 70)
			return rs, nil
		}
	}
}

func (a *ExecStmt) runPessimisticSelectForUpdate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_adapter_00000, 71)
	rs := &recordSet{
		executor: e,
		stmt:     a,
	}
	defer func() {
		trace_util_0.Count(_adapter_00000, 74)
		terror.Log(rs.Close())
	}()

	trace_util_0.Count(_adapter_00000, 72)
	var rows []chunk.Row
	var err error
	fields := rs.Fields()
	req := rs.NewRecordBatch()
	for {
		trace_util_0.Count(_adapter_00000, 75)
		err = rs.Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 79)
			// Handle 'write conflict' error.
			break
		}
		trace_util_0.Count(_adapter_00000, 76)
		if req.NumRows() == 0 {
			trace_util_0.Count(_adapter_00000, 80)
			return &chunkRowRecordSet{rows: rows, fields: fields, e: e}, nil
		}
		trace_util_0.Count(_adapter_00000, 77)
		iter := chunk.NewIterator4Chunk(req.Chunk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			trace_util_0.Count(_adapter_00000, 81)
			rows = append(rows, r)
		}
		trace_util_0.Count(_adapter_00000, 78)
		req.Chunk = chunk.Renew(req.Chunk, a.Ctx.GetSessionVars().MaxChunkSize)
	}
	trace_util_0.Count(_adapter_00000, 73)
	return nil, err
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_adapter_00000, 82)
	sctx := a.Ctx
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_adapter_00000, 87)
		span1 := span.Tracer().StartSpan("executor.handleNoDelayExecutor", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	trace_util_0.Count(_adapter_00000, 83)
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadDataExec, *DDLExec:
		trace_util_0.Count(_adapter_00000, 88)
		snapshotTS := sctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			trace_util_0.Count(_adapter_00000, 90)
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
		trace_util_0.Count(_adapter_00000, 89)
		lowResolutionTSO := sctx.GetSessionVars().LowResolutionTSO
		if lowResolutionTSO {
			trace_util_0.Count(_adapter_00000, 91)
			return nil, errors.New("can not execute write statement when 'tidb_low_resolution_tso' is set")
		}
	}

	trace_util_0.Count(_adapter_00000, 84)
	var err error
	defer func() {
		trace_util_0.Count(_adapter_00000, 92)
		terror.Log(e.Close())
		a.logAudit()
	}()

	trace_util_0.Count(_adapter_00000, 85)
	err = Next(ctx, e, chunk.NewRecordBatch(newFirstChunk(e)))
	if err != nil {
		trace_util_0.Count(_adapter_00000, 93)
		return nil, err
	}
	trace_util_0.Count(_adapter_00000, 86)
	return nil, err
}

func (a *ExecStmt) handlePessimisticDML(ctx context.Context, e Executor) error {
	trace_util_0.Count(_adapter_00000, 94)
	sctx := a.Ctx
	txn, err := sctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 96)
		return err
	}
	trace_util_0.Count(_adapter_00000, 95)
	txnCtx := sctx.GetSessionVars().TxnCtx
	for {
		trace_util_0.Count(_adapter_00000, 97)
		_, err = a.handleNoDelayExecutor(ctx, e)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 102)
			return err
		}
		trace_util_0.Count(_adapter_00000, 98)
		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			trace_util_0.Count(_adapter_00000, 103)
			return err1
		}
		trace_util_0.Count(_adapter_00000, 99)
		if len(keys) == 0 {
			trace_util_0.Count(_adapter_00000, 104)
			return nil
		}
		trace_util_0.Count(_adapter_00000, 100)
		forUpdateTS := txnCtx.GetForUpdateTS()
		err = txn.LockKeys(ctx, forUpdateTS, keys...)
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 105)
			return err
		}
		trace_util_0.Count(_adapter_00000, 101)
		if e == nil {
			trace_util_0.Count(_adapter_00000, 106)
			return nil
		}
	}
}

// handlePessimisticLockError updates TS and rebuild executor if the err is write conflict.
func (a *ExecStmt) handlePessimisticLockError(ctx context.Context, err error) (Executor, error) {
	trace_util_0.Count(_adapter_00000, 107)
	if err == nil {
		trace_util_0.Count(_adapter_00000, 115)
		return nil, nil
	}
	trace_util_0.Count(_adapter_00000, 108)
	txnCtx := a.Ctx.GetSessionVars().TxnCtx
	var newForUpdateTS uint64
	if deadlock, ok := errors.Cause(err).(*tikv.ErrDeadlock); ok {
		trace_util_0.Count(_adapter_00000, 116)
		if !deadlock.IsRetryable {
			trace_util_0.Count(_adapter_00000, 118)
			return nil, ErrDeadlock
		}
		trace_util_0.Count(_adapter_00000, 117)
		logutil.Logger(ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Binary("lockKey", deadlock.LockKey),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
	} else {
		trace_util_0.Count(_adapter_00000, 119)
		if terror.ErrorEqual(kv.ErrWriteConflict, err) {
			trace_util_0.Count(_adapter_00000, 120)
			conflictCommitTS := extractConflictCommitTS(err.Error())
			if conflictCommitTS == 0 {
				trace_util_0.Count(_adapter_00000, 122)
				logutil.Logger(ctx).Warn("failed to extract conflictCommitTS from a conflict error")
			}
			trace_util_0.Count(_adapter_00000, 121)
			forUpdateTS := txnCtx.GetForUpdateTS()
			logutil.Logger(ctx).Info("pessimistic write conflict, retry statement",
				zap.Uint64("txn", txnCtx.StartTS),
				zap.Uint64("forUpdateTS", forUpdateTS),
				zap.Uint64("conflictCommitTS", conflictCommitTS))
			if conflictCommitTS > forUpdateTS {
				trace_util_0.Count(_adapter_00000, 123)
				newForUpdateTS = conflictCommitTS
			}
		} else {
			trace_util_0.Count(_adapter_00000, 124)
			{
				return nil, err
			}
		}
	}
	trace_util_0.Count(_adapter_00000, 109)
	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		trace_util_0.Count(_adapter_00000, 125)
		return nil, errors.New("pessimistic lock retry limit reached")
	}
	trace_util_0.Count(_adapter_00000, 110)
	a.retryCount++
	if newForUpdateTS == 0 {
		trace_util_0.Count(_adapter_00000, 126)
		newForUpdateTS, err = a.Ctx.GetStore().GetOracle().GetTimestamp(ctx)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 127)
			return nil, err
		}
	}
	trace_util_0.Count(_adapter_00000, 111)
	txnCtx.SetForUpdateTS(newForUpdateTS)
	txn, err := a.Ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 128)
		return nil, err
	}
	trace_util_0.Count(_adapter_00000, 112)
	txn.SetOption(kv.SnapshotTS, newForUpdateTS)
	e, err := a.buildExecutor()
	if err != nil {
		trace_util_0.Count(_adapter_00000, 129)
		return nil, err
	}
	// Rollback the statement change before retry it.
	trace_util_0.Count(_adapter_00000, 113)
	a.Ctx.StmtRollback()
	a.Ctx.GetSessionVars().StmtCtx.ResetForRetry()

	if err = e.Open(ctx); err != nil {
		trace_util_0.Count(_adapter_00000, 130)
		return nil, err
	}
	trace_util_0.Count(_adapter_00000, 114)
	return e, nil
}

func extractConflictCommitTS(errStr string) uint64 {
	trace_util_0.Count(_adapter_00000, 131)
	strs := strings.Split(errStr, "conflictCommitTS=")
	if len(strs) != 2 {
		trace_util_0.Count(_adapter_00000, 135)
		return 0
	}
	trace_util_0.Count(_adapter_00000, 132)
	tsPart := strs[1]
	length := strings.IndexByte(tsPart, ',')
	if length < 0 {
		trace_util_0.Count(_adapter_00000, 136)
		return 0
	}
	trace_util_0.Count(_adapter_00000, 133)
	tsStr := tsPart[:length]
	ts, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 137)
		return 0
	}
	trace_util_0.Count(_adapter_00000, 134)
	return ts
}

type pessimisticTxn interface {
	kv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]kv.Key, error)
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (Executor, error) {
	trace_util_0.Count(_adapter_00000, 138)
	ctx := a.Ctx
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		trace_util_0.Count(_adapter_00000, 143)
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		useMaxTS, err := IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 147)
			return nil, err
		}
		trace_util_0.Count(_adapter_00000, 144)
		if useMaxTS {
			trace_util_0.Count(_adapter_00000, 148)
			logutil.Logger(context.Background()).Debug("init txnStartTS with MaxUint64", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.String("text", a.Text))
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else {
			trace_util_0.Count(_adapter_00000, 149)
			if ctx.GetSessionVars().SnapshotTS != 0 {
				trace_util_0.Count(_adapter_00000, 150)
				if _, ok := a.Plan.(*plannercore.CheckTable); ok {
					trace_util_0.Count(_adapter_00000, 151)
					err = ctx.InitTxnWithStartTS(ctx.GetSessionVars().SnapshotTS)
				}
			}
		}
		trace_util_0.Count(_adapter_00000, 145)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 152)
			return nil, err
		}

		trace_util_0.Count(_adapter_00000, 146)
		stmtCtx := ctx.GetSessionVars().StmtCtx
		if stmtPri := stmtCtx.Priority; stmtPri == mysql.NoPriority {
			trace_util_0.Count(_adapter_00000, 153)
			switch {
			case useMaxTS:
				trace_util_0.Count(_adapter_00000, 154)
				stmtCtx.Priority = kv.PriorityHigh
			case a.LowerPriority:
				trace_util_0.Count(_adapter_00000, 155)
				stmtCtx.Priority = kv.PriorityLow
			}
		}
	}
	trace_util_0.Count(_adapter_00000, 139)
	if _, ok := a.Plan.(*plannercore.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
		trace_util_0.Count(_adapter_00000, 156)
		ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
	}

	trace_util_0.Count(_adapter_00000, 140)
	b := newExecutorBuilder(ctx, a.InfoSchema)
	e := b.build(a.Plan)
	if b.err != nil {
		trace_util_0.Count(_adapter_00000, 157)
		return nil, errors.Trace(b.err)
	}

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	trace_util_0.Count(_adapter_00000, 141)
	if executorExec, ok := e.(*ExecuteExec); ok {
		trace_util_0.Count(_adapter_00000, 158)
		err := executorExec.Build(b)
		if err != nil {
			trace_util_0.Count(_adapter_00000, 161)
			return nil, err
		}
		trace_util_0.Count(_adapter_00000, 159)
		a.isPreparedStmt = true
		a.Plan = executorExec.plan
		if executorExec.lowerPriority {
			trace_util_0.Count(_adapter_00000, 162)
			ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
		}
		trace_util_0.Count(_adapter_00000, 160)
		e = executorExec.stmtExec
	}
	trace_util_0.Count(_adapter_00000, 142)
	a.isSelectForUpdate = b.isSelectForUpdate
	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *ExecStmt) logAudit() {
	trace_util_0.Count(_adapter_00000, 163)
	sessVars := a.Ctx.GetSessionVars()
	if sessVars.InRestrictedSQL {
		trace_util_0.Count(_adapter_00000, 166)
		return
	}
	trace_util_0.Count(_adapter_00000, 164)
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		trace_util_0.Count(_adapter_00000, 167)
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			trace_util_0.Count(_adapter_00000, 169)
			cmd := mysql.Command2Str[byte(atomic.LoadUint32(&a.Ctx.GetSessionVars().CommandValue))]
			audit.OnGeneralEvent(context.Background(), sessVars, plugin.Log, cmd)
		}
		trace_util_0.Count(_adapter_00000, 168)
		return nil
	})
	trace_util_0.Count(_adapter_00000, 165)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 170)
		log.Error("log audit log failure", zap.Error(err))
	}
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmt) LogSlowQuery(txnTS uint64, succ bool) {
	trace_util_0.Count(_adapter_00000, 171)
	sessVars := a.Ctx.GetSessionVars()
	level := log.GetLevel()
	if level > zapcore.WarnLevel {
		trace_util_0.Count(_adapter_00000, 177)
		return
	}
	trace_util_0.Count(_adapter_00000, 172)
	cfg := config.GetGlobalConfig()
	costTime := time.Since(a.StartTime)
	threshold := time.Duration(atomic.LoadUint64(&cfg.Log.SlowThreshold)) * time.Millisecond
	if costTime < threshold && level > zapcore.DebugLevel {
		trace_util_0.Count(_adapter_00000, 178)
		return
	}
	trace_util_0.Count(_adapter_00000, 173)
	sql := a.Text
	if maxQueryLen := atomic.LoadUint64(&cfg.Log.QueryLogMaxLen); uint64(len(sql)) > maxQueryLen {
		trace_util_0.Count(_adapter_00000, 179)
		sql = fmt.Sprintf("%.*q(len:%d)", maxQueryLen, sql, len(a.Text))
	}
	trace_util_0.Count(_adapter_00000, 174)
	sql = QueryReplacer.Replace(sql) + sessVars.GetExecuteArgumentsInfo()

	var tableIDs, indexIDs string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		trace_util_0.Count(_adapter_00000, 180)
		tableIDs = strings.Replace(fmt.Sprintf("%v", a.Ctx.GetSessionVars().StmtCtx.TableIDs), " ", ",", -1)
	}
	trace_util_0.Count(_adapter_00000, 175)
	if len(sessVars.StmtCtx.IndexIDs) > 0 {
		trace_util_0.Count(_adapter_00000, 181)
		indexIDs = strings.Replace(fmt.Sprintf("%v", a.Ctx.GetSessionVars().StmtCtx.IndexIDs), " ", ",", -1)
	}
	trace_util_0.Count(_adapter_00000, 176)
	execDetail := sessVars.StmtCtx.GetExecDetails()
	copTaskInfo := sessVars.StmtCtx.CopTasksDetails()
	statsInfos := plannercore.GetStatsInfo(a.Plan)
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	if costTime < threshold {
		trace_util_0.Count(_adapter_00000, 182)
		_, digest := sessVars.StmtCtx.SQLDigest()
		logutil.SlowQueryLogger.Debug(sessVars.SlowLogFormat(txnTS, costTime, execDetail, indexIDs, digest, statsInfos, copTaskInfo, memMax, sql))
	} else {
		trace_util_0.Count(_adapter_00000, 183)
		{
			_, digest := sessVars.StmtCtx.SQLDigest()
			logutil.SlowQueryLogger.Warn(sessVars.SlowLogFormat(txnTS, costTime, execDetail, indexIDs, digest, statsInfos, copTaskInfo, memMax, sql))
			metrics.TotalQueryProcHistogram.Observe(costTime.Seconds())
			metrics.TotalCopProcHistogram.Observe(execDetail.ProcessTime.Seconds())
			metrics.TotalCopWaitHistogram.Observe(execDetail.WaitTime.Seconds())
			var userString string
			if sessVars.User != nil {
				trace_util_0.Count(_adapter_00000, 185)
				userString = sessVars.User.String()
			}
			trace_util_0.Count(_adapter_00000, 184)
			domain.GetDomain(a.Ctx).LogSlowQuery(&domain.SlowQueryInfo{
				SQL:      sql,
				Digest:   digest,
				Start:    a.StartTime,
				Duration: costTime,
				Detail:   sessVars.StmtCtx.GetExecDetails(),
				Succ:     succ,
				ConnID:   sessVars.ConnectionID,
				TxnTS:    txnTS,
				User:     userString,
				DB:       sessVars.CurrentDB,
				TableIDs: tableIDs,
				IndexIDs: indexIDs,
				Internal: sessVars.InRestrictedSQL,
			})
		}
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. txn is not valid
//  2. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p plannercore.Plan) (bool, error) {
	trace_util_0.Count(_adapter_00000, 186)
	// check auto commit
	if !ctx.GetSessionVars().IsAutocommit() {
		trace_util_0.Count(_adapter_00000, 191)
		return false, nil
	}

	// check txn
	trace_util_0.Count(_adapter_00000, 187)
	txn, err := ctx.Txn(false)
	if err != nil {
		trace_util_0.Count(_adapter_00000, 192)
		return false, err
	}
	trace_util_0.Count(_adapter_00000, 188)
	if txn.Valid() {
		trace_util_0.Count(_adapter_00000, 193)
		return false, nil
	}

	// check plan
	trace_util_0.Count(_adapter_00000, 189)
	if proj, ok := p.(*plannercore.PhysicalProjection); ok {
		trace_util_0.Count(_adapter_00000, 194)
		if len(proj.Children()) != 1 {
			trace_util_0.Count(_adapter_00000, 196)
			return false, nil
		}
		trace_util_0.Count(_adapter_00000, 195)
		p = proj.Children()[0]
	}

	trace_util_0.Count(_adapter_00000, 190)
	switch v := p.(type) {
	case *plannercore.PhysicalIndexReader:
		trace_util_0.Count(_adapter_00000, 197)
		indexScan := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx), nil
	case *plannercore.PhysicalTableReader:
		trace_util_0.Count(_adapter_00000, 198)
		tableScan := v.TablePlans[0].(*plannercore.PhysicalTableScan)
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint(ctx.GetSessionVars().StmtCtx), nil
	case *plannercore.PointGetPlan:
		trace_util_0.Count(_adapter_00000, 199)
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		return v.IndexInfo == nil, nil
	default:
		trace_util_0.Count(_adapter_00000, 200)
		return false, nil
	}
}

var _adapter_00000 = "executor/adapter.go"
