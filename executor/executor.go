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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	_ Executor = &baseExecutor{}
	_ Executor = &CheckTableExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowNextRowIDExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &ShowDDLJobsExec{}
	_ Executor = &ShowDDLJobQueriesExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}
	_ Executor = &CheckIndexExec{}
	_ Executor = &HashJoinExec{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &MergeJoinExec{}
)

type baseExecutor struct {
	ctx           sessionctx.Context
	id            fmt.Stringer
	schema        *expression.Schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.RuntimeStats
}

// base returns the baseExecutor of an executor, don't override this method!
func (e *baseExecutor) base() *baseExecutor {
	trace_util_0.Count(_executor_00000, 0)
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 1)
	for _, child := range e.children {
		trace_util_0.Count(_executor_00000, 3)
		err := child.Open(ctx)
		if err != nil {
			trace_util_0.Count(_executor_00000, 4)
			return err
		}
	}
	trace_util_0.Count(_executor_00000, 2)
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	trace_util_0.Count(_executor_00000, 5)
	for _, child := range e.children {
		trace_util_0.Count(_executor_00000, 7)
		err := child.Close()
		if err != nil {
			trace_util_0.Count(_executor_00000, 8)
			return err
		}
	}
	trace_util_0.Count(_executor_00000, 6)
	return nil
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	trace_util_0.Count(_executor_00000, 9)
	if e.schema == nil {
		trace_util_0.Count(_executor_00000, 11)
		return expression.NewSchema()
	}
	trace_util_0.Count(_executor_00000, 10)
	return e.schema
}

// newFirstChunk creates a new chunk to buffer current executor's result.
func newFirstChunk(e Executor) *chunk.Chunk {
	trace_util_0.Count(_executor_00000, 12)
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// retTypes returns all output column types.
func retTypes(e Executor) []*types.FieldType {
	trace_util_0.Count(_executor_00000, 13)
	base := e.base()
	return base.retFieldTypes
}

// Next fills mutiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 14)
	return nil
}

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id fmt.Stringer, children ...Executor) baseExecutor {
	trace_util_0.Count(_executor_00000, 15)
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().InitChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		trace_util_0.Count(_executor_00000, 18)
		if e.id != nil {
			trace_util_0.Count(_executor_00000, 19)
			e.runtimeStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.id.String())
		}
	}
	trace_util_0.Count(_executor_00000, 16)
	if schema != nil {
		trace_util_0.Count(_executor_00000, 20)
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			trace_util_0.Count(_executor_00000, 21)
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	trace_util_0.Count(_executor_00000, 17)
	return e
}

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.RecordBatch) error
	Close() error
	Schema() *expression.Schema
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 22)
	sessVars := e.base().ctx.GetSessionVars()
	if atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0) {
		trace_util_0.Count(_executor_00000, 24)
		return ErrQueryInterrupted
	}

	trace_util_0.Count(_executor_00000, 23)
	return e.Next(ctx, req)
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 25)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 29)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 30)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 26)
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		trace_util_0.Count(_executor_00000, 31)
		return nil
	}
	trace_util_0.Count(_executor_00000, 27)
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		trace_util_0.Count(_executor_00000, 32)
		req.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
		if e.errs[i] != nil {
			trace_util_0.Count(_executor_00000, 33)
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			trace_util_0.Count(_executor_00000, 34)
			{
				req.AppendString(1, "successful")
			}
		}
	}
	trace_util_0.Count(_executor_00000, 28)
	e.cursor += numCurBatch
	return nil
}

// ShowNextRowIDExec represents a show the next row ID executor.
type ShowNextRowIDExec struct {
	baseExecutor
	tblName *ast.TableName
	done    bool
}

// Next implements the Executor Next interface.
func (e *ShowNextRowIDExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 35)
	req.Reset()
	if e.done {
		trace_util_0.Count(_executor_00000, 40)
		return nil
	}
	trace_util_0.Count(_executor_00000, 36)
	is := domain.GetDomain(e.ctx).InfoSchema()
	tbl, err := is.TableByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		trace_util_0.Count(_executor_00000, 41)
		return err
	}
	trace_util_0.Count(_executor_00000, 37)
	colName := model.ExtraHandleName
	for _, col := range tbl.Meta().Columns {
		trace_util_0.Count(_executor_00000, 42)
		if mysql.HasAutoIncrementFlag(col.Flag) {
			trace_util_0.Count(_executor_00000, 43)
			colName = col.Name
			break
		}
	}
	trace_util_0.Count(_executor_00000, 38)
	nextGlobalID, err := tbl.Allocator(e.ctx).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		trace_util_0.Count(_executor_00000, 44)
		return err
	}
	trace_util_0.Count(_executor_00000, 39)
	req.AppendString(0, e.tblName.Schema.O)
	req.AppendString(1, e.tblName.Name.O)
	req.AppendString(2, colName.O)
	req.AppendInt64(3, nextGlobalID)
	e.done = true
	return nil
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	baseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *admin.DDLInfo
	done       bool
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 45)
	req.Reset()
	if e.done {
		trace_util_0.Count(_executor_00000, 49)
		return nil
	}

	trace_util_0.Count(_executor_00000, 46)
	ddlJobs := ""
	query := ""
	l := len(e.ddlInfo.Jobs)
	for i, job := range e.ddlInfo.Jobs {
		trace_util_0.Count(_executor_00000, 50)
		ddlJobs += job.String()
		query += job.Query
		if i != l-1 {
			trace_util_0.Count(_executor_00000, 51)
			ddlJobs += "\n"
			query += "\n"
		}
	}

	trace_util_0.Count(_executor_00000, 47)
	do := domain.GetDomain(e.ctx)
	serverInfo, err := do.InfoSyncer().GetServerInfoByID(ctx, e.ddlOwnerID)
	if err != nil {
		trace_util_0.Count(_executor_00000, 52)
		return err
	}

	trace_util_0.Count(_executor_00000, 48)
	serverAddress := serverInfo.IP + ":" +
		strconv.FormatUint(uint64(serverInfo.Port), 10)

	req.AppendInt64(0, e.ddlInfo.SchemaVer)
	req.AppendString(1, e.ddlOwnerID)
	req.AppendString(2, serverAddress)
	req.AppendString(3, ddlJobs)
	req.AppendString(4, e.selfID)
	req.AppendString(5, query)

	e.done = true
	return nil
}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor    int
	jobs      []*model.Job
	jobNumber int64
	is        infoschema.InfoSchema
}

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs
type ShowDDLJobQueriesExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 53)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 58)
		return err
	}
	trace_util_0.Count(_executor_00000, 54)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_executor_00000, 59)
		return err
	}
	trace_util_0.Count(_executor_00000, 55)
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		trace_util_0.Count(_executor_00000, 60)
		return err
	}
	trace_util_0.Count(_executor_00000, 56)
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	if err != nil {
		trace_util_0.Count(_executor_00000, 61)
		return err
	}

	trace_util_0.Count(_executor_00000, 57)
	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 62)
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		trace_util_0.Count(_executor_00000, 66)
		return nil
	}
	trace_util_0.Count(_executor_00000, 63)
	if len(e.jobIDs) >= len(e.jobs) {
		trace_util_0.Count(_executor_00000, 67)
		return nil
	}
	trace_util_0.Count(_executor_00000, 64)
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		trace_util_0.Count(_executor_00000, 68)
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			trace_util_0.Count(_executor_00000, 69)
			if id == e.jobs[i].ID {
				trace_util_0.Count(_executor_00000, 70)
				req.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	trace_util_0.Count(_executor_00000, 65)
	e.cursor += numCurBatch
	return nil
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobsExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 71)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 77)
		return err
	}
	trace_util_0.Count(_executor_00000, 72)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_executor_00000, 78)
		return err
	}
	trace_util_0.Count(_executor_00000, 73)
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		trace_util_0.Count(_executor_00000, 79)
		return err
	}
	trace_util_0.Count(_executor_00000, 74)
	if e.jobNumber == 0 {
		trace_util_0.Count(_executor_00000, 80)
		e.jobNumber = admin.DefNumHistoryJobs
	}
	trace_util_0.Count(_executor_00000, 75)
	historyJobs, err := admin.GetHistoryDDLJobs(txn, int(e.jobNumber))
	if err != nil {
		trace_util_0.Count(_executor_00000, 81)
		return err
	}
	trace_util_0.Count(_executor_00000, 76)
	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 82)
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		trace_util_0.Count(_executor_00000, 85)
		return nil
	}
	trace_util_0.Count(_executor_00000, 83)
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		trace_util_0.Count(_executor_00000, 86)
		req.AppendInt64(0, e.jobs[i].ID)
		req.AppendString(1, getSchemaName(e.is, e.jobs[i].SchemaID))
		req.AppendString(2, getTableName(e.is, e.jobs[i].TableID))
		req.AppendString(3, e.jobs[i].Type.String())
		req.AppendString(4, e.jobs[i].SchemaState.String())
		req.AppendInt64(5, e.jobs[i].SchemaID)
		req.AppendInt64(6, e.jobs[i].TableID)
		req.AppendInt64(7, e.jobs[i].RowCount)
		req.AppendString(8, model.TSConvert2Time(e.jobs[i].StartTS).String())
		req.AppendString(9, e.jobs[i].State.String())
	}
	trace_util_0.Count(_executor_00000, 84)
	e.cursor += numCurBatch
	return nil
}

func getSchemaName(is infoschema.InfoSchema, id int64) string {
	trace_util_0.Count(_executor_00000, 87)
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		trace_util_0.Count(_executor_00000, 89)
		schemaName = DBInfo.Name.O
		return schemaName
	}

	trace_util_0.Count(_executor_00000, 88)
	return schemaName
}

func getTableName(is infoschema.InfoSchema, id int64) string {
	trace_util_0.Count(_executor_00000, 90)
	var tableName string
	table, ok := is.TableByID(id)
	if ok {
		trace_util_0.Count(_executor_00000, 92)
		tableName = table.Meta().Name.O
		return tableName
	}

	trace_util_0.Count(_executor_00000, 91)
	return tableName
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

	tables []*ast.TableName
	done   bool
	is     infoschema.InfoSchema

	genExprs map[model.TableColumnID]expression.Expression
}

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 93)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 95)
		return err
	}
	trace_util_0.Count(_executor_00000, 94)
	e.done = false
	return nil
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 96)
	if e.done {
		trace_util_0.Count(_executor_00000, 100)
		return nil
	}
	trace_util_0.Count(_executor_00000, 97)
	defer func() { trace_util_0.Count(_executor_00000, 101); e.done = true }()
	trace_util_0.Count(_executor_00000, 98)
	for _, t := range e.tables {
		trace_util_0.Count(_executor_00000, 102)
		dbName := t.DBInfo.Name
		tb, err := e.is.TableByName(dbName, t.Name)
		if err != nil {
			trace_util_0.Count(_executor_00000, 105)
			return err
		}
		trace_util_0.Count(_executor_00000, 103)
		if tb.Meta().GetPartitionInfo() != nil {
			trace_util_0.Count(_executor_00000, 106)
			err = e.doCheckPartitionedTable(tb.(table.PartitionedTable))
		} else {
			trace_util_0.Count(_executor_00000, 107)
			{
				err = e.doCheckTable(tb)
			}
		}
		trace_util_0.Count(_executor_00000, 104)
		if err != nil {
			trace_util_0.Count(_executor_00000, 108)
			logutil.Logger(ctx).Warn("check table failed", zap.String("tableName", t.Name.O), zap.Error(err))
			if admin.ErrDataInConsistent.Equal(err) {
				trace_util_0.Count(_executor_00000, 110)
				return ErrAdminCheckTable.GenWithStack("%v err:%v", t.Name, err)
			}

			trace_util_0.Count(_executor_00000, 109)
			return errors.Errorf("%v err:%v", t.Name, err)
		}
	}
	trace_util_0.Count(_executor_00000, 99)
	return nil
}

func (e *CheckTableExec) doCheckPartitionedTable(tbl table.PartitionedTable) error {
	trace_util_0.Count(_executor_00000, 111)
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		trace_util_0.Count(_executor_00000, 113)
		pid := def.ID
		partition := tbl.GetPartition(pid)
		if err := e.doCheckTable(partition); err != nil {
			trace_util_0.Count(_executor_00000, 114)
			return err
		}
	}
	trace_util_0.Count(_executor_00000, 112)
	return nil
}

func (e *CheckTableExec) doCheckTable(tbl table.Table) error {
	trace_util_0.Count(_executor_00000, 115)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_executor_00000, 118)
		return err
	}
	trace_util_0.Count(_executor_00000, 116)
	for _, idx := range tbl.Indices() {
		trace_util_0.Count(_executor_00000, 119)
		if idx.Meta().State != model.StatePublic {
			trace_util_0.Count(_executor_00000, 121)
			continue
		}
		trace_util_0.Count(_executor_00000, 120)
		err := admin.CompareIndexData(e.ctx, txn, tbl, idx, e.genExprs)
		if err != nil {
			trace_util_0.Count(_executor_00000, 122)
			return err
		}
	}
	trace_util_0.Count(_executor_00000, 117)
	return nil
}

// CheckIndexExec represents the executor of checking an index.
// It is built from the "admin check index" statement, and it checks
// the consistency of the index data with the records of the table.
type CheckIndexExec struct {
	baseExecutor

	dbName    string
	tableName string
	idxName   string
	src       *IndexLookUpExecutor
	done      bool
	is        infoschema.InfoSchema
}

// Open implements the Executor Open interface.
func (e *CheckIndexExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 123)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 126)
		return err
	}
	trace_util_0.Count(_executor_00000, 124)
	if err := e.src.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 127)
		return err
	}
	trace_util_0.Count(_executor_00000, 125)
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckIndexExec) Close() error {
	trace_util_0.Count(_executor_00000, 128)
	return e.src.Close()
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 129)
	if e.done {
		trace_util_0.Count(_executor_00000, 134)
		return nil
	}
	trace_util_0.Count(_executor_00000, 130)
	defer func() { trace_util_0.Count(_executor_00000, 135); e.done = true }()

	trace_util_0.Count(_executor_00000, 131)
	err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tableName, []string{e.idxName})
	if err != nil {
		trace_util_0.Count(_executor_00000, 136)
		return err
	}
	trace_util_0.Count(_executor_00000, 132)
	chk := newFirstChunk(e.src)
	for {
		trace_util_0.Count(_executor_00000, 137)
		err := Next(ctx, e.src, chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_executor_00000, 139)
			return err
		}
		trace_util_0.Count(_executor_00000, 138)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_executor_00000, 140)
			break
		}
	}
	trace_util_0.Count(_executor_00000, 133)
	return nil
}

// ShowSlowExec represents the executor of showing the slow queries.
// It is build from the "admin show slow" statement:
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	baseExecutor

	ShowSlow *ast.ShowSlow
	result   []*domain.SlowQueryInfo
	cursor   int
}

// Open implements the Executor Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 141)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 143)
		return err
	}

	trace_util_0.Count(_executor_00000, 142)
	dom := domain.GetDomain(e.ctx)
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowSlowExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 144)
	req.Reset()
	if e.cursor >= len(e.result) {
		trace_util_0.Count(_executor_00000, 147)
		return nil
	}

	trace_util_0.Count(_executor_00000, 145)
	for e.cursor < len(e.result) && req.NumRows() < e.maxChunkSize {
		trace_util_0.Count(_executor_00000, 148)
		slow := e.result[e.cursor]
		req.AppendString(0, slow.SQL)
		req.AppendTime(1, types.Time{
			Time: types.FromGoTime(slow.Start),
			Type: mysql.TypeTimestamp,
			Fsp:  types.MaxFsp,
		})
		req.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		req.AppendString(3, slow.Detail.String())
		if slow.Succ {
			trace_util_0.Count(_executor_00000, 151)
			req.AppendInt64(4, 1)
		} else {
			trace_util_0.Count(_executor_00000, 152)
			{
				req.AppendInt64(4, 0)
			}
		}
		trace_util_0.Count(_executor_00000, 149)
		req.AppendUint64(5, slow.ConnID)
		req.AppendUint64(6, slow.TxnTS)
		req.AppendString(7, slow.User)
		req.AppendString(8, slow.DB)
		req.AppendString(9, slow.TableIDs)
		req.AppendString(10, slow.IndexIDs)
		if slow.Internal {
			trace_util_0.Count(_executor_00000, 153)
			req.AppendInt64(11, 1)
		} else {
			trace_util_0.Count(_executor_00000, 154)
			{
				req.AppendInt64(11, 0)
			}
		}
		trace_util_0.Count(_executor_00000, 150)
		req.AppendString(12, slow.Digest)
		e.cursor++
	}
	trace_util_0.Count(_executor_00000, 146)
	return nil
}

// SelectLockExec represents a select lock executor.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Executor.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	baseExecutor

	Lock ast.SelectLockType
	keys []kv.Key
}

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 155)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 158)
		return err
	}

	trace_util_0.Count(_executor_00000, 156)
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	for id := range e.Schema().TblID2Handle {
		trace_util_0.Count(_executor_00000, 159)
		// This operation is only for schema validator check.
		txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
	}
	trace_util_0.Count(_executor_00000, 157)
	return nil
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 160)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 166)
		span1 := span.Tracer().StartSpan("selectLock.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_executor_00000, 161)
	req.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		trace_util_0.Count(_executor_00000, 167)
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	trace_util_0.Count(_executor_00000, 162)
	if len(e.Schema().TblID2Handle) == 0 || e.Lock != ast.SelectLockForUpdate {
		trace_util_0.Count(_executor_00000, 168)
		return nil
	}
	trace_util_0.Count(_executor_00000, 163)
	if req.NumRows() != 0 {
		trace_util_0.Count(_executor_00000, 169)
		iter := chunk.NewIterator4Chunk(req.Chunk)
		for id, cols := range e.Schema().TblID2Handle {
			trace_util_0.Count(_executor_00000, 171)
			for _, col := range cols {
				trace_util_0.Count(_executor_00000, 172)
				for row := iter.Begin(); row != iter.End(); row = iter.Next() {
					trace_util_0.Count(_executor_00000, 173)
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(id, row.GetInt64(col.Index)))
				}
			}
		}
		trace_util_0.Count(_executor_00000, 170)
		return nil
	}
	// Lock keys only once when finished fetching all results.
	trace_util_0.Count(_executor_00000, 164)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_executor_00000, 174)
		return err
	}
	trace_util_0.Count(_executor_00000, 165)
	forUpdateTS := e.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	return txn.LockKeys(ctx, forUpdateTS, e.keys...)
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 175)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 183)
		span1 := span.Tracer().StartSpan("limit.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 176)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 184)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 185)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 177)
	req.Reset()
	if e.cursor >= e.end {
		trace_util_0.Count(_executor_00000, 186)
		return nil
	}
	trace_util_0.Count(_executor_00000, 178)
	for !e.meetFirstBatch {
		trace_util_0.Count(_executor_00000, 187)
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.maxChunkSize)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(e.adjustRequiredRows(e.childResult)))
		if err != nil {
			trace_util_0.Count(_executor_00000, 191)
			return err
		}
		trace_util_0.Count(_executor_00000, 188)
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			trace_util_0.Count(_executor_00000, 192)
			return nil
		}
		trace_util_0.Count(_executor_00000, 189)
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			trace_util_0.Count(_executor_00000, 193)
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				trace_util_0.Count(_executor_00000, 196)
				end = e.end - e.cursor
			}
			trace_util_0.Count(_executor_00000, 194)
			e.cursor += end
			if begin == end {
				trace_util_0.Count(_executor_00000, 197)
				break
			}
			trace_util_0.Count(_executor_00000, 195)
			req.Append(e.childResult, int(begin), int(end))
			return nil
		}
		trace_util_0.Count(_executor_00000, 190)
		e.cursor += batchSize
	}
	trace_util_0.Count(_executor_00000, 179)
	e.adjustRequiredRows(req.Chunk)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		trace_util_0.Count(_executor_00000, 198)
		return err
	}
	trace_util_0.Count(_executor_00000, 180)
	batchSize := uint64(req.NumRows())
	// no more data.
	if batchSize == 0 {
		trace_util_0.Count(_executor_00000, 199)
		return nil
	}
	trace_util_0.Count(_executor_00000, 181)
	if e.cursor+batchSize > e.end {
		trace_util_0.Count(_executor_00000, 200)
		req.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	trace_util_0.Count(_executor_00000, 182)
	e.cursor += batchSize
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 201)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 203)
		return err
	}
	trace_util_0.Count(_executor_00000, 202)
	e.childResult = newFirstChunk(e.children[0])
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	trace_util_0.Count(_executor_00000, 204)
	e.childResult = nil
	return e.baseExecutor.Close()
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	trace_util_0.Count(_executor_00000, 205)
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		trace_util_0.Count(_executor_00000, 207)
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		trace_util_0.Count(_executor_00000, 208)
		{
			// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
			limitRequired = chk.RequiredRows()
		}
	}

	trace_util_0.Count(_executor_00000, 206)
	return chk.SetRequiredRows(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

func init() {
	trace_util_0.Count(_executor_00000, 209)
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubquery = func(p plannercore.PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) (rows [][]types.Datum, err error) {
		trace_util_0.Count(_executor_00000, 210)
		e := &executorBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			trace_util_0.Count(_executor_00000, 213)
			return rows, err
		}
		trace_util_0.Count(_executor_00000, 211)
		ctx := context.TODO()
		err = exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			trace_util_0.Count(_executor_00000, 214)
			return rows, err
		}
		trace_util_0.Count(_executor_00000, 212)
		chk := newFirstChunk(exec)
		for {
			trace_util_0.Count(_executor_00000, 215)
			err = Next(ctx, exec, chunk.NewRecordBatch(chk))
			if err != nil {
				trace_util_0.Count(_executor_00000, 219)
				return rows, err
			}
			trace_util_0.Count(_executor_00000, 216)
			if chk.NumRows() == 0 {
				trace_util_0.Count(_executor_00000, 220)
				return rows, nil
			}
			trace_util_0.Count(_executor_00000, 217)
			iter := chunk.NewIterator4Chunk(chk)
			for r := iter.Begin(); r != iter.End(); r = iter.Next() {
				trace_util_0.Count(_executor_00000, 221)
				row := r.GetDatumRow(retTypes(exec))
				rows = append(rows, row)
			}
			trace_util_0.Count(_executor_00000, 218)
			chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
		}
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 222)
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 223)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 228)
		span1 := span.Tracer().StartSpan("tableDual.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 224)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 229)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 230)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 225)
	req.Reset()
	if e.numReturned >= e.numDualRows {
		trace_util_0.Count(_executor_00000, 231)
		return nil
	}
	trace_util_0.Count(_executor_00000, 226)
	if e.Schema().Len() == 0 {
		trace_util_0.Count(_executor_00000, 232)
		req.SetNumVirtualRows(1)
	} else {
		trace_util_0.Count(_executor_00000, 233)
		{
			for i := range e.Schema().Columns {
				trace_util_0.Count(_executor_00000, 234)
				req.AppendNull(i)
			}
		}
	}
	trace_util_0.Count(_executor_00000, 227)
	e.numReturned = e.numDualRows
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	baseExecutor

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 235)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 238)
		return err
	}
	trace_util_0.Count(_executor_00000, 236)
	e.childResult = newFirstChunk(e.children[0])
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		trace_util_0.Count(_executor_00000, 239)
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	trace_util_0.Count(_executor_00000, 237)
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plannercore.Plan Close interface.
func (e *SelectionExec) Close() error {
	trace_util_0.Count(_executor_00000, 240)
	e.childResult = nil
	e.selected = nil
	return e.baseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 241)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 245)
		span1 := span.Tracer().StartSpan("selection.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 242)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 246)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 247)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 243)
	req.GrowAndReset(e.maxChunkSize)

	if !e.batched {
		trace_util_0.Count(_executor_00000, 248)
		return e.unBatchedNext(ctx, req.Chunk)
	}

	trace_util_0.Count(_executor_00000, 244)
	for {
		trace_util_0.Count(_executor_00000, 249)
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			trace_util_0.Count(_executor_00000, 254)
			if !e.selected[e.inputRow.Idx()] {
				trace_util_0.Count(_executor_00000, 257)
				continue
			}
			trace_util_0.Count(_executor_00000, 255)
			if req.IsFull() {
				trace_util_0.Count(_executor_00000, 258)
				return nil
			}
			trace_util_0.Count(_executor_00000, 256)
			req.AppendRow(e.inputRow)
		}
		trace_util_0.Count(_executor_00000, 250)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(e.childResult))
		if err != nil {
			trace_util_0.Count(_executor_00000, 259)
			return err
		}
		// no more data.
		trace_util_0.Count(_executor_00000, 251)
		if e.childResult.NumRows() == 0 {
			trace_util_0.Count(_executor_00000, 260)
			return nil
		}
		trace_util_0.Count(_executor_00000, 252)
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			trace_util_0.Count(_executor_00000, 261)
			return err
		}
		trace_util_0.Count(_executor_00000, 253)
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_executor_00000, 262)
	for {
		trace_util_0.Count(_executor_00000, 263)
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			trace_util_0.Count(_executor_00000, 266)
			selected, _, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				trace_util_0.Count(_executor_00000, 268)
				return err
			}
			trace_util_0.Count(_executor_00000, 267)
			if selected {
				trace_util_0.Count(_executor_00000, 269)
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		trace_util_0.Count(_executor_00000, 264)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(e.childResult))
		if err != nil {
			trace_util_0.Count(_executor_00000, 270)
			return err
		}
		trace_util_0.Count(_executor_00000, 265)
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			trace_util_0.Count(_executor_00000, 271)
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t                     table.Table
	seekHandle            int64
	iter                  kv.Iterator
	columns               []*model.ColumnInfo
	isVirtualTable        bool
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 272)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 278)
		span1 := span.Tracer().StartSpan("tableScan.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 273)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 279)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 280)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 274)
	req.GrowAndReset(e.maxChunkSize)
	if e.isVirtualTable {
		trace_util_0.Count(_executor_00000, 281)
		return e.nextChunk4InfoSchema(ctx, req.Chunk)
	}
	trace_util_0.Count(_executor_00000, 275)
	handle, found, err := e.nextHandle()
	if err != nil || !found {
		trace_util_0.Count(_executor_00000, 282)
		return err
	}

	trace_util_0.Count(_executor_00000, 276)
	mutableRow := chunk.MutRowFromTypes(retTypes(e))
	for req.NumRows() < req.Capacity() {
		trace_util_0.Count(_executor_00000, 283)
		row, err := e.getRow(handle)
		if err != nil {
			trace_util_0.Count(_executor_00000, 285)
			return err
		}
		trace_util_0.Count(_executor_00000, 284)
		e.seekHandle = handle + 1
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	trace_util_0.Count(_executor_00000, 277)
	return nil
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_executor_00000, 286)
	chk.GrowAndReset(e.maxChunkSize)
	if e.virtualTableChunkList == nil {
		trace_util_0.Count(_executor_00000, 289)
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			trace_util_0.Count(_executor_00000, 292)
			columns[i] = table.ToColumn(colInfo)
		}
		trace_util_0.Count(_executor_00000, 290)
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			trace_util_0.Count(_executor_00000, 293)
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		trace_util_0.Count(_executor_00000, 291)
		if err != nil {
			trace_util_0.Count(_executor_00000, 294)
			return err
		}
	}
	// no more data.
	trace_util_0.Count(_executor_00000, 287)
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		trace_util_0.Count(_executor_00000, 295)
		return nil
	}
	trace_util_0.Count(_executor_00000, 288)
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// nextHandle gets the unique handle for next row.
func (e *TableScanExec) nextHandle() (handle int64, found bool, err error) {
	trace_util_0.Count(_executor_00000, 296)
	for {
		trace_util_0.Count(_executor_00000, 297)
		handle, found, err = e.t.Seek(e.ctx, e.seekHandle)
		if err != nil || !found {
			trace_util_0.Count(_executor_00000, 299)
			return 0, false, err
		}
		trace_util_0.Count(_executor_00000, 298)
		return handle, true, nil
	}
}

func (e *TableScanExec) getRow(handle int64) ([]types.Datum, error) {
	trace_util_0.Count(_executor_00000, 300)
	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		trace_util_0.Count(_executor_00000, 303)
		columns[i] = table.ToColumn(v)
	}
	trace_util_0.Count(_executor_00000, 301)
	row, err := e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		trace_util_0.Count(_executor_00000, 304)
		return nil, err
	}

	trace_util_0.Count(_executor_00000, 302)
	return row, nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 305)
	e.iter = nil
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 306)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 308)
		return err
	}
	trace_util_0.Count(_executor_00000, 307)
	e.evaluated = false
	return nil
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 309)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 317)
		span1 := span.Tracer().StartSpan("maxOneRow.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 310)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 318)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 319)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 311)
	req.Reset()
	if e.evaluated {
		trace_util_0.Count(_executor_00000, 320)
		return nil
	}
	trace_util_0.Count(_executor_00000, 312)
	e.evaluated = true
	err := Next(ctx, e.children[0], req)
	if err != nil {
		trace_util_0.Count(_executor_00000, 321)
		return err
	}

	trace_util_0.Count(_executor_00000, 313)
	if num := req.NumRows(); num == 0 {
		trace_util_0.Count(_executor_00000, 322)
		for i := range e.schema.Columns {
			trace_util_0.Count(_executor_00000, 324)
			req.AppendNull(i)
		}
		trace_util_0.Count(_executor_00000, 323)
		return nil
	} else {
		trace_util_0.Count(_executor_00000, 325)
		if num != 1 {
			trace_util_0.Count(_executor_00000, 326)
			return errors.New("subquery returns more than 1 row")
		}
	}

	trace_util_0.Count(_executor_00000, 314)
	childChunk := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], chunk.NewRecordBatch(childChunk))
	if err != nil {
		trace_util_0.Count(_executor_00000, 327)
		return err
	}
	trace_util_0.Count(_executor_00000, 315)
	if childChunk.NumRows() != 0 {
		trace_util_0.Count(_executor_00000, 328)
		return errors.New("subquery returns more than 1 row")
	}

	trace_util_0.Count(_executor_00000, 316)
	return nil
}

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionExec struct {
	baseExecutor

	stopFetchData atomic.Value
	wg            sync.WaitGroup

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult
	initialized   bool

	childrenResults []*chunk.Chunk
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished() {
	trace_util_0.Count(_executor_00000, 329)
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	trace_util_0.Count(_executor_00000, 330)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_executor_00000, 333)
		return err
	}
	trace_util_0.Count(_executor_00000, 331)
	for _, child := range e.children {
		trace_util_0.Count(_executor_00000, 334)
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}
	trace_util_0.Count(_executor_00000, 332)
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	trace_util_0.Count(_executor_00000, 335)
	e.resultPool = make(chan *unionWorkerResult, len(e.children))
	e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
	for i := range e.children {
		trace_util_0.Count(_executor_00000, 337)
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.childrenResults[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	trace_util_0.Count(_executor_00000, 336)
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, childID int) {
	trace_util_0.Count(_executor_00000, 338)
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
	}
	defer func() {
		trace_util_0.Count(_executor_00000, 340)
		if r := recover(); r != nil {
			trace_util_0.Count(_executor_00000, 342)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("resultPuller panicked", zap.String("stack", string(buf)))
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		trace_util_0.Count(_executor_00000, 341)
		e.wg.Done()
	}()
	trace_util_0.Count(_executor_00000, 339)
	for {
		trace_util_0.Count(_executor_00000, 343)
		if e.stopFetchData.Load().(bool) {
			trace_util_0.Count(_executor_00000, 347)
			return
		}
		trace_util_0.Count(_executor_00000, 344)
		select {
		case <-e.finished:
			trace_util_0.Count(_executor_00000, 348)
			return
		case result.chk = <-e.resourcePools[childID]:
			trace_util_0.Count(_executor_00000, 349)
		}
		trace_util_0.Count(_executor_00000, 345)
		result.err = Next(ctx, e.children[childID], chunk.NewRecordBatch(result.chk))
		if result.err == nil && result.chk.NumRows() == 0 {
			trace_util_0.Count(_executor_00000, 350)
			return
		}
		trace_util_0.Count(_executor_00000, 346)
		e.resultPool <- result
		if result.err != nil {
			trace_util_0.Count(_executor_00000, 351)
			e.stopFetchData.Store(true)
			return
		}
	}
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_executor_00000, 352)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_executor_00000, 358)
		span1 := span.Tracer().StartSpan("union.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_executor_00000, 353)
	if e.runtimeStats != nil {
		trace_util_0.Count(_executor_00000, 359)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_executor_00000, 360)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_executor_00000, 354)
	req.GrowAndReset(e.maxChunkSize)
	if !e.initialized {
		trace_util_0.Count(_executor_00000, 361)
		e.initialize(ctx)
		e.initialized = true
	}
	trace_util_0.Count(_executor_00000, 355)
	result, ok := <-e.resultPool
	if !ok {
		trace_util_0.Count(_executor_00000, 362)
		return nil
	}
	trace_util_0.Count(_executor_00000, 356)
	if result.err != nil {
		trace_util_0.Count(_executor_00000, 363)
		return errors.Trace(result.err)
	}

	trace_util_0.Count(_executor_00000, 357)
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	trace_util_0.Count(_executor_00000, 364)
	close(e.finished)
	e.childrenResults = nil
	if e.resultPool != nil {
		trace_util_0.Count(_executor_00000, 366)
		for range e.resultPool {
			trace_util_0.Count(_executor_00000, 367)
		}
	}
	trace_util_0.Count(_executor_00000, 365)
	e.resourcePools = nil
	return e.baseExecutor.Close()
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	trace_util_0.Count(_executor_00000, 368)
	vars := ctx.GetSessionVars()
	sc := &stmtctx.StatementContext{
		TimeZone:   vars.Location(),
		MemTracker: memory.NewTracker(stringutil.MemoizeStr(s.Text), vars.MemQuotaQuery),
	}
	switch config.GetGlobalConfig().OOMAction {
	case config.OOMActionCancel:
		trace_util_0.Count(_executor_00000, 377)
		action := &memory.PanicOnExceed{ConnID: ctx.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	case config.OOMActionLog:
		trace_util_0.Count(_executor_00000, 378)
		fallthrough
	default:
		trace_util_0.Count(_executor_00000, 379)
		action := &memory.LogOnExceed{ConnID: ctx.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	}

	trace_util_0.Count(_executor_00000, 369)
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		trace_util_0.Count(_executor_00000, 380)
		s, err = getPreparedStmt(execStmt, vars)
		if err != nil {
			trace_util_0.Count(_executor_00000, 381)
			return
		}
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.
	trace_util_0.Count(_executor_00000, 370)
	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		trace_util_0.Count(_executor_00000, 382)
		sc.InUpdateStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.DeleteStmt:
		trace_util_0.Count(_executor_00000, 383)
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		trace_util_0.Count(_executor_00000, 384)
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning or BadNullAsWarning,
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		trace_util_0.Count(_executor_00000, 385)
		// Make sure the sql_mode is strict when checking column default value.
	case *ast.LoadDataStmt:
		trace_util_0.Count(_executor_00000, 386)
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		sc.TruncateAsWarning = !vars.StrictSQLMode
		sc.InLoadDataStmt = true
	case *ast.SelectStmt:
		trace_util_0.Count(_executor_00000, 387)
		sc.InSelectStmt = true

		// see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
		// said "For statements such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if opts := stmt.SelectStmtOpts; opts != nil {
			trace_util_0.Count(_executor_00000, 393)
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		trace_util_0.Count(_executor_00000, 388)
		sc.PadCharToFullLength = ctx.GetSessionVars().SQLMode.HasPadCharToFullLengthMode()
	case *ast.ExplainStmt:
		trace_util_0.Count(_executor_00000, 389)
		sc.InExplainStmt = true
	case *ast.ShowStmt:
		trace_util_0.Count(_executor_00000, 390)
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors {
			trace_util_0.Count(_executor_00000, 394)
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		trace_util_0.Count(_executor_00000, 391)
		sc.IgnoreTruncate = false
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	default:
		trace_util_0.Count(_executor_00000, 392)
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	}
	trace_util_0.Count(_executor_00000, 371)
	vars.PreparedParams = vars.PreparedParams[:0]
	if !vars.InRestrictedSQL {
		trace_util_0.Count(_executor_00000, 395)
		if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
			trace_util_0.Count(_executor_00000, 396)
			sc.Priority = priority
		}
	}
	trace_util_0.Count(_executor_00000, 372)
	if vars.StmtCtx.LastInsertID > 0 {
		trace_util_0.Count(_executor_00000, 397)
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		trace_util_0.Count(_executor_00000, 398)
		{
			sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
		}
	}
	trace_util_0.Count(_executor_00000, 373)
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt {
		trace_util_0.Count(_executor_00000, 399)
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else {
		trace_util_0.Count(_executor_00000, 400)
		if vars.StmtCtx.InSelectStmt {
			trace_util_0.Count(_executor_00000, 401)
			sc.PrevAffectedRows = -1
		}
	}
	trace_util_0.Count(_executor_00000, 374)
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	err = vars.SetSystemVar("warning_count", warnCount)
	if err != nil {
		trace_util_0.Count(_executor_00000, 402)
		return err
	}
	trace_util_0.Count(_executor_00000, 375)
	err = vars.SetSystemVar("error_count", errCount)
	if err != nil {
		trace_util_0.Count(_executor_00000, 403)
		return err
	}
	// execute missed stmtID uses empty sql
	trace_util_0.Count(_executor_00000, 376)
	sc.OriginalSQL = s.Text()
	vars.StmtCtx = sc
	return
}

var _executor_00000 = "executor/executor.go"
