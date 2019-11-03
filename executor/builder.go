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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	executorCounterMergeJoinExec       = metrics.ExecutorCounter.WithLabelValues("MergeJoinExec")
	executorCountHashJoinExec          = metrics.ExecutorCounter.WithLabelValues("HashJoinExec")
	executorCounterHashAggExec         = metrics.ExecutorCounter.WithLabelValues("HashAggExec")
	executorStreamAggExec              = metrics.ExecutorCounter.WithLabelValues("StreamAggExec")
	executorCounterSortExec            = metrics.ExecutorCounter.WithLabelValues("SortExec")
	executorCounterTopNExec            = metrics.ExecutorCounter.WithLabelValues("TopNExec")
	executorCounterNestedLoopApplyExec = metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec")
	executorCounterIndexLookUpJoin     = metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin")
	executorCounterIndexLookUpExecutor = metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor")
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	startTS uint64 // cached when the first time getStartTS() is called
	// err is set when there is error happened during Executor building process.
	err               error
	isSelectForUpdate bool
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema) *executorBuilder {
	trace_util_0.Count(_builder_00000, 0)
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

// MockPhysicalPlan is used to return a specified executor in when build.
// It is mainly used for testing.
type MockPhysicalPlan interface {
	plannercore.PhysicalPlan
	GetExecutor() Executor
}

func (b *executorBuilder) build(p plannercore.Plan) Executor {
	trace_util_0.Count(_builder_00000, 1)
	switch v := p.(type) {
	case nil:
		trace_util_0.Count(_builder_00000, 2)
		return nil
	case *plannercore.Change:
		trace_util_0.Count(_builder_00000, 3)
		return b.buildChange(v)
	case *plannercore.CheckTable:
		trace_util_0.Count(_builder_00000, 4)
		return b.buildCheckTable(v)
	case *plannercore.CheckIndex:
		trace_util_0.Count(_builder_00000, 5)
		return b.buildCheckIndex(v)
	case *plannercore.RecoverIndex:
		trace_util_0.Count(_builder_00000, 6)
		return b.buildRecoverIndex(v)
	case *plannercore.CleanupIndex:
		trace_util_0.Count(_builder_00000, 7)
		return b.buildCleanupIndex(v)
	case *plannercore.CheckIndexRange:
		trace_util_0.Count(_builder_00000, 8)
		return b.buildCheckIndexRange(v)
	case *plannercore.ChecksumTable:
		trace_util_0.Count(_builder_00000, 9)
		return b.buildChecksumTable(v)
	case *plannercore.ReloadExprPushdownBlacklist:
		trace_util_0.Count(_builder_00000, 10)
		return b.buildReloadExprPushdownBlacklist(v)
	case *plannercore.DDL:
		trace_util_0.Count(_builder_00000, 11)
		return b.buildDDL(v)
	case *plannercore.Deallocate:
		trace_util_0.Count(_builder_00000, 12)
		return b.buildDeallocate(v)
	case *plannercore.Delete:
		trace_util_0.Count(_builder_00000, 13)
		return b.buildDelete(v)
	case *plannercore.Execute:
		trace_util_0.Count(_builder_00000, 14)
		return b.buildExecute(v)
	case *plannercore.Trace:
		trace_util_0.Count(_builder_00000, 15)
		return b.buildTrace(v)
	case *plannercore.Explain:
		trace_util_0.Count(_builder_00000, 16)
		return b.buildExplain(v)
	case *plannercore.PointGetPlan:
		trace_util_0.Count(_builder_00000, 17)
		return b.buildPointGet(v)
	case *plannercore.Insert:
		trace_util_0.Count(_builder_00000, 18)
		return b.buildInsert(v)
	case *plannercore.LoadData:
		trace_util_0.Count(_builder_00000, 19)
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		trace_util_0.Count(_builder_00000, 20)
		return b.buildLoadStats(v)
	case *plannercore.PhysicalLimit:
		trace_util_0.Count(_builder_00000, 21)
		return b.buildLimit(v)
	case *plannercore.Prepare:
		trace_util_0.Count(_builder_00000, 22)
		return b.buildPrepare(v)
	case *plannercore.PhysicalLock:
		trace_util_0.Count(_builder_00000, 23)
		return b.buildSelectLock(v)
	case *plannercore.CancelDDLJobs:
		trace_util_0.Count(_builder_00000, 24)
		return b.buildCancelDDLJobs(v)
	case *plannercore.ShowNextRowID:
		trace_util_0.Count(_builder_00000, 25)
		return b.buildShowNextRowID(v)
	case *plannercore.ShowDDL:
		trace_util_0.Count(_builder_00000, 26)
		return b.buildShowDDL(v)
	case *plannercore.ShowDDLJobs:
		trace_util_0.Count(_builder_00000, 27)
		return b.buildShowDDLJobs(v)
	case *plannercore.ShowDDLJobQueries:
		trace_util_0.Count(_builder_00000, 28)
		return b.buildShowDDLJobQueries(v)
	case *plannercore.ShowSlow:
		trace_util_0.Count(_builder_00000, 29)
		return b.buildShowSlow(v)
	case *plannercore.Show:
		trace_util_0.Count(_builder_00000, 30)
		return b.buildShow(v)
	case *plannercore.Simple:
		trace_util_0.Count(_builder_00000, 31)
		return b.buildSimple(v)
	case *plannercore.Set:
		trace_util_0.Count(_builder_00000, 32)
		return b.buildSet(v)
	case *plannercore.PhysicalSort:
		trace_util_0.Count(_builder_00000, 33)
		return b.buildSort(v)
	case *plannercore.PhysicalTopN:
		trace_util_0.Count(_builder_00000, 34)
		return b.buildTopN(v)
	case *plannercore.PhysicalUnionAll:
		trace_util_0.Count(_builder_00000, 35)
		return b.buildUnionAll(v)
	case *plannercore.Update:
		trace_util_0.Count(_builder_00000, 36)
		return b.buildUpdate(v)
	case *plannercore.PhysicalUnionScan:
		trace_util_0.Count(_builder_00000, 37)
		return b.buildUnionScanExec(v)
	case *plannercore.PhysicalHashJoin:
		trace_util_0.Count(_builder_00000, 38)
		return b.buildHashJoin(v)
	case *plannercore.PhysicalMergeJoin:
		trace_util_0.Count(_builder_00000, 39)
		return b.buildMergeJoin(v)
	case *plannercore.PhysicalIndexJoin:
		trace_util_0.Count(_builder_00000, 40)
		return b.buildIndexLookUpJoin(v)
	case *plannercore.PhysicalSelection:
		trace_util_0.Count(_builder_00000, 41)
		return b.buildSelection(v)
	case *plannercore.PhysicalHashAgg:
		trace_util_0.Count(_builder_00000, 42)
		return b.buildHashAgg(v)
	case *plannercore.PhysicalStreamAgg:
		trace_util_0.Count(_builder_00000, 43)
		return b.buildStreamAgg(v)
	case *plannercore.PhysicalProjection:
		trace_util_0.Count(_builder_00000, 44)
		return b.buildProjection(v)
	case *plannercore.PhysicalMemTable:
		trace_util_0.Count(_builder_00000, 45)
		return b.buildMemTable(v)
	case *plannercore.PhysicalTableDual:
		trace_util_0.Count(_builder_00000, 46)
		return b.buildTableDual(v)
	case *plannercore.PhysicalApply:
		trace_util_0.Count(_builder_00000, 47)
		return b.buildApply(v)
	case *plannercore.PhysicalMaxOneRow:
		trace_util_0.Count(_builder_00000, 48)
		return b.buildMaxOneRow(v)
	case *plannercore.Analyze:
		trace_util_0.Count(_builder_00000, 49)
		return b.buildAnalyze(v)
	case *plannercore.PhysicalTableReader:
		trace_util_0.Count(_builder_00000, 50)
		return b.buildTableReader(v)
	case *plannercore.PhysicalIndexReader:
		trace_util_0.Count(_builder_00000, 51)
		return b.buildIndexReader(v)
	case *plannercore.PhysicalIndexLookUpReader:
		trace_util_0.Count(_builder_00000, 52)
		return b.buildIndexLookUpReader(v)
	case *plannercore.PhysicalWindow:
		trace_util_0.Count(_builder_00000, 53)
		return b.buildWindow(v)
	case *plannercore.SQLBindPlan:
		trace_util_0.Count(_builder_00000, 54)
		return b.buildSQLBindExec(v)
	case *plannercore.SplitRegion:
		trace_util_0.Count(_builder_00000, 55)
		return b.buildSplitRegion(v)
	default:
		trace_util_0.Count(_builder_00000, 56)
		if mp, ok := p.(MockPhysicalPlan); ok {
			trace_util_0.Count(_builder_00000, 58)
			return mp.GetExecutor()
		}

		trace_util_0.Count(_builder_00000, 57)
		b.err = ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildCancelDDLJobs(v *plannercore.CancelDDLJobs) Executor {
	trace_util_0.Count(_builder_00000, 59)
	e := &CancelDDLJobsExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_builder_00000, 62)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 60)
	e.errs, b.err = admin.CancelJobs(txn, e.jobIDs)
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 63)
		return nil
	}
	trace_util_0.Count(_builder_00000, 61)
	return e
}

func (b *executorBuilder) buildChange(v *plannercore.Change) Executor {
	trace_util_0.Count(_builder_00000, 64)
	return &ChangeExec{
		ChangeStmt: v.ChangeStmt,
	}
}

func (b *executorBuilder) buildShowNextRowID(v *plannercore.ShowNextRowID) Executor {
	trace_util_0.Count(_builder_00000, 65)
	e := &ShowNextRowIDExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tblName:      v.TableName,
	}
	return e
}

func (b *executorBuilder) buildShowDDL(v *plannercore.ShowDDL) Executor {
	trace_util_0.Count(_builder_00000, 66)
	// We get DDLInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DDLInfo.
	e := &ShowDDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}

	var err error
	ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.ddlOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		trace_util_0.Count(_builder_00000, 70)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 67)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_builder_00000, 71)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 68)
	ddlInfo, err := admin.GetDDLInfo(txn)
	if err != nil {
		trace_util_0.Count(_builder_00000, 72)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 69)
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plannercore.ShowDDLJobs) Executor {
	trace_util_0.Count(_builder_00000, 73)
	e := &ShowDDLJobsExec{
		jobNumber:    v.JobNumber,
		is:           b.is,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	return e
}

func (b *executorBuilder) buildShowDDLJobQueries(v *plannercore.ShowDDLJobQueries) Executor {
	trace_util_0.Count(_builder_00000, 74)
	e := &ShowDDLJobQueriesExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildShowSlow(v *plannercore.ShowSlow) Executor {
	trace_util_0.Count(_builder_00000, 75)
	e := &ShowSlowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

func (b *executorBuilder) buildCheckIndex(v *plannercore.CheckIndex) Executor {
	trace_util_0.Count(_builder_00000, 76)
	readerExec, err := buildNoRangeIndexLookUpReader(b, v.IndexLookUpReader)
	if err != nil {
		trace_util_0.Count(_builder_00000, 78)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 77)
	readerExec.ranges = ranger.FullRange()
	readerExec.isCheckOp = true

	e := &CheckIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dbName:       v.DBName,
		tableName:    readerExec.table.Meta().Name.L,
		idxName:      v.IdxName,
		is:           b.is,
		src:          readerExec,
	}
	return e
}

func (b *executorBuilder) buildCheckTable(v *plannercore.CheckTable) Executor {
	trace_util_0.Count(_builder_00000, 79)
	e := &CheckTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tables:       v.Tables,
		is:           b.is,
		genExprs:     v.GenExprs,
	}
	return e
}

func buildRecoverIndexCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	trace_util_0.Count(_builder_00000, 80)
	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	for _, idxCol := range indexInfo.Columns {
		trace_util_0.Count(_builder_00000, 82)
		columns = append(columns, tblInfo.Columns[idxCol.Offset])
	}

	trace_util_0.Count(_builder_00000, 81)
	handleOffset := len(columns)
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: handleOffset,
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	return columns
}

func (b *executorBuilder) buildRecoverIndex(v *plannercore.RecoverIndex) Executor {
	trace_util_0.Count(_builder_00000, 83)
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
	if err != nil {
		trace_util_0.Count(_builder_00000, 87)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 84)
	idxName := strings.ToLower(v.IndexName)
	indices := t.WritableIndices()
	var index table.Index
	for _, idx := range indices {
		trace_util_0.Count(_builder_00000, 88)
		if idxName == idx.Meta().Name.L {
			trace_util_0.Count(_builder_00000, 89)
			index = idx
			break
		}
	}

	trace_util_0.Count(_builder_00000, 85)
	if index == nil {
		trace_util_0.Count(_builder_00000, 90)
		b.err = errors.Errorf("index `%v` is not found in table `%v`.", v.IndexName, v.Table.Name.O)
		return nil
	}
	trace_util_0.Count(_builder_00000, 86)
	e := &RecoverIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		columns:      buildRecoverIndexCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
	}
	return e
}

func buildCleanupIndexCols(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	trace_util_0.Count(_builder_00000, 91)
	columns := make([]*model.ColumnInfo, 0, len(indexInfo.Columns)+1)
	for _, idxCol := range indexInfo.Columns {
		trace_util_0.Count(_builder_00000, 93)
		columns = append(columns, tblInfo.Columns[idxCol.Offset])
	}
	trace_util_0.Count(_builder_00000, 92)
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: len(tblInfo.Columns),
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	return columns
}

func (b *executorBuilder) buildCleanupIndex(v *plannercore.CleanupIndex) Executor {
	trace_util_0.Count(_builder_00000, 94)
	tblInfo := v.Table.TableInfo
	t, err := b.is.TableByName(v.Table.Schema, tblInfo.Name)
	if err != nil {
		trace_util_0.Count(_builder_00000, 98)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 95)
	idxName := strings.ToLower(v.IndexName)
	var index table.Index
	for _, idx := range t.Indices() {
		trace_util_0.Count(_builder_00000, 99)
		if idx.Meta().State != model.StatePublic {
			trace_util_0.Count(_builder_00000, 101)
			continue
		}
		trace_util_0.Count(_builder_00000, 100)
		if idxName == idx.Meta().Name.L {
			trace_util_0.Count(_builder_00000, 102)
			index = idx
			break
		}
	}

	trace_util_0.Count(_builder_00000, 96)
	if index == nil {
		trace_util_0.Count(_builder_00000, 103)
		b.err = errors.Errorf("index `%v` is not found in table `%v`.", v.IndexName, v.Table.Name.O)
		return nil
	}
	trace_util_0.Count(_builder_00000, 97)
	e := &CleanupIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		idxCols:      buildCleanupIndexCols(tblInfo, index.Meta()),
		index:        index,
		table:        t,
		batchSize:    20000,
	}
	return e
}

func (b *executorBuilder) buildCheckIndexRange(v *plannercore.CheckIndexRange) Executor {
	trace_util_0.Count(_builder_00000, 104)
	tb, err := b.is.TableByName(v.Table.Schema, v.Table.Name)
	if err != nil {
		trace_util_0.Count(_builder_00000, 107)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 105)
	e := &CheckIndexRangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		handleRanges: v.HandleRanges,
		table:        tb.Meta(),
		is:           b.is,
	}
	idxName := strings.ToLower(v.IndexName)
	for _, idx := range tb.Indices() {
		trace_util_0.Count(_builder_00000, 108)
		if idx.Meta().Name.L == idxName {
			trace_util_0.Count(_builder_00000, 109)
			e.index = idx.Meta()
			e.startKey = make([]types.Datum, len(e.index.Columns))
			break
		}
	}
	trace_util_0.Count(_builder_00000, 106)
	return e
}

func (b *executorBuilder) buildChecksumTable(v *plannercore.ChecksumTable) Executor {
	trace_util_0.Count(_builder_00000, 110)
	e := &ChecksumTableExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tables:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs, err := b.getStartTS()
	if err != nil {
		trace_util_0.Count(_builder_00000, 113)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 111)
	for _, t := range v.Tables {
		trace_util_0.Count(_builder_00000, 114)
		e.tables[t.TableInfo.ID] = newChecksumContext(t.DBInfo, t.TableInfo, startTs)
	}
	trace_util_0.Count(_builder_00000, 112)
	return e
}

func (b *executorBuilder) buildReloadExprPushdownBlacklist(v *plannercore.ReloadExprPushdownBlacklist) Executor {
	trace_util_0.Count(_builder_00000, 115)
	return &ReloadExprPushdownBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildDeallocate(v *plannercore.Deallocate) Executor {
	trace_util_0.Count(_builder_00000, 116)
	base := newBaseExecutor(b.ctx, nil, v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &DeallocateExec{
		baseExecutor: base,
		Name:         v.Name,
	}
	return e
}

func (b *executorBuilder) buildSelectLock(v *plannercore.PhysicalLock) Executor {
	trace_util_0.Count(_builder_00000, 117)
	b.isSelectForUpdate = true
	// Build 'select for update' using the 'for update' ts.
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()

	src := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 120)
		return nil
	}
	trace_util_0.Count(_builder_00000, 118)
	if !b.ctx.GetSessionVars().InTxn() {
		trace_util_0.Count(_builder_00000, 121)
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	trace_util_0.Count(_builder_00000, 119)
	e := &SelectLockExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		Lock:         v.Lock,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) Executor {
	trace_util_0.Count(_builder_00000, 122)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 124)
		return nil
	}
	trace_util_0.Count(_builder_00000, 123)
	n := int(mathutil.MinUint64(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) Executor {
	trace_util_0.Count(_builder_00000, 125)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           b.is,
		name:         v.Name,
		sqlText:      v.SQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) Executor {
	trace_util_0.Count(_builder_00000, 126)
	e := &ExecuteExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.UsingVars,
		id:           v.ExecID,
		stmt:         v.Stmt,
		plan:         v.Plan,
	}
	return e
}

func (b *executorBuilder) buildShow(v *plannercore.Show) Executor {
	trace_util_0.Count(_builder_00000, 127)
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Column:       v.Column,
		User:         v.User,
		Roles:        v.Roles,
		IfNotExists:  v.IfNotExists,
		Flag:         v.Flag,
		Full:         v.Full,
		GlobalScope:  v.GlobalScope,
		is:           b.is,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		trace_util_0.Count(_builder_00000, 131)
		// The input is a "show grants" statement, fulfill the user and roles field.
		// Note: "show grants" result are different from "show grants for current_user",
		// The former determine privileges with roles, while the later doesn't.
		vars := e.ctx.GetSessionVars()
		e.User = vars.User
		e.Roles = vars.ActiveRoles
	}
	trace_util_0.Count(_builder_00000, 128)
	if e.Tp == ast.ShowMasterStatus {
		trace_util_0.Count(_builder_00000, 132)
		// show master status need start ts.
		if _, err := e.ctx.Txn(true); err != nil {
			trace_util_0.Count(_builder_00000, 133)
			b.err = err
		}
	}
	trace_util_0.Count(_builder_00000, 129)
	if len(v.Conditions) == 0 {
		trace_util_0.Count(_builder_00000, 134)
		return e
	}
	trace_util_0.Count(_builder_00000, 130)
	sel := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), e),
		filters:      v.Conditions,
	}
	return sel
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) Executor {
	trace_util_0.Count(_builder_00000, 135)
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		trace_util_0.Count(_builder_00000, 137)
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		trace_util_0.Count(_builder_00000, 138)
		return b.buildRevoke(s)
	}
	trace_util_0.Count(_builder_00000, 136)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) Executor {
	trace_util_0.Count(_builder_00000, 139)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) Executor {
	trace_util_0.Count(_builder_00000, 140)
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 145)
		return nil
	}
	trace_util_0.Count(_builder_00000, 141)
	var baseExec baseExecutor
	if selectExec != nil {
		trace_util_0.Count(_builder_00000, 146)
		baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID(), selectExec)
	} else {
		trace_util_0.Count(_builder_00000, 147)
		{
			baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID())
		}
	}
	trace_util_0.Count(_builder_00000, 142)
	baseExec.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseExecutor: baseExec,
		Table:        v.Table,
		Columns:      v.Columns,
		Lists:        v.Lists,
		SetList:      v.SetList,
		GenColumns:   v.GenCols.Columns,
		GenExprs:     v.GenCols.Exprs,
		hasRefCols:   v.NeedFillDefaultValue,
		SelectExec:   selectExec,
	}
	err := ivs.initInsertColumns()
	if err != nil {
		trace_util_0.Count(_builder_00000, 148)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 143)
	if v.IsReplace {
		trace_util_0.Count(_builder_00000, 149)
		return b.buildReplace(ivs)
	}
	trace_util_0.Count(_builder_00000, 144)
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenCols.OnDuplicates...),
	}
	return insert
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) Executor {
	trace_util_0.Count(_builder_00000, 150)
	tbl, ok := b.is.TableByID(v.Table.TableInfo.ID)
	if !ok {
		trace_util_0.Count(_builder_00000, 153)
		b.err = errors.Errorf("Can not get table %d", v.Table.TableInfo.ID)
		return nil
	}
	trace_util_0.Count(_builder_00000, 151)
	insertVal := &InsertValues{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		Table:        tbl,
		Columns:      v.Columns,
		GenColumns:   v.GenCols.Columns,
		GenExprs:     v.GenCols.Exprs,
	}
	err := insertVal.initInsertColumns()
	if err != nil {
		trace_util_0.Count(_builder_00000, 154)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 152)
	loadDataExec := &LoadDataExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		IsLocal:      v.IsLocal,
		OnDuplicate:  v.OnDuplicate,
		loadDataInfo: &LoadDataInfo{
			row:          make([]types.Datum, len(insertVal.insertColumns)),
			InsertValues: insertVal,
			Path:         v.Path,
			Table:        tbl,
			FieldsInfo:   v.FieldsInfo,
			LinesInfo:    v.LinesInfo,
			IgnoreLines:  v.IgnoreLines,
			Ctx:          b.ctx,
		},
	}

	return loadDataExec
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) Executor {
	trace_util_0.Count(_builder_00000, 155)
	e := &LoadStatsExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ExplainID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	trace_util_0.Count(_builder_00000, 156)
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

var (
	grantStmtLabel  fmt.Stringer = stringutil.StringerStr("GrantStmt")
	revokeStmtLabel fmt.Stringer = stringutil.StringerStr("RevokeStmt")
)

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	trace_util_0.Count(_builder_00000, 157)
	e := &GrantExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, grantStmtLabel),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	trace_util_0.Count(_builder_00000, 158)
	e := &RevokeExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, revokeStmtLabel),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) Executor {
	trace_util_0.Count(_builder_00000, 159)
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) Executor {
	trace_util_0.Count(_builder_00000, 160)
	return &TraceExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmtNode:     v.StmtNode,
		builder:      b,
		format:       v.Format,
	}
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) Executor {
	trace_util_0.Count(_builder_00000, 161)
	explainExec := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		explain:      v,
	}
	if v.Analyze {
		trace_util_0.Count(_builder_00000, 163)
		b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl()
		explainExec.analyzeExec = b.build(v.ExecPlan)
	}
	trace_util_0.Count(_builder_00000, 162)
	return explainExec
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) Executor {
	trace_util_0.Count(_builder_00000, 164)
	reader := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 167)
		return nil
	}
	trace_util_0.Count(_builder_00000, 165)
	us, err := b.buildUnionScanFromReader(reader, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 168)
		b.err = err
		return nil
	}
	trace_util_0.Count(_builder_00000, 166)
	return us
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader Executor, v *plannercore.PhysicalUnionScan) (Executor, error) {
	trace_util_0.Count(_builder_00000, 169)
	var err error
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), reader)}
	// Get the handle column index of the below plannercore.
	// We can guarantee that there must be only one col in the map.
	for _, cols := range v.Children()[0].Schema().TblID2Handle {
		trace_util_0.Count(_builder_00000, 173)
		us.belowHandleIndex = cols[0].Index
	}
	trace_util_0.Count(_builder_00000, 170)
	switch x := reader.(type) {
	case *TableReaderExecutor:
		trace_util_0.Count(_builder_00000, 174)
		us.desc = x.desc
		// Union scan can only be in a write transaction, so DirtyDB should has non-nil value now, thus
		// GetDirtyDB() is safe here. If this table has been modified in the transaction, non-nil DirtyTable
		// can be found in DirtyDB now, so GetDirtyTable is safe; if this table has not been modified in the
		// transaction, empty DirtyTable would be inserted into DirtyDB, it does not matter when multiple
		// goroutines write empty DirtyTable to DirtyDB for this table concurrently. Thus we don't use lock
		// to synchronize here.
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows(x.table)
	case *IndexReaderExecutor:
		trace_util_0.Count(_builder_00000, 175)
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			trace_util_0.Count(_builder_00000, 180)
			for i, col := range x.schema.Columns {
				trace_util_0.Count(_builder_00000, 181)
				if col.ColName.L == ic.Name.L {
					trace_util_0.Count(_builder_00000, 182)
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		trace_util_0.Count(_builder_00000, 176)
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows(x.table)
	case *IndexLookUpExecutor:
		trace_util_0.Count(_builder_00000, 177)
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			trace_util_0.Count(_builder_00000, 183)
			for i, col := range x.schema.Columns {
				trace_util_0.Count(_builder_00000, 184)
				if col.ColName.L == ic.Name.L {
					trace_util_0.Count(_builder_00000, 185)
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		trace_util_0.Count(_builder_00000, 178)
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		err = us.buildAndSortAddedRows(x.table)
	default:
		trace_util_0.Count(_builder_00000, 179)
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return reader, nil
	}
	trace_util_0.Count(_builder_00000, 171)
	if err != nil {
		trace_util_0.Count(_builder_00000, 186)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 172)
	return us, nil
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) Executor {
	trace_util_0.Count(_builder_00000, 187)
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 193)
		return nil
	}

	trace_util_0.Count(_builder_00000, 188)
	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 194)
		return nil
	}

	trace_util_0.Count(_builder_00000, 189)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		trace_util_0.Count(_builder_00000, 195)
		if v.JoinType == plannercore.RightOuterJoin {
			trace_util_0.Count(_builder_00000, 196)
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			trace_util_0.Count(_builder_00000, 197)
			{
				defaultValues = make([]types.Datum, rightExec.Schema().Len())
			}
		}
	}

	trace_util_0.Count(_builder_00000, 190)
	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == plannercore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			retTypes(leftExec),
			retTypes(rightExec),
		),
		isOuterJoin: v.JoinType.IsOuterJoin(),
	}

	leftKeys := v.LeftKeys
	rightKeys := v.RightKeys

	e.outerIdx = 0
	innerFilter := v.RightConditions

	e.innerTable = &mergeJoinInnerTable{
		reader:   rightExec,
		joinKeys: rightKeys,
	}

	e.outerTable = &mergeJoinOuterTable{
		reader: leftExec,
		filter: v.LeftConditions,
		keys:   leftKeys,
	}

	if v.JoinType == plannercore.RightOuterJoin {
		trace_util_0.Count(_builder_00000, 198)
		e.outerIdx = 1
		e.outerTable.reader = rightExec
		e.outerTable.filter = v.RightConditions
		e.outerTable.keys = rightKeys

		innerFilter = v.LeftConditions
		e.innerTable.reader = leftExec
		e.innerTable.joinKeys = leftKeys
	}

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	trace_util_0.Count(_builder_00000, 191)
	if len(innerFilter) != 0 {
		trace_util_0.Count(_builder_00000, 199)
		b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	trace_util_0.Count(_builder_00000, 192)
	executorCounterMergeJoinExec.Inc()
	return e
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) Executor {
	trace_util_0.Count(_builder_00000, 200)
	leftHashKey := make([]*expression.Column, 0, len(v.EqualConditions))
	rightHashKey := make([]*expression.Column, 0, len(v.EqualConditions))
	for _, eqCond := range v.EqualConditions {
		trace_util_0.Count(_builder_00000, 206)
		ln, _ := eqCond.GetArgs()[0].(*expression.Column)
		rn, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftHashKey = append(leftHashKey, ln)
		rightHashKey = append(rightHashKey, rn)
	}

	trace_util_0.Count(_builder_00000, 201)
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 207)
		return nil
	}

	trace_util_0.Count(_builder_00000, 202)
	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 208)
		return nil
	}

	trace_util_0.Count(_builder_00000, 203)
	e := &HashJoinExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		concurrency:  v.Concurrency,
		joinType:     v.JoinType,
		isOuterJoin:  v.JoinType.IsOuterJoin(),
		innerIdx:     v.InnerChildIdx,
	}

	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := retTypes(leftExec), retTypes(rightExec)
	if v.InnerChildIdx == 0 {
		trace_util_0.Count(_builder_00000, 209)
		if len(v.LeftConditions) > 0 {
			trace_util_0.Count(_builder_00000, 211)
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
		trace_util_0.Count(_builder_00000, 210)
		e.innerExec = leftExec
		e.outerExec = rightExec
		e.outerFilter = v.RightConditions
		e.innerKeys = leftHashKey
		e.outerKeys = rightHashKey
		if defaultValues == nil {
			trace_util_0.Count(_builder_00000, 212)
			defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
		}
	} else {
		trace_util_0.Count(_builder_00000, 213)
		{
			if len(v.RightConditions) > 0 {
				trace_util_0.Count(_builder_00000, 215)
				b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
				return nil
			}
			trace_util_0.Count(_builder_00000, 214)
			e.innerExec = rightExec
			e.outerExec = leftExec
			e.outerFilter = v.LeftConditions
			e.innerKeys = rightHashKey
			e.outerKeys = leftHashKey
			if defaultValues == nil {
				trace_util_0.Count(_builder_00000, 216)
				defaultValues = make([]types.Datum, e.innerExec.Schema().Len())
			}
		}
	}
	trace_util_0.Count(_builder_00000, 204)
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		trace_util_0.Count(_builder_00000, 217)
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes)
	}
	trace_util_0.Count(_builder_00000, 205)
	executorCountHashJoinExec.Inc()
	return e
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) Executor {
	trace_util_0.Count(_builder_00000, 218)
	src := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 224)
		return nil
	}
	trace_util_0.Count(_builder_00000, 219)
	sessionVars := b.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create table t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// mysql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// mysql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// mysql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 row in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		trace_util_0.Count(_builder_00000, 225)
		e.defaultVal = nil
	} else {
		trace_util_0.Count(_builder_00000, 226)
		{
			e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
		}
	}
	trace_util_0.Count(_builder_00000, 220)
	for _, aggDesc := range v.AggFuncs {
		trace_util_0.Count(_builder_00000, 227)
		if aggDesc.HasDistinct {
			trace_util_0.Count(_builder_00000, 228)
			e.isUnparallelExec = true
		}
	}
	// When we set both tidb_hashagg_final_concurrency and tidb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	trace_util_0.Count(_builder_00000, 221)
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency, sessionVars.HashAggPartialConcurrency; finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		trace_util_0.Count(_builder_00000, 229)
		e.isUnparallelExec = true
	}
	trace_util_0.Count(_builder_00000, 222)
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {
		trace_util_0.Count(_builder_00000, 230)
		if e.isUnparallelExec {
			trace_util_0.Count(_builder_00000, 232)
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(b.ctx, aggDesc, i))
		} else {
			trace_util_0.Count(_builder_00000, 233)
			{
				ordinal := []int{partialOrdinal}
				partialOrdinal++
				if aggDesc.Name == ast.AggFuncAvg {
					trace_util_0.Count(_builder_00000, 235)
					ordinal = append(ordinal, partialOrdinal+1)
					partialOrdinal++
				}
				trace_util_0.Count(_builder_00000, 234)
				partialAggDesc, finalDesc := aggDesc.Split(ordinal)
				partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
				finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
				e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
				e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
				if partialAggDesc.Name == ast.AggFuncGroupConcat {
					trace_util_0.Count(_builder_00000, 236)
					// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
					finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
						partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
					)
				}
			}
		}
		trace_util_0.Count(_builder_00000, 231)
		if e.defaultVal != nil {
			trace_util_0.Count(_builder_00000, 237)
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	trace_util_0.Count(_builder_00000, 223)
	executorCounterHashAggExec.Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plannercore.PhysicalStreamAgg) Executor {
	trace_util_0.Count(_builder_00000, 238)
	src := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 242)
		return nil
	}
	trace_util_0.Count(_builder_00000, 239)
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		groupChecker: newGroupChecker(b.ctx.GetSessionVars().StmtCtx, v.GroupByItems),
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		trace_util_0.Count(_builder_00000, 243)
		e.defaultVal = nil
	} else {
		trace_util_0.Count(_builder_00000, 244)
		{
			e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
		}
	}
	trace_util_0.Count(_builder_00000, 240)
	for i, aggDesc := range v.AggFuncs {
		trace_util_0.Count(_builder_00000, 245)
		aggFunc := aggfuncs.Build(b.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			trace_util_0.Count(_builder_00000, 246)
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	trace_util_0.Count(_builder_00000, 241)
	executorStreamAggExec.Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) Executor {
	trace_util_0.Count(_builder_00000, 247)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 249)
		return nil
	}
	trace_util_0.Count(_builder_00000, 248)
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		filters:      v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) Executor {
	trace_util_0.Count(_builder_00000, 250)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 253)
		return nil
	}
	trace_util_0.Count(_builder_00000, 251)
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		numWorkers:       b.ctx.GetSessionVars().ProjectionConcurrency,
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		trace_util_0.Count(_builder_00000, 254)
		e.numWorkers = 0
	}
	trace_util_0.Count(_builder_00000, 252)
	return e
}

func (b *executorBuilder) buildTableDual(v *plannercore.PhysicalTableDual) Executor {
	trace_util_0.Count(_builder_00000, 255)
	if v.RowCount != 0 && v.RowCount != 1 {
		trace_util_0.Count(_builder_00000, 257)
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	trace_util_0.Count(_builder_00000, 256)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = v.RowCount
	e := &TableDualExec{
		baseExecutor: base,
		numDualRows:  v.RowCount,
	}
	return e
}

func (b *executorBuilder) getStartTS() (uint64, error) {
	trace_util_0.Count(_builder_00000, 258)
	if b.startTS != 0 {
		trace_util_0.Count(_builder_00000, 263)
		// Return the cached value.
		return b.startTS, nil
	}

	trace_util_0.Count(_builder_00000, 259)
	startTS := b.ctx.GetSessionVars().SnapshotTS
	txn, err := b.ctx.Txn(true)
	if err != nil {
		trace_util_0.Count(_builder_00000, 264)
		return 0, err
	}
	trace_util_0.Count(_builder_00000, 260)
	if startTS == 0 && txn.Valid() {
		trace_util_0.Count(_builder_00000, 265)
		startTS = txn.StartTS()
	}
	trace_util_0.Count(_builder_00000, 261)
	b.startTS = startTS
	if b.startTS == 0 {
		trace_util_0.Count(_builder_00000, 266)
		return 0, errors.Trace(ErrGetStartTS)
	}
	trace_util_0.Count(_builder_00000, 262)
	return startTS, nil
}

func (b *executorBuilder) buildMemTable(v *plannercore.PhysicalMemTable) Executor {
	trace_util_0.Count(_builder_00000, 267)
	tb, _ := b.is.TableByID(v.Table.ID)
	e := &TableScanExec{
		baseExecutor:   newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		t:              tb,
		columns:        v.Columns,
		seekHandle:     math.MinInt64,
		isVirtualTable: tb.Type() == table.VirtualTable,
	}
	return e
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) Executor {
	trace_util_0.Count(_builder_00000, 268)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 270)
		return nil
	}
	trace_util_0.Count(_builder_00000, 269)
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	executorCounterSortExec.Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) Executor {
	trace_util_0.Count(_builder_00000, 271)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 273)
		return nil
	}
	trace_util_0.Count(_builder_00000, 272)
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	executorCounterTopNExec.Inc()
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildApply(v *plannercore.PhysicalApply) *NestedLoopApplyExec {
	trace_util_0.Count(_builder_00000, 274)
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 279)
		return nil
	}
	trace_util_0.Count(_builder_00000, 275)
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 280)
		return nil
	}
	trace_util_0.Count(_builder_00000, 276)
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		trace_util_0.Count(_builder_00000, 281)
		defaultValues = make([]types.Datum, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	trace_util_0.Count(_builder_00000, 277)
	tupleJoiner := newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild))
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		trace_util_0.Count(_builder_00000, 282)
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	trace_util_0.Count(_builder_00000, 278)
	e := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), outerExec, innerExec),
		innerExec:    innerExec,
		outerExec:    outerExec,
		outerFilter:  outerFilter,
		innerFilter:  innerFilter,
		outer:        v.JoinType != plannercore.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  v.OuterSchema,
	}
	executorCounterNestedLoopApplyExec.Inc()
	return e
}

func (b *executorBuilder) buildMaxOneRow(v *plannercore.PhysicalMaxOneRow) Executor {
	trace_util_0.Count(_builder_00000, 283)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 285)
		return nil
	}
	trace_util_0.Count(_builder_00000, 284)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = 2
	base.maxChunkSize = 2
	e := &MaxOneRowExec{baseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plannercore.PhysicalUnionAll) Executor {
	trace_util_0.Count(_builder_00000, 286)
	childExecs := make([]Executor, len(v.Children()))
	for i, child := range v.Children() {
		trace_util_0.Count(_builder_00000, 288)
		childExecs[i] = b.build(child)
		if b.err != nil {
			trace_util_0.Count(_builder_00000, 289)
			return nil
		}
	}
	trace_util_0.Count(_builder_00000, 287)
	e := &UnionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExecs...),
	}
	return e
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) Executor {
	trace_util_0.Count(_builder_00000, 290)
	base := newBaseExecutor(b.ctx, nil, v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	if v.IndexInfo != nil {
		trace_util_0.Count(_builder_00000, 293)
		return &SplitIndexRegionExec{
			baseExecutor: base,
			tableInfo:    v.TableInfo,
			indexInfo:    v.IndexInfo,
			lower:        v.Lower,
			upper:        v.Upper,
			num:          v.Num,
			valueLists:   v.ValueLists,
		}
	}
	trace_util_0.Count(_builder_00000, 291)
	if len(v.ValueLists) > 0 {
		trace_util_0.Count(_builder_00000, 294)
		return &SplitTableRegionExec{
			baseExecutor: base,
			tableInfo:    v.TableInfo,
			valueLists:   v.ValueLists,
		}
	}
	trace_util_0.Count(_builder_00000, 292)
	return &SplitTableRegionExec{
		baseExecutor: base,
		tableInfo:    v.TableInfo,
		lower:        v.Lower[0],
		upper:        v.Upper[0],
		num:          v.Num,
	}
}

func (b *executorBuilder) buildUpdate(v *plannercore.Update) Executor {
	trace_util_0.Count(_builder_00000, 295)
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		trace_util_0.Count(_builder_00000, 298)
		tblID2table[id], _ = b.is.TableByID(id)
	}
	trace_util_0.Count(_builder_00000, 296)
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 299)
		return nil
	}
	trace_util_0.Count(_builder_00000, 297)
	columns2Handle := buildColumns2Handle(v.SelectPlan.Schema(), tblID2table)
	base := newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec)
	base.initCap = chunk.ZeroCapacity
	updateExec := &UpdateExec{
		baseExecutor:   base,
		SelectExec:     selExec,
		OrderedList:    v.OrderedList,
		tblID2table:    tblID2table,
		columns2Handle: columns2Handle,
	}
	return updateExec
}

// cols2Handle represents an mapper from column index to handle index.
type cols2Handle struct {
	// start and end represent the ordinal range [start, end) of the consecutive columns.
	start, end int32
	// handleOrdinal represents the ordinal of the handle column.
	handleOrdinal int32
}

// cols2HandleSlice attaches the methods of sort.Interface to []cols2Handle sorting in increasing order.
type cols2HandleSlice []cols2Handle

// Len implements sort.Interface#Len.
func (c cols2HandleSlice) Len() int {
	trace_util_0.Count(_builder_00000, 300)
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c cols2HandleSlice) Swap(i, j int) {
	trace_util_0.Count(_builder_00000, 301)
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c cols2HandleSlice) Less(i, j int) bool {
	trace_util_0.Count(_builder_00000, 302)
	return c[i].start < c[j].start
}

// findHandle finds the ordinal of the corresponding handle column.
func (c cols2HandleSlice) findHandle(ordinal int32) (int32, bool) {
	trace_util_0.Count(_builder_00000, 303)
	if c == nil || len(c) == 0 {
		trace_util_0.Count(_builder_00000, 307)
		return 0, false
	}
	// find the smallest index of the range that its start great than ordinal.
	// @see https://godoc.org/sort#Search
	trace_util_0.Count(_builder_00000, 304)
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { trace_util_0.Count(_builder_00000, 308); return c[i].start > ordinal })
	trace_util_0.Count(_builder_00000, 305)
	if rangeBehindOrdinal == 0 {
		trace_util_0.Count(_builder_00000, 309)
		return 0, false
	}
	trace_util_0.Count(_builder_00000, 306)
	return c[rangeBehindOrdinal-1].handleOrdinal, true
}

// buildColumns2Handle builds columns to handle mapping.
func buildColumns2Handle(schema *expression.Schema, tblID2Table map[int64]table.Table) cols2HandleSlice {
	trace_util_0.Count(_builder_00000, 310)
	if len(schema.TblID2Handle) < 2 {
		trace_util_0.Count(_builder_00000, 313)
		// skip buildColumns2Handle mapping if there are only single table.
		return nil
	}
	trace_util_0.Count(_builder_00000, 311)
	var cols2Handles cols2HandleSlice
	for tblID, handleCols := range schema.TblID2Handle {
		trace_util_0.Count(_builder_00000, 314)
		tbl := tblID2Table[tblID]
		for _, handleCol := range handleCols {
			trace_util_0.Count(_builder_00000, 315)
			offset := getTableOffset(schema, handleCol)
			end := offset + len(tbl.WritableCols())
			cols2Handles = append(cols2Handles, cols2Handle{int32(offset), int32(end), int32(handleCol.Index)})
		}
	}
	trace_util_0.Count(_builder_00000, 312)
	sort.Sort(cols2Handles)
	return cols2Handles
}

func (b *executorBuilder) buildDelete(v *plannercore.Delete) Executor {
	trace_util_0.Count(_builder_00000, 316)
	tblID2table := make(map[int64]table.Table)
	for id := range v.SelectPlan.Schema().TblID2Handle {
		trace_util_0.Count(_builder_00000, 319)
		tblID2table[id], _ = b.is.TableByID(id)
	}
	trace_util_0.Count(_builder_00000, 317)
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 320)
		return nil
	}
	trace_util_0.Count(_builder_00000, 318)
	base := newBaseExecutor(b.ctx, nil, v.ExplainID(), selExec)
	base.initCap = chunk.ZeroCapacity
	deleteExec := &DeleteExec{
		baseExecutor: base,
		SelectExec:   selExec,
		Tables:       v.Tables,
		IsMultiTable: v.IsMultiTable,
		tblID2Table:  tblID2table,
	}
	return deleteExec
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask, maxNumBuckets uint64, autoAnalyze string) *analyzeTask {
	trace_util_0.Count(_builder_00000, 321)
	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	e := &AnalyzeIndexExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		idxInfo:         task.IndexInfo,
		concurrency:     b.ctx.GetSessionVars().IndexSerialScanConcurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeIndex,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: offset,
		},
		maxNumBuckets: maxNumBuckets,
	}
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: int64(maxNumBuckets),
		NumColumns: int32(len(task.IndexInfo.Columns)),
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze index " + task.IndexInfo.Name.O}
	return &analyzeTask{taskType: idxTask, idxExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzeIndexIncremental(task plannercore.AnalyzeIndexTask, maxNumBuckets uint64) *analyzeTask {
	trace_util_0.Count(_builder_00000, 322)
	h := domain.GetDomain(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&model.TableInfo{}, task.PhysicalTableID)
	analyzeTask := b.buildAnalyzeIndexPushdown(task, maxNumBuckets, "")
	if statsTbl.Pseudo {
		trace_util_0.Count(_builder_00000, 326)
		return analyzeTask
	}
	trace_util_0.Count(_builder_00000, 323)
	idx, ok := statsTbl.Indices[task.IndexInfo.ID]
	if !ok || idx.Len() == 0 || idx.LastAnalyzePos.IsNull() {
		trace_util_0.Count(_builder_00000, 327)
		return analyzeTask
	}
	trace_util_0.Count(_builder_00000, 324)
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(idx.Flag) {
		trace_util_0.Count(_builder_00000, 328)
		exec := analyzeTask.idxExec
		if idx.CMSketch != nil {
			trace_util_0.Count(_builder_00000, 330)
			width, depth := idx.CMSketch.GetWidthAndDepth()
			exec.analyzePB.IdxReq.CmsketchWidth = &width
			exec.analyzePB.IdxReq.CmsketchDepth = &depth
		}
		trace_util_0.Count(_builder_00000, 329)
		oldHist = idx.Histogram.Copy()
	} else {
		trace_util_0.Count(_builder_00000, 331)
		{
			_, bktID := idx.LessRowCountWithBktIdx(idx.LastAnalyzePos)
			if bktID == 0 {
				trace_util_0.Count(_builder_00000, 333)
				return analyzeTask
			}
			trace_util_0.Count(_builder_00000, 332)
			oldHist = idx.TruncateHistogram(bktID)
		}
	}
	trace_util_0.Count(_builder_00000, 325)
	oldHist = oldHist.RemoveUpperBound()
	analyzeTask.taskType = idxIncrementalTask
	analyzeTask.idxIncrementalExec = &analyzeIndexIncrementalExec{AnalyzeIndexExec: *analyzeTask.idxExec, oldHist: oldHist, oldCMS: idx.CMSketch}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "analyze incremental index " + task.IndexInfo.Name.O}
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plannercore.AnalyzeColumnsTask, maxNumBuckets uint64, autoAnalyze string) *analyzeTask {
	trace_util_0.Count(_builder_00000, 334)
	cols := task.ColsInfo
	if task.PKInfo != nil {
		trace_util_0.Count(_builder_00000, 336)
		cols = append([]*model.ColumnInfo{task.PKInfo}, cols...)
	}

	trace_util_0.Count(_builder_00000, 335)
	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	e := &AnalyzeColumnsExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		colsInfo:        task.ColsInfo,
		pkInfo:          task.PKInfo,
		concurrency:     b.ctx.GetSessionVars().DistSQLScanConcurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeColumn,
			StartTs:        math.MaxUint64,
			Flags:          statementContextToFlags(b.ctx.GetSessionVars().StmtCtx),
			TimeZoneOffset: offset,
		},
		maxNumBuckets: maxNumBuckets,
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    int64(maxNumBuckets),
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   model.ColumnsToProto(cols, task.PKInfo != nil),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	b.err = plannercore.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols)
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze columns"}
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzePKIncremental(task plannercore.AnalyzeColumnsTask, maxNumBuckets uint64) *analyzeTask {
	trace_util_0.Count(_builder_00000, 337)
	h := domain.GetDomain(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&model.TableInfo{}, task.PhysicalTableID)
	analyzeTask := b.buildAnalyzeColumnsPushdown(task, maxNumBuckets, "")
	if statsTbl.Pseudo {
		trace_util_0.Count(_builder_00000, 341)
		return analyzeTask
	}
	trace_util_0.Count(_builder_00000, 338)
	col, ok := statsTbl.Columns[task.PKInfo.ID]
	if !ok || col.Len() == 0 || col.LastAnalyzePos.IsNull() {
		trace_util_0.Count(_builder_00000, 342)
		return analyzeTask
	}
	trace_util_0.Count(_builder_00000, 339)
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(col.Flag) {
		trace_util_0.Count(_builder_00000, 343)
		oldHist = col.Histogram.Copy()
	} else {
		trace_util_0.Count(_builder_00000, 344)
		{
			d, err := col.LastAnalyzePos.ConvertTo(b.ctx.GetSessionVars().StmtCtx, col.Tp)
			if err != nil {
				trace_util_0.Count(_builder_00000, 347)
				b.err = err
				return nil
			}
			trace_util_0.Count(_builder_00000, 345)
			_, bktID := col.LessRowCountWithBktIdx(d)
			if bktID == 0 {
				trace_util_0.Count(_builder_00000, 348)
				return analyzeTask
			}
			trace_util_0.Count(_builder_00000, 346)
			oldHist = col.TruncateHistogram(bktID)
			oldHist.NDV = int64(oldHist.TotalRowCount())
		}
	}
	trace_util_0.Count(_builder_00000, 340)
	exec := analyzeTask.colExec
	analyzeTask.taskType = pkIncrementalTask
	analyzeTask.colIncrementalExec = &analyzePKIncrementalExec{AnalyzeColumnsExec: *exec, oldHist: oldHist}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "analyze incremental primary key"}
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeFastColumn(e *AnalyzeExec, task plannercore.AnalyzeColumnsTask, maxNumBuckets uint64) {
	trace_util_0.Count(_builder_00000, 349)
	findTask := false
	for _, eTask := range e.tasks {
		trace_util_0.Count(_builder_00000, 351)
		if eTask.fastExec.physicalTableID == task.PhysicalTableID {
			trace_util_0.Count(_builder_00000, 352)
			eTask.fastExec.colsInfo = append(eTask.fastExec.colsInfo, task.ColsInfo...)
			findTask = true
			break
		}
	}
	trace_util_0.Count(_builder_00000, 350)
	if !findTask {
		trace_util_0.Count(_builder_00000, 353)
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			trace_util_0.Count(_builder_00000, 355)
			return
		}
		trace_util_0.Count(_builder_00000, 354)
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: &AnalyzeFastExec{
				ctx:             b.ctx,
				physicalTableID: task.PhysicalTableID,
				colsInfo:        task.ColsInfo,
				pkInfo:          task.PKInfo,
				maxNumBuckets:   maxNumBuckets,
				tblInfo:         task.TblInfo,
				concurrency:     concurrency,
				wg:              &sync.WaitGroup{},
			},
			job: &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: "fast analyze columns"},
		})
	}
}

func (b *executorBuilder) buildAnalyzeFastIndex(e *AnalyzeExec, task plannercore.AnalyzeIndexTask, maxNumBuckets uint64) {
	trace_util_0.Count(_builder_00000, 356)
	findTask := false
	for _, eTask := range e.tasks {
		trace_util_0.Count(_builder_00000, 358)
		if eTask.fastExec.physicalTableID == task.PhysicalTableID {
			trace_util_0.Count(_builder_00000, 359)
			eTask.fastExec.idxsInfo = append(eTask.fastExec.idxsInfo, task.IndexInfo)
			findTask = true
			break
		}
	}
	trace_util_0.Count(_builder_00000, 357)
	if !findTask {
		trace_util_0.Count(_builder_00000, 360)
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			trace_util_0.Count(_builder_00000, 362)
			return
		}
		trace_util_0.Count(_builder_00000, 361)
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: &AnalyzeFastExec{
				ctx:             b.ctx,
				physicalTableID: task.PhysicalTableID,
				idxsInfo:        []*model.IndexInfo{task.IndexInfo},
				maxNumBuckets:   maxNumBuckets,
				tblInfo:         task.TblInfo,
				concurrency:     concurrency,
				wg:              &sync.WaitGroup{},
			},
			job: &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: "fast analyze index " + task.IndexInfo.Name.O},
		})
	}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) Executor {
	trace_util_0.Count(_builder_00000, 363)
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
		wg:           &sync.WaitGroup{},
	}
	enableFastAnalyze := b.ctx.GetSessionVars().EnableFastAnalyze
	autoAnalyze := ""
	if b.ctx.GetSessionVars().InRestrictedSQL {
		trace_util_0.Count(_builder_00000, 367)
		autoAnalyze = "auto "
	}
	trace_util_0.Count(_builder_00000, 364)
	for _, task := range v.ColTasks {
		trace_util_0.Count(_builder_00000, 368)
		if task.Incremental {
			trace_util_0.Count(_builder_00000, 370)
			e.tasks = append(e.tasks, b.buildAnalyzePKIncremental(task, v.MaxNumBuckets))
		} else {
			trace_util_0.Count(_builder_00000, 371)
			{
				if enableFastAnalyze {
					trace_util_0.Count(_builder_00000, 372)
					b.buildAnalyzeFastColumn(e, task, v.MaxNumBuckets)
				} else {
					trace_util_0.Count(_builder_00000, 373)
					{
						e.tasks = append(e.tasks, b.buildAnalyzeColumnsPushdown(task, v.MaxNumBuckets, autoAnalyze))
					}
				}
			}
		}
		trace_util_0.Count(_builder_00000, 369)
		if b.err != nil {
			trace_util_0.Count(_builder_00000, 374)
			return nil
		}
	}
	trace_util_0.Count(_builder_00000, 365)
	for _, task := range v.IdxTasks {
		trace_util_0.Count(_builder_00000, 375)
		if task.Incremental {
			trace_util_0.Count(_builder_00000, 377)
			e.tasks = append(e.tasks, b.buildAnalyzeIndexIncremental(task, v.MaxNumBuckets))
		} else {
			trace_util_0.Count(_builder_00000, 378)
			{
				if enableFastAnalyze {
					trace_util_0.Count(_builder_00000, 379)
					b.buildAnalyzeFastIndex(e, task, v.MaxNumBuckets)
				} else {
					trace_util_0.Count(_builder_00000, 380)
					{
						e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.MaxNumBuckets, autoAnalyze))
					}
				}
			}
		}
		trace_util_0.Count(_builder_00000, 376)
		if b.err != nil {
			trace_util_0.Count(_builder_00000, 381)
			return nil
		}
	}
	trace_util_0.Count(_builder_00000, 366)
	return e
}

func constructDistExec(sctx sessionctx.Context, plans []plannercore.PhysicalPlan) ([]*tipb.Executor, bool, error) {
	trace_util_0.Count(_builder_00000, 382)
	streaming := true
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		trace_util_0.Count(_builder_00000, 384)
		execPB, err := p.ToPB(sctx)
		if err != nil {
			trace_util_0.Count(_builder_00000, 387)
			return nil, false, err
		}
		trace_util_0.Count(_builder_00000, 385)
		if !plannercore.SupportStreaming(p) {
			trace_util_0.Count(_builder_00000, 388)
			streaming = false
		}
		trace_util_0.Count(_builder_00000, 386)
		executors = append(executors, execPB)
	}
	trace_util_0.Count(_builder_00000, 383)
	return executors, streaming, nil
}

func (b *executorBuilder) constructDAGReq(plans []plannercore.PhysicalPlan) (dagReq *tipb.DAGRequest, streaming bool, err error) {
	trace_util_0.Count(_builder_00000, 389)
	dagReq = &tipb.DAGRequest{}
	dagReq.StartTs, err = b.getStartTS()
	if err != nil {
		trace_util_0.Count(_builder_00000, 391)
		return nil, false, err
	}
	trace_util_0.Count(_builder_00000, 390)
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	dagReq.Executors, streaming, err = constructDistExec(b.ctx, plans)
	return dagReq, streaming, err
}

func (b *executorBuilder) corColInDistPlan(plans []plannercore.PhysicalPlan) bool {
	trace_util_0.Count(_builder_00000, 392)
	for _, p := range plans {
		trace_util_0.Count(_builder_00000, 394)
		x, ok := p.(*plannercore.PhysicalSelection)
		if !ok {
			trace_util_0.Count(_builder_00000, 396)
			continue
		}
		trace_util_0.Count(_builder_00000, 395)
		for _, cond := range x.Conditions {
			trace_util_0.Count(_builder_00000, 397)
			if len(expression.ExtractCorColumns(cond)) > 0 {
				trace_util_0.Count(_builder_00000, 398)
				return true
			}
		}
	}
	trace_util_0.Count(_builder_00000, 393)
	return false
}

// corColInAccess checks whether there's correlated column in access conditions.
func (b *executorBuilder) corColInAccess(p plannercore.PhysicalPlan) bool {
	trace_util_0.Count(_builder_00000, 399)
	var access []expression.Expression
	switch x := p.(type) {
	case *plannercore.PhysicalTableScan:
		trace_util_0.Count(_builder_00000, 402)
		access = x.AccessCondition
	case *plannercore.PhysicalIndexScan:
		trace_util_0.Count(_builder_00000, 403)
		access = x.AccessCondition
	}
	trace_util_0.Count(_builder_00000, 400)
	for _, cond := range access {
		trace_util_0.Count(_builder_00000, 404)
		if len(expression.ExtractCorColumns(cond)) > 0 {
			trace_util_0.Count(_builder_00000, 405)
			return true
		}
	}
	trace_util_0.Count(_builder_00000, 401)
	return false
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plannercore.PhysicalIndexJoin) Executor {
	trace_util_0.Count(_builder_00000, 406)
	outerExec := b.build(v.Children()[v.OuterIndex])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 413)
		return nil
	}
	trace_util_0.Count(_builder_00000, 407)
	outerTypes := retTypes(outerExec)
	innerPlan := v.Children()[1-v.OuterIndex]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		trace_util_0.Count(_builder_00000, 414)
		innerTypes[i] = col.RetType
	}

	trace_util_0.Count(_builder_00000, 408)
	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.OuterIndex == 1 {
		trace_util_0.Count(_builder_00000, 415)
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			trace_util_0.Count(_builder_00000, 416)
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		trace_util_0.Count(_builder_00000, 417)
		{
			leftTypes, rightTypes = outerTypes, innerTypes
			outerFilter = v.LeftConditions
			if len(v.RightConditions) > 0 {
				trace_util_0.Count(_builder_00000, 418)
				b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
				return nil
			}
		}
	}
	trace_util_0.Count(_builder_00000, 409)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		trace_util_0.Count(_builder_00000, 419)
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	trace_util_0.Count(_builder_00000, 410)
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), outerExec),
		outerCtx: outerCtx{
			rowTypes: outerTypes,
			filter:   outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{Plan: innerPlan, executorBuilder: b},
			rowTypes:      innerTypes,
		},
		workerWg:      new(sync.WaitGroup),
		joiner:        newJoiner(b.ctx, v.JoinType, v.OuterIndex == 1, defaultValues, v.OtherConditions, leftTypes, rightTypes),
		isOuterJoin:   v.JoinType.IsOuterJoin(),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
		lastColHelper: v.CompareFilters,
	}
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		trace_util_0.Count(_builder_00000, 420)
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	trace_util_0.Count(_builder_00000, 411)
	e.outerCtx.keyCols = outerKeyCols
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		trace_util_0.Count(_builder_00000, 421)
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
	}
	trace_util_0.Count(_builder_00000, 412)
	e.innerCtx.keyCols = innerKeyCols
	e.joinResult = newFirstChunk(e)
	executorCounterIndexLookUpJoin.Inc()
	return e
}

// containsLimit tests if the execs contains Limit because we do not know whether `Limit` has consumed all of its' source,
// so the feedback may not be accurate.
func containsLimit(execs []*tipb.Executor) bool {
	trace_util_0.Count(_builder_00000, 422)
	for _, exec := range execs {
		trace_util_0.Count(_builder_00000, 424)
		if exec.Limit != nil {
			trace_util_0.Count(_builder_00000, 425)
			return true
		}
	}
	trace_util_0.Count(_builder_00000, 423)
	return false
}

func buildNoRangeTableReader(b *executorBuilder, v *plannercore.PhysicalTableReader) (*TableReaderExecutor, error) {
	trace_util_0.Count(_builder_00000, 426)
	dagReq, streaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		trace_util_0.Count(_builder_00000, 432)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 427)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	tbl, _ := b.is.TableByID(ts.Table.ID)
	if isPartition, physicalTableID := ts.IsPartition(); isPartition {
		trace_util_0.Count(_builder_00000, 433)
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	trace_util_0.Count(_builder_00000, 428)
	e := &TableReaderExecutor{
		baseExecutor:   newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:          dagReq,
		table:          tbl,
		keepOrder:      ts.KeepOrder,
		desc:           ts.Desc,
		columns:        ts.Columns,
		streaming:      streaming,
		corColInFilter: b.corColInDistPlan(v.TablePlans),
		corColInAccess: b.corColInAccess(v.TablePlans[0]),
		plans:          v.TablePlans,
	}
	if containsLimit(dagReq.Executors) {
		trace_util_0.Count(_builder_00000, 434)
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
	} else {
		trace_util_0.Count(_builder_00000, 435)
		{
			e.feedback = statistics.NewQueryFeedback(getPhysicalTableID(tbl), ts.Hist, int64(ts.StatsCount()), ts.Desc)
		}
	}
	trace_util_0.Count(_builder_00000, 429)
	collect := (b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil) || e.feedback.CollectFeedback(len(ts.Ranges))
	if !collect {
		trace_util_0.Count(_builder_00000, 436)
		e.feedback.Invalidate()
	}
	trace_util_0.Count(_builder_00000, 430)
	e.dagPB.CollectRangeCounts = &collect

	for i := range v.Schema().Columns {
		trace_util_0.Count(_builder_00000, 437)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	trace_util_0.Count(_builder_00000, 431)
	return e, nil
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plannercore.PhysicalTableReader) *TableReaderExecutor {
	trace_util_0.Count(_builder_00000, 438)
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 440)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 439)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	trace_util_0.Count(_builder_00000, 441)
	dagReq, streaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		trace_util_0.Count(_builder_00000, 447)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 442)
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	tbl, _ := b.is.TableByID(is.Table.ID)
	isPartition, physicalTableID := is.IsPartition()
	if isPartition {
		trace_util_0.Count(_builder_00000, 448)
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	} else {
		trace_util_0.Count(_builder_00000, 449)
		{
			physicalTableID = is.Table.ID
		}
	}
	trace_util_0.Count(_builder_00000, 443)
	e := &IndexReaderExecutor{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:           dagReq,
		physicalTableID: physicalTableID,
		table:           tbl,
		index:           is.Index,
		keepOrder:       is.KeepOrder,
		desc:            is.Desc,
		columns:         is.Columns,
		streaming:       streaming,
		corColInFilter:  b.corColInDistPlan(v.IndexPlans),
		corColInAccess:  b.corColInAccess(v.IndexPlans[0]),
		idxCols:         is.IdxCols,
		colLens:         is.IdxColLens,
		plans:           v.IndexPlans,
	}
	if containsLimit(dagReq.Executors) {
		trace_util_0.Count(_builder_00000, 450)
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		trace_util_0.Count(_builder_00000, 451)
		{
			e.feedback = statistics.NewQueryFeedback(e.physicalTableID, is.Hist, int64(is.StatsCount()), is.Desc)
		}
	}
	trace_util_0.Count(_builder_00000, 444)
	collect := (b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil) || e.feedback.CollectFeedback(len(is.Ranges))
	if !collect {
		trace_util_0.Count(_builder_00000, 452)
		e.feedback.Invalidate()
	}
	trace_util_0.Count(_builder_00000, 445)
	e.dagPB.CollectRangeCounts = &collect

	for _, col := range v.OutputColumns {
		trace_util_0.Count(_builder_00000, 453)
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	trace_util_0.Count(_builder_00000, 446)
	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) *IndexReaderExecutor {
	trace_util_0.Count(_builder_00000, 454)
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 456)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 455)
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexIDs = append(sctx.IndexIDs, is.Index.ID)
	return ret
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	trace_util_0.Count(_builder_00000, 457)
	indexReq, indexStreaming, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		trace_util_0.Count(_builder_00000, 465)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 458)
	tableReq, tableStreaming, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		trace_util_0.Count(_builder_00000, 466)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 459)
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	tbl, _ := b.is.TableByID(is.Table.ID)

	for i := 0; i < v.Schema().Len(); i++ {
		trace_util_0.Count(_builder_00000, 467)
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	trace_util_0.Count(_builder_00000, 460)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	if isPartition, physicalTableID := ts.IsPartition(); isPartition {
		trace_util_0.Count(_builder_00000, 468)
		pt := tbl.(table.PartitionedTable)
		tbl = pt.GetPartition(physicalTableID)
	}
	trace_util_0.Count(_builder_00000, 461)
	e := &IndexLookUpExecutor{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:             indexReq,
		table:             tbl,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		tableRequest:      tableReq,
		columns:           ts.Columns,
		indexStreaming:    indexStreaming,
		tableStreaming:    tableStreaming,
		dataReaderBuilder: &dataReaderBuilder{executorBuilder: b},
		corColInIdxSide:   b.corColInDistPlan(v.IndexPlans),
		corColInTblSide:   b.corColInDistPlan(v.TablePlans),
		corColInAccess:    b.corColInAccess(v.IndexPlans[0]),
		idxCols:           is.IdxCols,
		colLens:           is.IdxColLens,
		idxPlans:          v.IndexPlans,
		tblPlans:          v.TablePlans,
	}

	if containsLimit(indexReq.Executors) {
		trace_util_0.Count(_builder_00000, 469)
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		trace_util_0.Count(_builder_00000, 470)
		{
			e.feedback = statistics.NewQueryFeedback(getPhysicalTableID(tbl), is.Hist, int64(is.StatsCount()), is.Desc)
		}
	}
	// do not collect the feedback for table request.
	trace_util_0.Count(_builder_00000, 462)
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	collectIndex := (b.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil) || e.feedback.CollectFeedback(len(is.Ranges))
	if !collectIndex {
		trace_util_0.Count(_builder_00000, 471)
		e.feedback.Invalidate()
	}
	trace_util_0.Count(_builder_00000, 463)
	e.dagPB.CollectRangeCounts = &collectIndex
	if cols, ok := v.Schema().TblID2Handle[is.Table.ID]; ok {
		trace_util_0.Count(_builder_00000, 472)
		e.handleIdx = cols[0].Index
	}
	trace_util_0.Count(_builder_00000, 464)
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) *IndexLookUpExecutor {
	trace_util_0.Count(_builder_00000, 473)
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 475)
		b.err = err
		return nil
	}

	trace_util_0.Count(_builder_00000, 474)
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)

	ret.ranges = is.Ranges
	executorCounterIndexLookUpExecutor.Inc()
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexIDs = append(sctx.IndexIDs, is.Index.ID)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plannercore.Plan
	*executorBuilder

	selectResultHook // for testing
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(ctx context.Context, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (Executor, error) {
	trace_util_0.Count(_builder_00000, 476)
	switch v := builder.Plan.(type) {
	case *plannercore.PhysicalTableReader:
		trace_util_0.Count(_builder_00000, 478)
		return builder.buildTableReaderForIndexJoin(ctx, v, lookUpContents)
	case *plannercore.PhysicalIndexReader:
		trace_util_0.Count(_builder_00000, 479)
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *plannercore.PhysicalIndexLookUpReader:
		trace_util_0.Count(_builder_00000, 480)
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *plannercore.PhysicalUnionScan:
		trace_util_0.Count(_builder_00000, 481)
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	}
	trace_util_0.Count(_builder_00000, 477)
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *plannercore.PhysicalUnionScan,
	values []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (Executor, error) {
	trace_util_0.Count(_builder_00000, 482)
	childBuilder := &dataReaderBuilder{Plan: v.Children()[0], executorBuilder: builder.executorBuilder}
	reader, err := childBuilder.buildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		trace_util_0.Count(_builder_00000, 485)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 483)
	e, err := builder.buildUnionScanFromReader(reader, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 486)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 484)
	us := e.(*UnionScanExec)
	us.snapshotChunkBuffer = newFirstChunk(us)
	return us, nil
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalTableReader, lookUpContents []*indexJoinLookUpContent) (Executor, error) {
	trace_util_0.Count(_builder_00000, 487)
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 490)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 488)
	handles := make([]int64, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		trace_util_0.Count(_builder_00000, 491)
		handles = append(handles, content.keys[0].GetInt64())
	}
	trace_util_0.Count(_builder_00000, 489)
	return builder.buildTableReaderFromHandles(ctx, e, handles)
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []int64) (Executor, error) {
	trace_util_0.Count(_builder_00000, 492)
	if e.runtimeStats != nil && e.dagPB.CollectExecutionSummaries == nil {
		trace_util_0.Count(_builder_00000, 496)
		colExec := true
		e.dagPB.CollectExecutionSummaries = &colExec
	}

	trace_util_0.Count(_builder_00000, 493)
	sort.Sort(sortutil.Int64Slice(handles))
	var b distsql.RequestBuilder
	kvReq, err := b.SetTableHandles(getPhysicalTableID(e.table), handles).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		trace_util_0.Count(_builder_00000, 497)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 494)
	e.resultHandler = &tableResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans))
	if err != nil {
		trace_util_0.Count(_builder_00000, 498)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 495)
	result.Fetch(ctx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (Executor, error) {
	trace_util_0.Count(_builder_00000, 499)
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 502)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 500)
	kvRanges, err := buildKvRangesForIndexJoin(e.ctx, e.physicalTableID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		trace_util_0.Count(_builder_00000, 503)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 501)
	err = e.open(ctx, kvRanges)
	return e, err
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexLookUpReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) (Executor, error) {
	trace_util_0.Count(_builder_00000, 504)
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		trace_util_0.Count(_builder_00000, 507)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 505)
	e.kvRanges, err = buildKvRangesForIndexJoin(e.ctx, getPhysicalTableID(e.table), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		trace_util_0.Count(_builder_00000, 508)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 506)
	err = e.open(ctx)
	return e, err
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(ctx sessionctx.Context, tableID, indexID int64, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.ColWithCmpFuncManager) ([]kv.KeyRange, error) {
	trace_util_0.Count(_builder_00000, 509)
	kvRanges := make([]kv.KeyRange, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	sc := ctx.GetSessionVars().StmtCtx
	for _, content := range lookUpContents {
		trace_util_0.Count(_builder_00000, 512)
		for _, ran := range ranges {
			trace_util_0.Count(_builder_00000, 516)
			for keyOff, idxOff := range keyOff2IdxOff {
				trace_util_0.Count(_builder_00000, 517)
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		trace_util_0.Count(_builder_00000, 513)
		if cwc != nil {
			trace_util_0.Count(_builder_00000, 518)
			nextColRanges, err := cwc.BuildRangesByRow(ctx, content.row)
			if err != nil {
				trace_util_0.Count(_builder_00000, 521)
				return nil, err
			}
			trace_util_0.Count(_builder_00000, 519)
			for _, nextColRan := range nextColRanges {
				trace_util_0.Count(_builder_00000, 522)
				for _, ran := range ranges {
					trace_util_0.Count(_builder_00000, 525)
					ran.LowVal[lastPos] = nextColRan.LowVal[0]
					ran.HighVal[lastPos] = nextColRan.HighVal[0]
					ran.LowExclude = nextColRan.LowExclude
					ran.HighExclude = nextColRan.HighExclude
				}
				trace_util_0.Count(_builder_00000, 523)
				tmpKvRanges, err := distsql.IndexRangesToKVRanges(sc, tableID, indexID, ranges, nil)
				if err != nil {
					trace_util_0.Count(_builder_00000, 526)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_builder_00000, 524)
				kvRanges = append(kvRanges, tmpKvRanges...)
			}
			trace_util_0.Count(_builder_00000, 520)
			continue
		}

		trace_util_0.Count(_builder_00000, 514)
		tmpKvRanges, err := distsql.IndexRangesToKVRanges(sc, tableID, indexID, ranges, nil)
		if err != nil {
			trace_util_0.Count(_builder_00000, 527)
			return nil, err
		}
		trace_util_0.Count(_builder_00000, 515)
		kvRanges = append(kvRanges, tmpKvRanges...)
	}
	// kvRanges don't overlap each other. So compare StartKey is enough.
	trace_util_0.Count(_builder_00000, 510)
	sort.Slice(kvRanges, func(i, j int) bool {
		trace_util_0.Count(_builder_00000, 528)
		return bytes.Compare(kvRanges[i].StartKey, kvRanges[j].StartKey) < 0
	})
	trace_util_0.Count(_builder_00000, 511)
	return kvRanges, nil
}

func (b *executorBuilder) buildWindow(v *plannercore.PhysicalWindow) *WindowExec {
	trace_util_0.Count(_builder_00000, 529)
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		trace_util_0.Count(_builder_00000, 535)
		return nil
	}
	trace_util_0.Count(_builder_00000, 530)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		trace_util_0.Count(_builder_00000, 536)
		groupByItems = append(groupByItems, item.Col)
	}
	trace_util_0.Count(_builder_00000, 531)
	orderByCols := make([]*expression.Column, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		trace_util_0.Count(_builder_00000, 537)
		orderByCols = append(orderByCols, item.Col)
	}
	trace_util_0.Count(_builder_00000, 532)
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultColIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	for _, desc := range v.WindowFuncDescs {
		trace_util_0.Count(_builder_00000, 538)
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, desc.Name, desc.Args, false)
		if err != nil {
			trace_util_0.Count(_builder_00000, 540)
			b.err = err
			return nil
		}
		trace_util_0.Count(_builder_00000, 539)
		agg := aggfuncs.BuildWindowFunctions(b.ctx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResults = append(partialResults, agg.AllocPartialResult())
		resultColIdx++
	}
	trace_util_0.Count(_builder_00000, 533)
	var processor windowProcessor
	if v.Frame == nil {
		trace_util_0.Count(_builder_00000, 541)
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else {
		trace_util_0.Count(_builder_00000, 542)
		if v.Frame.Type == ast.Rows {
			trace_util_0.Count(_builder_00000, 543)
			processor = &rowFrameWindowProcessor{
				windowFuncs:    windowFuncs,
				partialResults: partialResults,
				start:          v.Frame.Start,
				end:            v.Frame.End,
			}
		} else {
			trace_util_0.Count(_builder_00000, 544)
			{
				cmpResult := int64(-1)
				if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
					trace_util_0.Count(_builder_00000, 546)
					cmpResult = 1
				}
				trace_util_0.Count(_builder_00000, 545)
				processor = &rangeFrameWindowProcessor{
					windowFuncs:       windowFuncs,
					partialResults:    partialResults,
					start:             v.Frame.Start,
					end:               v.Frame.End,
					orderByCols:       orderByCols,
					expectedCmpResult: cmpResult,
				}
			}
		}
	}
	trace_util_0.Count(_builder_00000, 534)
	return &WindowExec{baseExecutor: base,
		processor:      processor,
		groupChecker:   newGroupChecker(b.ctx.GetSessionVars().StmtCtx, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *executorBuilder) buildSQLBindExec(v *plannercore.SQLBindPlan) Executor {
	trace_util_0.Count(_builder_00000, 547)
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity

	e := &SQLBindExec{
		baseExecutor: base,
		sqlBindOp:    v.SQLBindOp,
		normdOrigSQL: v.NormdOrigSQL,
		bindSQL:      v.BindSQL,
		charset:      v.Charset,
		collation:    v.Collation,
		isGlobal:     v.IsGlobal,
		bindAst:      v.BindStmt,
	}
	return e
}

func getPhysicalTableID(t table.Table) int64 {
	trace_util_0.Count(_builder_00000, 548)
	if p, ok := t.(table.PhysicalTable); ok {
		trace_util_0.Count(_builder_00000, 550)
		return p.GetPhysicalID()
	}
	trace_util_0.Count(_builder_00000, 549)
	return t.Meta().ID
}

var _builder_00000 = "executor/builder.go"
