// Copyright 2017 PingCAP, Inc.
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

package mocktikv

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	mockpkg "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	evalCtx   *evalContext
}

func (h *rpcHandler) handleCopDAGRequest(req *coprocessor.Request) *coprocessor.Response {
	trace_util_0.Count(_cop_handler_dag_00000, 0)
	resp := &coprocessor.Response{}
	if err := h.checkRequestContext(req.GetContext()); err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 5)
		resp.RegionError = err
		return resp
	}
	trace_util_0.Count(_cop_handler_dag_00000, 1)
	dagCtx, e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 6)
		resp.OtherError = err.Error()
		return resp
	}

	trace_util_0.Count(_cop_handler_dag_00000, 2)
	var (
		chunks []tipb.Chunk
		rowCnt int
	)
	ctx := context.TODO()
	for {
		trace_util_0.Count(_cop_handler_dag_00000, 7)
		var row [][]byte
		row, err = e.Next(ctx)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 11)
			break
		}
		trace_util_0.Count(_cop_handler_dag_00000, 8)
		if row == nil {
			trace_util_0.Count(_cop_handler_dag_00000, 12)
			break
		}
		trace_util_0.Count(_cop_handler_dag_00000, 9)
		data := dummySlice
		for _, offset := range dagReq.OutputOffsets {
			trace_util_0.Count(_cop_handler_dag_00000, 13)
			data = append(data, row[offset]...)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 10)
		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	trace_util_0.Count(_cop_handler_dag_00000, 3)
	warnings := dagCtx.evalCtx.sc.GetWarnings()

	var execDetails []*execDetail
	if dagReq.CollectExecutionSummaries != nil && *dagReq.CollectExecutionSummaries {
		trace_util_0.Count(_cop_handler_dag_00000, 14)
		execDetails = e.ExecDetails()
	}
	trace_util_0.Count(_cop_handler_dag_00000, 4)
	return buildResp(chunks, e.Counts(), execDetails, err, warnings)
}

func (h *rpcHandler) buildDAGExecutor(req *coprocessor.Request) (*dagContext, executor, *tipb.DAGRequest, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 15)
	if len(req.Ranges) == 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 21)
		return nil, nil, nil, errors.New("request range is null")
	}
	trace_util_0.Count(_cop_handler_dag_00000, 16)
	if req.GetTp() != kv.ReqTypeDAG {
		trace_util_0.Count(_cop_handler_dag_00000, 22)
		return nil, nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	trace_util_0.Count(_cop_handler_dag_00000, 17)
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 23)
		return nil, nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 18)
	sc := flagsToStatementContext(dagReq.Flags)
	sc.TimeZone, err = constructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 24)
		return nil, nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 19)
	ctx := &dagContext{
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		evalCtx:   &evalContext{sc: sc},
	}
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 25)
		return nil, nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 20)
	return ctx, e, dagReq, err
}

// constructTimeZone constructs timezone by name first. When the timezone name
// is set, the daylight saving problem must be considered. Otherwise the
// timezone offset in seconds east of UTC is used to constructed the timezone.
func constructTimeZone(name string, offset int) (*time.Location, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 26)
	if name != "" {
		trace_util_0.Count(_cop_handler_dag_00000, 28)
		return timeutil.LoadLocation(name)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 27)
	return time.FixedZone("", offset), nil
}

func (h *rpcHandler) handleCopStream(ctx context.Context, req *coprocessor.Request) (tikvpb.Tikv_CoprocessorStreamClient, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 29)
	dagCtx, e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 31)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 30)
	return &mockCopStreamClient{
		exec:   e,
		req:    dagReq,
		ctx:    ctx,
		dagCtx: dagCtx,
	}, nil
}

func (h *rpcHandler) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 32)
	var currExec executor
	var err error
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		trace_util_0.Count(_cop_handler_dag_00000, 34)
		currExec, err = h.buildTableScan(ctx, curr)
	case tipb.ExecType_TypeIndexScan:
		trace_util_0.Count(_cop_handler_dag_00000, 35)
		currExec, err = h.buildIndexScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		trace_util_0.Count(_cop_handler_dag_00000, 36)
		currExec, err = h.buildSelection(ctx, curr)
	case tipb.ExecType_TypeAggregation:
		trace_util_0.Count(_cop_handler_dag_00000, 37)
		currExec, err = h.buildHashAgg(ctx, curr)
	case tipb.ExecType_TypeStreamAgg:
		trace_util_0.Count(_cop_handler_dag_00000, 38)
		currExec, err = h.buildStreamAgg(ctx, curr)
	case tipb.ExecType_TypeTopN:
		trace_util_0.Count(_cop_handler_dag_00000, 39)
		currExec, err = h.buildTopN(ctx, curr)
	case tipb.ExecType_TypeLimit:
		trace_util_0.Count(_cop_handler_dag_00000, 40)
		currExec = &limitExec{limit: curr.Limit.GetLimit(), execDetail: new(execDetail)}
	default:
		trace_util_0.Count(_cop_handler_dag_00000, 41)
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	trace_util_0.Count(_cop_handler_dag_00000, 33)
	return currExec, errors.Trace(err)
}

func (h *rpcHandler) buildDAG(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 42)
	var src executor
	for i := 0; i < len(executors); i++ {
		trace_util_0.Count(_cop_handler_dag_00000, 44)
		curr, err := h.buildExec(ctx, executors[i])
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 46)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 45)
		curr.SetSrcExec(src)
		src = curr
	}
	trace_util_0.Count(_cop_handler_dag_00000, 43)
	return src, nil
}

func (h *rpcHandler) buildTableScan(ctx *dagContext, executor *tipb.Executor) (*tableScanExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 47)
	columns := executor.TblScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.TblScan.Desc)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 50)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 48)
	e := &tableScanExec{
		TableScan:      executor.TblScan,
		kvRanges:       ranges,
		colIDs:         ctx.evalCtx.colIDs,
		startTS:        ctx.dagReq.GetStartTs(),
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
		execDetail:     new(execDetail),
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		trace_util_0.Count(_cop_handler_dag_00000, 51)
		e.counts = make([]int64, len(ranges))
	}
	trace_util_0.Count(_cop_handler_dag_00000, 49)
	return e, nil
}

func (h *rpcHandler) buildIndexScan(ctx *dagContext, executor *tipb.Executor) (*indexScanExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 52)
	var err error
	columns := executor.IdxScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	length := len(columns)
	pkStatus := pkColNotExists
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		trace_util_0.Count(_cop_handler_dag_00000, 56)
		if mysql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			trace_util_0.Count(_cop_handler_dag_00000, 58)
			pkStatus = pkColIsUnsigned
		} else {
			trace_util_0.Count(_cop_handler_dag_00000, 59)
			{
				pkStatus = pkColIsSigned
			}
		}
		trace_util_0.Count(_cop_handler_dag_00000, 57)
		columns = columns[:length-1]
	} else {
		trace_util_0.Count(_cop_handler_dag_00000, 60)
		if columns[length-1].ColumnId == model.ExtraHandleID {
			trace_util_0.Count(_cop_handler_dag_00000, 61)
			pkStatus = pkColIsSigned
			columns = columns[:length-1]
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 53)
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.IdxScan.Desc)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 62)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 54)
	e := &indexScanExec{
		IndexScan:      executor.IdxScan,
		kvRanges:       ranges,
		colsLen:        len(columns),
		startTS:        ctx.dagReq.GetStartTs(),
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
		pkStatus:       pkStatus,
		execDetail:     new(execDetail),
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		trace_util_0.Count(_cop_handler_dag_00000, 63)
		e.counts = make([]int64, len(ranges))
	}
	trace_util_0.Count(_cop_handler_dag_00000, 55)
	return e, nil
}

func (h *rpcHandler) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 64)
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		trace_util_0.Count(_cop_handler_dag_00000, 67)
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 68)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 65)
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 69)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 66)
	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h *rpcHandler) getAggInfo(ctx *dagContext, executor *tipb.Executor) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 70)
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		trace_util_0.Count(_cop_handler_dag_00000, 74)
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sc)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 76)
			return nil, nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 75)
		aggs = append(aggs, aggExpr)
		relatedColOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 77)
			return nil, nil, nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 71)
	for _, item := range executor.Aggregation.GroupBy {
		trace_util_0.Count(_cop_handler_dag_00000, 78)
		relatedColOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 79)
			return nil, nil, nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 72)
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, executor.Aggregation.GetGroupBy())
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 80)
		return nil, nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 73)
	return aggs, groupBys, relatedColOffsets, nil
}

func (h *rpcHandler) buildHashAgg(ctx *dagContext, executor *tipb.Executor) (*hashAggExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 81)
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 83)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 82)
	return &hashAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		groupByExprs:      groupBys,
		groups:            make(map[string]struct{}),
		groupKeys:         make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h *rpcHandler) buildStreamAgg(ctx *dagContext, executor *tipb.Executor) (*streamAggExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 84)
	aggs, groupBys, relatedColOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 87)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 85)
	aggCtxs := make([]*aggregation.AggEvaluateContext, 0, len(aggs))
	for _, agg := range aggs {
		trace_util_0.Count(_cop_handler_dag_00000, 88)
		aggCtxs = append(aggCtxs, agg.CreateContext(ctx.evalCtx.sc))
	}

	trace_util_0.Count(_cop_handler_dag_00000, 86)
	return &streamAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		aggCtxs:           aggCtxs,
		groupByExprs:      groupBys,
		currGroupByValues: make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

func (h *rpcHandler) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 89)
	topN := executor.TopN
	var err error
	var relatedColOffsets []int
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		trace_util_0.Count(_cop_handler_dag_00000, 92)
		relatedColOffsets, err = extractOffsetsInExpr(item.Expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 94)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 93)
		pbConds[i] = item.Expr
	}
	trace_util_0.Count(_cop_handler_dag_00000, 90)
	heap := &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sc,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 95)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 91)
	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		execDetail:        new(execDetail),
	}, nil
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sc          *stmtctx.StatementContext
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	trace_util_0.Count(_cop_handler_dag_00000, 96)
	e.columnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.columnInfos, cols)

	e.colIDs = make(map[int64]int)
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		trace_util_0.Count(_cop_handler_dag_00000, 97)
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

// decodeRelatedColumnVals decodes data to Datum slice according to the row information.
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, value [][]byte, row []types.Datum) error {
	trace_util_0.Count(_cop_handler_dag_00000, 98)
	var err error
	for _, offset := range relatedColOffsets {
		trace_util_0.Count(_cop_handler_dag_00000, 100)
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset], e.sc.TimeZone)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 101)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 99)
	return nil
}

// flagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *stmtctx.StatementContext {
	trace_util_0.Count(_cop_handler_dag_00000, 102)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = (flags & model.FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & model.FlagTruncateAsWarning) > 0
	sc.PadCharToFullLength = (flags & model.FlagPadCharToFullLength) > 0
	return sc
}

// MockGRPCClientStream is exported for testing purpose.
func MockGRPCClientStream() grpc.ClientStream {
	trace_util_0.Count(_cop_handler_dag_00000, 103)
	return mockClientStream{}
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

func (mockClientStream) Header() (metadata.MD, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 104)
	return nil, nil
}
func (mockClientStream) Trailer() metadata.MD {
	trace_util_0.Count(_cop_handler_dag_00000, 105)
	return nil
}
func (mockClientStream) CloseSend() error { trace_util_0.Count(_cop_handler_dag_00000, 106); return nil }
func (mockClientStream) Context() context.Context {
	trace_util_0.Count(_cop_handler_dag_00000, 107)
	return nil
}
func (mockClientStream) SendMsg(m interface{}) error {
	trace_util_0.Count(_cop_handler_dag_00000, 108)
	return nil
}
func (mockClientStream) RecvMsg(m interface{}) error {
	trace_util_0.Count(_cop_handler_dag_00000, 109)
	return nil
}

type mockCopStreamClient struct {
	mockClientStream

	req      *tipb.DAGRequest
	exec     executor
	ctx      context.Context
	dagCtx   *dagContext
	finished bool
}

type mockCopStreamErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockCopStreamErrClient) Recv() (*coprocessor.Response, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 110)
	return &coprocessor.Response{
		RegionError: mock.Error,
	}, nil
}

func (mock *mockCopStreamClient) Recv() (*coprocessor.Response, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 111)
	select {
	case <-mock.ctx.Done():
		trace_util_0.Count(_cop_handler_dag_00000, 121)
		return nil, mock.ctx.Err()
	default:
		trace_util_0.Count(_cop_handler_dag_00000, 122)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 112)
	if mock.finished {
		trace_util_0.Count(_cop_handler_dag_00000, 123)
		return nil, io.EOF
	}

	trace_util_0.Count(_cop_handler_dag_00000, 113)
	if hook := mock.ctx.Value(mockpkg.HookKeyForTest("mockTiKVStreamRecvHook")); hook != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 124)
		hook.(func(context.Context))(mock.ctx)
	}

	trace_util_0.Count(_cop_handler_dag_00000, 114)
	var resp coprocessor.Response
	chunk, finish, ran, counts, warnings, err := mock.readBlockFromExecutor()
	resp.Range = ran
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 125)
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			trace_util_0.Count(_cop_handler_dag_00000, 127)
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			trace_util_0.Count(_cop_handler_dag_00000, 128)
			{
				resp.OtherError = err.Error()
			}
		}
		trace_util_0.Count(_cop_handler_dag_00000, 126)
		return &resp, nil
	}
	trace_util_0.Count(_cop_handler_dag_00000, 115)
	if finish {
		trace_util_0.Count(_cop_handler_dag_00000, 129)
		// Just mark it, need to handle the last chunk.
		mock.finished = true
	}

	trace_util_0.Count(_cop_handler_dag_00000, 116)
	data, err := chunk.Marshal()
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 130)
		resp.OtherError = err.Error()
		return &resp, nil
	}
	trace_util_0.Count(_cop_handler_dag_00000, 117)
	var Warnings []*tipb.Error
	if len(warnings) > 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 131)
		Warnings = make([]*tipb.Error, 0, len(warnings))
		for i := range warnings {
			trace_util_0.Count(_cop_handler_dag_00000, 132)
			Warnings = append(Warnings, toPBError(warnings[i].Err))
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 118)
	streamResponse := tipb.StreamResponse{
		Error:    toPBError(err),
		Data:     data,
		Warnings: Warnings,
	}
	// The counts was the output count of each executor, but now it is the scan count of each range,
	// so we need a flag to tell them apart.
	if counts != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 133)
		streamResponse.OutputCounts = make([]int64, 1+len(counts))
		copy(streamResponse.OutputCounts, counts)
		streamResponse.OutputCounts[len(counts)] = -1
	}
	trace_util_0.Count(_cop_handler_dag_00000, 119)
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 134)
		resp.OtherError = err.Error()
	}
	trace_util_0.Count(_cop_handler_dag_00000, 120)
	return &resp, nil
}

func (mock *mockCopStreamClient) readBlockFromExecutor() (tipb.Chunk, bool, *coprocessor.KeyRange, []int64, []stmtctx.SQLWarn, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 135)
	var chunk tipb.Chunk
	var ran coprocessor.KeyRange
	var finish bool
	var desc bool
	mock.exec.ResetCounts()
	ran.Start, desc = mock.exec.Cursor()
	for count := 0; count < rowsPerChunk; count++ {
		trace_util_0.Count(_cop_handler_dag_00000, 138)
		row, err := mock.exec.Next(mock.ctx)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 141)
			ran.End, _ = mock.exec.Cursor()
			return chunk, false, &ran, nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 139)
		if row == nil {
			trace_util_0.Count(_cop_handler_dag_00000, 142)
			finish = true
			break
		}
		trace_util_0.Count(_cop_handler_dag_00000, 140)
		for _, offset := range mock.req.OutputOffsets {
			trace_util_0.Count(_cop_handler_dag_00000, 143)
			chunk.RowsData = append(chunk.RowsData, row[offset]...)
		}
	}

	trace_util_0.Count(_cop_handler_dag_00000, 136)
	ran.End, _ = mock.exec.Cursor()
	if desc {
		trace_util_0.Count(_cop_handler_dag_00000, 144)
		ran.Start, ran.End = ran.End, ran.Start
	}
	trace_util_0.Count(_cop_handler_dag_00000, 137)
	warnings := mock.dagCtx.evalCtx.sc.GetWarnings()
	mock.dagCtx.evalCtx.sc.SetWarnings(nil)
	return chunk, finish, &ran, mock.exec.Counts(), warnings, nil
}

func buildResp(chunks []tipb.Chunk, counts []int64, execDetails []*execDetail, err error, warnings []stmtctx.SQLWarn) *coprocessor.Response {
	trace_util_0.Count(_cop_handler_dag_00000, 145)
	resp := &coprocessor.Response{}
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		Chunks:       chunks,
		OutputCounts: counts,
	}
	if len(execDetails) > 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 150)
		execSummary := make([]*tipb.ExecutorExecutionSummary, 0, len(execDetails))
		for _, d := range execDetails {
			trace_util_0.Count(_cop_handler_dag_00000, 152)
			costNs := uint64(d.timeProcessed / time.Nanosecond)
			rows := uint64(d.numProducedRows)
			numIter := uint64(d.numIterations)
			execSummary = append(execSummary, &tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &costNs,
				NumProducedRows: &rows,
				NumIterations:   &numIter,
			})
		}
		trace_util_0.Count(_cop_handler_dag_00000, 151)
		selResp.ExecutionSummaries = execSummary
	}
	trace_util_0.Count(_cop_handler_dag_00000, 146)
	if len(warnings) > 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 153)
		selResp.Warnings = make([]*tipb.Error, 0, len(warnings))
		for i := range warnings {
			trace_util_0.Count(_cop_handler_dag_00000, 154)
			selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 147)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 155)
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			trace_util_0.Count(_cop_handler_dag_00000, 156)
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			trace_util_0.Count(_cop_handler_dag_00000, 157)
			{
				resp.OtherError = err.Error()
			}
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 148)
	data, err := proto.Marshal(selResp)
	if err != nil {
		trace_util_0.Count(_cop_handler_dag_00000, 158)
		resp.OtherError = err.Error()
		return resp
	}
	trace_util_0.Count(_cop_handler_dag_00000, 149)
	resp.Data = data
	return resp
}

func toPBError(err error) *tipb.Error {
	trace_util_0.Count(_cop_handler_dag_00000, 159)
	if err == nil {
		trace_util_0.Count(_cop_handler_dag_00000, 162)
		return nil
	}
	trace_util_0.Count(_cop_handler_dag_00000, 160)
	perr := new(tipb.Error)
	switch x := err.(type) {
	case *terror.Error:
		trace_util_0.Count(_cop_handler_dag_00000, 163)
		sqlErr := x.ToSQLError()
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		trace_util_0.Count(_cop_handler_dag_00000, 164)
		perr.Code = int32(1)
		perr.Msg = err.Error()
	}
	trace_util_0.Count(_cop_handler_dag_00000, 161)
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func (h *rpcHandler) extractKVRanges(keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange, err error) {
	trace_util_0.Count(_cop_handler_dag_00000, 165)
	for _, kran := range keyRanges {
		trace_util_0.Count(_cop_handler_dag_00000, 168)
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			trace_util_0.Count(_cop_handler_dag_00000, 172)
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		trace_util_0.Count(_cop_handler_dag_00000, 169)
		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.rawStartKey) <= 0 {
			trace_util_0.Count(_cop_handler_dag_00000, 173)
			continue
		}
		trace_util_0.Count(_cop_handler_dag_00000, 170)
		lowerKey := kran.GetStart()
		if len(h.rawEndKey) != 0 && bytes.Compare(lowerKey, h.rawEndKey) >= 0 {
			trace_util_0.Count(_cop_handler_dag_00000, 174)
			break
		}
		trace_util_0.Count(_cop_handler_dag_00000, 171)
		var kvr kv.KeyRange
		kvr.StartKey = kv.Key(maxStartKey(lowerKey, h.rawStartKey))
		kvr.EndKey = kv.Key(minEndKey(upperKey, h.rawEndKey))
		kvRanges = append(kvRanges, kvr)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 166)
	if descScan {
		trace_util_0.Count(_cop_handler_dag_00000, 175)
		reverseKVRanges(kvRanges)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 167)
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	trace_util_0.Count(_cop_handler_dag_00000, 176)
	for i := 0; i < len(kvRanges)/2; i++ {
		trace_util_0.Count(_cop_handler_dag_00000, 177)
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	trace_util_0.Count(_cop_handler_dag_00000, 178)
	if rowCnt%rowsPerChunk == 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 180)
		chunks = append(chunks, tipb.Chunk{})
	}
	trace_util_0.Count(_cop_handler_dag_00000, 179)
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func maxStartKey(rangeStartKey kv.Key, regionStartKey []byte) []byte {
	trace_util_0.Count(_cop_handler_dag_00000, 181)
	if bytes.Compare([]byte(rangeStartKey), regionStartKey) > 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 183)
		return []byte(rangeStartKey)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 182)
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	trace_util_0.Count(_cop_handler_dag_00000, 184)
	if len(regionEndKey) == 0 || bytes.Compare([]byte(rangeEndKey), regionEndKey) < 0 {
		trace_util_0.Count(_cop_handler_dag_00000, 186)
		return []byte(rangeEndKey)
	}
	trace_util_0.Count(_cop_handler_dag_00000, 185)
	return regionEndKey
}

func isDuplicated(offsets []int, offset int) bool {
	trace_util_0.Count(_cop_handler_dag_00000, 187)
	for _, idx := range offsets {
		trace_util_0.Count(_cop_handler_dag_00000, 189)
		if idx == offset {
			trace_util_0.Count(_cop_handler_dag_00000, 190)
			return true
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 188)
	return false
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	trace_util_0.Count(_cop_handler_dag_00000, 191)
	if expr == nil {
		trace_util_0.Count(_cop_handler_dag_00000, 195)
		return nil, nil
	}
	trace_util_0.Count(_cop_handler_dag_00000, 192)
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		trace_util_0.Count(_cop_handler_dag_00000, 196)
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 199)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_cop_handler_dag_00000, 197)
		if !isDuplicated(collector, int(idx)) {
			trace_util_0.Count(_cop_handler_dag_00000, 200)
			collector = append(collector, int(idx))
		}
		trace_util_0.Count(_cop_handler_dag_00000, 198)
		return collector, nil
	}
	trace_util_0.Count(_cop_handler_dag_00000, 193)
	var err error
	for _, child := range expr.Children {
		trace_util_0.Count(_cop_handler_dag_00000, 201)
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			trace_util_0.Count(_cop_handler_dag_00000, 202)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_cop_handler_dag_00000, 194)
	return collector, nil
}

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	trace_util_0.Count(_cop_handler_dag_00000, 203)
	return &types.FieldType{
		Tp:      byte(col.GetTp()),
		Flag:    uint(col.Flag),
		Flen:    int(col.GetColumnLen()),
		Decimal: int(col.GetDecimal()),
		Elems:   col.Elems,
		Collate: mysql.Collations[uint8(col.GetCollation())],
	}
}

var _cop_handler_dag_00000 = "store/mockstore/mocktikv/cop_handler_dag.go"
