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
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

func (h *rpcHandler) handleCopAnalyzeRequest(req *coprocessor.Request) *coprocessor.Response {
	trace_util_0.Count(_analyze_00000, 0)
	resp := &coprocessor.Response{}
	if len(req.Ranges) == 0 {
		trace_util_0.Count(_analyze_00000, 7)
		return resp
	}
	trace_util_0.Count(_analyze_00000, 1)
	if req.GetTp() != kv.ReqTypeAnalyze {
		trace_util_0.Count(_analyze_00000, 8)
		return resp
	}
	trace_util_0.Count(_analyze_00000, 2)
	if err := h.checkRequestContext(req.GetContext()); err != nil {
		trace_util_0.Count(_analyze_00000, 9)
		resp.RegionError = err
		return resp
	}
	trace_util_0.Count(_analyze_00000, 3)
	analyzeReq := new(tipb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 10)
		resp.OtherError = err.Error()
		return resp
	}
	trace_util_0.Count(_analyze_00000, 4)
	if analyzeReq.Tp == tipb.AnalyzeType_TypeIndex {
		trace_util_0.Count(_analyze_00000, 11)
		resp, err = h.handleAnalyzeIndexReq(req, analyzeReq)
	} else {
		trace_util_0.Count(_analyze_00000, 12)
		{
			resp, err = h.handleAnalyzeColumnsReq(req, analyzeReq)
		}
	}
	trace_util_0.Count(_analyze_00000, 5)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 13)
		resp.OtherError = err.Error()
	}
	trace_util_0.Count(_analyze_00000, 6)
	return resp
}

func (h *rpcHandler) handleAnalyzeIndexReq(req *coprocessor.Request, analyzeReq *tipb.AnalyzeReq) (*coprocessor.Response, error) {
	trace_util_0.Count(_analyze_00000, 14)
	ranges, err := h.extractKVRanges(req.Ranges, false)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 20)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 15)
	e := &indexScanExec{
		colsLen:        int(analyzeReq.IdxReq.NumColumns),
		kvRanges:       ranges,
		startTS:        analyzeReq.StartTs,
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
		IndexScan:      &tipb.IndexScan{Desc: false},
		execDetail:     new(execDetail),
	}
	statsBuilder := statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob))
	var cms *statistics.CMSketch
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		trace_util_0.Count(_analyze_00000, 21)
		cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	trace_util_0.Count(_analyze_00000, 16)
	ctx := context.TODO()
	var values [][]byte
	for {
		trace_util_0.Count(_analyze_00000, 22)
		values, err = e.Next(ctx)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 26)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_analyze_00000, 23)
		if values == nil {
			trace_util_0.Count(_analyze_00000, 27)
			break
		}
		trace_util_0.Count(_analyze_00000, 24)
		var value []byte
		for _, val := range values {
			trace_util_0.Count(_analyze_00000, 28)
			value = append(value, val...)
			if cms != nil {
				trace_util_0.Count(_analyze_00000, 29)
				cms.InsertBytes(value)
			}
		}
		trace_util_0.Count(_analyze_00000, 25)
		err = statsBuilder.Iterate(types.NewBytesDatum(value))
		if err != nil {
			trace_util_0.Count(_analyze_00000, 30)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_analyze_00000, 17)
	hg := statistics.HistogramToProto(statsBuilder.Hist())
	var cm *tipb.CMSketch
	if cms != nil {
		trace_util_0.Count(_analyze_00000, 31)
		cm = statistics.CMSketchToProto(cms)
	}
	trace_util_0.Count(_analyze_00000, 18)
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		trace_util_0.Count(_analyze_00000, 32)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 19)
	return &coprocessor.Response{Data: data}, nil
}

type analyzeColumnsExec struct {
	tblExec *tableScanExec
	fields  []*ast.ResultField
}

func (h *rpcHandler) handleAnalyzeColumnsReq(req *coprocessor.Request, analyzeReq *tipb.AnalyzeReq) (_ *coprocessor.Response, err error) {
	trace_util_0.Count(_analyze_00000, 33)
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone, err = constructTimeZone("", int(analyzeReq.TimeZoneOffset))
	if err != nil {
		trace_util_0.Count(_analyze_00000, 44)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_analyze_00000, 34)
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.ColReq.ColumnsInfo
	evalCtx.setColumnInfo(columns)
	ranges, err := h.extractKVRanges(req.Ranges, false)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 45)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 35)
	e := &analyzeColumnsExec{
		tblExec: &tableScanExec{
			TableScan:      &tipb.TableScan{Columns: columns},
			kvRanges:       ranges,
			colIDs:         evalCtx.colIDs,
			startTS:        analyzeReq.GetStartTs(),
			isolationLevel: h.isolationLevel,
			mvccStore:      h.mvccStore,
			execDetail:     new(execDetail),
		},
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		trace_util_0.Count(_analyze_00000, 46)
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = types.FieldType{Tp: mysql.TypeBlob, Flen: mysql.MaxBlobWidth, Charset: mysql.DefaultCharset, Collate: mysql.DefaultCollationName}
		e.fields[i] = rf
	}

	trace_util_0.Count(_analyze_00000, 36)
	pkID := int64(-1)
	numCols := len(columns)
	if columns[0].GetPkHandle() {
		trace_util_0.Count(_analyze_00000, 47)
		pkID = columns[0].ColumnId
		numCols--
	}
	trace_util_0.Count(_analyze_00000, 37)
	colReq := analyzeReq.ColReq
	builder := statistics.SampleBuilder{
		Sc:              sc,
		RecordSet:       e,
		ColLen:          numCols,
		MaxBucketSize:   colReq.BucketSize,
		MaxFMSketchSize: colReq.SketchSize,
		MaxSampleSize:   colReq.SampleSize,
	}
	if pkID != -1 {
		trace_util_0.Count(_analyze_00000, 48)
		builder.PkBuilder = statistics.NewSortedBuilder(sc, builder.MaxBucketSize, pkID, types.NewFieldType(mysql.TypeBlob))
	}
	trace_util_0.Count(_analyze_00000, 38)
	if colReq.CmsketchWidth != nil && colReq.CmsketchDepth != nil {
		trace_util_0.Count(_analyze_00000, 49)
		builder.CMSketchWidth = *colReq.CmsketchWidth
		builder.CMSketchDepth = *colReq.CmsketchDepth
	}
	trace_util_0.Count(_analyze_00000, 39)
	collectors, pkBuilder, err := builder.CollectColumnStats()
	if err != nil {
		trace_util_0.Count(_analyze_00000, 50)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 40)
	colResp := &tipb.AnalyzeColumnsResp{}
	if pkID != -1 {
		trace_util_0.Count(_analyze_00000, 51)
		colResp.PkHist = statistics.HistogramToProto(pkBuilder.Hist())
	}
	trace_util_0.Count(_analyze_00000, 41)
	for _, c := range collectors {
		trace_util_0.Count(_analyze_00000, 52)
		colResp.Collectors = append(colResp.Collectors, statistics.SampleCollectorToProto(c))
	}
	trace_util_0.Count(_analyze_00000, 42)
	data, err := proto.Marshal(colResp)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 53)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 43)
	return &coprocessor.Response{Data: data}, nil
}

// Fields implements the sqlexec.RecordSet Fields interface.
func (e *analyzeColumnsExec) Fields() []*ast.ResultField {
	trace_util_0.Count(_analyze_00000, 54)
	return e.fields
}

func (e *analyzeColumnsExec) getNext(ctx context.Context) ([]types.Datum, error) {
	trace_util_0.Count(_analyze_00000, 55)
	values, err := e.tblExec.Next(ctx)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 59)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 56)
	if values == nil {
		trace_util_0.Count(_analyze_00000, 60)
		return nil, nil
	}
	trace_util_0.Count(_analyze_00000, 57)
	datumRow := make([]types.Datum, 0, len(values))
	for _, val := range values {
		trace_util_0.Count(_analyze_00000, 61)
		d := types.NewBytesDatum(val)
		if len(val) == 1 && val[0] == codec.NilFlag {
			trace_util_0.Count(_analyze_00000, 63)
			d.SetNull()
		}
		trace_util_0.Count(_analyze_00000, 62)
		datumRow = append(datumRow, d)
	}
	trace_util_0.Count(_analyze_00000, 58)
	return datumRow, nil
}

func (e *analyzeColumnsExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_analyze_00000, 64)
	req.Reset()
	row, err := e.getNext(ctx)
	if row == nil || err != nil {
		trace_util_0.Count(_analyze_00000, 67)
		return errors.Trace(err)
	}
	trace_util_0.Count(_analyze_00000, 65)
	for i := 0; i < len(row); i++ {
		trace_util_0.Count(_analyze_00000, 68)
		req.AppendDatum(i, &row[i])
	}
	trace_util_0.Count(_analyze_00000, 66)
	return nil
}

func (e *analyzeColumnsExec) NewRecordBatch() *chunk.RecordBatch {
	trace_util_0.Count(_analyze_00000, 69)
	fields := make([]*types.FieldType, 0, len(e.fields))
	for _, field := range e.fields {
		trace_util_0.Count(_analyze_00000, 71)
		fields = append(fields, &field.Column.FieldType)
	}
	trace_util_0.Count(_analyze_00000, 70)
	return chunk.NewRecordBatch(chunk.NewChunkWithCapacity(fields, 1))
}

// Close implements the sqlexec.RecordSet Close interface.
func (e *analyzeColumnsExec) Close() error {
	trace_util_0.Count(_analyze_00000, 72)
	return nil
}

var _analyze_00000 = "store/mockstore/mocktikv/analyze.go"
