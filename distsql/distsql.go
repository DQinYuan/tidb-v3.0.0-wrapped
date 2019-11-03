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

package distsql

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// XAPI error codes.
const (
	codeInvalidResp = 1
)

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
	trace_util_0.Count(_distsql_00000, 0)
	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		trace_util_0.Count(_distsql_00000, 6)
		hook.(func(*kv.Request))(kvReq)
	}

	trace_util_0.Count(_distsql_00000, 1)
	if !sctx.GetSessionVars().EnableStreaming {
		trace_util_0.Count(_distsql_00000, 7)
		kvReq.Streaming = false
	}
	trace_util_0.Count(_distsql_00000, 2)
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars)
	if resp == nil {
		trace_util_0.Count(_distsql_00000, 8)
		err := errors.New("client returns nil response")
		return nil, err
	}

	// kvReq.MemTracker is used to trace and control memory usage in DistSQL layer;
	// for streamResult, since it is a pipeline which has no buffer, it's not necessary to trace it;
	// for selectResult, we just use the kvReq.MemTracker prepared for co-processor
	// instead of creating a new one for simplification.
	trace_util_0.Count(_distsql_00000, 3)
	if kvReq.Streaming {
		trace_util_0.Count(_distsql_00000, 9)
		return &streamResult{
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        sctx,
			feedback:   fb,
		}, nil
	}

	trace_util_0.Count(_distsql_00000, 4)
	label := metrics.LblGeneral
	if sctx.GetSessionVars().InRestrictedSQL {
		trace_util_0.Count(_distsql_00000, 10)
		label = metrics.LblInternal
	}
	trace_util_0.Count(_distsql_00000, 5)
	return &selectResult{
		label:      "dag",
		resp:       resp,
		results:    make(chan resultWithErr, kvReq.Concurrency),
		closed:     make(chan struct{}),
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   fb,
		sqlType:    label,
		memTracker: kvReq.MemTracker,
	}, nil
}

// SelectWithRuntimeStats sends a DAG request, returns SelectResult.
// The difference from Select is that SelectWithRuntimeStats will set copPlanIDs into selectResult,
// which can help selectResult to collect runtime stats.
func SelectWithRuntimeStats(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []fmt.Stringer) (SelectResult, error) {
	trace_util_0.Count(_distsql_00000, 11)
	sr, err := Select(ctx, sctx, kvReq, fieldTypes, fb)
	if err != nil {
		trace_util_0.Count(_distsql_00000, 14)
		return sr, err
	}
	trace_util_0.Count(_distsql_00000, 12)
	if selectResult, ok := sr.(*selectResult); ok {
		trace_util_0.Count(_distsql_00000, 15)
		selectResult.copPlanIDs = copPlanIDs
	}
	trace_util_0.Count(_distsql_00000, 13)
	return sr, err
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables,
	isRestrict bool) (SelectResult, error) {
	trace_util_0.Count(_distsql_00000, 16)
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		trace_util_0.Count(_distsql_00000, 19)
		return nil, errors.New("client returns nil response")
	}
	trace_util_0.Count(_distsql_00000, 17)
	label := metrics.LblGeneral
	if isRestrict {
		trace_util_0.Count(_distsql_00000, 20)
		label = metrics.LblInternal
	}
	trace_util_0.Count(_distsql_00000, 18)
	result := &selectResult{
		label:    "analyze",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:  label,
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	trace_util_0.Count(_distsql_00000, 21)
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		trace_util_0.Count(_distsql_00000, 23)
		return nil, errors.New("client returns nil response")
	}
	trace_util_0.Count(_distsql_00000, 22)
	result := &selectResult{
		label:    "checksum",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:  metrics.LblGeneral,
	}
	return result, nil
}

var _distsql_00000 = "distsql/distsql.go"
