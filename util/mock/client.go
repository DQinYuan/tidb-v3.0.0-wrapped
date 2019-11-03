// Copyright 2018 PingCAP, Inc.
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

package mock

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tipb/go-tipb"
)

// Client implement kv.Client interface, mocked from "CopClient" defined in
// "store/tikv/copprocessor.go".
type Client struct {
	MockResponse kv.Response
}

// Send implement kv.Client interface.
func (c *Client) Send(ctx context.Context, req *kv.Request, kv *kv.Variables) kv.Response {
	trace_util_0.Count(_client_00000, 0)
	return c.MockResponse
}

// IsRequestTypeSupported implement kv.Client interface.
func (c *Client) IsRequestTypeSupported(reqType, subType int64) bool {
	trace_util_0.Count(_client_00000, 1)
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		trace_util_0.Count(_client_00000, 3)
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			trace_util_0.Count(_client_00000, 6)
			return true
		default:
			trace_util_0.Count(_client_00000, 7)
			return c.supportExpr(tipb.ExprType(subType))
		}
	case kv.ReqTypeDAG:
		trace_util_0.Count(_client_00000, 4)
		return c.supportExpr(tipb.ExprType(subType))
	case kv.ReqTypeAnalyze:
		trace_util_0.Count(_client_00000, 5)
		return true
	}
	trace_util_0.Count(_client_00000, 2)
	return false
}

func (c *Client) supportExpr(exprType tipb.ExprType) bool {
	trace_util_0.Count(_client_00000, 8)
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef:
		trace_util_0.Count(_client_00000, 9)
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr:
		trace_util_0.Count(_client_00000, 10)
		return true
	case kv.ReqSubTypeDesc:
		trace_util_0.Count(_client_00000, 11)
		return true
	case kv.ReqSubTypeSignature:
		trace_util_0.Count(_client_00000, 12)
		return true
	default:
		trace_util_0.Count(_client_00000, 13)
		return false
	}
}

var _client_00000 = "util/mock/client.go"
