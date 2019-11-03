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

package aggregation

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tipb/go-tipb"
)

// AggFuncToPBExpr converts aggregate function to pb.
func AggFuncToPBExpr(sc *stmtctx.StatementContext, client kv.Client, aggFunc *AggFuncDesc) *tipb.Expr {
	trace_util_0.Count(_agg_to_pb_00000, 0)
	if aggFunc.HasDistinct {
		trace_util_0.Count(_agg_to_pb_00000, 5)
		return nil
	}
	trace_util_0.Count(_agg_to_pb_00000, 1)
	pc := expression.NewPBConverter(client, sc)
	var tp tipb.ExprType
	switch aggFunc.Name {
	case ast.AggFuncCount:
		trace_util_0.Count(_agg_to_pb_00000, 6)
		tp = tipb.ExprType_Count
	case ast.AggFuncFirstRow:
		trace_util_0.Count(_agg_to_pb_00000, 7)
		tp = tipb.ExprType_First
	case ast.AggFuncGroupConcat:
		trace_util_0.Count(_agg_to_pb_00000, 8)
		tp = tipb.ExprType_GroupConcat
	case ast.AggFuncMax:
		trace_util_0.Count(_agg_to_pb_00000, 9)
		tp = tipb.ExprType_Max
	case ast.AggFuncMin:
		trace_util_0.Count(_agg_to_pb_00000, 10)
		tp = tipb.ExprType_Min
	case ast.AggFuncSum:
		trace_util_0.Count(_agg_to_pb_00000, 11)
		tp = tipb.ExprType_Sum
	case ast.AggFuncAvg:
		trace_util_0.Count(_agg_to_pb_00000, 12)
		tp = tipb.ExprType_Avg
	case ast.AggFuncBitOr:
		trace_util_0.Count(_agg_to_pb_00000, 13)
		tp = tipb.ExprType_Agg_BitOr
	case ast.AggFuncBitXor:
		trace_util_0.Count(_agg_to_pb_00000, 14)
		tp = tipb.ExprType_Agg_BitXor
	case ast.AggFuncBitAnd:
		trace_util_0.Count(_agg_to_pb_00000, 15)
		tp = tipb.ExprType_Agg_BitAnd
	}
	trace_util_0.Count(_agg_to_pb_00000, 2)
	if !client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		trace_util_0.Count(_agg_to_pb_00000, 16)
		return nil
	}

	trace_util_0.Count(_agg_to_pb_00000, 3)
	children := make([]*tipb.Expr, 0, len(aggFunc.Args))
	for _, arg := range aggFunc.Args {
		trace_util_0.Count(_agg_to_pb_00000, 17)
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			trace_util_0.Count(_agg_to_pb_00000, 19)
			return nil
		}
		trace_util_0.Count(_agg_to_pb_00000, 18)
		children = append(children, pbArg)
	}
	trace_util_0.Count(_agg_to_pb_00000, 4)
	return &tipb.Expr{Tp: tp, Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp)}
}

var _agg_to_pb_00000 = "expression/aggregation/agg_to_pb.go"
