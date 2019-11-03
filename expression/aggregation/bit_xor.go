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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type bitXorFunction struct {
	aggFunction
}

func (bf *bitXorFunction) CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext {
	trace_util_0.Count(_bit_xor_00000, 0)
	evalCtx := bf.aggFunction.CreateContext(sc)
	evalCtx.Value.SetUint64(0)
	return evalCtx
}

func (bf *bitXorFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_bit_xor_00000, 1)
	evalCtx.Value.SetUint64(0)
}

// Update implements Aggregation interface.
func (bf *bitXorFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_bit_xor_00000, 2)
	a := bf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		trace_util_0.Count(_bit_xor_00000, 5)
		return err
	}
	trace_util_0.Count(_bit_xor_00000, 3)
	if !value.IsNull() {
		trace_util_0.Count(_bit_xor_00000, 6)
		if value.Kind() == types.KindUint64 {
			trace_util_0.Count(_bit_xor_00000, 7)
			evalCtx.Value.SetUint64(evalCtx.Value.GetUint64() ^ value.GetUint64())
		} else {
			trace_util_0.Count(_bit_xor_00000, 8)
			{
				int64Value, err := value.ToInt64(sc)
				if err != nil {
					trace_util_0.Count(_bit_xor_00000, 10)
					return err
				}
				trace_util_0.Count(_bit_xor_00000, 9)
				evalCtx.Value.SetUint64(evalCtx.Value.GetUint64() ^ uint64(int64Value))
			}
		}
	}
	trace_util_0.Count(_bit_xor_00000, 4)
	return nil
}

// GetResult implements Aggregation interface.
func (bf *bitXorFunction) GetResult(evalCtx *AggEvaluateContext) types.Datum {
	trace_util_0.Count(_bit_xor_00000, 11)
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (bf *bitXorFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_bit_xor_00000, 12)
	return []types.Datum{bf.GetResult(evalCtx)}
}

var _bit_xor_00000 = "expression/aggregation/bit_xor.go"
