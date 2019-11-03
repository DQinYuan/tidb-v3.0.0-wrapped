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

type countFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (cf *countFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_count_00000, 0)
	var datumBuf []types.Datum
	if cf.HasDistinct {
		trace_util_0.Count(_count_00000, 5)
		datumBuf = make([]types.Datum, 0, len(cf.Args))
	}
	trace_util_0.Count(_count_00000, 1)
	for _, a := range cf.Args {
		trace_util_0.Count(_count_00000, 6)
		value, err := a.Eval(row)
		if err != nil {
			trace_util_0.Count(_count_00000, 10)
			return err
		}
		trace_util_0.Count(_count_00000, 7)
		if value.IsNull() {
			trace_util_0.Count(_count_00000, 11)
			return nil
		}
		trace_util_0.Count(_count_00000, 8)
		if cf.Mode == FinalMode || cf.Mode == Partial2Mode {
			trace_util_0.Count(_count_00000, 12)
			evalCtx.Count += value.GetInt64()
		}
		trace_util_0.Count(_count_00000, 9)
		if cf.HasDistinct {
			trace_util_0.Count(_count_00000, 13)
			datumBuf = append(datumBuf, value)
		}
	}
	trace_util_0.Count(_count_00000, 2)
	if cf.HasDistinct {
		trace_util_0.Count(_count_00000, 14)
		d, err := evalCtx.DistinctChecker.Check(datumBuf)
		if err != nil {
			trace_util_0.Count(_count_00000, 16)
			return err
		}
		trace_util_0.Count(_count_00000, 15)
		if !d {
			trace_util_0.Count(_count_00000, 17)
			return nil
		}
	}
	trace_util_0.Count(_count_00000, 3)
	if cf.Mode == CompleteMode || cf.Mode == Partial1Mode {
		trace_util_0.Count(_count_00000, 18)
		evalCtx.Count++
	}
	trace_util_0.Count(_count_00000, 4)
	return nil
}

func (cf *countFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_count_00000, 19)
	if cf.HasDistinct {
		trace_util_0.Count(_count_00000, 21)
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	trace_util_0.Count(_count_00000, 20)
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (cf *countFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	trace_util_0.Count(_count_00000, 22)
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_count_00000, 23)
	return []types.Datum{cf.GetResult(evalCtx)}
}

var _count_00000 = "expression/aggregation/count.go"
