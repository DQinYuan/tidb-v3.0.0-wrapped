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

type maxMinFunction struct {
	aggFunction
	isMax bool
}

// GetResult implements Aggregation interface.
func (mmf *maxMinFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	trace_util_0.Count(_max_min_00000, 0)
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (mmf *maxMinFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_max_min_00000, 1)
	return []types.Datum{mmf.GetResult(evalCtx)}
}

// Update implements Aggregation interface.
func (mmf *maxMinFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_max_min_00000, 2)
	a := mmf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		trace_util_0.Count(_max_min_00000, 8)
		return err
	}
	trace_util_0.Count(_max_min_00000, 3)
	if evalCtx.Value.IsNull() {
		trace_util_0.Count(_max_min_00000, 9)
		evalCtx.Value = *(&value).Copy()
	}
	trace_util_0.Count(_max_min_00000, 4)
	if value.IsNull() {
		trace_util_0.Count(_max_min_00000, 10)
		return nil
	}
	trace_util_0.Count(_max_min_00000, 5)
	var c int
	c, err = evalCtx.Value.CompareDatum(sc, &value)
	if err != nil {
		trace_util_0.Count(_max_min_00000, 11)
		return err
	}
	trace_util_0.Count(_max_min_00000, 6)
	if (mmf.isMax && c == -1) || (!mmf.isMax && c == 1) {
		trace_util_0.Count(_max_min_00000, 12)
		evalCtx.Value = *(&value).Copy()
	}
	trace_util_0.Count(_max_min_00000, 7)
	return nil
}

var _max_min_00000 = "expression/aggregation/max_min.go"
