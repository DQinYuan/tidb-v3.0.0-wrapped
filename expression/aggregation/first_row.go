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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type firstRowFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (ff *firstRowFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_first_row_00000, 0)
	if evalCtx.GotFirstRow {
		trace_util_0.Count(_first_row_00000, 4)
		return nil
	}
	trace_util_0.Count(_first_row_00000, 1)
	if len(ff.Args) != 1 {
		trace_util_0.Count(_first_row_00000, 5)
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	trace_util_0.Count(_first_row_00000, 2)
	value, err := ff.Args[0].Eval(row)
	if err != nil {
		trace_util_0.Count(_first_row_00000, 6)
		return err
	}
	trace_util_0.Count(_first_row_00000, 3)
	evalCtx.Value = types.CloneDatum(value)
	evalCtx.GotFirstRow = true
	return nil
}

// GetResult implements Aggregation interface.
func (ff *firstRowFunction) GetResult(evalCtx *AggEvaluateContext) types.Datum {
	trace_util_0.Count(_first_row_00000, 7)
	return evalCtx.Value
}

func (ff *firstRowFunction) ResetContext(_ *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_first_row_00000, 8)
	evalCtx.GotFirstRow = false
}

// GetPartialResult implements Aggregation interface.
func (ff *firstRowFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_first_row_00000, 9)
	return []types.Datum{ff.GetResult(evalCtx)}
}

var _first_row_00000 = "expression/aggregation/first_row.go"
