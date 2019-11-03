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
	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type avgFunction struct {
	aggFunction
}

func (af *avgFunction) updateAvg(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	trace_util_0.Count(_avg_00000, 0)
	a := af.Args[1]
	value, err := a.Eval(row)
	if err != nil {
		trace_util_0.Count(_avg_00000, 5)
		return err
	}
	trace_util_0.Count(_avg_00000, 1)
	if value.IsNull() {
		trace_util_0.Count(_avg_00000, 6)
		return nil
	}
	trace_util_0.Count(_avg_00000, 2)
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		trace_util_0.Count(_avg_00000, 7)
		return err
	}
	trace_util_0.Count(_avg_00000, 3)
	count, err := af.Args[0].Eval(row)
	if err != nil {
		trace_util_0.Count(_avg_00000, 8)
		return err
	}
	trace_util_0.Count(_avg_00000, 4)
	evalCtx.Count += count.GetInt64()
	return nil
}

func (af *avgFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_avg_00000, 9)
	if af.HasDistinct {
		trace_util_0.Count(_avg_00000, 11)
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	trace_util_0.Count(_avg_00000, 10)
	evalCtx.Value.SetNull()
	evalCtx.Count = 0
}

// Update implements Aggregation interface.
func (af *avgFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) (err error) {
	trace_util_0.Count(_avg_00000, 12)
	switch af.Mode {
	case Partial1Mode, CompleteMode:
		trace_util_0.Count(_avg_00000, 14)
		err = af.updateSum(sc, evalCtx, row)
	case Partial2Mode, FinalMode:
		trace_util_0.Count(_avg_00000, 15)
		err = af.updateAvg(sc, evalCtx, row)
	case DedupMode:
		trace_util_0.Count(_avg_00000, 16)
		panic("DedupMode is not supported now.")
	}
	trace_util_0.Count(_avg_00000, 13)
	return err
}

// GetResult implements Aggregation interface.
func (af *avgFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	trace_util_0.Count(_avg_00000, 17)
	switch evalCtx.Value.Kind() {
	case types.KindFloat64:
		trace_util_0.Count(_avg_00000, 19)
		sum := evalCtx.Value.GetFloat64()
		d.SetFloat64(sum / float64(evalCtx.Count))
		return
	case types.KindMysqlDecimal:
		trace_util_0.Count(_avg_00000, 20)
		x := evalCtx.Value.GetMysqlDecimal()
		y := types.NewDecFromInt(evalCtx.Count)
		to := new(types.MyDecimal)
		err := types.DecimalDiv(x, y, to, types.DivFracIncr)
		terror.Log(err)
		frac := af.RetTp.Decimal
		if frac == -1 {
			trace_util_0.Count(_avg_00000, 22)
			frac = mysql.MaxDecimalScale
		}
		trace_util_0.Count(_avg_00000, 21)
		err = to.Round(to, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
		terror.Log(err)
		d.SetMysqlDecimal(to)
	}
	trace_util_0.Count(_avg_00000, 18)
	return
}

// GetPartialResult implements Aggregation interface.
func (af *avgFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_avg_00000, 23)
	return []types.Datum{types.NewIntDatum(evalCtx.Count), evalCtx.Value}
}

var _avg_00000 = "expression/aggregation/avg.go"
