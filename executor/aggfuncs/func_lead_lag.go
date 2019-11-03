// Copyright 2019 PingCAP, Inc.
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

package aggfuncs

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

type baseLeadLag struct {
	baseAggFunc
	valueEvaluator // TODO: move it to partial result when parallel execution is supported.

	defaultExpr expression.Expression
	offset      uint64
}

type partialResult4LeadLag struct {
	rows   []chunk.Row
	curIdx uint64
}

func (v *baseLeadLag) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_lead_lag_00000, 0)
	return PartialResult(&partialResult4LeadLag{})
}

func (v *baseLeadLag) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_lead_lag_00000, 1)
	p := (*partialResult4LeadLag)(pr)
	p.rows = p.rows[:0]
	p.curIdx = 0
}

func (v *baseLeadLag) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_lead_lag_00000, 2)
	p := (*partialResult4LeadLag)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	return nil
}

type lead struct {
	baseLeadLag
}

func (v *lead) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_lead_lag_00000, 3)
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx+v.offset < uint64(len(p.rows)) {
		trace_util_0.Count(_func_lead_lag_00000, 6)
		err = v.evaluateRow(sctx, v.args[0], p.rows[p.curIdx+v.offset])
	} else {
		trace_util_0.Count(_func_lead_lag_00000, 7)
		{
			err = v.evaluateRow(sctx, v.defaultExpr, p.rows[p.curIdx])
		}
	}
	trace_util_0.Count(_func_lead_lag_00000, 4)
	if err != nil {
		trace_util_0.Count(_func_lead_lag_00000, 8)
		return err
	}
	trace_util_0.Count(_func_lead_lag_00000, 5)
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}

type lag struct {
	baseLeadLag
}

func (v *lag) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_lead_lag_00000, 9)
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx >= v.offset {
		trace_util_0.Count(_func_lead_lag_00000, 12)
		err = v.evaluateRow(sctx, v.args[0], p.rows[p.curIdx-v.offset])
	} else {
		trace_util_0.Count(_func_lead_lag_00000, 13)
		{
			err = v.evaluateRow(sctx, v.defaultExpr, p.rows[p.curIdx])
		}
	}
	trace_util_0.Count(_func_lead_lag_00000, 10)
	if err != nil {
		trace_util_0.Count(_func_lead_lag_00000, 14)
		return err
	}
	trace_util_0.Count(_func_lead_lag_00000, 11)
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}

var _func_lead_lag_00000 = "executor/aggfuncs/func_lead_lag.go"
