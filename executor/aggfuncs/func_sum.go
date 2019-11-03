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

package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

type partialResult4SumFloat64 struct {
	val    float64
	isNull bool
}

type partialResult4SumDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type partialResult4SumDistinctFloat64 struct {
	partialResult4SumFloat64
	valSet set.Float64Set
}

type partialResult4SumDistinctDecimal struct {
	partialResult4SumDecimal
	valSet set.StringSet
}

type baseSumAggFunc struct {
	baseAggFunc
}

type sum4Float64 struct {
	baseSumAggFunc
}

func (e *sum4Float64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_sum_00000, 0)
	p := new(partialResult4SumFloat64)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Float64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_sum_00000, 1)
	p := (*partialResult4SumFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *sum4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_sum_00000, 2)
	p := (*partialResult4SumFloat64)(pr)
	if p.isNull {
		trace_util_0.Count(_func_sum_00000, 4)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 3)
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *sum4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 5)
	p := (*partialResult4SumFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_sum_00000, 7)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 11)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 8)
		if isNull {
			trace_util_0.Count(_func_sum_00000, 12)
			continue
		}
		trace_util_0.Count(_func_sum_00000, 9)
		if p.isNull {
			trace_util_0.Count(_func_sum_00000, 13)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_sum_00000, 10)
		p.val += input
	}
	trace_util_0.Count(_func_sum_00000, 6)
	return nil
}

func (e *sum4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 14)
	p1, p2 := (*partialResult4SumFloat64)(src), (*partialResult4SumFloat64)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_sum_00000, 16)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 15)
	p2.val += p1.val
	p2.isNull = false
	return nil
}

type sum4Decimal struct {
	baseSumAggFunc
}

func (e *sum4Decimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_sum_00000, 17)
	p := new(partialResult4SumDecimal)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Decimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_sum_00000, 18)
	p := (*partialResult4SumDecimal)(pr)
	p.isNull = true
}

func (e *sum4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_sum_00000, 19)
	p := (*partialResult4SumDecimal)(pr)
	if p.isNull {
		trace_util_0.Count(_func_sum_00000, 21)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 20)
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *sum4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 22)
	p := (*partialResult4SumDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_sum_00000, 24)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 29)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 25)
		if isNull {
			trace_util_0.Count(_func_sum_00000, 30)
			continue
		}
		trace_util_0.Count(_func_sum_00000, 26)
		if p.isNull {
			trace_util_0.Count(_func_sum_00000, 31)
			p.val = *input
			p.isNull = false
			continue
		}

		trace_util_0.Count(_func_sum_00000, 27)
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 32)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 28)
		p.val = *newSum
	}
	trace_util_0.Count(_func_sum_00000, 23)
	return nil
}

func (e *sum4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 33)
	p1, p2 := (*partialResult4SumDecimal)(src), (*partialResult4SumDecimal)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_sum_00000, 36)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 34)
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&p1.val, &p2.val, newSum)
	if err != nil {
		trace_util_0.Count(_func_sum_00000, 37)
		return err
	}
	trace_util_0.Count(_func_sum_00000, 35)
	p2.val = *newSum
	p2.isNull = false
	return nil
}

type sum4DistinctFloat64 struct {
	baseSumAggFunc
}

func (e *sum4DistinctFloat64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_sum_00000, 38)
	p := new(partialResult4SumDistinctFloat64)
	p.isNull = true
	p.valSet = set.NewFloat64Set()
	return PartialResult(p)
}

func (e *sum4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_sum_00000, 39)
	p := (*partialResult4SumDistinctFloat64)(pr)
	p.isNull = true
	p.valSet = set.NewFloat64Set()
}

func (e *sum4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 40)
	p := (*partialResult4SumDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_sum_00000, 42)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 46)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 43)
		if isNull || p.valSet.Exist(input) {
			trace_util_0.Count(_func_sum_00000, 47)
			continue
		}
		trace_util_0.Count(_func_sum_00000, 44)
		p.valSet.Insert(input)
		if p.isNull {
			trace_util_0.Count(_func_sum_00000, 48)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_sum_00000, 45)
		p.val += input
	}
	trace_util_0.Count(_func_sum_00000, 41)
	return nil
}

func (e *sum4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_sum_00000, 49)
	p := (*partialResult4SumDistinctFloat64)(pr)
	if p.isNull {
		trace_util_0.Count(_func_sum_00000, 51)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 50)
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type sum4DistinctDecimal struct {
	baseSumAggFunc
}

func (e *sum4DistinctDecimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_sum_00000, 52)
	p := new(partialResult4SumDistinctDecimal)
	p.isNull = true
	p.valSet = set.NewStringSet()
	return PartialResult(p)
}

func (e *sum4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_sum_00000, 53)
	p := (*partialResult4SumDistinctDecimal)(pr)
	p.isNull = true
	p.valSet = set.NewStringSet()
}

func (e *sum4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_sum_00000, 54)
	p := (*partialResult4SumDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_sum_00000, 56)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 63)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 57)
		if isNull {
			trace_util_0.Count(_func_sum_00000, 64)
			continue
		}
		trace_util_0.Count(_func_sum_00000, 58)
		hash, err := input.ToHashKey()
		if err != nil {
			trace_util_0.Count(_func_sum_00000, 65)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 59)
		decStr := string(hack.String(hash))
		if p.valSet.Exist(decStr) {
			trace_util_0.Count(_func_sum_00000, 66)
			continue
		}
		trace_util_0.Count(_func_sum_00000, 60)
		p.valSet.Insert(decStr)
		if p.isNull {
			trace_util_0.Count(_func_sum_00000, 67)
			p.val = *input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_sum_00000, 61)
		newSum := new(types.MyDecimal)
		if err = types.DecimalAdd(&p.val, input, newSum); err != nil {
			trace_util_0.Count(_func_sum_00000, 68)
			return err
		}
		trace_util_0.Count(_func_sum_00000, 62)
		p.val = *newSum
	}
	trace_util_0.Count(_func_sum_00000, 55)
	return nil
}

func (e *sum4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_sum_00000, 69)
	p := (*partialResult4SumDistinctDecimal)(pr)
	if p.isNull {
		trace_util_0.Count(_func_sum_00000, 71)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_sum_00000, 70)
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

var _func_sum_00000 = "executor/aggfuncs/func_sum.go"
