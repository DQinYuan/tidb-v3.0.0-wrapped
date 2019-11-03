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
	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

// All the following avg function implementations return the decimal result,
// which store the partial results in "partialResult4AvgDecimal".
//
// "baseAvgDecimal" is wrapped by:
// - "avgOriginal4Decimal"
// - "avgPartial4Decimal"
type baseAvgDecimal struct {
	baseAggFunc
}

type partialResult4AvgDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *baseAvgDecimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_avg_00000, 0)
	return PartialResult(&partialResult4AvgDecimal{})
}

func (e *baseAvgDecimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_avg_00000, 1)
	p := (*partialResult4AvgDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseAvgDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_avg_00000, 2)
	p := (*partialResult4AvgDecimal)(pr)
	if p.count == 0 {
		trace_util_0.Count(_func_avg_00000, 8)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_avg_00000, 3)
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		trace_util_0.Count(_func_avg_00000, 9)
		return err
	}
	// Make the decimal be the result of type inferring.
	trace_util_0.Count(_func_avg_00000, 4)
	frac := e.args[0].GetType().Decimal
	if len(e.args) == 2 {
		trace_util_0.Count(_func_avg_00000, 10)
		frac = e.args[1].GetType().Decimal
	}
	trace_util_0.Count(_func_avg_00000, 5)
	if frac == -1 {
		trace_util_0.Count(_func_avg_00000, 11)
		frac = mysql.MaxDecimalScale
	}
	trace_util_0.Count(_func_avg_00000, 6)
	err = finalResult.Round(finalResult, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_func_avg_00000, 12)
		return err
	}
	trace_util_0.Count(_func_avg_00000, 7)
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type avgOriginal4Decimal struct {
	baseAvgDecimal
}

func (e *avgOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 13)
	p := (*partialResult4AvgDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 15)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 19)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 16)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 20)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 17)
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 21)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 18)
		p.sum = *newSum
		p.count++
	}
	trace_util_0.Count(_func_avg_00000, 14)
	return nil
}

type avgPartial4Decimal struct {
	baseAvgDecimal
}

func (e *avgPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 22)
	p := (*partialResult4AvgDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 24)
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 30)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 25)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 31)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 26)
		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 32)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 27)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 33)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 28)
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 34)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 29)
		p.sum = *newSum
		p.count += inputCount
	}
	trace_util_0.Count(_func_avg_00000, 23)
	return nil
}

func (e *avgPartial4Decimal) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 35)
	p1, p2 := (*partialResult4AvgDecimal)(src), (*partialResult4AvgDecimal)(dst)
	if p1.count == 0 {
		trace_util_0.Count(_func_avg_00000, 38)
		return nil
	}
	trace_util_0.Count(_func_avg_00000, 36)
	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&p1.sum, &p2.sum, newSum)
	if err != nil {
		trace_util_0.Count(_func_avg_00000, 39)
		return err
	}
	trace_util_0.Count(_func_avg_00000, 37)
	p2.sum = *newSum
	p2.count += p1.count
	return nil
}

type partialResult4AvgDistinctDecimal struct {
	partialResult4AvgDecimal
	valSet set.StringSet
}

type avgOriginal4DistinctDecimal struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctDecimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_avg_00000, 40)
	p := &partialResult4AvgDistinctDecimal{
		valSet: set.NewStringSet(),
	}
	return PartialResult(p)
}

func (e *avgOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_avg_00000, 41)
	p := (*partialResult4AvgDistinctDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewStringSet()
}

func (e *avgOriginal4DistinctDecimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 42)
	p := (*partialResult4AvgDistinctDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 44)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 50)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 45)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 51)
			continue
		}
		trace_util_0.Count(_func_avg_00000, 46)
		hash, err := input.ToHashKey()
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 52)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 47)
		decStr := string(hack.String(hash))
		if p.valSet.Exist(decStr) {
			trace_util_0.Count(_func_avg_00000, 53)
			continue
		}
		trace_util_0.Count(_func_avg_00000, 48)
		p.valSet.Insert(decStr)
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 54)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 49)
		p.sum = *newSum
		p.count++
	}
	trace_util_0.Count(_func_avg_00000, 43)
	return nil
}

func (e *avgOriginal4DistinctDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_avg_00000, 55)
	p := (*partialResult4AvgDistinctDecimal)(pr)
	if p.count == 0 {
		trace_util_0.Count(_func_avg_00000, 60)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_avg_00000, 56)
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		trace_util_0.Count(_func_avg_00000, 61)
		return err
	}
	// Make the decimal be the result of type inferring.
	trace_util_0.Count(_func_avg_00000, 57)
	frac := e.args[0].GetType().Decimal
	if frac == -1 {
		trace_util_0.Count(_func_avg_00000, 62)
		frac = mysql.MaxDecimalScale
	}
	trace_util_0.Count(_func_avg_00000, 58)
	err = finalResult.Round(finalResult, mathutil.Min(frac, mysql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		trace_util_0.Count(_func_avg_00000, 63)
		return err
	}
	trace_util_0.Count(_func_avg_00000, 59)
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

// All the following avg function implementations return the float64 result,
// which store the partial results in "partialResult4AvgFloat64".
//
// "baseAvgFloat64" is wrapped by:
// - "avgOriginal4Float64"
// - "avgPartial4Float64"
type baseAvgFloat64 struct {
	baseAggFunc
}

type partialResult4AvgFloat64 struct {
	sum   float64
	count int64
}

func (e *baseAvgFloat64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_avg_00000, 64)
	return (PartialResult)(&partialResult4AvgFloat64{})
}

func (e *baseAvgFloat64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_avg_00000, 65)
	p := (*partialResult4AvgFloat64)(pr)
	p.sum = 0
	p.count = 0
}

func (e *baseAvgFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_avg_00000, 66)
	p := (*partialResult4AvgFloat64)(pr)
	if p.count == 0 {
		trace_util_0.Count(_func_avg_00000, 68)
		chk.AppendNull(e.ordinal)
	} else {
		trace_util_0.Count(_func_avg_00000, 69)
		{
			chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
		}
	}
	trace_util_0.Count(_func_avg_00000, 67)
	return nil
}

type avgOriginal4Float64 struct {
	baseAvgFloat64
}

func (e *avgOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 70)
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 72)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 75)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 73)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 76)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 74)
		p.sum += input
		p.count++
	}
	trace_util_0.Count(_func_avg_00000, 71)
	return nil
}

type avgPartial4Float64 struct {
	baseAvgFloat64
}

func (e *avgPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 77)
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 79)
		inputSum, isNull, err := e.args[1].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 84)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 80)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 85)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 81)
		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 86)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 82)
		if isNull {
			trace_util_0.Count(_func_avg_00000, 87)
			continue
		}
		trace_util_0.Count(_func_avg_00000, 83)
		p.sum += inputSum
		p.count += inputCount
	}
	trace_util_0.Count(_func_avg_00000, 78)
	return nil
}

func (e *avgPartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 88)
	p1, p2 := (*partialResult4AvgFloat64)(src), (*partialResult4AvgFloat64)(dst)
	p2.sum += p1.sum
	p2.count += p1.count
	return nil
}

type partialResult4AvgDistinctFloat64 struct {
	partialResult4AvgFloat64
	valSet set.Float64Set
}

type avgOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_avg_00000, 89)
	p := &partialResult4AvgDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *avgOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_avg_00000, 90)
	p := (*partialResult4AvgDistinctFloat64)(pr)
	p.sum = float64(0)
	p.count = int64(0)
	p.valSet = set.NewFloat64Set()
}

func (e *avgOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_avg_00000, 91)
	p := (*partialResult4AvgDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_avg_00000, 93)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_avg_00000, 96)
			return err
		}
		trace_util_0.Count(_func_avg_00000, 94)
		if isNull || p.valSet.Exist(input) {
			trace_util_0.Count(_func_avg_00000, 97)
			continue
		}

		trace_util_0.Count(_func_avg_00000, 95)
		p.sum += input
		p.count++
		p.valSet.Insert(input)
	}
	trace_util_0.Count(_func_avg_00000, 92)
	return nil
}

func (e *avgOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_avg_00000, 98)
	p := (*partialResult4AvgDistinctFloat64)(pr)
	if p.count == 0 {
		trace_util_0.Count(_func_avg_00000, 100)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_avg_00000, 99)
	chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	return nil
}

var _func_avg_00000 = "executor/aggfuncs/func_avg.go"
