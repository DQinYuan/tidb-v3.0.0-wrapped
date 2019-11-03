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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

// valueEvaluator is used to evaluate values for `first_value`, `last_value`, `nth_value`,
// `lead` and `lag`.
type valueEvaluator interface {
	// evaluateRow evaluates the expression using row and stores the result inside.
	evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error
	// appendResult appends the result to chunk.
	appendResult(chk *chunk.Chunk, colIdx int)
}

type value4Int struct {
	val    int64
	isNull bool
}

func (v *value4Int) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 0)
	var err error
	v.val, v.isNull, err = expr.EvalInt(ctx, row)
	return err
}

func (v *value4Int) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 1)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 2)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 3)
		{
			chk.AppendInt64(colIdx, v.val)
		}
	}
}

type value4Float32 struct {
	val    float32
	isNull bool
}

func (v *value4Float32) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 4)
	var err error
	var val float64
	val, v.isNull, err = expr.EvalReal(ctx, row)
	v.val = float32(val)
	return err
}

func (v *value4Float32) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 5)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 6)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 7)
		{
			chk.AppendFloat32(colIdx, v.val)
		}
	}
}

type value4Decimal struct {
	val    *types.MyDecimal
	isNull bool
}

func (v *value4Decimal) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 8)
	var err error
	v.val, v.isNull, err = expr.EvalDecimal(ctx, row)
	return err
}

func (v *value4Decimal) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 9)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 10)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 11)
		{
			chk.AppendMyDecimal(colIdx, v.val)
		}
	}
}

type value4Float64 struct {
	val    float64
	isNull bool
}

func (v *value4Float64) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 12)
	var err error
	v.val, v.isNull, err = expr.EvalReal(ctx, row)
	return err
}

func (v *value4Float64) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 13)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 14)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 15)
		{
			chk.AppendFloat64(colIdx, v.val)
		}
	}
}

type value4String struct {
	val    string
	isNull bool
}

func (v *value4String) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 16)
	var err error
	v.val, v.isNull, err = expr.EvalString(ctx, row)
	return err
}

func (v *value4String) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 17)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 18)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 19)
		{
			chk.AppendString(colIdx, v.val)
		}
	}
}

type value4Time struct {
	val    types.Time
	isNull bool
}

func (v *value4Time) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 20)
	var err error
	v.val, v.isNull, err = expr.EvalTime(ctx, row)
	return err
}

func (v *value4Time) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 21)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 22)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 23)
		{
			chk.AppendTime(colIdx, v.val)
		}
	}
}

type value4Duration struct {
	val    types.Duration
	isNull bool
}

func (v *value4Duration) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 24)
	var err error
	v.val, v.isNull, err = expr.EvalDuration(ctx, row)
	return err
}

func (v *value4Duration) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 25)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 26)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 27)
		{
			chk.AppendDuration(colIdx, v.val)
		}
	}
}

type value4JSON struct {
	val    json.BinaryJSON
	isNull bool
}

func (v *value4JSON) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	trace_util_0.Count(_func_value_00000, 28)
	var err error
	v.val, v.isNull, err = expr.EvalJSON(ctx, row)
	v.val = v.val.Copy() // deep copy to avoid content change.
	return err
}

func (v *value4JSON) appendResult(chk *chunk.Chunk, colIdx int) {
	trace_util_0.Count(_func_value_00000, 29)
	if v.isNull {
		trace_util_0.Count(_func_value_00000, 30)
		chk.AppendNull(colIdx)
	} else {
		trace_util_0.Count(_func_value_00000, 31)
		{
			chk.AppendJSON(colIdx, v.val)
		}
	}
}

func buildValueEvaluator(tp *types.FieldType) valueEvaluator {
	trace_util_0.Count(_func_value_00000, 32)
	evalType := tp.EvalType()
	if tp.Tp == mysql.TypeBit {
		trace_util_0.Count(_func_value_00000, 35)
		evalType = types.ETString
	}
	trace_util_0.Count(_func_value_00000, 33)
	switch evalType {
	case types.ETInt:
		trace_util_0.Count(_func_value_00000, 36)
		return &value4Int{}
	case types.ETReal:
		trace_util_0.Count(_func_value_00000, 37)
		switch tp.Tp {
		case mysql.TypeFloat:
			trace_util_0.Count(_func_value_00000, 43)
			return &value4Float32{}
		case mysql.TypeDouble:
			trace_util_0.Count(_func_value_00000, 44)
			return &value4Float64{}
		}
	case types.ETDecimal:
		trace_util_0.Count(_func_value_00000, 38)
		return &value4Decimal{}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_func_value_00000, 39)
		return &value4Time{}
	case types.ETDuration:
		trace_util_0.Count(_func_value_00000, 40)
		return &value4Duration{}
	case types.ETString:
		trace_util_0.Count(_func_value_00000, 41)
		return &value4String{}
	case types.ETJson:
		trace_util_0.Count(_func_value_00000, 42)
		return &value4JSON{}
	}
	trace_util_0.Count(_func_value_00000, 34)
	return nil
}

type firstValue struct {
	baseAggFunc

	tp *types.FieldType
}

type partialResult4FirstValue struct {
	gotFirstValue bool
	evaluator     valueEvaluator
}

func (v *firstValue) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_value_00000, 45)
	return PartialResult(&partialResult4FirstValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *firstValue) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_value_00000, 46)
	p := (*partialResult4FirstValue)(pr)
	p.gotFirstValue = false
}

func (v *firstValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_value_00000, 47)
	p := (*partialResult4FirstValue)(pr)
	if p.gotFirstValue {
		trace_util_0.Count(_func_value_00000, 50)
		return nil
	}
	trace_util_0.Count(_func_value_00000, 48)
	if len(rowsInGroup) > 0 {
		trace_util_0.Count(_func_value_00000, 51)
		p.gotFirstValue = true
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[0])
		if err != nil {
			trace_util_0.Count(_func_value_00000, 52)
			return err
		}
	}
	trace_util_0.Count(_func_value_00000, 49)
	return nil
}

func (v *firstValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_value_00000, 53)
	p := (*partialResult4FirstValue)(pr)
	if !p.gotFirstValue {
		trace_util_0.Count(_func_value_00000, 55)
		chk.AppendNull(v.ordinal)
	} else {
		trace_util_0.Count(_func_value_00000, 56)
		{
			p.evaluator.appendResult(chk, v.ordinal)
		}
	}
	trace_util_0.Count(_func_value_00000, 54)
	return nil
}

type lastValue struct {
	baseAggFunc

	tp *types.FieldType
}

type partialResult4LastValue struct {
	gotLastValue bool
	evaluator    valueEvaluator
}

func (v *lastValue) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_value_00000, 57)
	return PartialResult(&partialResult4LastValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *lastValue) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_value_00000, 58)
	p := (*partialResult4LastValue)(pr)
	p.gotLastValue = false
}

func (v *lastValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_value_00000, 59)
	p := (*partialResult4LastValue)(pr)
	if len(rowsInGroup) > 0 {
		trace_util_0.Count(_func_value_00000, 61)
		p.gotLastValue = true
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[len(rowsInGroup)-1])
		if err != nil {
			trace_util_0.Count(_func_value_00000, 62)
			return err
		}
	}
	trace_util_0.Count(_func_value_00000, 60)
	return nil
}

func (v *lastValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_value_00000, 63)
	p := (*partialResult4LastValue)(pr)
	if !p.gotLastValue {
		trace_util_0.Count(_func_value_00000, 65)
		chk.AppendNull(v.ordinal)
	} else {
		trace_util_0.Count(_func_value_00000, 66)
		{
			p.evaluator.appendResult(chk, v.ordinal)
		}
	}
	trace_util_0.Count(_func_value_00000, 64)
	return nil
}

type nthValue struct {
	baseAggFunc

	tp  *types.FieldType
	nth uint64
}

type partialResult4NthValue struct {
	seenRows  uint64
	evaluator valueEvaluator
}

func (v *nthValue) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_value_00000, 67)
	return PartialResult(&partialResult4NthValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *nthValue) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_value_00000, 68)
	p := (*partialResult4NthValue)(pr)
	p.seenRows = 0
}

func (v *nthValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_value_00000, 69)
	if v.nth == 0 {
		trace_util_0.Count(_func_value_00000, 72)
		return nil
	}
	trace_util_0.Count(_func_value_00000, 70)
	p := (*partialResult4NthValue)(pr)
	numRows := uint64(len(rowsInGroup))
	if v.nth > p.seenRows && v.nth-p.seenRows <= numRows {
		trace_util_0.Count(_func_value_00000, 73)
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[v.nth-p.seenRows-1])
		if err != nil {
			trace_util_0.Count(_func_value_00000, 74)
			return err
		}
	}
	trace_util_0.Count(_func_value_00000, 71)
	p.seenRows += numRows
	return nil
}

func (v *nthValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_value_00000, 75)
	p := (*partialResult4NthValue)(pr)
	if v.nth == 0 || p.seenRows < v.nth {
		trace_util_0.Count(_func_value_00000, 77)
		chk.AppendNull(v.ordinal)
	} else {
		trace_util_0.Count(_func_value_00000, 78)
		{
			p.evaluator.appendResult(chk, v.ordinal)
		}
	}
	trace_util_0.Count(_func_value_00000, 76)
	return nil
}

var _func_value_00000 = "executor/aggfuncs/func_value.go"
