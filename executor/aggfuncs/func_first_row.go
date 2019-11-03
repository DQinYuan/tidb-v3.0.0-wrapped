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
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

type basePartialResult4FirstRow struct {
	// isNull indicates whether the first row is null.
	isNull bool
	// gotFirstRow indicates whether the first row has been got,
	// if so, we would avoid evaluating the values of the remained rows.
	gotFirstRow bool
}

type partialResult4FirstRowInt struct {
	basePartialResult4FirstRow

	val int64
}

type partialResult4FirstRowFloat32 struct {
	basePartialResult4FirstRow

	val float32
}

type partialResult4FirstRowDecimal struct {
	basePartialResult4FirstRow

	val types.MyDecimal
}

type partialResult4FirstRowFloat64 struct {
	basePartialResult4FirstRow

	val float64
}

type partialResult4FirstRowString struct {
	basePartialResult4FirstRow

	val string
}

type partialResult4FirstRowTime struct {
	basePartialResult4FirstRow

	val types.Time
}

type partialResult4FirstRowDuration struct {
	basePartialResult4FirstRow

	val types.Duration
}

type partialResult4FirstRowJSON struct {
	basePartialResult4FirstRow

	val json.BinaryJSON
}

type firstRow4Int struct {
	baseAggFunc
}

func (e *firstRow4Int) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 0)
	return PartialResult(new(partialResult4FirstRowInt))
}

func (e *firstRow4Int) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 1)
	p := (*partialResult4FirstRowInt)(pr)
	p.val, p.isNull, p.gotFirstRow = 0, false, false
}

func (e *firstRow4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 2)
	p := (*partialResult4FirstRowInt)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 5)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 3)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 6)
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 8)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 7)
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
		break
	}
	trace_util_0.Count(_func_first_row_00000, 4)
	return nil
}

func (*firstRow4Int) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 9)
	p1, p2 := (*partialResult4FirstRowInt)(src), (*partialResult4FirstRowInt)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 11)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 10)
	return nil
}

func (e *firstRow4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 12)
	p := (*partialResult4FirstRowInt)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 14)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 13)
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

type firstRow4Float32 struct {
	baseAggFunc
}

func (e *firstRow4Float32) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 15)
	return PartialResult(new(partialResult4FirstRowFloat32))
}

func (e *firstRow4Float32) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 16)
	p := (*partialResult4FirstRowFloat32)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 17)
	p := (*partialResult4FirstRowFloat32)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 20)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 18)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 21)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 23)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 22)
		p.gotFirstRow, p.isNull, p.val = true, isNull, float32(input)
		break
	}
	trace_util_0.Count(_func_first_row_00000, 19)
	return nil
}
func (*firstRow4Float32) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 24)
	p1, p2 := (*partialResult4FirstRowFloat32)(src), (*partialResult4FirstRowFloat32)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 26)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 25)
	return nil
}

func (e *firstRow4Float32) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 27)
	p := (*partialResult4FirstRowFloat32)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 29)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 28)
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

type firstRow4Float64 struct {
	baseAggFunc
}

func (e *firstRow4Float64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 30)
	return PartialResult(new(partialResult4FirstRowFloat64))
}

func (e *firstRow4Float64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 31)
	p := (*partialResult4FirstRowFloat64)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 32)
	p := (*partialResult4FirstRowFloat64)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 35)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 33)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 36)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 38)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 37)
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
		break
	}
	trace_util_0.Count(_func_first_row_00000, 34)
	return nil
}

func (*firstRow4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 39)
	p1, p2 := (*partialResult4FirstRowFloat64)(src), (*partialResult4FirstRowFloat64)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 41)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 40)
	return nil
}
func (e *firstRow4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 42)
	p := (*partialResult4FirstRowFloat64)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 44)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 43)
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type firstRow4String struct {
	baseAggFunc
}

func (e *firstRow4String) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 45)
	return PartialResult(new(partialResult4FirstRowString))
}

func (e *firstRow4String) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 46)
	p := (*partialResult4FirstRowString)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 47)
	p := (*partialResult4FirstRowString)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 50)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 48)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 51)
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 53)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 52)
		p.gotFirstRow, p.isNull, p.val = true, isNull, stringutil.Copy(input)
		break
	}
	trace_util_0.Count(_func_first_row_00000, 49)
	return nil
}

func (*firstRow4String) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 54)
	p1, p2 := (*partialResult4FirstRowString)(src), (*partialResult4FirstRowString)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 56)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 55)
	return nil
}

func (e *firstRow4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 57)
	p := (*partialResult4FirstRowString)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 59)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 58)
	chk.AppendString(e.ordinal, p.val)
	return nil
}

type firstRow4Time struct {
	baseAggFunc
}

func (e *firstRow4Time) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 60)
	return PartialResult(new(partialResult4FirstRowTime))
}

func (e *firstRow4Time) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 61)
	p := (*partialResult4FirstRowTime)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 62)
	p := (*partialResult4FirstRowTime)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 65)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 63)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 66)
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 68)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 67)
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
		break
	}
	trace_util_0.Count(_func_first_row_00000, 64)
	return nil
}
func (*firstRow4Time) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 69)
	p1, p2 := (*partialResult4FirstRowTime)(src), (*partialResult4FirstRowTime)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 71)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 70)
	return nil

}

func (e *firstRow4Time) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 72)
	p := (*partialResult4FirstRowTime)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 74)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 73)
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

type firstRow4Duration struct {
	baseAggFunc
}

func (e *firstRow4Duration) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 75)
	return PartialResult(new(partialResult4FirstRowDuration))
}

func (e *firstRow4Duration) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 76)
	p := (*partialResult4FirstRowDuration)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 77)
	p := (*partialResult4FirstRowDuration)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 80)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 78)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 81)
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 83)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 82)
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
		break
	}
	trace_util_0.Count(_func_first_row_00000, 79)
	return nil
}
func (*firstRow4Duration) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 84)
	p1, p2 := (*partialResult4FirstRowDuration)(src), (*partialResult4FirstRowDuration)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 86)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 85)
	return nil
}

func (e *firstRow4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 87)
	p := (*partialResult4FirstRowDuration)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 89)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 88)
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

type firstRow4JSON struct {
	baseAggFunc
}

func (e *firstRow4JSON) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 90)
	return PartialResult(new(partialResult4FirstRowJSON))
}

func (e *firstRow4JSON) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 91)
	p := (*partialResult4FirstRowJSON)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 92)
	p := (*partialResult4FirstRowJSON)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 95)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 93)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 96)
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 98)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 97)
		p.gotFirstRow, p.isNull, p.val = true, isNull, input.Copy()
		break
	}
	trace_util_0.Count(_func_first_row_00000, 94)
	return nil
}
func (*firstRow4JSON) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 99)
	p1, p2 := (*partialResult4FirstRowJSON)(src), (*partialResult4FirstRowJSON)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 101)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 100)
	return nil
}

func (e *firstRow4JSON) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 102)
	p := (*partialResult4FirstRowJSON)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 104)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 103)
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

type firstRow4Decimal struct {
	baseAggFunc
}

func (e *firstRow4Decimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_first_row_00000, 105)
	return PartialResult(new(partialResult4FirstRowDecimal))
}

func (e *firstRow4Decimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_first_row_00000, 106)
	p := (*partialResult4FirstRowDecimal)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 107)
	p := (*partialResult4FirstRowDecimal)(pr)
	if p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 110)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 108)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_first_row_00000, 111)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_first_row_00000, 114)
			return err
		}
		trace_util_0.Count(_func_first_row_00000, 112)
		p.gotFirstRow, p.isNull = true, isNull
		if input != nil {
			trace_util_0.Count(_func_first_row_00000, 115)
			p.val = *input
		}
		trace_util_0.Count(_func_first_row_00000, 113)
		break
	}
	trace_util_0.Count(_func_first_row_00000, 109)
	return nil
}

func (e *firstRow4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_first_row_00000, 116)
	p := (*partialResult4FirstRowDecimal)(pr)
	if p.isNull || !p.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 118)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_first_row_00000, 117)
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (*firstRow4Decimal) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	trace_util_0.Count(_func_first_row_00000, 119)
	p1, p2 := (*partialResult4FirstRowDecimal)(src), (*partialResult4FirstRowDecimal)(dst)
	if !p2.gotFirstRow {
		trace_util_0.Count(_func_first_row_00000, 121)
		*p2 = *p1
	}
	trace_util_0.Count(_func_first_row_00000, 120)
	return nil
}

var _func_first_row_00000 = "executor/aggfuncs/func_first_row.go"
