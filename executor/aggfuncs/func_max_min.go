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

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
}

type partialResult4Time struct {
	val    types.Time
	isNull bool
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
}

type partialResult4MaxMinString struct {
	val    string
	isNull bool
}

type partialResult4MaxMinJSON struct {
	val    json.BinaryJSON
	isNull bool
}

type baseMaxMinAggFunc struct {
	baseAggFunc

	isMax bool
}

type maxMin4Int struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Int) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 0)
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 1)
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 2)
	p := (*partialResult4MaxMinInt)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 4)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 3)
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 5)
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 7)
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 11)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 8)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 12)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 9)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 13)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 10)
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			trace_util_0.Count(_func_max_min_00000, 14)
			p.val = input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 6)
	return nil
}

func (e *maxMin4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 15)
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 19)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 16)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 20)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 17)
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		trace_util_0.Count(_func_max_min_00000, 21)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 18)
	return nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Uint) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 22)
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Uint) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 23)
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Uint) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 24)
	p := (*partialResult4MaxMinUint)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 26)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 25)
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Uint) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 27)
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 29)
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 33)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 30)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 34)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 31)
		uintVal := uint64(input)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 35)
			p.val = uintVal
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 32)
		if e.isMax && uintVal > p.val || !e.isMax && uintVal < p.val {
			trace_util_0.Count(_func_max_min_00000, 36)
			p.val = uintVal
		}
	}
	trace_util_0.Count(_func_max_min_00000, 28)
	return nil
}

func (e *maxMin4Uint) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 37)
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 41)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 38)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 42)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 39)
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		trace_util_0.Count(_func_max_min_00000, 43)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 40)
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float32) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 44)
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Float32) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 45)
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 46)
	p := (*partialResult4MaxMinFloat32)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 48)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 47)
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 49)
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 51)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 55)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 52)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 56)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 53)
		f := float32(input)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 57)
			p.val = f
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 54)
		if e.isMax && f > p.val || !e.isMax && f < p.val {
			trace_util_0.Count(_func_max_min_00000, 58)
			p.val = f
		}
	}
	trace_util_0.Count(_func_max_min_00000, 50)
	return nil
}

func (e *maxMin4Float32) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 59)
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 63)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 60)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 64)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 61)
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		trace_util_0.Count(_func_max_min_00000, 65)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 62)
	return nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 66)
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Float64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 67)
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 68)
	p := (*partialResult4MaxMinFloat64)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 70)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 69)
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 71)
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 73)
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 77)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 74)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 78)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 75)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 79)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 76)
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			trace_util_0.Count(_func_max_min_00000, 80)
			p.val = input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 72)
	return nil
}

func (e *maxMin4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 81)
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 85)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 82)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 86)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 83)
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		trace_util_0.Count(_func_max_min_00000, 87)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 84)
	return nil
}

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Decimal) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 88)
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 89)
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 90)
	p := (*partialResult4MaxMinDecimal)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 92)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 91)
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 93)
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 95)
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 99)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 96)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 100)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 97)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 101)
			p.val = *input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 98)
		cmp := input.Compare(&p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			trace_util_0.Count(_func_max_min_00000, 102)
			p.val = *input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 94)
	return nil
}

func (e *maxMin4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 103)
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 107)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 104)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 108)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 105)
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		trace_util_0.Count(_func_max_min_00000, 109)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 106)
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
}

func (e *maxMin4String) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 110)
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4String) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 111)
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
}

func (e *maxMin4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 112)
	p := (*partialResult4MaxMinString)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 114)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 113)
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *maxMin4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 115)
	p := (*partialResult4MaxMinString)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 117)
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 121)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 118)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 122)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 119)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 123)
			// The string returned by `EvalString` may be referenced to an underlying buffer,
			// for example ‘Chunk’, which could be reset and reused multiply times.
			// We have to deep copy that string to avoid some potential risks
			// when the content of that underlying buffer changed.
			p.val = stringutil.Copy(input)
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 120)
		cmp := types.CompareString(input, p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			trace_util_0.Count(_func_max_min_00000, 124)
			p.val = stringutil.Copy(input)
		}
	}
	trace_util_0.Count(_func_max_min_00000, 116)
	return nil
}

func (e *maxMin4String) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 125)
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 129)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 126)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 130)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 127)
	cmp := types.CompareString(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		trace_util_0.Count(_func_max_min_00000, 131)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 128)
	return nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Time) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 132)
	p := new(partialResult4Time)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Time) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 133)
	p := (*partialResult4Time)(pr)
	p.isNull = true
}

func (e *maxMin4Time) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 134)
	p := (*partialResult4Time)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 136)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 135)
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 137)
	p := (*partialResult4Time)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 139)
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 143)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 140)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 144)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 141)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 145)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 142)
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			trace_util_0.Count(_func_max_min_00000, 146)
			p.val = input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 138)
	return nil
}

func (e *maxMin4Time) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 147)
	p1, p2 := (*partialResult4Time)(src), (*partialResult4Time)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 151)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 148)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 152)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 149)
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		trace_util_0.Count(_func_max_min_00000, 153)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 150)
	return nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Duration) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 154)
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Duration) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 155)
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
}

func (e *maxMin4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 156)
	p := (*partialResult4MaxMinDuration)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 158)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 157)
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 159)
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 161)
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 165)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 162)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 166)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 163)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 167)
			p.val = input
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 164)
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			trace_util_0.Count(_func_max_min_00000, 168)
			p.val = input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 160)
	return nil
}

func (e *maxMin4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 169)
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 173)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 170)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 174)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 171)
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		trace_util_0.Count(_func_max_min_00000, 175)
		p2.val, p2.isNull = p1.val, false
	}
	trace_util_0.Count(_func_max_min_00000, 172)
	return nil
}

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (e *maxMin4JSON) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_max_min_00000, 176)
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4JSON) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_max_min_00000, 177)
	p := (*partialResult4MaxMinJSON)(pr)
	p.isNull = true
}

func (e *maxMin4JSON) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_max_min_00000, 178)
	p := (*partialResult4MaxMinJSON)(pr)
	if p.isNull {
		trace_util_0.Count(_func_max_min_00000, 180)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 179)
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *maxMin4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 181)
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_max_min_00000, 183)
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_max_min_00000, 187)
			return err
		}
		trace_util_0.Count(_func_max_min_00000, 184)
		if isNull {
			trace_util_0.Count(_func_max_min_00000, 188)
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 185)
		if p.isNull {
			trace_util_0.Count(_func_max_min_00000, 189)
			p.val = input.Copy()
			p.isNull = false
			continue
		}
		trace_util_0.Count(_func_max_min_00000, 186)
		cmp := json.CompareBinary(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			trace_util_0.Count(_func_max_min_00000, 190)
			p.val = input
		}
	}
	trace_util_0.Count(_func_max_min_00000, 182)
	return nil
}

func (e *maxMin4JSON) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_max_min_00000, 191)
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		trace_util_0.Count(_func_max_min_00000, 195)
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 192)
	if p2.isNull {
		trace_util_0.Count(_func_max_min_00000, 196)
		*p2 = *p1
		return nil
	}
	trace_util_0.Count(_func_max_min_00000, 193)
	cmp := json.CompareBinary(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		trace_util_0.Count(_func_max_min_00000, 197)
		p2.val = p1.val
		p2.isNull = false
	}
	trace_util_0.Count(_func_max_min_00000, 194)
	return nil
}

var _func_max_min_00000 = "executor/aggfuncs/func_max_min.go"
