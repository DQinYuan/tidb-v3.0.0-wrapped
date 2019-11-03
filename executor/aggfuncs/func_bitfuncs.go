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
	"math"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

type baseBitAggFunc struct {
	baseAggFunc
}

type partialResult4BitFunc = uint64

func (e *baseBitAggFunc) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_bitfuncs_00000, 0)
	return PartialResult(new(partialResult4BitFunc))
}

func (e *baseBitAggFunc) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_bitfuncs_00000, 1)
	p := (*partialResult4BitFunc)(pr)
	*p = 0
}

func (e *baseBitAggFunc) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_bitfuncs_00000, 2)
	p := (*partialResult4BitFunc)(pr)
	chk.AppendUint64(e.ordinal, *p)
	return nil
}

type bitOrUint64 struct {
	baseBitAggFunc
}

func (e *bitOrUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 3)
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_bitfuncs_00000, 5)
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_bitfuncs_00000, 8)
			return err
		}
		trace_util_0.Count(_func_bitfuncs_00000, 6)
		if isNull {
			trace_util_0.Count(_func_bitfuncs_00000, 9)
			continue
		}
		trace_util_0.Count(_func_bitfuncs_00000, 7)
		*p |= uint64(inputValue)
	}
	trace_util_0.Count(_func_bitfuncs_00000, 4)
	return nil
}

func (*bitOrUint64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 10)
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 |= uint64(*p1)
	return nil
}

type bitXorUint64 struct {
	baseBitAggFunc
}

func (e *bitXorUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 11)
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_bitfuncs_00000, 13)
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_bitfuncs_00000, 16)
			return err
		}
		trace_util_0.Count(_func_bitfuncs_00000, 14)
		if isNull {
			trace_util_0.Count(_func_bitfuncs_00000, 17)
			continue
		}
		trace_util_0.Count(_func_bitfuncs_00000, 15)
		*p ^= uint64(inputValue)
	}
	trace_util_0.Count(_func_bitfuncs_00000, 12)
	return nil
}

func (*bitXorUint64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 18)
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 ^= uint64(*p1)
	return nil
}

type bitAndUint64 struct {
	baseBitAggFunc
}

func (e *bitAndUint64) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_bitfuncs_00000, 19)
	p := new(partialResult4BitFunc)
	*p = math.MaxUint64
	return PartialResult(p)
}

func (e *bitAndUint64) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_bitfuncs_00000, 20)
	p := (*partialResult4BitFunc)(pr)
	*p = math.MaxUint64
}

func (e *bitAndUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 21)
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_bitfuncs_00000, 23)
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			trace_util_0.Count(_func_bitfuncs_00000, 26)
			return err
		}
		trace_util_0.Count(_func_bitfuncs_00000, 24)
		if isNull {
			trace_util_0.Count(_func_bitfuncs_00000, 27)
			continue
		}
		trace_util_0.Count(_func_bitfuncs_00000, 25)
		*p &= uint64(inputValue)
	}
	trace_util_0.Count(_func_bitfuncs_00000, 22)
	return nil
}

func (*bitAndUint64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_bitfuncs_00000, 28)
	p1, p2 := (*partialResult4BitFunc)(src), (*partialResult4BitFunc)(dst)
	*p2 &= uint64(*p1)
	return nil
}

var _func_bitfuncs_00000 = "executor/aggfuncs/func_bitfuncs.go"
