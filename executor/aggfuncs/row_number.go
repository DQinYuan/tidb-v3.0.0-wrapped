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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

type rowNumber struct {
	baseAggFunc
}

type partialResult4RowNumber struct {
	curIdx int64
}

func (rn *rowNumber) AllocPartialResult() PartialResult {
	trace_util_0.Count(_row_number_00000, 0)
	return PartialResult(&partialResult4RowNumber{})
}

func (rn *rowNumber) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_row_number_00000, 1)
	p := (*partialResult4RowNumber)(pr)
	p.curIdx = 0
}

func (rn *rowNumber) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_row_number_00000, 2)
	return nil
}

func (rn *rowNumber) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_row_number_00000, 3)
	p := (*partialResult4RowNumber)(pr)
	p.curIdx++
	chk.AppendInt64(rn.ordinal, p.curIdx)
	return nil
}

var _row_number_00000 = "executor/aggfuncs/row_number.go"
