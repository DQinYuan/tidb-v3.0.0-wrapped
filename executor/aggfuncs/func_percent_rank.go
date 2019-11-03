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

// percentRank calculates the percentage of partition values less than the value in the current row, excluding the highest value.
// It can be calculated as `(rank - 1) / (total_rows_in_set - 1).
type percentRank struct {
	baseAggFunc
	rowComparer
}

func (pr *percentRank) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_percent_rank_00000, 0)
	return PartialResult(&partialResult4Rank{})
}

func (pr *percentRank) ResetPartialResult(partial PartialResult) {
	trace_util_0.Count(_func_percent_rank_00000, 1)
	p := (*partialResult4Rank)(partial)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (pr *percentRank) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partial PartialResult) error {
	trace_util_0.Count(_func_percent_rank_00000, 2)
	p := (*partialResult4Rank)(partial)
	p.rows = append(p.rows, rowsInGroup...)
	return nil
}

func (pr *percentRank) AppendFinalResult2Chunk(sctx sessionctx.Context, partial PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_percent_rank_00000, 3)
	p := (*partialResult4Rank)(partial)
	numRows := int64(len(p.rows))
	p.curIdx++
	if p.curIdx == 1 {
		trace_util_0.Count(_func_percent_rank_00000, 6)
		p.lastRank = 1
		chk.AppendFloat64(pr.ordinal, 0)
		return nil
	}
	trace_util_0.Count(_func_percent_rank_00000, 4)
	if pr.compareRows(p.rows[p.curIdx-2], p.rows[p.curIdx-1]) == 0 {
		trace_util_0.Count(_func_percent_rank_00000, 7)
		chk.AppendFloat64(pr.ordinal, float64(p.lastRank-1)/float64(numRows-1))
		return nil
	}
	trace_util_0.Count(_func_percent_rank_00000, 5)
	p.lastRank = p.curIdx
	chk.AppendFloat64(pr.ordinal, float64(p.lastRank-1)/float64(numRows-1))
	return nil
}

var _func_percent_rank_00000 = "executor/aggfuncs/func_percent_rank.go"
