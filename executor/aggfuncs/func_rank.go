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

type rank struct {
	baseAggFunc
	isDense bool
	rowComparer
}

type partialResult4Rank struct {
	curIdx   int64
	lastRank int64
	rows     []chunk.Row
}

func (r *rank) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_rank_00000, 0)
	return PartialResult(&partialResult4Rank{})
}

func (r *rank) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_rank_00000, 1)
	p := (*partialResult4Rank)(pr)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (r *rank) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_rank_00000, 2)
	p := (*partialResult4Rank)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	return nil
}

func (r *rank) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_rank_00000, 3)
	p := (*partialResult4Rank)(pr)
	p.curIdx++
	if p.curIdx == 1 {
		trace_util_0.Count(_func_rank_00000, 7)
		p.lastRank = 1
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	trace_util_0.Count(_func_rank_00000, 4)
	if r.compareRows(p.rows[p.curIdx-2], p.rows[p.curIdx-1]) == 0 {
		trace_util_0.Count(_func_rank_00000, 8)
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	trace_util_0.Count(_func_rank_00000, 5)
	if r.isDense {
		trace_util_0.Count(_func_rank_00000, 9)
		p.lastRank++
	} else {
		trace_util_0.Count(_func_rank_00000, 10)
		{
			p.lastRank = p.curIdx
		}
	}
	trace_util_0.Count(_func_rank_00000, 6)
	chk.AppendInt64(r.ordinal, p.lastRank)
	return nil
}

type rowComparer struct {
	cmpFuncs []chunk.CompareFunc
	colIdx   []int
}

func buildRowComparer(cols []*expression.Column) rowComparer {
	trace_util_0.Count(_func_rank_00000, 11)
	rc := rowComparer{}
	rc.colIdx = make([]int, 0, len(cols))
	rc.cmpFuncs = make([]chunk.CompareFunc, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_func_rank_00000, 13)
		cmpFunc := chunk.GetCompareFunc(col.RetType)
		if cmpFunc == nil {
			trace_util_0.Count(_func_rank_00000, 15)
			continue
		}
		trace_util_0.Count(_func_rank_00000, 14)
		rc.cmpFuncs = append(rc.cmpFuncs, chunk.GetCompareFunc(col.RetType))
		rc.colIdx = append(rc.colIdx, col.Index)
	}
	trace_util_0.Count(_func_rank_00000, 12)
	return rc
}

func (rc *rowComparer) compareRows(prev, curr chunk.Row) int {
	trace_util_0.Count(_func_rank_00000, 16)
	for i, idx := range rc.colIdx {
		trace_util_0.Count(_func_rank_00000, 18)
		res := rc.cmpFuncs[i](prev, idx, curr, idx)
		if res != 0 {
			trace_util_0.Count(_func_rank_00000, 19)
			return res
		}
	}
	trace_util_0.Count(_func_rank_00000, 17)
	return 0
}

var _func_rank_00000 = "executor/aggfuncs/func_rank.go"
