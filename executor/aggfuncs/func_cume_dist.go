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

type cumeDist struct {
	baseAggFunc
	rowComparer
}

type partialResult4CumeDist struct {
	curIdx   int
	lastRank int
	rows     []chunk.Row
}

func (r *cumeDist) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_cume_dist_00000, 0)
	return PartialResult(&partialResult4Rank{})
}

func (r *cumeDist) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_cume_dist_00000, 1)
	p := (*partialResult4Rank)(pr)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (r *cumeDist) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_cume_dist_00000, 2)
	p := (*partialResult4CumeDist)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	return nil
}

func (r *cumeDist) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_cume_dist_00000, 3)
	p := (*partialResult4CumeDist)(pr)
	numRows := len(p.rows)
	for p.lastRank < numRows && r.compareRows(p.rows[p.curIdx], p.rows[p.lastRank]) == 0 {
		trace_util_0.Count(_func_cume_dist_00000, 5)
		p.lastRank++
	}
	trace_util_0.Count(_func_cume_dist_00000, 4)
	p.curIdx++
	chk.AppendFloat64(r.ordinal, float64(p.lastRank)/float64(numRows))
	return nil
}

var _func_cume_dist_00000 = "executor/aggfuncs/func_cume_dist.go"
