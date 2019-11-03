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

// ntile divides the partition into n ranked groups and returns the group number a row belongs to.
// e.g. We have 11 rows and n = 3. They will be divided into 3 groups.
//      First 4 rows belongs to group 1. Following 4 rows belongs to group 2. The last 3 rows belongs to group 3.
type ntile struct {
	n uint64
	baseAggFunc
}

type partialResult4Ntile struct {
	curIdx      uint64
	curGroupIdx uint64
	remainder   uint64
	quotient    uint64
	rows        []chunk.Row
}

func (n *ntile) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_ntile_00000, 0)
	return PartialResult(&partialResult4Ntile{curGroupIdx: 1})
}

func (n *ntile) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_ntile_00000, 1)
	p := (*partialResult4Ntile)(pr)
	p.curIdx = 0
	p.curGroupIdx = 1
	p.rows = p.rows[:0]
}

func (n *ntile) UpdatePartialResult(_ sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	trace_util_0.Count(_func_ntile_00000, 2)
	p := (*partialResult4Ntile)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	// Update the quotient and remainder.
	if n.n != 0 {
		trace_util_0.Count(_func_ntile_00000, 4)
		p.quotient = uint64(len(p.rows)) / n.n
		p.remainder = uint64(len(p.rows)) % n.n
	}
	trace_util_0.Count(_func_ntile_00000, 3)
	return nil
}

func (n *ntile) AppendFinalResult2Chunk(_ sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_ntile_00000, 5)
	p := (*partialResult4Ntile)(pr)

	// If the divisor is 0, the arg of NTILE would be NULL. So we just return NULL.
	if n.n == 0 {
		trace_util_0.Count(_func_ntile_00000, 9)
		chk.AppendNull(n.ordinal)
		return nil
	}

	trace_util_0.Count(_func_ntile_00000, 6)
	chk.AppendUint64(n.ordinal, p.curGroupIdx)

	p.curIdx++
	curMaxIdx := p.quotient
	if p.curGroupIdx <= p.remainder {
		trace_util_0.Count(_func_ntile_00000, 10)
		curMaxIdx++
	}
	trace_util_0.Count(_func_ntile_00000, 7)
	if p.curIdx == curMaxIdx {
		trace_util_0.Count(_func_ntile_00000, 11)
		p.curIdx = 0
		p.curGroupIdx++
	}
	trace_util_0.Count(_func_ntile_00000, 8)
	return nil
}

var _func_ntile_00000 = "executor/aggfuncs/func_ntile.go"
