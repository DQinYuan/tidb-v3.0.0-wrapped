// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

// ExplainExec represents an explain executor.
type ExplainExec struct {
	baseExecutor

	explain     *core.Explain
	analyzeExec Executor
	rows        [][]string
	cursor      int
}

// Open implements the Executor Open interface.
func (e *ExplainExec) Open(ctx context.Context) error {
	trace_util_0.Count(_explain_00000, 0)
	if e.analyzeExec != nil {
		trace_util_0.Count(_explain_00000, 2)
		return e.analyzeExec.Open(ctx)
	}
	trace_util_0.Count(_explain_00000, 1)
	return nil
}

// Close implements the Executor Close interface.
func (e *ExplainExec) Close() error {
	trace_util_0.Count(_explain_00000, 3)
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *ExplainExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_explain_00000, 4)
	if e.rows == nil {
		trace_util_0.Count(_explain_00000, 8)
		var err error
		e.rows, err = e.generateExplainInfo(ctx)
		if err != nil {
			trace_util_0.Count(_explain_00000, 9)
			return err
		}
	}

	trace_util_0.Count(_explain_00000, 5)
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		trace_util_0.Count(_explain_00000, 10)
		return nil
	}

	trace_util_0.Count(_explain_00000, 6)
	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		trace_util_0.Count(_explain_00000, 11)
		for j := range e.rows[i] {
			trace_util_0.Count(_explain_00000, 12)
			req.AppendString(j, e.rows[i][j])
		}
	}
	trace_util_0.Count(_explain_00000, 7)
	e.cursor += numCurRows
	return nil
}

func (e *ExplainExec) generateExplainInfo(ctx context.Context) ([][]string, error) {
	trace_util_0.Count(_explain_00000, 13)
	if e.analyzeExec != nil {
		trace_util_0.Count(_explain_00000, 17)
		chk := newFirstChunk(e.analyzeExec)
		for {
			trace_util_0.Count(_explain_00000, 19)
			err := e.analyzeExec.Next(ctx, chunk.NewRecordBatch(chk))
			if err != nil {
				trace_util_0.Count(_explain_00000, 21)
				return nil, err
			}
			trace_util_0.Count(_explain_00000, 20)
			if chk.NumRows() == 0 {
				trace_util_0.Count(_explain_00000, 22)
				break
			}
		}
		trace_util_0.Count(_explain_00000, 18)
		if err := e.analyzeExec.Close(); err != nil {
			trace_util_0.Count(_explain_00000, 23)
			return nil, err
		}
	}
	trace_util_0.Count(_explain_00000, 14)
	if err := e.explain.RenderResult(); err != nil {
		trace_util_0.Count(_explain_00000, 24)
		return nil, err
	}
	trace_util_0.Count(_explain_00000, 15)
	if e.analyzeExec != nil {
		trace_util_0.Count(_explain_00000, 25)
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	}
	trace_util_0.Count(_explain_00000, 16)
	return e.explain.Rows, nil
}

var _explain_00000 = "executor/explain.go"
