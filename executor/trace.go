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

package executor

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"sourcegraph.com/sourcegraph/appdash"
	traceImpl "sourcegraph.com/sourcegraph/appdash/opentracing"
)

// TraceExec represents a root executor of trace query.
type TraceExec struct {
	baseExecutor
	// CollectedSpans collects all span during execution. Span is appended via
	// callback method which passes into tracer implementation.
	CollectedSpans []basictracer.RawSpan
	// exhausted being true means there is no more result.
	exhausted bool
	// stmtNode is the real query ast tree and it is used for building real query's plan.
	stmtNode ast.StmtNode
	// rootTrace represents root span which is father of all other span.
	rootTrace opentracing.Span

	builder *executorBuilder
	format  string
}

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_trace_00000, 0)
	req.Reset()
	if e.exhausted {
		trace_util_0.Count(_trace_00000, 9)
		return nil
	}
	trace_util_0.Count(_trace_00000, 1)
	se, ok := e.ctx.(sqlexec.SQLExecutor)
	if !ok {
		trace_util_0.Count(_trace_00000, 10)
		e.exhausted = true
		return nil
	}

	trace_util_0.Count(_trace_00000, 2)
	store := appdash.NewMemoryStore()
	tracer := traceImpl.NewTracer(store)
	span := tracer.StartSpan("trace")
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)
	recordSets, err := se.Execute(ctx, e.stmtNode.Text())
	if err != nil {
		trace_util_0.Count(_trace_00000, 11)
		return errors.Trace(err)
	}

	trace_util_0.Count(_trace_00000, 3)
	for _, rs := range recordSets {
		trace_util_0.Count(_trace_00000, 12)
		_, err = drainRecordSet(ctx, e.ctx, rs)
		if err != nil {
			trace_util_0.Count(_trace_00000, 14)
			return errors.Trace(err)
		}
		trace_util_0.Count(_trace_00000, 13)
		if err = rs.Close(); err != nil {
			trace_util_0.Count(_trace_00000, 15)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_trace_00000, 4)
	traces, err := store.Traces(appdash.TracesOpts{})
	if err != nil {
		trace_util_0.Count(_trace_00000, 16)
		return errors.Trace(err)
	}

	// Row format.
	trace_util_0.Count(_trace_00000, 5)
	if e.format != "json" {
		trace_util_0.Count(_trace_00000, 17)
		if len(traces) < 1 {
			trace_util_0.Count(_trace_00000, 19)
			e.exhausted = true
			return nil
		}
		trace_util_0.Count(_trace_00000, 18)
		trace := traces[0]
		sortTraceByStartTime(trace)
		dfsTree(trace, "", false, req.Chunk)
		e.exhausted = true
		return nil
	}

	// Json format.
	trace_util_0.Count(_trace_00000, 6)
	data, err := json.Marshal(traces)
	if err != nil {
		trace_util_0.Count(_trace_00000, 20)
		return errors.Trace(err)
	}

	// Split json data into rows to avoid the max packet size limitation.
	trace_util_0.Count(_trace_00000, 7)
	const maxRowLen = 4096
	for len(data) > maxRowLen {
		trace_util_0.Count(_trace_00000, 21)
		req.AppendString(0, string(data[:maxRowLen]))
		data = data[maxRowLen:]
	}
	trace_util_0.Count(_trace_00000, 8)
	req.AppendString(0, string(data))
	e.exhausted = true
	return nil
}

func drainRecordSet(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	trace_util_0.Count(_trace_00000, 22)
	var rows []chunk.Row
	req := rs.NewRecordBatch()

	for {
		trace_util_0.Count(_trace_00000, 23)
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			trace_util_0.Count(_trace_00000, 26)
			return rows, errors.Trace(err)
		}
		trace_util_0.Count(_trace_00000, 24)
		iter := chunk.NewIterator4Chunk(req.Chunk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			trace_util_0.Count(_trace_00000, 27)
			rows = append(rows, r)
		}
		trace_util_0.Count(_trace_00000, 25)
		req.Chunk = chunk.Renew(req.Chunk, sctx.GetSessionVars().MaxChunkSize)
	}
}

type sortByStartTime []*appdash.Trace

func (t sortByStartTime) Len() int { trace_util_0.Count(_trace_00000, 28); return len(t) }
func (t sortByStartTime) Less(i, j int) bool {
	trace_util_0.Count(_trace_00000, 29)
	return getStartTime(t[j]).After(getStartTime(t[i]))
}
func (t sortByStartTime) Swap(i, j int) { trace_util_0.Count(_trace_00000, 30); t[i], t[j] = t[j], t[i] }

func getStartTime(trace *appdash.Trace) (t time.Time) {
	trace_util_0.Count(_trace_00000, 31)
	if e, err := trace.TimespanEvent(); err == nil {
		trace_util_0.Count(_trace_00000, 33)
		t = e.Start()
	}
	trace_util_0.Count(_trace_00000, 32)
	return
}

func sortTraceByStartTime(trace *appdash.Trace) {
	trace_util_0.Count(_trace_00000, 34)
	sort.Sort(sortByStartTime(trace.Sub))
	for _, t := range trace.Sub {
		trace_util_0.Count(_trace_00000, 35)
		sortTraceByStartTime(t)
	}
}

func dfsTree(t *appdash.Trace, prefix string, isLast bool, chk *chunk.Chunk) {
	trace_util_0.Count(_trace_00000, 36)
	var newPrefix, suffix string
	if len(prefix) == 0 {
		trace_util_0.Count(_trace_00000, 39)
		newPrefix = prefix + "  "
	} else {
		trace_util_0.Count(_trace_00000, 40)
		{
			if !isLast {
				trace_util_0.Count(_trace_00000, 41)
				suffix = "├─"
				newPrefix = prefix + "│ "
			} else {
				trace_util_0.Count(_trace_00000, 42)
				{
					suffix = "└─"
					newPrefix = prefix + "  "
				}
			}
		}
	}

	trace_util_0.Count(_trace_00000, 37)
	var start time.Time
	var duration time.Duration
	if e, err := t.TimespanEvent(); err == nil {
		trace_util_0.Count(_trace_00000, 43)
		start = e.Start()
		end := e.End()
		duration = end.Sub(start)
	}

	trace_util_0.Count(_trace_00000, 38)
	chk.AppendString(0, prefix+suffix+t.Span.Name())
	chk.AppendString(1, start.Format("15:04:05.000000"))
	chk.AppendString(2, duration.String())

	for i, sp := range t.Sub {
		trace_util_0.Count(_trace_00000, 44)
		dfsTree(sp, newPrefix, i == (len(t.Sub))-1 /*last element of array*/, chk)
	}
}

var _trace_00000 = "executor/trace.go"
