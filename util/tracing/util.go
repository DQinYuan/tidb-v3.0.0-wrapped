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

package tracing

import (
	"context"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/trace_util_0"
)

// TiDBTrace is set as Baggage on traces which are used for tidb tracing.
const TiDBTrace = "tr"

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	trace_util_0.Count(_util_00000, 0)
	cr(sp)
}

// NewRecordedTrace returns a Span which records directly via the specified
// callback.
func NewRecordedTrace(opName string, callback func(sp basictracer.RawSpan)) opentracing.Span {
	trace_util_0.Count(_util_00000, 1)
	tr := basictracer.New(CallbackRecorder(callback))
	opentracing.SetGlobalTracer(tr)
	sp := tr.StartSpan(opName)
	sp.SetBaggageItem(TiDBTrace, "1")
	return sp
}

// noopSpan returns a Span which discards all operations.
func noopSpan() opentracing.Span {
	trace_util_0.Count(_util_00000, 2)
	return (opentracing.NoopTracer{}).StartSpan("DefaultSpan")
}

// SpanFromContext returns the span obtained from the context or, if none is found, a new one started through tracer.
func SpanFromContext(ctx context.Context) (sp opentracing.Span) {
	trace_util_0.Count(_util_00000, 3)
	if sp = opentracing.SpanFromContext(ctx); sp == nil {
		trace_util_0.Count(_util_00000, 5)
		return noopSpan()
	}
	trace_util_0.Count(_util_00000, 4)
	return sp
}

// ChildSpanFromContxt return a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContxt(ctx context.Context, opName string) (opentracing.Span, context.Context) {
	trace_util_0.Count(_util_00000, 6)
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		trace_util_0.Count(_util_00000, 8)
		if _, ok := sp.Tracer().(opentracing.NoopTracer); !ok {
			trace_util_0.Count(_util_00000, 9)
			child := opentracing.StartSpan(opName, opentracing.ChildOf(sp.Context()))
			return child, opentracing.ContextWithSpan(ctx, child)
		}
	}
	trace_util_0.Count(_util_00000, 7)
	return noopSpan(), ctx
}

var _util_00000 = "util/tracing/util.go"
