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

package distsql

import (
	"fmt"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// RequestBuilder is used to build a "kv.Request".
// It is called before we issue a kv request by "Select".
type RequestBuilder struct {
	kv.Request
	err error
}

// Build builds a "kv.Request".
func (builder *RequestBuilder) Build() (*kv.Request, error) {
	trace_util_0.Count(_request_builder_00000, 0)
	return &builder.Request, builder.err
}

// SetMemTracker sets a memTracker for this request.
func (builder *RequestBuilder) SetMemTracker(sctx sessionctx.Context, label fmt.Stringer) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 1)
	t := memory.NewTracker(label, sctx.GetSessionVars().MemQuotaDistSQL)
	t.AttachTo(sctx.GetSessionVars().StmtCtx.MemTracker)
	builder.Request.MemTracker = t
	return builder
}

// SetTableRanges sets "KeyRanges" for "kv.Request" by converting "tableRanges"
// to "KeyRanges" firstly.
func (builder *RequestBuilder) SetTableRanges(tid int64, tableRanges []*ranger.Range, fb *statistics.QueryFeedback) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 2)
	if builder.err != nil {
		trace_util_0.Count(_request_builder_00000, 4)
		return builder
	}
	trace_util_0.Count(_request_builder_00000, 3)
	builder.Request.KeyRanges = TableRangesToKVRanges(tid, tableRanges, fb)
	return builder
}

// SetIndexRanges sets "KeyRanges" for "kv.Request" by converting index range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 5)
	if builder.err != nil {
		trace_util_0.Count(_request_builder_00000, 7)
		return builder
	}
	trace_util_0.Count(_request_builder_00000, 6)
	builder.Request.KeyRanges, builder.err = IndexRangesToKVRanges(sc, tid, idxID, ranges, nil)
	return builder
}

// SetTableHandles sets "KeyRanges" for "kv.Request" by converting table handles
// "handles" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetTableHandles(tid int64, handles []int64) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 8)
	builder.Request.KeyRanges = TableHandlesToKVRanges(tid, handles)
	return builder
}

// SetDAGRequest sets the request type to "ReqTypeDAG" and construct request data.
func (builder *RequestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 9)
	if builder.err != nil {
		trace_util_0.Count(_request_builder_00000, 11)
		return builder
	}

	trace_util_0.Count(_request_builder_00000, 10)
	builder.Request.Tp = kv.ReqTypeDAG
	builder.Request.StartTs = dag.StartTs
	builder.Request.Data, builder.err = dag.Marshal()
	return builder
}

// SetAnalyzeRequest sets the request type to "ReqTypeAnalyze" and cosntruct request data.
func (builder *RequestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 12)
	if builder.err != nil {
		trace_util_0.Count(_request_builder_00000, 14)
		return builder
	}

	trace_util_0.Count(_request_builder_00000, 13)
	builder.Request.Tp = kv.ReqTypeAnalyze
	builder.Request.StartTs = ana.StartTs
	builder.Request.Data, builder.err = ana.Marshal()
	builder.Request.NotFillCache = true
	builder.Request.IsolationLevel = kv.RC
	builder.Request.Priority = kv.PriorityLow
	return builder
}

// SetChecksumRequest sets the request type to "ReqTypeChecksum" and construct request data.
func (builder *RequestBuilder) SetChecksumRequest(checksum *tipb.ChecksumRequest) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 15)
	if builder.err != nil {
		trace_util_0.Count(_request_builder_00000, 17)
		return builder
	}

	trace_util_0.Count(_request_builder_00000, 16)
	builder.Request.Tp = kv.ReqTypeChecksum
	builder.Request.StartTs = checksum.StartTs
	builder.Request.Data, builder.err = checksum.Marshal()
	builder.Request.NotFillCache = true
	return builder
}

// SetKeyRanges sets "KeyRanges" for "kv.Request".
func (builder *RequestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 18)
	builder.Request.KeyRanges = keyRanges
	return builder
}

// SetDesc sets "Desc" for "kv.Request".
func (builder *RequestBuilder) SetDesc(desc bool) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 19)
	builder.Request.Desc = desc
	return builder
}

// SetKeepOrder sets "KeepOrder" for "kv.Request".
func (builder *RequestBuilder) SetKeepOrder(order bool) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 20)
	builder.Request.KeepOrder = order
	return builder
}

func (builder *RequestBuilder) getIsolationLevel() kv.IsoLevel {
	trace_util_0.Count(_request_builder_00000, 21)
	switch builder.Tp {
	case kv.ReqTypeAnalyze:
		trace_util_0.Count(_request_builder_00000, 23)
		return kv.RC
	}
	trace_util_0.Count(_request_builder_00000, 22)
	return kv.SI
}

func (builder *RequestBuilder) getKVPriority(sv *variable.SessionVars) int {
	trace_util_0.Count(_request_builder_00000, 24)
	switch sv.StmtCtx.Priority {
	case mysql.NoPriority, mysql.DelayedPriority:
		trace_util_0.Count(_request_builder_00000, 26)
		return kv.PriorityNormal
	case mysql.LowPriority:
		trace_util_0.Count(_request_builder_00000, 27)
		return kv.PriorityLow
	case mysql.HighPriority:
		trace_util_0.Count(_request_builder_00000, 28)
		return kv.PriorityHigh
	}
	trace_util_0.Count(_request_builder_00000, 25)
	return kv.PriorityNormal
}

// SetFromSessionVars sets the following fields for "kv.Request" from session variables:
// "Concurrency", "IsolationLevel", "NotFillCache".
func (builder *RequestBuilder) SetFromSessionVars(sv *variable.SessionVars) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 29)
	builder.Request.Concurrency = sv.DistSQLScanConcurrency
	builder.Request.IsolationLevel = builder.getIsolationLevel()
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	builder.Request.Priority = builder.getKVPriority(sv)
	return builder
}

// SetStreaming sets "Streaming" flag for "kv.Request".
func (builder *RequestBuilder) SetStreaming(streaming bool) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 30)
	builder.Request.Streaming = streaming
	return builder
}

// SetConcurrency sets "Concurrency" for "kv.Request".
func (builder *RequestBuilder) SetConcurrency(concurrency int) *RequestBuilder {
	trace_util_0.Count(_request_builder_00000, 31)
	builder.Request.Concurrency = concurrency
	return builder
}

// TableRangesToKVRanges converts table ranges to "KeyRange".
func TableRangesToKVRanges(tid int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []kv.KeyRange {
	trace_util_0.Count(_request_builder_00000, 32)
	if fb == nil || fb.Hist == nil {
		trace_util_0.Count(_request_builder_00000, 35)
		return tableRangesToKVRangesWithoutSplit(tid, ranges)
	}
	trace_util_0.Count(_request_builder_00000, 33)
	krs := make([]kv.KeyRange, 0, len(ranges))
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_request_builder_00000, 36)
		low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
		high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
		if ran.LowExclude {
			trace_util_0.Count(_request_builder_00000, 39)
			low = []byte(kv.Key(low).PrefixNext())
		}
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PerfixNext` will make the
		// high value greater than upper bound, so we store the range here.
		trace_util_0.Count(_request_builder_00000, 37)
		r := &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}}
		feedbackRanges = append(feedbackRanges, r)

		if !ran.HighExclude {
			trace_util_0.Count(_request_builder_00000, 40)
			high = []byte(kv.Key(high).PrefixNext())
		}
		trace_util_0.Count(_request_builder_00000, 38)
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	trace_util_0.Count(_request_builder_00000, 34)
	fb.StoreRanges(feedbackRanges)
	return krs
}

func tableRangesToKVRangesWithoutSplit(tid int64, ranges []*ranger.Range) []kv.KeyRange {
	trace_util_0.Count(_request_builder_00000, 41)
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_request_builder_00000, 43)
		low, high := encodeHandleKey(ran)
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	trace_util_0.Count(_request_builder_00000, 42)
	return krs
}

func encodeHandleKey(ran *ranger.Range) ([]byte, []byte) {
	trace_util_0.Count(_request_builder_00000, 44)
	low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
	high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
	if ran.LowExclude {
		trace_util_0.Count(_request_builder_00000, 47)
		low = []byte(kv.Key(low).PrefixNext())
	}
	trace_util_0.Count(_request_builder_00000, 45)
	if !ran.HighExclude {
		trace_util_0.Count(_request_builder_00000, 48)
		high = []byte(kv.Key(high).PrefixNext())
	}
	trace_util_0.Count(_request_builder_00000, 46)
	return low, high
}

// TableHandlesToKVRanges converts sorted handle to kv ranges.
// For continuous handles, we should merge them to a single key range.
func TableHandlesToKVRanges(tid int64, handles []int64) []kv.KeyRange {
	trace_util_0.Count(_request_builder_00000, 49)
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		trace_util_0.Count(_request_builder_00000, 51)
		j := i + 1
		for ; j < len(handles) && handles[j-1] != math.MaxInt64; j++ {
			trace_util_0.Count(_request_builder_00000, 53)
			if handles[j] != handles[j-1]+1 {
				trace_util_0.Count(_request_builder_00000, 54)
				break
			}
		}
		trace_util_0.Count(_request_builder_00000, 52)
		low := codec.EncodeInt(nil, handles[i])
		high := codec.EncodeInt(nil, handles[j-1])
		high = []byte(kv.Key(high).PrefixNext())
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	trace_util_0.Count(_request_builder_00000, 50)
	return krs
}

// IndexRangesToKVRanges converts index ranges to "KeyRange".
func IndexRangesToKVRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	trace_util_0.Count(_request_builder_00000, 55)
	if fb == nil || fb.Hist == nil {
		trace_util_0.Count(_request_builder_00000, 60)
		return indexRangesToKVWithoutSplit(sc, tid, idxID, ranges)
	}
	trace_util_0.Count(_request_builder_00000, 56)
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_request_builder_00000, 61)
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			trace_util_0.Count(_request_builder_00000, 63)
			return nil, err
		}
		trace_util_0.Count(_request_builder_00000, 62)
		feedbackRanges = append(feedbackRanges, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true})
	}
	trace_util_0.Count(_request_builder_00000, 57)
	feedbackRanges, ok := fb.Hist.SplitRange(sc, feedbackRanges, true)
	if !ok {
		trace_util_0.Count(_request_builder_00000, 64)
		fb.Invalidate()
	}
	trace_util_0.Count(_request_builder_00000, 58)
	krs := make([]kv.KeyRange, 0, len(feedbackRanges))
	for _, ran := range feedbackRanges {
		trace_util_0.Count(_request_builder_00000, 65)
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			trace_util_0.Count(_request_builder_00000, 68)
			low = kv.Key(low).PrefixNext()
		}
		trace_util_0.Count(_request_builder_00000, 66)
		ran.LowVal[0].SetBytes(low)
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PerfixNext` will make the
		// high value greater than upper bound, so we store the high value here.
		ran.HighVal[0].SetBytes(high)
		if !ran.HighExclude {
			trace_util_0.Count(_request_builder_00000, 69)
			high = kv.Key(high).PrefixNext()
		}
		trace_util_0.Count(_request_builder_00000, 67)
		startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	trace_util_0.Count(_request_builder_00000, 59)
	fb.StoreRanges(feedbackRanges)
	return krs, nil
}

func indexRangesToKVWithoutSplit(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range) ([]kv.KeyRange, error) {
	trace_util_0.Count(_request_builder_00000, 70)
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_request_builder_00000, 72)
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			trace_util_0.Count(_request_builder_00000, 74)
			return nil, err
		}
		trace_util_0.Count(_request_builder_00000, 73)
		startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	trace_util_0.Count(_request_builder_00000, 71)
	return krs, nil
}

func encodeIndexKey(sc *stmtctx.StatementContext, ran *ranger.Range) ([]byte, []byte, error) {
	trace_util_0.Count(_request_builder_00000, 75)
	low, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		trace_util_0.Count(_request_builder_00000, 82)
		return nil, nil, err
	}
	trace_util_0.Count(_request_builder_00000, 76)
	if ran.LowExclude {
		trace_util_0.Count(_request_builder_00000, 83)
		low = []byte(kv.Key(low).PrefixNext())
	}
	trace_util_0.Count(_request_builder_00000, 77)
	high, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		trace_util_0.Count(_request_builder_00000, 84)
		return nil, nil, err
	}

	trace_util_0.Count(_request_builder_00000, 78)
	if !ran.HighExclude {
		trace_util_0.Count(_request_builder_00000, 85)
		high = []byte(kv.Key(high).PrefixNext())
	}

	trace_util_0.Count(_request_builder_00000, 79)
	var hasNull bool
	for _, highVal := range ran.HighVal {
		trace_util_0.Count(_request_builder_00000, 86)
		if highVal.IsNull() {
			trace_util_0.Count(_request_builder_00000, 87)
			hasNull = true
			break
		}
	}

	trace_util_0.Count(_request_builder_00000, 80)
	if hasNull {
		trace_util_0.Count(_request_builder_00000, 88)
		// Append 0 to make unique-key range [null, null] to be a scan rather than point-get.
		high = []byte(kv.Key(high).Next())
	}
	trace_util_0.Count(_request_builder_00000, 81)
	return low, high, nil
}

var _request_builder_00000 = "distsql/request_builder.go"
