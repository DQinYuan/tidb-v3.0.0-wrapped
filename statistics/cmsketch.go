// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/spaolacci/murmur3"
)

// topNThreshold is the minimum ratio of the number of topn elements in CMSketch, 10 means 1 / 10 = 10%.
const topNThreshold = uint64(10)

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
type CMSketch struct {
	depth        int32
	width        int32
	count        uint64 // TopN is not counted in count
	defaultValue uint64 // In sampled data, if cmsketch returns a small value (less than avg value / 2), then this will returned.
	table        [][]uint32
	topN         map[uint64][]*TopNMeta
}

// TopNMeta is a simple counter used by BuildTopN.
type TopNMeta struct {
	h2    uint64 // h2 is the second part of `murmur3.Sum128()`, it is always used with the first part `h1`.
	Data  []byte
	Count uint64
}

// NewCMSketch returns a new CM sketch.
func NewCMSketch(d, w int32) *CMSketch {
	trace_util_0.Count(_cmsketch_00000, 0)
	tbl := make([][]uint32, d)
	for i := range tbl {
		trace_util_0.Count(_cmsketch_00000, 2)
		tbl[i] = make([]uint32, w)
	}
	trace_util_0.Count(_cmsketch_00000, 1)
	return &CMSketch{depth: d, width: w, table: tbl}
}

// topNHelper wraps some variables used when building cmsketch with top n.
type topNHelper struct {
	sampleSize    uint64
	counter       map[hack.MutableString]uint64
	sorted        []uint64
	onlyOnceItems uint64
	sumTopN       uint64
	lastVal       uint64
}

func newTopNHelper(sample [][]byte, numTop uint32) *topNHelper {
	trace_util_0.Count(_cmsketch_00000, 3)
	counter := make(map[hack.MutableString]uint64)
	for i := range sample {
		trace_util_0.Count(_cmsketch_00000, 8)
		counter[hack.String(sample[i])]++
	}
	trace_util_0.Count(_cmsketch_00000, 4)
	sorted, onlyOnceItems := make([]uint64, 0, len(counter)), uint64(0)
	for _, cnt := range counter {
		trace_util_0.Count(_cmsketch_00000, 9)
		sorted = append(sorted, cnt)
		if cnt == 1 {
			trace_util_0.Count(_cmsketch_00000, 10)
			onlyOnceItems++
		}
	}
	trace_util_0.Count(_cmsketch_00000, 5)
	sort.Slice(sorted, func(i, j int) bool {
		trace_util_0.Count(_cmsketch_00000, 11)
		return sorted[i] > sorted[j]
	})

	trace_util_0.Count(_cmsketch_00000, 6)
	var (
		// last is the last element in top N index should occurres atleast `last` times.
		last      uint64
		sumTopN   uint64
		sampleNDV = uint32(len(sorted))
	)
	numTop = mathutil.MinUint32(sampleNDV, numTop) // Ensure numTop no larger than sampNDV.
	// Only element whose frequency is not smaller than 2/3 multiples the
	// frequency of the n-th element are added to the TopN statistics. We chose
	// 2/3 as an empirical value because the average cardinality estimation
	// error is relatively small compared with 1/2.
	for i := uint32(0); i < sampleNDV && i < numTop*2; i++ {
		trace_util_0.Count(_cmsketch_00000, 12)
		if i >= numTop && sorted[i]*3 < sorted[numTop-1]*2 && last != sorted[i] {
			trace_util_0.Count(_cmsketch_00000, 14)
			break
		}
		trace_util_0.Count(_cmsketch_00000, 13)
		last = sorted[i]
		sumTopN += sorted[i]
	}

	trace_util_0.Count(_cmsketch_00000, 7)
	return &topNHelper{uint64(len(sample)), counter, sorted, onlyOnceItems, sumTopN, last}
}

// NewCMSketchWithTopN returns a new CM sketch with TopN elements, the estimate NDV and the scale ratio.
func NewCMSketchWithTopN(d, w int32, sample [][]byte, numTop uint32, rowCount uint64) (*CMSketch, uint64, uint64) {
	trace_util_0.Count(_cmsketch_00000, 15)
	helper := newTopNHelper(sample, numTop)
	// rowCount is not a accurate value when fast analyzing
	// In some cases, if user triggers fast analyze when rowCount is close to sampleSize, unexpected bahavior might happen.
	rowCount = mathutil.MaxUint64(rowCount, uint64(len(sample)))
	estimateNDV, scaleRatio := calculateEstimateNDV(helper, rowCount)
	defaultVal := calculateDefaultVal(helper, estimateNDV, scaleRatio, rowCount)
	c := buildCMSWithTopN(helper, d, w, scaleRatio, defaultVal)
	return c, estimateNDV, scaleRatio
}

func buildCMSWithTopN(helper *topNHelper, d, w int32, scaleRatio uint64, defaultVal uint64) (c *CMSketch) {
	trace_util_0.Count(_cmsketch_00000, 16)
	c = NewCMSketch(d, w)
	enableTopN := helper.sampleSize/topNThreshold <= helper.sumTopN
	if enableTopN {
		trace_util_0.Count(_cmsketch_00000, 19)
		c.topN = make(map[uint64][]*TopNMeta)
	}
	trace_util_0.Count(_cmsketch_00000, 17)
	c.defaultValue = defaultVal
	for counterKey, cnt := range helper.counter {
		trace_util_0.Count(_cmsketch_00000, 20)
		data := hack.Slice(string(counterKey))
		// If the value only occurred once in the sample, we assumes that there is no difference with
		// value that does not occurred in the sample.
		rowCount := defaultVal
		if cnt > 1 {
			trace_util_0.Count(_cmsketch_00000, 22)
			rowCount = cnt * scaleRatio
		}
		trace_util_0.Count(_cmsketch_00000, 21)
		if enableTopN && cnt >= helper.lastVal {
			trace_util_0.Count(_cmsketch_00000, 23)
			h1, h2 := murmur3.Sum128(data)
			c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, data, rowCount})
		} else {
			trace_util_0.Count(_cmsketch_00000, 24)
			{
				c.insertBytesByCount(data, rowCount)
			}
		}
	}
	trace_util_0.Count(_cmsketch_00000, 18)
	return
}

func calculateDefaultVal(helper *topNHelper, estimateNDV, scaleRatio, rowCount uint64) uint64 {
	trace_util_0.Count(_cmsketch_00000, 25)
	sampleNDV := uint64(len(helper.sorted))
	if rowCount <= (helper.sampleSize-uint64(helper.onlyOnceItems))*scaleRatio {
		trace_util_0.Count(_cmsketch_00000, 27)
		return 1
	}
	trace_util_0.Count(_cmsketch_00000, 26)
	estimateRemainingCount := rowCount - (helper.sampleSize-uint64(helper.onlyOnceItems))*scaleRatio
	return estimateRemainingCount / mathutil.MaxUint64(1, estimateNDV-uint64(sampleNDV)+helper.onlyOnceItems)
}

func (c *CMSketch) findTopNMeta(h1, h2 uint64, d []byte) *TopNMeta {
	trace_util_0.Count(_cmsketch_00000, 28)
	for _, meta := range c.topN[h1] {
		trace_util_0.Count(_cmsketch_00000, 30)
		if meta.h2 == h2 && bytes.Equal(d, meta.Data) {
			trace_util_0.Count(_cmsketch_00000, 31)
			return meta
		}
	}
	trace_util_0.Count(_cmsketch_00000, 29)
	return nil
}

// queryAddTopN TopN adds count to CMSketch.topN if exists, and returns the count of such elements after insert.
// If such elements does not in topn elements, nothing will happen and false will be returned.
func (c *CMSketch) updateTopNWithDelta(h1, h2 uint64, d []byte, delta uint64) bool {
	trace_util_0.Count(_cmsketch_00000, 32)
	if c.topN == nil {
		trace_util_0.Count(_cmsketch_00000, 35)
		return false
	}
	trace_util_0.Count(_cmsketch_00000, 33)
	meta := c.findTopNMeta(h1, h2, d)
	if meta != nil {
		trace_util_0.Count(_cmsketch_00000, 36)
		meta.Count += delta
		return true
	}
	trace_util_0.Count(_cmsketch_00000, 34)
	return false
}

func (c *CMSketch) queryTopN(h1, h2 uint64, d []byte) (uint64, bool) {
	trace_util_0.Count(_cmsketch_00000, 37)
	if c.topN == nil {
		trace_util_0.Count(_cmsketch_00000, 40)
		return 0, false
	}
	trace_util_0.Count(_cmsketch_00000, 38)
	meta := c.findTopNMeta(h1, h2, d)
	if meta != nil {
		trace_util_0.Count(_cmsketch_00000, 41)
		return meta.Count, true
	}
	trace_util_0.Count(_cmsketch_00000, 39)
	return 0, false
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	trace_util_0.Count(_cmsketch_00000, 42)
	c.insertBytesByCount(bytes, 1)
}

// insertBytesByCount adds the bytes value into the TopN (if value already in TopN) or CM Sketch by delta, this does not updates c.defaultValue.
func (c *CMSketch) insertBytesByCount(bytes []byte, count uint64) {
	trace_util_0.Count(_cmsketch_00000, 43)
	h1, h2 := murmur3.Sum128(bytes)
	if c.updateTopNWithDelta(h1, h2, bytes, count) {
		trace_util_0.Count(_cmsketch_00000, 45)
		return
	}
	trace_util_0.Count(_cmsketch_00000, 44)
	c.count += count
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 46)
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += uint32(count)
	}
}

func (c *CMSketch) considerDefVal(cnt uint64) bool {
	trace_util_0.Count(_cmsketch_00000, 47)
	return (cnt == 0 || (cnt > c.defaultValue && cnt < 2*(c.count/uint64(c.width)))) && c.defaultValue > 0
}

// updateValueBytes updates value of d to count.
func (c *CMSketch) updateValueBytes(d []byte, count uint64) {
	trace_util_0.Count(_cmsketch_00000, 48)
	h1, h2 := murmur3.Sum128(d)
	if oriCount, ok := c.queryTopN(h1, h2, d); ok {
		trace_util_0.Count(_cmsketch_00000, 50)
		deltaCount := count - oriCount
		c.updateTopNWithDelta(h1, h2, d, deltaCount)
	}
	trace_util_0.Count(_cmsketch_00000, 49)
	c.setValue(h1, h2, count)
}

// setValue sets the count for value that hashed into (h1, h2), and update defaultValue if necessary.
func (c *CMSketch) setValue(h1, h2 uint64, count uint64) {
	trace_util_0.Count(_cmsketch_00000, 51)
	oriCount := c.queryHashValue(h1, h2)
	if c.considerDefVal(oriCount) {
		trace_util_0.Count(_cmsketch_00000, 53)
		// We should update c.defaultValue if we used c.defaultValue when getting the estimate count.
		// This should make estimation better, remove this line if it does not work as expected.
		c.defaultValue = uint64(float64(c.defaultValue)*0.95 + float64(c.defaultValue)*0.05)
		if c.defaultValue == 0 {
			trace_util_0.Count(_cmsketch_00000, 54)
			// c.defaultValue never guess 0 since we are using a sampled data.
			c.defaultValue = 1
		}
	}

	trace_util_0.Count(_cmsketch_00000, 52)
	c.count += count - oriCount
	// let it overflow naturally
	deltaCount := uint32(count) - uint32(oriCount)
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 55)
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] + deltaCount
	}
}

func (c *CMSketch) queryValue(sc *stmtctx.StatementContext, val types.Datum) (uint64, error) {
	trace_util_0.Count(_cmsketch_00000, 56)
	bytes, err := codec.EncodeValue(sc, nil, val)
	if err != nil {
		trace_util_0.Count(_cmsketch_00000, 58)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_cmsketch_00000, 57)
	return c.QueryBytes(bytes), nil
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint64 {
	trace_util_0.Count(_cmsketch_00000, 59)
	h1, h2 := murmur3.Sum128(d)
	if count, ok := c.queryTopN(h1, h2, d); ok {
		trace_util_0.Count(_cmsketch_00000, 61)
		return count
	}
	trace_util_0.Count(_cmsketch_00000, 60)
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint64 {
	trace_util_0.Count(_cmsketch_00000, 62)
	vals := make([]uint32, c.depth)
	min := uint32(math.MaxUint32)
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 66)
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		if min > c.table[i][j] {
			trace_util_0.Count(_cmsketch_00000, 68)
			min = c.table[i][j]
		}
		trace_util_0.Count(_cmsketch_00000, 67)
		noise := (c.count - uint64(c.table[i][j])) / (uint64(c.width) - 1)
		if uint64(c.table[i][j]) < noise {
			trace_util_0.Count(_cmsketch_00000, 69)
			vals[i] = 0
		} else {
			trace_util_0.Count(_cmsketch_00000, 70)
			{
				vals[i] = c.table[i][j] - uint32(noise)
			}
		}
	}
	trace_util_0.Count(_cmsketch_00000, 63)
	sort.Sort(sortutil.Uint32Slice(vals))
	res := vals[(c.depth-1)/2] + (vals[c.depth/2]-vals[(c.depth-1)/2])/2
	if res > min {
		trace_util_0.Count(_cmsketch_00000, 71)
		res = min
	}
	trace_util_0.Count(_cmsketch_00000, 64)
	if c.considerDefVal(uint64(res)) {
		trace_util_0.Count(_cmsketch_00000, 72)
		return c.defaultValue
	}
	trace_util_0.Count(_cmsketch_00000, 65)
	return uint64(res)
}

func (c *CMSketch) mergeTopN(lTopN map[uint64][]*TopNMeta, rTopN map[uint64][]*TopNMeta, numTop uint32) {
	trace_util_0.Count(_cmsketch_00000, 73)
	counter := make(map[hack.MutableString]uint64)
	for _, metas := range lTopN {
		trace_util_0.Count(_cmsketch_00000, 78)
		for _, meta := range metas {
			trace_util_0.Count(_cmsketch_00000, 79)
			counter[hack.String(meta.Data)] += meta.Count
		}
	}
	trace_util_0.Count(_cmsketch_00000, 74)
	for _, metas := range rTopN {
		trace_util_0.Count(_cmsketch_00000, 80)
		for _, meta := range metas {
			trace_util_0.Count(_cmsketch_00000, 81)
			counter[hack.String(meta.Data)] += meta.Count
		}
	}
	trace_util_0.Count(_cmsketch_00000, 75)
	sorted := make([]uint64, len(counter))
	for _, cnt := range counter {
		trace_util_0.Count(_cmsketch_00000, 82)
		sorted = append(sorted, cnt)
	}
	trace_util_0.Count(_cmsketch_00000, 76)
	sort.Slice(sorted, func(i, j int) bool {
		trace_util_0.Count(_cmsketch_00000, 83)
		return sorted[i] > sorted[j]
	})
	trace_util_0.Count(_cmsketch_00000, 77)
	numTop = mathutil.MinUint32(uint32(len(counter)), numTop)
	lastTopCnt := sorted[numTop-1]
	c.topN = make(map[uint64][]*TopNMeta)
	for value, cnt := range counter {
		trace_util_0.Count(_cmsketch_00000, 84)
		data := hack.Slice(string(value))
		if cnt >= lastTopCnt {
			trace_util_0.Count(_cmsketch_00000, 85)
			h1, h2 := murmur3.Sum128(data)
			c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, data, cnt})
		} else {
			trace_util_0.Count(_cmsketch_00000, 86)
			{
				c.insertBytesByCount(data, cnt)
			}
		}
	}
}

// MergeCMSketch merges two CM Sketch.
func (c *CMSketch) MergeCMSketch(rc *CMSketch, numTopN uint32) error {
	trace_util_0.Count(_cmsketch_00000, 87)
	if c.depth != rc.depth || c.width != rc.width {
		trace_util_0.Count(_cmsketch_00000, 91)
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	trace_util_0.Count(_cmsketch_00000, 88)
	if c.topN != nil || rc.topN != nil {
		trace_util_0.Count(_cmsketch_00000, 92)
		c.mergeTopN(c.topN, rc.topN, numTopN)
	}
	trace_util_0.Count(_cmsketch_00000, 89)
	c.count += rc.count
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 93)
		for j := range c.table[i] {
			trace_util_0.Count(_cmsketch_00000, 94)
			c.table[i][j] += rc.table[i][j]
		}
	}
	trace_util_0.Count(_cmsketch_00000, 90)
	return nil
}

// MergeCMSketch4IncrementalAnalyze merges two CM Sketch for incremental analyze. Since there is no value
// that appears partially in `c` and `rc` for incremental analyze, it uses `max` to merge them.
// Here is a simple proof: when we query from the CM sketch, we use the `min` to get the answer:
//   (1): For values that only appears in `c, using `max` to merge them affects the `min` query result less than using `sum`;
//   (2): For values that only appears in `rc`, it is the same as condition (1);
//   (3): For values that appears both in `c` and `rc`, if they do not appear partially in `c` and `rc`, for example,
//        if `v` appears 5 times in the table, it can appears 5 times in `c` and 3 times in `rc`, then `max` also gives the correct answer.
// So in fact, if we can know the number of appearances of each value in the first place, it is better to use `max` to construct the CM sketch rather than `sum`.
func (c *CMSketch) MergeCMSketch4IncrementalAnalyze(rc *CMSketch) error {
	trace_util_0.Count(_cmsketch_00000, 95)
	if c.depth != rc.depth || c.width != rc.width {
		trace_util_0.Count(_cmsketch_00000, 99)
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	trace_util_0.Count(_cmsketch_00000, 96)
	if c.topN != nil || rc.topN != nil {
		trace_util_0.Count(_cmsketch_00000, 100)
		return errors.New("CMSketch with Top-N does not support merge")
	}
	trace_util_0.Count(_cmsketch_00000, 97)
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 101)
		c.count = 0
		for j := range c.table[i] {
			trace_util_0.Count(_cmsketch_00000, 102)
			c.table[i][j] = mathutil.MaxUint32(c.table[i][j], rc.table[i][j])
			c.count += uint64(c.table[i][j])
		}
	}
	trace_util_0.Count(_cmsketch_00000, 98)
	return nil
}

// CMSketchToProto converts CMSketch to its protobuf representation.
func CMSketchToProto(c *CMSketch) *tipb.CMSketch {
	trace_util_0.Count(_cmsketch_00000, 103)
	protoSketch := &tipb.CMSketch{Rows: make([]*tipb.CMSketchRow, c.depth)}
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 106)
		protoSketch.Rows[i] = &tipb.CMSketchRow{Counters: make([]uint32, c.width)}
		for j := range c.table[i] {
			trace_util_0.Count(_cmsketch_00000, 107)
			protoSketch.Rows[i].Counters[j] = c.table[i][j]
		}
	}
	trace_util_0.Count(_cmsketch_00000, 104)
	for _, dataSlice := range c.topN {
		trace_util_0.Count(_cmsketch_00000, 108)
		for _, dataMeta := range dataSlice {
			trace_util_0.Count(_cmsketch_00000, 109)
			protoSketch.TopN = append(protoSketch.TopN, &tipb.CMSketchTopN{Data: dataMeta.Data, Count: dataMeta.Count})
		}
	}
	trace_util_0.Count(_cmsketch_00000, 105)
	protoSketch.DefaultValue = c.defaultValue
	return protoSketch
}

// CMSketchFromProto converts CMSketch from its protobuf representation.
func CMSketchFromProto(protoSketch *tipb.CMSketch) *CMSketch {
	trace_util_0.Count(_cmsketch_00000, 110)
	if protoSketch == nil {
		trace_util_0.Count(_cmsketch_00000, 115)
		return nil
	}
	trace_util_0.Count(_cmsketch_00000, 111)
	c := NewCMSketch(int32(len(protoSketch.Rows)), int32(len(protoSketch.Rows[0].Counters)))
	for i, row := range protoSketch.Rows {
		trace_util_0.Count(_cmsketch_00000, 116)
		c.count = 0
		for j, counter := range row.Counters {
			trace_util_0.Count(_cmsketch_00000, 117)
			c.table[i][j] = counter
			c.count = c.count + uint64(counter)
		}
	}
	trace_util_0.Count(_cmsketch_00000, 112)
	if len(protoSketch.TopN) == 0 {
		trace_util_0.Count(_cmsketch_00000, 118)
		return c
	}
	trace_util_0.Count(_cmsketch_00000, 113)
	c.defaultValue = protoSketch.DefaultValue
	c.topN = make(map[uint64][]*TopNMeta)
	for _, e := range protoSketch.TopN {
		trace_util_0.Count(_cmsketch_00000, 119)
		h1, h2 := murmur3.Sum128(e.Data)
		c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, e.Data, e.Count})
	}
	trace_util_0.Count(_cmsketch_00000, 114)
	return c
}

// EncodeCMSketchWithoutTopN encodes the given CMSketch to byte slice.
// Note that it does not include the topN.
func EncodeCMSketchWithoutTopN(c *CMSketch) ([]byte, error) {
	trace_util_0.Count(_cmsketch_00000, 120)
	if c == nil {
		trace_util_0.Count(_cmsketch_00000, 122)
		return nil, nil
	}
	trace_util_0.Count(_cmsketch_00000, 121)
	p := CMSketchToProto(c)
	p.TopN = nil
	protoData, err := p.Marshal()
	return protoData, err
}

// decodeCMSketch decode a CMSketch from the given byte slice.
func decodeCMSketch(data []byte, topN []*TopNMeta) (*CMSketch, error) {
	trace_util_0.Count(_cmsketch_00000, 123)
	if data == nil {
		trace_util_0.Count(_cmsketch_00000, 128)
		return nil, nil
	}
	trace_util_0.Count(_cmsketch_00000, 124)
	p := &tipb.CMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		trace_util_0.Count(_cmsketch_00000, 129)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_cmsketch_00000, 125)
	if len(p.Rows) == 0 && len(topN) == 0 {
		trace_util_0.Count(_cmsketch_00000, 130)
		return nil, nil
	}
	trace_util_0.Count(_cmsketch_00000, 126)
	for _, meta := range topN {
		trace_util_0.Count(_cmsketch_00000, 131)
		p.TopN = append(p.TopN, &tipb.CMSketchTopN{Data: meta.Data, Count: meta.Count})
	}
	trace_util_0.Count(_cmsketch_00000, 127)
	return CMSketchFromProto(p), nil
}

// LoadCMSketchWithTopN loads the CM sketch with topN from storage.
func LoadCMSketchWithTopN(exec sqlexec.RestrictedSQLExecutor, tableID, isIndex, histID int64, cms []byte) (*CMSketch, error) {
	trace_util_0.Count(_cmsketch_00000, 132)
	sql := fmt.Sprintf("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, histID)
	topNRows, _, err := exec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_cmsketch_00000, 135)
		return nil, err
	}
	trace_util_0.Count(_cmsketch_00000, 133)
	topN := make([]*TopNMeta, 0, len(topNRows))
	for _, row := range topNRows {
		trace_util_0.Count(_cmsketch_00000, 136)
		topN = append(topN, &TopNMeta{Data: row.GetBytes(0), Count: row.GetUint64(1)})
	}
	trace_util_0.Count(_cmsketch_00000, 134)
	return decodeCMSketch(cms, topN)
}

// TotalCount returns the count, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
	trace_util_0.Count(_cmsketch_00000, 137)
	return c.count
}

// Equal tests if two CM Sketch equal, it is only used for test.
func (c *CMSketch) Equal(rc *CMSketch) bool {
	trace_util_0.Count(_cmsketch_00000, 138)
	if c == nil || rc == nil {
		trace_util_0.Count(_cmsketch_00000, 144)
		return c == nil && rc == nil
	}
	trace_util_0.Count(_cmsketch_00000, 139)
	if c.width != rc.width || c.depth != rc.depth || c.count != rc.count || c.defaultValue != rc.defaultValue {
		trace_util_0.Count(_cmsketch_00000, 145)
		return false
	}
	trace_util_0.Count(_cmsketch_00000, 140)
	for i := range c.table {
		trace_util_0.Count(_cmsketch_00000, 146)
		for j := range c.table[i] {
			trace_util_0.Count(_cmsketch_00000, 147)
			if c.table[i][j] != rc.table[i][j] {
				trace_util_0.Count(_cmsketch_00000, 148)
				return false
			}
		}
	}
	trace_util_0.Count(_cmsketch_00000, 141)
	if len(c.topN) != len(rc.topN) {
		trace_util_0.Count(_cmsketch_00000, 149)
		return false
	}
	trace_util_0.Count(_cmsketch_00000, 142)
	for h1, topNData := range c.topN {
		trace_util_0.Count(_cmsketch_00000, 150)
		if len(topNData) != len(rc.topN[h1]) {
			trace_util_0.Count(_cmsketch_00000, 152)
			return false
		}
		trace_util_0.Count(_cmsketch_00000, 151)
		for _, val := range topNData {
			trace_util_0.Count(_cmsketch_00000, 153)
			meta := rc.findTopNMeta(h1, val.h2, val.Data)
			if meta == nil || meta.Count != val.Count {
				trace_util_0.Count(_cmsketch_00000, 154)
				return false
			}
		}
	}
	trace_util_0.Count(_cmsketch_00000, 143)
	return true
}

// Copy makes a copy for current CMSketch.
func (c *CMSketch) Copy() *CMSketch {
	trace_util_0.Count(_cmsketch_00000, 155)
	if c == nil {
		trace_util_0.Count(_cmsketch_00000, 159)
		return nil
	}
	trace_util_0.Count(_cmsketch_00000, 156)
	tbl := make([][]uint32, c.depth)
	for i := range tbl {
		trace_util_0.Count(_cmsketch_00000, 160)
		tbl[i] = make([]uint32, c.width)
		copy(tbl[i], c.table[i])
	}
	trace_util_0.Count(_cmsketch_00000, 157)
	var topN map[uint64][]*TopNMeta
	if c.topN != nil {
		trace_util_0.Count(_cmsketch_00000, 161)
		topN = make(map[uint64][]*TopNMeta)
		for h1, vals := range c.topN {
			trace_util_0.Count(_cmsketch_00000, 162)
			newVals := make([]*TopNMeta, 0, len(vals))
			for _, val := range vals {
				trace_util_0.Count(_cmsketch_00000, 164)
				newVal := TopNMeta{h2: val.h2, Count: val.Count, Data: make([]byte, len(val.Data))}
				copy(newVal.Data, val.Data)
				newVals = append(newVals, &newVal)
			}
			trace_util_0.Count(_cmsketch_00000, 163)
			topN[h1] = newVals
		}
	}
	trace_util_0.Count(_cmsketch_00000, 158)
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, defaultValue: c.defaultValue, topN: topN}
}

// TopN gets all the topN meta.
func (c *CMSketch) TopN() []*TopNMeta {
	trace_util_0.Count(_cmsketch_00000, 165)
	if c == nil {
		trace_util_0.Count(_cmsketch_00000, 168)
		return nil
	}
	trace_util_0.Count(_cmsketch_00000, 166)
	topN := make([]*TopNMeta, 0, len(c.topN))
	for _, meta := range c.topN {
		trace_util_0.Count(_cmsketch_00000, 169)
		topN = append(topN, meta...)
	}
	trace_util_0.Count(_cmsketch_00000, 167)
	return topN
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (int32, int32) {
	trace_util_0.Count(_cmsketch_00000, 170)
	return c.width, c.depth
}

var _cmsketch_00000 = "statistics/cmsketch.go"
