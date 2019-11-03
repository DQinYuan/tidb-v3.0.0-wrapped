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

package statistics

import (
	"bytes"
	"context"
	"encoding/gob"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/spaolacci/murmur3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Feedback represents the total scan count in range [lower, upper).
type Feedback struct {
	Lower  *types.Datum
	Upper  *types.Datum
	Count  int64
	Repeat int64
}

// QueryFeedback is used to represent the query feedback info. It contains the query's scan ranges and number of rows
// in each range.
type QueryFeedback struct {
	PhysicalID int64
	Hist       *Histogram
	Tp         int
	Feedback   []Feedback
	Expected   int64 // Expected is the Expected scan count of corresponding query.
	actual     int64 // actual is the actual scan count of corresponding query.
	Valid      bool  // Valid represents the whether this query feedback is still Valid.
	desc       bool  // desc represents the corresponding query is desc scan.
}

// NewQueryFeedback returns a new query feedback.
func NewQueryFeedback(physicalID int64, hist *Histogram, expected int64, desc bool) *QueryFeedback {
	trace_util_0.Count(_feedback_00000, 0)
	if hist != nil && hist.Len() == 0 {
		trace_util_0.Count(_feedback_00000, 3)
		hist = nil
	}
	trace_util_0.Count(_feedback_00000, 1)
	tp := PkType
	if hist != nil && hist.IsIndexHist() {
		trace_util_0.Count(_feedback_00000, 4)
		tp = IndexType
	}
	trace_util_0.Count(_feedback_00000, 2)
	return &QueryFeedback{
		PhysicalID: physicalID,
		Valid:      true,
		Tp:         tp,
		Hist:       hist,
		Expected:   expected,
		desc:       desc,
	}
}

var (
	// MaxNumberOfRanges is the max number of ranges before split to collect feedback.
	MaxNumberOfRanges = 20
	// FeedbackProbability is the probability to collect the feedback.
	FeedbackProbability = atomic.NewFloat64(0)
)

// CalcErrorRate calculates the error rate the current QueryFeedback.
func (q *QueryFeedback) CalcErrorRate() float64 {
	trace_util_0.Count(_feedback_00000, 5)
	expected := float64(q.Expected)
	if q.actual == 0 {
		trace_util_0.Count(_feedback_00000, 7)
		if expected == 0 {
			trace_util_0.Count(_feedback_00000, 9)
			return 0
		}
		trace_util_0.Count(_feedback_00000, 8)
		return 1
	}
	trace_util_0.Count(_feedback_00000, 6)
	return math.Abs(expected-float64(q.actual)) / float64(q.actual)
}

// CollectFeedback decides whether to collect the feedback. It returns false when:
// 1: the histogram is nil or has no buckets;
// 2: the number of scan ranges exceeds the limit because it may affect the performance;
// 3: it does not pass the probabilistic sampler.
func (q *QueryFeedback) CollectFeedback(numOfRanges int) bool {
	trace_util_0.Count(_feedback_00000, 10)
	if q.Hist == nil || q.Hist.Len() == 0 {
		trace_util_0.Count(_feedback_00000, 13)
		return false
	}
	trace_util_0.Count(_feedback_00000, 11)
	if numOfRanges > MaxNumberOfRanges || rand.Float64() > FeedbackProbability.Load() {
		trace_util_0.Count(_feedback_00000, 14)
		return false
	}
	trace_util_0.Count(_feedback_00000, 12)
	return true
}

// DecodeToRanges decode the feedback to ranges.
func (q *QueryFeedback) DecodeToRanges(isIndex bool) ([]*ranger.Range, error) {
	trace_util_0.Count(_feedback_00000, 15)
	ranges := make([]*ranger.Range, 0, len(q.Feedback))
	for _, val := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 17)
		low, high := *val.Lower, *val.Upper
		var lowVal, highVal []types.Datum
		if isIndex {
			trace_util_0.Count(_feedback_00000, 19)
			var err error
			// As we do not know the origin length, just use a custom value here.
			lowVal, err = codec.DecodeRange(low.GetBytes(), 4)
			if err != nil {
				trace_util_0.Count(_feedback_00000, 21)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_feedback_00000, 20)
			highVal, err = codec.DecodeRange(high.GetBytes(), 4)
			if err != nil {
				trace_util_0.Count(_feedback_00000, 22)
				return nil, errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_feedback_00000, 23)
			{
				_, lowInt, err := codec.DecodeInt(val.Lower.GetBytes())
				if err != nil {
					trace_util_0.Count(_feedback_00000, 26)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_feedback_00000, 24)
				_, highInt, err := codec.DecodeInt(val.Upper.GetBytes())
				if err != nil {
					trace_util_0.Count(_feedback_00000, 27)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_feedback_00000, 25)
				lowVal = []types.Datum{types.NewIntDatum(lowInt)}
				highVal = []types.Datum{types.NewIntDatum(highInt)}
			}
		}
		trace_util_0.Count(_feedback_00000, 18)
		ranges = append(ranges, &(ranger.Range{
			LowVal:      lowVal,
			HighVal:     highVal,
			HighExclude: true,
		}))
	}
	trace_util_0.Count(_feedback_00000, 16)
	return ranges, nil
}

// DecodeIntValues is called when the current Feedback stores encoded int values.
func (q *QueryFeedback) DecodeIntValues() *QueryFeedback {
	trace_util_0.Count(_feedback_00000, 28)
	nq := &QueryFeedback{}
	nq.Feedback = make([]Feedback, 0, len(q.Feedback))
	for _, fb := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 30)
		_, lowInt, err := codec.DecodeInt(fb.Lower.GetBytes())
		if err != nil {
			trace_util_0.Count(_feedback_00000, 33)
			logutil.Logger(context.Background()).Debug("decode feedback lower bound value to integer failed", zap.Binary("value", fb.Lower.GetBytes()), zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 31)
		_, highInt, err := codec.DecodeInt(fb.Upper.GetBytes())
		if err != nil {
			trace_util_0.Count(_feedback_00000, 34)
			logutil.Logger(context.Background()).Debug("decode feedback upper bound value to integer failed", zap.Binary("value", fb.Upper.GetBytes()), zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 32)
		low, high := types.NewIntDatum(lowInt), types.NewIntDatum(highInt)
		nq.Feedback = append(nq.Feedback, Feedback{Lower: &low, Upper: &high, Count: fb.Count})
	}
	trace_util_0.Count(_feedback_00000, 29)
	return nq
}

// StoreRanges stores the ranges for update.
func (q *QueryFeedback) StoreRanges(ranges []*ranger.Range) {
	trace_util_0.Count(_feedback_00000, 35)
	q.Feedback = make([]Feedback, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_feedback_00000, 36)
		q.Feedback = append(q.Feedback, Feedback{&ran.LowVal[0], &ran.HighVal[0], 0, 0})
	}
}

// Invalidate is used to invalidate the query feedback.
func (q *QueryFeedback) Invalidate() {
	trace_util_0.Count(_feedback_00000, 37)
	q.Feedback = nil
	q.Hist = nil
	q.Valid = false
	q.actual = -1
}

// Actual gets the actual row count.
func (q *QueryFeedback) Actual() int64 {
	trace_util_0.Count(_feedback_00000, 38)
	if !q.Valid {
		trace_util_0.Count(_feedback_00000, 40)
		return -1
	}
	trace_util_0.Count(_feedback_00000, 39)
	return q.actual
}

// Update updates the query feedback. `startKey` is the start scan key of the partial result, used to find
// the range for update. `counts` is the scan counts of each range, used to update the feedback count info.
func (q *QueryFeedback) Update(startKey kv.Key, counts []int64) {
	trace_util_0.Count(_feedback_00000, 41)
	// Older version do not have the counts info.
	if len(counts) == 0 {
		trace_util_0.Count(_feedback_00000, 50)
		q.Invalidate()
		return
	}
	trace_util_0.Count(_feedback_00000, 42)
	sum := int64(0)
	for _, count := range counts {
		trace_util_0.Count(_feedback_00000, 51)
		sum += count
	}
	trace_util_0.Count(_feedback_00000, 43)
	metrics.DistSQLScanKeysPartialHistogram.Observe(float64(sum))
	q.actual += sum
	if !q.Valid || q.Hist == nil {
		trace_util_0.Count(_feedback_00000, 52)
		return
	}

	trace_util_0.Count(_feedback_00000, 44)
	if q.Tp == IndexType {
		trace_util_0.Count(_feedback_00000, 53)
		startKey = tablecodec.CutIndexPrefix(startKey)
	} else {
		trace_util_0.Count(_feedback_00000, 54)
		{
			startKey = tablecodec.CutRowKeyPrefix(startKey)
		}
	}
	// Find the range that startKey falls in.
	trace_util_0.Count(_feedback_00000, 45)
	idx := sort.Search(len(q.Feedback), func(i int) bool {
		trace_util_0.Count(_feedback_00000, 55)
		return bytes.Compare(q.Feedback[i].Lower.GetBytes(), startKey) > 0
	})
	trace_util_0.Count(_feedback_00000, 46)
	idx--
	if idx < 0 {
		trace_util_0.Count(_feedback_00000, 56)
		return
	}
	// If the desc is true, the counts is reversed, so here we need to reverse it back.
	trace_util_0.Count(_feedback_00000, 47)
	if q.desc {
		trace_util_0.Count(_feedback_00000, 57)
		for i := 0; i < len(counts)/2; i++ {
			trace_util_0.Count(_feedback_00000, 58)
			j := len(counts) - i - 1
			counts[i], counts[j] = counts[j], counts[i]
		}
	}
	// Update the feedback count info.
	trace_util_0.Count(_feedback_00000, 48)
	for i, count := range counts {
		trace_util_0.Count(_feedback_00000, 59)
		if i+idx >= len(q.Feedback) {
			trace_util_0.Count(_feedback_00000, 61)
			q.Invalidate()
			break
		}
		trace_util_0.Count(_feedback_00000, 60)
		q.Feedback[i+idx].Count += count
	}
	trace_util_0.Count(_feedback_00000, 49)
	return
}

// BucketFeedback stands for all the feedback for a bucket.
type BucketFeedback struct {
	feedback []Feedback   // All the feedback info in the same bucket.
	lower    *types.Datum // The lower bound of the new bucket.
	upper    *types.Datum // The upper bound of the new bucket.
}

// outOfRange checks if the `val` is between `min` and `max`.
func outOfRange(sc *stmtctx.StatementContext, min, max, val *types.Datum) (int, error) {
	trace_util_0.Count(_feedback_00000, 62)
	result, err := val.CompareDatum(sc, min)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 67)
		return 0, err
	}
	trace_util_0.Count(_feedback_00000, 63)
	if result < 0 {
		trace_util_0.Count(_feedback_00000, 68)
		return result, nil
	}
	trace_util_0.Count(_feedback_00000, 64)
	result, err = val.CompareDatum(sc, max)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 69)
		return 0, err
	}
	trace_util_0.Count(_feedback_00000, 65)
	if result > 0 {
		trace_util_0.Count(_feedback_00000, 70)
		return result, nil
	}
	trace_util_0.Count(_feedback_00000, 66)
	return 0, nil
}

// adjustFeedbackBoundaries adjust the feedback boundaries according to the `min` and `max`.
// If the feedback has no intersection with `min` and `max`, we could just skip this feedback.
func (f *Feedback) adjustFeedbackBoundaries(sc *stmtctx.StatementContext, min, max *types.Datum) (bool, error) {
	trace_util_0.Count(_feedback_00000, 71)
	result, err := outOfRange(sc, min, max, f.Lower)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 78)
		return false, err
	}
	trace_util_0.Count(_feedback_00000, 72)
	if result > 0 {
		trace_util_0.Count(_feedback_00000, 79)
		return true, nil
	}
	trace_util_0.Count(_feedback_00000, 73)
	if result < 0 {
		trace_util_0.Count(_feedback_00000, 80)
		f.Lower = min
	}
	trace_util_0.Count(_feedback_00000, 74)
	result, err = outOfRange(sc, min, max, f.Upper)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 81)
		return false, err
	}
	trace_util_0.Count(_feedback_00000, 75)
	if result < 0 {
		trace_util_0.Count(_feedback_00000, 82)
		return true, nil
	}
	trace_util_0.Count(_feedback_00000, 76)
	if result > 0 {
		trace_util_0.Count(_feedback_00000, 83)
		f.Upper = max
	}
	trace_util_0.Count(_feedback_00000, 77)
	return false, nil
}

// buildBucketFeedback build the feedback for each bucket from the histogram feedback.
func buildBucketFeedback(h *Histogram, feedback *QueryFeedback) (map[int]*BucketFeedback, int) {
	trace_util_0.Count(_feedback_00000, 84)
	bktID2FB := make(map[int]*BucketFeedback)
	if len(feedback.Feedback) == 0 {
		trace_util_0.Count(_feedback_00000, 87)
		return bktID2FB, 0
	}
	trace_util_0.Count(_feedback_00000, 85)
	total := 0
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	min, max := GetMinValue(h.Tp), GetMaxValue(h.Tp)
	for _, fb := range feedback.Feedback {
		trace_util_0.Count(_feedback_00000, 88)
		skip, err := fb.adjustFeedbackBoundaries(sc, &min, &max)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 96)
			logutil.Logger(context.Background()).Debug("adjust feedback boundaries failed", zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 89)
		if skip {
			trace_util_0.Count(_feedback_00000, 97)
			continue
		}
		trace_util_0.Count(_feedback_00000, 90)
		idx, _ := h.Bounds.LowerBound(0, fb.Lower)
		bktIdx := 0
		// The last bucket also stores the feedback that falls outside the upper bound.
		if idx >= h.Bounds.NumRows()-2 {
			trace_util_0.Count(_feedback_00000, 98)
			bktIdx = h.Len() - 1
		} else {
			trace_util_0.Count(_feedback_00000, 99)
			{
				bktIdx = idx / 2
				// Make sure that this feedback lies within the bucket.
				if chunk.Compare(h.Bounds.GetRow(2*bktIdx+1), 0, fb.Upper) < 0 {
					trace_util_0.Count(_feedback_00000, 100)
					continue
				}
			}
		}
		trace_util_0.Count(_feedback_00000, 91)
		total++
		bkt := bktID2FB[bktIdx]
		if bkt == nil {
			trace_util_0.Count(_feedback_00000, 101)
			bkt = &BucketFeedback{lower: h.GetLower(bktIdx), upper: h.GetUpper(bktIdx)}
			bktID2FB[bktIdx] = bkt
		}
		trace_util_0.Count(_feedback_00000, 92)
		bkt.feedback = append(bkt.feedback, fb)
		// Update the bound if necessary.
		res, err := bkt.lower.CompareDatum(nil, fb.Lower)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 102)
			logutil.Logger(context.Background()).Debug("compare datum failed", zap.Any("value1", bkt.lower), zap.Any("value2", fb.Lower), zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 93)
		if res > 0 {
			trace_util_0.Count(_feedback_00000, 103)
			bkt.lower = fb.Lower
		}
		trace_util_0.Count(_feedback_00000, 94)
		res, err = bkt.upper.CompareDatum(nil, fb.Upper)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 104)
			logutil.Logger(context.Background()).Debug("compare datum failed", zap.Any("value1", bkt.upper), zap.Any("value2", fb.Upper), zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 95)
		if res < 0 {
			trace_util_0.Count(_feedback_00000, 105)
			bkt.upper = fb.Upper
		}
	}
	trace_util_0.Count(_feedback_00000, 86)
	return bktID2FB, total
}

// getBoundaries gets the new boundaries after split.
func (b *BucketFeedback) getBoundaries(num int) []types.Datum {
	trace_util_0.Count(_feedback_00000, 106)
	// Get all the possible new boundaries.
	vals := make([]types.Datum, 0, len(b.feedback)*2+2)
	for _, fb := range b.feedback {
		trace_util_0.Count(_feedback_00000, 111)
		vals = append(vals, *fb.Lower, *fb.Upper)
	}
	trace_util_0.Count(_feedback_00000, 107)
	vals = append(vals, *b.lower)
	err := types.SortDatums(nil, vals)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 112)
		logutil.Logger(context.Background()).Debug("sort datums failed", zap.Error(err))
		vals = vals[:0]
		vals = append(vals, *b.lower, *b.upper)
		return vals
	}
	trace_util_0.Count(_feedback_00000, 108)
	total, interval := 0, len(vals)/num
	// Pick values per `interval`.
	for i := 0; i < len(vals); i, total = i+interval, total+1 {
		trace_util_0.Count(_feedback_00000, 113)
		vals[total] = vals[i]
	}
	// Append the upper bound.
	trace_util_0.Count(_feedback_00000, 109)
	vals[total] = *b.upper
	vals = vals[:total+1]
	total = 1
	// Erase the repeat values.
	for i := 1; i < len(vals); i++ {
		trace_util_0.Count(_feedback_00000, 114)
		cmp, err := vals[total-1].CompareDatum(nil, &vals[i])
		if err != nil {
			trace_util_0.Count(_feedback_00000, 117)
			logutil.Logger(context.Background()).Debug("compare datum failed", zap.Any("value1", vals[total-1]), zap.Any("value2", vals[i]), zap.Error(err))
			continue
		}
		trace_util_0.Count(_feedback_00000, 115)
		if cmp == 0 {
			trace_util_0.Count(_feedback_00000, 118)
			continue
		}
		trace_util_0.Count(_feedback_00000, 116)
		vals[total] = vals[i]
		total++
	}
	trace_util_0.Count(_feedback_00000, 110)
	return vals[:total]
}

// There are only two types of datum in bucket: one is `Blob`, which is for index; the other one
// is `Int`, which is for primary key.
type bucket = Feedback

// splitBucket firstly splits this "BucketFeedback" to "newNumBkts" new buckets,
// calculates the count for each new bucket, merge the new bucket whose count
// is smaller than "minBucketFraction*totalCount" with the next new bucket
// until the last new bucket.
func (b *BucketFeedback) splitBucket(newNumBkts int, totalCount float64, originBucketCount float64) []bucket {
	trace_util_0.Count(_feedback_00000, 119)
	// Split the bucket.
	bounds := b.getBoundaries(newNumBkts + 1)
	bkts := make([]bucket, 0, len(bounds)-1)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for i := 1; i < len(bounds); i++ {
		trace_util_0.Count(_feedback_00000, 121)
		newBkt := bucket{&bounds[i-1], bounds[i].Copy(), 0, 0}
		// get bucket count
		_, ratio := getOverlapFraction(Feedback{b.lower, b.upper, int64(originBucketCount), 0}, newBkt)
		countInNewBkt := originBucketCount * ratio
		countInNewBkt = b.refineBucketCount(sc, newBkt, countInNewBkt)
		// do not split if the count of result bucket is too small.
		if countInNewBkt < minBucketFraction*totalCount {
			trace_util_0.Count(_feedback_00000, 123)
			bounds[i] = bounds[i-1]
			continue
		}
		trace_util_0.Count(_feedback_00000, 122)
		newBkt.Count = int64(countInNewBkt)
		bkts = append(bkts, newBkt)
		// To guarantee that each bucket's range will not overlap.
		setNextValue(&bounds[i])
	}
	trace_util_0.Count(_feedback_00000, 120)
	return bkts
}

// getOverlapFraction gets the overlap fraction of feedback and bucket range. In order to get the bucket count, it also
// returns the ratio between bucket fraction and feedback fraction.
func getOverlapFraction(fb Feedback, bkt bucket) (float64, float64) {
	trace_util_0.Count(_feedback_00000, 124)
	datums := make([]types.Datum, 0, 4)
	datums = append(datums, *fb.Lower, *fb.Upper)
	datums = append(datums, *bkt.Lower, *bkt.Upper)
	err := types.SortDatums(nil, datums)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 128)
		return 0, 0
	}
	trace_util_0.Count(_feedback_00000, 125)
	minValue, maxValue := &datums[0], &datums[3]
	fbLower := calcFraction4Datums(minValue, maxValue, fb.Lower)
	fbUpper := calcFraction4Datums(minValue, maxValue, fb.Upper)
	bktLower := calcFraction4Datums(minValue, maxValue, bkt.Lower)
	bktUpper := calcFraction4Datums(minValue, maxValue, bkt.Upper)
	ratio := (bktUpper - bktLower) / (fbUpper - fbLower)
	// full overlap
	if fbLower <= bktLower && bktUpper <= fbUpper {
		trace_util_0.Count(_feedback_00000, 129)
		return bktUpper - bktLower, ratio
	}
	trace_util_0.Count(_feedback_00000, 126)
	if bktLower <= fbLower && fbUpper <= bktUpper {
		trace_util_0.Count(_feedback_00000, 130)
		return fbUpper - fbLower, ratio
	}
	// partial overlap
	trace_util_0.Count(_feedback_00000, 127)
	overlap := math.Min(bktUpper-fbLower, fbUpper-bktLower)
	return overlap, ratio
}

// mergeFullyContainedFeedback merges the max fraction of non-overlapped feedbacks that are fully contained in the bucket.
func (b *BucketFeedback) mergeFullyContainedFeedback(sc *stmtctx.StatementContext, bkt bucket) (float64, float64, bool) {
	trace_util_0.Count(_feedback_00000, 131)
	var feedbacks []Feedback
	// Get all the fully contained feedbacks.
	for _, fb := range b.feedback {
		trace_util_0.Count(_feedback_00000, 137)
		res, err := outOfRange(sc, bkt.Lower, bkt.Upper, fb.Lower)
		if res != 0 || err != nil {
			trace_util_0.Count(_feedback_00000, 140)
			return 0, 0, false
		}
		trace_util_0.Count(_feedback_00000, 138)
		res, err = outOfRange(sc, bkt.Lower, bkt.Upper, fb.Upper)
		if res != 0 || err != nil {
			trace_util_0.Count(_feedback_00000, 141)
			return 0, 0, false
		}
		trace_util_0.Count(_feedback_00000, 139)
		feedbacks = append(feedbacks, fb)
	}
	trace_util_0.Count(_feedback_00000, 132)
	if len(feedbacks) == 0 {
		trace_util_0.Count(_feedback_00000, 142)
		return 0, 0, false
	}
	// Sort feedbacks by end point and start point incrementally, then pick every feedback that is not overlapped
	// with the previous chosen feedbacks.
	trace_util_0.Count(_feedback_00000, 133)
	var existsErr bool
	sort.Slice(feedbacks, func(i, j int) bool {
		trace_util_0.Count(_feedback_00000, 143)
		res, err := feedbacks[i].Upper.CompareDatum(sc, feedbacks[j].Upper)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 147)
			existsErr = true
		}
		trace_util_0.Count(_feedback_00000, 144)
		if existsErr || res != 0 {
			trace_util_0.Count(_feedback_00000, 148)
			return res < 0
		}
		trace_util_0.Count(_feedback_00000, 145)
		res, err = feedbacks[i].Lower.CompareDatum(sc, feedbacks[j].Lower)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 149)
			existsErr = true
		}
		trace_util_0.Count(_feedback_00000, 146)
		return res < 0
	})
	trace_util_0.Count(_feedback_00000, 134)
	if existsErr {
		trace_util_0.Count(_feedback_00000, 150)
		return 0, 0, false
	}
	trace_util_0.Count(_feedback_00000, 135)
	previousEnd := &types.Datum{}
	var sumFraction, sumCount float64
	for _, fb := range feedbacks {
		trace_util_0.Count(_feedback_00000, 151)
		res, err := previousEnd.CompareDatum(sc, fb.Lower)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 153)
			return 0, 0, false
		}
		trace_util_0.Count(_feedback_00000, 152)
		if res <= 0 {
			trace_util_0.Count(_feedback_00000, 154)
			fraction, _ := getOverlapFraction(fb, bkt)
			sumFraction += fraction
			sumCount += float64(fb.Count)
			previousEnd = fb.Upper
		}
	}
	trace_util_0.Count(_feedback_00000, 136)
	return sumFraction, sumCount, true
}

// refineBucketCount refine the newly split bucket count. It uses the feedback that overlaps most
// with the bucket to get the bucket count.
func (b *BucketFeedback) refineBucketCount(sc *stmtctx.StatementContext, bkt bucket, defaultCount float64) float64 {
	trace_util_0.Count(_feedback_00000, 155)
	bestFraction := minBucketFraction
	count := defaultCount
	sumFraction, sumCount, ok := b.mergeFullyContainedFeedback(sc, bkt)
	if ok && sumFraction > bestFraction {
		trace_util_0.Count(_feedback_00000, 158)
		bestFraction = sumFraction
		count = sumCount / sumFraction
	}
	trace_util_0.Count(_feedback_00000, 156)
	for _, fb := range b.feedback {
		trace_util_0.Count(_feedback_00000, 159)
		fraction, ratio := getOverlapFraction(fb, bkt)
		// choose the max overlap fraction
		if fraction > bestFraction {
			trace_util_0.Count(_feedback_00000, 160)
			bestFraction = fraction
			count = float64(fb.Count) * ratio
		}
	}
	trace_util_0.Count(_feedback_00000, 157)
	return count
}

const (
	defaultSplitCount = 10
	splitPerFeedback  = 10
)

// getSplitCount gets the split count for the histogram. It is based on the intuition that:
// 1: If we have more remaining unused buckets, we can split more.
// 2: We cannot split too aggressive, thus we make it split every `splitPerFeedback`.
func getSplitCount(numFeedbacks, remainBuckets int) int {
	trace_util_0.Count(_feedback_00000, 161)
	// Split more if have more buckets available.
	splitCount := mathutil.Max(remainBuckets, defaultSplitCount)
	return mathutil.Min(splitCount, numFeedbacks/splitPerFeedback)
}

type bucketScore struct {
	id    int
	score float64
}

type bucketScores []bucketScore

func (bs bucketScores) Len() int { trace_util_0.Count(_feedback_00000, 162); return len(bs) }
func (bs bucketScores) Swap(i, j int) {
	trace_util_0.Count(_feedback_00000, 163)
	bs[i], bs[j] = bs[j], bs[i]
}
func (bs bucketScores) Less(i, j int) bool {
	trace_util_0.Count(_feedback_00000, 164)
	return bs[i].score < bs[j].score
}

const (
	// To avoid the histogram been too imbalanced, we constrain the count of a bucket in range
	// [minBucketFraction * totalCount, maxBucketFraction * totalCount].
	minBucketFraction = 1 / 10000.0
	maxBucketFraction = 1 / 10.0
)

// getBucketScore gets the score for merge this bucket with previous one.
// TODO: We also need to consider the bucket hit count.
func getBucketScore(bkts []bucket, totalCount float64, id int) bucketScore {
	trace_util_0.Count(_feedback_00000, 165)
	preCount, count := float64(bkts[id-1].Count), float64(bkts[id].Count)
	// do not merge if the result bucket is too large
	if (preCount + count) > maxBucketFraction*totalCount {
		trace_util_0.Count(_feedback_00000, 168)
		return bucketScore{id, math.MaxFloat64}
	}
	// Merge them if the result bucket is already too small.
	trace_util_0.Count(_feedback_00000, 166)
	if (preCount + count) < minBucketFraction*totalCount {
		trace_util_0.Count(_feedback_00000, 169)
		return bucketScore{id, 0}
	}
	trace_util_0.Count(_feedback_00000, 167)
	low, mid, high := bkts[id-1].Lower, bkts[id-1].Upper, bkts[id].Upper
	// If we choose to merge, err is the absolute estimate error for the previous bucket.
	err := calcFraction4Datums(low, high, mid)*(preCount+count) - preCount
	return bucketScore{id, math.Abs(err / (preCount + count))}
}

// defaultBucketCount is the number of buckets a column histogram has.
var defaultBucketCount = 256

func mergeBuckets(bkts []bucket, isNewBuckets []bool, totalCount float64) []bucket {
	trace_util_0.Count(_feedback_00000, 170)
	mergeCount := len(bkts) - defaultBucketCount
	if mergeCount <= 0 {
		trace_util_0.Count(_feedback_00000, 175)
		return bkts
	}
	trace_util_0.Count(_feedback_00000, 171)
	bs := make(bucketScores, 0, len(bkts))
	for i := 1; i < len(bkts); i++ {
		trace_util_0.Count(_feedback_00000, 176)
		// Do not merge the newly created buckets.
		if !isNewBuckets[i] && !isNewBuckets[i-1] {
			trace_util_0.Count(_feedback_00000, 177)
			bs = append(bs, getBucketScore(bkts, totalCount, i))
		}
	}
	trace_util_0.Count(_feedback_00000, 172)
	sort.Sort(bs)
	ids := make([]int, 0, mergeCount)
	for i := 0; i < mergeCount; i++ {
		trace_util_0.Count(_feedback_00000, 178)
		ids = append(ids, bs[i].id)
	}
	trace_util_0.Count(_feedback_00000, 173)
	sort.Ints(ids)
	idCursor, bktCursor := 0, 0
	for i := range bkts {
		trace_util_0.Count(_feedback_00000, 179)
		// Merge this bucket with last one.
		if idCursor < mergeCount && ids[idCursor] == i {
			trace_util_0.Count(_feedback_00000, 180)
			bkts[bktCursor-1].Upper = bkts[i].Upper
			bkts[bktCursor-1].Count += bkts[i].Count
			bkts[bktCursor-1].Repeat = bkts[i].Repeat
			idCursor++
		} else {
			trace_util_0.Count(_feedback_00000, 181)
			{
				bkts[bktCursor] = bkts[i]
				bktCursor++
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 174)
	bkts = bkts[:bktCursor]
	return bkts
}

// splitBuckets split the histogram buckets according to the feedback.
func splitBuckets(h *Histogram, feedback *QueryFeedback) ([]bucket, []bool, int64) {
	trace_util_0.Count(_feedback_00000, 182)
	bktID2FB, numTotalFBs := buildBucketFeedback(h, feedback)
	buckets := make([]bucket, 0, h.Len())
	isNewBuckets := make([]bool, 0, h.Len())
	splitCount := getSplitCount(numTotalFBs, defaultBucketCount-h.Len())
	for i := 0; i < h.Len(); i++ {
		trace_util_0.Count(_feedback_00000, 185)
		bktFB, ok := bktID2FB[i]
		// No feedback, just use the original one.
		if !ok {
			trace_util_0.Count(_feedback_00000, 187)
			buckets = append(buckets, bucket{h.GetLower(i), h.GetUpper(i), h.bucketCount(i), h.Buckets[i].Repeat})
			isNewBuckets = append(isNewBuckets, false)
			continue
		}
		// Distribute the total split count to bucket based on number of bucket feedback.
		trace_util_0.Count(_feedback_00000, 186)
		newBktNums := splitCount * len(bktFB.feedback) / numTotalFBs
		bkts := bktFB.splitBucket(newBktNums, h.TotalRowCount(), float64(h.bucketCount(i)))
		buckets = append(buckets, bkts...)
		if len(bkts) == 1 {
			trace_util_0.Count(_feedback_00000, 188)
			isNewBuckets = append(isNewBuckets, false)
		} else {
			trace_util_0.Count(_feedback_00000, 189)
			{
				for i := 0; i < len(bkts); i++ {
					trace_util_0.Count(_feedback_00000, 190)
					isNewBuckets = append(isNewBuckets, true)
				}
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 183)
	totCount := int64(0)
	for _, bkt := range buckets {
		trace_util_0.Count(_feedback_00000, 191)
		totCount += bkt.Count
	}
	trace_util_0.Count(_feedback_00000, 184)
	return buckets, isNewBuckets, totCount
}

// UpdateHistogram updates the histogram according buckets.
func UpdateHistogram(h *Histogram, feedback *QueryFeedback) *Histogram {
	trace_util_0.Count(_feedback_00000, 192)
	buckets, isNewBuckets, totalCount := splitBuckets(h, feedback)
	buckets = mergeBuckets(buckets, isNewBuckets, float64(totalCount))
	hist := buildNewHistogram(h, buckets)
	// Update the NDV of primary key column.
	if feedback.Tp == PkType {
		trace_util_0.Count(_feedback_00000, 194)
		hist.NDV = int64(hist.TotalRowCount())
	}
	trace_util_0.Count(_feedback_00000, 193)
	return hist
}

// UpdateCMSketch updates the CMSketch by feedback.
func UpdateCMSketch(c *CMSketch, eqFeedbacks []Feedback) *CMSketch {
	trace_util_0.Count(_feedback_00000, 195)
	if c == nil || len(eqFeedbacks) == 0 {
		trace_util_0.Count(_feedback_00000, 198)
		return c
	}
	trace_util_0.Count(_feedback_00000, 196)
	newCMSketch := c.Copy()
	for _, fb := range eqFeedbacks {
		trace_util_0.Count(_feedback_00000, 199)
		newCMSketch.updateValueBytes(fb.Lower.GetBytes(), uint64(fb.Count))
	}
	trace_util_0.Count(_feedback_00000, 197)
	return newCMSketch
}

func buildNewHistogram(h *Histogram, buckets []bucket) *Histogram {
	trace_util_0.Count(_feedback_00000, 200)
	hist := NewHistogram(h.ID, h.NDV, h.NullCount, h.LastUpdateVersion, h.Tp, len(buckets), h.TotColSize)
	preCount := int64(0)
	for _, bkt := range buckets {
		trace_util_0.Count(_feedback_00000, 202)
		hist.AppendBucket(bkt.Lower, bkt.Upper, bkt.Count+preCount, bkt.Repeat)
		preCount += bkt.Count
	}
	trace_util_0.Count(_feedback_00000, 201)
	return hist
}

// queryFeedback is used to serialize the QueryFeedback.
type queryFeedback struct {
	IntRanges []int64
	// HashValues is the murmur hash values for each index point.
	HashValues  []uint64
	IndexRanges [][]byte
	// Counts is the number of scan keys in each range. It first stores the count for `IntRanges`, `IndexRanges` or `ColumnRanges`.
	// After that, it stores the Ranges for `HashValues`.
	Counts       []int64
	ColumnRanges [][]byte
}

func encodePKFeedback(q *QueryFeedback) (*queryFeedback, error) {
	trace_util_0.Count(_feedback_00000, 203)
	pb := &queryFeedback{}
	for _, fb := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 205)
		// There is no need to update the point queries.
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			trace_util_0.Count(_feedback_00000, 209)
			continue
		}
		trace_util_0.Count(_feedback_00000, 206)
		_, low, err := codec.DecodeInt(fb.Lower.GetBytes())
		if err != nil {
			trace_util_0.Count(_feedback_00000, 210)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_feedback_00000, 207)
		_, high, err := codec.DecodeInt(fb.Upper.GetBytes())
		if err != nil {
			trace_util_0.Count(_feedback_00000, 211)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_feedback_00000, 208)
		pb.IntRanges = append(pb.IntRanges, low, high)
		pb.Counts = append(pb.Counts, fb.Count)
	}
	trace_util_0.Count(_feedback_00000, 204)
	return pb, nil
}

func encodeIndexFeedback(q *QueryFeedback) *queryFeedback {
	trace_util_0.Count(_feedback_00000, 212)
	pb := &queryFeedback{}
	var pointCounts []int64
	for _, fb := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 214)
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			trace_util_0.Count(_feedback_00000, 215)
			h1, h2 := murmur3.Sum128(fb.Lower.GetBytes())
			pb.HashValues = append(pb.HashValues, h1, h2)
			pointCounts = append(pointCounts, fb.Count)
		} else {
			trace_util_0.Count(_feedback_00000, 216)
			{
				pb.IndexRanges = append(pb.IndexRanges, fb.Lower.GetBytes(), fb.Upper.GetBytes())
				pb.Counts = append(pb.Counts, fb.Count)
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 213)
	pb.Counts = append(pb.Counts, pointCounts...)
	return pb
}

func encodeColumnFeedback(q *QueryFeedback) (*queryFeedback, error) {
	trace_util_0.Count(_feedback_00000, 217)
	pb := &queryFeedback{}
	sc := stmtctx.StatementContext{TimeZone: time.UTC}
	for _, fb := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 219)
		lowerBytes, err := codec.EncodeKey(&sc, nil, *fb.Lower)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 222)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_feedback_00000, 220)
		upperBytes, err := codec.EncodeKey(&sc, nil, *fb.Upper)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 223)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_feedback_00000, 221)
		pb.ColumnRanges = append(pb.ColumnRanges, lowerBytes, upperBytes)
		pb.Counts = append(pb.Counts, fb.Count)
	}
	trace_util_0.Count(_feedback_00000, 218)
	return pb, nil
}

// EncodeFeedback encodes the given feedback to byte slice.
func EncodeFeedback(q *QueryFeedback) ([]byte, error) {
	trace_util_0.Count(_feedback_00000, 224)
	var pb *queryFeedback
	var err error
	switch q.Tp {
	case PkType:
		trace_util_0.Count(_feedback_00000, 228)
		pb, err = encodePKFeedback(q)
	case IndexType:
		trace_util_0.Count(_feedback_00000, 229)
		pb = encodeIndexFeedback(q)
	case ColType:
		trace_util_0.Count(_feedback_00000, 230)
		pb, err = encodeColumnFeedback(q)
	}
	trace_util_0.Count(_feedback_00000, 225)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 231)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_feedback_00000, 226)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(pb)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 232)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_feedback_00000, 227)
	return buf.Bytes(), nil
}

func decodeFeedbackForIndex(q *QueryFeedback, pb *queryFeedback, c *CMSketch) {
	trace_util_0.Count(_feedback_00000, 233)
	q.Tp = IndexType
	// decode the index range feedback
	for i := 0; i < len(pb.IndexRanges); i += 2 {
		trace_util_0.Count(_feedback_00000, 235)
		lower, upper := types.NewBytesDatum(pb.IndexRanges[i]), types.NewBytesDatum(pb.IndexRanges[i+1])
		q.Feedback = append(q.Feedback, Feedback{&lower, &upper, pb.Counts[i/2], 0})
	}
	trace_util_0.Count(_feedback_00000, 234)
	if c != nil {
		trace_util_0.Count(_feedback_00000, 236)
		// decode the index point feedback, just set value count in CM Sketch
		start := len(pb.IndexRanges) / 2
		for i := 0; i < len(pb.HashValues); i += 2 {
			trace_util_0.Count(_feedback_00000, 237)
			// TODO: update using raw bytes instead of hash values.
			c.setValue(pb.HashValues[i], pb.HashValues[i+1], uint64(pb.Counts[start+i/2]))
		}
	}
}

func decodeFeedbackForPK(q *QueryFeedback, pb *queryFeedback, isUnsigned bool) {
	trace_util_0.Count(_feedback_00000, 238)
	q.Tp = PkType
	// decode feedback for primary key
	for i := 0; i < len(pb.IntRanges); i += 2 {
		trace_util_0.Count(_feedback_00000, 239)
		var lower, upper types.Datum
		if isUnsigned {
			trace_util_0.Count(_feedback_00000, 241)
			lower.SetUint64(uint64(pb.IntRanges[i]))
			upper.SetUint64(uint64(pb.IntRanges[i+1]))
		} else {
			trace_util_0.Count(_feedback_00000, 242)
			{
				lower.SetInt64(pb.IntRanges[i])
				upper.SetInt64(pb.IntRanges[i+1])
			}
		}
		trace_util_0.Count(_feedback_00000, 240)
		q.Feedback = append(q.Feedback, Feedback{&lower, &upper, pb.Counts[i/2], 0})
	}
}

// ConvertDatumsType converts the datums type to `ft`.
func ConvertDatumsType(vals []types.Datum, ft *types.FieldType, loc *time.Location) error {
	trace_util_0.Count(_feedback_00000, 243)
	for i, val := range vals {
		trace_util_0.Count(_feedback_00000, 245)
		if val.Kind() == types.KindMinNotNull || val.Kind() == types.KindMaxValue {
			trace_util_0.Count(_feedback_00000, 248)
			continue
		}
		trace_util_0.Count(_feedback_00000, 246)
		newVal, err := tablecodec.UnflattenDatums([]types.Datum{val}, []*types.FieldType{ft}, loc)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 249)
			return err
		}
		trace_util_0.Count(_feedback_00000, 247)
		vals[i] = newVal[0]
	}
	trace_util_0.Count(_feedback_00000, 244)
	return nil
}

func decodeColumnBounds(data []byte, ft *types.FieldType) ([]types.Datum, error) {
	trace_util_0.Count(_feedback_00000, 250)
	vals, err := codec.DecodeRange(data, 1)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 252)
		return nil, err
	}
	trace_util_0.Count(_feedback_00000, 251)
	err = ConvertDatumsType(vals, ft, time.UTC)
	return vals, err
}

func decodeFeedbackForColumn(q *QueryFeedback, pb *queryFeedback, ft *types.FieldType) error {
	trace_util_0.Count(_feedback_00000, 253)
	q.Tp = ColType
	for i := 0; i < len(pb.ColumnRanges); i += 2 {
		trace_util_0.Count(_feedback_00000, 255)
		low, err := decodeColumnBounds(pb.ColumnRanges[i], ft)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 258)
			return err
		}
		trace_util_0.Count(_feedback_00000, 256)
		high, err := decodeColumnBounds(pb.ColumnRanges[i+1], ft)
		if err != nil {
			trace_util_0.Count(_feedback_00000, 259)
			return err
		}
		trace_util_0.Count(_feedback_00000, 257)
		q.Feedback = append(q.Feedback, Feedback{&low[0], &high[0], pb.Counts[i/2], 0})
	}
	trace_util_0.Count(_feedback_00000, 254)
	return nil
}

// DecodeFeedback decodes a byte slice to feedback.
func DecodeFeedback(val []byte, q *QueryFeedback, c *CMSketch, ft *types.FieldType) error {
	trace_util_0.Count(_feedback_00000, 260)
	buf := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buf)
	pb := &queryFeedback{}
	err := dec.Decode(pb)
	if err != nil {
		trace_util_0.Count(_feedback_00000, 263)
		return errors.Trace(err)
	}
	trace_util_0.Count(_feedback_00000, 261)
	if len(pb.IndexRanges) > 0 || len(pb.HashValues) > 0 {
		trace_util_0.Count(_feedback_00000, 264)
		decodeFeedbackForIndex(q, pb, c)
	} else {
		trace_util_0.Count(_feedback_00000, 265)
		if len(pb.IntRanges) > 0 {
			trace_util_0.Count(_feedback_00000, 266)
			decodeFeedbackForPK(q, pb, mysql.HasUnsignedFlag(ft.Flag))
		} else {
			trace_util_0.Count(_feedback_00000, 267)
			{
				err := decodeFeedbackForColumn(q, pb, ft)
				if err != nil {
					trace_util_0.Count(_feedback_00000, 268)
					return errors.Trace(err)
				}
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 262)
	return nil
}

// Equal tests if two query feedback equal, it is only used in test.
func (q *QueryFeedback) Equal(rq *QueryFeedback) bool {
	trace_util_0.Count(_feedback_00000, 269)
	if len(q.Feedback) != len(rq.Feedback) {
		trace_util_0.Count(_feedback_00000, 272)
		return false
	}
	trace_util_0.Count(_feedback_00000, 270)
	for i, fb := range q.Feedback {
		trace_util_0.Count(_feedback_00000, 273)
		rfb := rq.Feedback[i]
		if fb.Count != rfb.Count {
			trace_util_0.Count(_feedback_00000, 275)
			return false
		}
		trace_util_0.Count(_feedback_00000, 274)
		if fb.Lower.Kind() == types.KindInt64 {
			trace_util_0.Count(_feedback_00000, 276)
			if fb.Lower.GetInt64() != rfb.Lower.GetInt64() {
				trace_util_0.Count(_feedback_00000, 278)
				return false
			}
			trace_util_0.Count(_feedback_00000, 277)
			if fb.Upper.GetInt64() != rfb.Upper.GetInt64() {
				trace_util_0.Count(_feedback_00000, 279)
				return false
			}
		} else {
			trace_util_0.Count(_feedback_00000, 280)
			{
				if !bytes.Equal(fb.Lower.GetBytes(), rfb.Lower.GetBytes()) {
					trace_util_0.Count(_feedback_00000, 282)
					return false
				}
				trace_util_0.Count(_feedback_00000, 281)
				if !bytes.Equal(fb.Upper.GetBytes(), rfb.Upper.GetBytes()) {
					trace_util_0.Count(_feedback_00000, 283)
					return false
				}
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 271)
	return true
}

// SplitFeedbackByQueryType splits the feedbacks into equality feedbacks and range feedbacks.
func SplitFeedbackByQueryType(feedbacks []Feedback) ([]Feedback, []Feedback) {
	trace_util_0.Count(_feedback_00000, 284)
	var eqFB, ranFB []Feedback
	for _, fb := range feedbacks {
		trace_util_0.Count(_feedback_00000, 286)
		// Use `>=` here because sometimes the lower is equal to upper.
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			trace_util_0.Count(_feedback_00000, 287)
			eqFB = append(eqFB, fb)
		} else {
			trace_util_0.Count(_feedback_00000, 288)
			{
				ranFB = append(ranFB, fb)
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 285)
	return eqFB, ranFB
}

// setNextValue sets the next value for the given datum. For types like float,
// we do not set because it is not discrete and does not matter too much when estimating the scalar info.
func setNextValue(d *types.Datum) {
	trace_util_0.Count(_feedback_00000, 289)
	switch d.Kind() {
	case types.KindBytes, types.KindString:
		trace_util_0.Count(_feedback_00000, 290)
		d.SetBytes(kv.Key(d.GetBytes()).PrefixNext())
	case types.KindInt64:
		trace_util_0.Count(_feedback_00000, 291)
		d.SetInt64(d.GetInt64() + 1)
	case types.KindUint64:
		trace_util_0.Count(_feedback_00000, 292)
		d.SetUint64(d.GetUint64() + 1)
	case types.KindMysqlDuration:
		trace_util_0.Count(_feedback_00000, 293)
		duration := d.GetMysqlDuration()
		duration.Duration = duration.Duration + 1
		d.SetMysqlDuration(duration)
	case types.KindMysqlTime:
		trace_util_0.Count(_feedback_00000, 294)
		t := d.GetMysqlTime()
		sc := &stmtctx.StatementContext{TimeZone: types.BoundTimezone}
		if _, err := t.Add(sc, types.Duration{Duration: 1, Fsp: 0}); err != nil {
			trace_util_0.Count(_feedback_00000, 296)
			log.Error(errors.ErrorStack(err))
		}
		trace_util_0.Count(_feedback_00000, 295)
		d.SetMysqlTime(t)
	}
}

// SupportColumnType checks if the type of the column can be updated by feedback.
func SupportColumnType(ft *types.FieldType) bool {
	trace_util_0.Count(_feedback_00000, 297)
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat,
		mysql.TypeDouble, mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeNewDecimal, mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_feedback_00000, 298)
		return true
	default:
		trace_util_0.Count(_feedback_00000, 299)
		return false
	}
}

// GetMaxValue returns the max value datum for each type.
func GetMaxValue(ft *types.FieldType) (max types.Datum) {
	trace_util_0.Count(_feedback_00000, 300)
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_feedback_00000, 302)
		if mysql.HasUnsignedFlag(ft.Flag) {
			trace_util_0.Count(_feedback_00000, 310)
			max.SetUint64(types.IntergerUnsignedUpperBound(ft.Tp))
		} else {
			trace_util_0.Count(_feedback_00000, 311)
			{
				max.SetInt64(types.IntergerSignedUpperBound(ft.Tp))
			}
		}
	case mysql.TypeFloat:
		trace_util_0.Count(_feedback_00000, 303)
		max.SetFloat32(float32(types.GetMaxFloat(ft.Flen, ft.Decimal)))
	case mysql.TypeDouble:
		trace_util_0.Count(_feedback_00000, 304)
		max.SetFloat64(types.GetMaxFloat(ft.Flen, ft.Decimal))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_feedback_00000, 305)
		val := types.MaxValueDatum()
		bytes, err := codec.EncodeKey(nil, nil, val)
		// should not happen
		if err != nil {
			trace_util_0.Count(_feedback_00000, 312)
			logutil.Logger(context.Background()).Error("encode key fail", zap.Error(err))
		}
		trace_util_0.Count(_feedback_00000, 306)
		max.SetBytes(bytes)
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_feedback_00000, 307)
		max.SetMysqlDecimal(types.NewMaxOrMinDec(false, ft.Flen, ft.Decimal))
	case mysql.TypeDuration:
		trace_util_0.Count(_feedback_00000, 308)
		max.SetMysqlDuration(types.Duration{Duration: types.MaxTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_feedback_00000, 309)
		if ft.Tp == mysql.TypeDate || ft.Tp == mysql.TypeDatetime {
			trace_util_0.Count(_feedback_00000, 313)
			max.SetMysqlTime(types.Time{Time: types.MaxDatetime, Type: ft.Tp})
		} else {
			trace_util_0.Count(_feedback_00000, 314)
			{
				max.SetMysqlTime(types.MaxTimestamp)
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 301)
	return
}

// GetMinValue returns the min value datum for each type.
func GetMinValue(ft *types.FieldType) (min types.Datum) {
	trace_util_0.Count(_feedback_00000, 315)
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_feedback_00000, 317)
		if mysql.HasUnsignedFlag(ft.Flag) {
			trace_util_0.Count(_feedback_00000, 325)
			min.SetUint64(0)
		} else {
			trace_util_0.Count(_feedback_00000, 326)
			{
				min.SetInt64(types.IntergerSignedLowerBound(ft.Tp))
			}
		}
	case mysql.TypeFloat:
		trace_util_0.Count(_feedback_00000, 318)
		min.SetFloat32(float32(-types.GetMaxFloat(ft.Flen, ft.Decimal)))
	case mysql.TypeDouble:
		trace_util_0.Count(_feedback_00000, 319)
		min.SetFloat64(-types.GetMaxFloat(ft.Flen, ft.Decimal))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_feedback_00000, 320)
		val := types.MinNotNullDatum()
		bytes, err := codec.EncodeKey(nil, nil, val)
		// should not happen
		if err != nil {
			trace_util_0.Count(_feedback_00000, 327)
			logutil.Logger(context.Background()).Error("encode key fail", zap.Error(err))
		}
		trace_util_0.Count(_feedback_00000, 321)
		min.SetBytes(bytes)
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_feedback_00000, 322)
		min.SetMysqlDecimal(types.NewMaxOrMinDec(true, ft.Flen, ft.Decimal))
	case mysql.TypeDuration:
		trace_util_0.Count(_feedback_00000, 323)
		min.SetMysqlDuration(types.Duration{Duration: types.MinTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_feedback_00000, 324)
		if ft.Tp == mysql.TypeDate || ft.Tp == mysql.TypeDatetime {
			trace_util_0.Count(_feedback_00000, 328)
			min.SetMysqlTime(types.Time{Time: types.MinDatetime, Type: ft.Tp})
		} else {
			trace_util_0.Count(_feedback_00000, 329)
			{
				min.SetMysqlTime(types.MinTimestamp)
			}
		}
	}
	trace_util_0.Count(_feedback_00000, 316)
	return
}

var _feedback_00000 = "statistics/feedback.go"
