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
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// Histogram represents statistics for a column or index.
type Histogram struct {
	ID        int64 // Column ID.
	NDV       int64 // Number of distinct values.
	NullCount int64 // Number of null values.
	// LastUpdateVersion is the version that this histogram updated last time.
	LastUpdateVersion uint64

	Tp *types.FieldType

	// Histogram elements.
	//
	// A bucket bound is the smallest and greatest values stored in the bucket. The lower and upper bound
	// are stored in one column.
	//
	// A bucket count is the number of items stored in all previous buckets and the current bucket.
	// Bucket counts are always in increasing order.
	//
	// A bucket repeat is the number of repeats of the bucket value, it can be used to find popular values.
	Bounds  *chunk.Chunk
	Buckets []Bucket

	// Used for estimating fraction of the interval [lower, upper] that lies within the [lower, value].
	// For some types like `Int`, we do not build it because we can get them directly from `Bounds`.
	scalars []scalar
	// TotColSize is the total column size for the histogram.
	TotColSize int64

	// Correlation is the statistical correlation between physical row ordering and logical ordering of
	// the column values. This ranges from -1 to +1, and it is only valid for Column histogram, not for
	// Index histogram.
	Correlation float64
}

// Bucket store the bucket count and repeat.
type Bucket struct {
	Count  int64
	Repeat int64
}

type scalar struct {
	lower        float64
	upper        float64
	commonPfxLen int // commonPfxLen is the common prefix length of the lower bound and upper bound when the value type is KindString or KindBytes.
}

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int, totColSize int64) *Histogram {
	trace_util_0.Count(_histogram_00000, 0)
	return &Histogram{
		ID:                id,
		NDV:               ndv,
		NullCount:         nullCount,
		LastUpdateVersion: version,
		Tp:                tp,
		Bounds:            chunk.NewChunkWithCapacity([]*types.FieldType{tp}, 2*bucketSize),
		Buckets:           make([]Bucket, 0, bucketSize),
		TotColSize:        totColSize,
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Datum {
	trace_util_0.Count(_histogram_00000, 1)
	d := hg.Bounds.GetRow(2*idx).GetDatum(0, hg.Tp)
	return &d
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Datum {
	trace_util_0.Count(_histogram_00000, 2)
	d := hg.Bounds.GetRow(2*idx+1).GetDatum(0, hg.Tp)
	return &d
}

// AvgColSize is the average column size of the histogram.
func (c *Column) AvgColSize(count int64) float64 {
	trace_util_0.Count(_histogram_00000, 3)
	if count == 0 {
		trace_util_0.Count(_histogram_00000, 5)
		return 0
	}
	trace_util_0.Count(_histogram_00000, 4)
	switch c.Histogram.Tp.Tp {
	case mysql.TypeFloat:
		trace_util_0.Count(_histogram_00000, 6)
		return 4
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeDouble, mysql.TypeYear:
		trace_util_0.Count(_histogram_00000, 7)
		return 8
	case mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_histogram_00000, 8)
		return 16
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_histogram_00000, 9)
		return types.MyDecimalStructSize
	default:
		trace_util_0.Count(_histogram_00000, 10)
		// Keep two decimal place.
		return math.Round(float64(c.TotColSize)/float64(count)*100) / 100
	}
}

// AppendBucket appends a bucket into `hg`.
func (hg *Histogram) AppendBucket(lower *types.Datum, upper *types.Datum, count, repeat int64) {
	trace_util_0.Count(_histogram_00000, 11)
	hg.Buckets = append(hg.Buckets, Bucket{Count: count, Repeat: repeat})
	hg.Bounds.AppendDatum(0, lower)
	hg.Bounds.AppendDatum(0, upper)
}

func (hg *Histogram) updateLastBucket(upper *types.Datum, count, repeat int64) {
	trace_util_0.Count(_histogram_00000, 12)
	len := hg.Len()
	hg.Bounds.TruncateTo(2*len - 1)
	hg.Bounds.AppendDatum(0, upper)
	hg.Buckets[len-1] = Bucket{Count: count, Repeat: repeat}
}

// DecodeTo decodes the histogram bucket values into `Tp`.
func (hg *Histogram) DecodeTo(tp *types.FieldType, timeZone *time.Location) error {
	trace_util_0.Count(_histogram_00000, 13)
	oldIter := chunk.NewIterator4Chunk(hg.Bounds)
	hg.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{tp}, oldIter.Len())
	hg.Tp = tp
	for row := oldIter.Begin(); row != oldIter.End(); row = oldIter.Next() {
		trace_util_0.Count(_histogram_00000, 15)
		datum, err := tablecodec.DecodeColumnValue(row.GetBytes(0), tp, timeZone)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 17)
			return errors.Trace(err)
		}
		trace_util_0.Count(_histogram_00000, 16)
		hg.Bounds.AppendDatum(0, &datum)
	}
	trace_util_0.Count(_histogram_00000, 14)
	return nil
}

// ConvertTo converts the histogram bucket values into `Tp`.
func (hg *Histogram) ConvertTo(sc *stmtctx.StatementContext, tp *types.FieldType) (*Histogram, error) {
	trace_util_0.Count(_histogram_00000, 18)
	hist := NewHistogram(hg.ID, hg.NDV, hg.NullCount, hg.LastUpdateVersion, tp, hg.Len(), hg.TotColSize)
	hist.Correlation = hg.Correlation
	iter := chunk.NewIterator4Chunk(hg.Bounds)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		trace_util_0.Count(_histogram_00000, 20)
		d := row.GetDatum(0, hg.Tp)
		d, err := d.ConvertTo(sc, tp)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 22)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_histogram_00000, 21)
		hist.Bounds.AppendDatum(0, &d)
	}
	trace_util_0.Count(_histogram_00000, 19)
	hist.Buckets = hg.Buckets
	return hist, nil
}

// Len is the number of buckets in the histogram.
func (hg *Histogram) Len() int {
	trace_util_0.Count(_histogram_00000, 23)
	return len(hg.Buckets)
}

// HistogramEqual tests if two histograms are equal.
func HistogramEqual(a, b *Histogram, ignoreID bool) bool {
	trace_util_0.Count(_histogram_00000, 24)
	if ignoreID {
		trace_util_0.Count(_histogram_00000, 26)
		old := b.ID
		b.ID = a.ID
		defer func() { trace_util_0.Count(_histogram_00000, 27); b.ID = old }()
	}
	trace_util_0.Count(_histogram_00000, 25)
	return bytes.Equal([]byte(a.ToString(0)), []byte(b.ToString(0)))
}

// constants for stats version. These const can be used for solving compatibility issue.
const (
	CurStatsVersion = Version1
	Version1        = 1
)

// AnalyzeFlag is set when the statistics comes from analyze and has not been modified by feedback.
const AnalyzeFlag = 1

// IsAnalyzed checks whether this flag contains AnalyzeFlag.
func IsAnalyzed(flag int64) bool {
	trace_util_0.Count(_histogram_00000, 28)
	return (flag & AnalyzeFlag) > 0
}

// ResetAnalyzeFlag resets the AnalyzeFlag because it has been modified by feedback.
func ResetAnalyzeFlag(flag int64) int64 {
	trace_util_0.Count(_histogram_00000, 29)
	return flag &^ AnalyzeFlag
}

// ValueToString converts a possible encoded value to a formatted string. If the value is encoded, then
// idxCols equals to number of origin values, else idxCols is 0.
func ValueToString(value *types.Datum, idxCols int) (string, error) {
	trace_util_0.Count(_histogram_00000, 30)
	if idxCols == 0 {
		trace_util_0.Count(_histogram_00000, 34)
		return value.ToString()
	}
	trace_util_0.Count(_histogram_00000, 31)
	decodedVals, err := codec.DecodeRange(value.GetBytes(), idxCols)
	if err != nil {
		trace_util_0.Count(_histogram_00000, 35)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_histogram_00000, 32)
	str, err := types.DatumsToString(decodedVals, true)
	if err != nil {
		trace_util_0.Count(_histogram_00000, 36)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_histogram_00000, 33)
	return str, nil
}

// BucketToString change the given bucket to string format.
func (hg *Histogram) BucketToString(bktID, idxCols int) string {
	trace_util_0.Count(_histogram_00000, 37)
	upperVal, err := ValueToString(hg.GetUpper(bktID), idxCols)
	terror.Log(errors.Trace(err))
	lowerVal, err := ValueToString(hg.GetLower(bktID), idxCols)
	terror.Log(errors.Trace(err))
	return fmt.Sprintf("num: %d lower_bound: %s upper_bound: %s repeats: %d", hg.bucketCount(bktID), lowerVal, upperVal, hg.Buckets[bktID].Repeat)
}

// ToString gets the string representation for the histogram.
func (hg *Histogram) ToString(idxCols int) string {
	trace_util_0.Count(_histogram_00000, 38)
	strs := make([]string, 0, hg.Len()+1)
	if idxCols > 0 {
		trace_util_0.Count(_histogram_00000, 41)
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		trace_util_0.Count(_histogram_00000, 42)
		{
			strs = append(strs, fmt.Sprintf("column:%d ndv:%d totColSize:%d", hg.ID, hg.NDV, hg.TotColSize))
		}
	}
	trace_util_0.Count(_histogram_00000, 39)
	for i := 0; i < hg.Len(); i++ {
		trace_util_0.Count(_histogram_00000, 43)
		strs = append(strs, hg.BucketToString(i, idxCols))
	}
	trace_util_0.Count(_histogram_00000, 40)
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
func (hg *Histogram) equalRowCount(value types.Datum) float64 {
	trace_util_0.Count(_histogram_00000, 44)
	index, match := hg.Bounds.LowerBound(0, &value)
	// Since we store the lower and upper bound together, if the index is an odd number, then it points to a upper bound.
	if index%2 == 1 {
		trace_util_0.Count(_histogram_00000, 47)
		if match {
			trace_util_0.Count(_histogram_00000, 49)
			return float64(hg.Buckets[index/2].Repeat)
		}
		trace_util_0.Count(_histogram_00000, 48)
		return hg.notNullCount() / float64(hg.NDV)
	}
	trace_util_0.Count(_histogram_00000, 45)
	if match {
		trace_util_0.Count(_histogram_00000, 50)
		cmp := chunk.GetCompareFunc(hg.Tp)
		if cmp(hg.Bounds.GetRow(index), 0, hg.Bounds.GetRow(index+1), 0) == 0 {
			trace_util_0.Count(_histogram_00000, 52)
			return float64(hg.Buckets[index/2].Repeat)
		}
		trace_util_0.Count(_histogram_00000, 51)
		return hg.notNullCount() / float64(hg.NDV)
	}
	trace_util_0.Count(_histogram_00000, 46)
	return 0
}

// greaterRowCount estimates the row count where the column greater than value.
func (hg *Histogram) greaterRowCount(value types.Datum) float64 {
	trace_util_0.Count(_histogram_00000, 53)
	gtCount := hg.notNullCount() - hg.lessRowCount(value) - hg.equalRowCount(value)
	if gtCount < 0 {
		trace_util_0.Count(_histogram_00000, 55)
		gtCount = 0
	}
	trace_util_0.Count(_histogram_00000, 54)
	return gtCount
}

// LessRowCountWithBktIdx estimates the row count where the column less than value.
func (hg *Histogram) LessRowCountWithBktIdx(value types.Datum) (float64, int) {
	trace_util_0.Count(_histogram_00000, 56)
	// All the values are null.
	if hg.Bounds.NumRows() == 0 {
		trace_util_0.Count(_histogram_00000, 61)
		return 0, 0
	}
	trace_util_0.Count(_histogram_00000, 57)
	index, match := hg.Bounds.LowerBound(0, &value)
	if index == hg.Bounds.NumRows() {
		trace_util_0.Count(_histogram_00000, 62)
		return hg.notNullCount(), hg.Len() - 1
	}
	// Since we store the lower and upper bound together, so dividing the index by 2 will get the bucket index.
	trace_util_0.Count(_histogram_00000, 58)
	bucketIdx := index / 2
	curCount, curRepeat := float64(hg.Buckets[bucketIdx].Count), float64(hg.Buckets[bucketIdx].Repeat)
	preCount := float64(0)
	if bucketIdx > 0 {
		trace_util_0.Count(_histogram_00000, 63)
		preCount = float64(hg.Buckets[bucketIdx-1].Count)
	}
	trace_util_0.Count(_histogram_00000, 59)
	if index%2 == 1 {
		trace_util_0.Count(_histogram_00000, 64)
		if match {
			trace_util_0.Count(_histogram_00000, 66)
			return curCount - curRepeat, bucketIdx
		}
		trace_util_0.Count(_histogram_00000, 65)
		return preCount + hg.calcFraction(bucketIdx, &value)*(curCount-curRepeat-preCount), bucketIdx
	}
	trace_util_0.Count(_histogram_00000, 60)
	return preCount, bucketIdx
}

func (hg *Histogram) lessRowCount(value types.Datum) float64 {
	trace_util_0.Count(_histogram_00000, 67)
	result, _ := hg.LessRowCountWithBktIdx(value)
	return result
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (hg *Histogram) BetweenRowCount(a, b types.Datum) float64 {
	trace_util_0.Count(_histogram_00000, 68)
	lessCountA := hg.lessRowCount(a)
	lessCountB := hg.lessRowCount(b)
	// If lessCountA is not less than lessCountB, it may be that they fall to the same bucket and we cannot estimate
	// the fraction, so we use `totalCount / NDV` to estimate the row count, but the result should not greater than
	// lessCountB or notNullCount-lessCountA.
	if lessCountA >= lessCountB && hg.NDV > 0 {
		trace_util_0.Count(_histogram_00000, 70)
		result := math.Min(lessCountB, hg.notNullCount()-lessCountA)
		return math.Min(result, hg.notNullCount()/float64(hg.NDV))
	}
	trace_util_0.Count(_histogram_00000, 69)
	return lessCountB - lessCountA
}

// TotalRowCount returns the total count of this histogram.
func (hg *Histogram) TotalRowCount() float64 {
	trace_util_0.Count(_histogram_00000, 71)
	return hg.notNullCount() + float64(hg.NullCount)
}

// notNullCount indicates the count of non-null values in column histogram and single-column index histogram,
// for multi-column index histogram, since we cannot define null for the row, we treat all rows as non-null, that means,
// notNullCount would return same value as TotalRowCount for multi-column index histograms.
func (hg *Histogram) notNullCount() float64 {
	trace_util_0.Count(_histogram_00000, 72)
	if hg.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 74)
		return 0
	}
	trace_util_0.Count(_histogram_00000, 73)
	return float64(hg.Buckets[hg.Len()-1].Count)
}

// mergeBuckets is used to Merge every two neighbor buckets.
func (hg *Histogram) mergeBuckets(bucketIdx int) {
	trace_util_0.Count(_histogram_00000, 75)
	curBuck := 0
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp}, bucketIdx)
	for i := 0; i+1 <= bucketIdx; i += 2 {
		trace_util_0.Count(_histogram_00000, 78)
		hg.Buckets[curBuck] = hg.Buckets[i+1]
		c.AppendDatum(0, hg.GetLower(i))
		c.AppendDatum(0, hg.GetUpper(i+1))
		curBuck++
	}
	trace_util_0.Count(_histogram_00000, 76)
	if bucketIdx%2 == 0 {
		trace_util_0.Count(_histogram_00000, 79)
		hg.Buckets[curBuck] = hg.Buckets[bucketIdx]
		c.AppendDatum(0, hg.GetLower(bucketIdx))
		c.AppendDatum(0, hg.GetUpper(bucketIdx))
		curBuck++
	}
	trace_util_0.Count(_histogram_00000, 77)
	hg.Bounds = c
	hg.Buckets = hg.Buckets[:curBuck]
	return
}

// GetIncreaseFactor will return a factor of data increasing after the last analysis.
func (hg *Histogram) GetIncreaseFactor(totalCount int64) float64 {
	trace_util_0.Count(_histogram_00000, 80)
	columnCount := hg.TotalRowCount()
	if columnCount == 0 {
		trace_util_0.Count(_histogram_00000, 82)
		// avoid dividing by 0
		return 1.0
	}
	trace_util_0.Count(_histogram_00000, 81)
	return float64(totalCount) / columnCount
}

// validRange checks if the range is Valid, it is used by `SplitRange` to remove the invalid range,
// the possible types of range are index key range and handle key range.
func validRange(sc *stmtctx.StatementContext, ran *ranger.Range, encoded bool) bool {
	trace_util_0.Count(_histogram_00000, 83)
	var low, high []byte
	if encoded {
		trace_util_0.Count(_histogram_00000, 87)
		low, high = ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
	} else {
		trace_util_0.Count(_histogram_00000, 88)
		{
			var err error
			low, err = codec.EncodeKey(sc, nil, ran.LowVal[0])
			if err != nil {
				trace_util_0.Count(_histogram_00000, 90)
				return false
			}
			trace_util_0.Count(_histogram_00000, 89)
			high, err = codec.EncodeKey(sc, nil, ran.HighVal[0])
			if err != nil {
				trace_util_0.Count(_histogram_00000, 91)
				return false
			}
		}
	}
	trace_util_0.Count(_histogram_00000, 84)
	if ran.LowExclude {
		trace_util_0.Count(_histogram_00000, 92)
		low = kv.Key(low).PrefixNext()
	}
	trace_util_0.Count(_histogram_00000, 85)
	if !ran.HighExclude {
		trace_util_0.Count(_histogram_00000, 93)
		high = kv.Key(high).PrefixNext()
	}
	trace_util_0.Count(_histogram_00000, 86)
	return bytes.Compare(low, high) < 0
}

func checkKind(vals []types.Datum, kind byte) bool {
	trace_util_0.Count(_histogram_00000, 94)
	if kind == types.KindString {
		trace_util_0.Count(_histogram_00000, 97)
		kind = types.KindBytes
	}
	trace_util_0.Count(_histogram_00000, 95)
	for _, val := range vals {
		trace_util_0.Count(_histogram_00000, 98)
		valKind := val.Kind()
		if valKind == types.KindNull || valKind == types.KindMinNotNull || valKind == types.KindMaxValue {
			trace_util_0.Count(_histogram_00000, 102)
			continue
		}
		trace_util_0.Count(_histogram_00000, 99)
		if valKind == types.KindString {
			trace_util_0.Count(_histogram_00000, 103)
			valKind = types.KindBytes
		}
		trace_util_0.Count(_histogram_00000, 100)
		if valKind != kind {
			trace_util_0.Count(_histogram_00000, 104)
			return false
		}
		// Only check the first non-null value.
		trace_util_0.Count(_histogram_00000, 101)
		break
	}
	trace_util_0.Count(_histogram_00000, 96)
	return true
}

func (hg *Histogram) typeMatch(ranges []*ranger.Range) bool {
	trace_util_0.Count(_histogram_00000, 105)
	kind := hg.GetLower(0).Kind()
	for _, ran := range ranges {
		trace_util_0.Count(_histogram_00000, 107)
		if !checkKind(ran.LowVal, kind) || !checkKind(ran.HighVal, kind) {
			trace_util_0.Count(_histogram_00000, 108)
			return false
		}
	}
	trace_util_0.Count(_histogram_00000, 106)
	return true
}

// SplitRange splits the range according to the histogram upper bound. Note that we treat last bucket's upper bound
// as inf, so all the split Ranges will totally fall in one of the (-inf, u(0)], (u(0), u(1)],...(u(n-3), u(n-2)],
// (u(n-2), +inf), where n is the number of buckets, u(i) is the i-th bucket's upper bound.
func (hg *Histogram) SplitRange(sc *stmtctx.StatementContext, oldRanges []*ranger.Range, encoded bool) ([]*ranger.Range, bool) {
	trace_util_0.Count(_histogram_00000, 109)
	if !hg.typeMatch(oldRanges) {
		trace_util_0.Count(_histogram_00000, 113)
		return oldRanges, false
	}
	trace_util_0.Count(_histogram_00000, 110)
	ranges := make([]*ranger.Range, 0, len(oldRanges))
	for _, ran := range oldRanges {
		trace_util_0.Count(_histogram_00000, 114)
		ranges = append(ranges, ran.Clone())
	}
	trace_util_0.Count(_histogram_00000, 111)
	split := make([]*ranger.Range, 0, len(ranges))
	for len(ranges) > 0 {
		trace_util_0.Count(_histogram_00000, 115)
		// Find the last bound that greater or equal to the LowVal.
		idx := hg.Bounds.UpperBound(0, &ranges[0].LowVal[0])
		if !ranges[0].LowExclude && idx > 0 {
			trace_util_0.Count(_histogram_00000, 121)
			cmp := chunk.Compare(hg.Bounds.GetRow(idx-1), 0, &ranges[0].LowVal[0])
			if cmp == 0 {
				trace_util_0.Count(_histogram_00000, 122)
				idx--
			}
		}
		// Treat last bucket's upper bound as inf, so we do not need split any more.
		trace_util_0.Count(_histogram_00000, 116)
		if idx >= hg.Bounds.NumRows()-2 {
			trace_util_0.Count(_histogram_00000, 123)
			split = append(split, ranges...)
			break
		}
		// Get the corresponding upper bound.
		trace_util_0.Count(_histogram_00000, 117)
		if idx%2 == 0 {
			trace_util_0.Count(_histogram_00000, 124)
			idx++
		}
		trace_util_0.Count(_histogram_00000, 118)
		upperBound := hg.Bounds.GetRow(idx)
		var i int
		// Find the first range that need to be split by the upper bound.
		for ; i < len(ranges); i++ {
			trace_util_0.Count(_histogram_00000, 125)
			if chunk.Compare(upperBound, 0, &ranges[i].HighVal[0]) < 0 {
				trace_util_0.Count(_histogram_00000, 126)
				break
			}
		}
		trace_util_0.Count(_histogram_00000, 119)
		split = append(split, ranges[:i]...)
		ranges = ranges[i:]
		if len(ranges) == 0 {
			trace_util_0.Count(_histogram_00000, 127)
			break
		}
		// Split according to the upper bound.
		trace_util_0.Count(_histogram_00000, 120)
		cmp := chunk.Compare(upperBound, 0, &ranges[0].LowVal[0])
		if cmp > 0 || (cmp == 0 && !ranges[0].LowExclude) {
			trace_util_0.Count(_histogram_00000, 128)
			upper := upperBound.GetDatum(0, hg.Tp)
			split = append(split, &ranger.Range{
				LowExclude:  ranges[0].LowExclude,
				LowVal:      []types.Datum{ranges[0].LowVal[0]},
				HighVal:     []types.Datum{upper},
				HighExclude: false})
			ranges[0].LowVal[0] = upper
			ranges[0].LowExclude = true
			if !validRange(sc, ranges[0], encoded) {
				trace_util_0.Count(_histogram_00000, 129)
				ranges = ranges[1:]
			}
		}
	}
	trace_util_0.Count(_histogram_00000, 112)
	return split, true
}

func (hg *Histogram) bucketCount(idx int) int64 {
	trace_util_0.Count(_histogram_00000, 130)
	if idx == 0 {
		trace_util_0.Count(_histogram_00000, 132)
		return hg.Buckets[0].Count
	}
	trace_util_0.Count(_histogram_00000, 131)
	return hg.Buckets[idx].Count - hg.Buckets[idx-1].Count
}

// HistogramToProto converts Histogram to its protobuf representation.
// Note that when this is used, the lower/upper bound in the bucket must be BytesDatum.
func HistogramToProto(hg *Histogram) *tipb.Histogram {
	trace_util_0.Count(_histogram_00000, 133)
	protoHg := &tipb.Histogram{
		Ndv: hg.NDV,
	}
	for i := 0; i < hg.Len(); i++ {
		trace_util_0.Count(_histogram_00000, 135)
		bkt := &tipb.Bucket{
			Count:      hg.Buckets[i].Count,
			LowerBound: hg.GetLower(i).GetBytes(),
			UpperBound: hg.GetUpper(i).GetBytes(),
			Repeats:    hg.Buckets[i].Repeat,
		}
		protoHg.Buckets = append(protoHg.Buckets, bkt)
	}
	trace_util_0.Count(_histogram_00000, 134)
	return protoHg
}

// HistogramFromProto converts Histogram from its protobuf representation.
// Note that we will set BytesDatum for the lower/upper bound in the bucket, the decode will
// be after all histograms merged.
func HistogramFromProto(protoHg *tipb.Histogram) *Histogram {
	trace_util_0.Count(_histogram_00000, 136)
	tp := types.NewFieldType(mysql.TypeBlob)
	hg := NewHistogram(0, protoHg.Ndv, 0, 0, tp, len(protoHg.Buckets), 0)
	for _, bucket := range protoHg.Buckets {
		trace_util_0.Count(_histogram_00000, 138)
		lower, upper := types.NewBytesDatum(bucket.LowerBound), types.NewBytesDatum(bucket.UpperBound)
		hg.AppendBucket(&lower, &upper, bucket.Count, bucket.Repeats)
	}
	trace_util_0.Count(_histogram_00000, 137)
	return hg
}

func (hg *Histogram) popFirstBucket() {
	trace_util_0.Count(_histogram_00000, 139)
	hg.Buckets = hg.Buckets[1:]
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp, hg.Tp}, hg.Bounds.NumRows()-2)
	c.Append(hg.Bounds, 2, hg.Bounds.NumRows())
	hg.Bounds = c
}

// IsIndexHist checks whether current histogram is one for index.
func (hg *Histogram) IsIndexHist() bool {
	trace_util_0.Count(_histogram_00000, 140)
	return hg.Tp.Tp == mysql.TypeBlob
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int) (*Histogram, error) {
	trace_util_0.Count(_histogram_00000, 141)
	if lh.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 153)
		return rh, nil
	}
	trace_util_0.Count(_histogram_00000, 142)
	if rh.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 154)
		return lh, nil
	}
	trace_util_0.Count(_histogram_00000, 143)
	lh.NDV += rh.NDV
	lLen := lh.Len()
	cmp, err := lh.GetUpper(lLen-1).CompareDatum(sc, rh.GetLower(0))
	if err != nil {
		trace_util_0.Count(_histogram_00000, 155)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_histogram_00000, 144)
	offset := int64(0)
	if cmp == 0 {
		trace_util_0.Count(_histogram_00000, 156)
		lh.NDV--
		lh.updateLastBucket(rh.GetUpper(0), lh.Buckets[lLen-1].Count+rh.Buckets[0].Count, rh.Buckets[0].Repeat)
		offset = rh.Buckets[0].Count
		rh.popFirstBucket()
	}
	trace_util_0.Count(_histogram_00000, 145)
	for lh.Len() > bucketSize {
		trace_util_0.Count(_histogram_00000, 157)
		lh.mergeBuckets(lh.Len() - 1)
	}
	trace_util_0.Count(_histogram_00000, 146)
	if rh.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 158)
		return lh, nil
	}
	trace_util_0.Count(_histogram_00000, 147)
	for rh.Len() > bucketSize {
		trace_util_0.Count(_histogram_00000, 159)
		rh.mergeBuckets(rh.Len() - 1)
	}
	trace_util_0.Count(_histogram_00000, 148)
	lCount := lh.Buckets[lh.Len()-1].Count
	rCount := rh.Buckets[rh.Len()-1].Count - offset
	lAvg := float64(lCount) / float64(lh.Len())
	rAvg := float64(rCount) / float64(rh.Len())
	for lh.Len() > 1 && lAvg*2 <= rAvg {
		trace_util_0.Count(_histogram_00000, 160)
		lh.mergeBuckets(lh.Len() - 1)
		lAvg *= 2
	}
	trace_util_0.Count(_histogram_00000, 149)
	for rh.Len() > 1 && rAvg*2 <= lAvg {
		trace_util_0.Count(_histogram_00000, 161)
		rh.mergeBuckets(rh.Len() - 1)
		rAvg *= 2
	}
	trace_util_0.Count(_histogram_00000, 150)
	for i := 0; i < rh.Len(); i++ {
		trace_util_0.Count(_histogram_00000, 162)
		lh.AppendBucket(rh.GetLower(i), rh.GetUpper(i), rh.Buckets[i].Count+lCount-offset, rh.Buckets[i].Repeat)
	}
	trace_util_0.Count(_histogram_00000, 151)
	for lh.Len() > bucketSize {
		trace_util_0.Count(_histogram_00000, 163)
		lh.mergeBuckets(lh.Len() - 1)
	}
	trace_util_0.Count(_histogram_00000, 152)
	return lh, nil
}

// AvgCountPerNotNullValue gets the average row count per value by the data of histogram.
func (hg *Histogram) AvgCountPerNotNullValue(totalCount int64) float64 {
	trace_util_0.Count(_histogram_00000, 164)
	factor := hg.GetIncreaseFactor(totalCount)
	totalNotNull := hg.notNullCount() * factor
	curNDV := float64(hg.NDV) * factor
	if curNDV == 0 {
		trace_util_0.Count(_histogram_00000, 166)
		curNDV = 1
	}
	trace_util_0.Count(_histogram_00000, 165)
	return totalNotNull / curNDV
}

func (hg *Histogram) outOfRange(val types.Datum) bool {
	trace_util_0.Count(_histogram_00000, 167)
	if hg.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 169)
		return true
	}
	trace_util_0.Count(_histogram_00000, 168)
	return chunk.Compare(hg.Bounds.GetRow(0), 0, &val) > 0 ||
		chunk.Compare(hg.Bounds.GetRow(hg.Bounds.NumRows()-1), 0, &val) < 0
}

// Copy deep copies the histogram.
func (hg *Histogram) Copy() *Histogram {
	trace_util_0.Count(_histogram_00000, 170)
	newHist := *hg
	newHist.Bounds = hg.Bounds.CopyConstruct()
	newHist.Buckets = make([]Bucket, 0, len(hg.Buckets))
	for _, bkt := range hg.Buckets {
		trace_util_0.Count(_histogram_00000, 172)
		newHist.Buckets = append(newHist.Buckets, bkt)
	}
	trace_util_0.Count(_histogram_00000, 171)
	return &newHist
}

// RemoveUpperBound removes the upper bound from histogram.
// It is used when merge stats for incremental analyze.
func (hg *Histogram) RemoveUpperBound() *Histogram {
	trace_util_0.Count(_histogram_00000, 173)
	hg.Buckets[hg.Len()-1].Count -= hg.Buckets[hg.Len()-1].Repeat
	hg.Buckets[hg.Len()-1].Repeat = 0
	return hg
}

// TruncateHistogram truncates the histogram to `numBkt` buckets.
func (hg *Histogram) TruncateHistogram(numBkt int) *Histogram {
	trace_util_0.Count(_histogram_00000, 174)
	hist := hg.Copy()
	hist.Buckets = hist.Buckets[:numBkt]
	hist.Bounds.TruncateTo(numBkt * 2)
	return hist
}

// ErrorRate is the error rate of estimate row count by bucket and cm sketch.
type ErrorRate struct {
	ErrorTotal float64
	QueryTotal int64
}

// MaxErrorRate is the max error rate of estimate row count of a not pseudo column.
// If the table is pseudo, but the average error rate is less than MaxErrorRate,
// then the column is not pseudo.
const MaxErrorRate = 0.25

// NotAccurate is true when the total of query is zero or the average error
// rate is greater than MaxErrorRate.
func (e *ErrorRate) NotAccurate() bool {
	trace_util_0.Count(_histogram_00000, 175)
	if e.QueryTotal == 0 {
		trace_util_0.Count(_histogram_00000, 177)
		return true
	}
	trace_util_0.Count(_histogram_00000, 176)
	return e.ErrorTotal/float64(e.QueryTotal) > MaxErrorRate
}

// Update updates the ErrorRate.
func (e *ErrorRate) Update(rate float64) {
	trace_util_0.Count(_histogram_00000, 178)
	e.QueryTotal++
	e.ErrorTotal += rate
}

// Merge range merges two ErrorRate.
func (e *ErrorRate) Merge(rate *ErrorRate) {
	trace_util_0.Count(_histogram_00000, 179)
	e.QueryTotal += rate.QueryTotal
	e.ErrorTotal += rate.ErrorTotal
}

// Column represents a column histogram.
type Column struct {
	Histogram
	*CMSketch
	PhysicalID int64
	Count      int64
	Info       *model.ColumnInfo
	IsHandle   bool
	ErrorRate
	Flag           int64
	LastAnalyzePos types.Datum
}

func (c *Column) String() string {
	trace_util_0.Count(_histogram_00000, 180)
	return c.Histogram.ToString(0)
}

// HistogramNeededColumns stores the columns whose Histograms need to be loaded from physical kv layer.
// Currently, we only load index/pk's Histogram from kv automatically. Columns' are loaded by needs.
var HistogramNeededColumns = neededColumnMap{cols: map[tableColumnID]struct{}{}}

// IsInvalid checks if this column is invalid. If this column has histogram but not loaded yet, then we mark it
// as need histogram.
func (c *Column) IsInvalid(sc *stmtctx.StatementContext, collPseudo bool) bool {
	trace_util_0.Count(_histogram_00000, 181)
	if collPseudo && c.NotAccurate() {
		trace_util_0.Count(_histogram_00000, 184)
		return true
	}
	trace_util_0.Count(_histogram_00000, 182)
	if c.NDV > 0 && c.Len() == 0 && sc != nil {
		trace_util_0.Count(_histogram_00000, 185)
		sc.SetHistogramsNotLoad()
		HistogramNeededColumns.insert(tableColumnID{TableID: c.PhysicalID, ColumnID: c.Info.ID})
	}
	trace_util_0.Count(_histogram_00000, 183)
	return c.TotalRowCount() == 0 || (c.NDV > 0 && c.Len() == 0)
}

func (c *Column) equalRowCount(sc *stmtctx.StatementContext, val types.Datum, modifyCount int64) (float64, error) {
	trace_util_0.Count(_histogram_00000, 186)
	if val.IsNull() {
		trace_util_0.Count(_histogram_00000, 191)
		return float64(c.NullCount), nil
	}
	// All the values are null.
	trace_util_0.Count(_histogram_00000, 187)
	if c.Histogram.Bounds.NumRows() == 0 {
		trace_util_0.Count(_histogram_00000, 192)
		return 0.0, nil
	}
	trace_util_0.Count(_histogram_00000, 188)
	if c.NDV > 0 && c.outOfRange(val) {
		trace_util_0.Count(_histogram_00000, 193)
		return float64(modifyCount) / float64(c.NDV), nil
	}
	trace_util_0.Count(_histogram_00000, 189)
	if c.CMSketch != nil {
		trace_util_0.Count(_histogram_00000, 194)
		count, err := c.CMSketch.queryValue(sc, val)
		return float64(count), errors.Trace(err)
	}
	trace_util_0.Count(_histogram_00000, 190)
	return c.Histogram.equalRowCount(val), nil
}

// GetColumnRowCount estimates the row count by a slice of Range.
func (c *Column) GetColumnRowCount(sc *stmtctx.StatementContext, ranges []*ranger.Range, modifyCount int64) (float64, error) {
	trace_util_0.Count(_histogram_00000, 195)
	var rowCount float64
	for _, rg := range ranges {
		trace_util_0.Count(_histogram_00000, 198)
		cmp, err := rg.LowVal[0].CompareDatum(sc, &rg.HighVal[0])
		if err != nil {
			trace_util_0.Count(_histogram_00000, 205)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_histogram_00000, 199)
		if cmp == 0 {
			trace_util_0.Count(_histogram_00000, 206)
			// the point case.
			if !rg.LowExclude && !rg.HighExclude {
				trace_util_0.Count(_histogram_00000, 208)
				var cnt float64
				cnt, err = c.equalRowCount(sc, rg.LowVal[0], modifyCount)
				if err != nil {
					trace_util_0.Count(_histogram_00000, 210)
					return 0, errors.Trace(err)
				}
				trace_util_0.Count(_histogram_00000, 209)
				rowCount += cnt
			}
			trace_util_0.Count(_histogram_00000, 207)
			continue
		}
		// The interval case.
		trace_util_0.Count(_histogram_00000, 200)
		cnt := c.BetweenRowCount(rg.LowVal[0], rg.HighVal[0])
		if (c.outOfRange(rg.LowVal[0]) && !rg.LowVal[0].IsNull()) || c.outOfRange(rg.HighVal[0]) {
			trace_util_0.Count(_histogram_00000, 211)
			cnt += float64(modifyCount) / outOfRangeBetweenRate
		}
		// `betweenRowCount` returns count for [l, h) range, we adjust cnt for boudaries here.
		// Note that, `cnt` does not include null values, we need specially handle cases
		// where null is the lower bound.
		trace_util_0.Count(_histogram_00000, 201)
		if rg.LowExclude && !rg.LowVal[0].IsNull() {
			trace_util_0.Count(_histogram_00000, 212)
			lowCnt, err := c.equalRowCount(sc, rg.LowVal[0], modifyCount)
			if err != nil {
				trace_util_0.Count(_histogram_00000, 214)
				return 0, errors.Trace(err)
			}
			trace_util_0.Count(_histogram_00000, 213)
			cnt -= lowCnt
		}
		trace_util_0.Count(_histogram_00000, 202)
		if !rg.LowExclude && rg.LowVal[0].IsNull() {
			trace_util_0.Count(_histogram_00000, 215)
			cnt += float64(c.NullCount)
		}
		trace_util_0.Count(_histogram_00000, 203)
		if !rg.HighExclude {
			trace_util_0.Count(_histogram_00000, 216)
			highCnt, err := c.equalRowCount(sc, rg.HighVal[0], modifyCount)
			if err != nil {
				trace_util_0.Count(_histogram_00000, 218)
				return 0, errors.Trace(err)
			}
			trace_util_0.Count(_histogram_00000, 217)
			cnt += highCnt
		}
		trace_util_0.Count(_histogram_00000, 204)
		rowCount += cnt
	}
	trace_util_0.Count(_histogram_00000, 196)
	if rowCount > c.TotalRowCount() {
		trace_util_0.Count(_histogram_00000, 219)
		rowCount = c.TotalRowCount()
	} else {
		trace_util_0.Count(_histogram_00000, 220)
		if rowCount < 0 {
			trace_util_0.Count(_histogram_00000, 221)
			rowCount = 0
		}
	}
	trace_util_0.Count(_histogram_00000, 197)
	return rowCount, nil
}

// Index represents an index histogram.
type Index struct {
	Histogram
	*CMSketch
	ErrorRate
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Info           *model.IndexInfo
	Flag           int64
	LastAnalyzePos types.Datum
}

func (idx *Index) String() string {
	trace_util_0.Count(_histogram_00000, 222)
	return idx.Histogram.ToString(len(idx.Info.Columns))
}

// IsInvalid checks if this index is invalid.
func (idx *Index) IsInvalid(collPseudo bool) bool {
	trace_util_0.Count(_histogram_00000, 223)
	return (collPseudo && idx.NotAccurate()) || idx.TotalRowCount() == 0
}

var nullKeyBytes, _ = codec.EncodeKey(nil, nil, types.NewDatum(nil))

func (idx *Index) equalRowCount(sc *stmtctx.StatementContext, b []byte, modifyCount int64) (float64, error) {
	trace_util_0.Count(_histogram_00000, 224)
	if len(idx.Info.Columns) == 1 {
		trace_util_0.Count(_histogram_00000, 228)
		if bytes.Equal(b, nullKeyBytes) {
			trace_util_0.Count(_histogram_00000, 229)
			return float64(idx.NullCount), nil
		}
	}
	trace_util_0.Count(_histogram_00000, 225)
	val := types.NewBytesDatum(b)
	if idx.NDV > 0 && idx.outOfRange(val) {
		trace_util_0.Count(_histogram_00000, 230)
		return float64(modifyCount) / (float64(idx.NDV)), nil
	}
	trace_util_0.Count(_histogram_00000, 226)
	if idx.CMSketch != nil {
		trace_util_0.Count(_histogram_00000, 231)
		return float64(idx.CMSketch.QueryBytes(b)), nil
	}
	trace_util_0.Count(_histogram_00000, 227)
	return idx.Histogram.equalRowCount(val), nil
}

// GetRowCount returns the row count of the given ranges.
// It uses the modifyCount to adjust the influence of modifications on the table.
func (idx *Index) GetRowCount(sc *stmtctx.StatementContext, indexRanges []*ranger.Range, modifyCount int64) (float64, error) {
	trace_util_0.Count(_histogram_00000, 232)
	totalCount := float64(0)
	isSingleCol := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		trace_util_0.Count(_histogram_00000, 235)
		lb, err := codec.EncodeKey(sc, nil, indexRange.LowVal...)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 242)
			return 0, err
		}
		trace_util_0.Count(_histogram_00000, 236)
		rb, err := codec.EncodeKey(sc, nil, indexRange.HighVal...)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 243)
			return 0, err
		}
		trace_util_0.Count(_histogram_00000, 237)
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if bytes.Equal(lb, rb) {
			trace_util_0.Count(_histogram_00000, 244)
			if indexRange.LowExclude || indexRange.HighExclude {
				trace_util_0.Count(_histogram_00000, 246)
				continue
			}
			trace_util_0.Count(_histogram_00000, 245)
			if fullLen {
				trace_util_0.Count(_histogram_00000, 247)
				count, err := idx.equalRowCount(sc, lb, modifyCount)
				if err != nil {
					trace_util_0.Count(_histogram_00000, 249)
					return 0, err
				}
				trace_util_0.Count(_histogram_00000, 248)
				totalCount += count
				continue
			}
		}
		trace_util_0.Count(_histogram_00000, 238)
		if indexRange.LowExclude {
			trace_util_0.Count(_histogram_00000, 250)
			lb = kv.Key(lb).PrefixNext()
		}
		trace_util_0.Count(_histogram_00000, 239)
		if !indexRange.HighExclude {
			trace_util_0.Count(_histogram_00000, 251)
			rb = kv.Key(rb).PrefixNext()
		}
		trace_util_0.Count(_histogram_00000, 240)
		l := types.NewBytesDatum(lb)
		r := types.NewBytesDatum(rb)
		totalCount += idx.BetweenRowCount(l, r)
		lowIsNull := bytes.Equal(lb, nullKeyBytes)
		if (idx.outOfRange(l) && !(isSingleCol && lowIsNull)) || idx.outOfRange(r) {
			trace_util_0.Count(_histogram_00000, 252)
			totalCount += float64(modifyCount) / outOfRangeBetweenRate
		}
		trace_util_0.Count(_histogram_00000, 241)
		if isSingleCol && lowIsNull {
			trace_util_0.Count(_histogram_00000, 253)
			totalCount += float64(idx.NullCount)
		}
	}
	trace_util_0.Count(_histogram_00000, 233)
	if totalCount > idx.TotalRowCount() {
		trace_util_0.Count(_histogram_00000, 254)
		totalCount = idx.TotalRowCount()
	}
	trace_util_0.Count(_histogram_00000, 234)
	return totalCount, nil
}

type countByRangeFunc = func(*stmtctx.StatementContext, int64, []*ranger.Range) (float64, error)

// newHistogramBySelectivity fulfills the content of new histogram by the given selectivity result.
// TODO: Datum is not efficient, try to avoid using it here.
//  Also, there're redundant calculation with Selectivity(). We need to reduce it too.
func newHistogramBySelectivity(sc *stmtctx.StatementContext, histID int64, oldHist, newHist *Histogram, ranges []*ranger.Range, cntByRangeFunc countByRangeFunc) error {
	trace_util_0.Count(_histogram_00000, 255)
	cntPerVal := int64(oldHist.AvgCountPerNotNullValue(int64(oldHist.TotalRowCount())))
	var totCnt int64
	for boundIdx, ranIdx, highRangeIdx := 0, 0, 0; boundIdx < oldHist.Bounds.NumRows() && ranIdx < len(ranges); boundIdx, ranIdx = boundIdx+2, highRangeIdx {
		trace_util_0.Count(_histogram_00000, 257)
		for highRangeIdx < len(ranges) && chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx].HighVal[0]) >= 0 {
			trace_util_0.Count(_histogram_00000, 265)
			highRangeIdx++
		}
		trace_util_0.Count(_histogram_00000, 258)
		if boundIdx+2 >= oldHist.Bounds.NumRows() && highRangeIdx < len(ranges) && ranges[highRangeIdx].HighVal[0].Kind() == types.KindMaxValue {
			trace_util_0.Count(_histogram_00000, 266)
			highRangeIdx++
		}
		trace_util_0.Count(_histogram_00000, 259)
		if ranIdx == highRangeIdx {
			trace_util_0.Count(_histogram_00000, 267)
			continue
		}
		trace_util_0.Count(_histogram_00000, 260)
		cnt, err := cntByRangeFunc(sc, histID, ranges[ranIdx:highRangeIdx])
		// This should not happen.
		if err != nil {
			trace_util_0.Count(_histogram_00000, 268)
			return err
		}
		trace_util_0.Count(_histogram_00000, 261)
		if cnt == 0 {
			trace_util_0.Count(_histogram_00000, 269)
			continue
		}
		trace_util_0.Count(_histogram_00000, 262)
		if int64(cnt) > oldHist.bucketCount(boundIdx/2) {
			trace_util_0.Count(_histogram_00000, 270)
			cnt = float64(oldHist.bucketCount(boundIdx / 2))
		}
		trace_util_0.Count(_histogram_00000, 263)
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx))
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx + 1))
		totCnt += int64(cnt)
		bkt := Bucket{Count: totCnt}
		if chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx-1].HighVal[0]) == 0 && !ranges[highRangeIdx-1].HighExclude {
			trace_util_0.Count(_histogram_00000, 271)
			bkt.Repeat = cntPerVal
		}
		trace_util_0.Count(_histogram_00000, 264)
		newHist.Buckets = append(newHist.Buckets, bkt)
		switch newHist.Tp.EvalType() {
		case types.ETString, types.ETDecimal, types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_histogram_00000, 272)
			newHist.scalars = append(newHist.scalars, oldHist.scalars[boundIdx/2])
		}
	}
	trace_util_0.Count(_histogram_00000, 256)
	return nil
}

func (idx *Index) newIndexBySelectivity(sc *stmtctx.StatementContext, statsNode *StatsNode) (*Index, error) {
	trace_util_0.Count(_histogram_00000, 273)
	var (
		ranLowEncode, ranHighEncode []byte
		err                         error
	)
	newIndexHist := &Index{Info: idx.Info, StatsVer: idx.StatsVer, CMSketch: idx.CMSketch}
	newIndexHist.Histogram = *NewHistogram(idx.ID, int64(float64(idx.NDV)*statsNode.Selectivity), 0, 0, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)

	lowBucketIdx, highBucketIdx := 0, 0
	var totCnt int64

	// Bucket bound of index is encoded one, so we need to decode it if we want to calculate the fraction accurately.
	// TODO: enhance its calculation.
	// Now just remove the bucket that no range fell in.
	for _, ran := range statsNode.Ranges {
		trace_util_0.Count(_histogram_00000, 275)
		lowBucketIdx = highBucketIdx
		ranLowEncode, ranHighEncode, err = ran.Encode(sc, ranLowEncode, ranHighEncode)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 280)
			return nil, err
		}
		trace_util_0.Count(_histogram_00000, 276)
		for ; highBucketIdx < idx.Len(); highBucketIdx++ {
			trace_util_0.Count(_histogram_00000, 281)
			// Encoded value can only go to its next quickly. So ranHighEncode is actually range.HighVal's PrefixNext value.
			// So the Bound should also go to its PrefixNext.
			bucketLowerEncoded := idx.Bounds.GetRow(highBucketIdx * 2).GetBytes(0)
			if bytes.Compare(ranHighEncode, kv.Key(bucketLowerEncoded).PrefixNext()) < 0 {
				trace_util_0.Count(_histogram_00000, 282)
				break
			}
		}
		trace_util_0.Count(_histogram_00000, 277)
		for ; lowBucketIdx < highBucketIdx; lowBucketIdx++ {
			trace_util_0.Count(_histogram_00000, 283)
			bucketUpperEncoded := idx.Bounds.GetRow(lowBucketIdx*2 + 1).GetBytes(0)
			if bytes.Compare(ranLowEncode, bucketUpperEncoded) <= 0 {
				trace_util_0.Count(_histogram_00000, 284)
				break
			}
		}
		trace_util_0.Count(_histogram_00000, 278)
		if lowBucketIdx >= idx.Len() {
			trace_util_0.Count(_histogram_00000, 285)
			break
		}
		trace_util_0.Count(_histogram_00000, 279)
		for i := lowBucketIdx; i < highBucketIdx; i++ {
			trace_util_0.Count(_histogram_00000, 286)
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i * 2))
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i*2 + 1))
			totCnt += idx.bucketCount(i)
			newIndexHist.Buckets = append(newIndexHist.Buckets, Bucket{Repeat: idx.Buckets[i].Repeat, Count: totCnt})
			newIndexHist.scalars = append(newIndexHist.scalars, idx.scalars[i])
		}
	}
	trace_util_0.Count(_histogram_00000, 274)
	return newIndexHist, nil
}

// NewHistCollBySelectivity creates new HistColl by the given statsNodes.
func (coll *HistColl) NewHistCollBySelectivity(sc *stmtctx.StatementContext, statsNodes []*StatsNode) *HistColl {
	trace_util_0.Count(_histogram_00000, 287)
	newColl := &HistColl{
		Columns:       make(map[int64]*Column),
		Indices:       make(map[int64]*Index),
		Idx2ColumnIDs: coll.Idx2ColumnIDs,
		ColID2IdxID:   coll.ColID2IdxID,
		Count:         coll.Count,
	}
	for _, node := range statsNodes {
		trace_util_0.Count(_histogram_00000, 291)
		if node.Tp == IndexType {
			trace_util_0.Count(_histogram_00000, 298)
			idxHist, ok := coll.Indices[node.ID]
			if !ok {
				trace_util_0.Count(_histogram_00000, 301)
				continue
			}
			trace_util_0.Count(_histogram_00000, 299)
			newIdxHist, err := idxHist.newIndexBySelectivity(sc, node)
			if err != nil {
				trace_util_0.Count(_histogram_00000, 302)
				logutil.Logger(context.Background()).Warn("[Histogram-in-plan]: error happened when calculating row count, failed to build histogram for index %v of table %v",
					zap.String("index", idxHist.Info.Name.O), zap.String("table", idxHist.Info.Table.O), zap.Error(err))
				continue
			}
			trace_util_0.Count(_histogram_00000, 300)
			newColl.Indices[node.ID] = newIdxHist
			continue
		}
		trace_util_0.Count(_histogram_00000, 292)
		oldCol, ok := coll.Columns[node.ID]
		if !ok {
			trace_util_0.Count(_histogram_00000, 303)
			continue
		}
		trace_util_0.Count(_histogram_00000, 293)
		newCol := &Column{
			PhysicalID: oldCol.PhysicalID,
			Info:       oldCol.Info,
			IsHandle:   oldCol.IsHandle,
			CMSketch:   oldCol.CMSketch,
		}
		newCol.Histogram = *NewHistogram(oldCol.ID, int64(float64(oldCol.NDV)*node.Selectivity), 0, 0, oldCol.Tp, chunk.InitialCapacity, 0)
		var err error
		splitRanges, ok := oldCol.Histogram.SplitRange(sc, node.Ranges, false)
		if !ok {
			trace_util_0.Count(_histogram_00000, 304)
			logutil.Logger(context.Background()).Warn("[Histogram-in-plan]: the type of histogram and ranges mismatch")
			continue
		}
		// Deal with some corner case.
		trace_util_0.Count(_histogram_00000, 294)
		if len(splitRanges) > 0 {
			trace_util_0.Count(_histogram_00000, 305)
			// Deal with NULL values.
			if splitRanges[0].LowVal[0].IsNull() {
				trace_util_0.Count(_histogram_00000, 306)
				newCol.NullCount = oldCol.NullCount
				if splitRanges[0].HighVal[0].IsNull() {
					trace_util_0.Count(_histogram_00000, 307)
					splitRanges = splitRanges[1:]
				} else {
					trace_util_0.Count(_histogram_00000, 308)
					{
						splitRanges[0].LowVal[0].SetMinNotNull()
					}
				}
			}
		}
		trace_util_0.Count(_histogram_00000, 295)
		if oldCol.IsHandle {
			trace_util_0.Count(_histogram_00000, 309)
			err = newHistogramBySelectivity(sc, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByIntColumnRanges)
		} else {
			trace_util_0.Count(_histogram_00000, 310)
			{
				err = newHistogramBySelectivity(sc, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByColumnRanges)
			}
		}
		trace_util_0.Count(_histogram_00000, 296)
		if err != nil {
			trace_util_0.Count(_histogram_00000, 311)
			logutil.Logger(context.Background()).Warn("[Histogram-in-plan]: error happened when calculating row count", zap.Error(err))
			continue
		}
		trace_util_0.Count(_histogram_00000, 297)
		newColl.Columns[node.ID] = newCol
	}
	trace_util_0.Count(_histogram_00000, 288)
	for id, idx := range coll.Indices {
		trace_util_0.Count(_histogram_00000, 312)
		_, ok := newColl.Indices[id]
		if !ok {
			trace_util_0.Count(_histogram_00000, 313)
			newColl.Indices[id] = idx
		}
	}
	trace_util_0.Count(_histogram_00000, 289)
	for id, col := range coll.Columns {
		trace_util_0.Count(_histogram_00000, 314)
		_, ok := newColl.Columns[id]
		if !ok {
			trace_util_0.Count(_histogram_00000, 315)
			newColl.Columns[id] = col
		}
	}
	trace_util_0.Count(_histogram_00000, 290)
	return newColl
}

func (idx *Index) outOfRange(val types.Datum) bool {
	trace_util_0.Count(_histogram_00000, 316)
	if idx.Histogram.Len() == 0 {
		trace_util_0.Count(_histogram_00000, 318)
		return true
	}
	trace_util_0.Count(_histogram_00000, 317)
	withInLowBoundOrPrefixMatch := chunk.Compare(idx.Bounds.GetRow(0), 0, &val) <= 0 ||
		matchPrefix(idx.Bounds.GetRow(0), 0, &val)
	withInHighBound := chunk.Compare(idx.Bounds.GetRow(idx.Bounds.NumRows()-1), 0, &val) >= 0
	return !withInLowBoundOrPrefixMatch || !withInHighBound
}

// matchPrefix checks whether ad is the prefix of value
func matchPrefix(row chunk.Row, colIdx int, ad *types.Datum) bool {
	trace_util_0.Count(_histogram_00000, 319)
	switch ad.Kind() {
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindMysqlBit:
		trace_util_0.Count(_histogram_00000, 321)
		return strings.HasPrefix(row.GetString(colIdx), ad.GetString())
	}
	trace_util_0.Count(_histogram_00000, 320)
	return false
}

var _histogram_00000 = "statistics/histogram.go"
