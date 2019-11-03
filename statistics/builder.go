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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// SortedBuilder is used to build histograms for PK and index.
type SortedBuilder struct {
	sc              *stmtctx.StatementContext
	numBuckets      int64
	valuesPerBucket int64
	lastNumber      int64
	bucketIdx       int64
	Count           int64
	hist            *Histogram
}

// NewSortedBuilder creates a new SortedBuilder.
func NewSortedBuilder(sc *stmtctx.StatementContext, numBuckets, id int64, tp *types.FieldType) *SortedBuilder {
	trace_util_0.Count(_builder_00000, 0)
	return &SortedBuilder{
		sc:              sc,
		numBuckets:      numBuckets,
		valuesPerBucket: 1,
		hist:            NewHistogram(id, 0, 0, 0, tp, int(numBuckets), 0),
	}
}

// Hist returns the histogram built by SortedBuilder.
func (b *SortedBuilder) Hist() *Histogram {
	trace_util_0.Count(_builder_00000, 1)
	return b.hist
}

// Iterate updates the histogram incrementally.
func (b *SortedBuilder) Iterate(data types.Datum) error {
	trace_util_0.Count(_builder_00000, 2)
	b.Count++
	if b.Count == 1 {
		trace_util_0.Count(_builder_00000, 6)
		b.hist.AppendBucket(&data, &data, 1, 1)
		b.hist.NDV = 1
		return nil
	}
	trace_util_0.Count(_builder_00000, 3)
	cmp, err := b.hist.GetUpper(int(b.bucketIdx)).CompareDatum(b.sc, &data)
	if err != nil {
		trace_util_0.Count(_builder_00000, 7)
		return errors.Trace(err)
	}
	trace_util_0.Count(_builder_00000, 4)
	if cmp == 0 {
		trace_util_0.Count(_builder_00000, 8)
		// The new item has the same value as current bucket value, to ensure that
		// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
		// valuesPerBucket.
		b.hist.Buckets[b.bucketIdx].Count++
		b.hist.Buckets[b.bucketIdx].Repeat++
	} else {
		trace_util_0.Count(_builder_00000, 9)
		if b.hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
			trace_util_0.Count(_builder_00000, 10)
			// The bucket still have room to store a new item, update the bucket.
			b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1)
			b.hist.NDV++
		} else {
			trace_util_0.Count(_builder_00000, 11)
			{
				// All buckets are full, we should merge buckets.
				if b.bucketIdx+1 == b.numBuckets {
					trace_util_0.Count(_builder_00000, 14)
					b.hist.mergeBuckets(int(b.bucketIdx))
					b.valuesPerBucket *= 2
					b.bucketIdx = b.bucketIdx / 2
					if b.bucketIdx == 0 {
						trace_util_0.Count(_builder_00000, 15)
						b.lastNumber = 0
					} else {
						trace_util_0.Count(_builder_00000, 16)
						{
							b.lastNumber = b.hist.Buckets[b.bucketIdx-1].Count
						}
					}
				}
				// We may merge buckets, so we should check it again.
				trace_util_0.Count(_builder_00000, 12)
				if b.hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
					trace_util_0.Count(_builder_00000, 17)
					b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1)
				} else {
					trace_util_0.Count(_builder_00000, 18)
					{
						b.lastNumber = b.hist.Buckets[b.bucketIdx].Count
						b.bucketIdx++
						b.hist.AppendBucket(&data, &data, b.lastNumber+1, 1)
					}
				}
				trace_util_0.Count(_builder_00000, 13)
				b.hist.NDV++
			}
		}
	}
	trace_util_0.Count(_builder_00000, 5)
	return nil
}

// BuildColumnHist build a histogram for a column.
// numBuckets: number of buckets for the histogram.
// id: the id of the table.
// collector: the collector of samples.
// tp: the FieldType for the column.
// count: represents the row count for the column.
// ndv: represents the number of distinct values for the column.
// nullCount: represents the number of null values for the column.
func BuildColumnHist(ctx sessionctx.Context, numBuckets, id int64, collector *SampleCollector, tp *types.FieldType, count int64, ndv int64, nullCount int64) (*Histogram, error) {
	trace_util_0.Count(_builder_00000, 19)
	if ndv > count {
		trace_util_0.Count(_builder_00000, 26)
		ndv = count
	}
	trace_util_0.Count(_builder_00000, 20)
	if count == 0 || len(collector.Samples) == 0 {
		trace_util_0.Count(_builder_00000, 27)
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil
	}
	trace_util_0.Count(_builder_00000, 21)
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	err := SortSampleItems(sc, samples)
	if err != nil {
		trace_util_0.Count(_builder_00000, 28)
		return nil, err
	}
	trace_util_0.Count(_builder_00000, 22)
	hg := NewHistogram(id, ndv, nullCount, 0, tp, int(numBuckets), collector.TotalSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))
	// Since bucket count is increased by sampleFactor, so the actual max values per bucket is
	// floor(valuesPerBucket/sampleFactor)*sampleFactor, which may less than valuesPerBucket,
	// thus we need to add a sampleFactor to avoid building too many buckets.
	valuesPerBucket := float64(count)/float64(numBuckets) + sampleFactor
	ndvFactor := float64(count) / float64(hg.NDV)
	if ndvFactor > sampleFactor {
		trace_util_0.Count(_builder_00000, 29)
		ndvFactor = sampleFactor
	}
	trace_util_0.Count(_builder_00000, 23)
	bucketIdx := 0
	var lastCount int64
	var corrXYSum float64
	hg.AppendBucket(&samples[0].Value, &samples[0].Value, int64(sampleFactor), int64(ndvFactor))
	for i := int64(1); i < sampleNum; i++ {
		trace_util_0.Count(_builder_00000, 30)
		corrXYSum += float64(i) * float64(samples[i].Ordinal)
		cmp, err := hg.GetUpper(bucketIdx).CompareDatum(sc, &samples[i].Value)
		if err != nil {
			trace_util_0.Count(_builder_00000, 32)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_builder_00000, 31)
		totalCount := float64(i+1) * sampleFactor
		if cmp == 0 {
			trace_util_0.Count(_builder_00000, 33)
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			hg.Buckets[bucketIdx].Count = int64(totalCount)
			if float64(hg.Buckets[bucketIdx].Repeat) == ndvFactor {
				trace_util_0.Count(_builder_00000, 34)
				hg.Buckets[bucketIdx].Repeat = int64(2 * sampleFactor)
			} else {
				trace_util_0.Count(_builder_00000, 35)
				{
					hg.Buckets[bucketIdx].Repeat += int64(sampleFactor)
				}
			}
		} else {
			trace_util_0.Count(_builder_00000, 36)
			if totalCount-float64(lastCount) <= valuesPerBucket {
				trace_util_0.Count(_builder_00000, 37)
				// The bucket still have room to store a new item, update the bucket.
				hg.updateLastBucket(&samples[i].Value, int64(totalCount), int64(ndvFactor))
			} else {
				trace_util_0.Count(_builder_00000, 38)
				{
					lastCount = hg.Buckets[bucketIdx].Count
					// The bucket is full, store the item in the next bucket.
					bucketIdx++
					hg.AppendBucket(&samples[i].Value, &samples[i].Value, int64(totalCount), int64(ndvFactor))
				}
			}
		}
	}
	// Compute column order correlation with handle.
	trace_util_0.Count(_builder_00000, 24)
	if sampleNum == 1 {
		trace_util_0.Count(_builder_00000, 39)
		hg.Correlation = 1
		return hg, nil
	}
	// X means the ordinal of the item in original sequence, Y means the oridnal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	trace_util_0.Count(_builder_00000, 25)
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	hg.Correlation = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return hg, nil
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx sessionctx.Context, numBuckets, id int64, collector *SampleCollector, tp *types.FieldType) (*Histogram, error) {
	trace_util_0.Count(_builder_00000, 40)
	return BuildColumnHist(ctx, numBuckets, id, collector, tp, collector.Count, collector.FMSketch.NDV(), collector.NullCount)
}

var _builder_00000 = "statistics/builder.go"
