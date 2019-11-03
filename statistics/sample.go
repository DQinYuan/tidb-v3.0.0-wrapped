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
	"context"
	"math/rand"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

// SampleItem is an item of sampled column value.
type SampleItem struct {
	// Value is the sampled column value.
	Value types.Datum
	// Ordinal is original position of this item in SampleCollector before sorting. This
	// is used for computing correlation.
	Ordinal int
	// RowID is the row id of the sample in its key.
	// This property is used to calculate Ordinal in fast analyze.
	RowID int64
}

// SortSampleItems sorts a slice of SampleItem.
func SortSampleItems(sc *stmtctx.StatementContext, items []*SampleItem) error {
	trace_util_0.Count(_sample_00000, 0)
	sorter := sampleItemSorter{items: items, sc: sc}
	sort.Stable(&sorter)
	return sorter.err
}

type sampleItemSorter struct {
	items []*SampleItem
	sc    *stmtctx.StatementContext
	err   error
}

func (s *sampleItemSorter) Len() int {
	trace_util_0.Count(_sample_00000, 1)
	return len(s.items)
}

func (s *sampleItemSorter) Less(i, j int) bool {
	trace_util_0.Count(_sample_00000, 2)
	var cmp int
	cmp, s.err = s.items[i].Value.CompareDatum(s.sc, &s.items[j].Value)
	if s.err != nil {
		trace_util_0.Count(_sample_00000, 4)
		return true
	}
	trace_util_0.Count(_sample_00000, 3)
	return cmp < 0
}

func (s *sampleItemSorter) Swap(i, j int) {
	trace_util_0.Count(_sample_00000, 5)
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

// SampleCollector will collect Samples and calculate the count and ndv of an attribute.
type SampleCollector struct {
	Samples       []*SampleItem
	seenValues    int64 // seenValues is the current seen values.
	IsMerger      bool
	NullCount     int64
	Count         int64 // Count is the number of non-null rows.
	MaxSampleSize int64
	FMSketch      *FMSketch
	CMSketch      *CMSketch
	TotalSize     int64 // TotalSize is the total size of column.
}

// MergeSampleCollector merges two sample collectors.
func (c *SampleCollector) MergeSampleCollector(sc *stmtctx.StatementContext, rc *SampleCollector) {
	trace_util_0.Count(_sample_00000, 6)
	c.NullCount += rc.NullCount
	c.Count += rc.Count
	c.TotalSize += rc.TotalSize
	c.FMSketch.mergeFMSketch(rc.FMSketch)
	if rc.CMSketch != nil {
		trace_util_0.Count(_sample_00000, 8)
		err := c.CMSketch.MergeCMSketch(rc.CMSketch, 0)
		terror.Log(errors.Trace(err))
	}
	trace_util_0.Count(_sample_00000, 7)
	for _, item := range rc.Samples {
		trace_util_0.Count(_sample_00000, 9)
		err := c.collect(sc, item.Value)
		terror.Log(errors.Trace(err))
	}
}

// SampleCollectorToProto converts SampleCollector to its protobuf representation.
func SampleCollectorToProto(c *SampleCollector) *tipb.SampleCollector {
	trace_util_0.Count(_sample_00000, 10)
	collector := &tipb.SampleCollector{
		NullCount: c.NullCount,
		Count:     c.Count,
		FmSketch:  FMSketchToProto(c.FMSketch),
		TotalSize: &c.TotalSize,
	}
	if c.CMSketch != nil {
		trace_util_0.Count(_sample_00000, 13)
		collector.CmSketch = CMSketchToProto(c.CMSketch)
	}
	trace_util_0.Count(_sample_00000, 11)
	for _, item := range c.Samples {
		trace_util_0.Count(_sample_00000, 14)
		collector.Samples = append(collector.Samples, item.Value.GetBytes())
	}
	trace_util_0.Count(_sample_00000, 12)
	return collector
}

const maxSampleValueLength = mysql.MaxFieldVarCharLength / 2

// SampleCollectorFromProto converts SampleCollector from its protobuf representation.
func SampleCollectorFromProto(collector *tipb.SampleCollector) *SampleCollector {
	trace_util_0.Count(_sample_00000, 15)
	s := &SampleCollector{
		NullCount: collector.NullCount,
		Count:     collector.Count,
		FMSketch:  FMSketchFromProto(collector.FmSketch),
	}
	if collector.TotalSize != nil {
		trace_util_0.Count(_sample_00000, 18)
		s.TotalSize = *collector.TotalSize
	}
	trace_util_0.Count(_sample_00000, 16)
	s.CMSketch = CMSketchFromProto(collector.CmSketch)
	for _, val := range collector.Samples {
		trace_util_0.Count(_sample_00000, 19)
		// When store the histogram bucket boundaries to kv, we need to limit the length of the value.
		if len(val) <= maxSampleValueLength {
			trace_util_0.Count(_sample_00000, 20)
			item := &SampleItem{Value: types.NewBytesDatum(val)}
			s.Samples = append(s.Samples, item)
		}
	}
	trace_util_0.Count(_sample_00000, 17)
	return s
}

func (c *SampleCollector) collect(sc *stmtctx.StatementContext, d types.Datum) error {
	trace_util_0.Count(_sample_00000, 21)
	if !c.IsMerger {
		trace_util_0.Count(_sample_00000, 24)
		if d.IsNull() {
			trace_util_0.Count(_sample_00000, 28)
			c.NullCount++
			return nil
		}
		trace_util_0.Count(_sample_00000, 25)
		c.Count++
		if err := c.FMSketch.InsertValue(sc, d); err != nil {
			trace_util_0.Count(_sample_00000, 29)
			return errors.Trace(err)
		}
		trace_util_0.Count(_sample_00000, 26)
		if c.CMSketch != nil {
			trace_util_0.Count(_sample_00000, 30)
			c.CMSketch.InsertBytes(d.GetBytes())
		}
		// Minus one is to remove the flag byte.
		trace_util_0.Count(_sample_00000, 27)
		c.TotalSize += int64(len(d.GetBytes()) - 1)
	}
	trace_util_0.Count(_sample_00000, 22)
	c.seenValues++
	// The following code use types.CloneDatum(d) because d may have a deep reference
	// to the underlying slice, GC can't free them which lead to memory leak eventually.
	// TODO: Refactor the proto to avoid copying here.
	if len(c.Samples) < int(c.MaxSampleSize) {
		trace_util_0.Count(_sample_00000, 31)
		newItem := &SampleItem{Value: types.CloneDatum(d)}
		c.Samples = append(c.Samples, newItem)
	} else {
		trace_util_0.Count(_sample_00000, 32)
		{
			shouldAdd := rand.Int63n(c.seenValues) < c.MaxSampleSize
			if shouldAdd {
				trace_util_0.Count(_sample_00000, 33)
				idx := rand.Intn(int(c.MaxSampleSize))
				newItem := &SampleItem{Value: types.CloneDatum(d)}
				// To keep the order of the elements, we use delete and append, not direct replacement.
				c.Samples = append(c.Samples[:idx], c.Samples[idx+1:]...)
				c.Samples = append(c.Samples, newItem)
			}
		}
	}
	trace_util_0.Count(_sample_00000, 23)
	return nil
}

// CalcTotalSize is to calculate total size based on samples.
func (c *SampleCollector) CalcTotalSize() {
	trace_util_0.Count(_sample_00000, 34)
	c.TotalSize = 0
	for _, item := range c.Samples {
		trace_util_0.Count(_sample_00000, 35)
		c.TotalSize += int64(len(item.Value.GetBytes()))
	}
}

// SampleBuilder is used to build samples for columns.
// Also, if primary key is handle, it will directly build histogram for it.
type SampleBuilder struct {
	Sc              *stmtctx.StatementContext
	RecordSet       sqlexec.RecordSet
	ColLen          int // ColLen is the number of columns need to be sampled.
	PkBuilder       *SortedBuilder
	MaxBucketSize   int64
	MaxSampleSize   int64
	MaxFMSketchSize int64
	CMSketchDepth   int32
	CMSketchWidth   int32
}

// CollectColumnStats collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process.
// It returns the sample collectors which contain total count, null count, distinct values count and CM Sketch.
// It also returns the statistic builder for PK which contains the histogram.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
func (s SampleBuilder) CollectColumnStats() ([]*SampleCollector, *SortedBuilder, error) {
	trace_util_0.Count(_sample_00000, 36)
	collectors := make([]*SampleCollector, s.ColLen)
	for i := range collectors {
		trace_util_0.Count(_sample_00000, 39)
		collectors[i] = &SampleCollector{
			MaxSampleSize: s.MaxSampleSize,
			FMSketch:      NewFMSketch(int(s.MaxFMSketchSize)),
		}
	}
	trace_util_0.Count(_sample_00000, 37)
	if s.CMSketchDepth > 0 && s.CMSketchWidth > 0 {
		trace_util_0.Count(_sample_00000, 40)
		for i := range collectors {
			trace_util_0.Count(_sample_00000, 41)
			collectors[i].CMSketch = NewCMSketch(s.CMSketchDepth, s.CMSketchWidth)
		}
	}
	trace_util_0.Count(_sample_00000, 38)
	ctx := context.TODO()
	req := s.RecordSet.NewRecordBatch()
	it := chunk.NewIterator4Chunk(req.Chunk)
	for {
		trace_util_0.Count(_sample_00000, 42)
		err := s.RecordSet.Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_sample_00000, 46)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_sample_00000, 43)
		if req.NumRows() == 0 {
			trace_util_0.Count(_sample_00000, 47)
			return collectors, s.PkBuilder, nil
		}
		trace_util_0.Count(_sample_00000, 44)
		if len(s.RecordSet.Fields()) == 0 {
			trace_util_0.Count(_sample_00000, 48)
			return nil, nil, errors.Errorf("collect column stats failed: record set has 0 field")
		}
		trace_util_0.Count(_sample_00000, 45)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			trace_util_0.Count(_sample_00000, 49)
			datums := RowToDatums(row, s.RecordSet.Fields())
			if s.PkBuilder != nil {
				trace_util_0.Count(_sample_00000, 51)
				err = s.PkBuilder.Iterate(datums[0])
				if err != nil {
					trace_util_0.Count(_sample_00000, 53)
					return nil, nil, errors.Trace(err)
				}
				trace_util_0.Count(_sample_00000, 52)
				datums = datums[1:]
			}
			trace_util_0.Count(_sample_00000, 50)
			for i, val := range datums {
				trace_util_0.Count(_sample_00000, 54)
				err = collectors[i].collect(s.Sc, val)
				if err != nil {
					trace_util_0.Count(_sample_00000, 55)
					return nil, nil, errors.Trace(err)
				}
			}
		}
	}
}

// RowToDatums converts row to datum slice.
func RowToDatums(row chunk.Row, fields []*ast.ResultField) []types.Datum {
	trace_util_0.Count(_sample_00000, 56)
	datums := make([]types.Datum, len(fields))
	for i, f := range fields {
		trace_util_0.Count(_sample_00000, 58)
		datums[i] = row.GetDatum(i, &f.Column.FieldType)
	}
	trace_util_0.Count(_sample_00000, 57)
	return datums
}

var _sample_00000 = "statistics/sample.go"
