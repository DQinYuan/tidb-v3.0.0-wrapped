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

package ranger

import (
	"bytes"
	"math"
	"sort"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

func validInterval(sc *stmtctx.StatementContext, low, high point) (bool, error) {
	trace_util_0.Count(_ranger_00000, 0)
	l, err := codec.EncodeKey(sc, nil, low.value)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 5)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_ranger_00000, 1)
	if low.excl {
		trace_util_0.Count(_ranger_00000, 6)
		l = []byte(kv.Key(l).PrefixNext())
	}
	trace_util_0.Count(_ranger_00000, 2)
	r, err := codec.EncodeKey(sc, nil, high.value)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 7)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_ranger_00000, 3)
	if !high.excl {
		trace_util_0.Count(_ranger_00000, 8)
		r = []byte(kv.Key(r).PrefixNext())
	}
	trace_util_0.Count(_ranger_00000, 4)
	return bytes.Compare(l, r) < 0, nil
}

// points2Ranges build index ranges from range points.
// Only one column is built there. If there're multiple columns, use appendPoints2Ranges.
func points2Ranges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 9)
	ranges := make([]*Range, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		trace_util_0.Count(_ranger_00000, 11)
		startPoint, err := convertPoint(sc, rangePoints[i], tp)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 17)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 12)
		endPoint, err := convertPoint(sc, rangePoints[i+1], tp)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 18)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 13)
		less, err := validInterval(sc, startPoint, endPoint)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 19)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 14)
		if !less {
			trace_util_0.Count(_ranger_00000, 20)
			continue
		}
		// If column has not null flag, [null, null] should be removed.
		trace_util_0.Count(_ranger_00000, 15)
		if mysql.HasNotNullFlag(tp.Flag) && endPoint.value.Kind() == types.KindNull {
			trace_util_0.Count(_ranger_00000, 21)
			continue
		}

		trace_util_0.Count(_ranger_00000, 16)
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		ranges = append(ranges, ran)
	}
	trace_util_0.Count(_ranger_00000, 10)
	return ranges, nil
}

func convertPoint(sc *stmtctx.StatementContext, point point, tp *types.FieldType) (point, error) {
	trace_util_0.Count(_ranger_00000, 22)
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		trace_util_0.Count(_ranger_00000, 28)
		return point, nil
	}
	trace_util_0.Count(_ranger_00000, 23)
	casted, err := point.value.ConvertTo(sc, tp)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 29)
		return point, errors.Trace(err)
	}
	trace_util_0.Count(_ranger_00000, 24)
	valCmpCasted, err := point.value.CompareDatum(sc, &casted)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 30)
		return point, errors.Trace(err)
	}
	trace_util_0.Count(_ranger_00000, 25)
	point.value = casted
	if valCmpCasted == 0 {
		trace_util_0.Count(_ranger_00000, 31)
		return point, nil
	}
	trace_util_0.Count(_ranger_00000, 26)
	if point.start {
		trace_util_0.Count(_ranger_00000, 32)
		if point.excl {
			trace_util_0.Count(_ranger_00000, 33)
			if valCmpCasted < 0 {
				trace_util_0.Count(_ranger_00000, 34)
				// e.g. "a > 1.9" convert to "a >= 2".
				point.excl = false
			}
		} else {
			trace_util_0.Count(_ranger_00000, 35)
			{
				if valCmpCasted > 0 {
					trace_util_0.Count(_ranger_00000, 36)
					// e.g. "a >= 1.1 convert to "a > 1"
					point.excl = true
				}
			}
		}
	} else {
		trace_util_0.Count(_ranger_00000, 37)
		{
			if point.excl {
				trace_util_0.Count(_ranger_00000, 38)
				if valCmpCasted > 0 {
					trace_util_0.Count(_ranger_00000, 39)
					// e.g. "a < 1.1" convert to "a <= 1"
					point.excl = false
				}
			} else {
				trace_util_0.Count(_ranger_00000, 40)
				{
					if valCmpCasted < 0 {
						trace_util_0.Count(_ranger_00000, 41)
						// e.g. "a <= 1.9" convert to "a < 2"
						point.excl = true
					}
				}
			}
		}
	}
	trace_util_0.Count(_ranger_00000, 27)
	return point, nil
}

// appendPoints2Ranges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func appendPoints2Ranges(sc *stmtctx.StatementContext, origin []*Range, rangePoints []point,
	ft *types.FieldType) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 42)
	var newIndexRanges []*Range
	for i := 0; i < len(origin); i++ {
		trace_util_0.Count(_ranger_00000, 44)
		oRange := origin[i]
		if !oRange.IsPoint(sc) {
			trace_util_0.Count(_ranger_00000, 45)
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			trace_util_0.Count(_ranger_00000, 46)
			{
				newRanges, err := appendPoints2IndexRange(sc, oRange, rangePoints, ft)
				if err != nil {
					trace_util_0.Count(_ranger_00000, 48)
					return nil, errors.Trace(err)
				}
				trace_util_0.Count(_ranger_00000, 47)
				newIndexRanges = append(newIndexRanges, newRanges...)
			}
		}
	}
	trace_util_0.Count(_ranger_00000, 43)
	return newIndexRanges, nil
}

func appendPoints2IndexRange(sc *stmtctx.StatementContext, origin *Range, rangePoints []point,
	ft *types.FieldType) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 49)
	newRanges := make([]*Range, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		trace_util_0.Count(_ranger_00000, 51)
		startPoint, err := convertPoint(sc, rangePoints[i], ft)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 56)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 52)
		endPoint, err := convertPoint(sc, rangePoints[i+1], ft)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 57)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 53)
		less, err := validInterval(sc, startPoint, endPoint)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 58)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 54)
		if !less {
			trace_util_0.Count(_ranger_00000, 59)
			continue
		}

		trace_util_0.Count(_ranger_00000, 55)
		lowVal := make([]types.Datum, len(origin.LowVal)+1)
		copy(lowVal, origin.LowVal)
		lowVal[len(origin.LowVal)] = startPoint.value

		highVal := make([]types.Datum, len(origin.HighVal)+1)
		copy(highVal, origin.HighVal)
		highVal[len(origin.HighVal)] = endPoint.value

		ir := &Range{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	trace_util_0.Count(_ranger_00000, 50)
	return newRanges, nil
}

// points2TableRanges build ranges for table scan from range points.
// It will remove the nil and convert MinNotNull and MaxValue to MinInt64 or MinUint64 and MaxInt64 or MaxUint64.
func points2TableRanges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 60)
	ranges := make([]*Range, 0, len(rangePoints)/2)
	var minValueDatum, maxValueDatum types.Datum
	// Currently, table's kv range cannot accept encoded value of MaxValueDatum. we need to convert it.
	if mysql.HasUnsignedFlag(tp.Flag) {
		trace_util_0.Count(_ranger_00000, 63)
		minValueDatum.SetUint64(0)
		maxValueDatum.SetUint64(math.MaxUint64)
	} else {
		trace_util_0.Count(_ranger_00000, 64)
		{
			minValueDatum.SetInt64(math.MinInt64)
			maxValueDatum.SetInt64(math.MaxInt64)
		}
	}
	trace_util_0.Count(_ranger_00000, 61)
	for i := 0; i < len(rangePoints); i += 2 {
		trace_util_0.Count(_ranger_00000, 65)
		startPoint, err := convertPoint(sc, rangePoints[i], tp)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 72)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 66)
		if startPoint.value.Kind() == types.KindNull {
			trace_util_0.Count(_ranger_00000, 73)
			startPoint.value = minValueDatum
			startPoint.excl = false
		} else {
			trace_util_0.Count(_ranger_00000, 74)
			if startPoint.value.Kind() == types.KindMinNotNull {
				trace_util_0.Count(_ranger_00000, 75)
				startPoint.value = minValueDatum
			}
		}
		trace_util_0.Count(_ranger_00000, 67)
		endPoint, err := convertPoint(sc, rangePoints[i+1], tp)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 76)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 68)
		if endPoint.value.Kind() == types.KindMaxValue {
			trace_util_0.Count(_ranger_00000, 77)
			endPoint.value = maxValueDatum
		} else {
			trace_util_0.Count(_ranger_00000, 78)
			if endPoint.value.Kind() == types.KindNull {
				trace_util_0.Count(_ranger_00000, 79)
				continue
			}
		}
		trace_util_0.Count(_ranger_00000, 69)
		less, err := validInterval(sc, startPoint, endPoint)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 80)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 70)
		if !less {
			trace_util_0.Count(_ranger_00000, 81)
			continue
		}
		trace_util_0.Count(_ranger_00000, 71)
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		ranges = append(ranges, ran)
	}
	trace_util_0.Count(_ranger_00000, 62)
	return ranges, nil
}

// buildColumnRange builds range from CNF conditions.
func buildColumnRange(accessConditions []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType, tableRange bool, colLen int) (ranges []*Range, err error) {
	trace_util_0.Count(_ranger_00000, 82)
	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range accessConditions {
		trace_util_0.Count(_ranger_00000, 87)
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			trace_util_0.Count(_ranger_00000, 88)
			return nil, errors.Trace(rb.err)
		}
	}
	trace_util_0.Count(_ranger_00000, 83)
	newTp := newFieldType(tp)
	if tableRange {
		trace_util_0.Count(_ranger_00000, 89)
		ranges, err = points2TableRanges(sc, rangePoints, newTp)
	} else {
		trace_util_0.Count(_ranger_00000, 90)
		{
			ranges, err = points2Ranges(sc, rangePoints, newTp)
		}
	}
	trace_util_0.Count(_ranger_00000, 84)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 91)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ranger_00000, 85)
	if colLen != types.UnspecifiedLength {
		trace_util_0.Count(_ranger_00000, 92)
		for _, ran := range ranges {
			trace_util_0.Count(_ranger_00000, 93)
			if fixRangeDatum(&ran.LowVal[0], colLen, tp) {
				trace_util_0.Count(_ranger_00000, 95)
				ran.LowExclude = false
			}
			trace_util_0.Count(_ranger_00000, 94)
			if fixRangeDatum(&ran.HighVal[0], colLen, tp) {
				trace_util_0.Count(_ranger_00000, 96)
				ran.HighExclude = false
			}
		}
	}
	trace_util_0.Count(_ranger_00000, 86)
	return ranges, nil
}

// BuildTableRange builds range of PK column for PhysicalTableScan.
func BuildTableRange(accessConditions []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 97)
	return buildColumnRange(accessConditions, sc, tp, true, types.UnspecifiedLength)
}

// BuildColumnRange builds range from access conditions for general columns.
func BuildColumnRange(conds []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType, colLen int) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 98)
	if len(conds) == 0 {
		trace_util_0.Count(_ranger_00000, 100)
		return []*Range{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{types.MaxValueDatum()}}}, nil
	}
	trace_util_0.Count(_ranger_00000, 99)
	return buildColumnRange(conds, sc, tp, false, colLen)
}

// buildCNFIndexRange builds the range for index where the top layer is CNF.
func buildCNFIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, newTp []*types.FieldType, lengths []int,
	eqAndInCount int, accessCondition []expression.Expression) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 101)
	rb := builder{sc: sc}
	var (
		ranges []*Range
		err    error
	)
	for _, col := range cols {
		trace_util_0.Count(_ranger_00000, 108)
		newTp = append(newTp, newFieldType(col.RetType))
	}
	trace_util_0.Count(_ranger_00000, 102)
	for i := 0; i < eqAndInCount; i++ {
		trace_util_0.Count(_ranger_00000, 109)
		if sf, ok := accessCondition[i].(*expression.ScalarFunction); !ok || (sf.FuncName.L != ast.EQ && sf.FuncName.L != ast.In) {
			trace_util_0.Count(_ranger_00000, 113)
			break
		}
		// Build ranges for equal or in access conditions.
		trace_util_0.Count(_ranger_00000, 110)
		point := rb.build(accessCondition[i])
		if rb.err != nil {
			trace_util_0.Count(_ranger_00000, 114)
			return nil, errors.Trace(rb.err)
		}
		trace_util_0.Count(_ranger_00000, 111)
		if i == 0 {
			trace_util_0.Count(_ranger_00000, 115)
			ranges, err = points2Ranges(sc, point, newTp[i])
		} else {
			trace_util_0.Count(_ranger_00000, 116)
			{
				ranges, err = appendPoints2Ranges(sc, ranges, point, newTp[i])
			}
		}
		trace_util_0.Count(_ranger_00000, 112)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 117)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_ranger_00000, 103)
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessCondition); i++ {
		trace_util_0.Count(_ranger_00000, 118)
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
		if rb.err != nil {
			trace_util_0.Count(_ranger_00000, 119)
			return nil, errors.Trace(rb.err)
		}
	}
	trace_util_0.Count(_ranger_00000, 104)
	if eqAndInCount == 0 {
		trace_util_0.Count(_ranger_00000, 120)
		ranges, err = points2Ranges(sc, rangePoints, newTp[0])
	} else {
		trace_util_0.Count(_ranger_00000, 121)
		if eqAndInCount < len(accessCondition) {
			trace_util_0.Count(_ranger_00000, 122)
			ranges, err = appendPoints2Ranges(sc, ranges, rangePoints, newTp[eqAndInCount])
		}
	}
	trace_util_0.Count(_ranger_00000, 105)
	if err != nil {
		trace_util_0.Count(_ranger_00000, 123)
		return nil, errors.Trace(err)
	}

	// Take prefix index into consideration.
	trace_util_0.Count(_ranger_00000, 106)
	if hasPrefix(lengths) {
		trace_util_0.Count(_ranger_00000, 124)
		if fixPrefixColRange(ranges, lengths, newTp) {
			trace_util_0.Count(_ranger_00000, 125)
			ranges, err = unionRanges(sc, ranges)
			if err != nil {
				trace_util_0.Count(_ranger_00000, 126)
				return nil, errors.Trace(err)
			}
		}
	}

	trace_util_0.Count(_ranger_00000, 107)
	return ranges, nil
}

type sortRange struct {
	originalValue *Range
	encodedStart  []byte
	encodedEnd    []byte
}

func unionRanges(sc *stmtctx.StatementContext, ranges []*Range) ([]*Range, error) {
	trace_util_0.Count(_ranger_00000, 127)
	if len(ranges) == 0 {
		trace_util_0.Count(_ranger_00000, 132)
		return nil, nil
	}
	trace_util_0.Count(_ranger_00000, 128)
	objects := make([]*sortRange, 0, len(ranges))
	for _, ran := range ranges {
		trace_util_0.Count(_ranger_00000, 133)
		left, err := codec.EncodeKey(sc, nil, ran.LowVal...)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 138)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 134)
		if ran.LowExclude {
			trace_util_0.Count(_ranger_00000, 139)
			left = kv.Key(left).PrefixNext()
		}
		trace_util_0.Count(_ranger_00000, 135)
		right, err := codec.EncodeKey(sc, nil, ran.HighVal...)
		if err != nil {
			trace_util_0.Count(_ranger_00000, 140)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ranger_00000, 136)
		if !ran.HighExclude {
			trace_util_0.Count(_ranger_00000, 141)
			right = kv.Key(right).PrefixNext()
		}
		trace_util_0.Count(_ranger_00000, 137)
		objects = append(objects, &sortRange{originalValue: ran, encodedStart: left, encodedEnd: right})
	}
	trace_util_0.Count(_ranger_00000, 129)
	sort.Slice(objects, func(i, j int) bool {
		trace_util_0.Count(_ranger_00000, 142)
		return bytes.Compare(objects[i].encodedStart, objects[j].encodedStart) < 0
	})
	trace_util_0.Count(_ranger_00000, 130)
	ranges = ranges[:0]
	lastRange := objects[0]
	for i := 1; i < len(objects); i++ {
		trace_util_0.Count(_ranger_00000, 143)
		// For two intervals [a, b], [c, d], we have guaranteed that a >= c. If b >= c. Then two intervals are overlapped.
		// And this two can be merged as [a, max(b, d)].
		// Otherwise they aren't overlapped.
		if bytes.Compare(lastRange.encodedEnd, objects[i].encodedStart) >= 0 {
			trace_util_0.Count(_ranger_00000, 144)
			if bytes.Compare(lastRange.encodedEnd, objects[i].encodedEnd) < 0 {
				trace_util_0.Count(_ranger_00000, 145)
				lastRange.encodedEnd = objects[i].encodedEnd
				lastRange.originalValue.HighVal = objects[i].originalValue.HighVal
				lastRange.originalValue.HighExclude = objects[i].originalValue.HighExclude
			}
		} else {
			trace_util_0.Count(_ranger_00000, 146)
			{
				ranges = append(ranges, lastRange.originalValue)
				lastRange = objects[i]
			}
		}
	}
	trace_util_0.Count(_ranger_00000, 131)
	ranges = append(ranges, lastRange.originalValue)
	return ranges, nil
}

func hasPrefix(lengths []int) bool {
	trace_util_0.Count(_ranger_00000, 147)
	for _, l := range lengths {
		trace_util_0.Count(_ranger_00000, 149)
		if l != types.UnspecifiedLength {
			trace_util_0.Count(_ranger_00000, 150)
			return true
		}
	}
	trace_util_0.Count(_ranger_00000, 148)
	return false
}

// fixPrefixColRange checks whether the range of one column exceeds the length and needs to be cut.
// It specially handles the last column of each range point. If the last one need to be cut, it will
// change the exclude status of that point and return `true` to tell
// that we need do a range merging since that interval may have intersection.
// e.g. if the interval is (-inf -inf, a xxxxx), (a xxxxx, +inf +inf) and the length of the last column is 3,
//      then we'll change it to (-inf -inf, a xxx], [a xxx, +inf +inf). You can see that this two interval intersect,
//      so we need a merge operation.
// Q: only checking the last column to decide whether the endpoint's exclude status needs to be reset is enough?
// A: Yes, suppose that the interval is (-inf -inf, a xxxxx b) and only the second column needs to be cut.
//    The result would be (-inf -inf, a xxx b) if the length of it is 3. Obviously we only need to care about the data
//    whose the first two key is `a` and `xxx`. It read all data whose index value begins with `a` and `xxx` and the third
//    value less than `b`, covering the values begin with `a` and `xxxxx` and the third value less than `b` perfectly.
//    So in this case we don't need to reset its exclude status. The right endpoint case can be proved in the same way.
func fixPrefixColRange(ranges []*Range, lengths []int, tp []*types.FieldType) bool {
	trace_util_0.Count(_ranger_00000, 151)
	var hasCut bool
	for _, ran := range ranges {
		trace_util_0.Count(_ranger_00000, 153)
		lowTail := len(ran.LowVal) - 1
		for i := 0; i < lowTail; i++ {
			trace_util_0.Count(_ranger_00000, 158)
			fixRangeDatum(&ran.LowVal[i], lengths[i], tp[i])
		}
		trace_util_0.Count(_ranger_00000, 154)
		lowCut := fixRangeDatum(&ran.LowVal[lowTail], lengths[lowTail], tp[lowTail])
		if lowCut {
			trace_util_0.Count(_ranger_00000, 159)
			ran.LowExclude = false
		}
		trace_util_0.Count(_ranger_00000, 155)
		highTail := len(ran.HighVal) - 1
		for i := 0; i < highTail; i++ {
			trace_util_0.Count(_ranger_00000, 160)
			fixRangeDatum(&ran.HighVal[i], lengths[i], tp[i])
		}
		trace_util_0.Count(_ranger_00000, 156)
		highCut := fixRangeDatum(&ran.HighVal[highTail], lengths[highTail], tp[highTail])
		if highCut {
			trace_util_0.Count(_ranger_00000, 161)
			ran.HighExclude = false
		}
		trace_util_0.Count(_ranger_00000, 157)
		hasCut = lowCut || highCut
	}
	trace_util_0.Count(_ranger_00000, 152)
	return hasCut
}

func fixRangeDatum(v *types.Datum, length int, tp *types.FieldType) bool {
	trace_util_0.Count(_ranger_00000, 162)
	// If this column is prefix and the prefix length is smaller than the range, cut it.
	// In case of UTF8, prefix should be cut by characters rather than bytes
	if v.Kind() == types.KindString || v.Kind() == types.KindBytes {
		trace_util_0.Count(_ranger_00000, 164)
		colCharset := tp.Charset
		colValue := v.GetBytes()
		isUTF8Charset := colCharset == charset.CharsetUTF8 || colCharset == charset.CharsetUTF8MB4
		if isUTF8Charset {
			trace_util_0.Count(_ranger_00000, 165)
			if length != types.UnspecifiedLength && utf8.RuneCount(colValue) > length {
				trace_util_0.Count(_ranger_00000, 166)
				rs := bytes.Runes(colValue)
				truncateStr := string(rs[:length])
				// truncate value and limit its length
				v.SetString(truncateStr)
				return true
			}
		} else {
			trace_util_0.Count(_ranger_00000, 167)
			if length != types.UnspecifiedLength && len(colValue) > length {
				trace_util_0.Count(_ranger_00000, 168)
				// truncate value and limit its length
				v.SetBytes(colValue[:length])
				return true
			}
		}
	}
	trace_util_0.Count(_ranger_00000, 163)
	return false
}

// We cannot use the FieldType of column directly. e.g. the column a is int32 and we have a > 1111111111111111111.
// Obviously the constant is bigger than MaxInt32, so we will get overflow error if we use the FieldType of column a.
func newFieldType(tp *types.FieldType) *types.FieldType {
	trace_util_0.Count(_ranger_00000, 169)
	switch tp.Tp {
	// To avoid overflow error.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_ranger_00000, 170)
		newTp := types.NewFieldType(mysql.TypeLonglong)
		newTp.Flag = tp.Flag
		newTp.Charset = tp.Charset
		return newTp
	// To avoid data truncate error.
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		trace_util_0.Count(_ranger_00000, 171)
		newTp := types.NewFieldType(tp.Tp)
		newTp.Charset = tp.Charset
		return newTp
	default:
		trace_util_0.Count(_ranger_00000, 172)
		return tp
	}
}

// points2EqOrInCond constructs a 'EQUAL' or 'IN' scalar function based on the
// 'points'. The target column is extracted from the 'expr'.
// NOTE:
// 1. 'expr' must be either 'EQUAL' or 'IN' function.
// 2. 'points' should not be empty.
func points2EqOrInCond(ctx sessionctx.Context, points []point, expr expression.Expression) expression.Expression {
	trace_util_0.Count(_ranger_00000, 173)
	// len(points) cannot be 0 here, since we impose early termination in ExtractEqAndInCondition
	sf, _ := expr.(*expression.ScalarFunction)
	// Constant and Column args should have same RetType, simply get from first arg
	retType := sf.GetArgs()[0].GetType()
	args := make([]expression.Expression, 0, len(points)/2)
	if sf.FuncName.L == ast.EQ {
		trace_util_0.Count(_ranger_00000, 177)
		if c, ok := sf.GetArgs()[0].(*expression.Column); ok {
			trace_util_0.Count(_ranger_00000, 178)
			args = append(args, c)
		} else {
			trace_util_0.Count(_ranger_00000, 179)
			if c, ok := sf.GetArgs()[1].(*expression.Column); ok {
				trace_util_0.Count(_ranger_00000, 180)
				args = append(args, c)
			}
		}
	} else {
		trace_util_0.Count(_ranger_00000, 181)
		{
			args = append(args, sf.GetArgs()[0])
		}
	}
	trace_util_0.Count(_ranger_00000, 174)
	for i := 0; i < len(points); i = i + 2 {
		trace_util_0.Count(_ranger_00000, 182)
		value := &expression.Constant{
			Value:   points[i].value,
			RetType: retType,
		}
		args = append(args, value)
	}
	trace_util_0.Count(_ranger_00000, 175)
	funcName := ast.EQ
	if len(args) > 2 {
		trace_util_0.Count(_ranger_00000, 183)
		funcName = ast.In
	}
	trace_util_0.Count(_ranger_00000, 176)
	f := expression.NewFunctionInternal(ctx, funcName, sf.GetType(), args...)
	return f
}

var _ranger_00000 = "util/ranger/ranger.go"
