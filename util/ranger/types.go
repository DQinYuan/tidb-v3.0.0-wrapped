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
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// Range represents a range generated in physical plan building phase.
type Range struct {
	LowVal  []types.Datum
	HighVal []types.Datum

	LowExclude  bool // Low value is exclusive.
	HighExclude bool // High value is exclusive.
}

// Clone clones a Range.
func (ran *Range) Clone() *Range {
	trace_util_0.Count(_types_00000, 0)
	newRange := &Range{
		LowVal:      make([]types.Datum, 0, len(ran.LowVal)),
		HighVal:     make([]types.Datum, 0, len(ran.HighVal)),
		LowExclude:  ran.LowExclude,
		HighExclude: ran.HighExclude,
	}
	for i, length := 0, len(ran.LowVal); i < length; i++ {
		trace_util_0.Count(_types_00000, 3)
		newRange.LowVal = append(newRange.LowVal, ran.LowVal[i])
	}
	trace_util_0.Count(_types_00000, 1)
	for i, length := 0, len(ran.HighVal); i < length; i++ {
		trace_util_0.Count(_types_00000, 4)
		newRange.HighVal = append(newRange.HighVal, ran.HighVal[i])
	}
	trace_util_0.Count(_types_00000, 2)
	return newRange
}

// IsPoint returns if the range is a point.
func (ran *Range) IsPoint(sc *stmtctx.StatementContext) bool {
	trace_util_0.Count(_types_00000, 5)
	if len(ran.LowVal) != len(ran.HighVal) {
		trace_util_0.Count(_types_00000, 8)
		return false
	}
	trace_util_0.Count(_types_00000, 6)
	for i := range ran.LowVal {
		trace_util_0.Count(_types_00000, 9)
		a := ran.LowVal[i]
		b := ran.HighVal[i]
		if a.Kind() == types.KindMinNotNull || b.Kind() == types.KindMaxValue {
			trace_util_0.Count(_types_00000, 13)
			return false
		}
		trace_util_0.Count(_types_00000, 10)
		cmp, err := a.CompareDatum(sc, &b)
		if err != nil {
			trace_util_0.Count(_types_00000, 14)
			return false
		}
		trace_util_0.Count(_types_00000, 11)
		if cmp != 0 {
			trace_util_0.Count(_types_00000, 15)
			return false
		}

		trace_util_0.Count(_types_00000, 12)
		if a.IsNull() {
			trace_util_0.Count(_types_00000, 16)
			return false
		}
	}
	trace_util_0.Count(_types_00000, 7)
	return !ran.LowExclude && !ran.HighExclude
}

// String implements the Stringer interface.
func (ran *Range) String() string {
	trace_util_0.Count(_types_00000, 17)
	lowStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.LowVal {
		trace_util_0.Count(_types_00000, 22)
		lowStrs = append(lowStrs, formatDatum(d, true))
	}
	trace_util_0.Count(_types_00000, 18)
	highStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.HighVal {
		trace_util_0.Count(_types_00000, 23)
		highStrs = append(highStrs, formatDatum(d, false))
	}
	trace_util_0.Count(_types_00000, 19)
	l, r := "[", "]"
	if ran.LowExclude {
		trace_util_0.Count(_types_00000, 24)
		l = "("
	}
	trace_util_0.Count(_types_00000, 20)
	if ran.HighExclude {
		trace_util_0.Count(_types_00000, 25)
		r = ")"
	}
	trace_util_0.Count(_types_00000, 21)
	return l + strings.Join(lowStrs, " ") + "," + strings.Join(highStrs, " ") + r
}

// Encode encodes the range to its encoded value.
func (ran *Range) Encode(sc *stmtctx.StatementContext, lowBuffer, highBuffer []byte) ([]byte, []byte, error) {
	trace_util_0.Count(_types_00000, 26)
	var err error
	lowBuffer, err = codec.EncodeKey(sc, lowBuffer[:0], ran.LowVal...)
	if err != nil {
		trace_util_0.Count(_types_00000, 31)
		return nil, nil, err
	}
	trace_util_0.Count(_types_00000, 27)
	if ran.LowExclude {
		trace_util_0.Count(_types_00000, 32)
		lowBuffer = kv.Key(lowBuffer).PrefixNext()
	}
	trace_util_0.Count(_types_00000, 28)
	highBuffer, err = codec.EncodeKey(sc, highBuffer[:0], ran.HighVal...)
	if err != nil {
		trace_util_0.Count(_types_00000, 33)
		return nil, nil, err
	}
	trace_util_0.Count(_types_00000, 29)
	if !ran.HighExclude {
		trace_util_0.Count(_types_00000, 34)
		highBuffer = kv.Key(highBuffer).PrefixNext()
	}
	trace_util_0.Count(_types_00000, 30)
	return lowBuffer, highBuffer, nil
}

// PrefixEqualLen tells you how long the prefix of the range is a point.
// e.g. If this range is (1 2 3, 1 2 +inf), then the return value is 2.
func (ran *Range) PrefixEqualLen(sc *stmtctx.StatementContext) (int, error) {
	trace_util_0.Count(_types_00000, 35)
	// Here, len(ran.LowVal) always equal to len(ran.HighVal)
	for i := 0; i < len(ran.LowVal); i++ {
		trace_util_0.Count(_types_00000, 37)
		cmp, err := ran.LowVal[i].CompareDatum(sc, &ran.HighVal[i])
		if err != nil {
			trace_util_0.Count(_types_00000, 39)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_types_00000, 38)
		if cmp != 0 {
			trace_util_0.Count(_types_00000, 40)
			return i, nil
		}
	}
	trace_util_0.Count(_types_00000, 36)
	return len(ran.LowVal), nil
}

func formatDatum(d types.Datum, isLeftSide bool) string {
	trace_util_0.Count(_types_00000, 41)
	switch d.Kind() {
	case types.KindNull:
		trace_util_0.Count(_types_00000, 43)
		return "NULL"
	case types.KindMinNotNull:
		trace_util_0.Count(_types_00000, 44)
		return "-inf"
	case types.KindMaxValue:
		trace_util_0.Count(_types_00000, 45)
		return "+inf"
	case types.KindInt64:
		trace_util_0.Count(_types_00000, 46)
		switch d.GetInt64() {
		case math.MinInt64:
			trace_util_0.Count(_types_00000, 49)
			if isLeftSide {
				trace_util_0.Count(_types_00000, 51)
				return "-inf"
			}
		case math.MaxInt64:
			trace_util_0.Count(_types_00000, 50)
			if !isLeftSide {
				trace_util_0.Count(_types_00000, 52)
				return "+inf"
			}
		}
	case types.KindUint64:
		trace_util_0.Count(_types_00000, 47)
		if d.GetUint64() == math.MaxUint64 && !isLeftSide {
			trace_util_0.Count(_types_00000, 53)
			return "+inf"
		}
	case types.KindString, types.KindBytes, types.KindMysqlEnum, types.KindMysqlSet,
		types.KindMysqlJSON, types.KindBinaryLiteral, types.KindMysqlBit:
		trace_util_0.Count(_types_00000, 48)
		return fmt.Sprintf("\"%v\"", d.GetValue())
	}
	trace_util_0.Count(_types_00000, 42)
	return fmt.Sprintf("%v", d.GetValue())
}

var _types_00000 = "util/ranger/types.go"
