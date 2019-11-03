// Copyright 2015 PingCAP, Inc.
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
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Error instances.
var (
	ErrUnsupportedType = terror.ClassOptimizer.New(CodeUnsupportedType, "Unsupported type")
)

// Error codes.
const (
	CodeUnsupportedType terror.ErrCode = 1
)

// RangeType is alias for int.
type RangeType int

// RangeType constants.
const (
	IntRangeType RangeType = iota
	ColumnRangeType
	IndexRangeType
)

// Point is the end point of range interval.
type point struct {
	value types.Datum
	excl  bool // exclude
	start bool
}

func (rp point) String() string {
	trace_util_0.Count(_points_00000, 0)
	val := rp.value.GetValue()
	if rp.value.Kind() == types.KindMinNotNull {
		trace_util_0.Count(_points_00000, 4)
		val = "-inf"
	} else {
		trace_util_0.Count(_points_00000, 5)
		if rp.value.Kind() == types.KindMaxValue {
			trace_util_0.Count(_points_00000, 6)
			val = "+inf"
		}
	}
	trace_util_0.Count(_points_00000, 1)
	if rp.start {
		trace_util_0.Count(_points_00000, 7)
		symbol := "["
		if rp.excl {
			trace_util_0.Count(_points_00000, 9)
			symbol = "("
		}
		trace_util_0.Count(_points_00000, 8)
		return fmt.Sprintf("%s%v", symbol, val)
	}
	trace_util_0.Count(_points_00000, 2)
	symbol := "]"
	if rp.excl {
		trace_util_0.Count(_points_00000, 10)
		symbol = ")"
	}
	trace_util_0.Count(_points_00000, 3)
	return fmt.Sprintf("%v%s", val, symbol)
}

type pointSorter struct {
	points []point
	err    error
	sc     *stmtctx.StatementContext
}

func (r *pointSorter) Len() int {
	trace_util_0.Count(_points_00000, 11)
	return len(r.points)
}

func (r *pointSorter) Less(i, j int) bool {
	trace_util_0.Count(_points_00000, 12)
	a := r.points[i]
	b := r.points[j]
	less, err := rangePointLess(r.sc, a, b)
	if err != nil {
		trace_util_0.Count(_points_00000, 14)
		r.err = err
	}
	trace_util_0.Count(_points_00000, 13)
	return less
}

func rangePointLess(sc *stmtctx.StatementContext, a, b point) (bool, error) {
	trace_util_0.Count(_points_00000, 15)
	cmp, err := a.value.CompareDatum(sc, &b.value)
	if cmp != 0 {
		trace_util_0.Count(_points_00000, 17)
		return cmp < 0, nil
	}
	trace_util_0.Count(_points_00000, 16)
	return rangePointEqualValueLess(a, b), errors.Trace(err)
}

func rangePointEqualValueLess(a, b point) bool {
	trace_util_0.Count(_points_00000, 18)
	if a.start && b.start {
		trace_util_0.Count(_points_00000, 20)
		return !a.excl && b.excl
	} else {
		trace_util_0.Count(_points_00000, 21)
		if a.start {
			trace_util_0.Count(_points_00000, 22)
			return !a.excl && !b.excl
		} else {
			trace_util_0.Count(_points_00000, 23)
			if b.start {
				trace_util_0.Count(_points_00000, 24)
				return a.excl || b.excl
			}
		}
	}
	trace_util_0.Count(_points_00000, 19)
	return a.excl && !b.excl
}

func (r *pointSorter) Swap(i, j int) {
	trace_util_0.Count(_points_00000, 25)
	r.points[i], r.points[j] = r.points[j], r.points[i]
}

// fullRange is (-∞, +∞).
var fullRange = []point{
	{start: true},
	{value: types.MaxValueDatum()},
}

// FullIntRange is used for table range. Since table range cannot accept MaxValueDatum as the max value.
// So we need to set it to MaxInt64.
func FullIntRange(isUnsigned bool) []*Range {
	trace_util_0.Count(_points_00000, 26)
	if isUnsigned {
		trace_util_0.Count(_points_00000, 28)
		return []*Range{{LowVal: []types.Datum{types.NewUintDatum(0)}, HighVal: []types.Datum{types.NewUintDatum(math.MaxUint64)}}}
	}
	trace_util_0.Count(_points_00000, 27)
	return []*Range{{LowVal: []types.Datum{types.NewIntDatum(math.MinInt64)}, HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)}}}
}

// FullRange is [null, +∞) for Range.
func FullRange() []*Range {
	trace_util_0.Count(_points_00000, 29)
	return []*Range{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{types.MaxValueDatum()}}}
}

// FullNotNullRange is (-∞, +∞) for Range.
func FullNotNullRange() []*Range {
	trace_util_0.Count(_points_00000, 30)
	return []*Range{{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: []types.Datum{types.MaxValueDatum()}}}
}

// NullRange is [null, null] for Range.
func NullRange() []*Range {
	trace_util_0.Count(_points_00000, 31)
	return []*Range{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{{}}}}
}

// builder is the range builder struct.
type builder struct {
	err error
	sc  *stmtctx.StatementContext
}

func (r *builder) build(expr expression.Expression) []point {
	trace_util_0.Count(_points_00000, 32)
	switch x := expr.(type) {
	case *expression.Column:
		trace_util_0.Count(_points_00000, 34)
		return r.buildFromColumn(x)
	case *expression.ScalarFunction:
		trace_util_0.Count(_points_00000, 35)
		return r.buildFromScalarFunc(x)
	case *expression.Constant:
		trace_util_0.Count(_points_00000, 36)
		return r.buildFromConstant(x)
	}

	trace_util_0.Count(_points_00000, 33)
	return fullRange
}

func (r *builder) buildFromConstant(expr *expression.Constant) []point {
	trace_util_0.Count(_points_00000, 37)
	dt, err := expr.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_points_00000, 42)
		r.err = err
		return nil
	}
	trace_util_0.Count(_points_00000, 38)
	if dt.IsNull() {
		trace_util_0.Count(_points_00000, 43)
		return nil
	}

	trace_util_0.Count(_points_00000, 39)
	val, err := dt.ToBool(r.sc)
	if err != nil {
		trace_util_0.Count(_points_00000, 44)
		r.err = err
		return nil
	}

	trace_util_0.Count(_points_00000, 40)
	if val == 0 {
		trace_util_0.Count(_points_00000, 45)
		return nil
	}
	trace_util_0.Count(_points_00000, 41)
	return fullRange
}

func (r *builder) buildFromColumn(expr *expression.Column) []point {
	trace_util_0.Count(_points_00000, 46)
	// column name expression is equivalent to column name is true.
	startPoint1 := point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueDatum()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFormBinOp(expr *expression.ScalarFunction) []point {
	trace_util_0.Count(_points_00000, 47)
	// This has been checked that the binary operation is comparison operation, and one of
	// the operand is column name expression.
	var (
		op    string
		value types.Datum
		err   error
		ft    *types.FieldType
	)
	if col, ok := expr.GetArgs()[0].(*expression.Column); ok {
		trace_util_0.Count(_points_00000, 54)
		value, err = expr.GetArgs()[1].Eval(chunk.Row{})
		op = expr.FuncName.L
		ft = col.RetType
	} else {
		trace_util_0.Count(_points_00000, 55)
		{
			col, ok := expr.GetArgs()[1].(*expression.Column)
			if !ok {
				trace_util_0.Count(_points_00000, 57)
				return nil
			}
			trace_util_0.Count(_points_00000, 56)
			ft = col.RetType

			value, err = expr.GetArgs()[0].Eval(chunk.Row{})
			switch expr.FuncName.L {
			case ast.GE:
				trace_util_0.Count(_points_00000, 58)
				op = ast.LE
			case ast.GT:
				trace_util_0.Count(_points_00000, 59)
				op = ast.LT
			case ast.LT:
				trace_util_0.Count(_points_00000, 60)
				op = ast.GT
			case ast.LE:
				trace_util_0.Count(_points_00000, 61)
				op = ast.GE
			default:
				trace_util_0.Count(_points_00000, 62)
				op = expr.FuncName.L
			}
		}
	}
	trace_util_0.Count(_points_00000, 48)
	if err != nil {
		trace_util_0.Count(_points_00000, 63)
		return nil
	}
	trace_util_0.Count(_points_00000, 49)
	if value.IsNull() {
		trace_util_0.Count(_points_00000, 64)
		return nil
	}

	trace_util_0.Count(_points_00000, 50)
	value, err = HandlePadCharToFullLength(r.sc, ft, value)
	if err != nil {
		trace_util_0.Count(_points_00000, 65)
		return nil
	}

	trace_util_0.Count(_points_00000, 51)
	value, op, isValidRange := handleUnsignedIntCol(ft, value, op)
	if !isValidRange {
		trace_util_0.Count(_points_00000, 66)
		return nil
	}

	trace_util_0.Count(_points_00000, 52)
	switch op {
	case ast.EQ:
		trace_util_0.Count(_points_00000, 67)
		startPoint := point{value: value, start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.NE:
		trace_util_0.Count(_points_00000, 68)
		startPoint1 := point{value: types.MinNotNullDatum(), start: true}
		endPoint1 := point{value: value, excl: true}
		startPoint2 := point{value: value, start: true, excl: true}
		endPoint2 := point{value: types.MaxValueDatum()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	case ast.LT:
		trace_util_0.Count(_points_00000, 69)
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: value, excl: true}
		return []point{startPoint, endPoint}
	case ast.LE:
		trace_util_0.Count(_points_00000, 70)
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.GT:
		trace_util_0.Count(_points_00000, 71)
		startPoint := point{value: value, start: true, excl: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	case ast.GE:
		trace_util_0.Count(_points_00000, 72)
		startPoint := point{value: value, start: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	}
	trace_util_0.Count(_points_00000, 53)
	return nil
}

// HandlePadCharToFullLength handles the "PAD_CHAR_TO_FULL_LENGTH" sql mode for
// CHAR[N] index columns.
// NOTE: kv.ErrNotExist is returned to indicate that this value can not match
//		 any (key, value) pair in tikv storage. This error should be handled by
//		 the caller.
func HandlePadCharToFullLength(sc *stmtctx.StatementContext, ft *types.FieldType, val types.Datum) (types.Datum, error) {
	trace_util_0.Count(_points_00000, 73)
	isChar := (ft.Tp == mysql.TypeString)
	isBinary := (isChar && ft.Collate == charset.CollationBin)
	isVarchar := (ft.Tp == mysql.TypeVarString || ft.Tp == mysql.TypeVarchar)
	isVarBinary := (isVarchar && ft.Collate == charset.CollationBin)

	if !isChar && !isVarchar && !isBinary && !isVarBinary {
		trace_util_0.Count(_points_00000, 76)
		return val, nil
	}

	trace_util_0.Count(_points_00000, 74)
	hasBinaryFlag := mysql.HasBinaryFlag(ft.Flag)
	targetStr, err := val.ToString()
	if err != nil {
		trace_util_0.Count(_points_00000, 77)
		return val, err
	}

	trace_util_0.Count(_points_00000, 75)
	switch {
	case isBinary || isVarBinary:
		trace_util_0.Count(_points_00000, 78)
		val.SetString(targetStr)
		return val, nil
	case isVarchar && hasBinaryFlag:
		trace_util_0.Count(_points_00000, 79)
		noTrailingSpace := strings.TrimRight(targetStr, " ")
		if numSpacesToFill := ft.Flen - len(noTrailingSpace); numSpacesToFill > 0 {
			trace_util_0.Count(_points_00000, 87)
			noTrailingSpace += strings.Repeat(" ", numSpacesToFill)
		}
		trace_util_0.Count(_points_00000, 80)
		val.SetString(noTrailingSpace)
		return val, nil
	case isVarchar && !hasBinaryFlag:
		trace_util_0.Count(_points_00000, 81)
		val.SetString(targetStr)
		return val, nil
	case isChar && hasBinaryFlag:
		trace_util_0.Count(_points_00000, 82)
		noTrailingSpace := strings.TrimRight(targetStr, " ")
		val.SetString(noTrailingSpace)
		return val, nil
	case isChar && !hasBinaryFlag && !sc.PadCharToFullLength:
		trace_util_0.Count(_points_00000, 83)
		val.SetString(targetStr)
		return val, nil
	case isChar && !hasBinaryFlag && sc.PadCharToFullLength:
		trace_util_0.Count(_points_00000, 84)
		if len(targetStr) != ft.Flen {
			trace_util_0.Count(_points_00000, 88)
			// return kv.ErrNotExist to indicate that this value can not match any
			// (key, value) pair in tikv storage.
			return val, kv.ErrNotExist
		}
		// Trailing spaces of data typed "CHAR[N]" is trimed in the storage, we
		// need to trim these trailing spaces as well.
		trace_util_0.Count(_points_00000, 85)
		noTrailingSpace := strings.TrimRight(targetStr, " ")
		val.SetString(noTrailingSpace)
		return val, nil
	default:
		trace_util_0.Count(_points_00000, 86)
		return val, nil
	}
}

// handleUnsignedIntCol handles the case when unsigned column meets negative integer value.
// The three returned values are: fixed constant value, fixed operator, and a boolean
// which indicates whether the range is valid or not.
func handleUnsignedIntCol(ft *types.FieldType, val types.Datum, op string) (types.Datum, string, bool) {
	trace_util_0.Count(_points_00000, 89)
	isUnsigned := mysql.HasUnsignedFlag(ft.Flag)
	isIntegerType := mysql.IsIntegerType(ft.Tp)
	isNegativeInteger := (val.Kind() == types.KindInt64 && val.GetInt64() < 0)

	if !isUnsigned || !isIntegerType || !isNegativeInteger {
		trace_util_0.Count(_points_00000, 92)
		return val, op, true
	}

	// If the operator is GT, GE or NE, the range should be [0, +inf].
	// Otherwise the value is out of valid range.
	trace_util_0.Count(_points_00000, 90)
	if op == ast.GT || op == ast.GE || op == ast.NE {
		trace_util_0.Count(_points_00000, 93)
		op = ast.GE
		val.SetUint64(0)
		return val, op, true
	}

	trace_util_0.Count(_points_00000, 91)
	return val, op, false
}

func (r *builder) buildFromIsTrue(expr *expression.ScalarFunction, isNot int) []point {
	trace_util_0.Count(_points_00000, 94)
	if isNot == 1 {
		trace_util_0.Count(_points_00000, 96)
		// NOT TRUE range is {[null null] [0, 0]}
		startPoint1 := point{start: true}
		endPoint1 := point{}
		startPoint2 := point{start: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{}
		endPoint2.value.SetInt64(0)
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// TRUE range is {[-inf 0) (0 +inf]}
	trace_util_0.Count(_points_00000, 95)
	startPoint1 := point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueDatum()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFromIsFalse(expr *expression.ScalarFunction, isNot int) []point {
	trace_util_0.Count(_points_00000, 97)
	if isNot == 1 {
		trace_util_0.Count(_points_00000, 99)
		// NOT FALSE range is {[-inf, 0), (0, +inf], [null, null]}
		startPoint1 := point{start: true}
		endPoint1 := point{excl: true}
		endPoint1.value.SetInt64(0)
		startPoint2 := point{start: true, excl: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{value: types.MaxValueDatum()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// FALSE range is {[0, 0]}
	trace_util_0.Count(_points_00000, 98)
	startPoint := point{start: true}
	startPoint.value.SetInt64(0)
	endPoint := point{}
	endPoint.value.SetInt64(0)
	return []point{startPoint, endPoint}
}

func (r *builder) buildFromIn(expr *expression.ScalarFunction) ([]point, bool) {
	trace_util_0.Count(_points_00000, 100)
	list := expr.GetArgs()[1:]
	rangePoints := make([]point, 0, len(list)*2)
	hasNull := false
	for _, e := range list {
		trace_util_0.Count(_points_00000, 105)
		v, ok := e.(*expression.Constant)
		if !ok {
			trace_util_0.Count(_points_00000, 109)
			r.err = ErrUnsupportedType.GenWithStack("expr:%v is not constant", e)
			return fullRange, hasNull
		}
		trace_util_0.Count(_points_00000, 106)
		dt, err := v.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_points_00000, 110)
			r.err = ErrUnsupportedType.GenWithStack("expr:%v is not evaluated", e)
			return fullRange, hasNull
		}
		trace_util_0.Count(_points_00000, 107)
		if dt.IsNull() {
			trace_util_0.Count(_points_00000, 111)
			hasNull = true
			continue
		}
		trace_util_0.Count(_points_00000, 108)
		startPoint := point{value: types.NewDatum(dt.GetValue()), start: true}
		endPoint := point{value: types.NewDatum(dt.GetValue())}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	trace_util_0.Count(_points_00000, 101)
	sorter := pointSorter{points: rangePoints, sc: r.sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		trace_util_0.Count(_points_00000, 112)
		r.err = sorter.err
	}
	// check and remove duplicates
	trace_util_0.Count(_points_00000, 102)
	curPos, frontPos := 0, 0
	for frontPos < len(rangePoints) {
		trace_util_0.Count(_points_00000, 113)
		if rangePoints[curPos].start == rangePoints[frontPos].start {
			trace_util_0.Count(_points_00000, 114)
			frontPos++
		} else {
			trace_util_0.Count(_points_00000, 115)
			{
				curPos++
				rangePoints[curPos] = rangePoints[frontPos]
				frontPos++
			}
		}
	}
	trace_util_0.Count(_points_00000, 103)
	if curPos > 0 {
		trace_util_0.Count(_points_00000, 116)
		curPos++
	}
	trace_util_0.Count(_points_00000, 104)
	return rangePoints[:curPos], hasNull
}

func (r *builder) newBuildFromPatternLike(expr *expression.ScalarFunction) []point {
	trace_util_0.Count(_points_00000, 117)
	pdt, err := expr.GetArgs()[1].(*expression.Constant).Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_points_00000, 126)
		r.err = errors.Trace(err)
		return fullRange
	}
	trace_util_0.Count(_points_00000, 118)
	pattern, err := pdt.ToString()
	if err != nil {
		trace_util_0.Count(_points_00000, 127)
		r.err = errors.Trace(err)
		return fullRange
	}
	trace_util_0.Count(_points_00000, 119)
	if pattern == "" {
		trace_util_0.Count(_points_00000, 128)
		startPoint := point{value: types.NewStringDatum(""), start: true}
		endPoint := point{value: types.NewStringDatum("")}
		return []point{startPoint, endPoint}
	}
	trace_util_0.Count(_points_00000, 120)
	lowValue := make([]byte, 0, len(pattern))
	edt, err := expr.GetArgs()[2].(*expression.Constant).Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_points_00000, 129)
		r.err = errors.Trace(err)
		return fullRange
	}
	trace_util_0.Count(_points_00000, 121)
	escape := byte(edt.GetInt64())
	var exclude bool
	isExactMatch := true
	for i := 0; i < len(pattern); i++ {
		trace_util_0.Count(_points_00000, 130)
		if pattern[i] == escape {
			trace_util_0.Count(_points_00000, 133)
			i++
			if i < len(pattern) {
				trace_util_0.Count(_points_00000, 135)
				lowValue = append(lowValue, pattern[i])
			} else {
				trace_util_0.Count(_points_00000, 136)
				{
					lowValue = append(lowValue, escape)
				}
			}
			trace_util_0.Count(_points_00000, 134)
			continue
		}
		trace_util_0.Count(_points_00000, 131)
		if pattern[i] == '%' {
			trace_util_0.Count(_points_00000, 137)
			// Get the prefix.
			isExactMatch = false
			break
		} else {
			trace_util_0.Count(_points_00000, 138)
			if pattern[i] == '_' {
				trace_util_0.Count(_points_00000, 139)
				// Get the prefix, but exclude the prefix.
				// e.g., "abc_x", the start point exclude "abc",
				// because the string length is more than 3.
				exclude = true
				isExactMatch = false
				break
			}
		}
		trace_util_0.Count(_points_00000, 132)
		lowValue = append(lowValue, pattern[i])
	}
	trace_util_0.Count(_points_00000, 122)
	if len(lowValue) == 0 {
		trace_util_0.Count(_points_00000, 140)
		return []point{{value: types.MinNotNullDatum(), start: true}, {value: types.MaxValueDatum()}}
	}
	trace_util_0.Count(_points_00000, 123)
	if isExactMatch {
		trace_util_0.Count(_points_00000, 141)
		val := types.NewStringDatum(string(lowValue))
		return []point{{value: val, start: true}, {value: val}}
	}
	trace_util_0.Count(_points_00000, 124)
	startPoint := point{start: true, excl: exclude}
	startPoint.value.SetBytesAsString(lowValue)
	highValue := make([]byte, len(lowValue))
	copy(highValue, lowValue)
	endPoint := point{excl: true}
	for i := len(highValue) - 1; i >= 0; i-- {
		trace_util_0.Count(_points_00000, 142)
		// Make the end point value more than the start point value,
		// and the length of the end point value is the same as the length of the start point value.
		// e.g., the start point value is "abc", so the end point value is "abd".
		highValue[i]++
		if highValue[i] != 0 {
			trace_util_0.Count(_points_00000, 144)
			endPoint.value.SetBytesAsString(highValue)
			break
		}
		// If highValue[i] is 255 and highValue[i]++ is 0, then the end point value is max value.
		trace_util_0.Count(_points_00000, 143)
		if i == 0 {
			trace_util_0.Count(_points_00000, 145)
			endPoint.value = types.MaxValueDatum()
		}
	}
	trace_util_0.Count(_points_00000, 125)
	return []point{startPoint, endPoint}
}

func (r *builder) buildFromNot(expr *expression.ScalarFunction) []point {
	trace_util_0.Count(_points_00000, 146)
	switch n := expr.FuncName.L; n {
	case ast.IsTruth:
		trace_util_0.Count(_points_00000, 148)
		return r.buildFromIsTrue(expr, 1)
	case ast.IsFalsity:
		trace_util_0.Count(_points_00000, 149)
		return r.buildFromIsFalse(expr, 1)
	case ast.In:
		trace_util_0.Count(_points_00000, 150)
		var (
			isUnsignedIntCol bool
			nonNegativePos   int
		)
		rangePoints, hasNull := r.buildFromIn(expr)
		if hasNull {
			trace_util_0.Count(_points_00000, 157)
			return nil
		}
		trace_util_0.Count(_points_00000, 151)
		if x, ok := expr.GetArgs()[0].(*expression.Column); ok {
			trace_util_0.Count(_points_00000, 158)
			isUnsignedIntCol = mysql.HasUnsignedFlag(x.RetType.Flag) && mysql.IsIntegerType(x.RetType.Tp)
		}
		// negative ranges can be directly ignored for unsigned int columns.
		trace_util_0.Count(_points_00000, 152)
		if isUnsignedIntCol {
			trace_util_0.Count(_points_00000, 159)
			for nonNegativePos = 0; nonNegativePos < len(rangePoints); nonNegativePos += 2 {
				trace_util_0.Count(_points_00000, 161)
				if rangePoints[nonNegativePos].value.Kind() == types.KindUint64 || rangePoints[nonNegativePos].value.GetInt64() >= 0 {
					trace_util_0.Count(_points_00000, 162)
					break
				}
			}
			trace_util_0.Count(_points_00000, 160)
			rangePoints = rangePoints[nonNegativePos:]
		}
		trace_util_0.Count(_points_00000, 153)
		retRangePoints := make([]point, 0, 2+len(rangePoints))
		previousValue := types.Datum{}
		for i := 0; i < len(rangePoints); i += 2 {
			trace_util_0.Count(_points_00000, 163)
			retRangePoints = append(retRangePoints, point{value: previousValue, start: true, excl: true})
			retRangePoints = append(retRangePoints, point{value: rangePoints[i].value, excl: true})
			previousValue = rangePoints[i].value
		}
		// Append the interval (last element, max value].
		trace_util_0.Count(_points_00000, 154)
		retRangePoints = append(retRangePoints, point{value: previousValue, start: true, excl: true})
		retRangePoints = append(retRangePoints, point{value: types.MaxValueDatum()})
		return retRangePoints
	case ast.Like:
		trace_util_0.Count(_points_00000, 155)
		// Pattern not like is not supported.
		r.err = ErrUnsupportedType.GenWithStack("NOT LIKE is not supported.")
		return fullRange
	case ast.IsNull:
		trace_util_0.Count(_points_00000, 156)
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	}
	trace_util_0.Count(_points_00000, 147)
	return nil
}

func (r *builder) buildFromScalarFunc(expr *expression.ScalarFunction) []point {
	trace_util_0.Count(_points_00000, 164)
	switch op := expr.FuncName.L; op {
	case ast.GE, ast.GT, ast.LT, ast.LE, ast.EQ, ast.NE:
		trace_util_0.Count(_points_00000, 166)
		return r.buildFormBinOp(expr)
	case ast.LogicAnd:
		trace_util_0.Count(_points_00000, 167)
		return r.intersection(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.LogicOr:
		trace_util_0.Count(_points_00000, 168)
		return r.union(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.IsTruth:
		trace_util_0.Count(_points_00000, 169)
		return r.buildFromIsTrue(expr, 0)
	case ast.IsFalsity:
		trace_util_0.Count(_points_00000, 170)
		return r.buildFromIsFalse(expr, 0)
	case ast.In:
		trace_util_0.Count(_points_00000, 171)
		retPoints, _ := r.buildFromIn(expr)
		return retPoints
	case ast.Like:
		trace_util_0.Count(_points_00000, 172)
		return r.newBuildFromPatternLike(expr)
	case ast.IsNull:
		trace_util_0.Count(_points_00000, 173)
		startPoint := point{start: true}
		endPoint := point{}
		return []point{startPoint, endPoint}
	case ast.UnaryNot:
		trace_util_0.Count(_points_00000, 174)
		return r.buildFromNot(expr.GetArgs()[0].(*expression.ScalarFunction))
	}

	trace_util_0.Count(_points_00000, 165)
	return nil
}

func (r *builder) intersection(a, b []point) []point {
	trace_util_0.Count(_points_00000, 175)
	return r.merge(a, b, false)
}

func (r *builder) union(a, b []point) []point {
	trace_util_0.Count(_points_00000, 176)
	return r.merge(a, b, true)
}

func (r *builder) merge(a, b []point, union bool) []point {
	trace_util_0.Count(_points_00000, 177)
	sorter := pointSorter{points: append(a, b...), sc: r.sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		trace_util_0.Count(_points_00000, 181)
		r.err = sorter.err
		return nil
	}
	trace_util_0.Count(_points_00000, 178)
	var (
		inRangeCount         int
		requiredInRangeCount int
	)
	if union {
		trace_util_0.Count(_points_00000, 182)
		requiredInRangeCount = 1
	} else {
		trace_util_0.Count(_points_00000, 183)
		{
			requiredInRangeCount = 2
		}
	}
	trace_util_0.Count(_points_00000, 179)
	merged := make([]point, 0, len(sorter.points))
	for _, val := range sorter.points {
		trace_util_0.Count(_points_00000, 184)
		if val.start {
			trace_util_0.Count(_points_00000, 185)
			inRangeCount++
			if inRangeCount == requiredInRangeCount {
				trace_util_0.Count(_points_00000, 186)
				// just reached the required in range count, a new range started.
				merged = append(merged, val)
			}
		} else {
			trace_util_0.Count(_points_00000, 187)
			{
				if inRangeCount == requiredInRangeCount {
					trace_util_0.Count(_points_00000, 189)
					// just about to leave the required in range count, the range is ended.
					merged = append(merged, val)
				}
				trace_util_0.Count(_points_00000, 188)
				inRangeCount--
			}
		}
	}
	trace_util_0.Count(_points_00000, 180)
	return merged
}

var _points_00000 = "util/ranger/points.go"
