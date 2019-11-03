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

package aggfuncs

import (
	"fmt"
	"strconv"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Build is used to build a specific AggFunc implementation according to the
// input aggFuncDesc.
func Build(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 0)
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		trace_util_0.Count(_builder_00000, 2)
		return buildCount(aggFuncDesc, ordinal)
	case ast.AggFuncSum:
		trace_util_0.Count(_builder_00000, 3)
		return buildSum(aggFuncDesc, ordinal)
	case ast.AggFuncAvg:
		trace_util_0.Count(_builder_00000, 4)
		return buildAvg(aggFuncDesc, ordinal)
	case ast.AggFuncFirstRow:
		trace_util_0.Count(_builder_00000, 5)
		return buildFirstRow(aggFuncDesc, ordinal)
	case ast.AggFuncMax:
		trace_util_0.Count(_builder_00000, 6)
		return buildMaxMin(aggFuncDesc, ordinal, true)
	case ast.AggFuncMin:
		trace_util_0.Count(_builder_00000, 7)
		return buildMaxMin(aggFuncDesc, ordinal, false)
	case ast.AggFuncGroupConcat:
		trace_util_0.Count(_builder_00000, 8)
		return buildGroupConcat(ctx, aggFuncDesc, ordinal)
	case ast.AggFuncBitOr:
		trace_util_0.Count(_builder_00000, 9)
		return buildBitOr(aggFuncDesc, ordinal)
	case ast.AggFuncBitXor:
		trace_util_0.Count(_builder_00000, 10)
		return buildBitXor(aggFuncDesc, ordinal)
	case ast.AggFuncBitAnd:
		trace_util_0.Count(_builder_00000, 11)
		return buildBitAnd(aggFuncDesc, ordinal)
	}
	trace_util_0.Count(_builder_00000, 1)
	return nil
}

// BuildWindowFunctions builds specific window function according to function description and order by columns.
func BuildWindowFunctions(ctx sessionctx.Context, windowFuncDesc *aggregation.AggFuncDesc, ordinal int, orderByCols []*expression.Column) AggFunc {
	trace_util_0.Count(_builder_00000, 12)
	switch windowFuncDesc.Name {
	case ast.WindowFuncRank:
		trace_util_0.Count(_builder_00000, 13)
		return buildRank(ordinal, orderByCols, false)
	case ast.WindowFuncDenseRank:
		trace_util_0.Count(_builder_00000, 14)
		return buildRank(ordinal, orderByCols, true)
	case ast.WindowFuncRowNumber:
		trace_util_0.Count(_builder_00000, 15)
		return buildRowNumber(windowFuncDesc, ordinal)
	case ast.WindowFuncFirstValue:
		trace_util_0.Count(_builder_00000, 16)
		return buildFirstValue(windowFuncDesc, ordinal)
	case ast.WindowFuncLastValue:
		trace_util_0.Count(_builder_00000, 17)
		return buildLastValue(windowFuncDesc, ordinal)
	case ast.WindowFuncCumeDist:
		trace_util_0.Count(_builder_00000, 18)
		return buildCumeDist(ordinal, orderByCols)
	case ast.WindowFuncNthValue:
		trace_util_0.Count(_builder_00000, 19)
		return buildNthValue(windowFuncDesc, ordinal)
	case ast.WindowFuncNtile:
		trace_util_0.Count(_builder_00000, 20)
		return buildNtile(windowFuncDesc, ordinal)
	case ast.WindowFuncPercentRank:
		trace_util_0.Count(_builder_00000, 21)
		return buildPercenRank(ordinal, orderByCols)
	case ast.WindowFuncLead:
		trace_util_0.Count(_builder_00000, 22)
		return buildLead(windowFuncDesc, ordinal)
	case ast.WindowFuncLag:
		trace_util_0.Count(_builder_00000, 23)
		return buildLag(windowFuncDesc, ordinal)
	default:
		trace_util_0.Count(_builder_00000, 24)
		return Build(ctx, windowFuncDesc, ordinal)
	}
}

// buildCount builds the AggFunc implementation for function "COUNT".
func buildCount(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 25)
	// If mode is DedupMode, we return nil for not implemented.
	if aggFuncDesc.Mode == aggregation.DedupMode {
		trace_util_0.Count(_builder_00000, 29)
		return nil // not implemented yet.
	}

	trace_util_0.Count(_builder_00000, 26)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}

	// If HasDistinct and mode is CompleteMode or Partial1Mode, we should
	// use countOriginalWithDistinct.
	if aggFuncDesc.HasDistinct &&
		(aggFuncDesc.Mode == aggregation.CompleteMode || aggFuncDesc.Mode == aggregation.Partial1Mode) {
		trace_util_0.Count(_builder_00000, 30)
		return &countOriginalWithDistinct{baseCount{base}}
	}

	trace_util_0.Count(_builder_00000, 27)
	switch aggFuncDesc.Mode {
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		trace_util_0.Count(_builder_00000, 31)
		switch aggFuncDesc.Args[0].GetType().EvalType() {
		case types.ETInt:
			trace_util_0.Count(_builder_00000, 33)
			return &countOriginal4Int{baseCount{base}}
		case types.ETReal:
			trace_util_0.Count(_builder_00000, 34)
			return &countOriginal4Real{baseCount{base}}
		case types.ETDecimal:
			trace_util_0.Count(_builder_00000, 35)
			return &countOriginal4Decimal{baseCount{base}}
		case types.ETTimestamp, types.ETDatetime:
			trace_util_0.Count(_builder_00000, 36)
			return &countOriginal4Time{baseCount{base}}
		case types.ETDuration:
			trace_util_0.Count(_builder_00000, 37)
			return &countOriginal4Duration{baseCount{base}}
		case types.ETJson:
			trace_util_0.Count(_builder_00000, 38)
			return &countOriginal4JSON{baseCount{base}}
		case types.ETString:
			trace_util_0.Count(_builder_00000, 39)
			return &countOriginal4String{baseCount{base}}
		}
	case aggregation.Partial2Mode, aggregation.FinalMode:
		trace_util_0.Count(_builder_00000, 32)
		return &countPartial{baseCount{base}}
	}

	trace_util_0.Count(_builder_00000, 28)
	return nil
}

// buildSum builds the AggFunc implementation for function "SUM".
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 40)
	base := baseSumAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		trace_util_0.Count(_builder_00000, 41)
		return nil
	default:
		trace_util_0.Count(_builder_00000, 42)
		switch aggFuncDesc.RetTp.EvalType() {
		case types.ETDecimal:
			trace_util_0.Count(_builder_00000, 43)
			if aggFuncDesc.HasDistinct {
				trace_util_0.Count(_builder_00000, 47)
				return &sum4DistinctDecimal{base}
			}
			trace_util_0.Count(_builder_00000, 44)
			return &sum4Decimal{base}
		default:
			trace_util_0.Count(_builder_00000, 45)
			if aggFuncDesc.HasDistinct {
				trace_util_0.Count(_builder_00000, 48)
				return &sum4DistinctFloat64{base}
			}
			trace_util_0.Count(_builder_00000, 46)
			return &sum4Float64{base}
		}
	}
}

// buildAvg builds the AggFunc implementation for function "AVG".
func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 49)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	switch aggFuncDesc.Mode {
	// Build avg functions which consume the original data and remove the
	// duplicated input of the same group.
	case aggregation.DedupMode:
		trace_util_0.Count(_builder_00000, 51)
		return nil // not implemented yet.

	// Build avg functions which consume the original data and update their
	// partial results.
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		trace_util_0.Count(_builder_00000, 52)
		switch aggFuncDesc.RetTp.EvalType() {
		case types.ETDecimal:
			trace_util_0.Count(_builder_00000, 54)
			if aggFuncDesc.HasDistinct {
				trace_util_0.Count(_builder_00000, 58)
				return &avgOriginal4DistinctDecimal{base}
			}
			trace_util_0.Count(_builder_00000, 55)
			return &avgOriginal4Decimal{baseAvgDecimal{base}}
		default:
			trace_util_0.Count(_builder_00000, 56)
			if aggFuncDesc.HasDistinct {
				trace_util_0.Count(_builder_00000, 59)
				return &avgOriginal4DistinctFloat64{base}
			}
			trace_util_0.Count(_builder_00000, 57)
			return &avgOriginal4Float64{baseAvgFloat64{base}}
		}

	// Build avg functions which consume the partial result of other avg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		trace_util_0.Count(_builder_00000, 53)
		switch aggFuncDesc.RetTp.Tp {
		case mysql.TypeNewDecimal:
			trace_util_0.Count(_builder_00000, 60)
			return &avgPartial4Decimal{baseAvgDecimal{base}}
		case mysql.TypeDouble:
			trace_util_0.Count(_builder_00000, 61)
			return &avgPartial4Float64{baseAvgFloat64{base}}
		}
	}
	trace_util_0.Count(_builder_00000, 50)
	return nil
}

// buildFirstRow builds the AggFunc implementation for function "FIRST_ROW".
func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 62)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		trace_util_0.Count(_builder_00000, 65)
		evalType = types.ETString
	}
	trace_util_0.Count(_builder_00000, 63)
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		trace_util_0.Count(_builder_00000, 66)
	default:
		trace_util_0.Count(_builder_00000, 67)
		switch evalType {
		case types.ETInt:
			trace_util_0.Count(_builder_00000, 68)
			return &firstRow4Int{base}
		case types.ETReal:
			trace_util_0.Count(_builder_00000, 69)
			switch fieldType.Tp {
			case mysql.TypeFloat:
				trace_util_0.Count(_builder_00000, 75)
				return &firstRow4Float32{base}
			case mysql.TypeDouble:
				trace_util_0.Count(_builder_00000, 76)
				return &firstRow4Float64{base}
			}
		case types.ETDecimal:
			trace_util_0.Count(_builder_00000, 70)
			return &firstRow4Decimal{base}
		case types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_builder_00000, 71)
			return &firstRow4Time{base}
		case types.ETDuration:
			trace_util_0.Count(_builder_00000, 72)
			return &firstRow4Duration{base}
		case types.ETString:
			trace_util_0.Count(_builder_00000, 73)
			return &firstRow4String{base}
		case types.ETJson:
			trace_util_0.Count(_builder_00000, 74)
			return &firstRow4JSON{base}
		}
	}
	trace_util_0.Count(_builder_00000, 64)
	return nil
}

// buildMaxMin builds the AggFunc implementation for function "MAX" and "MIN".
func buildMaxMin(aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	trace_util_0.Count(_builder_00000, 77)
	base := baseMaxMinAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
		isMax: isMax,
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		trace_util_0.Count(_builder_00000, 80)
		evalType = types.ETString
	}
	trace_util_0.Count(_builder_00000, 78)
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		trace_util_0.Count(_builder_00000, 81)
	default:
		trace_util_0.Count(_builder_00000, 82)
		switch evalType {
		case types.ETInt:
			trace_util_0.Count(_builder_00000, 83)
			if mysql.HasUnsignedFlag(fieldType.Flag) {
				trace_util_0.Count(_builder_00000, 91)
				return &maxMin4Uint{base}
			}
			trace_util_0.Count(_builder_00000, 84)
			return &maxMin4Int{base}
		case types.ETReal:
			trace_util_0.Count(_builder_00000, 85)
			switch fieldType.Tp {
			case mysql.TypeFloat:
				trace_util_0.Count(_builder_00000, 92)
				return &maxMin4Float32{base}
			case mysql.TypeDouble:
				trace_util_0.Count(_builder_00000, 93)
				return &maxMin4Float64{base}
			}
		case types.ETDecimal:
			trace_util_0.Count(_builder_00000, 86)
			return &maxMin4Decimal{base}
		case types.ETString:
			trace_util_0.Count(_builder_00000, 87)
			return &maxMin4String{base}
		case types.ETDatetime, types.ETTimestamp:
			trace_util_0.Count(_builder_00000, 88)
			return &maxMin4Time{base}
		case types.ETDuration:
			trace_util_0.Count(_builder_00000, 89)
			return &maxMin4Duration{base}
		case types.ETJson:
			trace_util_0.Count(_builder_00000, 90)
			return &maxMin4JSON{base}
		}
	}
	trace_util_0.Count(_builder_00000, 79)
	return nil
}

// buildGroupConcat builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 94)
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		trace_util_0.Count(_builder_00000, 95)
		return nil
	default:
		trace_util_0.Count(_builder_00000, 96)
		base := baseAggFunc{
			args:    aggFuncDesc.Args[:len(aggFuncDesc.Args)-1],
			ordinal: ordinal,
		}
		// The last arg is promised to be a not-null string constant, so the error can be ignored.
		c, _ := aggFuncDesc.Args[len(aggFuncDesc.Args)-1].(*expression.Constant)
		sep, _, err := c.EvalString(nil, chunk.Row{})
		// This err should never happen.
		if err != nil {
			trace_util_0.Count(_builder_00000, 101)
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", err.Error()))
		}
		trace_util_0.Count(_builder_00000, 97)
		var s string
		s, err = variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			trace_util_0.Count(_builder_00000, 102)
			panic(fmt.Sprintf("Error happened when buildGroupConcat: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		trace_util_0.Count(_builder_00000, 98)
		maxLen, err := strconv.ParseUint(s, 10, 64)
		// Should never happen
		if err != nil {
			trace_util_0.Count(_builder_00000, 103)
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", err.Error()))
		}
		trace_util_0.Count(_builder_00000, 99)
		var truncated int32
		if aggFuncDesc.HasDistinct {
			trace_util_0.Count(_builder_00000, 104)
			return &groupConcatDistinct{baseGroupConcat4String{baseAggFunc: base, sep: sep, maxLen: maxLen, truncated: &truncated}}
		}
		trace_util_0.Count(_builder_00000, 100)
		return &groupConcat{baseGroupConcat4String{baseAggFunc: base, sep: sep, maxLen: maxLen, truncated: &truncated}}
	}
}

// buildBitOr builds the AggFunc implementation for function "BIT_OR".
func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 105)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitOrUint64{baseBitAggFunc{base}}
}

// buildBitXor builds the AggFunc implementation for function "BIT_XOR".
func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 106)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitXorUint64{baseBitAggFunc{base}}
}

// buildBitAnd builds the AggFunc implementation for function "BIT_AND".
func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 107)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitAndUint64{baseBitAggFunc{base}}
}

// buildRowNumber builds the AggFunc implementation for function "ROW_NUMBER".
func buildRowNumber(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 108)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &rowNumber{base}
}

func buildRank(ordinal int, orderByCols []*expression.Column, isDense bool) AggFunc {
	trace_util_0.Count(_builder_00000, 109)
	base := baseAggFunc{
		ordinal: ordinal,
	}
	r := &rank{baseAggFunc: base, isDense: isDense, rowComparer: buildRowComparer(orderByCols)}
	return r
}

func buildFirstValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 110)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &firstValue{baseAggFunc: base, tp: aggFuncDesc.RetTp}
}

func buildLastValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 111)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &lastValue{baseAggFunc: base, tp: aggFuncDesc.RetTp}
}

func buildCumeDist(ordinal int, orderByCols []*expression.Column) AggFunc {
	trace_util_0.Count(_builder_00000, 112)
	base := baseAggFunc{
		ordinal: ordinal,
	}
	r := &cumeDist{baseAggFunc: base, rowComparer: buildRowComparer(orderByCols)}
	return r
}

func buildNthValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 113)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	// Already checked when building the function description.
	nth, _, _ := expression.GetUint64FromConstant(aggFuncDesc.Args[1])
	return &nthValue{baseAggFunc: base, tp: aggFuncDesc.RetTp, nth: nth}
}

func buildNtile(aggFuncDes *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 114)
	base := baseAggFunc{
		args:    aggFuncDes.Args,
		ordinal: ordinal,
	}
	n, _, _ := expression.GetUint64FromConstant(aggFuncDes.Args[0])
	return &ntile{baseAggFunc: base, n: n}
}

func buildPercenRank(ordinal int, orderByCols []*expression.Column) AggFunc {
	trace_util_0.Count(_builder_00000, 115)
	base := baseAggFunc{
		ordinal: ordinal,
	}
	return &percentRank{baseAggFunc: base, rowComparer: buildRowComparer(orderByCols)}
}

func buildLeadLag(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) baseLeadLag {
	trace_util_0.Count(_builder_00000, 116)
	offset := uint64(1)
	if len(aggFuncDesc.Args) >= 2 {
		trace_util_0.Count(_builder_00000, 119)
		offset, _, _ = expression.GetUint64FromConstant(aggFuncDesc.Args[1])
	}
	trace_util_0.Count(_builder_00000, 117)
	var defaultExpr expression.Expression
	defaultExpr = expression.Null
	if len(aggFuncDesc.Args) == 3 {
		trace_util_0.Count(_builder_00000, 120)
		defaultExpr = aggFuncDesc.Args[2]
	}
	trace_util_0.Count(_builder_00000, 118)
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return baseLeadLag{baseAggFunc: base, offset: offset, defaultExpr: defaultExpr, valueEvaluator: buildValueEvaluator(aggFuncDesc.RetTp)}
}

func buildLead(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 121)
	return &lead{buildLeadLag(aggFuncDesc, ordinal)}
}

func buildLag(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	trace_util_0.Count(_builder_00000, 122)
	return &lag{buildLeadLag(aggFuncDesc, ordinal)}
}

var _builder_00000 = "executor/aggfuncs/builder.go"
