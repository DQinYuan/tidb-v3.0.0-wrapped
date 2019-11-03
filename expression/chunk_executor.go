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

package expression

import (
	"strconv"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Vectorizable checks whether a list of expressions can employ vectorized execution.
func Vectorizable(exprs []Expression) bool {
	trace_util_0.Count(_chunk_executor_00000, 0)
	for _, expr := range exprs {
		trace_util_0.Count(_chunk_executor_00000, 2)
		if HasGetSetVarFunc(expr) {
			trace_util_0.Count(_chunk_executor_00000, 3)
			return false
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 1)
	return true
}

// HasGetSetVarFunc checks whether an expression contains SetVar/GetVar function.
func HasGetSetVarFunc(expr Expression) bool {
	trace_util_0.Count(_chunk_executor_00000, 4)
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		trace_util_0.Count(_chunk_executor_00000, 9)
		return false
	}
	trace_util_0.Count(_chunk_executor_00000, 5)
	if scalaFunc.FuncName.L == ast.SetVar {
		trace_util_0.Count(_chunk_executor_00000, 10)
		return true
	}
	trace_util_0.Count(_chunk_executor_00000, 6)
	if scalaFunc.FuncName.L == ast.GetVar {
		trace_util_0.Count(_chunk_executor_00000, 11)
		return true
	}
	trace_util_0.Count(_chunk_executor_00000, 7)
	for _, arg := range scalaFunc.GetArgs() {
		trace_util_0.Count(_chunk_executor_00000, 12)
		if HasGetSetVarFunc(arg) {
			trace_util_0.Count(_chunk_executor_00000, 13)
			return true
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 8)
	return false
}

// VectorizedExecute evaluates a list of expressions column by column and append their results to "output" Chunk.
func VectorizedExecute(ctx sessionctx.Context, exprs []Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk) error {
	trace_util_0.Count(_chunk_executor_00000, 14)
	for colID, expr := range exprs {
		trace_util_0.Count(_chunk_executor_00000, 16)
		err := evalOneColumn(ctx, expr, iterator, output, colID)
		if err != nil {
			trace_util_0.Count(_chunk_executor_00000, 17)
			return err
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 15)
	return nil
}

func evalOneColumn(ctx sessionctx.Context, expr Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk, colID int) (err error) {
	trace_util_0.Count(_chunk_executor_00000, 18)
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		trace_util_0.Count(_chunk_executor_00000, 20)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 27)
			err = executeToInt(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETReal:
		trace_util_0.Count(_chunk_executor_00000, 21)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 28)
			err = executeToReal(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDecimal:
		trace_util_0.Count(_chunk_executor_00000, 22)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 29)
			err = executeToDecimal(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_chunk_executor_00000, 23)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 30)
			err = executeToDatetime(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDuration:
		trace_util_0.Count(_chunk_executor_00000, 24)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 31)
			err = executeToDuration(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETJson:
		trace_util_0.Count(_chunk_executor_00000, 25)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 32)
			err = executeToJSON(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETString:
		trace_util_0.Count(_chunk_executor_00000, 26)
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 33)
			err = executeToString(ctx, expr, fieldType, row, output, colID)
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 19)
	return err
}

func evalOneCell(ctx sessionctx.Context, expr Expression, row chunk.Row, output *chunk.Chunk, colID int) (err error) {
	trace_util_0.Count(_chunk_executor_00000, 34)
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.ETInt:
		trace_util_0.Count(_chunk_executor_00000, 36)
		err = executeToInt(ctx, expr, fieldType, row, output, colID)
	case types.ETReal:
		trace_util_0.Count(_chunk_executor_00000, 37)
		err = executeToReal(ctx, expr, fieldType, row, output, colID)
	case types.ETDecimal:
		trace_util_0.Count(_chunk_executor_00000, 38)
		err = executeToDecimal(ctx, expr, fieldType, row, output, colID)
	case types.ETDatetime, types.ETTimestamp:
		trace_util_0.Count(_chunk_executor_00000, 39)
		err = executeToDatetime(ctx, expr, fieldType, row, output, colID)
	case types.ETDuration:
		trace_util_0.Count(_chunk_executor_00000, 40)
		err = executeToDuration(ctx, expr, fieldType, row, output, colID)
	case types.ETJson:
		trace_util_0.Count(_chunk_executor_00000, 41)
		err = executeToJSON(ctx, expr, fieldType, row, output, colID)
	case types.ETString:
		trace_util_0.Count(_chunk_executor_00000, 42)
		err = executeToString(ctx, expr, fieldType, row, output, colID)
	}
	trace_util_0.Count(_chunk_executor_00000, 35)
	return err
}

func executeToInt(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 43)
	res, isNull, err := expr.EvalInt(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 48)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 44)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 49)
		output.AppendNull(colID)
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 45)
	if fieldType.Tp == mysql.TypeBit {
		trace_util_0.Count(_chunk_executor_00000, 50)
		output.AppendBytes(colID, strconv.AppendUint(make([]byte, 0, 8), uint64(res), 10))
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 46)
	if mysql.HasUnsignedFlag(fieldType.Flag) {
		trace_util_0.Count(_chunk_executor_00000, 51)
		output.AppendUint64(colID, uint64(res))
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 47)
	output.AppendInt64(colID, res)
	return nil
}

func executeToReal(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 52)
	res, isNull, err := expr.EvalReal(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 56)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 53)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 57)
		output.AppendNull(colID)
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 54)
	if fieldType.Tp == mysql.TypeFloat {
		trace_util_0.Count(_chunk_executor_00000, 58)
		output.AppendFloat32(colID, float32(res))
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 55)
	output.AppendFloat64(colID, res)
	return nil
}

func executeToDecimal(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 59)
	res, isNull, err := expr.EvalDecimal(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 62)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 60)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 63)
		output.AppendNull(colID)
		return nil
	}
	trace_util_0.Count(_chunk_executor_00000, 61)
	output.AppendMyDecimal(colID, res)
	return nil
}

func executeToDatetime(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 64)
	res, isNull, err := expr.EvalTime(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 67)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 65)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 68)
		output.AppendNull(colID)
	} else {
		trace_util_0.Count(_chunk_executor_00000, 69)
		{
			output.AppendTime(colID, res)
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 66)
	return nil
}

func executeToDuration(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 70)
	res, isNull, err := expr.EvalDuration(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 73)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 71)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 74)
		output.AppendNull(colID)
	} else {
		trace_util_0.Count(_chunk_executor_00000, 75)
		{
			output.AppendDuration(colID, res)
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 72)
	return nil
}

func executeToJSON(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 76)
	res, isNull, err := expr.EvalJSON(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 79)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 77)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 80)
		output.AppendNull(colID)
	} else {
		trace_util_0.Count(_chunk_executor_00000, 81)
		{
			output.AppendJSON(colID, res)
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 78)
	return nil
}

func executeToString(ctx sessionctx.Context, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	trace_util_0.Count(_chunk_executor_00000, 82)
	res, isNull, err := expr.EvalString(ctx, row)
	if err != nil {
		trace_util_0.Count(_chunk_executor_00000, 85)
		return err
	}
	trace_util_0.Count(_chunk_executor_00000, 83)
	if isNull {
		trace_util_0.Count(_chunk_executor_00000, 86)
		output.AppendNull(colID)
	} else {
		trace_util_0.Count(_chunk_executor_00000, 87)
		if fieldType.Tp == mysql.TypeEnum {
			trace_util_0.Count(_chunk_executor_00000, 88)
			val := types.Enum{Value: uint64(0), Name: res}
			output.AppendEnum(colID, val)
		} else {
			trace_util_0.Count(_chunk_executor_00000, 89)
			if fieldType.Tp == mysql.TypeSet {
				trace_util_0.Count(_chunk_executor_00000, 90)
				val := types.Set{Value: uint64(0), Name: res}
				output.AppendSet(colID, val)
			} else {
				trace_util_0.Count(_chunk_executor_00000, 91)
				{
					output.AppendString(colID, res)
				}
			}
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 84)
	return nil
}

// VectorizedFilter applies a list of filters to a Chunk and
// returns a bool slice, which indicates whether a row is passed the filters.
// Filters is executed vectorized.
func VectorizedFilter(ctx sessionctx.Context, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool) ([]bool, error) {
	trace_util_0.Count(_chunk_executor_00000, 92)
	selected = selected[:0]
	for i, numRows := 0, iterator.Len(); i < numRows; i++ {
		trace_util_0.Count(_chunk_executor_00000, 95)
		selected = append(selected, true)
	}
	trace_util_0.Count(_chunk_executor_00000, 93)
	for _, filter := range filters {
		trace_util_0.Count(_chunk_executor_00000, 96)
		isIntType := true
		if filter.GetType().EvalType() != types.ETInt {
			trace_util_0.Count(_chunk_executor_00000, 98)
			isIntType = false
		}
		trace_util_0.Count(_chunk_executor_00000, 97)
		for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
			trace_util_0.Count(_chunk_executor_00000, 99)
			if !selected[row.Idx()] {
				trace_util_0.Count(_chunk_executor_00000, 101)
				continue
			}
			trace_util_0.Count(_chunk_executor_00000, 100)
			if isIntType {
				trace_util_0.Count(_chunk_executor_00000, 102)
				filterResult, isNull, err := filter.EvalInt(ctx, row)
				if err != nil {
					trace_util_0.Count(_chunk_executor_00000, 104)
					return nil, err
				}
				trace_util_0.Count(_chunk_executor_00000, 103)
				selected[row.Idx()] = selected[row.Idx()] && !isNull && (filterResult != 0)
			} else {
				trace_util_0.Count(_chunk_executor_00000, 105)
				{
					// TODO: should rewrite the filter to `cast(expr as SIGNED) != 0` and always use `EvalInt`.
					bVal, _, err := EvalBool(ctx, []Expression{filter}, row)
					if err != nil {
						trace_util_0.Count(_chunk_executor_00000, 107)
						return nil, err
					}
					trace_util_0.Count(_chunk_executor_00000, 106)
					selected[row.Idx()] = selected[row.Idx()] && bVal
				}
			}
		}
	}
	trace_util_0.Count(_chunk_executor_00000, 94)
	return selected, nil
}

var _chunk_executor_00000 = "expression/chunk_executor.go"
