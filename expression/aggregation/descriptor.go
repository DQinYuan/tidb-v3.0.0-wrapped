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

package aggregation

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression, hasDistinct bool) (*AggFuncDesc, error) {
	trace_util_0.Count(_descriptor_00000, 0)
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		trace_util_0.Count(_descriptor_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_descriptor_00000, 1)
	return &AggFuncDesc{baseFuncDesc: b, HasDistinct: hasDistinct}, nil
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx sessionctx.Context, other *AggFuncDesc) bool {
	trace_util_0.Count(_descriptor_00000, 3)
	if a.HasDistinct != other.HasDistinct {
		trace_util_0.Count(_descriptor_00000, 5)
		return false
	}
	trace_util_0.Count(_descriptor_00000, 4)
	return a.baseFuncDesc.equal(ctx, &other.baseFuncDesc)
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	trace_util_0.Count(_descriptor_00000, 6)
	clone := *a
	clone.baseFuncDesc = *a.baseFuncDesc.clone()
	return &clone
}

// Split splits `a` into two aggregate descriptors for partial phase and
// final phase individually.
// This function is only used when executing aggregate function parallelly.
// ordinal indicates the column ordinal of the intermediate result.
func (a *AggFuncDesc) Split(ordinal []int) (partialAggDesc, finalAggDesc *AggFuncDesc) {
	trace_util_0.Count(_descriptor_00000, 7)
	partialAggDesc = a.Clone()
	if a.Mode == CompleteMode {
		trace_util_0.Count(_descriptor_00000, 10)
		partialAggDesc.Mode = Partial1Mode
	} else {
		trace_util_0.Count(_descriptor_00000, 11)
		if a.Mode == FinalMode {
			trace_util_0.Count(_descriptor_00000, 12)
			partialAggDesc.Mode = Partial2Mode
		} else {
			trace_util_0.Count(_descriptor_00000, 13)
			{
				panic("Error happened during AggFuncDesc.Split, the AggFunctionMode is not CompleteMode or FinalMode.")
			}
		}
	}
	trace_util_0.Count(_descriptor_00000, 8)
	finalAggDesc = &AggFuncDesc{
		Mode:        FinalMode, // We only support FinalMode now in final phase.
		HasDistinct: a.HasDistinct,
	}
	finalAggDesc.Name = a.Name
	finalAggDesc.RetTp = a.RetTp
	switch a.Name {
	case ast.AggFuncAvg:
		trace_util_0.Count(_descriptor_00000, 14)
		args := make([]expression.Expression, 0, 2)
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("avg_final_col_%d", ordinal[0])),
			Index:   ordinal[0],
			RetType: types.NewFieldType(mysql.TypeLonglong),
		})
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("avg_final_col_%d", ordinal[1])),
			Index:   ordinal[1],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	default:
		trace_util_0.Count(_descriptor_00000, 15)
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("%s_final_col_%d", a.Name, ordinal[0])),
			Index:   ordinal[0],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
		if finalAggDesc.Name == ast.AggFuncGroupConcat {
			trace_util_0.Count(_descriptor_00000, 16)
			finalAggDesc.Args = append(finalAggDesc.Args, a.Args[len(a.Args)-1]) // separator
		}
	}
	trace_util_0.Count(_descriptor_00000, 9)
	return
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching row for the inner table of an outer join,
// an aggregation function only involves constant and/or columns belongs to the inner table
// will be set to the null value.
// The input stands for the schema of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Table t with only one row:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Table s which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	trace_util_0.Count(_descriptor_00000, 17)
	switch a.Name {
	case ast.AggFuncCount:
		trace_util_0.Count(_descriptor_00000, 18)
		return a.evalNullValueInOuterJoin4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstRow:
		trace_util_0.Count(_descriptor_00000, 19)
		return a.evalNullValueInOuterJoin4Sum(ctx, schema)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		trace_util_0.Count(_descriptor_00000, 20)
		return types.Datum{}, false
	case ast.AggFuncBitAnd:
		trace_util_0.Count(_descriptor_00000, 21)
		return a.evalNullValueInOuterJoin4BitAnd(ctx, schema)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		trace_util_0.Count(_descriptor_00000, 22)
		return a.evalNullValueInOuterJoin4BitOr(ctx, schema)
	default:
		trace_util_0.Count(_descriptor_00000, 23)
		panic("unsupported agg function")
	}
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx sessionctx.Context) Aggregation {
	trace_util_0.Count(_descriptor_00000, 24)
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		trace_util_0.Count(_descriptor_00000, 25)
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		trace_util_0.Count(_descriptor_00000, 26)
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		trace_util_0.Count(_descriptor_00000, 27)
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncGroupConcat:
		trace_util_0.Count(_descriptor_00000, 28)
		var s string
		var err error
		var maxLen uint64
		s, err = variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			trace_util_0.Count(_descriptor_00000, 38)
			panic(fmt.Sprintf("Error happened when GetAggFunc: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		trace_util_0.Count(_descriptor_00000, 29)
		maxLen, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			trace_util_0.Count(_descriptor_00000, 39)
			panic(fmt.Sprintf("Error happened when GetAggFunc: illegal value for system variable named '%s'", variable.GroupConcatMaxLen))
		}
		trace_util_0.Count(_descriptor_00000, 30)
		return &concatFunction{aggFunction: aggFunc, maxLen: maxLen}
	case ast.AggFuncMax:
		trace_util_0.Count(_descriptor_00000, 31)
		return &maxMinFunction{aggFunction: aggFunc, isMax: true}
	case ast.AggFuncMin:
		trace_util_0.Count(_descriptor_00000, 32)
		return &maxMinFunction{aggFunction: aggFunc, isMax: false}
	case ast.AggFuncFirstRow:
		trace_util_0.Count(_descriptor_00000, 33)
		return &firstRowFunction{aggFunction: aggFunc}
	case ast.AggFuncBitOr:
		trace_util_0.Count(_descriptor_00000, 34)
		return &bitOrFunction{aggFunction: aggFunc}
	case ast.AggFuncBitXor:
		trace_util_0.Count(_descriptor_00000, 35)
		return &bitXorFunction{aggFunction: aggFunc}
	case ast.AggFuncBitAnd:
		trace_util_0.Count(_descriptor_00000, 36)
		return &bitAndFunction{aggFunction: aggFunc}
	default:
		trace_util_0.Count(_descriptor_00000, 37)
		panic("unsupported agg function")
	}
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	trace_util_0.Count(_descriptor_00000, 40)
	for _, arg := range a.Args {
		trace_util_0.Count(_descriptor_00000, 42)
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			trace_util_0.Count(_descriptor_00000, 43)
			return types.Datum{}, ok
		}
	}
	trace_util_0.Count(_descriptor_00000, 41)
	return types.NewDatum(1), true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	trace_util_0.Count(_descriptor_00000, 44)
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		trace_util_0.Count(_descriptor_00000, 46)
		return types.Datum{}, ok
	}
	trace_util_0.Count(_descriptor_00000, 45)
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitAnd(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	trace_util_0.Count(_descriptor_00000, 47)
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		trace_util_0.Count(_descriptor_00000, 49)
		return types.NewDatum(uint64(math.MaxUint64)), true
	}
	trace_util_0.Count(_descriptor_00000, 48)
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitOr(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	trace_util_0.Count(_descriptor_00000, 50)
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		trace_util_0.Count(_descriptor_00000, 52)
		return types.NewDatum(0), true
	}
	trace_util_0.Count(_descriptor_00000, 51)
	return con.Value, true
}

var _descriptor_00000 = "expression/aggregation/descriptor.go"
