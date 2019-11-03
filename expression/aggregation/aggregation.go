// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// Aggregation stands for aggregate functions.
type Aggregation interface {
	// Update during executing.
	Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error

	// GetPartialResult will called by coprocessor to get partial results. For avg function, partial results will return
	// sum and count values at the same time.
	GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum

	// GetResult will be called when all data have been processed.
	GetResult(evalCtx *AggEvaluateContext) types.Datum

	// CreateContext creates a new AggEvaluateContext for the aggregation function.
	CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext

	// ResetContext resets the content of the evaluate context.
	ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext)

	// GetFinalAggFunc constructs the final agg functions, only used in parallel execution.
	GetFinalAggFunc(ctx sessionctx.Context, idx int) (int, Aggregation)

	// GetArgs gets the args of the aggregate function.
	GetArgs() []expression.Expression

	// Clone deep copy the Aggregation.
	Clone(ctx sessionctx.Context) Aggregation
}

// NewDistAggFunc creates new Aggregate function for mock tikv.
func NewDistAggFunc(expr *tipb.Expr, fieldTps []*types.FieldType, sc *stmtctx.StatementContext) (Aggregation, error) {
	trace_util_0.Count(_aggregation_00000, 0)
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		trace_util_0.Count(_aggregation_00000, 3)
		arg, err := expression.PBToExpr(child, fieldTps, sc)
		if err != nil {
			trace_util_0.Count(_aggregation_00000, 5)
			return nil, err
		}
		trace_util_0.Count(_aggregation_00000, 4)
		args = append(args, arg)
	}
	trace_util_0.Count(_aggregation_00000, 1)
	switch expr.Tp {
	case tipb.ExprType_Sum:
		trace_util_0.Count(_aggregation_00000, 6)
		return &sumFunction{aggFunction: newAggFunc(ast.AggFuncSum, args, false)}, nil
	case tipb.ExprType_Count:
		trace_util_0.Count(_aggregation_00000, 7)
		return &countFunction{aggFunction: newAggFunc(ast.AggFuncCount, args, false)}, nil
	case tipb.ExprType_Avg:
		trace_util_0.Count(_aggregation_00000, 8)
		return &avgFunction{aggFunction: newAggFunc(ast.AggFuncAvg, args, false)}, nil
	case tipb.ExprType_GroupConcat:
		trace_util_0.Count(_aggregation_00000, 9)
		return &concatFunction{aggFunction: newAggFunc(ast.AggFuncGroupConcat, args, false)}, nil
	case tipb.ExprType_Max:
		trace_util_0.Count(_aggregation_00000, 10)
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMax, args, false), isMax: true}, nil
	case tipb.ExprType_Min:
		trace_util_0.Count(_aggregation_00000, 11)
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMin, args, false)}, nil
	case tipb.ExprType_First:
		trace_util_0.Count(_aggregation_00000, 12)
		return &firstRowFunction{aggFunction: newAggFunc(ast.AggFuncFirstRow, args, false)}, nil
	case tipb.ExprType_Agg_BitOr:
		trace_util_0.Count(_aggregation_00000, 13)
		return &bitOrFunction{aggFunction: newAggFunc(ast.AggFuncBitOr, args, false)}, nil
	case tipb.ExprType_Agg_BitXor:
		trace_util_0.Count(_aggregation_00000, 14)
		return &bitXorFunction{aggFunction: newAggFunc(ast.AggFuncBitXor, args, false)}, nil
	case tipb.ExprType_Agg_BitAnd:
		trace_util_0.Count(_aggregation_00000, 15)
		return &bitAndFunction{aggFunction: newAggFunc(ast.AggFuncBitAnd, args, false)}, nil
	}
	trace_util_0.Count(_aggregation_00000, 2)
	return nil, errors.Errorf("Unknown aggregate function type %v", expr.Tp)
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstRow     bool          // It will check if the agg has met the first row key.
}

// AggFunctionMode stands for the aggregation function's mode.
type AggFunctionMode int

// |-----------------|--------------|--------------|
// | AggFunctionMode | input        | output       |
// |-----------------|--------------|--------------|
// | CompleteMode    | origin data  | final result |
// | FinalMode       | partial data | final result |
// | Partial1Mode    | origin data  | partial data |
// | Partial2Mode    | partial data | partial data |
// | DedupMode       | origin data  | origin data  |
// |-----------------|--------------|--------------|
const (
	CompleteMode AggFunctionMode = iota
	FinalMode
	Partial1Mode
	Partial2Mode
	DedupMode
)

type aggFunction struct {
	*AggFuncDesc
}

func newAggFunc(funcName string, args []expression.Expression, hasDistinct bool) aggFunction {
	trace_util_0.Count(_aggregation_00000, 16)
	agg := &AggFuncDesc{HasDistinct: hasDistinct}
	agg.Name = funcName
	agg.Args = args
	return aggFunction{AggFuncDesc: agg}
}

// CreateContext implements Aggregation interface.
func (af *aggFunction) CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext {
	trace_util_0.Count(_aggregation_00000, 17)
	evalCtx := &AggEvaluateContext{}
	if af.HasDistinct {
		trace_util_0.Count(_aggregation_00000, 19)
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	trace_util_0.Count(_aggregation_00000, 18)
	return evalCtx
}

func (af *aggFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_aggregation_00000, 20)
	if af.HasDistinct {
		trace_util_0.Count(_aggregation_00000, 22)
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	trace_util_0.Count(_aggregation_00000, 21)
	evalCtx.Value.SetNull()
}

func (af *aggFunction) updateSum(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	trace_util_0.Count(_aggregation_00000, 23)
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		trace_util_0.Count(_aggregation_00000, 28)
		return err
	}
	trace_util_0.Count(_aggregation_00000, 24)
	if value.IsNull() {
		trace_util_0.Count(_aggregation_00000, 29)
		return nil
	}
	trace_util_0.Count(_aggregation_00000, 25)
	if af.HasDistinct {
		trace_util_0.Count(_aggregation_00000, 30)
		d, err1 := evalCtx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			trace_util_0.Count(_aggregation_00000, 32)
			return err1
		}
		trace_util_0.Count(_aggregation_00000, 31)
		if !d {
			trace_util_0.Count(_aggregation_00000, 33)
			return nil
		}
	}
	trace_util_0.Count(_aggregation_00000, 26)
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		trace_util_0.Count(_aggregation_00000, 34)
		return err
	}
	trace_util_0.Count(_aggregation_00000, 27)
	evalCtx.Count++
	return nil
}

func (af *aggFunction) GetFinalAggFunc(ctx sessionctx.Context, idx int) (_ int, newAggFunc Aggregation) {
	trace_util_0.Count(_aggregation_00000, 35)
	switch af.Mode {
	case DedupMode:
		trace_util_0.Count(_aggregation_00000, 37)
		panic("DedupMode is not supported now.")
	case Partial1Mode:
		trace_util_0.Count(_aggregation_00000, 38)
		args := make([]expression.Expression, 0, 2)
		if NeedCount(af.Name) {
			trace_util_0.Count(_aggregation_00000, 43)
			args = append(args, &expression.Column{
				ColName: model.NewCIStr(fmt.Sprintf("col_%d", idx)),
				Index:   idx,
				RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 21, Charset: charset.CharsetBin, Collate: charset.CollationBin},
			})
			idx++
		}
		trace_util_0.Count(_aggregation_00000, 39)
		if NeedValue(af.Name) {
			trace_util_0.Count(_aggregation_00000, 44)
			args = append(args, &expression.Column{
				ColName: model.NewCIStr(fmt.Sprintf("col_%d", idx)),
				Index:   idx,
				RetType: af.RetTp,
			})
			idx++
			if af.Name == ast.AggFuncGroupConcat {
				trace_util_0.Count(_aggregation_00000, 45)
				separator := af.Args[len(af.Args)-1]
				args = append(args, separator.Clone())
			}
		}
		trace_util_0.Count(_aggregation_00000, 40)
		desc := af.AggFuncDesc.Clone()
		desc.Mode = FinalMode
		desc.Args = args
		newAggFunc = desc.GetAggFunc(ctx)
	case Partial2Mode:
		trace_util_0.Count(_aggregation_00000, 41)
		desc := af.AggFuncDesc.Clone()
		desc.Mode = FinalMode
		idx += len(desc.Args)
		newAggFunc = desc.GetAggFunc(ctx)
	case FinalMode, CompleteMode:
		trace_util_0.Count(_aggregation_00000, 42)
		panic("GetFinalAggFunc should not be called when aggMode is FinalMode/CompleteMode.")
	}
	trace_util_0.Count(_aggregation_00000, 36)
	return idx, newAggFunc
}

func (af *aggFunction) GetArgs() []expression.Expression {
	trace_util_0.Count(_aggregation_00000, 46)
	return af.Args
}

func (af *aggFunction) Clone(ctx sessionctx.Context) Aggregation {
	trace_util_0.Count(_aggregation_00000, 47)
	desc := af.AggFuncDesc.Clone()
	return desc.GetAggFunc(ctx)
}

// NeedCount indicates whether the aggregate function should record count.
func NeedCount(name string) bool {
	trace_util_0.Count(_aggregation_00000, 48)
	return name == ast.AggFuncCount || name == ast.AggFuncAvg
}

// NeedValue indicates whether the aggregate function should record value.
func NeedValue(name string) bool {
	trace_util_0.Count(_aggregation_00000, 49)
	switch name {
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncGroupConcat, ast.AggFuncBitOr, ast.AggFuncBitAnd, ast.AggFuncBitXor:
		trace_util_0.Count(_aggregation_00000, 50)
		return true
	default:
		trace_util_0.Count(_aggregation_00000, 51)
		return false
	}
}

// IsAllFirstRow checks whether functions in `aggFuncs` are all FirstRow.
func IsAllFirstRow(aggFuncs []*AggFuncDesc) bool {
	trace_util_0.Count(_aggregation_00000, 52)
	for _, fun := range aggFuncs {
		trace_util_0.Count(_aggregation_00000, 54)
		if fun.Name != ast.AggFuncFirstRow {
			trace_util_0.Count(_aggregation_00000, 55)
			return false
		}
	}
	trace_util_0.Count(_aggregation_00000, 53)
	return true
}

var _aggregation_00000 = "expression/aggregation/aggregation.go"
