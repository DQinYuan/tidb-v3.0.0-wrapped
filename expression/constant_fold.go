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

package expression

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// specialFoldHandler stores functions for special UDF to constant fold
var specialFoldHandler = map[string]func(*ScalarFunction) (Expression, bool){}

func init() {
	trace_util_0.Count(_constant_fold_00000, 0)
	specialFoldHandler = map[string]func(*ScalarFunction) (Expression, bool){
		ast.If:     ifFoldHandler,
		ast.Ifnull: ifNullFoldHandler,
	}
}

// FoldConstant does constant folding optimization on an expression excluding deferred ones.
func FoldConstant(expr Expression) Expression {
	trace_util_0.Count(_constant_fold_00000, 1)
	e, _ := foldConstant(expr)
	return e
}

func ifFoldHandler(expr *ScalarFunction) (Expression, bool) {
	trace_util_0.Count(_constant_fold_00000, 2)
	args := expr.GetArgs()
	foldedArg0, _ := foldConstant(args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		trace_util_0.Count(_constant_fold_00000, 4)
		arg0, isNull0, err := constArg.EvalInt(expr.Function.getCtx(), chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_fold_00000, 7)
			// Failed to fold this expr to a constant, print the DEBUG log and
			// return the original expression to let the error to be evaluated
			// again, in that time, the error is returned to the client.
			logutil.Logger(context.Background()).Debug("fold expression to constant", zap.String("expression", expr.ExplainInfo()), zap.Error(err))
			return expr, false
		}
		trace_util_0.Count(_constant_fold_00000, 5)
		if !isNull0 && arg0 != 0 {
			trace_util_0.Count(_constant_fold_00000, 8)
			return foldConstant(args[1])
		}
		trace_util_0.Count(_constant_fold_00000, 6)
		return foldConstant(args[2])
	}
	trace_util_0.Count(_constant_fold_00000, 3)
	var isDeferred, isDeferredConst bool
	expr.GetArgs()[1], isDeferred = foldConstant(args[1])
	isDeferredConst = isDeferredConst || isDeferred
	expr.GetArgs()[2], isDeferred = foldConstant(args[2])
	isDeferredConst = isDeferredConst || isDeferred
	return expr, isDeferredConst
}

func ifNullFoldHandler(expr *ScalarFunction) (Expression, bool) {
	trace_util_0.Count(_constant_fold_00000, 9)
	args := expr.GetArgs()
	foldedArg0, isDeferred := foldConstant(args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		trace_util_0.Count(_constant_fold_00000, 11)
		// Only check constArg.Value here. Because deferred expression is
		// evaluated to constArg.Value after foldConstant(args[0]), it's not
		// needed to be checked.
		if constArg.Value.IsNull() {
			trace_util_0.Count(_constant_fold_00000, 13)
			return foldConstant(args[1])
		}
		trace_util_0.Count(_constant_fold_00000, 12)
		return constArg, isDeferred
	}
	trace_util_0.Count(_constant_fold_00000, 10)
	var isDeferredConst bool
	expr.GetArgs()[1], isDeferredConst = foldConstant(args[1])
	return expr, isDeferredConst
}

func foldConstant(expr Expression) (Expression, bool) {
	trace_util_0.Count(_constant_fold_00000, 14)
	switch x := expr.(type) {
	case *ScalarFunction:
		trace_util_0.Count(_constant_fold_00000, 16)
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			trace_util_0.Count(_constant_fold_00000, 24)
			return expr, false
		}
		trace_util_0.Count(_constant_fold_00000, 17)
		if function := specialFoldHandler[x.FuncName.L]; function != nil {
			trace_util_0.Count(_constant_fold_00000, 25)
			return function(x)
		}

		trace_util_0.Count(_constant_fold_00000, 18)
		args := x.GetArgs()
		sc := x.GetCtx().GetSessionVars().StmtCtx
		argIsConst := make([]bool, len(args))
		hasNullArg := false
		allConstArg := true
		isDeferredConst := false
		for i := 0; i < len(args); i++ {
			trace_util_0.Count(_constant_fold_00000, 26)
			foldedArg, isDeferred := foldConstant(args[i])
			x.GetArgs()[i] = foldedArg
			con, conOK := foldedArg.(*Constant)
			argIsConst[i] = conOK
			allConstArg = allConstArg && conOK
			hasNullArg = hasNullArg || (conOK && con.Value.IsNull())
			isDeferredConst = isDeferredConst || isDeferred
		}
		trace_util_0.Count(_constant_fold_00000, 19)
		if !allConstArg {
			trace_util_0.Count(_constant_fold_00000, 27)
			if !hasNullArg || !sc.InNullRejectCheck || x.FuncName.L == ast.NullEQ {
				trace_util_0.Count(_constant_fold_00000, 34)
				return expr, isDeferredConst
			}
			trace_util_0.Count(_constant_fold_00000, 28)
			constArgs := make([]Expression, len(args))
			for i, arg := range args {
				trace_util_0.Count(_constant_fold_00000, 35)
				if argIsConst[i] {
					trace_util_0.Count(_constant_fold_00000, 36)
					constArgs[i] = arg
				} else {
					trace_util_0.Count(_constant_fold_00000, 37)
					{
						constArgs[i] = One
					}
				}
			}
			trace_util_0.Count(_constant_fold_00000, 29)
			dummyScalarFunc, err := NewFunctionBase(x.GetCtx(), x.FuncName.L, x.GetType(), constArgs...)
			if err != nil {
				trace_util_0.Count(_constant_fold_00000, 38)
				return expr, isDeferredConst
			}
			trace_util_0.Count(_constant_fold_00000, 30)
			value, err := dummyScalarFunc.Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_constant_fold_00000, 39)
				return expr, isDeferredConst
			}
			trace_util_0.Count(_constant_fold_00000, 31)
			if value.IsNull() {
				trace_util_0.Count(_constant_fold_00000, 40)
				if isDeferredConst {
					trace_util_0.Count(_constant_fold_00000, 42)
					return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x}, true
				}
				trace_util_0.Count(_constant_fold_00000, 41)
				return &Constant{Value: value, RetType: x.RetType}, false
			}
			trace_util_0.Count(_constant_fold_00000, 32)
			if isTrue, err := value.ToBool(sc); err == nil && isTrue == 0 {
				trace_util_0.Count(_constant_fold_00000, 43)
				if isDeferredConst {
					trace_util_0.Count(_constant_fold_00000, 45)
					return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x}, true
				}
				trace_util_0.Count(_constant_fold_00000, 44)
				return &Constant{Value: value, RetType: x.RetType}, false
			}
			trace_util_0.Count(_constant_fold_00000, 33)
			return expr, isDeferredConst
		}
		trace_util_0.Count(_constant_fold_00000, 20)
		value, err := x.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_fold_00000, 46)
			logutil.Logger(context.Background()).Debug("fold expression to constant", zap.String("expression", x.ExplainInfo()), zap.Error(err))
			return expr, isDeferredConst
		}
		trace_util_0.Count(_constant_fold_00000, 21)
		if isDeferredConst {
			trace_util_0.Count(_constant_fold_00000, 47)
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x}, true
		}
		trace_util_0.Count(_constant_fold_00000, 22)
		return &Constant{Value: value, RetType: x.RetType}, false
	case *Constant:
		trace_util_0.Count(_constant_fold_00000, 23)
		if x.DeferredExpr != nil {
			trace_util_0.Count(_constant_fold_00000, 48)
			value, err := x.DeferredExpr.Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_constant_fold_00000, 50)
				logutil.Logger(context.Background()).Debug("fold expression to constant", zap.String("expression", x.ExplainInfo()), zap.Error(err))
				return expr, true
			}
			trace_util_0.Count(_constant_fold_00000, 49)
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x.DeferredExpr}, true
		}
	}
	trace_util_0.Count(_constant_fold_00000, 15)
	return expr, false
}

var _constant_fold_00000 = "expression/constant_fold.go"
