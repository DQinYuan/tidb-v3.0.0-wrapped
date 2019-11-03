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
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// WindowFuncDesc describes a window function signature, only used in planner.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (*WindowFuncDesc, error) {
	trace_util_0.Count(_window_func_00000, 0)
	switch strings.ToLower(name) {
	case ast.WindowFuncNthValue:
		trace_util_0.Count(_window_func_00000, 3)
		val, isNull, ok := expression.GetUint64FromConstant(args[1])
		// nth_value does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			trace_util_0.Count(_window_func_00000, 7)
			return nil, nil
		}
	case ast.WindowFuncNtile:
		trace_util_0.Count(_window_func_00000, 4)
		val, isNull, ok := expression.GetUint64FromConstant(args[0])
		// ntile does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			trace_util_0.Count(_window_func_00000, 8)
			return nil, nil
		}
	case ast.WindowFuncLead, ast.WindowFuncLag:
		trace_util_0.Count(_window_func_00000, 5)
		if len(args) < 2 {
			trace_util_0.Count(_window_func_00000, 9)
			break
		}
		trace_util_0.Count(_window_func_00000, 6)
		_, isNull, ok := expression.GetUint64FromConstant(args[1])
		if !ok || isNull {
			trace_util_0.Count(_window_func_00000, 10)
			return nil, nil
		}
	}
	trace_util_0.Count(_window_func_00000, 1)
	base, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		trace_util_0.Count(_window_func_00000, 11)
		return nil, err
	}
	trace_util_0.Count(_window_func_00000, 2)
	return &WindowFuncDesc{base}, nil
}

// noFrameWindowFuncs is the functions that operate on the entire partition,
// they should not have frame specifications.
var noFrameWindowFuncs = map[string]struct{}{
	ast.WindowFuncCumeDist:    {},
	ast.WindowFuncDenseRank:   {},
	ast.WindowFuncLag:         {},
	ast.WindowFuncLead:        {},
	ast.WindowFuncNtile:       {},
	ast.WindowFuncPercentRank: {},
	ast.WindowFuncRank:        {},
	ast.WindowFuncRowNumber:   {},
}

// NeedFrame checks if the function need frame specification.
func NeedFrame(name string) bool {
	trace_util_0.Count(_window_func_00000, 12)
	_, ok := noFrameWindowFuncs[strings.ToLower(name)]
	return !ok
}

var _window_func_00000 = "expression/aggregation/window_func.go"
