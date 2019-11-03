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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// conditionChecker checks if this condition can be pushed to index planner.
type conditionChecker struct {
	colUniqueID   int64
	shouldReserve bool // check if a access condition should be reserved in filter conditions.
	length        int
}

func (c *conditionChecker) check(condition expression.Expression) bool {
	trace_util_0.Count(_checker_00000, 0)
	switch x := condition.(type) {
	case *expression.ScalarFunction:
		trace_util_0.Count(_checker_00000, 2)
		return c.checkScalarFunction(x)
	case *expression.Column:
		trace_util_0.Count(_checker_00000, 3)
		return c.checkColumn(x)
	case *expression.Constant:
		trace_util_0.Count(_checker_00000, 4)
		return true
	}
	trace_util_0.Count(_checker_00000, 1)
	return false
}

func (c *conditionChecker) checkScalarFunction(scalar *expression.ScalarFunction) bool {
	trace_util_0.Count(_checker_00000, 5)
	switch scalar.FuncName.L {
	case ast.LogicOr, ast.LogicAnd:
		trace_util_0.Count(_checker_00000, 7)
		return c.check(scalar.GetArgs()[0]) && c.check(scalar.GetArgs()[1])
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT:
		trace_util_0.Count(_checker_00000, 8)
		if _, ok := scalar.GetArgs()[0].(*expression.Constant); ok {
			trace_util_0.Count(_checker_00000, 18)
			if c.checkColumn(scalar.GetArgs()[1]) {
				trace_util_0.Count(_checker_00000, 19)
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
		trace_util_0.Count(_checker_00000, 9)
		if _, ok := scalar.GetArgs()[1].(*expression.Constant); ok {
			trace_util_0.Count(_checker_00000, 20)
			if c.checkColumn(scalar.GetArgs()[0]) {
				trace_util_0.Count(_checker_00000, 21)
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
	case ast.IsNull, ast.IsTruth, ast.IsFalsity:
		trace_util_0.Count(_checker_00000, 10)
		return c.checkColumn(scalar.GetArgs()[0])
	case ast.UnaryNot:
		trace_util_0.Count(_checker_00000, 11)
		// TODO: support "not like" convert to access conditions.
		if s, ok := scalar.GetArgs()[0].(*expression.ScalarFunction); ok {
			trace_util_0.Count(_checker_00000, 22)
			if s.FuncName.L == ast.Like {
				trace_util_0.Count(_checker_00000, 23)
				return false
			}
		} else {
			trace_util_0.Count(_checker_00000, 24)
			{
				// "not column" or "not constant" can't lead to a range.
				return false
			}
		}
		trace_util_0.Count(_checker_00000, 12)
		return c.check(scalar.GetArgs()[0])
	case ast.In:
		trace_util_0.Count(_checker_00000, 13)
		if !c.checkColumn(scalar.GetArgs()[0]) {
			trace_util_0.Count(_checker_00000, 25)
			return false
		}
		trace_util_0.Count(_checker_00000, 14)
		for _, v := range scalar.GetArgs()[1:] {
			trace_util_0.Count(_checker_00000, 26)
			if _, ok := v.(*expression.Constant); !ok {
				trace_util_0.Count(_checker_00000, 27)
				return false
			}
		}
		trace_util_0.Count(_checker_00000, 15)
		return true
	case ast.Like:
		trace_util_0.Count(_checker_00000, 16)
		return c.checkLikeFunc(scalar)
	case ast.GetParam:
		trace_util_0.Count(_checker_00000, 17)
		return true
	}
	trace_util_0.Count(_checker_00000, 6)
	return false
}

func (c *conditionChecker) checkLikeFunc(scalar *expression.ScalarFunction) bool {
	trace_util_0.Count(_checker_00000, 28)
	if !c.checkColumn(scalar.GetArgs()[0]) {
		trace_util_0.Count(_checker_00000, 35)
		return false
	}
	trace_util_0.Count(_checker_00000, 29)
	pattern, ok := scalar.GetArgs()[1].(*expression.Constant)
	if !ok {
		trace_util_0.Count(_checker_00000, 36)
		return false

	}
	trace_util_0.Count(_checker_00000, 30)
	if pattern.Value.IsNull() {
		trace_util_0.Count(_checker_00000, 37)
		return false
	}
	trace_util_0.Count(_checker_00000, 31)
	patternStr, err := pattern.Value.ToString()
	if err != nil {
		trace_util_0.Count(_checker_00000, 38)
		return false
	}
	trace_util_0.Count(_checker_00000, 32)
	if len(patternStr) == 0 {
		trace_util_0.Count(_checker_00000, 39)
		return true
	}
	trace_util_0.Count(_checker_00000, 33)
	escape := byte(scalar.GetArgs()[2].(*expression.Constant).Value.GetInt64())
	for i := 0; i < len(patternStr); i++ {
		trace_util_0.Count(_checker_00000, 40)
		if patternStr[i] == escape {
			trace_util_0.Count(_checker_00000, 44)
			i++
			if i < len(patternStr)-1 {
				trace_util_0.Count(_checker_00000, 46)
				continue
			}
			trace_util_0.Count(_checker_00000, 45)
			break
		}
		trace_util_0.Count(_checker_00000, 41)
		if i == 0 && (patternStr[i] == '%' || patternStr[i] == '_') {
			trace_util_0.Count(_checker_00000, 47)
			return false
		}
		trace_util_0.Count(_checker_00000, 42)
		if patternStr[i] == '%' {
			trace_util_0.Count(_checker_00000, 48)
			if i != len(patternStr)-1 {
				trace_util_0.Count(_checker_00000, 50)
				c.shouldReserve = true
			}
			trace_util_0.Count(_checker_00000, 49)
			break
		}
		trace_util_0.Count(_checker_00000, 43)
		if patternStr[i] == '_' {
			trace_util_0.Count(_checker_00000, 51)
			c.shouldReserve = true
			break
		}
	}
	trace_util_0.Count(_checker_00000, 34)
	return true
}

func (c *conditionChecker) checkColumn(expr expression.Expression) bool {
	trace_util_0.Count(_checker_00000, 52)
	col, ok := expr.(*expression.Column)
	if !ok {
		trace_util_0.Count(_checker_00000, 54)
		return false
	}
	trace_util_0.Count(_checker_00000, 53)
	return c.colUniqueID == col.UniqueID
}

var _checker_00000 = "util/ranger/checker.go"
