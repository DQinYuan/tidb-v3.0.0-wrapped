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
	"math"
	"strings"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
)

func boolToInt64(v bool) int64 {
	trace_util_0.Count(_helper_00000, 0)
	if v {
		trace_util_0.Count(_helper_00000, 2)
		return 1
	}
	trace_util_0.Count(_helper_00000, 1)
	return 0
}

// IsCurrentTimestampExpr returns whether e is CurrentTimestamp expression.
func IsCurrentTimestampExpr(e ast.ExprNode) bool {
	trace_util_0.Count(_helper_00000, 3)
	if fn, ok := e.(*ast.FuncCallExpr); ok && fn.FnName.L == ast.CurrentTimestamp {
		trace_util_0.Count(_helper_00000, 5)
		return true
	}
	trace_util_0.Count(_helper_00000, 4)
	return false
}

// GetTimeValue gets the time value with type tp.
func GetTimeValue(ctx sessionctx.Context, v interface{}, tp byte, fsp int) (d types.Datum, err error) {
	trace_util_0.Count(_helper_00000, 6)
	value := types.Time{
		Type: tp,
		Fsp:  fsp,
	}

	sc := ctx.GetSessionVars().StmtCtx
	switch x := v.(type) {
	case string:
		trace_util_0.Count(_helper_00000, 8)
		upperX := strings.ToUpper(x)
		if upperX == strings.ToUpper(ast.CurrentTimestamp) {
			trace_util_0.Count(_helper_00000, 16)
			defaultTime, err := getSystemTimestamp(ctx)
			if err != nil {
				trace_util_0.Count(_helper_00000, 18)
				return d, err
			}
			trace_util_0.Count(_helper_00000, 17)
			value.Time = types.FromGoTime(defaultTime.Truncate(time.Duration(math.Pow10(9-fsp)) * time.Nanosecond))
			if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
				trace_util_0.Count(_helper_00000, 19)
				err = value.ConvertTimeZone(time.Local, ctx.GetSessionVars().Location())
				if err != nil {
					trace_util_0.Count(_helper_00000, 20)
					return d, err
				}
			}
		} else {
			trace_util_0.Count(_helper_00000, 21)
			if upperX == types.ZeroDatetimeStr {
				trace_util_0.Count(_helper_00000, 22)
				value, err = types.ParseTimeFromNum(sc, 0, tp, fsp)
				terror.Log(err)
			} else {
				trace_util_0.Count(_helper_00000, 23)
				{
					value, err = types.ParseTime(sc, x, tp, fsp)
					if err != nil {
						trace_util_0.Count(_helper_00000, 24)
						return d, err
					}
				}
			}
		}
	case *driver.ValueExpr:
		trace_util_0.Count(_helper_00000, 9)
		switch x.Kind() {
		case types.KindString:
			trace_util_0.Count(_helper_00000, 25)
			value, err = types.ParseTime(sc, x.GetString(), tp, fsp)
			if err != nil {
				trace_util_0.Count(_helper_00000, 29)
				return d, err
			}
		case types.KindInt64:
			trace_util_0.Count(_helper_00000, 26)
			value, err = types.ParseTimeFromNum(sc, x.GetInt64(), tp, fsp)
			if err != nil {
				trace_util_0.Count(_helper_00000, 30)
				return d, err
			}
		case types.KindNull:
			trace_util_0.Count(_helper_00000, 27)
			return d, nil
		default:
			trace_util_0.Count(_helper_00000, 28)
			return d, errDefaultValue
		}
	case *ast.FuncCallExpr:
		trace_util_0.Count(_helper_00000, 10)
		if x.FnName.L == ast.CurrentTimestamp {
			trace_util_0.Count(_helper_00000, 31)
			d.SetString(strings.ToUpper(ast.CurrentTimestamp))
			return d, nil
		}
		trace_util_0.Count(_helper_00000, 11)
		return d, errDefaultValue
	case *ast.UnaryOperationExpr:
		trace_util_0.Count(_helper_00000, 12)
		// support some expression, like `-1`
		v, err := EvalAstExpr(ctx, x)
		if err != nil {
			trace_util_0.Count(_helper_00000, 32)
			return d, err
		}
		trace_util_0.Count(_helper_00000, 13)
		ft := types.NewFieldType(mysql.TypeLonglong)
		xval, err := v.ConvertTo(ctx.GetSessionVars().StmtCtx, ft)
		if err != nil {
			trace_util_0.Count(_helper_00000, 33)
			return d, err
		}

		trace_util_0.Count(_helper_00000, 14)
		value, err = types.ParseTimeFromNum(sc, xval.GetInt64(), tp, fsp)
		if err != nil {
			trace_util_0.Count(_helper_00000, 34)
			return d, err
		}
	default:
		trace_util_0.Count(_helper_00000, 15)
		return d, nil
	}
	trace_util_0.Count(_helper_00000, 7)
	d.SetMysqlTime(value)
	return d, nil
}

func getSystemTimestamp(ctx sessionctx.Context) (time.Time, error) {
	trace_util_0.Count(_helper_00000, 35)
	now := time.Now()

	if ctx == nil {
		trace_util_0.Count(_helper_00000, 41)
		return now, nil
	}

	trace_util_0.Count(_helper_00000, 36)
	sessionVars := ctx.GetSessionVars()
	timestampStr, err := variable.GetSessionSystemVar(sessionVars, "timestamp")
	if err != nil {
		trace_util_0.Count(_helper_00000, 42)
		return now, err
	}

	trace_util_0.Count(_helper_00000, 37)
	if timestampStr == "" {
		trace_util_0.Count(_helper_00000, 43)
		return now, nil
	}
	trace_util_0.Count(_helper_00000, 38)
	timestamp, err := types.StrToInt(sessionVars.StmtCtx, timestampStr)
	if err != nil {
		trace_util_0.Count(_helper_00000, 44)
		return time.Time{}, err
	}
	trace_util_0.Count(_helper_00000, 39)
	if timestamp <= 0 {
		trace_util_0.Count(_helper_00000, 45)
		return now, nil
	}
	trace_util_0.Count(_helper_00000, 40)
	return time.Unix(timestamp, 0), nil
}

var _helper_00000 = "expression/helper.go"
