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

package driver

import (
	"fmt"
	"io"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
)

// The purpose of driver package is to decompose the dependency of the parser and
// types package.
// It provides the NewValueExpr function for the ast package, so the ast package
// do not depends on the concrete definition of `types.Datum`, thus get rid of
// the dependency of the types package.
// The parser package depends on the ast package, but not the types package.
// The whole relationship:
// ast imports []
// tidb/types imports [parser/types]
// parser imports [ast, parser/types]
// driver imports [ast, tidb/types]
// tidb imports [parser, driver]

func init() {
	trace_util_0.Count(_value_expr_00000, 0)
	ast.NewValueExpr = newValueExpr
	ast.NewParamMarkerExpr = newParamMarkerExpr
	ast.NewDecimal = func(str string) (interface{}, error) {
		trace_util_0.Count(_value_expr_00000, 3)
		dec := new(types.MyDecimal)
		err := dec.FromString(hack.Slice(str))
		return dec, err
	}
	trace_util_0.Count(_value_expr_00000, 1)
	ast.NewHexLiteral = func(str string) (interface{}, error) {
		trace_util_0.Count(_value_expr_00000, 4)
		h, err := types.NewHexLiteral(str)
		return h, err
	}
	trace_util_0.Count(_value_expr_00000, 2)
	ast.NewBitLiteral = func(str string) (interface{}, error) {
		trace_util_0.Count(_value_expr_00000, 5)
		b, err := types.NewBitLiteral(str)
		return b, err
	}
}

var (
	_ ast.ParamMarkerExpr = &ParamMarkerExpr{}
	_ ast.ValueExpr       = &ValueExpr{}
)

// ValueExpr is the simple value expression.
type ValueExpr struct {
	ast.TexprNode
	types.Datum
	projectionOffset int
}

// Restore implements Node interface.
func (n *ValueExpr) Restore(ctx *format.RestoreCtx) error {
	trace_util_0.Count(_value_expr_00000, 6)
	switch n.Kind() {
	case types.KindNull:
		trace_util_0.Count(_value_expr_00000, 8)
		ctx.WriteKeyWord("NULL")
	case types.KindInt64:
		trace_util_0.Count(_value_expr_00000, 9)
		if n.Type.Flag&mysql.IsBooleanFlag != 0 {
			trace_util_0.Count(_value_expr_00000, 20)
			if n.GetInt64() > 0 {
				trace_util_0.Count(_value_expr_00000, 21)
				ctx.WriteKeyWord("TRUE")
			} else {
				trace_util_0.Count(_value_expr_00000, 22)
				{
					ctx.WriteKeyWord("FALSE")
				}
			}
		} else {
			trace_util_0.Count(_value_expr_00000, 23)
			{
				ctx.WritePlain(strconv.FormatInt(n.GetInt64(), 10))
			}
		}
	case types.KindUint64:
		trace_util_0.Count(_value_expr_00000, 10)
		ctx.WritePlain(strconv.FormatUint(n.GetUint64(), 10))
	case types.KindFloat32:
		trace_util_0.Count(_value_expr_00000, 11)
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32))
	case types.KindFloat64:
		trace_util_0.Count(_value_expr_00000, 12)
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64))
	case types.KindString:
		trace_util_0.Count(_value_expr_00000, 13)
		if n.Type.Charset != "" && n.Type.Charset != mysql.DefaultCharset {
			trace_util_0.Count(_value_expr_00000, 24)
			ctx.WritePlain("_")
			ctx.WriteKeyWord(n.Type.Charset)
		}
		trace_util_0.Count(_value_expr_00000, 14)
		ctx.WriteString(n.GetString())
	case types.KindBytes:
		trace_util_0.Count(_value_expr_00000, 15)
		ctx.WriteString(n.GetString())
	case types.KindMysqlDecimal:
		trace_util_0.Count(_value_expr_00000, 16)
		ctx.WritePlain(n.GetMysqlDecimal().String())
	case types.KindBinaryLiteral:
		trace_util_0.Count(_value_expr_00000, 17)
		if n.Type.Flag&mysql.UnsignedFlag != 0 {
			trace_util_0.Count(_value_expr_00000, 25)
			ctx.WritePlainf("x'%x'", n.GetBytes())
		} else {
			trace_util_0.Count(_value_expr_00000, 26)
			{
				ctx.WritePlain(n.GetBinaryLiteral().ToBitLiteralString(true))
			}
		}
	case types.KindMysqlDuration, types.KindMysqlEnum,
		types.KindMysqlBit, types.KindMysqlSet, types.KindMysqlTime,
		types.KindInterface, types.KindMinNotNull, types.KindMaxValue,
		types.KindRaw, types.KindMysqlJSON:
		trace_util_0.Count(_value_expr_00000, 18)
		// TODO implement Restore function
		return errors.New("Not implemented")
	default:
		trace_util_0.Count(_value_expr_00000, 19)
		return errors.New("can't format to string")
	}
	trace_util_0.Count(_value_expr_00000, 7)
	return nil
}

// GetDatumString implements the ast.ValueExpr interface.
func (n *ValueExpr) GetDatumString() string {
	trace_util_0.Count(_value_expr_00000, 27)
	return n.GetString()
}

// Format the ExprNode into a Writer.
func (n *ValueExpr) Format(w io.Writer) {
	trace_util_0.Count(_value_expr_00000, 28)
	var s string
	switch n.Kind() {
	case types.KindNull:
		trace_util_0.Count(_value_expr_00000, 30)
		s = "NULL"
	case types.KindInt64:
		trace_util_0.Count(_value_expr_00000, 31)
		if n.Type.Flag&mysql.IsBooleanFlag != 0 {
			trace_util_0.Count(_value_expr_00000, 39)
			if n.GetInt64() > 0 {
				trace_util_0.Count(_value_expr_00000, 40)
				s = "TRUE"
			} else {
				trace_util_0.Count(_value_expr_00000, 41)
				{
					s = "FALSE"
				}
			}
		} else {
			trace_util_0.Count(_value_expr_00000, 42)
			{
				s = strconv.FormatInt(n.GetInt64(), 10)
			}
		}
	case types.KindUint64:
		trace_util_0.Count(_value_expr_00000, 32)
		s = strconv.FormatUint(n.GetUint64(), 10)
	case types.KindFloat32:
		trace_util_0.Count(_value_expr_00000, 33)
		s = strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32)
	case types.KindFloat64:
		trace_util_0.Count(_value_expr_00000, 34)
		s = strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64)
	case types.KindString, types.KindBytes:
		trace_util_0.Count(_value_expr_00000, 35)
		s = strconv.Quote(n.GetString())
	case types.KindMysqlDecimal:
		trace_util_0.Count(_value_expr_00000, 36)
		s = n.GetMysqlDecimal().String()
	case types.KindBinaryLiteral:
		trace_util_0.Count(_value_expr_00000, 37)
		if n.Type.Flag&mysql.UnsignedFlag != 0 {
			trace_util_0.Count(_value_expr_00000, 43)
			s = fmt.Sprintf("x'%x'", n.GetBytes())
		} else {
			trace_util_0.Count(_value_expr_00000, 44)
			{
				s = n.GetBinaryLiteral().ToBitLiteralString(true)
			}
		}
	default:
		trace_util_0.Count(_value_expr_00000, 38)
		panic("Can't format to string")
	}
	trace_util_0.Count(_value_expr_00000, 29)
	fmt.Fprint(w, s)
}

// newValueExpr creates a ValueExpr with value, and sets default field type.
func newValueExpr(value interface{}) ast.ValueExpr {
	trace_util_0.Count(_value_expr_00000, 45)
	if ve, ok := value.(*ValueExpr); ok {
		trace_util_0.Count(_value_expr_00000, 47)
		return ve
	}
	trace_util_0.Count(_value_expr_00000, 46)
	ve := &ValueExpr{}
	ve.SetValue(value)
	types.DefaultTypeForValue(value, &ve.Type)
	ve.projectionOffset = -1
	return ve
}

// SetProjectionOffset sets ValueExpr.projectionOffset for logical plan builder.
func (n *ValueExpr) SetProjectionOffset(offset int) {
	trace_util_0.Count(_value_expr_00000, 48)
	n.projectionOffset = offset
}

// GetProjectionOffset returns ValueExpr.projectionOffset.
func (n *ValueExpr) GetProjectionOffset() int {
	trace_util_0.Count(_value_expr_00000, 49)
	return n.projectionOffset
}

// Accept implements Node interface.
func (n *ValueExpr) Accept(v ast.Visitor) (ast.Node, bool) {
	trace_util_0.Count(_value_expr_00000, 50)
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		trace_util_0.Count(_value_expr_00000, 52)
		return v.Leave(newNode)
	}
	trace_util_0.Count(_value_expr_00000, 51)
	n = newNode.(*ValueExpr)
	return v.Leave(n)
}

// ParamMarkerExpr expression holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarkerExpr struct {
	ValueExpr
	Offset    int
	Order     int
	InExecute bool
}

// Restore implements Node interface.
func (n *ParamMarkerExpr) Restore(ctx *format.RestoreCtx) error {
	trace_util_0.Count(_value_expr_00000, 53)
	ctx.WritePlain("?")
	return nil
}

func newParamMarkerExpr(offset int) ast.ParamMarkerExpr {
	trace_util_0.Count(_value_expr_00000, 54)
	return &ParamMarkerExpr{
		Offset: offset,
	}
}

// Format the ExprNode into a Writer.
func (n *ParamMarkerExpr) Format(w io.Writer) {
	trace_util_0.Count(_value_expr_00000, 55)
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ParamMarkerExpr) Accept(v ast.Visitor) (ast.Node, bool) {
	trace_util_0.Count(_value_expr_00000, 56)
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		trace_util_0.Count(_value_expr_00000, 58)
		return v.Leave(newNode)
	}
	trace_util_0.Count(_value_expr_00000, 57)
	n = newNode.(*ParamMarkerExpr)
	return v.Leave(n)
}

// SetOrder implements the ast.ParamMarkerExpr interface.
func (n *ParamMarkerExpr) SetOrder(order int) {
	trace_util_0.Count(_value_expr_00000, 59)
	n.Order = order
}

var _value_expr_00000 = "types/parser_driver/value_expr.go"
