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
	"context"
	"fmt"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	// One stands for a number 1.
	One = &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Zero stands for a number 0.
	Zero = &Constant{
		Value:   types.NewDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Null stands for null constant.
	Null = &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
)

// Constant stands for a constant value.
type Constant struct {
	Value        types.Datum
	RetType      *types.FieldType
	DeferredExpr Expression // parameter getter expression
	hashcode     []byte
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	trace_util_0.Count(_constant_00000, 0)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 2)
		dt, err := c.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 4)
			logutil.Logger(context.Background()).Error("eval constant failed", zap.Error(err))
			return ""
		}
		trace_util_0.Count(_constant_00000, 3)
		c.Value.SetValue(dt.GetValue())
	}
	trace_util_0.Count(_constant_00000, 1)
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	trace_util_0.Count(_constant_00000, 5)
	return []byte(fmt.Sprintf("\"%s\"", c)), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	trace_util_0.Count(_constant_00000, 6)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 8)
		con := *c
		return &con
	}
	trace_util_0.Count(_constant_00000, 7)
	return c
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	trace_util_0.Count(_constant_00000, 9)
	return c.RetType
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ chunk.Row) (types.Datum, error) {
	trace_util_0.Count(_constant_00000, 10)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 12)
		if sf, sfOK := c.DeferredExpr.(*ScalarFunction); sfOK {
			trace_util_0.Count(_constant_00000, 13)
			dt, err := sf.Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_constant_00000, 17)
				return c.Value, err
			}
			trace_util_0.Count(_constant_00000, 14)
			if dt.IsNull() {
				trace_util_0.Count(_constant_00000, 18)
				c.Value.SetNull()
				return c.Value, nil
			}
			trace_util_0.Count(_constant_00000, 15)
			val, err := dt.ConvertTo(sf.GetCtx().GetSessionVars().StmtCtx, c.RetType)
			if err != nil {
				trace_util_0.Count(_constant_00000, 19)
				return dt, err
			}
			trace_util_0.Count(_constant_00000, 16)
			c.Value.SetValue(val.GetValue())
		}
	}
	trace_util_0.Count(_constant_00000, 11)
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx sessionctx.Context, _ chunk.Row) (int64, bool, error) {
	trace_util_0.Count(_constant_00000, 20)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 23)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 27)
			return 0, true, err
		}
		trace_util_0.Count(_constant_00000, 24)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 28)
			return 0, true, nil
		}
		trace_util_0.Count(_constant_00000, 25)
		val, err := dt.ToInt64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			trace_util_0.Count(_constant_00000, 29)
			return 0, true, err
		}
		trace_util_0.Count(_constant_00000, 26)
		c.Value.SetInt64(val)
	} else {
		trace_util_0.Count(_constant_00000, 30)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 31)
				return 0, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 21)
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		trace_util_0.Count(_constant_00000, 32)
		res, err := c.Value.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	trace_util_0.Count(_constant_00000, 22)
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx sessionctx.Context, _ chunk.Row) (float64, bool, error) {
	trace_util_0.Count(_constant_00000, 33)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 36)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 40)
			return 0, true, err
		}
		trace_util_0.Count(_constant_00000, 37)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 41)
			return 0, true, nil
		}
		trace_util_0.Count(_constant_00000, 38)
		val, err := dt.ToFloat64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			trace_util_0.Count(_constant_00000, 42)
			return 0, true, err
		}
		trace_util_0.Count(_constant_00000, 39)
		c.Value.SetFloat64(val)
	} else {
		trace_util_0.Count(_constant_00000, 43)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 44)
				return 0, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 34)
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindBinaryLiteral || c.Value.Kind() == types.KindString {
		trace_util_0.Count(_constant_00000, 45)
		res, err := c.Value.ToFloat64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	trace_util_0.Count(_constant_00000, 35)
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sessionctx.Context, _ chunk.Row) (string, bool, error) {
	trace_util_0.Count(_constant_00000, 46)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 48)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 52)
			return "", true, err
		}
		trace_util_0.Count(_constant_00000, 49)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 53)
			return "", true, nil
		}
		trace_util_0.Count(_constant_00000, 50)
		val, err := dt.ToString()
		if err != nil {
			trace_util_0.Count(_constant_00000, 54)
			return "", true, err
		}
		trace_util_0.Count(_constant_00000, 51)
		c.Value.SetString(val)
	} else {
		trace_util_0.Count(_constant_00000, 55)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 56)
				return "", true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 47)
	res, err := c.Value.ToString()
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx sessionctx.Context, _ chunk.Row) (*types.MyDecimal, bool, error) {
	trace_util_0.Count(_constant_00000, 57)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 59)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 62)
			return nil, true, err
		}
		trace_util_0.Count(_constant_00000, 60)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 63)
			return nil, true, nil
		}
		trace_util_0.Count(_constant_00000, 61)
		c.Value.SetValue(dt.GetValue())
	} else {
		trace_util_0.Count(_constant_00000, 64)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 65)
				return nil, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 58)
	res, err := c.Value.ToDecimal(ctx.GetSessionVars().StmtCtx)
	return res, err != nil, err
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx sessionctx.Context, _ chunk.Row) (val types.Time, isNull bool, err error) {
	trace_util_0.Count(_constant_00000, 66)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 68)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 73)
			return types.Time{}, true, err
		}
		trace_util_0.Count(_constant_00000, 69)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 74)
			return types.Time{}, true, nil
		}
		trace_util_0.Count(_constant_00000, 70)
		val, err := dt.ToString()
		if err != nil {
			trace_util_0.Count(_constant_00000, 75)
			return types.Time{}, true, err
		}
		trace_util_0.Count(_constant_00000, 71)
		tim, err := types.ParseDatetime(ctx.GetSessionVars().StmtCtx, val)
		if err != nil {
			trace_util_0.Count(_constant_00000, 76)
			return types.Time{}, true, err
		}
		trace_util_0.Count(_constant_00000, 72)
		c.Value.SetMysqlTime(tim)
	} else {
		trace_util_0.Count(_constant_00000, 77)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 78)
				return types.Time{}, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 67)
	return c.Value.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx sessionctx.Context, _ chunk.Row) (val types.Duration, isNull bool, err error) {
	trace_util_0.Count(_constant_00000, 79)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 81)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 86)
			return types.Duration{}, true, err
		}
		trace_util_0.Count(_constant_00000, 82)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 87)
			return types.Duration{}, true, nil
		}
		trace_util_0.Count(_constant_00000, 83)
		val, err := dt.ToString()
		if err != nil {
			trace_util_0.Count(_constant_00000, 88)
			return types.Duration{}, true, err
		}
		trace_util_0.Count(_constant_00000, 84)
		dur, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, val, types.MaxFsp)
		if err != nil {
			trace_util_0.Count(_constant_00000, 89)
			return types.Duration{}, true, err
		}
		trace_util_0.Count(_constant_00000, 85)
		c.Value.SetMysqlDuration(dur)
	} else {
		trace_util_0.Count(_constant_00000, 90)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 91)
				return types.Duration{}, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 80)
	return c.Value.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(ctx sessionctx.Context, _ chunk.Row) (json.BinaryJSON, bool, error) {
	trace_util_0.Count(_constant_00000, 92)
	if c.DeferredExpr != nil {
		trace_util_0.Count(_constant_00000, 94)
		dt, err := c.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constant_00000, 98)
			return json.BinaryJSON{}, true, err
		}
		trace_util_0.Count(_constant_00000, 95)
		if dt.IsNull() {
			trace_util_0.Count(_constant_00000, 99)
			return json.BinaryJSON{}, true, nil
		}
		trace_util_0.Count(_constant_00000, 96)
		val, err := dt.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeJSON))
		if err != nil {
			trace_util_0.Count(_constant_00000, 100)
			return json.BinaryJSON{}, true, err
		}
		trace_util_0.Count(_constant_00000, 97)
		fmt.Println("const eval json", val.GetMysqlJSON().String())
		c.Value.SetMysqlJSON(val.GetMysqlJSON())
		c.GetType().Tp = mysql.TypeJSON
	} else {
		trace_util_0.Count(_constant_00000, 101)
		{
			if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
				trace_util_0.Count(_constant_00000, 102)
				return json.BinaryJSON{}, true, nil
			}
		}
	}
	trace_util_0.Count(_constant_00000, 93)
	return c.Value.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx sessionctx.Context, b Expression) bool {
	trace_util_0.Count(_constant_00000, 103)
	y, ok := b.(*Constant)
	if !ok {
		trace_util_0.Count(_constant_00000, 107)
		return false
	}
	trace_util_0.Count(_constant_00000, 104)
	_, err1 := y.Eval(chunk.Row{})
	_, err2 := c.Eval(chunk.Row{})
	if err1 != nil || err2 != nil {
		trace_util_0.Count(_constant_00000, 108)
		return false
	}
	trace_util_0.Count(_constant_00000, 105)
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		trace_util_0.Count(_constant_00000, 109)
		return false
	}
	trace_util_0.Count(_constant_00000, 106)
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	trace_util_0.Count(_constant_00000, 110)
	return false
}

// ConstItem implements Expression interface.
func (c *Constant) ConstItem() bool {
	trace_util_0.Count(_constant_00000, 111)
	return true
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	trace_util_0.Count(_constant_00000, 112)
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode(sc *stmtctx.StatementContext) []byte {
	trace_util_0.Count(_constant_00000, 113)
	if len(c.hashcode) > 0 {
		trace_util_0.Count(_constant_00000, 117)
		return c.hashcode
	}
	trace_util_0.Count(_constant_00000, 114)
	_, err := c.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_constant_00000, 118)
		terror.Log(err)
	}
	trace_util_0.Count(_constant_00000, 115)
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode, err = codec.EncodeValue(sc, c.hashcode, c.Value)
	if err != nil {
		trace_util_0.Count(_constant_00000, 119)
		terror.Log(err)
	}
	trace_util_0.Count(_constant_00000, 116)
	return c.hashcode
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) (Expression, error) {
	trace_util_0.Count(_constant_00000, 120)
	return c, nil
}

func (c *Constant) resolveIndices(_ *Schema) error {
	trace_util_0.Count(_constant_00000, 121)
	return nil
}

var _constant_00000 = "expression/constant.go"
