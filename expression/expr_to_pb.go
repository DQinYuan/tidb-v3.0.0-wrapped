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
	"sync/atomic"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExpressionsToPB converts expression to tipb.Expr.
func ExpressionsToPB(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbCNF *tipb.Expr, pushed []Expression, remained []Expression) {
	trace_util_0.Count(_expr_to_pb_00000, 0)
	pc := PbConverter{client: client, sc: sc}
	retTypeOfAnd := &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    1,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}

	for _, expr := range exprs {
		trace_util_0.Count(_expr_to_pb_00000, 2)
		pbExpr := pc.ExprToPB(expr)
		if pbExpr == nil {
			trace_util_0.Count(_expr_to_pb_00000, 5)
			remained = append(remained, expr)
			continue
		}

		trace_util_0.Count(_expr_to_pb_00000, 3)
		pushed = append(pushed, expr)
		if pbCNF == nil {
			trace_util_0.Count(_expr_to_pb_00000, 6)
			pbCNF = pbExpr
			continue
		}

		// Merge multiple converted pb expression into a CNF.
		trace_util_0.Count(_expr_to_pb_00000, 4)
		pbCNF = &tipb.Expr{
			Tp:        tipb.ExprType_ScalarFunc,
			Sig:       tipb.ScalarFuncSig_LogicalAnd,
			Children:  []*tipb.Expr{pbCNF, pbExpr},
			FieldType: ToPBFieldType(retTypeOfAnd),
		}
	}
	trace_util_0.Count(_expr_to_pb_00000, 1)
	return
}

// ExpressionsToPBList converts expressions to tipb.Expr list for new plan.
func ExpressionsToPBList(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client) (pbExpr []*tipb.Expr) {
	trace_util_0.Count(_expr_to_pb_00000, 7)
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		trace_util_0.Count(_expr_to_pb_00000, 9)
		v := pc.ExprToPB(expr)
		pbExpr = append(pbExpr, v)
	}
	trace_util_0.Count(_expr_to_pb_00000, 8)
	return
}

// PbConverter supplys methods to convert TiDB expressions to TiPB.
type PbConverter struct {
	client kv.Client
	sc     *stmtctx.StatementContext
}

// NewPBConverter creates a PbConverter.
func NewPBConverter(client kv.Client, sc *stmtctx.StatementContext) PbConverter {
	trace_util_0.Count(_expr_to_pb_00000, 10)
	return PbConverter{client: client, sc: sc}
}

// ExprToPB converts Expression to TiPB.
func (pc PbConverter) ExprToPB(expr Expression) *tipb.Expr {
	trace_util_0.Count(_expr_to_pb_00000, 11)
	switch x := expr.(type) {
	case *Constant, *CorrelatedColumn:
		trace_util_0.Count(_expr_to_pb_00000, 13)
		return pc.conOrCorColToPBExpr(expr)
	case *Column:
		trace_util_0.Count(_expr_to_pb_00000, 14)
		return pc.columnToPBExpr(x)
	case *ScalarFunction:
		trace_util_0.Count(_expr_to_pb_00000, 15)
		return pc.scalarFuncToPBExpr(x)
	}
	trace_util_0.Count(_expr_to_pb_00000, 12)
	return nil
}

func (pc PbConverter) conOrCorColToPBExpr(expr Expression) *tipb.Expr {
	trace_util_0.Count(_expr_to_pb_00000, 16)
	ft := expr.GetType()
	d, err := expr.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_expr_to_pb_00000, 20)
		logutil.Logger(context.Background()).Error("eval constant or correlated column", zap.String("expression", expr.ExplainInfo()), zap.Error(err))
		return nil
	}
	trace_util_0.Count(_expr_to_pb_00000, 17)
	tp, val, ok := pc.encodeDatum(ft, d)
	if !ok {
		trace_util_0.Count(_expr_to_pb_00000, 21)
		return nil
	}

	trace_util_0.Count(_expr_to_pb_00000, 18)
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		trace_util_0.Count(_expr_to_pb_00000, 22)
		return nil
	}
	trace_util_0.Count(_expr_to_pb_00000, 19)
	return &tipb.Expr{Tp: tp, Val: val, FieldType: ToPBFieldType(ft)}
}

func (pc *PbConverter) encodeDatum(ft *types.FieldType, d types.Datum) (tipb.ExprType, []byte, bool) {
	trace_util_0.Count(_expr_to_pb_00000, 23)
	var (
		tp  tipb.ExprType
		val []byte
	)
	switch d.Kind() {
	case types.KindNull:
		trace_util_0.Count(_expr_to_pb_00000, 25)
		tp = tipb.ExprType_Null
	case types.KindInt64:
		trace_util_0.Count(_expr_to_pb_00000, 26)
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		trace_util_0.Count(_expr_to_pb_00000, 27)
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString, types.KindBinaryLiteral:
		trace_util_0.Count(_expr_to_pb_00000, 28)
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		trace_util_0.Count(_expr_to_pb_00000, 29)
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		trace_util_0.Count(_expr_to_pb_00000, 30)
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		trace_util_0.Count(_expr_to_pb_00000, 31)
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		trace_util_0.Count(_expr_to_pb_00000, 32)
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		trace_util_0.Count(_expr_to_pb_00000, 33)
		tp = tipb.ExprType_MysqlDecimal
		var err error
		val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil {
			trace_util_0.Count(_expr_to_pb_00000, 37)
			logutil.Logger(context.Background()).Error("encode decimal", zap.Error(err))
			return tp, nil, false
		}
	case types.KindMysqlTime:
		trace_util_0.Count(_expr_to_pb_00000, 34)
		if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, int64(tipb.ExprType_MysqlTime)) {
			trace_util_0.Count(_expr_to_pb_00000, 38)
			tp = tipb.ExprType_MysqlTime
			val, err := codec.EncodeMySQLTime(pc.sc, d, ft.Tp, nil)
			if err != nil {
				trace_util_0.Count(_expr_to_pb_00000, 40)
				logutil.Logger(context.Background()).Error("encode mysql time", zap.Error(err))
				return tp, nil, false
			}
			trace_util_0.Count(_expr_to_pb_00000, 39)
			return tp, val, true
		}
		trace_util_0.Count(_expr_to_pb_00000, 35)
		return tp, nil, false
	default:
		trace_util_0.Count(_expr_to_pb_00000, 36)
		return tp, nil, false
	}
	trace_util_0.Count(_expr_to_pb_00000, 24)
	return tp, val, true
}

// ToPBFieldType converts *types.FieldType to *tipb.FieldType.
func ToPBFieldType(ft *types.FieldType) *tipb.FieldType {
	trace_util_0.Count(_expr_to_pb_00000, 41)
	return &tipb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		Collate: collationToProto(ft.Collate),
	}
}

func collationToProto(c string) int32 {
	trace_util_0.Count(_expr_to_pb_00000, 42)
	v, ok := mysql.CollationNames[c]
	if ok {
		trace_util_0.Count(_expr_to_pb_00000, 44)
		return int32(v)
	}
	trace_util_0.Count(_expr_to_pb_00000, 43)
	return int32(mysql.DefaultCollationID)
}

func (pc PbConverter) columnToPBExpr(column *Column) *tipb.Expr {
	trace_util_0.Count(_expr_to_pb_00000, 45)
	if !pc.client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		trace_util_0.Count(_expr_to_pb_00000, 50)
		return nil
	}
	trace_util_0.Count(_expr_to_pb_00000, 46)
	switch column.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeUnspecified:
		trace_util_0.Count(_expr_to_pb_00000, 51)
		return nil
	}

	trace_util_0.Count(_expr_to_pb_00000, 47)
	if pc.client.IsRequestTypeSupported(kv.ReqTypeDAG, kv.ReqSubTypeBasic) {
		trace_util_0.Count(_expr_to_pb_00000, 52)
		return &tipb.Expr{
			Tp:        tipb.ExprType_ColumnRef,
			Val:       codec.EncodeInt(nil, int64(column.Index)),
			FieldType: ToPBFieldType(column.RetType),
		}
	}
	trace_util_0.Count(_expr_to_pb_00000, 48)
	id := column.ID
	// Zero Column ID is not a column from table, can not support for now.
	if id == 0 || id == -1 {
		trace_util_0.Count(_expr_to_pb_00000, 53)
		return nil
	}

	trace_util_0.Count(_expr_to_pb_00000, 49)
	return &tipb.Expr{
		Tp:  tipb.ExprType_ColumnRef,
		Val: codec.EncodeInt(nil, id)}
}

func (pc PbConverter) scalarFuncToPBExpr(expr *ScalarFunction) *tipb.Expr {
	trace_util_0.Count(_expr_to_pb_00000, 54)
	// check whether this function can be pushed.
	if !pc.canFuncBePushed(expr) {
		trace_util_0.Count(_expr_to_pb_00000, 58)
		return nil
	}

	// check whether this function has ProtoBuf signature.
	trace_util_0.Count(_expr_to_pb_00000, 55)
	pbCode := expr.Function.PbCode()
	if pbCode < 0 {
		trace_util_0.Count(_expr_to_pb_00000, 59)
		return nil
	}

	// check whether all of its parameters can be pushed.
	trace_util_0.Count(_expr_to_pb_00000, 56)
	children := make([]*tipb.Expr, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		trace_util_0.Count(_expr_to_pb_00000, 60)
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			trace_util_0.Count(_expr_to_pb_00000, 62)
			return nil
		}
		trace_util_0.Count(_expr_to_pb_00000, 61)
		children = append(children, pbArg)
	}

	// construct expression ProtoBuf.
	trace_util_0.Count(_expr_to_pb_00000, 57)
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Sig:       pbCode,
		Children:  children,
		FieldType: ToPBFieldType(expr.RetType),
	}
}

// GroupByItemToPB converts group by items to pb.
func GroupByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression) *tipb.ByItem {
	trace_util_0.Count(_expr_to_pb_00000, 63)
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		trace_util_0.Count(_expr_to_pb_00000, 65)
		return nil
	}
	trace_util_0.Count(_expr_to_pb_00000, 64)
	return &tipb.ByItem{Expr: e}
}

// SortByItemToPB converts order by items to pb.
func SortByItemToPB(sc *stmtctx.StatementContext, client kv.Client, expr Expression, desc bool) *tipb.ByItem {
	trace_util_0.Count(_expr_to_pb_00000, 66)
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		trace_util_0.Count(_expr_to_pb_00000, 68)
		return nil
	}
	trace_util_0.Count(_expr_to_pb_00000, 67)
	return &tipb.ByItem{Expr: e, Desc: desc}
}

func (pc PbConverter) canFuncBePushed(sf *ScalarFunction) bool {
	trace_util_0.Count(_expr_to_pb_00000, 69)
	switch sf.FuncName.L {
	case
		// logical functions.
		ast.LogicAnd,
		ast.LogicOr,
		ast.UnaryNot,

		// compare functions.
		ast.LT,
		ast.LE,
		ast.EQ,
		ast.NE,
		ast.GE,
		ast.GT,
		ast.NullEQ,
		ast.In,
		ast.IsNull,
		ast.Like,
		ast.IsTruth,
		ast.IsFalsity,

		// arithmetical functions.
		ast.Plus,
		ast.Minus,
		ast.Mul,
		ast.Div,
		ast.Abs,
		ast.Ceil,
		ast.Ceiling,
		ast.Floor,

		// control flow functions.
		ast.Case,
		ast.If,
		ast.Ifnull,
		ast.Coalesce,

		// json functions.
		ast.JSONType,
		ast.JSONExtract,
		ast.JSONUnquote,
		ast.JSONObject,
		ast.JSONArray,
		ast.JSONMerge,
		ast.JSONSet,
		ast.JSONInsert,
		ast.JSONReplace,
		ast.JSONRemove,

		// date functions.
		ast.DateFormat:
		trace_util_0.Count(_expr_to_pb_00000, 71)
		_, disallowPushdown := DefaultExprPushdownBlacklist.Load().(map[string]struct{})[sf.FuncName.L]
		return true && !disallowPushdown
	}
	trace_util_0.Count(_expr_to_pb_00000, 70)
	return false
}

// DefaultExprPushdownBlacklist indicates the expressions which can not be pushed down to TiKV.
var DefaultExprPushdownBlacklist *atomic.Value

func init() {
	trace_util_0.Count(_expr_to_pb_00000, 72)
	DefaultExprPushdownBlacklist = new(atomic.Value)
	DefaultExprPushdownBlacklist.Store(make(map[string]struct{}))
}

var _expr_to_pb_00000 = "expression/expr_to_pb.go"
