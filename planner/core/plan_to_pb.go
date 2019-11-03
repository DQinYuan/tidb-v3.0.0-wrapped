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

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 0)
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 1)
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	aggExec := &tipb.Aggregation{
		GroupBy: expression.ExpressionsToPBList(sc, p.GroupByItems, client),
	}
	for _, aggFunc := range p.AggFuncs {
		trace_util_0.Count(_plan_to_pb_00000, 3)
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	trace_util_0.Count(_plan_to_pb_00000, 2)
	return &tipb.Executor{Tp: tipb.ExecType_TypeAggregation, Aggregation: aggExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 4)
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	aggExec := &tipb.Aggregation{
		GroupBy: expression.ExpressionsToPBList(sc, p.GroupByItems, client),
	}
	for _, aggFunc := range p.AggFuncs {
		trace_util_0.Count(_plan_to_pb_00000, 6)
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	trace_util_0.Count(_plan_to_pb_00000, 5)
	return &tipb.Executor{Tp: tipb.ExecType_TypeStreamAgg, Aggregation: aggExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 7)
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	selExec := &tipb.Selection{
		Conditions: expression.ExpressionsToPBList(sc, p.Conditions, client),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 8)
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		trace_util_0.Count(_plan_to_pb_00000, 10)
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	trace_util_0.Count(_plan_to_pb_00000, 9)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: topNExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 11)
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 12)
	columns := p.Columns
	tsExec := &tipb.TableScan{
		TableId: p.Table.ID,
		Columns: model.ColumnsToProto(columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	err := SetPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec}, err
}

// checkCoverIndex checks whether we can pass unique info to TiKV. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *model.IndexInfo, ranges []*ranger.Range) bool {
	trace_util_0.Count(_plan_to_pb_00000, 13)
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		trace_util_0.Count(_plan_to_pb_00000, 16)
		return false
	}
	trace_util_0.Count(_plan_to_pb_00000, 14)
	for _, rg := range ranges {
		trace_util_0.Count(_plan_to_pb_00000, 17)
		if len(rg.LowVal) != len(idx.Columns) {
			trace_util_0.Count(_plan_to_pb_00000, 18)
			return false
		}
	}
	trace_util_0.Count(_plan_to_pb_00000, 15)
	return true
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	trace_util_0.Count(_plan_to_pb_00000, 19)
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.schema.Columns {
		trace_util_0.Count(_plan_to_pb_00000, 21)
		if col.ID == model.ExtraHandleID {
			trace_util_0.Count(_plan_to_pb_00000, 22)
			columns = append(columns, model.NewExtraHandleColInfo())
		} else {
			trace_util_0.Count(_plan_to_pb_00000, 23)
			{
				columns = append(columns, model.FindColumnInfo(tableColumns, col.ColName.L))
			}
		}
	}
	trace_util_0.Count(_plan_to_pb_00000, 20)
	idxExec := &tipb.IndexScan{
		TableId: p.Table.ID,
		IndexId: p.Index.ID,
		Columns: model.ColumnsToProto(columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func SetPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	trace_util_0.Count(_plan_to_pb_00000, 24)
	for i, c := range columns {
		trace_util_0.Count(_plan_to_pb_00000, 26)
		if c.OriginDefaultValue == nil {
			trace_util_0.Count(_plan_to_pb_00000, 29)
			continue
		}

		trace_util_0.Count(_plan_to_pb_00000, 27)
		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			trace_util_0.Count(_plan_to_pb_00000, 30)
			return err
		}

		trace_util_0.Count(_plan_to_pb_00000, 28)
		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx, d)
		if err != nil {
			trace_util_0.Count(_plan_to_pb_00000, 31)
			return err
		}
	}
	trace_util_0.Count(_plan_to_pb_00000, 25)
	return nil
}

// SupportStreaming returns true if a pushed down operation supports using coprocessor streaming API.
// Note that this function handle pushed down physical plan only! It's called in constructDAGReq.
// Some plans are difficult (if possible) to implement streaming, and some are pointless to do so.
// TODO: Support more kinds of physical plan.
func SupportStreaming(p PhysicalPlan) bool {
	trace_util_0.Count(_plan_to_pb_00000, 32)
	switch p.(type) {
	case *PhysicalTableScan, *PhysicalIndexScan, *PhysicalSelection:
		trace_util_0.Count(_plan_to_pb_00000, 34)
		return true
	}
	trace_util_0.Count(_plan_to_pb_00000, 33)
	return false
}

var _plan_to_pb_00000 = "planner/core/plan_to_pb.go"
