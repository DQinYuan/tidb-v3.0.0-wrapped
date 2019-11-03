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
	"bytes"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
)

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalLock) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 0)
	return p.Lock.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 1)
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		trace_util_0.Count(_explain_00000, 9)
		tblName = p.TableAsName.O
	}
	trace_util_0.Count(_explain_00000, 2)
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		trace_util_0.Count(_explain_00000, 10)
		if pi := p.Table.GetPartitionInfo(); pi != nil {
			trace_util_0.Count(_explain_00000, 11)
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	trace_util_0.Count(_explain_00000, 3)
	if len(p.Index.Columns) > 0 {
		trace_util_0.Count(_explain_00000, 12)
		buffer.WriteString(", index:")
		for i, idxCol := range p.Index.Columns {
			trace_util_0.Count(_explain_00000, 13)
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.Index.Columns) {
				trace_util_0.Count(_explain_00000, 14)
				buffer.WriteString(", ")
			}
		}
	}
	trace_util_0.Count(_explain_00000, 4)
	haveCorCol := false
	for _, cond := range p.AccessCondition {
		trace_util_0.Count(_explain_00000, 15)
		if len(expression.ExtractCorColumns(cond)) > 0 {
			trace_util_0.Count(_explain_00000, 16)
			haveCorCol = true
			break
		}
	}
	trace_util_0.Count(_explain_00000, 5)
	if len(p.rangeInfo) > 0 {
		trace_util_0.Count(_explain_00000, 17)
		fmt.Fprintf(buffer, ", range: decided by %v", p.rangeInfo)
	} else {
		trace_util_0.Count(_explain_00000, 18)
		if haveCorCol {
			trace_util_0.Count(_explain_00000, 19)
			fmt.Fprintf(buffer, ", range: decided by %v", p.AccessCondition)
		} else {
			trace_util_0.Count(_explain_00000, 20)
			if len(p.Ranges) > 0 {
				trace_util_0.Count(_explain_00000, 21)
				fmt.Fprint(buffer, ", range:")
				for i, idxRange := range p.Ranges {
					trace_util_0.Count(_explain_00000, 22)
					fmt.Fprint(buffer, idxRange.String())
					if i+1 < len(p.Ranges) {
						trace_util_0.Count(_explain_00000, 23)
						fmt.Fprint(buffer, ", ")
					}
				}
			}
		}
	}
	trace_util_0.Count(_explain_00000, 6)
	fmt.Fprintf(buffer, ", keep order:%v", p.KeepOrder)
	if p.Desc {
		trace_util_0.Count(_explain_00000, 24)
		buffer.WriteString(", desc")
	}
	trace_util_0.Count(_explain_00000, 7)
	if p.stats.StatsVersion == statistics.PseudoVersion {
		trace_util_0.Count(_explain_00000, 25)
		buffer.WriteString(", stats:pseudo")
	}
	trace_util_0.Count(_explain_00000, 8)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableScan) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 26)
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		trace_util_0.Count(_explain_00000, 34)
		tblName = p.TableAsName.O
	}
	trace_util_0.Count(_explain_00000, 27)
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.isPartition {
		trace_util_0.Count(_explain_00000, 35)
		if pi := p.Table.GetPartitionInfo(); pi != nil {
			trace_util_0.Count(_explain_00000, 36)
			partitionName := pi.GetNameByID(p.physicalTableID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	trace_util_0.Count(_explain_00000, 28)
	if p.pkCol != nil {
		trace_util_0.Count(_explain_00000, 37)
		fmt.Fprintf(buffer, ", pk col:%s", p.pkCol.ExplainInfo())
	}
	trace_util_0.Count(_explain_00000, 29)
	haveCorCol := false
	for _, cond := range p.AccessCondition {
		trace_util_0.Count(_explain_00000, 38)
		if len(expression.ExtractCorColumns(cond)) > 0 {
			trace_util_0.Count(_explain_00000, 39)
			haveCorCol = true
			break
		}
	}
	trace_util_0.Count(_explain_00000, 30)
	if len(p.rangeDecidedBy) > 0 {
		trace_util_0.Count(_explain_00000, 40)
		fmt.Fprintf(buffer, ", range: decided by %v", p.rangeDecidedBy)
	} else {
		trace_util_0.Count(_explain_00000, 41)
		if haveCorCol {
			trace_util_0.Count(_explain_00000, 42)
			fmt.Fprintf(buffer, ", range: decided by %v", p.AccessCondition)
		} else {
			trace_util_0.Count(_explain_00000, 43)
			if len(p.Ranges) > 0 {
				trace_util_0.Count(_explain_00000, 44)
				fmt.Fprint(buffer, ", range:")
				for i, idxRange := range p.Ranges {
					trace_util_0.Count(_explain_00000, 45)
					fmt.Fprint(buffer, idxRange.String())
					if i+1 < len(p.Ranges) {
						trace_util_0.Count(_explain_00000, 46)
						fmt.Fprint(buffer, ", ")
					}
				}
			}
		}
	}
	trace_util_0.Count(_explain_00000, 31)
	fmt.Fprintf(buffer, ", keep order:%v", p.KeepOrder)
	if p.Desc {
		trace_util_0.Count(_explain_00000, 47)
		buffer.WriteString(", desc")
	}
	trace_util_0.Count(_explain_00000, 32)
	if p.stats.StatsVersion == statistics.PseudoVersion {
		trace_util_0.Count(_explain_00000, 48)
		buffer.WriteString(", stats:pseudo")
	}
	trace_util_0.Count(_explain_00000, 33)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 49)
	return "data:" + p.tablePlan.ExplainID().String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 50)
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 51)
	// The children can be inferred by the relation symbol.
	return ""
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 52)
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 53)
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 54)
	return string(expression.ExplainExpressionList(p.Exprs))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableDual) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 55)
	return fmt.Sprintf("rows:%v", p.RowCount)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalSort) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 56)
	buffer := bytes.NewBufferString("")
	for i, item := range p.ByItems {
		trace_util_0.Count(_explain_00000, 58)
		order := "asc"
		if item.Desc {
			trace_util_0.Count(_explain_00000, 60)
			order = "desc"
		}
		trace_util_0.Count(_explain_00000, 59)
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainInfo(), order)
		if i+1 < len(p.ByItems) {
			trace_util_0.Count(_explain_00000, 61)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 57)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 62)
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *basePhysicalAgg) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 63)
	buffer := bytes.NewBufferString("")
	if len(p.GroupByItems) > 0 {
		trace_util_0.Count(_explain_00000, 66)
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(p.GroupByItems))
	}
	trace_util_0.Count(_explain_00000, 64)
	if len(p.AggFuncs) > 0 {
		trace_util_0.Count(_explain_00000, 67)
		buffer.WriteString("funcs:")
		for i, agg := range p.AggFuncs {
			trace_util_0.Count(_explain_00000, 68)
			buffer.WriteString(aggregation.ExplainAggFunc(agg))
			if i+1 < len(p.AggFuncs) {
				trace_util_0.Count(_explain_00000, 69)
				buffer.WriteString(", ")
			}
		}
	}
	trace_util_0.Count(_explain_00000, 65)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 70)
	buffer := bytes.NewBufferString(p.JoinType.String())
	fmt.Fprintf(buffer, ", inner:%s", p.Children()[1-p.OuterIndex].ExplainID())
	if len(p.OuterJoinKeys) > 0 {
		trace_util_0.Count(_explain_00000, 76)
		fmt.Fprintf(buffer, ", outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys))
	}
	trace_util_0.Count(_explain_00000, 71)
	if len(p.InnerJoinKeys) > 0 {
		trace_util_0.Count(_explain_00000, 77)
		fmt.Fprintf(buffer, ", inner key:%s",
			expression.ExplainColumnList(p.InnerJoinKeys))
	}
	trace_util_0.Count(_explain_00000, 72)
	if len(p.LeftConditions) > 0 {
		trace_util_0.Count(_explain_00000, 78)
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(p.LeftConditions))
	}
	trace_util_0.Count(_explain_00000, 73)
	if len(p.RightConditions) > 0 {
		trace_util_0.Count(_explain_00000, 79)
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	trace_util_0.Count(_explain_00000, 74)
	if len(p.OtherConditions) > 0 {
		trace_util_0.Count(_explain_00000, 80)
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	trace_util_0.Count(_explain_00000, 75)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 81)
	buffer := bytes.NewBufferString(p.JoinType.String())
	fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	if len(p.EqualConditions) > 0 {
		trace_util_0.Count(_explain_00000, 86)
		fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
	}
	trace_util_0.Count(_explain_00000, 82)
	if len(p.LeftConditions) > 0 {
		trace_util_0.Count(_explain_00000, 87)
		fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
	}
	trace_util_0.Count(_explain_00000, 83)
	if len(p.RightConditions) > 0 {
		trace_util_0.Count(_explain_00000, 88)
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	trace_util_0.Count(_explain_00000, 84)
	if len(p.OtherConditions) > 0 {
		trace_util_0.Count(_explain_00000, 89)
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	trace_util_0.Count(_explain_00000, 85)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 90)
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.LeftKeys) > 0 {
		trace_util_0.Count(_explain_00000, 96)
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainColumnList(p.LeftKeys))
	}
	trace_util_0.Count(_explain_00000, 91)
	if len(p.RightKeys) > 0 {
		trace_util_0.Count(_explain_00000, 97)
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainColumnList(p.RightKeys))
	}
	trace_util_0.Count(_explain_00000, 92)
	if len(p.LeftConditions) > 0 {
		trace_util_0.Count(_explain_00000, 98)
		fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
	}
	trace_util_0.Count(_explain_00000, 93)
	if len(p.RightConditions) > 0 {
		trace_util_0.Count(_explain_00000, 99)
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	trace_util_0.Count(_explain_00000, 94)
	if len(p.OtherConditions) > 0 {
		trace_util_0.Count(_explain_00000, 100)
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	trace_util_0.Count(_explain_00000, 95)
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTopN) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 101)
	buffer := bytes.NewBufferString("")
	for i, item := range p.ByItems {
		trace_util_0.Count(_explain_00000, 103)
		order := "asc"
		if item.Desc {
			trace_util_0.Count(_explain_00000, 105)
			order = "desc"
		}
		trace_util_0.Count(_explain_00000, 104)
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainInfo(), order)
		if i+1 < len(p.ByItems) {
			trace_util_0.Count(_explain_00000, 106)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 102)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}

func (p *PhysicalWindow) formatFrameBound(buffer *bytes.Buffer, bound *FrameBound) {
	trace_util_0.Count(_explain_00000, 107)
	if bound.Type == ast.CurrentRow {
		trace_util_0.Count(_explain_00000, 110)
		buffer.WriteString("current row")
		return
	}
	trace_util_0.Count(_explain_00000, 108)
	if bound.UnBounded {
		trace_util_0.Count(_explain_00000, 111)
		buffer.WriteString("unbounded")
	} else {
		trace_util_0.Count(_explain_00000, 112)
		if len(bound.CalcFuncs) > 0 {
			trace_util_0.Count(_explain_00000, 113)
			sf := bound.CalcFuncs[0].(*expression.ScalarFunction)
			switch sf.FuncName.L {
			case ast.DateAdd, ast.DateSub:
				trace_util_0.Count(_explain_00000, 114)
				// For `interval '2:30' minute_second`.
				fmt.Fprintf(buffer, "interval %s %s", sf.GetArgs()[1].ExplainInfo(), sf.GetArgs()[2].ExplainInfo())
			case ast.Plus, ast.Minus:
				trace_util_0.Count(_explain_00000, 115)
				// For `1 preceding` of range frame.
				fmt.Fprintf(buffer, "%s", sf.GetArgs()[1].ExplainInfo())
			}
		} else {
			trace_util_0.Count(_explain_00000, 116)
			{
				fmt.Fprintf(buffer, "%d", bound.Num)
			}
		}
	}
	trace_util_0.Count(_explain_00000, 109)
	if bound.Type == ast.Preceding {
		trace_util_0.Count(_explain_00000, 117)
		buffer.WriteString(" preceding")
	} else {
		trace_util_0.Count(_explain_00000, 118)
		{
			buffer.WriteString(" following")
		}
	}
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalWindow) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 119)
	buffer := bytes.NewBufferString("")
	formatWindowFuncDescs(buffer, p.WindowFuncDescs)
	buffer.WriteString(" over(")
	isFirst := true
	if len(p.PartitionBy) > 0 {
		trace_util_0.Count(_explain_00000, 123)
		buffer.WriteString("partition by ")
		for i, item := range p.PartitionBy {
			trace_util_0.Count(_explain_00000, 125)
			fmt.Fprintf(buffer, "%s", item.Col.ExplainInfo())
			if i+1 < len(p.PartitionBy) {
				trace_util_0.Count(_explain_00000, 126)
				buffer.WriteString(", ")
			}
		}
		trace_util_0.Count(_explain_00000, 124)
		isFirst = false
	}
	trace_util_0.Count(_explain_00000, 120)
	if len(p.OrderBy) > 0 {
		trace_util_0.Count(_explain_00000, 127)
		if !isFirst {
			trace_util_0.Count(_explain_00000, 130)
			buffer.WriteString(" ")
		}
		trace_util_0.Count(_explain_00000, 128)
		buffer.WriteString("order by ")
		for i, item := range p.OrderBy {
			trace_util_0.Count(_explain_00000, 131)
			order := "asc"
			if item.Desc {
				trace_util_0.Count(_explain_00000, 133)
				order = "desc"
			}
			trace_util_0.Count(_explain_00000, 132)
			fmt.Fprintf(buffer, "%s %s", item.Col.ExplainInfo(), order)
			if i+1 < len(p.OrderBy) {
				trace_util_0.Count(_explain_00000, 134)
				buffer.WriteString(", ")
			}
		}
		trace_util_0.Count(_explain_00000, 129)
		isFirst = false
	}
	trace_util_0.Count(_explain_00000, 121)
	if p.Frame != nil {
		trace_util_0.Count(_explain_00000, 135)
		if !isFirst {
			trace_util_0.Count(_explain_00000, 138)
			buffer.WriteString(" ")
		}
		trace_util_0.Count(_explain_00000, 136)
		if p.Frame.Type == ast.Rows {
			trace_util_0.Count(_explain_00000, 139)
			buffer.WriteString("rows")
		} else {
			trace_util_0.Count(_explain_00000, 140)
			{
				buffer.WriteString("range")
			}
		}
		trace_util_0.Count(_explain_00000, 137)
		buffer.WriteString(" between ")
		p.formatFrameBound(buffer, p.Frame.Start)
		buffer.WriteString(" and ")
		p.formatFrameBound(buffer, p.Frame.End)
	}
	trace_util_0.Count(_explain_00000, 122)
	buffer.WriteString(")")
	return buffer.String()
}

func formatWindowFuncDescs(buffer *bytes.Buffer, descs []*aggregation.WindowFuncDesc) *bytes.Buffer {
	trace_util_0.Count(_explain_00000, 141)
	for i, desc := range descs {
		trace_util_0.Count(_explain_00000, 143)
		if i != 0 {
			trace_util_0.Count(_explain_00000, 145)
			buffer.WriteString(", ")
		}
		trace_util_0.Count(_explain_00000, 144)
		buffer.WriteString(desc.String())
	}
	trace_util_0.Count(_explain_00000, 142)
	return buffer
}

var _explain_00000 = "planner/core/explain.go"
