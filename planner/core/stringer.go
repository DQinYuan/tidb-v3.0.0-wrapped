// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/tidb/trace_util_0"
	"strings"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	trace_util_0.Count(_stringer_00000, 0)
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	trace_util_0.Count(_stringer_00000, 1)
	switch x := in.(type) {
	case LogicalPlan:
		trace_util_0.Count(_stringer_00000, 4)
		if len(x.Children()) > 1 {
			trace_util_0.Count(_stringer_00000, 8)
			idxs = append(idxs, len(strs))
		}

		trace_util_0.Count(_stringer_00000, 5)
		for _, c := range x.Children() {
			trace_util_0.Count(_stringer_00000, 9)
			strs, idxs = toString(c, strs, idxs)
		}
	case PhysicalPlan:
		trace_util_0.Count(_stringer_00000, 6)
		if len(x.Children()) > 1 {
			trace_util_0.Count(_stringer_00000, 10)
			idxs = append(idxs, len(strs))
		}

		trace_util_0.Count(_stringer_00000, 7)
		for _, c := range x.Children() {
			trace_util_0.Count(_stringer_00000, 11)
			strs, idxs = toString(c, strs, idxs)
		}
	}

	trace_util_0.Count(_stringer_00000, 2)
	var str string
	switch x := in.(type) {
	case *CheckTable:
		trace_util_0.Count(_stringer_00000, 12)
		str = "CheckTable"
	case *PhysicalIndexScan:
		trace_util_0.Count(_stringer_00000, 13)
		str = fmt.Sprintf("Index(%s.%s)%v", x.Table.Name.L, x.Index.Name.L, x.Ranges)
	case *PhysicalTableScan:
		trace_util_0.Count(_stringer_00000, 14)
		str = fmt.Sprintf("Table(%s)", x.Table.Name.L)
	case *PhysicalHashJoin:
		trace_util_0.Count(_stringer_00000, 15)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		if x.InnerChildIdx == 0 {
			trace_util_0.Count(_stringer_00000, 53)
			str = "RightHashJoin{" + strings.Join(children, "->") + "}"
		} else {
			trace_util_0.Count(_stringer_00000, 54)
			{
				str = "LeftHashJoin{" + strings.Join(children, "->") + "}"
			}
		}
		trace_util_0.Count(_stringer_00000, 16)
		for _, eq := range x.EqualConditions {
			trace_util_0.Count(_stringer_00000, 55)
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalMergeJoin:
		trace_util_0.Count(_stringer_00000, 17)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		id := "MergeJoin"
		switch x.JoinType {
		case SemiJoin:
			trace_util_0.Count(_stringer_00000, 56)
			id = "MergeSemiJoin"
		case AntiSemiJoin:
			trace_util_0.Count(_stringer_00000, 57)
			id = "MergeAntiSemiJoin"
		case LeftOuterSemiJoin:
			trace_util_0.Count(_stringer_00000, 58)
			id = "MergeLeftOuterSemiJoin"
		case AntiLeftOuterSemiJoin:
			trace_util_0.Count(_stringer_00000, 59)
			id = "MergeAntiLeftOuterSemiJoin"
		case LeftOuterJoin:
			trace_util_0.Count(_stringer_00000, 60)
			id = "MergeLeftOuterJoin"
		case RightOuterJoin:
			trace_util_0.Count(_stringer_00000, 61)
			id = "MergeRightOuterJoin"
		case InnerJoin:
			trace_util_0.Count(_stringer_00000, 62)
			id = "MergeInnerJoin"
		}
		trace_util_0.Count(_stringer_00000, 18)
		str = id + "{" + strings.Join(children, "->") + "}"
		for i := range x.LeftKeys {
			trace_util_0.Count(_stringer_00000, 63)
			l := x.LeftKeys[i].String()
			r := x.RightKeys[i].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalApply, *PhysicalApply:
		trace_util_0.Count(_stringer_00000, 19)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
	case *LogicalMaxOneRow, *PhysicalMaxOneRow:
		trace_util_0.Count(_stringer_00000, 20)
		str = "MaxOneRow"
	case *LogicalLimit, *PhysicalLimit:
		trace_util_0.Count(_stringer_00000, 21)
		str = "Limit"
	case *PhysicalLock, *LogicalLock:
		trace_util_0.Count(_stringer_00000, 22)
		str = "Lock"
	case *ShowDDL:
		trace_util_0.Count(_stringer_00000, 23)
		str = "ShowDDL"
	case *Show:
		trace_util_0.Count(_stringer_00000, 24)
		if len(x.Conditions) == 0 {
			trace_util_0.Count(_stringer_00000, 64)
			str = "Show"
		} else {
			trace_util_0.Count(_stringer_00000, 65)
			{
				str = fmt.Sprintf("Show(%s)", x.Conditions)
			}
		}
	case *LogicalSort, *PhysicalSort:
		trace_util_0.Count(_stringer_00000, 25)
		str = "Sort"
	case *LogicalJoin:
		trace_util_0.Count(_stringer_00000, 26)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
		for _, eq := range x.EqualConditions {
			trace_util_0.Count(_stringer_00000, 66)
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalUnionAll, *PhysicalUnionAll:
		trace_util_0.Count(_stringer_00000, 27)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "UnionAll{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *DataSource:
		trace_util_0.Count(_stringer_00000, 28)
		if x.isPartition {
			trace_util_0.Count(_stringer_00000, 67)
			str = fmt.Sprintf("Partition(%d)", x.physicalTableID)
		} else {
			trace_util_0.Count(_stringer_00000, 68)
			{
				if x.TableAsName != nil && x.TableAsName.L != "" {
					trace_util_0.Count(_stringer_00000, 69)
					str = fmt.Sprintf("DataScan(%s)", x.TableAsName)
				} else {
					trace_util_0.Count(_stringer_00000, 70)
					{
						str = fmt.Sprintf("DataScan(%s)", x.tableInfo.Name)
					}
				}
			}
		}
	case *LogicalSelection:
		trace_util_0.Count(_stringer_00000, 29)
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *PhysicalSelection:
		trace_util_0.Count(_stringer_00000, 30)
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *LogicalProjection, *PhysicalProjection:
		trace_util_0.Count(_stringer_00000, 31)
		str = "Projection"
	case *LogicalTopN:
		trace_util_0.Count(_stringer_00000, 32)
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *PhysicalTopN:
		trace_util_0.Count(_stringer_00000, 33)
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *LogicalTableDual, *PhysicalTableDual:
		trace_util_0.Count(_stringer_00000, 34)
		str = "Dual"
	case *PhysicalHashAgg:
		trace_util_0.Count(_stringer_00000, 35)
		str = "HashAgg"
	case *PhysicalStreamAgg:
		trace_util_0.Count(_stringer_00000, 36)
		str = "StreamAgg"
	case *LogicalAggregation:
		trace_util_0.Count(_stringer_00000, 37)
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			trace_util_0.Count(_stringer_00000, 71)
			str += aggFunc.String()
			if i != len(x.AggFuncs)-1 {
				trace_util_0.Count(_stringer_00000, 72)
				str += ","
			}
		}
		trace_util_0.Count(_stringer_00000, 38)
		str += ")"
	case *PhysicalTableReader:
		trace_util_0.Count(_stringer_00000, 39)
		str = fmt.Sprintf("TableReader(%s)", ToString(x.tablePlan))
	case *PhysicalIndexReader:
		trace_util_0.Count(_stringer_00000, 40)
		str = fmt.Sprintf("IndexReader(%s)", ToString(x.indexPlan))
	case *PhysicalIndexLookUpReader:
		trace_util_0.Count(_stringer_00000, 41)
		str = fmt.Sprintf("IndexLookUp(%s, %s)", ToString(x.indexPlan), ToString(x.tablePlan))
	case *PhysicalUnionScan:
		trace_util_0.Count(_stringer_00000, 42)
		str = fmt.Sprintf("UnionScan(%s)", x.Conditions)
	case *PhysicalIndexJoin:
		trace_util_0.Count(_stringer_00000, 43)
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			trace_util_0.Count(_stringer_00000, 73)
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *Analyze:
		trace_util_0.Count(_stringer_00000, 44)
		str = "Analyze{"
		var children []string
		for _, idx := range x.IdxTasks {
			trace_util_0.Count(_stringer_00000, 74)
			children = append(children, fmt.Sprintf("Index(%s)", idx.IndexInfo.Name.O))
		}
		trace_util_0.Count(_stringer_00000, 45)
		for _, col := range x.ColTasks {
			trace_util_0.Count(_stringer_00000, 75)
			var colNames []string
			if col.PKInfo != nil {
				trace_util_0.Count(_stringer_00000, 78)
				colNames = append(colNames, col.PKInfo.Name.O)
			}
			trace_util_0.Count(_stringer_00000, 76)
			for _, c := range col.ColsInfo {
				trace_util_0.Count(_stringer_00000, 79)
				colNames = append(colNames, c.Name.O)
			}
			trace_util_0.Count(_stringer_00000, 77)
			children = append(children, fmt.Sprintf("Table(%s)", strings.Join(colNames, ", ")))
		}
		trace_util_0.Count(_stringer_00000, 46)
		str = str + strings.Join(children, ",") + "}"
	case *Update:
		trace_util_0.Count(_stringer_00000, 47)
		str = fmt.Sprintf("%s->Update", ToString(x.SelectPlan))
	case *Delete:
		trace_util_0.Count(_stringer_00000, 48)
		str = fmt.Sprintf("%s->Delete", ToString(x.SelectPlan))
	case *Insert:
		trace_util_0.Count(_stringer_00000, 49)
		str = "Insert"
		if x.SelectPlan != nil {
			trace_util_0.Count(_stringer_00000, 80)
			str = fmt.Sprintf("%s->Insert", ToString(x.SelectPlan))
		}
	case *LogicalWindow:
		trace_util_0.Count(_stringer_00000, 50)
		buffer := bytes.NewBufferString("")
		formatWindowFuncDescs(buffer, x.WindowFuncDescs)
		str = fmt.Sprintf("Window(%s)", buffer.String())
	case *PhysicalWindow:
		trace_util_0.Count(_stringer_00000, 51)
		str = fmt.Sprintf("Window(%s)", x.ExplainInfo())
	default:
		trace_util_0.Count(_stringer_00000, 52)
		str = fmt.Sprintf("%T", in)
	}
	trace_util_0.Count(_stringer_00000, 3)
	strs = append(strs, str)
	return strs, idxs
}

var _stringer_00000 = "planner/core/stringer.go"
