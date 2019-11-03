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

package property

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/codec"
)

// Item wraps the column and its order.
type Item struct {
	Col  *expression.Column
	Desc bool
}

// PhysicalProperty stands for the required physical property by parents.
// It contains the orders and the task types.
type PhysicalProperty struct {
	Items []Item

	// TaskTp means the type of task that an operator requires.
	//
	// It needs to be specified because two different tasks can't be compared
	// with cost directly. e.g. If a copTask takes less cost than a rootTask,
	// we can't sure that we must choose the former one. Because the copTask
	// must be finished and increase its cost in sometime, but we can't make
	// sure the finishing time. So the best way to let the comparison fair is
	// to add TaskType to required property.
	TaskTp TaskType

	// ExpectedCnt means this operator may be closed after fetching ExpectedCnt
	// records.
	ExpectedCnt float64

	// hashcode stores the hash code of a PhysicalProperty, will be lazily
	// calculated when function "HashCode()" being called.
	hashcode []byte

	// whether need to enforce property.
	Enforced bool
}

// NewPhysicalProperty builds property from columns.
func NewPhysicalProperty(taskTp TaskType, cols []*expression.Column, desc bool, expectCnt float64, enforced bool) *PhysicalProperty {
	trace_util_0.Count(_physical_property_00000, 0)
	return &PhysicalProperty{
		Items:       ItemsFromCols(cols, desc),
		TaskTp:      taskTp,
		ExpectedCnt: expectCnt,
		Enforced:    enforced,
	}
}

// ItemsFromCols builds property items from columns.
func ItemsFromCols(cols []*expression.Column, desc bool) []Item {
	trace_util_0.Count(_physical_property_00000, 1)
	items := make([]Item, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_physical_property_00000, 3)
		items = append(items, Item{Col: col, Desc: desc})
	}
	trace_util_0.Count(_physical_property_00000, 2)
	return items
}

// AllColsFromSchema checks whether all the columns needed by this physical
// property can be found in the given schema.
func (p *PhysicalProperty) AllColsFromSchema(schema *expression.Schema) bool {
	trace_util_0.Count(_physical_property_00000, 4)
	for _, col := range p.Items {
		trace_util_0.Count(_physical_property_00000, 6)
		if schema.ColumnIndex(col.Col) == -1 {
			trace_util_0.Count(_physical_property_00000, 7)
			return false
		}
	}
	trace_util_0.Count(_physical_property_00000, 5)
	return true
}

// IsPrefix checks whether the order property is the prefix of another.
func (p *PhysicalProperty) IsPrefix(prop *PhysicalProperty) bool {
	trace_util_0.Count(_physical_property_00000, 8)
	if len(p.Items) > len(prop.Items) {
		trace_util_0.Count(_physical_property_00000, 11)
		return false
	}
	trace_util_0.Count(_physical_property_00000, 9)
	for i := range p.Items {
		trace_util_0.Count(_physical_property_00000, 12)
		if !p.Items[i].Col.Equal(nil, prop.Items[i].Col) || p.Items[i].Desc != prop.Items[i].Desc {
			trace_util_0.Count(_physical_property_00000, 13)
			return false
		}
	}
	trace_util_0.Count(_physical_property_00000, 10)
	return true
}

// IsEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsEmpty() bool {
	trace_util_0.Count(_physical_property_00000, 14)
	return len(p.Items) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	trace_util_0.Count(_physical_property_00000, 15)
	if p.hashcode != nil {
		trace_util_0.Count(_physical_property_00000, 19)
		return p.hashcode
	}
	trace_util_0.Count(_physical_property_00000, 16)
	hashcodeSize := 8 + 8 + 8 + (16+8)*len(p.Items) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.Enforced {
		trace_util_0.Count(_physical_property_00000, 20)
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		trace_util_0.Count(_physical_property_00000, 21)
		{
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	trace_util_0.Count(_physical_property_00000, 17)
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.TaskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for _, item := range p.Items {
		trace_util_0.Count(_physical_property_00000, 22)
		p.hashcode = append(p.hashcode, item.Col.HashCode(nil)...)
		if item.Desc {
			trace_util_0.Count(_physical_property_00000, 23)
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			trace_util_0.Count(_physical_property_00000, 24)
			{
				p.hashcode = codec.EncodeInt(p.hashcode, 0)
			}
		}
	}
	trace_util_0.Count(_physical_property_00000, 18)
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	trace_util_0.Count(_physical_property_00000, 25)
	return fmt.Sprintf("Prop{cols: %v, TaskTp: %s, expectedCount: %v}", p.Items, p.TaskTp, p.ExpectedCnt)
}

// Clone returns a copy of PhysicalProperty. Currently, this function is only used to build new
// required property for children plan in `exhaustPhysicalPlans`, so we don't copy `Enforced` field
// because if `Enforced` is true, the `Items` must be empty now, this makes `Enforced` meaningless
// for children nodes.
func (p *PhysicalProperty) Clone() *PhysicalProperty {
	trace_util_0.Count(_physical_property_00000, 26)
	prop := &PhysicalProperty{
		Items:       p.Items,
		TaskTp:      p.TaskTp,
		ExpectedCnt: p.ExpectedCnt,
	}
	return prop
}

// AllSameOrder checks if all the items have same order.
func (p *PhysicalProperty) AllSameOrder() (bool, bool) {
	trace_util_0.Count(_physical_property_00000, 27)
	if len(p.Items) == 0 {
		trace_util_0.Count(_physical_property_00000, 30)
		return true, false
	}
	trace_util_0.Count(_physical_property_00000, 28)
	for i := 1; i < len(p.Items); i++ {
		trace_util_0.Count(_physical_property_00000, 31)
		if p.Items[i].Desc != p.Items[i-1].Desc {
			trace_util_0.Count(_physical_property_00000, 32)
			return false, false
		}
	}
	trace_util_0.Count(_physical_property_00000, 29)
	return true, p.Items[0].Desc
}

var _physical_property_00000 = "planner/property/physical_property.go"
