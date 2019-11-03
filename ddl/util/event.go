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

package util

import (
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/trace_util_0"
)

// Event is an event that a ddl operation happened.
type Event struct {
	Tp         model.ActionType
	TableInfo  *model.TableInfo
	ColumnInfo *model.ColumnInfo
	IndexInfo  *model.IndexInfo
}

// String implements fmt.Stringer interface.
func (e *Event) String() string {
	trace_util_0.Count(_event_00000, 0)
	ret := fmt.Sprintf("(Event Type: %s", e.Tp)
	if e.TableInfo != nil {
		trace_util_0.Count(_event_00000, 4)
		ret += fmt.Sprintf(", Table ID: %d, Table Name %s", e.TableInfo.ID, e.TableInfo.Name)
	}
	trace_util_0.Count(_event_00000, 1)
	if e.ColumnInfo != nil {
		trace_util_0.Count(_event_00000, 5)
		ret += fmt.Sprintf(", Column ID: %d, Column Name %s", e.ColumnInfo.ID, e.ColumnInfo.Name)
	}
	trace_util_0.Count(_event_00000, 2)
	if e.IndexInfo != nil {
		trace_util_0.Count(_event_00000, 6)
		ret += fmt.Sprintf(", Index ID: %d, Index Name %s", e.IndexInfo.ID, e.IndexInfo.Name)
	}
	trace_util_0.Count(_event_00000, 3)
	return ret
}

var _event_00000 = "ddl/util/event.go"
