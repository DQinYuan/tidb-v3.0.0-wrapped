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

import

// TaskType is the type of execution task.
"github.com/pingcap/tidb/trace_util_0"

type TaskType int

const (
	// RootTaskType stands for the tasks that executed in the TiDB layer.
	RootTaskType TaskType = iota

	// CopSingleReadTaskType stands for the a TableScan or IndexScan tasks
	// executed in the coprocessor layer.
	CopSingleReadTaskType

	// CopDoubleReadTaskType stands for the a IndexLookup tasks executed in the
	// coprocessor layer.
	CopDoubleReadTaskType
)

// String implements fmt.Stringer interface.
func (t TaskType) String() string {
	trace_util_0.Count(_task_type_00000, 0)
	switch t {
	case RootTaskType:
		trace_util_0.Count(_task_type_00000, 2)
		return "rootTask"
	case CopSingleReadTaskType:
		trace_util_0.Count(_task_type_00000, 3)
		return "copSingleReadTask"
	case CopDoubleReadTaskType:
		trace_util_0.Count(_task_type_00000, 4)
		return "copDoubleReadTask"
	}
	trace_util_0.Count(_task_type_00000, 1)
	return "UnknownTaskType"
}

var _task_type_00000 = "planner/property/task_type.go"
