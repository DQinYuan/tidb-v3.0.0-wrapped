// Copyright 2019 PingCAP, Inc.
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

package plugin

import

// Kind presents the kind of plugin.
"github.com/pingcap/tidb/trace_util_0"

type Kind uint8

const (
	// Audit indicates it is a Audit plugin.
	Audit Kind = 1 + iota
	// Authentication indicate it is a Authentication plugin.
	Authentication
	// Schema indicate a plugin that can change TiDB schema.
	Schema
	// Daemon indicate a plugin that can run as daemon task.
	Daemon
)

func (k Kind) String() (str string) {
	trace_util_0.Count(_const_00000, 0)
	switch k {
	case Audit:
		trace_util_0.Count(_const_00000, 2)
		str = "Audit"
	case Authentication:
		trace_util_0.Count(_const_00000, 3)
		str = "Authentication"
	case Schema:
		trace_util_0.Count(_const_00000, 4)
		str = "Schema"
	case Daemon:
		trace_util_0.Count(_const_00000, 5)
		str = "Daemon"
	}
	trace_util_0.Count(_const_00000, 1)
	return
}

// State present the state of plugin.
type State uint8

const (
	// Uninitialized indicates plugin is uninitialized.
	Uninitialized State = iota
	// Ready indicates plugin is ready to work.
	Ready
	// Dying indicates plugin will be close soon.
	Dying
	// Disable indicate plugin is disabled.
	Disable
)

func (s State) String() (str string) {
	trace_util_0.Count(_const_00000, 6)
	switch s {
	case Uninitialized:
		trace_util_0.Count(_const_00000, 8)
		str = "Uninitialized"
	case Ready:
		trace_util_0.Count(_const_00000, 9)
		str = "Ready"
	case Dying:
		trace_util_0.Count(_const_00000, 10)
		str = "Dying"
	case Disable:
		trace_util_0.Count(_const_00000, 11)
		str = "Disable"
	}
	trace_util_0.Count(_const_00000, 7)
	return
}

var _const_00000 = "plugin/const.go"
