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

package kv

import

// Variables defines the variables used by KV storage.
"github.com/pingcap/tidb/trace_util_0"

type Variables struct {
	// BackoffLockFast specifies the LockFast backoff base duration in milliseconds.
	BackoffLockFast int

	// BackOffWeight specifies the weight of the max back off time duration.
	BackOffWeight int

	// Hook is used for test to verify the variable take effect.
	Hook func(name string, vars *Variables)
}

// NewVariables create a new Variables instance with default values.
func NewVariables() *Variables {
	trace_util_0.Count(_variables_00000, 0)
	return &Variables{
		BackoffLockFast: DefBackoffLockFast,
		BackOffWeight:   DefBackOffWeight,
	}
}

// DefaultVars is the default variables instance.
var DefaultVars = NewVariables()

// Default values
const (
	DefBackoffLockFast = 100
	DefBackOffWeight   = 2
)

var _variables_00000 = "kv/variables.go"
