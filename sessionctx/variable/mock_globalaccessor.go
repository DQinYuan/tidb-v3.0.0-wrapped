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

package variable

import

// MockGlobalAccessor implements GlobalVarAccessor interface. it's used in tests
"github.com/pingcap/tidb/trace_util_0"

type MockGlobalAccessor struct {
}

// NewMockGlobalAccessor implements GlobalVarAccessor interface.
func NewMockGlobalAccessor() *MockGlobalAccessor {
	trace_util_0.Count(_mock_globalaccessor_00000, 0)
	return new(MockGlobalAccessor)
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (m *MockGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	trace_util_0.Count(_mock_globalaccessor_00000, 1)
	v, ok := SysVars[name]
	if ok {
		trace_util_0.Count(_mock_globalaccessor_00000, 3)
		return v.Value, nil
	}
	trace_util_0.Count(_mock_globalaccessor_00000, 2)
	return "", nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (m *MockGlobalAccessor) SetGlobalSysVar(name string, value string) error {
	trace_util_0.Count(_mock_globalaccessor_00000, 4)
	panic("not supported")
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (m *MockGlobalAccessor) GetAllSysVars() (map[string]string, error) {
	trace_util_0.Count(_mock_globalaccessor_00000, 5)
	panic("not supported")
}

var _mock_globalaccessor_00000 = "sessionctx/variable/mock_globalaccessor.go"
