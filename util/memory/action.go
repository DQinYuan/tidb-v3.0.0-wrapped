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

package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ActionOnExceed is the action taken when memory usage exceeds memory quota.
// NOTE: All the implementors should be thread-safe.
type ActionOnExceed interface {
	// Action will be called when memory usage exceeds memory quota by the
	// corresponding Tracker.
	Action(t *Tracker)
	// SetLogHook binds a log hook which will be triggered and log an detailed
	// message for the out-of-memory sql.
	SetLogHook(hook func(uint64))
}

// LogOnExceed logs a warning only once when memory usage exceeds memory quota.
type LogOnExceed struct {
	mutex   sync.Mutex // For synchronization.
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for LogOnExceed.
func (a *LogOnExceed) SetLogHook(hook func(uint64)) {
	trace_util_0.Count(_action_00000, 0)
	a.logHook = hook
}

// Action logs a warning only once when memory usage exceeds memory quota.
func (a *LogOnExceed) Action(t *Tracker) {
	trace_util_0.Count(_action_00000, 1)
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if !a.acted {
		trace_util_0.Count(_action_00000, 2)
		a.acted = true
		if a.logHook == nil {
			trace_util_0.Count(_action_00000, 4)
			logutil.Logger(context.Background()).Warn("memory exceeds quota",
				zap.Error(errMemExceedThreshold.GenWithStackByArgs(t.label, t.BytesConsumed(), t.bytesLimit, t.String())))
			return
		}
		trace_util_0.Count(_action_00000, 3)
		a.logHook(a.ConnID)
	}
}

// PanicOnExceed panics when memory usage exceeds memory quota.
type PanicOnExceed struct {
	mutex   sync.Mutex // For synchronization.
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *PanicOnExceed) SetLogHook(hook func(uint64)) {
	trace_util_0.Count(_action_00000, 5)
	a.logHook = hook
}

// Action panics when memory usage exceeds memory quota.
func (a *PanicOnExceed) Action(t *Tracker) {
	trace_util_0.Count(_action_00000, 6)
	a.mutex.Lock()
	if a.acted {
		trace_util_0.Count(_action_00000, 9)
		a.mutex.Unlock()
		return
	}
	trace_util_0.Count(_action_00000, 7)
	a.acted = true
	a.mutex.Unlock()
	if a.logHook != nil {
		trace_util_0.Count(_action_00000, 10)
		a.logHook(a.ConnID)
	}
	trace_util_0.Count(_action_00000, 8)
	panic(PanicMemoryExceed + fmt.Sprintf("[conn_id=%d]", a.ConnID))
}

var (
	errMemExceedThreshold = terror.ClassExecutor.New(codeMemExceedThreshold, mysql.MySQLErrName[mysql.ErrMemExceedThreshold])
)

const (
	codeMemExceedThreshold terror.ErrCode = 8001

	// PanicMemoryExceed represents the panic message when out of memory quota.
	PanicMemoryExceed string = "Out Of Memory Quota!"
)

var _action_00000 = "util/memory/action.go"
