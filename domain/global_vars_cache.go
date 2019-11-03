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

package domain

import (
	"sync"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

// GlobalVariableCache caches global variables.
type GlobalVariableCache struct {
	sync.RWMutex
	lastModify time.Time
	rows       []chunk.Row
	fields     []*ast.ResultField

	// Unit test may like to disable it.
	disable bool
}

const globalVariableCacheExpiry time.Duration = 2 * time.Second

// Update updates the global variable cache.
func (gvc *GlobalVariableCache) Update(rows []chunk.Row, fields []*ast.ResultField) {
	trace_util_0.Count(_global_vars_cache_00000, 0)
	gvc.Lock()
	gvc.lastModify = time.Now()
	gvc.rows = rows
	gvc.fields = fields
	gvc.Unlock()
}

// Get gets the global variables from cache.
func (gvc *GlobalVariableCache) Get() (succ bool, rows []chunk.Row, fields []*ast.ResultField) {
	trace_util_0.Count(_global_vars_cache_00000, 1)
	gvc.RLock()
	defer gvc.RUnlock()
	if time.Since(gvc.lastModify) < globalVariableCacheExpiry {
		trace_util_0.Count(_global_vars_cache_00000, 3)
		succ, rows, fields = !gvc.disable, gvc.rows, gvc.fields
		return
	}
	trace_util_0.Count(_global_vars_cache_00000, 2)
	succ = false
	return
}

// Disable disables the global variabe cache, used in test only.
func (gvc *GlobalVariableCache) Disable() {
	trace_util_0.Count(_global_vars_cache_00000, 4)
	gvc.Lock()
	defer gvc.Unlock()
	gvc.disable = true
	return
}

// GetGlobalVarsCache gets the global variable cache.
func (do *Domain) GetGlobalVarsCache() *GlobalVariableCache {
	trace_util_0.Count(_global_vars_cache_00000, 5)
	return &do.gvc
}

var _global_vars_cache_00000 = "domain/global_vars_cache.go"
