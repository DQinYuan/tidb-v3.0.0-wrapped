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

package chunk

import (
	"sync"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// Pool is the column pool.
// NOTE: Pool is non-copyable.
type Pool struct {
	initCap int

	varLenColPool   *sync.Pool
	fixLenColPool4  *sync.Pool
	fixLenColPool8  *sync.Pool
	fixLenColPool16 *sync.Pool
	fixLenColPool40 *sync.Pool
}

// NewPool creates a new Pool.
func NewPool(initCap int) *Pool {
	trace_util_0.Count(_pool_00000, 0)
	return &Pool{
		initCap:         initCap,
		varLenColPool:   &sync.Pool{New: func() interface{} { trace_util_0.Count(_pool_00000, 1); return newVarLenColumn(initCap, nil) }},
		fixLenColPool4:  &sync.Pool{New: func() interface{} { trace_util_0.Count(_pool_00000, 2); return newFixedLenColumn(4, initCap) }},
		fixLenColPool8:  &sync.Pool{New: func() interface{} { trace_util_0.Count(_pool_00000, 3); return newFixedLenColumn(8, initCap) }},
		fixLenColPool16: &sync.Pool{New: func() interface{} { trace_util_0.Count(_pool_00000, 4); return newFixedLenColumn(16, initCap) }},
		fixLenColPool40: &sync.Pool{New: func() interface{} { trace_util_0.Count(_pool_00000, 5); return newFixedLenColumn(40, initCap) }},
	}
}

// GetChunk gets a Chunk from the Pool.
func (p *Pool) GetChunk(fields []*types.FieldType) *Chunk {
	trace_util_0.Count(_pool_00000, 6)
	chk := new(Chunk)
	chk.capacity = p.initCap
	chk.columns = make([]*column, len(fields))
	for i, f := range fields {
		trace_util_0.Count(_pool_00000, 8)
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			trace_util_0.Count(_pool_00000, 9)
			chk.columns[i] = p.varLenColPool.Get().(*column)
		case 4:
			trace_util_0.Count(_pool_00000, 10)
			chk.columns[i] = p.fixLenColPool4.Get().(*column)
		case 8:
			trace_util_0.Count(_pool_00000, 11)
			chk.columns[i] = p.fixLenColPool8.Get().(*column)
		case 16:
			trace_util_0.Count(_pool_00000, 12)
			chk.columns[i] = p.fixLenColPool16.Get().(*column)
		case 40:
			trace_util_0.Count(_pool_00000, 13)
			chk.columns[i] = p.fixLenColPool40.Get().(*column)
		}
	}
	trace_util_0.Count(_pool_00000, 7)
	return chk
}

// PutChunk puts a Chunk back to the Pool.
func (p *Pool) PutChunk(fields []*types.FieldType, chk *Chunk) {
	trace_util_0.Count(_pool_00000, 14)
	for i, f := range fields {
		trace_util_0.Count(_pool_00000, 16)
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			trace_util_0.Count(_pool_00000, 17)
			p.varLenColPool.Put(chk.columns[i])
		case 4:
			trace_util_0.Count(_pool_00000, 18)
			p.fixLenColPool4.Put(chk.columns[i])
		case 8:
			trace_util_0.Count(_pool_00000, 19)
			p.fixLenColPool8.Put(chk.columns[i])
		case 16:
			trace_util_0.Count(_pool_00000, 20)
			p.fixLenColPool16.Put(chk.columns[i])
		case 40:
			trace_util_0.Count(_pool_00000, 21)
			p.fixLenColPool40.Put(chk.columns[i])
		}
	}
	trace_util_0.Count(_pool_00000, 15)
	chk.columns = nil // release the column references.
}

var _pool_00000 = "util/chunk/pool.go"
