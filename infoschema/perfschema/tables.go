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

package perfschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
)

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

var pluginTable = make(map[string]func(autoid.Allocator, *model.TableInfo) (table.Table, error))

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocator, *model.TableInfo) (table.Table, error)) {
	trace_util_0.Count(_tables_00000, 0)
	perfSchemaTables = append(perfSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	trace_util_0.Count(_tables_00000, 1)
	if f, ok := pluginTable[meta.Name.L]; ok {
		trace_util_0.Count(_tables_00000, 3)
		ret, err := f(alloc, meta)
		return ret, err
	}
	trace_util_0.Count(_tables_00000, 2)
	return createPerfSchemaTable(meta), nil
}

// createPerfSchemaTable creates all perfSchemaTables
func createPerfSchemaTable(meta *model.TableInfo) *perfSchemaTable {
	trace_util_0.Count(_tables_00000, 4)
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		trace_util_0.Count(_tables_00000, 6)
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	trace_util_0.Count(_tables_00000, 5)
	t := &perfSchemaTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *perfSchemaTable) Cols() []*table.Column {
	trace_util_0.Count(_tables_00000, 7)
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *perfSchemaTable) WritableCols() []*table.Column {
	trace_util_0.Count(_tables_00000, 8)
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *perfSchemaTable) GetPhysicalID() int64 {
	trace_util_0.Count(_tables_00000, 9)
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *perfSchemaTable) Meta() *model.TableInfo {
	trace_util_0.Count(_tables_00000, 10)
	return vt.meta
}

var _tables_00000 = "infoschema/perfschema/tables.go"
