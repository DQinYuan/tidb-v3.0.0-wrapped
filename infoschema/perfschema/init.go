// Copyright 2016 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/trace_util_0"
)

var once sync.Once

// Init register the PERFORMANCE_SCHEMA virtual tables.
// It should be init(), and the ideal usage should be:
//
// import _ "github.com/pingcap/tidb/perfschema"
//
// This function depends on plan/core.init(), which initialize the expression.EvalAstExpr function.
// The initialize order is a problem if init() is used as the function name.
func Init() {
	trace_util_0.Count(_init_00000, 0)
	initOnce := func() {
		trace_util_0.Count(_init_00000, 2)
		p := parser.New()
		tbls := make([]*model.TableInfo, 0)
		dbID := autoid.GenLocalSchemaID()

		for _, sql := range perfSchemaTables {
			trace_util_0.Count(_init_00000, 4)
			stmt, err := p.ParseOneStmt(sql, "", "")
			if err != nil {
				trace_util_0.Count(_init_00000, 7)
				panic(err)
			}
			trace_util_0.Count(_init_00000, 5)
			meta, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
			if err != nil {
				trace_util_0.Count(_init_00000, 8)
				panic(err)
			}
			trace_util_0.Count(_init_00000, 6)
			tbls = append(tbls, meta)
			meta.ID = autoid.GenLocalSchemaID()
			for _, c := range meta.Columns {
				trace_util_0.Count(_init_00000, 9)
				c.ID = autoid.GenLocalSchemaID()
			}
		}
		trace_util_0.Count(_init_00000, 3)
		dbInfo := &model.DBInfo{
			ID:      dbID,
			Name:    model.NewCIStr(Name),
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Tables:  tbls,
		}
		infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
	}
	trace_util_0.Count(_init_00000, 1)
	if expression.EvalAstExpr != nil {
		trace_util_0.Count(_init_00000, 10)
		once.Do(initOnce)
	}
}

var _init_00000 = "infoschema/perfschema/init.go"
