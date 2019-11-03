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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ReloadExprPushdownBlacklistExec indicates ReloadExprPushdownBlacklist executor.
type ReloadExprPushdownBlacklistExec struct {
	baseExecutor
}

// Next implements the Executor Next interface.
func (e *ReloadExprPushdownBlacklistExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 0)
	return LoadExprPushdownBlacklist(e.ctx)
}

// LoadExprPushdownBlacklist loads the latest data from table mysql.expr_pushdown_blacklist.
func LoadExprPushdownBlacklist(ctx sessionctx.Context) (err error) {
	trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 1)
	sql := "select HIGH_PRIORITY name from mysql.expr_pushdown_blacklist"
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 4)
		return err
	}
	trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 2)
	newBlacklist := make(map[string]struct{})
	for _, row := range rows {
		trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 5)
		name := row.GetString(0)
		newBlacklist[strings.ToLower(name)] = struct{}{}
	}
	trace_util_0.Count(_reload_expr_pushdown_blacklist_00000, 3)
	expression.DefaultExprPushdownBlacklist.Store(newBlacklist)
	return nil
}

var _reload_expr_pushdown_blacklist_00000 = "executor/reload_expr_pushdown_blacklist.go"
