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

package gcutil

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	selectVariableValueSQL = `SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name='%s'`
	insertVariableValueSQL = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
                              ON DUPLICATE KEY
			                  UPDATE variable_value = '%[2]s', comment = '%[3]s'`
)

// CheckGCEnable is use to check whether GC is enable.
func CheckGCEnable(ctx sessionctx.Context) (enable bool, err error) {
	trace_util_0.Count(_gcutil_00000, 0)
	sql := fmt.Sprintf(selectVariableValueSQL, "tikv_gc_enable")
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_gcutil_00000, 3)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_gcutil_00000, 1)
	if len(rows) != 1 {
		trace_util_0.Count(_gcutil_00000, 4)
		return false, errors.New("can not get 'tikv_gc_enable'")
	}
	trace_util_0.Count(_gcutil_00000, 2)
	return rows[0].GetString(0) == "true", nil
}

// DisableGC will disable GC enable variable.
func DisableGC(ctx sessionctx.Context) error {
	trace_util_0.Count(_gcutil_00000, 5)
	sql := fmt.Sprintf(insertVariableValueSQL, "tikv_gc_enable", "false", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// EnableGC will enable GC enable variable.
func EnableGC(ctx sessionctx.Context) error {
	trace_util_0.Count(_gcutil_00000, 6)
	sql := fmt.Sprintf(insertVariableValueSQL, "tikv_gc_enable", "true", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx sessionctx.Context, snapshotTS uint64) error {
	trace_util_0.Count(_gcutil_00000, 7)
	safePointTS, err := GetGCSafePoint(ctx)
	if err != nil {
		trace_util_0.Count(_gcutil_00000, 10)
		return errors.Trace(err)
	}
	trace_util_0.Count(_gcutil_00000, 8)
	if safePointTS > snapshotTS {
		trace_util_0.Count(_gcutil_00000, 11)
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	trace_util_0.Count(_gcutil_00000, 9)
	return nil
}

// ValidateSnapshotWithGCSafePoint checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshotWithGCSafePoint(snapshotTS, safePointTS uint64) error {
	trace_util_0.Count(_gcutil_00000, 12)
	if safePointTS > snapshotTS {
		trace_util_0.Count(_gcutil_00000, 14)
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	trace_util_0.Count(_gcutil_00000, 13)
	return nil
}

// GetGCSafePoint loads GC safe point time from mysql.tidb.
func GetGCSafePoint(ctx sessionctx.Context) (uint64, error) {
	trace_util_0.Count(_gcutil_00000, 15)
	sql := fmt.Sprintf(selectVariableValueSQL, "tikv_gc_safe_point")
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		trace_util_0.Count(_gcutil_00000, 19)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_gcutil_00000, 16)
	if len(rows) != 1 {
		trace_util_0.Count(_gcutil_00000, 20)
		return 0, errors.New("can not get 'tikv_gc_safe_point'")
	}
	trace_util_0.Count(_gcutil_00000, 17)
	safePointString := rows[0].GetString(0)
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		trace_util_0.Count(_gcutil_00000, 21)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_gcutil_00000, 18)
	ts := variable.GoTimeToTS(safePointTime)
	return ts, nil
}

var _gcutil_00000 = "util/gcutil/gcutil.go"
