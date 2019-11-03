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

package testutil

import (
	"context"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/trace_util_0"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(c *check.C, s kv.Storage, sql string, done chan error) {
	trace_util_0.Count(_testutil_00000, 0)
	ExecMultiSQLInGoroutine(c, s, "test_db", []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(c *check.C, s kv.Storage, dbName string, multiSQL []string, done chan error) {
	trace_util_0.Count(_testutil_00000, 1)
	go func() {
		trace_util_0.Count(_testutil_00000, 2)
		se, err := session.CreateSession4Test(s)
		if err != nil {
			trace_util_0.Count(_testutil_00000, 5)
			done <- errors.Trace(err)
			return
		}
		trace_util_0.Count(_testutil_00000, 3)
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			trace_util_0.Count(_testutil_00000, 6)
			done <- errors.Trace(err)
			return
		}
		trace_util_0.Count(_testutil_00000, 4)
		for _, sql := range multiSQL {
			trace_util_0.Count(_testutil_00000, 7)
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				trace_util_0.Count(_testutil_00000, 10)
				done <- errors.Trace(err)
				return
			}
			trace_util_0.Count(_testutil_00000, 8)
			if rs != nil {
				trace_util_0.Count(_testutil_00000, 11)
				done <- errors.Errorf("RecordSet should be empty.")
				return
			}
			trace_util_0.Count(_testutil_00000, 9)
			done <- nil
		}
	}()
}

var _testutil_00000 = "ddl/testutil/testutil.go"
