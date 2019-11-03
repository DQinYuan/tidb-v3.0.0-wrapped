// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// CompareUnorderedStringSlice compare two string slices.
// If a and b is exactly the same except the order, it returns true.
// In otherwise return false.
func CompareUnorderedStringSlice(a []string, b []string) bool {
	trace_util_0.Count(_testutil_00000, 0)
	if a == nil && b == nil {
		trace_util_0.Count(_testutil_00000, 6)
		return true
	}
	trace_util_0.Count(_testutil_00000, 1)
	if a == nil || b == nil {
		trace_util_0.Count(_testutil_00000, 7)
		return false
	}
	trace_util_0.Count(_testutil_00000, 2)
	if len(a) != len(b) {
		trace_util_0.Count(_testutil_00000, 8)
		return false
	}
	trace_util_0.Count(_testutil_00000, 3)
	m := make(map[string]int, len(a))
	for _, i := range a {
		trace_util_0.Count(_testutil_00000, 9)
		_, ok := m[i]
		if !ok {
			trace_util_0.Count(_testutil_00000, 10)
			m[i] = 1
		} else {
			trace_util_0.Count(_testutil_00000, 11)
			{
				m[i]++
			}
		}
	}

	trace_util_0.Count(_testutil_00000, 4)
	for _, i := range b {
		trace_util_0.Count(_testutil_00000, 12)
		_, ok := m[i]
		if !ok {
			trace_util_0.Count(_testutil_00000, 14)
			return false
		}
		trace_util_0.Count(_testutil_00000, 13)
		m[i]--
		if m[i] == 0 {
			trace_util_0.Count(_testutil_00000, 15)
			delete(m, i)
		}
	}
	trace_util_0.Count(_testutil_00000, 5)
	return len(m) == 0
}

// datumEqualsChecker is a checker for DatumEquals.
type datumEqualsChecker struct {
	*check.CheckerInfo
}

// DatumEquals checker verifies that the obtained value is equal to
// the expected value.
// For example:
//     c.Assert(value, DatumEquals, NewDatum(42))
var DatumEquals check.Checker = &datumEqualsChecker{
	&check.CheckerInfo{Name: "DatumEquals", Params: []string{"obtained", "expected"}},
}

func (checker *datumEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	trace_util_0.Count(_testutil_00000, 16)
	defer func() {
		trace_util_0.Count(_testutil_00000, 21)
		if v := recover(); v != nil {
			trace_util_0.Count(_testutil_00000, 22)
			result = false
			error = fmt.Sprint(v)
		}
	}()
	trace_util_0.Count(_testutil_00000, 17)
	paramFirst, ok := params[0].(types.Datum)
	if !ok {
		trace_util_0.Count(_testutil_00000, 23)
		panic("the first param should be datum")
	}
	trace_util_0.Count(_testutil_00000, 18)
	paramSecond, ok := params[1].(types.Datum)
	if !ok {
		trace_util_0.Count(_testutil_00000, 24)
		panic("the second param should be datum")
	}
	trace_util_0.Count(_testutil_00000, 19)
	sc := new(stmtctx.StatementContext)
	res, err := paramFirst.CompareDatum(sc, &paramSecond)
	if err != nil {
		trace_util_0.Count(_testutil_00000, 25)
		panic(err)
	}
	trace_util_0.Count(_testutil_00000, 20)
	return res == 0, ""
}

// RowsWithSep is a convenient function to wrap args to a slice of []interface.
// The arg represents a row, split by sep.
func RowsWithSep(sep string, args ...string) [][]interface{} {
	trace_util_0.Count(_testutil_00000, 26)
	rows := make([][]interface{}, len(args))
	for i, v := range args {
		trace_util_0.Count(_testutil_00000, 28)
		strs := strings.Split(v, sep)
		row := make([]interface{}, len(strs))
		for j, s := range strs {
			trace_util_0.Count(_testutil_00000, 30)
			row[j] = s
		}
		trace_util_0.Count(_testutil_00000, 29)
		rows[i] = row
	}
	trace_util_0.Count(_testutil_00000, 27)
	return rows
}

var _testutil_00000 = "util/testutil/testutil.go"
