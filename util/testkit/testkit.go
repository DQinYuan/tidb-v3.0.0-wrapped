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

package testkit

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testutil"
)

// TestKit is a utility to run sql test.
type TestKit struct {
	c     *check.C
	store kv.Storage
	Se    session.Session
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	trace_util_0.Count(_testkit_00000, 0)
	resBuff := bytes.NewBufferString("")
	for _, row := range res.rows {
		trace_util_0.Count(_testkit_00000, 3)
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	trace_util_0.Count(_testkit_00000, 1)
	needBuff := bytes.NewBufferString("")
	for _, row := range expected {
		trace_util_0.Count(_testkit_00000, 4)
		fmt.Fprintf(needBuff, "%s\n", row)
	}
	trace_util_0.Count(_testkit_00000, 2)
	res.c.Assert(resBuff.String(), check.Equals, needBuff.String(), res.comment)
}

// CheckAt asserts the result of selected columns equals the expected results.
func (res *Result) CheckAt(cols []int, expected [][]interface{}) {
	trace_util_0.Count(_testkit_00000, 5)
	for _, e := range expected {
		trace_util_0.Count(_testkit_00000, 8)
		res.c.Assert(len(cols), check.Equals, len(e))
	}

	trace_util_0.Count(_testkit_00000, 6)
	rows := make([][]string, 0, len(expected))
	for i := range res.rows {
		trace_util_0.Count(_testkit_00000, 9)
		row := make([]string, 0, len(cols))
		for _, r := range cols {
			trace_util_0.Count(_testkit_00000, 11)
			row = append(row, res.rows[i][r])
		}
		trace_util_0.Count(_testkit_00000, 10)
		rows = append(rows, row)
	}
	trace_util_0.Count(_testkit_00000, 7)
	got := fmt.Sprintf("%s", rows)
	need := fmt.Sprintf("%s", expected)
	res.c.Assert(got, check.Equals, need, res.comment)
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	trace_util_0.Count(_testkit_00000, 12)
	ifacesSlice := make([][]interface{}, len(res.rows))
	for i := range res.rows {
		trace_util_0.Count(_testkit_00000, 14)
		ifaces := make([]interface{}, len(res.rows[i]))
		for j := range res.rows[i] {
			trace_util_0.Count(_testkit_00000, 16)
			ifaces[j] = res.rows[i][j]
		}
		trace_util_0.Count(_testkit_00000, 15)
		ifacesSlice[i] = ifaces
	}
	trace_util_0.Count(_testkit_00000, 13)
	return ifacesSlice
}

// Sort sorts and return the result.
func (res *Result) Sort() *Result {
	trace_util_0.Count(_testkit_00000, 17)
	sort.Slice(res.rows, func(i, j int) bool {
		trace_util_0.Count(_testkit_00000, 19)
		a := res.rows[i]
		b := res.rows[j]
		for i := range a {
			trace_util_0.Count(_testkit_00000, 21)
			if a[i] < b[i] {
				trace_util_0.Count(_testkit_00000, 22)
				return true
			} else {
				trace_util_0.Count(_testkit_00000, 23)
				if a[i] > b[i] {
					trace_util_0.Count(_testkit_00000, 24)
					return false
				}
			}
		}
		trace_util_0.Count(_testkit_00000, 20)
		return false
	})
	trace_util_0.Count(_testkit_00000, 18)
	return res
}

// NewTestKit returns a new *TestKit.
func NewTestKit(c *check.C, store kv.Storage) *TestKit {
	trace_util_0.Count(_testkit_00000, 25)
	return &TestKit{
		c:     c,
		store: store,
	}
}

// NewTestKitWithInit returns a new *TestKit and creates a session.
func NewTestKitWithInit(c *check.C, store kv.Storage) *TestKit {
	trace_util_0.Count(_testkit_00000, 26)
	tk := NewTestKit(c, store)
	// Use test and prepare a session.
	tk.MustExec("use test")
	return tk
}

var connectionID uint64

// Exec executes a sql statement.
func (tk *TestKit) Exec(sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_testkit_00000, 27)
	var err error
	if tk.Se == nil {
		trace_util_0.Count(_testkit_00000, 33)
		tk.Se, err = session.CreateSession4Test(tk.store)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		tk.Se.SetConnectionID(id)
	}
	trace_util_0.Count(_testkit_00000, 28)
	ctx := context.Background()
	if len(args) == 0 {
		trace_util_0.Count(_testkit_00000, 34)
		var rss []sqlexec.RecordSet
		rss, err = tk.Se.Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			trace_util_0.Count(_testkit_00000, 36)
			return rss[0], nil
		}
		trace_util_0.Count(_testkit_00000, 35)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_testkit_00000, 29)
	stmtID, _, _, err := tk.Se.PrepareStmt(sql)
	if err != nil {
		trace_util_0.Count(_testkit_00000, 37)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_testkit_00000, 30)
	rs, err := tk.Se.ExecutePreparedStmt(ctx, stmtID, args...)
	if err != nil {
		trace_util_0.Count(_testkit_00000, 38)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_testkit_00000, 31)
	err = tk.Se.DropPreparedStmt(stmtID)
	if err != nil {
		trace_util_0.Count(_testkit_00000, 39)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_testkit_00000, 32)
	return rs, nil
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *TestKit) CheckExecResult(affectedRows, insertID int64) {
	trace_util_0.Count(_testkit_00000, 40)
	tk.c.Assert(affectedRows, check.Equals, int64(tk.Se.AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(tk.Se.LastInsertID()))
}

// CheckLastMessage checks last message after executing MustExec
func (tk *TestKit) CheckLastMessage(msg string) {
	trace_util_0.Count(_testkit_00000, 41)
	tk.c.Assert(tk.Se.LastMessage(), check.Equals, msg)
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...interface{}) {
	trace_util_0.Count(_testkit_00000, 42)
	res, err := tk.Exec(sql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err)))
	if res != nil {
		trace_util_0.Count(_testkit_00000, 43)
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// MustIndexLookup checks whether the plan for the sql is Point_Get.
func (tk *TestKit) MustIndexLookup(sql string, args ...interface{}) *Result {
	trace_util_0.Count(_testkit_00000, 44)
	rs := tk.MustQuery("explain "+sql, args...)
	hasIndexLookup := false
	for i := range rs.rows {
		trace_util_0.Count(_testkit_00000, 46)
		if strings.Contains(rs.rows[i][0], "IndexLookUp") {
			trace_util_0.Count(_testkit_00000, 47)
			hasIndexLookup = true
			break
		}
	}
	trace_util_0.Count(_testkit_00000, 45)
	tk.c.Assert(hasIndexLookup, check.IsTrue)
	return tk.MustQuery(sql, args...)
}

// MustPointGet checks whether the plan for the sql is Point_Get.
func (tk *TestKit) MustPointGet(sql string, args ...interface{}) *Result {
	trace_util_0.Count(_testkit_00000, 48)
	rs := tk.MustQuery("explain "+sql, args...)
	tk.c.Assert(len(rs.rows), check.Equals, 1)
	tk.c.Assert(strings.Contains(rs.rows[0][0], "Point_Get"), check.IsTrue)
	return tk.MustQuery(sql, args...)
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(sql string, args ...interface{}) *Result {
	trace_util_0.Count(_testkit_00000, 49)
	comment := check.Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(sql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.ResultSetToResult(rs, comment)
}

// QueryToErr executes a sql statement and discard results.
func (tk *TestKit) QueryToErr(sql string, args ...interface{}) error {
	trace_util_0.Count(_testkit_00000, 50)
	comment := check.Commentf("sql:%s, args:%v", sql, args)
	res, err := tk.Exec(sql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(res, check.NotNil, comment)
	_, resErr := session.GetRows4Test(context.Background(), tk.Se, res)
	tk.c.Assert(res.Close(), check.IsNil)
	return resErr
}

// ExecToErr executes a sql statement and discard results.
func (tk *TestKit) ExecToErr(sql string, args ...interface{}) error {
	trace_util_0.Count(_testkit_00000, 51)
	res, err := tk.Exec(sql, args...)
	if res != nil {
		trace_util_0.Count(_testkit_00000, 53)
		tk.c.Assert(res.Close(), check.IsNil)
	}
	trace_util_0.Count(_testkit_00000, 52)
	return err
}

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *TestKit) ResultSetToResult(rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	trace_util_0.Count(_testkit_00000, 54)
	return tk.ResultSetToResultWithCtx(context.Background(), rs, comment)
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s session.Session, rs sqlexec.RecordSet) ([][]string, error) {
	trace_util_0.Count(_testkit_00000, 55)
	rows, err := session.GetRows4Test(ctx, s, rs)
	if err != nil {
		trace_util_0.Count(_testkit_00000, 59)
		return nil, err
	}
	trace_util_0.Count(_testkit_00000, 56)
	err = rs.Close()
	if err != nil {
		trace_util_0.Count(_testkit_00000, 60)
		return nil, err
	}
	trace_util_0.Count(_testkit_00000, 57)
	sRows := make([][]string, len(rows))
	for i := range rows {
		trace_util_0.Count(_testkit_00000, 61)
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			trace_util_0.Count(_testkit_00000, 63)
			if row.IsNull(j) {
				trace_util_0.Count(_testkit_00000, 64)
				iRow[j] = "<nil>"
			} else {
				trace_util_0.Count(_testkit_00000, 65)
				{
					d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
					iRow[j], err = d.ToString()
					if err != nil {
						trace_util_0.Count(_testkit_00000, 66)
						return nil, err
					}
				}
			}
		}
		trace_util_0.Count(_testkit_00000, 62)
		sRows[i] = iRow
	}
	trace_util_0.Count(_testkit_00000, 58)
	return sRows, nil
}

// ResultSetToResultWithCtx converts sqlexec.RecordSet to testkit.Result.
func (tk *TestKit) ResultSetToResultWithCtx(ctx context.Context, rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	trace_util_0.Count(_testkit_00000, 67)
	sRows, err := ResultSetToStringSlice(ctx, tk.Se, rs)
	tk.c.Check(err, check.IsNil, comment)
	return &Result{rows: sRows, c: tk.c, comment: comment}
}

// Rows is similar to RowsWithSep, use white space as separator string.
func Rows(args ...string) [][]interface{} {
	trace_util_0.Count(_testkit_00000, 68)
	return testutil.RowsWithSep(" ", args...)
}

// GetTableID gets table ID by name.
func (tk *TestKit) GetTableID(tableName string) int64 {
	trace_util_0.Count(_testkit_00000, 69)
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(tableName))
	tk.c.Assert(err, check.IsNil)
	return tbl.Meta().ID
}

var _testkit_00000 = "util/testkit/testkit.go"
