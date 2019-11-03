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

package testkit

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

type contextKeyType int

const sessionKey contextKeyType = iota

func getSession(ctx context.Context) session.Session {
	trace_util_0.Count(_ctestkit_00000, 0)
	s := ctx.Value(sessionKey)
	if s == nil {
		trace_util_0.Count(_ctestkit_00000, 2)
		return nil
	}
	trace_util_0.Count(_ctestkit_00000, 1)
	return s.(session.Session)
}

func setSession(ctx context.Context, se session.Session) context.Context {
	trace_util_0.Count(_ctestkit_00000, 3)
	return context.WithValue(ctx, sessionKey, se)
}

// CTestKit is a utility to run sql test with concurrent execution support.
type CTestKit struct {
	c     *check.C
	store kv.Storage
}

// NewCTestKit returns a new *CTestKit.
func NewCTestKit(c *check.C, store kv.Storage) *CTestKit {
	trace_util_0.Count(_ctestkit_00000, 4)
	return &CTestKit{
		c:     c,
		store: store,
	}
}

// OpenSession opens new session ctx if no exists one.
func (tk *CTestKit) OpenSession(ctx context.Context) context.Context {
	trace_util_0.Count(_ctestkit_00000, 5)
	if getSession(ctx) == nil {
		trace_util_0.Count(_ctestkit_00000, 7)
		se, err := session.CreateSession4Test(tk.store)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		se.SetConnectionID(id)
		ctx = setSession(ctx, se)
	}
	trace_util_0.Count(_ctestkit_00000, 6)
	return ctx
}

// OpenSessionWithDB opens new session ctx if no exists one and use db.
func (tk *CTestKit) OpenSessionWithDB(ctx context.Context, db string) context.Context {
	trace_util_0.Count(_ctestkit_00000, 8)
	ctx = tk.OpenSession(ctx)
	tk.MustExec(ctx, "use "+db)
	return ctx
}

// CloseSession closes exists session from ctx.
func (tk *CTestKit) CloseSession(ctx context.Context) {
	trace_util_0.Count(_ctestkit_00000, 9)
	se := getSession(ctx)
	tk.c.Assert(se, check.NotNil)
	se.Close()
}

// Exec executes a sql statement.
func (tk *CTestKit) Exec(ctx context.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	trace_util_0.Count(_ctestkit_00000, 10)
	var err error
	tk.c.Assert(getSession(ctx), check.NotNil)
	if len(args) == 0 {
		trace_util_0.Count(_ctestkit_00000, 15)
		var rss []sqlexec.RecordSet
		rss, err = getSession(ctx).Execute(ctx, sql)
		if err == nil && len(rss) > 0 {
			trace_util_0.Count(_ctestkit_00000, 17)
			return rss[0], nil
		}
		trace_util_0.Count(_ctestkit_00000, 16)
		return nil, err
	}
	trace_util_0.Count(_ctestkit_00000, 11)
	stmtID, _, _, err := getSession(ctx).PrepareStmt(sql)
	if err != nil {
		trace_util_0.Count(_ctestkit_00000, 18)
		return nil, err
	}
	trace_util_0.Count(_ctestkit_00000, 12)
	rs, err := getSession(ctx).ExecutePreparedStmt(ctx, stmtID, args...)
	if err != nil {
		trace_util_0.Count(_ctestkit_00000, 19)
		return nil, err
	}
	trace_util_0.Count(_ctestkit_00000, 13)
	err = getSession(ctx).DropPreparedStmt(stmtID)
	if err != nil {
		trace_util_0.Count(_ctestkit_00000, 20)
		return nil, err
	}
	trace_util_0.Count(_ctestkit_00000, 14)
	return rs, nil
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *CTestKit) CheckExecResult(ctx context.Context, affectedRows, insertID int64) {
	trace_util_0.Count(_ctestkit_00000, 21)
	tk.c.Assert(getSession(ctx), check.NotNil)
	tk.c.Assert(affectedRows, check.Equals, int64(getSession(ctx).AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(getSession(ctx).LastInsertID()))
}

// MustExec executes a sql statement and asserts nil error.
func (tk *CTestKit) MustExec(ctx context.Context, sql string, args ...interface{}) {
	trace_util_0.Count(_ctestkit_00000, 22)
	res, err := tk.Exec(ctx, sql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err)))
	if res != nil {
		trace_util_0.Count(_ctestkit_00000, 23)
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *CTestKit) MustQuery(ctx context.Context, sql string, args ...interface{}) *Result {
	trace_util_0.Count(_ctestkit_00000, 24)
	comment := check.Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(ctx, sql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.resultSetToResult(ctx, rs, comment)
}

// resultSetToResult converts ast.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *CTestKit) resultSetToResult(ctx context.Context, rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	trace_util_0.Count(_ctestkit_00000, 25)
	rows, err := session.GetRows4Test(context.Background(), getSession(ctx), rs)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	err = rs.Close()
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	sRows := make([][]string, len(rows))
	for i := range rows {
		trace_util_0.Count(_ctestkit_00000, 27)
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			trace_util_0.Count(_ctestkit_00000, 29)
			if row.IsNull(j) {
				trace_util_0.Count(_ctestkit_00000, 30)
				iRow[j] = "<nil>"
			} else {
				trace_util_0.Count(_ctestkit_00000, 31)
				{
					d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
					iRow[j], err = d.ToString()
					tk.c.Assert(err, check.IsNil)
				}
			}
		}
		trace_util_0.Count(_ctestkit_00000, 28)
		sRows[i] = iRow
	}
	trace_util_0.Count(_ctestkit_00000, 26)
	return &Result{rows: sRows, c: tk.c, comment: comment}
}

// ConcurrentRun run test in current.
// - concurrent: controls the concurrent worker count.
// - loops: controls run test how much times.
// - prepareFunc: provide test data and will be called for every loop.
// - checkFunc: used to do some check after all workers done.
// works like create table better be put in front of this method calling.
// see more example at TestBatchInsertWithOnDuplicate
func (tk *CTestKit) ConcurrentRun(c *check.C, concurrent int, loops int,
	prepareFunc func(ctx context.Context, tk *CTestKit, concurrent int, currentLoop int) [][][]interface{},
	writeFunc func(ctx context.Context, tk *CTestKit, input [][]interface{}),
	checkFunc func(ctx context.Context, tk *CTestKit)) {
	trace_util_0.Count(_ctestkit_00000, 32)
	var (
		channel = make([]chan [][]interface{}, concurrent)
		ctxs    = make([]context.Context, concurrent)
		dones   = make([]context.CancelFunc, concurrent)
	)
	for i := 0; i < concurrent; i++ {
		trace_util_0.Count(_ctestkit_00000, 38)
		w := i
		channel[w] = make(chan [][]interface{}, 1)
		ctxs[w], dones[w] = context.WithCancel(context.Background())
		ctxs[w] = tk.OpenSessionWithDB(ctxs[w], "test")
		go func() {
			trace_util_0.Count(_ctestkit_00000, 39)
			defer func() {
				trace_util_0.Count(_ctestkit_00000, 41)
				r := recover()
				if r != nil {
					trace_util_0.Count(_ctestkit_00000, 43)
					c.Fatal(r, string(util.GetStack()))
				}
				trace_util_0.Count(_ctestkit_00000, 42)
				dones[w]()
			}()
			trace_util_0.Count(_ctestkit_00000, 40)
			for input := range channel[w] {
				trace_util_0.Count(_ctestkit_00000, 44)
				writeFunc(ctxs[w], tk, input)
			}
		}()
	}
	trace_util_0.Count(_ctestkit_00000, 33)
	defer func() {
		trace_util_0.Count(_ctestkit_00000, 45)
		for i := 0; i < concurrent; i++ {
			trace_util_0.Count(_ctestkit_00000, 46)
			tk.CloseSession(ctxs[i])
		}
	}()

	trace_util_0.Count(_ctestkit_00000, 34)
	ctx := tk.OpenSessionWithDB(context.Background(), "test")
	defer tk.CloseSession(ctx)
	tk.MustExec(ctx, "use test")

	for j := 0; j < loops; j++ {
		trace_util_0.Count(_ctestkit_00000, 47)
		datas := prepareFunc(ctx, tk, concurrent, j)
		for i := 0; i < concurrent; i++ {
			trace_util_0.Count(_ctestkit_00000, 48)
			channel[i] <- datas[i]
		}
	}

	trace_util_0.Count(_ctestkit_00000, 35)
	for i := 0; i < concurrent; i++ {
		trace_util_0.Count(_ctestkit_00000, 49)
		close(channel[i])
	}

	trace_util_0.Count(_ctestkit_00000, 36)
	for i := 0; i < concurrent; i++ {
		trace_util_0.Count(_ctestkit_00000, 50)
		<-ctxs[i].Done()
	}
	trace_util_0.Count(_ctestkit_00000, 37)
	checkFunc(ctx, tk)
}

// PermInt returns, as a slice of n ints, a pseudo-random permutation of the integers [0,n).
func (tk *CTestKit) PermInt(n int) []interface{} {
	trace_util_0.Count(_ctestkit_00000, 51)
	var v []interface{}
	for _, i := range rand.Perm(n) {
		trace_util_0.Count(_ctestkit_00000, 53)
		v = append(v, i)
	}
	trace_util_0.Count(_ctestkit_00000, 52)
	return v
}

// IgnoreError ignores error and make errcheck tool happy.
// Deprecated: it's normal to ignore some error in concurrent test, but please don't use this method in other place.
func (tk *CTestKit) IgnoreError(_ error) { trace_util_0.Count(_ctestkit_00000, 54) }

var _ctestkit_00000 = "util/testkit/ctestkit.go"
