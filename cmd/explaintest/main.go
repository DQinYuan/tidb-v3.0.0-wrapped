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

package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

const dbName = "test"

var (
	logLevel string
	record   bool
	create   bool
)

func init() {
	trace_util_0.Count(_main_00000, 0)
	flag.StringVar(&logLevel, "log-level", "error", "set log level: info, warn, error, debug [default: error]")
	flag.BoolVar(&record, "record", false, "record the test output in the result file")
	flag.BoolVar(&create, "create", false, "create and import data into table, and save json file of stats")
}

var mdb *sql.DB

type query struct {
	Query string
	Line  int
}

type tester struct {
	name string

	tx *sql.Tx

	buf bytes.Buffer

	// enable query log will output origin statement into result file too
	// use --disable_query_log or --enable_query_log to control it
	enableQueryLog bool

	singleQuery bool

	// check expected error, use --error before the statement
	// see http://dev.mysql.com/doc/mysqltest/2.0/en/writing-tests-expecting-errors.html
	expectedErrs []string

	// only for test, not record, every time we execute a statement, we should read the result
	// data to check correction.
	resultFD *os.File
	// ctx is used for Compile sql statement
	ctx sessionctx.Context
}

func newTester(name string) *tester {
	trace_util_0.Count(_main_00000, 1)
	t := new(tester)

	t.name = name
	t.enableQueryLog = true
	t.ctx = mock.NewContext()
	t.ctx.GetSessionVars().EnableWindowFunction = true

	return t
}

func (t *tester) Run() error {
	trace_util_0.Count(_main_00000, 2)
	queries, err := t.loadQueries()
	if err != nil {
		trace_util_0.Count(_main_00000, 7)
		return errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 3)
	if err = t.openResult(); err != nil {
		trace_util_0.Count(_main_00000, 8)
		return errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 4)
	var s string
	defer func() {
		trace_util_0.Count(_main_00000, 9)
		if t.tx != nil {
			trace_util_0.Count(_main_00000, 11)
			log.Error("transaction is not committed correctly, rollback")
			err = t.rollback()
			if err != nil {
				trace_util_0.Count(_main_00000, 12)
				log.Error("transaction is failed rollback", zap.Error(err))
			}
		}

		trace_util_0.Count(_main_00000, 10)
		if t.resultFD != nil {
			trace_util_0.Count(_main_00000, 13)
			err = t.resultFD.Close()
			if err != nil {
				trace_util_0.Count(_main_00000, 14)
				log.Error("result fd close failed", zap.Error(err))
			}
		}
	}()

	trace_util_0.Count(_main_00000, 5)
LOOP:
	for _, q := range queries {
		trace_util_0.Count(_main_00000, 15)
		s = q.Query
		if strings.HasPrefix(s, "--") {
			trace_util_0.Count(_main_00000, 16)
			// clear expected errors
			t.expectedErrs = nil

			switch s {
			case "--enable_query_log":
				trace_util_0.Count(_main_00000, 17)
				t.enableQueryLog = true
			case "--disable_query_log":
				trace_util_0.Count(_main_00000, 18)
				t.enableQueryLog = false
			case "--single_query":
				trace_util_0.Count(_main_00000, 19)
				t.singleQuery = true
			case "--halt":
				trace_util_0.Count(_main_00000, 20)
				// if we meet halt, we will ignore following tests
				break LOOP
			default:
				trace_util_0.Count(_main_00000, 21)
				if strings.HasPrefix(s, "--error") {
					trace_util_0.Count(_main_00000, 22)
					t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimLeft(s, "--error")), ",")
				} else {
					trace_util_0.Count(_main_00000, 23)
					if strings.HasPrefix(s, "-- error") {
						trace_util_0.Count(_main_00000, 24)
						t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimLeft(s, "-- error")), ",")
					} else {
						trace_util_0.Count(_main_00000, 25)
						if strings.HasPrefix(s, "--echo") {
							trace_util_0.Count(_main_00000, 26)
							echo := strings.TrimSpace(strings.TrimLeft(s, "--echo"))
							t.buf.WriteString(echo)
							t.buf.WriteString("\n")
						}
					}
				}
			}
		} else {
			trace_util_0.Count(_main_00000, 27)
			{
				if err = t.execute(q); err != nil {
					trace_util_0.Count(_main_00000, 28)
					return errors.Annotate(err, fmt.Sprintf("sql:%v", q.Query))
				}
			}
		}
	}

	trace_util_0.Count(_main_00000, 6)
	return t.flushResult()
}

func (t *tester) loadQueries() ([]query, error) {
	trace_util_0.Count(_main_00000, 29)
	data, err := ioutil.ReadFile(t.testFileName())
	if err != nil {
		trace_util_0.Count(_main_00000, 32)
		return nil, err
	}

	trace_util_0.Count(_main_00000, 30)
	seps := bytes.Split(data, []byte("\n"))
	queries := make([]query, 0, len(seps))
	newStmt := true
	for i, v := range seps {
		trace_util_0.Count(_main_00000, 33)
		s := string(bytes.TrimSpace(v))
		// we will skip # comment here
		if strings.HasPrefix(s, "#") {
			trace_util_0.Count(_main_00000, 36)
			newStmt = true
			continue
		} else {
			trace_util_0.Count(_main_00000, 37)
			if strings.HasPrefix(s, "--") {
				trace_util_0.Count(_main_00000, 38)
				queries = append(queries, query{Query: s, Line: i + 1})
				newStmt = true
				continue
			} else {
				trace_util_0.Count(_main_00000, 39)
				if len(s) == 0 {
					trace_util_0.Count(_main_00000, 40)
					continue
				}
			}
		}

		trace_util_0.Count(_main_00000, 34)
		if newStmt {
			trace_util_0.Count(_main_00000, 41)
			queries = append(queries, query{Query: s, Line: i + 1})
		} else {
			trace_util_0.Count(_main_00000, 42)
			{
				lastQuery := queries[len(queries)-1]
				lastQuery.Query = fmt.Sprintf("%s\n%s", lastQuery.Query, s)
				queries[len(queries)-1] = lastQuery
			}
		}

		// if the line has a ; in the end, we will treat new line as the new statement.
		trace_util_0.Count(_main_00000, 35)
		newStmt = strings.HasSuffix(s, ";")
	}
	trace_util_0.Count(_main_00000, 31)
	return queries, nil
}

// parserErrorHandle handle mysql_test syntax `--error ER_PARSE_ERROR`, to allow following query
// return parser error.
func (t *tester) parserErrorHandle(query query, err error) error {
	trace_util_0.Count(_main_00000, 43)
	offset := t.buf.Len()
	for _, expectedErr := range t.expectedErrs {
		trace_util_0.Count(_main_00000, 47)
		if expectedErr == "ER_PARSE_ERROR" {
			trace_util_0.Count(_main_00000, 48)
			if t.enableQueryLog {
				trace_util_0.Count(_main_00000, 50)
				t.buf.WriteString(query.Query)
				t.buf.WriteString("\n")
			}

			trace_util_0.Count(_main_00000, 49)
			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
			break
		}
	}

	trace_util_0.Count(_main_00000, 44)
	if err != nil {
		trace_util_0.Count(_main_00000, 51)
		return errors.Trace(err)
	}

	// clear expected errors after we execute the first query
	trace_util_0.Count(_main_00000, 45)
	t.expectedErrs = nil
	t.singleQuery = false

	if !record && !create {
		trace_util_0.Count(_main_00000, 52)
		// check test result now
		gotBuf := t.buf.Bytes()[offset:]
		buf := make([]byte, t.buf.Len()-offset)
		if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
			trace_util_0.Count(_main_00000, 54)
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", query.Query, query.Line, gotBuf, err))
		}

		trace_util_0.Count(_main_00000, 53)
		if !bytes.Equal(gotBuf, buf) {
			trace_util_0.Count(_main_00000, 55)
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need(%v):\n%s\nbut got(%v):\n%s\n", query.Query, query.Line, len(buf), buf, len(gotBuf), gotBuf))
		}
	}

	trace_util_0.Count(_main_00000, 46)
	return errors.Trace(err)
}

func (t *tester) executeDefault(qText string) (err error) {
	trace_util_0.Count(_main_00000, 56)
	if t.tx != nil {
		trace_util_0.Count(_main_00000, 61)
		return filterWarning(t.executeStmt(qText))
	}

	// if begin or following commit fails, we don't think
	// this error is the expected one.
	trace_util_0.Count(_main_00000, 57)
	if t.tx, err = mdb.Begin(); err != nil {
		trace_util_0.Count(_main_00000, 62)
		err2 := t.rollback()
		if err2 != nil {
			trace_util_0.Count(_main_00000, 64)
			log.Error("transaction is failed to rollback", zap.Error(err))
		}
		trace_util_0.Count(_main_00000, 63)
		return err
	}

	trace_util_0.Count(_main_00000, 58)
	if err = filterWarning(t.executeStmt(qText)); err != nil {
		trace_util_0.Count(_main_00000, 65)
		err2 := t.rollback()
		if err2 != nil {
			trace_util_0.Count(_main_00000, 67)
			log.Error("transaction is failed rollback", zap.Error(err))
		}
		trace_util_0.Count(_main_00000, 66)
		return err
	}

	trace_util_0.Count(_main_00000, 59)
	if err = t.commit(); err != nil {
		trace_util_0.Count(_main_00000, 68)
		err2 := t.rollback()
		if err2 != nil {
			trace_util_0.Count(_main_00000, 70)
			log.Error("transaction is failed rollback", zap.Error(err))
		}
		trace_util_0.Count(_main_00000, 69)
		return err
	}
	trace_util_0.Count(_main_00000, 60)
	return nil
}

func (t *tester) execute(query query) error {
	trace_util_0.Count(_main_00000, 71)
	if len(query.Query) == 0 {
		trace_util_0.Count(_main_00000, 75)
		return nil
	}

	trace_util_0.Count(_main_00000, 72)
	list, err := session.Parse(t.ctx, query.Query)
	if err != nil {
		trace_util_0.Count(_main_00000, 76)
		return t.parserErrorHandle(query, err)
	}
	trace_util_0.Count(_main_00000, 73)
	for _, st := range list {
		trace_util_0.Count(_main_00000, 77)
		var qText string
		if t.singleQuery {
			trace_util_0.Count(_main_00000, 83)
			qText = query.Query
		} else {
			trace_util_0.Count(_main_00000, 84)
			{
				qText = st.Text()
			}
		}
		trace_util_0.Count(_main_00000, 78)
		offset := t.buf.Len()
		if t.enableQueryLog {
			trace_util_0.Count(_main_00000, 85)
			t.buf.WriteString(qText)
			t.buf.WriteString("\n")
		}
		trace_util_0.Count(_main_00000, 79)
		switch st.(type) {
		case *ast.BeginStmt:
			trace_util_0.Count(_main_00000, 86)
			t.tx, err = mdb.Begin()
			if err != nil {
				trace_util_0.Count(_main_00000, 90)
				err2 := t.rollback()
				if err2 != nil {
					trace_util_0.Count(_main_00000, 92)
					log.Error("transaction is failed rollback", zap.Error(err))
				}
				trace_util_0.Count(_main_00000, 91)
				break
			}
		case *ast.CommitStmt:
			trace_util_0.Count(_main_00000, 87)
			err = t.commit()
			if err != nil {
				trace_util_0.Count(_main_00000, 93)
				err2 := t.rollback()
				if err2 != nil {
					trace_util_0.Count(_main_00000, 95)
					log.Error("transaction is failed rollback", zap.Error(err))
				}
				trace_util_0.Count(_main_00000, 94)
				break
			}
		case *ast.RollbackStmt:
			trace_util_0.Count(_main_00000, 88)
			err = t.rollback()
			if err != nil {
				trace_util_0.Count(_main_00000, 96)
				break
			}
		default:
			trace_util_0.Count(_main_00000, 89)
			if create {
				trace_util_0.Count(_main_00000, 97)
				createStmt, isCreate := st.(*ast.CreateTableStmt)
				if isCreate {
					trace_util_0.Count(_main_00000, 98)
					if err = t.create(createStmt.Table.Name.String(), qText); err != nil {
						trace_util_0.Count(_main_00000, 99)
						break
					}
				} else {
					trace_util_0.Count(_main_00000, 100)
					{
						_, isDrop := st.(*ast.DropTableStmt)
						_, isAnalyze := st.(*ast.AnalyzeTableStmt)
						if isDrop || isAnalyze {
							trace_util_0.Count(_main_00000, 101)
							if err = t.executeDefault(qText); err != nil {
								trace_util_0.Count(_main_00000, 102)
								break
							}
						}
					}
				}
			} else {
				trace_util_0.Count(_main_00000, 103)
				if err = t.executeDefault(qText); err != nil {
					trace_util_0.Count(_main_00000, 104)
					break
				}
			}
		}

		trace_util_0.Count(_main_00000, 80)
		if err != nil && len(t.expectedErrs) > 0 {
			trace_util_0.Count(_main_00000, 105)
			// TODO: check whether this err is expected.
			// but now we think it is.

			// output expected err
			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
		}
		// clear expected errors after we execute the first query
		trace_util_0.Count(_main_00000, 81)
		t.expectedErrs = nil
		t.singleQuery = false

		if err != nil {
			trace_util_0.Count(_main_00000, 106)
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", st.Text(), query.Line, err))
		}

		trace_util_0.Count(_main_00000, 82)
		if !record && !create {
			trace_util_0.Count(_main_00000, 107)
			// check test result now
			gotBuf := t.buf.Bytes()[offset:]

			buf := make([]byte, t.buf.Len()-offset)
			if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
				trace_util_0.Count(_main_00000, 109)
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", st.Text(), query.Line, gotBuf, err))
			}

			trace_util_0.Count(_main_00000, 108)
			if !bytes.Equal(gotBuf, buf) {
				trace_util_0.Count(_main_00000, 110)
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need:\n%s\nbut got:\n%s\n", query.Query, query.Line, buf, gotBuf))
			}
		}
	}
	trace_util_0.Count(_main_00000, 74)
	return errors.Trace(err)
}

func filterWarning(err error) error {
	trace_util_0.Count(_main_00000, 111)
	causeErr := errors.Cause(err)
	if _, ok := causeErr.(mysql.MySQLWarnings); ok {
		trace_util_0.Count(_main_00000, 113)
		return nil
	}
	trace_util_0.Count(_main_00000, 112)
	return err
}

func (t *tester) create(tableName string, qText string) error {
	trace_util_0.Count(_main_00000, 114)
	fmt.Printf("import data for table %s of test %s:\n", tableName, t.name)

	path := "./importer -t \"" + qText + "\" -P 4001 -n 2000 -c 100"
	cmd := exec.Command("sh", "-c", path)
	stdoutIn, err := cmd.StdoutPipe()
	if err != nil {
		trace_util_0.Count(_main_00000, 126)
		log.Error("open stdout pipe failed", zap.Error(err))
	}
	trace_util_0.Count(_main_00000, 115)
	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		trace_util_0.Count(_main_00000, 127)
		log.Error("open stderr pipe failed", zap.Error(err))
	}

	trace_util_0.Count(_main_00000, 116)
	var stdoutBuf, stderrBuf bytes.Buffer
	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)

	if err = cmd.Start(); err != nil {
		trace_util_0.Count(_main_00000, 128)
		return errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 117)
	go func() {
		trace_util_0.Count(_main_00000, 129)
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()
	trace_util_0.Count(_main_00000, 118)
	go func() {
		trace_util_0.Count(_main_00000, 130)
		_, errStderr = io.Copy(stderr, stderrIn)
	}()

	trace_util_0.Count(_main_00000, 119)
	if err = cmd.Wait(); err != nil {
		trace_util_0.Count(_main_00000, 131)
		log.Fatal("importer failed", zap.Error(err))
		return err
	}

	trace_util_0.Count(_main_00000, 120)
	if errStdout != nil {
		trace_util_0.Count(_main_00000, 132)
		return errors.Trace(errStdout)
	}

	trace_util_0.Count(_main_00000, 121)
	if errStderr != nil {
		trace_util_0.Count(_main_00000, 133)
		return errors.Trace(errStderr)
	}

	trace_util_0.Count(_main_00000, 122)
	if err = t.analyze(tableName); err != nil {
		trace_util_0.Count(_main_00000, 134)
		return err
	}

	trace_util_0.Count(_main_00000, 123)
	resp, err := http.Get("http://127.0.0.1:10081/stats/dump/" + dbName + "/" + tableName)
	if err != nil {
		trace_util_0.Count(_main_00000, 135)
		return err
	}

	trace_util_0.Count(_main_00000, 124)
	js, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		trace_util_0.Count(_main_00000, 136)
		return err
	}

	trace_util_0.Count(_main_00000, 125)
	return ioutil.WriteFile(t.statsFileName(tableName), js, 0644)
}

func (t *tester) commit() error {
	trace_util_0.Count(_main_00000, 137)
	err := t.tx.Commit()
	if err != nil {
		trace_util_0.Count(_main_00000, 139)
		return err
	}
	trace_util_0.Count(_main_00000, 138)
	t.tx = nil
	return nil
}

func (t *tester) rollback() error {
	trace_util_0.Count(_main_00000, 140)
	if t.tx == nil {
		trace_util_0.Count(_main_00000, 142)
		return nil
	}
	trace_util_0.Count(_main_00000, 141)
	err := t.tx.Rollback()
	t.tx = nil
	return err
}

func (t *tester) analyze(tableName string) error {
	trace_util_0.Count(_main_00000, 143)
	return t.execute(query{Query: "analyze table " + tableName + ";", Line: 0})
}

func (t *tester) executeStmt(query string) error {
	trace_util_0.Count(_main_00000, 144)
	if session.IsQuery(query) {
		trace_util_0.Count(_main_00000, 146)
		rows, err := t.tx.Query(query)
		if err != nil {
			trace_util_0.Count(_main_00000, 152)
			return errors.Trace(err)
		}
		trace_util_0.Count(_main_00000, 147)
		cols, err := rows.Columns()
		if err != nil {
			trace_util_0.Count(_main_00000, 153)
			return errors.Trace(err)
		}

		trace_util_0.Count(_main_00000, 148)
		for i, c := range cols {
			trace_util_0.Count(_main_00000, 154)
			t.buf.WriteString(c)
			if i != len(cols)-1 {
				trace_util_0.Count(_main_00000, 155)
				t.buf.WriteString("\t")
			}
		}
		trace_util_0.Count(_main_00000, 149)
		t.buf.WriteString("\n")

		values := make([][]byte, len(cols))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			trace_util_0.Count(_main_00000, 156)
			scanArgs[i] = &values[i]
		}

		trace_util_0.Count(_main_00000, 150)
		for rows.Next() {
			trace_util_0.Count(_main_00000, 157)
			err = rows.Scan(scanArgs...)
			if err != nil {
				trace_util_0.Count(_main_00000, 160)
				return errors.Trace(err)
			}

			trace_util_0.Count(_main_00000, 158)
			var value string
			for i, col := range values {
				trace_util_0.Count(_main_00000, 161)
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					trace_util_0.Count(_main_00000, 163)
					value = "NULL"
				} else {
					trace_util_0.Count(_main_00000, 164)
					{
						value = string(col)
					}
				}
				trace_util_0.Count(_main_00000, 162)
				t.buf.WriteString(value)
				if i < len(values)-1 {
					trace_util_0.Count(_main_00000, 165)
					t.buf.WriteString("\t")
				}
			}
			trace_util_0.Count(_main_00000, 159)
			t.buf.WriteString("\n")
		}
		trace_util_0.Count(_main_00000, 151)
		err = rows.Err()
		if err != nil {
			trace_util_0.Count(_main_00000, 166)
			return errors.Trace(err)
		}
	} else {
		trace_util_0.Count(_main_00000, 167)
		{
			// TODO: rows affected and last insert id
			_, err := t.tx.Exec(query)
			if err != nil {
				trace_util_0.Count(_main_00000, 168)
				return errors.Trace(err)
			}
		}
	}
	trace_util_0.Count(_main_00000, 145)
	return nil
}

func (t *tester) openResult() error {
	trace_util_0.Count(_main_00000, 169)
	if record || create {
		trace_util_0.Count(_main_00000, 171)
		return nil
	}

	trace_util_0.Count(_main_00000, 170)
	var err error
	t.resultFD, err = os.Open(t.resultFileName())
	return err
}

func (t *tester) flushResult() error {
	trace_util_0.Count(_main_00000, 172)
	if !record {
		trace_util_0.Count(_main_00000, 174)
		return nil
	}
	trace_util_0.Count(_main_00000, 173)
	return ioutil.WriteFile(t.resultFileName(), t.buf.Bytes(), 0644)
}

func (t *tester) statsFileName(tableName string) string {
	trace_util_0.Count(_main_00000, 175)
	return fmt.Sprintf("./s/%s_%s.json", t.name, tableName)
}

func (t *tester) testFileName() string {
	trace_util_0.Count(_main_00000, 176)
	// test and result must be in current ./t the same as MySQL
	return fmt.Sprintf("./t/%s.test", t.name)
}

func (t *tester) resultFileName() string {
	trace_util_0.Count(_main_00000, 177)
	// test and result must be in current ./r, the same as MySQL
	return fmt.Sprintf("./r/%s.result", t.name)
}

func loadAllTests() ([]string, error) {
	trace_util_0.Count(_main_00000, 178)
	// tests must be in t folder
	files, err := ioutil.ReadDir("./t")
	if err != nil {
		trace_util_0.Count(_main_00000, 181)
		return nil, err
	}

	trace_util_0.Count(_main_00000, 179)
	tests := make([]string, 0, len(files))
	for _, f := range files {
		trace_util_0.Count(_main_00000, 182)
		if f.IsDir() {
			trace_util_0.Count(_main_00000, 184)
			continue
		}

		// the test file must have a suffix .test
		trace_util_0.Count(_main_00000, 183)
		name := f.Name()
		if strings.HasSuffix(name, ".test") {
			trace_util_0.Count(_main_00000, 185)
			name = strings.TrimSuffix(name, ".test")

			if create && !strings.HasSuffix(name, "_stats") {
				trace_util_0.Count(_main_00000, 187)
				continue
			}

			trace_util_0.Count(_main_00000, 186)
			tests = append(tests, name)
		}
	}

	trace_util_0.Count(_main_00000, 180)
	return tests, nil
}

func resultExists(name string) bool {
	trace_util_0.Count(_main_00000, 188)
	resultFile := fmt.Sprintf("./r/%s.result", name)

	if _, err := os.Stat(resultFile); err != nil {
		trace_util_0.Count(_main_00000, 190)
		if os.IsNotExist(err) {
			trace_util_0.Count(_main_00000, 191)
			return false
		}
	}
	trace_util_0.Count(_main_00000, 189)
	return true
}

// openDBWithRetry opens a database specified by its database driver name and a
// driver-specific data source name. And it will do some retries if the connection fails.
func openDBWithRetry(driverName, dataSourceName string) (mdb *sql.DB, err error) {
	trace_util_0.Count(_main_00000, 192)
	startTime := time.Now()
	sleepTime := time.Millisecond * 500
	retryCnt := 60
	// The max retry interval is 30 s.
	for i := 0; i < retryCnt; i++ {
		trace_util_0.Count(_main_00000, 195)
		mdb, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			trace_util_0.Count(_main_00000, 199)
			log.Warn("open DB failed", zap.Int("retry count", i), zap.Error(err))
			time.Sleep(sleepTime)
			continue
		}
		trace_util_0.Count(_main_00000, 196)
		err = mdb.Ping()
		if err == nil {
			trace_util_0.Count(_main_00000, 200)
			break
		}
		trace_util_0.Count(_main_00000, 197)
		log.Warn("ping DB failed", zap.Int("retry count", i), zap.Error(err))
		err = mdb.Close()
		if err != nil {
			trace_util_0.Count(_main_00000, 201)
			log.Error("close DB failed", zap.Error(err))
		}
		trace_util_0.Count(_main_00000, 198)
		time.Sleep(sleepTime)
	}
	trace_util_0.Count(_main_00000, 193)
	if err != nil {
		trace_util_0.Count(_main_00000, 202)
		log.Error("open Db failed", zap.Duration("take time", time.Since(startTime)), zap.Error(err))
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 194)
	return
}

func main() {
	trace_util_0.Count(_main_00000, 203)
	flag.Parse()

	err := logutil.InitZapLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		trace_util_0.Count(_main_00000, 215)
		panic("init logger fail, " + err.Error())
	}

	trace_util_0.Count(_main_00000, 204)
	mdb, err = openDBWithRetry(
		"mysql",
		"root@tcp(localhost:4001)/"+dbName+"?strict=true&allowAllFiles=true",
	)
	if err != nil {
		trace_util_0.Count(_main_00000, 216)
		log.Fatal("open DB failed", zap.Error(err))
	}

	trace_util_0.Count(_main_00000, 205)
	defer func() {
		trace_util_0.Count(_main_00000, 217)
		log.Warn("close DB")
		err = mdb.Close()
		if err != nil {
			trace_util_0.Count(_main_00000, 218)
			log.Error("close DB failed", zap.Error(err))
		}
	}()

	trace_util_0.Count(_main_00000, 206)
	log.Warn("create new DB", zap.Reflect("DB", mdb))

	if _, err = mdb.Exec("DROP DATABASE IF EXISTS test"); err != nil {
		trace_util_0.Count(_main_00000, 219)
		log.Fatal("executing drop DB test failed", zap.Error(err))
	}
	trace_util_0.Count(_main_00000, 207)
	if _, err = mdb.Exec("CREATE DATABASE test"); err != nil {
		trace_util_0.Count(_main_00000, 220)
		log.Fatal("executing create DB test failed", zap.Error(err))
	}
	trace_util_0.Count(_main_00000, 208)
	if _, err = mdb.Exec("USE test"); err != nil {
		trace_util_0.Count(_main_00000, 221)
		log.Fatal("executing use test failed", zap.Error(err))
	}
	trace_util_0.Count(_main_00000, 209)
	if _, err = mdb.Exec("set @@tidb_hash_join_concurrency=1"); err != nil {
		trace_util_0.Count(_main_00000, 222)
		log.Fatal("set @@tidb_hash_join_concurrency=1 failed", zap.Error(err))
	}

	trace_util_0.Count(_main_00000, 210)
	if _, err = mdb.Exec("set sql_mode='STRICT_TRANS_TABLES'"); err != nil {
		trace_util_0.Count(_main_00000, 223)
		log.Fatal("set sql_mode='STRICT_TRANS_TABLES' failed", zap.Error(err))
	}

	trace_util_0.Count(_main_00000, 211)
	tests := flag.Args()

	// we will run all tests if no tests assigned
	if len(tests) == 0 {
		trace_util_0.Count(_main_00000, 224)
		if tests, err = loadAllTests(); err != nil {
			trace_util_0.Count(_main_00000, 225)
			log.Fatal("load all tests failed", zap.Error(err))
		}
	}

	trace_util_0.Count(_main_00000, 212)
	if record {
		trace_util_0.Count(_main_00000, 226)
		log.Info("recording tests", zap.Strings("tests", tests))
	} else {
		trace_util_0.Count(_main_00000, 227)
		if create {
			trace_util_0.Count(_main_00000, 228)
			log.Info("creating data", zap.Strings("tests", tests))
		} else {
			trace_util_0.Count(_main_00000, 229)
			{
				log.Info("running tests", zap.Strings("tests", tests))
			}
		}
	}

	trace_util_0.Count(_main_00000, 213)
	for _, t := range tests {
		trace_util_0.Count(_main_00000, 230)
		if strings.Contains(t, "--log-level") {
			trace_util_0.Count(_main_00000, 233)
			continue
		}
		trace_util_0.Count(_main_00000, 231)
		tr := newTester(t)
		if err = tr.Run(); err != nil {
			trace_util_0.Count(_main_00000, 234)
			log.Fatal("run test", zap.String("test", t), zap.Error(err))
		}
		trace_util_0.Count(_main_00000, 232)
		log.Info("run test ok", zap.String("test", t))
	}

	trace_util_0.Count(_main_00000, 214)
	println("\nGreat, All tests passed")
}

var _main_00000 = "cmd/explaintest/main.go"
