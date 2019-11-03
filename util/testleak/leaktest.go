// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
// +build leak

package testleak

import (
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/trace_util_0"
)

func interestingGoroutines() (gs []string) {
	trace_util_0.Count(_leaktest_00000, 0)
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		trace_util_0.Count(_leaktest_00000, 2)
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			trace_util_0.Count(_leaktest_00000, 5)
			continue
		}
		trace_util_0.Count(_leaktest_00000, 3)
		stack := strings.TrimSpace(sl[1])
		if stack == "" ||
			strings.Contains(stack, "created by github.com/pingcap/tidb.init") ||
			strings.Contains(stack, "testing.RunTests") ||
			strings.Contains(stack, "check.(*resultTracker).start") ||
			strings.Contains(stack, "check.(*suiteRunner).runFunc") ||
			strings.Contains(stack, "check.(*suiteRunner).parallelRun") ||
			strings.Contains(stack, "localstore.(*dbStore).scheduler") ||
			strings.Contains(stack, "tikv.(*noGCHandler).Start") ||
			strings.Contains(stack, "ddl.(*ddl).start") ||
			strings.Contains(stack, "ddl.(*delRange).startEmulator") ||
			strings.Contains(stack, "domain.NewDomain") ||
			strings.Contains(stack, "testing.(*T).Run") ||
			strings.Contains(stack, "domain.(*Domain).LoadPrivilegeLoop") ||
			strings.Contains(stack, "domain.(*Domain).UpdateTableStatsLoop") ||
			strings.Contains(stack, "testing.Main(") ||
			strings.Contains(stack, "runtime.goexit") ||
			strings.Contains(stack, "created by runtime.gc") ||
			strings.Contains(stack, "interestingGoroutines") ||
			strings.Contains(stack, "runtime.MHeap_Scavenger") {
			trace_util_0.Count(_leaktest_00000, 6)
			continue
		}
		trace_util_0.Count(_leaktest_00000, 4)
		gs = append(gs, stack)
	}
	trace_util_0.Count(_leaktest_00000, 1)
	sort.Strings(gs)
	return
}

var beforeTestGorountines = map[string]bool{}

// BeforeTest gets the current goroutines.
// It's used for check.Suite.SetUpSuite() function.
// Now it's only used in the tidb_test.go.
func BeforeTest() {
	trace_util_0.Count(_leaktest_00000, 7)
	for _, g := range interestingGoroutines() {
		trace_util_0.Count(_leaktest_00000, 8)
		beforeTestGorountines[g] = true
	}
}

const defaultCheckCnt = 50

func checkLeakAfterTest(errorFunc func(cnt int, g string)) func() {
	trace_util_0.Count(_leaktest_00000, 9)
	if len(beforeTestGorountines) == 0 {
		trace_util_0.Count(_leaktest_00000, 11)
		for _, g := range interestingGoroutines() {
			trace_util_0.Count(_leaktest_00000, 12)
			beforeTestGorountines[g] = true
		}
	}

	trace_util_0.Count(_leaktest_00000, 10)
	cnt := defaultCheckCnt
	return func() {
		trace_util_0.Count(_leaktest_00000, 13)
		defer func() {
			trace_util_0.Count(_leaktest_00000, 16)
			beforeTestGorountines = map[string]bool{}
		}()

		trace_util_0.Count(_leaktest_00000, 14)
		var leaked []string
		for i := 0; i < cnt; i++ {
			trace_util_0.Count(_leaktest_00000, 17)
			leaked = leaked[:0]
			for _, g := range interestingGoroutines() {
				trace_util_0.Count(_leaktest_00000, 20)
				if !beforeTestGorountines[g] {
					trace_util_0.Count(_leaktest_00000, 21)
					leaked = append(leaked, g)
				}
			}
			// Bad stuff found, but goroutines might just still be
			// shutting down, so give it some time.
			trace_util_0.Count(_leaktest_00000, 18)
			if len(leaked) != 0 {
				trace_util_0.Count(_leaktest_00000, 22)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			trace_util_0.Count(_leaktest_00000, 19)
			return
		}
		trace_util_0.Count(_leaktest_00000, 15)
		for _, g := range leaked {
			trace_util_0.Count(_leaktest_00000, 23)
			errorFunc(cnt, g)
		}
	}
}

// AfterTest gets the current goroutines and runs the returned function to
// get the goroutines at that time to contrast whether any goroutines leaked.
// Usage: defer testleak.AfterTest(c)()
// It can call with BeforeTest() at the beginning of check.Suite.TearDownSuite() or
// call alone at the beginning of each test.
func AfterTest(c *check.C) func() {
	trace_util_0.Count(_leaktest_00000, 24)
	errorFunc := func(cnt int, g string) {
		trace_util_0.Count(_leaktest_00000, 26)
		c.Errorf("Test %s check-count %d appears to have leaked: %v", c.TestName(), cnt, g)
	}
	trace_util_0.Count(_leaktest_00000, 25)
	return checkLeakAfterTest(errorFunc)
}

// AfterTestT is used after all the test cases is finished.
func AfterTestT(t *testing.T) func() {
	trace_util_0.Count(_leaktest_00000, 27)
	errorFunc := func(cnt int, g string) {
		trace_util_0.Count(_leaktest_00000, 29)
		t.Errorf("Test %s check-count %d appears to have leaked: %v", t.Name(), cnt, g)
	}
	trace_util_0.Count(_leaktest_00000, 28)
	return checkLeakAfterTest(errorFunc)
}

var _leaktest_00000 = "util/testleak/leaktest.go"
