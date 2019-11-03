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

package util

import (
	"context"
	"runtime"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultMaxRetries indicates the max retry count.
	DefaultMaxRetries = 30
	// RetryInterval indicates retry interval.
	RetryInterval uint64 = 500
	// GCTimeFormat is the format that gc_worker used to store times.
	GCTimeFormat = "20060102-15:04:05 -0700"
)

// RunWithRetry will run the f with backoff and retry.
// retryCnt: Max retry count
// backoff: When run f failed, it will sleep backoff * triedCount time.Millisecond.
// Function f should have two return value. The first one is an bool which indicate if the err if retryable.
// The second is if the f meet any error.
func RunWithRetry(retryCnt int, backoff uint64, f func() (bool, error)) (err error) {
	trace_util_0.Count(_misc_00000, 0)
	for i := 1; i <= retryCnt; i++ {
		trace_util_0.Count(_misc_00000, 2)
		var retryAble bool
		retryAble, err = f()
		if err == nil || !retryAble {
			trace_util_0.Count(_misc_00000, 4)
			return errors.Trace(err)
		}
		trace_util_0.Count(_misc_00000, 3)
		sleepTime := time.Duration(backoff*uint64(i)) * time.Millisecond
		time.Sleep(sleepTime)
	}
	trace_util_0.Count(_misc_00000, 1)
	return errors.Trace(err)
}

// GetStack gets the stacktrace.
func GetStack() []byte {
	trace_util_0.Count(_misc_00000, 5)
	const size = 4096
	buf := make([]byte, size)
	stackSize := runtime.Stack(buf, false)
	buf = buf[:stackSize]
	return buf
}

// WithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//   exec:      execute logic function.
//   recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
func WithRecovery(exec func(), recoverFn func(r interface{})) {
	trace_util_0.Count(_misc_00000, 6)
	defer func() {
		trace_util_0.Count(_misc_00000, 8)
		r := recover()
		if recoverFn != nil {
			trace_util_0.Count(_misc_00000, 10)
			recoverFn(r)
		}
		trace_util_0.Count(_misc_00000, 9)
		if r != nil {
			trace_util_0.Count(_misc_00000, 11)
			logutil.Logger(context.Background()).Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	trace_util_0.Count(_misc_00000, 7)
	exec()
}

// CompatibleParseGCTime parses a string with `GCTimeFormat` and returns a time.Time. If `value` can't be parsed as that
// format, truncate to last space and try again. This function is only useful when loading times that saved by
// gc_worker. We have changed the format that gc_worker saves time (removed the last field), but when loading times it
// should be compatible with the old format.
func CompatibleParseGCTime(value string) (time.Time, error) {
	trace_util_0.Count(_misc_00000, 12)
	t, err := time.Parse(GCTimeFormat, value)

	if err != nil {
		trace_util_0.Count(_misc_00000, 15)
		// Remove the last field that separated by space
		parts := strings.Split(value, " ")
		prefix := strings.Join(parts[:len(parts)-1], " ")
		t, err = time.Parse(GCTimeFormat, prefix)
	}

	trace_util_0.Count(_misc_00000, 13)
	if err != nil {
		trace_util_0.Count(_misc_00000, 16)
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\"", value, GCTimeFormat)
	}
	trace_util_0.Count(_misc_00000, 14)
	return t, err
}

const (
	// syntaxErrorPrefix is the common prefix for SQL syntax error in TiDB.
	syntaxErrorPrefix = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use"
)

// SyntaxError converts parser error to TiDB's syntax error.
func SyntaxError(err error) error {
	trace_util_0.Count(_misc_00000, 17)
	if err == nil {
		trace_util_0.Count(_misc_00000, 20)
		return nil
	}
	trace_util_0.Count(_misc_00000, 18)
	logutil.Logger(context.Background()).Error("syntax error", zap.Error(err))

	// If the error is already a terror with stack, pass it through.
	if errors.HasStack(err) {
		trace_util_0.Count(_misc_00000, 21)
		cause := errors.Cause(err)
		if _, ok := cause.(*terror.Error); ok {
			trace_util_0.Count(_misc_00000, 22)
			return err
		}
	}

	trace_util_0.Count(_misc_00000, 19)
	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

// SyntaxWarn converts parser warn to TiDB's syntax warn.
func SyntaxWarn(err error) error {
	trace_util_0.Count(_misc_00000, 23)
	if err == nil {
		trace_util_0.Count(_misc_00000, 25)
		return nil
	}
	trace_util_0.Count(_misc_00000, 24)
	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

var _misc_00000 = "util/misc.go"
