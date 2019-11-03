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

package kv

import (
	"context"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ContextKey is the type of context's key
type ContextKey string

// RunInNewTxn will run the f in a new transaction environment.
func RunInNewTxn(store Storage, retryable bool, f func(txn Transaction) error) error {
	trace_util_0.Count(_txn_00000, 0)
	var (
		err           error
		originalTxnTS uint64
		txn           Transaction
	)
	for i := uint(0); i < maxRetryCnt; i++ {
		trace_util_0.Count(_txn_00000, 2)
		txn, err = store.Begin()
		if err != nil {
			trace_util_0.Count(_txn_00000, 8)
			logutil.Logger(context.Background()).Error("RunInNewTxn", zap.Error(err))
			return err
		}

		// originalTxnTS is used to trace the original transaction when the function is retryable.
		trace_util_0.Count(_txn_00000, 3)
		if i == 0 {
			trace_util_0.Count(_txn_00000, 9)
			originalTxnTS = txn.StartTS()
		}

		trace_util_0.Count(_txn_00000, 4)
		err = f(txn)
		if err != nil {
			trace_util_0.Count(_txn_00000, 10)
			err1 := txn.Rollback()
			terror.Log(err1)
			if retryable && IsTxnRetryableError(err) {
				trace_util_0.Count(_txn_00000, 12)
				logutil.Logger(context.Background()).Warn("RunInNewTxn",
					zap.Uint64("retry txn", txn.StartTS()),
					zap.Uint64("original txn", originalTxnTS),
					zap.Error(err))
				continue
			}
			trace_util_0.Count(_txn_00000, 11)
			return err
		}

		trace_util_0.Count(_txn_00000, 5)
		err = txn.Commit(context.Background())
		if err == nil {
			trace_util_0.Count(_txn_00000, 13)
			break
		}
		trace_util_0.Count(_txn_00000, 6)
		if retryable && IsTxnRetryableError(err) {
			trace_util_0.Count(_txn_00000, 14)
			logutil.Logger(context.Background()).Warn("RunInNewTxn",
				zap.Uint64("retry txn", txn.StartTS()),
				zap.Uint64("original txn", originalTxnTS),
				zap.Error(err))
			BackOff(i)
			continue
		}
		trace_util_0.Count(_txn_00000, 7)
		return err
	}
	trace_util_0.Count(_txn_00000, 1)
	return err
}

var (
	// maxRetryCnt represents maximum retry times in RunInNewTxn.
	maxRetryCnt uint = 100
	// retryBackOffBase is the initial duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffBase = 1
	// retryBackOffCap is the max amount of duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffCap = 100
)

// BackOff Implements exponential backoff with full jitter.
// Returns real back off time in microsecond.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html.
func BackOff(attempts uint) int {
	trace_util_0.Count(_txn_00000, 15)
	upper := int(math.Min(float64(retryBackOffCap), float64(retryBackOffBase)*math.Pow(2.0, float64(attempts))))
	sleep := time.Duration(rand.Intn(upper)) * time.Millisecond
	time.Sleep(sleep)
	return int(sleep)
}

// mockCommitErrorEnable uses to enable `mockCommitError` and only mock error once.
var mockCommitErrorEnable = int64(0)

// MockCommitErrorEnable exports for gofail testing.
func MockCommitErrorEnable() {
	trace_util_0.Count(_txn_00000, 16)
	atomic.StoreInt64(&mockCommitErrorEnable, 1)
}

// MockCommitErrorDisable exports for gofail testing.
func MockCommitErrorDisable() {
	trace_util_0.Count(_txn_00000, 17)
	atomic.StoreInt64(&mockCommitErrorEnable, 0)
}

// IsMockCommitErrorEnable exports for gofail testing.
func IsMockCommitErrorEnable() bool {
	trace_util_0.Count(_txn_00000, 18)
	return atomic.LoadInt64(&mockCommitErrorEnable) == 1
}

var _txn_00000 = "kv/txn.go"
