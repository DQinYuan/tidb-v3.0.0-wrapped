// Copyright 2017 PingCAP, Inc.
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

package tikv

import (
	"context"
	"crypto/tls"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Safe point constants.
const (
	// This is almost the same as 'tikv_gc_safe_point' in the table 'mysql.tidb',
	// save this to pd instead of tikv, because we can't use interface of table
	// if the safepoint on tidb is expired.
	GcSavedSafePoint = "/tidb/store/gcworker/saved_safe_point"

	GcSafePointCacheInterval       = time.Second * 100
	gcCPUTimeInaccuracyBound       = time.Second
	gcSafePointUpdateInterval      = time.Second * 10
	gcSafePointQuickRepeatInterval = time.Second
)

// SafePointKV is used for a seamingless integration for mockTest and runtime.
type SafePointKV interface {
	Put(k string, v string) error
	Get(k string) (string, error)
}

// MockSafePointKV implements SafePointKV at mock test
type MockSafePointKV struct {
	store    map[string]string
	mockLock sync.RWMutex
}

// NewMockSafePointKV creates an instance of MockSafePointKV
func NewMockSafePointKV() *MockSafePointKV {
	trace_util_0.Count(_safepoint_00000, 0)
	return &MockSafePointKV{
		store: make(map[string]string),
	}
}

// Put implements the Put method for SafePointKV
func (w *MockSafePointKV) Put(k string, v string) error {
	trace_util_0.Count(_safepoint_00000, 1)
	w.mockLock.Lock()
	defer w.mockLock.Unlock()
	w.store[k] = v
	return nil
}

// Get implements the Get method for SafePointKV
func (w *MockSafePointKV) Get(k string) (string, error) {
	trace_util_0.Count(_safepoint_00000, 2)
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	elem := w.store[k]
	return elem, nil
}

// EtcdSafePointKV implements SafePointKV at runtime
type EtcdSafePointKV struct {
	cli *clientv3.Client
}

// NewEtcdSafePointKV creates an instance of EtcdSafePointKV
func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config) (*EtcdSafePointKV, error) {
	trace_util_0.Count(_safepoint_00000, 3)
	etcdCli, err := createEtcdKV(addrs, tlsConfig)
	if err != nil {
		trace_util_0.Count(_safepoint_00000, 5)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_safepoint_00000, 4)
	return &EtcdSafePointKV{cli: etcdCli}, nil
}

// Put implements the Put method for SafePointKV
func (w *EtcdSafePointKV) Put(k string, v string) error {
	trace_util_0.Count(_safepoint_00000, 6)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	_, err := w.cli.Put(ctx, k, v)
	cancel()
	if err != nil {
		trace_util_0.Count(_safepoint_00000, 8)
		return errors.Trace(err)
	}
	trace_util_0.Count(_safepoint_00000, 7)
	return nil
}

// Get implements the Get method for SafePointKV
func (w *EtcdSafePointKV) Get(k string) (string, error) {
	trace_util_0.Count(_safepoint_00000, 9)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	resp, err := w.cli.Get(ctx, k)
	cancel()
	if err != nil {
		trace_util_0.Count(_safepoint_00000, 12)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_safepoint_00000, 10)
	if len(resp.Kvs) > 0 {
		trace_util_0.Count(_safepoint_00000, 13)
		return string(resp.Kvs[0].Value), nil
	}
	trace_util_0.Count(_safepoint_00000, 11)
	return "", nil
}

func saveSafePoint(kv SafePointKV, key string, t uint64) error {
	trace_util_0.Count(_safepoint_00000, 14)
	s := strconv.FormatUint(t, 10)
	err := kv.Put(GcSavedSafePoint, s)
	if err != nil {
		trace_util_0.Count(_safepoint_00000, 16)
		logutil.Logger(context.Background()).Error("save safepoint failed", zap.Error(err))
		return errors.Trace(err)
	}
	trace_util_0.Count(_safepoint_00000, 15)
	return nil
}

func loadSafePoint(kv SafePointKV, key string) (uint64, error) {
	trace_util_0.Count(_safepoint_00000, 17)
	str, err := kv.Get(GcSavedSafePoint)

	if err != nil {
		trace_util_0.Count(_safepoint_00000, 21)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_safepoint_00000, 18)
	if str == "" {
		trace_util_0.Count(_safepoint_00000, 22)
		return 0, nil
	}

	trace_util_0.Count(_safepoint_00000, 19)
	t, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		trace_util_0.Count(_safepoint_00000, 23)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_safepoint_00000, 20)
	return t, nil
}

var _safepoint_00000 = "store/tikv/safepoint.go"
