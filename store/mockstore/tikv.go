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

package mockstore

import (
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
)

// MockDriver is in memory mock TiKV driver.
type MockDriver struct {
}

// Open creates a MockTiKV storage.
func (d MockDriver) Open(path string) (kv.Storage, error) {
	trace_util_0.Count(_tikv_00000, 0)
	u, err := url.Parse(path)
	if err != nil {
		trace_util_0.Count(_tikv_00000, 4)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_tikv_00000, 1)
	if !strings.EqualFold(u.Scheme, "mocktikv") {
		trace_util_0.Count(_tikv_00000, 5)
		return nil, errors.Errorf("Uri scheme expected(mocktikv) but found (%s)", u.Scheme)
	}

	trace_util_0.Count(_tikv_00000, 2)
	opts := []MockTiKVStoreOption{WithPath(u.Path)}
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	if txnLocalLatches.Enabled {
		trace_util_0.Count(_tikv_00000, 6)
		opts = append(opts, WithTxnLocalLatches(txnLocalLatches.Capacity))
	}
	trace_util_0.Count(_tikv_00000, 3)
	return NewMockTikvStore(opts...)
}

type mockOptions struct {
	cluster         *mocktikv.Cluster
	mvccStore       mocktikv.MVCCStore
	clientHijack    func(tikv.Client) tikv.Client
	pdClientHijack  func(pd.Client) pd.Client
	path            string
	txnLocalLatches uint
}

// MockTiKVStoreOption is used to control some behavior of mock tikv.
type MockTiKVStoreOption func(*mockOptions)

// WithHijackClient hijacks KV client's behavior, makes it easy to simulate the network
// problem between TiDB and TiKV.
func WithHijackClient(wrap func(tikv.Client) tikv.Client) MockTiKVStoreOption {
	trace_util_0.Count(_tikv_00000, 7)
	return func(c *mockOptions) {
		trace_util_0.Count(_tikv_00000, 8)
		c.clientHijack = wrap
	}
}

// WithCluster provides the customized cluster.
func WithCluster(cluster *mocktikv.Cluster) MockTiKVStoreOption {
	trace_util_0.Count(_tikv_00000, 9)
	return func(c *mockOptions) {
		trace_util_0.Count(_tikv_00000, 10)
		c.cluster = cluster
	}
}

// WithMVCCStore provides the customized mvcc store.
func WithMVCCStore(store mocktikv.MVCCStore) MockTiKVStoreOption {
	trace_util_0.Count(_tikv_00000, 11)
	return func(c *mockOptions) {
		trace_util_0.Count(_tikv_00000, 12)
		c.mvccStore = store
	}
}

// WithPath specifies the mocktikv path.
func WithPath(path string) MockTiKVStoreOption {
	trace_util_0.Count(_tikv_00000, 13)
	return func(c *mockOptions) {
		trace_util_0.Count(_tikv_00000, 14)
		c.path = path
	}
}

// WithTxnLocalLatches enable txnLocalLatches, when capacity > 0.
func WithTxnLocalLatches(capacity uint) MockTiKVStoreOption {
	trace_util_0.Count(_tikv_00000, 15)
	return func(c *mockOptions) {
		trace_util_0.Count(_tikv_00000, 16)
		c.txnLocalLatches = capacity
	}
}

// NewMockTikvStore creates a mocked tikv store, the path is the file path to store the data.
// If path is an empty string, a memory storage will be created.
func NewMockTikvStore(options ...MockTiKVStoreOption) (kv.Storage, error) {
	trace_util_0.Count(_tikv_00000, 17)
	var opt mockOptions
	for _, f := range options {
		trace_util_0.Count(_tikv_00000, 20)
		f(&opt)
	}

	trace_util_0.Count(_tikv_00000, 18)
	client, pdClient, err := mocktikv.NewTiKVAndPDClient(opt.cluster, opt.mvccStore, opt.path)
	if err != nil {
		trace_util_0.Count(_tikv_00000, 21)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_tikv_00000, 19)
	return tikv.NewTestTiKVStore(client, pdClient, opt.clientHijack, opt.pdClientHijack, opt.txnLocalLatches)
}

var _tikv_00000 = "store/mockstore/tikv.go"
