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
	"github.com/pingcap/errors"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/twinj/uuid"
)

// NewTestTiKVStore creates a test store with Option
func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint) (kv.Storage, error) {
	trace_util_0.Count(_test_util_00000, 0)
	if clientHijack != nil {
		trace_util_0.Count(_test_util_00000, 4)
		client = clientHijack(client)
	}

	trace_util_0.Count(_test_util_00000, 1)
	pdCli := pd.Client(&codecPDClient{pdClient})
	if pdClientHijack != nil {
		trace_util_0.Count(_test_util_00000, 5)
		pdCli = pdClientHijack(pdCli)
	}

	// Make sure the uuid is unique.
	trace_util_0.Count(_test_util_00000, 2)
	uid := uuid.NewV4().String()
	spkv := NewMockSafePointKV()
	tikvStore, err := newTikvStore(uid, pdCli, spkv, client, false)

	if txnLocalLatches > 0 {
		trace_util_0.Count(_test_util_00000, 6)
		tikvStore.EnableTxnLocalLatches(txnLocalLatches)
	}

	trace_util_0.Count(_test_util_00000, 3)
	tikvStore.mock = true
	return tikvStore, errors.Trace(err)
}

var _test_util_00000 = "store/tikv/test_util.go"
