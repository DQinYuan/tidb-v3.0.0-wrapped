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

package mock

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
)

// Store implements kv.Storage interface.
type Store struct {
	Client kv.Client
}

// GetClient implements kv.Storage interface.
func (s *Store) GetClient() kv.Client { trace_util_0.Count(_store_00000, 0); return s.Client }

// GetOracle implements kv.Storage interface.
func (s *Store) GetOracle() oracle.Oracle { trace_util_0.Count(_store_00000, 1); return nil }

// Begin implements kv.Storage interface.
func (s *Store) Begin() (kv.Transaction, error) { trace_util_0.Count(_store_00000, 2); return nil, nil }

// BeginWithStartTS implements kv.Storage interface.
func (s *Store) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	trace_util_0.Count(_store_00000, 3)
	return s.Begin()
}

// GetSnapshot implements kv.Storage interface.
func (s *Store) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	trace_util_0.Count(_store_00000, 4)
	return nil, nil
}

// Close implements kv.Storage interface.
func (s *Store) Close() error { trace_util_0.Count(_store_00000, 5); return nil }

// UUID implements kv.Storage interface.
func (s *Store) UUID() string { trace_util_0.Count(_store_00000, 6); return "mock" }

// CurrentVersion implements kv.Storage interface.
func (s *Store) CurrentVersion() (kv.Version, error) {
	trace_util_0.Count(_store_00000, 7)
	return kv.Version{}, nil
}

// SupportDeleteRange implements kv.Storage interface.
func (s *Store) SupportDeleteRange() bool { trace_util_0.Count(_store_00000, 8); return false }

// Name implements kv.Storage interface.
func (s *Store) Name() string { trace_util_0.Count(_store_00000, 9); return "UtilMockStorage" }

// Describe implements kv.Storage interface.
func (s *Store) Describe() string {
	trace_util_0.Count(_store_00000, 10)
	return "UtilMockStorage is a mock Store implementation, only for unittests in util package"
}

// ShowStatus implements kv.Storage interface.
func (s *Store) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	trace_util_0.Count(_store_00000, 11)
	return nil, nil
}

var _store_00000 = "util/mock/store.go"
