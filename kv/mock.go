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

package kv

import (
	"context"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
)

// mockTxn is a txn that returns a retryAble error when called Commit.
type mockTxn struct {
	opts  map[Option]interface{}
	valid bool
}

// Commit always returns a retryable error.
func (t *mockTxn) Commit(ctx context.Context) error {
	trace_util_0.Count(_mock_00000, 0)
	return ErrTxnRetryable
}

func (t *mockTxn) Rollback() error {
	trace_util_0.Count(_mock_00000, 1)
	t.valid = false
	return nil
}

func (t *mockTxn) String() string {
	trace_util_0.Count(_mock_00000, 2)
	return ""
}

func (t *mockTxn) LockKeys(_ context.Context, _ uint64, _ ...Key) error {
	trace_util_0.Count(_mock_00000, 3)
	return nil
}

func (t *mockTxn) SetOption(opt Option, val interface{}) {
	trace_util_0.Count(_mock_00000, 4)
	t.opts[opt] = val
}

func (t *mockTxn) DelOption(opt Option) {
	trace_util_0.Count(_mock_00000, 5)
	delete(t.opts, opt)
}

func (t *mockTxn) GetOption(opt Option) interface{} {
	trace_util_0.Count(_mock_00000, 6)
	return t.opts[opt]
}

func (t *mockTxn) IsReadOnly() bool {
	trace_util_0.Count(_mock_00000, 7)
	return true
}

func (t *mockTxn) StartTS() uint64 {
	trace_util_0.Count(_mock_00000, 8)
	return uint64(0)
}
func (t *mockTxn) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_mock_00000, 9)
	return nil, nil
}

func (t *mockTxn) BatchGet(keys []Key) (map[string][]byte, error) {
	trace_util_0.Count(_mock_00000, 10)
	return nil, nil
}

func (t *mockTxn) Iter(k Key, upperBound Key) (Iterator, error) {
	trace_util_0.Count(_mock_00000, 11)
	return nil, nil
}

func (t *mockTxn) IterReverse(k Key) (Iterator, error) {
	trace_util_0.Count(_mock_00000, 12)
	return nil, nil
}

func (t *mockTxn) Set(k Key, v []byte) error {
	trace_util_0.Count(_mock_00000, 13)
	return nil
}
func (t *mockTxn) Delete(k Key) error {
	trace_util_0.Count(_mock_00000, 14)
	return nil
}

func (t *mockTxn) Valid() bool {
	trace_util_0.Count(_mock_00000, 15)
	return t.valid
}

func (t *mockTxn) Len() int {
	trace_util_0.Count(_mock_00000, 16)
	return 0
}

func (t *mockTxn) Size() int {
	trace_util_0.Count(_mock_00000, 17)
	return 0
}

func (t *mockTxn) GetMemBuffer() MemBuffer {
	trace_util_0.Count(_mock_00000, 18)
	return nil
}

func (t *mockTxn) SetCap(cap int) {
	trace_util_0.Count(_mock_00000, 19)

}

func (t *mockTxn) Reset() {
	trace_util_0.Count(_mock_00000, 20)
	t.valid = false
}

func (t *mockTxn) SetVars(vars *Variables) {
	trace_util_0.Count(_mock_00000, 21)

}

func (t *mockTxn) SetAssertion(key Key, assertion AssertionType) { trace_util_0.Count(_mock_00000, 22) }

// NewMockTxn new a mockTxn.
func NewMockTxn() Transaction {
	trace_util_0.Count(_mock_00000, 23)
	return &mockTxn{
		opts:  make(map[Option]interface{}),
		valid: true,
	}
}

// mockStorage is used to start a must commit-failed txn.
type mockStorage struct {
}

func (s *mockStorage) Begin() (Transaction, error) {
	trace_util_0.Count(_mock_00000, 24)
	tx := &mockTxn{
		opts:  make(map[Option]interface{}),
		valid: true,
	}
	return tx, nil
}

func (*mockTxn) IsPessimistic() bool {
	trace_util_0.Count(_mock_00000, 25)
	return false
}

// BeginWithStartTS begins a transaction with startTS.
func (s *mockStorage) BeginWithStartTS(startTS uint64) (Transaction, error) {
	trace_util_0.Count(_mock_00000, 26)
	return s.Begin()
}

func (s *mockStorage) GetSnapshot(ver Version) (Snapshot, error) {
	trace_util_0.Count(_mock_00000, 27)
	return &mockSnapshot{
		store: NewMemDbBuffer(DefaultTxnMembufCap),
	}, nil
}

func (s *mockStorage) Close() error {
	trace_util_0.Count(_mock_00000, 28)
	return nil
}

func (s *mockStorage) UUID() string {
	trace_util_0.Count(_mock_00000, 29)
	return ""
}

// CurrentVersion returns current max committed version.
func (s *mockStorage) CurrentVersion() (Version, error) {
	trace_util_0.Count(_mock_00000, 30)
	return NewVersion(1), nil
}

func (s *mockStorage) GetClient() Client {
	trace_util_0.Count(_mock_00000, 31)
	return nil
}

func (s *mockStorage) GetOracle() oracle.Oracle {
	trace_util_0.Count(_mock_00000, 32)
	return nil
}

func (s *mockStorage) SupportDeleteRange() (supported bool) {
	trace_util_0.Count(_mock_00000, 33)
	return false
}

func (s *mockStorage) Name() string {
	trace_util_0.Count(_mock_00000, 34)
	return "KVMockStorage"
}

func (s *mockStorage) Describe() string {
	trace_util_0.Count(_mock_00000, 35)
	return "KVMockStorage is a mock Store implementation, only for unittests in KV package"
}

func (s *mockStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	trace_util_0.Count(_mock_00000, 36)
	return nil, nil
}

// MockTxn is used for test cases that need more interfaces than Transaction.
type MockTxn interface {
	Transaction
	GetOption(opt Option) interface{}
}

// NewMockStorage creates a new mockStorage.
func NewMockStorage() Storage {
	trace_util_0.Count(_mock_00000, 37)
	return &mockStorage{}
}

type mockSnapshot struct {
	store MemBuffer
}

func (s *mockSnapshot) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_mock_00000, 38)
	return s.store.Get(k)
}

func (s *mockSnapshot) SetPriority(priority int) {
	trace_util_0.Count(_mock_00000, 39)

}

func (s *mockSnapshot) BatchGet(keys []Key) (map[string][]byte, error) {
	trace_util_0.Count(_mock_00000, 40)
	m := make(map[string][]byte)
	for _, k := range keys {
		trace_util_0.Count(_mock_00000, 42)
		v, err := s.store.Get(k)
		if IsErrNotFound(err) {
			trace_util_0.Count(_mock_00000, 45)
			continue
		}
		trace_util_0.Count(_mock_00000, 43)
		if err != nil {
			trace_util_0.Count(_mock_00000, 46)
			return nil, err
		}
		trace_util_0.Count(_mock_00000, 44)
		m[string(k)] = v
	}
	trace_util_0.Count(_mock_00000, 41)
	return m, nil
}

func (s *mockSnapshot) Iter(k Key, upperBound Key) (Iterator, error) {
	trace_util_0.Count(_mock_00000, 47)
	return s.store.Iter(k, upperBound)
}

func (s *mockSnapshot) IterReverse(k Key) (Iterator, error) {
	trace_util_0.Count(_mock_00000, 48)
	return s.store.IterReverse(k)
}

var _mock_00000 = "kv/mock.go"
