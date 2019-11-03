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
	"github.com/pingcap/tidb/trace_util_0"
	"sync"
)

// InjectionConfig is used for fault injections for KV components.
type InjectionConfig struct {
	sync.RWMutex
	getError    error // kv.Get() always return this error.
	commitError error // Transaction.Commit() always return this error.
}

// SetGetError injects an error for all kv.Get() methods.
func (c *InjectionConfig) SetGetError(err error) {
	trace_util_0.Count(_fault_injection_00000, 0)
	c.Lock()
	defer c.Unlock()

	c.getError = err
}

// SetCommitError injects an error for all Transaction.Commit() methods.
func (c *InjectionConfig) SetCommitError(err error) {
	trace_util_0.Count(_fault_injection_00000, 1)
	c.Lock()
	defer c.Unlock()
	c.commitError = err
}

// InjectedStore wraps a Storage with injections.
type InjectedStore struct {
	Storage
	cfg *InjectionConfig
}

// NewInjectedStore creates a InjectedStore with config.
func NewInjectedStore(store Storage, cfg *InjectionConfig) Storage {
	trace_util_0.Count(_fault_injection_00000, 2)
	return &InjectedStore{
		Storage: store,
		cfg:     cfg,
	}
}

// Begin creates an injected Transaction.
func (s *InjectedStore) Begin() (Transaction, error) {
	trace_util_0.Count(_fault_injection_00000, 3)
	txn, err := s.Storage.Begin()
	return &InjectedTransaction{
		Transaction: txn,
		cfg:         s.cfg,
	}, err
}

// BeginWithStartTS creates an injected Transaction with startTS.
func (s *InjectedStore) BeginWithStartTS(startTS uint64) (Transaction, error) {
	trace_util_0.Count(_fault_injection_00000, 4)
	txn, err := s.Storage.BeginWithStartTS(startTS)
	return &InjectedTransaction{
		Transaction: txn,
		cfg:         s.cfg,
	}, err
}

// GetSnapshot creates an injected Snapshot.
func (s *InjectedStore) GetSnapshot(ver Version) (Snapshot, error) {
	trace_util_0.Count(_fault_injection_00000, 5)
	snapshot, err := s.Storage.GetSnapshot(ver)
	return &InjectedSnapshot{
		Snapshot: snapshot,
		cfg:      s.cfg,
	}, err
}

// InjectedTransaction wraps a Transaction with injections.
type InjectedTransaction struct {
	Transaction
	cfg *InjectionConfig
}

// Get returns an error if cfg.getError is set.
func (t *InjectedTransaction) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_fault_injection_00000, 6)
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		trace_util_0.Count(_fault_injection_00000, 8)
		return nil, t.cfg.getError
	}
	trace_util_0.Count(_fault_injection_00000, 7)
	return t.Transaction.Get(k)
}

// BatchGet returns an error if cfg.getError is set.
func (t *InjectedTransaction) BatchGet(keys []Key) (map[string][]byte, error) {
	trace_util_0.Count(_fault_injection_00000, 9)
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		trace_util_0.Count(_fault_injection_00000, 11)
		return nil, t.cfg.getError
	}
	trace_util_0.Count(_fault_injection_00000, 10)
	return t.Transaction.BatchGet(keys)
}

// Commit returns an error if cfg.commitError is set.
func (t *InjectedTransaction) Commit(ctx context.Context) error {
	trace_util_0.Count(_fault_injection_00000, 12)
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.commitError != nil {
		trace_util_0.Count(_fault_injection_00000, 14)
		return t.cfg.commitError
	}
	trace_util_0.Count(_fault_injection_00000, 13)
	return t.Transaction.Commit(ctx)
}

// InjectedSnapshot wraps a Snapshot with injections.
type InjectedSnapshot struct {
	Snapshot
	cfg *InjectionConfig
}

// Get returns an error if cfg.getError is set.
func (t *InjectedSnapshot) Get(k Key) ([]byte, error) {
	trace_util_0.Count(_fault_injection_00000, 15)
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		trace_util_0.Count(_fault_injection_00000, 17)
		return nil, t.cfg.getError
	}
	trace_util_0.Count(_fault_injection_00000, 16)
	return t.Snapshot.Get(k)
}

// BatchGet returns an error if cfg.getError is set.
func (t *InjectedSnapshot) BatchGet(keys []Key) (map[string][]byte, error) {
	trace_util_0.Count(_fault_injection_00000, 18)
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		trace_util_0.Count(_fault_injection_00000, 20)
		return nil, t.cfg.getError
	}
	trace_util_0.Count(_fault_injection_00000, 19)
	return t.Snapshot.BatchGet(keys)
}

var _fault_injection_00000 = "kv/fault_injection.go"
