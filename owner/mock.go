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

package owner

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockManager struct {
	owner  int32
	id     string // id is the ID of manager.
	cancel context.CancelFunc
}

// NewMockManager creates a new mock Manager.
func NewMockManager(id string, cancel context.CancelFunc) Manager {
	trace_util_0.Count(_mock_00000, 0)
	return &mockManager{
		id:     id,
		cancel: cancel,
	}
}

// ID implements Manager.ID interface.
func (m *mockManager) ID() string {
	trace_util_0.Count(_mock_00000, 1)
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *mockManager) IsOwner() bool {
	trace_util_0.Count(_mock_00000, 2)
	return atomic.LoadInt32(&m.owner) == 1
}

func (m *mockManager) toBeOwner() {
	trace_util_0.Count(_mock_00000, 3)
	atomic.StoreInt32(&m.owner, 1)
}

// RetireOwner implements Manager.RetireOwner interface.
func (m *mockManager) RetireOwner() {
	trace_util_0.Count(_mock_00000, 4)
	atomic.StoreInt32(&m.owner, 0)
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	trace_util_0.Count(_mock_00000, 5)
	m.cancel()
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *mockManager) GetOwnerID(ctx context.Context) (string, error) {
	trace_util_0.Count(_mock_00000, 6)
	if m.IsOwner() {
		trace_util_0.Count(_mock_00000, 8)
		return m.ID(), nil
	}
	trace_util_0.Count(_mock_00000, 7)
	return "", errors.New("no owner")
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *mockManager) CampaignOwner(_ context.Context) error {
	trace_util_0.Count(_mock_00000, 9)
	m.toBeOwner()
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *mockManager) ResignOwner(ctx context.Context) error {
	trace_util_0.Count(_mock_00000, 10)
	if m.IsOwner() {
		trace_util_0.Count(_mock_00000, 12)
		m.RetireOwner()
	}
	trace_util_0.Count(_mock_00000, 11)
	return nil
}

var _mock_00000 = "owner/mock.go"
