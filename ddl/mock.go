// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://wwm.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

var _ SchemaSyncer = &MockSchemaSyncer{}

const mockCheckVersInterval = 2 * time.Millisecond

// MockSchemaSyncer is a mock schema syncer, it is exported for tesing.
type MockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
	mockSession       chan struct{}
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() SchemaSyncer {
	trace_util_0.Count(_mock_00000, 0)
	return &MockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *MockSchemaSyncer) Init(ctx context.Context) error {
	trace_util_0.Count(_mock_00000, 1)
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *MockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	trace_util_0.Count(_mock_00000, 2)
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *MockSchemaSyncer) WatchGlobalSchemaVer(context.Context) { trace_util_0.Count(_mock_00000, 3) }

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *MockSchemaSyncer) UpdateSelfVersion(ctx context.Context, version int64) error {
	trace_util_0.Count(_mock_00000, 4)
	atomic.StoreInt64(&s.selfSchemaVersion, version)
	return nil
}

// Done implements SchemaSyncer.Done interface.
func (s *MockSchemaSyncer) Done() <-chan struct{} {
	trace_util_0.Count(_mock_00000, 5)
	return s.mockSession
}

// CloseSession mockSession, it is exported for testing.
func (s *MockSchemaSyncer) CloseSession() {
	trace_util_0.Count(_mock_00000, 6)
	close(s.mockSession)
}

// Restart implements SchemaSyncer.Restart interface.
func (s *MockSchemaSyncer) Restart(_ context.Context) error {
	trace_util_0.Count(_mock_00000, 7)
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *MockSchemaSyncer) RemoveSelfVersionPath() error {
	trace_util_0.Count(_mock_00000, 8)
	return nil
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *MockSchemaSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	trace_util_0.Count(_mock_00000, 9)
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
		trace_util_0.Count(_mock_00000, 11)
	default:
		trace_util_0.Count(_mock_00000, 12)
	}
	trace_util_0.Count(_mock_00000, 10)
	return nil
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *MockSchemaSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	trace_util_0.Count(_mock_00000, 13)
	return 0, nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *MockSchemaSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	trace_util_0.Count(_mock_00000, 14)
	ticker := time.NewTicker(mockCheckVersInterval)
	defer ticker.Stop()

	for {
		trace_util_0.Count(_mock_00000, 15)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_mock_00000, 16)
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			trace_util_0.Count(_mock_00000, 17)
			ver := atomic.LoadInt64(&s.selfSchemaVersion)
			if ver == latestVer {
				trace_util_0.Count(_mock_00000, 18)
				return nil
			}
		}
	}
}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	trace_util_0.Count(_mock_00000, 19)
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (dr *mockDelRange) addDelRangeJob(job *model.Job) error {
	trace_util_0.Count(_mock_00000, 20)
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *mockDelRange) removeFromGCDeleteRange(jobID, tableID int64) error {
	trace_util_0.Count(_mock_00000, 21)
	return nil
}

// start implements delRangeManager interface.
func (dr *mockDelRange) start() { trace_util_0.Count(_mock_00000, 22) }

// clear implements delRangeManager interface.
func (dr *mockDelRange) clear() { trace_util_0.Count(_mock_00000, 23) }

// MockTableInfo mocks a table info by create table stmt ast and a specified table id.
func MockTableInfo(ctx sessionctx.Context, stmt *ast.CreateTableStmt, tableID int64) (*model.TableInfo, error) {
	trace_util_0.Count(_mock_00000, 24)
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, stmt.Cols, stmt.Constraints, "", "")
	if err != nil {
		trace_util_0.Count(_mock_00000, 29)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_mock_00000, 25)
	tbl, err := buildTableInfo(ctx, nil, stmt.Table.Name, cols, newConstraints)
	if err != nil {
		trace_util_0.Count(_mock_00000, 30)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_mock_00000, 26)
	tbl.ID = tableID

	// The specified charset will be handled in handleTableOptions
	if err = handleTableOptions(stmt.Options, tbl); err != nil {
		trace_util_0.Count(_mock_00000, 31)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_mock_00000, 27)
	if err = resolveDefaultTableCharsetAndCollation(tbl, ""); err != nil {
		trace_util_0.Count(_mock_00000, 32)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_mock_00000, 28)
	return tbl, nil
}

var _mock_00000 = "ddl/mock.go"
