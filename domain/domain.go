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

package domain

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/ngaut/pools"
	"github.com/ngaut/sync2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/expensivequery"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store                kv.Storage
	infoHandle           *infoschema.Handle
	privHandle           *privileges.Handle
	bindHandle           *bindinfo.BindHandle
	statsHandle          unsafe.Pointer
	statsLease           time.Duration
	statsUpdating        sync2.AtomicInt32
	ddl                  ddl.DDL
	info                 *InfoSyncer
	m                    sync.Mutex
	SchemaValidator      SchemaValidator
	sysSessionPool       *sessionPool
	exit                 chan struct{}
	etcdClient           *clientv3.Client
	wg                   sync.WaitGroup
	gvc                  GlobalVariableCache
	slowQuery            *topNSlowQueries
	expensiveQueryHandle *expensivequery.Handle
}

// loadInfoSchema loads infoschema at startTS into handle, usedSchemaVersion is the currently used
// infoschema version, if it is the same as the schema version at startTS, we don't need to reload again.
// It returns the latest schema version, the changed table IDs, whether it's a full load and an error.
func (do *Domain) loadInfoSchema(handle *infoschema.Handle, usedSchemaVersion int64, startTS uint64) (int64, []int64, bool, error) {
	trace_util_0.Count(_domain_00000, 0)
	var fullLoad bool
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		trace_util_0.Count(_domain_00000, 9)
		return 0, nil, fullLoad, err
	}
	trace_util_0.Count(_domain_00000, 1)
	m := meta.NewSnapshotMeta(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		trace_util_0.Count(_domain_00000, 10)
		return 0, nil, fullLoad, err
	}
	trace_util_0.Count(_domain_00000, 2)
	if usedSchemaVersion != 0 && usedSchemaVersion == neededSchemaVersion {
		trace_util_0.Count(_domain_00000, 11)
		return neededSchemaVersion, nil, fullLoad, nil
	}

	// Update self schema version to etcd.
	trace_util_0.Count(_domain_00000, 3)
	defer func() {
		trace_util_0.Count(_domain_00000, 12)
		// There are two possibilities for not updating the self schema version to etcd.
		// 1. Failed to loading schema information.
		// 2. When users use history read feature, the neededSchemaVersion isn't the latest schema version.
		if err != nil || neededSchemaVersion < do.InfoSchema().SchemaMetaVersion() {
			trace_util_0.Count(_domain_00000, 14)
			logutil.Logger(context.Background()).Info("do not update self schema version to etcd",
				zap.Int64("usedSchemaVersion", usedSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
			return
		}

		trace_util_0.Count(_domain_00000, 13)
		err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), neededSchemaVersion)
		if err != nil {
			trace_util_0.Count(_domain_00000, 15)
			logutil.Logger(context.Background()).Info("update self version failed",
				zap.Int64("usedSchemaVersion", usedSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
		}
	}()

	trace_util_0.Count(_domain_00000, 4)
	startTime := time.Now()
	ok, tblIDs, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, neededSchemaVersion)
	if err != nil {
		trace_util_0.Count(_domain_00000, 16)
		// We can fall back to full load, don't need to return the error.
		logutil.Logger(context.Background()).Error("failed to load schema diff", zap.Error(err))
	}
	trace_util_0.Count(_domain_00000, 5)
	if ok {
		trace_util_0.Count(_domain_00000, 17)
		logutil.Logger(context.Background()).Info("diff load InfoSchema success",
			zap.Int64("usedSchemaVersion", usedSchemaVersion),
			zap.Int64("neededSchemaVersion", neededSchemaVersion),
			zap.Duration("start time", time.Since(startTime)),
			zap.Int64s("tblIDs", tblIDs))
		return neededSchemaVersion, tblIDs, fullLoad, nil
	}

	trace_util_0.Count(_domain_00000, 6)
	fullLoad = true
	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		trace_util_0.Count(_domain_00000, 18)
		return 0, nil, fullLoad, err
	}

	trace_util_0.Count(_domain_00000, 7)
	newISBuilder, err := infoschema.NewBuilder(handle).InitWithDBInfos(schemas, neededSchemaVersion)
	if err != nil {
		trace_util_0.Count(_domain_00000, 19)
		return 0, nil, fullLoad, err
	}
	trace_util_0.Count(_domain_00000, 8)
	logutil.Logger(context.Background()).Info("full load InfoSchema success",
		zap.Int64("usedSchemaVersion", usedSchemaVersion),
		zap.Int64("neededSchemaVersion", neededSchemaVersion),
		zap.Duration("start time", time.Since(startTime)))
	newISBuilder.Build()
	return neededSchemaVersion, nil, fullLoad, nil
}

func (do *Domain) fetchAllSchemasWithTables(m *meta.Meta) ([]*model.DBInfo, error) {
	trace_util_0.Count(_domain_00000, 20)
	allSchemas, err := m.ListDatabases()
	if err != nil {
		trace_util_0.Count(_domain_00000, 24)
		return nil, err
	}
	trace_util_0.Count(_domain_00000, 21)
	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	doneCh := make(chan error, len(splittedSchemas))
	for _, schemas := range splittedSchemas {
		trace_util_0.Count(_domain_00000, 25)
		go do.fetchSchemasWithTables(schemas, m, doneCh)
	}
	trace_util_0.Count(_domain_00000, 22)
	for range splittedSchemas {
		trace_util_0.Count(_domain_00000, 26)
		err = <-doneCh
		if err != nil {
			trace_util_0.Count(_domain_00000, 27)
			return nil, err
		}
	}
	trace_util_0.Count(_domain_00000, 23)
	return allSchemas, nil
}

const fetchSchemaConcurrency = 8

func (do *Domain) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
	trace_util_0.Count(_domain_00000, 28)
	groupSize := (len(schemas) + fetchSchemaConcurrency - 1) / fetchSchemaConcurrency
	splitted := make([][]*model.DBInfo, 0, fetchSchemaConcurrency)
	schemaCnt := len(schemas)
	for i := 0; i < schemaCnt; i += groupSize {
		trace_util_0.Count(_domain_00000, 30)
		end := i + groupSize
		if end > schemaCnt {
			trace_util_0.Count(_domain_00000, 32)
			end = schemaCnt
		}
		trace_util_0.Count(_domain_00000, 31)
		splitted = append(splitted, schemas[i:end])
	}
	trace_util_0.Count(_domain_00000, 29)
	return splitted
}

func (do *Domain) fetchSchemasWithTables(schemas []*model.DBInfo, m *meta.Meta, done chan error) {
	trace_util_0.Count(_domain_00000, 33)
	for _, di := range schemas {
		trace_util_0.Count(_domain_00000, 35)
		if di.State != model.StatePublic {
			trace_util_0.Count(_domain_00000, 39)
			// schema is not public, can't be used outside.
			continue
		}
		trace_util_0.Count(_domain_00000, 36)
		tables, err := m.ListTables(di.ID)
		if err != nil {
			trace_util_0.Count(_domain_00000, 40)
			done <- err
			return
		}
		// If TreatOldVersionUTF8AsUTF8MB4 was enable, need to convert the old version schema UTF8 charset to UTF8MB4.
		trace_util_0.Count(_domain_00000, 37)
		if config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
			trace_util_0.Count(_domain_00000, 41)
			for _, tbInfo := range tables {
				trace_util_0.Count(_domain_00000, 42)
				infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo)
			}
		}
		trace_util_0.Count(_domain_00000, 38)
		di.Tables = make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			trace_util_0.Count(_domain_00000, 43)
			if tbl.State != model.StatePublic {
				trace_util_0.Count(_domain_00000, 45)
				// schema is not public, can't be used outside.
				continue
			}
			trace_util_0.Count(_domain_00000, 44)
			infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
			di.Tables = append(di.Tables, tbl)
		}
	}
	trace_util_0.Count(_domain_00000, 34)
	done <- nil
}

const (
	initialVersion         = 0
	maxNumberOfDiffsToLoad = 100
)

func isTooOldSchema(usedVersion, newVersion int64) bool {
	trace_util_0.Count(_domain_00000, 46)
	if usedVersion == initialVersion || newVersion-usedVersion > maxNumberOfDiffsToLoad {
		trace_util_0.Count(_domain_00000, 48)
		return true
	}
	trace_util_0.Count(_domain_00000, 47)
	return false
}

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table IDs.
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (bool, []int64, error) {
	trace_util_0.Count(_domain_00000, 49)
	// If there isn't any used version, or used version is too old, we do full load.
	// And when users use history read feature, we will set usedVersion to initialVersion, then full load is needed.
	if isTooOldSchema(usedVersion, newVersion) {
		trace_util_0.Count(_domain_00000, 53)
		return false, nil, nil
	}
	trace_util_0.Count(_domain_00000, 50)
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		trace_util_0.Count(_domain_00000, 54)
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			trace_util_0.Count(_domain_00000, 57)
			return false, nil, err
		}
		trace_util_0.Count(_domain_00000, 55)
		if diff == nil {
			trace_util_0.Count(_domain_00000, 58)
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return false, nil, nil
		}
		trace_util_0.Count(_domain_00000, 56)
		diffs = append(diffs, diff)
	}
	trace_util_0.Count(_domain_00000, 51)
	builder := infoschema.NewBuilder(do.infoHandle).InitWithOldInfoSchema()
	tblIDs := make([]int64, 0, len(diffs))
	for _, diff := range diffs {
		trace_util_0.Count(_domain_00000, 59)
		ids, err := builder.ApplyDiff(m, diff)
		if err != nil {
			trace_util_0.Count(_domain_00000, 61)
			return false, nil, err
		}
		trace_util_0.Count(_domain_00000, 60)
		tblIDs = append(tblIDs, ids...)
	}
	trace_util_0.Count(_domain_00000, 52)
	builder.Build()
	return true, tblIDs, nil
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	trace_util_0.Count(_domain_00000, 62)
	return do.infoHandle.Get()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	trace_util_0.Count(_domain_00000, 63)
	snapHandle := do.infoHandle.EmptyClone()
	// For the snapHandle, it's an empty Handle, so its usedSchemaVersion is initialVersion.
	_, _, _, err := do.loadInfoSchema(snapHandle, initialVersion, snapshotTS)
	if err != nil {
		trace_util_0.Count(_domain_00000, 65)
		return nil, err
	}
	trace_util_0.Count(_domain_00000, 64)
	return snapHandle.Get(), nil
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) (*meta.Meta, error) {
	trace_util_0.Count(_domain_00000, 66)
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		trace_util_0.Count(_domain_00000, 68)
		return nil, err
	}
	trace_util_0.Count(_domain_00000, 67)
	return meta.NewSnapshotMeta(snapshot), nil
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	trace_util_0.Count(_domain_00000, 69)
	return do.ddl
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *InfoSyncer {
	trace_util_0.Count(_domain_00000, 70)
	return do.info
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	trace_util_0.Count(_domain_00000, 71)
	return do.store
}

// GetScope gets the status variables scope.
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	trace_util_0.Count(_domain_00000, 72)
	// Now domain status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	trace_util_0.Count(_domain_00000, 73)
	failpoint.Inject("ErrorMockReloadFailed", func(val failpoint.Value) {
		trace_util_0.Count(_domain_00000, 80)
		if val.(bool) {
			trace_util_0.Count(_domain_00000, 81)
			failpoint.Return(errors.New("mock reload failed"))
		}
	})

	// Lock here for only once at the same time.
	trace_util_0.Count(_domain_00000, 74)
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()

	var err error
	var neededSchemaVersion int64

	ver, err := do.store.CurrentVersion()
	if err != nil {
		trace_util_0.Count(_domain_00000, 82)
		return err
	}

	trace_util_0.Count(_domain_00000, 75)
	schemaVersion := int64(0)
	oldInfoSchema := do.infoHandle.Get()
	if oldInfoSchema != nil {
		trace_util_0.Count(_domain_00000, 83)
		schemaVersion = oldInfoSchema.SchemaMetaVersion()
	}

	trace_util_0.Count(_domain_00000, 76)
	var (
		fullLoad        bool
		changedTableIDs []int64
	)
	neededSchemaVersion, changedTableIDs, fullLoad, err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
	metrics.LoadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		trace_util_0.Count(_domain_00000, 84)
		metrics.LoadSchemaCounter.WithLabelValues("failed").Inc()
		return err
	}
	trace_util_0.Count(_domain_00000, 77)
	metrics.LoadSchemaCounter.WithLabelValues("succ").Inc()

	if fullLoad {
		trace_util_0.Count(_domain_00000, 85)
		logutil.Logger(context.Background()).Info("full load and reset schema validator")
		do.SchemaValidator.Reset()
	}
	trace_util_0.Count(_domain_00000, 78)
	do.SchemaValidator.Update(ver.Ver, schemaVersion, neededSchemaVersion, changedTableIDs)

	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schema time elapses more than this interval,
	// some query maybe responded by ErrInfoSchemaExpired error.
	if sub > (lease/2) && lease > 0 {
		trace_util_0.Count(_domain_00000, 86)
		logutil.Logger(context.Background()).Warn("loading schema takes a long time", zap.Duration("take time", sub))
	}

	trace_util_0.Count(_domain_00000, 79)
	return nil
}

// LogSlowQuery keeps topN recent slow queries in domain.
func (do *Domain) LogSlowQuery(query *SlowQueryInfo) {
	trace_util_0.Count(_domain_00000, 87)
	do.slowQuery.mu.RLock()
	defer do.slowQuery.mu.RUnlock()
	if do.slowQuery.mu.closed {
		trace_util_0.Count(_domain_00000, 89)
		return
	}

	trace_util_0.Count(_domain_00000, 88)
	select {
	case do.slowQuery.ch <- query:
		trace_util_0.Count(_domain_00000, 90)
	default:
		trace_util_0.Count(_domain_00000, 91)
	}
}

// ShowSlowQuery returns the slow queries.
func (do *Domain) ShowSlowQuery(showSlow *ast.ShowSlow) []*SlowQueryInfo {
	trace_util_0.Count(_domain_00000, 92)
	msg := &showSlowMessage{
		request: showSlow,
	}
	msg.Add(1)
	do.slowQuery.msgCh <- msg
	msg.Wait()
	return msg.result
}

func (do *Domain) topNSlowQueryLoop() {
	trace_util_0.Count(_domain_00000, 93)
	defer recoverInDomain("topNSlowQueryLoop", false)
	defer do.wg.Done()
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	for {
		trace_util_0.Count(_domain_00000, 94)
		select {
		case now := <-ticker.C:
			trace_util_0.Count(_domain_00000, 95)
			do.slowQuery.RemoveExpired(now)
		case info, ok := <-do.slowQuery.ch:
			trace_util_0.Count(_domain_00000, 96)
			if !ok {
				trace_util_0.Count(_domain_00000, 100)
				return
			}
			trace_util_0.Count(_domain_00000, 97)
			do.slowQuery.Append(info)
		case msg := <-do.slowQuery.msgCh:
			trace_util_0.Count(_domain_00000, 98)
			req := msg.request
			switch req.Tp {
			case ast.ShowSlowTop:
				trace_util_0.Count(_domain_00000, 101)
				msg.result = do.slowQuery.QueryTop(int(req.Count), req.Kind)
			case ast.ShowSlowRecent:
				trace_util_0.Count(_domain_00000, 102)
				msg.result = do.slowQuery.QueryRecent(int(req.Count))
			default:
				trace_util_0.Count(_domain_00000, 103)
				msg.result = do.slowQuery.QueryAll()
			}
			trace_util_0.Count(_domain_00000, 99)
			msg.Done()
		}
	}
}

func (do *Domain) infoSyncerKeeper() {
	trace_util_0.Count(_domain_00000, 104)
	defer do.wg.Done()
	defer recoverInDomain("infoSyncerKeeper", false)
	for {
		trace_util_0.Count(_domain_00000, 105)
		select {
		case <-do.info.Done():
			trace_util_0.Count(_domain_00000, 106)
			logutil.Logger(context.Background()).Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				trace_util_0.Count(_domain_00000, 109)
				logutil.Logger(context.Background()).Error("server restart failed", zap.Error(err))
			}
			trace_util_0.Count(_domain_00000, 107)
			logutil.Logger(context.Background()).Info("server info syncer restarted")
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 108)
			return
		}
	}
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	trace_util_0.Count(_domain_00000, 110)
	defer do.wg.Done()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	defer recoverInDomain("loadSchemaInLoop", true)
	syncer := do.ddl.SchemaSyncer()

	for {
		trace_util_0.Count(_domain_00000, 111)
		select {
		case <-ticker.C:
			trace_util_0.Count(_domain_00000, 112)
			err := do.Reload()
			if err != nil {
				trace_util_0.Count(_domain_00000, 119)
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
		case _, ok := <-syncer.GlobalVersionCh():
			trace_util_0.Count(_domain_00000, 113)
			err := do.Reload()
			if err != nil {
				trace_util_0.Count(_domain_00000, 120)
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
			trace_util_0.Count(_domain_00000, 114)
			if !ok {
				trace_util_0.Count(_domain_00000, 121)
				logutil.Logger(context.Background()).Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			trace_util_0.Count(_domain_00000, 115)
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			logutil.Logger(context.Background()).Info("reload schema in loop, schema syncer need restart")
			// The etcd is responsible for schema synchronization, we should ensure there is at most two different schema version
			// in the TiDB cluster, to make the data/schema be consistent. If we lost connection/session to etcd, the cluster
			// will treats this TiDB as a down instance, and etcd will remove the key of `/tidb/ddl/all_schema_versions/tidb-id`.
			// Say the schema version now is 1, the owner is changing the schema version to 2, it will not wait for this down TiDB syncing the schema,
			// then continue to change the TiDB schema to version 3. Unfortunately, this down TiDB schema version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schema validator to prohibit the DML executing.
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer()
			if err != nil {
				trace_util_0.Count(_domain_00000, 122)
				logutil.Logger(context.Background()).Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			trace_util_0.Count(_domain_00000, 116)
			exitLoop := do.mustReload()
			if exitLoop {
				trace_util_0.Count(_domain_00000, 123)
				// domain is closed.
				logutil.Logger(context.Background()).Error("domain is closed, exit loadSchemaInLoop")
				return
			}
			trace_util_0.Count(_domain_00000, 117)
			do.SchemaValidator.Restart()
			logutil.Logger(context.Background()).Info("schema syncer restarted")
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 118)
			return
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stoped.
func (do *Domain) mustRestartSyncer() error {
	trace_util_0.Count(_domain_00000, 124)
	ctx := context.Background()
	syncer := do.ddl.SchemaSyncer()

	for {
		trace_util_0.Count(_domain_00000, 125)
		err := syncer.Restart(ctx)
		if err == nil {
			trace_util_0.Count(_domain_00000, 128)
			return nil
		}
		// If the domain has stopped, we return an error immediately.
		trace_util_0.Count(_domain_00000, 126)
		select {
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 129)
			return err
		default:
			trace_util_0.Count(_domain_00000, 130)
		}
		trace_util_0.Count(_domain_00000, 127)
		time.Sleep(time.Second)
		logutil.Logger(context.Background()).Info("restart the schema syncer failed", zap.Error(err))
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (do *Domain) mustReload() (exitLoop bool) {
	trace_util_0.Count(_domain_00000, 131)
	for {
		trace_util_0.Count(_domain_00000, 132)
		err := do.Reload()
		if err == nil {
			trace_util_0.Count(_domain_00000, 135)
			logutil.Logger(context.Background()).Info("mustReload succeed")
			return false
		}

		trace_util_0.Count(_domain_00000, 133)
		logutil.Logger(context.Background()).Info("reload the schema failed", zap.Error(err))
		// If the domain is closed, we returns immediately.
		select {
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 136)
			logutil.Logger(context.Background()).Info("domain is closed")
			return true
		default:
			trace_util_0.Count(_domain_00000, 137)
		}

		trace_util_0.Count(_domain_00000, 134)
		time.Sleep(200 * time.Millisecond)
	}
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	trace_util_0.Count(_domain_00000, 138)
	startTime := time.Now()
	if do.ddl != nil {
		trace_util_0.Count(_domain_00000, 142)
		terror.Log(do.ddl.Stop())
	}
	trace_util_0.Count(_domain_00000, 139)
	if do.info != nil {
		trace_util_0.Count(_domain_00000, 143)
		do.info.RemoveServerInfo()
	}
	trace_util_0.Count(_domain_00000, 140)
	close(do.exit)
	if do.etcdClient != nil {
		trace_util_0.Count(_domain_00000, 144)
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}
	trace_util_0.Count(_domain_00000, 141)
	do.sysSessionPool.Close()
	do.slowQuery.Close()
	do.wg.Wait()
	logutil.Logger(context.Background()).Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	trace_util_0.Count(_domain_00000, 145)
	if err != nil {
		trace_util_0.Count(_domain_00000, 148)
		return err
	}
	trace_util_0.Count(_domain_00000, 146)
	logutil.Logger(context.Background()).Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		trace_util_0.Count(_domain_00000, 149)
		logutil.Logger(context.Background()).Error("performing DDL change failed", zap.Error(err))
	}

	trace_util_0.Count(_domain_00000, 147)
	return nil
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, ddlLease time.Duration, statsLease time.Duration, factory pools.Factory) *Domain {
	trace_util_0.Count(_domain_00000, 150)
	capacity := 200 // capacity of the sysSessionPool size
	return &Domain{
		store:           store,
		SchemaValidator: NewSchemaValidator(ddlLease),
		exit:            make(chan struct{}),
		sysSessionPool:  newSessionPool(capacity, factory),
		statsLease:      statsLease,
		infoHandle:      infoschema.NewHandle(store),
		slowQuery:       newTopNSlowQueries(30, time.Hour*24*7, 500),
	}
}

// Init initializes a domain.
func (do *Domain) Init(ddlLease time.Duration, sysFactory func(*Domain) (pools.Resource, error)) error {
	trace_util_0.Count(_domain_00000, 151)
	perfschema.Init()
	if ebd, ok := do.store.(tikv.EtcdBackend); ok {
		trace_util_0.Count(_domain_00000, 158)
		if addrs := ebd.EtcdAddrs(); addrs != nil {
			trace_util_0.Count(_domain_00000, 159)
			cfg := config.GetGlobalConfig()
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:                time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
						Timeout:             time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
						PermitWithoutStream: true,
					}),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				trace_util_0.Count(_domain_00000, 161)
				return errors.Trace(err)
			}
			trace_util_0.Count(_domain_00000, 160)
			do.etcdClient = cli
		}
	}

	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	trace_util_0.Count(_domain_00000, 152)
	sysFac := func() (pools.Resource, error) {
		trace_util_0.Count(_domain_00000, 162)
		return sysFactory(do)
	}
	trace_util_0.Count(_domain_00000, 153)
	sysCtxPool := pools.NewResourcePool(sysFac, 2, 2, resourceIdleTimeout)
	ctx := context.Background()
	callback := &ddlCallback{do: do}
	do.ddl = ddl.NewDDL(ctx, do.etcdClient, do.store, do.infoHandle, callback, ddlLease, sysCtxPool)

	err := do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		trace_util_0.Count(_domain_00000, 163)
		return err
	}
	trace_util_0.Count(_domain_00000, 154)
	do.info = NewInfoSyncer(do.ddl.GetID(), do.etcdClient)
	err = do.info.Init(ctx)
	if err != nil {
		trace_util_0.Count(_domain_00000, 164)
		return err
	}
	trace_util_0.Count(_domain_00000, 155)
	err = do.Reload()
	if err != nil {
		trace_util_0.Count(_domain_00000, 165)
		return err
	}

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop.
	trace_util_0.Count(_domain_00000, 156)
	if ddlLease > 0 {
		trace_util_0.Count(_domain_00000, 166)
		do.wg.Add(1)
		// Local store needs to get the change information for every DDL state in each session.
		go do.loadSchemaInLoop(ddlLease)
	}
	trace_util_0.Count(_domain_00000, 157)
	do.wg.Add(1)
	go do.topNSlowQueryLoop()

	do.wg.Add(1)
	go do.infoSyncerKeeper()
	return nil
}

type sessionPool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

func newSessionPool(cap int, factory pools.Factory) *sessionPool {
	trace_util_0.Count(_domain_00000, 167)
	return &sessionPool{
		resources: make(chan pools.Resource, cap),
		factory:   factory,
	}
}

func (p *sessionPool) Get() (resource pools.Resource, err error) {
	trace_util_0.Count(_domain_00000, 168)
	var ok bool
	select {
	case resource, ok = <-p.resources:
		trace_util_0.Count(_domain_00000, 170)
		if !ok {
			trace_util_0.Count(_domain_00000, 172)
			err = errors.New("session pool closed")
		}
	default:
		trace_util_0.Count(_domain_00000, 171)
		resource, err = p.factory()
	}
	trace_util_0.Count(_domain_00000, 169)
	return
}

func (p *sessionPool) Put(resource pools.Resource) {
	trace_util_0.Count(_domain_00000, 173)
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		trace_util_0.Count(_domain_00000, 175)
		resource.Close()
		return
	}

	trace_util_0.Count(_domain_00000, 174)
	select {
	case p.resources <- resource:
		trace_util_0.Count(_domain_00000, 176)
	default:
		trace_util_0.Count(_domain_00000, 177)
		resource.Close()
	}
}
func (p *sessionPool) Close() {
	trace_util_0.Count(_domain_00000, 178)
	p.mu.Lock()
	if p.mu.closed {
		trace_util_0.Count(_domain_00000, 180)
		p.mu.Unlock()
		return
	}
	trace_util_0.Count(_domain_00000, 179)
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		trace_util_0.Count(_domain_00000, 181)
		r.Close()
	}
}

// SysSessionPool returns the system session pool.
func (do *Domain) SysSessionPool() *sessionPool {
	trace_util_0.Count(_domain_00000, 182)
	return do.sysSessionPool
}

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	trace_util_0.Count(_domain_00000, 183)
	return do.etcdClient
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(ctx sessionctx.Context) error {
	trace_util_0.Count(_domain_00000, 184)
	ctx.GetSessionVars().InRestrictedSQL = true
	do.privHandle = privileges.NewHandle()
	err := do.privHandle.Update(ctx)
	if err != nil {
		trace_util_0.Count(_domain_00000, 188)
		return err
	}

	trace_util_0.Count(_domain_00000, 185)
	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		trace_util_0.Count(_domain_00000, 189)
		watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
		duration = 10 * time.Minute
	}

	trace_util_0.Count(_domain_00000, 186)
	do.wg.Add(1)
	go func() {
		trace_util_0.Count(_domain_00000, 190)
		defer do.wg.Done()
		defer recoverInDomain("loadPrivilegeInLoop", false)
		var count int
		for {
			trace_util_0.Count(_domain_00000, 191)
			ok := true
			select {
			case <-do.exit:
				trace_util_0.Count(_domain_00000, 194)
				return
			case _, ok = <-watchCh:
				trace_util_0.Count(_domain_00000, 195)
			case <-time.After(duration):
				trace_util_0.Count(_domain_00000, 196)
			}
			trace_util_0.Count(_domain_00000, 192)
			if !ok {
				trace_util_0.Count(_domain_00000, 197)
				logutil.Logger(context.Background()).Error("load privilege loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
				count++
				if count > 10 {
					trace_util_0.Count(_domain_00000, 199)
					time.Sleep(time.Duration(count) * time.Second)
				}
				trace_util_0.Count(_domain_00000, 198)
				continue
			}

			trace_util_0.Count(_domain_00000, 193)
			count = 0
			err := do.privHandle.Update(ctx)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				trace_util_0.Count(_domain_00000, 200)
				logutil.Logger(context.Background()).Error("load privilege failed", zap.Error(err))
			} else {
				trace_util_0.Count(_domain_00000, 201)
				{
					logutil.Logger(context.Background()).Debug("reload privilege success")
				}
			}
		}
	}()
	trace_util_0.Count(_domain_00000, 187)
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	trace_util_0.Count(_domain_00000, 202)
	return do.privHandle
}

// BindHandle returns domain's bindHandle.
func (do *Domain) BindHandle() *bindinfo.BindHandle {
	trace_util_0.Count(_domain_00000, 203)
	return do.bindHandle
}

// LoadBindInfoLoop create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) LoadBindInfoLoop(ctx sessionctx.Context) error {
	trace_util_0.Count(_domain_00000, 204)
	ctx.GetSessionVars().InRestrictedSQL = true
	do.bindHandle = bindinfo.NewBindHandle(ctx)
	err := do.bindHandle.Update(true)
	if err != nil || bindinfo.Lease == 0 {
		trace_util_0.Count(_domain_00000, 206)
		return err
	}

	trace_util_0.Count(_domain_00000, 205)
	do.loadBindInfoLoop()
	do.handleInvalidBindTaskLoop()
	return nil
}

func (do *Domain) loadBindInfoLoop() {
	trace_util_0.Count(_domain_00000, 207)
	do.wg.Add(1)
	go func() {
		trace_util_0.Count(_domain_00000, 208)
		defer do.wg.Done()
		defer recoverInDomain("loadBindInfoLoop", false)
		for {
			trace_util_0.Count(_domain_00000, 209)
			select {
			case <-do.exit:
				trace_util_0.Count(_domain_00000, 211)
				return
			case <-time.After(bindinfo.Lease):
				trace_util_0.Count(_domain_00000, 212)
			}
			trace_util_0.Count(_domain_00000, 210)
			err := do.bindHandle.Update(false)
			if err != nil {
				trace_util_0.Count(_domain_00000, 213)
				logutil.Logger(context.Background()).Error("update bindinfo failed", zap.Error(err))
			}
		}
	}()
}

func (do *Domain) handleInvalidBindTaskLoop() {
	trace_util_0.Count(_domain_00000, 214)
	do.wg.Add(1)
	go func() {
		trace_util_0.Count(_domain_00000, 215)
		defer do.wg.Done()
		defer recoverInDomain("loadBindInfoLoop-dropInvalidBindInfo", false)
		for {
			trace_util_0.Count(_domain_00000, 216)
			select {
			case <-do.exit:
				trace_util_0.Count(_domain_00000, 218)
				return
			case <-time.After(bindinfo.Lease):
				trace_util_0.Count(_domain_00000, 219)
			}
			trace_util_0.Count(_domain_00000, 217)
			do.bindHandle.DropInvalidBindRecord()
		}
	}()
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	trace_util_0.Count(_domain_00000, 220)
	return (*handle.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx sessionctx.Context) {
	trace_util_0.Count(_domain_00000, 221)
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(handle.NewHandle(ctx, do.statsLease)))
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	trace_util_0.Count(_domain_00000, 222)
	return do.statsUpdating.Get() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	trace_util_0.Count(_domain_00000, 223)
	if val {
		trace_util_0.Count(_domain_00000, 224)
		do.statsUpdating.Set(1)
	} else {
		trace_util_0.Count(_domain_00000, 225)
		{
			do.statsUpdating.Set(0)
		}
	}
}

// RunAutoAnalyze indicates if this TiDB server starts auto analyze worker and can run auto analyze job.
var RunAutoAnalyze = true

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx sessionctx.Context) error {
	trace_util_0.Count(_domain_00000, 226)
	ctx.GetSessionVars().InRestrictedSQL = true
	statsHandle := handle.NewHandle(ctx, do.statsLease)
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.ddl.RegisterEventCh(statsHandle.DDLEventCh())
	do.wg.Add(1)
	go do.loadStatsWorker()
	if do.statsLease <= 0 {
		trace_util_0.Count(_domain_00000, 229)
		return nil
	}
	trace_util_0.Count(_domain_00000, 227)
	owner := do.newStatsOwner()
	do.wg.Add(1)
	do.SetStatsUpdating(true)
	go do.updateStatsWorker(ctx, owner)
	if RunAutoAnalyze {
		trace_util_0.Count(_domain_00000, 230)
		do.wg.Add(1)
		go do.autoAnalyzeWorker(owner)
	}
	trace_util_0.Count(_domain_00000, 228)
	return nil
}

func (do *Domain) newStatsOwner() owner.Manager {
	trace_util_0.Count(_domain_00000, 231)
	id := do.ddl.OwnerManager().ID()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		trace_util_0.Count(_domain_00000, 234)
		statsOwner = owner.NewMockManager(id, cancelFunc)
	} else {
		trace_util_0.Count(_domain_00000, 235)
		{
			statsOwner = owner.NewOwnerManager(do.etcdClient, handle.StatsPrompt, id, handle.StatsOwnerKey, cancelFunc)
		}
	}
	// TODO: Need to do something when err is not nil.
	trace_util_0.Count(_domain_00000, 232)
	err := statsOwner.CampaignOwner(cancelCtx)
	if err != nil {
		trace_util_0.Count(_domain_00000, 236)
		logutil.Logger(context.Background()).Warn("campaign owner failed", zap.Error(err))
	}
	trace_util_0.Count(_domain_00000, 233)
	return statsOwner
}

func (do *Domain) loadStatsWorker() {
	trace_util_0.Count(_domain_00000, 237)
	defer recoverInDomain("loadStatsWorker", false)
	defer do.wg.Done()
	lease := do.statsLease
	if lease == 0 {
		trace_util_0.Count(_domain_00000, 240)
		lease = 3 * time.Second
	}
	trace_util_0.Count(_domain_00000, 238)
	loadTicker := time.NewTicker(lease)
	defer loadTicker.Stop()
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.InfoSchema())
	if err != nil {
		trace_util_0.Count(_domain_00000, 241)
		logutil.Logger(context.Background()).Debug("init stats info failed", zap.Error(err))
	} else {
		trace_util_0.Count(_domain_00000, 242)
		{
			logutil.Logger(context.Background()).Info("init stats info time", zap.Duration("take time", time.Since(t)))
		}
	}
	trace_util_0.Count(_domain_00000, 239)
	for {
		trace_util_0.Count(_domain_00000, 243)
		select {
		case <-loadTicker.C:
			trace_util_0.Count(_domain_00000, 244)
			err = statsHandle.Update(do.InfoSchema())
			if err != nil {
				trace_util_0.Count(_domain_00000, 247)
				logutil.Logger(context.Background()).Debug("update stats info failed", zap.Error(err))
			}
			trace_util_0.Count(_domain_00000, 245)
			err = statsHandle.LoadNeededHistograms()
			if err != nil {
				trace_util_0.Count(_domain_00000, 248)
				logutil.Logger(context.Background()).Debug("load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 246)
			return
		}
	}
}

func (do *Domain) updateStatsWorker(ctx sessionctx.Context, owner owner.Manager) {
	trace_util_0.Count(_domain_00000, 249)
	defer recoverInDomain("updateStatsWorker", false)
	lease := do.statsLease
	deltaUpdateTicker := time.NewTicker(20 * lease)
	defer deltaUpdateTicker.Stop()
	gcStatsTicker := time.NewTicker(100 * lease)
	defer gcStatsTicker.Stop()
	dumpFeedbackTicker := time.NewTicker(200 * lease)
	defer dumpFeedbackTicker.Stop()
	loadFeedbackTicker := time.NewTicker(5 * lease)
	defer loadFeedbackTicker.Stop()
	statsHandle := do.StatsHandle()
	defer func() {
		trace_util_0.Count(_domain_00000, 251)
		do.SetStatsUpdating(false)
		do.wg.Done()
	}()
	trace_util_0.Count(_domain_00000, 250)
	for {
		trace_util_0.Count(_domain_00000, 252)
		select {
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 253)
			statsHandle.FlushStats()
			return
			// This channel is sent only by ddl owner.
		case t := <-statsHandle.DDLEventCh():
			trace_util_0.Count(_domain_00000, 254)
			err := statsHandle.HandleDDLEvent(t)
			if err != nil {
				trace_util_0.Count(_domain_00000, 262)
				logutil.Logger(context.Background()).Debug("handle ddl event failed", zap.Error(err))
			}
		case <-deltaUpdateTicker.C:
			trace_util_0.Count(_domain_00000, 255)
			err := statsHandle.DumpStatsDeltaToKV(handle.DumpDelta)
			if err != nil {
				trace_util_0.Count(_domain_00000, 263)
				logutil.Logger(context.Background()).Debug("dump stats delta failed", zap.Error(err))
			}
			trace_util_0.Count(_domain_00000, 256)
			statsHandle.UpdateErrorRate(do.InfoSchema())
		case <-loadFeedbackTicker.C:
			trace_util_0.Count(_domain_00000, 257)
			statsHandle.UpdateStatsByLocalFeedback(do.InfoSchema())
			if !owner.IsOwner() {
				trace_util_0.Count(_domain_00000, 264)
				continue
			}
			trace_util_0.Count(_domain_00000, 258)
			err := statsHandle.HandleUpdateStats(do.InfoSchema())
			if err != nil {
				trace_util_0.Count(_domain_00000, 265)
				logutil.Logger(context.Background()).Debug("update stats using feedback failed", zap.Error(err))
			}
		case <-dumpFeedbackTicker.C:
			trace_util_0.Count(_domain_00000, 259)
			err := statsHandle.DumpStatsFeedbackToKV()
			if err != nil {
				trace_util_0.Count(_domain_00000, 266)
				logutil.Logger(context.Background()).Debug("dump stats feedback failed", zap.Error(err))
			}
		case <-gcStatsTicker.C:
			trace_util_0.Count(_domain_00000, 260)
			if !owner.IsOwner() {
				trace_util_0.Count(_domain_00000, 267)
				continue
			}
			trace_util_0.Count(_domain_00000, 261)
			err := statsHandle.GCStats(do.InfoSchema(), do.DDL().GetLease())
			if err != nil {
				trace_util_0.Count(_domain_00000, 268)
				logutil.Logger(context.Background()).Debug("GC stats failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker(owner owner.Manager) {
	trace_util_0.Count(_domain_00000, 269)
	defer recoverInDomain("autoAnalyzeWorker", false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		trace_util_0.Count(_domain_00000, 271)
		analyzeTicker.Stop()
		do.wg.Done()
	}()
	trace_util_0.Count(_domain_00000, 270)
	for {
		trace_util_0.Count(_domain_00000, 272)
		select {
		case <-analyzeTicker.C:
			trace_util_0.Count(_domain_00000, 273)
			if owner.IsOwner() {
				trace_util_0.Count(_domain_00000, 275)
				statsHandle.HandleAutoAnalyze(do.InfoSchema())
			}
		case <-do.exit:
			trace_util_0.Count(_domain_00000, 274)
			return
		}
	}
}

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Domain) ExpensiveQueryHandle() *expensivequery.Handle {
	trace_util_0.Count(_domain_00000, 276)
	return do.expensiveQueryHandle
}

// InitExpensiveQueryHandle init the expensive query handler.
func (do *Domain) InitExpensiveQueryHandle() {
	trace_util_0.Count(_domain_00000, 277)
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
}

const privilegeKey = "/tidb/privilege"

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(ctx sessionctx.Context) {
	trace_util_0.Count(_domain_00000, 278)
	if do.etcdClient != nil {
		trace_util_0.Count(_domain_00000, 280)
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), privilegeKey, "")
		if err != nil {
			trace_util_0.Count(_domain_00000, 281)
			logutil.Logger(context.Background()).Warn("notify update privilege failed", zap.Error(err))
		}
	}
	// update locally
	trace_util_0.Count(_domain_00000, 279)
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, `FLUSH PRIVILEGES`)
	if err != nil {
		trace_util_0.Count(_domain_00000, 282)
		logutil.Logger(context.Background()).Error("unable to update privileges", zap.Error(err))
	}
}

func recoverInDomain(funcName string, quit bool) {
	trace_util_0.Count(_domain_00000, 283)
	r := recover()
	if r == nil {
		trace_util_0.Count(_domain_00000, 285)
		return
	}
	trace_util_0.Count(_domain_00000, 284)
	buf := util.GetStack()
	logutil.Logger(context.Background()).Error("recover in domain failed", zap.String("funcName", funcName),
		zap.Any("error", r), zap.String("buffer", string(buf)))
	metrics.PanicCounter.WithLabelValues(metrics.LabelDomain).Inc()
	if quit {
		trace_util_0.Count(_domain_00000, 286)
		// Wait for metrics to be pushed.
		time.Sleep(time.Second * 15)
		os.Exit(1)
	}
}

// Domain error codes.
const (
	codeInfoSchemaExpired terror.ErrCode = 1
	codeInfoSchemaChanged terror.ErrCode = 2
)

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = terror.ClassDomain.New(codeInfoSchemaExpired, "Information schema is out of date.")
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = terror.ClassDomain.New(codeInfoSchemaChanged,
		"Information schema is changed. "+kv.TxnRetryableMark)
)

func init() {
	trace_util_0.Count(_domain_00000, 287)
	// Map error codes to mysql error codes.
	terror.ErrClassToMySQLCodes[terror.ClassDomain] = make(map[terror.ErrCode]uint16)
}

var _domain_00000 = "domain/domain.go"
