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

package ddl

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DDLAllSchemaVersions is the path on etcd that is used to store all servers current schema versions.
	// It's exported for testing.
	DDLAllSchemaVersions = "/tidb/ddl/all_schema_versions"
	// DDLGlobalSchemaVersion is the path on etcd that is used to store the latest schema versions.
	// It's exported for testing.
	DDLGlobalSchemaVersion = "/tidb/ddl/global_schema_version"
	// InitialVersion is the initial schema version for every server.
	// It's exported for testing.
	InitialVersion       = "0"
	putKeyNoRetry        = 1
	keyOpDefaultRetryCnt = 3
	putKeyRetryUnlimited = math.MaxInt64
	keyOpDefaultTimeout  = 2 * time.Second
	keyOpRetryInterval   = 30 * time.Millisecond
	checkVersInterval    = 20 * time.Millisecond
)

var (
	// CheckVersFirstWaitTime is a waitting time before the owner checks all the servers of the schema version,
	// and it's an exported variable for testing.
	CheckVersFirstWaitTime = 50 * time.Millisecond
	// SyncerSessionTTL is the etcd session's TTL in seconds.
	// and it's an exported variable for testing.
	SyncerSessionTTL = 90
	// WaitTimeWhenErrorOccured is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccured = 1 * time.Second
)

// SchemaSyncer is used to synchronize schema version between the DDL worker leader and followers through etcd.
type SchemaSyncer interface {
	// Init sets the global schema version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schema version to etcd.
	Init(ctx context.Context) error
	// UpdateSelfVersion updates the current version to the self path on etcd.
	UpdateSelfVersion(ctx context.Context, version int64) error
	// RemoveSelfVersionPath remove the self path from etcd.
	RemoveSelfVersionPath() error
	// OwnerUpdateGlobalVersion updates the latest version to the global path on etcd until updating is successful or the ctx is done.
	OwnerUpdateGlobalVersion(ctx context.Context, version int64) error
	// GlobalVersionCh gets the chan for watching global version.
	GlobalVersionCh() clientv3.WatchChan
	// WatchGlobalSchemaVer watches the global schema version.
	WatchGlobalSchemaVer(ctx context.Context)
	// MustGetGlobalVersion gets the global version. The only reason it fails is that ctx is done.
	MustGetGlobalVersion(ctx context.Context) (int64, error)
	// Done returns a channel that closes when the syncer is no longer being refreshed.
	Done() <-chan struct{}
	// Restart restarts the syncer when it's on longer being refreshed.
	Restart(ctx context.Context) error
	// OwnerCheckAllVersions checks whether all followers' schema version are equal to
	// the latest schema version. If the result is false, wait for a while and check again util the processing time reach 2 * lease.
	// It returns until all servers' versions are equal to the latest version or the ctx is done.
	OwnerCheckAllVersions(ctx context.Context, latestVer int64) error
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	session           unsafe.Pointer
	mu                struct {
		sync.RWMutex
		globalVerCh clientv3.WatchChan
	}
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(etcdCli *clientv3.Client, id string) SchemaSyncer {
	trace_util_0.Count(_syncer_00000, 0)
	return &schemaVersionSyncer{
		etcdCli:           etcdCli,
		selfSchemaVerPath: fmt.Sprintf("%s/%s", DDLAllSchemaVersions, id),
	}
}

// PutKVToEtcd puts key value to etcd.
// etcdCli is client of etcd.
// retryCnt is retry time when an error occurs.
// opts is configures of etcd Operations.
func PutKVToEtcd(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	trace_util_0.Count(_syncer_00000, 1)
	var err error
	for i := 0; i < retryCnt; i++ {
		trace_util_0.Count(_syncer_00000, 3)
		if isContextDone(ctx) {
			trace_util_0.Count(_syncer_00000, 6)
			return errors.Trace(ctx.Err())
		}

		trace_util_0.Count(_syncer_00000, 4)
		childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
		_, err = etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			trace_util_0.Count(_syncer_00000, 7)
			return nil
		}
		trace_util_0.Count(_syncer_00000, 5)
		logutil.Logger(ddlLogCtx).Warn("[ddl] etcd-cli put kv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
		time.Sleep(keyOpRetryInterval)
	}
	trace_util_0.Count(_syncer_00000, 2)
	return errors.Trace(err)
}

// Init implements SchemaSyncer.Init interface.
func (s *schemaVersionSyncer) Init(ctx context.Context) error {
	trace_util_0.Count(_syncer_00000, 8)
	startTime := time.Now()
	var err error
	defer func() {
		trace_util_0.Count(_syncer_00000, 12)
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerInit, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	trace_util_0.Count(_syncer_00000, 9)
	_, err = s.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(DDLGlobalSchemaVersion), "=", 0)).
		Then(clientv3.OpPut(DDLGlobalSchemaVersion, InitialVersion)).
		Commit()
	if err != nil {
		trace_util_0.Count(_syncer_00000, 13)
		return errors.Trace(err)
	}
	trace_util_0.Count(_syncer_00000, 10)
	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)
	session, err := owner.NewSession(ctx, logPrefix, s.etcdCli, owner.NewSessionDefaultRetryCnt, SyncerSessionTTL)
	if err != nil {
		trace_util_0.Count(_syncer_00000, 14)
		return errors.Trace(err)
	}
	trace_util_0.Count(_syncer_00000, 11)
	s.storeSession(session)

	s.mu.Lock()
	s.mu.globalVerCh = s.etcdCli.Watch(ctx, DDLGlobalSchemaVersion)
	s.mu.Unlock()

	err = PutKVToEtcd(ctx, s.etcdCli, keyOpDefaultRetryCnt, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadSession().Lease()))
	return errors.Trace(err)
}

func (s *schemaVersionSyncer) loadSession() *concurrency.Session {
	trace_util_0.Count(_syncer_00000, 15)
	return (*concurrency.Session)(atomic.LoadPointer(&s.session))
}

func (s *schemaVersionSyncer) storeSession(session *concurrency.Session) {
	trace_util_0.Count(_syncer_00000, 16)
	atomic.StorePointer(&s.session, (unsafe.Pointer)(session))
}

// Done implements SchemaSyncer.Done interface.
func (s *schemaVersionSyncer) Done() <-chan struct{} {
	trace_util_0.Count(_syncer_00000, 17)
	return s.loadSession().Done()
}

// Restart implements SchemaSyncer.Restart interface.
func (s *schemaVersionSyncer) Restart(ctx context.Context) error {
	trace_util_0.Count(_syncer_00000, 18)
	startTime := time.Now()
	var err error
	defer func() {
		trace_util_0.Count(_syncer_00000, 21)
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRestart, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	trace_util_0.Count(_syncer_00000, 19)
	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)
	// NewSession's context will affect the exit of the session.
	session, err := owner.NewSession(ctx, logPrefix, s.etcdCli, owner.NewSessionRetryUnlimited, SyncerSessionTTL)
	if err != nil {
		trace_util_0.Count(_syncer_00000, 22)
		return errors.Trace(err)
	}
	trace_util_0.Count(_syncer_00000, 20)
	s.storeSession(session)

	childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
	defer cancel()
	err = PutKVToEtcd(childCtx, s.etcdCli, putKeyRetryUnlimited, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadSession().Lease()))

	return errors.Trace(err)
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *schemaVersionSyncer) GlobalVersionCh() clientv3.WatchChan {
	trace_util_0.Count(_syncer_00000, 23)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *schemaVersionSyncer) WatchGlobalSchemaVer(ctx context.Context) {
	trace_util_0.Count(_syncer_00000, 24)
	startTime := time.Now()
	// Make sure the globalVerCh doesn't receive the information of 'close' before we finish the rewatch.
	s.mu.Lock()
	s.mu.globalVerCh = nil
	s.mu.Unlock()

	go func() {
		trace_util_0.Count(_syncer_00000, 25)
		defer func() {
			trace_util_0.Count(_syncer_00000, 27)
			metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRewatch, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
		}()
		trace_util_0.Count(_syncer_00000, 26)
		ch := s.etcdCli.Watch(ctx, DDLGlobalSchemaVersion)

		s.mu.Lock()
		s.mu.globalVerCh = ch
		s.mu.Unlock()
		logutil.Logger(ddlLogCtx).Info("[ddl] syncer watch global schema finished")
	}()
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *schemaVersionSyncer) UpdateSelfVersion(ctx context.Context, version int64) error {
	trace_util_0.Count(_syncer_00000, 28)
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyNoRetry, s.selfSchemaVerPath, ver,
		clientv3.WithLease(s.loadSession().Lease()))

	metrics.UpdateSelfVersionHistogram.WithLabelValues(metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *schemaVersionSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	trace_util_0.Count(_syncer_00000, 29)
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	// TODO: If the version is larger than the original global version, we need set the version.
	// Otherwise, we'd better set the original global version.
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyRetryUnlimited, DDLGlobalSchemaVersion, ver)
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerUpdateGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *schemaVersionSyncer) RemoveSelfVersionPath() error {
	trace_util_0.Count(_syncer_00000, 30)
	startTime := time.Now()
	var err error
	defer func() {
		trace_util_0.Count(_syncer_00000, 32)
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerClear, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	trace_util_0.Count(_syncer_00000, 31)
	err = DeleteKeyFromEtcd(s.selfSchemaVerPath, s.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	return errors.Trace(err)
}

// DeleteKeyFromEtcd deletes key value from etcd.
func DeleteKeyFromEtcd(key string, etcdCli *clientv3.Client, retryCnt int, timeout time.Duration) error {
	trace_util_0.Count(_syncer_00000, 33)
	var err error
	ctx := context.Background()
	for i := 0; i < retryCnt; i++ {
		trace_util_0.Count(_syncer_00000, 35)
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = etcdCli.Delete(childCtx, key)
		cancel()
		if err == nil {
			trace_util_0.Count(_syncer_00000, 37)
			return nil
		}
		trace_util_0.Count(_syncer_00000, 36)
		logutil.Logger(ddlLogCtx).Warn("[ddl] etcd-cli delete key failed", zap.String("key", key), zap.Error(err), zap.Int("retryCnt", i))
	}
	trace_util_0.Count(_syncer_00000, 34)
	return errors.Trace(err)
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *schemaVersionSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	trace_util_0.Count(_syncer_00000, 38)
	startTime := time.Now()
	var (
		err  error
		ver  int
		resp *clientv3.GetResponse
	)
	failedCnt := 0
	intervalCnt := int(time.Second / keyOpRetryInterval)

	defer func() {
		trace_util_0.Count(_syncer_00000, 40)
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerGetGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	trace_util_0.Count(_syncer_00000, 39)
	for {
		trace_util_0.Count(_syncer_00000, 41)
		if err != nil {
			trace_util_0.Count(_syncer_00000, 45)
			if failedCnt%intervalCnt == 0 {
				trace_util_0.Count(_syncer_00000, 47)
				logutil.Logger(ddlLogCtx).Info("[ddl] syncer get global version failed", zap.Error(err))
			}
			trace_util_0.Count(_syncer_00000, 46)
			time.Sleep(keyOpRetryInterval)
			failedCnt++
		}

		trace_util_0.Count(_syncer_00000, 42)
		if isContextDone(ctx) {
			trace_util_0.Count(_syncer_00000, 48)
			err = errors.Trace(ctx.Err())
			return 0, err
		}

		trace_util_0.Count(_syncer_00000, 43)
		resp, err = s.etcdCli.Get(ctx, DDLGlobalSchemaVersion)
		if err != nil {
			trace_util_0.Count(_syncer_00000, 49)
			continue
		}
		trace_util_0.Count(_syncer_00000, 44)
		if len(resp.Kvs) > 0 {
			trace_util_0.Count(_syncer_00000, 50)
			ver, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err == nil {
				trace_util_0.Count(_syncer_00000, 51)
				return int64(ver), nil
			}
		}
	}
}

func isContextDone(ctx context.Context) bool {
	trace_util_0.Count(_syncer_00000, 52)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_syncer_00000, 54)
		return true
	default:
		trace_util_0.Count(_syncer_00000, 55)
	}
	trace_util_0.Count(_syncer_00000, 53)
	return false
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *schemaVersionSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	trace_util_0.Count(_syncer_00000, 56)
	startTime := time.Now()
	time.Sleep(CheckVersFirstWaitTime)
	notMatchVerCnt := 0
	intervalCnt := int(time.Second / checkVersInterval)
	updatedMap := make(map[string]struct{})

	var err error
	defer func() {
		trace_util_0.Count(_syncer_00000, 58)
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerCheckAllVersions, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	trace_util_0.Count(_syncer_00000, 57)
	for {
		trace_util_0.Count(_syncer_00000, 59)
		if isContextDone(ctx) {
			trace_util_0.Count(_syncer_00000, 64)
			// ctx is canceled or timeout.
			err = errors.Trace(ctx.Err())
			return err
		}

		trace_util_0.Count(_syncer_00000, 60)
		resp, err := s.etcdCli.Get(ctx, DDLAllSchemaVersions, clientv3.WithPrefix())
		if err != nil {
			trace_util_0.Count(_syncer_00000, 65)
			logutil.Logger(ddlLogCtx).Info("[ddl] syncer check all versions failed, continue checking.", zap.Error(err))
			continue
		}

		trace_util_0.Count(_syncer_00000, 61)
		succ := true
		for _, kv := range resp.Kvs {
			trace_util_0.Count(_syncer_00000, 66)
			if _, ok := updatedMap[string(kv.Key)]; ok {
				trace_util_0.Count(_syncer_00000, 70)
				continue
			}

			trace_util_0.Count(_syncer_00000, 67)
			ver, err := strconv.Atoi(string(kv.Value))
			if err != nil {
				trace_util_0.Count(_syncer_00000, 71)
				logutil.Logger(ddlLogCtx).Info("[ddl] syncer check all versions, convert value to int failed, continue checking.", zap.String("ddl", string(kv.Key)), zap.String("value", string(kv.Value)), zap.Error(err))
				succ = false
				break
			}
			trace_util_0.Count(_syncer_00000, 68)
			if int64(ver) < latestVer {
				trace_util_0.Count(_syncer_00000, 72)
				if notMatchVerCnt%intervalCnt == 0 {
					trace_util_0.Count(_syncer_00000, 74)
					logutil.Logger(ddlLogCtx).Info("[ddl] syncer check all versions, someone is not synced, continue checking",
						zap.String("ddl", string(kv.Key)), zap.Int("currentVer", ver), zap.Int64("latestVer", latestVer))
				}
				trace_util_0.Count(_syncer_00000, 73)
				succ = false
				notMatchVerCnt++
				break
			}
			trace_util_0.Count(_syncer_00000, 69)
			updatedMap[string(kv.Key)] = struct{}{}
		}
		trace_util_0.Count(_syncer_00000, 62)
		if succ {
			trace_util_0.Count(_syncer_00000, 75)
			return nil
		}
		trace_util_0.Count(_syncer_00000, 63)
		time.Sleep(checkVersInterval)
	}
}

var _syncer_00000 = "ddl/syncer.go"
