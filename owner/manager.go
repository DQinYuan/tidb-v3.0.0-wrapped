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
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt          = int(3 * time.Second / newSessionRetryInterval)
)

// Manager is used to campaign the owner and manage the owner information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
	// RetireOwner make the manager to be a not owner. It's exported for testing.
	RetireOwner()
	// GetOwnerID gets the owner ID.
	GetOwnerID(ctx context.Context) (string, error)
	// CampaignOwner campaigns the owner.
	CampaignOwner(ctx context.Context) error
	// ResignOwner lets the owner start a new election.
	ResignOwner(ctx context.Context) error
	// Cancel cancels this etcd ownerManager campaign.
	Cancel()
}

const (
	// NewSessionDefaultRetryCnt is the default retry times when create new session.
	NewSessionDefaultRetryCnt = 3
	// NewSessionRetryUnlimited is the unlimited retry times when create new session.
	NewSessionRetryUnlimited = math.MaxInt64
	keyOpDefaultTimeout      = 5 * time.Second
)

// DDLOwnerChecker is used to check whether tidb is owner.
type DDLOwnerChecker interface {
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
}

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	id        string // id is the ID of the manager.
	key       string
	prompt    string
	logPrefix string
	logCtx    context.Context
	etcdCli   *clientv3.Client
	cancel    context.CancelFunc
	elec      unsafe.Pointer
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(etcdCli *clientv3.Client, prompt, id, key string, cancel context.CancelFunc) Manager {
	trace_util_0.Count(_manager_00000, 0)
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", prompt, key, id)
	return &ownerManager{
		etcdCli:   etcdCli,
		id:        id,
		key:       key,
		prompt:    prompt,
		cancel:    cancel,
		logPrefix: logPrefix,
		logCtx:    logutil.WithKeyValue(context.Background(), "owner info", logPrefix),
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	trace_util_0.Count(_manager_00000, 1)
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	trace_util_0.Count(_manager_00000, 2)
	return atomic.LoadPointer(&m.elec) != unsafe.Pointer(nil)
}

// Cancel implements Manager.Cancel interface.
func (m *ownerManager) Cancel() {
	trace_util_0.Count(_manager_00000, 3)
	m.cancel()
}

// ManagerSessionTTL is the etcd session's TTL in seconds. It's exported for testing.
var ManagerSessionTTL = 60

// setManagerSessionTTL sets the ManagerSessionTTL value, it's used for testing.
func setManagerSessionTTL() error {
	trace_util_0.Count(_manager_00000, 4)
	ttlStr := os.Getenv("tidb_manager_ttl")
	if len(ttlStr) == 0 {
		trace_util_0.Count(_manager_00000, 7)
		return nil
	}
	trace_util_0.Count(_manager_00000, 5)
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		trace_util_0.Count(_manager_00000, 8)
		return errors.Trace(err)
	}
	trace_util_0.Count(_manager_00000, 6)
	ManagerSessionTTL = ttl
	return nil
}

// NewSession creates a new etcd session.
func NewSession(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	trace_util_0.Count(_manager_00000, 9)
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		trace_util_0.Count(_manager_00000, 11)
		if err = contextDone(ctx, err); err != nil {
			trace_util_0.Count(_manager_00000, 17)
			return etcdSession, errors.Trace(err)
		}

		trace_util_0.Count(_manager_00000, 12)
		failpoint.Inject("closeClient", func(val failpoint.Value) {
			trace_util_0.Count(_manager_00000, 18)
			if val.(bool) {
				trace_util_0.Count(_manager_00000, 19)
				if err := etcdCli.Close(); err != nil {
					trace_util_0.Count(_manager_00000, 20)
					failpoint.Return(etcdSession, errors.Trace(err))
				}
			}
		})

		trace_util_0.Count(_manager_00000, 13)
		failpoint.Inject("closeGrpc", func(val failpoint.Value) {
			trace_util_0.Count(_manager_00000, 21)
			if val.(bool) {
				trace_util_0.Count(_manager_00000, 22)
				if err := etcdCli.ActiveConnection().Close(); err != nil {
					trace_util_0.Count(_manager_00000, 23)
					failpoint.Return(etcdSession, errors.Trace(err))
				}
			}
		})

		trace_util_0.Count(_manager_00000, 14)
		startTime := time.Now()
		etcdSession, err = concurrency.NewSession(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		metrics.NewSessionHistogram.WithLabelValues(logPrefix, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err == nil {
			trace_util_0.Count(_manager_00000, 24)
			break
		}
		trace_util_0.Count(_manager_00000, 15)
		if failedCnt%logIntervalCnt == 0 {
			trace_util_0.Count(_manager_00000, 25)
			logutil.Logger(context.Background()).Warn("failed to new session to etcd", zap.String("ownerInfo", logPrefix), zap.Error(err))
		}

		trace_util_0.Count(_manager_00000, 16)
		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	trace_util_0.Count(_manager_00000, 10)
	return etcdSession, errors.Trace(err)
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *ownerManager) CampaignOwner(ctx context.Context) error {
	trace_util_0.Count(_manager_00000, 26)
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	session, err := NewSession(ctx, logPrefix, m.etcdCli, NewSessionDefaultRetryCnt, ManagerSessionTTL)
	if err != nil {
		trace_util_0.Count(_manager_00000, 28)
		return errors.Trace(err)
	}
	trace_util_0.Count(_manager_00000, 27)
	go m.campaignLoop(ctx, session)
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	trace_util_0.Count(_manager_00000, 29)
	elec := (*concurrency.Election)(atomic.LoadPointer(&m.elec))
	if elec == nil {
		trace_util_0.Count(_manager_00000, 32)
		return errors.Errorf("This node is not a ddl owner, can't be resigned.")
	}

	trace_util_0.Count(_manager_00000, 30)
	childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
	err := elec.Resign(childCtx)
	cancel()
	if err != nil {
		trace_util_0.Count(_manager_00000, 33)
		return errors.Trace(err)
	}

	trace_util_0.Count(_manager_00000, 31)
	logutil.Logger(m.logCtx).Warn("resign ddl owner success")
	return nil
}

func (m *ownerManager) toBeOwner(elec *concurrency.Election) {
	trace_util_0.Count(_manager_00000, 34)
	atomic.StorePointer(&m.elec, unsafe.Pointer(elec))
}

// RetireOwner make the manager to be a not owner.
func (m *ownerManager) RetireOwner() {
	trace_util_0.Count(_manager_00000, 35)
	atomic.StorePointer(&m.elec, nil)
}

func (m *ownerManager) campaignLoop(ctx context.Context, etcdSession *concurrency.Session) {
	trace_util_0.Count(_manager_00000, 36)
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer func() {
		trace_util_0.Count(_manager_00000, 38)
		cancel()
		if r := recover(); r != nil {
			trace_util_0.Count(_manager_00000, 39)
			buf := util.GetStack()
			logutil.Logger(context.Background()).Error("recover panic", zap.String("prompt", m.prompt), zap.Any("error", r), zap.String("buffer", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDLOwner).Inc()
		}
	}()

	trace_util_0.Count(_manager_00000, 37)
	logPrefix := m.logPrefix
	logCtx := m.logCtx
	var err error
	for {
		trace_util_0.Count(_manager_00000, 40)
		if err != nil {
			trace_util_0.Count(_manager_00000, 46)
			metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, err.Error()).Inc()
		}

		trace_util_0.Count(_manager_00000, 41)
		select {
		case <-etcdSession.Done():
			trace_util_0.Count(_manager_00000, 47)
			logutil.Logger(logCtx).Info("etcd session is done, creates a new one")
			leaseID := etcdSession.Lease()
			etcdSession, err = NewSession(ctx, logPrefix, m.etcdCli, NewSessionRetryUnlimited, ManagerSessionTTL)
			if err != nil {
				trace_util_0.Count(_manager_00000, 50)
				logutil.Logger(logCtx).Info("break campaign loop, NewSession failed", zap.Error(err))
				m.revokeSession(logPrefix, leaseID)
				return
			}
		case <-ctx.Done():
			trace_util_0.Count(_manager_00000, 48)
			logutil.Logger(logCtx).Info("break campaign loop, context is done")
			m.revokeSession(logPrefix, etcdSession.Lease())
			return
		default:
			trace_util_0.Count(_manager_00000, 49)
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this session's lease ID, but etcd session doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		trace_util_0.Count(_manager_00000, 42)
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			trace_util_0.Count(_manager_00000, 51)
			if etcdSession != nil {
				trace_util_0.Count(_manager_00000, 53)
				err = etcdSession.Close()
				logutil.Logger(logCtx).Info("etcd session encounters the error of lease not found, closes it", zap.Error(err))
			}
			trace_util_0.Count(_manager_00000, 52)
			continue
		}

		trace_util_0.Count(_manager_00000, 43)
		elec := concurrency.NewElection(etcdSession, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			trace_util_0.Count(_manager_00000, 54)
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		trace_util_0.Count(_manager_00000, 44)
		ownerKey, err := GetOwnerInfo(ctx, logCtx, elec, m.id)
		if err != nil {
			trace_util_0.Count(_manager_00000, 55)
			continue
		}

		trace_util_0.Count(_manager_00000, 45)
		m.toBeOwner(elec)
		m.watchOwner(ctx, etcdSession, ownerKey)
		m.RetireOwner()

		metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, metrics.NoLongerOwner).Inc()
		logutil.Logger(logCtx).Warn("is not the owner")
	}
}

func (m *ownerManager) revokeSession(logPrefix string, leaseID clientv3.LeaseID) {
	trace_util_0.Count(_manager_00000, 56)
	// Revoke the session lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ManagerSessionTTL)*time.Second)
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	cancel()
	logutil.Logger(m.logCtx).Info("revoke session", zap.Error(err))
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *ownerManager) GetOwnerID(ctx context.Context) (string, error) {
	trace_util_0.Count(_manager_00000, 57)
	resp, err := m.etcdCli.Get(ctx, m.key, clientv3.WithFirstCreate()...)
	if err != nil {
		trace_util_0.Count(_manager_00000, 60)
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_manager_00000, 58)
	if len(resp.Kvs) == 0 {
		trace_util_0.Count(_manager_00000, 61)
		return "", concurrency.ErrElectionNoLeader
	}
	trace_util_0.Count(_manager_00000, 59)
	return string(resp.Kvs[0].Value), nil
}

// GetOwnerInfo gets the owner information.
func GetOwnerInfo(ctx, logCtx context.Context, elec *concurrency.Election, id string) (string, error) {
	trace_util_0.Count(_manager_00000, 62)
	resp, err := elec.Leader(ctx)
	if err != nil {
		trace_util_0.Count(_manager_00000, 65)
		// If no leader elected currently, it returns ErrElectionNoLeader.
		logutil.Logger(logCtx).Info("failed to get leader", zap.Error(err))
		return "", errors.Trace(err)
	}
	trace_util_0.Count(_manager_00000, 63)
	ownerID := string(resp.Kvs[0].Value)
	logutil.Logger(logCtx).Info("get owner", zap.String("ownerID", ownerID))
	if ownerID != id {
		trace_util_0.Count(_manager_00000, 66)
		logutil.Logger(logCtx).Warn("is not the owner")
		return "", errors.New("ownerInfoNotMatch")
	}

	trace_util_0.Count(_manager_00000, 64)
	return string(resp.Kvs[0].Key), nil
}

func (m *ownerManager) watchOwner(ctx context.Context, etcdSession *concurrency.Session, key string) {
	trace_util_0.Count(_manager_00000, 67)
	logPrefix := fmt.Sprintf("[%s] ownerManager %s watch owner key %v", m.prompt, m.id, key)
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	logutil.Logger(context.Background()).Debug(logPrefix)
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		trace_util_0.Count(_manager_00000, 68)
		select {
		case resp, ok := <-watchCh:
			trace_util_0.Count(_manager_00000, 69)
			if !ok {
				trace_util_0.Count(_manager_00000, 74)
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.WatcherClosed).Inc()
				logutil.Logger(logCtx).Info("watcher is closed, no owner")
				return
			}
			trace_util_0.Count(_manager_00000, 70)
			if resp.Canceled {
				trace_util_0.Count(_manager_00000, 75)
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Cancelled).Inc()
				logutil.Logger(logCtx).Info("watch canceled, no owner")
				return
			}

			trace_util_0.Count(_manager_00000, 71)
			for _, ev := range resp.Events {
				trace_util_0.Count(_manager_00000, 76)
				if ev.Type == mvccpb.DELETE {
					trace_util_0.Count(_manager_00000, 77)
					metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Deleted).Inc()
					logutil.Logger(logCtx).Info("watch failed, owner is deleted")
					return
				}
			}
		case <-etcdSession.Done():
			trace_util_0.Count(_manager_00000, 72)
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.SessionDone).Inc()
			return
		case <-ctx.Done():
			trace_util_0.Count(_manager_00000, 73)
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.CtxDone).Inc()
			return
		}
	}
}

func init() {
	trace_util_0.Count(_manager_00000, 78)
	err := setManagerSessionTTL()
	if err != nil {
		trace_util_0.Count(_manager_00000, 79)
		logutil.Logger(context.Background()).Warn("set manager session TTL failed", zap.Error(err))
	}
}

func contextDone(ctx context.Context, err error) error {
	trace_util_0.Count(_manager_00000, 80)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_manager_00000, 83)
		return errors.Trace(ctx.Err())
	default:
		trace_util_0.Count(_manager_00000, 84)
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	trace_util_0.Count(_manager_00000, 81)
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		terror.ErrorEqual(err, grpc.ErrClientConnClosing) {
		trace_util_0.Count(_manager_00000, 85)
		return errors.Trace(err)
	}

	trace_util_0.Count(_manager_00000, 82)
	return nil
}

var _manager_00000 = "owner/manager.go"
