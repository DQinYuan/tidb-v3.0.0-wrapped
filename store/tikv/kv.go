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

package tikv

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/latch"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	trace_util_0.Count(_kv_00000, 0)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		TLS:              tlsConfig,
	})
	if err != nil {
		trace_util_0.Count(_kv_00000, 2)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 1)
	return cli, nil
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (kv.Storage, error) {
	trace_util_0.Count(_kv_00000, 3)
	mc.Lock()
	defer mc.Unlock()

	security := config.GetGlobalConfig().Security
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	etcdAddrs, disableGC, err := parsePath(path)
	if err != nil {
		trace_util_0.Count(_kv_00000, 11)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_kv_00000, 4)
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})

	if err != nil {
		trace_util_0.Count(_kv_00000, 12)
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	trace_util_0.Count(_kv_00000, 5)
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		trace_util_0.Count(_kv_00000, 13)
		return store, nil
	}

	trace_util_0.Count(_kv_00000, 6)
	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		trace_util_0.Count(_kv_00000, 14)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_kv_00000, 7)
	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		trace_util_0.Count(_kv_00000, 15)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_kv_00000, 8)
	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), !disableGC)
	if err != nil {
		trace_util_0.Count(_kv_00000, 16)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 9)
	if txnLocalLatches.Enabled {
		trace_util_0.Count(_kv_00000, 17)
		s.EnableTxnLocalLatches(txnLocalLatches.Capacity)
	}
	trace_util_0.Count(_kv_00000, 10)
	s.etcdAddrs = etcdAddrs
	s.tlsConfig = tlsConfig

	mc.cache[uuid] = s
	return s, nil
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() []string
	TLSConfig() *tls.Config
	StartGCWorker() error
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type tikvStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	pdClient     pd.Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	txnLatches   *latch.LatchesScheduler
	gcWorker     GCHandler
	etcdAddrs    []string
	tlsConfig    *tls.Config
	mock         bool
	enableGC     bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to nofity when the store is closed
}

func (s *tikvStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	trace_util_0.Count(_kv_00000, 18)
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

func (s *tikvStore) CheckVisibility(startTime uint64) error {
	trace_util_0.Count(_kv_00000, 19)
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		trace_util_0.Count(_kv_00000, 22)
		return ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point")
	}

	trace_util_0.Count(_kv_00000, 20)
	if startTime < cachedSafePoint {
		trace_util_0.Count(_kv_00000, 23)
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return ErrGCTooEarly.GenWithStackByArgs(t1, t2)
	}

	trace_util_0.Count(_kv_00000, 21)
	return nil
}

func newTikvStore(uuid string, pdClient pd.Client, spkv SafePointKV, client Client, enableGC bool) (*tikvStore, error) {
	trace_util_0.Count(_kv_00000, 24)
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		trace_util_0.Count(_kv_00000, 26)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 25)
	store := &tikvStore{
		clusterID:   pdClient.GetClusterID(context.TODO()),
		uuid:        uuid,
		oracle:      o,
		client:      client,
		pdClient:    pdClient,
		regionCache: NewRegionCache(pdClient),
		kv:          spkv,
		safePoint:   0,
		spTime:      time.Now(),
		closed:      make(chan struct{}),
	}
	store.lockResolver = newLockResolver(store)
	store.enableGC = enableGC

	go store.runSafePointChecker()

	return store, nil
}

func (s *tikvStore) EnableTxnLocalLatches(size uint) {
	trace_util_0.Count(_kv_00000, 27)
	s.txnLatches = latch.NewScheduler(size)
}

// IsLatchEnabled is used by mockstore.TestConfig.
func (s *tikvStore) IsLatchEnabled() bool {
	trace_util_0.Count(_kv_00000, 28)
	return s.txnLatches != nil
}

func (s *tikvStore) EtcdAddrs() []string {
	trace_util_0.Count(_kv_00000, 29)
	return s.etcdAddrs
}

func (s *tikvStore) TLSConfig() *tls.Config {
	trace_util_0.Count(_kv_00000, 30)
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapSession, don't call this function more than once.
func (s *tikvStore) StartGCWorker() error {
	trace_util_0.Count(_kv_00000, 31)
	if !s.enableGC || NewGCHandlerFunc == nil {
		trace_util_0.Count(_kv_00000, 34)
		return nil
	}

	trace_util_0.Count(_kv_00000, 32)
	gcWorker, err := NewGCHandlerFunc(s, s.pdClient)
	if err != nil {
		trace_util_0.Count(_kv_00000, 35)
		return errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 33)
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

func (s *tikvStore) runSafePointChecker() {
	trace_util_0.Count(_kv_00000, 36)
	d := gcSafePointUpdateInterval
	for {
		trace_util_0.Count(_kv_00000, 37)
		select {
		case spCachedTime := <-time.After(d):
			trace_util_0.Count(_kv_00000, 38)
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV(), GcSavedSafePoint)
			if err == nil {
				trace_util_0.Count(_kv_00000, 40)
				metrics.TiKVLoadSafepointCounter.WithLabelValues("ok").Inc()
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {
				trace_util_0.Count(_kv_00000, 41)
				{
					metrics.TiKVLoadSafepointCounter.WithLabelValues("fail").Inc()
					logutil.Logger(context.Background()).Error("fail to load safepoint from pd", zap.Error(err))
					d = gcSafePointQuickRepeatInterval
				}
			}
		case <-s.Closed():
			trace_util_0.Count(_kv_00000, 39)
			return
		}
	}
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	trace_util_0.Count(_kv_00000, 42)
	txn, err := newTiKVTxn(s)
	if err != nil {
		trace_util_0.Count(_kv_00000, 44)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 43)
	metrics.TiKVTxnCounter.Inc()
	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *tikvStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	trace_util_0.Count(_kv_00000, 45)
	txn, err := newTikvTxnWithStartTS(s, startTS)
	if err != nil {
		trace_util_0.Count(_kv_00000, 47)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 46)
	metrics.TiKVTxnCounter.Inc()
	return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	trace_util_0.Count(_kv_00000, 48)
	snapshot := newTiKVSnapshot(s, ver)
	metrics.TiKVSnapshotCounter.Inc()
	return snapshot, nil
}

func (s *tikvStore) Close() error {
	trace_util_0.Count(_kv_00000, 49)
	mc.Lock()
	defer mc.Unlock()

	delete(mc.cache, s.uuid)
	s.oracle.Close()
	s.pdClient.Close()
	if s.gcWorker != nil {
		trace_util_0.Count(_kv_00000, 53)
		s.gcWorker.Close()
	}

	trace_util_0.Count(_kv_00000, 50)
	close(s.closed)
	if err := s.client.Close(); err != nil {
		trace_util_0.Count(_kv_00000, 54)
		return errors.Trace(err)
	}

	trace_util_0.Count(_kv_00000, 51)
	if s.txnLatches != nil {
		trace_util_0.Count(_kv_00000, 55)
		s.txnLatches.Close()
	}
	trace_util_0.Count(_kv_00000, 52)
	s.regionCache.Close()
	return nil
}

func (s *tikvStore) UUID() string {
	trace_util_0.Count(_kv_00000, 56)
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	trace_util_0.Count(_kv_00000, 57)
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := s.getTimestampWithRetry(bo)
	if err != nil {
		trace_util_0.Count(_kv_00000, 59)
		return kv.NewVersion(0), errors.Trace(err)
	}
	trace_util_0.Count(_kv_00000, 58)
	return kv.NewVersion(startTS), nil
}

func (s *tikvStore) getTimestampWithRetry(bo *Backoffer) (uint64, error) {
	trace_util_0.Count(_kv_00000, 60)
	for {
		trace_util_0.Count(_kv_00000, 61)
		startTS, err := s.oracle.GetTimestamp(bo.ctx)
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			trace_util_0.Count(_kv_00000, 64)
			if val.(bool) && !kv.IsMockCommitErrorEnable() {
				trace_util_0.Count(_kv_00000, 65)
				err = ErrPDServerTimeout.GenWithStackByArgs("mock PD timeout")
			}
		})

		trace_util_0.Count(_kv_00000, 62)
		if err == nil {
			trace_util_0.Count(_kv_00000, 66)
			return startTS, nil
		}
		trace_util_0.Count(_kv_00000, 63)
		err = bo.Backoff(BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			trace_util_0.Count(_kv_00000, 67)
			return 0, errors.Trace(err)
		}
	}
}

func (s *tikvStore) GetClient() kv.Client {
	trace_util_0.Count(_kv_00000, 68)
	return &CopClient{
		store: s,
	}
}

func (s *tikvStore) GetOracle() oracle.Oracle {
	trace_util_0.Count(_kv_00000, 69)
	return s.oracle
}

func (s *tikvStore) Name() string {
	trace_util_0.Count(_kv_00000, 70)
	return "TiKV"
}

func (s *tikvStore) Describe() string {
	trace_util_0.Count(_kv_00000, 71)
	return "TiKV is a distributed transactional key-value database"
}

func (s *tikvStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	trace_util_0.Count(_kv_00000, 72)
	return nil, kv.ErrNotImplemented
}

func (s *tikvStore) SupportDeleteRange() (supported bool) {
	trace_util_0.Count(_kv_00000, 73)
	return !s.mock
}

func (s *tikvStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	trace_util_0.Count(_kv_00000, 74)
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *tikvStore) GetRegionCache() *RegionCache {
	trace_util_0.Count(_kv_00000, 75)
	return s.regionCache
}

func (s *tikvStore) GetLockResolver() *LockResolver {
	trace_util_0.Count(_kv_00000, 76)
	return s.lockResolver
}

func (s *tikvStore) GetGCHandler() GCHandler {
	trace_util_0.Count(_kv_00000, 77)
	return s.gcWorker
}

func (s *tikvStore) Closed() <-chan struct{} {
	trace_util_0.Count(_kv_00000, 78)
	return s.closed
}

func (s *tikvStore) GetSafePointKV() SafePointKV {
	trace_util_0.Count(_kv_00000, 79)
	return s.kv
}

func (s *tikvStore) SetOracle(oracle oracle.Oracle) {
	trace_util_0.Count(_kv_00000, 80)
	s.oracle = oracle
}

func (s *tikvStore) SetTiKVClient(client Client) {
	trace_util_0.Count(_kv_00000, 81)
	s.client = client
}

func (s *tikvStore) GetTiKVClient() (client Client) {
	trace_util_0.Count(_kv_00000, 82)
	return s.client
}

func parsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	trace_util_0.Count(_kv_00000, 83)
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		trace_util_0.Count(_kv_00000, 87)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_kv_00000, 84)
	if strings.ToLower(u.Scheme) != "tikv" {
		trace_util_0.Count(_kv_00000, 88)
		err = errors.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		logutil.Logger(context.Background()).Error("parsePath error", zap.Error(err))
		return
	}
	trace_util_0.Count(_kv_00000, 85)
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		trace_util_0.Count(_kv_00000, 89)
		disableGC = true
	case "false", "":
		trace_util_0.Count(_kv_00000, 90)
	default:
		trace_util_0.Count(_kv_00000, 91)
		err = errors.New("disableGC flag should be true/false")
		return
	}
	trace_util_0.Count(_kv_00000, 86)
	etcdAddrs = strings.Split(u.Host, ",")
	return
}

func init() {
	trace_util_0.Count(_kv_00000, 92)
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}

var _kv_00000 = "store/tikv/kv.go"
