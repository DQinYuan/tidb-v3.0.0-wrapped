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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree                = 32
	rcDefaultRegionCacheTTLSec = 600
	invalidatedLastAccessTime  = -1
)

var (
	tikvRegionCacheCounterWithInvalidateRegionFromCacheOK = metrics.TiKVRegionCacheCounter.WithLabelValues("invalidate_region_from_cache", "ok")
	tikvRegionCacheCounterWithSendFail                    = metrics.TiKVRegionCacheCounter.WithLabelValues("send_fail", "ok")
	tikvRegionCacheCounterWithGetRegionByIDOK             = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "ok")
	tikvRegionCacheCounterWithGetRegionByIDError          = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "err")
	tikvRegionCacheCounterWithGetRegionOK                 = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "ok")
	tikvRegionCacheCounterWithGetRegionError              = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "err")
	tikvRegionCacheCounterWithGetStoreOK                  = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "ok")
	tikvRegionCacheCounterWithGetStoreError               = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "err")
)

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              //  need sync new region info.
)

// Region presents kv region
type Region struct {
	meta       *metapb.Region // raw region meta from PD immutable after init
	store      unsafe.Pointer // point to region store info, see RegionStore
	syncFlag   int32          // region need be sync in next turn
	lastAccess int64          // last region access time, see checkRegionCacheTTL
}

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workStoreIdx int32    // point to current work peer in meta.Peers and work store in stores(same idx)
	stores       []*Store // stores in this region
}

// clone clones region store struct.
func (r *RegionStore) clone() *RegionStore {
	trace_util_0.Count(_region_cache_00000, 0)
	return &RegionStore{
		workStoreIdx: r.workStoreIdx,
		stores:       r.stores,
	}
}

// init initializes region after constructed.
func (r *Region) init(c *RegionCache) {
	trace_util_0.Count(_region_cache_00000, 1)
	// region store pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &RegionStore{
		workStoreIdx: 0,
		stores:       make([]*Store, 0, len(r.meta.Peers)),
	}
	for _, p := range r.meta.Peers {
		trace_util_0.Count(_region_cache_00000, 3)
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			trace_util_0.Count(_region_cache_00000, 5)
			store = c.getStoreByStoreID(p.StoreId)
		}
		trace_util_0.Count(_region_cache_00000, 4)
		rs.stores = append(rs.stores, store)
	}
	trace_util_0.Count(_region_cache_00000, 2)
	atomic.StorePointer(&r.store, unsafe.Pointer(rs))

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
}

func (r *Region) getStore() (store *RegionStore) {
	trace_util_0.Count(_region_cache_00000, 6)
	store = (*RegionStore)(atomic.LoadPointer(&r.store))
	return
}

func (r *Region) compareAndSwapStore(oldStore, newStore *RegionStore) bool {
	trace_util_0.Count(_region_cache_00000, 7)
	return atomic.CompareAndSwapPointer(&r.store, unsafe.Pointer(oldStore), unsafe.Pointer(newStore))
}

func (r *Region) checkRegionCacheTTL(ts int64) bool {
	trace_util_0.Count(_region_cache_00000, 8)
	for {
		trace_util_0.Count(_region_cache_00000, 9)
		lastAccess := atomic.LoadInt64(&r.lastAccess)
		if ts-lastAccess > rcDefaultRegionCacheTTLSec {
			trace_util_0.Count(_region_cache_00000, 11)
			return false
		}
		trace_util_0.Count(_region_cache_00000, 10)
		if atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
			trace_util_0.Count(_region_cache_00000, 12)
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate() {
	trace_util_0.Count(_region_cache_00000, 13)
	tikvRegionCacheCounterWithInvalidateRegionFromCacheOK.Inc()
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
func (r *Region) scheduleReload() {
	trace_util_0.Count(_region_cache_00000, 14)
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		trace_util_0.Count(_region_cache_00000, 16)
		return
	}
	trace_util_0.Count(_region_cache_00000, 15)
	atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync)
}

// needReload checks whether region need reload.
func (r *Region) needReload() bool {
	trace_util_0.Count(_region_cache_00000, 17)
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		trace_util_0.Count(_region_cache_00000, 19)
		return false
	}
	trace_util_0.Count(_region_cache_00000, 18)
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client

	mu struct {
		sync.RWMutex                         // mutex protect cached region
		regions      map[RegionVerID]*Region // cached regions be organized as regionVerID to region ref mapping
		sorted       *btree.BTree            // cache regions be organized as sorted key to region ref mapping
	}
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	notifyCheckCh chan struct{}
	closeCh       chan struct{}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	trace_util_0.Count(_region_cache_00000, 20)
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	go c.asyncCheckAndResolveLoop()
	return c
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	trace_util_0.Count(_region_cache_00000, 21)
	close(c.closeCh)
}

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop() {
	trace_util_0.Count(_region_cache_00000, 22)
	var needCheckStores []*Store
	for {
		trace_util_0.Count(_region_cache_00000, 23)
		select {
		case <-c.closeCh:
			trace_util_0.Count(_region_cache_00000, 24)
			return
		case <-c.notifyCheckCh:
			trace_util_0.Count(_region_cache_00000, 25)
			needCheckStores = needCheckStores[:0]
			c.checkAndResolve(needCheckStores)
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store) {
	trace_util_0.Count(_region_cache_00000, 26)
	defer func() {
		trace_util_0.Count(_region_cache_00000, 29)
		r := recover()
		if r != nil {
			trace_util_0.Count(_region_cache_00000, 30)
			logutil.Logger(context.Background()).Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	trace_util_0.Count(_region_cache_00000, 27)
	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		trace_util_0.Count(_region_cache_00000, 31)
		state := store.getResolveState()
		if state == needCheck {
			trace_util_0.Count(_region_cache_00000, 32)
			needCheckStores = append(needCheckStores, store)
		}
	}
	trace_util_0.Count(_region_cache_00000, 28)
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		trace_util_0.Count(_region_cache_00000, 33)
		store.reResolve(c)
	}
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region  RegionVerID
	Meta    *metapb.Region
	Peer    *metapb.Peer
	PeerIdx int
	Store   *Store
	Addr    string
}

// GetStoreID returns StoreID.
func (c *RPCContext) GetStoreID() uint64 {
	trace_util_0.Count(_region_cache_00000, 34)
	if c.Store != nil {
		trace_util_0.Count(_region_cache_00000, 36)
		return c.Store.storeID
	}
	trace_util_0.Count(_region_cache_00000, 35)
	return 0
}

func (c *RPCContext) String() string {
	trace_util_0.Count(_region_cache_00000, 37)
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.PeerIdx)
}

// GetRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetRPCContext(bo *Backoffer, id RegionVerID) (*RPCContext, error) {
	trace_util_0.Count(_region_cache_00000, 38)
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		trace_util_0.Count(_region_cache_00000, 43)
		return nil, nil
	}

	trace_util_0.Count(_region_cache_00000, 39)
	if !cachedRegion.checkRegionCacheTTL(ts) {
		trace_util_0.Count(_region_cache_00000, 44)
		return nil, nil
	}

	trace_util_0.Count(_region_cache_00000, 40)
	regionStore := cachedRegion.getStore()
	store, peer, storeIdx := cachedRegion.WorkStorePeer(regionStore)
	addr, err := c.getStoreAddr(bo, cachedRegion, store, storeIdx)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 45)
		return nil, err
	}
	trace_util_0.Count(_region_cache_00000, 41)
	if store == nil || len(addr) == 0 {
		trace_util_0.Count(_region_cache_00000, 46)
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	trace_util_0.Count(_region_cache_00000, 42)
	return &RPCContext{
		Region:  id,
		Meta:    cachedRegion.meta,
		Peer:    peer,
		PeerIdx: storeIdx,
		Store:   store,
		Addr:    addr,
	}, nil
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RegionVerID
	StartKey []byte
	EndKey   []byte
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	trace_util_0.Count(_region_cache_00000, 47)
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// LocateKey searches for the region and range that the key is located.
func (c *RegionCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	trace_util_0.Count(_region_cache_00000, 48)
	r, err := c.findRegionByKey(bo, key, false)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 50)
		return nil, err
	}
	trace_util_0.Count(_region_cache_00000, 49)
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

func (c *RegionCache) loadAndInsertRegion(bo *Backoffer, key []byte) (*Region, error) {
	trace_util_0.Count(_region_cache_00000, 51)
	r, err := c.loadRegion(bo, key, false)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 53)
		return nil, err
	}
	trace_util_0.Count(_region_cache_00000, 52)
	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return r, nil
}

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
func (c *RegionCache) LocateEndKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	trace_util_0.Count(_region_cache_00000, 54)
	r, err := c.findRegionByKey(bo, key, true)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 56)
		return nil, err
	}
	trace_util_0.Count(_region_cache_00000, 55)
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

func (c *RegionCache) findRegionByKey(bo *Backoffer, key []byte, isEndKey bool) (r *Region, err error) {
	trace_util_0.Count(_region_cache_00000, 57)
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil {
		trace_util_0.Count(_region_cache_00000, 59)
		// load region when it is not exists or expired.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 61)
			// no region data, return error if failure.
			return nil, err
		}
		trace_util_0.Count(_region_cache_00000, 60)
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
	} else {
		trace_util_0.Count(_region_cache_00000, 62)
		if r.needReload() {
			trace_util_0.Count(_region_cache_00000, 63)
			// load region when it be marked as need reload.
			lr, err := c.loadRegion(bo, key, isEndKey)
			if err != nil {
				trace_util_0.Count(_region_cache_00000, 64)
				// ignore error and use old region info.
				logutil.Logger(bo.ctx).Error("load region failure",
					zap.ByteString("key", key), zap.Error(err))
			} else {
				trace_util_0.Count(_region_cache_00000, 65)
				{
					r = lr
					c.mu.Lock()
					c.insertRegionToCache(r)
					c.mu.Unlock()
				}
			}
		}
	}
	trace_util_0.Count(_region_cache_00000, 58)
	return r, nil
}

// OnSendFail handles send request fail logic.
func (c *RegionCache) OnSendFail(bo *Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	trace_util_0.Count(_region_cache_00000, 66)
	tikvRegionCacheCounterWithSendFail.Inc()
	r := c.getCachedRegionWithRLock(ctx.Region)
	if r != nil {
		trace_util_0.Count(_region_cache_00000, 67)
		c.switchNextPeer(r, ctx.PeerIdx)
		if scheduleReload {
			trace_util_0.Count(_region_cache_00000, 69)
			r.scheduleReload()
		}
		trace_util_0.Count(_region_cache_00000, 68)
		logutil.Logger(bo.ctx).Info("switch region peer to next due to send request fail",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	}
}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	trace_util_0.Count(_region_cache_00000, 70)
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	c.mu.RUnlock()
	if r != nil {
		trace_util_0.Count(_region_cache_00000, 73)
		if r.needReload() {
			trace_util_0.Count(_region_cache_00000, 75)
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				trace_util_0.Count(_region_cache_00000, 76)
				// ignore error and use old region info.
				logutil.Logger(bo.ctx).Error("load region failure",
					zap.Uint64("regionID", regionID), zap.Error(err))
			} else {
				trace_util_0.Count(_region_cache_00000, 77)
				{
					r = lr
					c.mu.Lock()
					c.insertRegionToCache(r)
					c.mu.Unlock()
				}
			}
		}
		trace_util_0.Count(_region_cache_00000, 74)
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		return loc, nil
	}

	trace_util_0.Count(_region_cache_00000, 71)
	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 78)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_region_cache_00000, 72)
	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
func (c *RegionCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte) (map[RegionVerID][][]byte, RegionVerID, error) {
	trace_util_0.Count(_region_cache_00000, 79)
	groups := make(map[RegionVerID][][]byte)
	var first RegionVerID
	var lastLoc *KeyLocation
	for i, k := range keys {
		trace_util_0.Count(_region_cache_00000, 81)
		if lastLoc == nil || !lastLoc.Contains(k) {
			trace_util_0.Count(_region_cache_00000, 84)
			var err error
			lastLoc, err = c.LocateKey(bo, k)
			if err != nil {
				trace_util_0.Count(_region_cache_00000, 85)
				return nil, first, errors.Trace(err)
			}
		}
		trace_util_0.Count(_region_cache_00000, 82)
		id := lastLoc.Region
		if i == 0 {
			trace_util_0.Count(_region_cache_00000, 86)
			first = id
		}
		trace_util_0.Count(_region_cache_00000, 83)
		groups[id] = append(groups[id], k)
	}
	trace_util_0.Count(_region_cache_00000, 80)
	return groups, first, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
func (c *RegionCache) ListRegionIDsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	trace_util_0.Count(_region_cache_00000, 87)
	for {
		trace_util_0.Count(_region_cache_00000, 89)
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 92)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_region_cache_00000, 90)
		regionIDs = append(regionIDs, curRegion.Region.id)
		if curRegion.Contains(endKey) {
			trace_util_0.Count(_region_cache_00000, 93)
			break
		}
		trace_util_0.Count(_region_cache_00000, 91)
		startKey = curRegion.EndKey
	}
	trace_util_0.Count(_region_cache_00000, 88)
	return regionIDs, nil
}

// InvalidateCachedRegion removes a cached Region.
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	trace_util_0.Count(_region_cache_00000, 94)
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		trace_util_0.Count(_region_cache_00000, 96)
		return
	}
	trace_util_0.Count(_region_cache_00000, 95)
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64, currentPeerIdx int) {
	trace_util_0.Count(_region_cache_00000, 97)
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		trace_util_0.Count(_region_cache_00000, 100)
		logutil.Logger(context.Background()).Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}

	trace_util_0.Count(_region_cache_00000, 98)
	if leaderStoreID == 0 {
		trace_util_0.Count(_region_cache_00000, 101)
		c.switchNextPeer(r, currentPeerIdx)
		logutil.Logger(context.Background()).Info("switch region peer to next due to NotLeader with NULL leader",
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	trace_util_0.Count(_region_cache_00000, 99)
	if !c.switchToPeer(r, leaderStoreID) {
		trace_util_0.Count(_region_cache_00000, 102)
		logutil.Logger(context.Background()).Info("invalidate region cache due to cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", currentPeerIdx),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	} else {
		trace_util_0.Count(_region_cache_00000, 103)
		{
			logutil.Logger(context.Background()).Info("switch region leader to specific leader due to kv return NotLeader",
				zap.Uint64("regionID", regionID.GetID()),
				zap.Int("currIdx", currentPeerIdx),
				zap.Uint64("leaderStoreID", leaderStoreID))
		}
	}
}

// insertRegionToCache tries to insert the Region to cache.
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	trace_util_0.Count(_region_cache_00000, 104)
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		trace_util_0.Count(_region_cache_00000, 106)
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.VerID())
	}
	trace_util_0.Count(_region_cache_00000, 105)
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	trace_util_0.Count(_region_cache_00000, 107)
	ts := time.Now().Unix()
	var r *Region
	c.mu.RLock()
	c.mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		trace_util_0.Count(_region_cache_00000, 110)
		r = item.(*btreeItem).cachedRegion
		if isEndKey && bytes.Equal(r.StartKey(), key) {
			trace_util_0.Count(_region_cache_00000, 113)
			r = nil     // clear result
			return true // iterate next item
		}
		trace_util_0.Count(_region_cache_00000, 111)
		if !r.checkRegionCacheTTL(ts) {
			trace_util_0.Count(_region_cache_00000, 114)
			r = nil
			return true
		}
		trace_util_0.Count(_region_cache_00000, 112)
		return false
	})
	trace_util_0.Count(_region_cache_00000, 108)
	c.mu.RUnlock()
	if r != nil && (!isEndKey && r.Contains(key) || isEndKey && r.ContainsByEnd(key)) {
		trace_util_0.Count(_region_cache_00000, 115)
		return r
	}
	trace_util_0.Count(_region_cache_00000, 109)
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	trace_util_0.Count(_region_cache_00000, 116)
	for v, r := range c.mu.regions {
		trace_util_0.Count(_region_cache_00000, 118)
		if v.id == regionID {
			trace_util_0.Count(_region_cache_00000, 119)
			return r
		}
	}
	trace_util_0.Count(_region_cache_00000, 117)
	return nil
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte, isEndKey bool) (*Region, error) {
	trace_util_0.Count(_region_cache_00000, 120)
	var backoffErr error
	searchPrev := false
	for {
		trace_util_0.Count(_region_cache_00000, 121)
		if backoffErr != nil {
			trace_util_0.Count(_region_cache_00000, 130)
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				trace_util_0.Count(_region_cache_00000, 131)
				return nil, errors.Trace(err)
			}
		}
		trace_util_0.Count(_region_cache_00000, 122)
		var meta *metapb.Region
		var leader *metapb.Peer
		var err error
		if searchPrev {
			trace_util_0.Count(_region_cache_00000, 132)
			meta, leader, err = c.pdClient.GetPrevRegion(bo.ctx, key)
		} else {
			trace_util_0.Count(_region_cache_00000, 133)
			{
				meta, leader, err = c.pdClient.GetRegion(bo.ctx, key)
			}
		}
		trace_util_0.Count(_region_cache_00000, 123)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 134)
			tikvRegionCacheCounterWithGetRegionError.Inc()
		} else {
			trace_util_0.Count(_region_cache_00000, 135)
			{
				tikvRegionCacheCounterWithGetRegionOK.Inc()
			}
		}
		trace_util_0.Count(_region_cache_00000, 124)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 136)
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", key, err)
			continue
		}
		trace_util_0.Count(_region_cache_00000, 125)
		if meta == nil {
			trace_util_0.Count(_region_cache_00000, 137)
			backoffErr = errors.Errorf("region not found for key %q", key)
			continue
		}
		trace_util_0.Count(_region_cache_00000, 126)
		if len(meta.Peers) == 0 {
			trace_util_0.Count(_region_cache_00000, 138)
			return nil, errors.New("receive Region with no peer")
		}
		trace_util_0.Count(_region_cache_00000, 127)
		if isEndKey && !searchPrev && bytes.Compare(meta.StartKey, key) == 0 && len(meta.StartKey) != 0 {
			trace_util_0.Count(_region_cache_00000, 139)
			searchPrev = true
			continue
		}
		trace_util_0.Count(_region_cache_00000, 128)
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			trace_util_0.Count(_region_cache_00000, 140)
			c.switchToPeer(region, leader.StoreId)
		}
		trace_util_0.Count(_region_cache_00000, 129)
		return region, nil
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegionByID(bo *Backoffer, regionID uint64) (*Region, error) {
	trace_util_0.Count(_region_cache_00000, 141)
	var backoffErr error
	for {
		trace_util_0.Count(_region_cache_00000, 142)
		if backoffErr != nil {
			trace_util_0.Count(_region_cache_00000, 149)
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				trace_util_0.Count(_region_cache_00000, 150)
				return nil, errors.Trace(err)
			}
		}
		trace_util_0.Count(_region_cache_00000, 143)
		meta, leader, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 151)
			tikvRegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			trace_util_0.Count(_region_cache_00000, 152)
			{
				tikvRegionCacheCounterWithGetRegionByIDOK.Inc()
			}
		}
		trace_util_0.Count(_region_cache_00000, 144)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 153)
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		trace_util_0.Count(_region_cache_00000, 145)
		if meta == nil {
			trace_util_0.Count(_region_cache_00000, 154)
			backoffErr = errors.Errorf("region not found for regionID %q", regionID)
			continue
		}
		trace_util_0.Count(_region_cache_00000, 146)
		if len(meta.Peers) == 0 {
			trace_util_0.Count(_region_cache_00000, 155)
			return nil, errors.New("receive Region with no peer")
		}
		trace_util_0.Count(_region_cache_00000, 147)
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			trace_util_0.Count(_region_cache_00000, 156)
			c.switchToPeer(region, leader.GetStoreId())
		}
		trace_util_0.Count(_region_cache_00000, 148)
		return region, nil
	}
}

func (c *RegionCache) getCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	trace_util_0.Count(_region_cache_00000, 157)
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

func (c *RegionCache) getStoreAddr(bo *Backoffer, region *Region, store *Store, storeIdx int) (addr string, err error) {
	trace_util_0.Count(_region_cache_00000, 158)
	state := store.getResolveState()
	switch state {
	case resolved, needCheck:
		trace_util_0.Count(_region_cache_00000, 159)
		addr = store.addr
		return
	case unresolved:
		trace_util_0.Count(_region_cache_00000, 160)
		addr, err = store.initResolve(bo, c)
		return
	case deleted:
		trace_util_0.Count(_region_cache_00000, 161)
		addr = c.changeToActiveStore(region, store, storeIdx)
		return
	default:
		trace_util_0.Count(_region_cache_00000, 162)
		panic("unsupported resolve state")
	}
}

func (c *RegionCache) changeToActiveStore(region *Region, store *Store, storeIdx int) (addr string) {
	trace_util_0.Count(_region_cache_00000, 163)
	c.storeMu.RLock()
	store = c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
	for {
		trace_util_0.Count(_region_cache_00000, 165)
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for i, s := range oldRegionStore.stores {
			trace_util_0.Count(_region_cache_00000, 167)
			if i == storeIdx {
				trace_util_0.Count(_region_cache_00000, 168)
				newRegionStore.stores = append(newRegionStore.stores, store)
			} else {
				trace_util_0.Count(_region_cache_00000, 169)
				{
					newRegionStore.stores = append(newRegionStore.stores, s)
				}
			}
		}
		trace_util_0.Count(_region_cache_00000, 166)
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			trace_util_0.Count(_region_cache_00000, 170)
			break
		}
	}
	trace_util_0.Count(_region_cache_00000, 164)
	addr = store.addr
	return
}

func (c *RegionCache) getStoreByStoreID(storeID uint64) (store *Store) {
	trace_util_0.Count(_region_cache_00000, 171)
	var ok bool
	c.storeMu.Lock()
	store, ok = c.storeMu.stores[storeID]
	if ok {
		trace_util_0.Count(_region_cache_00000, 173)
		c.storeMu.Unlock()
		return
	}
	trace_util_0.Count(_region_cache_00000, 172)
	store = &Store{storeID: storeID}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
func (c *RegionCache) OnRegionEpochNotMatch(bo *Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) error {
	trace_util_0.Count(_region_cache_00000, 174)
	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		trace_util_0.Count(_region_cache_00000, 178)
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			trace_util_0.Count(_region_cache_00000, 179)
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.Logger(context.Background()).Info("region epoch is ahead of tikv", zap.Error(err))
			return bo.Backoff(BoRegionMiss, err)
		}
	}

	trace_util_0.Count(_region_cache_00000, 175)
	c.mu.Lock()
	defer c.mu.Unlock()
	needInvalidateOld := true
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		trace_util_0.Count(_region_cache_00000, 180)
		if _, ok := c.pdClient.(*codecPDClient); ok {
			trace_util_0.Count(_region_cache_00000, 182)
			if err := decodeRegionMetaKey(meta); err != nil {
				trace_util_0.Count(_region_cache_00000, 183)
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		trace_util_0.Count(_region_cache_00000, 181)
		region := &Region{meta: meta}
		region.init(c)
		c.switchToPeer(region, ctx.Store.storeID)
		c.insertRegionToCache(region)
		if ctx.Region == region.VerID() {
			trace_util_0.Count(_region_cache_00000, 184)
			needInvalidateOld = false
		}
	}
	trace_util_0.Count(_region_cache_00000, 176)
	if needInvalidateOld {
		trace_util_0.Count(_region_cache_00000, 185)
		cachedRegion, ok := c.mu.regions[ctx.Region]
		if ok {
			trace_util_0.Count(_region_cache_00000, 186)
			cachedRegion.invalidate()
		}
	}
	trace_util_0.Count(_region_cache_00000, 177)
	return nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	trace_util_0.Count(_region_cache_00000, 187)
	return c.pdClient
}

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key          []byte
	cachedRegion *Region
}

func newBtreeItem(cr *Region) *btreeItem {
	trace_util_0.Count(_region_cache_00000, 188)
	return &btreeItem{
		key:          cr.StartKey(),
		cachedRegion: cr,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	trace_util_0.Count(_region_cache_00000, 189)
	return &btreeItem{
		key: key,
	}
}

func (item *btreeItem) Less(other btree.Item) bool {
	trace_util_0.Count(_region_cache_00000, 190)
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	trace_util_0.Count(_region_cache_00000, 191)
	return r.meta.GetId()
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, idx int) {
	trace_util_0.Count(_region_cache_00000, 192)
	idx = int(rs.workStoreIdx)
	store = rs.stores[rs.workStoreIdx]
	peer = r.meta.Peers[rs.workStoreIdx]
	return
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
}

// GetID returns the id of the region
func (r *RegionVerID) GetID() uint64 {
	trace_util_0.Count(_region_cache_00000, 193)
	return r.id
}

// VerID returns the Region's RegionVerID.
func (r *Region) VerID() RegionVerID {
	trace_util_0.Count(_region_cache_00000, 194)
	return RegionVerID{
		id:      r.meta.GetId(),
		confVer: r.meta.GetRegionEpoch().GetConfVer(),
		ver:     r.meta.GetRegionEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Region) StartKey() []byte {
	trace_util_0.Count(_region_cache_00000, 195)
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Region) EndKey() []byte {
	trace_util_0.Count(_region_cache_00000, 196)
	return r.meta.EndKey
}

// switchToPeer switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
func (c *RegionCache) switchToPeer(r *Region, targetStoreID uint64) (found bool) {
	trace_util_0.Count(_region_cache_00000, 197)
	leaderIdx, found := c.getPeerStoreIndex(r, targetStoreID)
	c.switchWorkIdx(r, leaderIdx)
	return
}

func (c *RegionCache) switchNextPeer(r *Region, currentPeerIdx int) {
	trace_util_0.Count(_region_cache_00000, 198)
	regionStore := r.getStore()
	if int(regionStore.workStoreIdx) != currentPeerIdx {
		trace_util_0.Count(_region_cache_00000, 200)
		return
	}
	trace_util_0.Count(_region_cache_00000, 199)
	nextIdx := (currentPeerIdx + 1) % len(regionStore.stores)
	newRegionStore := regionStore.clone()
	newRegionStore.workStoreIdx = int32(nextIdx)
	r.compareAndSwapStore(regionStore, newRegionStore)
}

func (c *RegionCache) getPeerStoreIndex(r *Region, id uint64) (idx int, found bool) {
	trace_util_0.Count(_region_cache_00000, 201)
	if len(r.meta.Peers) == 0 {
		trace_util_0.Count(_region_cache_00000, 204)
		return
	}
	trace_util_0.Count(_region_cache_00000, 202)
	for i, p := range r.meta.Peers {
		trace_util_0.Count(_region_cache_00000, 205)
		if p.GetStoreId() == id {
			trace_util_0.Count(_region_cache_00000, 206)
			idx = i
			found = true
			return
		}
	}
	trace_util_0.Count(_region_cache_00000, 203)
	return
}

func (c *RegionCache) switchWorkIdx(r *Region, leaderIdx int) {
	trace_util_0.Count(_region_cache_00000, 207)
retry:
	// switch to new leader.
	trace_util_0.Count(_region_cache_00000, 208)
	oldRegionStore := r.getStore()
	if oldRegionStore.workStoreIdx == int32(leaderIdx) {
		trace_util_0.Count(_region_cache_00000, 211)
		return
	}
	trace_util_0.Count(_region_cache_00000, 209)
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workStoreIdx = int32(leaderIdx)
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		trace_util_0.Count(_region_cache_00000, 212)
		goto retry
	}
	trace_util_0.Count(_region_cache_00000, 210)
	return
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	trace_util_0.Count(_region_cache_00000, 213)
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// ContainsByEnd check the region contains the greatest key that is less than key.
// for the maximum region endKey is empty.
// startKey < key <= endKey.
func (r *Region) ContainsByEnd(key []byte) bool {
	trace_util_0.Count(_region_cache_00000, 214)
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a kv process's address.
type Store struct {
	addr         string     // loaded store address
	storeID      uint64     // store's id
	state        uint64     // unsafe store storeState
	resolveMutex sync.Mutex // protect pd from concurrent init requests
}

type resolveState uint64

const (
	unresolved resolveState = iota
	resolved
	needCheck
	deleted
)

// initResolve resolves addr for store that never resolved.
func (s *Store) initResolve(bo *Backoffer, c *RegionCache) (addr string, err error) {
	trace_util_0.Count(_region_cache_00000, 215)
	s.resolveMutex.Lock()
	state := s.getResolveState()
	defer s.resolveMutex.Unlock()
	if state != unresolved {
		trace_util_0.Count(_region_cache_00000, 217)
		addr = s.addr
		return
	}
	trace_util_0.Count(_region_cache_00000, 216)
	var store *metapb.Store
	for {
		trace_util_0.Count(_region_cache_00000, 218)
		store, err = c.pdClient.GetStore(bo.ctx, s.storeID)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 225)
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			trace_util_0.Count(_region_cache_00000, 226)
			{
				tikvRegionCacheCounterWithGetStoreOK.Inc()
			}
		}
		trace_util_0.Count(_region_cache_00000, 219)
		if err != nil {
			trace_util_0.Count(_region_cache_00000, 227)
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				trace_util_0.Count(_region_cache_00000, 230)
				return
			}
			trace_util_0.Count(_region_cache_00000, 228)
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				trace_util_0.Count(_region_cache_00000, 231)
				return
			}
			trace_util_0.Count(_region_cache_00000, 229)
			continue
		}
		trace_util_0.Count(_region_cache_00000, 220)
		if store == nil {
			trace_util_0.Count(_region_cache_00000, 232)
			return
		}
		trace_util_0.Count(_region_cache_00000, 221)
		addr = store.GetAddress()
		s.addr = addr
	retry:
		trace_util_0.Count(_region_cache_00000, 222)
		state = s.getResolveState()
		if state != unresolved {
			trace_util_0.Count(_region_cache_00000, 233)
			addr = s.addr
			return
		}
		trace_util_0.Count(_region_cache_00000, 223)
		if !s.compareAndSwapState(state, resolved) {
			trace_util_0.Count(_region_cache_00000, 234)
			goto retry
		}
		trace_util_0.Count(_region_cache_00000, 224)
		return
	}
}

// reResolve try to resolve addr for store that need check.
func (s *Store) reResolve(c *RegionCache) {
	trace_util_0.Count(_region_cache_00000, 235)
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 242)
		tikvRegionCacheCounterWithGetStoreError.Inc()
	} else {
		trace_util_0.Count(_region_cache_00000, 243)
		{
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
	}
	trace_util_0.Count(_region_cache_00000, 236)
	if err != nil {
		trace_util_0.Count(_region_cache_00000, 244)
		logutil.Logger(context.Background()).Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return
	}
	trace_util_0.Count(_region_cache_00000, 237)
	if store == nil {
		trace_util_0.Count(_region_cache_00000, 245)
		return
	}

	trace_util_0.Count(_region_cache_00000, 238)
	addr = store.GetAddress()
	if s.addr != addr {
		trace_util_0.Count(_region_cache_00000, 246)
		state := resolved
		newStore := &Store{storeID: s.storeID, addr: addr}
		newStore.state = *(*uint64)(unsafe.Pointer(&state))
		c.storeMu.Lock()
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
	retryMarkDel:
		// all region used those
		trace_util_0.Count(_region_cache_00000, 247)
		oldState := s.getResolveState()
		if oldState == deleted {
			trace_util_0.Count(_region_cache_00000, 250)
			return
		}
		trace_util_0.Count(_region_cache_00000, 248)
		newState := deleted
		if !s.compareAndSwapState(oldState, newState) {
			trace_util_0.Count(_region_cache_00000, 251)
			goto retryMarkDel
		}
		trace_util_0.Count(_region_cache_00000, 249)
		return
	}
retryMarkResolved:
	trace_util_0.Count(_region_cache_00000, 239)
	oldState := s.getResolveState()
	if oldState != needCheck {
		trace_util_0.Count(_region_cache_00000, 252)
		return
	}
	trace_util_0.Count(_region_cache_00000, 240)
	newState := resolved
	if !s.compareAndSwapState(oldState, newState) {
		trace_util_0.Count(_region_cache_00000, 253)
		goto retryMarkResolved
	}
	trace_util_0.Count(_region_cache_00000, 241)
	return
}

func (s *Store) getResolveState() resolveState {
	trace_util_0.Count(_region_cache_00000, 254)
	var state resolveState
	if s == nil {
		trace_util_0.Count(_region_cache_00000, 256)
		return state
	}
	trace_util_0.Count(_region_cache_00000, 255)
	return resolveState(atomic.LoadUint64(&s.state))
}

func (s *Store) compareAndSwapState(oldState, newState resolveState) bool {
	trace_util_0.Count(_region_cache_00000, 257)
	return atomic.CompareAndSwapUint64(&s.state, uint64(oldState), uint64(newState))
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
	trace_util_0.Count(_region_cache_00000, 258)
retry:
	trace_util_0.Count(_region_cache_00000, 259)
	oldState := s.getResolveState()
	if oldState != resolved {
		trace_util_0.Count(_region_cache_00000, 262)
		return
	}
	trace_util_0.Count(_region_cache_00000, 260)
	if !s.compareAndSwapState(oldState, needCheck) {
		trace_util_0.Count(_region_cache_00000, 263)
		goto retry
	}
	trace_util_0.Count(_region_cache_00000, 261)
	select {
	case notifyCheckCh <- struct{}{}:
		trace_util_0.Count(_region_cache_00000, 264)
	default:
		trace_util_0.Count(_region_cache_00000, 265)
	}

}

var _region_cache_00000 = "store/tikv/region_cache.go"
