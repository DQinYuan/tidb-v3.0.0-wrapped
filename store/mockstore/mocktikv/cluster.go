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

package mocktikv

import (
	"bytes"
	"context"
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
)

// Cluster simulates a TiKV cluster. It focuses on management and the change of
// meta data. A Cluster mainly includes following 3 kinds of meta data:
// 1) Region: A Region is a fragment of TiKV's data whose range is [start, end).
//    The data of a Region is duplicated to multiple Peers and distributed in
//    multiple Stores.
// 2) Peer: A Peer is a replica of a Region's data. All peers of a Region form
//    a group, each group elects a Leader to provide services.
// 3) Store: A Store is a storage/service node. Try to think it as a TiKV server
//    process. Only the store with request's Region's leader Peer could respond
//    to client's request.
type Cluster struct {
	sync.RWMutex
	id      uint64
	stores  map[uint64]*Store
	regions map[uint64]*Region

	// delayEvents is used to control the execution sequence of rpc requests for test.
	delayEvents map[delayKey]time.Duration
	delayMu     sync.Mutex
}

type delayKey struct {
	startTS  uint64
	regionID uint64
}

// NewCluster creates an empty cluster. It needs to be bootstrapped before
// providing service.
func NewCluster() *Cluster {
	trace_util_0.Count(_cluster_00000, 0)
	return &Cluster{
		stores:      make(map[uint64]*Store),
		regions:     make(map[uint64]*Region),
		delayEvents: make(map[delayKey]time.Duration),
	}
}

// AllocID creates an unique ID in cluster. The ID could be used as either
// StoreID, RegionID, or PeerID.
func (c *Cluster) AllocID() uint64 {
	trace_util_0.Count(_cluster_00000, 1)
	c.Lock()
	defer c.Unlock()

	return c.allocID()
}

// AllocIDs creates multiple IDs.
func (c *Cluster) AllocIDs(n int) []uint64 {
	trace_util_0.Count(_cluster_00000, 2)
	c.Lock()
	defer c.Unlock()

	var ids []uint64
	for len(ids) < n {
		trace_util_0.Count(_cluster_00000, 4)
		ids = append(ids, c.allocID())
	}
	trace_util_0.Count(_cluster_00000, 3)
	return ids
}

func (c *Cluster) allocID() uint64 {
	trace_util_0.Count(_cluster_00000, 5)
	c.id++
	return c.id
}

// GetAllRegions gets all the regions in the cluster.
func (c *Cluster) GetAllRegions() []*Region {
	trace_util_0.Count(_cluster_00000, 6)
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		trace_util_0.Count(_cluster_00000, 8)
		regions = append(regions, region)
	}
	trace_util_0.Count(_cluster_00000, 7)
	return regions
}

// GetStore returns a Store's meta.
func (c *Cluster) GetStore(storeID uint64) *metapb.Store {
	trace_util_0.Count(_cluster_00000, 9)
	c.RLock()
	defer c.RUnlock()

	if store := c.stores[storeID]; store != nil {
		trace_util_0.Count(_cluster_00000, 11)
		return proto.Clone(store.meta).(*metapb.Store)
	}
	trace_util_0.Count(_cluster_00000, 10)
	return nil
}

// GetAllStores returns all Stores' meta.
func (c *Cluster) GetAllStores() []*metapb.Store {
	trace_util_0.Count(_cluster_00000, 12)
	c.RLock()
	defer c.RUnlock()

	stores := make([]*metapb.Store, 0, len(c.stores))
	for _, store := range c.stores {
		trace_util_0.Count(_cluster_00000, 14)
		stores = append(stores, proto.Clone(store.meta).(*metapb.Store))
	}
	trace_util_0.Count(_cluster_00000, 13)
	return stores
}

// StopStore stops a store with storeID.
func (c *Cluster) StopStore(storeID uint64) {
	trace_util_0.Count(_cluster_00000, 15)
	c.Lock()
	defer c.Unlock()

	if store := c.stores[storeID]; store != nil {
		trace_util_0.Count(_cluster_00000, 16)
		store.meta.State = metapb.StoreState_Offline
	}
}

// StartStore starts a store with storeID.
func (c *Cluster) StartStore(storeID uint64) {
	trace_util_0.Count(_cluster_00000, 17)
	c.Lock()
	defer c.Unlock()

	if store := c.stores[storeID]; store != nil {
		trace_util_0.Count(_cluster_00000, 18)
		store.meta.State = metapb.StoreState_Up
	}
}

// CancelStore makes the store with cancel state true.
func (c *Cluster) CancelStore(storeID uint64) {
	trace_util_0.Count(_cluster_00000, 19)
	c.Lock()
	defer c.Unlock()

	//A store returns context.Cancelled Error when cancel is true.
	if store := c.stores[storeID]; store != nil {
		trace_util_0.Count(_cluster_00000, 20)
		store.cancel = true
	}
}

// UnCancelStore makes the store with cancel state false.
func (c *Cluster) UnCancelStore(storeID uint64) {
	trace_util_0.Count(_cluster_00000, 21)
	c.Lock()
	defer c.Unlock()

	if store := c.stores[storeID]; store != nil {
		trace_util_0.Count(_cluster_00000, 22)
		store.cancel = false
	}
}

// GetStoreByAddr returns a Store's meta by an addr.
func (c *Cluster) GetStoreByAddr(addr string) *metapb.Store {
	trace_util_0.Count(_cluster_00000, 23)
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		trace_util_0.Count(_cluster_00000, 25)
		if s.meta.GetAddress() == addr {
			trace_util_0.Count(_cluster_00000, 26)
			return proto.Clone(s.meta).(*metapb.Store)
		}
	}
	trace_util_0.Count(_cluster_00000, 24)
	return nil
}

// GetAndCheckStoreByAddr checks and returns a Store's meta by an addr
func (c *Cluster) GetAndCheckStoreByAddr(addr string) (*metapb.Store, error) {
	trace_util_0.Count(_cluster_00000, 27)
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		trace_util_0.Count(_cluster_00000, 29)
		if s.cancel {
			trace_util_0.Count(_cluster_00000, 31)
			return nil, context.Canceled
		}
		trace_util_0.Count(_cluster_00000, 30)
		if s.meta.GetAddress() == addr {
			trace_util_0.Count(_cluster_00000, 32)
			return proto.Clone(s.meta).(*metapb.Store), nil
		}
	}
	trace_util_0.Count(_cluster_00000, 28)
	return nil, nil
}

// AddStore add a new Store to the cluster.
func (c *Cluster) AddStore(storeID uint64, addr string) {
	trace_util_0.Count(_cluster_00000, 33)
	c.Lock()
	defer c.Unlock()

	c.stores[storeID] = newStore(storeID, addr)
}

// RemoveStore removes a Store from the cluster.
func (c *Cluster) RemoveStore(storeID uint64) {
	trace_util_0.Count(_cluster_00000, 34)
	c.Lock()
	defer c.Unlock()

	delete(c.stores, storeID)
}

// UpdateStoreAddr updates store address for cluster.
func (c *Cluster) UpdateStoreAddr(storeID uint64, addr string) {
	trace_util_0.Count(_cluster_00000, 35)
	c.Lock()
	defer c.Unlock()
	c.stores[storeID] = newStore(storeID, addr)
}

// GetRegion returns a Region's meta and leader ID.
func (c *Cluster) GetRegion(regionID uint64) (*metapb.Region, uint64) {
	trace_util_0.Count(_cluster_00000, 36)
	c.RLock()
	defer c.RUnlock()

	r := c.regions[regionID]
	if r == nil {
		trace_util_0.Count(_cluster_00000, 38)
		return nil, 0
	}
	trace_util_0.Count(_cluster_00000, 37)
	return proto.Clone(r.Meta).(*metapb.Region), r.leader
}

// GetRegionByKey returns the Region and its leader whose range contains the key.
func (c *Cluster) GetRegionByKey(key []byte) (*metapb.Region, *metapb.Peer) {
	trace_util_0.Count(_cluster_00000, 39)
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		trace_util_0.Count(_cluster_00000, 41)
		if regionContains(r.Meta.StartKey, r.Meta.EndKey, key) {
			trace_util_0.Count(_cluster_00000, 42)
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	trace_util_0.Count(_cluster_00000, 40)
	return nil, nil
}

// GetPrevRegionByKey returns the previous Region and its leader whose range contains the key.
func (c *Cluster) GetPrevRegionByKey(key []byte) (*metapb.Region, *metapb.Peer) {
	trace_util_0.Count(_cluster_00000, 43)
	c.RLock()
	defer c.RUnlock()

	currentRegion, _ := c.GetRegionByKey(key)
	if len(currentRegion.StartKey) == 0 {
		trace_util_0.Count(_cluster_00000, 46)
		return nil, nil
	}
	trace_util_0.Count(_cluster_00000, 44)
	for _, r := range c.regions {
		trace_util_0.Count(_cluster_00000, 47)
		if bytes.Equal(r.Meta.EndKey, currentRegion.StartKey) {
			trace_util_0.Count(_cluster_00000, 48)
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	trace_util_0.Count(_cluster_00000, 45)
	return nil, nil
}

// GetRegionByID returns the Region and its leader whose ID is regionID.
func (c *Cluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	trace_util_0.Count(_cluster_00000, 49)
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		trace_util_0.Count(_cluster_00000, 51)
		if r.Meta.GetId() == regionID {
			trace_util_0.Count(_cluster_00000, 52)
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	trace_util_0.Count(_cluster_00000, 50)
	return nil, nil
}

// Bootstrap creates the first Region. The Stores should be in the Cluster before
// bootstrap.
func (c *Cluster) Bootstrap(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) {
	trace_util_0.Count(_cluster_00000, 53)
	c.Lock()
	defer c.Unlock()

	if len(storeIDs) != len(peerIDs) {
		trace_util_0.Count(_cluster_00000, 55)
		panic("len(storeIDs) != len(peerIDs)")
	}
	trace_util_0.Count(_cluster_00000, 54)
	c.regions[regionID] = newRegion(regionID, storeIDs, peerIDs, leaderPeerID)
}

// AddPeer adds a new Peer for the Region on the Store.
func (c *Cluster) AddPeer(regionID, storeID, peerID uint64) {
	trace_util_0.Count(_cluster_00000, 56)
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].addPeer(peerID, storeID)
}

// RemovePeer removes the Peer from the Region. Note that if the Peer is leader,
// the Region will have no leader before calling ChangeLeader().
func (c *Cluster) RemovePeer(regionID, storeID uint64) {
	trace_util_0.Count(_cluster_00000, 57)
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].removePeer(storeID)
}

// ChangeLeader sets the Region's leader Peer. Caller should guarantee the Peer
// exists.
func (c *Cluster) ChangeLeader(regionID, leaderPeerID uint64) {
	trace_util_0.Count(_cluster_00000, 58)
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].changeLeader(leaderPeerID)
}

// GiveUpLeader sets the Region's leader to 0. The Region will have no leader
// before calling ChangeLeader().
func (c *Cluster) GiveUpLeader(regionID uint64) {
	trace_util_0.Count(_cluster_00000, 59)
	c.ChangeLeader(regionID, 0)
}

// Split splits a Region at the key (encoded) and creates new Region.
func (c *Cluster) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	trace_util_0.Count(_cluster_00000, 60)
	c.SplitRaw(regionID, newRegionID, NewMvccKey(key), peerIDs, leaderPeerID)
}

// SplitRaw splits a Region at the key (not encoded) and creates new Region.
func (c *Cluster) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) {
	trace_util_0.Count(_cluster_00000, 61)
	c.Lock()
	defer c.Unlock()

	newRegion := c.regions[regionID].split(newRegionID, rawKey, peerIDs, leaderPeerID)
	c.regions[newRegionID] = newRegion
}

// Merge merges 2 regions, their key ranges should be adjacent.
func (c *Cluster) Merge(regionID1, regionID2 uint64) {
	trace_util_0.Count(_cluster_00000, 62)
	c.Lock()
	defer c.Unlock()

	c.regions[regionID1].merge(c.regions[regionID2].Meta.GetEndKey())
	delete(c.regions, regionID2)
}

// SplitTable evenly splits the data in table into count regions.
// Only works for single store.
func (c *Cluster) SplitTable(mvccStore MVCCStore, tableID int64, count int) {
	trace_util_0.Count(_cluster_00000, 63)
	tableStart := tablecodec.GenTableRecordPrefix(tableID)
	tableEnd := tableStart.PrefixNext()
	c.splitRange(mvccStore, NewMvccKey(tableStart), NewMvccKey(tableEnd), count)
}

// SplitIndex evenly splits the data in index into count regions.
// Only works for single store.
func (c *Cluster) SplitIndex(mvccStore MVCCStore, tableID, indexID int64, count int) {
	trace_util_0.Count(_cluster_00000, 64)
	indexStart := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	indexEnd := indexStart.PrefixNext()
	c.splitRange(mvccStore, NewMvccKey(indexStart), NewMvccKey(indexEnd), count)
}

// SplitKeys evenly splits the start, end key into "count" regions.
// Only works for single store.
func (c *Cluster) SplitKeys(mvccStore MVCCStore, start, end kv.Key, count int) {
	trace_util_0.Count(_cluster_00000, 65)
	c.splitRange(mvccStore, NewMvccKey(start), NewMvccKey(end), count)
}

// ScheduleDelay schedules a delay event for a transaction on a region.
func (c *Cluster) ScheduleDelay(startTS, regionID uint64, dur time.Duration) {
	trace_util_0.Count(_cluster_00000, 66)
	c.delayMu.Lock()
	c.delayEvents[delayKey{startTS: startTS, regionID: regionID}] = dur
	c.delayMu.Unlock()
}

func (c *Cluster) handleDelay(startTS, regionID uint64) {
	trace_util_0.Count(_cluster_00000, 67)
	key := delayKey{startTS: startTS, regionID: regionID}
	c.delayMu.Lock()
	dur, ok := c.delayEvents[key]
	if ok {
		trace_util_0.Count(_cluster_00000, 69)
		delete(c.delayEvents, key)
	}
	trace_util_0.Count(_cluster_00000, 68)
	c.delayMu.Unlock()
	if ok {
		trace_util_0.Count(_cluster_00000, 70)
		time.Sleep(dur)
	}
}

func (c *Cluster) splitRange(mvccStore MVCCStore, start, end MvccKey, count int) {
	trace_util_0.Count(_cluster_00000, 71)
	c.Lock()
	defer c.Unlock()
	c.evacuateOldRegionRanges(start, end)
	regionPairs := c.getEntriesGroupByRegions(mvccStore, start, end, count)
	c.createNewRegions(regionPairs, start, end)
}

// getEntriesGroupByRegions groups the key value pairs into splitted regions.
func (c *Cluster) getEntriesGroupByRegions(mvccStore MVCCStore, start, end MvccKey, count int) [][]Pair {
	trace_util_0.Count(_cluster_00000, 72)
	startTS := uint64(math.MaxUint64)
	limit := int(math.MaxInt32)
	pairs := mvccStore.Scan(start.Raw(), end.Raw(), limit, startTS, kvrpcpb.IsolationLevel_SI)
	regionEntriesSlice := make([][]Pair, 0, count)
	quotient := len(pairs) / count
	remainder := len(pairs) % count
	i := 0
	for i < len(pairs) {
		trace_util_0.Count(_cluster_00000, 74)
		regionEntryCount := quotient
		if remainder > 0 {
			trace_util_0.Count(_cluster_00000, 76)
			remainder--
			regionEntryCount++
		}
		trace_util_0.Count(_cluster_00000, 75)
		regionEntries := pairs[i : i+regionEntryCount]
		regionEntriesSlice = append(regionEntriesSlice, regionEntries)
		i += regionEntryCount
	}
	trace_util_0.Count(_cluster_00000, 73)
	return regionEntriesSlice
}

func (c *Cluster) createNewRegions(regionPairs [][]Pair, start, end MvccKey) {
	trace_util_0.Count(_cluster_00000, 77)
	for i := range regionPairs {
		trace_util_0.Count(_cluster_00000, 78)
		peerID := c.allocID()
		newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
		var regionStartKey, regionEndKey MvccKey
		if i == 0 {
			trace_util_0.Count(_cluster_00000, 81)
			regionStartKey = start
		} else {
			trace_util_0.Count(_cluster_00000, 82)
			{
				regionStartKey = NewMvccKey(regionPairs[i][0].Key)
			}
		}
		trace_util_0.Count(_cluster_00000, 79)
		if i == len(regionPairs)-1 {
			trace_util_0.Count(_cluster_00000, 83)
			regionEndKey = end
		} else {
			trace_util_0.Count(_cluster_00000, 84)
			{
				// Use the next region's first key as region end key.
				regionEndKey = NewMvccKey(regionPairs[i+1][0].Key)
			}
		}
		trace_util_0.Count(_cluster_00000, 80)
		newRegion.updateKeyRange(regionStartKey, regionEndKey)
		c.regions[newRegion.Meta.Id] = newRegion
	}
}

// evacuateOldRegionRanges evacuate the range [start, end].
// Old regions has intersection with [start, end) will be updated or deleted.
func (c *Cluster) evacuateOldRegionRanges(start, end MvccKey) {
	trace_util_0.Count(_cluster_00000, 85)
	oldRegions := c.getRegionsCoverRange(start, end)
	for _, oldRegion := range oldRegions {
		trace_util_0.Count(_cluster_00000, 86)
		startCmp := bytes.Compare(oldRegion.Meta.StartKey, start)
		endCmp := bytes.Compare(oldRegion.Meta.EndKey, end)
		if len(oldRegion.Meta.EndKey) == 0 {
			trace_util_0.Count(_cluster_00000, 88)
			endCmp = 1
		}
		trace_util_0.Count(_cluster_00000, 87)
		if startCmp >= 0 && endCmp <= 0 {
			trace_util_0.Count(_cluster_00000, 89)
			// The region is within table data, it will be replaced by new regions.
			delete(c.regions, oldRegion.Meta.Id)
		} else {
			trace_util_0.Count(_cluster_00000, 90)
			if startCmp < 0 && endCmp > 0 {
				trace_util_0.Count(_cluster_00000, 91)
				// A single Region covers table data, split into two regions that do not overlap table data.
				oldEnd := oldRegion.Meta.EndKey
				oldRegion.updateKeyRange(oldRegion.Meta.StartKey, start)
				peerID := c.allocID()
				newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
				newRegion.updateKeyRange(end, oldEnd)
				c.regions[newRegion.Meta.Id] = newRegion
			} else {
				trace_util_0.Count(_cluster_00000, 92)
				if startCmp < 0 {
					trace_util_0.Count(_cluster_00000, 93)
					oldRegion.updateKeyRange(oldRegion.Meta.StartKey, start)
				} else {
					trace_util_0.Count(_cluster_00000, 94)
					{
						oldRegion.updateKeyRange(end, oldRegion.Meta.EndKey)
					}
				}
			}
		}
	}
}

func (c *Cluster) firstStoreID() uint64 {
	trace_util_0.Count(_cluster_00000, 95)
	for id := range c.stores {
		trace_util_0.Count(_cluster_00000, 97)
		return id
	}
	trace_util_0.Count(_cluster_00000, 96)
	return 0
}

// getRegionsCoverRange gets regions in the cluster that has intersection with [start, end).
func (c *Cluster) getRegionsCoverRange(start, end MvccKey) []*Region {
	trace_util_0.Count(_cluster_00000, 98)
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		trace_util_0.Count(_cluster_00000, 100)
		onRight := bytes.Compare(end, region.Meta.StartKey) <= 0
		onLeft := bytes.Compare(region.Meta.EndKey, start) <= 0
		if len(region.Meta.EndKey) == 0 {
			trace_util_0.Count(_cluster_00000, 103)
			onLeft = false
		}
		trace_util_0.Count(_cluster_00000, 101)
		if onLeft || onRight {
			trace_util_0.Count(_cluster_00000, 104)
			continue
		}
		trace_util_0.Count(_cluster_00000, 102)
		regions = append(regions, region)
	}
	trace_util_0.Count(_cluster_00000, 99)
	return regions
}

// Region is the Region meta data.
type Region struct {
	Meta   *metapb.Region
	leader uint64
}

func newPeerMeta(peerID, storeID uint64) *metapb.Peer {
	trace_util_0.Count(_cluster_00000, 105)
	return &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
}

func newRegion(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) *Region {
	trace_util_0.Count(_cluster_00000, 106)
	if len(storeIDs) != len(peerIDs) {
		trace_util_0.Count(_cluster_00000, 109)
		panic("len(storeIDs) != len(peerIds)")
	}
	trace_util_0.Count(_cluster_00000, 107)
	peers := make([]*metapb.Peer, 0, len(storeIDs))
	for i := range storeIDs {
		trace_util_0.Count(_cluster_00000, 110)
		peers = append(peers, newPeerMeta(peerIDs[i], storeIDs[i]))
	}
	trace_util_0.Count(_cluster_00000, 108)
	meta := &metapb.Region{
		Id:    regionID,
		Peers: peers,
	}
	return &Region{
		Meta:   meta,
		leader: leaderPeerID,
	}
}

func (r *Region) addPeer(peerID, storeID uint64) {
	trace_util_0.Count(_cluster_00000, 111)
	r.Meta.Peers = append(r.Meta.Peers, newPeerMeta(peerID, storeID))
	r.incConfVer()
}

func (r *Region) removePeer(peerID uint64) {
	trace_util_0.Count(_cluster_00000, 112)
	for i, peer := range r.Meta.Peers {
		trace_util_0.Count(_cluster_00000, 115)
		if peer.GetId() == peerID {
			trace_util_0.Count(_cluster_00000, 116)
			r.Meta.Peers = append(r.Meta.Peers[:i], r.Meta.Peers[i+1:]...)
			break
		}
	}
	trace_util_0.Count(_cluster_00000, 113)
	if r.leader == peerID {
		trace_util_0.Count(_cluster_00000, 117)
		r.leader = 0
	}
	trace_util_0.Count(_cluster_00000, 114)
	r.incConfVer()
}

func (r *Region) changeLeader(leaderID uint64) {
	trace_util_0.Count(_cluster_00000, 118)
	r.leader = leaderID
}

func (r *Region) leaderPeer() *metapb.Peer {
	trace_util_0.Count(_cluster_00000, 119)
	for _, p := range r.Meta.Peers {
		trace_util_0.Count(_cluster_00000, 121)
		if p.GetId() == r.leader {
			trace_util_0.Count(_cluster_00000, 122)
			return p
		}
	}
	trace_util_0.Count(_cluster_00000, 120)
	return nil
}

func (r *Region) split(newRegionID uint64, key MvccKey, peerIDs []uint64, leaderPeerID uint64) *Region {
	trace_util_0.Count(_cluster_00000, 123)
	if len(r.Meta.Peers) != len(peerIDs) {
		trace_util_0.Count(_cluster_00000, 126)
		panic("len(r.meta.Peers) != len(peerIDs)")
	}
	trace_util_0.Count(_cluster_00000, 124)
	storeIDs := make([]uint64, 0, len(r.Meta.Peers))
	for _, peer := range r.Meta.Peers {
		trace_util_0.Count(_cluster_00000, 127)
		storeIDs = append(storeIDs, peer.GetStoreId())
	}
	trace_util_0.Count(_cluster_00000, 125)
	region := newRegion(newRegionID, storeIDs, peerIDs, leaderPeerID)
	region.updateKeyRange(key, r.Meta.EndKey)
	r.updateKeyRange(r.Meta.StartKey, key)
	return region
}

func (r *Region) merge(endKey MvccKey) {
	trace_util_0.Count(_cluster_00000, 128)
	r.Meta.EndKey = endKey
	r.incVersion()
}

func (r *Region) updateKeyRange(start, end MvccKey) {
	trace_util_0.Count(_cluster_00000, 129)
	r.Meta.StartKey = start
	r.Meta.EndKey = end
	r.incVersion()
}

func (r *Region) incConfVer() {
	trace_util_0.Count(_cluster_00000, 130)
	r.Meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer() + 1,
		Version: r.Meta.GetRegionEpoch().GetVersion(),
	}
}

func (r *Region) incVersion() {
	trace_util_0.Count(_cluster_00000, 131)
	r.Meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer(),
		Version: r.Meta.GetRegionEpoch().GetVersion() + 1,
	}
}

// Store is the Store's meta data.
type Store struct {
	meta   *metapb.Store
	cancel bool // return context.Cancelled error when cancel is true.
}

func newStore(storeID uint64, addr string) *Store {
	trace_util_0.Count(_cluster_00000, 132)
	return &Store{
		meta: &metapb.Store{
			Id:      storeID,
			Address: addr,
		},
	}
}

var _cluster_00000 = "store/mockstore/mocktikv/cluster.go"
