// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

const (
	protocol = "http://"
)

// Helper is a middleware to get some information from tikv/pd. It can be used for TiDB's http api or mem table.
type Helper struct {
	Store       tikv.Storage
	RegionCache *tikv.RegionCache
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	trace_util_0.Count(_helper_00000, 0)
	keyLocation, err := h.RegionCache.LocateKey(tikv.NewBackoffer(context.Background(), 500), encodedKey)
	if err != nil {
		trace_util_0.Count(_helper_00000, 3)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_helper_00000, 1)
	tikvReq := &tikvrpc.Request{
		Type: tikvrpc.CmdMvccGetByKey,
		MvccGetByKey: &kvrpcpb.MvccGetByKeyRequest{
			Key: encodedKey,
		},
	}
	kvResp, err := h.Store.SendReq(tikv.NewBackoffer(context.Background(), 500), tikvReq, keyLocation.Region, time.Minute)
	if err != nil {
		trace_util_0.Count(_helper_00000, 4)
		logutil.Logger(context.Background()).Info("get MVCC by encoded key failed",
			zap.Binary("encodeKey", encodedKey),
			zap.Reflect("region", keyLocation.Region),
			zap.Binary("startKey", keyLocation.StartKey),
			zap.Binary("endKey", keyLocation.EndKey),
			zap.Reflect("kvResp", kvResp),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 2)
	return kvResp.MvccGetByKey, nil
}

// StoreHotRegionInfos records all hog region stores.
// it's the response of PD.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*HotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*HotRegionsStat `json:"as_leader"`
}

// HotRegionsStat records echo store's hot region.
// it's the response of PD.
type HotRegionsStat struct {
	RegionsStat []RegionStat `json:"statistics"`
}

// RegionStat records each hot region's statistics
// it's the response of PD.
type RegionStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	HotDegree int    `json:"hot_degree"`
}

// RegionMetric presents the final metric output entry.
type RegionMetric struct {
	FlowBytes    uint64 `json:"flow_bytes"`
	MaxHotDegree int    `json:"max_hot_degree"`
	Count        int    `json:"region_count"`
}

// ScrapeHotInfo gets the needed hot region information by the url given.
func (h *Helper) ScrapeHotInfo(rw string, allSchemas []*model.DBInfo) (map[TblIndex]RegionMetric, error) {
	trace_util_0.Count(_helper_00000, 5)
	regionMetrics, err := h.FetchHotRegion(rw)
	if err != nil {
		trace_util_0.Count(_helper_00000, 7)
		return nil, err
	}
	trace_util_0.Count(_helper_00000, 6)
	return h.FetchRegionTableIndex(regionMetrics, allSchemas)
}

// FetchHotRegion fetches the hot region information from PD's http api.
func (h *Helper) FetchHotRegion(rw string) (map[uint64]RegionMetric, error) {
	trace_util_0.Count(_helper_00000, 8)
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		trace_util_0.Count(_helper_00000, 16)
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	trace_util_0.Count(_helper_00000, 9)
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		trace_util_0.Count(_helper_00000, 17)
		return nil, errors.New("pd unavailable")
	}
	trace_util_0.Count(_helper_00000, 10)
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+rw, nil)
	if err != nil {
		trace_util_0.Count(_helper_00000, 18)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 11)
	timeout, cancelFunc := context.WithTimeout(context.Background(), 50*time.Millisecond)
	resp, err := http.DefaultClient.Do(req.WithContext(timeout))
	cancelFunc()
	if err != nil {
		trace_util_0.Count(_helper_00000, 19)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 12)
	defer func() {
		trace_util_0.Count(_helper_00000, 20)
		err = resp.Body.Close()
		if err != nil {
			trace_util_0.Count(_helper_00000, 21)
			logutil.Logger(context.Background()).Error("close body failed", zap.Error(err))
		}
	}()
	trace_util_0.Count(_helper_00000, 13)
	var regionResp StoreHotRegionInfos
	err = json.NewDecoder(resp.Body).Decode(&regionResp)
	if err != nil {
		trace_util_0.Count(_helper_00000, 22)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 14)
	metric := make(map[uint64]RegionMetric)
	for _, hotRegions := range regionResp.AsLeader {
		trace_util_0.Count(_helper_00000, 23)
		for _, region := range hotRegions.RegionsStat {
			trace_util_0.Count(_helper_00000, 24)
			metric[region.RegionID] = RegionMetric{FlowBytes: region.FlowBytes, MaxHotDegree: region.HotDegree}
		}
	}
	trace_util_0.Count(_helper_00000, 15)
	return metric, nil
}

// TblIndex stores the things to index one table.
type TblIndex struct {
	DbName    string
	TableName string
	TableID   int64
	IndexName string
	IndexID   int64
}

// FrameItem includes a index's or record's meta data with table's info.
type FrameItem struct {
	DBName      string   `json:"db_name"`
	TableName   string   `json:"table_name"`
	TableID     int64    `json:"table_id"`
	IsRecord    bool     `json:"is_record"`
	RecordID    int64    `json:"record_id,omitempty"`
	IndexName   string   `json:"index_name,omitempty"`
	IndexID     int64    `json:"index_id,omitempty"`
	IndexValues []string `json:"index_values,omitempty"`
}

// RegionFrameRange contains a frame range info which the region covered.
type RegionFrameRange struct {
	First  *FrameItem        // start frame of the region
	Last   *FrameItem        // end frame of the region
	region *tikv.KeyLocation // the region
}

// FetchRegionTableIndex constructs a map that maps a table to its hot region information by the given raw hot region metrics.
func (h *Helper) FetchRegionTableIndex(metrics map[uint64]RegionMetric, allSchemas []*model.DBInfo) (map[TblIndex]RegionMetric, error) {
	trace_util_0.Count(_helper_00000, 25)
	idxMetrics := make(map[TblIndex]RegionMetric)
	for regionID, regionMetric := range metrics {
		trace_util_0.Count(_helper_00000, 27)
		region, err := h.RegionCache.LocateRegionByID(tikv.NewBackoffer(context.Background(), 500), regionID)
		if err != nil {
			trace_util_0.Count(_helper_00000, 30)
			logutil.Logger(context.Background()).Error("locate region failed", zap.Error(err))
			continue
		}

		trace_util_0.Count(_helper_00000, 28)
		hotRange, err := NewRegionFrameRange(region)
		if err != nil {
			trace_util_0.Count(_helper_00000, 31)
			return nil, err
		}

		trace_util_0.Count(_helper_00000, 29)
		f := h.FindTableIndexOfRegion(allSchemas, hotRange)
		if f != nil {
			trace_util_0.Count(_helper_00000, 32)
			idx := TblIndex{
				DbName:    f.DBName,
				TableName: f.TableName,
				TableID:   f.TableID,
				IndexName: f.IndexName,
				IndexID:   f.IndexID,
			}
			metric, exists := idxMetrics[idx]
			if !exists {
				trace_util_0.Count(_helper_00000, 33)
				metric = regionMetric
				metric.Count++
				idxMetrics[idx] = metric
			} else {
				trace_util_0.Count(_helper_00000, 34)
				{
					metric.FlowBytes += regionMetric.FlowBytes
					if metric.MaxHotDegree < regionMetric.MaxHotDegree {
						trace_util_0.Count(_helper_00000, 36)
						metric.MaxHotDegree = regionMetric.MaxHotDegree
					}
					trace_util_0.Count(_helper_00000, 35)
					metric.Count++
				}
			}
		}
	}

	trace_util_0.Count(_helper_00000, 26)
	return idxMetrics, nil
}

// FindTableIndexOfRegion finds what table is involved in this hot region. And constructs the new frame item for future use.
func (h *Helper) FindTableIndexOfRegion(allSchemas []*model.DBInfo, hotRange *RegionFrameRange) *FrameItem {
	trace_util_0.Count(_helper_00000, 37)
	for _, db := range allSchemas {
		trace_util_0.Count(_helper_00000, 39)
		for _, tbl := range db.Tables {
			trace_util_0.Count(_helper_00000, 40)
			if f := hotRange.GetRecordFrame(tbl.ID, db.Name.O, tbl.Name.O); f != nil {
				trace_util_0.Count(_helper_00000, 42)
				return f
			}
			trace_util_0.Count(_helper_00000, 41)
			for _, idx := range tbl.Indices {
				trace_util_0.Count(_helper_00000, 43)
				if f := hotRange.GetIndexFrame(tbl.ID, idx.ID, db.Name.O, tbl.Name.O, idx.Name.O); f != nil {
					trace_util_0.Count(_helper_00000, 44)
					return f
				}
			}
		}
	}
	trace_util_0.Count(_helper_00000, 38)
	return nil
}

// NewRegionFrameRange init a NewRegionFrameRange with region info.
func NewRegionFrameRange(region *tikv.KeyLocation) (idxRange *RegionFrameRange, err error) {
	trace_util_0.Count(_helper_00000, 45)
	var first, last *FrameItem
	// check and init first frame
	if len(region.StartKey) > 0 {
		trace_util_0.Count(_helper_00000, 48)
		first, err = NewFrameItemFromRegionKey(region.StartKey)
		if err != nil {
			trace_util_0.Count(_helper_00000, 49)
			return
		}
	} else {
		trace_util_0.Count(_helper_00000, 50)
		{ // empty startKey means start with -infinite
			first = &FrameItem{
				IndexID:  int64(math.MinInt64),
				IsRecord: false,
				TableID:  int64(math.MinInt64),
			}
		}
	}

	// check and init last frame
	trace_util_0.Count(_helper_00000, 46)
	if len(region.EndKey) > 0 {
		trace_util_0.Count(_helper_00000, 51)
		last, err = NewFrameItemFromRegionKey(region.EndKey)
		if err != nil {
			trace_util_0.Count(_helper_00000, 52)
			return
		}
	} else {
		trace_util_0.Count(_helper_00000, 53)
		{ // empty endKey means end with +infinite
			last = &FrameItem{
				TableID:  int64(math.MaxInt64),
				IndexID:  int64(math.MaxInt64),
				IsRecord: true,
			}
		}
	}

	trace_util_0.Count(_helper_00000, 47)
	idxRange = &RegionFrameRange{
		region: region,
		First:  first,
		Last:   last,
	}
	return idxRange, nil
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	trace_util_0.Count(_helper_00000, 54)
	frame = &FrameItem{}
	frame.TableID, frame.IndexID, frame.IsRecord, err = tablecodec.DecodeKeyHead(key)
	if err == nil {
		trace_util_0.Count(_helper_00000, 58)
		if frame.IsRecord {
			trace_util_0.Count(_helper_00000, 60)
			_, frame.RecordID, err = tablecodec.DecodeRecordKey(key)
		} else {
			trace_util_0.Count(_helper_00000, 61)
			{
				_, _, frame.IndexValues, err = tablecodec.DecodeIndexKey(key)
			}
		}
		trace_util_0.Count(_helper_00000, 59)
		logutil.Logger(context.Background()).Warn("decode region key failed", zap.ByteString("key", key), zap.Error(err))
		// Ignore decode errors.
		err = nil
		return
	}
	trace_util_0.Count(_helper_00000, 55)
	if bytes.HasPrefix(key, tablecodec.TablePrefix()) {
		trace_util_0.Count(_helper_00000, 62)
		// If SplitTable is enabled, the key may be `t{id}`.
		if len(key) == tablecodec.TableSplitKeyLen {
			trace_util_0.Count(_helper_00000, 64)
			frame.TableID = tablecodec.DecodeTableID(key)
			return frame, nil
		}
		trace_util_0.Count(_helper_00000, 63)
		return nil, errors.Trace(err)
	}

	// key start with tablePrefix must be either record key or index key
	// That's means table's record key and index key are always together
	// in the continuous interval. And for key with prefix smaller than
	// tablePrefix, is smaller than all tables. While for key with prefix
	// bigger than tablePrefix, means is bigger than all tables.
	trace_util_0.Count(_helper_00000, 56)
	err = nil
	if bytes.Compare(key, tablecodec.TablePrefix()) < 0 {
		trace_util_0.Count(_helper_00000, 65)
		frame.TableID = math.MinInt64
		frame.IndexID = math.MinInt64
		frame.IsRecord = false
		return
	}
	// bigger than tablePrefix, means is bigger than all tables.
	trace_util_0.Count(_helper_00000, 57)
	frame.TableID = math.MaxInt64
	frame.TableID = math.MaxInt64
	frame.IsRecord = true
	return
}

// GetRecordFrame returns the record frame of a table. If the table's records
// are not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetRecordFrame(tableID int64, dbName, tableName string) *FrameItem {
	trace_util_0.Count(_helper_00000, 66)
	if tableID == r.First.TableID && r.First.IsRecord {
		trace_util_0.Count(_helper_00000, 70)
		r.First.DBName, r.First.TableName = dbName, tableName
		return r.First
	}
	trace_util_0.Count(_helper_00000, 67)
	if tableID == r.Last.TableID && r.Last.IsRecord {
		trace_util_0.Count(_helper_00000, 71)
		r.Last.DBName, r.Last.TableName = dbName, tableName
		return r.Last
	}

	trace_util_0.Count(_helper_00000, 68)
	if tableID >= r.First.TableID && tableID < r.Last.TableID {
		trace_util_0.Count(_helper_00000, 72)
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  true,
		}
	}
	trace_util_0.Count(_helper_00000, 69)
	return nil
}

// GetIndexFrame returns the indnex frame of a table. If the table's indices are
// not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetIndexFrame(tableID, indexID int64, dbName, tableName, indexName string) *FrameItem {
	trace_util_0.Count(_helper_00000, 73)
	if tableID == r.First.TableID && !r.First.IsRecord && indexID == r.First.IndexID {
		trace_util_0.Count(_helper_00000, 77)
		r.First.DBName, r.First.TableName, r.First.IndexName = dbName, tableName, indexName
		return r.First
	}
	trace_util_0.Count(_helper_00000, 74)
	if tableID == r.Last.TableID && indexID == r.Last.IndexID {
		trace_util_0.Count(_helper_00000, 78)
		r.Last.DBName, r.Last.TableName, r.Last.IndexName = dbName, tableName, indexName
		return r.Last
	}

	trace_util_0.Count(_helper_00000, 75)
	greaterThanFirst := tableID > r.First.TableID || (tableID == r.First.TableID && !r.First.IsRecord && indexID > r.First.IndexID)
	lessThanLast := tableID < r.Last.TableID || (tableID == r.Last.TableID && (r.Last.IsRecord || indexID < r.Last.IndexID))
	if greaterThanFirst && lessThanLast {
		trace_util_0.Count(_helper_00000, 79)
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  false,
			IndexName: indexName,
			IndexID:   indexID,
		}
	}
	trace_util_0.Count(_helper_00000, 76)
	return nil
}

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	RegionPeer
	DownSec int64 `json:"down_seconds"`
}

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ID              int64            `json:"id"`
	StartKey        string           `json:"start_key"`
	EndKey          string           `json:"end_key"`
	Epoch           RegionEpoch      `json:"epoch"`
	Peers           []RegionPeer     `json:"peers"`
	Leader          RegionPeer       `json:"leader"`
	DownPeers       []RegionPeerStat `json:"down_peers"`
	PendingPeers    []RegionPeer     `json:"pending_peers"`
	WrittenBytes    int64            `json:"written_bytes"`
	ReadBytes       int64            `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// GetRegionsInfo gets the region information of current store by using PD's api.
func (h *Helper) GetRegionsInfo() (*RegionsInfo, error) {
	trace_util_0.Count(_helper_00000, 80)
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		trace_util_0.Count(_helper_00000, 87)
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	trace_util_0.Count(_helper_00000, 81)
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		trace_util_0.Count(_helper_00000, 88)
		return nil, errors.New("pd unavailable")
	}
	trace_util_0.Count(_helper_00000, 82)
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+pdapi.Regions, nil)
	if err != nil {
		trace_util_0.Count(_helper_00000, 89)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 83)
	timeout, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := http.DefaultClient.Do(req.WithContext(timeout))
	defer cancelFunc()
	if err != nil {
		trace_util_0.Count(_helper_00000, 90)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 84)
	defer func() {
		trace_util_0.Count(_helper_00000, 91)
		err = resp.Body.Close()
		if err != nil {
			trace_util_0.Count(_helper_00000, 92)
			logutil.Logger(context.Background()).Error("close body failed", zap.Error(err))
		}
	}()
	trace_util_0.Count(_helper_00000, 85)
	var regionsInfo RegionsInfo
	err = json.NewDecoder(resp.Body).Decode(&regionsInfo)
	if err != nil {
		trace_util_0.Count(_helper_00000, 93)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 86)
	return &regionsInfo, nil
}

// StoresStat stores all information get from PD's api.
type StoresStat struct {
	Count  int         `json:"count"`
	Stores []StoreStat `json:"stores"`
}

// StoreStat stores information of one store.
type StoreStat struct {
	Store  StoreBaseStat   `json:"store"`
	Status StoreDetailStat `json:"status"`
}

// StoreBaseStat stores the basic information of one store.
type StoreBaseStat struct {
	ID        int64        `json:"id"`
	Address   string       `json:"address"`
	State     int64        `json:"state"`
	StateName string       `json:"state_name"`
	Version   string       `json:"version"`
	Labels    []StoreLabel `json:"labels"`
}

// StoreLabel stores the information of one store label.
type StoreLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreDetailStat stores the detail information of one store.
type StoreDetailStat struct {
	Capacity        string    `json:"capacity"`
	Available       string    `json:"available"`
	LeaderCount     int64     `json:"leader_count"`
	LeaderWeight    int64     `json:"leader_weight"`
	LeaderScore     int64     `json:"leader_score"`
	LeaderSize      int64     `json:"leader_size"`
	RegionCount     int64     `json:"region_count"`
	RegionWeight    int64     `json:"region_weight"`
	RegionScore     int64     `json:"region_score"`
	RegionSize      int64     `json:"region_size"`
	StartTs         time.Time `json:"start_ts"`
	LastHeartbeatTs time.Time `json:"last_heartbeat_ts"`
	Uptime          string    `json:"uptime"`
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (h *Helper) GetStoresStat() (*StoresStat, error) {
	trace_util_0.Count(_helper_00000, 94)
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		trace_util_0.Count(_helper_00000, 101)
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	trace_util_0.Count(_helper_00000, 95)
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		trace_util_0.Count(_helper_00000, 102)
		return nil, errors.New("pd unavailable")
	}
	trace_util_0.Count(_helper_00000, 96)
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+pdapi.Stores, nil)
	if err != nil {
		trace_util_0.Count(_helper_00000, 103)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 97)
	timeout, cancelFunc := context.WithTimeout(context.Background(), 50*time.Millisecond)
	resp, err := http.DefaultClient.Do(req.WithContext(timeout))
	defer cancelFunc()
	if err != nil {
		trace_util_0.Count(_helper_00000, 104)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 98)
	defer func() {
		trace_util_0.Count(_helper_00000, 105)
		err = resp.Body.Close()
		if err != nil {
			trace_util_0.Count(_helper_00000, 106)
			logutil.Logger(context.Background()).Error("close body failed", zap.Error(err))
		}
	}()
	trace_util_0.Count(_helper_00000, 99)
	var storesStat StoresStat
	err = json.NewDecoder(resp.Body).Decode(&storesStat)
	if err != nil {
		trace_util_0.Count(_helper_00000, 107)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_helper_00000, 100)
	return &storesStat, nil
}

var _helper_00000 = "store/helper/helper.go"
