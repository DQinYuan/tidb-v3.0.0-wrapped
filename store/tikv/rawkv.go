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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

var (
	tikvRawkvCmdHistogramWithGet           = metrics.TiKVRawkvCmdHistogram.WithLabelValues("get")
	tikvRawkvCmdHistogramWithBatchGet      = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_get")
	tikvRawkvCmdHistogramWithBatchPut      = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_put")
	tikvRawkvCmdHistogramWithDelete        = metrics.TiKVRawkvCmdHistogram.WithLabelValues("delete")
	tikvRawkvCmdHistogramWithBatchDelete   = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_delete")
	tikvRawkvCmdHistogramWithRawScan       = metrics.TiKVRawkvCmdHistogram.WithLabelValues("raw_scan")
	tikvRawkvCmdHistogramWithRawReversScan = metrics.TiKVRawkvCmdHistogram.WithLabelValues("raw_reverse_scan")

	tikvRawkvSizeHistogramWithKey   = metrics.TiKVRawkvSizeHistogram.WithLabelValues("key")
	tikvRawkvSizeHistogramWithValue = metrics.TiKVRawkvSizeHistogram.WithLabelValues("value")
)

const (
	// rawBatchPutSize is the maximum size limit for rawkv each batch put request.
	rawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawkv each batch get/delete request.
	rawBatchPairCount = 512
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *RegionCache
	pdClient    pd.Client
	rpcClient   Client
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewRawKVClient(pdAddrs []string, security config.Security) (*RawKVClient, error) {
	trace_util_0.Count(_rawkv_00000, 0)
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 2)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 1)
	return &RawKVClient{
		clusterID:   pdCli.GetClusterID(context.TODO()),
		regionCache: NewRegionCache(pdCli),
		pdClient:    pdCli,
		rpcClient:   newRPCClient(security),
	}, nil
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	trace_util_0.Count(_rawkv_00000, 3)
	if c.pdClient != nil {
		trace_util_0.Count(_rawkv_00000, 7)
		c.pdClient.Close()
	}
	trace_util_0.Count(_rawkv_00000, 4)
	if c.regionCache != nil {
		trace_util_0.Count(_rawkv_00000, 8)
		c.regionCache.Close()
	}
	trace_util_0.Count(_rawkv_00000, 5)
	if c.rpcClient == nil {
		trace_util_0.Count(_rawkv_00000, 9)
		return nil
	}
	trace_util_0.Count(_rawkv_00000, 6)
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	trace_util_0.Count(_rawkv_00000, 10)
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	trace_util_0.Count(_rawkv_00000, 11)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 17)
		tikvRawkvCmdHistogramWithGet.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 12)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		RawGet: &kvrpcpb.RawGetRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 18)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 13)
	cmdResp := resp.RawGet
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 19)
		return nil, errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_rawkv_00000, 14)
	if cmdResp.GetError() != "" {
		trace_util_0.Count(_rawkv_00000, 20)
		return nil, errors.New(cmdResp.GetError())
	}
	trace_util_0.Count(_rawkv_00000, 15)
	if len(cmdResp.Value) == 0 {
		trace_util_0.Count(_rawkv_00000, 21)
		return nil, nil
	}
	trace_util_0.Count(_rawkv_00000, 16)
	return cmdResp.Value, nil
}

// BatchGet queries values with the keys.
func (c *RawKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	trace_util_0.Count(_rawkv_00000, 22)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 28)
		tikvRawkvCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 23)
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, tikvrpc.CmdRawBatchGet)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 29)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_rawkv_00000, 24)
	cmdResp := resp.RawBatchGet
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 30)
		return nil, errors.Trace(ErrBodyMissing)
	}

	trace_util_0.Count(_rawkv_00000, 25)
	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		trace_util_0.Count(_rawkv_00000, 31)
		keyToValue[string(pair.Key)] = pair.Value
	}

	trace_util_0.Count(_rawkv_00000, 26)
	values := make([][]byte, len(keys))
	for i, key := range keys {
		trace_util_0.Count(_rawkv_00000, 32)
		values[i] = keyToValue[string(key)]
	}
	trace_util_0.Count(_rawkv_00000, 27)
	return values, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	trace_util_0.Count(_rawkv_00000, 33)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 39)
		tikvRawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds())
	}()
	trace_util_0.Count(_rawkv_00000, 34)
	tikvRawkvSizeHistogramWithKey.Observe(float64(len(key)))
	tikvRawkvSizeHistogramWithValue.Observe(float64(len(value)))

	if len(value) == 0 {
		trace_util_0.Count(_rawkv_00000, 40)
		return errors.New("empty value is not supported")
	}

	trace_util_0.Count(_rawkv_00000, 35)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   key,
			Value: value,
		},
	}
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 41)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 36)
	cmdResp := resp.RawPut
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 42)
		return errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_rawkv_00000, 37)
	if cmdResp.GetError() != "" {
		trace_util_0.Count(_rawkv_00000, 43)
		return errors.New(cmdResp.GetError())
	}
	trace_util_0.Count(_rawkv_00000, 38)
	return nil
}

// BatchPut stores key-value pairs to TiKV.
func (c *RawKVClient) BatchPut(keys, values [][]byte) error {
	trace_util_0.Count(_rawkv_00000, 44)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 48)
		tikvRawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 45)
	if len(keys) != len(values) {
		trace_util_0.Count(_rawkv_00000, 49)
		return errors.New("the len of keys is not equal to the len of values")
	}
	trace_util_0.Count(_rawkv_00000, 46)
	for _, value := range values {
		trace_util_0.Count(_rawkv_00000, 50)
		if len(value) == 0 {
			trace_util_0.Count(_rawkv_00000, 51)
			return errors.New("empty value is not supported")
		}
	}
	trace_util_0.Count(_rawkv_00000, 47)
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	err := c.sendBatchPut(bo, keys, values)
	return errors.Trace(err)
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	trace_util_0.Count(_rawkv_00000, 52)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 57)
		tikvRawkvCmdHistogramWithDelete.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 53)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawDelete,
		RawDelete: &kvrpcpb.RawDeleteRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 58)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 54)
	cmdResp := resp.RawDelete
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 59)
		return errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_rawkv_00000, 55)
	if cmdResp.GetError() != "" {
		trace_util_0.Count(_rawkv_00000, 60)
		return errors.New(cmdResp.GetError())
	}
	trace_util_0.Count(_rawkv_00000, 56)
	return nil
}

// BatchDelete deletes key-value pairs from TiKV
func (c *RawKVClient) BatchDelete(keys [][]byte) error {
	trace_util_0.Count(_rawkv_00000, 61)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 66)
		tikvRawkvCmdHistogramWithBatchDelete.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 62)
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, tikvrpc.CmdRawBatchDelete)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 67)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 63)
	cmdResp := resp.RawBatchDelete
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 68)
		return errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_rawkv_00000, 64)
	if cmdResp.GetError() != "" {
		trace_util_0.Count(_rawkv_00000, 69)
		return errors.New(cmdResp.GetError())
	}
	trace_util_0.Count(_rawkv_00000, 65)
	return nil
}

// DeleteRange deletes all key-value pairs in a range from TiKV
func (c *RawKVClient) DeleteRange(startKey []byte, endKey []byte) error {
	trace_util_0.Count(_rawkv_00000, 70)
	start := time.Now()
	var err error
	defer func() {
		trace_util_0.Count(_rawkv_00000, 73)
		var label = "delete_range"
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 75)
			label += "_error"
		}
		trace_util_0.Count(_rawkv_00000, 74)
		metrics.TiKVRawkvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	trace_util_0.Count(_rawkv_00000, 71)
	for !bytes.Equal(startKey, endKey) {
		trace_util_0.Count(_rawkv_00000, 76)
		var resp *tikvrpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(startKey, endKey)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 80)
			return errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 77)
		cmdResp := resp.RawDeleteRange
		if cmdResp == nil {
			trace_util_0.Count(_rawkv_00000, 81)
			return errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_rawkv_00000, 78)
		if cmdResp.GetError() != "" {
			trace_util_0.Count(_rawkv_00000, 82)
			return errors.New(cmdResp.GetError())
		}
		trace_util_0.Count(_rawkv_00000, 79)
		startKey = actualEndKey
	}

	trace_util_0.Count(_rawkv_00000, 72)
	return nil
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, append a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(append(startKey, '\0'), append(endKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	trace_util_0.Count(_rawkv_00000, 83)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 87)
		tikvRawkvCmdHistogramWithRawScan.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 84)
	if limit > MaxRawKVScanLimit {
		trace_util_0.Count(_rawkv_00000, 88)
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	trace_util_0.Count(_rawkv_00000, 85)
	for len(keys) < limit {
		trace_util_0.Count(_rawkv_00000, 89)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: startKey,
				EndKey:   endKey,
				Limit:    uint32(limit - len(keys)),
			},
		}
		resp, loc, err := c.sendReq(startKey, req, false)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 93)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 90)
		cmdResp := resp.RawScan
		if cmdResp == nil {
			trace_util_0.Count(_rawkv_00000, 94)
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_rawkv_00000, 91)
		for _, pair := range cmdResp.Kvs {
			trace_util_0.Count(_rawkv_00000, 95)
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		trace_util_0.Count(_rawkv_00000, 92)
		startKey = loc.EndKey
		if len(startKey) == 0 {
			trace_util_0.Count(_rawkv_00000, 96)
			break
		}
	}
	trace_util_0.Count(_rawkv_00000, 86)
	return
}

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
// If endKey is empty, it means unbounded.
// If you want to include the startKey or exclude the endKey, append a '\0' to the key. For example, to scan
// (endKey, startKey], you can write:
// `ReverseScan(append(startKey, '\0'), append(endKey, '\0'), limit)`.
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (c *RawKVClient) ReverseScan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	trace_util_0.Count(_rawkv_00000, 97)
	start := time.Now()
	defer func() {
		trace_util_0.Count(_rawkv_00000, 101)
		tikvRawkvCmdHistogramWithRawReversScan.Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_rawkv_00000, 98)
	if limit > MaxRawKVScanLimit {
		trace_util_0.Count(_rawkv_00000, 102)
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	trace_util_0.Count(_rawkv_00000, 99)
	for len(keys) < limit {
		trace_util_0.Count(_rawkv_00000, 103)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: startKey,
				EndKey:   endKey,
				Limit:    uint32(limit - len(keys)),
				Reverse:  true,
			},
		}
		resp, loc, err := c.sendReq(startKey, req, true)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 107)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 104)
		cmdResp := resp.RawScan
		if cmdResp == nil {
			trace_util_0.Count(_rawkv_00000, 108)
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		trace_util_0.Count(_rawkv_00000, 105)
		for _, pair := range cmdResp.Kvs {
			trace_util_0.Count(_rawkv_00000, 109)
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		trace_util_0.Count(_rawkv_00000, 106)
		startKey = loc.StartKey
		if len(startKey) == 0 {
			trace_util_0.Count(_rawkv_00000, 110)
			break
		}
	}
	trace_util_0.Count(_rawkv_00000, 100)
	return
}

func (c *RawKVClient) sendReq(key []byte, req *tikvrpc.Request, reverse bool) (*tikvrpc.Response, *KeyLocation, error) {
	trace_util_0.Count(_rawkv_00000, 111)
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		trace_util_0.Count(_rawkv_00000, 112)
		var loc *KeyLocation
		var err error
		if reverse {
			trace_util_0.Count(_rawkv_00000, 118)
			loc, err = c.regionCache.LocateEndKey(bo, key)
		} else {
			trace_util_0.Count(_rawkv_00000, 119)
			{
				loc, err = c.regionCache.LocateKey(bo, key)
			}
		}
		trace_util_0.Count(_rawkv_00000, 113)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 120)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 114)
		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 121)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 115)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 122)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 116)
		if regionErr != nil {
			trace_util_0.Count(_rawkv_00000, 123)
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_rawkv_00000, 125)
				return nil, nil, errors.Trace(err)
			}
			trace_util_0.Count(_rawkv_00000, 124)
			continue
		}
		trace_util_0.Count(_rawkv_00000, 117)
		return resp, loc, nil
	}
}

func (c *RawKVClient) sendBatchReq(bo *Backoffer, keys [][]byte, cmdType tikvrpc.CmdType) (*tikvrpc.Response, error) {
	trace_util_0.Count(_rawkv_00000, 126) // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 132)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_rawkv_00000, 127)
	var batches []batch
	for regionID, groupKeys := range groups {
		trace_util_0.Count(_rawkv_00000, 133)
		batches = appendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	trace_util_0.Count(_rawkv_00000, 128)
	bo, cancel := bo.Fork()
	ches := make(chan singleBatchResp, len(batches))
	for _, batch := range batches {
		trace_util_0.Count(_rawkv_00000, 134)
		batch1 := batch
		go func() {
			trace_util_0.Count(_rawkv_00000, 135)
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, cmdType)
		}()
	}

	trace_util_0.Count(_rawkv_00000, 129)
	var firstError error
	var resp *tikvrpc.Response
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		trace_util_0.Count(_rawkv_00000, 136)
		resp = &tikvrpc.Response{Type: tikvrpc.CmdRawBatchGet, RawBatchGet: &kvrpcpb.RawBatchGetResponse{}}
	case tikvrpc.CmdRawBatchDelete:
		trace_util_0.Count(_rawkv_00000, 137)
		resp = &tikvrpc.Response{Type: tikvrpc.CmdRawBatchDelete, RawBatchDelete: &kvrpcpb.RawBatchDeleteResponse{}}
	}
	trace_util_0.Count(_rawkv_00000, 130)
	for i := 0; i < len(batches); i++ {
		trace_util_0.Count(_rawkv_00000, 138)
		singleResp, ok := <-ches
		if ok {
			trace_util_0.Count(_rawkv_00000, 139)
			if singleResp.err != nil {
				trace_util_0.Count(_rawkv_00000, 140)
				cancel()
				if firstError == nil {
					trace_util_0.Count(_rawkv_00000, 141)
					firstError = singleResp.err
				}
			} else {
				trace_util_0.Count(_rawkv_00000, 142)
				if cmdType == tikvrpc.CmdRawBatchGet {
					trace_util_0.Count(_rawkv_00000, 143)
					cmdResp := singleResp.resp.RawBatchGet
					resp.RawBatchGet.Pairs = append(resp.RawBatchGet.Pairs, cmdResp.Pairs...)
				}
			}
		}
	}

	trace_util_0.Count(_rawkv_00000, 131)
	return resp, firstError
}

func (c *RawKVClient) doBatchReq(bo *Backoffer, batch batch, cmdType tikvrpc.CmdType) singleBatchResp {
	trace_util_0.Count(_rawkv_00000, 144)
	var req *tikvrpc.Request
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		trace_util_0.Count(_rawkv_00000, 150)
		req = &tikvrpc.Request{
			Type: cmdType,
			RawBatchGet: &kvrpcpb.RawBatchGetRequest{
				Keys: batch.keys,
			},
		}
	case tikvrpc.CmdRawBatchDelete:
		trace_util_0.Count(_rawkv_00000, 151)
		req = &tikvrpc.Request{
			Type: cmdType,
			RawBatchDelete: &kvrpcpb.RawBatchDeleteRequest{
				Keys: batch.keys,
			},
		}
	}

	trace_util_0.Count(_rawkv_00000, 145)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)

	batchResp := singleBatchResp{}
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 152)
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	trace_util_0.Count(_rawkv_00000, 146)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 153)
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	trace_util_0.Count(_rawkv_00000, 147)
	if regionErr != nil {
		trace_util_0.Count(_rawkv_00000, 154)
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 156)
			batchResp.err = errors.Trace(err)
			return batchResp
		}
		trace_util_0.Count(_rawkv_00000, 155)
		resp, err = c.sendBatchReq(bo, batch.keys, cmdType)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	trace_util_0.Count(_rawkv_00000, 148)
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		trace_util_0.Count(_rawkv_00000, 157)
		batchResp.resp = resp
	case tikvrpc.CmdRawBatchDelete:
		trace_util_0.Count(_rawkv_00000, 158)
		cmdResp := resp.RawBatchDelete
		if cmdResp == nil {
			trace_util_0.Count(_rawkv_00000, 161)
			batchResp.err = errors.Trace(ErrBodyMissing)
			return batchResp
		}
		trace_util_0.Count(_rawkv_00000, 159)
		if cmdResp.GetError() != "" {
			trace_util_0.Count(_rawkv_00000, 162)
			batchResp.err = errors.New(cmdResp.GetError())
			return batchResp
		}
		trace_util_0.Count(_rawkv_00000, 160)
		batchResp.resp = resp
	}
	trace_util_0.Count(_rawkv_00000, 149)
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *RawKVClient) sendDeleteRangeReq(startKey []byte, endKey []byte) (*tikvrpc.Response, []byte, error) {
	trace_util_0.Count(_rawkv_00000, 163)
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		trace_util_0.Count(_rawkv_00000, 164)
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 170)
			return nil, nil, errors.Trace(err)
		}

		trace_util_0.Count(_rawkv_00000, 165)
		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			trace_util_0.Count(_rawkv_00000, 171)
			actualEndKey = loc.EndKey
		}

		trace_util_0.Count(_rawkv_00000, 166)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawDeleteRange,
			RawDeleteRange: &kvrpcpb.RawDeleteRangeRequest{
				StartKey: startKey,
				EndKey:   actualEndKey,
			},
		}

		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 172)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 167)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 173)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_rawkv_00000, 168)
		if regionErr != nil {
			trace_util_0.Count(_rawkv_00000, 174)
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_rawkv_00000, 176)
				return nil, nil, errors.Trace(err)
			}
			trace_util_0.Count(_rawkv_00000, 175)
			continue
		}
		trace_util_0.Count(_rawkv_00000, 169)
		return resp, actualEndKey, nil
	}
}

func (c *RawKVClient) sendBatchPut(bo *Backoffer, keys, values [][]byte) error {
	trace_util_0.Count(_rawkv_00000, 177)
	keyToValue := make(map[string][]byte)
	for i, key := range keys {
		trace_util_0.Count(_rawkv_00000, 183)
		keyToValue[string(key)] = values[i]
	}
	trace_util_0.Count(_rawkv_00000, 178)
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 184)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 179)
	var batches []batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		trace_util_0.Count(_rawkv_00000, 185)
		batches = appendBatches(batches, regionID, groupKeys, keyToValue, rawBatchPutSize)
	}
	trace_util_0.Count(_rawkv_00000, 180)
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		trace_util_0.Count(_rawkv_00000, 186)
		batch1 := batch
		go func() {
			trace_util_0.Count(_rawkv_00000, 187)
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1)
		}()
	}

	trace_util_0.Count(_rawkv_00000, 181)
	for i := 0; i < len(batches); i++ {
		trace_util_0.Count(_rawkv_00000, 188)
		if e := <-ch; e != nil {
			trace_util_0.Count(_rawkv_00000, 189)
			cancel()
			// catch the first error
			if err == nil {
				trace_util_0.Count(_rawkv_00000, 190)
				err = e
			}
		}
	}
	trace_util_0.Count(_rawkv_00000, 182)
	return errors.Trace(err)
}

func appendKeyBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, limit int) []batch {
	trace_util_0.Count(_rawkv_00000, 191)
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		trace_util_0.Count(_rawkv_00000, 194)
		if count > limit {
			trace_util_0.Count(_rawkv_00000, 196)
			batches = append(batches, batch{regionID: regionID, keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		trace_util_0.Count(_rawkv_00000, 195)
		keys = append(keys, groupKeys[start])
		count++
	}
	trace_util_0.Count(_rawkv_00000, 192)
	if len(keys) != 0 {
		trace_util_0.Count(_rawkv_00000, 197)
		batches = append(batches, batch{regionID: regionID, keys: keys})
	}
	trace_util_0.Count(_rawkv_00000, 193)
	return batches
}

func appendBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, limit int) []batch {
	trace_util_0.Count(_rawkv_00000, 198)
	var start, size int
	var keys, values [][]byte
	for start = 0; start < len(groupKeys); start++ {
		trace_util_0.Count(_rawkv_00000, 201)
		if size >= limit {
			trace_util_0.Count(_rawkv_00000, 203)
			batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			size = 0
		}
		trace_util_0.Count(_rawkv_00000, 202)
		key := groupKeys[start]
		value := keyToValue[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		size += len(key)
		size += len(value)
	}
	trace_util_0.Count(_rawkv_00000, 199)
	if len(keys) != 0 {
		trace_util_0.Count(_rawkv_00000, 204)
		batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
	}
	trace_util_0.Count(_rawkv_00000, 200)
	return batches
}

func (c *RawKVClient) doBatchPut(bo *Backoffer, batch batch) error {
	trace_util_0.Count(_rawkv_00000, 205)
	kvPair := make([]*kvrpcpb.KvPair, 0, len(batch.keys))
	for i, key := range batch.keys {
		trace_util_0.Count(_rawkv_00000, 212)
		kvPair = append(kvPair, &kvrpcpb.KvPair{Key: key, Value: batch.values[i]})
	}

	trace_util_0.Count(_rawkv_00000, 206)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawBatchPut,
		RawBatchPut: &kvrpcpb.RawBatchPutRequest{
			Pairs: kvPair,
		},
	}

	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 213)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 207)
	regionErr, err := resp.GetRegionError()
	if err != nil {
		trace_util_0.Count(_rawkv_00000, 214)
		return errors.Trace(err)
	}
	trace_util_0.Count(_rawkv_00000, 208)
	if regionErr != nil {
		trace_util_0.Count(_rawkv_00000, 215)
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			trace_util_0.Count(_rawkv_00000, 217)
			return errors.Trace(err)
		}
		// recursive call
		trace_util_0.Count(_rawkv_00000, 216)
		return c.sendBatchPut(bo, batch.keys, batch.values)
	}

	trace_util_0.Count(_rawkv_00000, 209)
	cmdResp := resp.RawBatchPut
	if cmdResp == nil {
		trace_util_0.Count(_rawkv_00000, 218)
		return errors.Trace(ErrBodyMissing)
	}
	trace_util_0.Count(_rawkv_00000, 210)
	if cmdResp.GetError() != "" {
		trace_util_0.Count(_rawkv_00000, 219)
		return errors.New(cmdResp.GetError())
	}
	trace_util_0.Count(_rawkv_00000, 211)
	return nil
}

type batch struct {
	regionID RegionVerID
	keys     [][]byte
	values   [][]byte
}

type singleBatchResp struct {
	resp *tikvrpc.Response
	err  error
}

var _rawkv_00000 = "store/tikv/rawkv.go"
