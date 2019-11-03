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
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/trace_util_0"
)

// Use global variables to prevent pdClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

type pdClient struct {
	cluster *Cluster
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewPDClient(cluster *Cluster) pd.Client {
	trace_util_0.Count(_pd_00000, 0)
	return &pdClient{
		cluster: cluster,
	}
}

func (c *pdClient) GetClusterID(ctx context.Context) uint64 {
	trace_util_0.Count(_pd_00000, 1)
	return 1
}

func (c *pdClient) GetTS(context.Context) (int64, int64, error) {
	trace_util_0.Count(_pd_00000, 2)
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	if tsMu.physicalTS >= ts {
		trace_util_0.Count(_pd_00000, 4)
		tsMu.logicalTS++
	} else {
		trace_util_0.Count(_pd_00000, 5)
		{
			tsMu.physicalTS = ts
			tsMu.logicalTS = 0
		}
	}
	trace_util_0.Count(_pd_00000, 3)
	return tsMu.physicalTS, tsMu.logicalTS, nil
}

func (c *pdClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	trace_util_0.Count(_pd_00000, 6)
	return &mockTSFuture{c, ctx}
}

type mockTSFuture struct {
	pdc *pdClient
	ctx context.Context
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	trace_util_0.Count(_pd_00000, 7)
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_00000, 8)
	region, peer := c.cluster.GetRegionByKey(key)
	return region, peer, nil
}

func (c *pdClient) GetPrevRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_00000, 9)
	region, peer := c.cluster.GetPrevRegionByKey(key)
	return region, peer, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_00000, 10)
	region, peer := c.cluster.GetRegionByID(regionID)
	return region, peer, nil
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	trace_util_0.Count(_pd_00000, 11)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_pd_00000, 13)
		return nil, ctx.Err()
	default:
		trace_util_0.Count(_pd_00000, 14)
	}
	trace_util_0.Count(_pd_00000, 12)
	store := c.cluster.GetStore(storeID)
	return store, nil
}

func (c *pdClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	trace_util_0.Count(_pd_00000, 15)
	return c.cluster.GetAllStores(), nil
}

func (c *pdClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	trace_util_0.Count(_pd_00000, 16)
	panic("unimplemented")
}

func (c *pdClient) Close() {
	trace_util_0.Count(_pd_00000, 17)
}

func (c *pdClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	trace_util_0.Count(_pd_00000, 18)
	return nil
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	trace_util_0.Count(_pd_00000, 19)
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

var _pd_00000 = "store/mockstore/mocktikv/pd.go"
