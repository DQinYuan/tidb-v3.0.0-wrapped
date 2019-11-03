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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/codec"
)

type codecPDClient struct {
	pd.Client
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_codec_00000, 0)
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, peer, err := c.Client.GetRegion(ctx, encodedKey)
	return processRegionResult(region, peer, err)
}

func (c *codecPDClient) GetPrevRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_codec_00000, 1)
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, peer, err := c.Client.GetPrevRegion(ctx, encodedKey)
	return processRegionResult(region, peer, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *codecPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_codec_00000, 2)
	region, peer, err := c.Client.GetRegionByID(ctx, regionID)
	return processRegionResult(region, peer, err)
}

func processRegionResult(region *metapb.Region, peer *metapb.Peer, err error) (*metapb.Region, *metapb.Peer, error) {
	trace_util_0.Count(_pd_codec_00000, 3)
	if err != nil {
		trace_util_0.Count(_pd_codec_00000, 7)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_pd_codec_00000, 4)
	if region == nil {
		trace_util_0.Count(_pd_codec_00000, 8)
		return nil, nil, nil
	}
	trace_util_0.Count(_pd_codec_00000, 5)
	err = decodeRegionMetaKey(region)
	if err != nil {
		trace_util_0.Count(_pd_codec_00000, 9)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_pd_codec_00000, 6)
	return region, peer, nil
}

func decodeRegionMetaKey(r *metapb.Region) error {
	trace_util_0.Count(_pd_codec_00000, 10)
	if len(r.StartKey) != 0 {
		trace_util_0.Count(_pd_codec_00000, 13)
		_, decoded, err := codec.DecodeBytes(r.StartKey, nil)
		if err != nil {
			trace_util_0.Count(_pd_codec_00000, 15)
			return errors.Trace(err)
		}
		trace_util_0.Count(_pd_codec_00000, 14)
		r.StartKey = decoded
	}
	trace_util_0.Count(_pd_codec_00000, 11)
	if len(r.EndKey) != 0 {
		trace_util_0.Count(_pd_codec_00000, 16)
		_, decoded, err := codec.DecodeBytes(r.EndKey, nil)
		if err != nil {
			trace_util_0.Count(_pd_codec_00000, 18)
			return errors.Trace(err)
		}
		trace_util_0.Count(_pd_codec_00000, 17)
		r.EndKey = decoded
	}
	trace_util_0.Count(_pd_codec_00000, 12)
	return nil
}

var _pd_codec_00000 = "store/tikv/pd_codec.go"
