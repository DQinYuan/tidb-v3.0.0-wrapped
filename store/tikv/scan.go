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

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Scanner support tikv scan
type Scanner struct {
	snapshot     *tikvSnapshot
	batchSize    int
	valid        bool
	cache        []*pb.KvPair
	idx          int
	nextStartKey []byte
	endKey       []byte
	eof          bool

	// Use for reverse scan.
	reverse    bool
	nextEndKey []byte
}

func newScanner(snapshot *tikvSnapshot, startKey []byte, endKey []byte, batchSize int, reverse bool) (*Scanner, error) {
	trace_util_0.Count(_scan_00000, 0)
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		trace_util_0.Count(_scan_00000, 3)
		batchSize = scanBatchSize
	}
	trace_util_0.Count(_scan_00000, 1)
	scanner := &Scanner{
		snapshot:     snapshot,
		batchSize:    batchSize,
		valid:        true,
		nextStartKey: startKey,
		endKey:       endKey,
		reverse:      reverse,
		nextEndKey:   endKey,
	}
	err := scanner.Next()
	if kv.IsErrNotFound(err) {
		trace_util_0.Count(_scan_00000, 4)
		return scanner, nil
	}
	trace_util_0.Count(_scan_00000, 2)
	return scanner, errors.Trace(err)
}

// Valid return valid.
func (s *Scanner) Valid() bool {
	trace_util_0.Count(_scan_00000, 5)
	return s.valid
}

// Key return key.
func (s *Scanner) Key() kv.Key {
	trace_util_0.Count(_scan_00000, 6)
	if s.valid {
		trace_util_0.Count(_scan_00000, 8)
		return s.cache[s.idx].Key
	}
	trace_util_0.Count(_scan_00000, 7)
	return nil
}

// Value return value.
func (s *Scanner) Value() []byte {
	trace_util_0.Count(_scan_00000, 9)
	if s.valid {
		trace_util_0.Count(_scan_00000, 11)
		return s.cache[s.idx].Value
	}
	trace_util_0.Count(_scan_00000, 10)
	return nil
}

// Next return next element.
func (s *Scanner) Next() error {
	trace_util_0.Count(_scan_00000, 12)
	bo := NewBackoffer(context.WithValue(context.Background(), txnStartKey, s.snapshot.version.Ver), scannerNextMaxBackoff)
	if !s.valid {
		trace_util_0.Count(_scan_00000, 14)
		return errors.New("scanner iterator is invalid")
	}
	trace_util_0.Count(_scan_00000, 13)
	var err error
	for {
		trace_util_0.Count(_scan_00000, 15)
		s.idx++
		if s.idx >= len(s.cache) {
			trace_util_0.Count(_scan_00000, 19)
			if s.eof {
				trace_util_0.Count(_scan_00000, 22)
				s.Close()
				return nil
			}
			trace_util_0.Count(_scan_00000, 20)
			err = s.getData(bo)
			if err != nil {
				trace_util_0.Count(_scan_00000, 23)
				s.Close()
				return errors.Trace(err)
			}
			trace_util_0.Count(_scan_00000, 21)
			if s.idx >= len(s.cache) {
				trace_util_0.Count(_scan_00000, 24)
				continue
			}
		}

		trace_util_0.Count(_scan_00000, 16)
		current := s.cache[s.idx]
		if (!s.reverse && (len(s.endKey) > 0 && kv.Key(current.Key).Cmp(kv.Key(s.endKey)) >= 0)) ||
			(s.reverse && len(s.nextStartKey) > 0 && kv.Key(current.Key).Cmp(kv.Key(s.nextStartKey)) < 0) {
			trace_util_0.Count(_scan_00000, 25)
			s.eof = true
			s.Close()
			return nil
		}
		// Try to resolve the lock
		trace_util_0.Count(_scan_00000, 17)
		if current.GetError() != nil {
			trace_util_0.Count(_scan_00000, 26)
			// 'current' would be modified if the lock being resolved
			if err := s.resolveCurrentLock(bo, current); err != nil {
				trace_util_0.Count(_scan_00000, 28)
				s.Close()
				return errors.Trace(err)
			}

			// The check here does not violate the KeyOnly semantic, because current's value
			// is filled by resolveCurrentLock which fetches the value by snapshot.get, so an empty
			// value stands for NotExist
			trace_util_0.Count(_scan_00000, 27)
			if len(current.Value) == 0 {
				trace_util_0.Count(_scan_00000, 29)
				continue
			}
		}
		trace_util_0.Count(_scan_00000, 18)
		return nil
	}
}

// Close close iterator.
func (s *Scanner) Close() {
	trace_util_0.Count(_scan_00000, 30)
	s.valid = false
}

func (s *Scanner) startTS() uint64 {
	trace_util_0.Count(_scan_00000, 31)
	return s.snapshot.version.Ver
}

func (s *Scanner) resolveCurrentLock(bo *Backoffer, current *pb.KvPair) error {
	trace_util_0.Count(_scan_00000, 32)
	val, err := s.snapshot.get(bo, kv.Key(current.Key))
	if err != nil {
		trace_util_0.Count(_scan_00000, 34)
		return errors.Trace(err)
	}
	trace_util_0.Count(_scan_00000, 33)
	current.Error = nil
	current.Value = val
	return nil
}

func (s *Scanner) getData(bo *Backoffer) error {
	trace_util_0.Count(_scan_00000, 35)
	logutil.Logger(context.Background()).Debug("txn getData",
		zap.Binary("nextStartKey", s.nextStartKey),
		zap.Binary("nextEndKey", s.nextEndKey),
		zap.Bool("reverse", s.reverse),
		zap.Uint64("txnStartTS", s.startTS()))
	sender := NewRegionRequestSender(s.snapshot.store.regionCache, s.snapshot.store.client)
	var reqEndKey, reqStartKey []byte
	var loc *KeyLocation
	var err error
	for {
		trace_util_0.Count(_scan_00000, 36)
		if !s.reverse {
			trace_util_0.Count(_scan_00000, 49)
			loc, err = s.snapshot.store.regionCache.LocateKey(bo, s.nextStartKey)
		} else {
			trace_util_0.Count(_scan_00000, 50)
			{
				loc, err = s.snapshot.store.regionCache.LocateEndKey(bo, s.nextEndKey)
			}
		}
		trace_util_0.Count(_scan_00000, 37)
		if err != nil {
			trace_util_0.Count(_scan_00000, 51)
			return errors.Trace(err)
		}

		trace_util_0.Count(_scan_00000, 38)
		if !s.reverse {
			trace_util_0.Count(_scan_00000, 52)
			reqEndKey = s.endKey
			if len(reqEndKey) > 0 && len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, reqEndKey) < 0 {
				trace_util_0.Count(_scan_00000, 53)
				reqEndKey = loc.EndKey
			}
		} else {
			trace_util_0.Count(_scan_00000, 54)
			{
				reqStartKey = s.nextStartKey
				if len(reqStartKey) == 0 ||
					(len(loc.StartKey) > 0 && bytes.Compare(loc.StartKey, reqStartKey) > 0) {
					trace_util_0.Count(_scan_00000, 55)
					reqStartKey = loc.StartKey
				}
			}
		}

		trace_util_0.Count(_scan_00000, 39)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdScan,
			Scan: &pb.ScanRequest{
				StartKey: s.nextStartKey,
				EndKey:   reqEndKey,
				Limit:    uint32(s.batchSize),
				Version:  s.startTS(),
				KeyOnly:  s.snapshot.keyOnly,
			},
			Context: pb.Context{
				Priority:     s.snapshot.priority,
				NotFillCache: s.snapshot.notFillCache,
			},
		}
		if s.reverse {
			trace_util_0.Count(_scan_00000, 56)
			req.Scan.StartKey = s.nextEndKey
			req.Scan.EndKey = reqStartKey
			req.Scan.Reverse = true
		}
		trace_util_0.Count(_scan_00000, 40)
		resp, err := sender.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			trace_util_0.Count(_scan_00000, 57)
			return errors.Trace(err)
		}
		trace_util_0.Count(_scan_00000, 41)
		regionErr, err := resp.GetRegionError()
		if err != nil {
			trace_util_0.Count(_scan_00000, 58)
			return errors.Trace(err)
		}
		trace_util_0.Count(_scan_00000, 42)
		if regionErr != nil {
			trace_util_0.Count(_scan_00000, 59)
			logutil.Logger(context.Background()).Debug("scanner getData failed",
				zap.Stringer("regionErr", regionErr))
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				trace_util_0.Count(_scan_00000, 61)
				return errors.Trace(err)
			}
			trace_util_0.Count(_scan_00000, 60)
			continue
		}
		trace_util_0.Count(_scan_00000, 43)
		cmdScanResp := resp.Scan
		if cmdScanResp == nil {
			trace_util_0.Count(_scan_00000, 62)
			return errors.Trace(ErrBodyMissing)
		}

		trace_util_0.Count(_scan_00000, 44)
		err = s.snapshot.store.CheckVisibility(s.startTS())
		if err != nil {
			trace_util_0.Count(_scan_00000, 63)
			return errors.Trace(err)
		}

		trace_util_0.Count(_scan_00000, 45)
		kvPairs := cmdScanResp.Pairs
		// Check if kvPair contains error, it should be a Lock.
		for _, pair := range kvPairs {
			trace_util_0.Count(_scan_00000, 64)
			if keyErr := pair.GetError(); keyErr != nil {
				trace_util_0.Count(_scan_00000, 65)
				lock, err := extractLockFromKeyErr(keyErr)
				if err != nil {
					trace_util_0.Count(_scan_00000, 67)
					return errors.Trace(err)
				}
				trace_util_0.Count(_scan_00000, 66)
				pair.Key = lock.Key
			}
		}

		trace_util_0.Count(_scan_00000, 46)
		s.cache, s.idx = kvPairs, 0
		if len(kvPairs) < s.batchSize {
			trace_util_0.Count(_scan_00000, 68)
			// No more data in current Region. Next getData() starts
			// from current Region's endKey.
			if !s.reverse {
				trace_util_0.Count(_scan_00000, 71)
				s.nextStartKey = loc.EndKey
			} else {
				trace_util_0.Count(_scan_00000, 72)
				{
					s.nextEndKey = reqStartKey
				}
			}
			trace_util_0.Count(_scan_00000, 69)
			if (!s.reverse && (len(loc.EndKey) == 0 || (len(s.endKey) > 0 && kv.Key(s.nextStartKey).Cmp(kv.Key(s.endKey)) >= 0))) ||
				(s.reverse && (len(loc.StartKey) == 0 || (len(s.nextStartKey) > 0 && kv.Key(s.nextStartKey).Cmp(kv.Key(s.nextEndKey)) >= 0))) {
				trace_util_0.Count(_scan_00000, 73)
				// Current Region is the last one.
				s.eof = true
			}
			trace_util_0.Count(_scan_00000, 70)
			return nil
		}
		// next getData() starts from the last key in kvPairs (but skip
		// it by appending a '\x00' to the key). Note that next getData()
		// may get an empty response if the Region in fact does not have
		// more data.
		trace_util_0.Count(_scan_00000, 47)
		lastKey := kvPairs[len(kvPairs)-1].GetKey()
		if !s.reverse {
			trace_util_0.Count(_scan_00000, 74)
			s.nextStartKey = kv.Key(lastKey).Next()
		} else {
			trace_util_0.Count(_scan_00000, 75)
			{
				s.nextEndKey = kv.Key(lastKey)
			}
		}
		trace_util_0.Count(_scan_00000, 48)
		return nil
	}
}

var _scan_00000 = "store/tikv/scan.go"
