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

package statistics

import (
	"hash"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/spaolacci/murmur3"
)

// FMSketch is used to count the number of distinct elements in a set.
type FMSketch struct {
	hashset  map[uint64]bool
	mask     uint64
	maxSize  int
	hashFunc hash.Hash64
}

// NewFMSketch returns a new FM sketch.
func NewFMSketch(maxSize int) *FMSketch {
	trace_util_0.Count(_fmsketch_00000, 0)
	return &FMSketch{
		hashset:  make(map[uint64]bool),
		maxSize:  maxSize,
		hashFunc: murmur3.New64(),
	}
}

// NDV returns the ndv of the sketch.
func (s *FMSketch) NDV() int64 {
	trace_util_0.Count(_fmsketch_00000, 1)
	return int64(s.mask+1) * int64(len(s.hashset))
}

func (s *FMSketch) insertHashValue(hashVal uint64) {
	trace_util_0.Count(_fmsketch_00000, 2)
	if (hashVal & s.mask) != 0 {
		trace_util_0.Count(_fmsketch_00000, 4)
		return
	}
	trace_util_0.Count(_fmsketch_00000, 3)
	s.hashset[hashVal] = true
	if len(s.hashset) > s.maxSize {
		trace_util_0.Count(_fmsketch_00000, 5)
		s.mask = s.mask*2 + 1
		for key := range s.hashset {
			trace_util_0.Count(_fmsketch_00000, 6)
			if (key & s.mask) != 0 {
				trace_util_0.Count(_fmsketch_00000, 7)
				delete(s.hashset, key)
			}
		}
	}
}

// InsertValue inserts a value into the FM sketch.
func (s *FMSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	trace_util_0.Count(_fmsketch_00000, 8)
	bytes, err := codec.EncodeValue(sc, nil, value)
	if err != nil {
		trace_util_0.Count(_fmsketch_00000, 11)
		return errors.Trace(err)
	}
	trace_util_0.Count(_fmsketch_00000, 9)
	s.hashFunc.Reset()
	_, err = s.hashFunc.Write(bytes)
	if err != nil {
		trace_util_0.Count(_fmsketch_00000, 12)
		return errors.Trace(err)
	}
	trace_util_0.Count(_fmsketch_00000, 10)
	s.insertHashValue(s.hashFunc.Sum64())
	return nil
}

func buildFMSketch(sc *stmtctx.StatementContext, values []types.Datum, maxSize int) (*FMSketch, int64, error) {
	trace_util_0.Count(_fmsketch_00000, 13)
	s := NewFMSketch(maxSize)
	for _, value := range values {
		trace_util_0.Count(_fmsketch_00000, 15)
		err := s.InsertValue(sc, value)
		if err != nil {
			trace_util_0.Count(_fmsketch_00000, 16)
			return nil, 0, errors.Trace(err)
		}
	}
	trace_util_0.Count(_fmsketch_00000, 14)
	return s, s.NDV(), nil
}

func (s *FMSketch) mergeFMSketch(rs *FMSketch) {
	trace_util_0.Count(_fmsketch_00000, 17)
	if s.mask < rs.mask {
		trace_util_0.Count(_fmsketch_00000, 19)
		s.mask = rs.mask
		for key := range s.hashset {
			trace_util_0.Count(_fmsketch_00000, 20)
			if (key & s.mask) != 0 {
				trace_util_0.Count(_fmsketch_00000, 21)
				delete(s.hashset, key)
			}
		}
	}
	trace_util_0.Count(_fmsketch_00000, 18)
	for key := range rs.hashset {
		trace_util_0.Count(_fmsketch_00000, 22)
		s.insertHashValue(key)
	}
}

// FMSketchToProto converts FMSketch to its protobuf representation.
func FMSketchToProto(s *FMSketch) *tipb.FMSketch {
	trace_util_0.Count(_fmsketch_00000, 23)
	protoSketch := new(tipb.FMSketch)
	protoSketch.Mask = s.mask
	for val := range s.hashset {
		trace_util_0.Count(_fmsketch_00000, 25)
		protoSketch.Hashset = append(protoSketch.Hashset, val)
	}
	trace_util_0.Count(_fmsketch_00000, 24)
	return protoSketch
}

// FMSketchFromProto converts FMSketch from its protobuf representation.
func FMSketchFromProto(protoSketch *tipb.FMSketch) *FMSketch {
	trace_util_0.Count(_fmsketch_00000, 26)
	sketch := &FMSketch{
		hashset: make(map[uint64]bool),
		mask:    protoSketch.Mask,
	}
	for _, val := range protoSketch.Hashset {
		trace_util_0.Count(_fmsketch_00000, 28)
		sketch.hashset[val] = true
	}
	trace_util_0.Count(_fmsketch_00000, 27)
	return sketch
}

var _fmsketch_00000 = "statistics/fmsketch.go"
