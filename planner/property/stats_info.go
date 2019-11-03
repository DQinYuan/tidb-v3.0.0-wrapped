// Copyright 2018 PingCAP, Inc.
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

package property

import (
	"fmt"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
)

// StatsInfo stores the basic information of statistics for the plan's output. It is used for cost estimation.
type StatsInfo struct {
	RowCount    float64
	Cardinality []float64

	HistColl *statistics.HistColl
	// StatsVersion indicates the statistics version of a table.
	// If the StatsInfo is calculated using the pseudo statistics on a table, StatsVersion will be PseudoVersion.
	StatsVersion uint64
}

// NewSimpleStats creates a simple StatsInfo with rowCount.
func NewSimpleStats(rowCount float64) *StatsInfo {
	trace_util_0.Count(_stats_info_00000, 0)
	return &StatsInfo{RowCount: rowCount}
}

// String implements fmt.Stringer interface.
func (s *StatsInfo) String() string {
	trace_util_0.Count(_stats_info_00000, 1)
	return fmt.Sprintf("count %v, Cardinality %v", s.RowCount, s.Cardinality)
}

// Count gets the RowCount in the StatsInfo.
func (s *StatsInfo) Count() int64 {
	trace_util_0.Count(_stats_info_00000, 2)
	return int64(s.RowCount)
}

// Scale receives a selectivity and multiplies it with RowCount and Cardinality.
func (s *StatsInfo) Scale(factor float64) *StatsInfo {
	trace_util_0.Count(_stats_info_00000, 3)
	profile := &StatsInfo{
		RowCount:     s.RowCount * factor,
		Cardinality:  make([]float64, len(s.Cardinality)),
		HistColl:     s.HistColl,
		StatsVersion: s.StatsVersion,
	}
	for i := range profile.Cardinality {
		trace_util_0.Count(_stats_info_00000, 5)
		profile.Cardinality[i] = s.Cardinality[i] * factor
	}
	trace_util_0.Count(_stats_info_00000, 4)
	return profile
}

// ScaleByExpectCnt tries to Scale StatsInfo to an expectCnt which must be
// smaller than the derived cnt.
// TODO: try to use a better way to do this.
func (s *StatsInfo) ScaleByExpectCnt(expectCnt float64) *StatsInfo {
	trace_util_0.Count(_stats_info_00000, 6)
	if expectCnt > s.RowCount {
		trace_util_0.Count(_stats_info_00000, 9)
		return s
	}
	trace_util_0.Count(_stats_info_00000, 7)
	if s.RowCount > 1.0 {
		trace_util_0.Count(_stats_info_00000, 10) // if s.RowCount is too small, it will cause overflow
		return s.Scale(expectCnt / s.RowCount)
	}
	trace_util_0.Count(_stats_info_00000, 8)
	return s
}

var _stats_info_00000 = "planner/property/stats_info.go"
