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

package executor

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

var _ Executor = &LoadStatsExec{}

// LoadStatsExec represents a load statistic executor.
type LoadStatsExec struct {
	baseExecutor
	info *LoadStatsInfo
}

// LoadStatsInfo saves the information of loading statistic operation.
type LoadStatsInfo struct {
	Path string
	Ctx  sessionctx.Context
}

// loadStatsVarKeyType is a dummy type to avoid naming collision in context.
type loadStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadStatsVarKeyType) String() string {
	trace_util_0.Count(_load_stats_00000, 0)
	return "load_stats_var"
}

// LoadStatsVarKey is a variable key for load statistic.
const LoadStatsVarKey loadStatsVarKeyType = 0

// Next implements the Executor Next interface.
func (e *LoadStatsExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_load_stats_00000, 1)
	req.GrowAndReset(e.maxChunkSize)
	if len(e.info.Path) == 0 {
		trace_util_0.Count(_load_stats_00000, 4)
		return errors.New("Load Stats: file path is empty")
	}
	trace_util_0.Count(_load_stats_00000, 2)
	val := e.ctx.Value(LoadStatsVarKey)
	if val != nil {
		trace_util_0.Count(_load_stats_00000, 5)
		e.ctx.SetValue(LoadStatsVarKey, nil)
		return errors.New("Load Stats: previous load stats option isn't closed normally")
	}
	trace_util_0.Count(_load_stats_00000, 3)
	e.ctx.SetValue(LoadStatsVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *LoadStatsExec) Close() error {
	trace_util_0.Count(_load_stats_00000, 6)
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadStatsExec) Open(ctx context.Context) error {
	trace_util_0.Count(_load_stats_00000, 7)
	return nil
}

// Update updates the stats of the corresponding table according to the data.
func (e *LoadStatsInfo) Update(data []byte) error {
	trace_util_0.Count(_load_stats_00000, 8)
	jsonTbl := &handle.JSONTable{}
	if err := json.Unmarshal(data, jsonTbl); err != nil {
		trace_util_0.Count(_load_stats_00000, 11)
		return errors.Trace(err)
	}
	trace_util_0.Count(_load_stats_00000, 9)
	do := domain.GetDomain(e.Ctx)
	h := do.StatsHandle()
	if h == nil {
		trace_util_0.Count(_load_stats_00000, 12)
		return errors.New("Load Stats: handle is nil")
	}
	trace_util_0.Count(_load_stats_00000, 10)
	return h.LoadStatsFromJSON(GetInfoSchema(e.Ctx), jsonTbl)
}

var _load_stats_00000 = "executor/load_stats.go"
