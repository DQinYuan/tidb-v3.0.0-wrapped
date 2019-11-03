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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

// ChangeExec represents a change executor.
type ChangeExec struct {
	baseExecutor
	*ast.ChangeStmt
}

// Next implements the Executor Next interface.
func (e *ChangeExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_change_00000, 0)
	kind := strings.ToLower(e.NodeType)
	urls := config.GetGlobalConfig().Path
	registry, err := createRegistry(urls)
	if err != nil {
		trace_util_0.Count(_change_00000, 4)
		return err
	}
	trace_util_0.Count(_change_00000, 1)
	nodes, _, err := registry.Nodes(ctx, node.NodePrefix[kind])
	if err != nil {
		trace_util_0.Count(_change_00000, 5)
		return err
	}
	trace_util_0.Count(_change_00000, 2)
	state := e.State
	nodeID := e.NodeID
	for _, n := range nodes {
		trace_util_0.Count(_change_00000, 6)
		if n.NodeID != nodeID {
			trace_util_0.Count(_change_00000, 8)
			continue
		}
		trace_util_0.Count(_change_00000, 7)
		switch state {
		case node.Online, node.Pausing, node.Paused, node.Closing, node.Offline:
			trace_util_0.Count(_change_00000, 9)
			n.State = state
			return registry.UpdateNode(ctx, node.NodePrefix[kind], n)
		default:
			trace_util_0.Count(_change_00000, 10)
			return errors.Errorf("state %s is illegal", state)
		}
	}
	trace_util_0.Count(_change_00000, 3)
	return errors.NotFoundf("node %s, id %s from etcd %s", kind, nodeID, urls)
}

var _change_00000 = "executor/change.go"
