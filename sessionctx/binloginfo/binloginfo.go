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

package binloginfo

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	binlog "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func init() {
	trace_util_0.Count(_binloginfo_00000, 0)
	grpc.EnableTracing = false
}

// pumpsClient is the client to write binlog, it is opened on server start and never close,
// shared by all sessions.
var pumpsClient *pumpcli.PumpsClient
var pumpsClientLock sync.RWMutex

// BinlogInfo contains binlog data and binlog client.
type BinlogInfo struct {
	Data   *binlog.Binlog
	Client *pumpcli.PumpsClient
}

// GetPumpsClient gets the pumps client instance.
func GetPumpsClient() *pumpcli.PumpsClient {
	trace_util_0.Count(_binloginfo_00000, 1)
	pumpsClientLock.RLock()
	client := pumpsClient
	pumpsClientLock.RUnlock()
	return client
}

// SetPumpsClient sets the pumps client instance.
func SetPumpsClient(client *pumpcli.PumpsClient) {
	trace_util_0.Count(_binloginfo_00000, 2)
	pumpsClientLock.Lock()
	pumpsClient = client
	pumpsClientLock.Unlock()
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx sessionctx.Context, createIfNotExists bool) *binlog.PrewriteValue {
	trace_util_0.Count(_binloginfo_00000, 3)
	vars := ctx.GetSessionVars()
	v, ok := vars.TxnCtx.Binlog.(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		trace_util_0.Count(_binloginfo_00000, 5)
		schemaVer := ctx.GetSessionVars().TxnCtx.SchemaVersion
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		vars.TxnCtx.Binlog = v
	}
	trace_util_0.Count(_binloginfo_00000, 4)
	return v
}

var skipBinlog uint32
var ignoreError uint32

// DisableSkipBinlogFlag disable the skipBinlog flag.
func DisableSkipBinlogFlag() {
	trace_util_0.Count(_binloginfo_00000, 6)
	atomic.StoreUint32(&skipBinlog, 0)
	logutil.Logger(context.Background()).Warn("[binloginfo] disable the skipBinlog flag")
}

// SetIgnoreError sets the ignoreError flag, this function called when TiDB start
// up and find config.Binlog.IgnoreError is true.
func SetIgnoreError(on bool) {
	trace_util_0.Count(_binloginfo_00000, 7)
	if on {
		trace_util_0.Count(_binloginfo_00000, 8)
		atomic.StoreUint32(&ignoreError, 1)
	} else {
		trace_util_0.Count(_binloginfo_00000, 9)
		{
			atomic.StoreUint32(&ignoreError, 0)
		}
	}
}

// WriteBinlog writes a binlog to Pump.
func (info *BinlogInfo) WriteBinlog(clusterID uint64) error {
	trace_util_0.Count(_binloginfo_00000, 10)
	skip := atomic.LoadUint32(&skipBinlog)
	if skip > 0 {
		trace_util_0.Count(_binloginfo_00000, 14)
		metrics.CriticalErrorCounter.Add(1)
		return nil
	}

	trace_util_0.Count(_binloginfo_00000, 11)
	if info.Client == nil {
		trace_util_0.Count(_binloginfo_00000, 15)
		return errors.New("pumps client is nil")
	}

	// it will retry in PumpsClient if write binlog fail.
	trace_util_0.Count(_binloginfo_00000, 12)
	err := info.Client.WriteBinlog(info.Data)
	if err != nil {
		trace_util_0.Count(_binloginfo_00000, 16)
		logutil.Logger(context.Background()).Error("write binlog failed", zap.Error(err))
		if atomic.LoadUint32(&ignoreError) == 1 {
			trace_util_0.Count(_binloginfo_00000, 19)
			logutil.Logger(context.Background()).Error("write binlog fail but error ignored")
			metrics.CriticalErrorCounter.Add(1)
			// If error happens once, we'll stop writing binlog.
			atomic.CompareAndSwapUint32(&skipBinlog, skip, skip+1)
			return nil
		}

		trace_util_0.Count(_binloginfo_00000, 17)
		if strings.Contains(err.Error(), "received message larger than max") {
			trace_util_0.Count(_binloginfo_00000, 20)
			// This kind of error is not critical, return directly.
			return errors.Errorf("binlog data is too large (%s)", err.Error())
		}

		trace_util_0.Count(_binloginfo_00000, 18)
		return terror.ErrCritical.GenWithStackByArgs(err)
	}

	trace_util_0.Count(_binloginfo_00000, 13)
	return nil
}

// SetDDLBinlog sets DDL binlog in the kv.Transaction.
func SetDDLBinlog(client *pumpcli.PumpsClient, txn kv.Transaction, jobID int64, ddlQuery string) {
	trace_util_0.Count(_binloginfo_00000, 21)
	if client == nil {
		trace_util_0.Count(_binloginfo_00000, 23)
		return
	}

	trace_util_0.Count(_binloginfo_00000, 22)
	ddlQuery = addSpecialComment(ddlQuery)
	info := &BinlogInfo{
		Data: &binlog.Binlog{
			Tp:       binlog.BinlogType_Prewrite,
			DdlJobId: jobID,
			DdlQuery: []byte(ddlQuery),
		},
		Client: client,
	}
	txn.SetOption(kv.BinlogInfo, info)
}

const specialPrefix = `/*!90000 `

func addSpecialComment(ddlQuery string) string {
	trace_util_0.Count(_binloginfo_00000, 24)
	if strings.Contains(ddlQuery, specialPrefix) {
		trace_util_0.Count(_binloginfo_00000, 27)
		return ddlQuery
	}
	trace_util_0.Count(_binloginfo_00000, 25)
	upperQuery := strings.ToUpper(ddlQuery)
	reg, err := regexp.Compile(`SHARD_ROW_ID_BITS\s*=\s*\d+`)
	terror.Log(err)
	loc := reg.FindStringIndex(upperQuery)
	if len(loc) < 2 {
		trace_util_0.Count(_binloginfo_00000, 28)
		return ddlQuery
	}
	trace_util_0.Count(_binloginfo_00000, 26)
	return ddlQuery[:loc[0]] + specialPrefix + ddlQuery[loc[0]:loc[1]] + ` */` + ddlQuery[loc[1]:]
}

// MockPumpsClient creates a PumpsClient, used for test.
func MockPumpsClient(client binlog.PumpClient) *pumpcli.PumpsClient {
	trace_util_0.Count(_binloginfo_00000, 29)
	nodeID := "pump-1"
	pump := &pumpcli.PumpStatus{
		Status: node.Status{
			NodeID: nodeID,
			State:  node.Online,
		},
		Client: client,
	}

	pumpInfos := &pumpcli.PumpInfos{
		Pumps:            make(map[string]*pumpcli.PumpStatus),
		AvaliablePumps:   make(map[string]*pumpcli.PumpStatus),
		UnAvaliablePumps: make(map[string]*pumpcli.PumpStatus),
	}
	pumpInfos.Pumps[nodeID] = pump
	pumpInfos.AvaliablePumps[nodeID] = pump

	pCli := &pumpcli.PumpsClient{
		ClusterID:          1,
		Pumps:              pumpInfos,
		Selector:           pumpcli.NewSelector(pumpcli.Range),
		BinlogWriteTimeout: time.Second,
	}
	pCli.Selector.SetPumps([]*pumpcli.PumpStatus{pump})

	return pCli
}

var _binloginfo_00000 = "sessionctx/binloginfo/binloginfo.go"
