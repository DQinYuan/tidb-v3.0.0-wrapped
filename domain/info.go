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

package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"go.uber.org/zap"
)

const (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
	// keyOpDefaultRetryCnt is the default retry count for etcd store.
	keyOpDefaultRetryCnt = 2
	// keyOpDefaultTimeout is the default time out for etcd store.
	keyOpDefaultTimeout = 1 * time.Second
)

// InfoSessionTTL is the etcd session's TTL in seconds. It's exported for testing.
var InfoSessionTTL = 1 * 60

// InfoSyncer stores server info to etcd when the tidb-server starts and delete when tidb-server shuts down.
type InfoSyncer struct {
	etcdCli        *clientv3.Client
	info           *ServerInfo
	serverInfoPath string
	session        *concurrency.Session
}

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	Port       uint   `json:"listening_port"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// NewInfoSyncer return new InfoSyncer. It is exported for testing.
func NewInfoSyncer(id string, etcdCli *clientv3.Client) *InfoSyncer {
	trace_util_0.Count(_info_00000, 0)
	return &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
	}
}

// Init creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) Init(ctx context.Context) error {
	trace_util_0.Count(_info_00000, 1)
	return is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// GetServerInfo gets self server static information.
func (is *InfoSyncer) GetServerInfo() *ServerInfo {
	trace_util_0.Count(_info_00000, 2)
	return is.info
}

// GetServerInfoByID gets server static information from etcd.
func (is *InfoSyncer) GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	trace_util_0.Count(_info_00000, 3)
	if is.etcdCli == nil || id == is.info.ID {
		trace_util_0.Count(_info_00000, 7)
		return is.info, nil
	}
	trace_util_0.Count(_info_00000, 4)
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		trace_util_0.Count(_info_00000, 8)
		return nil, err
	}
	trace_util_0.Count(_info_00000, 5)
	info, ok := infoMap[id]
	if !ok {
		trace_util_0.Count(_info_00000, 9)
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	trace_util_0.Count(_info_00000, 6)
	return info, nil
}

// GetAllServerInfo gets all servers static information from etcd.
func (is *InfoSyncer) GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	trace_util_0.Count(_info_00000, 10)
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		trace_util_0.Count(_info_00000, 13)
		allInfo[is.info.ID] = is.info
		return allInfo, nil
	}
	trace_util_0.Count(_info_00000, 11)
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, keyOpDefaultRetryCnt, keyOpDefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		trace_util_0.Count(_info_00000, 14)
		return nil, err
	}
	trace_util_0.Count(_info_00000, 12)
	return allInfo, nil
}

// storeServerInfo stores self server static information to etcd.
func (is *InfoSyncer) storeServerInfo(ctx context.Context) error {
	trace_util_0.Count(_info_00000, 15)
	if is.etcdCli == nil {
		trace_util_0.Count(_info_00000, 18)
		return nil
	}
	trace_util_0.Count(_info_00000, 16)
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		trace_util_0.Count(_info_00000, 19)
		return errors.Trace(err)
	}
	trace_util_0.Count(_info_00000, 17)
	str := string(hack.String(infoBuf))
	err = ddl.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.serverInfoPath, str, clientv3.WithLease(is.session.Lease()))
	return err
}

// RemoveServerInfo remove self server static information from etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	trace_util_0.Count(_info_00000, 20)
	if is.etcdCli == nil {
		trace_util_0.Count(_info_00000, 22)
		return
	}
	trace_util_0.Count(_info_00000, 21)
	err := ddl.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		trace_util_0.Count(_info_00000, 23)
		logutil.Logger(context.Background()).Error("remove server info failed", zap.Error(err))
	}
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (is InfoSyncer) Done() <-chan struct{} {
	trace_util_0.Count(_info_00000, 24)
	if is.etcdCli == nil {
		trace_util_0.Count(_info_00000, 26)
		return make(chan struct{}, 1)
	}
	trace_util_0.Count(_info_00000, 25)
	return is.session.Done()
}

// Restart restart the info syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) Restart(ctx context.Context) error {
	trace_util_0.Count(_info_00000, 27)
	return is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// newSessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newSessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	trace_util_0.Count(_info_00000, 28)
	if is.etcdCli == nil {
		trace_util_0.Count(_info_00000, 31)
		return nil
	}
	trace_util_0.Count(_info_00000, 29)
	logPrefix := fmt.Sprintf("[Info-syncer] %s", is.serverInfoPath)
	session, err := owner.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, InfoSessionTTL)
	if err != nil {
		trace_util_0.Count(_info_00000, 32)
		return err
	}
	trace_util_0.Count(_info_00000, 30)
	is.session = session

	err = is.storeServerInfo(ctx)
	return err
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	trace_util_0.Count(_info_00000, 33)
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for i := 0; i < retryCnt; i++ {
		trace_util_0.Count(_info_00000, 35)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_info_00000, 39)
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
			trace_util_0.Count(_info_00000, 40)
		}
		trace_util_0.Count(_info_00000, 36)
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			trace_util_0.Count(_info_00000, 41)
			logutil.Logger(context.Background()).Info("get key failed", zap.String("key", key), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		trace_util_0.Count(_info_00000, 37)
		for _, kv := range resp.Kvs {
			trace_util_0.Count(_info_00000, 42)
			info := &ServerInfo{}
			err = json.Unmarshal(kv.Value, info)
			if err != nil {
				trace_util_0.Count(_info_00000, 44)
				logutil.Logger(context.Background()).Info("get key failed", zap.String("key", string(kv.Key)), zap.ByteString("value", kv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_info_00000, 43)
			allInfo[info.ID] = info
		}
		trace_util_0.Count(_info_00000, 38)
		return allInfo, nil
	}
	trace_util_0.Count(_info_00000, 34)
	return nil, errors.Trace(err)
}

// getServerInfo gets self tidb server information.
func getServerInfo(id string) *ServerInfo {
	trace_util_0.Count(_info_00000, 45)
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:         id,
		IP:         cfg.AdvertiseAddress,
		Port:       cfg.Port,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}

var _info_00000 = "domain/info.go"
