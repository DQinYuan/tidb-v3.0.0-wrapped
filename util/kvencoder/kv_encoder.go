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

package kvenc

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var _ KvEncoder = &kvEncoder{}
var mockConnID uint64

// KvPair is a key-value pair.
type KvPair struct {
	// Key is the key of the pair.
	Key []byte
	// Val is the value of the pair. if the op is delete, the len(Val) == 0
	Val []byte
}

// KvEncoder is an encoder that transfer sql to key-value pairs.
type KvEncoder interface {
	// Encode transfers sql to kv pairs.
	// Before use Encode() method, please make sure you already created schame by calling ExecDDLSQL() method.
	// NOTE: now we just support transfers insert statement to kv pairs.
	// (if we wanna support other statement, we need to add a kv.Storage parameter,
	// and pass tikv store in.)
	// return encoded kvs array that generate by sql, and affectRows count.
	Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error)

	// PrepareStmt prepare query statement, and return statement id.
	// Pass stmtID into EncodePrepareStmt to execute a prepare statement.
	PrepareStmt(query string) (stmtID uint32, err error)

	// EncodePrepareStmt transfer prepare query to kv pairs.
	// stmtID is generated by PrepareStmt.
	EncodePrepareStmt(tableID int64, stmtID uint32, param ...interface{}) (kvPairs []KvPair, affectedRows uint64, err error)

	// ExecDDLSQL executes ddl sql, you must use it to create schema infos.
	ExecDDLSQL(sql string) error

	// EncodeMetaAutoID encode the table meta info, autoID to coresponding key-value pair.
	EncodeMetaAutoID(dbID, tableID, autoID int64) (KvPair, error)

	// SetSystemVariable set system variable name = value.
	SetSystemVariable(name string, value string) error

	// GetSystemVariable get the system variable value of name.
	GetSystemVariable(name string) (string, bool)

	// Close cleanup the kvEncoder.
	Close() error
}

var (
	// refCount is used to ensure that there is only one domain.Domain instance.
	refCount    int64
	mu          sync.Mutex
	storeGlobal kv.Storage
	domGlobal   *domain.Domain
)

type kvEncoder struct {
	se    session.Session
	store kv.Storage
	dom   *domain.Domain
}

// New new a KvEncoder
func New(dbName string, idAlloc autoid.Allocator) (KvEncoder, error) {
	trace_util_0.Count(_kv_encoder_00000, 0)
	kvEnc := &kvEncoder{}
	mu.Lock()
	defer mu.Unlock()
	if refCount == 0 {
		trace_util_0.Count(_kv_encoder_00000, 3)
		if err := initGlobal(); err != nil {
			trace_util_0.Count(_kv_encoder_00000, 4)
			return nil, err
		}
	}
	trace_util_0.Count(_kv_encoder_00000, 1)
	err := kvEnc.initial(dbName, idAlloc)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 5)
		return nil, err
	}
	trace_util_0.Count(_kv_encoder_00000, 2)
	refCount++
	return kvEnc, nil
}

func (e *kvEncoder) Close() error {
	trace_util_0.Count(_kv_encoder_00000, 6)
	e.se.Close()
	mu.Lock()
	defer mu.Unlock()
	refCount--
	if refCount == 0 {
		trace_util_0.Count(_kv_encoder_00000, 8)
		e.dom.Close()
		if err := e.store.Close(); err != nil {
			trace_util_0.Count(_kv_encoder_00000, 9)
			return err
		}
	}
	trace_util_0.Count(_kv_encoder_00000, 7)
	return nil
}

func (e *kvEncoder) Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error) {
	trace_util_0.Count(_kv_encoder_00000, 10)
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	defer e.se.RollbackTxn(context.Background())

	_, err = e.se.Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 12)
		return nil, 0, err
	}

	trace_util_0.Count(_kv_encoder_00000, 11)
	return e.getKvPairsInMemBuffer(tableID)
}

func (e *kvEncoder) getKvPairsInMemBuffer(tableID int64) (kvPairs []KvPair, affectedRows uint64, err error) {
	trace_util_0.Count(_kv_encoder_00000, 13)
	txn, err := e.se.Txn(true)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 17)
		return nil, 0, err
	}
	trace_util_0.Count(_kv_encoder_00000, 14)
	txnMemBuffer := txn.GetMemBuffer()
	kvPairs = make([]KvPair, 0, txnMemBuffer.Len())
	err = kv.WalkMemBuffer(txnMemBuffer, func(k kv.Key, v []byte) error {
		trace_util_0.Count(_kv_encoder_00000, 18)
		if bytes.HasPrefix(k, tablecodec.TablePrefix()) {
			trace_util_0.Count(_kv_encoder_00000, 20)
			k = tablecodec.ReplaceRecordKeyTableID(k, tableID)
		}
		trace_util_0.Count(_kv_encoder_00000, 19)
		kvPairs = append(kvPairs, KvPair{Key: k, Val: v})
		return nil
	})

	trace_util_0.Count(_kv_encoder_00000, 15)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 21)
		return nil, 0, err
	}
	trace_util_0.Count(_kv_encoder_00000, 16)
	return kvPairs, e.se.GetSessionVars().StmtCtx.AffectedRows(), nil
}

func (e *kvEncoder) PrepareStmt(query string) (stmtID uint32, err error) {
	trace_util_0.Count(_kv_encoder_00000, 22)
	stmtID, _, _, err = e.se.PrepareStmt(query)
	return
}

func (e *kvEncoder) EncodePrepareStmt(tableID int64, stmtID uint32, param ...interface{}) (kvPairs []KvPair, affectedRows uint64, err error) {
	trace_util_0.Count(_kv_encoder_00000, 23)
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	defer e.se.RollbackTxn(context.Background())

	_, err = e.se.ExecutePreparedStmt(context.Background(), stmtID, param...)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 25)
		return nil, 0, err
	}

	trace_util_0.Count(_kv_encoder_00000, 24)
	return e.getKvPairsInMemBuffer(tableID)
}

func (e *kvEncoder) EncodeMetaAutoID(dbID, tableID, autoID int64) (KvPair, error) {
	trace_util_0.Count(_kv_encoder_00000, 26)
	mockTxn := kv.NewMockTxn()
	m := meta.NewMeta(mockTxn)
	k, v := m.GenAutoTableIDKeyValue(dbID, tableID, autoID)
	return KvPair{Key: k, Val: v}, nil
}

func (e *kvEncoder) ExecDDLSQL(sql string) error {
	trace_util_0.Count(_kv_encoder_00000, 27)
	_, err := e.se.Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 29)
		return err
	}

	trace_util_0.Count(_kv_encoder_00000, 28)
	return nil
}

func (e *kvEncoder) SetSystemVariable(name string, value string) error {
	trace_util_0.Count(_kv_encoder_00000, 30)
	name = strings.ToLower(name)
	if e.se != nil {
		trace_util_0.Count(_kv_encoder_00000, 32)
		return e.se.GetSessionVars().SetSystemVar(name, value)
	}
	trace_util_0.Count(_kv_encoder_00000, 31)
	return errors.Errorf("e.se is nil, please new KvEncoder by kvencoder.New().")
}

func (e *kvEncoder) GetSystemVariable(name string) (string, bool) {
	trace_util_0.Count(_kv_encoder_00000, 33)
	name = strings.ToLower(name)
	if e.se == nil {
		trace_util_0.Count(_kv_encoder_00000, 35)
		return "", false
	}

	trace_util_0.Count(_kv_encoder_00000, 34)
	return e.se.GetSessionVars().GetSystemVar(name)
}

func newMockTikvWithBootstrap() (kv.Storage, *domain.Domain, error) {
	trace_util_0.Count(_kv_encoder_00000, 36)
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 38)
		return nil, nil, err
	}
	trace_util_0.Count(_kv_encoder_00000, 37)
	session.SetSchemaLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, err
}

func (e *kvEncoder) initial(dbName string, idAlloc autoid.Allocator) (err error) {
	trace_util_0.Count(_kv_encoder_00000, 39)
	se, err := session.CreateSession(storeGlobal)
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 43)
		return
	}

	trace_util_0.Count(_kv_encoder_00000, 40)
	dbName = strings.Replace(dbName, "`", "``", -1)

	se.SetConnectionID(atomic.AddUint64(&mockConnID, 1))
	_, err = se.Execute(context.Background(), fmt.Sprintf("create database if not exists `%s`", dbName))
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 44)
		return
	}
	trace_util_0.Count(_kv_encoder_00000, 41)
	_, err = se.Execute(context.Background(), fmt.Sprintf("use `%s`", dbName))
	if err != nil {
		trace_util_0.Count(_kv_encoder_00000, 45)
		return
	}

	trace_util_0.Count(_kv_encoder_00000, 42)
	se.GetSessionVars().IDAllocator = idAlloc
	se.GetSessionVars().LightningMode = true
	se.GetSessionVars().SkipUTF8Check = true
	e.se = se
	e.store = storeGlobal
	e.dom = domGlobal
	return nil
}

// initGlobal modify the global domain and store
func initGlobal() error {
	trace_util_0.Count(_kv_encoder_00000, 46)
	// disable stats update.
	session.SetStatsLease(0)
	var err error
	storeGlobal, domGlobal, err = newMockTikvWithBootstrap()
	if err == nil {
		trace_util_0.Count(_kv_encoder_00000, 50)
		return nil
	}

	trace_util_0.Count(_kv_encoder_00000, 47)
	if storeGlobal != nil {
		trace_util_0.Count(_kv_encoder_00000, 51)
		if err1 := storeGlobal.Close(); err1 != nil {
			trace_util_0.Count(_kv_encoder_00000, 52)
			logutil.Logger(context.Background()).Error("storeGlobal close error", zap.Error(err1))
		}
	}
	trace_util_0.Count(_kv_encoder_00000, 48)
	if domGlobal != nil {
		trace_util_0.Count(_kv_encoder_00000, 53)
		domGlobal.Close()
	}
	trace_util_0.Count(_kv_encoder_00000, 49)
	return err
}

var _kv_encoder_00000 = "util/kvencoder/kv_encoder.go"
