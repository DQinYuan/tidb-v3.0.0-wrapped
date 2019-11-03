// Copyright 2015 PingCAP, Inc.
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

// Package mock is just for test only.
package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/sqlexec"
	binlog "github.com/pingcap/tipb/go-binlog"
)

var _ sessionctx.Context = (*Context)(nil)
var _ sqlexec.SQLExecutor = (*Context)(nil)

// Context represents mocked sessionctx.Context.
type Context struct {
	values      map[fmt.Stringer]interface{}
	txn         wrapTxn    // mock global variable
	Store       kv.Storage // mock global variable
	sessionVars *variable.SessionVars
	mux         sync.Mutex // fix data race in ddl test.
	ctx         context.Context
	cancel      context.CancelFunc
	sm          util.SessionManager
	pcache      *kvcache.SimpleLRUCache
}

type wrapTxn struct {
	kv.Transaction
}

func (txn *wrapTxn) Valid() bool {
	trace_util_0.Count(_context_00000, 0)
	return txn.Transaction != nil && txn.Transaction.Valid()
}

// Execute implements sqlexec.SQLExecutor Execute interface.
func (c *Context) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	trace_util_0.Count(_context_00000, 1)
	return nil, errors.Errorf("Not Support.")
}

// DDLOwnerChecker returns owner.DDLOwnerChecker.
func (c *Context) DDLOwnerChecker() owner.DDLOwnerChecker {
	trace_util_0.Count(_context_00000, 2)
	return nil
}

// SetValue implements sessionctx.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value interface{}) {
	trace_util_0.Count(_context_00000, 3)
	c.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (c *Context) Value(key fmt.Stringer) interface{} {
	trace_util_0.Count(_context_00000, 4)
	value := c.values[key]
	return value
}

// ClearValue implements sessionctx.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	trace_util_0.Count(_context_00000, 5)
	delete(c.values, key)
}

// GetSessionVars implements the sessionctx.Context GetSessionVars interface.
func (c *Context) GetSessionVars() *variable.SessionVars {
	trace_util_0.Count(_context_00000, 6)
	return c.sessionVars
}

// Txn implements sessionctx.Context Txn interface.
func (c *Context) Txn(bool) (kv.Transaction, error) {
	trace_util_0.Count(_context_00000, 7)
	return &c.txn, nil
}

// GetClient implements sessionctx.Context GetClient interface.
func (c *Context) GetClient() kv.Client {
	trace_util_0.Count(_context_00000, 8)
	if c.Store == nil {
		trace_util_0.Count(_context_00000, 10)
		return nil
	}
	trace_util_0.Count(_context_00000, 9)
	return c.Store.GetClient()
}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (c *Context) GetGlobalSysVar(ctx sessionctx.Context, name string) (string, error) {
	trace_util_0.Count(_context_00000, 11)
	v := variable.GetSysVar(name)
	if v == nil {
		trace_util_0.Count(_context_00000, 13)
		return "", variable.UnknownSystemVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_context_00000, 12)
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (c *Context) SetGlobalSysVar(ctx sessionctx.Context, name string, value string) error {
	trace_util_0.Count(_context_00000, 14)
	v := variable.GetSysVar(name)
	if v == nil {
		trace_util_0.Count(_context_00000, 16)
		return variable.UnknownSystemVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_context_00000, 15)
	v.Value = value
	return nil
}

// PreparedPlanCache implements the sessionctx.Context interface.
func (c *Context) PreparedPlanCache() *kvcache.SimpleLRUCache {
	trace_util_0.Count(_context_00000, 17)
	return c.pcache
}

// NewTxn implements the sessionctx.Context interface.
func (c *Context) NewTxn(context.Context) error {
	trace_util_0.Count(_context_00000, 18)
	if c.Store == nil {
		trace_util_0.Count(_context_00000, 22)
		return errors.New("store is not set")
	}
	trace_util_0.Count(_context_00000, 19)
	if c.txn.Valid() {
		trace_util_0.Count(_context_00000, 23)
		err := c.txn.Commit(c.ctx)
		if err != nil {
			trace_util_0.Count(_context_00000, 24)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_context_00000, 20)
	txn, err := c.Store.Begin()
	if err != nil {
		trace_util_0.Count(_context_00000, 25)
		return errors.Trace(err)
	}
	trace_util_0.Count(_context_00000, 21)
	c.txn.Transaction = txn
	return nil
}

// RefreshTxnCtx implements the sessionctx.Context interface.
func (c *Context) RefreshTxnCtx(ctx context.Context) error {
	trace_util_0.Count(_context_00000, 26)
	return errors.Trace(c.NewTxn(ctx))
}

// InitTxnWithStartTS implements the sessionctx.Context interface with startTS.
func (c *Context) InitTxnWithStartTS(startTS uint64) error {
	trace_util_0.Count(_context_00000, 27)
	if c.txn.Valid() {
		trace_util_0.Count(_context_00000, 30)
		return nil
	}
	trace_util_0.Count(_context_00000, 28)
	if c.Store != nil {
		trace_util_0.Count(_context_00000, 31)
		membufCap := kv.DefaultTxnMembufCap
		if c.sessionVars.LightningMode {
			trace_util_0.Count(_context_00000, 34)
			membufCap = kv.ImportingTxnMembufCap
		}
		trace_util_0.Count(_context_00000, 32)
		txn, err := c.Store.BeginWithStartTS(startTS)
		if err != nil {
			trace_util_0.Count(_context_00000, 35)
			return errors.Trace(err)
		}
		trace_util_0.Count(_context_00000, 33)
		txn.SetCap(membufCap)
		c.txn.Transaction = txn
	}
	trace_util_0.Count(_context_00000, 29)
	return nil
}

// GetStore gets the store of session.
func (c *Context) GetStore() kv.Storage {
	trace_util_0.Count(_context_00000, 36)
	return c.Store
}

// GetSessionManager implements the sessionctx.Context interface.
func (c *Context) GetSessionManager() util.SessionManager {
	trace_util_0.Count(_context_00000, 37)
	return c.sm
}

// SetSessionManager set the session manager.
func (c *Context) SetSessionManager(sm util.SessionManager) {
	trace_util_0.Count(_context_00000, 38)
	c.sm = sm
}

// Cancel implements the Session interface.
func (c *Context) Cancel() {
	trace_util_0.Count(_context_00000, 39)
	c.cancel()
}

// GoCtx returns standard sessionctx.Context that bind with current transaction.
func (c *Context) GoCtx() context.Context {
	trace_util_0.Count(_context_00000, 40)
	return c.ctx
}

// StoreQueryFeedback stores the query feedback.
func (c *Context) StoreQueryFeedback(_ interface{}) { trace_util_0.Count(_context_00000, 41) }

// StmtCommit implements the sessionctx.Context interface.
func (c *Context) StmtCommit() error {
	trace_util_0.Count(_context_00000, 42)
	return nil
}

// StmtRollback implements the sessionctx.Context interface.
func (c *Context) StmtRollback() {
	trace_util_0.Count(_context_00000, 43)
}

// StmtGetMutation implements the sessionctx.Context interface.
func (c *Context) StmtGetMutation(tableID int64) *binlog.TableMutation {
	trace_util_0.Count(_context_00000, 44)
	return nil
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface.
func (c *Context) StmtAddDirtyTableOP(op int, tid int64, handle int64, row []types.Datum) {
	trace_util_0.Count(_context_00000, 45)
}

// NewContext creates a new mocked sessionctx.Context.
func NewContext() *Context {
	trace_util_0.Count(_context_00000, 46)
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &Context{
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: variable.NewSessionVars(),
		ctx:         ctx,
		cancel:      cancel,
	}
	sctx.sessionVars.InitChunkSize = 2
	sctx.sessionVars.MaxChunkSize = 32
	sctx.sessionVars.StmtCtx.TimeZone = time.UTC
	sctx.sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	return sctx
}

// HookKeyForTest is as alias, used by context.WithValue.
// golint forbits using string type as key in context.WithValue.
type HookKeyForTest string

var _context_00000 = "util/mock/context.go"
