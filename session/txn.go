// Copyright 2018 PingCAP, Inc.

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

package session

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type TxnState struct {
	// States of a TxnState should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	kv.Transaction
	txnFuture *txnFuture

	buf          kv.MemBuffer
	mutations    map[int64]*binlog.TableMutation
	dirtyTableOP []dirtyTableOperation

	// If doNotCommit is not nil, Commit() will not commit the transaction.
	// doNotCommit flag may be set when StmtCommit fail.
	doNotCommit error
}

func (st *TxnState) init() {
	trace_util_0.Count(_txn_00000, 0)
	st.buf = kv.NewMemDbBuffer(kv.DefaultTxnMembufCap)
	st.mutations = make(map[int64]*binlog.TableMutation)
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	trace_util_0.Count(_txn_00000, 1)
	return st.Transaction != nil && st.Transaction.Valid()
}

func (st *TxnState) pending() bool {
	trace_util_0.Count(_txn_00000, 2)
	return st.Transaction == nil && st.txnFuture != nil
}

func (st *TxnState) validOrPending() bool {
	trace_util_0.Count(_txn_00000, 3)
	return st.txnFuture != nil || st.Valid()
}

func (st *TxnState) String() string {
	trace_util_0.Count(_txn_00000, 4)
	if st.Transaction != nil {
		trace_util_0.Count(_txn_00000, 7)
		return st.Transaction.String()
	}
	trace_util_0.Count(_txn_00000, 5)
	if st.txnFuture != nil {
		trace_util_0.Count(_txn_00000, 8)
		return "txnFuture"
	}
	trace_util_0.Count(_txn_00000, 6)
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (st *TxnState) GoString() string {
	trace_util_0.Count(_txn_00000, 9)
	var s strings.Builder
	s.WriteString("Txn{")
	if st.pending() {
		trace_util_0.Count(_txn_00000, 11)
		s.WriteString("state=pending")
	} else {
		trace_util_0.Count(_txn_00000, 12)
		if st.Valid() {
			trace_util_0.Count(_txn_00000, 13)
			s.WriteString("state=valid")
			fmt.Fprintf(&s, ", txnStartTS=%d", st.Transaction.StartTS())
			if len(st.dirtyTableOP) > 0 {
				trace_util_0.Count(_txn_00000, 16)
				fmt.Fprintf(&s, ", len(dirtyTable)=%d, %#v", len(st.dirtyTableOP), st.dirtyTableOP)
			}
			trace_util_0.Count(_txn_00000, 14)
			if len(st.mutations) > 0 {
				trace_util_0.Count(_txn_00000, 17)
				fmt.Fprintf(&s, ", len(mutations)=%d, %#v", len(st.mutations), st.mutations)
			}
			trace_util_0.Count(_txn_00000, 15)
			if st.buf != nil && st.buf.Len() != 0 {
				trace_util_0.Count(_txn_00000, 18)
				fmt.Fprintf(&s, ", buf.length: %d, buf.size: %d", st.buf.Len(), st.buf.Size())
			}
		} else {
			trace_util_0.Count(_txn_00000, 19)
			{
				s.WriteString("state=invalid")
			}
		}
	}

	trace_util_0.Count(_txn_00000, 10)
	s.WriteString("}")
	return s.String()
}

func (st *TxnState) changeInvalidToValid(txn kv.Transaction) {
	trace_util_0.Count(_txn_00000, 20)
	st.Transaction = txn
	st.txnFuture = nil
}

func (st *TxnState) changeInvalidToPending(future *txnFuture) {
	trace_util_0.Count(_txn_00000, 21)
	st.Transaction = nil
	st.txnFuture = future
}

func (st *TxnState) changePendingToValid(txnCap int) error {
	trace_util_0.Count(_txn_00000, 22)
	if st.txnFuture == nil {
		trace_util_0.Count(_txn_00000, 25)
		return errors.New("transaction future is not set")
	}

	trace_util_0.Count(_txn_00000, 23)
	future := st.txnFuture
	st.txnFuture = nil

	txn, err := future.wait()
	if err != nil {
		trace_util_0.Count(_txn_00000, 26)
		st.Transaction = nil
		return err
	}
	trace_util_0.Count(_txn_00000, 24)
	txn.SetCap(txnCap)
	st.Transaction = txn
	return nil
}

func (st *TxnState) changeToInvalid() {
	trace_util_0.Count(_txn_00000, 27)
	st.Transaction = nil
	st.txnFuture = nil
}

// dirtyTableOperation represents an operation to dirtyTable, we log the operation
// first and apply the operation log when statement commit.
type dirtyTableOperation struct {
	kind   int
	tid    int64
	handle int64
	row    []types.Datum
}

var hasMockAutoIDRetry = int64(0)

func enableMockAutoIDRetry() {
	trace_util_0.Count(_txn_00000, 28)
	atomic.StoreInt64(&hasMockAutoIDRetry, 1)
}

func mockAutoIDRetry() bool {
	trace_util_0.Count(_txn_00000, 29)
	return atomic.LoadInt64(&hasMockAutoIDRetry) == 1
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	trace_util_0.Count(_txn_00000, 30)
	defer st.reset()
	if len(st.mutations) != 0 || len(st.dirtyTableOP) != 0 || st.buf.Len() != 0 {
		trace_util_0.Count(_txn_00000, 35)
		logutil.Logger(context.Background()).Error("the code should never run here",
			zap.String("TxnState", st.GoString()),
			zap.Stack("something must be wrong"))
		return errors.New("invalid transaction")
	}
	trace_util_0.Count(_txn_00000, 31)
	if st.doNotCommit != nil {
		trace_util_0.Count(_txn_00000, 36)
		if err1 := st.Transaction.Rollback(); err1 != nil {
			trace_util_0.Count(_txn_00000, 38)
			logutil.Logger(context.Background()).Error("rollback error", zap.Error(err1))
		}
		trace_util_0.Count(_txn_00000, 37)
		return errors.Trace(st.doNotCommit)
	}

	// mockCommitError8942 is used for PR #8942.
	trace_util_0.Count(_txn_00000, 32)
	failpoint.Inject("mockCommitError8942", func(val failpoint.Value) {
		trace_util_0.Count(_txn_00000, 39)
		if val.(bool) {
			trace_util_0.Count(_txn_00000, 40)
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	// mockCommitRetryForAutoID is used to mock an commit retry for adjustAutoIncrementDatum.
	trace_util_0.Count(_txn_00000, 33)
	failpoint.Inject("mockCommitRetryForAutoID", func(val failpoint.Value) {
		trace_util_0.Count(_txn_00000, 41)
		if val.(bool) && !mockAutoIDRetry() {
			trace_util_0.Count(_txn_00000, 42)
			enableMockAutoIDRetry()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	trace_util_0.Count(_txn_00000, 34)
	return st.Transaction.Commit(ctx)
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	trace_util_0.Count(_txn_00000, 43)
	defer st.reset()
	return st.Transaction.Rollback()
}

func (st *TxnState) reset() {
	trace_util_0.Count(_txn_00000, 44)
	st.doNotCommit = nil
	st.cleanup()
	st.changeToInvalid()
}

// Get overrides the Transaction interface.
func (st *TxnState) Get(k kv.Key) ([]byte, error) {
	trace_util_0.Count(_txn_00000, 45)
	val, err := st.buf.Get(k)
	if kv.IsErrNotFound(err) {
		trace_util_0.Count(_txn_00000, 49)
		val, err = st.Transaction.Get(k)
		if kv.IsErrNotFound(err) {
			trace_util_0.Count(_txn_00000, 50)
			return nil, err
		}
	}
	trace_util_0.Count(_txn_00000, 46)
	if err != nil {
		trace_util_0.Count(_txn_00000, 51)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 47)
	if len(val) == 0 {
		trace_util_0.Count(_txn_00000, 52)
		return nil, kv.ErrNotExist
	}
	trace_util_0.Count(_txn_00000, 48)
	return val, nil
}

// BatchGet overrides the Transaction interface.
func (st *TxnState) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	trace_util_0.Count(_txn_00000, 53)
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		trace_util_0.Count(_txn_00000, 57)
		val, err := st.buf.Get(key)
		if kv.IsErrNotFound(err) {
			trace_util_0.Count(_txn_00000, 60)
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		trace_util_0.Count(_txn_00000, 58)
		if err != nil {
			trace_util_0.Count(_txn_00000, 61)
			return nil, err
		}
		trace_util_0.Count(_txn_00000, 59)
		if len(val) != 0 {
			trace_util_0.Count(_txn_00000, 62)
			bufferValues[i] = val
		}
	}
	trace_util_0.Count(_txn_00000, 54)
	storageValues, err := st.Transaction.BatchGet(shrinkKeys)
	if err != nil {
		trace_util_0.Count(_txn_00000, 63)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 55)
	for i, key := range keys {
		trace_util_0.Count(_txn_00000, 64)
		if bufferValues[i] == nil {
			trace_util_0.Count(_txn_00000, 66)
			continue
		}
		trace_util_0.Count(_txn_00000, 65)
		storageValues[string(key)] = bufferValues[i]
	}
	trace_util_0.Count(_txn_00000, 56)
	return storageValues, nil
}

// Set overrides the Transaction interface.
func (st *TxnState) Set(k kv.Key, v []byte) error {
	trace_util_0.Count(_txn_00000, 67)
	return st.buf.Set(k, v)
}

// Delete overrides the Transaction interface.
func (st *TxnState) Delete(k kv.Key) error {
	trace_util_0.Count(_txn_00000, 68)
	return st.buf.Delete(k)
}

// Iter overrides the Transaction interface.
func (st *TxnState) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_txn_00000, 69)
	bufferIt, err := st.buf.Iter(k, upperBound)
	if err != nil {
		trace_util_0.Count(_txn_00000, 72)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 70)
	retrieverIt, err := st.Transaction.Iter(k, upperBound)
	if err != nil {
		trace_util_0.Count(_txn_00000, 73)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 71)
	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse overrides the Transaction interface.
func (st *TxnState) IterReverse(k kv.Key) (kv.Iterator, error) {
	trace_util_0.Count(_txn_00000, 74)
	bufferIt, err := st.buf.IterReverse(k)
	if err != nil {
		trace_util_0.Count(_txn_00000, 77)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 75)
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		trace_util_0.Count(_txn_00000, 78)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 76)
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

func (st *TxnState) cleanup() {
	trace_util_0.Count(_txn_00000, 79)
	st.buf.Reset()
	for key := range st.mutations {
		trace_util_0.Count(_txn_00000, 81)
		delete(st.mutations, key)
	}
	trace_util_0.Count(_txn_00000, 80)
	if st.dirtyTableOP != nil {
		trace_util_0.Count(_txn_00000, 82)
		empty := dirtyTableOperation{}
		for i := 0; i < len(st.dirtyTableOP); i++ {
			trace_util_0.Count(_txn_00000, 84)
			st.dirtyTableOP[i] = empty
		}
		trace_util_0.Count(_txn_00000, 83)
		st.dirtyTableOP = st.dirtyTableOP[:0]
	}
}

// KeysNeedToLock returns the keys need to be locked.
func (st *TxnState) KeysNeedToLock() ([]kv.Key, error) {
	trace_util_0.Count(_txn_00000, 85)
	keys := make([]kv.Key, 0, st.buf.Len())
	if err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
		trace_util_0.Count(_txn_00000, 87)
		if !keyNeedToLock(k, v) {
			trace_util_0.Count(_txn_00000, 89)
			return nil
		}
		// If the key is already locked, it will be deduplicated in LockKeys method later.
		// The statement MemBuffer will be reused, so we must copy the key here.
		trace_util_0.Count(_txn_00000, 88)
		keys = append(keys, append([]byte{}, k...))
		return nil
	}); err != nil {
		trace_util_0.Count(_txn_00000, 90)
		return nil, err
	}
	trace_util_0.Count(_txn_00000, 86)
	return keys, nil
}

func keyNeedToLock(k, v []byte) bool {
	trace_util_0.Count(_txn_00000, 91)
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		trace_util_0.Count(_txn_00000, 94)
		// meta key always need to lock.
		return true
	}
	trace_util_0.Count(_txn_00000, 92)
	isDelete := len(v) == 0
	if isDelete {
		trace_util_0.Count(_txn_00000, 95)
		// only need to delete row key.
		return k[10] == 'r'
	}
	trace_util_0.Count(_txn_00000, 93)
	isNonUniqueIndex := len(v) == 1 && v[0] == '0'
	// Put row key and unique index need to lock.
	return !isNonUniqueIndex
}

func getBinlogMutation(ctx sessionctx.Context, tableID int64) *binlog.TableMutation {
	trace_util_0.Count(_txn_00000, 96)
	bin := binloginfo.GetPrewriteValue(ctx, true)
	for i := range bin.Mutations {
		trace_util_0.Count(_txn_00000, 98)
		if bin.Mutations[i].TableId == tableID {
			trace_util_0.Count(_txn_00000, 99)
			return &bin.Mutations[i]
		}
	}
	trace_util_0.Count(_txn_00000, 97)
	idx := len(bin.Mutations)
	bin.Mutations = append(bin.Mutations, binlog.TableMutation{TableId: tableID})
	return &bin.Mutations[idx]
}

func mergeToMutation(m1, m2 *binlog.TableMutation) {
	trace_util_0.Count(_txn_00000, 100)
	m1.InsertedRows = append(m1.InsertedRows, m2.InsertedRows...)
	m1.UpdatedRows = append(m1.UpdatedRows, m2.UpdatedRows...)
	m1.DeletedIds = append(m1.DeletedIds, m2.DeletedIds...)
	m1.DeletedPks = append(m1.DeletedPks, m2.DeletedPks...)
	m1.DeletedRows = append(m1.DeletedRows, m2.DeletedRows...)
	m1.Sequence = append(m1.Sequence, m2.Sequence...)
}

func mergeToDirtyDB(dirtyDB *executor.DirtyDB, op dirtyTableOperation) {
	trace_util_0.Count(_txn_00000, 101)
	dt := dirtyDB.GetDirtyTable(op.tid)
	switch op.kind {
	case table.DirtyTableAddRow:
		trace_util_0.Count(_txn_00000, 102)
		dt.AddRow(op.handle, op.row)
	case table.DirtyTableDeleteRow:
		trace_util_0.Count(_txn_00000, 103)
		dt.DeleteRow(op.handle)
	case table.DirtyTableTruncate:
		trace_util_0.Count(_txn_00000, 104)
		dt.TruncateTable()
	}
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage

	mockFail bool
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	trace_util_0.Count(_txn_00000, 105)
	if tf.mockFail {
		trace_util_0.Count(_txn_00000, 108)
		return nil, errors.New("mock get timestamp fail")
	}

	trace_util_0.Count(_txn_00000, 106)
	startTS, err := tf.future.Wait()
	if err == nil {
		trace_util_0.Count(_txn_00000, 109)
		return tf.store.BeginWithStartTS(startTS)
	}

	// It would retry get timestamp.
	trace_util_0.Count(_txn_00000, 107)
	return tf.store.Begin()
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
	trace_util_0.Count(_txn_00000, 110)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_txn_00000, 114)
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	trace_util_0.Count(_txn_00000, 111)
	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		trace_util_0.Count(_txn_00000, 115)
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx)
	} else {
		trace_util_0.Count(_txn_00000, 116)
		{
			tsFuture = oracleStore.GetTimestampAsync(ctx)
		}
	}
	trace_util_0.Count(_txn_00000, 112)
	ret := &txnFuture{future: tsFuture, store: s.store}
	if x := ctx.Value("mockGetTSFail"); x != nil {
		trace_util_0.Count(_txn_00000, 117)
		ret.mockFail = true
	}
	trace_util_0.Count(_txn_00000, 113)
	return ret
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() error {
	trace_util_0.Count(_txn_00000, 118)
	defer s.txn.cleanup()
	st := &s.txn
	var count int
	err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
		trace_util_0.Count(_txn_00000, 123)
		failpoint.Inject("mockStmtCommitError", func(val failpoint.Value) {
			trace_util_0.Count(_txn_00000, 127)
			if val.(bool) {
				trace_util_0.Count(_txn_00000, 128)
				count++
			}
		})

		trace_util_0.Count(_txn_00000, 124)
		if count > 3 {
			trace_util_0.Count(_txn_00000, 129)
			return errors.New("mock stmt commit error")
		}

		trace_util_0.Count(_txn_00000, 125)
		if len(v) == 0 {
			trace_util_0.Count(_txn_00000, 130)
			return st.Transaction.Delete(k)
		}
		trace_util_0.Count(_txn_00000, 126)
		return st.Transaction.Set(k, v)
	})
	trace_util_0.Count(_txn_00000, 119)
	if err != nil {
		trace_util_0.Count(_txn_00000, 131)
		st.doNotCommit = err
		return err
	}

	// Need to flush binlog.
	trace_util_0.Count(_txn_00000, 120)
	for tableID, delta := range st.mutations {
		trace_util_0.Count(_txn_00000, 132)
		mutation := getBinlogMutation(s, tableID)
		mergeToMutation(mutation, delta)
	}

	trace_util_0.Count(_txn_00000, 121)
	if len(st.dirtyTableOP) > 0 {
		trace_util_0.Count(_txn_00000, 133)
		dirtyDB := executor.GetDirtyDB(s)
		for _, op := range st.dirtyTableOP {
			trace_util_0.Count(_txn_00000, 134)
			mergeToDirtyDB(dirtyDB, op)
		}
	}
	trace_util_0.Count(_txn_00000, 122)
	return nil
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	trace_util_0.Count(_txn_00000, 135)
	s.txn.cleanup()
	return
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	trace_util_0.Count(_txn_00000, 136)
	st := &s.txn
	if _, ok := st.mutations[tableID]; !ok {
		trace_util_0.Count(_txn_00000, 138)
		st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	trace_util_0.Count(_txn_00000, 137)
	return st.mutations[tableID]
}

func (s *session) StmtAddDirtyTableOP(op int, tid int64, handle int64, row []types.Datum) {
	trace_util_0.Count(_txn_00000, 139)
	s.txn.dirtyTableOP = append(s.txn.dirtyTableOP, dirtyTableOperation{op, tid, handle, row})
}

var _txn_00000 = "session/txn.go"
