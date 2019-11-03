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

package bindinfo

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// BindHandle is used to handle all global sql bind operations.
type BindHandle struct {
	sctx struct {
		sync.Mutex
		sessionctx.Context
	}

	// bindInfo caches the sql bind info from storage.
	//
	// The Mutex protects that there is only one goroutine changes the content
	// of atmoic.Value.
	//
	// NOTE: Concurrent Value Write:
	//
	//    bindInfo.Lock()
	//    newCache := bindInfo.Value.Load()
	//    do the write operation on the newCache
	//    bindInfo.Value.Store(newCache)
	//
	// NOTE: Concurrent Value Read:
	//
	//    cache := bindInfo.Load().
	//    read the content
	//
	bindInfo struct {
		sync.Mutex
		atomic.Value
		parser *parser.Parser
	}

	// invalidBindRecordMap indicates the invalid bind records found during querying.
	// A record will be deleted from this map, after 2 bind-lease, after it is dropped from the kv.
	invalidBindRecordMap struct {
		sync.Mutex
		atomic.Value
	}

	lastUpdateTime types.Time
}

// Lease influences the duration of loading bind info and handling invalid bind.
var Lease = 3 * time.Second

type invalidBindRecordMap struct {
	bindRecord  *BindRecord
	droppedTime time.Time
}

// NewBindHandle creates a new BindHandle.
func NewBindHandle(ctx sessionctx.Context) *BindHandle {
	trace_util_0.Count(_handle_00000, 0)
	handle := &BindHandle{}
	handle.sctx.Context = ctx
	handle.bindInfo.Value.Store(make(cache, 32))
	handle.bindInfo.parser = parser.New()
	handle.invalidBindRecordMap.Value.Store(make(map[string]*invalidBindRecordMap))
	return handle
}

// Update updates the global sql bind cache.
func (h *BindHandle) Update(fullLoad bool) (err error) {
	trace_util_0.Count(_handle_00000, 1)
	sql := "select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info"
	if !fullLoad {
		trace_util_0.Count(_handle_00000, 6)
		sql += " where update_time >= \"" + h.lastUpdateTime.String() + "\""
	}

	// No need to acquire the session context lock for ExecRestrictedSQL, it
	// uses another background session.
	trace_util_0.Count(_handle_00000, 2)
	rows, _, err := h.sctx.Context.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(nil, sql)
	if err != nil {
		trace_util_0.Count(_handle_00000, 7)
		return err
	}

	// Make sure there is only one goroutine writes the cache.
	trace_util_0.Count(_handle_00000, 3)
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		trace_util_0.Count(_handle_00000, 8)
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	trace_util_0.Count(_handle_00000, 4)
	for _, row := range rows {
		trace_util_0.Count(_handle_00000, 9)
		hash, meta, err := h.newBindMeta(newBindRecord(row))
		// Update lastUpdateTime to the newest one.
		if meta.UpdateTime.Compare(h.lastUpdateTime) > 0 {
			trace_util_0.Count(_handle_00000, 12)
			h.lastUpdateTime = meta.UpdateTime
		}
		trace_util_0.Count(_handle_00000, 10)
		if err != nil {
			trace_util_0.Count(_handle_00000, 13)
			logutil.Logger(context.Background()).Error("update bindinfo failed", zap.Error(err))
			continue
		}

		trace_util_0.Count(_handle_00000, 11)
		newCache.removeStaleBindMetas(hash, meta)
		if meta.Status == Using {
			trace_util_0.Count(_handle_00000, 14)
			newCache[hash] = append(newCache[hash], meta)
		}
	}
	trace_util_0.Count(_handle_00000, 5)
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindMeta to the cache.
func (h *BindHandle) AddBindRecord(record *BindRecord) (err error) {
	trace_util_0.Count(_handle_00000, 15)
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	h.sctx.Lock()
	_, err = exec.Execute(context.TODO(), "BEGIN")
	if err != nil {
		trace_util_0.Count(_handle_00000, 20)
		h.sctx.Unlock()
		return
	}

	trace_util_0.Count(_handle_00000, 16)
	defer func() {
		trace_util_0.Count(_handle_00000, 21)
		if err != nil {
			trace_util_0.Count(_handle_00000, 25)
			_, err1 := exec.Execute(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		trace_util_0.Count(_handle_00000, 22)
		_, err = exec.Execute(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			trace_util_0.Count(_handle_00000, 26)
			return
		}

		// Make sure there is only one goroutine writes the cache and use parser.
		trace_util_0.Count(_handle_00000, 23)
		h.bindInfo.Lock()
		// update the BindMeta to the cache.
		hash, meta, err1 := h.newBindMeta(record)
		if err1 != nil {
			trace_util_0.Count(_handle_00000, 27)
			err = err1
			h.bindInfo.Unlock()
			return
		}

		trace_util_0.Count(_handle_00000, 24)
		h.appendBindMeta(hash, meta)
		h.bindInfo.Unlock()
	}()

	// remove all the unused sql binds.
	trace_util_0.Count(_handle_00000, 17)
	_, err = exec.Execute(context.TODO(), h.deleteBindInfoSQL(record.OriginalSQL, record.Db))
	if err != nil {
		trace_util_0.Count(_handle_00000, 28)
		return err
	}

	trace_util_0.Count(_handle_00000, 18)
	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		trace_util_0.Count(_handle_00000, 29)
		return err1
	}
	trace_util_0.Count(_handle_00000, 19)
	record.CreateTime = types.Time{
		Time: types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.UpdateTime = record.CreateTime
	record.Status = Using
	record.BindSQL = h.getEscapeCharacter(record.BindSQL)

	// insert the BindRecord to the storage.
	_, err = exec.Execute(context.TODO(), h.insertBindInfoSQL(record))
	return err
}

// DropBindRecord drops a BindRecord to the storage and BindMeta int the cache.
func (h *BindHandle) DropBindRecord(record *BindRecord) (err error) {
	trace_util_0.Count(_handle_00000, 30)
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	h.sctx.Lock()

	_, err = exec.Execute(context.TODO(), "BEGIN")
	if err != nil {
		trace_util_0.Count(_handle_00000, 34)
		h.sctx.Unlock()
		return
	}

	trace_util_0.Count(_handle_00000, 31)
	defer func() {
		trace_util_0.Count(_handle_00000, 35)
		if err != nil {
			trace_util_0.Count(_handle_00000, 38)
			_, err1 := exec.Execute(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		trace_util_0.Count(_handle_00000, 36)
		_, err = exec.Execute(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			trace_util_0.Count(_handle_00000, 39)
			return
		}

		trace_util_0.Count(_handle_00000, 37)
		hash, meta := newBindMetaWithoutAst(record)
		h.removeBindMeta(hash, meta)
	}()

	trace_util_0.Count(_handle_00000, 32)
	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		trace_util_0.Count(_handle_00000, 40)
		return err1
	}

	trace_util_0.Count(_handle_00000, 33)
	updateTs := types.Time{
		Time: types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.Status = deleted
	record.UpdateTime = updateTs

	_, err = exec.Execute(context.TODO(), h.logicalDeleteBindInfoSQL(record.OriginalSQL, record.Db, updateTs))
	return err
}

// DropInvalidBindRecord execute the drop bindRecord task.
func (h *BindHandle) DropInvalidBindRecord() {
	trace_util_0.Count(_handle_00000, 41)
	invalidBindRecordMap := copyInvalidBindRecordMap(h.invalidBindRecordMap.Load().(map[string]*invalidBindRecordMap))
	for key, invalidBindRecord := range invalidBindRecordMap {
		trace_util_0.Count(_handle_00000, 43)
		if invalidBindRecord.droppedTime.IsZero() {
			trace_util_0.Count(_handle_00000, 45)
			err := h.DropBindRecord(invalidBindRecord.bindRecord)
			if err != nil {
				trace_util_0.Count(_handle_00000, 47)
				logutil.Logger(context.Background()).Error("DropInvalidBindRecord failed", zap.Error(err))
			}
			trace_util_0.Count(_handle_00000, 46)
			invalidBindRecord.droppedTime = time.Now()
			continue
		}

		trace_util_0.Count(_handle_00000, 44)
		if time.Since(invalidBindRecord.droppedTime) > 6*time.Second {
			trace_util_0.Count(_handle_00000, 48)
			delete(invalidBindRecordMap, key)
		}
	}
	trace_util_0.Count(_handle_00000, 42)
	h.invalidBindRecordMap.Store(invalidBindRecordMap)
}

// AddDropInvalidBindTask add bindRecord to invalidBindRecordMap when the bindRecord need to be deleted.
func (h *BindHandle) AddDropInvalidBindTask(invalidBindRecord *BindRecord) {
	trace_util_0.Count(_handle_00000, 49)
	key := invalidBindRecord.OriginalSQL + ":" + invalidBindRecord.Db
	if _, ok := h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap)[key]; ok {
		trace_util_0.Count(_handle_00000, 52)
		return
	}
	trace_util_0.Count(_handle_00000, 50)
	h.invalidBindRecordMap.Lock()
	defer h.invalidBindRecordMap.Unlock()
	if _, ok := h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap)[key]; ok {
		trace_util_0.Count(_handle_00000, 53)
		return
	}
	trace_util_0.Count(_handle_00000, 51)
	newMap := copyInvalidBindRecordMap(h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap))
	newMap[key] = &invalidBindRecordMap{
		bindRecord: invalidBindRecord,
	}
	h.invalidBindRecordMap.Store(newMap)
}

// Size return the size of bind info cache.
func (h *BindHandle) Size() int {
	trace_util_0.Count(_handle_00000, 54)
	size := 0
	for _, bindRecords := range h.bindInfo.Load().(cache) {
		trace_util_0.Count(_handle_00000, 56)
		size += len(bindRecords)
	}
	trace_util_0.Count(_handle_00000, 55)
	return size
}

// GetBindRecord return the bindMeta of the (normdOrigSQL,db) if bindMeta exist.
func (h *BindHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindMeta {
	trace_util_0.Count(_handle_00000, 57)
	return h.bindInfo.Load().(cache).getBindRecord(hash, normdOrigSQL, db)
}

// GetAllBindRecord return all bind record in cache.
func (h *BindHandle) GetAllBindRecord() (bindRecords []*BindMeta) {
	trace_util_0.Count(_handle_00000, 58)
	bindRecordMap := h.bindInfo.Load().(cache)
	for _, bindRecord := range bindRecordMap {
		trace_util_0.Count(_handle_00000, 60)
		bindRecords = append(bindRecords, bindRecord...)
	}
	trace_util_0.Count(_handle_00000, 59)
	return bindRecords
}

func (h *BindHandle) newBindMeta(record *BindRecord) (hash string, meta *BindMeta, err error) {
	trace_util_0.Count(_handle_00000, 61)
	hash = parser.DigestHash(record.OriginalSQL)
	stmtNodes, _, err := h.bindInfo.parser.Parse(record.BindSQL, record.Charset, record.Collation)
	if err != nil {
		trace_util_0.Count(_handle_00000, 63)
		return "", nil, err
	}
	trace_util_0.Count(_handle_00000, 62)
	meta = &BindMeta{BindRecord: record, Ast: stmtNodes[0]}
	return hash, meta, nil
}

func newBindMetaWithoutAst(record *BindRecord) (hash string, meta *BindMeta) {
	trace_util_0.Count(_handle_00000, 64)
	hash = parser.DigestHash(record.OriginalSQL)
	meta = &BindMeta{BindRecord: record}
	return hash, meta
}

// appendBindMeta addes the BindMeta to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *BindHandle) appendBindMeta(hash string, meta *BindMeta) {
	trace_util_0.Count(_handle_00000, 65)
	newCache := h.bindInfo.Value.Load().(cache).copy()
	newCache.removeStaleBindMetas(hash, meta)
	newCache[hash] = append(newCache[hash], meta)
	h.bindInfo.Value.Store(newCache)
}

// removeBindMeta removes the BindMeta from the cache.
func (h *BindHandle) removeBindMeta(hash string, meta *BindMeta) {
	trace_util_0.Count(_handle_00000, 66)
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		trace_util_0.Count(_handle_00000, 68)
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	trace_util_0.Count(_handle_00000, 67)
	newCache.removeDeletedBindMeta(hash, meta)
}

// removeDeletedBindMeta removes all the BindMeta which originSQL and db are the same with the parameter's meta.
func (c cache) removeDeletedBindMeta(hash string, meta *BindMeta) {
	trace_util_0.Count(_handle_00000, 69)
	metas, ok := c[hash]
	if !ok {
		trace_util_0.Count(_handle_00000, 71)
		return
	}

	trace_util_0.Count(_handle_00000, 70)
	for i := len(metas) - 1; i >= 0; i-- {
		trace_util_0.Count(_handle_00000, 72)
		if meta.isSame(meta) {
			trace_util_0.Count(_handle_00000, 73)
			metas = append(metas[:i], metas[i+1:]...)
			if len(metas) == 0 {
				trace_util_0.Count(_handle_00000, 74)
				delete(c, hash)
				return
			}
		}
	}
}

// removeStaleBindMetas removes all the stale BindMeta in the cache.
func (c cache) removeStaleBindMetas(hash string, meta *BindMeta) {
	trace_util_0.Count(_handle_00000, 75)
	metas, ok := c[hash]
	if !ok {
		trace_util_0.Count(_handle_00000, 77)
		return
	}

	// remove stale bindMetas.
	trace_util_0.Count(_handle_00000, 76)
	for i := len(metas) - 1; i >= 0; i-- {
		trace_util_0.Count(_handle_00000, 78)
		if metas[i].isStale(meta) {
			trace_util_0.Count(_handle_00000, 79)
			metas = append(metas[:i], metas[i+1:]...)
			if len(metas) == 0 {
				trace_util_0.Count(_handle_00000, 80)
				delete(c, hash)
				return
			}
		}
	}
}

func (c cache) copy() cache {
	trace_util_0.Count(_handle_00000, 81)
	newCache := make(cache, len(c))
	for k, v := range c {
		trace_util_0.Count(_handle_00000, 83)
		newCache[k] = v
	}
	trace_util_0.Count(_handle_00000, 82)
	return newCache
}

func copyInvalidBindRecordMap(oldMap map[string]*invalidBindRecordMap) map[string]*invalidBindRecordMap {
	trace_util_0.Count(_handle_00000, 84)
	newMap := make(map[string]*invalidBindRecordMap, len(oldMap))
	for k, v := range oldMap {
		trace_util_0.Count(_handle_00000, 86)
		newMap[k] = v
	}
	trace_util_0.Count(_handle_00000, 85)
	return newMap
}

func (c cache) getBindRecord(hash, normdOrigSQL, db string) *BindMeta {
	trace_util_0.Count(_handle_00000, 87)
	bindRecords := c[hash]
	if bindRecords != nil {
		trace_util_0.Count(_handle_00000, 89)
		for _, bindRecord := range bindRecords {
			trace_util_0.Count(_handle_00000, 90)
			if bindRecord.OriginalSQL == normdOrigSQL && bindRecord.Db == db {
				trace_util_0.Count(_handle_00000, 91)
				return bindRecord
			}
		}
	}
	trace_util_0.Count(_handle_00000, 88)
	return nil
}

// isStale checks whether this BindMeta is stale compared with the other BindMeta.
func (m *BindMeta) isStale(other *BindMeta) bool {
	trace_util_0.Count(_handle_00000, 92)
	return m.OriginalSQL == other.OriginalSQL && m.Db == other.Db &&
		m.UpdateTime.Compare(other.UpdateTime) <= 0
}

func (m *BindMeta) isSame(other *BindMeta) bool {
	trace_util_0.Count(_handle_00000, 93)
	return m.OriginalSQL == other.OriginalSQL && m.Db == other.Db
}

func (h *BindHandle) deleteBindInfoSQL(normdOrigSQL, db string) string {
	trace_util_0.Count(_handle_00000, 94)
	return fmt.Sprintf(
		"DELETE FROM mysql.bind_info WHERE original_sql='%s' AND default_db='%s'",
		normdOrigSQL,
		db,
	)
}

func (h *BindHandle) insertBindInfoSQL(record *BindRecord) string {
	trace_util_0.Count(_handle_00000, 95)
	return fmt.Sprintf(`INSERT INTO mysql.bind_info VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s')`,
		record.OriginalSQL,
		record.BindSQL,
		record.Db,
		record.Status,
		record.CreateTime,
		record.UpdateTime,
		record.Charset,
		record.Collation,
	)
}

func (h *BindHandle) logicalDeleteBindInfoSQL(normdOrigSQL, db string, updateTs types.Time) string {
	trace_util_0.Count(_handle_00000, 96)
	return fmt.Sprintf(`UPDATE mysql.bind_info SET status='%s',update_time='%s' WHERE original_sql='%s' and default_db='%s'`,
		deleted,
		updateTs,
		normdOrigSQL,
		db)
}

func (h *BindHandle) getEscapeCharacter(str string) string {
	trace_util_0.Count(_handle_00000, 97)
	var buffer bytes.Buffer
	for _, v := range str {
		trace_util_0.Count(_handle_00000, 99)
		if v == '\'' || v == '"' || v == '\\' {
			trace_util_0.Count(_handle_00000, 101)
			buffer.WriteString("\\")
		}
		trace_util_0.Count(_handle_00000, 100)
		buffer.WriteString(string(v))
	}
	trace_util_0.Count(_handle_00000, 98)
	return buffer.String()
}

var _handle_00000 = "bindinfo/handle.go"
