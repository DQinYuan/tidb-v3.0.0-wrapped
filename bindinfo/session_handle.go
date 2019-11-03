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
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// SessionHandle is used to handle all session sql bind operations.
type SessionHandle struct {
	ch     cache
	parser *parser.Parser
}

// NewSessionBindHandle creates a new SessionBindHandle.
func NewSessionBindHandle(parser *parser.Parser) *SessionHandle {
	trace_util_0.Count(_session_handle_00000, 0)
	sessionHandle := &SessionHandle{parser: parser}
	sessionHandle.ch = make(cache)
	return sessionHandle
}

// appendBindMeta addes the BindMeta to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *SessionHandle) appendBindMeta(hash string, meta *BindMeta) {
	trace_util_0.Count(_session_handle_00000, 1)
	// Make sure there is only one goroutine writes the cache.
	h.ch.removeStaleBindMetas(hash, meta)
	h.ch[hash] = append(h.ch[hash], meta)
}

func (h *SessionHandle) newBindMeta(record *BindRecord) (hash string, meta *BindMeta, err error) {
	trace_util_0.Count(_session_handle_00000, 2)
	hash = parser.DigestHash(record.OriginalSQL)
	stmtNodes, _, err := h.parser.Parse(record.BindSQL, record.Charset, record.Collation)
	if err != nil {
		trace_util_0.Count(_session_handle_00000, 4)
		return "", nil, err
	}
	trace_util_0.Count(_session_handle_00000, 3)
	meta = &BindMeta{BindRecord: record, Ast: stmtNodes[0]}
	return hash, meta, nil
}

// AddBindRecord new a BindRecord with BindMeta, add it to the cache.
func (h *SessionHandle) AddBindRecord(record *BindRecord) error {
	trace_util_0.Count(_session_handle_00000, 5)
	record.CreateTime = types.Time{
		Time: types.FromGoTime(time.Now()),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.UpdateTime = record.CreateTime

	// update the BindMeta to the cache.
	hash, meta, err := h.newBindMeta(record)
	if err == nil {
		trace_util_0.Count(_session_handle_00000, 7)
		h.appendBindMeta(hash, meta)
	}
	trace_util_0.Count(_session_handle_00000, 6)
	return err
}

// DropBindRecord drops a BindRecord in the cache.
func (h *SessionHandle) DropBindRecord(record *BindRecord) {
	trace_util_0.Count(_session_handle_00000, 8)
	meta := &BindMeta{BindRecord: record}
	meta.Status = deleted
	hash := parser.DigestHash(record.OriginalSQL)
	h.ch.removeDeletedBindMeta(hash, meta)
	h.appendBindMeta(hash, meta)
}

// GetBindRecord return the BindMeta of the (normdOrigSQL,db) if BindMeta exist.
func (h *SessionHandle) GetBindRecord(normdOrigSQL, db string) *BindMeta {
	trace_util_0.Count(_session_handle_00000, 9)
	hash := parser.DigestHash(normdOrigSQL)
	bindRecords := h.ch[hash]
	if bindRecords != nil {
		trace_util_0.Count(_session_handle_00000, 11)
		for _, bindRecord := range bindRecords {
			trace_util_0.Count(_session_handle_00000, 12)
			if bindRecord.OriginalSQL == normdOrigSQL && bindRecord.Db == db {
				trace_util_0.Count(_session_handle_00000, 13)
				return bindRecord
			}
		}
	}
	trace_util_0.Count(_session_handle_00000, 10)
	return nil
}

// GetAllBindRecord return all session bind info.
func (h *SessionHandle) GetAllBindRecord() (bindRecords []*BindMeta) {
	trace_util_0.Count(_session_handle_00000, 14)
	for _, bindRecord := range h.ch {
		trace_util_0.Count(_session_handle_00000, 16)
		bindRecords = append(bindRecords, bindRecord...)
	}
	trace_util_0.Count(_session_handle_00000, 15)
	return bindRecords
}

// sessionBindInfoKeyType is a dummy type to avoid naming collision in context.
type sessionBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k sessionBindInfoKeyType) String() string {
	trace_util_0.Count(_session_handle_00000, 17)
	return "session_bindinfo"
}

// SessionBindInfoKeyType is a variable key for store session bind info.
const SessionBindInfoKeyType sessionBindInfoKeyType = 0

var _session_handle_00000 = "bindinfo/session_handle.go"
