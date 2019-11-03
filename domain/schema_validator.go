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

package domain

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type checkResult int

const (
	// ResultSucc means schemaValidator's check is passing.
	ResultSucc checkResult = iota
	// ResultFail means schemaValidator's check is fail.
	ResultFail
	// ResultUnknown means schemaValidator doesn't know the check would be success or fail.
	ResultUnknown
)

// SchemaValidator is the interface for checking the validity of schema version.
type SchemaValidator interface {
	// Update the schema validator, add a new item, delete the expired deltaSchemaInfos.
	// The latest schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed table IDs to the new schema information,
	// which is produced when the oldSchemaVer is updated to the newSchemaVer.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, changedTableIDs []int64)
	// Check is it valid for a transaction to use schemaVer and related tables, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64, relatedTableIDs []int64) checkResult
	// Stop stops checking the valid of transaction.
	Stop()
	// Restart restarts the schema validator after it is stopped.
	Restart()
	// Reset resets SchemaValidator to initial state.
	Reset()
	// IsStarted indicates whether SchemaValidator is started.
	IsStarted() bool
}

type deltaSchemaInfo struct {
	schemaVersion   int64
	relatedTableIDs []int64
}

type schemaValidator struct {
	isStarted          bool
	mux                sync.RWMutex
	lease              time.Duration
	latestSchemaVer    int64
	latestSchemaExpire time.Time
	// deltaSchemaInfos is a queue that maintain the history of changes.
	deltaSchemaInfos []deltaSchemaInfo
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration) SchemaValidator {
	trace_util_0.Count(_schema_validator_00000, 0)
	return &schemaValidator{
		isStarted:        true,
		lease:            lease,
		deltaSchemaInfos: make([]deltaSchemaInfo, 0, maxNumberOfDiffsToLoad),
	}
}

func (s *schemaValidator) IsStarted() bool {
	trace_util_0.Count(_schema_validator_00000, 1)
	s.mux.Lock()
	isStarted := s.isStarted
	s.mux.Unlock()
	return isStarted
}

func (s *schemaValidator) Stop() {
	trace_util_0.Count(_schema_validator_00000, 2)
	logutil.Logger(context.Background()).Info("the schema validator stops")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = false
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = make([]deltaSchemaInfo, 0, maxNumberOfDiffsToLoad)
}

func (s *schemaValidator) Restart() {
	trace_util_0.Count(_schema_validator_00000, 3)
	logutil.Logger(context.Background()).Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
}

func (s *schemaValidator) Reset() {
	trace_util_0.Count(_schema_validator_00000, 4)
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = make([]deltaSchemaInfo, 0, maxNumberOfDiffsToLoad)
}

func (s *schemaValidator) Update(leaseGrantTS uint64, oldVer, currVer int64, changedTableIDs []int64) {
	trace_util_0.Count(_schema_validator_00000, 5)
	s.mux.Lock()
	defer s.mux.Unlock()

	if !s.isStarted {
		trace_util_0.Count(_schema_validator_00000, 7)
		logutil.Logger(context.Background()).Info("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	trace_util_0.Count(_schema_validator_00000, 6)
	s.latestSchemaVer = currVer
	leaseGrantTime := oracle.GetTimeFromTS(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	s.latestSchemaExpire = leaseExpire

	// Update the schema deltaItem information.
	if currVer != oldVer {
		trace_util_0.Count(_schema_validator_00000, 8)
		logutil.Logger(context.Background()).Debug("update schema validator", zap.Int64("oldVer", oldVer),
			zap.Int64("currVer", currVer), zap.Int64s("changedTableIDs", changedTableIDs))
		s.enqueue(currVer, changedTableIDs)
	}
}

func hasRelatedTableID(relatedTableIDs, updateTableIDs []int64) bool {
	trace_util_0.Count(_schema_validator_00000, 9)
	for _, tblID := range updateTableIDs {
		trace_util_0.Count(_schema_validator_00000, 11)
		for _, relatedTblID := range relatedTableIDs {
			trace_util_0.Count(_schema_validator_00000, 12)
			if tblID == relatedTblID {
				trace_util_0.Count(_schema_validator_00000, 13)
				return true
			}
		}
	}
	trace_util_0.Count(_schema_validator_00000, 10)
	return false
}

// isRelatedTablesChanged returns the result whether relatedTableIDs is changed
// from usedVer to the latest schema version.
// NOTE, this function should be called under lock!
func (s *schemaValidator) isRelatedTablesChanged(currVer int64, tableIDs []int64) bool {
	trace_util_0.Count(_schema_validator_00000, 14)
	if len(s.deltaSchemaInfos) == 0 {
		trace_util_0.Count(_schema_validator_00000, 18)
		logutil.Logger(context.Background()).Info("schema change history is empty", zap.Int64("currVer", currVer))
		return true
	}
	trace_util_0.Count(_schema_validator_00000, 15)
	newerDeltas := s.findNewerDeltas(currVer)
	if len(newerDeltas) == len(s.deltaSchemaInfos) {
		trace_util_0.Count(_schema_validator_00000, 19)
		logutil.Logger(context.Background()).Info("the schema version is much older than the latest version", zap.Int64("currVer", currVer),
			zap.Int64("latestSchemaVer", s.latestSchemaVer))
		return true
	}
	trace_util_0.Count(_schema_validator_00000, 16)
	for _, item := range newerDeltas {
		trace_util_0.Count(_schema_validator_00000, 20)
		if hasRelatedTableID(item.relatedTableIDs, tableIDs) {
			trace_util_0.Count(_schema_validator_00000, 21)
			return true
		}
	}
	trace_util_0.Count(_schema_validator_00000, 17)
	return false
}

func (s *schemaValidator) findNewerDeltas(currVer int64) []deltaSchemaInfo {
	trace_util_0.Count(_schema_validator_00000, 22)
	q := s.deltaSchemaInfos
	pos := len(q)
	for i := len(q) - 1; i >= 0 && q[i].schemaVersion > currVer; i-- {
		trace_util_0.Count(_schema_validator_00000, 24)
		pos = i
	}
	trace_util_0.Count(_schema_validator_00000, 23)
	return q[pos:]
}

// Check checks schema validity, returns true if use schemaVer and related tables at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64, relatedTableIDs []int64) checkResult {
	trace_util_0.Count(_schema_validator_00000, 25)
	s.mux.RLock()
	defer s.mux.RUnlock()
	if !s.isStarted {
		trace_util_0.Count(_schema_validator_00000, 30)
		logutil.Logger(context.Background()).Info("the schema validator stopped before checking")
		return ResultUnknown
	}
	trace_util_0.Count(_schema_validator_00000, 26)
	if s.lease == 0 {
		trace_util_0.Count(_schema_validator_00000, 31)
		return ResultSucc
	}

	// Schema changed, result decided by whether related tables change.
	trace_util_0.Count(_schema_validator_00000, 27)
	if schemaVer < s.latestSchemaVer {
		trace_util_0.Count(_schema_validator_00000, 32)
		// The DDL relatedTableIDs is empty.
		if len(relatedTableIDs) == 0 {
			trace_util_0.Count(_schema_validator_00000, 35)
			logutil.Logger(context.Background()).Info("the related table ID is empty", zap.Int64("schemaVer", schemaVer),
				zap.Int64("latestSchemaVer", s.latestSchemaVer))
			return ResultFail
		}

		trace_util_0.Count(_schema_validator_00000, 33)
		if s.isRelatedTablesChanged(schemaVer, relatedTableIDs) {
			trace_util_0.Count(_schema_validator_00000, 36)
			return ResultFail
		}
		trace_util_0.Count(_schema_validator_00000, 34)
		return ResultSucc
	}

	// Schema unchanged, maybe success or the schema validator is unavailable.
	trace_util_0.Count(_schema_validator_00000, 28)
	t := oracle.GetTimeFromTS(txnTS)
	if t.After(s.latestSchemaExpire) {
		trace_util_0.Count(_schema_validator_00000, 37)
		return ResultUnknown
	}
	trace_util_0.Count(_schema_validator_00000, 29)
	return ResultSucc
}

func (s *schemaValidator) enqueue(schemaVersion int64, relatedTableIDs []int64) {
	trace_util_0.Count(_schema_validator_00000, 38)
	s.deltaSchemaInfos = append(s.deltaSchemaInfos, deltaSchemaInfo{schemaVersion, relatedTableIDs})
	if len(s.deltaSchemaInfos) > maxNumberOfDiffsToLoad {
		trace_util_0.Count(_schema_validator_00000, 39)
		s.deltaSchemaInfos = s.deltaSchemaInfos[1:]
	}
}

var _schema_validator_00000 = "domain/schema_validator.go"
