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
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/trace_util_0"
)

// SchemaChecker is used for checking schema-validity.
type SchemaChecker struct {
	SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

var (
	// SchemaOutOfDateRetryInterval is the backoff time before retrying.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is the max retry count when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

// NewSchemaChecker creates a new schema checker.
func NewSchemaChecker(do *Domain, schemaVer int64, relatedTableIDs []int64) *SchemaChecker {
	trace_util_0.Count(_schema_checker_00000, 0)
	return &SchemaChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
	}
}

// Check checks the validity of the schema version.
func (s *SchemaChecker) Check(txnTS uint64) error {
	trace_util_0.Count(_schema_checker_00000, 1)
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		trace_util_0.Count(_schema_checker_00000, 3)
		result := s.SchemaValidator.Check(txnTS, s.schemaVer, s.relatedTableIDs)
		switch result {
		case ResultSucc:
			trace_util_0.Count(_schema_checker_00000, 4)
			return nil
		case ResultFail:
			trace_util_0.Count(_schema_checker_00000, 5)
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return ErrInfoSchemaChanged
		case ResultUnknown:
			trace_util_0.Count(_schema_checker_00000, 6)
			metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	trace_util_0.Count(_schema_checker_00000, 2)
	return ErrInfoSchemaExpired
}

var _schema_checker_00000 = "domain/schema_checker.go"
