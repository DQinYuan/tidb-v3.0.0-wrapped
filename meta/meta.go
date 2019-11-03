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

package meta

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	globalIDMutex sync.Mutex
)

// Meta structure:
//	NextGlobalID -> int64
//	SchemaVersion -> int64
//	DBs -> {
//		DB:1 -> db meta data []byte
//		DB:2 -> db meta data []byte
//	}
//	DB:1 -> {
//		Table:1 -> table meta data []byte
//		Table:2 -> table meta data []byte
//		TID:1 -> int64
//		TID:2 -> int64
//	}
//

var (
	mMetaPrefix       = []byte("m")
	mNextGlobalIDKey  = []byte("NextGlobalID")
	mSchemaVersionKey = []byte("SchemaVersionKey")
	mDBs              = []byte("DBs")
	mDBPrefix         = "DB"
	mTablePrefix      = "Table"
	mTableIDPrefix    = "TID"
	mBootstrapKey     = []byte("BootstrapKey")
	mSchemaDiffPrefix = "Diff"
)

var (
	errInvalidTableKey = terror.ClassMeta.New(codeInvalidTableKey, "invalid table meta key")
	errInvalidDBKey    = terror.ClassMeta.New(codeInvalidDBKey, "invalid db key")

	// ErrDBExists is the error for db exists.
	ErrDBExists = terror.ClassMeta.New(codeDatabaseExists, "database already exists")
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = terror.ClassMeta.New(codeDatabaseNotExists, "database doesn't exist")
	// ErrTableExists is the error for table exists.
	ErrTableExists = terror.ClassMeta.New(codeTableExists, "table already exists")
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = terror.ClassMeta.New(codeTableNotExists, "table doesn't exist")
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn        *structure.TxStructure
	StartTS    uint64 // StartTS is the txn's start TS.
	jobListKey JobListKeyType
}

// NewMeta creates a Meta in transaction txn.
// If the current Meta needs to handle a job, jobListKey is the type of the job's list.
func NewMeta(txn kv.Transaction, jobListKeys ...JobListKeyType) *Meta {
	trace_util_0.Count(_meta_00000, 0)
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	txn.SetOption(kv.SyncLog, true)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	listKey := DefaultJobListKey
	if len(jobListKeys) != 0 {
		trace_util_0.Count(_meta_00000, 2)
		listKey = jobListKeys[0]
	}
	trace_util_0.Count(_meta_00000, 1)
	return &Meta{txn: t,
		StartTS:    txn.StartTS(),
		jobListKey: listKey,
	}
}

// NewSnapshotMeta creates a Meta with snapshot.
func NewSnapshotMeta(snapshot kv.Snapshot) *Meta {
	trace_util_0.Count(_meta_00000, 3)
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	return &Meta{txn: t}
}

// GenGlobalID generates next id globally.
func (m *Meta) GenGlobalID() (int64, error) {
	trace_util_0.Count(_meta_00000, 4)
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	return m.txn.Inc(mNextGlobalIDKey, 1)
}

// GetGlobalID gets current global id.
func (m *Meta) GetGlobalID() (int64, error) {
	trace_util_0.Count(_meta_00000, 5)
	return m.txn.GetInt64(mNextGlobalIDKey)
}

func (m *Meta) dbKey(dbID int64) []byte {
	trace_util_0.Count(_meta_00000, 6)
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
}

func (m *Meta) autoTableIDKey(tableID int64) []byte {
	trace_util_0.Count(_meta_00000, 7)
	return []byte(fmt.Sprintf("%s:%d", mTableIDPrefix, tableID))
}

func (m *Meta) tableKey(tableID int64) []byte {
	trace_util_0.Count(_meta_00000, 8)
	return []byte(fmt.Sprintf("%s:%d", mTablePrefix, tableID))
}

// DDLJobHistoryKey is only used for testing.
func DDLJobHistoryKey(m *Meta, jobID int64) []byte {
	trace_util_0.Count(_meta_00000, 9)
	return m.txn.EncodeHashDataKey(mDDLJobHistoryKey, m.jobIDKey(jobID))
}

// GenAutoTableIDKeyValue generates meta key by dbID, tableID and corresponding value by autoID.
func (m *Meta) GenAutoTableIDKeyValue(dbID, tableID, autoID int64) (key, value []byte) {
	trace_util_0.Count(_meta_00000, 10)
	dbKey := m.dbKey(dbID)
	autoTableIDKey := m.autoTableIDKey(tableID)
	return m.txn.EncodeHashAutoIDKeyValue(dbKey, autoTableIDKey, autoID)
}

// GenAutoTableID adds step to the auto ID of the table and returns the sum.
func (m *Meta) GenAutoTableID(dbID, tableID, step int64) (int64, error) {
	trace_util_0.Count(_meta_00000, 11)
	// Check if DB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 14)
		return 0, errors.Trace(err)
	}
	// Check if table exists.
	trace_util_0.Count(_meta_00000, 12)
	tableKey := m.tableKey(tableID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		trace_util_0.Count(_meta_00000, 15)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 13)
	return m.txn.HInc(dbKey, m.autoTableIDKey(tableID), step)
}

// GetAutoTableID gets current auto id with table id.
func (m *Meta) GetAutoTableID(dbID int64, tableID int64) (int64, error) {
	trace_util_0.Count(_meta_00000, 16)
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoTableIDKey(tableID))
}

// GetSchemaVersion gets current global schema version.
func (m *Meta) GetSchemaVersion() (int64, error) {
	trace_util_0.Count(_meta_00000, 17)
	return m.txn.GetInt64(mSchemaVersionKey)
}

// GenSchemaVersion generates next schema version.
func (m *Meta) GenSchemaVersion() (int64, error) {
	trace_util_0.Count(_meta_00000, 18)
	return m.txn.Inc(mSchemaVersionKey, 1)
}

func (m *Meta) checkDBExists(dbKey []byte) error {
	trace_util_0.Count(_meta_00000, 19)
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v == nil {
		trace_util_0.Count(_meta_00000, 21)
		err = ErrDBNotExists
	}
	trace_util_0.Count(_meta_00000, 20)
	return errors.Trace(err)
}

func (m *Meta) checkDBNotExists(dbKey []byte) error {
	trace_util_0.Count(_meta_00000, 22)
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v != nil {
		trace_util_0.Count(_meta_00000, 24)
		err = ErrDBExists
	}
	trace_util_0.Count(_meta_00000, 23)
	return errors.Trace(err)
}

func (m *Meta) checkTableExists(dbKey []byte, tableKey []byte) error {
	trace_util_0.Count(_meta_00000, 25)
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v == nil {
		trace_util_0.Count(_meta_00000, 27)
		err = ErrTableNotExists
	}
	trace_util_0.Count(_meta_00000, 26)
	return errors.Trace(err)
}

func (m *Meta) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	trace_util_0.Count(_meta_00000, 28)
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v != nil {
		trace_util_0.Count(_meta_00000, 30)
		err = ErrTableExists
	}
	trace_util_0.Count(_meta_00000, 29)
	return errors.Trace(err)
}

// CreateDatabase creates a database with db info.
func (m *Meta) CreateDatabase(dbInfo *model.DBInfo) error {
	trace_util_0.Count(_meta_00000, 31)
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBNotExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 34)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 32)
	data, err := json.Marshal(dbInfo)
	if err != nil {
		trace_util_0.Count(_meta_00000, 35)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 33)
	return m.txn.HSet(mDBs, dbKey, data)
}

// UpdateDatabase updates a database with db info.
func (m *Meta) UpdateDatabase(dbInfo *model.DBInfo) error {
	trace_util_0.Count(_meta_00000, 36)
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 39)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 37)
	data, err := json.Marshal(dbInfo)
	if err != nil {
		trace_util_0.Count(_meta_00000, 40)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 38)
	return m.txn.HSet(mDBs, dbKey, data)
}

// CreateTableOrView creates a table with tableInfo in database.
func (m *Meta) CreateTableOrView(dbID int64, tableInfo *model.TableInfo) error {
	trace_util_0.Count(_meta_00000, 41)
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 45)
		return errors.Trace(err)
	}

	// Check if table exists.
	trace_util_0.Count(_meta_00000, 42)
	tableKey := m.tableKey(tableInfo.ID)
	if err := m.checkTableNotExists(dbKey, tableKey); err != nil {
		trace_util_0.Count(_meta_00000, 46)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 43)
	data, err := json.Marshal(tableInfo)
	if err != nil {
		trace_util_0.Count(_meta_00000, 47)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 44)
	return m.txn.HSet(dbKey, tableKey, data)
}

// CreateTableAndSetAutoID creates a table with tableInfo in database,
// and rebases the table autoID.
func (m *Meta) CreateTableAndSetAutoID(dbID int64, tableInfo *model.TableInfo, autoID int64) error {
	trace_util_0.Count(_meta_00000, 48)
	err := m.CreateTableOrView(dbID, tableInfo)
	if err != nil {
		trace_util_0.Count(_meta_00000, 50)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 49)
	_, err = m.txn.HInc(m.dbKey(dbID), m.autoTableIDKey(tableInfo.ID), autoID)
	return errors.Trace(err)
}

// DropDatabase drops whole database.
func (m *Meta) DropDatabase(dbID int64) error {
	trace_util_0.Count(_meta_00000, 51)
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.txn.HClear(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 54)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 52)
	if err := m.txn.HDel(mDBs, dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 55)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 53)
	return nil
}

// DropTableOrView drops table in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the table.
// For rename table, we do not need to rename auto_increment id key-value.
func (m *Meta) DropTableOrView(dbID int64, tblID int64, delAutoID bool) error {
	trace_util_0.Count(_meta_00000, 56)
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 61)
		return errors.Trace(err)
	}

	// Check if table exists.
	trace_util_0.Count(_meta_00000, 57)
	tableKey := m.tableKey(tblID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		trace_util_0.Count(_meta_00000, 62)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 58)
	if err := m.txn.HDel(dbKey, tableKey); err != nil {
		trace_util_0.Count(_meta_00000, 63)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 59)
	if delAutoID {
		trace_util_0.Count(_meta_00000, 64)
		if err := m.txn.HDel(dbKey, m.autoTableIDKey(tblID)); err != nil {
			trace_util_0.Count(_meta_00000, 65)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_meta_00000, 60)
	return nil
}

// UpdateTable updates the table with table info.
func (m *Meta) UpdateTable(dbID int64, tableInfo *model.TableInfo) error {
	trace_util_0.Count(_meta_00000, 66)
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 70)
		return errors.Trace(err)
	}

	// Check if table exists.
	trace_util_0.Count(_meta_00000, 67)
	tableKey := m.tableKey(tableInfo.ID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		trace_util_0.Count(_meta_00000, 71)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 68)
	data, err := json.Marshal(tableInfo)
	if err != nil {
		trace_util_0.Count(_meta_00000, 72)
		return errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 69)
	err = m.txn.HSet(dbKey, tableKey, data)
	return errors.Trace(err)
}

// ListTables shows all tables in database.
func (m *Meta) ListTables(dbID int64) ([]*model.TableInfo, error) {
	trace_util_0.Count(_meta_00000, 73)
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 77)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 74)
	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		trace_util_0.Count(_meta_00000, 78)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 75)
	tables := make([]*model.TableInfo, 0, len(res)/2)
	for _, r := range res {
		trace_util_0.Count(_meta_00000, 79)
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			trace_util_0.Count(_meta_00000, 82)
			continue
		}

		trace_util_0.Count(_meta_00000, 80)
		tbInfo := &model.TableInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			trace_util_0.Count(_meta_00000, 83)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_meta_00000, 81)
		tables = append(tables, tbInfo)
	}

	trace_util_0.Count(_meta_00000, 76)
	return tables, nil
}

// ListDatabases shows all databases.
func (m *Meta) ListDatabases() ([]*model.DBInfo, error) {
	trace_util_0.Count(_meta_00000, 84)
	res, err := m.txn.HGetAll(mDBs)
	if err != nil {
		trace_util_0.Count(_meta_00000, 87)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 85)
	dbs := make([]*model.DBInfo, 0, len(res))
	for _, r := range res {
		trace_util_0.Count(_meta_00000, 88)
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			trace_util_0.Count(_meta_00000, 90)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_meta_00000, 89)
		dbs = append(dbs, dbInfo)
	}
	trace_util_0.Count(_meta_00000, 86)
	return dbs, nil
}

// GetDatabase gets the database value with ID.
func (m *Meta) GetDatabase(dbID int64) (*model.DBInfo, error) {
	trace_util_0.Count(_meta_00000, 91)
	dbKey := m.dbKey(dbID)
	value, err := m.txn.HGet(mDBs, dbKey)
	if err != nil || value == nil {
		trace_util_0.Count(_meta_00000, 93)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 92)
	dbInfo := &model.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return dbInfo, errors.Trace(err)
}

// GetTable gets the table value in database with tableID.
func (m *Meta) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
	trace_util_0.Count(_meta_00000, 94)
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		trace_util_0.Count(_meta_00000, 97)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 95)
	tableKey := m.tableKey(tableID)
	value, err := m.txn.HGet(dbKey, tableKey)
	if err != nil || value == nil {
		trace_util_0.Count(_meta_00000, 98)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 96)
	tableInfo := &model.TableInfo{}
	err = json.Unmarshal(value, tableInfo)
	return tableInfo, errors.Trace(err)
}

// DDL job structure
//	DDLJobList: list jobs
//	DDLJobHistory: hash
//	DDLJobReorg: hash
//
// for multi DDL workers, only one can become the owner
// to operate DDL jobs, and dispatch them to MR Jobs.

var (
	mDDLJobListKey    = []byte("DDLJobList")
	mDDLJobAddIdxList = []byte("DDLJobAddIdxList")
	mDDLJobHistoryKey = []byte("DDLJobHistory")
	mDDLJobReorgKey   = []byte("DDLJobReorg")
)

// JobListKeyType is a key type of the DDL job queue.
type JobListKeyType []byte

var (
	// DefaultJobListKey keeps all actions of DDL jobs except "add index".
	DefaultJobListKey JobListKeyType = mDDLJobListKey
	// AddIndexJobListKey only keeps the action of adding index.
	AddIndexJobListKey JobListKeyType = mDDLJobAddIdxList
)

func (m *Meta) enQueueDDLJob(key []byte, job *model.Job) error {
	trace_util_0.Count(_meta_00000, 99)
	b, err := job.Encode(true)
	if err == nil {
		trace_util_0.Count(_meta_00000, 101)
		err = m.txn.RPush(key, b)
	}
	trace_util_0.Count(_meta_00000, 100)
	return errors.Trace(err)
}

// EnQueueDDLJob adds a DDL job to the list.
func (m *Meta) EnQueueDDLJob(job *model.Job) error {
	trace_util_0.Count(_meta_00000, 102)
	return m.enQueueDDLJob(m.jobListKey, job)
}

func (m *Meta) deQueueDDLJob(key []byte) (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 103)
	value, err := m.txn.LPop(key)
	if err != nil || value == nil {
		trace_util_0.Count(_meta_00000, 105)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 104)
	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// DeQueueDDLJob pops a DDL job from the list.
func (m *Meta) DeQueueDDLJob() (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 106)
	return m.deQueueDDLJob(m.jobListKey)
}

func (m *Meta) getDDLJob(key []byte, index int64) (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 107)
	value, err := m.txn.LIndex(key, index)
	if err != nil || value == nil {
		trace_util_0.Count(_meta_00000, 110)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 108)
	job := &model.Job{
		// For compatibility, if the job is enqueued by old version TiDB and Priority field is omitted,
		// set the default priority to kv.PriorityLow.
		Priority: kv.PriorityLow,
	}
	err = job.Decode(value)
	// Check if the job.Priority is valid.
	if job.Priority < kv.PriorityNormal || job.Priority > kv.PriorityHigh {
		trace_util_0.Count(_meta_00000, 111)
		job.Priority = kv.PriorityLow
	}
	trace_util_0.Count(_meta_00000, 109)
	return job, errors.Trace(err)
}

// GetDDLJobByIdx returns the corresponding DDL job by the index.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetDDLJobByIdx(index int64, jobListKeys ...JobListKeyType) (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 112)
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		trace_util_0.Count(_meta_00000, 114)
		listKey = jobListKeys[0]
	}

	trace_util_0.Count(_meta_00000, 113)
	startTime := time.Now()
	job, err := m.getDDLJob(listKey, index)
	metrics.MetaHistogram.WithLabelValues(metrics.GetDDLJobByIdx, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// updateDDLJob updates the DDL job with index and key.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
func (m *Meta) updateDDLJob(index int64, job *model.Job, key []byte, updateRawArgs bool) error {
	trace_util_0.Count(_meta_00000, 115)
	b, err := job.Encode(updateRawArgs)
	if err == nil {
		trace_util_0.Count(_meta_00000, 117)
		err = m.txn.LSet(key, index, b)
	}
	trace_util_0.Count(_meta_00000, 116)
	return errors.Trace(err)
}

// UpdateDDLJob updates the DDL job with index.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) UpdateDDLJob(index int64, job *model.Job, updateRawArgs bool, jobListKeys ...JobListKeyType) error {
	trace_util_0.Count(_meta_00000, 118)
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		trace_util_0.Count(_meta_00000, 120)
		listKey = jobListKeys[0]
	}

	trace_util_0.Count(_meta_00000, 119)
	startTime := time.Now()
	err := m.updateDDLJob(index, job, listKey, updateRawArgs)
	metrics.MetaHistogram.WithLabelValues(metrics.UpdateDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// DDLJobQueueLen returns the DDL job queue length.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) DDLJobQueueLen(jobListKeys ...JobListKeyType) (int64, error) {
	trace_util_0.Count(_meta_00000, 121)
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		trace_util_0.Count(_meta_00000, 123)
		listKey = jobListKeys[0]
	}
	trace_util_0.Count(_meta_00000, 122)
	return m.txn.LLen(listKey)
}

// GetAllDDLJobsInQueue gets all DDL Jobs in the current queue.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetAllDDLJobsInQueue(jobListKeys ...JobListKeyType) ([]*model.Job, error) {
	trace_util_0.Count(_meta_00000, 124)
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		trace_util_0.Count(_meta_00000, 128)
		listKey = jobListKeys[0]
	}

	trace_util_0.Count(_meta_00000, 125)
	values, err := m.txn.LGetAll(listKey)
	if err != nil || values == nil {
		trace_util_0.Count(_meta_00000, 129)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 126)
	jobs := make([]*model.Job, 0, len(values))
	for _, val := range values {
		trace_util_0.Count(_meta_00000, 130)
		job := &model.Job{}
		err = job.Decode(val)
		if err != nil {
			trace_util_0.Count(_meta_00000, 132)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_meta_00000, 131)
		jobs = append(jobs, job)
	}

	trace_util_0.Count(_meta_00000, 127)
	return jobs, nil
}

func (m *Meta) jobIDKey(id int64) []byte {
	trace_util_0.Count(_meta_00000, 133)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Meta) reorgJobStartHandle(id int64) []byte {
	trace_util_0.Count(_meta_00000, 134)
	// There is no "_start", to make it compatible with the older TiDB versions.
	return m.jobIDKey(id)
}

func (m *Meta) reorgJobEndHandle(id int64) []byte {
	trace_util_0.Count(_meta_00000, 135)
	b := make([]byte, 8, 12)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, "_end"...)
	return b
}

func (m *Meta) reorgJobPhysicalTableID(id int64) []byte {
	trace_util_0.Count(_meta_00000, 136)
	b := make([]byte, 8, 12)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, "_pid"...)
	return b
}

func (m *Meta) addHistoryDDLJob(key []byte, job *model.Job) error {
	trace_util_0.Count(_meta_00000, 137)
	b, err := job.Encode(true)
	if err == nil {
		trace_util_0.Count(_meta_00000, 139)
		err = m.txn.HSet(key, m.jobIDKey(job.ID), b)
	}
	trace_util_0.Count(_meta_00000, 138)
	return errors.Trace(err)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Meta) AddHistoryDDLJob(job *model.Job) error {
	trace_util_0.Count(_meta_00000, 140)
	return m.addHistoryDDLJob(mDDLJobHistoryKey, job)
}

func (m *Meta) getHistoryDDLJob(key []byte, id int64) (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 141)
	value, err := m.txn.HGet(key, m.jobIDKey(id))
	if err != nil || value == nil {
		trace_util_0.Count(_meta_00000, 143)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_meta_00000, 142)
	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetHistoryDDLJob gets a history DDL job.
func (m *Meta) GetHistoryDDLJob(id int64) (*model.Job, error) {
	trace_util_0.Count(_meta_00000, 144)
	startTime := time.Now()
	job, err := m.getHistoryDDLJob(mDDLJobHistoryKey, id)
	metrics.MetaHistogram.WithLabelValues(metrics.GetHistoryDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// GetAllHistoryDDLJobs gets all history DDL jobs.
func (m *Meta) GetAllHistoryDDLJobs() ([]*model.Job, error) {
	trace_util_0.Count(_meta_00000, 145)
	pairs, err := m.txn.HGetAll(mDDLJobHistoryKey)
	if err != nil {
		trace_util_0.Count(_meta_00000, 147)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 146)
	return decodeAndSortJob(pairs)
}

// GetLastNHistoryDDLJobs gets latest N history ddl jobs.
func (m *Meta) GetLastNHistoryDDLJobs(num int) ([]*model.Job, error) {
	trace_util_0.Count(_meta_00000, 148)
	pairs, err := m.txn.HGetLastN(mDDLJobHistoryKey, num)
	if err != nil {
		trace_util_0.Count(_meta_00000, 150)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 149)
	return decodeAndSortJob(pairs)
}

func decodeAndSortJob(jobPairs []structure.HashPair) ([]*model.Job, error) {
	trace_util_0.Count(_meta_00000, 151)
	jobs := make([]*model.Job, 0, len(jobPairs))
	for _, pair := range jobPairs {
		trace_util_0.Count(_meta_00000, 153)
		job := &model.Job{}
		err := job.Decode(pair.Value)
		if err != nil {
			trace_util_0.Count(_meta_00000, 155)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_meta_00000, 154)
		jobs = append(jobs, job)
	}
	trace_util_0.Count(_meta_00000, 152)
	sorter := &jobsSorter{jobs: jobs}
	sort.Sort(sorter)
	return jobs, nil
}

// jobsSorter implements the sort.Interface interface.
type jobsSorter struct {
	jobs []*model.Job
}

func (s *jobsSorter) Swap(i, j int) {
	trace_util_0.Count(_meta_00000, 156)
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

func (s *jobsSorter) Len() int {
	trace_util_0.Count(_meta_00000, 157)
	return len(s.jobs)
}

func (s *jobsSorter) Less(i, j int) bool {
	trace_util_0.Count(_meta_00000, 158)
	return s.jobs[i].ID < s.jobs[j].ID
}

// GetBootstrapVersion returns the version of the server which boostrap the store.
// If the store is not bootstraped, the version will be zero.
func (m *Meta) GetBootstrapVersion() (int64, error) {
	trace_util_0.Count(_meta_00000, 159)
	value, err := m.txn.GetInt64(mBootstrapKey)
	return value, errors.Trace(err)
}

// FinishBootstrap finishes bootstrap.
func (m *Meta) FinishBootstrap(version int64) error {
	trace_util_0.Count(_meta_00000, 160)
	err := m.txn.Set(mBootstrapKey, []byte(fmt.Sprintf("%d", version)))
	return errors.Trace(err)
}

// UpdateDDLReorgStartHandle saves the job reorganization latest processed start handle for later resuming.
func (m *Meta) UpdateDDLReorgStartHandle(job *model.Job, startHandle int64) error {
	trace_util_0.Count(_meta_00000, 161)
	err := m.txn.HSet(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID), []byte(strconv.FormatInt(startHandle, 10)))
	return errors.Trace(err)
}

// UpdateDDLReorgHandle saves the job reorganization latest processed information for later resuming.
func (m *Meta) UpdateDDLReorgHandle(job *model.Job, startHandle, endHandle, physicalTableID int64) error {
	trace_util_0.Count(_meta_00000, 162)
	err := m.txn.HSet(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID), []byte(strconv.FormatInt(startHandle, 10)))
	if err != nil {
		trace_util_0.Count(_meta_00000, 165)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 163)
	err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobEndHandle(job.ID), []byte(strconv.FormatInt(endHandle, 10)))
	if err != nil {
		trace_util_0.Count(_meta_00000, 166)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 164)
	err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID), []byte(strconv.FormatInt(physicalTableID, 10)))
	return errors.Trace(err)
}

// RemoveDDLReorgHandle removes the job reorganization related handles.
func (m *Meta) RemoveDDLReorgHandle(job *model.Job) error {
	trace_util_0.Count(_meta_00000, 167)
	err := m.txn.HDel(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID))
	if err != nil {
		trace_util_0.Count(_meta_00000, 171)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 168)
	if err = m.txn.HDel(mDDLJobReorgKey, m.reorgJobEndHandle(job.ID)); err != nil {
		trace_util_0.Count(_meta_00000, 172)
		logutil.Logger(context.Background()).Warn("remove DDL reorg end handle", zap.Error(err))
	}
	trace_util_0.Count(_meta_00000, 169)
	if err = m.txn.HDel(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID)); err != nil {
		trace_util_0.Count(_meta_00000, 173)
		logutil.Logger(context.Background()).Warn("remove DDL reorg physical ID", zap.Error(err))
	}
	trace_util_0.Count(_meta_00000, 170)
	return nil
}

// GetDDLReorgHandle gets the latest processed DDL reorganize position.
func (m *Meta) GetDDLReorgHandle(job *model.Job) (startHandle, endHandle, physicalTableID int64, err error) {
	trace_util_0.Count(_meta_00000, 174)
	startHandle, err = m.txn.HGetInt64(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID))
	if err != nil {
		trace_util_0.Count(_meta_00000, 179)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_meta_00000, 175)
	endHandle, err = m.txn.HGetInt64(mDDLJobReorgKey, m.reorgJobEndHandle(job.ID))
	if err != nil {
		trace_util_0.Count(_meta_00000, 180)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_meta_00000, 176)
	physicalTableID, err = m.txn.HGetInt64(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID))
	if err != nil {
		trace_util_0.Count(_meta_00000, 181)
		err = errors.Trace(err)
		return
	}
	// physicalTableID may be 0, because older version TiDB (without table partition) doesn't store them.
	// update them to table's in this case.
	trace_util_0.Count(_meta_00000, 177)
	if physicalTableID == 0 {
		trace_util_0.Count(_meta_00000, 182)
		if job.ReorgMeta != nil {
			trace_util_0.Count(_meta_00000, 184)
			endHandle = job.ReorgMeta.EndHandle
		} else {
			trace_util_0.Count(_meta_00000, 185)
			{
				endHandle = math.MaxInt64
			}
		}
		trace_util_0.Count(_meta_00000, 183)
		physicalTableID = job.TableID
		logutil.Logger(context.Background()).Warn("new TiDB binary running on old TiDB DDL reorg data",
			zap.Int64("partition ID", physicalTableID),
			zap.Int64("startHandle", startHandle),
			zap.Int64("endHandle", endHandle))
	}
	trace_util_0.Count(_meta_00000, 178)
	return
}

func (m *Meta) schemaDiffKey(schemaVersion int64) []byte {
	trace_util_0.Count(_meta_00000, 186)
	return []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Meta) GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error) {
	trace_util_0.Count(_meta_00000, 187)
	diffKey := m.schemaDiffKey(schemaVersion)
	startTime := time.Now()
	data, err := m.txn.Get(diffKey)
	metrics.MetaHistogram.WithLabelValues(metrics.GetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil || len(data) == 0 {
		trace_util_0.Count(_meta_00000, 189)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 188)
	diff := &model.SchemaDiff{}
	err = json.Unmarshal(data, diff)
	return diff, errors.Trace(err)
}

// SetSchemaDiff sets the modification information on a given schema version.
func (m *Meta) SetSchemaDiff(diff *model.SchemaDiff) error {
	trace_util_0.Count(_meta_00000, 190)
	data, err := json.Marshal(diff)
	if err != nil {
		trace_util_0.Count(_meta_00000, 192)
		return errors.Trace(err)
	}
	trace_util_0.Count(_meta_00000, 191)
	diffKey := m.schemaDiffKey(diff.Version)
	startTime := time.Now()
	err = m.txn.Set(diffKey, data)
	metrics.MetaHistogram.WithLabelValues(metrics.SetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// meta error codes.
const (
	codeInvalidTableKey terror.ErrCode = 1
	codeInvalidDBKey                   = 2

	codeDatabaseExists    = 1007
	codeDatabaseNotExists = 1049
	codeTableExists       = 1050
	codeTableNotExists    = 1146
)

func init() {
	trace_util_0.Count(_meta_00000, 193)
	metaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDatabaseExists:    mysql.ErrDBCreateExists,
		codeDatabaseNotExists: mysql.ErrBadDB,
		codeTableNotExists:    mysql.ErrNoSuchTable,
		codeTableExists:       mysql.ErrTableExists,
	}
	terror.ErrClassToMySQLCodes[terror.ClassMeta] = metaMySQLErrCodes
}

var _meta_00000 = "meta/meta.go"
