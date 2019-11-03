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

package variable

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/cpuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
)

const (
	codeCantGetValidID terror.ErrCode = 1
	codeCantSetToNull  terror.ErrCode = 2
	codeSnapshotTooOld terror.ErrCode = 3
)

var preparedStmtCount int64

// Error instances.
var (
	errCantGetValidID = terror.ClassVariable.New(codeCantGetValidID, "cannot get valid auto-increment id in retry")
	ErrCantSetToNull  = terror.ClassVariable.New(codeCantSetToNull, "cannot set variable to null")
	ErrSnapshotTooOld = terror.ClassVariable.New(codeSnapshotTooOld, "snapshot is older than GC safe point %s")
)

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	currRetryOff           int
	autoIncrementIDs       []int64
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	trace_util_0.Count(_session_00000, 0)
	r.currRetryOff = 0
	if len(r.autoIncrementIDs) > 0 {
		trace_util_0.Count(_session_00000, 2)
		r.autoIncrementIDs = r.autoIncrementIDs[:0]
	}
	trace_util_0.Count(_session_00000, 1)
	if len(r.DroppedPreparedStmtIDs) > 0 {
		trace_util_0.Count(_session_00000, 3)
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// AddAutoIncrementID adds id to AutoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	trace_util_0.Count(_session_00000, 4)
	r.autoIncrementIDs = append(r.autoIncrementIDs, id)
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	trace_util_0.Count(_session_00000, 5)
	r.currRetryOff = 0
}

// GetCurrAutoIncrementID gets current AutoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	trace_util_0.Count(_session_00000, 6)
	if r.currRetryOff >= len(r.autoIncrementIDs) {
		trace_util_0.Count(_session_00000, 8)
		return 0, errCantGetValidID
	}
	trace_util_0.Count(_session_00000, 7)
	id := r.autoIncrementIDs[r.currRetryOff]
	r.currRetryOff++

	return id, nil
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	ForUpdate     bool
	forUpdateTS   uint64
	DirtyDB       interface{}
	Binlog        interface{}
	InfoSchema    interface{}
	History       interface{}
	SchemaVersion int64
	StartTS       uint64
	Shard         *int64
	TableDeltaMap map[int64]TableDelta
	IsPessimistic bool

	// For metrics.
	CreateTime     time.Time
	StatementCount int
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(tableID int64, delta int64, count int64, colSize map[int64]int64) {
	trace_util_0.Count(_session_00000, 9)
	if tc.TableDeltaMap == nil {
		trace_util_0.Count(_session_00000, 13)
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	trace_util_0.Count(_session_00000, 10)
	item := tc.TableDeltaMap[tableID]
	if item.ColSize == nil && colSize != nil {
		trace_util_0.Count(_session_00000, 14)
		item.ColSize = make(map[int64]int64)
	}
	trace_util_0.Count(_session_00000, 11)
	item.Delta += delta
	item.Count += count
	for key, val := range colSize {
		trace_util_0.Count(_session_00000, 15)
		item.ColSize[key] += val
	}
	trace_util_0.Count(_session_00000, 12)
	tc.TableDeltaMap[tableID] = item
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	trace_util_0.Count(_session_00000, 16)
	//tc.InfoSchema = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.DirtyDB = nil
	tc.Binlog = nil
	tc.History = nil
	tc.TableDeltaMap = nil
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	trace_util_0.Count(_session_00000, 17)
	tc.TableDeltaMap = nil
}

// GetForUpdateTS returns the ts for update.
func (tc *TransactionContext) GetForUpdateTS() uint64 {
	trace_util_0.Count(_session_00000, 18)
	if tc.forUpdateTS > tc.StartTS {
		trace_util_0.Count(_session_00000, 20)
		return tc.forUpdateTS
	}
	trace_util_0.Count(_session_00000, 19)
	return tc.StartTS
}

// SetForUpdateTS sets the ts for update.
func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
	trace_util_0.Count(_session_00000, 21)
	if forUpdateTS > tc.forUpdateTS {
		trace_util_0.Count(_session_00000, 22)
		tc.forUpdateTS = forUpdateTS
	}
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// BufStore stores temp KVs for a row when executing insert statement.
	// We could reuse a BufStore for multiple rows of a session to reduce memory allocations.
	BufStore *kv.BufferStore
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	trace_util_0.Count(_session_00000, 23)
	ib.BufStore = nil
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency
	MemQuota
	BatchSize
	RetryLimit          int64
	DisableTxnAutoRetry bool
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]string
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]*ast.Prepared
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// params for prepared statements
	PreparedParams []types.Datum

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	// retry information
	RetryInfo *RetryInfo
	// Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *kv.Variables

	// TxnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	TxnIsolationLevelOneShot struct {
		// state 0 means default
		// state 1 means it's set in current transaction.
		// state 2 means it should be used in current transaction.
		State int
		Value string
	}

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// CurrentDB is the default database of this session.
	CurrentDB string

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

	// SnapshotInfoschema is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	SnapshotInfoschema interface{}

	// BinlogClient is used to write binlog.
	BinlogClient *pumpcli.PumpsClient

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowWriteRowID can be set to false to forbid write data to _tidb_rowid.
	// This variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	AllowInSubqToJoinAndAgg bool

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	/* TiDB system variables */

	// LightningMode is true when the lightning use the kvencoder to transfer sql to raw kv.
	LightningMode bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// BatchCommit indicates if we should split the transaction into multiple batches.
	BatchCommit bool

	// IDAllocator is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Table.alloc.
	IDAllocator autoid.Allocator

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int

	// EnableTablePartition enables table partition feature.
	EnableTablePartition string

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// WaitTableSplitFinish defines the create table pre-split behaviour is sync or async.
	WaitTableSplitFinish bool

	// EnableStreaming indicates whether the coprocessor request can use streaming API.
	// TODO: remove this after tidb-server configuration "enable-streaming' removed.
	EnableStreaming bool

	writeStmtBufs WriteStmtBufs

	// L2CacheSize indicates the size of CPU L2 cache, using byte as unit.
	L2CacheSize int

	// EnableRadixJoin indicates whether to use radix hash join to execute
	// HashJoin.
	EnableRadixJoin bool

	// ConstraintCheckInPlace indicates whether to check the constraint when the SQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current session is doing.
	CommandValue uint32

	// TIDBOptJoinOrderAlgoThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	TiDBOptJoinReorderThreshold int

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	SlowQueryFile string

	// EnableFastAnalyze indicates whether to take fast analyze.
	EnableFastAnalyze bool

	// TxnMode indicates should be pessimistic or optimistic.
	TxnMode string

	// LowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds.
	LowResolutionTSO bool

	// Killed is a flag to indicate that this query is killed.
	Killed uint32
}

// ConnectionInfo present connection used by audit.
type ConnectionInfo struct {
	ConnectionID      uint32
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	DB                string
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	trace_util_0.Count(_session_00000, 24)
	vars := &SessionVars{
		Users:                       make(map[string]string),
		systems:                     make(map[string]string),
		PreparedStmts:               make(map[uint32]*ast.Prepared),
		PreparedStmtNameToID:        make(map[string]uint32),
		PreparedParams:              make([]types.Datum, 0, 10),
		TxnCtx:                      &TransactionContext{},
		KVVars:                      kv.NewVariables(),
		RetryInfo:                   &RetryInfo{},
		ActiveRoles:                 make([]*auth.RoleIdentity, 0, 10),
		StrictSQLMode:               true,
		Status:                      mysql.ServerStatusAutocommit,
		StmtCtx:                     new(stmtctx.StatementContext),
		AllowAggPushDown:            false,
		OptimizerSelectivityLevel:   DefTiDBOptimizerSelectivityLevel,
		RetryLimit:                  DefTiDBRetryLimit,
		DisableTxnAutoRetry:         DefTiDBDisableTxnAutoRetry,
		DDLReorgPriority:            kv.PriorityLow,
		AllowInSubqToJoinAndAgg:     DefOptInSubqToJoinAndAgg,
		CorrelationThreshold:        DefOptCorrelationThreshold,
		CorrelationExpFactor:        DefOptCorrelationExpFactor,
		EnableRadixJoin:             false,
		L2CacheSize:                 cpuid.CPU.Cache.L2,
		CommandValue:                uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold: DefTiDBOptJoinReorderThreshold,
		SlowQueryFile:               config.GetGlobalConfig().Log.SlowQueryFile,
	}
	vars.Concurrency = Concurrency{
		IndexLookupConcurrency:     DefIndexLookupConcurrency,
		IndexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		IndexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		HashJoinConcurrency:        DefTiDBHashJoinConcurrency,
		ProjectionConcurrency:      DefTiDBProjectionConcurrency,
		DistSQLScanConcurrency:     DefDistSQLScanConcurrency,
		HashAggPartialConcurrency:  DefTiDBHashAggPartialConcurrency,
		HashAggFinalConcurrency:    DefTiDBHashAggFinalConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:             config.GetGlobalConfig().MemQuotaQuery,
		MemQuotaHashJoin:          DefTiDBMemQuotaHashJoin,
		MemQuotaMergeJoin:         DefTiDBMemQuotaMergeJoin,
		MemQuotaSort:              DefTiDBMemQuotaSort,
		MemQuotaTopn:              DefTiDBMemQuotaTopn,
		MemQuotaIndexLookupReader: DefTiDBMemQuotaIndexLookupReader,
		MemQuotaIndexLookupJoin:   DefTiDBMemQuotaIndexLookupJoin,
		MemQuotaNestedLoopApply:   DefTiDBMemQuotaNestedLoopApply,
		MemQuotaDistSQL:           DefTiDBMemQuotaDistSQL,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		InitChunkSize:      DefInitChunkSize,
		MaxChunkSize:       DefMaxChunkSize,
		DMLBatchSize:       DefDMLBatchSize,
	}
	var enableStreaming string
	if config.GetGlobalConfig().EnableStreaming {
		trace_util_0.Count(_session_00000, 26)
		enableStreaming = "1"
	} else {
		trace_util_0.Count(_session_00000, 27)
		{
			enableStreaming = "0"
		}
	}
	trace_util_0.Count(_session_00000, 25)
	terror.Log(vars.SetSystemVar(TiDBEnableStreaming, enableStreaming))
	return vars
}

// GetWriteStmtBufs get pointer of SessionVars.writeStmtBufs.
func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
	trace_util_0.Count(_session_00000, 28)
	return &s.writeStmtBufs
}

// CleanBuffers cleans the temporary bufs
func (s *SessionVars) CleanBuffers() {
	trace_util_0.Count(_session_00000, 29)
	if !s.LightningMode {
		trace_util_0.Count(_session_00000, 30)
		s.GetWriteStmtBufs().clean()
	}
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	trace_util_0.Count(_session_00000, 31)
	s.PlanColumnID++
	return s.PlanColumnID
}

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	trace_util_0.Count(_session_00000, 32)
	charset = s.systems[CharacterSetConnection]
	collation = s.systems[CollationConnection]
	return
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	trace_util_0.Count(_session_00000, 33)
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the session server status variable.
// If on is ture sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	trace_util_0.Count(_session_00000, 34)
	if on {
		trace_util_0.Count(_session_00000, 36)
		s.Status |= flag
		return
	}
	trace_util_0.Count(_session_00000, 35)
	s.Status &= ^flag
}

// GetStatusFlag gets the session server status variable, returns true if it is on.
func (s *SessionVars) GetStatusFlag(flag uint16) bool {
	trace_util_0.Count(_session_00000, 37)
	return s.Status&flag > 0
}

// InTxn returns if the session is in transaction.
func (s *SessionVars) InTxn() bool {
	trace_util_0.Count(_session_00000, 38)
	return s.GetStatusFlag(mysql.ServerStatusInTrans)
}

// IsAutocommit returns if the session is set to autocommit.
func (s *SessionVars) IsAutocommit() bool {
	trace_util_0.Count(_session_00000, 39)
	return s.GetStatusFlag(mysql.ServerStatusAutocommit)
}

// GetNextPreparedStmtID generates and returns the next session scope prepared statement id.
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	trace_util_0.Count(_session_00000, 40)
	s.preparedStmtID++
	return s.preparedStmtID
}

// Location returns the value of time_zone session variable. If it is nil, then return time.Local.
func (s *SessionVars) Location() *time.Location {
	trace_util_0.Count(_session_00000, 41)
	loc := s.TimeZone
	if loc == nil {
		trace_util_0.Count(_session_00000, 43)
		loc = timeutil.SystemLocation()
	}
	trace_util_0.Count(_session_00000, 42)
	return loc
}

// GetExecuteArgumentsInfo gets the argument list as a string of execute statement.
func (s *SessionVars) GetExecuteArgumentsInfo() string {
	trace_util_0.Count(_session_00000, 44)
	if len(s.PreparedParams) == 0 {
		trace_util_0.Count(_session_00000, 47)
		return ""
	}
	trace_util_0.Count(_session_00000, 45)
	args := make([]string, 0, len(s.PreparedParams))
	for _, v := range s.PreparedParams {
		trace_util_0.Count(_session_00000, 48)
		if v.IsNull() {
			trace_util_0.Count(_session_00000, 49)
			args = append(args, "<nil>")
		} else {
			trace_util_0.Count(_session_00000, 50)
			{
				str, err := v.ToString()
				if err != nil {
					trace_util_0.Count(_session_00000, 52)
					terror.Log(err)
				}
				trace_util_0.Count(_session_00000, 51)
				args = append(args, str)
			}
		}
	}
	trace_util_0.Count(_session_00000, 46)
	return fmt.Sprintf(" [arguments: %s]", strings.Join(args, ", "))
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	trace_util_0.Count(_session_00000, 53)
	val, ok := s.systems[name]
	return val, ok
}

// deleteSystemVar deletes a system variable.
func (s *SessionVars) deleteSystemVar(name string) error {
	trace_util_0.Count(_session_00000, 54)
	if name != CharacterSetResults {
		trace_util_0.Count(_session_00000, 56)
		return ErrCantSetToNull
	}
	trace_util_0.Count(_session_00000, 55)
	delete(s.systems, name)
	return nil
}

func (s *SessionVars) setDDLReorgPriority(val string) {
	trace_util_0.Count(_session_00000, 57)
	val = strings.ToLower(val)
	switch val {
	case "priority_low":
		trace_util_0.Count(_session_00000, 58)
		s.DDLReorgPriority = kv.PriorityLow
	case "priority_normal":
		trace_util_0.Count(_session_00000, 59)
		s.DDLReorgPriority = kv.PriorityNormal
	case "priority_high":
		trace_util_0.Count(_session_00000, 60)
		s.DDLReorgPriority = kv.PriorityHigh
	default:
		trace_util_0.Count(_session_00000, 61)
		s.DDLReorgPriority = kv.PriorityLow
	}
}

// AddPreparedStmt adds prepareStmt to current session and count in global.
func (s *SessionVars) AddPreparedStmt(stmtID uint32, stmt *ast.Prepared) error {
	trace_util_0.Count(_session_00000, 62)
	if _, exists := s.PreparedStmts[stmtID]; !exists {
		trace_util_0.Count(_session_00000, 64)
		valStr, _ := s.GetSystemVar(MaxPreparedStmtCount)
		maxPreparedStmtCount, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			trace_util_0.Count(_session_00000, 67)
			maxPreparedStmtCount = DefMaxPreparedStmtCount
		}
		trace_util_0.Count(_session_00000, 65)
		newPreparedStmtCount := atomic.AddInt64(&preparedStmtCount, 1)
		if maxPreparedStmtCount >= 0 && newPreparedStmtCount > maxPreparedStmtCount {
			trace_util_0.Count(_session_00000, 68)
			atomic.AddInt64(&preparedStmtCount, -1)
			return ErrMaxPreparedStmtCountReached.GenWithStackByArgs(maxPreparedStmtCount)
		}
		trace_util_0.Count(_session_00000, 66)
		metrics.PreparedStmtGauge.Set(float64(newPreparedStmtCount))
	}
	trace_util_0.Count(_session_00000, 63)
	s.PreparedStmts[stmtID] = stmt
	return nil
}

// RemovePreparedStmt removes preparedStmt from current session and decrease count in global.
func (s *SessionVars) RemovePreparedStmt(stmtID uint32) {
	trace_util_0.Count(_session_00000, 69)
	_, exists := s.PreparedStmts[stmtID]
	if !exists {
		trace_util_0.Count(_session_00000, 71)
		return
	}
	trace_util_0.Count(_session_00000, 70)
	delete(s.PreparedStmts, stmtID)
	afterMinus := atomic.AddInt64(&preparedStmtCount, -1)
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// WithdrawAllPreparedStmt remove all preparedStmt in current session and decrease count in global.
func (s *SessionVars) WithdrawAllPreparedStmt() {
	trace_util_0.Count(_session_00000, 72)
	psCount := len(s.PreparedStmts)
	if psCount == 0 {
		trace_util_0.Count(_session_00000, 74)
		return
	}
	trace_util_0.Count(_session_00000, 73)
	afterMinus := atomic.AddInt64(&preparedStmtCount, -int64(psCount))
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// SetSystemVar sets the value of a system variable.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	trace_util_0.Count(_session_00000, 75)
	switch name {
	case TxnIsolationOneShot:
		trace_util_0.Count(_session_00000, 77)
		switch val {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			trace_util_0.Count(_session_00000, 141)
			skipIsolationLevelCheck, err := GetSessionSystemVar(s, TiDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(val)
			if err != nil {
				trace_util_0.Count(_session_00000, 144)
				returnErr = err
			}
			trace_util_0.Count(_session_00000, 142)
			if !TiDBOptOn(skipIsolationLevelCheck) || err != nil {
				trace_util_0.Count(_session_00000, 145)
				return returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			trace_util_0.Count(_session_00000, 143)
			if name == "transaction_isolation" {
				trace_util_0.Count(_session_00000, 146)
				s.StmtCtx.AppendWarning(returnErr)
			}
		}
		trace_util_0.Count(_session_00000, 78)
		s.TxnIsolationLevelOneShot.State = 1
		s.TxnIsolationLevelOneShot.Value = val
	case TimeZone:
		trace_util_0.Count(_session_00000, 79)
		tz, err := parseTimeZone(val)
		if err != nil {
			trace_util_0.Count(_session_00000, 147)
			return err
		}
		trace_util_0.Count(_session_00000, 80)
		s.TimeZone = tz
	case SQLModeVar:
		trace_util_0.Count(_session_00000, 81)
		val = mysql.FormatSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := mysql.GetSQLMode(val)
		if err2 != nil {
			trace_util_0.Count(_session_00000, 148)
			return errors.Trace(err2)
		}
		trace_util_0.Count(_session_00000, 82)
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case TiDBSnapshot:
		trace_util_0.Count(_session_00000, 83)
		err := setSnapshotTS(s, val)
		if err != nil {
			trace_util_0.Count(_session_00000, 149)
			return err
		}
	case AutoCommit:
		trace_util_0.Count(_session_00000, 84)
		isAutocommit := TiDBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			trace_util_0.Count(_session_00000, 150)
			s.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case TiDBSkipUTF8Check:
		trace_util_0.Count(_session_00000, 85)
		s.SkipUTF8Check = TiDBOptOn(val)
	case TiDBOptAggPushDown:
		trace_util_0.Count(_session_00000, 86)
		s.AllowAggPushDown = TiDBOptOn(val)
	case TiDBOptWriteRowID:
		trace_util_0.Count(_session_00000, 87)
		s.AllowWriteRowID = TiDBOptOn(val)
	case TiDBOptInSubqToJoinAndAgg:
		trace_util_0.Count(_session_00000, 88)
		s.AllowInSubqToJoinAndAgg = TiDBOptOn(val)
	case TiDBOptCorrelationThreshold:
		trace_util_0.Count(_session_00000, 89)
		s.CorrelationThreshold = tidbOptFloat64(val, DefOptCorrelationThreshold)
	case TiDBOptCorrelationExpFactor:
		trace_util_0.Count(_session_00000, 90)
		s.CorrelationExpFactor = int(tidbOptInt64(val, DefOptCorrelationExpFactor))
	case TiDBIndexLookupConcurrency:
		trace_util_0.Count(_session_00000, 91)
		s.IndexLookupConcurrency = tidbOptPositiveInt32(val, DefIndexLookupConcurrency)
	case TiDBIndexLookupJoinConcurrency:
		trace_util_0.Count(_session_00000, 92)
		s.IndexLookupJoinConcurrency = tidbOptPositiveInt32(val, DefIndexLookupJoinConcurrency)
	case TiDBIndexJoinBatchSize:
		trace_util_0.Count(_session_00000, 93)
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, DefIndexJoinBatchSize)
	case TiDBIndexLookupSize:
		trace_util_0.Count(_session_00000, 94)
		s.IndexLookupSize = tidbOptPositiveInt32(val, DefIndexLookupSize)
	case TiDBHashJoinConcurrency:
		trace_util_0.Count(_session_00000, 95)
		s.HashJoinConcurrency = tidbOptPositiveInt32(val, DefTiDBHashJoinConcurrency)
	case TiDBProjectionConcurrency:
		trace_util_0.Count(_session_00000, 96)
		s.ProjectionConcurrency = tidbOptInt64(val, DefTiDBProjectionConcurrency)
	case TiDBHashAggPartialConcurrency:
		trace_util_0.Count(_session_00000, 97)
		s.HashAggPartialConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggPartialConcurrency)
	case TiDBHashAggFinalConcurrency:
		trace_util_0.Count(_session_00000, 98)
		s.HashAggFinalConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggFinalConcurrency)
	case TiDBDistSQLScanConcurrency:
		trace_util_0.Count(_session_00000, 99)
		s.DistSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
	case TiDBIndexSerialScanConcurrency:
		trace_util_0.Count(_session_00000, 100)
		s.IndexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
	case TiDBBackoffLockFast:
		trace_util_0.Count(_session_00000, 101)
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, kv.DefBackoffLockFast)
	case TiDBBackOffWeight:
		trace_util_0.Count(_session_00000, 102)
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, kv.DefBackOffWeight)
	case TiDBConstraintCheckInPlace:
		trace_util_0.Count(_session_00000, 103)
		s.ConstraintCheckInPlace = TiDBOptOn(val)
	case TiDBBatchInsert:
		trace_util_0.Count(_session_00000, 104)
		s.BatchInsert = TiDBOptOn(val)
	case TiDBBatchDelete:
		trace_util_0.Count(_session_00000, 105)
		s.BatchDelete = TiDBOptOn(val)
	case TiDBBatchCommit:
		trace_util_0.Count(_session_00000, 106)
		s.BatchCommit = TiDBOptOn(val)
	case TiDBDMLBatchSize:
		trace_util_0.Count(_session_00000, 107)
		s.DMLBatchSize = tidbOptPositiveInt32(val, DefDMLBatchSize)
	case TiDBCurrentTS, TiDBConfig:
		trace_util_0.Count(_session_00000, 108)
		return ErrReadOnly
	case TiDBMaxChunkSize:
		trace_util_0.Count(_session_00000, 109)
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
	case TiDBInitChunkSize:
		trace_util_0.Count(_session_00000, 110)
		s.InitChunkSize = tidbOptPositiveInt32(val, DefInitChunkSize)
	case TIDBMemQuotaQuery:
		trace_util_0.Count(_session_00000, 111)
		s.MemQuotaQuery = tidbOptInt64(val, config.GetGlobalConfig().MemQuotaQuery)
	case TIDBMemQuotaHashJoin:
		trace_util_0.Count(_session_00000, 112)
		s.MemQuotaHashJoin = tidbOptInt64(val, DefTiDBMemQuotaHashJoin)
	case TIDBMemQuotaMergeJoin:
		trace_util_0.Count(_session_00000, 113)
		s.MemQuotaMergeJoin = tidbOptInt64(val, DefTiDBMemQuotaMergeJoin)
	case TIDBMemQuotaSort:
		trace_util_0.Count(_session_00000, 114)
		s.MemQuotaSort = tidbOptInt64(val, DefTiDBMemQuotaSort)
	case TIDBMemQuotaTopn:
		trace_util_0.Count(_session_00000, 115)
		s.MemQuotaTopn = tidbOptInt64(val, DefTiDBMemQuotaTopn)
	case TIDBMemQuotaIndexLookupReader:
		trace_util_0.Count(_session_00000, 116)
		s.MemQuotaIndexLookupReader = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupReader)
	case TIDBMemQuotaIndexLookupJoin:
		trace_util_0.Count(_session_00000, 117)
		s.MemQuotaIndexLookupJoin = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupJoin)
	case TIDBMemQuotaNestedLoopApply:
		trace_util_0.Count(_session_00000, 118)
		s.MemQuotaNestedLoopApply = tidbOptInt64(val, DefTiDBMemQuotaNestedLoopApply)
	case TiDBGeneralLog:
		trace_util_0.Count(_session_00000, 119)
		atomic.StoreUint32(&ProcessGeneralLog, uint32(tidbOptPositiveInt32(val, DefTiDBGeneralLog)))
	case TiDBSlowLogThreshold:
		trace_util_0.Count(_session_00000, 120)
		atomic.StoreUint64(&config.GetGlobalConfig().Log.SlowThreshold, uint64(tidbOptInt64(val, logutil.DefaultSlowThreshold)))
	case TiDBDDLSlowOprThreshold:
		trace_util_0.Count(_session_00000, 121)
		atomic.StoreUint32(&DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, DefTiDBDDLSlowOprThreshold)))
	case TiDBQueryLogMaxLen:
		trace_util_0.Count(_session_00000, 122)
		atomic.StoreUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen, uint64(tidbOptInt64(val, logutil.DefaultQueryLogMaxLen)))
	case TiDBRetryLimit:
		trace_util_0.Count(_session_00000, 123)
		s.RetryLimit = tidbOptInt64(val, DefTiDBRetryLimit)
	case TiDBDisableTxnAutoRetry:
		trace_util_0.Count(_session_00000, 124)
		s.DisableTxnAutoRetry = TiDBOptOn(val)
	case TiDBEnableStreaming:
		trace_util_0.Count(_session_00000, 125)
		s.EnableStreaming = TiDBOptOn(val)
	case TiDBEnableCascadesPlanner:
		trace_util_0.Count(_session_00000, 126)
		s.EnableCascadesPlanner = TiDBOptOn(val)
	case TiDBOptimizerSelectivityLevel:
		trace_util_0.Count(_session_00000, 127)
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
	case TiDBEnableTablePartition:
		trace_util_0.Count(_session_00000, 128)
		s.EnableTablePartition = val
	case TiDBDDLReorgPriority:
		trace_util_0.Count(_session_00000, 129)
		s.setDDLReorgPriority(val)
	case TiDBForcePriority:
		trace_util_0.Count(_session_00000, 130)
		atomic.StoreInt32(&ForcePriority, int32(mysql.Str2Priority(val)))
	case TiDBEnableRadixJoin:
		trace_util_0.Count(_session_00000, 131)
		s.EnableRadixJoin = TiDBOptOn(val)
	case TiDBEnableWindowFunction:
		trace_util_0.Count(_session_00000, 132)
		s.EnableWindowFunction = TiDBOptOn(val)
	case TiDBOptJoinReorderThreshold:
		trace_util_0.Count(_session_00000, 133)
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
	case TiDBCheckMb4ValueInUTF8:
		trace_util_0.Count(_session_00000, 134)
		config.GetGlobalConfig().CheckMb4ValueInUTF8 = TiDBOptOn(val)
	case TiDBSlowQueryFile:
		trace_util_0.Count(_session_00000, 135)
		s.SlowQueryFile = val
	case TiDBEnableFastAnalyze:
		trace_util_0.Count(_session_00000, 136)
		s.EnableFastAnalyze = TiDBOptOn(val)
	case TiDBWaitTableSplitFinish:
		trace_util_0.Count(_session_00000, 137)
		s.WaitTableSplitFinish = TiDBOptOn(val)
	case TiDBExpensiveQueryTimeThreshold:
		trace_util_0.Count(_session_00000, 138)
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveQueryTimeThreshold)))
	case TiDBTxnMode:
		trace_util_0.Count(_session_00000, 139)
		if err := s.setTxnMode(val); err != nil {
			trace_util_0.Count(_session_00000, 151)
			return err
		}
	case TiDBLowResolutionTSO:
		trace_util_0.Count(_session_00000, 140)
		s.LowResolutionTSO = TiDBOptOn(val)
	}
	trace_util_0.Count(_session_00000, 76)
	s.systems[name] = val
	return nil
}

func (s *SessionVars) setTxnMode(val string) error {
	trace_util_0.Count(_session_00000, 152)
	switch strings.ToUpper(val) {
	case ast.Pessimistic:
		trace_util_0.Count(_session_00000, 154)
		s.TxnMode = ast.Pessimistic
	case ast.Optimistic:
		trace_util_0.Count(_session_00000, 155)
		s.TxnMode = ast.Optimistic
	case "":
		trace_util_0.Count(_session_00000, 156)
		s.TxnMode = ""
	default:
		trace_util_0.Count(_session_00000, 157)
		return ErrWrongValueForVar.FastGenByArgs(TiDBTxnMode, val)
	}
	trace_util_0.Count(_session_00000, 153)
	return nil
}

// SetLocalSystemVar sets values of the local variables which in "server" scope.
func SetLocalSystemVar(name string, val string) {
	trace_util_0.Count(_session_00000, 158)
	switch name {
	case TiDBDDLReorgWorkerCount:
		trace_util_0.Count(_session_00000, 159)
		SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)))
	case TiDBDDLReorgBatchSize:
		trace_util_0.Count(_session_00000, 160)
		SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgBatchSize)))
	case TiDBDDLErrorCountLimit:
		trace_util_0.Count(_session_00000, 161)
		SetDDLErrorCountLimit(tidbOptInt64(val, DefTiDBDDLErrorCountLimit))
	}
}

// special session variables.
const (
	SQLModeVar           = "sql_mode"
	CharacterSetResults  = "character_set_results"
	MaxAllowedPacket     = "max_allowed_packet"
	TimeZone             = "time_zone"
	TxnIsolation         = "tx_isolation"
	TransactionIsolation = "transaction_isolation"
	TxnIsolationOneShot  = "tx_isolation_one_shot"
)

// these variables are useless for TiDB, but still need to validate their values for some compatible issues.
// TODO: some more variables need to be added here.
const (
	serverReadOnly = "read_only"
)

var (
	// TxIsolationNames are the valid values of the variable "tx_isolation" or "transaction_isolation".
	TxIsolationNames = map[string]struct{}{
		"READ-UNCOMMITTED": {},
		"READ-COMMITTED":   {},
		"REPEATABLE-READ":  {},
		"SERIALIZABLE":     {},
	}
)

// TableDelta stands for the changed count for one table.
type TableDelta struct {
	Delta    int64
	Count    int64
	ColSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
}

// Concurrency defines concurrency values.
type Concurrency struct {
	// IndexLookupConcurrency is the number of concurrent index lookup worker.
	IndexLookupConcurrency int

	// IndexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	IndexLookupJoinConcurrency int

	// DistSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	DistSQLScanConcurrency int

	// HashJoinConcurrency is the number of concurrent hash join outer worker.
	HashJoinConcurrency int

	// ProjectionConcurrency is the number of concurrent projection worker.
	ProjectionConcurrency int64

	// HashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	HashAggPartialConcurrency int

	// HashAggPartialConcurrency is the number of concurrent hash aggregation final worker.
	HashAggFinalConcurrency int

	// IndexSerialScanConcurrency is the number of concurrent index serial scan worker.
	IndexSerialScanConcurrency int
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64
	// MemQuotaHashJoin defines the memory quota for a hash join executor.
	MemQuotaHashJoin int64
	// MemQuotaMergeJoin defines the memory quota for a merge join executor.
	MemQuotaMergeJoin int64
	// MemQuotaSort defines the memory quota for a sort executor.
	MemQuotaSort int64
	// MemQuotaTopn defines the memory quota for a top n executor.
	MemQuotaTopn int64
	// MemQuotaIndexLookupReader defines the memory quota for a index lookup reader executor.
	MemQuotaIndexLookupReader int64
	// MemQuotaIndexLookupJoin defines the memory quota for a index lookup join executor.
	MemQuotaIndexLookupJoin int64
	// MemQuotaNestedLoopApply defines the memory quota for a nested loop apply executor.
	MemQuotaNestedLoopApply int64
	// MemQuotaDistSQL defines the memory quota for all operators in DistSQL layer like co-processor and selectResult.
	MemQuotaDistSQL int64
}

// BatchSize defines batch size values.
type BatchSize struct {
	// DMLBatchSize indicates the size of batches for DML.
	// It will be used when BatchInsert or BatchDelete is on.
	DMLBatchSize int

	// IndexJoinBatchSize is the batch size of a index lookup join.
	IndexJoinBatchSize int

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// InitChunkSize defines init row count of a Chunk during query execution.
	InitChunkSize int

	// MaxChunkSize defines max row count of a Chunk during query execution.
	MaxChunkSize int
}

const (
	// SlowLogRowPrefixStr is slow log row prefix.
	SlowLogRowPrefixStr = "# "
	// SlowLogSpaceMarkStr is slow log space mark.
	SlowLogSpaceMarkStr = ": "
	// SlowLogSQLSuffixStr is slow log suffix.
	SlowLogSQLSuffixStr = ";"
	// SlowLogTimeStr is slow log field name.
	SlowLogTimeStr = "Time"
	// SlowLogStartPrefixStr is slow log start row prefix.
	SlowLogStartPrefixStr = SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr
	// SlowLogTxnStartTSStr is slow log field name.
	SlowLogTxnStartTSStr = "Txn_start_ts"
	// SlowLogUserStr is slow log field name.
	SlowLogUserStr = "User"
	// SlowLogHostStr only for slow_query table usage.
	SlowLogHostStr = "Host"
	// SlowLogConnIDStr is slow log field name.
	SlowLogConnIDStr = "Conn_ID"
	// SlowLogQueryTimeStr is slow log field name.
	SlowLogQueryTimeStr = "Query_time"
	// SlowLogDBStr is slow log field name.
	SlowLogDBStr = "DB"
	// SlowLogIsInternalStr is slow log field name.
	SlowLogIsInternalStr = "Is_internal"
	// SlowLogIndexIDsStr is slow log field name.
	SlowLogIndexIDsStr = "Index_ids"
	// SlowLogDigestStr is slow log field name.
	SlowLogDigestStr = "Digest"
	// SlowLogQuerySQLStr is slow log field name.
	SlowLogQuerySQLStr = "Query" // use for slow log table, slow log will not print this field name but print sql directly.
	// SlowLogStatsInfoStr is plan stats info.
	SlowLogStatsInfoStr = "Stats"
	// SlowLogNumCopTasksStr is the number of cop-tasks.
	SlowLogNumCopTasksStr = "Num_cop_tasks"
	// SlowLogCopProcAvg is the average process time of all cop-tasks.
	SlowLogCopProcAvg = "Cop_proc_avg"
	// SlowLogCopProcP90 is the p90 process time of all cop-tasks.
	SlowLogCopProcP90 = "Cop_proc_p90"
	// SlowLogCopProcMax is the max process time of all cop-tasks.
	SlowLogCopProcMax = "Cop_proc_max"
	// SlowLogCopProcAddr is the address of TiKV where the cop-task which cost max process time run.
	SlowLogCopProcAddr = "Cop_proc_addr"
	// SlowLogCopWaitAvg is the average wait time of all cop-tasks.
	SlowLogCopWaitAvg = "Cop_wait_avg"
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90"
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of TiKV where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"
)

// SlowLogFormat uses for formatting slow log.
// The slow log output is like below:
// # Time: 2019-04-28T15:24:04.309074+08:00
// # Txn_start_ts: 406315658548871171
// # User: root@127.0.0.1
// # Conn_ID: 6
// # Query_time: 4.895492
// # Process_time: 0.161 Request_count: 1 Total_keys: 100001 Processed_keys: 100000
// # DB: test
// # Index_ids: [1,2]
// # Is_internal: false
// # Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # Stats: t1:1,t2:2
// # Num_cop_tasks: 10
// # Cop_process: Avg_time: 1s P90_time: 2s Max_time: 3s Max_addr: 10.6.131.78
// # Cop_wait: Avg_time: 10ms P90_time: 20ms Max_time: 30ms Max_Addr: 10.6.131.79
// # Memory_max: 4096
// select * from t_slim;
func (s *SessionVars) SlowLogFormat(txnTS uint64, costTime time.Duration, execDetail execdetails.ExecDetails, indexIDs string, digest string,
	statsInfos map[string]uint64, copTasks *stmtctx.CopTasksDetails, memMax int64, sql string) string {
	trace_util_0.Count(_session_00000, 162)
	var buf bytes.Buffer
	execDetailStr := execDetail.String()
	buf.WriteString(SlowLogRowPrefixStr + SlowLogTxnStartTSStr + SlowLogSpaceMarkStr + strconv.FormatUint(txnTS, 10) + "\n")
	if s.User != nil {
		trace_util_0.Count(_session_00000, 174)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogUserStr + SlowLogSpaceMarkStr + s.User.String() + "\n")
	}
	trace_util_0.Count(_session_00000, 163)
	if s.ConnectionID != 0 {
		trace_util_0.Count(_session_00000, 175)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogConnIDStr + SlowLogSpaceMarkStr + strconv.FormatUint(s.ConnectionID, 10) + "\n")
	}
	trace_util_0.Count(_session_00000, 164)
	buf.WriteString(SlowLogRowPrefixStr + SlowLogQueryTimeStr + SlowLogSpaceMarkStr + strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64) + "\n")
	if len(execDetailStr) > 0 {
		trace_util_0.Count(_session_00000, 176)
		buf.WriteString(SlowLogRowPrefixStr + execDetailStr + "\n")
	}
	trace_util_0.Count(_session_00000, 165)
	if len(s.CurrentDB) > 0 {
		trace_util_0.Count(_session_00000, 177)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogDBStr + SlowLogSpaceMarkStr + s.CurrentDB + "\n")
	}
	trace_util_0.Count(_session_00000, 166)
	if len(indexIDs) > 0 {
		trace_util_0.Count(_session_00000, 178)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogIndexIDsStr + SlowLogSpaceMarkStr + indexIDs + "\n")
	}
	trace_util_0.Count(_session_00000, 167)
	buf.WriteString(SlowLogRowPrefixStr + SlowLogIsInternalStr + SlowLogSpaceMarkStr + strconv.FormatBool(s.InRestrictedSQL) + "\n")
	if len(digest) > 0 {
		trace_util_0.Count(_session_00000, 179)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogDigestStr + SlowLogSpaceMarkStr + digest + "\n")
	}
	trace_util_0.Count(_session_00000, 168)
	if len(statsInfos) > 0 {
		trace_util_0.Count(_session_00000, 180)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogStatsInfoStr + SlowLogSpaceMarkStr)
		firstComma := false
		vStr := ""
		for k, v := range statsInfos {
			trace_util_0.Count(_session_00000, 182)
			if v == 0 {
				trace_util_0.Count(_session_00000, 184)
				vStr = "pseudo"
			} else {
				trace_util_0.Count(_session_00000, 185)
				{
					vStr = strconv.FormatUint(v, 10)

				}
			}
			trace_util_0.Count(_session_00000, 183)
			if firstComma {
				trace_util_0.Count(_session_00000, 186)
				buf.WriteString("," + k + ":" + vStr)
			} else {
				trace_util_0.Count(_session_00000, 187)
				{
					buf.WriteString(k + ":" + vStr)
					firstComma = true
				}
			}
		}
		trace_util_0.Count(_session_00000, 181)
		buf.WriteString("\n")
	}
	trace_util_0.Count(_session_00000, 169)
	if copTasks != nil {
		trace_util_0.Count(_session_00000, 188)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogNumCopTasksStr + SlowLogSpaceMarkStr + strconv.FormatInt(int64(copTasks.NumCopTasks), 10) + "\n")
		buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
			SlowLogCopProcAvg, SlowLogSpaceMarkStr, copTasks.AvgProcessTime.Seconds(),
			SlowLogCopProcP90, SlowLogSpaceMarkStr, copTasks.P90ProcessTime.Seconds(),
			SlowLogCopProcMax, SlowLogSpaceMarkStr, copTasks.MaxProcessTime.Seconds(),
			SlowLogCopProcAddr, SlowLogSpaceMarkStr, copTasks.MaxProcessAddress) + "\n")
		buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
			SlowLogCopWaitAvg, SlowLogSpaceMarkStr, copTasks.AvgWaitTime.Seconds(),
			SlowLogCopWaitP90, SlowLogSpaceMarkStr, copTasks.P90WaitTime.Seconds(),
			SlowLogCopWaitMax, SlowLogSpaceMarkStr, copTasks.MaxWaitTime.Seconds(),
			SlowLogCopWaitAddr, SlowLogSpaceMarkStr, copTasks.MaxWaitAddress) + "\n")
	}
	trace_util_0.Count(_session_00000, 170)
	if memMax > 0 {
		trace_util_0.Count(_session_00000, 189)
		buf.WriteString(SlowLogRowPrefixStr + SlowLogMemMax + SlowLogSpaceMarkStr + strconv.FormatInt(memMax, 10) + "\n")
	}
	trace_util_0.Count(_session_00000, 171)
	if len(sql) == 0 {
		trace_util_0.Count(_session_00000, 190)
		sql = ";"
	}
	trace_util_0.Count(_session_00000, 172)
	buf.WriteString(sql)
	if sql[len(sql)-1] != ';' {
		trace_util_0.Count(_session_00000, 191)
		buf.WriteString(";")
	}
	trace_util_0.Count(_session_00000, 173)
	return buf.String()
}

var _session_00000 = "sessionctx/variable/session.go"
