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

package stmtctx

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
	// WarnLevelNote represents level "Note" for 'SHOW WARNINGS' syntax.
	WarnLevelNote = "Note"
)

// SQLWarn relates a sql warning and it's level.
type SQLWarn struct {
	Level string
	Err   error
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution

	// IsDDLJobInQueue is used to mark whether the DDL job is put into the queue.
	// If IsDDLJobInQueue is true, it means the DDL job is in the queue of storage, and it can be handled by the DDL worker.
	IsDDLJobInQueue        bool
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	InExplainStmt          bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	DupKeyAsWarning        bool
	BadNullAsWarning       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool
	UseCache               bool
	PadCharToFullLength    bool
	BatchCheck             bool
	InNullRejectCheck      bool
	AllowInvalidDate       bool

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex

		affectedRows uint64
		foundRows    uint64

		/*
			following variables are ported from 'COPY_INFO' struct of MySQL server source,
			they are used to count rows for INSERT/REPLACE/UPDATE queries:
			  If a row is inserted then the copied variable is incremented.
			  If a row is updated by the INSERT ... ON DUPLICATE KEY UPDATE and the
			     new data differs from the old one then the copied and the updated
			     variables are incremented.
			  The touched variable is incremented if a row was touched by the update part
			     of the INSERT ... ON DUPLICATE KEY UPDATE no matter whether the row
			     was actually changed or not.

			see https://github.com/mysql/mysql-server/blob/d2029238d6d9f648077664e4cdd611e231a6dc14/sql/sql_data_change.h#L60 for more details
		*/
		records uint64
		updated uint64
		copied  uint64
		touched uint64

		message           string
		warnings          []SQLWarn
		errorCount        uint16
		histogramsNotLoad bool
		execDetails       execdetails.ExecDetails
		allExecDetails    []*execdetails.ExecDetails
	}
	// PrevAffectedRows is the affected-rows value(DDL is 0, DML is the number of affected rows).
	PrevAffectedRows int64
	// PrevLastInsertID is the last insert ID of previous statement.
	PrevLastInsertID uint64
	// LastInsertID is the auto-generated ID in the current statement.
	LastInsertID uint64
	// InsertID is the given insert ID of an auto_increment column.
	InsertID uint64

	// Copied from SessionVars.TimeZone.
	TimeZone         *time.Location
	Priority         mysql.PriorityEnum
	NotFillCache     bool
	MemTracker       *memory.Tracker
	RuntimeStatsColl *execdetails.RuntimeStatsColl
	TableIDs         []int64
	IndexIDs         []int64
	NowTs            time.Time
	SysTs            time.Time
	StmtType         string
	OriginalSQL      string
	digestMemo       struct {
		sync.Once
		normalized string
		digest     string
	}
	Tables []TableEntry
}

// SQLDigest gets normalized and digest for provided sql.
// it will cache result after first calling.
func (sc *StatementContext) SQLDigest() (normalized, sqlDigest string) {
	trace_util_0.Count(_stmtctx_00000, 0)
	sc.digestMemo.Do(func() {
		trace_util_0.Count(_stmtctx_00000, 2)
		sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(sc.OriginalSQL)
	})
	trace_util_0.Count(_stmtctx_00000, 1)
	return sc.digestMemo.normalized, sc.digestMemo.digest
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 3)
	sc.mu.Lock()
	sc.mu.affectedRows += rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 4)
	sc.mu.Lock()
	rows := sc.mu.affectedRows
	sc.mu.Unlock()
	return rows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 5)
	sc.mu.Lock()
	rows := sc.mu.foundRows
	sc.mu.Unlock()
	return rows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 6)
	sc.mu.Lock()
	sc.mu.foundRows += rows
	sc.mu.Unlock()
}

// RecordRows is used to generate info message
func (sc *StatementContext) RecordRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 7)
	sc.mu.Lock()
	rows := sc.mu.records
	sc.mu.Unlock()
	return rows
}

// AddRecordRows adds record rows.
func (sc *StatementContext) AddRecordRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 8)
	sc.mu.Lock()
	sc.mu.records += rows
	sc.mu.Unlock()
}

// UpdatedRows is used to generate info message
func (sc *StatementContext) UpdatedRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 9)
	sc.mu.Lock()
	rows := sc.mu.updated
	sc.mu.Unlock()
	return rows
}

// AddUpdatedRows adds updated rows.
func (sc *StatementContext) AddUpdatedRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 10)
	sc.mu.Lock()
	sc.mu.updated += rows
	sc.mu.Unlock()
}

// CopiedRows is used to generate info message
func (sc *StatementContext) CopiedRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 11)
	sc.mu.Lock()
	rows := sc.mu.copied
	sc.mu.Unlock()
	return rows
}

// AddCopiedRows adds copied rows.
func (sc *StatementContext) AddCopiedRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 12)
	sc.mu.Lock()
	sc.mu.copied += rows
	sc.mu.Unlock()
}

// TouchedRows is used to generate info message
func (sc *StatementContext) TouchedRows() uint64 {
	trace_util_0.Count(_stmtctx_00000, 13)
	sc.mu.Lock()
	rows := sc.mu.touched
	sc.mu.Unlock()
	return rows
}

// AddTouchedRows adds touched rows.
func (sc *StatementContext) AddTouchedRows(rows uint64) {
	trace_util_0.Count(_stmtctx_00000, 14)
	sc.mu.Lock()
	sc.mu.touched += rows
	sc.mu.Unlock()
}

// GetMessage returns the extra message of the last executed command, if there is no message, it returns empty string
func (sc *StatementContext) GetMessage() string {
	trace_util_0.Count(_stmtctx_00000, 15)
	sc.mu.Lock()
	msg := sc.mu.message
	sc.mu.Unlock()
	return msg
}

// SetMessage sets the info message generated by some commands
func (sc *StatementContext) SetMessage(msg string) {
	trace_util_0.Count(_stmtctx_00000, 16)
	sc.mu.Lock()
	sc.mu.message = msg
	sc.mu.Unlock()
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []SQLWarn {
	trace_util_0.Count(_stmtctx_00000, 17)
	sc.mu.Lock()
	warns := make([]SQLWarn, len(sc.mu.warnings))
	copy(warns, sc.mu.warnings)
	sc.mu.Unlock()
	return warns
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	trace_util_0.Count(_stmtctx_00000, 18)
	if sc.InShowWarning {
		trace_util_0.Count(_stmtctx_00000, 20)
		return 0
	}
	trace_util_0.Count(_stmtctx_00000, 19)
	sc.mu.Lock()
	wc := uint16(len(sc.mu.warnings))
	sc.mu.Unlock()
	return wc
}

const zero = "0"

// NumErrorWarnings gets warning and error count.
func (sc *StatementContext) NumErrorWarnings() (ec, wc string) {
	trace_util_0.Count(_stmtctx_00000, 21)
	var (
		ecNum uint16
		wcNum int
	)
	sc.mu.Lock()
	ecNum = sc.mu.errorCount
	wcNum = len(sc.mu.warnings)
	sc.mu.Unlock()

	if ecNum == 0 {
		trace_util_0.Count(_stmtctx_00000, 24)
		ec = zero
	} else {
		trace_util_0.Count(_stmtctx_00000, 25)
		{
			ec = strconv.Itoa(int(ecNum))
		}
	}

	trace_util_0.Count(_stmtctx_00000, 22)
	if wcNum == 0 {
		trace_util_0.Count(_stmtctx_00000, 26)
		wc = zero
	} else {
		trace_util_0.Count(_stmtctx_00000, 27)
		{
			wc = strconv.Itoa(wcNum)
		}
	}
	trace_util_0.Count(_stmtctx_00000, 23)
	return
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []SQLWarn) {
	trace_util_0.Count(_stmtctx_00000, 28)
	sc.mu.Lock()
	sc.mu.warnings = warns
	for _, w := range warns {
		trace_util_0.Count(_stmtctx_00000, 30)
		if w.Level == WarnLevelError {
			trace_util_0.Count(_stmtctx_00000, 31)
			sc.mu.errorCount++
		}
	}
	trace_util_0.Count(_stmtctx_00000, 29)
	sc.mu.Unlock()
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	trace_util_0.Count(_stmtctx_00000, 32)
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		trace_util_0.Count(_stmtctx_00000, 34)
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelWarning, warn})
	}
	trace_util_0.Count(_stmtctx_00000, 33)
	sc.mu.Unlock()
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	trace_util_0.Count(_stmtctx_00000, 35)
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		trace_util_0.Count(_stmtctx_00000, 37)
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelNote, warn})
	}
	trace_util_0.Count(_stmtctx_00000, 36)
	sc.mu.Unlock()
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	trace_util_0.Count(_stmtctx_00000, 38)
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		trace_util_0.Count(_stmtctx_00000, 40)
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelError, warn})
		sc.mu.errorCount++
	}
	trace_util_0.Count(_stmtctx_00000, 39)
	sc.mu.Unlock()
}

// SetHistogramsNotLoad sets histogramsNotLoad.
func (sc *StatementContext) SetHistogramsNotLoad() {
	trace_util_0.Count(_stmtctx_00000, 41)
	sc.mu.Lock()
	sc.mu.histogramsNotLoad = true
	sc.mu.Unlock()
}

// HistogramsNotLoad gets histogramsNotLoad.
func (sc *StatementContext) HistogramsNotLoad() bool {
	trace_util_0.Count(_stmtctx_00000, 42)
	sc.mu.Lock()
	notLoad := sc.mu.histogramsNotLoad
	sc.mu.Unlock()
	return notLoad
}

// HandleTruncate ignores or returns the error based on the StatementContext state.
func (sc *StatementContext) HandleTruncate(err error) error {
	trace_util_0.Count(_stmtctx_00000, 43)
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		trace_util_0.Count(_stmtctx_00000, 47)
		return nil
	}
	trace_util_0.Count(_stmtctx_00000, 44)
	if sc.IgnoreTruncate {
		trace_util_0.Count(_stmtctx_00000, 48)
		return nil
	}
	trace_util_0.Count(_stmtctx_00000, 45)
	if sc.TruncateAsWarning {
		trace_util_0.Count(_stmtctx_00000, 49)
		sc.AppendWarning(err)
		return nil
	}
	trace_util_0.Count(_stmtctx_00000, 46)
	return err
}

// HandleOverflow treats ErrOverflow as warnings or returns the error based on the StmtCtx.OverflowAsWarning state.
func (sc *StatementContext) HandleOverflow(err error, warnErr error) error {
	trace_util_0.Count(_stmtctx_00000, 50)
	if err == nil {
		trace_util_0.Count(_stmtctx_00000, 53)
		return nil
	}

	trace_util_0.Count(_stmtctx_00000, 51)
	if sc.OverflowAsWarning {
		trace_util_0.Count(_stmtctx_00000, 54)
		sc.AppendWarning(warnErr)
		return nil
	}
	trace_util_0.Count(_stmtctx_00000, 52)
	return err
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	trace_util_0.Count(_stmtctx_00000, 55)
	sc.mu.Lock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.records = 0
	sc.mu.updated = 0
	sc.mu.copied = 0
	sc.mu.touched = 0
	sc.mu.message = ""
	sc.mu.errorCount = 0
	sc.mu.warnings = nil
	sc.mu.execDetails = execdetails.ExecDetails{}
	sc.mu.allExecDetails = make([]*execdetails.ExecDetails, 0, 4)
	sc.mu.Unlock()
	sc.TableIDs = sc.TableIDs[:0]
	sc.IndexIDs = sc.IndexIDs[:0]
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (sc *StatementContext) MergeExecDetails(details *execdetails.ExecDetails, commitDetails *execdetails.CommitDetails) {
	trace_util_0.Count(_stmtctx_00000, 56)
	sc.mu.Lock()
	if details != nil {
		trace_util_0.Count(_stmtctx_00000, 58)
		sc.mu.execDetails.ProcessTime += details.ProcessTime
		sc.mu.execDetails.WaitTime += details.WaitTime
		sc.mu.execDetails.BackoffTime += details.BackoffTime
		sc.mu.execDetails.RequestCount++
		sc.mu.execDetails.TotalKeys += details.TotalKeys
		sc.mu.execDetails.ProcessedKeys += details.ProcessedKeys
		sc.mu.allExecDetails = append(sc.mu.allExecDetails, details)
	}
	trace_util_0.Count(_stmtctx_00000, 57)
	sc.mu.execDetails.CommitDetail = commitDetails
	sc.mu.Unlock()
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	trace_util_0.Count(_stmtctx_00000, 59)
	var details execdetails.ExecDetails
	sc.mu.Lock()
	details = sc.mu.execDetails
	sc.mu.Unlock()
	return details
}

// ShouldClipToZero indicates whether values less than 0 should be clipped to 0 for unsigned integer types.
// This is the case for `insert`, `update`, `alter table` and `load data infile` statements, when not in strict SQL mode.
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
func (sc *StatementContext) ShouldClipToZero() bool {
	trace_util_0.Count(_stmtctx_00000, 60)
	// TODO: Currently altering column of integer to unsigned integer is not supported.
	// If it is supported one day, that case should be added here.
	return sc.InInsertStmt || sc.InLoadDataStmt
}

// ShouldIgnoreOverflowError indicates whether we should ignore the error when type conversion overflows,
// so we can leave it for further processing like clipping values less than 0 to 0 for unsigned integer types.
func (sc *StatementContext) ShouldIgnoreOverflowError() bool {
	trace_util_0.Count(_stmtctx_00000, 61)
	if (sc.InInsertStmt && sc.TruncateAsWarning) || sc.InLoadDataStmt {
		trace_util_0.Count(_stmtctx_00000, 63)
		return true
	}
	trace_util_0.Count(_stmtctx_00000, 62)
	return false
}

// CopTasksDetails returns some useful information of cop-tasks during execution.
func (sc *StatementContext) CopTasksDetails() *CopTasksDetails {
	trace_util_0.Count(_stmtctx_00000, 64)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := len(sc.mu.allExecDetails)
	d := &CopTasksDetails{NumCopTasks: n}
	if n == 0 {
		trace_util_0.Count(_stmtctx_00000, 68)
		return d
	}
	trace_util_0.Count(_stmtctx_00000, 65)
	d.AvgProcessTime = sc.mu.execDetails.ProcessTime / time.Duration(n)
	d.AvgWaitTime = sc.mu.execDetails.WaitTime / time.Duration(n)

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		trace_util_0.Count(_stmtctx_00000, 69)
		return sc.mu.allExecDetails[i].ProcessTime < sc.mu.allExecDetails[j].ProcessTime
	})
	trace_util_0.Count(_stmtctx_00000, 66)
	d.P90ProcessTime = sc.mu.allExecDetails[n*9/10].ProcessTime
	d.MaxProcessTime = sc.mu.allExecDetails[n-1].ProcessTime
	d.MaxProcessAddress = sc.mu.allExecDetails[n-1].CalleeAddress

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		trace_util_0.Count(_stmtctx_00000, 70)
		return sc.mu.allExecDetails[i].WaitTime < sc.mu.allExecDetails[j].WaitTime
	})
	trace_util_0.Count(_stmtctx_00000, 67)
	d.P90WaitTime = sc.mu.allExecDetails[n*9/10].WaitTime
	d.MaxWaitTime = sc.mu.allExecDetails[n-1].WaitTime
	d.MaxWaitAddress = sc.mu.allExecDetails[n-1].CalleeAddress
	return d
}

//CopTasksDetails collects some useful information of cop-tasks during execution.
type CopTasksDetails struct {
	NumCopTasks int

	AvgProcessTime    time.Duration
	P90ProcessTime    time.Duration
	MaxProcessAddress string
	MaxProcessTime    time.Duration

	AvgWaitTime    time.Duration
	P90WaitTime    time.Duration
	MaxWaitAddress string
	MaxWaitTime    time.Duration
}

// ToZapFields wraps the CopTasksDetails as zap.Fileds.
func (d *CopTasksDetails) ToZapFields() (fields []zap.Field) {
	trace_util_0.Count(_stmtctx_00000, 71)
	if d.NumCopTasks == 0 {
		trace_util_0.Count(_stmtctx_00000, 73)
		return
	}
	trace_util_0.Count(_stmtctx_00000, 72)
	fields = make([]zap.Field, 0, 10)
	fields = append(fields, zap.Int("num_cop_tasks", d.NumCopTasks))
	fields = append(fields, zap.String("process_avg_time", strconv.FormatFloat(d.AvgProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_p90_time", strconv.FormatFloat(d.P90ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_time", strconv.FormatFloat(d.MaxProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_addr", d.MaxProcessAddress))
	fields = append(fields, zap.String("wait_avg_time", strconv.FormatFloat(d.AvgWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_p90_time", strconv.FormatFloat(d.P90WaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_time", strconv.FormatFloat(d.MaxWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.MaxWaitAddress))
	return fields
}

var _stmtctx_00000 = "sessionctx/stmtctx/stmtctx.go"
