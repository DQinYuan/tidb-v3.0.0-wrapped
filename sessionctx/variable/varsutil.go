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

package variable

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/timeutil"
)

// secondsPerYear represents seconds in a normal year. Leap year is not considered here.
const secondsPerYear = 60 * 60 * 24 * 365

// SetDDLReorgWorkerCounter sets ddlReorgWorkerCounter count.
// Max worker count is maxDDLReorgWorkerCount.
func SetDDLReorgWorkerCounter(cnt int32) {
	trace_util_0.Count(_varsutil_00000, 0)
	if cnt > maxDDLReorgWorkerCount {
		trace_util_0.Count(_varsutil_00000, 2)
		cnt = maxDDLReorgWorkerCount
	}
	trace_util_0.Count(_varsutil_00000, 1)
	atomic.StoreInt32(&ddlReorgWorkerCounter, cnt)
}

// GetDDLReorgWorkerCounter gets ddlReorgWorkerCounter.
func GetDDLReorgWorkerCounter() int32 {
	trace_util_0.Count(_varsutil_00000, 3)
	return atomic.LoadInt32(&ddlReorgWorkerCounter)
}

// SetDDLReorgBatchSize sets ddlReorgBatchSize size.
// Max batch size is MaxDDLReorgBatchSize.
func SetDDLReorgBatchSize(cnt int32) {
	trace_util_0.Count(_varsutil_00000, 4)
	if cnt > MaxDDLReorgBatchSize {
		trace_util_0.Count(_varsutil_00000, 7)
		cnt = MaxDDLReorgBatchSize
	}
	trace_util_0.Count(_varsutil_00000, 5)
	if cnt < MinDDLReorgBatchSize {
		trace_util_0.Count(_varsutil_00000, 8)
		cnt = MinDDLReorgBatchSize
	}
	trace_util_0.Count(_varsutil_00000, 6)
	atomic.StoreInt32(&ddlReorgBatchSize, cnt)
}

// GetDDLReorgBatchSize gets ddlReorgBatchSize.
func GetDDLReorgBatchSize() int32 {
	trace_util_0.Count(_varsutil_00000, 9)
	return atomic.LoadInt32(&ddlReorgBatchSize)
}

// SetDDLErrorCountLimit sets ddlErrorCountlimit size.
func SetDDLErrorCountLimit(cnt int64) {
	trace_util_0.Count(_varsutil_00000, 10)
	atomic.StoreInt64(&ddlErrorCountlimit, cnt)
}

// GetDDLErrorCountLimit gets ddlErrorCountlimit size.
func GetDDLErrorCountLimit() int64 {
	trace_util_0.Count(_varsutil_00000, 11)
	return atomic.LoadInt64(&ddlErrorCountlimit)
}

// GetSessionSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionSystemVar(s *SessionVars, key string) (string, error) {
	trace_util_0.Count(_varsutil_00000, 12)
	key = strings.ToLower(key)
	gVal, ok, err := GetSessionOnlySysVars(s, key)
	if err != nil || ok {
		trace_util_0.Count(_varsutil_00000, 15)
		return gVal, err
	}
	trace_util_0.Count(_varsutil_00000, 13)
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 16)
		return "", err
	}
	trace_util_0.Count(_varsutil_00000, 14)
	s.systems[key] = gVal
	return gVal, nil
}

// GetSessionOnlySysVars get the default value defined in code for session only variable.
// The return bool value indicates whether it's a session only variable.
func GetSessionOnlySysVars(s *SessionVars, key string) (string, bool, error) {
	trace_util_0.Count(_varsutil_00000, 17)
	sysVar := SysVars[key]
	if sysVar == nil {
		trace_util_0.Count(_varsutil_00000, 22)
		return "", false, UnknownSystemVar.GenWithStackByArgs(key)
	}
	// For virtual system variables:
	trace_util_0.Count(_varsutil_00000, 18)
	switch sysVar.Name {
	case TiDBCurrentTS:
		trace_util_0.Count(_varsutil_00000, 23)
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	case TiDBGeneralLog:
		trace_util_0.Count(_varsutil_00000, 24)
		return fmt.Sprintf("%d", atomic.LoadUint32(&ProcessGeneralLog)), true, nil
	case TiDBExpensiveQueryTimeThreshold:
		trace_util_0.Count(_varsutil_00000, 25)
		return fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), true, nil
	case TiDBConfig:
		trace_util_0.Count(_varsutil_00000, 26)
		conf := config.GetGlobalConfig()
		j, err := json.MarshalIndent(conf, "", "\t")
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 35)
			return "", false, err
		}
		trace_util_0.Count(_varsutil_00000, 27)
		return string(j), true, nil
	case TiDBForcePriority:
		trace_util_0.Count(_varsutil_00000, 28)
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], true, nil
	case TiDBSlowLogThreshold:
		trace_util_0.Count(_varsutil_00000, 29)
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.SlowThreshold), 10), true, nil
	case TiDBDDLSlowOprThreshold:
		trace_util_0.Count(_varsutil_00000, 30)
		return strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), true, nil
	case TiDBQueryLogMaxLen:
		trace_util_0.Count(_varsutil_00000, 31)
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10), true, nil
	case PluginDir:
		trace_util_0.Count(_varsutil_00000, 32)
		return config.GetGlobalConfig().Plugin.Dir, true, nil
	case PluginLoad:
		trace_util_0.Count(_varsutil_00000, 33)
		return config.GetGlobalConfig().Plugin.Load, true, nil
	case TiDBCheckMb4ValueInUTF8:
		trace_util_0.Count(_varsutil_00000, 34)
		return BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8), true, nil
	}
	trace_util_0.Count(_varsutil_00000, 19)
	sVal, ok := s.systems[key]
	if ok {
		trace_util_0.Count(_varsutil_00000, 36)
		return sVal, true, nil
	}
	trace_util_0.Count(_varsutil_00000, 20)
	if sysVar.Scope&ScopeGlobal == 0 {
		trace_util_0.Count(_varsutil_00000, 37)
		// None-Global variable can use pre-defined default value.
		return sysVar.Value, true, nil
	}
	trace_util_0.Count(_varsutil_00000, 21)
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *SessionVars, key string) (string, error) {
	trace_util_0.Count(_varsutil_00000, 38)
	key = strings.ToLower(key)
	gVal, ok, err := GetScopeNoneSystemVar(key)
	if err != nil || ok {
		trace_util_0.Count(_varsutil_00000, 41)
		return gVal, err
	}
	trace_util_0.Count(_varsutil_00000, 39)
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 42)
		return "", err
	}
	trace_util_0.Count(_varsutil_00000, 40)
	return gVal, nil
}

// GetScopeNoneSystemVar checks the validation of `key`,
// and return the default value if its scope is `ScopeNone`.
func GetScopeNoneSystemVar(key string) (string, bool, error) {
	trace_util_0.Count(_varsutil_00000, 43)
	sysVar := SysVars[key]
	if sysVar == nil {
		trace_util_0.Count(_varsutil_00000, 46)
		return "", false, UnknownSystemVar.GenWithStackByArgs(key)
	}
	trace_util_0.Count(_varsutil_00000, 44)
	if sysVar.Scope == ScopeNone {
		trace_util_0.Count(_varsutil_00000, 47)
		return sysVar.Value, true, nil
	}
	trace_util_0.Count(_varsutil_00000, 45)
	return "", false, nil
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// SetSessionSystemVar sets system variable and updates SessionVars states.
func SetSessionSystemVar(vars *SessionVars, name string, value types.Datum) error {
	trace_util_0.Count(_varsutil_00000, 48)
	name = strings.ToLower(name)
	sysVar := SysVars[name]
	if sysVar == nil {
		trace_util_0.Count(_varsutil_00000, 53)
		return UnknownSystemVar
	}
	trace_util_0.Count(_varsutil_00000, 49)
	sVal := ""
	var err error
	if !value.IsNull() {
		trace_util_0.Count(_varsutil_00000, 54)
		sVal, err = value.ToString()
	}
	trace_util_0.Count(_varsutil_00000, 50)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 55)
		return err
	}
	trace_util_0.Count(_varsutil_00000, 51)
	sVal, err = ValidateSetSystemVar(vars, name, sVal)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 56)
		return err
	}
	trace_util_0.Count(_varsutil_00000, 52)
	return vars.SetSystemVar(name, sVal)
}

// ValidateGetSystemVar checks if system variable exists and validates its scope when get system variable.
func ValidateGetSystemVar(name string, isGlobal bool) error {
	trace_util_0.Count(_varsutil_00000, 57)
	sysVar, exists := SysVars[name]
	if !exists {
		trace_util_0.Count(_varsutil_00000, 60)
		return UnknownSystemVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_varsutil_00000, 58)
	switch sysVar.Scope {
	case ScopeGlobal, ScopeNone:
		trace_util_0.Count(_varsutil_00000, 61)
		if !isGlobal {
			trace_util_0.Count(_varsutil_00000, 63)
			return ErrIncorrectScope.GenWithStackByArgs(name, "GLOBAL")
		}
	case ScopeSession:
		trace_util_0.Count(_varsutil_00000, 62)
		if isGlobal {
			trace_util_0.Count(_varsutil_00000, 64)
			return ErrIncorrectScope.GenWithStackByArgs(name, "SESSION")
		}
	}
	trace_util_0.Count(_varsutil_00000, 59)
	return nil
}

func checkUInt64SystemVar(name, value string, min, max uint64, vars *SessionVars) (string, error) {
	trace_util_0.Count(_varsutil_00000, 65)
	if len(value) == 0 {
		trace_util_0.Count(_varsutil_00000, 71)
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_varsutil_00000, 66)
	if value[0] == '-' {
		trace_util_0.Count(_varsutil_00000, 72)
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 74)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 73)
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	trace_util_0.Count(_varsutil_00000, 67)
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 75)
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_varsutil_00000, 68)
	if val < min {
		trace_util_0.Count(_varsutil_00000, 76)
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	trace_util_0.Count(_varsutil_00000, 69)
	if val > max {
		trace_util_0.Count(_varsutil_00000, 77)
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	trace_util_0.Count(_varsutil_00000, 70)
	return value, nil
}

func checkInt64SystemVar(name, value string, min, max int64, vars *SessionVars) (string, error) {
	trace_util_0.Count(_varsutil_00000, 78)
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 82)
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_varsutil_00000, 79)
	if val < min {
		trace_util_0.Count(_varsutil_00000, 83)
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	trace_util_0.Count(_varsutil_00000, 80)
	if val > max {
		trace_util_0.Count(_varsutil_00000, 84)
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	trace_util_0.Count(_varsutil_00000, 81)
	return value, nil
}

const (
	// initChunkSizeUpperBound indicates upper bound value of tidb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of tidb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// ValidateSetSystemVar checks if system variable satisfies specific restriction.
func ValidateSetSystemVar(vars *SessionVars, name string, value string) (string, error) {
	trace_util_0.Count(_varsutil_00000, 85)
	if strings.EqualFold(value, "DEFAULT") {
		trace_util_0.Count(_varsutil_00000, 88)
		if val := GetSysVar(name); val != nil {
			trace_util_0.Count(_varsutil_00000, 90)
			return val.Value, nil
		}
		trace_util_0.Count(_varsutil_00000, 89)
		return value, UnknownSystemVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_varsutil_00000, 86)
	switch name {
	case ConnectTimeout:
		trace_util_0.Count(_varsutil_00000, 91)
		return checkUInt64SystemVar(name, value, 2, secondsPerYear, vars)
	case DefaultWeekFormat:
		trace_util_0.Count(_varsutil_00000, 92)
		return checkUInt64SystemVar(name, value, 0, 7, vars)
	case DelayKeyWrite:
		trace_util_0.Count(_varsutil_00000, 93)
		if strings.EqualFold(value, "ON") || value == "1" {
			trace_util_0.Count(_varsutil_00000, 165)
			return "ON", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 166)
			if strings.EqualFold(value, "OFF") || value == "0" {
				trace_util_0.Count(_varsutil_00000, 167)
				return "OFF", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 168)
				if strings.EqualFold(value, "ALL") || value == "2" {
					trace_util_0.Count(_varsutil_00000, 169)
					return "ALL", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 94)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case FlushTime:
		trace_util_0.Count(_varsutil_00000, 95)
		return checkUInt64SystemVar(name, value, 0, secondsPerYear, vars)
	case ForeignKeyChecks:
		trace_util_0.Count(_varsutil_00000, 96)
		if strings.EqualFold(value, "ON") || value == "1" {
			trace_util_0.Count(_varsutil_00000, 170)
			// TiDB does not yet support foreign keys.
			// For now, resist the change and show a warning.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(name, value))
			return "OFF", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 171)
			if strings.EqualFold(value, "OFF") || value == "0" {
				trace_util_0.Count(_varsutil_00000, 172)
				return "OFF", nil
			}
		}
		trace_util_0.Count(_varsutil_00000, 97)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case GroupConcatMaxLen:
		trace_util_0.Count(_varsutil_00000, 98)
		// The reasonable range of 'group_concat_max_len' is 4~18446744073709551615(64-bit platforms)
		// See https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len for details
		return checkUInt64SystemVar(name, value, 4, math.MaxUint64, vars)
	case InteractiveTimeout:
		trace_util_0.Count(_varsutil_00000, 99)
		return checkUInt64SystemVar(name, value, 1, secondsPerYear, vars)
	case InnodbCommitConcurrency:
		trace_util_0.Count(_varsutil_00000, 100)
		return checkUInt64SystemVar(name, value, 0, 1000, vars)
	case InnodbFastShutdown:
		trace_util_0.Count(_varsutil_00000, 101)
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case InnodbLockWaitTimeout:
		trace_util_0.Count(_varsutil_00000, 102)
		return checkUInt64SystemVar(name, value, 1, 1073741824, vars)
	// See "https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet"
	case MaxAllowedPacket:
		trace_util_0.Count(_varsutil_00000, 103)
		return checkUInt64SystemVar(name, value, 1024, MaxOfMaxAllowedPacket, vars)
	case MaxConnections:
		trace_util_0.Count(_varsutil_00000, 104)
		return checkUInt64SystemVar(name, value, 1, 100000, vars)
	case MaxConnectErrors:
		trace_util_0.Count(_varsutil_00000, 105)
		return checkUInt64SystemVar(name, value, 1, math.MaxUint64, vars)
	case MaxSortLength:
		trace_util_0.Count(_varsutil_00000, 106)
		return checkUInt64SystemVar(name, value, 4, 8388608, vars)
	case MaxSpRecursionDepth:
		trace_util_0.Count(_varsutil_00000, 107)
		return checkUInt64SystemVar(name, value, 0, 255, vars)
	case MaxUserConnections:
		trace_util_0.Count(_varsutil_00000, 108)
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case OldPasswords:
		trace_util_0.Count(_varsutil_00000, 109)
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case SessionTrackGtids:
		trace_util_0.Count(_varsutil_00000, 110)
		if strings.EqualFold(value, "OFF") || value == "0" {
			trace_util_0.Count(_varsutil_00000, 173)
			return "OFF", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 174)
			if strings.EqualFold(value, "OWN_GTID") || value == "1" {
				trace_util_0.Count(_varsutil_00000, 175)
				return "OWN_GTID", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 176)
				if strings.EqualFold(value, "ALL_GTIDS") || value == "2" {
					trace_util_0.Count(_varsutil_00000, 177)
					return "ALL_GTIDS", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 111)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case SQLSelectLimit:
		trace_util_0.Count(_varsutil_00000, 112)
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case SyncBinlog:
		trace_util_0.Count(_varsutil_00000, 113)
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case TableDefinitionCache:
		trace_util_0.Count(_varsutil_00000, 114)
		return checkUInt64SystemVar(name, value, 400, 524288, vars)
	case TmpTableSize:
		trace_util_0.Count(_varsutil_00000, 115)
		return checkUInt64SystemVar(name, value, 1024, math.MaxUint64, vars)
	case WaitTimeout:
		trace_util_0.Count(_varsutil_00000, 116)
		return checkUInt64SystemVar(name, value, 0, 31536000, vars)
	case MaxPreparedStmtCount:
		trace_util_0.Count(_varsutil_00000, 117)
		return checkInt64SystemVar(name, value, -1, 1048576, vars)
	case TimeZone:
		trace_util_0.Count(_varsutil_00000, 118)
		if strings.EqualFold(value, "SYSTEM") {
			trace_util_0.Count(_varsutil_00000, 178)
			return "SYSTEM", nil
		}
		trace_util_0.Count(_varsutil_00000, 119)
		_, err := parseTimeZone(value)
		return value, err
	case ValidatePasswordLength, ValidatePasswordNumberCount:
		trace_util_0.Count(_varsutil_00000, 120)
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case WarningCount, ErrorCount:
		trace_util_0.Count(_varsutil_00000, 121)
		return value, ErrReadOnly.GenWithStackByArgs(name)
	case EnforceGtidConsistency:
		trace_util_0.Count(_varsutil_00000, 122)
		if strings.EqualFold(value, "OFF") || value == "0" {
			trace_util_0.Count(_varsutil_00000, 179)
			return "OFF", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 180)
			if strings.EqualFold(value, "ON") || value == "1" {
				trace_util_0.Count(_varsutil_00000, 181)
				return "ON", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 182)
				if strings.EqualFold(value, "WARN") || value == "2" {
					trace_util_0.Count(_varsutil_00000, 183)
					return "WARN", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 123)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case QueryCacheType:
		trace_util_0.Count(_varsutil_00000, 124)
		if strings.EqualFold(value, "OFF") || value == "0" {
			trace_util_0.Count(_varsutil_00000, 184)
			return "OFF", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 185)
			if strings.EqualFold(value, "ON") || value == "1" {
				trace_util_0.Count(_varsutil_00000, 186)
				return "ON", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 187)
				if strings.EqualFold(value, "DEMAND") || value == "2" {
					trace_util_0.Count(_varsutil_00000, 188)
					return "DEMAND", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 125)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case SecureAuth:
		trace_util_0.Count(_varsutil_00000, 126)
		if strings.EqualFold(value, "ON") || value == "1" {
			trace_util_0.Count(_varsutil_00000, 189)
			return "1", nil
		}
		trace_util_0.Count(_varsutil_00000, 127)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case GeneralLog, TiDBGeneralLog, AvoidTemporalUpgrade, BigTables, CheckProxyUsers, LogBin,
		CoreFile, EndMakersInJSON, SQLLogBin, OfflineMode, PseudoSlaveMode, LowPriorityUpdates,
		SkipNameResolve, SQLSafeUpdates, TiDBConstraintCheckInPlace, serverReadOnly, SlaveAllowBatching,
		Flush, PerformanceSchema, LocalInFile, ShowOldTemporals, KeepFilesOnCreate, AutoCommit,
		SQLWarnings, UniqueChecks, OldAlterTable, LogBinTrustFunctionCreators, SQLBigSelects,
		BinlogDirectNonTransactionalUpdates, SQLQuoteShowCreate, AutomaticSpPrivileges,
		RelayLogPurge, SQLAutoIsNull, QueryCacheWlockInvalidate, ValidatePasswordCheckUserName,
		SuperReadOnly, BinlogOrderCommits, MasterVerifyChecksum, BinlogRowQueryLogEvents, LogSlowSlaveStatements,
		LogSlowAdminStatements, LogQueriesNotUsingIndexes, Profiling:
		trace_util_0.Count(_varsutil_00000, 128)
		if strings.EqualFold(value, "ON") {
			trace_util_0.Count(_varsutil_00000, 190)
			return "1", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 191)
			if strings.EqualFold(value, "OFF") {
				trace_util_0.Count(_varsutil_00000, 192)
				return "0", nil
			}
		}
		trace_util_0.Count(_varsutil_00000, 129)
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			trace_util_0.Count(_varsutil_00000, 193)
			if val == 0 {
				trace_util_0.Count(_varsutil_00000, 194)
				return "0", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 195)
				if val == 1 {
					trace_util_0.Count(_varsutil_00000, 196)
					return "1", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 130)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MyISAMUseMmap, InnodbTableLocks, InnodbStatusOutput, InnodbAdaptiveFlushing, InnodbRandomReadAhead,
		InnodbStatsPersistent, InnodbBufferPoolLoadAbort, InnodbBufferPoolLoadNow, InnodbBufferPoolDumpNow,
		InnodbCmpPerIndexEnabled, InnodbFilePerTable, InnodbPrintAllDeadlocks,
		InnodbStrictMode, InnodbAdaptiveHashIndex, InnodbFtEnableStopword, InnodbStatusOutputLocks:
		trace_util_0.Count(_varsutil_00000, 131)
		if strings.EqualFold(value, "ON") {
			trace_util_0.Count(_varsutil_00000, 197)
			return "1", nil
		} else {
			trace_util_0.Count(_varsutil_00000, 198)
			if strings.EqualFold(value, "OFF") {
				trace_util_0.Count(_varsutil_00000, 199)
				return "0", nil
			}
		}
		trace_util_0.Count(_varsutil_00000, 132)
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			trace_util_0.Count(_varsutil_00000, 200)
			if val == 1 || val < 0 {
				trace_util_0.Count(_varsutil_00000, 201)
				return "1", nil
			} else {
				trace_util_0.Count(_varsutil_00000, 202)
				if val == 0 {
					trace_util_0.Count(_varsutil_00000, 203)
					return "0", nil
				}
			}
		}
		trace_util_0.Count(_varsutil_00000, 133)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBSkipUTF8Check, TiDBOptAggPushDown,
		TiDBOptInSubqToJoinAndAgg, TiDBEnableFastAnalyze,
		TiDBBatchInsert, TiDBDisableTxnAutoRetry, TiDBEnableStreaming,
		TiDBBatchDelete, TiDBBatchCommit, TiDBEnableCascadesPlanner, TiDBEnableWindowFunction,
		TiDBCheckMb4ValueInUTF8, TiDBLowResolutionTSO:
		trace_util_0.Count(_varsutil_00000, 134)
		if strings.EqualFold(value, "ON") || value == "1" || strings.EqualFold(value, "OFF") || value == "0" {
			trace_util_0.Count(_varsutil_00000, 204)
			return value, nil
		}
		trace_util_0.Count(_varsutil_00000, 135)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBEnableTablePartition:
		trace_util_0.Count(_varsutil_00000, 136)
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			trace_util_0.Count(_varsutil_00000, 205)
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			trace_util_0.Count(_varsutil_00000, 206)
			return "off", nil
		case strings.EqualFold(value, "AUTO"):
			trace_util_0.Count(_varsutil_00000, 207)
			return "auto", nil
		}
		trace_util_0.Count(_varsutil_00000, 137)
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBDDLReorgBatchSize:
		trace_util_0.Count(_varsutil_00000, 138)
		return checkUInt64SystemVar(name, value, uint64(MinDDLReorgBatchSize), uint64(MaxDDLReorgBatchSize), vars)
	case TiDBDDLErrorCountLimit:
		trace_util_0.Count(_varsutil_00000, 139)
		return checkUInt64SystemVar(name, value, uint64(0), math.MaxInt64, vars)
	case TiDBIndexLookupConcurrency, TiDBIndexLookupJoinConcurrency, TiDBIndexJoinBatchSize,
		TiDBIndexLookupSize,
		TiDBHashJoinConcurrency,
		TiDBHashAggPartialConcurrency,
		TiDBHashAggFinalConcurrency,
		TiDBDistSQLScanConcurrency,
		TiDBIndexSerialScanConcurrency, TiDBDDLReorgWorkerCount,
		TiDBBackoffLockFast, TiDBBackOffWeight,
		TiDBDMLBatchSize, TiDBOptimizerSelectivityLevel, TiDBExpensiveQueryTimeThreshold:
		trace_util_0.Count(_varsutil_00000, 140)
		v, err := strconv.Atoi(value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 208)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 141)
		if v <= 0 {
			trace_util_0.Count(_varsutil_00000, 209)
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		trace_util_0.Count(_varsutil_00000, 142)
		return value, nil
	case TiDBOptCorrelationExpFactor:
		trace_util_0.Count(_varsutil_00000, 143)
		v, err := strconv.Atoi(value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 210)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 144)
		if v < 0 {
			trace_util_0.Count(_varsutil_00000, 211)
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		trace_util_0.Count(_varsutil_00000, 145)
		return value, nil
	case TiDBOptCorrelationThreshold:
		trace_util_0.Count(_varsutil_00000, 146)
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 212)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 147)
		if v < 0 || v > 1 {
			trace_util_0.Count(_varsutil_00000, 213)
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		trace_util_0.Count(_varsutil_00000, 148)
		return value, nil
	case TiDBProjectionConcurrency,
		TIDBMemQuotaQuery,
		TIDBMemQuotaHashJoin,
		TIDBMemQuotaMergeJoin,
		TIDBMemQuotaSort,
		TIDBMemQuotaTopn,
		TIDBMemQuotaIndexLookupReader,
		TIDBMemQuotaIndexLookupJoin,
		TIDBMemQuotaNestedLoopApply,
		TiDBRetryLimit,
		TiDBSlowLogThreshold,
		TiDBQueryLogMaxLen:
		trace_util_0.Count(_varsutil_00000, 149)
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 214)
			return value, ErrWrongValueForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 150)
		return value, nil
	case TiDBAutoAnalyzeStartTime, TiDBAutoAnalyzeEndTime:
		trace_util_0.Count(_varsutil_00000, 151)
		v, err := setAnalyzeTime(vars, value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 215)
			return "", err
		}
		trace_util_0.Count(_varsutil_00000, 152)
		return v, nil
	case TxnIsolation, TransactionIsolation:
		trace_util_0.Count(_varsutil_00000, 153)
		upVal := strings.ToUpper(value)
		_, exists := TxIsolationNames[upVal]
		if !exists {
			trace_util_0.Count(_varsutil_00000, 216)
			return "", ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		trace_util_0.Count(_varsutil_00000, 154)
		switch upVal {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			trace_util_0.Count(_varsutil_00000, 217)
			skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(value)
			if err != nil {
				trace_util_0.Count(_varsutil_00000, 220)
				returnErr = err
			}
			trace_util_0.Count(_varsutil_00000, 218)
			if !TiDBOptOn(skipIsolationLevelCheck) || err != nil {
				trace_util_0.Count(_varsutil_00000, 221)
				return "", returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			trace_util_0.Count(_varsutil_00000, 219)
			if name == "transaction_isolation" {
				trace_util_0.Count(_varsutil_00000, 222)
				vars.StmtCtx.AppendWarning(returnErr)
			}
		}
		trace_util_0.Count(_varsutil_00000, 155)
		return upVal, nil
	case TiDBInitChunkSize:
		trace_util_0.Count(_varsutil_00000, 156)
		v, err := strconv.Atoi(value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 223)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 157)
		if v <= 0 {
			trace_util_0.Count(_varsutil_00000, 224)
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		trace_util_0.Count(_varsutil_00000, 158)
		if v > initChunkSizeUpperBound {
			trace_util_0.Count(_varsutil_00000, 225)
			return value, errors.Errorf("tidb_init_chunk_size(%d) cannot be bigger than %d", v, initChunkSizeUpperBound)
		}
		trace_util_0.Count(_varsutil_00000, 159)
		return value, nil
	case TiDBMaxChunkSize:
		trace_util_0.Count(_varsutil_00000, 160)
		v, err := strconv.Atoi(value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 226)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 161)
		if v < maxChunkSizeLowerBound {
			trace_util_0.Count(_varsutil_00000, 227)
			return value, errors.Errorf("tidb_max_chunk_size(%d) cannot be smaller than %d", v, maxChunkSizeLowerBound)
		}
		trace_util_0.Count(_varsutil_00000, 162)
		return value, nil
	case TiDBOptJoinReorderThreshold:
		trace_util_0.Count(_varsutil_00000, 163)
		v, err := strconv.Atoi(value)
		if err != nil {
			trace_util_0.Count(_varsutil_00000, 228)
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		trace_util_0.Count(_varsutil_00000, 164)
		if v < 0 || v >= 64 {
			trace_util_0.Count(_varsutil_00000, 229)
			return value, errors.Errorf("tidb_join_order_algo_threshold(%d) cannot be smaller than 0 or larger than 63", v)
		}
	}
	trace_util_0.Count(_varsutil_00000, 87)
	return value, nil
}

// TiDBOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func TiDBOptOn(opt string) bool {
	trace_util_0.Count(_varsutil_00000, 230)
	return strings.EqualFold(opt, "ON") || opt == "1"
}

func tidbOptPositiveInt32(opt string, defaultVal int) int {
	trace_util_0.Count(_varsutil_00000, 231)
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		trace_util_0.Count(_varsutil_00000, 233)
		return defaultVal
	}
	trace_util_0.Count(_varsutil_00000, 232)
	return val
}

func tidbOptInt64(opt string, defaultVal int64) int64 {
	trace_util_0.Count(_varsutil_00000, 234)
	val, err := strconv.ParseInt(opt, 10, 64)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 236)
		return defaultVal
	}
	trace_util_0.Count(_varsutil_00000, 235)
	return val
}

func tidbOptFloat64(opt string, defaultVal float64) float64 {
	trace_util_0.Count(_varsutil_00000, 237)
	val, err := strconv.ParseFloat(opt, 64)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 239)
		return defaultVal
	}
	trace_util_0.Count(_varsutil_00000, 238)
	return val
}

func parseTimeZone(s string) (*time.Location, error) {
	trace_util_0.Count(_varsutil_00000, 240)
	if strings.EqualFold(s, "SYSTEM") {
		trace_util_0.Count(_varsutil_00000, 244)
		return timeutil.SystemLocation(), nil
	}

	trace_util_0.Count(_varsutil_00000, 241)
	loc, err := time.LoadLocation(s)
	if err == nil {
		trace_util_0.Count(_varsutil_00000, 245)
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	trace_util_0.Count(_varsutil_00000, 242)
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		trace_util_0.Count(_varsutil_00000, 246)
		d, err := types.ParseDuration(nil, s[1:], 0)
		if err == nil {
			trace_util_0.Count(_varsutil_00000, 247)
			if s[0] == '-' {
				trace_util_0.Count(_varsutil_00000, 250)
				if d.Duration > 12*time.Hour+59*time.Minute {
					trace_util_0.Count(_varsutil_00000, 251)
					return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
				}
			} else {
				trace_util_0.Count(_varsutil_00000, 252)
				{
					if d.Duration > 14*time.Hour {
						trace_util_0.Count(_varsutil_00000, 253)
						return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
					}
				}
			}

			trace_util_0.Count(_varsutil_00000, 248)
			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				trace_util_0.Count(_varsutil_00000, 254)
				ofst = -ofst
			}
			trace_util_0.Count(_varsutil_00000, 249)
			return time.FixedZone("", ofst), nil
		}
	}

	trace_util_0.Count(_varsutil_00000, 243)
	return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
}

func setSnapshotTS(s *SessionVars, sVal string) error {
	trace_util_0.Count(_varsutil_00000, 255)
	if sVal == "" {
		trace_util_0.Count(_varsutil_00000, 259)
		s.SnapshotTS = 0
		return nil
	}

	trace_util_0.Count(_varsutil_00000, 256)
	if tso, err := strconv.ParseUint(sVal, 10, 64); err == nil {
		trace_util_0.Count(_varsutil_00000, 260)
		s.SnapshotTS = tso
		return nil
	}

	trace_util_0.Count(_varsutil_00000, 257)
	t, err := types.ParseTime(s.StmtCtx, sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 261)
		return err
	}

	// TODO: Consider time_zone variable.
	trace_util_0.Count(_varsutil_00000, 258)
	t1, err := t.Time.GoTime(time.Local)
	s.SnapshotTS = GoTimeToTS(t1)
	return err
}

// GoTimeToTS converts a Go time to uint64 timestamp.
func GoTimeToTS(t time.Time) uint64 {
	trace_util_0.Count(_varsutil_00000, 262)
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	return uint64(ts)
}

const (
	analyzeLocalTimeFormat = "15:04"
	// AnalyzeFullTimeFormat is the full format of analyze start time and end time.
	AnalyzeFullTimeFormat = "15:04 -0700"
)

func setAnalyzeTime(s *SessionVars, val string) (string, error) {
	trace_util_0.Count(_varsutil_00000, 263)
	var t time.Time
	var err error
	if len(val) <= len(analyzeLocalTimeFormat) {
		trace_util_0.Count(_varsutil_00000, 266)
		t, err = time.ParseInLocation(analyzeLocalTimeFormat, val, s.TimeZone)
	} else {
		trace_util_0.Count(_varsutil_00000, 267)
		{
			t, err = time.ParseInLocation(AnalyzeFullTimeFormat, val, s.TimeZone)
		}
	}
	trace_util_0.Count(_varsutil_00000, 264)
	if err != nil {
		trace_util_0.Count(_varsutil_00000, 268)
		return "", err
	}
	trace_util_0.Count(_varsutil_00000, 265)
	return t.Format(AnalyzeFullTimeFormat), nil
}

var _varsutil_00000 = "sessionctx/variable/varsutil.go"
