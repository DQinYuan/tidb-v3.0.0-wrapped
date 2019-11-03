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

package infoschema

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	binaryJson "github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	tableSchemata                           = "SCHEMATA"
	tableTables                             = "TABLES"
	tableColumns                            = "COLUMNS"
	tableStatistics                         = "STATISTICS"
	tableCharacterSets                      = "CHARACTER_SETS"
	tableCollations                         = "COLLATIONS"
	tableFiles                              = "FILES"
	catalogVal                              = "def"
	tableProfiling                          = "PROFILING"
	tablePartitions                         = "PARTITIONS"
	tableKeyColumm                          = "KEY_COLUMN_USAGE"
	tableReferConst                         = "REFERENTIAL_CONSTRAINTS"
	tableSessionVar                         = "SESSION_VARIABLES"
	tablePlugins                            = "PLUGINS"
	tableConstraints                        = "TABLE_CONSTRAINTS"
	tableTriggers                           = "TRIGGERS"
	tableUserPrivileges                     = "USER_PRIVILEGES"
	tableSchemaPrivileges                   = "SCHEMA_PRIVILEGES"
	tableTablePrivileges                    = "TABLE_PRIVILEGES"
	tableColumnPrivileges                   = "COLUMN_PRIVILEGES"
	tableEngines                            = "ENGINES"
	tableViews                              = "VIEWS"
	tableRoutines                           = "ROUTINES"
	tableParameters                         = "PARAMETERS"
	tableEvents                             = "EVENTS"
	tableGlobalStatus                       = "GLOBAL_STATUS"
	tableGlobalVariables                    = "GLOBAL_VARIABLES"
	tableSessionStatus                      = "SESSION_STATUS"
	tableOptimizerTrace                     = "OPTIMIZER_TRACE"
	tableTableSpaces                        = "TABLESPACES"
	tableCollationCharacterSetApplicability = "COLLATION_CHARACTER_SET_APPLICABILITY"
	tableProcesslist                        = "PROCESSLIST"
	tableTiDBIndexes                        = "TIDB_INDEXES"
	tableSlowLog                            = "SLOW_QUERY"
	tableTiDBHotRegions                     = "TIDB_HOT_REGIONS"
	tableTiKVStoreStatus                    = "TIKV_STORE_STATUS"
	tableAnalyzeStatus                      = "ANALYZE_STATUS"
	tableTiKVRegionStatus                   = "TIKV_REGION_STATUS"
	tableTiKVRegionPeers                    = "TIKV_REGION_PEERS"
)

type columnInfo struct {
	name  string
	tp    byte
	size  int
	flag  uint
	deflt interface{}
	elems []string
}

func buildColumnInfo(tableName string, col columnInfo) *model.ColumnInfo {
	trace_util_0.Count(_tables_00000, 0)
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob {
		trace_util_0.Count(_tables_00000, 2)
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
		mFlag = col.flag
	}
	trace_util_0.Count(_tables_00000, 1)
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      col.tp,
		Flen:    col.size,
		Flag:    mFlag,
	}
	return &model.ColumnInfo{
		Name:      model.NewCIStr(col.name),
		FieldType: fieldType,
		State:     model.StatePublic,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	trace_util_0.Count(_tables_00000, 3)
	cols := make([]*model.ColumnInfo, 0, len(cs))
	for _, c := range cs {
		trace_util_0.Count(_tables_00000, 6)
		cols = append(cols, buildColumnInfo(tableName, c))
	}
	trace_util_0.Count(_tables_00000, 4)
	for i, col := range cols {
		trace_util_0.Count(_tables_00000, 7)
		col.Offset = i
	}
	trace_util_0.Count(_tables_00000, 5)
	return &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
}

var schemataCols = []columnInfo{
	{"CATALOG_NAME", mysql.TypeVarchar, 512, 0, nil, nil},
	{"SCHEMA_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEFAULT_CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DEFAULT_COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"SQL_PATH", mysql.TypeVarchar, 512, 0, nil, nil},
}

var tablesCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ROW_FORMAT", mysql.TypeVarchar, 10, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTO_INCREMENT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, 19, 0, nil, nil},
	{"TABLE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, "utf8_bin", nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_OPTIONS", mysql.TypeVarchar, 255, 0, nil, nil},
	{"TABLE_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
	{"TIDB_TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
}

// See: http://dev.mysql.com/doc/refman/5.7/en/columns-table.html
var columnsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeLonglong, 64, 0, nil, nil},
	{"COLUMN_DEFAULT", mysql.TypeBlob, 196606, 0, nil, nil},
	{"IS_NULLABLE", mysql.TypeVarchar, 3, 0, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLUMN_TYPE", mysql.TypeBlob, 196606, 0, nil, nil},
	{"COLUMN_KEY", mysql.TypeVarchar, 3, 0, nil, nil},
	{"EXTRA", mysql.TypeVarchar, 30, 0, nil, nil},
	{"PRIVILEGES", mysql.TypeVarchar, 80, 0, nil, nil},
	{"COLUMN_COMMENT", mysql.TypeVarchar, 1024, 0, nil, nil},
	{"GENERATION_EXPRESSION", mysql.TypeBlob, 589779, mysql.NotNullFlag, nil, nil},
}

var statisticsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"NON_UNIQUE", mysql.TypeVarchar, 1, 0, nil, nil},
	{"INDEX_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SEQ_IN_INDEX", mysql.TypeLonglong, 2, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 21, 0, nil, nil},
	{"COLLATION", mysql.TypeVarchar, 1, 0, nil, nil},
	{"CARDINALITY", mysql.TypeLonglong, 21, 0, nil, nil},
	{"SUB_PART", mysql.TypeLonglong, 3, 0, nil, nil},
	{"PACKED", mysql.TypeVarchar, 10, 0, nil, nil},
	{"NULLABLE", mysql.TypeVarchar, 3, 0, nil, nil},
	{"INDEX_TYPE", mysql.TypeVarchar, 16, 0, nil, nil},
	{"COMMENT", mysql.TypeVarchar, 16, 0, nil, nil},
	{"INDEX_COMMENT", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var profilingCols = []columnInfo{
	{"QUERY_ID", mysql.TypeLong, 20, 0, nil, nil},
	{"SEQ", mysql.TypeLong, 20, 0, nil, nil},
	{"STATE", mysql.TypeVarchar, 30, 0, nil, nil},
	{"DURATION", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CPU_USER", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CPU_SYSTEM", mysql.TypeNewDecimal, 9, 0, nil, nil},
	{"CONTEXT_VOLUNTARY", mysql.TypeLong, 20, 0, nil, nil},
	{"CONTEXT_INVOLUNTARY", mysql.TypeLong, 20, 0, nil, nil},
	{"BLOCK_OPS_IN", mysql.TypeLong, 20, 0, nil, nil},
	{"BLOCK_OPS_OUT", mysql.TypeLong, 20, 0, nil, nil},
	{"MESSAGES_SENT", mysql.TypeLong, 20, 0, nil, nil},
	{"MESSAGES_RECEIVED", mysql.TypeLong, 20, 0, nil, nil},
	{"PAGE_FAULTS_MAJOR", mysql.TypeLong, 20, 0, nil, nil},
	{"PAGE_FAULTS_MINOR", mysql.TypeLong, 20, 0, nil, nil},
	{"SWAPS", mysql.TypeLong, 20, 0, nil, nil},
	{"SOURCE_FUNCTION", mysql.TypeVarchar, 30, 0, nil, nil},
	{"SOURCE_FILE", mysql.TypeVarchar, 20, 0, nil, nil},
	{"SOURCE_LINE", mysql.TypeLong, 20, 0, nil, nil},
}

var charsetCols = []columnInfo{
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DEFAULT_COLLATE_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DESCRIPTION", mysql.TypeVarchar, 60, 0, nil, nil},
	{"MAXLEN", mysql.TypeLonglong, 3, 0, nil, nil},
}

var collationsCols = []columnInfo{
	{"COLLATION_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, 0, nil, nil},
	{"ID", mysql.TypeLonglong, 11, 0, nil, nil},
	{"IS_DEFAULT", mysql.TypeVarchar, 3, 0, nil, nil},
	{"IS_COMPILED", mysql.TypeVarchar, 3, 0, nil, nil},
	{"SORTLEN", mysql.TypeLonglong, 3, 0, nil, nil},
}

var keyColumnUsageCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeLonglong, 10, mysql.NotNullFlag, nil, nil},
	{"POSITION_IN_UNIQUE_CONSTRAINT", mysql.TypeLonglong, 10, 0, nil, nil},
	{"REFERENCED_TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"REFERENCED_TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"REFERENCED_COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
}

// See http://dev.mysql.com/doc/refman/5.7/en/referential-constraints-table.html
var referConstCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UNIQUE_CONSTRAINT_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"MATCH_OPTION", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"UPDATE_RULE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"DELETE_RULE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"REFERENCED_TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
}

// See http://dev.mysql.com/doc/refman/5.7/en/variables-table.html
var sessionVarCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

// See https://dev.mysql.com/doc/refman/5.7/en/plugins-table.html
var pluginsCols = []columnInfo{
	{"PLUGIN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_STATUS", mysql.TypeVarchar, 10, 0, nil, nil},
	{"PLUGIN_TYPE", mysql.TypeVarchar, 80, 0, nil, nil},
	{"PLUGIN_TYPE_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_LIBRARY", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_LIBRARY_VERSION", mysql.TypeVarchar, 20, 0, nil, nil},
	{"PLUGIN_AUTHOR", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PLUGIN_DESCRIPTION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"PLUGIN_LICENSE", mysql.TypeVarchar, 80, 0, nil, nil},
	{"LOAD_OPTION", mysql.TypeVarchar, 64, 0, nil, nil},
}

// See https://dev.mysql.com/doc/refman/5.7/en/partitions-table.html
var partitionsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUBPARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_ORDINAL_POSITION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"SUBPARTITION_ORDINAL_POSITION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PARTITION_METHOD", mysql.TypeVarchar, 18, 0, nil, nil},
	{"SUBPARTITION_METHOD", mysql.TypeVarchar, 12, 0, nil, nil},
	{"PARTITION_EXPRESSION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"SUBPARTITION_EXPRESSION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"PARTITION_DESCRIPTION", mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PARTITION_COMMENT", mysql.TypeVarchar, 80, 0, nil, nil},
	{"NODEGROUP", mysql.TypeVarchar, 12, 0, nil, nil},
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableConstraintsCols = []columnInfo{
	{"CONSTRAINT_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"CONSTRAINT_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CONSTRAINT_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CONSTRAINT_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableTriggersCols = []columnInfo{
	{"TRIGGER_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"TRIGGER_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TRIGGER_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EVENT_MANIPULATION", mysql.TypeVarchar, 6, 0, nil, nil},
	{"EVENT_OBJECT_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"EVENT_OBJECT_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EVENT_OBJECT_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_ORDER", mysql.TypeLonglong, 4, 0, nil, nil},
	{"ACTION_CONDITION", mysql.TypeBlob, -1, 0, nil, nil},
	{"ACTION_STATEMENT", mysql.TypeBlob, -1, 0, nil, nil},
	{"ACTION_ORIENTATION", mysql.TypeVarchar, 9, 0, nil, nil},
	{"ACTION_TIMING", mysql.TypeVarchar, 6, 0, nil, nil},
	{"ACTION_REFERENCE_OLD_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_REFERENCE_NEW_TABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"ACTION_REFERENCE_OLD_ROW", mysql.TypeVarchar, 3, 0, nil, nil},
	{"ACTION_REFERENCE_NEW_ROW", mysql.TypeVarchar, 3, 0, nil, nil},
	{"CREATED", mysql.TypeDatetime, 2, 0, nil, nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, 0, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, 0, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, 0, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, 0, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, 0, nil, nil},
}

var tableUserPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, 0, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, 0, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, 0, nil, nil},
}

var tableSchemaPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableTablePrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableColumnPrivilegesCols = []columnInfo{
	{"GRANTEE", mysql.TypeVarchar, 81, mysql.NotNullFlag, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"PRIVILEGE_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"IS_GRANTABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
}

var tableEnginesCols = []columnInfo{
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUPPORT", mysql.TypeVarchar, 8, 0, nil, nil},
	{"COMMENT", mysql.TypeVarchar, 80, 0, nil, nil},
	{"TRANSACTIONS", mysql.TypeVarchar, 3, 0, nil, nil},
	{"XA", mysql.TypeVarchar, 3, 0, nil, nil},
	{"SAVEPOINTS", mysql.TypeVarchar, 3, 0, nil, nil},
}

var tableViewsCols = []columnInfo{
	{"TABLE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VIEW_DEFINITION", mysql.TypeLongBlob, 0, mysql.NotNullFlag, nil, nil},
	{"CHECK_OPTION", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"IS_UPDATABLE", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"SECURITY_TYPE", mysql.TypeVarchar, 7, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableRoutinesCols = []columnInfo{
	{"SPECIFIC_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeLong, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeLong, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeLong, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DTD_IDENTIFIER", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"ROUTINE_BODY", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_DEFINITION", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"EXTERNAL_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EXTERNAL_LANGUAGE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARAMETER_STYLE", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"IS_DETERMINISTIC", mysql.TypeVarchar, 3, mysql.NotNullFlag, nil, nil},
	{"SQL_DATA_ACCESS", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"SQL_PATH", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SECURITY_TYPE", mysql.TypeVarchar, 7, mysql.NotNullFlag, nil, nil},
	{"CREATED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_ALTERED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_COMMENT", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableParametersCols = []columnInfo{
	{"SPECIFIC_CATALOG", mysql.TypeVarchar, 512, mysql.NotNullFlag, nil, nil},
	{"SPECIFIC_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"SPECIFIC_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORDINAL_POSITION", mysql.TypeVarchar, 21, mysql.NotNullFlag, nil, nil},
	{"PARAMETER_MODE", mysql.TypeVarchar, 5, 0, nil, nil},
	{"PARAMETER_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DATA_TYPE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_MAXIMUM_LENGTH", mysql.TypeVarchar, 21, 0, nil, nil},
	{"CHARACTER_OCTET_LENGTH", mysql.TypeVarchar, 21, 0, nil, nil},
	{"NUMERIC_PRECISION", mysql.TypeVarchar, 21, 0, nil, nil},
	{"NUMERIC_SCALE", mysql.TypeVarchar, 21, 0, nil, nil},
	{"DATETIME_PRECISION", mysql.TypeVarchar, 21, 0, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"COLLATION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DTD_IDENTIFIER", mysql.TypeLongBlob, 0, mysql.NotNullFlag, nil, nil},
	{"ROUTINE_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
}

var tableEventsCols = []columnInfo{
	{"EVENT_CATALOG", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_SCHEMA", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"DEFINER", mysql.TypeVarchar, 77, mysql.NotNullFlag, nil, nil},
	{"TIME_ZONE", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"EVENT_BODY", mysql.TypeVarchar, 8, mysql.NotNullFlag, nil, nil},
	{"EVENT_DEFINITION", mysql.TypeLongBlob, 0, 0, nil, nil},
	{"EVENT_TYPE", mysql.TypeVarchar, 9, mysql.NotNullFlag, nil, nil},
	{"EXECUTE_AT", mysql.TypeDatetime, 0, 0, nil, nil},
	{"INTERVAL_VALUE", mysql.TypeVarchar, 256, 0, nil, nil},
	{"INTERVAL_FIELD", mysql.TypeVarchar, 18, 0, nil, nil},
	{"SQL_MODE", mysql.TypeVarchar, 8192, mysql.NotNullFlag, nil, nil},
	{"STARTS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"ENDS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"STATUS", mysql.TypeVarchar, 18, mysql.NotNullFlag, nil, nil},
	{"ON_COMPLETION", mysql.TypeVarchar, 12, mysql.NotNullFlag, nil, nil},
	{"CREATED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_ALTERED", mysql.TypeDatetime, 0, mysql.NotNullFlag, "0000-00-00 00:00:00", nil},
	{"LAST_EXECUTED", mysql.TypeDatetime, 0, 0, nil, nil},
	{"EVENT_COMMENT", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"ORIGINATOR", mysql.TypeLong, 10, mysql.NotNullFlag, 0, nil},
	{"CHARACTER_SET_CLIENT", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"COLLATION_CONNECTION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"DATABASE_COLLATION", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableGlobalStatusCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableGlobalVariablesCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableSessionStatusCols = []columnInfo{
	{"VARIABLE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, nil, nil},
	{"VARIABLE_VALUE", mysql.TypeVarchar, 1024, 0, nil, nil},
}

var tableOptimizerTraceCols = []columnInfo{
	{"QUERY", mysql.TypeLongBlob, 0, mysql.NotNullFlag, "", nil},
	{"TRACE", mysql.TypeLongBlob, 0, mysql.NotNullFlag, "", nil},
	{"MISSING_BYTES_BEYOND_MAX_MEM_SIZE", mysql.TypeShort, 20, mysql.NotNullFlag, 0, nil},
	{"INSUFFICIENT_PRIVILEGES", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
}

var tableTableSpacesCols = []columnInfo{
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"ENGINE", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"TABLESPACE_TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"EXTENT_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTOEXTEND_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAXIMUM_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NODEGROUP_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"TABLESPACE_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
}

var tableCollationCharacterSetApplicabilityCols = []columnInfo{
	{"COLLATION_NAME", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
	{"CHARACTER_SET_NAME", mysql.TypeVarchar, 32, mysql.NotNullFlag, nil, nil},
}

var tableProcesslistCols = []columnInfo{
	{"ID", mysql.TypeLonglong, 21, mysql.NotNullFlag, 0, nil},
	{"USER", mysql.TypeVarchar, 16, mysql.NotNullFlag, "", nil},
	{"HOST", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"DB", mysql.TypeVarchar, 64, mysql.NotNullFlag, "", nil},
	{"COMMAND", mysql.TypeVarchar, 16, mysql.NotNullFlag, "", nil},
	{"TIME", mysql.TypeLong, 7, mysql.NotNullFlag, 0, nil},
	{"STATE", mysql.TypeVarchar, 7, 0, nil, nil},
	{"INFO", mysql.TypeString, 512, 0, nil, nil},
}

var tableTiDBIndexesCols = []columnInfo{
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"NON_UNIQUE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"KEY_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SEQ_IN_INDEX", mysql.TypeLonglong, 21, 0, nil, nil},
	{"COLUMN_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"SUB_PART", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_COMMENT", mysql.TypeVarchar, 2048, 0, nil, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiDBHotRegionsCols = []columnInfo{
	{"TABLE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DB_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"INDEX_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TYPE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"MAX_HOT_DEGREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"FLOW_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiKVStoreStatusCols = []columnInfo{
	{"STORE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"STORE_STATE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STORE_STATE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LABEL", mysql.TypeJSON, 51, 0, nil, nil},
	{"VERSION", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CAPACITY", mysql.TypeVarchar, 64, 0, nil, nil},
	{"AVAILABLE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LEADER_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_WEIGHT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_SCORE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"LEADER_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_COUNT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_WEIGHT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_SCORE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"REGION_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"START_TS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"LAST_HEARTBEAT_TS", mysql.TypeDatetime, 0, 0, nil, nil},
	{"UPTIME", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableAnalyzeStatusCols = []columnInfo{
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PARTITION_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"JOB_INFO", mysql.TypeVarchar, 64, 0, nil, nil},
	{"PROCESSED_ROWS", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"START_TIME", mysql.TypeDatetime, 0, 0, nil, nil},
	{"STATE", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tableTiKVRegionStatusCols = []columnInfo{
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"START_KEY", mysql.TypeBlob, types.UnspecifiedLength, 0, nil, nil},
	{"END_KEY", mysql.TypeBlob, types.UnspecifiedLength, 0, nil, nil},
	{"EPOCH_CONF_VER", mysql.TypeLonglong, 21, 0, nil, nil},
	{"EPOCH_VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"WRITTEN_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"READ_BYTES", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"APPROXIMATE_KEYS", mysql.TypeLonglong, 21, 0, nil, nil},
}

var tableTiKVRegionPeersCols = []columnInfo{
	{"REGION_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"PEER_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STORE_ID", mysql.TypeLonglong, 21, 0, nil, nil},
	{"IS_LEARNER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"IS_LEADER", mysql.TypeTiny, 1, mysql.NotNullFlag, 0, nil},
	{"STATUS", mysql.TypeVarchar, 10, 0, 0, nil},
	{"DOWN_SECONDS", mysql.TypeLonglong, 21, 0, 0, nil},
}

func dataForTiKVRegionStatus(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 8)
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		trace_util_0.Count(_tables_00000, 12)
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	trace_util_0.Count(_tables_00000, 9)
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsStat, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		trace_util_0.Count(_tables_00000, 13)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 10)
	for _, regionStat := range regionsStat.Regions {
		trace_util_0.Count(_tables_00000, 14)
		row := make([]types.Datum, len(tableTiKVRegionStatusCols))
		row[0].SetInt64(regionStat.ID)
		row[1].SetString(regionStat.StartKey)
		row[2].SetString(regionStat.EndKey)
		row[3].SetInt64(regionStat.Epoch.ConfVer)
		row[4].SetInt64(regionStat.Epoch.Version)
		row[5].SetInt64(regionStat.WrittenBytes)
		row[6].SetInt64(regionStat.ReadBytes)
		row[7].SetInt64(regionStat.ApproximateSize)
		row[8].SetInt64(regionStat.ApproximateKeys)
		records = append(records, row)
	}
	trace_util_0.Count(_tables_00000, 11)
	return records, nil
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func dataForTikVRegionPeers(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 15)
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		trace_util_0.Count(_tables_00000, 19)
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	trace_util_0.Count(_tables_00000, 16)
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsStat, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		trace_util_0.Count(_tables_00000, 20)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 17)
	for _, regionStat := range regionsStat.Regions {
		trace_util_0.Count(_tables_00000, 21)
		pendingPeerIDSet := set.NewInt64Set()
		for _, peer := range regionStat.PendingPeers {
			trace_util_0.Count(_tables_00000, 24)
			pendingPeerIDSet.Insert(peer.ID)
		}
		trace_util_0.Count(_tables_00000, 22)
		downPeerMap := make(map[int64]int64)
		for _, peerStat := range regionStat.DownPeers {
			trace_util_0.Count(_tables_00000, 25)
			downPeerMap[peerStat.ID] = peerStat.DownSec
		}
		trace_util_0.Count(_tables_00000, 23)
		for _, peer := range regionStat.Peers {
			trace_util_0.Count(_tables_00000, 26)
			row := make([]types.Datum, len(tableTiKVRegionPeersCols))
			row[0].SetInt64(regionStat.ID)
			row[1].SetInt64(peer.ID)
			row[2].SetInt64(peer.StoreID)
			if peer.ID == regionStat.Leader.ID {
				trace_util_0.Count(_tables_00000, 30)
				row[3].SetInt64(1)
			} else {
				trace_util_0.Count(_tables_00000, 31)
				{
					row[3].SetInt64(0)
				}
			}
			trace_util_0.Count(_tables_00000, 27)
			if peer.IsLearner {
				trace_util_0.Count(_tables_00000, 32)
				row[4].SetInt64(1)
			} else {
				trace_util_0.Count(_tables_00000, 33)
				{
					row[4].SetInt64(0)
				}
			}
			trace_util_0.Count(_tables_00000, 28)
			if pendingPeerIDSet.Exist(peer.ID) {
				trace_util_0.Count(_tables_00000, 34)
				row[5].SetString(pendingPeer)
			} else {
				trace_util_0.Count(_tables_00000, 35)
				if downSec, ok := downPeerMap[peer.ID]; ok {
					trace_util_0.Count(_tables_00000, 36)
					row[5].SetString(downPeer)
					row[6].SetInt64(downSec)
				} else {
					trace_util_0.Count(_tables_00000, 37)
					{
						row[5].SetString(normalPeer)
					}
				}
			}
			trace_util_0.Count(_tables_00000, 29)
			records = append(records, row)
		}
	}
	trace_util_0.Count(_tables_00000, 18)
	return records, nil
}

func dataForTiKVStoreStatus(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 38)
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		trace_util_0.Count(_tables_00000, 42)
		return nil, errors.New("Information about TiKV store status can be gotten only when the storage is TiKV")
	}
	trace_util_0.Count(_tables_00000, 39)
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	storesStat, err := tikvHelper.GetStoresStat()
	if err != nil {
		trace_util_0.Count(_tables_00000, 43)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 40)
	for _, storeStat := range storesStat.Stores {
		trace_util_0.Count(_tables_00000, 44)
		row := make([]types.Datum, len(tableTiKVStoreStatusCols))
		row[0].SetInt64(storeStat.Store.ID)
		row[1].SetString(storeStat.Store.Address)
		row[2].SetInt64(storeStat.Store.State)
		row[3].SetString(storeStat.Store.StateName)
		data, err := json.Marshal(storeStat.Store.Labels)
		if err != nil {
			trace_util_0.Count(_tables_00000, 47)
			return nil, err
		}
		trace_util_0.Count(_tables_00000, 45)
		bj := binaryJson.BinaryJSON{}
		if err = bj.UnmarshalJSON(data); err != nil {
			trace_util_0.Count(_tables_00000, 48)
			return nil, err
		}
		trace_util_0.Count(_tables_00000, 46)
		row[4].SetMysqlJSON(bj)
		row[5].SetString(storeStat.Store.Version)
		row[6].SetString(storeStat.Status.Capacity)
		row[7].SetString(storeStat.Status.Available)
		row[8].SetInt64(storeStat.Status.LeaderCount)
		row[9].SetInt64(storeStat.Status.LeaderWeight)
		row[10].SetInt64(storeStat.Status.LeaderScore)
		row[11].SetInt64(storeStat.Status.LeaderSize)
		row[12].SetInt64(storeStat.Status.RegionCount)
		row[13].SetInt64(storeStat.Status.RegionWeight)
		row[14].SetInt64(storeStat.Status.RegionScore)
		row[15].SetInt64(storeStat.Status.RegionSize)
		startTs := types.Time{
			Time: types.FromGoTime(storeStat.Status.StartTs),
			Type: mysql.TypeDatetime,
			Fsp:  types.DefaultFsp,
		}
		row[16].SetMysqlTime(startTs)
		lastHeartbeatTs := types.Time{
			Time: types.FromGoTime(storeStat.Status.LastHeartbeatTs),
			Type: mysql.TypeDatetime,
			Fsp:  types.DefaultFsp,
		}
		row[17].SetMysqlTime(lastHeartbeatTs)
		row[18].SetString(storeStat.Status.Uptime)
		records = append(records, row)
	}
	trace_util_0.Count(_tables_00000, 41)
	return records, nil
}

func dataForCharacterSets() (records [][]types.Datum) {
	trace_util_0.Count(_tables_00000, 49)

	charsets := charset.GetSupportedCharsets()

	for _, charset := range charsets {
		trace_util_0.Count(_tables_00000, 51)

		records = append(records,
			types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen),
		)

	}

	trace_util_0.Count(_tables_00000, 50)
	return records

}

func dataForCollations() (records [][]types.Datum) {
	trace_util_0.Count(_tables_00000, 52)

	collations := charset.GetSupportedCollations()

	for _, collation := range collations {
		trace_util_0.Count(_tables_00000, 54)

		isDefault := ""
		if collation.IsDefault {
			trace_util_0.Count(_tables_00000, 56)
			isDefault = "Yes"
		}

		trace_util_0.Count(_tables_00000, 55)
		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName, collation.ID, isDefault, "Yes", 1),
		)

	}

	trace_util_0.Count(_tables_00000, 53)
	return records

}

func dataForCollationCharacterSetApplicability() (records [][]types.Datum) {
	trace_util_0.Count(_tables_00000, 57)

	collations := charset.GetSupportedCollations()

	for _, collation := range collations {
		trace_util_0.Count(_tables_00000, 59)

		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName),
		)

	}

	trace_util_0.Count(_tables_00000, 58)
	return records

}

func dataForSessionVar(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 60)
	sessionVars := ctx.GetSessionVars()
	for _, v := range variable.SysVars {
		trace_util_0.Count(_tables_00000, 62)
		var value string
		value, err = variable.GetSessionSystemVar(sessionVars, v.Name)
		if err != nil {
			trace_util_0.Count(_tables_00000, 64)
			return nil, err
		}
		trace_util_0.Count(_tables_00000, 63)
		row := types.MakeDatums(v.Name, value)
		records = append(records, row)
	}
	trace_util_0.Count(_tables_00000, 61)
	return
}

func dataForUserPrivileges(ctx sessionctx.Context) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 65)
	pm := privilege.GetPrivilegeManager(ctx)
	return pm.UserPrivilegesTable()
}

func dataForProcesslist(ctx sessionctx.Context) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 66)
	sm := ctx.GetSessionManager()
	if sm == nil {
		trace_util_0.Count(_tables_00000, 70)
		return nil
	}

	trace_util_0.Count(_tables_00000, 67)
	loginUser := ctx.GetSessionVars().User
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		trace_util_0.Count(_tables_00000, 71)
		if pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv) {
			trace_util_0.Count(_tables_00000, 72)
			hasProcessPriv = true
		}
	}

	trace_util_0.Count(_tables_00000, 68)
	pl := sm.ShowProcessList()
	records := make([][]types.Datum, 0, len(pl))
	for _, pi := range pl {
		trace_util_0.Count(_tables_00000, 73)
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && pi.User != loginUser.Username {
			trace_util_0.Count(_tables_00000, 75)
			continue
		}

		trace_util_0.Count(_tables_00000, 74)
		rows := pi.ToRow(true)
		record := types.MakeDatums(rows...)
		records = append(records, record)
	}
	trace_util_0.Count(_tables_00000, 69)
	return records
}

func dataForEngines() (records [][]types.Datum) {
	trace_util_0.Count(_tables_00000, 76)
	records = append(records,
		types.MakeDatums(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, row-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	return records
}

var filesCols = []columnInfo{
	{"FILE_ID", mysql.TypeLonglong, 4, 0, nil, nil},
	{"FILE_NAME", mysql.TypeVarchar, 4000, 0, nil, nil},
	{"FILE_TYPE", mysql.TypeVarchar, 20, 0, nil, nil},
	{"TABLESPACE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_CATALOG", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_SCHEMA", mysql.TypeVarchar, 64, 0, nil, nil},
	{"TABLE_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"LOGFILE_GROUP_NUMBER", mysql.TypeLonglong, 32, 0, nil, nil},
	{"ENGINE", mysql.TypeVarchar, 64, 0, nil, nil},
	{"FULLTEXT_KEYS", mysql.TypeVarchar, 64, 0, nil, nil},
	{"DELETED_ROWS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"UPDATE_COUNT", mysql.TypeLonglong, 4, 0, nil, nil},
	{"FREE_EXTENTS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"TOTAL_EXTENTS", mysql.TypeLonglong, 4, 0, nil, nil},
	{"EXTENT_SIZE", mysql.TypeLonglong, 4, 0, nil, nil},
	{"INITIAL_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAXIMUM_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AUTOEXTEND_SIZE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATION_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"LAST_UPDATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"LAST_ACCESS_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"RECOVER_TIME", mysql.TypeLonglong, 4, 0, nil, nil},
	{"TRANSACTION_COUNTER", mysql.TypeLonglong, 4, 0, nil, nil},
	{"VERSION", mysql.TypeLonglong, 21, 0, nil, nil},
	{"ROW_FORMAT", mysql.TypeVarchar, 10, 0, nil, nil},
	{"TABLE_ROWS", mysql.TypeLonglong, 21, 0, nil, nil},
	{"AVG_ROW_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"MAX_DATA_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"INDEX_LENGTH", mysql.TypeLonglong, 21, 0, nil, nil},
	{"DATA_FREE", mysql.TypeLonglong, 21, 0, nil, nil},
	{"CREATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"UPDATE_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"CHECK_TIME", mysql.TypeDatetime, -1, 0, nil, nil},
	{"CHECKSUM", mysql.TypeLonglong, 21, 0, nil, nil},
	{"STATUS", mysql.TypeVarchar, 20, 0, nil, nil},
	{"EXTRA", mysql.TypeVarchar, 255, 0, nil, nil},
}

func dataForSchemata(schemas []*model.DBInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 77)

	rows := make([][]types.Datum, 0, len(schemas))

	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 79)

		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(schema.Charset) > 0 {
			trace_util_0.Count(_tables_00000, 82)
			charset = schema.Charset // Overwrite default
		}

		trace_util_0.Count(_tables_00000, 80)
		if len(schema.Collate) > 0 {
			trace_util_0.Count(_tables_00000, 83)
			collation = schema.Collate // Overwrite default
		}

		trace_util_0.Count(_tables_00000, 81)
		record := types.MakeDatums(
			catalogVal,    // CATALOG_NAME
			schema.Name.O, // SCHEMA_NAME
			charset,       // DEFAULT_CHARACTER_SET_NAME
			collation,     // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	trace_util_0.Count(_tables_00000, 78)
	return rows
}

func getRowCountAllTable(ctx sessionctx.Context) (map[int64]uint64, error) {
	trace_util_0.Count(_tables_00000, 84)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, "select table_id, count from mysql.stats_meta")
	if err != nil {
		trace_util_0.Count(_tables_00000, 87)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 85)
	rowCountMap := make(map[int64]uint64, len(rows))
	for _, row := range rows {
		trace_util_0.Count(_tables_00000, 88)
		tableID := row.GetInt64(0)
		rowCnt := row.GetUint64(1)
		rowCountMap[tableID] = rowCnt
	}
	trace_util_0.Count(_tables_00000, 86)
	return rowCountMap, nil
}

type tableHistID struct {
	tableID int64
	histID  int64
}

func getColLengthAllTables(ctx sessionctx.Context) (map[tableHistID]uint64, error) {
	trace_util_0.Count(_tables_00000, 89)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0")
	if err != nil {
		trace_util_0.Count(_tables_00000, 92)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 90)
	colLengthMap := make(map[tableHistID]uint64, len(rows))
	for _, row := range rows {
		trace_util_0.Count(_tables_00000, 93)
		tableID := row.GetInt64(0)
		histID := row.GetInt64(1)
		totalSize := row.GetInt64(2)
		if totalSize < 0 {
			trace_util_0.Count(_tables_00000, 95)
			totalSize = 0
		}
		trace_util_0.Count(_tables_00000, 94)
		colLengthMap[tableHistID{tableID: tableID, histID: histID}] = uint64(totalSize)
	}
	trace_util_0.Count(_tables_00000, 91)
	return colLengthMap, nil
}

func getDataAndIndexLength(info *model.TableInfo, rowCount uint64, columnLengthMap map[tableHistID]uint64) (uint64, uint64) {
	trace_util_0.Count(_tables_00000, 96)
	columnLength := make(map[string]uint64)
	for _, col := range info.Columns {
		trace_util_0.Count(_tables_00000, 100)
		if col.State != model.StatePublic {
			trace_util_0.Count(_tables_00000, 102)
			continue
		}
		trace_util_0.Count(_tables_00000, 101)
		length := col.FieldType.StorageLength()
		if length != types.VarStorageLen {
			trace_util_0.Count(_tables_00000, 103)
			columnLength[col.Name.L] = rowCount * uint64(length)
		} else {
			trace_util_0.Count(_tables_00000, 104)
			{
				length := columnLengthMap[tableHistID{tableID: info.ID, histID: col.ID}]
				columnLength[col.Name.L] = length
			}
		}
	}
	trace_util_0.Count(_tables_00000, 97)
	dataLength, indexLength := uint64(0), uint64(0)
	for _, length := range columnLength {
		trace_util_0.Count(_tables_00000, 105)
		dataLength += length
	}
	trace_util_0.Count(_tables_00000, 98)
	for _, idx := range info.Indices {
		trace_util_0.Count(_tables_00000, 106)
		if idx.State != model.StatePublic {
			trace_util_0.Count(_tables_00000, 108)
			continue
		}
		trace_util_0.Count(_tables_00000, 107)
		for _, col := range idx.Columns {
			trace_util_0.Count(_tables_00000, 109)
			if col.Length == types.UnspecifiedLength {
				trace_util_0.Count(_tables_00000, 110)
				indexLength += columnLength[col.Name.L]
			} else {
				trace_util_0.Count(_tables_00000, 111)
				{
					indexLength += rowCount * uint64(col.Length)
				}
			}
		}
	}
	trace_util_0.Count(_tables_00000, 99)
	return dataLength, indexLength
}

type statsCache struct {
	mu         sync.Mutex
	loading    bool
	modifyTime time.Time
	tableRows  map[int64]uint64
	colLength  map[tableHistID]uint64
}

var tableStatsCache = &statsCache{}

// TableStatsCacheExpiry is the expiry time for table stats cache.
var TableStatsCacheExpiry = 3 * time.Second

func (c *statsCache) setLoading(loading bool) {
	trace_util_0.Count(_tables_00000, 112)
	c.mu.Lock()
	c.loading = loading
	c.mu.Unlock()
}

func (c *statsCache) get(ctx sessionctx.Context) (map[int64]uint64, map[tableHistID]uint64, error) {
	trace_util_0.Count(_tables_00000, 113)
	c.mu.Lock()
	if time.Since(c.modifyTime) < TableStatsCacheExpiry || c.loading {
		trace_util_0.Count(_tables_00000, 117)
		tableRows, colLength := c.tableRows, c.colLength
		c.mu.Unlock()
		return tableRows, colLength, nil
	}
	trace_util_0.Count(_tables_00000, 114)
	c.loading = true
	c.mu.Unlock()

	tableRows, err := getRowCountAllTable(ctx)
	if err != nil {
		trace_util_0.Count(_tables_00000, 118)
		c.setLoading(false)
		return nil, nil, err
	}
	trace_util_0.Count(_tables_00000, 115)
	colLength, err := getColLengthAllTables(ctx)
	if err != nil {
		trace_util_0.Count(_tables_00000, 119)
		c.setLoading(false)
		return nil, nil, err
	}

	trace_util_0.Count(_tables_00000, 116)
	c.mu.Lock()
	c.loading = false
	c.tableRows = tableRows
	c.colLength = colLength
	c.modifyTime = time.Now()
	c.mu.Unlock()
	return tableRows, colLength, nil
}

func getAutoIncrementID(ctx sessionctx.Context, schema *model.DBInfo, tblInfo *model.TableInfo) (int64, error) {
	trace_util_0.Count(_tables_00000, 120)
	hasAutoIncID := false
	for _, col := range tblInfo.Cols() {
		trace_util_0.Count(_tables_00000, 123)
		if mysql.HasAutoIncrementFlag(col.Flag) {
			trace_util_0.Count(_tables_00000, 124)
			hasAutoIncID = true
			break
		}
	}
	trace_util_0.Count(_tables_00000, 121)
	autoIncID := tblInfo.AutoIncID
	if hasAutoIncID {
		trace_util_0.Count(_tables_00000, 125)
		is := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema)
		tbl, err := is.TableByName(schema.Name, tblInfo.Name)
		if err != nil {
			trace_util_0.Count(_tables_00000, 127)
			return 0, err
		}
		trace_util_0.Count(_tables_00000, 126)
		autoIncID, err = tbl.Allocator(ctx).NextGlobalAutoID(tblInfo.ID)
		if err != nil {
			trace_util_0.Count(_tables_00000, 128)
			return 0, err
		}
	}
	trace_util_0.Count(_tables_00000, 122)
	return autoIncID, nil
}

func dataForViews(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 129)
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 131)
		for _, table := range schema.Tables {
			trace_util_0.Count(_tables_00000, 132)
			if !table.IsView() {
				trace_util_0.Count(_tables_00000, 137)
				continue
			}
			trace_util_0.Count(_tables_00000, 133)
			collation := table.Collate
			charset := table.Charset
			if collation == "" {
				trace_util_0.Count(_tables_00000, 138)
				collation = mysql.DefaultCollationName
			}
			trace_util_0.Count(_tables_00000, 134)
			if charset == "" {
				trace_util_0.Count(_tables_00000, 139)
				charset = mysql.DefaultCharset
			}
			trace_util_0.Count(_tables_00000, 135)
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				trace_util_0.Count(_tables_00000, 140)
				continue
			}
			trace_util_0.Count(_tables_00000, 136)
			record := types.MakeDatums(
				catalogVal,                      // TABLE_CATALOG
				schema.Name.O,                   // TABLE_SCHEMA
				table.Name.O,                    // TABLE_NAME
				table.View.SelectStmt,           // VIEW_DEFINITION
				table.View.CheckOption.String(), // CHECK_OPTION
				"NO",                            // IS_UPDATABLE
				table.View.Definer.String(),     // DEFINER
				table.View.Security.String(),    // SECURITY_TYPE
				charset,                         // CHARACTER_SET_CLIENT
				collation,                       // COLLATION_CONNECTION
			)
			rows = append(rows, record)
		}
	}
	trace_util_0.Count(_tables_00000, 130)
	return rows, nil
}

func dataForTables(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 141)
	tableRowsMap, colLengthMap, err := tableStatsCache.get(ctx)
	if err != nil {
		trace_util_0.Count(_tables_00000, 144)
		return nil, err
	}

	trace_util_0.Count(_tables_00000, 142)
	checker := privilege.GetPrivilegeManager(ctx)

	var rows [][]types.Datum
	createTimeTp := tablesCols[15].tp
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 145)
		for _, table := range schema.Tables {
			trace_util_0.Count(_tables_00000, 146)
			collation := table.Collate
			if collation == "" {
				trace_util_0.Count(_tables_00000, 149)
				collation = mysql.DefaultCollationName
			}
			trace_util_0.Count(_tables_00000, 147)
			createTime := types.Time{
				Time: types.FromGoTime(table.GetUpdateTime()),
				Type: createTimeTp,
			}

			createOptions := ""

			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				trace_util_0.Count(_tables_00000, 150)
				continue
			}

			trace_util_0.Count(_tables_00000, 148)
			if !table.IsView() {
				trace_util_0.Count(_tables_00000, 151)
				if table.GetPartitionInfo() != nil {
					trace_util_0.Count(_tables_00000, 155)
					createOptions = "partitioned"
				}
				trace_util_0.Count(_tables_00000, 152)
				autoIncID, err := getAutoIncrementID(ctx, schema, table)
				if err != nil {
					trace_util_0.Count(_tables_00000, 156)
					return nil, err
				}
				trace_util_0.Count(_tables_00000, 153)
				rowCount := tableRowsMap[table.ID]
				dataLength, indexLength := getDataAndIndexLength(table, rowCount, colLengthMap)
				avgRowLength := uint64(0)
				if rowCount != 0 {
					trace_util_0.Count(_tables_00000, 157)
					avgRowLength = dataLength / rowCount
				}
				trace_util_0.Count(_tables_00000, 154)
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"BASE TABLE",  // TABLE_TYPE
					"InnoDB",      // ENGINE
					uint64(10),    // VERSION
					"Compact",     // ROW_FORMAT
					rowCount,      // TABLE_ROWS
					avgRowLength,  // AVG_ROW_LENGTH
					dataLength,    // DATA_LENGTH
					uint64(0),     // MAX_DATA_LENGTH
					indexLength,   // INDEX_LENGTH
					uint64(0),     // DATA_FREE
					autoIncID,     // AUTO_INCREMENT
					createTime,    // CREATE_TIME
					nil,           // UPDATE_TIME
					nil,           // CHECK_TIME
					collation,     // TABLE_COLLATION
					nil,           // CHECKSUM
					createOptions, // CREATE_OPTIONS
					table.Comment, // TABLE_COMMENT
					table.ID,      // TIDB_TABLE_ID
				)
				rows = append(rows, record)
			} else {
				trace_util_0.Count(_tables_00000, 158)
				{
					record := types.MakeDatums(
						catalogVal,    // TABLE_CATALOG
						schema.Name.O, // TABLE_SCHEMA
						table.Name.O,  // TABLE_NAME
						"VIEW",        // TABLE_TYPE
						nil,           // ENGINE
						nil,           // VERSION
						nil,           // ROW_FORMAT
						nil,           // TABLE_ROWS
						nil,           // AVG_ROW_LENGTH
						nil,           // DATA_LENGTH
						nil,           // MAX_DATA_LENGTH
						nil,           // INDEX_LENGTH
						nil,           // DATA_FREE
						nil,           // AUTO_INCREMENT
						createTime,    // CREATE_TIME
						nil,           // UPDATE_TIME
						nil,           // CHECK_TIME
						nil,           // TABLE_COLLATION
						nil,           // CHECKSUM
						nil,           // CREATE_OPTIONS
						"VIEW",        // TABLE_COMMENT
						table.ID,      // TIDB_TABLE_ID
					)
					rows = append(rows, record)
				}
			}
		}
	}
	trace_util_0.Count(_tables_00000, 143)
	return rows, nil
}

func dataForIndexes(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 159)
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 161)
		for _, tb := range schema.Tables {
			trace_util_0.Count(_tables_00000, 162)
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tb.Name.L, "", mysql.AllPrivMask) {
				trace_util_0.Count(_tables_00000, 165)
				continue
			}

			trace_util_0.Count(_tables_00000, 163)
			if tb.PKIsHandle {
				trace_util_0.Count(_tables_00000, 166)
				var pkCol *model.ColumnInfo
				for _, col := range tb.Cols() {
					trace_util_0.Count(_tables_00000, 168)
					if mysql.HasPriKeyFlag(col.Flag) {
						trace_util_0.Count(_tables_00000, 169)
						pkCol = col
						break
					}
				}
				trace_util_0.Count(_tables_00000, 167)
				record := types.MakeDatums(
					schema.Name.O, // TABLE_SCHEMA
					tb.Name.O,     // TABLE_NAME
					0,             // NON_UNIQUE
					"PRIMARY",     // KEY_NAME
					1,             // SEQ_IN_INDEX
					pkCol.Name.O,  // COLUMN_NAME
					nil,           // SUB_PART
					"",            // INDEX_COMMENT
					0,             // INDEX_ID
				)
				rows = append(rows, record)
			}
			trace_util_0.Count(_tables_00000, 164)
			for _, idxInfo := range tb.Indices {
				trace_util_0.Count(_tables_00000, 170)
				if idxInfo.State != model.StatePublic {
					trace_util_0.Count(_tables_00000, 172)
					continue
				}
				trace_util_0.Count(_tables_00000, 171)
				for i, col := range idxInfo.Columns {
					trace_util_0.Count(_tables_00000, 173)
					nonUniq := 1
					if idxInfo.Unique {
						trace_util_0.Count(_tables_00000, 176)
						nonUniq = 0
					}
					trace_util_0.Count(_tables_00000, 174)
					var subPart interface{}
					if col.Length != types.UnspecifiedLength {
						trace_util_0.Count(_tables_00000, 177)
						subPart = col.Length
					}
					trace_util_0.Count(_tables_00000, 175)
					record := types.MakeDatums(
						schema.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						col.Name.O,      // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						idxInfo.ID,      // INDEX_ID
					)
					rows = append(rows, record)
				}
			}
		}
	}
	trace_util_0.Count(_tables_00000, 160)
	return rows, nil
}

func dataForColumns(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 178)
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 180)
		for _, table := range schema.Tables {
			trace_util_0.Count(_tables_00000, 181)
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				trace_util_0.Count(_tables_00000, 183)
				continue
			}

			trace_util_0.Count(_tables_00000, 182)
			rs := dataForColumnsInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	trace_util_0.Count(_tables_00000, 179)
	return rows
}

func dataForColumnsInTable(schema *model.DBInfo, tbl *model.TableInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 184)
	rows := make([][]types.Datum, 0, len(tbl.Columns))
	for i, col := range tbl.Columns {
		trace_util_0.Count(_tables_00000, 186)
		var charMaxLen, charOctLen, numericPrecision, numericScale, datetimePrecision interface{}
		colLen, decimal := col.Flen, col.Decimal
		defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.Tp)
		if decimal == types.UnspecifiedLength {
			trace_util_0.Count(_tables_00000, 191)
			decimal = defaultDecimal
		}
		trace_util_0.Count(_tables_00000, 187)
		if colLen == types.UnspecifiedLength {
			trace_util_0.Count(_tables_00000, 192)
			colLen = defaultFlen
		}
		trace_util_0.Count(_tables_00000, 188)
		if col.Tp == mysql.TypeSet {
			trace_util_0.Count(_tables_00000, 193)
			// Example: In MySQL set('a','bc','def','ghij') has length 13, because
			// len('a')+len('bc')+len('def')+len('ghij')+len(ThreeComma)=13
			// Reference link: https://bugs.mysql.com/bug.php?id=22613
			colLen = 0
			for _, ele := range col.Elems {
				trace_util_0.Count(_tables_00000, 196)
				colLen += len(ele)
			}
			trace_util_0.Count(_tables_00000, 194)
			if len(col.Elems) != 0 {
				trace_util_0.Count(_tables_00000, 197)
				colLen += (len(col.Elems) - 1)
			}
			trace_util_0.Count(_tables_00000, 195)
			charMaxLen = colLen
			charOctLen = colLen
		} else {
			trace_util_0.Count(_tables_00000, 198)
			if col.Tp == mysql.TypeEnum {
				trace_util_0.Count(_tables_00000, 199)
				// Example: In MySQL enum('a', 'ab', 'cdef') has length 4, because
				// the longest string in the enum is 'cdef'
				// Reference link: https://bugs.mysql.com/bug.php?id=22613
				colLen = 0
				for _, ele := range col.Elems {
					trace_util_0.Count(_tables_00000, 201)
					if len(ele) > colLen {
						trace_util_0.Count(_tables_00000, 202)
						colLen = len(ele)
					}
				}
				trace_util_0.Count(_tables_00000, 200)
				charMaxLen = colLen
				charOctLen = colLen
			} else {
				trace_util_0.Count(_tables_00000, 203)
				if types.IsString(col.Tp) {
					trace_util_0.Count(_tables_00000, 204)
					charMaxLen = colLen
					charOctLen = colLen
				} else {
					trace_util_0.Count(_tables_00000, 205)
					if types.IsTypeFractionable(col.Tp) {
						trace_util_0.Count(_tables_00000, 206)
						datetimePrecision = decimal
					} else {
						trace_util_0.Count(_tables_00000, 207)
						if types.IsTypeNumeric(col.Tp) {
							trace_util_0.Count(_tables_00000, 208)
							numericPrecision = colLen
							if col.Tp != mysql.TypeFloat && col.Tp != mysql.TypeDouble {
								trace_util_0.Count(_tables_00000, 209)
								numericScale = decimal
							} else {
								trace_util_0.Count(_tables_00000, 210)
								if decimal != -1 {
									trace_util_0.Count(_tables_00000, 211)
									numericScale = decimal
								}
							}
						}
					}
				}
			}
		}
		trace_util_0.Count(_tables_00000, 189)
		columnType := col.FieldType.InfoSchemaStr()
		columnDesc := table.NewColDesc(table.ToColumn(col))
		var columnDefault interface{}
		if columnDesc.DefaultValue != nil {
			trace_util_0.Count(_tables_00000, 212)
			columnDefault = fmt.Sprintf("%v", columnDesc.DefaultValue)
		}
		trace_util_0.Count(_tables_00000, 190)
		record := types.MakeDatums(
			catalogVal,                           // TABLE_CATALOG
			schema.Name.O,                        // TABLE_SCHEMA
			tbl.Name.O,                           // TABLE_NAME
			col.Name.O,                           // COLUMN_NAME
			i+1,                                  // ORIGINAL_POSITION
			columnDefault,                        // COLUMN_DEFAULT
			columnDesc.Null,                      // IS_NULLABLE
			types.TypeToStr(col.Tp, col.Charset), // DATA_TYPE
			charMaxLen,                           // CHARACTER_MAXIMUM_LENGTH
			charOctLen,                           // CHARACTER_OCTET_LENGTH
			numericPrecision,                     // NUMERIC_PRECISION
			numericScale,                         // NUMERIC_SCALE
			datetimePrecision,                    // DATETIME_PRECISION
			columnDesc.Charset,                   // CHARACTER_SET_NAME
			columnDesc.Collation,                 // COLLATION_NAME
			columnType,                           // COLUMN_TYPE
			columnDesc.Key,                       // COLUMN_KEY
			columnDesc.Extra,                     // EXTRA
			"select,insert,update,references",    // PRIVILEGES
			columnDesc.Comment,                   // COLUMN_COMMENT
			col.GeneratedExprString,              // GENERATION_EXPRESSION
		)
		rows = append(rows, record)
	}
	trace_util_0.Count(_tables_00000, 185)
	return rows
}

func dataForStatistics(schemas []*model.DBInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 213)
	var rows [][]types.Datum
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 215)
		for _, table := range schema.Tables {
			trace_util_0.Count(_tables_00000, 216)
			rs := dataForStatisticsInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	trace_util_0.Count(_tables_00000, 214)
	return rows
}

func dataForStatisticsInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 217)
	var rows [][]types.Datum
	if table.PKIsHandle {
		trace_util_0.Count(_tables_00000, 221)
		for _, col := range table.Columns {
			trace_util_0.Count(_tables_00000, 222)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_tables_00000, 223)
				record := types.MakeDatums(
					catalogVal,    // TABLE_CATALOG
					schema.Name.O, // TABLE_SCHEMA
					table.Name.O,  // TABLE_NAME
					"0",           // NON_UNIQUE
					schema.Name.O, // INDEX_SCHEMA
					"PRIMARY",     // INDEX_NAME
					1,             // SEQ_IN_INDEX
					col.Name.O,    // COLUMN_NAME
					"A",           // COLLATION
					nil,           // CARDINALITY
					nil,           // SUB_PART
					nil,           // PACKED
					"",            // NULLABLE
					"BTREE",       // INDEX_TYPE
					"",            // COMMENT
					"",            // INDEX_COMMENT
				)
				rows = append(rows, record)
			}
		}
	}
	trace_util_0.Count(_tables_00000, 218)
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		trace_util_0.Count(_tables_00000, 224)
		nameToCol[c.Name.L] = c
	}
	trace_util_0.Count(_tables_00000, 219)
	for _, index := range table.Indices {
		trace_util_0.Count(_tables_00000, 225)
		nonUnique := "1"
		if index.Unique {
			trace_util_0.Count(_tables_00000, 227)
			nonUnique = "0"
		}
		trace_util_0.Count(_tables_00000, 226)
		for i, key := range index.Columns {
			trace_util_0.Count(_tables_00000, 228)
			col := nameToCol[key.Name.L]
			nullable := "YES"
			if mysql.HasNotNullFlag(col.Flag) {
				trace_util_0.Count(_tables_00000, 230)
				nullable = ""
			}
			trace_util_0.Count(_tables_00000, 229)
			record := types.MakeDatums(
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				nonUnique,     // NON_UNIQUE
				schema.Name.O, // INDEX_SCHEMA
				index.Name.O,  // INDEX_NAME
				i+1,           // SEQ_IN_INDEX
				key.Name.O,    // COLUMN_NAME
				"A",           // COLLATION
				nil,           // CARDINALITY
				nil,           // SUB_PART
				nil,           // PACKED
				nullable,      // NULLABLE
				"BTREE",       // INDEX_TYPE
				"",            // COMMENT
				"",            // INDEX_COMMENT
			)
			rows = append(rows, record)
		}
	}
	trace_util_0.Count(_tables_00000, 220)
	return rows
}

const (
	primaryKeyType    = "PRIMARY KEY"
	primaryConstraint = "PRIMARY"
	uniqueKeyType     = "UNIQUE"
)

// dataForTableConstraints constructs data for table information_schema.constraints.See https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
func dataForTableConstraints(schemas []*model.DBInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 231)
	var rows [][]types.Datum
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 233)
		for _, tbl := range schema.Tables {
			trace_util_0.Count(_tables_00000, 234)
			if tbl.PKIsHandle {
				trace_util_0.Count(_tables_00000, 236)
				record := types.MakeDatums(
					catalogVal,           // CONSTRAINT_CATALOG
					schema.Name.O,        // CONSTRAINT_SCHEMA
					mysql.PrimaryKeyName, // CONSTRAINT_NAME
					schema.Name.O,        // TABLE_SCHEMA
					tbl.Name.O,           // TABLE_NAME
					primaryKeyType,       // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}

			trace_util_0.Count(_tables_00000, 235)
			for _, idx := range tbl.Indices {
				trace_util_0.Count(_tables_00000, 237)
				var cname, ctype string
				if idx.Primary {
					trace_util_0.Count(_tables_00000, 239)
					cname = mysql.PrimaryKeyName
					ctype = primaryKeyType
				} else {
					trace_util_0.Count(_tables_00000, 240)
					if idx.Unique {
						trace_util_0.Count(_tables_00000, 241)
						cname = idx.Name.O
						ctype = uniqueKeyType
					} else {
						trace_util_0.Count(_tables_00000, 242)
						{
							// The index has no constriant.
							continue
						}
					}
				}
				trace_util_0.Count(_tables_00000, 238)
				record := types.MakeDatums(
					catalogVal,    // CONSTRAINT_CATALOG
					schema.Name.O, // CONSTRAINT_SCHEMA
					cname,         // CONSTRAINT_NAME
					schema.Name.O, // TABLE_SCHEMA
					tbl.Name.O,    // TABLE_NAME
					ctype,         // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}
		}
	}
	trace_util_0.Count(_tables_00000, 232)
	return rows
}

// dataForPseudoProfiling returns pseudo data for table profiling when system variable `profiling` is set to `ON`.
func dataForPseudoProfiling() [][]types.Datum {
	trace_util_0.Count(_tables_00000, 243)
	var rows [][]types.Datum
	row := types.MakeDatums(
		0,                      // QUERY_ID
		0,                      // SEQ
		"",                     // STATE
		types.NewDecFromInt(0), // DURATION
		types.NewDecFromInt(0), // CPU_USER
		types.NewDecFromInt(0), // CPU_SYSTEM
		0,                      // CONTEXT_VOLUNTARY
		0,                      // CONTEXT_INVOLUNTARY
		0,                      // BLOCK_OPS_IN
		0,                      // BLOCK_OPS_OUT
		0,                      // MESSAGES_SENT
		0,                      // MESSAGES_RECEIVED
		0,                      // PAGE_FAULTS_MAJOR
		0,                      // PAGE_FAULTS_MINOR
		0,                      // SWAPS
		"",                     // SOURCE_FUNCTION
		"",                     // SOURCE_FILE
		0,                      // SOURCE_LINE
	)
	rows = append(rows, row)
	return rows
}

func dataForKeyColumnUsage(schemas []*model.DBInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 244)
	rows := make([][]types.Datum, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schema := range schemas {
		trace_util_0.Count(_tables_00000, 246)
		for _, table := range schema.Tables {
			trace_util_0.Count(_tables_00000, 247)
			rs := keyColumnUsageInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	trace_util_0.Count(_tables_00000, 245)
	return rows
}

func keyColumnUsageInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 248)
	var rows [][]types.Datum
	if table.PKIsHandle {
		trace_util_0.Count(_tables_00000, 253)
		for _, col := range table.Columns {
			trace_util_0.Count(_tables_00000, 254)
			if mysql.HasPriKeyFlag(col.Flag) {
				trace_util_0.Count(_tables_00000, 255)
				record := types.MakeDatums(
					catalogVal,        // CONSTRAINT_CATALOG
					schema.Name.O,     // CONSTRAINT_SCHEMA
					primaryConstraint, // CONSTRAINT_NAME
					catalogVal,        // TABLE_CATALOG
					schema.Name.O,     // TABLE_SCHEMA
					table.Name.O,      // TABLE_NAME
					col.Name.O,        // COLUMN_NAME
					1,                 // ORDINAL_POSITION
					1,                 // POSITION_IN_UNIQUE_CONSTRAINT
					nil,               // REFERENCED_TABLE_SCHEMA
					nil,               // REFERENCED_TABLE_NAME
					nil,               // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				break
			}
		}
	}
	trace_util_0.Count(_tables_00000, 249)
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		trace_util_0.Count(_tables_00000, 256)
		nameToCol[c.Name.L] = c
	}
	trace_util_0.Count(_tables_00000, 250)
	for _, index := range table.Indices {
		trace_util_0.Count(_tables_00000, 257)
		var idxName string
		if index.Primary {
			trace_util_0.Count(_tables_00000, 259)
			idxName = primaryConstraint
		} else {
			trace_util_0.Count(_tables_00000, 260)
			if index.Unique {
				trace_util_0.Count(_tables_00000, 261)
				idxName = index.Name.O
			} else {
				trace_util_0.Count(_tables_00000, 262)
				{
					// Only handle unique/primary key
					continue
				}
			}
		}
		trace_util_0.Count(_tables_00000, 258)
		for i, key := range index.Columns {
			trace_util_0.Count(_tables_00000, 263)
			col := nameToCol[key.Name.L]
			record := types.MakeDatums(
				catalogVal,    // CONSTRAINT_CATALOG
				schema.Name.O, // CONSTRAINT_SCHEMA
				idxName,       // CONSTRAINT_NAME
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				col.Name.O,    // COLUMN_NAME
				i+1,           // ORDINAL_POSITION,
				nil,           // POSITION_IN_UNIQUE_CONSTRAINT
				nil,           // REFERENCED_TABLE_SCHEMA
				nil,           // REFERENCED_TABLE_NAME
				nil,           // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	trace_util_0.Count(_tables_00000, 251)
	for _, fk := range table.ForeignKeys {
		trace_util_0.Count(_tables_00000, 264)
		fkRefCol := ""
		if len(fk.RefCols) > 0 {
			trace_util_0.Count(_tables_00000, 266)
			fkRefCol = fk.RefCols[0].O
		}
		trace_util_0.Count(_tables_00000, 265)
		for i, key := range fk.Cols {
			trace_util_0.Count(_tables_00000, 267)
			col := nameToCol[key.L]
			record := types.MakeDatums(
				catalogVal,    // CONSTRAINT_CATALOG
				schema.Name.O, // CONSTRAINT_SCHEMA
				fk.Name.O,     // CONSTRAINT_NAME
				catalogVal,    // TABLE_CATALOG
				schema.Name.O, // TABLE_SCHEMA
				table.Name.O,  // TABLE_NAME
				col.Name.O,    // COLUMN_NAME
				i+1,           // ORDINAL_POSITION,
				1,             // POSITION_IN_UNIQUE_CONSTRAINT
				schema.Name.O, // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O, // REFERENCED_TABLE_NAME
				fkRefCol,      // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	trace_util_0.Count(_tables_00000, 252)
	return rows
}

func dataForTiDBHotRegions(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 268)
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		trace_util_0.Count(_tables_00000, 272)
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	trace_util_0.Count(_tables_00000, 269)
	allSchemas := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema).AllSchemas()
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	metrics, err := tikvHelper.ScrapeHotInfo(pdapi.HotRead, allSchemas)
	if err != nil {
		trace_util_0.Count(_tables_00000, 273)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 270)
	records = append(records, dataForHotRegionByMetrics(metrics, "read")...)
	metrics, err = tikvHelper.ScrapeHotInfo(pdapi.HotWrite, allSchemas)
	if err != nil {
		trace_util_0.Count(_tables_00000, 274)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 271)
	records = append(records, dataForHotRegionByMetrics(metrics, "write")...)
	return records, nil
}

func dataForHotRegionByMetrics(metrics map[helper.TblIndex]helper.RegionMetric, tp string) [][]types.Datum {
	trace_util_0.Count(_tables_00000, 275)
	rows := make([][]types.Datum, 0, len(metrics))
	for tblIndex, regionMetric := range metrics {
		trace_util_0.Count(_tables_00000, 277)
		row := make([]types.Datum, len(tableTiDBHotRegionsCols))
		if tblIndex.IndexName != "" {
			trace_util_0.Count(_tables_00000, 279)
			row[1].SetInt64(tblIndex.IndexID)
			row[4].SetString(tblIndex.IndexName)
		} else {
			trace_util_0.Count(_tables_00000, 280)
			{
				row[1].SetNull()
				row[4].SetNull()
			}
		}
		trace_util_0.Count(_tables_00000, 278)
		row[0].SetInt64(tblIndex.TableID)
		row[2].SetString(tblIndex.DbName)
		row[3].SetString(tblIndex.TableName)
		row[5].SetString(tp)
		row[6].SetInt64(int64(regionMetric.MaxHotDegree))
		row[7].SetInt64(int64(regionMetric.Count))
		row[8].SetUint64(regionMetric.FlowBytes)
		rows = append(rows, row)
	}
	trace_util_0.Count(_tables_00000, 276)
	return rows
}

// DataForAnalyzeStatus gets all the analyze jobs.
func DataForAnalyzeStatus() (rows [][]types.Datum) {
	trace_util_0.Count(_tables_00000, 281)
	for _, job := range statistics.GetAllAnalyzeJobs() {
		trace_util_0.Count(_tables_00000, 283)
		job.Lock()
		var startTime interface{}
		if job.StartTime.IsZero() {
			trace_util_0.Count(_tables_00000, 285)
			startTime = nil
		} else {
			trace_util_0.Count(_tables_00000, 286)
			{
				startTime = types.Time{Time: types.FromGoTime(job.StartTime), Type: mysql.TypeDatetime}
			}
		}
		trace_util_0.Count(_tables_00000, 284)
		rows = append(rows, types.MakeDatums(
			job.DBName,        // TABLE_SCHEMA
			job.TableName,     // TABLE_NAME
			job.PartitionName, // PARTITION_NAME
			job.JobInfo,       // JOB_INFO
			job.RowCount,      // ROW_COUNT
			startTime,         // START_TIME
			job.State,         // STATE
		))
		job.Unlock()
	}
	trace_util_0.Count(_tables_00000, 282)
	return
}

var tableNameToColumns = map[string][]columnInfo{
	tableSchemata:                           schemataCols,
	tableTables:                             tablesCols,
	tableColumns:                            columnsCols,
	tableStatistics:                         statisticsCols,
	tableCharacterSets:                      charsetCols,
	tableCollations:                         collationsCols,
	tableFiles:                              filesCols,
	tableProfiling:                          profilingCols,
	tablePartitions:                         partitionsCols,
	tableKeyColumm:                          keyColumnUsageCols,
	tableReferConst:                         referConstCols,
	tableSessionVar:                         sessionVarCols,
	tablePlugins:                            pluginsCols,
	tableConstraints:                        tableConstraintsCols,
	tableTriggers:                           tableTriggersCols,
	tableUserPrivileges:                     tableUserPrivilegesCols,
	tableSchemaPrivileges:                   tableSchemaPrivilegesCols,
	tableTablePrivileges:                    tableTablePrivilegesCols,
	tableColumnPrivileges:                   tableColumnPrivilegesCols,
	tableEngines:                            tableEnginesCols,
	tableViews:                              tableViewsCols,
	tableRoutines:                           tableRoutinesCols,
	tableParameters:                         tableParametersCols,
	tableEvents:                             tableEventsCols,
	tableGlobalStatus:                       tableGlobalStatusCols,
	tableGlobalVariables:                    tableGlobalVariablesCols,
	tableSessionStatus:                      tableSessionStatusCols,
	tableOptimizerTrace:                     tableOptimizerTraceCols,
	tableTableSpaces:                        tableTableSpacesCols,
	tableCollationCharacterSetApplicability: tableCollationCharacterSetApplicabilityCols,
	tableProcesslist:                        tableProcesslistCols,
	tableTiDBIndexes:                        tableTiDBIndexesCols,
	tableSlowLog:                            slowQueryCols,
	tableTiDBHotRegions:                     tableTiDBHotRegionsCols,
	tableTiKVStoreStatus:                    tableTiKVStoreStatusCols,
	tableAnalyzeStatus:                      tableAnalyzeStatusCols,
	tableTiKVRegionStatus:                   tableTiKVRegionStatusCols,
	tableTiKVRegionPeers:                    tableTiKVRegionPeersCols,
}

func createInfoSchemaTable(handle *Handle, meta *model.TableInfo) *infoschemaTable {
	trace_util_0.Count(_tables_00000, 287)
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		trace_util_0.Count(_tables_00000, 289)
		columns[i] = table.ToColumn(col)
	}
	trace_util_0.Count(_tables_00000, 288)
	return &infoschemaTable{
		handle: handle,
		meta:   meta,
		cols:   columns,
	}
}

type infoschemaTable struct {
	handle *Handle
	meta   *model.TableInfo
	cols   []*table.Column
	rows   [][]types.Datum
}

// schemasSorter implements the sort.Interface interface, sorts DBInfo by name.
type schemasSorter []*model.DBInfo

func (s schemasSorter) Len() int {
	trace_util_0.Count(_tables_00000, 290)
	return len(s)
}

func (s schemasSorter) Swap(i, j int) {
	trace_util_0.Count(_tables_00000, 291)
	s[i], s[j] = s[j], s[i]
}

func (s schemasSorter) Less(i, j int) bool {
	trace_util_0.Count(_tables_00000, 292)
	return s[i].Name.L < s[j].Name.L
}

func (it *infoschemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	trace_util_0.Count(_tables_00000, 293)
	is := ctx.GetSessionVars().TxnCtx.InfoSchema.(InfoSchema)
	dbs := is.AllSchemas()
	sort.Sort(schemasSorter(dbs))
	switch it.meta.Name.O {
	case tableSchemata:
		trace_util_0.Count(_tables_00000, 298)
		fullRows = dataForSchemata(dbs)
	case tableTables:
		trace_util_0.Count(_tables_00000, 299)
		fullRows, err = dataForTables(ctx, dbs)
	case tableTiDBIndexes:
		trace_util_0.Count(_tables_00000, 300)
		fullRows, err = dataForIndexes(ctx, dbs)
	case tableColumns:
		trace_util_0.Count(_tables_00000, 301)
		fullRows = dataForColumns(ctx, dbs)
	case tableStatistics:
		trace_util_0.Count(_tables_00000, 302)
		fullRows = dataForStatistics(dbs)
	case tableCharacterSets:
		trace_util_0.Count(_tables_00000, 303)
		fullRows = dataForCharacterSets()
	case tableCollations:
		trace_util_0.Count(_tables_00000, 304)
		fullRows = dataForCollations()
	case tableSessionVar:
		trace_util_0.Count(_tables_00000, 305)
		fullRows, err = dataForSessionVar(ctx)
	case tableConstraints:
		trace_util_0.Count(_tables_00000, 306)
		fullRows = dataForTableConstraints(dbs)
	case tableFiles:
		trace_util_0.Count(_tables_00000, 307)
	case tableProfiling:
		trace_util_0.Count(_tables_00000, 308)
		if v, ok := ctx.GetSessionVars().GetSystemVar("profiling"); ok && variable.TiDBOptOn(v) {
			trace_util_0.Count(_tables_00000, 335)
			fullRows = dataForPseudoProfiling()
		}
	case tablePartitions:
		trace_util_0.Count(_tables_00000, 309)
	case tableKeyColumm:
		trace_util_0.Count(_tables_00000, 310)
		fullRows = dataForKeyColumnUsage(dbs)
	case tableReferConst:
		trace_util_0.Count(_tables_00000, 311)
	case tablePlugins, tableTriggers:
		trace_util_0.Count(_tables_00000, 312)
	case tableUserPrivileges:
		trace_util_0.Count(_tables_00000, 313)
		fullRows = dataForUserPrivileges(ctx)
	case tableEngines:
		trace_util_0.Count(_tables_00000, 314)
		fullRows = dataForEngines()
	case tableViews:
		trace_util_0.Count(_tables_00000, 315)
		fullRows, err = dataForViews(ctx, dbs)
	case tableRoutines:
		trace_util_0.Count(_tables_00000, 316)
	// TODO: Fill the following tables.
	case tableSchemaPrivileges:
		trace_util_0.Count(_tables_00000, 317)
	case tableTablePrivileges:
		trace_util_0.Count(_tables_00000, 318)
	case tableColumnPrivileges:
		trace_util_0.Count(_tables_00000, 319)
	case tableParameters:
		trace_util_0.Count(_tables_00000, 320)
	case tableEvents:
		trace_util_0.Count(_tables_00000, 321)
	case tableGlobalStatus:
		trace_util_0.Count(_tables_00000, 322)
	case tableGlobalVariables:
		trace_util_0.Count(_tables_00000, 323)
	case tableSessionStatus:
		trace_util_0.Count(_tables_00000, 324)
	case tableOptimizerTrace:
		trace_util_0.Count(_tables_00000, 325)
	case tableTableSpaces:
		trace_util_0.Count(_tables_00000, 326)
	case tableCollationCharacterSetApplicability:
		trace_util_0.Count(_tables_00000, 327)
		fullRows = dataForCollationCharacterSetApplicability()
	case tableProcesslist:
		trace_util_0.Count(_tables_00000, 328)
		fullRows = dataForProcesslist(ctx)
	case tableSlowLog:
		trace_util_0.Count(_tables_00000, 329)
		fullRows, err = dataForSlowLog(ctx)
	case tableTiDBHotRegions:
		trace_util_0.Count(_tables_00000, 330)
		fullRows, err = dataForTiDBHotRegions(ctx)
	case tableTiKVStoreStatus:
		trace_util_0.Count(_tables_00000, 331)
		fullRows, err = dataForTiKVStoreStatus(ctx)
	case tableAnalyzeStatus:
		trace_util_0.Count(_tables_00000, 332)
		fullRows = DataForAnalyzeStatus()
	case tableTiKVRegionStatus:
		trace_util_0.Count(_tables_00000, 333)
		fullRows, err = dataForTiKVRegionStatus(ctx)
	case tableTiKVRegionPeers:
		trace_util_0.Count(_tables_00000, 334)
		fullRows, err = dataForTikVRegionPeers(ctx)
	}
	trace_util_0.Count(_tables_00000, 294)
	if err != nil {
		trace_util_0.Count(_tables_00000, 336)
		return nil, err
	}
	trace_util_0.Count(_tables_00000, 295)
	if len(cols) == len(it.cols) {
		trace_util_0.Count(_tables_00000, 337)
		return
	}
	trace_util_0.Count(_tables_00000, 296)
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		trace_util_0.Count(_tables_00000, 338)
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			trace_util_0.Count(_tables_00000, 340)
			row[j] = fullRow[col.Offset]
		}
		trace_util_0.Count(_tables_00000, 339)
		rows[i] = row
	}
	trace_util_0.Count(_tables_00000, 297)
	return rows, nil
}

// IterRecords implements table.Table IterRecords interface.
func (it *infoschemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	trace_util_0.Count(_tables_00000, 341)
	if len(startKey) != 0 {
		trace_util_0.Count(_tables_00000, 345)
		return table.ErrUnsupportedOp
	}
	trace_util_0.Count(_tables_00000, 342)
	rows, err := it.getRows(ctx, cols)
	if err != nil {
		trace_util_0.Count(_tables_00000, 346)
		return err
	}
	trace_util_0.Count(_tables_00000, 343)
	for i, row := range rows {
		trace_util_0.Count(_tables_00000, 347)
		more, err := fn(int64(i), row, cols)
		if err != nil {
			trace_util_0.Count(_tables_00000, 349)
			return err
		}
		trace_util_0.Count(_tables_00000, 348)
		if !more {
			trace_util_0.Count(_tables_00000, 350)
			break
		}
	}
	trace_util_0.Count(_tables_00000, 344)
	return nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (it *infoschemaTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 351)
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (it *infoschemaTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 352)
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Cols interface.
func (it *infoschemaTable) Cols() []*table.Column {
	trace_util_0.Count(_tables_00000, 353)
	return it.cols
}

// WritableCols implements table.Table WritableCols interface.
func (it *infoschemaTable) WritableCols() []*table.Column {
	trace_util_0.Count(_tables_00000, 354)
	return it.cols
}

// Indices implements table.Table Indices interface.
func (it *infoschemaTable) Indices() []table.Index {
	trace_util_0.Count(_tables_00000, 355)
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (it *infoschemaTable) WritableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 356)
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (it *infoschemaTable) DeletableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 357)
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (it *infoschemaTable) RecordPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 358)
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (it *infoschemaTable) IndexPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 359)
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (it *infoschemaTable) FirstKey() kv.Key {
	trace_util_0.Count(_tables_00000, 360)
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (it *infoschemaTable) RecordKey(h int64) kv.Key {
	trace_util_0.Count(_tables_00000, 361)
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (it *infoschemaTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	trace_util_0.Count(_tables_00000, 362)
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (it *infoschemaTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	trace_util_0.Count(_tables_00000, 363)
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (it *infoschemaTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	trace_util_0.Count(_tables_00000, 364)
	return table.ErrUnsupportedOp
}

// AllocAutoIncrementValue implements table.Table AllocAutoIncrementValue interface.
func (it *infoschemaTable) AllocAutoIncrementValue(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 365)
	return 0, table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (it *infoschemaTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 366)
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (it *infoschemaTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	trace_util_0.Count(_tables_00000, 367)
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (it *infoschemaTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	trace_util_0.Count(_tables_00000, 368)
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Meta interface.
func (it *infoschemaTable) Meta() *model.TableInfo {
	trace_util_0.Count(_tables_00000, 369)
	return it.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (it *infoschemaTable) GetPhysicalID() int64 {
	trace_util_0.Count(_tables_00000, 370)
	return it.meta.ID
}

// Seek implements table.Table Seek interface.
func (it *infoschemaTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	trace_util_0.Count(_tables_00000, 371)
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (it *infoschemaTable) Type() table.Type {
	trace_util_0.Count(_tables_00000, 372)
	return table.VirtualTable
}

// VirtualTable is a dummy table.Table implementation.
type VirtualTable struct{}

// IterRecords implements table.Table IterRecords interface.
func (vt *VirtualTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	trace_util_0.Count(_tables_00000, 373)
	if len(startKey) != 0 {
		trace_util_0.Count(_tables_00000, 375)
		return table.ErrUnsupportedOp
	}
	trace_util_0.Count(_tables_00000, 374)
	return nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (vt *VirtualTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 376)
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (vt *VirtualTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	trace_util_0.Count(_tables_00000, 377)
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Cols interface.
func (vt *VirtualTable) Cols() []*table.Column {
	trace_util_0.Count(_tables_00000, 378)
	return nil
}

// WritableCols implements table.Table WritableCols interface.
func (vt *VirtualTable) WritableCols() []*table.Column {
	trace_util_0.Count(_tables_00000, 379)
	return nil
}

// Indices implements table.Table Indices interface.
func (vt *VirtualTable) Indices() []table.Index {
	trace_util_0.Count(_tables_00000, 380)
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (vt *VirtualTable) WritableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 381)
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (vt *VirtualTable) DeletableIndices() []table.Index {
	trace_util_0.Count(_tables_00000, 382)
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (vt *VirtualTable) RecordPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 383)
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (vt *VirtualTable) IndexPrefix() kv.Key {
	trace_util_0.Count(_tables_00000, 384)
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (vt *VirtualTable) FirstKey() kv.Key {
	trace_util_0.Count(_tables_00000, 385)
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (vt *VirtualTable) RecordKey(h int64) kv.Key {
	trace_util_0.Count(_tables_00000, 386)
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (vt *VirtualTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	trace_util_0.Count(_tables_00000, 387)
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (vt *VirtualTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	trace_util_0.Count(_tables_00000, 388)
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (vt *VirtualTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	trace_util_0.Count(_tables_00000, 389)
	return table.ErrUnsupportedOp
}

// AllocAutoIncrementValue implements table.Table AllocAutoIncrementValue interface.
func (vt *VirtualTable) AllocAutoIncrementValue(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 390)
	return 0, table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (vt *VirtualTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	trace_util_0.Count(_tables_00000, 391)
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (vt *VirtualTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	trace_util_0.Count(_tables_00000, 392)
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (vt *VirtualTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	trace_util_0.Count(_tables_00000, 393)
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Meta interface.
func (vt *VirtualTable) Meta() *model.TableInfo {
	trace_util_0.Count(_tables_00000, 394)
	return nil
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (vt *VirtualTable) GetPhysicalID() int64 {
	trace_util_0.Count(_tables_00000, 395)
	return 0
}

// Seek implements table.Table Seek interface.
func (vt *VirtualTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	trace_util_0.Count(_tables_00000, 396)
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (vt *VirtualTable) Type() table.Type {
	trace_util_0.Count(_tables_00000, 397)
	return table.VirtualTable
}

var _tables_00000 = "infoschema/tables.go"
