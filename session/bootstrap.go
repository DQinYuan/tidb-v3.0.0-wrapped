// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package session

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host				CHAR(64),
		User				CHAR(32),
		Password			CHAR(41),
		Select_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE if not exists mysql.db (
		Host			CHAR(60),
		DB			CHAR(64),
		User			CHAR(32),
		Select_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Insert_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Update_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Delete_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Drop_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Grant_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		References_priv 	ENUM('N','Y') Not Null DEFAULT 'N',
		Index_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Alter_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Event_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGloablVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGloablVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE if not exists mysql.tidb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`

	// CreateHelpTopic is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopic = `CREATE TABLE if not exists mysql.help_topic (
  		help_topic_id int(10) unsigned NOT NULL,
  		name char(64) NOT NULL,
  		help_category_id smallint(5) unsigned NOT NULL,
  		description text NOT NULL,
  		example text NOT NULL,
  		url text NOT NULL,
  		PRIMARY KEY (help_topic_id),
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE if not exists mysql.stats_histograms (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		distinct_count bigint(64) NOT NULL,
		null_count bigint(64) NOT NULL DEFAULT 0,
		tot_col_size bigint(64) NOT NULL DEFAULT 0,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) unsigned NOT NULL DEFAULT 0,
		cm_sketch blob,
		stats_ver bigint(64) NOT NULL DEFAULT 0,
		flag bigint(64) NOT NULL DEFAULT 0,
		correlation double NOT NULL DEFAULT 0,
		last_analyze_pos blob DEFAULT NULL,
		unique index tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists mysql.stats_buckets (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		bucket_id bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		repeats bigint(64) NOT NULL,
		upper_bound blob NOT NULL,
		lower_bound blob ,
		unique index tbl(table_id, is_index, hist_id, bucket_id)
	);`

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsFeedbackTable stores the feedback info which is used to update stats.
	CreateStatsFeedbackTable = `CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		feedback blob NOT NULL,
		index hist(table_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the sql bind info which is used to update globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
		original_sql text NOT NULL  ,
      	bind_sql text NOT NULL ,
      	default_db text  NOT NULL,
		status text NOT NULL,
		create_time timestamp(3) NOT NULL,
		update_time timestamp(3) NOT NULL,
		charset text NOT NULL,
		collation text NOT NULL,
		INDEX sql_index(original_sql(1024),default_db(1024)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateRoleEdgesTable stores the role and user relationship information.
	CreateRoleEdgesTable = `CREATE TABLE IF NOT EXISTS mysql.role_edges (
		FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);`

	// CreateDefaultRolesTable stores the active roles for a user.
	CreateDefaultRolesTable = `CREATE TABLE IF NOT EXISTS mysql.default_roles (
		HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
	)`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE if not exists mysql.stats_top_n (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		value longblob,
		count bigint(64) UNSIGNED NOT NULL,
		index tbl(table_id, is_index, hist_id)
	);`

	// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name char(100) NOT NULL
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	trace_util_0.Count(_bootstrap_00000, 0)
	startTime := time.Now()
	dom := domain.GetDomain(s)
	for {
		trace_util_0.Count(_bootstrap_00000, 1)
		b, err := checkBootstrapped(s)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 5)
			logutil.Logger(context.Background()).Fatal("check bootstrap error",
				zap.Error(err))
		}
		// For rolling upgrade, we can't do upgrade only in the owner.
		trace_util_0.Count(_bootstrap_00000, 2)
		if b {
			trace_util_0.Count(_bootstrap_00000, 6)
			upgrade(s)
			logutil.Logger(context.Background()).Info("upgrade successful in bootstrap",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		// To reduce conflict when multiple TiDB-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose DDL owner to do this.
		trace_util_0.Count(_bootstrap_00000, 3)
		if dom.DDL().OwnerManager().IsOwner() {
			trace_util_0.Count(_bootstrap_00000, 7)
			doDDLWorks(s)
			doDMLWorks(s)
			logutil.Logger(context.Background()).Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		trace_util_0.Count(_bootstrap_00000, 4)
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// The variable name in mysql.TiDB table.
	// It is used for checking if the store is boostrapped by any TiDB server.
	bootstrappedVar = "bootstrapped"
	// The variable value in mysql.TiDB table for bootstrappedVar.
	// If the value true, the store is already boostrapped by a TiDB server.
	bootstrappedVarTrue = "True"
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB server which bootstrapped the store.
	tidbServerVersionVar = "tidb_server_version"
	// The variable name in mysql.tidb table and it will be used when we want to know
	// system timezone.
	tidbSystemTZ = "system_tz"
	// Const for TiDB server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
	version14 = 14
	version15 = 15
	version16 = 16
	version17 = 17
	version18 = 18
	version19 = 19
	version20 = 20
	version21 = 21
	version22 = 22
	version23 = 23
	version24 = 24
	version25 = 25
	version26 = 26
	version27 = 27
	version28 = 28
	version29 = 29
	version30 = 30
	version31 = 31
	version32 = 32
	version33 = 33
)

func checkBootstrapped(s Session) (bool, error) {
	trace_util_0.Count(_bootstrap_00000, 8)
	//  Check if system db exists.
	_, err := s.Execute(context.Background(), fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err != nil && infoschema.ErrDatabaseNotExists.NotEqual(err) {
		trace_util_0.Count(_bootstrap_00000, 12)
		logutil.Logger(context.Background()).Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in TiDB table.
	trace_util_0.Count(_bootstrap_00000, 9)
	sVal, _, err := getTiDBVar(s, bootstrappedVar)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 13)
		if infoschema.ErrTableNotExists.Equal(err) {
			trace_util_0.Count(_bootstrap_00000, 15)
			return false, nil
		}
		trace_util_0.Count(_bootstrap_00000, 14)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 10)
	isBootstrapped := sVal == bootstrappedVarTrue
	if isBootstrapped {
		trace_util_0.Count(_bootstrap_00000, 16)
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(context.Background()); err != nil {
			trace_util_0.Count(_bootstrap_00000, 17)
			return false, errors.Trace(err)
		}
	}
	trace_util_0.Count(_bootstrap_00000, 11)
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s Session, name string) (sVal string, isNull bool, e error) {
	trace_util_0.Count(_bootstrap_00000, 18)
	sql := fmt.Sprintf(`SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, name)
	ctx := context.Background()
	rs, err := s.Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 23)
		return "", true, errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 19)
	if len(rs) != 1 {
		trace_util_0.Count(_bootstrap_00000, 24)
		return "", true, errors.New("Wrong number of Recordset")
	}
	trace_util_0.Count(_bootstrap_00000, 20)
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewRecordBatch()
	err = r.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		trace_util_0.Count(_bootstrap_00000, 25)
		return "", true, errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 21)
	row := req.GetRow(0)
	if row.IsNull(0) {
		trace_util_0.Count(_bootstrap_00000, 26)
		return "", true, nil
	}
	trace_util_0.Count(_bootstrap_00000, 22)
	return row.GetString(0), false, nil
}

// upgrade function  will do some upgrade works, when the system is boostrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s Session) {
	trace_util_0.Count(_bootstrap_00000, 27)
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		trace_util_0.Count(_bootstrap_00000, 61)
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}
	// Do upgrade works then update bootstrap version.
	trace_util_0.Count(_bootstrap_00000, 28)
	if ver < version2 {
		trace_util_0.Count(_bootstrap_00000, 62)
		upgradeToVer2(s)
		ver = version2
	}
	trace_util_0.Count(_bootstrap_00000, 29)
	if ver < version3 {
		trace_util_0.Count(_bootstrap_00000, 63)
		upgradeToVer3(s)
	}
	trace_util_0.Count(_bootstrap_00000, 30)
	if ver < version4 {
		trace_util_0.Count(_bootstrap_00000, 64)
		upgradeToVer4(s)
	}

	trace_util_0.Count(_bootstrap_00000, 31)
	if ver < version5 {
		trace_util_0.Count(_bootstrap_00000, 65)
		upgradeToVer5(s)
	}

	trace_util_0.Count(_bootstrap_00000, 32)
	if ver < version6 {
		trace_util_0.Count(_bootstrap_00000, 66)
		upgradeToVer6(s)
	}

	trace_util_0.Count(_bootstrap_00000, 33)
	if ver < version7 {
		trace_util_0.Count(_bootstrap_00000, 67)
		upgradeToVer7(s)
	}

	trace_util_0.Count(_bootstrap_00000, 34)
	if ver < version8 {
		trace_util_0.Count(_bootstrap_00000, 68)
		upgradeToVer8(s)
	}

	trace_util_0.Count(_bootstrap_00000, 35)
	if ver < version9 {
		trace_util_0.Count(_bootstrap_00000, 69)
		upgradeToVer9(s)
	}

	trace_util_0.Count(_bootstrap_00000, 36)
	if ver < version10 {
		trace_util_0.Count(_bootstrap_00000, 70)
		upgradeToVer10(s)
	}

	trace_util_0.Count(_bootstrap_00000, 37)
	if ver < version11 {
		trace_util_0.Count(_bootstrap_00000, 71)
		upgradeToVer11(s)
	}

	trace_util_0.Count(_bootstrap_00000, 38)
	if ver < version12 {
		trace_util_0.Count(_bootstrap_00000, 72)
		upgradeToVer12(s)
	}

	trace_util_0.Count(_bootstrap_00000, 39)
	if ver < version13 {
		trace_util_0.Count(_bootstrap_00000, 73)
		upgradeToVer13(s)
	}

	trace_util_0.Count(_bootstrap_00000, 40)
	if ver < version14 {
		trace_util_0.Count(_bootstrap_00000, 74)
		upgradeToVer14(s)
	}

	trace_util_0.Count(_bootstrap_00000, 41)
	if ver < version15 {
		trace_util_0.Count(_bootstrap_00000, 75)
		upgradeToVer15(s)
	}

	trace_util_0.Count(_bootstrap_00000, 42)
	if ver < version16 {
		trace_util_0.Count(_bootstrap_00000, 76)
		upgradeToVer16(s)
	}

	trace_util_0.Count(_bootstrap_00000, 43)
	if ver < version17 {
		trace_util_0.Count(_bootstrap_00000, 77)
		upgradeToVer17(s)
	}

	trace_util_0.Count(_bootstrap_00000, 44)
	if ver < version18 {
		trace_util_0.Count(_bootstrap_00000, 78)
		upgradeToVer18(s)
	}

	trace_util_0.Count(_bootstrap_00000, 45)
	if ver < version19 {
		trace_util_0.Count(_bootstrap_00000, 79)
		upgradeToVer19(s)
	}

	trace_util_0.Count(_bootstrap_00000, 46)
	if ver < version20 {
		trace_util_0.Count(_bootstrap_00000, 80)
		upgradeToVer20(s)
	}

	trace_util_0.Count(_bootstrap_00000, 47)
	if ver < version21 {
		trace_util_0.Count(_bootstrap_00000, 81)
		upgradeToVer21(s)
	}

	trace_util_0.Count(_bootstrap_00000, 48)
	if ver < version22 {
		trace_util_0.Count(_bootstrap_00000, 82)
		upgradeToVer22(s)
	}

	trace_util_0.Count(_bootstrap_00000, 49)
	if ver < version23 {
		trace_util_0.Count(_bootstrap_00000, 83)
		upgradeToVer23(s)
	}

	trace_util_0.Count(_bootstrap_00000, 50)
	if ver < version24 {
		trace_util_0.Count(_bootstrap_00000, 84)
		upgradeToVer24(s)
	}

	trace_util_0.Count(_bootstrap_00000, 51)
	if ver < version25 {
		trace_util_0.Count(_bootstrap_00000, 85)
		upgradeToVer25(s)
	}

	trace_util_0.Count(_bootstrap_00000, 52)
	if ver < version26 {
		trace_util_0.Count(_bootstrap_00000, 86)
		upgradeToVer26(s)
	}

	trace_util_0.Count(_bootstrap_00000, 53)
	if ver < version27 {
		trace_util_0.Count(_bootstrap_00000, 87)
		upgradeToVer27(s)
	}

	trace_util_0.Count(_bootstrap_00000, 54)
	if ver < version28 {
		trace_util_0.Count(_bootstrap_00000, 88)
		upgradeToVer28(s)
	}

	trace_util_0.Count(_bootstrap_00000, 55)
	if ver == version28 {
		trace_util_0.Count(_bootstrap_00000, 89)
		upgradeToVer29(s)
	}

	trace_util_0.Count(_bootstrap_00000, 56)
	if ver < version30 {
		trace_util_0.Count(_bootstrap_00000, 90)
		upgradeToVer30(s)
	}

	trace_util_0.Count(_bootstrap_00000, 57)
	if ver < version31 {
		trace_util_0.Count(_bootstrap_00000, 91)
		upgradeToVer31(s)
	}

	trace_util_0.Count(_bootstrap_00000, 58)
	if ver < version32 {
		trace_util_0.Count(_bootstrap_00000, 92)
		upgradeToVer29(s)
	}

	trace_util_0.Count(_bootstrap_00000, 59)
	if ver < version33 {
		trace_util_0.Count(_bootstrap_00000, 93)
		upgradeToVer33(s)
	}

	trace_util_0.Count(_bootstrap_00000, 60)
	updateBootstrapVer(s)
	_, err = s.Execute(context.Background(), "COMMIT")

	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 94)
		sleepTime := 1 * time.Second
		logutil.Logger(context.Background()).Info("update bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			trace_util_0.Count(_bootstrap_00000, 97)
			logutil.Logger(context.Background()).Fatal("upgrade failed", zap.Error(err1))
		}
		trace_util_0.Count(_bootstrap_00000, 95)
		if v >= currentBootstrapVersion {
			trace_util_0.Count(_bootstrap_00000, 98)
			// It is already bootstrapped/upgraded by a higher version TiDB server.
			return
		}
		trace_util_0.Count(_bootstrap_00000, 96)
		logutil.Logger(context.Background()).Fatal("[Upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s Session) {
	trace_util_0.Count(_bootstrap_00000, 99)
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.TiDBDistSQLScanConcurrency}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		trace_util_0.Count(_bootstrap_00000, 101)
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	trace_util_0.Count(_bootstrap_00000, 100)
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// upgradeToVer3 updates to version 3.
func upgradeToVer3(s Session) {
	trace_util_0.Count(_bootstrap_00000, 102)
	// Version 3 fix tx_read_only variable value.
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		mysql.SystemDB, mysql.GlobalVariablesTable)
	mustExecute(s, sql)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s Session) {
	trace_util_0.Count(_bootstrap_00000, 103)
	sql := CreateStatsMetaTable
	mustExecute(s, sql)
}

func upgradeToVer5(s Session) {
	trace_util_0.Count(_bootstrap_00000, 104)
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s Session) {
	trace_util_0.Count(_bootstrap_00000, 105)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s Session) {
	trace_util_0.Count(_bootstrap_00000, 106)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s Session) {
	trace_util_0.Count(_bootstrap_00000, 107)
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.Execute(context.Background(), "SELECT HIGH_PRIORITY `Process_priv` from mysql.user limit 0"); err == nil {
		trace_util_0.Count(_bootstrap_00000, 109)
		return
	}
	trace_util_0.Count(_bootstrap_00000, 108)
	upgradeToVer7(s)
}

func upgradeToVer9(s Session) {
	trace_util_0.Count(_bootstrap_00000, 110)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s Session, sql string, ignorableErrs ...error) {
	trace_util_0.Count(_bootstrap_00000, 111)
	_, err := s.Execute(context.Background(), sql)
	for _, ignorableErr := range ignorableErrs {
		trace_util_0.Count(_bootstrap_00000, 113)
		if terror.ErrorEqual(err, ignorableErr) {
			trace_util_0.Count(_bootstrap_00000, 114)
			return
		}
	}
	trace_util_0.Count(_bootstrap_00000, 112)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 115)
		logutil.Logger(context.Background()).Fatal("doReentrantDDL error", zap.Error(err))
	}
}

func upgradeToVer10(s Session) {
	trace_util_0.Count(_bootstrap_00000, 116)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", ddl.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", ddl.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s Session) {
	trace_util_0.Count(_bootstrap_00000, 117)
	_, err := s.Execute(context.Background(), "ALTER TABLE mysql.user ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 119)
		if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
			trace_util_0.Count(_bootstrap_00000, 121)
			return
		}
		trace_util_0.Count(_bootstrap_00000, 120)
		logutil.Logger(context.Background()).Fatal("upgradeToVer11 error", zap.Error(err))
	}
	trace_util_0.Count(_bootstrap_00000, 118)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s Session) {
	trace_util_0.Count(_bootstrap_00000, 122)
	ctx := context.Background()
	_, err := s.Execute(ctx, "BEGIN")
	terror.MustNil(err)
	sql := "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.Execute(ctx, sql)
	terror.MustNil(err)
	r := rs[0]
	sqls := make([]string, 0, 1)
	defer terror.Call(r.Close)
	req := r.NewRecordBatch()
	it := chunk.NewIterator4Chunk(req.Chunk)
	err = r.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		trace_util_0.Count(_bootstrap_00000, 125)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			trace_util_0.Count(_bootstrap_00000, 127)
			user := row.GetString(0)
			host := row.GetString(1)
			pass := row.GetString(2)
			var newPass string
			newPass, err = oldPasswordUpgrade(pass)
			terror.MustNil(err)
			updateSQL := fmt.Sprintf(`UPDATE HIGH_PRIORITY mysql.user set password = "%s" where user="%s" and host="%s"`, newPass, user, host)
			sqls = append(sqls, updateSQL)
		}
		trace_util_0.Count(_bootstrap_00000, 126)
		err = r.Next(ctx, req)
	}
	trace_util_0.Count(_bootstrap_00000, 123)
	terror.MustNil(err)

	for _, sql := range sqls {
		trace_util_0.Count(_bootstrap_00000, 128)
		mustExecute(s, sql)
	}

	trace_util_0.Count(_bootstrap_00000, 124)
	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, version12, version12)
	mustExecute(s, sql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s Session) {
	trace_util_0.Count(_bootstrap_00000, 129)
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		trace_util_0.Count(_bootstrap_00000, 131)
		_, err := s.Execute(ctx, sql)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 132)
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				trace_util_0.Count(_bootstrap_00000, 134)
				continue
			}
			trace_util_0.Count(_bootstrap_00000, 133)
			logutil.Logger(context.Background()).Fatal("upgradeToVer13 error", zap.Error(err))
		}
	}
	trace_util_0.Count(_bootstrap_00000, 130)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_view_priv='Y',Show_view_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y'")
}

func upgradeToVer14(s Session) {
	trace_util_0.Count(_bootstrap_00000, 135)
	sqls := []string{
		"ALTER TABLE mysql.db ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_tables_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		trace_util_0.Count(_bootstrap_00000, 136)
		_, err := s.Execute(ctx, sql)
		if err != nil {
			trace_util_0.Count(_bootstrap_00000, 137)
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				trace_util_0.Count(_bootstrap_00000, 139)
				continue
			}
			trace_util_0.Count(_bootstrap_00000, 138)
			logutil.Logger(context.Background()).Fatal("upgradeToVer14 error", zap.Error(err))
		}
	}
}

func upgradeToVer15(s Session) {
	trace_util_0.Count(_bootstrap_00000, 140)
	var err error
	_, err = s.Execute(context.Background(), CreateGCDeleteRangeTable)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 141)
		logutil.Logger(context.Background()).Fatal("upgradeToVer15 error", zap.Error(err))
	}
}

func upgradeToVer16(s Session) {
	trace_util_0.Count(_bootstrap_00000, 142)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` blob", infoschema.ErrColumnExists)
}

func upgradeToVer17(s Session) {
	trace_util_0.Count(_bootstrap_00000, 143)
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s Session) {
	trace_util_0.Count(_bootstrap_00000, 144)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer19(s Session) {
	trace_util_0.Count(_bootstrap_00000, 145)
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s Session) {
	trace_util_0.Count(_bootstrap_00000, 146)
	doReentrantDDL(s, CreateStatsFeedbackTable)
}

func upgradeToVer21(s Session) {
	trace_util_0.Count(_bootstrap_00000, 147)
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", ddl.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", ddl.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", ddl.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s Session) {
	trace_util_0.Count(_bootstrap_00000, 148)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer23(s Session) {
	trace_util_0.Count(_bootstrap_00000, 149)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.tidb
func writeSystemTZ(s Session) {
	trace_util_0.Count(_bootstrap_00000, 150)
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%s", "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, tidbSystemTZ, timeutil.InferSystemTZ(), timeutil.InferSystemTZ())
	mustExecute(s, sql)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s Session) {
	trace_util_0.Count(_bootstrap_00000, 151)
	writeSystemTZ(s)
}

// upgradeToVer25 updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s Session) {
	trace_util_0.Count(_bootstrap_00000, 152)
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBMaxChunkSize, variable.DefInitChunkSize)
	mustExecute(s, sql)
}

func upgradeToVer26(s Session) {
	trace_util_0.Count(_bootstrap_00000, 153)
	mustExecute(s, CreateRoleEdgesTable)
	mustExecute(s, CreateDefaultRolesTable)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y')", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y')", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y')", infoschema.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y'")
}

func upgradeToVer27(s Session) {
	trace_util_0.Count(_bootstrap_00000, 154)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` double NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer28(s Session) {
	trace_util_0.Count(_bootstrap_00000, 155)
	doReentrantDDL(s, CreateBindInfoTable)
}

func upgradeToVer29(s Session) {
	trace_util_0.Count(_bootstrap_00000, 156)
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info change create_time create_time timestamp(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info change update_time update_time timestamp(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info add index sql_index (original_sql(1024),default_db(1024))", ddl.ErrDupKeyName)
}

func upgradeToVer30(s Session) {
	trace_util_0.Count(_bootstrap_00000, 157)
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s Session) {
	trace_util_0.Count(_bootstrap_00000, 158)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` blob default null", infoschema.ErrColumnExists)
}

func upgradeToVer32(s Session) {
	trace_util_0.Count(_bootstrap_00000, 159)
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s Session) {
	trace_util_0.Count(_bootstrap_00000, 160)
	doReentrantDDL(s, CreateExprPushdownBlacklist)
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s Session) {
	trace_util_0.Count(_bootstrap_00000, 161)
	// Update bootstrap version.
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, sql)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s Session) (int64, error) {
	trace_util_0.Count(_bootstrap_00000, 162)
	sVal, isNull, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 165)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_bootstrap_00000, 163)
	if isNull {
		trace_util_0.Count(_bootstrap_00000, 166)
		return 0, nil
	}
	trace_util_0.Count(_bootstrap_00000, 164)
	return strconv.ParseInt(sVal, 10, 64)
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	trace_util_0.Count(_bootstrap_00000, 167)
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege tables.
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global system variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range table.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done table.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_feedback table.
	mustExecute(s, CreateStatsFeedbackTable)
	// Create role_edges table.
	mustExecute(s, CreateRoleEdgesTable)
	// Create default_roles table.
	mustExecute(s, CreateDefaultRolesTable)
	// Create bind_info table.
	mustExecute(s, CreateBindInfoTable)
	// Create stats_topn_store table.
	mustExecute(s, CreateStatsTopNTable)
	// Create expr_pushdown_blacklist table.
	mustExecute(s, CreateExprPushdownBlacklist)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	trace_util_0.Count(_bootstrap_00000, 168)
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		trace_util_0.Count(_bootstrap_00000, 170)
		// Session only variable should not be inserted.
		if v.Scope != variable.ScopeSession {
			trace_util_0.Count(_bootstrap_00000, 171)
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
			values = append(values, value)
		}
	}
	trace_util_0.Count(_bootstrap_00000, 169)
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, bootstrappedVarTrue, bootstrappedVarTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion)
	mustExecute(s, sql)

	writeSystemTZ(s)
	_, err := s.Execute(context.Background(), "COMMIT")
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 172)
		sleepTime := 1 * time.Second
		logutil.Logger(context.Background()).Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			trace_util_0.Count(_bootstrap_00000, 175)
			logutil.Logger(context.Background()).Fatal("doDMLWorks failed", zap.Error(err1))
		}
		trace_util_0.Count(_bootstrap_00000, 173)
		if b {
			trace_util_0.Count(_bootstrap_00000, 176)
			return
		}
		trace_util_0.Count(_bootstrap_00000, 174)
		logutil.Logger(context.Background()).Fatal("doDMLWorks failed", zap.Error(err))
	}
}

func mustExecute(s Session, sql string) {
	trace_util_0.Count(_bootstrap_00000, 177)
	_, err := s.Execute(context.Background(), sql)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 178)
		debug.PrintStack()
		logutil.Logger(context.Background()).Fatal("mustExecute error", zap.Error(err))
	}
}

// oldPasswordUpgrade upgrade password to MySQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	trace_util_0.Count(_bootstrap_00000, 179)
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		trace_util_0.Count(_bootstrap_00000, 181)
		return "", errors.Trace(err)
	}

	trace_util_0.Count(_bootstrap_00000, 180)
	hash2 := auth.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}

var _bootstrap_00000 = "session/bootstrap.go"
