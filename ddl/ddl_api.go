// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (d *ddl) CreateSchema(ctx sessionctx.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) (err error) {
	trace_util_0.Count(_ddl_api_00000, 0)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.SchemaByName(schema)
	if ok {
		trace_util_0.Count(_ddl_api_00000, 5)
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(schema)
	}

	trace_util_0.Count(_ddl_api_00000, 1)
	if err = checkTooLongSchema(schema); err != nil {
		trace_util_0.Count(_ddl_api_00000, 6)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 2)
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 7)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 3)
	schemaID := genIDs[0]
	dbInfo := &model.DBInfo{
		Name: schema,
	}

	if charsetInfo != nil {
		trace_util_0.Count(_ddl_api_00000, 8)
		err = checkCharsetAndCollation(charsetInfo.Chs, charsetInfo.Col)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 10)
			return errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 9)
		dbInfo.Charset = charsetInfo.Chs
		dbInfo.Collate = charsetInfo.Col
	} else {
		trace_util_0.Count(_ddl_api_00000, 11)
		{
			dbInfo.Charset, dbInfo.Collate = charset.GetDefaultCharsetAndCollate()
		}
	}

	trace_util_0.Count(_ddl_api_00000, 4)
	job := &model.Job{
		SchemaID:   schemaID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}
func (d *ddl) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	trace_util_0.Count(_ddl_api_00000, 12)
	// Resolve target charset and collation from options.
	var toCharset, toCollate string
	for _, val := range stmt.Options {
		trace_util_0.Count(_ddl_api_00000, 18)
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			trace_util_0.Count(_ddl_api_00000, 19)
			if toCharset == "" {
				trace_util_0.Count(_ddl_api_00000, 23)
				toCharset = val.Value
			} else {
				trace_util_0.Count(_ddl_api_00000, 24)
				if toCharset != val.Value {
					trace_util_0.Count(_ddl_api_00000, 25)
					return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
				}
			}
		case ast.DatabaseOptionCollate:
			trace_util_0.Count(_ddl_api_00000, 20)
			info, err := charset.GetCollationByName(val.Value)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 26)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 21)
			if toCharset == "" {
				trace_util_0.Count(_ddl_api_00000, 27)
				toCharset = info.CharsetName
			} else {
				trace_util_0.Count(_ddl_api_00000, 28)
				if toCharset != info.CharsetName {
					trace_util_0.Count(_ddl_api_00000, 29)
					return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
				}
			}
			trace_util_0.Count(_ddl_api_00000, 22)
			toCollate = info.Name
		}
	}
	trace_util_0.Count(_ddl_api_00000, 13)
	if toCollate == "" {
		trace_util_0.Count(_ddl_api_00000, 30)
		if toCollate, err = charset.GetDefaultCollation(toCharset); err != nil {
			trace_util_0.Count(_ddl_api_00000, 31)
			return errors.Trace(err)
		}
	}

	// Check if need to change charset/collation.
	trace_util_0.Count(_ddl_api_00000, 14)
	dbName := model.NewCIStr(stmt.Name)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 32)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}
	trace_util_0.Count(_ddl_api_00000, 15)
	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		trace_util_0.Count(_ddl_api_00000, 33)
		return nil
	}

	// Check the current TiDB limitations.
	trace_util_0.Count(_ddl_api_00000, 16)
	if err = modifiableCharsetAndCollation(toCharset, toCollate, dbInfo.Charset, dbInfo.Collate); err != nil {
		trace_util_0.Count(_ddl_api_00000, 34)
		return errors.Trace(err)
	}

	// Do the DDL job.
	trace_util_0.Count(_ddl_api_00000, 17)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionModifySchemaCharsetAndCollate,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{toCharset, toCollate},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx sessionctx.Context, schema model.CIStr) (err error) {
	trace_util_0.Count(_ddl_api_00000, 35)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	old, ok := is.SchemaByName(schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 37)
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	trace_util_0.Count(_ddl_api_00000, 36)
	job := &model.Job{
		SchemaID:   old.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTooLongSchema(schema model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 38)
	if len(schema.L) > mysql.MaxDatabaseNameLength {
		trace_util_0.Count(_ddl_api_00000, 40)
		return ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	trace_util_0.Count(_ddl_api_00000, 39)
	return nil
}

func checkTooLongTable(table model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 41)
	if len(table.L) > mysql.MaxTableNameLength {
		trace_util_0.Count(_ddl_api_00000, 43)
		return ErrTooLongIdent.GenWithStackByArgs(table)
	}
	trace_util_0.Count(_ddl_api_00000, 42)
	return nil
}

func checkTooLongIndex(index model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 44)
	if len(index.L) > mysql.MaxIndexIdentifierLen {
		trace_util_0.Count(_ddl_api_00000, 46)
		return ErrTooLongIdent.GenWithStackByArgs(index)
	}
	trace_util_0.Count(_ddl_api_00000, 45)
	return nil
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	trace_util_0.Count(_ddl_api_00000, 47)
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		trace_util_0.Count(_ddl_api_00000, 48)
		for _, key := range v.Keys {
			trace_util_0.Count(_ddl_api_00000, 51)
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				trace_util_0.Count(_ddl_api_00000, 53)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 52)
			c.Flag |= mysql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= mysql.NotNullFlag
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		trace_util_0.Count(_ddl_api_00000, 49)
		for i, key := range v.Keys {
			trace_util_0.Count(_ddl_api_00000, 54)
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				trace_util_0.Count(_ddl_api_00000, 56)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 55)
			if i == 0 {
				trace_util_0.Count(_ddl_api_00000, 57)
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					trace_util_0.Count(_ddl_api_00000, 58)
					c.Flag |= mysql.MultipleKeyFlag
				} else {
					trace_util_0.Count(_ddl_api_00000, 59)
					{
						c.Flag |= mysql.UniqueKeyFlag
					}
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		trace_util_0.Count(_ddl_api_00000, 50)
		for i, key := range v.Keys {
			trace_util_0.Count(_ddl_api_00000, 60)
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				trace_util_0.Count(_ddl_api_00000, 62)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 61)
			if i == 0 {
				trace_util_0.Count(_ddl_api_00000, 63)
				// Only the first column can be set.
				c.Flag |= mysql.MultipleKeyFlag
			}
		}
	}
}

func buildColumnsAndConstraints(ctx sessionctx.Context, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint, tblCharset, dbCharset string) ([]*table.Column, []*ast.Constraint, error) {
	trace_util_0.Count(_ddl_api_00000, 64)
	colMap := map[string]*table.Column{}
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		trace_util_0.Count(_ddl_api_00000, 68)
		if v.Tp == ast.ConstraintPrimaryKey {
			trace_util_0.Count(_ddl_api_00000, 69)
			outPriKeyConstraint = v
			break
		}
	}
	trace_util_0.Count(_ddl_api_00000, 65)
	cols := make([]*table.Column, 0, len(colDefs))
	for i, colDef := range colDefs {
		trace_util_0.Count(_ddl_api_00000, 70)
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, dbCharset)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 72)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 71)
		col.State = model.StatePublic
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	trace_util_0.Count(_ddl_api_00000, 66)
	for _, v := range constraints {
		trace_util_0.Count(_ddl_api_00000, 73)
		setColumnFlagWithConstraint(colMap, v)
	}
	trace_util_0.Count(_ddl_api_00000, 67)
	return cols, constraints, nil
}

// ResolveCharsetCollation will resolve the charset by the order: table charset > database charset > server default charset.
func ResolveCharsetCollation(tblCharset, dbCharset string) (string, string, error) {
	trace_util_0.Count(_ddl_api_00000, 74)
	if len(tblCharset) != 0 {
		trace_util_0.Count(_ddl_api_00000, 77)
		defCollate, err := charset.GetDefaultCollation(tblCharset)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 79)
			// return terror is better.
			return "", "", ErrUnknownCharacterSet.GenWithStackByArgs(tblCharset)
		}
		trace_util_0.Count(_ddl_api_00000, 78)
		return tblCharset, defCollate, nil
	}

	trace_util_0.Count(_ddl_api_00000, 75)
	if len(dbCharset) != 0 {
		trace_util_0.Count(_ddl_api_00000, 80)
		defCollate, err := charset.GetDefaultCollation(dbCharset)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 82)
			return "", "", ErrUnknownCharacterSet.GenWithStackByArgs(dbCharset)
		}
		trace_util_0.Count(_ddl_api_00000, 81)
		return dbCharset, defCollate, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 76)
	charset, collate := charset.GetDefaultCharsetAndCollate()
	return charset, collate, nil
}

func typesNeedCharset(tp byte) bool {
	trace_util_0.Count(_ddl_api_00000, 83)
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeEnum, mysql.TypeSet:
		trace_util_0.Count(_ddl_api_00000, 85)
		return true
	}
	trace_util_0.Count(_ddl_api_00000, 84)
	return false
}

func setCharsetCollationFlenDecimal(tp *types.FieldType, tblCharset string, dbCharset string) error {
	trace_util_0.Count(_ddl_api_00000, 86)
	tp.Charset = strings.ToLower(tp.Charset)
	tp.Collate = strings.ToLower(tp.Collate)
	if len(tp.Charset) == 0 {
		trace_util_0.Count(_ddl_api_00000, 90)
		if typesNeedCharset(tp.Tp) {
			trace_util_0.Count(_ddl_api_00000, 91)
			var err error
			tp.Charset, tp.Collate, err = ResolveCharsetCollation(tblCharset, dbCharset)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 92)
				return errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 93)
			{
				tp.Charset = charset.CharsetBin
				tp.Collate = charset.CharsetBin
			}
		}
	} else {
		trace_util_0.Count(_ddl_api_00000, 94)
		{
			if !charset.ValidCharsetAndCollation(tp.Charset, tp.Collate) {
				trace_util_0.Count(_ddl_api_00000, 96)
				return errUnsupportedCharset.GenWithStackByArgs(tp.Charset, tp.Collate)
			}
			trace_util_0.Count(_ddl_api_00000, 95)
			if len(tp.Collate) == 0 {
				trace_util_0.Count(_ddl_api_00000, 97)
				var err error
				tp.Collate, err = charset.GetDefaultCollation(tp.Charset)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 98)
					return errors.Trace(err)
				}
			}
		}
	}

	// Use default value for flen or decimal when they are unspecified.
	trace_util_0.Count(_ddl_api_00000, 87)
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.Tp)
	if tp.Flen == types.UnspecifiedLength {
		trace_util_0.Count(_ddl_api_00000, 99)
		tp.Flen = defaultFlen
		if mysql.HasUnsignedFlag(tp.Flag) && tp.Tp != mysql.TypeLonglong && mysql.IsIntegerType(tp.Tp) {
			trace_util_0.Count(_ddl_api_00000, 100)
			// Issue #4684: the flen of unsigned integer(except bigint) is 1 digit shorter than signed integer
			// because it has no prefix "+" or "-" character.
			tp.Flen--
		}
	}
	trace_util_0.Count(_ddl_api_00000, 88)
	if tp.Decimal == types.UnspecifiedLength {
		trace_util_0.Count(_ddl_api_00000, 101)
		tp.Decimal = defaultDecimal
	}
	trace_util_0.Count(_ddl_api_00000, 89)
	return nil
}

// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func buildColumnAndConstraint(ctx sessionctx.Context, offset int,
	colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint, tblCharset, dbCharset string) (*table.Column, []*ast.Constraint, error) {
	trace_util_0.Count(_ddl_api_00000, 102)
	if err := setCharsetCollationFlenDecimal(colDef.Tp, tblCharset, dbCharset); err != nil {
		trace_util_0.Count(_ddl_api_00000, 105)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 103)
	col, cts, err := columnDefToCol(ctx, offset, colDef, outPriKeyConstraint)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 106)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 104)
	return col, cts, nil
}

// checkColumnDefaultValue checks the default value of the column.
// In non-strict SQL mode, if the default value of the column is an empty string, the default value can be ignored.
// In strict SQL mode, TEXT/BLOB/JSON can't have not null default values.
// In NO_ZERO_DATE SQL mode, TIMESTAMP/DATE/DATETIME type can't have zero date like '0000-00-00' or '0000-00-00 00:00:00'.
func checkColumnDefaultValue(ctx sessionctx.Context, col *table.Column, value interface{}) (bool, interface{}, error) {
	trace_util_0.Count(_ddl_api_00000, 107)
	hasDefaultValue := true
	if value != nil && (col.Tp == mysql.TypeJSON ||
		col.Tp == mysql.TypeTinyBlob || col.Tp == mysql.TypeMediumBlob ||
		col.Tp == mysql.TypeLongBlob || col.Tp == mysql.TypeBlob) {
		trace_util_0.Count(_ddl_api_00000, 110)
		// In non-strict SQL mode.
		if !ctx.GetSessionVars().SQLMode.HasStrictMode() && value == "" {
			trace_util_0.Count(_ddl_api_00000, 112)
			if col.Tp == mysql.TypeBlob || col.Tp == mysql.TypeLongBlob {
				trace_util_0.Count(_ddl_api_00000, 115)
				// The TEXT/BLOB default value can be ignored.
				hasDefaultValue = false
			}
			// In non-strict SQL mode, if the column type is json and the default value is null, it is initialized to an empty array.
			trace_util_0.Count(_ddl_api_00000, 113)
			if col.Tp == mysql.TypeJSON {
				trace_util_0.Count(_ddl_api_00000, 116)
				value = `null`
			}
			trace_util_0.Count(_ddl_api_00000, 114)
			sc := ctx.GetSessionVars().StmtCtx
			sc.AppendWarning(errBlobCantHaveDefault.GenWithStackByArgs(col.Name.O))
			return hasDefaultValue, value, nil
		}
		// In strict SQL mode or default value is not an empty string.
		trace_util_0.Count(_ddl_api_00000, 111)
		return hasDefaultValue, value, errBlobCantHaveDefault.GenWithStackByArgs(col.Name.O)
	}
	trace_util_0.Count(_ddl_api_00000, 108)
	if value != nil && ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() &&
		ctx.GetSessionVars().SQLMode.HasStrictMode() && types.IsTypeTime(col.Tp) {
		trace_util_0.Count(_ddl_api_00000, 117)
		if vv, ok := value.(string); ok {
			trace_util_0.Count(_ddl_api_00000, 118)
			timeValue, err := expression.GetTimeValue(ctx, vv, col.Tp, col.Decimal)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 120)
				return hasDefaultValue, value, errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 119)
			if timeValue.GetMysqlTime().Time == types.ZeroTime {
				trace_util_0.Count(_ddl_api_00000, 121)
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	trace_util_0.Count(_ddl_api_00000, 109)
	return hasDefaultValue, value, nil
}

func convertTimestampDefaultValToUTC(ctx sessionctx.Context, defaultVal interface{}, col *table.Column) (interface{}, error) {
	trace_util_0.Count(_ddl_api_00000, 122)
	if defaultVal == nil || col.Tp != mysql.TypeTimestamp {
		trace_util_0.Count(_ddl_api_00000, 125)
		return defaultVal, nil
	}
	trace_util_0.Count(_ddl_api_00000, 123)
	if vv, ok := defaultVal.(string); ok {
		trace_util_0.Count(_ddl_api_00000, 126)
		if vv != types.ZeroDatetimeStr && strings.ToUpper(vv) != strings.ToUpper(ast.CurrentTimestamp) {
			trace_util_0.Count(_ddl_api_00000, 127)
			t, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, vv, col.Tp, col.Decimal)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 130)
				return defaultVal, errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 128)
			err = t.ConvertTimeZone(ctx.GetSessionVars().Location(), time.UTC)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 131)
				return defaultVal, errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 129)
			defaultVal = t.String()
		}
	}
	trace_util_0.Count(_ddl_api_00000, 124)
	return defaultVal, nil
}

// isExplicitTimeStamp is used to check if explicit_defaults_for_timestamp is on or off.
// Check out this link for more details.
// https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_explicit_defaults_for_timestamp
func isExplicitTimeStamp() bool {
	trace_util_0.Count(_ddl_api_00000, 132)
	// TODO: implement the behavior as MySQL when explicit_defaults_for_timestamp = off, then this function could return false.
	return true
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func columnDefToCol(ctx sessionctx.Context, offset int, colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint) (*table.Column, []*ast.Constraint, error) {
	trace_util_0.Count(_ddl_api_00000, 133)
	var constraints = make([]*ast.Constraint, 0)
	col := table.ToColumn(&model.ColumnInfo{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
		// TODO: remove this version field after there is no old version.
		Version: model.CurrLatestColumnInfoVersion,
	})

	if !isExplicitTimeStamp() {
		trace_util_0.Count(_ddl_api_00000, 144)
		// Check and set TimestampFlag, OnUpdateNowFlag and NotNullFlag.
		if col.Tp == mysql.TypeTimestamp {
			trace_util_0.Count(_ddl_api_00000, 145)
			col.Flag |= mysql.TimestampFlag
			col.Flag |= mysql.OnUpdateNowFlag
			col.Flag |= mysql.NotNullFlag
		}
	}
	trace_util_0.Count(_ddl_api_00000, 134)
	var err error
	setOnUpdateNow := false
	hasDefaultValue := false
	hasNullFlag := false
	if colDef.Options != nil {
		trace_util_0.Count(_ddl_api_00000, 146)
		length := types.UnspecifiedLength

		keys := []*ast.IndexColName{
			{
				Column: colDef.Name,
				Length: length,
			},
		}

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

		for _, v := range colDef.Options {
			trace_util_0.Count(_ddl_api_00000, 147)
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				trace_util_0.Count(_ddl_api_00000, 148)
				col.Flag |= mysql.NotNullFlag
			case ast.ColumnOptionNull:
				trace_util_0.Count(_ddl_api_00000, 149)
				col.Flag &= ^mysql.NotNullFlag
				removeOnUpdateNowFlag(col)
				hasNullFlag = true
			case ast.ColumnOptionAutoIncrement:
				trace_util_0.Count(_ddl_api_00000, 150)
				col.Flag |= mysql.AutoIncrementFlag
			case ast.ColumnOptionPrimaryKey:
				trace_util_0.Count(_ddl_api_00000, 151)
				constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ast.ColumnOptionUniqKey:
				trace_util_0.Count(_ddl_api_00000, 152)
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionDefaultValue:
				trace_util_0.Count(_ddl_api_00000, 153)
				hasDefaultValue, err = setDefaultValue(ctx, col, v)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 162)
					return nil, nil, errors.Trace(err)
				}
				trace_util_0.Count(_ddl_api_00000, 154)
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				trace_util_0.Count(_ddl_api_00000, 155)
				// TODO: Support other time functions.
				if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
					trace_util_0.Count(_ddl_api_00000, 163)
					if !expression.IsCurrentTimestampExpr(v.Expr) {
						trace_util_0.Count(_ddl_api_00000, 164)
						return nil, nil, ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
					}
				} else {
					trace_util_0.Count(_ddl_api_00000, 165)
					{
						return nil, nil, ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
					}
				}
				trace_util_0.Count(_ddl_api_00000, 156)
				col.Flag |= mysql.OnUpdateNowFlag
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				trace_util_0.Count(_ddl_api_00000, 157)
				err := setColumnComment(ctx, col, v)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 166)
					return nil, nil, errors.Trace(err)
				}
			case ast.ColumnOptionGenerated:
				trace_util_0.Count(_ddl_api_00000, 158)
				sb.Reset()
				err = v.Expr.Restore(restoreCtx)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 167)
					return nil, nil, errors.Trace(err)
				}
				trace_util_0.Count(_ddl_api_00000, 159)
				col.GeneratedExprString = sb.String()
				col.GeneratedStored = v.Stored
				_, dependColNames := findDependedColumnNames(colDef)
				col.Dependences = dependColNames
			case ast.ColumnOptionCollate:
				trace_util_0.Count(_ddl_api_00000, 160)
				if field_types.HasCharset(colDef.Tp) {
					trace_util_0.Count(_ddl_api_00000, 168)
					col.FieldType.Collate = v.StrValue
				}
			case ast.ColumnOptionFulltext:
				trace_util_0.Count(_ddl_api_00000, 161)
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 135)
	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)
	if col.FieldType.EvalType().IsStringKind() && col.Charset == charset.CharsetBin {
		trace_util_0.Count(_ddl_api_00000, 169)
		col.Flag |= mysql.BinaryFlag
	}
	trace_util_0.Count(_ddl_api_00000, 136)
	if col.Tp == mysql.TypeBit {
		trace_util_0.Count(_ddl_api_00000, 170)
		// For BIT field, it's charset is binary but does not have binary flag.
		col.Flag &= ^mysql.BinaryFlag
		col.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_ddl_api_00000, 137)
	if col.Tp == mysql.TypeYear {
		trace_util_0.Count(_ddl_api_00000, 171)
		// For Year field, it's charset is binary but does not have binary flag.
		col.Flag &= ^mysql.BinaryFlag
		col.Flag |= mysql.ZerofillFlag
	}
	// If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
	// See https://dev.mysql.com/doc/refman/5.7/en/numeric-type-overview.html for more details.
	// But some types like bit and year, won't show its unsigned flag in `show create table`.
	trace_util_0.Count(_ddl_api_00000, 138)
	if mysql.HasZerofillFlag(col.Flag) {
		trace_util_0.Count(_ddl_api_00000, 172)
		col.Flag |= mysql.UnsignedFlag
	}
	trace_util_0.Count(_ddl_api_00000, 139)
	err = checkPriKeyConstraint(col, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 173)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 140)
	err = checkColumnValueConstraint(col)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 174)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 141)
	err = checkDefaultValue(ctx, col, hasDefaultValue)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 175)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 142)
	err = checkColumnFieldLength(col)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 176)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 143)
	return col, constraints, nil
}

func getDefaultValue(ctx sessionctx.Context, colName string, c *ast.ColumnOption, t *types.FieldType) (interface{}, error) {
	trace_util_0.Count(_ddl_api_00000, 177)
	tp, fsp := t.Tp, t.Decimal
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		trace_util_0.Count(_ddl_api_00000, 184)
		switch x := c.Expr.(type) {
		case *ast.FuncCallExpr:
			trace_util_0.Count(_ddl_api_00000, 189)
			if x.FnName.L == ast.CurrentTimestamp {
				trace_util_0.Count(_ddl_api_00000, 190)
				defaultFsp := 0
				if len(x.Args) == 1 {
					trace_util_0.Count(_ddl_api_00000, 192)
					if val := x.Args[0].(*driver.ValueExpr); val != nil {
						trace_util_0.Count(_ddl_api_00000, 193)
						defaultFsp = int(val.GetInt64())
					}
				}
				trace_util_0.Count(_ddl_api_00000, 191)
				if defaultFsp != fsp {
					trace_util_0.Count(_ddl_api_00000, 194)
					return nil, ErrInvalidDefaultValue.GenWithStackByArgs(colName)
				}
			}
		}
		trace_util_0.Count(_ddl_api_00000, 185)
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, fsp)
		value := vd.GetValue()
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 195)
			return nil, ErrInvalidDefaultValue.GenWithStackByArgs(colName)
		}

		// Value is nil means `default null`.
		trace_util_0.Count(_ddl_api_00000, 186)
		if value == nil {
			trace_util_0.Count(_ddl_api_00000, 196)
			return nil, nil
		}

		// If value is types.Time, convert it to string.
		trace_util_0.Count(_ddl_api_00000, 187)
		if vv, ok := value.(types.Time); ok {
			trace_util_0.Count(_ddl_api_00000, 197)
			return vv.String(), nil
		}

		trace_util_0.Count(_ddl_api_00000, 188)
		return value, nil
	}
	trace_util_0.Count(_ddl_api_00000, 178)
	v, err := expression.EvalAstExpr(ctx, c.Expr)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 198)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 179)
	if v.IsNull() {
		trace_util_0.Count(_ddl_api_00000, 199)
		return nil, nil
	}

	trace_util_0.Count(_ddl_api_00000, 180)
	if v.Kind() == types.KindBinaryLiteral || v.Kind() == types.KindMysqlBit {
		trace_util_0.Count(_ddl_api_00000, 200)
		if tp == mysql.TypeBit ||
			tp == mysql.TypeString || tp == mysql.TypeVarchar || tp == mysql.TypeVarString ||
			tp == mysql.TypeBlob || tp == mysql.TypeLongBlob || tp == mysql.TypeMediumBlob || tp == mysql.TypeTinyBlob ||
			tp == mysql.TypeJSON {
			trace_util_0.Count(_ddl_api_00000, 203)
			// For BinaryLiteral / string fields, when getting default value we cast the value into BinaryLiteral{}, thus we return
			// its raw string content here.
			return v.GetBinaryLiteral().ToString(), nil
		}
		// For other kind of fields (e.g. INT), we supply its integer as string value.
		trace_util_0.Count(_ddl_api_00000, 201)
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 204)
			return nil, err
		}
		trace_util_0.Count(_ddl_api_00000, 202)
		return strconv.FormatUint(value, 10), nil
	}

	trace_util_0.Count(_ddl_api_00000, 181)
	if tp == mysql.TypeDuration {
		trace_util_0.Count(_ddl_api_00000, 205)
		var err error
		if v, err = v.ConvertTo(ctx.GetSessionVars().StmtCtx, t); err != nil {
			trace_util_0.Count(_ddl_api_00000, 206)
			return "", errors.Trace(err)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 182)
	if tp == mysql.TypeBit {
		trace_util_0.Count(_ddl_api_00000, 207)
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			trace_util_0.Count(_ddl_api_00000, 208)
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), nil
		}
	}

	trace_util_0.Count(_ddl_api_00000, 183)
	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	trace_util_0.Count(_ddl_api_00000, 209)
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 210)
		c.Flag &= ^mysql.OnUpdateNowFlag
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	trace_util_0.Count(_ddl_api_00000, 211)
	if hasDefaultValue {
		trace_util_0.Count(_ddl_api_00000, 213)
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	trace_util_0.Count(_ddl_api_00000, 212)
	if mysql.HasTimestampFlag(c.Flag) && mysql.HasNotNullFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 214)
		if setOnUpdateNow {
			trace_util_0.Count(_ddl_api_00000, 215)
			if err := c.SetDefaultValue(types.ZeroDatetimeStr); err != nil {
				trace_util_0.Count(_ddl_api_00000, 216)
				context.Background()
				logutil.Logger(ddlLogCtx).Error("set default value failed", zap.Error(err))
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 217)
			{
				if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
					trace_util_0.Count(_ddl_api_00000, 218)
					logutil.Logger(ddlLogCtx).Error("set default value failed", zap.Error(err))
				}
			}
		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
	trace_util_0.Count(_ddl_api_00000, 219)
	if hasDefaultValue {
		trace_util_0.Count(_ddl_api_00000, 222)
		return
	}

	trace_util_0.Count(_ddl_api_00000, 220)
	if !mysql.HasNotNullFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 223)
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	trace_util_0.Count(_ddl_api_00000, 221)
	if !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 224)
		c.Flag |= mysql.NoDefaultValueFlag
	}
}

func checkDefaultValue(ctx sessionctx.Context, c *table.Column, hasDefaultValue bool) error {
	trace_util_0.Count(_ddl_api_00000, 225)
	if !hasDefaultValue {
		trace_util_0.Count(_ddl_api_00000, 230)
		return nil
	}

	trace_util_0.Count(_ddl_api_00000, 226)
	if c.GetDefaultValue() != nil {
		trace_util_0.Count(_ddl_api_00000, 231)
		if _, err := table.GetColDefaultValue(ctx, c.ToInfo()); err != nil {
			trace_util_0.Count(_ddl_api_00000, 233)
			return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
		}
		trace_util_0.Count(_ddl_api_00000, 232)
		return nil
	}
	// Primary key default null is invalid.
	trace_util_0.Count(_ddl_api_00000, 227)
	if mysql.HasPriKeyFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 234)
		return ErrPrimaryCantHaveNull
	}

	// Set not null but default null is invalid.
	trace_util_0.Count(_ddl_api_00000, 228)
	if mysql.HasNotNullFlag(c.Flag) {
		trace_util_0.Count(_ddl_api_00000, 235)
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}

	trace_util_0.Count(_ddl_api_00000, 229)
	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(col *table.Column, hasDefaultValue, hasNullFlag bool, outPriKeyConstraint *ast.Constraint) error {
	trace_util_0.Count(_ddl_api_00000, 236)
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.Flag) && hasDefaultValue && col.GetDefaultValue() == nil {
		trace_util_0.Count(_ddl_api_00000, 240)
		return types.ErrInvalidDefault.GenWithStackByArgs(col.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create table t1 (id int , age int, primary key(id))
	trace_util_0.Count(_ddl_api_00000, 237)
	if !mysql.HasPriKeyFlag(col.Flag) && outPriKeyConstraint != nil {
		trace_util_0.Count(_ddl_api_00000, 241)
		for _, key := range outPriKeyConstraint.Keys {
			trace_util_0.Count(_ddl_api_00000, 242)
			if key.Column.Name.L != col.Name.L {
				trace_util_0.Count(_ddl_api_00000, 244)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 243)
			col.Flag |= mysql.PriKeyFlag
			break
		}
	}
	// Primary key should not be null.
	trace_util_0.Count(_ddl_api_00000, 238)
	if mysql.HasPriKeyFlag(col.Flag) && hasNullFlag {
		trace_util_0.Count(_ddl_api_00000, 245)
		return ErrPrimaryCantHaveNull
	}
	trace_util_0.Count(_ddl_api_00000, 239)
	return nil
}

func checkColumnValueConstraint(col *table.Column) error {
	trace_util_0.Count(_ddl_api_00000, 246)
	if col.Tp != mysql.TypeEnum && col.Tp != mysql.TypeSet {
		trace_util_0.Count(_ddl_api_00000, 249)
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 247)
	valueMap := make(map[string]string, len(col.Elems))
	for i := range col.Elems {
		trace_util_0.Count(_ddl_api_00000, 250)
		val := strings.ToLower(col.Elems[i])
		if _, ok := valueMap[val]; ok {
			trace_util_0.Count(_ddl_api_00000, 252)
			tpStr := "ENUM"
			if col.Tp == mysql.TypeSet {
				trace_util_0.Count(_ddl_api_00000, 254)
				tpStr = "SET"
			}
			trace_util_0.Count(_ddl_api_00000, 253)
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(col.Name, valueMap[val], tpStr)
		}
		trace_util_0.Count(_ddl_api_00000, 251)
		valueMap[val] = col.Elems[i]
	}
	trace_util_0.Count(_ddl_api_00000, 248)
	return nil
}

func checkDuplicateColumn(cols []interface{}) error {
	trace_util_0.Count(_ddl_api_00000, 255)
	colNames := set.StringSet{}
	colName := model.NewCIStr("")
	for _, col := range cols {
		trace_util_0.Count(_ddl_api_00000, 257)
		switch x := col.(type) {
		case *ast.ColumnDef:
			trace_util_0.Count(_ddl_api_00000, 260)
			colName = x.Name.Name
		case model.CIStr:
			trace_util_0.Count(_ddl_api_00000, 261)
			colName = x
		default:
			trace_util_0.Count(_ddl_api_00000, 262)
			colName.O, colName.L = "", ""
		}
		trace_util_0.Count(_ddl_api_00000, 258)
		if colNames.Exist(colName.L) {
			trace_util_0.Count(_ddl_api_00000, 263)
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		trace_util_0.Count(_ddl_api_00000, 259)
		colNames.Insert(colName.L)
	}
	trace_util_0.Count(_ddl_api_00000, 256)
	return nil
}

func checkIsAutoIncrementColumn(colDefs *ast.ColumnDef) bool {
	trace_util_0.Count(_ddl_api_00000, 264)
	for _, option := range colDefs.Options {
		trace_util_0.Count(_ddl_api_00000, 266)
		if option.Tp == ast.ColumnOptionAutoIncrement {
			trace_util_0.Count(_ddl_api_00000, 267)
			return true
		}
	}
	trace_util_0.Count(_ddl_api_00000, 265)
	return false
}

func checkGeneratedColumn(colDefs []*ast.ColumnDef) error {
	trace_util_0.Count(_ddl_api_00000, 268)
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	var exists bool
	var autoIncrementColumn string
	for i, colDef := range colDefs {
		trace_util_0.Count(_ddl_api_00000, 272)
		for _, option := range colDef.Options {
			trace_util_0.Count(_ddl_api_00000, 275)
			if option.Tp == ast.ColumnOptionGenerated {
				trace_util_0.Count(_ddl_api_00000, 276)
				if err := checkIllegalFn4GeneratedColumn(colDef.Name.Name.L, option.Expr); err != nil {
					trace_util_0.Count(_ddl_api_00000, 277)
					return errors.Trace(err)
				}
			}
		}
		trace_util_0.Count(_ddl_api_00000, 273)
		if checkIsAutoIncrementColumn(colDef) {
			trace_util_0.Count(_ddl_api_00000, 278)
			exists, autoIncrementColumn = true, colDef.Name.Name.L
		}
		trace_util_0.Count(_ddl_api_00000, 274)
		generated, depCols := findDependedColumnNames(colDef)
		if !generated {
			trace_util_0.Count(_ddl_api_00000, 279)
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 280)
			{
				colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
					position:    i,
					generated:   true,
					dependences: depCols,
				}
			}
		}
	}

	// Check whether the generated column refers to any auto-increment columns
	trace_util_0.Count(_ddl_api_00000, 269)
	if exists {
		trace_util_0.Count(_ddl_api_00000, 281)
		for colName, generated := range colName2Generation {
			trace_util_0.Count(_ddl_api_00000, 282)
			if _, found := generated.dependences[autoIncrementColumn]; found {
				trace_util_0.Count(_ddl_api_00000, 283)
				return ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(colName)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 270)
	for _, colDef := range colDefs {
		trace_util_0.Count(_ddl_api_00000, 284)
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			trace_util_0.Count(_ddl_api_00000, 285)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 271)
	return nil
}

func checkTooLongColumn(cols []interface{}) error {
	trace_util_0.Count(_ddl_api_00000, 286)
	var colName string
	for _, col := range cols {
		trace_util_0.Count(_ddl_api_00000, 288)
		switch x := col.(type) {
		case *ast.ColumnDef:
			trace_util_0.Count(_ddl_api_00000, 290)
			colName = x.Name.Name.O
		case model.CIStr:
			trace_util_0.Count(_ddl_api_00000, 291)
			colName = x.O
		default:
			trace_util_0.Count(_ddl_api_00000, 292)
			colName = ""
		}
		trace_util_0.Count(_ddl_api_00000, 289)
		if len(colName) > mysql.MaxColumnNameLength {
			trace_util_0.Count(_ddl_api_00000, 293)
			return ErrTooLongIdent.GenWithStackByArgs(colName)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 287)
	return nil
}

func checkTooManyColumns(colDefs []*ast.ColumnDef) error {
	trace_util_0.Count(_ddl_api_00000, 294)
	if uint32(len(colDefs)) > atomic.LoadUint32(&TableColumnCountLimit) {
		trace_util_0.Count(_ddl_api_00000, 296)
		return errTooManyFields
	}
	trace_util_0.Count(_ddl_api_00000, 295)
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*ast.ColumnDef) error {
	trace_util_0.Count(_ddl_api_00000, 297)
	for _, colDef := range colDefs {
		trace_util_0.Count(_ddl_api_00000, 299)
		if err := checkColumnAttributes(colDef.Name.OrigColName(), colDef.Tp); err != nil {
			trace_util_0.Count(_ddl_api_00000, 300)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 298)
	return nil
}

func checkColumnFieldLength(col *table.Column) error {
	trace_util_0.Count(_ddl_api_00000, 301)
	if col.Tp == mysql.TypeVarchar {
		trace_util_0.Count(_ddl_api_00000, 303)
		if err := IsTooBigFieldLength(col.Flen, col.Name.O, col.Charset); err != nil {
			trace_util_0.Count(_ddl_api_00000, 304)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 302)
	return nil
}

// IsTooBigFieldLength check if the varchar type column exceeds the maximum length limit.
func IsTooBigFieldLength(colDefTpFlen int, colDefName, setCharset string) error {
	trace_util_0.Count(_ddl_api_00000, 305)
	desc, err := charset.GetCharsetDesc(setCharset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 308)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 306)
	maxFlen := mysql.MaxFieldVarCharLength
	maxFlen /= desc.Maxlen
	if colDefTpFlen != types.UnspecifiedLength && colDefTpFlen > maxFlen {
		trace_util_0.Count(_ddl_api_00000, 309)
		return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDefName, maxFlen)
	}
	trace_util_0.Count(_ddl_api_00000, 307)
	return nil
}

// checkColumnAttributes check attributes for single column.
func checkColumnAttributes(colName string, tp *types.FieldType) error {
	trace_util_0.Count(_ddl_api_00000, 310)
	switch tp.Tp {
	case mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		trace_util_0.Count(_ddl_api_00000, 312)
		if tp.Flen < tp.Decimal {
			trace_util_0.Count(_ddl_api_00000, 314)
			return types.ErrMBiggerThanD.GenWithStackByArgs(colName)
		}
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		trace_util_0.Count(_ddl_api_00000, 313)
		if tp.Decimal != types.UnspecifiedFsp && (tp.Decimal < types.MinFsp || tp.Decimal > types.MaxFsp) {
			trace_util_0.Count(_ddl_api_00000, 315)
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Decimal, colName, types.MaxFsp)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 311)
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool) error {
	trace_util_0.Count(_ddl_api_00000, 316)
	if name == "" {
		trace_util_0.Count(_ddl_api_00000, 319)
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 317)
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		trace_util_0.Count(_ddl_api_00000, 320)
		if foreign {
			trace_util_0.Count(_ddl_api_00000, 322)
			return infoschema.ErrCannotAddForeign
		}
		trace_util_0.Count(_ddl_api_00000, 321)
		return ErrDupKeyName.GenWithStack("duplicate key name %s", name)
	}
	trace_util_0.Count(_ddl_api_00000, 318)
	namesMap[nameLower] = true
	return nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint, foreign bool) {
	trace_util_0.Count(_ddl_api_00000, 323)
	if constr.Name == "" && len(constr.Keys) > 0 {
		trace_util_0.Count(_ddl_api_00000, 324)
		colName := constr.Keys[0].Column.Name.L
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, mysql.PrimaryKeyName) {
			trace_util_0.Count(_ddl_api_00000, 327)
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		trace_util_0.Count(_ddl_api_00000, 325)
		for namesMap[constrName] {
			trace_util_0.Count(_ddl_api_00000, 328)
			// We loop forever until we find constrName that haven't been used.
			if foreign {
				trace_util_0.Count(_ddl_api_00000, 330)
				constrName = fmt.Sprintf("fk_%s_%d", colName, i)
			} else {
				trace_util_0.Count(_ddl_api_00000, 331)
				{
					constrName = fmt.Sprintf("%s_%d", colName, i)
				}
			}
			trace_util_0.Count(_ddl_api_00000, 329)
			i++
		}
		trace_util_0.Count(_ddl_api_00000, 326)
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(constraints []*ast.Constraint) error {
	trace_util_0.Count(_ddl_api_00000, 332)
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		trace_util_0.Count(_ddl_api_00000, 335)
		if constr.Tp == ast.ConstraintForeignKey {
			trace_util_0.Count(_ddl_api_00000, 336)
			err := checkDuplicateConstraint(fkNames, constr.Name, true)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 337)
				return errors.Trace(err)
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 338)
			{
				err := checkDuplicateConstraint(constrNames, constr.Name, false)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 339)
					return errors.Trace(err)
				}
			}
		}
	}

	// Set empty constraint names.
	trace_util_0.Count(_ddl_api_00000, 333)
	for _, constr := range constraints {
		trace_util_0.Count(_ddl_api_00000, 340)
		if constr.Tp == ast.ConstraintForeignKey {
			trace_util_0.Count(_ddl_api_00000, 341)
			setEmptyConstraintName(fkNames, constr, true)
		} else {
			trace_util_0.Count(_ddl_api_00000, 342)
			{
				setEmptyConstraintName(constrNames, constr, false)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 334)
	return nil
}

func buildTableInfo(ctx sessionctx.Context, d *ddl, tableName model.CIStr, cols []*table.Column, constraints []*ast.Constraint) (tbInfo *model.TableInfo, err error) {
	trace_util_0.Count(_ddl_api_00000, 343)
	tbInfo = &model.TableInfo{
		Name:    tableName,
		Version: model.CurrLatestTableInfoVersion,
	}
	// When this function is called by MockTableInfo, we should set a particular table id.
	// So the `ddl` structure may be nil.
	if d != nil {
		trace_util_0.Count(_ddl_api_00000, 347)
		genIDs, err := d.genGlobalIDs(1)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 349)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 348)
		tbInfo.ID = genIDs[0]
	}
	trace_util_0.Count(_ddl_api_00000, 344)
	for _, v := range cols {
		trace_util_0.Count(_ddl_api_00000, 350)
		v.ID = allocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
	}
	trace_util_0.Count(_ddl_api_00000, 345)
	for _, constr := range constraints {
		trace_util_0.Count(_ddl_api_00000, 351)
		if constr.Tp == ast.ConstraintForeignKey {
			trace_util_0.Count(_ddl_api_00000, 358)
			for _, fk := range tbInfo.ForeignKeys {
				trace_util_0.Count(_ddl_api_00000, 364)
				if fk.Name.L == strings.ToLower(constr.Name) {
					trace_util_0.Count(_ddl_api_00000, 365)
					return nil, infoschema.ErrCannotAddForeign
				}
			}
			trace_util_0.Count(_ddl_api_00000, 359)
			var fk model.FKInfo
			fk.Name = model.NewCIStr(constr.Name)
			fk.RefTable = constr.Refer.Table.Name
			fk.State = model.StatePublic
			for _, key := range constr.Keys {
				trace_util_0.Count(_ddl_api_00000, 366)
				if table.FindCol(cols, key.Column.Name.O) == nil {
					trace_util_0.Count(_ddl_api_00000, 368)
					return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
				}
				trace_util_0.Count(_ddl_api_00000, 367)
				fk.Cols = append(fk.Cols, key.Column.Name)
			}
			trace_util_0.Count(_ddl_api_00000, 360)
			for _, key := range constr.Refer.IndexColNames {
				trace_util_0.Count(_ddl_api_00000, 369)
				fk.RefCols = append(fk.RefCols, key.Column.Name)
			}
			trace_util_0.Count(_ddl_api_00000, 361)
			fk.OnDelete = int(constr.Refer.OnDelete.ReferOpt)
			fk.OnUpdate = int(constr.Refer.OnUpdate.ReferOpt)
			if len(fk.Cols) != len(fk.RefCols) {
				trace_util_0.Count(_ddl_api_00000, 370)
				return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs(tbInfo.Name.O)
			}
			trace_util_0.Count(_ddl_api_00000, 362)
			if len(fk.Cols) == 0 {
				trace_util_0.Count(_ddl_api_00000, 371)
				// TODO: In MySQL, this case will report a parse error.
				return nil, infoschema.ErrCannotAddForeign
			}
			trace_util_0.Count(_ddl_api_00000, 363)
			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, &fk)
			continue
		}
		trace_util_0.Count(_ddl_api_00000, 352)
		if constr.Tp == ast.ConstraintPrimaryKey {
			trace_util_0.Count(_ddl_api_00000, 372)
			var col *table.Column
			for _, key := range constr.Keys {
				trace_util_0.Count(_ddl_api_00000, 374)
				col = table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					trace_util_0.Count(_ddl_api_00000, 376)
					return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
				}
				// Virtual columns cannot be used in primary key.
				trace_util_0.Count(_ddl_api_00000, 375)
				if col.IsGenerated() && !col.GeneratedStored {
					trace_util_0.Count(_ddl_api_00000, 377)
					return nil, errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
				}
			}
			trace_util_0.Count(_ddl_api_00000, 373)
			if len(constr.Keys) == 1 {
				trace_util_0.Count(_ddl_api_00000, 378)
				switch col.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					trace_util_0.Count(_ddl_api_00000, 379)
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
			}
		}
		trace_util_0.Count(_ddl_api_00000, 353)
		if constr.Tp == ast.ConstraintFulltext {
			trace_util_0.Count(_ddl_api_00000, 380)
			sc := ctx.GetSessionVars().StmtCtx
			sc.AppendWarning(ErrTableCantHandleFt)
			continue
		}
		// build index info.
		trace_util_0.Count(_ddl_api_00000, 354)
		idxInfo, err := buildIndexInfo(tbInfo, model.NewCIStr(constr.Name), constr.Keys, model.StatePublic)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 381)
			return nil, errors.Trace(err)
		}
		//check if the index is primary or uniqiue.
		trace_util_0.Count(_ddl_api_00000, 355)
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			trace_util_0.Count(_ddl_api_00000, 382)
			idxInfo.Primary = true
			idxInfo.Unique = true
			idxInfo.Name = model.NewCIStr(mysql.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			trace_util_0.Count(_ddl_api_00000, 383)
			idxInfo.Unique = true
		}
		// set index type.
		trace_util_0.Count(_ddl_api_00000, 356)
		if constr.Option != nil {
			trace_util_0.Count(_ddl_api_00000, 384)
			idxInfo.Comment, err = validateCommentLength(ctx.GetSessionVars(),
				constr.Option.Comment,
				maxCommentLength,
				errTooLongIndexComment.GenWithStackByArgs(idxInfo.Name.String(), maxCommentLength))
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 386)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 385)
			if constr.Option.Tp == model.IndexTypeInvalid {
				trace_util_0.Count(_ddl_api_00000, 387)
				// Use btree as default index type.
				idxInfo.Tp = model.IndexTypeBtree
			} else {
				trace_util_0.Count(_ddl_api_00000, 388)
				{
					idxInfo.Tp = constr.Option.Tp
				}
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 389)
			{
				// Use btree as default index type.
				idxInfo.Tp = model.IndexTypeBtree
			}
		}
		trace_util_0.Count(_ddl_api_00000, 357)
		idxInfo.ID = allocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	trace_util_0.Count(_ddl_api_00000, 346)
	return
}

func (d *ddl) CreateTableWithLike(ctx sessionctx.Context, ident, referIdent ast.Ident, ifNotExists bool) error {
	trace_util_0.Count(_ddl_api_00000, 390)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.SchemaByName(referIdent.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 398)
		return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 391)
	referTbl, err := is.TableByName(referIdent.Schema, referIdent.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 399)
		return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 392)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 400)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	trace_util_0.Count(_ddl_api_00000, 393)
	if is.TableExists(ident.Schema, ident.Name) {
		trace_util_0.Count(_ddl_api_00000, 401)
		if ifNotExists {
			trace_util_0.Count(_ddl_api_00000, 403)
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrTableExists.GenWithStackByArgs(ident))
			return nil
		}
		trace_util_0.Count(_ddl_api_00000, 402)
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	trace_util_0.Count(_ddl_api_00000, 394)
	tblInfo := buildTableInfoWithLike(ident, referTbl.Meta())
	count := 1
	if tblInfo.Partition != nil {
		trace_util_0.Count(_ddl_api_00000, 404)
		count += len(tblInfo.Partition.Definitions)
	}
	trace_util_0.Count(_ddl_api_00000, 395)
	var genIDs []int64
	genIDs, err = d.genGlobalIDs(count)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 405)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 396)
	tblInfo.ID = genIDs[0]
	if tblInfo.Partition != nil {
		trace_util_0.Count(_ddl_api_00000, 406)
		for i := 0; i < len(tblInfo.Partition.Definitions); i++ {
			trace_util_0.Count(_ddl_api_00000, 407)
			tblInfo.Partition.Definitions[i].ID = genIDs[i+1]
		}
	}

	trace_util_0.Count(_ddl_api_00000, 397)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	trace_util_0.Count(_ddl_api_00000, 408)
	_, err := tables.TableFromMeta(nil, tblInfo)
	return err
}

func buildTableInfoWithLike(ident ast.Ident, referTblInfo *model.TableInfo) model.TableInfo {
	trace_util_0.Count(_ddl_api_00000, 409)
	tblInfo := *referTblInfo
	// Check non-public column and adjust column offset.
	newColumns := referTblInfo.Cols()
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_ddl_api_00000, 412)
		if idx.State == model.StatePublic {
			trace_util_0.Count(_ddl_api_00000, 413)
			newIndices = append(newIndices, idx)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 410)
	tblInfo.Columns = newColumns
	tblInfo.Indices = newIndices
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	if referTblInfo.Partition != nil {
		trace_util_0.Count(_ddl_api_00000, 414)
		pi := *referTblInfo.Partition
		pi.Definitions = make([]model.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		copy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}
	trace_util_0.Count(_ddl_api_00000, 411)
	return tblInfo
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// The SQL string should be a create table statement.
// Don't use this function to build a partitioned table.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	trace_util_0.Count(_ddl_api_00000, 415)
	return buildTableInfoWithCheck(mock.NewContext(), nil, s, mysql.DefaultCharset)
}

func buildTableInfoWithCheck(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt, dbCharset string) (*model.TableInfo, error) {
	trace_util_0.Count(_ddl_api_00000, 416)
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	colDefs := s.Cols
	colObjects := make([]interface{}, 0, len(colDefs))
	for _, col := range colDefs {
		trace_util_0.Count(_ddl_api_00000, 432)
		colObjects = append(colObjects, col)
	}
	trace_util_0.Count(_ddl_api_00000, 417)
	if err := checkTooLongTable(ident.Name); err != nil {
		trace_util_0.Count(_ddl_api_00000, 433)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 418)
	if err := checkDuplicateColumn(colObjects); err != nil {
		trace_util_0.Count(_ddl_api_00000, 434)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 419)
	if err := checkGeneratedColumn(colDefs); err != nil {
		trace_util_0.Count(_ddl_api_00000, 435)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 420)
	if err := checkTooLongColumn(colObjects); err != nil {
		trace_util_0.Count(_ddl_api_00000, 436)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 421)
	if err := checkTooManyColumns(colDefs); err != nil {
		trace_util_0.Count(_ddl_api_00000, 437)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 422)
	if err := checkColumnsAttributes(colDefs); err != nil {
		trace_util_0.Count(_ddl_api_00000, 438)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 423)
	tableCharset := findTableOptionCharset(s.Options)
	// The column charset haven't been resolved here.
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints, tableCharset, dbCharset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 439)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 424)
	err = checkConstraintNames(newConstraints)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 440)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 425)
	var tbInfo *model.TableInfo
	tbInfo, err = buildTableInfo(ctx, d, ident.Name, cols, newConstraints)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 441)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 426)
	pi, err := buildTablePartitionInfo(ctx, d, s)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 442)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 427)
	if pi != nil {
		trace_util_0.Count(_ddl_api_00000, 443)
		switch pi.Type {
		case model.PartitionTypeRange:
			trace_util_0.Count(_ddl_api_00000, 447)
			if len(pi.Columns) == 0 {
				trace_util_0.Count(_ddl_api_00000, 449)
				err = checkPartitionByRange(ctx, tbInfo, pi, s, cols, newConstraints)
			} else {
				trace_util_0.Count(_ddl_api_00000, 450)
				{
					err = checkPartitionByRangeColumn(ctx, tbInfo, pi, s)
				}
			}
		case model.PartitionTypeHash:
			trace_util_0.Count(_ddl_api_00000, 448)
			err = checkPartitionByHash(ctx, pi, s, cols, tbInfo)
		}
		trace_util_0.Count(_ddl_api_00000, 444)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 451)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 445)
		if err = checkRangePartitioningKeysConstraints(ctx, s, tbInfo, newConstraints); err != nil {
			trace_util_0.Count(_ddl_api_00000, 452)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 446)
		tbInfo.Partition = pi
	}

	// The specified charset will be handled in handleTableOptions
	trace_util_0.Count(_ddl_api_00000, 428)
	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		trace_util_0.Count(_ddl_api_00000, 453)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 429)
	if err = resolveDefaultTableCharsetAndCollation(tbInfo, dbCharset); err != nil {
		trace_util_0.Count(_ddl_api_00000, 454)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 430)
	if err = checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		trace_util_0.Count(_ddl_api_00000, 455)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 431)
	return tbInfo, nil
}

func (d *ddl) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	trace_util_0.Count(_ddl_api_00000, 456)
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	if s.ReferTable != nil {
		trace_util_0.Count(_ddl_api_00000, 464)
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		return d.CreateTableWithLike(ctx, ident, referIdent, s.IfNotExists)
	}
	trace_util_0.Count(_ddl_api_00000, 457)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 465)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	trace_util_0.Count(_ddl_api_00000, 458)
	if is.TableExists(ident.Schema, ident.Name) {
		trace_util_0.Count(_ddl_api_00000, 466)
		if s.IfNotExists {
			trace_util_0.Count(_ddl_api_00000, 468)
			ctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrTableExists.GenWithStackByArgs(ident))
			return nil
		}
		trace_util_0.Count(_ddl_api_00000, 467)
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	trace_util_0.Count(_ddl_api_00000, 459)
	tbInfo, err := buildTableInfoWithCheck(ctx, d, s, schema.Charset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 469)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 460)
	tbInfo.State = model.StatePublic
	err = checkTableInfoValid(tbInfo)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 470)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 461)
	tbInfo.State = model.StateNone

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo},
	}

	err = d.doDDLJob(ctx, job)
	if err == nil {
		trace_util_0.Count(_ddl_api_00000, 471)
		var preSplitAndScatter func()
		// do pre-split and scatter.
		if tbInfo.ShardRowIDBits > 0 && tbInfo.PreSplitRegions > 0 {
			trace_util_0.Count(_ddl_api_00000, 474)
			preSplitAndScatter = func() {
				trace_util_0.Count(_ddl_api_00000, 475)
				preSplitTableRegion(d.store, tbInfo, ctx.GetSessionVars().WaitTableSplitFinish)
			}
		} else {
			trace_util_0.Count(_ddl_api_00000, 476)
			if atomic.LoadUint32(&EnableSplitTableRegion) != 0 {
				trace_util_0.Count(_ddl_api_00000, 477)
				pi := tbInfo.GetPartitionInfo()
				if pi != nil {
					trace_util_0.Count(_ddl_api_00000, 478)
					preSplitAndScatter = func() { trace_util_0.Count(_ddl_api_00000, 479); splitPartitionTableRegion(d.store, pi) }
				} else {
					trace_util_0.Count(_ddl_api_00000, 480)
					{
						preSplitAndScatter = func() { trace_util_0.Count(_ddl_api_00000, 481); splitTableRegion(d.store, tbInfo.ID) }
					}
				}
			}
		}
		trace_util_0.Count(_ddl_api_00000, 472)
		if preSplitAndScatter != nil {
			trace_util_0.Count(_ddl_api_00000, 482)
			if ctx.GetSessionVars().WaitTableSplitFinish {
				trace_util_0.Count(_ddl_api_00000, 483)
				preSplitAndScatter()
			} else {
				trace_util_0.Count(_ddl_api_00000, 484)
				{
					go preSplitAndScatter()
				}
			}
		}

		trace_util_0.Count(_ddl_api_00000, 473)
		if tbInfo.AutoIncID > 1 {
			trace_util_0.Count(_ddl_api_00000, 485)
			// Default tableAutoIncID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			err = d.handleAutoIncID(tbInfo, schema.ID)
		}
	}

	// table exists, but if_not_exists flags is true, so we ignore this error.
	trace_util_0.Count(_ddl_api_00000, 462)
	if infoschema.ErrTableExists.Equal(err) && s.IfNotExists {
		trace_util_0.Count(_ddl_api_00000, 486)
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 463)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) RecoverTable(ctx sessionctx.Context, tbInfo *model.TableInfo, schemaID, autoID, dropJobID int64, snapshotTS uint64) (err error) {
	trace_util_0.Count(_ddl_api_00000, 487)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check schema exist.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 490)
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
		))
	}
	// Check not exist table with same name.
	trace_util_0.Count(_ddl_api_00000, 488)
	if ok := is.TableExists(schema.Name, tbInfo.Name); ok {
		trace_util_0.Count(_ddl_api_00000, 491)
		return infoschema.ErrTableExists.GenWithStackByArgs(tbInfo.Name)
	}

	trace_util_0.Count(_ddl_api_00000, 489)
	tbInfo.State = model.StateNone
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tbInfo.ID,
		Type:       model.ActionRecoverTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, autoID, dropJobID, snapshotTS, recoverTableCheckFlagNone},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	trace_util_0.Count(_ddl_api_00000, 492)
	ident := ast.Ident{Name: s.ViewName.Name, Schema: s.ViewName.Schema}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 504)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	trace_util_0.Count(_ddl_api_00000, 493)
	oldView, err := is.TableByName(ident.Schema, ident.Name)
	if err == nil && !s.OrReplace {
		trace_util_0.Count(_ddl_api_00000, 505)
		return infoschema.ErrTableExists.GenWithStackByArgs(ident)
	}

	trace_util_0.Count(_ddl_api_00000, 494)
	var oldViewTblID int64
	if oldView != nil {
		trace_util_0.Count(_ddl_api_00000, 506)
		if !oldView.Meta().IsView() {
			trace_util_0.Count(_ddl_api_00000, 508)
			return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "VIEW")
		}
		trace_util_0.Count(_ddl_api_00000, 507)
		oldViewTblID = oldView.Meta().ID
	}

	trace_util_0.Count(_ddl_api_00000, 495)
	if err = checkTooLongTable(ident.Name); err != nil {
		trace_util_0.Count(_ddl_api_00000, 509)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 496)
	viewInfo, cols := buildViewInfoWithTableColumns(ctx, s)

	colObjects := make([]interface{}, 0, len(viewInfo.Cols))
	for _, col := range viewInfo.Cols {
		trace_util_0.Count(_ddl_api_00000, 510)
		colObjects = append(colObjects, col)
	}

	trace_util_0.Count(_ddl_api_00000, 497)
	if err = checkTooLongColumn(colObjects); err != nil {
		trace_util_0.Count(_ddl_api_00000, 511)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 498)
	if err = checkDuplicateColumn(colObjects); err != nil {
		trace_util_0.Count(_ddl_api_00000, 512)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 499)
	tbInfo, err := buildTableInfo(ctx, d, ident.Name, cols, nil)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 513)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 500)
	tbInfo.View = viewInfo

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo, s.OrReplace, oldViewTblID},
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar("character_set_client"); ok {
		trace_util_0.Count(_ddl_api_00000, 514)
		tbInfo.Charset = v
	}
	trace_util_0.Count(_ddl_api_00000, 501)
	if v, ok := ctx.GetSessionVars().GetSystemVar("collation_connection"); ok {
		trace_util_0.Count(_ddl_api_00000, 515)
		tbInfo.Collate = v
	}
	trace_util_0.Count(_ddl_api_00000, 502)
	err = checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 516)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 503)
	err = d.doDDLJob(ctx, job)

	return d.callHookOnChanged(err)
}

func buildViewInfoWithTableColumns(ctx sessionctx.Context, s *ast.CreateViewStmt) (*model.ViewInfo, []*table.Column) {
	trace_util_0.Count(_ddl_api_00000, 517)
	viewInfo := &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: s.Select.Text(), CheckOption: s.CheckOption, Cols: s.SchemaCols}
	var tableColumns = make([]*table.Column, len(s.SchemaCols))
	if s.Cols == nil {
		trace_util_0.Count(_ddl_api_00000, 519)
		for i, v := range s.SchemaCols {
			trace_util_0.Count(_ddl_api_00000, 520)
			tableColumns[i] = table.ToColumn(&model.ColumnInfo{
				Name:    v,
				ID:      int64(i),
				Offset:  i,
				State:   model.StatePublic,
				Version: model.CurrLatestColumnInfoVersion,
			})
		}
	} else {
		trace_util_0.Count(_ddl_api_00000, 521)
		{
			for i, v := range s.Cols {
				trace_util_0.Count(_ddl_api_00000, 522)
				tableColumns[i] = table.ToColumn(&model.ColumnInfo{
					Name:    v,
					ID:      int64(i),
					Offset:  i,
					State:   model.StatePublic,
					Version: model.CurrLatestColumnInfoVersion,
				})
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 518)
	return viewInfo, tableColumns
}

func checkPartitionByHash(ctx sessionctx.Context, pi *model.PartitionInfo, s *ast.CreateTableStmt, cols []*table.Column, tbInfo *model.TableInfo) error {
	trace_util_0.Count(_ddl_api_00000, 523)
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		trace_util_0.Count(_ddl_api_00000, 527)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 524)
	if err := checkNoHashPartitions(ctx, pi.Num); err != nil {
		trace_util_0.Count(_ddl_api_00000, 528)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 525)
	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		trace_util_0.Count(_ddl_api_00000, 529)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 526)
	return checkPartitionFuncType(ctx, s, cols, tbInfo)
}

func checkPartitionByRange(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo, s *ast.CreateTableStmt, cols []*table.Column, newConstraints []*ast.Constraint) error {
	trace_util_0.Count(_ddl_api_00000, 530)
	if err := checkPartitionNameUnique(tbInfo, pi); err != nil {
		trace_util_0.Count(_ddl_api_00000, 536)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 531)
	if err := checkCreatePartitionValue(ctx, tbInfo, pi, cols); err != nil {
		trace_util_0.Count(_ddl_api_00000, 537)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 532)
	if err := checkAddPartitionTooManyPartitions(uint64(len(pi.Definitions))); err != nil {
		trace_util_0.Count(_ddl_api_00000, 538)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 533)
	if err := checkNoRangePartitions(len(pi.Definitions)); err != nil {
		trace_util_0.Count(_ddl_api_00000, 539)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 534)
	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		trace_util_0.Count(_ddl_api_00000, 540)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 535)
	return checkPartitionFuncType(ctx, s, cols, tbInfo)
}

func checkPartitionByRangeColumn(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo, s *ast.CreateTableStmt) error {
	trace_util_0.Count(_ddl_api_00000, 541)
	if err := checkPartitionNameUnique(tbInfo, pi); err != nil {
		trace_util_0.Count(_ddl_api_00000, 546)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 542)
	if err := checkRangeColumnsPartitionType(tbInfo, pi.Columns); err != nil {
		trace_util_0.Count(_ddl_api_00000, 547)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 543)
	if err := checkRangeColumnsPartitionValue(ctx, tbInfo, pi); err != nil {
		trace_util_0.Count(_ddl_api_00000, 548)
		return err
	}

	trace_util_0.Count(_ddl_api_00000, 544)
	if err := checkNoRangePartitions(len(pi.Definitions)); err != nil {
		trace_util_0.Count(_ddl_api_00000, 549)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 545)
	return checkAddPartitionTooManyPartitions(uint64(len(pi.Definitions)))
}

func checkRangeColumnsPartitionType(tbInfo *model.TableInfo, columns []model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 550)
	for _, col := range columns {
		trace_util_0.Count(_ddl_api_00000, 552)
		colInfo := getColumnInfoByName(tbInfo, col.L)
		if colInfo == nil {
			trace_util_0.Count(_ddl_api_00000, 554)
			return errors.Trace(ErrFieldNotFoundPart)
		}
		// The permitted data types are shown in the following list:
		// All integer types
		// DATE and DATETIME
		// CHAR, VARCHAR, BINARY, and VARBINARY
		// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
		trace_util_0.Count(_ddl_api_00000, 553)
		switch colInfo.FieldType.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			trace_util_0.Count(_ddl_api_00000, 555)
		case mysql.TypeDate, mysql.TypeDatetime:
			trace_util_0.Count(_ddl_api_00000, 556)
		case mysql.TypeVarchar, mysql.TypeString:
			trace_util_0.Count(_ddl_api_00000, 557)
		default:
			trace_util_0.Count(_ddl_api_00000, 558)
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.O)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 551)
	return nil
}

func checkRangeColumnsPartitionValue(ctx sessionctx.Context, tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
	trace_util_0.Count(_ddl_api_00000, 559)
	// Range columns partition key supports multiple data types with integer、datetime、string.
	defs := pi.Definitions
	if len(defs) < 1 {
		trace_util_0.Count(_ddl_api_00000, 563)
		return errors.Trace(ErrPartitionsMustBeDefined)
	}

	trace_util_0.Count(_ddl_api_00000, 560)
	curr := &defs[0]
	if len(curr.LessThan) != len(pi.Columns) {
		trace_util_0.Count(_ddl_api_00000, 564)
		return errors.Trace(ErrPartitionColumnList)
	}
	trace_util_0.Count(_ddl_api_00000, 561)
	for i := 1; i < len(defs); i++ {
		trace_util_0.Count(_ddl_api_00000, 565)
		prev, curr := curr, &defs[i]
		succ, err := checkTwoRangeColumns(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 567)
			return err
		}
		trace_util_0.Count(_ddl_api_00000, 566)
		if !succ {
			trace_util_0.Count(_ddl_api_00000, 568)
			return errors.Trace(ErrRangeNotIncreasing)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 562)
	return nil
}

func checkTwoRangeColumns(ctx sessionctx.Context, curr, prev *model.PartitionDefinition, pi *model.PartitionInfo, tbInfo *model.TableInfo) (bool, error) {
	trace_util_0.Count(_ddl_api_00000, 569)
	if len(curr.LessThan) != len(pi.Columns) {
		trace_util_0.Count(_ddl_api_00000, 572)
		return false, errors.Trace(ErrPartitionColumnList)
	}
	trace_util_0.Count(_ddl_api_00000, 570)
	for i := 0; i < len(pi.Columns); i++ {
		trace_util_0.Count(_ddl_api_00000, 573)
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) {
			trace_util_0.Count(_ddl_api_00000, 578)
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		trace_util_0.Count(_ddl_api_00000, 574)
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			trace_util_0.Count(_ddl_api_00000, 579)
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// Current and previous is the same.
		trace_util_0.Count(_ddl_api_00000, 575)
		if strings.EqualFold(curr.LessThan[i], prev.LessThan[i]) {
			trace_util_0.Count(_ddl_api_00000, 580)
			continue
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		trace_util_0.Count(_ddl_api_00000, 576)
		succ, err := parseAndEvalBoolExpr(ctx, fmt.Sprintf("(%s) > (%s)", curr.LessThan[i], prev.LessThan[i]), tbInfo)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 581)
			return false, err
		}

		trace_util_0.Count(_ddl_api_00000, 577)
		if succ {
			trace_util_0.Count(_ddl_api_00000, 582)
			return true, nil
		}
	}
	trace_util_0.Count(_ddl_api_00000, 571)
	return false, nil
}

func parseAndEvalBoolExpr(ctx sessionctx.Context, expr string, tbInfo *model.TableInfo) (bool, error) {
	trace_util_0.Count(_ddl_api_00000, 583)
	e, err := expression.ParseSimpleExprWithTableInfo(ctx, expr, tbInfo)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 586)
		return false, err
	}
	trace_util_0.Count(_ddl_api_00000, 584)
	res, _, err1 := e.EvalInt(ctx, chunk.Row{})
	if err1 != nil {
		trace_util_0.Count(_ddl_api_00000, 587)
		return false, err1
	}
	trace_util_0.Count(_ddl_api_00000, 585)
	return res > 0, nil
}

func checkCharsetAndCollation(cs string, co string) error {
	trace_util_0.Count(_ddl_api_00000, 588)
	if !charset.ValidCharsetAndCollation(cs, co) {
		trace_util_0.Count(_ddl_api_00000, 590)
		return ErrUnknownCharacterSet.GenWithStackByArgs(cs)
	}
	trace_util_0.Count(_ddl_api_00000, 589)
	return nil
}

// handleAutoIncID handles auto_increment option in DDL. It creates a ID counter for the table and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64) error {
	trace_util_0.Count(_ddl_api_00000, 591)
	alloc := autoid.NewAllocator(d.store, tbInfo.GetDBID(schemaID), tbInfo.IsAutoIncColUnsigned())
	tbInfo.State = model.StatePublic
	tb, err := table.TableFromMeta(alloc, tbInfo)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 594)
		return errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	trace_util_0.Count(_ddl_api_00000, 592)
	if err = tb.RebaseAutoID(nil, tbInfo.AutoIncID-1, false); err != nil {
		trace_util_0.Count(_ddl_api_00000, 595)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 593)
	return nil
}

func resolveDefaultTableCharsetAndCollation(tbInfo *model.TableInfo, dbCharset string) (err error) {
	trace_util_0.Count(_ddl_api_00000, 596)
	chr, collate, err := ResolveCharsetCollation(tbInfo.Charset, dbCharset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 600)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 597)
	if len(tbInfo.Charset) == 0 {
		trace_util_0.Count(_ddl_api_00000, 601)
		tbInfo.Charset = chr
	}

	trace_util_0.Count(_ddl_api_00000, 598)
	if len(tbInfo.Collate) == 0 {
		trace_util_0.Count(_ddl_api_00000, 602)
		tbInfo.Collate = collate
	}
	trace_util_0.Count(_ddl_api_00000, 599)
	return
}

func findTableOptionCharset(options []*ast.TableOption) string {
	trace_util_0.Count(_ddl_api_00000, 603)
	var tableCharset string
	for i := len(options) - 1; i >= 0; i-- {
		trace_util_0.Count(_ddl_api_00000, 605)
		op := options[i]
		if op.Tp == ast.TableOptionCharset {
			trace_util_0.Count(_ddl_api_00000, 606)
			// find the last one.
			tableCharset = op.StrValue
			break
		}
	}

	trace_util_0.Count(_ddl_api_00000, 604)
	return tableCharset
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) error {
	trace_util_0.Count(_ddl_api_00000, 607)
	for _, op := range options {
		trace_util_0.Count(_ddl_api_00000, 610)
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			trace_util_0.Count(_ddl_api_00000, 611)
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionComment:
			trace_util_0.Count(_ddl_api_00000, 612)
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCharset:
			trace_util_0.Count(_ddl_api_00000, 613)
			tbInfo.Charset = op.StrValue
		case ast.TableOptionCollate:
			trace_util_0.Count(_ddl_api_00000, 614)
			tbInfo.Collate = op.StrValue
		case ast.TableOptionCompression:
			trace_util_0.Count(_ddl_api_00000, 615)
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			trace_util_0.Count(_ddl_api_00000, 616)
			if op.UintValue > 0 && tbInfo.PKIsHandle {
				trace_util_0.Count(_ddl_api_00000, 620)
				return errUnsupportedShardRowIDBits
			}
			trace_util_0.Count(_ddl_api_00000, 617)
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				trace_util_0.Count(_ddl_api_00000, 621)
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
			trace_util_0.Count(_ddl_api_00000, 618)
			tbInfo.MaxShardRowIDBits = tbInfo.ShardRowIDBits
		case ast.TableOptionPreSplitRegion:
			trace_util_0.Count(_ddl_api_00000, 619)
			tbInfo.PreSplitRegions = op.UintValue
		}
	}
	trace_util_0.Count(_ddl_api_00000, 608)
	if tbInfo.PreSplitRegions > tbInfo.ShardRowIDBits {
		trace_util_0.Count(_ddl_api_00000, 622)
		tbInfo.PreSplitRegions = tbInfo.ShardRowIDBits
	}

	trace_util_0.Count(_ddl_api_00000, 609)
	return nil
}

func hasAutoIncrementColumn(tbInfo *model.TableInfo) (bool, string) {
	trace_util_0.Count(_ddl_api_00000, 623)
	for _, col := range tbInfo.Columns {
		trace_util_0.Count(_ddl_api_00000, 625)
		if mysql.HasAutoIncrementFlag(col.Flag) {
			trace_util_0.Count(_ddl_api_00000, 626)
			return true, col.Name.L
		}
	}
	trace_util_0.Count(_ddl_api_00000, 624)
	return false, ""
}

// isIgnorableSpec checks if the spec type is ignorable.
// Some specs are parsed by ignored. This is for compatibility.
func isIgnorableSpec(tp ast.AlterTableType) bool {
	trace_util_0.Count(_ddl_api_00000, 627)
	// AlterTableLock/AlterTableAlgorithm are ignored.
	return tp == ast.AlterTableLock || tp == ast.AlterTableAlgorithm
}

// getCharsetAndCollateInTableOption will iterate the charset and collate in the options,
// and returns the last charset and collate in options. If there is no charset in the options,
// the returns charset will be "", the same as collate.
func getCharsetAndCollateInTableOption(startIdx int, options []*ast.TableOption) (ca, co string, err error) {
	trace_util_0.Count(_ddl_api_00000, 628)
	charsets := make([]string, 0, len(options))
	collates := make([]string, 0, len(options))
	for i := startIdx; i < len(options); i++ {
		trace_util_0.Count(_ddl_api_00000, 633)
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			trace_util_0.Count(_ddl_api_00000, 634)
			charsets = append(charsets, opt.StrValue)
		case ast.TableOptionCollate:
			trace_util_0.Count(_ddl_api_00000, 635)
			collates = append(collates, opt.StrValue)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 629)
	if len(charsets) > 1 {
		trace_util_0.Count(_ddl_api_00000, 636)
		return "", "", ErrConflictingDeclarations.GenWithStackByArgs(charsets[0], charsets[1])
	}
	trace_util_0.Count(_ddl_api_00000, 630)
	if len(charsets) == 1 {
		trace_util_0.Count(_ddl_api_00000, 637)
		if charsets[0] == "" {
			trace_util_0.Count(_ddl_api_00000, 639)
			return "", "", ErrUnknownCharacterSet.GenWithStackByArgs("")
		}
		trace_util_0.Count(_ddl_api_00000, 638)
		ca = charsets[0]
	}
	trace_util_0.Count(_ddl_api_00000, 631)
	if len(collates) != 0 {
		trace_util_0.Count(_ddl_api_00000, 640)
		for i := range collates {
			trace_util_0.Count(_ddl_api_00000, 642)
			if collates[i] == "" {
				trace_util_0.Count(_ddl_api_00000, 644)
				return "", "", ErrUnknownCollation.GenWithStackByArgs("")
			}
			trace_util_0.Count(_ddl_api_00000, 643)
			if len(ca) != 0 && !charset.ValidCharsetAndCollation(ca, collates[i]) {
				trace_util_0.Count(_ddl_api_00000, 645)
				return "", "", ErrCollationCharsetMismatch.GenWithStackByArgs(collates[i], ca)
			}
		}
		trace_util_0.Count(_ddl_api_00000, 641)
		co = collates[len(collates)-1]
	}
	trace_util_0.Count(_ddl_api_00000, 632)
	return
}

// resolveAlterTableSpec resolves alter table algorithm and removes ignore table spec in specs.
// returns valied specs, and the occurred error.
func resolveAlterTableSpec(ctx sessionctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	trace_util_0.Count(_ddl_api_00000, 646)
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlterAlgorithmDefault
	for _, spec := range specs {
		trace_util_0.Count(_ddl_api_00000, 650)
		if spec.Tp == ast.AlterTableAlgorithm {
			trace_util_0.Count(_ddl_api_00000, 653)
			// Find the last AlterTableAlgorithm.
			algorithm = spec.Algorithm
		}
		trace_util_0.Count(_ddl_api_00000, 651)
		if isIgnorableSpec(spec.Tp) {
			trace_util_0.Count(_ddl_api_00000, 654)
			continue
		}
		trace_util_0.Count(_ddl_api_00000, 652)
		validSpecs = append(validSpecs, spec)
	}

	trace_util_0.Count(_ddl_api_00000, 647)
	if len(validSpecs) != 1 {
		trace_util_0.Count(_ddl_api_00000, 655)
		// TODO: Hanlde len(validSpecs) == 0.
		// Now we only allow one schema changing at the same time.
		return nil, errRunMultiSchemaChanges
	}

	// Verify whether the algorithm is supported.
	trace_util_0.Count(_ddl_api_00000, 648)
	for _, spec := range validSpecs {
		trace_util_0.Count(_ddl_api_00000, 656)
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 658)
			if algorithm != ast.AlterAlgorithmCopy {
				trace_util_0.Count(_ddl_api_00000, 660)
				return nil, errors.Trace(err)
			}
			// For the compatibility, we return warning instead of error when the algorithm is COPY,
			// because the COPY ALGORITHM is not supported in TiDB.
			trace_util_0.Count(_ddl_api_00000, 659)
			ctx.GetSessionVars().StmtCtx.AppendError(err)
		}

		trace_util_0.Count(_ddl_api_00000, 657)
		spec.Algorithm = resolvedAlgorithm
	}

	// Only handle valid specs.
	trace_util_0.Count(_ddl_api_00000, 649)
	return validSpecs, nil
}

func (d *ddl) AlterTable(ctx sessionctx.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	trace_util_0.Count(_ddl_api_00000, 661)
	validSpecs, err := resolveAlterTableSpec(ctx, specs)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 665)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 662)
	is := d.infoHandle.Get()
	if is.TableIsView(ident.Schema, ident.Name) {
		trace_util_0.Count(_ddl_api_00000, 666)
		return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
	}

	trace_util_0.Count(_ddl_api_00000, 663)
	for _, spec := range validSpecs {
		trace_util_0.Count(_ddl_api_00000, 667)
		var handledCharsetOrCollate bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			trace_util_0.Count(_ddl_api_00000, 669)
			if len(spec.NewColumns) != 1 {
				trace_util_0.Count(_ddl_api_00000, 687)
				return errRunMultiSchemaChanges
			}
			trace_util_0.Count(_ddl_api_00000, 670)
			err = d.AddColumn(ctx, ident, spec)
		case ast.AlterTableAddPartitions:
			trace_util_0.Count(_ddl_api_00000, 671)
			err = d.AddTablePartitions(ctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			trace_util_0.Count(_ddl_api_00000, 672)
			err = d.CoalescePartitions(ctx, ident, spec)
		case ast.AlterTableDropColumn:
			trace_util_0.Count(_ddl_api_00000, 673)
			err = d.DropColumn(ctx, ident, spec.OldColumnName.Name)
		case ast.AlterTableDropIndex:
			trace_util_0.Count(_ddl_api_00000, 674)
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableDropPartition:
			trace_util_0.Count(_ddl_api_00000, 675)
			err = d.DropTablePartition(ctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			trace_util_0.Count(_ddl_api_00000, 676)
			err = d.TruncateTablePartition(ctx, ident, spec)
		case ast.AlterTableAddConstraint:
			trace_util_0.Count(_ddl_api_00000, 677)
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				trace_util_0.Count(_ddl_api_00000, 688)
				err = d.CreateIndex(ctx, ident, false, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				trace_util_0.Count(_ddl_api_00000, 689)
				err = d.CreateIndex(ctx, ident, true, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintForeignKey:
				trace_util_0.Count(_ddl_api_00000, 690)
				err = d.CreateForeignKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				trace_util_0.Count(_ddl_api_00000, 691)
				err = ErrUnsupportedModifyPrimaryKey.GenWithStackByArgs("add")
			case ast.ConstraintFulltext:
				trace_util_0.Count(_ddl_api_00000, 692)
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
			default:
				trace_util_0.Count(_ddl_api_00000, 693)
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			trace_util_0.Count(_ddl_api_00000, 678)
			err = d.DropForeignKey(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			trace_util_0.Count(_ddl_api_00000, 679)
			err = d.ModifyColumn(ctx, ident, spec)
		case ast.AlterTableChangeColumn:
			trace_util_0.Count(_ddl_api_00000, 680)
			err = d.ChangeColumn(ctx, ident, spec)
		case ast.AlterTableAlterColumn:
			trace_util_0.Count(_ddl_api_00000, 681)
			err = d.AlterColumn(ctx, ident, spec)
		case ast.AlterTableRenameTable:
			trace_util_0.Count(_ddl_api_00000, 682)
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = d.RenameTable(ctx, ident, newIdent, isAlterTable)
		case ast.AlterTableDropPrimaryKey:
			trace_util_0.Count(_ddl_api_00000, 683)
			err = ErrUnsupportedModifyPrimaryKey.GenWithStackByArgs("drop")
		case ast.AlterTableRenameIndex:
			trace_util_0.Count(_ddl_api_00000, 684)
			err = d.RenameIndex(ctx, ident, spec)
		case ast.AlterTableOption:
			trace_util_0.Count(_ddl_api_00000, 685)
			for i, opt := range spec.Options {
				trace_util_0.Count(_ddl_api_00000, 694)
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					trace_util_0.Count(_ddl_api_00000, 696)
					if opt.UintValue > shardRowIDBitsMax {
						trace_util_0.Count(_ddl_api_00000, 703)
						opt.UintValue = shardRowIDBitsMax
					}
					trace_util_0.Count(_ddl_api_00000, 697)
					err = d.ShardRowID(ctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					trace_util_0.Count(_ddl_api_00000, 698)
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue))
				case ast.TableOptionComment:
					trace_util_0.Count(_ddl_api_00000, 699)
					spec.Comment = opt.StrValue
					err = d.AlterTableComment(ctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					trace_util_0.Count(_ddl_api_00000, 700)
					// getCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						trace_util_0.Count(_ddl_api_00000, 704)
						continue
					}
					trace_util_0.Count(_ddl_api_00000, 701)
					var toCharset, toCollate string
					toCharset, toCollate, err = getCharsetAndCollateInTableOption(i, spec.Options)
					if err != nil {
						trace_util_0.Count(_ddl_api_00000, 705)
						return err
					}
					trace_util_0.Count(_ddl_api_00000, 702)
					err = d.AlterTableCharsetAndCollate(ctx, ident, toCharset, toCollate)
					handledCharsetOrCollate = true
				}

				trace_util_0.Count(_ddl_api_00000, 695)
				if err != nil {
					trace_util_0.Count(_ddl_api_00000, 706)
					return errors.Trace(err)
				}
			}
		default:
			trace_util_0.Count(_ddl_api_00000, 686)
			// Nothing to do now.
		}

		trace_util_0.Count(_ddl_api_00000, 668)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 707)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 664)
	return nil
}

func (d *ddl) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64) error {
	trace_util_0.Count(_ddl_api_00000, 708)
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 711)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 709)
	autoIncID, err := t.Allocator(ctx).NextGlobalAutoID(t.Meta().ID)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 712)
		return errors.Trace(err)
	}
	// If newBase < autoIncID, we need to do a rebase before returning.
	// Assume there are 2 TiDB servers: TiDB-A with allocator range of 0 ~ 30000; TiDB-B with allocator range of 30001 ~ 60000.
	// If the user sends SQL `alter table t1 auto_increment = 100` to TiDB-B,
	// and TiDB-B finds 100 < 30001 but returns without any handling,
	// then TiDB-A may still allocate 99 for auto_increment column. This doesn't make sense for the user.
	trace_util_0.Count(_ddl_api_00000, 710)
	newBase = mathutil.MaxInt64(newBase, autoIncID)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionRebaseAutoID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newBase},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ShardRowID shards the implicit row ID by adding shard value to the row ID's first few bits.
func (d *ddl) ShardRowID(ctx sessionctx.Context, tableIdent ast.Ident, uVal uint64) error {
	trace_util_0.Count(_ddl_api_00000, 713)
	schema, t, err := d.getSchemaAndTableByIdent(ctx, tableIdent)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 718)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 714)
	if uVal == t.Meta().ShardRowIDBits {
		trace_util_0.Count(_ddl_api_00000, 719)
		// Nothing need to do.
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 715)
	if uVal > 0 && t.Meta().PKIsHandle {
		trace_util_0.Count(_ddl_api_00000, 720)
		return errUnsupportedShardRowIDBits
	}
	trace_util_0.Count(_ddl_api_00000, 716)
	err = verifyNoOverflowShardBits(d.sessPool, t, uVal)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 721)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 717)
	job := &model.Job{
		Type:       model.ActionShardRowID,
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{uVal},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) getSchemaAndTableByIdent(ctx sessionctx.Context, tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	trace_util_0.Count(_ddl_api_00000, 722)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(tableIdent.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 725)
		return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(tableIdent.Schema)
	}
	trace_util_0.Count(_ddl_api_00000, 723)
	t, err = is.TableByName(tableIdent.Schema, tableIdent.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 726)
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.Schema, tableIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 724)
	return schema, t, nil
}

func checkUnsupportedColumnConstraint(col *ast.ColumnDef, ti ast.Ident) error {
	trace_util_0.Count(_ddl_api_00000, 727)
	for _, constraint := range col.Options {
		trace_util_0.Count(_ddl_api_00000, 729)
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement:
			trace_util_0.Count(_ddl_api_00000, 730)
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint AUTO_INCREMENT when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionPrimaryKey:
			trace_util_0.Count(_ddl_api_00000, 731)
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint PRIMARY KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionUniqKey:
			trace_util_0.Count(_ddl_api_00000, 732)
			return errUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint UNIQUE KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 728)
	return nil
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 733)
	specNewColumn := spec.NewColumns[0]

	err := checkUnsupportedColumnConstraint(specNewColumn, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 743)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 734)
	colName := specNewColumn.Name.Name.O
	if err = checkColumnAttributes(colName, specNewColumn.Tp); err != nil {
		trace_util_0.Count(_ddl_api_00000, 744)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 735)
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 745)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 736)
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + 1); err != nil {
		trace_util_0.Count(_ddl_api_00000, 746)
		return errors.Trace(err)
	}
	// Check whether added column has existed.
	trace_util_0.Count(_ddl_api_00000, 737)
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		trace_util_0.Count(_ddl_api_00000, 747)
		return infoschema.ErrColumnExists.GenWithStackByArgs(colName)
	}

	// If new column is a generated column, do validation.
	// NOTE: Because now we can only append columns to table,
	// we don't need check whether the column refers other
	// generated columns occurring later in table.
	trace_util_0.Count(_ddl_api_00000, 738)
	for _, option := range specNewColumn.Options {
		trace_util_0.Count(_ddl_api_00000, 748)
		if option.Tp == ast.ColumnOptionGenerated {
			trace_util_0.Count(_ddl_api_00000, 749)
			if err := checkIllegalFn4GeneratedColumn(specNewColumn.Name.Name.L, option.Expr); err != nil {
				trace_util_0.Count(_ddl_api_00000, 753)
				return errors.Trace(err)
			}

			trace_util_0.Count(_ddl_api_00000, 750)
			if option.Stored {
				trace_util_0.Count(_ddl_api_00000, 754)
				return errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Adding generated stored column through ALTER TABLE")
			}

			trace_util_0.Count(_ddl_api_00000, 751)
			_, dependColNames := findDependedColumnNames(specNewColumn)
			if err = checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
				trace_util_0.Count(_ddl_api_00000, 755)
				return errors.Trace(err)
			}

			trace_util_0.Count(_ddl_api_00000, 752)
			if err = checkDependedColExist(dependColNames, t.Cols()); err != nil {
				trace_util_0.Count(_ddl_api_00000, 756)
				return errors.Trace(err)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 739)
	if len(colName) > mysql.MaxColumnNameLength {
		trace_util_0.Count(_ddl_api_00000, 757)
		return ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	// Ignore table constraints now, maybe return error later.
	// We use length(t.Cols()) as the default offset firstly, we will change the
	// column's offset later.
	trace_util_0.Count(_ddl_api_00000, 740)
	col, _, err = buildColumnAndConstraint(ctx, len(t.Cols()), specNewColumn, nil, t.Meta().Charset, schema.Charset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 758)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 741)
	col.OriginDefaultValue, err = generateOriginDefaultValue(col.ToInfo())
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 759)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 742)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, spec.Position, 0},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AddTablePartitions will add a new partition to the table.
func (d *ddl) AddTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 760)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 769)
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	trace_util_0.Count(_ddl_api_00000, 761)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 770)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 762)
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		trace_util_0.Count(_ddl_api_00000, 771)
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	// We don't support add hash type partition now.
	trace_util_0.Count(_ddl_api_00000, 763)
	if meta.Partition.Type == model.PartitionTypeHash {
		trace_util_0.Count(_ddl_api_00000, 772)
		return errors.Trace(ErrUnsupportedAddPartition)
	}

	trace_util_0.Count(_ddl_api_00000, 764)
	partInfo, err := buildPartitionInfo(meta, d, spec)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 773)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 765)
	err = checkAddPartitionTooManyPartitions(uint64(len(meta.Partition.Definitions) + len(partInfo.Definitions)))
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 774)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 766)
	err = checkPartitionNameUnique(meta, partInfo)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 775)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 767)
	err = checkAddPartitionValue(meta, partInfo)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 776)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 768)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionAddTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// CoalescePartitions coalesce partitions can be used with a table that is partitioned by hash or key to reduce the number of partitions by number.
func (d *ddl) CoalescePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 777)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 783)
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	trace_util_0.Count(_ddl_api_00000, 778)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 784)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 779)
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		trace_util_0.Count(_ddl_api_00000, 785)
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	// Coalesce partition can only be used on hash/key partitions.
	trace_util_0.Count(_ddl_api_00000, 780)
	if meta.Partition.Type == model.PartitionTypeRange {
		trace_util_0.Count(_ddl_api_00000, 786)
		return errors.Trace(ErrCoalesceOnlyOnHashPartition)
	}

	// We don't support coalesce partitions hash type partition now.
	trace_util_0.Count(_ddl_api_00000, 781)
	if meta.Partition.Type == model.PartitionTypeHash {
		trace_util_0.Count(_ddl_api_00000, 787)
		return errors.Trace(ErrUnsupportedCoalescePartition)
	}

	trace_util_0.Count(_ddl_api_00000, 782)
	return errors.Trace(err)
}

func (d *ddl) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 788)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 794)
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	trace_util_0.Count(_ddl_api_00000, 789)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 795)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	trace_util_0.Count(_ddl_api_00000, 790)
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		trace_util_0.Count(_ddl_api_00000, 796)
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	trace_util_0.Count(_ddl_api_00000, 791)
	var pid int64
	pid, err = tables.FindPartitionByName(meta, spec.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 797)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 792)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionTruncateTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{pid},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 798)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 793)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 799)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 805)
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	trace_util_0.Count(_ddl_api_00000, 800)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 806)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	trace_util_0.Count(_ddl_api_00000, 801)
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		trace_util_0.Count(_ddl_api_00000, 807)
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	trace_util_0.Count(_ddl_api_00000, 802)
	err = checkDropTablePartition(meta, spec.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 808)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 803)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    meta.ID,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.Name},
	}

	err = d.doDDLJob(ctx, job)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 809)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 804)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx sessionctx.Context, ti ast.Ident, colName model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 810)
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 815)
		return errors.Trace(err)
	}

	// Check whether dropped column has existed.
	trace_util_0.Count(_ddl_api_00000, 811)
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		trace_util_0.Count(_ddl_api_00000, 816)
		return ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}

	trace_util_0.Count(_ddl_api_00000, 812)
	tblInfo := t.Meta()
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		trace_util_0.Count(_ddl_api_00000, 817)
		return errors.Trace(err)
	}
	// We don't support dropping column with PK handle covered now.
	trace_util_0.Count(_ddl_api_00000, 813)
	if col.IsPKHandleColumn(tblInfo) {
		trace_util_0.Count(_ddl_api_00000, 818)
		return errUnsupportedPKHandle
	}

	trace_util_0.Count(_ddl_api_00000, 814)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// modifiableCharsetAndCollation returns error when the charset or collation is not modifiable.
func modifiableCharsetAndCollation(toCharset, toCollate, origCharset, origCollate string) error {
	trace_util_0.Count(_ddl_api_00000, 819)
	if !charset.ValidCharsetAndCollation(toCharset, toCollate) {
		trace_util_0.Count(_ddl_api_00000, 824)
		return ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toCharset, toCollate)
	}
	trace_util_0.Count(_ddl_api_00000, 820)
	if toCharset == charset.CharsetUTF8MB4 && origCharset == charset.CharsetUTF8 {
		trace_util_0.Count(_ddl_api_00000, 825)
		// TiDB only allow utf8 to be changed to utf8mb4.
		return nil
	}

	trace_util_0.Count(_ddl_api_00000, 821)
	if toCharset != origCharset {
		trace_util_0.Count(_ddl_api_00000, 826)
		msg := fmt.Sprintf("charset from %s to %s", origCharset, toCharset)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	trace_util_0.Count(_ddl_api_00000, 822)
	if toCollate != origCollate {
		trace_util_0.Count(_ddl_api_00000, 827)
		msg := fmt.Sprintf("collate from %s to %s", origCollate, toCollate)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	trace_util_0.Count(_ddl_api_00000, 823)
	return nil
}

// modifiable checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the table.
// It returns true if the two types has the same Charset and Collation, the same sign, both are
// integer types or string types, and new Flen and Decimal must be greater than or equal to origin.
func modifiable(origin *types.FieldType, to *types.FieldType) error {
	trace_util_0.Count(_ddl_api_00000, 828)
	// The root cause is modifying decimal precision needs to rewrite binary representation of that decimal.
	if origin.Tp == mysql.TypeNewDecimal && (to.Flen != origin.Flen || to.Decimal != origin.Decimal) {
		trace_util_0.Count(_ddl_api_00000, 835)
		return errUnsupportedModifyColumn.GenWithStack("unsupported modify decimal column precision")
	}
	trace_util_0.Count(_ddl_api_00000, 829)
	if to.Flen > 0 && to.Flen < origin.Flen {
		trace_util_0.Count(_ddl_api_00000, 836)
		msg := fmt.Sprintf("length %d is less than origin %d", to.Flen, origin.Flen)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	trace_util_0.Count(_ddl_api_00000, 830)
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		trace_util_0.Count(_ddl_api_00000, 837)
		msg := fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	trace_util_0.Count(_ddl_api_00000, 831)
	if err := modifiableCharsetAndCollation(to.Charset, to.Collate, origin.Charset, origin.Collate); err != nil {
		trace_util_0.Count(_ddl_api_00000, 838)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 832)
	toUnsigned := mysql.HasUnsignedFlag(to.Flag)
	originUnsigned := mysql.HasUnsignedFlag(origin.Flag)
	if originUnsigned != toUnsigned {
		trace_util_0.Count(_ddl_api_00000, 839)
		msg := fmt.Sprintf("unsigned %v not match origin %v", toUnsigned, originUnsigned)
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}
	trace_util_0.Count(_ddl_api_00000, 833)
	switch origin.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_ddl_api_00000, 840)
		switch to.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			trace_util_0.Count(_ddl_api_00000, 845)
			return nil
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		trace_util_0.Count(_ddl_api_00000, 841)
		switch to.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			trace_util_0.Count(_ddl_api_00000, 846)
			return nil
		}
	case mysql.TypeEnum:
		trace_util_0.Count(_ddl_api_00000, 842)
		if origin.Tp == to.Tp {
			trace_util_0.Count(_ddl_api_00000, 847)
			if len(to.Elems) < len(origin.Elems) {
				trace_util_0.Count(_ddl_api_00000, 850)
				msg := fmt.Sprintf("the number of enum column's elements is less than the original: %d", len(origin.Elems))
				return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
			}
			trace_util_0.Count(_ddl_api_00000, 848)
			for index, originElem := range origin.Elems {
				trace_util_0.Count(_ddl_api_00000, 851)
				toElem := to.Elems[index]
				if originElem != toElem {
					trace_util_0.Count(_ddl_api_00000, 852)
					msg := fmt.Sprintf("cannot modify enum column value %s to %s", originElem, toElem)
					return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
				}
			}
			trace_util_0.Count(_ddl_api_00000, 849)
			return nil
		}
		trace_util_0.Count(_ddl_api_00000, 843)
		msg := fmt.Sprintf("cannot modify enum type column's to type %s", to.String())
		return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
	default:
		trace_util_0.Count(_ddl_api_00000, 844)
		if origin.Tp == to.Tp {
			trace_util_0.Count(_ddl_api_00000, 853)
			return nil
		}
	}
	trace_util_0.Count(_ddl_api_00000, 834)
	msg := fmt.Sprintf("type %v not match origin %v", to.Tp, origin.Tp)
	return errUnsupportedModifyColumn.GenWithStackByArgs(msg)
}

func setDefaultValue(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) (bool, error) {
	trace_util_0.Count(_ddl_api_00000, 854)
	hasDefaultValue := false
	value, err := getDefaultValue(ctx, col.Name.L, option, &col.FieldType)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 859)
		return hasDefaultValue, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 855)
	if hasDefaultValue, value, err = checkColumnDefaultValue(ctx, col, value); err != nil {
		trace_util_0.Count(_ddl_api_00000, 860)
		return hasDefaultValue, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 856)
	value, err = convertTimestampDefaultValToUTC(ctx, value, col)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 861)
		return hasDefaultValue, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 857)
	err = col.SetDefaultValue(value)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 862)
		return hasDefaultValue, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 858)
	return hasDefaultValue, nil
}

func setColumnComment(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) error {
	trace_util_0.Count(_ddl_api_00000, 863)
	value, err := expression.EvalAstExpr(ctx, option.Expr)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 865)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 864)
	col.Comment, err = value.ToString()
	return errors.Trace(err)
}

// processColumnOptions is only used in getModifiableColumnJob.
func processColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	trace_util_0.Count(_ddl_api_00000, 866)
	if len(options) == 0 {
		trace_util_0.Count(_ddl_api_00000, 870)
		return nil
	}

	trace_util_0.Count(_ddl_api_00000, 867)
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	for _, opt := range options {
		trace_util_0.Count(_ddl_api_00000, 871)
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			trace_util_0.Count(_ddl_api_00000, 872)
			hasDefaultValue, err = setDefaultValue(ctx, col, opt)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 883)
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			trace_util_0.Count(_ddl_api_00000, 873)
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 884)
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			trace_util_0.Count(_ddl_api_00000, 874)
			col.Flag |= mysql.NotNullFlag
		case ast.ColumnOptionNull:
			trace_util_0.Count(_ddl_api_00000, 875)
			col.Flag &= ^mysql.NotNullFlag
		case ast.ColumnOptionAutoIncrement:
			trace_util_0.Count(_ddl_api_00000, 876)
			col.Flag |= mysql.AutoIncrementFlag
		case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
			trace_util_0.Count(_ddl_api_00000, 877)
			return errUnsupportedModifyColumn.GenWithStack("unsupported modify column constraint - %v", opt.Tp)
		case ast.ColumnOptionOnUpdate:
			trace_util_0.Count(_ddl_api_00000, 878)
			// TODO: Support other time functions.
			if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
				trace_util_0.Count(_ddl_api_00000, 885)
				if !expression.IsCurrentTimestampExpr(opt.Expr) {
					trace_util_0.Count(_ddl_api_00000, 886)
					return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
			} else {
				trace_util_0.Count(_ddl_api_00000, 887)
				{
					return ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
			}
			trace_util_0.Count(_ddl_api_00000, 879)
			col.Flag |= mysql.OnUpdateNowFlag
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			trace_util_0.Count(_ddl_api_00000, 880)
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 888)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 881)
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			col.GeneratedExpr = opt.Expr
			for _, colName := range findColumnNamesInExpr(opt.Expr) {
				trace_util_0.Count(_ddl_api_00000, 889)
				col.Dependences[colName.Name.L] = struct{}{}
			}
		default:
			trace_util_0.Count(_ddl_api_00000, 882)
			// TODO: Support other types.
			return errors.Trace(errUnsupportedModifyColumn.GenWithStackByArgs(opt.Tp))
		}
	}

	trace_util_0.Count(_ddl_api_00000, 868)
	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	if hasDefaultValue {
		trace_util_0.Count(_ddl_api_00000, 890)
		return errors.Trace(checkDefaultValue(ctx, col, true))
	}

	trace_util_0.Count(_ddl_api_00000, 869)
	return nil
}

func (d *ddl) getModifiableColumnJob(ctx sessionctx.Context, ident ast.Ident, originalColName model.CIStr,
	spec *ast.AlterTableSpec) (*model.Job, error) {
	trace_util_0.Count(_ddl_api_00000, 891)
	specNewColumn := spec.NewColumns[0]
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 907)
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	trace_util_0.Count(_ddl_api_00000, 892)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 908)
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 893)
	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		trace_util_0.Count(_ddl_api_00000, 909)
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 894)
	newColName := specNewColumn.Name.Name
	// If we want to rename the column name, we need to check whether it already exists.
	if newColName.L != originalColName.L {
		trace_util_0.Count(_ddl_api_00000, 910)
		c := table.FindCol(t.Cols(), newColName.L)
		if c != nil {
			trace_util_0.Count(_ddl_api_00000, 911)
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
		}
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `processColumnOptions` later.
	trace_util_0.Count(_ddl_api_00000, 895)
	if specNewColumn.Tp == nil {
		trace_util_0.Count(_ddl_api_00000, 912)
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(errUnsupportedModifyColumn)
	}

	trace_util_0.Count(_ddl_api_00000, 896)
	if err = checkColumnAttributes(specNewColumn.Name.OrigColName(), specNewColumn.Tp); err != nil {
		trace_util_0.Count(_ddl_api_00000, 913)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 897)
	newCol := table.ToColumn(&model.ColumnInfo{
		ID: col.ID,
		// We use this PR(https://github.com/pingcap/tidb/pull/6274) as the dividing line to define whether it is a new version or an old version TiDB.
		// The old version TiDB initializes the column's offset and state here.
		// The new version TiDB doesn't initialize the column's offset and state, and it will do the initialization in run DDL function.
		// When we do the rolling upgrade the following may happen:
		// a new version TiDB builds the DDL job that doesn't be set the column's offset and state,
		// and the old version TiDB is the DDL owner, it doesn't get offset and state from the store. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:             col.Offset,
		State:              col.State,
		OriginDefaultValue: col.OriginDefaultValue,
		FieldType:          *specNewColumn.Tp,
		Name:               newColName,
		Version:            col.Version,
	})

	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.Charset) == 0 && t.Meta().Version < model.TableInfoVersion1 {
		trace_util_0.Count(_ddl_api_00000, 914)
		newCol.FieldType.Charset = col.FieldType.Charset
		newCol.FieldType.Collate = col.FieldType.Collate
	}
	trace_util_0.Count(_ddl_api_00000, 898)
	err = setCharsetCollationFlenDecimal(&newCol.FieldType, t.Meta().Charset, schema.Charset)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 915)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 899)
	err = modifiable(&col.FieldType, &newCol.FieldType)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 916)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 900)
	if err = processColumnOptions(ctx, newCol, specNewColumn.Options); err != nil {
		trace_util_0.Count(_ddl_api_00000, 917)
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	trace_util_0.Count(_ddl_api_00000, 901)
	indexFlags := col.FieldType.Flag & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.Flag |= indexFlags
	if mysql.HasPriKeyFlag(col.FieldType.Flag) {
		trace_util_0.Count(_ddl_api_00000, 918)
		newCol.FieldType.Flag |= mysql.NotNullFlag
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	trace_util_0.Count(_ddl_api_00000, 902)
	if !mysql.HasAutoIncrementFlag(col.Flag) && mysql.HasAutoIncrementFlag(newCol.Flag) {
		trace_util_0.Count(_ddl_api_00000, 919)
		return nil, errUnsupportedModifyColumn.GenWithStackByArgs("set auto_increment")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	trace_util_0.Count(_ddl_api_00000, 903)
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		trace_util_0.Count(_ddl_api_00000, 920)
		if err = checkForNullValue(ctx, col.Tp == newCol.Tp, ident.Schema, ident.Name, col.Name, newCol.Name); err != nil {
			trace_util_0.Count(_ddl_api_00000, 922)
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		trace_util_0.Count(_ddl_api_00000, 921)
		modifyColumnTp = mysql.TypeNull
	}

	trace_util_0.Count(_ddl_api_00000, 904)
	if err = checkColumnFieldLength(newCol); err != nil {
		trace_util_0.Count(_ddl_api_00000, 923)
		return nil, errors.Trace(err)
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	trace_util_0.Count(_ddl_api_00000, 905)
	if err = checkModifyGeneratedColumn(t.Cols(), col, newCol); err != nil {
		trace_util_0.Count(_ddl_api_00000, 924)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 906)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName, spec.Position, modifyColumnTp},
	}
	return job, nil
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ChangeColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 925)
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		trace_util_0.Count(_ddl_api_00000, 931)
		return ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	trace_util_0.Count(_ddl_api_00000, 926)
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		trace_util_0.Count(_ddl_api_00000, 932)
		return ErrWrongDBName.GenWithStackByArgs(spec.OldColumnName.Schema.O)
	}
	trace_util_0.Count(_ddl_api_00000, 927)
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		trace_util_0.Count(_ddl_api_00000, 933)
		return ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}
	trace_util_0.Count(_ddl_api_00000, 928)
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		trace_util_0.Count(_ddl_api_00000, 934)
		return ErrWrongTableName.GenWithStackByArgs(spec.OldColumnName.Table.O)
	}

	trace_util_0.Count(_ddl_api_00000, 929)
	job, err := d.getModifiableColumnJob(ctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 935)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 930)
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ModifyColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 936)
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		trace_util_0.Count(_ddl_api_00000, 941)
		return ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	trace_util_0.Count(_ddl_api_00000, 937)
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		trace_util_0.Count(_ddl_api_00000, 942)
		return ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}

	// If the modified column is generated, check whether it refers to any auto-increment columns.
	trace_util_0.Count(_ddl_api_00000, 938)
	for _, option := range specNewColumn.Options {
		trace_util_0.Count(_ddl_api_00000, 943)
		if option.Tp == ast.ColumnOptionGenerated {
			trace_util_0.Count(_ddl_api_00000, 944)
			_, t, err := d.getSchemaAndTableByIdent(ctx, ident)
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 946)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 945)
			_, dependColNames := findDependedColumnNames(specNewColumn)
			if err := checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
				trace_util_0.Count(_ddl_api_00000, 947)
				return errors.Trace(err)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 939)
	originalColName := specNewColumn.Name.Name
	job, err := d.getModifiableColumnJob(ctx, ident, originalColName, spec)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 948)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 940)
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) AlterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 949)
	specNewColumn := spec.NewColumns[0]
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 954)
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 950)
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 955)
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}

	trace_util_0.Count(_ddl_api_00000, 951)
	colName := specNewColumn.Name.Name
	// Check whether alter column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		trace_util_0.Count(_ddl_api_00000, 956)
		return ErrBadField.GenWithStackByArgs(colName, ident.Name)
	}

	// Clean the NoDefaultValueFlag value.
	trace_util_0.Count(_ddl_api_00000, 952)
	col.Flag &= ^mysql.NoDefaultValueFlag
	if len(specNewColumn.Options) == 0 {
		trace_util_0.Count(_ddl_api_00000, 957)
		err = col.SetDefaultValue(nil)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 959)
			return errors.Trace(err)
		}
		trace_util_0.Count(_ddl_api_00000, 958)
		setNoDefaultValueFlag(col, false)
	} else {
		trace_util_0.Count(_ddl_api_00000, 960)
		{
			hasDefaultValue, err := setDefaultValue(ctx, col, specNewColumn.Options[0])
			if err != nil {
				trace_util_0.Count(_ddl_api_00000, 962)
				return errors.Trace(err)
			}
			trace_util_0.Count(_ddl_api_00000, 961)
			if err = checkDefaultValue(ctx, col, hasDefaultValue); err != nil {
				trace_util_0.Count(_ddl_api_00000, 963)
				return errors.Trace(err)
			}
		}
	}

	trace_util_0.Count(_ddl_api_00000, 953)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionSetDefaultValue,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableComment updates the table comment information.
func (d *ddl) AlterTableComment(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 964)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 967)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	trace_util_0.Count(_ddl_api_00000, 965)
	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 968)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 966)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionModifyTableComment,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.Comment},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableCharset changes the table charset and collate.
func (d *ddl) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string) error {
	trace_util_0.Count(_ddl_api_00000, 969)
	// use the last one.
	if toCharset == "" && toCollate == "" {
		trace_util_0.Count(_ddl_api_00000, 977)
		return ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	trace_util_0.Count(_ddl_api_00000, 970)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 978)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	trace_util_0.Count(_ddl_api_00000, 971)
	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 979)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 972)
	if toCharset == "" {
		trace_util_0.Count(_ddl_api_00000, 980)
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	trace_util_0.Count(_ddl_api_00000, 973)
	if toCollate == "" {
		trace_util_0.Count(_ddl_api_00000, 981)
		// get the default collation of the charset.
		toCollate, err = charset.GetDefaultCollation(toCharset)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 982)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_ddl_api_00000, 974)
	doNothing, err := checkAlterTableCharset(tb.Meta(), schema, toCharset, toCollate)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 983)
		return err
	}
	trace_util_0.Count(_ddl_api_00000, 975)
	if doNothing {
		trace_util_0.Count(_ddl_api_00000, 984)
		return nil
	}

	trace_util_0.Count(_ddl_api_00000, 976)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionModifyTableCharsetAndCollate,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{toCharset, toCollate},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of table.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of table.
// err: if err is not nil, means it is not possible to change table charset to target charset.
func checkAlterTableCharset(tblInfo *model.TableInfo, dbInfo *model.DBInfo, toCharset, toCollate string) (doNothing bool, err error) {
	trace_util_0.Count(_ddl_api_00000, 985)
	origCharset := tblInfo.Charset
	origCollate := tblInfo.Collate
	// Old version schema charset maybe modified when load schema if TreatOldVersionUTF8AsUTF8MB4 was enable.
	// So even if the origCharset equal toCharset, we still need to do the ddl for old version schema.
	if origCharset == toCharset && origCollate == toCollate && tblInfo.Version >= model.TableInfoVersion2 {
		trace_util_0.Count(_ddl_api_00000, 990)
		// nothing to do.
		doNothing = true
		for _, col := range tblInfo.Columns {
			trace_util_0.Count(_ddl_api_00000, 992)
			if col.Charset == charset.CharsetBin {
				trace_util_0.Count(_ddl_api_00000, 995)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 993)
			if col.Charset == toCharset && col.Collate == toCollate {
				trace_util_0.Count(_ddl_api_00000, 996)
				continue
			}
			trace_util_0.Count(_ddl_api_00000, 994)
			doNothing = false
		}
		trace_util_0.Count(_ddl_api_00000, 991)
		if doNothing {
			trace_util_0.Count(_ddl_api_00000, 997)
			return doNothing, nil
		}
	}

	trace_util_0.Count(_ddl_api_00000, 986)
	if len(origCharset) == 0 {
		trace_util_0.Count(_ddl_api_00000, 998)
		// The table charset may be "", if the table is create in old TiDB version, such as v2.0.8.
		// This DDL will update the table charset to default charset.
		origCharset, origCollate, err = ResolveCharsetCollation("", dbInfo.Charset)
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 999)
			return doNothing, err
		}
	}

	trace_util_0.Count(_ddl_api_00000, 987)
	if err = modifiableCharsetAndCollation(toCharset, toCollate, origCharset, origCollate); err != nil {
		trace_util_0.Count(_ddl_api_00000, 1000)
		return doNothing, err
	}

	trace_util_0.Count(_ddl_api_00000, 988)
	for _, col := range tblInfo.Columns {
		trace_util_0.Count(_ddl_api_00000, 1001)
		if col.Tp == mysql.TypeVarchar {
			trace_util_0.Count(_ddl_api_00000, 1005)
			if err = IsTooBigFieldLength(col.Flen, col.Name.O, toCharset); err != nil {
				trace_util_0.Count(_ddl_api_00000, 1006)
				return doNothing, err
			}
		}
		trace_util_0.Count(_ddl_api_00000, 1002)
		if col.Charset == charset.CharsetBin {
			trace_util_0.Count(_ddl_api_00000, 1007)
			continue
		}
		trace_util_0.Count(_ddl_api_00000, 1003)
		if len(col.Charset) == 0 {
			trace_util_0.Count(_ddl_api_00000, 1008)
			continue
		}
		trace_util_0.Count(_ddl_api_00000, 1004)
		if err = modifiableCharsetAndCollation(toCharset, toCollate, col.Charset, col.Collate); err != nil {
			trace_util_0.Count(_ddl_api_00000, 1009)
			return doNothing, err
		}
	}
	trace_util_0.Count(_ddl_api_00000, 989)
	return doNothing, nil
}

// RenameIndex renames an index.
// In TiDB, indexes are case-insensitive (so index 'a' and 'A" are considered the same index),
// but index names are case-sensitive (we can rename index 'a' to 'A')
func (d *ddl) RenameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	trace_util_0.Count(_ddl_api_00000, 1010)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1015)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	trace_util_0.Count(_ddl_api_00000, 1011)
	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1016)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	trace_util_0.Count(_ddl_api_00000, 1012)
	duplicate, err := validateRenameIndex(spec.FromKey, spec.ToKey, tb.Meta())
	if duplicate {
		trace_util_0.Count(_ddl_api_00000, 1017)
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 1013)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1018)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 1014)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionRenameIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{spec.FromKey, spec.ToKey},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx sessionctx.Context, ti ast.Ident) (err error) {
	trace_util_0.Count(_ddl_api_00000, 1019)
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1021)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 1020)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropView will proceed even if some view in the list does not exists.
func (d *ddl) DropView(ctx sessionctx.Context, ti ast.Ident) (err error) {
	trace_util_0.Count(_ddl_api_00000, 1022)
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1025)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 1023)
	if !tb.Meta().IsView() {
		trace_util_0.Count(_ddl_api_00000, 1026)
		return ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "VIEW")
	}

	trace_util_0.Count(_ddl_api_00000, 1024)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropView,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) TruncateTable(ctx sessionctx.Context, ti ast.Ident) error {
	trace_util_0.Count(_ddl_api_00000, 1027)
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1030)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 1028)
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1031)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 1029)
	newTableID := genIDs[0]
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) RenameTable(ctx sessionctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	trace_util_0.Count(_ddl_api_00000, 1032)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	oldSchema, ok := is.SchemaByName(oldIdent.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1038)
		if isAlterTable {
			trace_util_0.Count(_ddl_api_00000, 1041)
			return infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		trace_util_0.Count(_ddl_api_00000, 1039)
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			trace_util_0.Count(_ddl_api_00000, 1042)
			return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		trace_util_0.Count(_ddl_api_00000, 1040)
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 1033)
	oldTbl, err := is.TableByName(oldIdent.Schema, oldIdent.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1043)
		if isAlterTable {
			trace_util_0.Count(_ddl_api_00000, 1046)
			return infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		trace_util_0.Count(_ddl_api_00000, 1044)
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			trace_util_0.Count(_ddl_api_00000, 1047)
			return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		trace_util_0.Count(_ddl_api_00000, 1045)
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 1034)
	if isAlterTable && newIdent.Schema.L == oldIdent.Schema.L && newIdent.Name.L == oldIdent.Name.L {
		trace_util_0.Count(_ddl_api_00000, 1048)
		// oldIdent is equal to newIdent, do nothing
		return nil
	}
	trace_util_0.Count(_ddl_api_00000, 1035)
	newSchema, ok := is.SchemaByName(newIdent.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1049)
		return errErrorOnRename.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name, newIdent.Schema, newIdent.Name)
	}
	trace_util_0.Count(_ddl_api_00000, 1036)
	if is.TableExists(newIdent.Schema, newIdent.Name) {
		trace_util_0.Count(_ddl_api_00000, 1050)
		return infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
	}

	trace_util_0.Count(_ddl_api_00000, 1037)
	job := &model.Job{
		SchemaID:   newSchema.ID,
		TableID:    oldTbl.Meta().ID,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchema.ID, newIdent.Name},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func getAnonymousIndex(t table.Table, colName model.CIStr) model.CIStr {
	trace_util_0.Count(_ddl_api_00000, 1051)
	id := 2
	l := len(t.Indices())
	indexName := colName
	for i := 0; i < l; i++ {
		trace_util_0.Count(_ddl_api_00000, 1053)
		if t.Indices()[i].Meta().Name.L == indexName.L {
			trace_util_0.Count(_ddl_api_00000, 1054)
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			i = -1
			id++
		}
	}
	trace_util_0.Count(_ddl_api_00000, 1052)
	return indexName
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, ti ast.Ident, unique bool, indexName model.CIStr,
	idxColNames []*ast.IndexColName, indexOption *ast.IndexOption) error {
	trace_util_0.Count(_ddl_api_00000, 1055)
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1063)
		return errors.Trace(err)
	}

	// Deal with anonymous index.
	trace_util_0.Count(_ddl_api_00000, 1056)
	if len(indexName.L) == 0 {
		trace_util_0.Count(_ddl_api_00000, 1064)
		indexName = getAnonymousIndex(t, idxColNames[0].Column.Name)
	}

	trace_util_0.Count(_ddl_api_00000, 1057)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		trace_util_0.Count(_ddl_api_00000, 1065)
		return ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	trace_util_0.Count(_ddl_api_00000, 1058)
	if err = checkTooLongIndex(indexName); err != nil {
		trace_util_0.Count(_ddl_api_00000, 1066)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 1059)
	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redudant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	_, err = buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1067)
		return errors.Trace(err)
	}
	trace_util_0.Count(_ddl_api_00000, 1060)
	if unique && tblInfo.GetPartitionInfo() != nil {
		trace_util_0.Count(_ddl_api_00000, 1068)
		if err := checkPartitionKeysConstraint(ctx, tblInfo.GetPartitionInfo().Expr, idxColNames, tblInfo); err != nil {
			trace_util_0.Count(_ddl_api_00000, 1069)
			return err
		}
	}

	trace_util_0.Count(_ddl_api_00000, 1061)
	if indexOption != nil {
		trace_util_0.Count(_ddl_api_00000, 1070)
		// May be truncate comment here, when index comment too long and sql_mode is't strict.
		indexOption.Comment, err = validateCommentLength(ctx.GetSessionVars(),
			indexOption.Comment,
			maxCommentLength,
			errTooLongIndexComment.GenWithStackByArgs(indexName.String(), maxCommentLength))
		if err != nil {
			trace_util_0.Count(_ddl_api_00000, 1071)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_ddl_api_00000, 1062)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption},
		Priority:   ctx.GetSessionVars().DDLReorgPriority,
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildFKInfo(fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef, cols []*table.Column) (*model.FKInfo, error) {
	trace_util_0.Count(_ddl_api_00000, 1072)
	var fkInfo model.FKInfo
	fkInfo.Name = fkName
	fkInfo.RefTable = refer.Table.Name

	fkInfo.Cols = make([]model.CIStr, len(keys))
	for i, key := range keys {
		trace_util_0.Count(_ddl_api_00000, 1075)
		if table.FindCol(cols, key.Column.Name.O) == nil {
			trace_util_0.Count(_ddl_api_00000, 1077)
			return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		trace_util_0.Count(_ddl_api_00000, 1076)
		fkInfo.Cols[i] = key.Column.Name
	}

	trace_util_0.Count(_ddl_api_00000, 1073)
	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexColNames))
	for i, key := range refer.IndexColNames {
		trace_util_0.Count(_ddl_api_00000, 1078)
		fkInfo.RefCols[i] = key.Column.Name
	}

	trace_util_0.Count(_ddl_api_00000, 1074)
	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return &fkInfo, nil

}

func (d *ddl) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) error {
	trace_util_0.Count(_ddl_api_00000, 1079)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1083)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	trace_util_0.Count(_ddl_api_00000, 1080)
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1084)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 1081)
	fkInfo, err := buildFKInfo(fkName, keys, refer, t.Cols())
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1085)
		return errors.Trace(err)
	}

	trace_util_0.Count(_ddl_api_00000, 1082)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)

}

func (d *ddl) DropForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 1086)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1089)
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	trace_util_0.Count(_ddl_api_00000, 1087)
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1090)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 1088)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 1091)
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		trace_util_0.Count(_ddl_api_00000, 1095)
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	trace_util_0.Count(_ddl_api_00000, 1092)
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1096)
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	trace_util_0.Count(_ddl_api_00000, 1093)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo == nil {
		trace_util_0.Count(_ddl_api_00000, 1097)
		return ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	trace_util_0.Count(_ddl_api_00000, 1094)
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func isDroppableColumn(tblInfo *model.TableInfo, colName model.CIStr) error {
	trace_util_0.Count(_ddl_api_00000, 1098)
	// Check whether there are other columns depend on this column or not.
	for _, col := range tblInfo.Columns {
		trace_util_0.Count(_ddl_api_00000, 1102)
		for dep := range col.Dependences {
			trace_util_0.Count(_ddl_api_00000, 1103)
			if dep == colName.L {
				trace_util_0.Count(_ddl_api_00000, 1104)
				return errDependentByGeneratedColumn.GenWithStackByArgs(dep)
			}
		}
	}
	trace_util_0.Count(_ddl_api_00000, 1099)
	if len(tblInfo.Columns) == 1 {
		trace_util_0.Count(_ddl_api_00000, 1105)
		return ErrCantRemoveAllFields.GenWithStack("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}
	// We don't support dropping column with index covered now.
	// We must drop the index first, then drop the column.
	trace_util_0.Count(_ddl_api_00000, 1100)
	if isColumnWithIndex(colName.L, tblInfo.Indices) {
		trace_util_0.Count(_ddl_api_00000, 1106)
		return errCantDropColWithIndex.GenWithStack("can't drop column %s with index covered now", colName)
	}
	trace_util_0.Count(_ddl_api_00000, 1101)
	return nil
}

// validateCommentLength checks comment length of table, column, index and partition.
// If comment length is more than the standard length truncate it
// and store the comment length upto the standard comment length size.
func validateCommentLength(vars *variable.SessionVars, comment string, maxLen int, err error) (string, error) {
	trace_util_0.Count(_ddl_api_00000, 1107)
	if len(comment) > maxLen {
		trace_util_0.Count(_ddl_api_00000, 1109)
		if vars.StrictSQLMode {
			trace_util_0.Count(_ddl_api_00000, 1111)
			return "", err
		}
		trace_util_0.Count(_ddl_api_00000, 1110)
		vars.StmtCtx.AppendWarning(err)
		return comment[:maxLen], nil
	}
	trace_util_0.Count(_ddl_api_00000, 1108)
	return comment, nil
}

func buildPartitionInfo(meta *model.TableInfo, d *ddl, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	trace_util_0.Count(_ddl_api_00000, 1112)
	if meta.Partition.Type == model.PartitionTypeRange && len(spec.PartDefinitions) == 0 {
		trace_util_0.Count(_ddl_api_00000, 1116)
		return nil, errors.Trace(ErrPartitionsMustBeDefined)
	}
	trace_util_0.Count(_ddl_api_00000, 1113)
	part := &model.PartitionInfo{
		Type:    meta.Partition.Type,
		Expr:    meta.Partition.Expr,
		Columns: meta.Partition.Columns,
		Enable:  meta.Partition.Enable,
	}
	genIDs, err := d.genGlobalIDs(len(spec.PartDefinitions))
	if err != nil {
		trace_util_0.Count(_ddl_api_00000, 1117)
		return nil, err
	}
	trace_util_0.Count(_ddl_api_00000, 1114)
	buf := new(bytes.Buffer)
	for ith, def := range spec.PartDefinitions {
		trace_util_0.Count(_ddl_api_00000, 1118)
		for _, expr := range def.LessThan {
			trace_util_0.Count(_ddl_api_00000, 1121)
			tp := expr.GetType().Tp
			if len(part.Columns) == 0 {
				trace_util_0.Count(_ddl_api_00000, 1122)
				// Partition by range.
				if !(tp == mysql.TypeLong || tp == mysql.TypeLonglong) {
					trace_util_0.Count(_ddl_api_00000, 1123)
					expr.Format(buf)
					if strings.EqualFold(buf.String(), "MAXVALUE") {
						trace_util_0.Count(_ddl_api_00000, 1125)
						continue
					}
					trace_util_0.Count(_ddl_api_00000, 1124)
					buf.Reset()
					return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(buf.String(), "partition function")
				}
			}
			// Partition by range columns if len(part.Columns) != 0.
		}
		trace_util_0.Count(_ddl_api_00000, 1119)
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			ID:      genIDs[ith],
			Comment: def.Comment,
		}

		buf := new(bytes.Buffer)
		for _, expr := range def.LessThan {
			trace_util_0.Count(_ddl_api_00000, 1126)
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		trace_util_0.Count(_ddl_api_00000, 1120)
		part.Definitions = append(part.Definitions, piDef)
	}
	trace_util_0.Count(_ddl_api_00000, 1115)
	return part, nil
}

var _ddl_api_00000 = "ddl/ddl_api.go"
