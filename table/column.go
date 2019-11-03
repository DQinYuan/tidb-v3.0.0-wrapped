// Copyright 2016 The ql Authors. All rights reserved.
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

package table

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	trace_util_0.Count(_column_00000, 0)
	ans := []string{c.Name.O, types.TypeToStr(c.Tp, c.Charset)}
	if mysql.HasAutoIncrementFlag(c.Flag) {
		trace_util_0.Count(_column_00000, 3)
		ans = append(ans, "AUTO_INCREMENT")
	}
	trace_util_0.Count(_column_00000, 1)
	if mysql.HasNotNullFlag(c.Flag) {
		trace_util_0.Count(_column_00000, 4)
		ans = append(ans, "NOT NULL")
	}
	trace_util_0.Count(_column_00000, 2)
	return strings.Join(ans, " ")
}

// ToInfo casts Column to model.ColumnInfo
// NOTE: DONT modify return value.
func (c *Column) ToInfo() *model.ColumnInfo {
	trace_util_0.Count(_column_00000, 5)
	return c.ColumnInfo
}

// FindCol finds column in cols by name.
func FindCol(cols []*Column, name string) *Column {
	trace_util_0.Count(_column_00000, 6)
	for _, col := range cols {
		trace_util_0.Count(_column_00000, 8)
		if strings.EqualFold(col.Name.O, name) {
			trace_util_0.Count(_column_00000, 9)
			return col
		}
	}
	trace_util_0.Count(_column_00000, 7)
	return nil
}

// ToColumn converts a *model.ColumnInfo to *Column.
func ToColumn(col *model.ColumnInfo) *Column {
	trace_util_0.Count(_column_00000, 10)
	return &Column{
		col,
		nil,
	}
}

// FindCols finds columns in cols by names.
// If pkIsHandle is false and name is ExtraHandleName, the extra handle column will be added.
func FindCols(cols []*Column, names []string, pkIsHandle bool) ([]*Column, error) {
	trace_util_0.Count(_column_00000, 11)
	var rcols []*Column
	for _, name := range names {
		trace_util_0.Count(_column_00000, 13)
		col := FindCol(cols, name)
		if col != nil {
			trace_util_0.Count(_column_00000, 14)
			rcols = append(rcols, col)
		} else {
			trace_util_0.Count(_column_00000, 15)
			if name == model.ExtraHandleName.L && !pkIsHandle {
				trace_util_0.Count(_column_00000, 16)
				col := &Column{}
				col.ColumnInfo = model.NewExtraHandleColInfo()
				col.ColumnInfo.Offset = len(cols)
				rcols = append(rcols, col)
			} else {
				trace_util_0.Count(_column_00000, 17)
				{
					return nil, errUnknownColumn.GenWithStack("unknown column %s", name)
				}
			}
		}
	}

	trace_util_0.Count(_column_00000, 12)
	return rcols, nil
}

// FindOnUpdateCols finds columns which have OnUpdateNow flag.
func FindOnUpdateCols(cols []*Column) []*Column {
	trace_util_0.Count(_column_00000, 18)
	var rcols []*Column
	for _, col := range cols {
		trace_util_0.Count(_column_00000, 20)
		if mysql.HasOnUpdateNowFlag(col.Flag) {
			trace_util_0.Count(_column_00000, 21)
			rcols = append(rcols, col)
		}
	}

	trace_util_0.Count(_column_00000, 19)
	return rcols
}

// truncateTrailingSpaces trancates trailing spaces for CHAR[(M)] column.
// fix: https://github.com/pingcap/tidb/issues/3660
func truncateTrailingSpaces(v *types.Datum) {
	trace_util_0.Count(_column_00000, 22)
	if v.Kind() == types.KindNull {
		trace_util_0.Count(_column_00000, 25)
		return
	}
	trace_util_0.Count(_column_00000, 23)
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		trace_util_0.Count(_column_00000, 26)
		length--
	}
	trace_util_0.Count(_column_00000, 24)
	b = b[:length]
	str := string(hack.String(b))
	v.SetString(str)
}

// CastValues casts values based on columns type.
func CastValues(ctx sessionctx.Context, rec []types.Datum, cols []*Column) (err error) {
	trace_util_0.Count(_column_00000, 27)
	sc := ctx.GetSessionVars().StmtCtx
	for _, c := range cols {
		trace_util_0.Count(_column_00000, 29)
		var converted types.Datum
		converted, err = CastValue(ctx, rec[c.Offset], c.ToInfo())
		if err != nil {
			trace_util_0.Count(_column_00000, 31)
			if sc.DupKeyAsWarning {
				trace_util_0.Count(_column_00000, 32)
				sc.AppendWarning(err)
				logutil.Logger(context.Background()).Warn("CastValues failed", zap.Error(err))
			} else {
				trace_util_0.Count(_column_00000, 33)
				{
					return err
				}
			}
		}
		trace_util_0.Count(_column_00000, 30)
		rec[c.Offset] = converted
	}
	trace_util_0.Count(_column_00000, 28)
	return nil
}

func handleWrongUtf8Value(ctx sessionctx.Context, col *model.ColumnInfo, casted *types.Datum, str string, i int) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 34)
	sc := ctx.GetSessionVars().StmtCtx
	err := ErrTruncateWrongValue.FastGen("incorrect utf8 value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	logutil.Logger(context.Background()).Error("incorrect UTF-8 value", zap.Uint64("conn", ctx.GetSessionVars().ConnectionID), zap.Error(err))
	// Truncate to valid utf8 string.
	truncateVal := types.NewStringDatum(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

// CastValue casts a value based on column type.
func CastValue(ctx sessionctx.Context, val types.Datum, col *model.ColumnInfo) (casted types.Datum, err error) {
	trace_util_0.Count(_column_00000, 35)
	sc := ctx.GetSessionVars().StmtCtx
	casted, err = val.ConvertTo(sc, &col.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	err = sc.HandleTruncate(err)
	if err != nil {
		trace_util_0.Count(_column_00000, 41)
		return casted, err
	}

	trace_util_0.Count(_column_00000, 36)
	if col.Tp == mysql.TypeString && !types.IsBinaryStr(&col.FieldType) {
		trace_util_0.Count(_column_00000, 42)
		truncateTrailingSpaces(&casted)
	}

	trace_util_0.Count(_column_00000, 37)
	if ctx.GetSessionVars().SkipUTF8Check {
		trace_util_0.Count(_column_00000, 43)
		return casted, nil
	}
	trace_util_0.Count(_column_00000, 38)
	if !mysql.IsUTF8Charset(col.Charset) {
		trace_util_0.Count(_column_00000, 44)
		return casted, nil
	}
	trace_util_0.Count(_column_00000, 39)
	str := casted.GetString()
	utf8Charset := col.Charset == mysql.UTF8Charset
	doMB4CharCheck := utf8Charset && config.GetGlobalConfig().CheckMb4ValueInUTF8
	for i, w := 0, 0; i < len(str); i += w {
		trace_util_0.Count(_column_00000, 45)
		runeValue, width := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError {
			trace_util_0.Count(_column_00000, 47)
			if strings.HasPrefix(str[i:], string(utf8.RuneError)) {
				trace_util_0.Count(_column_00000, 49)
				w = width
				continue
			}
			trace_util_0.Count(_column_00000, 48)
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		} else {
			trace_util_0.Count(_column_00000, 50)
			if width > 3 && doMB4CharCheck {
				trace_util_0.Count(_column_00000, 51)
				// Handle non-BMP characters.
				casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
				break
			}
		}
		trace_util_0.Count(_column_00000, 46)
		w = width
	}

	trace_util_0.Count(_column_00000, 40)
	return casted, err
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	Charset interface{}
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	Collation    interface{}
	Null         string
	Key          string
	DefaultValue interface{}
	Extra        string
	Privileges   string
	Comment      string
}

const defaultPrivileges = "select,insert,update,references"

// GetTypeDesc gets the description for column type.
func (c *Column) GetTypeDesc() string {
	trace_util_0.Count(_column_00000, 52)
	desc := c.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(c.Flag) && c.Tp != mysql.TypeBit && c.Tp != mysql.TypeYear {
		trace_util_0.Count(_column_00000, 55)
		desc += " unsigned"
	}
	trace_util_0.Count(_column_00000, 53)
	if mysql.HasZerofillFlag(c.Flag) && c.Tp != mysql.TypeYear {
		trace_util_0.Count(_column_00000, 56)
		desc += " zerofill"
	}
	trace_util_0.Count(_column_00000, 54)
	return desc
}

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(col *Column) *ColDesc {
	trace_util_0.Count(_column_00000, 57)
	// TODO: if we have no primary key and a unique index which's columns are all not null
	// we will set these columns' flag as PriKeyFlag
	// see https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
	// create table
	name := col.Name
	nullFlag := "YES"
	if mysql.HasNotNullFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 63)
		nullFlag = "NO"
	}
	trace_util_0.Count(_column_00000, 58)
	keyFlag := ""
	if mysql.HasPriKeyFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 64)
		keyFlag = "PRI"
	} else {
		trace_util_0.Count(_column_00000, 65)
		if mysql.HasUniKeyFlag(col.Flag) {
			trace_util_0.Count(_column_00000, 66)
			keyFlag = "UNI"
		} else {
			trace_util_0.Count(_column_00000, 67)
			if mysql.HasMultipleKeyFlag(col.Flag) {
				trace_util_0.Count(_column_00000, 68)
				keyFlag = "MUL"
			}
		}
	}
	trace_util_0.Count(_column_00000, 59)
	var defaultValue interface{}
	if !mysql.HasNoDefaultValueFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 69)
		defaultValue = col.GetDefaultValue()
	}

	trace_util_0.Count(_column_00000, 60)
	extra := ""
	if mysql.HasAutoIncrementFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 70)
		extra = "auto_increment"
	} else {
		trace_util_0.Count(_column_00000, 71)
		if mysql.HasOnUpdateNowFlag(col.Flag) {
			trace_util_0.Count(_column_00000, 72)
			//in order to match the rules of mysql 8.0.16 version
			//see https://github.com/pingcap/tidb/issues/10337
			extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
		} else {
			trace_util_0.Count(_column_00000, 73)
			if col.IsGenerated() {
				trace_util_0.Count(_column_00000, 74)
				if col.GeneratedStored {
					trace_util_0.Count(_column_00000, 75)
					extra = "STORED GENERATED"
				} else {
					trace_util_0.Count(_column_00000, 76)
					{
						extra = "VIRTUAL GENERATED"
					}
				}
			}
		}
	}

	trace_util_0.Count(_column_00000, 61)
	desc := &ColDesc{
		Field:        name.O,
		Type:         col.GetTypeDesc(),
		Charset:      col.Charset,
		Collation:    col.Collate,
		Null:         nullFlag,
		Key:          keyFlag,
		DefaultValue: defaultValue,
		Extra:        extra,
		Privileges:   defaultPrivileges,
		Comment:      col.Comment,
	}
	if !field_types.HasCharset(&col.ColumnInfo.FieldType) {
		trace_util_0.Count(_column_00000, 77)
		desc.Charset = nil
		desc.Collation = nil
	}
	trace_util_0.Count(_column_00000, 62)
	return desc
}

// ColDescFieldNames returns the fields name in result set for desc and show columns.
func ColDescFieldNames(full bool) []string {
	trace_util_0.Count(_column_00000, 78)
	if full {
		trace_util_0.Count(_column_00000, 80)
		return []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	}
	trace_util_0.Count(_column_00000, 79)
	return []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
}

// CheckOnce checks if there are duplicated column names in cols.
func CheckOnce(cols []*Column) error {
	trace_util_0.Count(_column_00000, 81)
	m := map[string]struct{}{}
	for _, col := range cols {
		trace_util_0.Count(_column_00000, 83)
		name := col.Name
		_, ok := m[name.L]
		if ok {
			trace_util_0.Count(_column_00000, 85)
			return errDuplicateColumn.GenWithStack("column specified twice - %s", name)
		}

		trace_util_0.Count(_column_00000, 84)
		m[name.L] = struct{}{}
	}

	trace_util_0.Count(_column_00000, 82)
	return nil
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
func (c *Column) CheckNotNull(data types.Datum) error {
	trace_util_0.Count(_column_00000, 86)
	if (mysql.HasNotNullFlag(c.Flag) || mysql.HasPreventNullInsertFlag(c.Flag)) && data.IsNull() {
		trace_util_0.Count(_column_00000, 88)
		return ErrColumnCantNull.GenWithStackByArgs(c.Name)
	}
	trace_util_0.Count(_column_00000, 87)
	return nil
}

// HandleBadNull handles the bad null error.
// If BadNullAsWarning is true, it will append the error as a warning, else return the error.
func (c *Column) HandleBadNull(d types.Datum, sc *stmtctx.StatementContext) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 89)
	if err := c.CheckNotNull(d); err != nil {
		trace_util_0.Count(_column_00000, 91)
		if sc.BadNullAsWarning {
			trace_util_0.Count(_column_00000, 93)
			sc.AppendWarning(err)
			return GetZeroValue(c.ToInfo()), nil
		}
		trace_util_0.Count(_column_00000, 92)
		return types.Datum{}, err
	}
	trace_util_0.Count(_column_00000, 90)
	return d, nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	trace_util_0.Count(_column_00000, 94)
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// CheckNotNull checks if row has nil value set to a column with NotNull flag set.
func CheckNotNull(cols []*Column, row []types.Datum) error {
	trace_util_0.Count(_column_00000, 95)
	for _, c := range cols {
		trace_util_0.Count(_column_00000, 97)
		if err := c.CheckNotNull(row[c.Offset]); err != nil {
			trace_util_0.Count(_column_00000, 98)
			return err
		}
	}
	trace_util_0.Count(_column_00000, 96)
	return nil
}

// GetColOriginDefaultValue gets default value of the column from original default value.
func GetColOriginDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 99)
	return getColDefaultValue(ctx, col, col.OriginDefaultValue)
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 100)
	return getColDefaultValue(ctx, col, col.GetDefaultValue())
}

func getColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultVal interface{}) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 101)
	if defaultVal == nil {
		trace_util_0.Count(_column_00000, 107)
		return getColDefaultValueFromNil(ctx, col)
	}

	trace_util_0.Count(_column_00000, 102)
	if col.Tp != mysql.TypeTimestamp && col.Tp != mysql.TypeDatetime {
		trace_util_0.Count(_column_00000, 108)
		value, err := CastValue(ctx, types.NewDatum(defaultVal), col)
		if err != nil {
			trace_util_0.Count(_column_00000, 110)
			return types.Datum{}, err
		}
		trace_util_0.Count(_column_00000, 109)
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	trace_util_0.Count(_column_00000, 103)
	sc := ctx.GetSessionVars().StmtCtx
	var needChangeTimeZone bool
	// If the column's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if col.Tp == mysql.TypeTimestamp {
		trace_util_0.Count(_column_00000, 111)
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && strings.ToUpper(vv) != strings.ToUpper(ast.CurrentTimestamp) {
			trace_util_0.Count(_column_00000, 112)
			needChangeTimeZone = true
			originalTZ := sc.TimeZone
			// For col.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			sc.TimeZone = timeutil.SystemLocation()
			if col.Version >= model.ColumnInfoVersion1 {
				trace_util_0.Count(_column_00000, 114)
				sc.TimeZone = time.UTC
			}
			trace_util_0.Count(_column_00000, 113)
			defer func() { trace_util_0.Count(_column_00000, 115); sc.TimeZone = originalTZ }()
		}
	}
	trace_util_0.Count(_column_00000, 104)
	value, err := expression.GetTimeValue(ctx, defaultVal, col.Tp, col.Decimal)
	if err != nil {
		trace_util_0.Count(_column_00000, 116)
		return types.Datum{}, errGetDefaultFailed.GenWithStack("Field '%s' get default value fail - %s",
			col.Name, err)
	}
	// If the column's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current session time zone.
	trace_util_0.Count(_column_00000, 105)
	if needChangeTimeZone {
		trace_util_0.Count(_column_00000, 117)
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(sc.TimeZone, ctx.GetSessionVars().Location())
		if err != nil {
			trace_util_0.Count(_column_00000, 119)
			return value, err
		}
		trace_util_0.Count(_column_00000, 118)
		value.SetMysqlTime(t)
	}
	trace_util_0.Count(_column_00000, 106)
	return value, nil
}

func getColDefaultValueFromNil(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	trace_util_0.Count(_column_00000, 120)
	if !mysql.HasNotNullFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 127)
		return types.Datum{}, nil
	}
	trace_util_0.Count(_column_00000, 121)
	if col.Tp == mysql.TypeEnum {
		trace_util_0.Count(_column_00000, 128)
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(col.FieldType.Elems, 1)
		if err != nil {
			trace_util_0.Count(_column_00000, 130)
			return types.Datum{}, err
		}
		trace_util_0.Count(_column_00000, 129)
		return types.NewMysqlEnumDatum(defEnum), nil
	}
	trace_util_0.Count(_column_00000, 122)
	if mysql.HasAutoIncrementFlag(col.Flag) {
		trace_util_0.Count(_column_00000, 131)
		// Auto increment column doesn't has default value and we should not return error.
		return GetZeroValue(col), nil
	}
	trace_util_0.Count(_column_00000, 123)
	if col.IsGenerated() {
		trace_util_0.Count(_column_00000, 132)
		return types.Datum{}, nil
	}
	trace_util_0.Count(_column_00000, 124)
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	if sc.BadNullAsWarning {
		trace_util_0.Count(_column_00000, 133)
		sc.AppendWarning(ErrColumnCantNull.GenWithStackByArgs(col.Name))
		return GetZeroValue(col), nil
	}
	trace_util_0.Count(_column_00000, 125)
	if !vars.StrictSQLMode {
		trace_util_0.Count(_column_00000, 134)
		sc.AppendWarning(ErrNoDefaultValue.GenWithStackByArgs(col.Name))
		return GetZeroValue(col), nil
	}
	trace_util_0.Count(_column_00000, 126)
	return types.Datum{}, ErrNoDefaultValue.GenWithStackByArgs(col.Name)
}

// GetZeroValue gets zero value for given column type.
func GetZeroValue(col *model.ColumnInfo) types.Datum {
	trace_util_0.Count(_column_00000, 135)
	var d types.Datum
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		trace_util_0.Count(_column_00000, 137)
		if mysql.HasUnsignedFlag(col.Flag) {
			trace_util_0.Count(_column_00000, 152)
			d.SetUint64(0)
		} else {
			trace_util_0.Count(_column_00000, 153)
			{
				d.SetInt64(0)
			}
		}
	case mysql.TypeFloat:
		trace_util_0.Count(_column_00000, 138)
		d.SetFloat32(0)
	case mysql.TypeDouble:
		trace_util_0.Count(_column_00000, 139)
		d.SetFloat64(0)
	case mysql.TypeNewDecimal:
		trace_util_0.Count(_column_00000, 140)
		d.SetMysqlDecimal(new(types.MyDecimal))
	case mysql.TypeString:
		trace_util_0.Count(_column_00000, 141)
		if col.Flen > 0 && col.Charset == charset.CharsetBin {
			trace_util_0.Count(_column_00000, 154)
			d.SetBytes(make([]byte, col.Flen))
		} else {
			trace_util_0.Count(_column_00000, 155)
			{
				d.SetString("")
			}
		}
	case mysql.TypeVarString, mysql.TypeVarchar:
		trace_util_0.Count(_column_00000, 142)
		d.SetString("")
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_column_00000, 143)
		d.SetBytes([]byte{})
	case mysql.TypeDuration:
		trace_util_0.Count(_column_00000, 144)
		d.SetMysqlDuration(types.ZeroDuration)
	case mysql.TypeDate:
		trace_util_0.Count(_column_00000, 145)
		d.SetMysqlTime(types.ZeroDate)
	case mysql.TypeTimestamp:
		trace_util_0.Count(_column_00000, 146)
		d.SetMysqlTime(types.ZeroTimestamp)
	case mysql.TypeDatetime:
		trace_util_0.Count(_column_00000, 147)
		d.SetMysqlTime(types.ZeroDatetime)
	case mysql.TypeBit:
		trace_util_0.Count(_column_00000, 148)
		d.SetMysqlBit(types.ZeroBinaryLiteral)
	case mysql.TypeSet:
		trace_util_0.Count(_column_00000, 149)
		d.SetMysqlSet(types.Set{})
	case mysql.TypeEnum:
		trace_util_0.Count(_column_00000, 150)
		d.SetMysqlEnum(types.Enum{})
	case mysql.TypeJSON:
		trace_util_0.Count(_column_00000, 151)
		d.SetMysqlJSON(json.CreateBinary(nil))
	}
	trace_util_0.Count(_column_00000, 136)
	return d
}

var _column_00000 = "table/column.go"
