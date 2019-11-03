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

package infoschema

import (
	"sort"
	"sync/atomic"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
)

var (
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(codeDBDropExists, "Can't drop database '%s'; database doesn't exist")
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(codeDatabaseNotExists, "Unknown database '%s'")
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(codeTableNotExists, "Table '%s.%s' doesn't exist")
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(codeColumnNotExists, "Unknown column '%s' in '%s'")
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(codeWrongFkDef, "Incorrect foreign key definition for '%s': Key reference and table reference don't match")
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(codeCannotAddForeign, "Cannot add foreign key constraint")
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(codeForeignKeyNotExists, "Can't DROP '%s'; check that column/key exists")
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(codeDatabaseExists, "Can't create database '%s'; database exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(codeTableExists, "Table '%s' already exists")
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(codeBadTable, "Unknown table '%s'")
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.New(codeBadUser, "User %s does not exist.")
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(codeColumnExists, "Duplicate column name '%s'")
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(codeIndexExists, "Duplicate Index")
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.New(codeKeyNameDuplicate, "Duplicate key name '%s'")
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.New(codeKeyNotExists, "Key '%s' doesn't exist in table '%s'")
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(codeMultiplePriKey, "Multiple primary key defined")
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(codeTooManyKeyParts, "Too many key parts specified; max %d parts allowed")
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
// TODO: add more methods to retrieve tables and columns.
type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	SchemaExists(schema model.CIStr) bool
	TableByName(schema, table model.CIStr) (table.Table, error)
	TableExists(schema, table model.CIStr) bool
	SchemaByID(id int64) (*model.DBInfo, bool)
	SchemaByTable(tableInfo *model.TableInfo) (*model.DBInfo, bool)
	TableByID(id int64) (table.Table, bool)
	AllocByID(id int64) (autoid.Allocator, bool)
	AllSchemaNames() []string
	AllSchemas() []*model.DBInfo
	Clone() (result []*model.DBInfo)
	SchemaTables(schema model.CIStr) []table.Table
	SchemaMetaVersion() int64
	// TableIsView indicates whether the schema.table is a view.
	TableIsView(schema, table model.CIStr) bool
}

// Information Schema Name.
const (
	Name = "INFORMATION_SCHEMA"
)

type sortedTables []table.Table

func (s sortedTables) Len() int {
	trace_util_0.Count(_infoschema_00000, 0)
	return len(s)
}

func (s sortedTables) Swap(i, j int) {
	trace_util_0.Count(_infoschema_00000, 1)
	s[i], s[j] = s[j], s[i]
}

func (s sortedTables) Less(i, j int) bool {
	trace_util_0.Count(_infoschema_00000, 2)
	return s[i].Meta().ID < s[j].Meta().ID
}

func (s sortedTables) searchTable(id int64) int {
	trace_util_0.Count(_infoschema_00000, 3)
	idx := sort.Search(len(s), func(i int) bool {
		trace_util_0.Count(_infoschema_00000, 6)
		return s[i].Meta().ID >= id
	})
	trace_util_0.Count(_infoschema_00000, 4)
	if idx == len(s) || s[idx].Meta().ID != id {
		trace_util_0.Count(_infoschema_00000, 7)
		return -1
	}
	trace_util_0.Count(_infoschema_00000, 5)
	return idx
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]table.Table
}

const bucketCount = 512

type infoSchema struct {
	schemaMap map[string]*schemaTables

	// sortedTablesBuckets is a slice of sortedTables, a table's bucket index is (tableID % bucketCount).
	sortedTablesBuckets []sortedTables

	// schemaMetaVersion is the version of schema, and we should check version when change schema.
	schemaMetaVersion int64
}

// MockInfoSchema only serves for test.
func MockInfoSchema(tbList []*model.TableInfo) InfoSchema {
	trace_util_0.Count(_infoschema_00000, 8)
	result := &infoSchema{}
	result.schemaMap = make(map[string]*schemaTables)
	result.sortedTablesBuckets = make([]sortedTables, bucketCount)
	dbInfo := &model.DBInfo{ID: 0, Name: model.NewCIStr("test"), Tables: tbList}
	tableNames := &schemaTables{
		dbInfo: dbInfo,
		tables: make(map[string]table.Table),
	}
	result.schemaMap["test"] = tableNames
	for _, tb := range tbList {
		trace_util_0.Count(_infoschema_00000, 11)
		tbl := table.MockTableFromMeta(tb)
		tableNames.tables[tb.Name.L] = tbl
		bucketIdx := tableBucketIdx(tb.ID)
		result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
	}
	trace_util_0.Count(_infoschema_00000, 9)
	for i := range result.sortedTablesBuckets {
		trace_util_0.Count(_infoschema_00000, 12)
		sort.Sort(result.sortedTablesBuckets[i])
	}
	trace_util_0.Count(_infoschema_00000, 10)
	return result
}

var _ InfoSchema = (*infoSchema)(nil)

func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	trace_util_0.Count(_infoschema_00000, 13)
	tableNames, ok := is.schemaMap[schema.L]
	if !ok {
		trace_util_0.Count(_infoschema_00000, 15)
		return
	}
	trace_util_0.Count(_infoschema_00000, 14)
	return tableNames.dbInfo, true
}

func (is *infoSchema) SchemaMetaVersion() int64 {
	trace_util_0.Count(_infoschema_00000, 16)
	return is.schemaMetaVersion
}

func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
	trace_util_0.Count(_infoschema_00000, 17)
	_, ok := is.schemaMap[schema.L]
	return ok
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	trace_util_0.Count(_infoschema_00000, 18)
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		trace_util_0.Count(_infoschema_00000, 20)
		if t, ok = tbNames.tables[table.L]; ok {
			trace_util_0.Count(_infoschema_00000, 21)
			return
		}
	}
	trace_util_0.Count(_infoschema_00000, 19)
	return nil, ErrTableNotExists.GenWithStackByArgs(schema, table)
}

func (is *infoSchema) TableIsView(schema, table model.CIStr) bool {
	trace_util_0.Count(_infoschema_00000, 22)
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		trace_util_0.Count(_infoschema_00000, 24)
		if t, ok := tbNames.tables[table.L]; ok {
			trace_util_0.Count(_infoschema_00000, 25)
			return t.Meta().IsView()
		}
	}
	trace_util_0.Count(_infoschema_00000, 23)
	return false
}

func (is *infoSchema) TableExists(schema, table model.CIStr) bool {
	trace_util_0.Count(_infoschema_00000, 26)
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		trace_util_0.Count(_infoschema_00000, 28)
		if _, ok = tbNames.tables[table.L]; ok {
			trace_util_0.Count(_infoschema_00000, 29)
			return true
		}
	}
	trace_util_0.Count(_infoschema_00000, 27)
	return false
}

func (is *infoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	trace_util_0.Count(_infoschema_00000, 30)
	for _, v := range is.schemaMap {
		trace_util_0.Count(_infoschema_00000, 32)
		if v.dbInfo.ID == id {
			trace_util_0.Count(_infoschema_00000, 33)
			return v.dbInfo, true
		}
	}
	trace_util_0.Count(_infoschema_00000, 31)
	return nil, false
}

func (is *infoSchema) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	trace_util_0.Count(_infoschema_00000, 34)
	if tableInfo == nil {
		trace_util_0.Count(_infoschema_00000, 37)
		return nil, false
	}
	trace_util_0.Count(_infoschema_00000, 35)
	for _, v := range is.schemaMap {
		trace_util_0.Count(_infoschema_00000, 38)
		if tbl, ok := v.tables[tableInfo.Name.L]; ok {
			trace_util_0.Count(_infoschema_00000, 39)
			if tbl.Meta().ID == tableInfo.ID {
				trace_util_0.Count(_infoschema_00000, 40)
				return v.dbInfo, true
			}
		}
	}
	trace_util_0.Count(_infoschema_00000, 36)
	return nil, false
}

func (is *infoSchema) TableByID(id int64) (val table.Table, ok bool) {
	trace_util_0.Count(_infoschema_00000, 41)
	slice := is.sortedTablesBuckets[tableBucketIdx(id)]
	idx := slice.searchTable(id)
	if idx == -1 {
		trace_util_0.Count(_infoschema_00000, 43)
		return nil, false
	}
	trace_util_0.Count(_infoschema_00000, 42)
	return slice[idx], true
}

func (is *infoSchema) AllocByID(id int64) (autoid.Allocator, bool) {
	trace_util_0.Count(_infoschema_00000, 44)
	tbl, ok := is.TableByID(id)
	if !ok {
		trace_util_0.Count(_infoschema_00000, 46)
		return nil, false
	}
	trace_util_0.Count(_infoschema_00000, 45)
	return tbl.Allocator(nil), true
}

func (is *infoSchema) AllSchemaNames() (names []string) {
	trace_util_0.Count(_infoschema_00000, 47)
	for _, v := range is.schemaMap {
		trace_util_0.Count(_infoschema_00000, 49)
		names = append(names, v.dbInfo.Name.O)
	}
	trace_util_0.Count(_infoschema_00000, 48)
	return
}

func (is *infoSchema) AllSchemas() (schemas []*model.DBInfo) {
	trace_util_0.Count(_infoschema_00000, 50)
	for _, v := range is.schemaMap {
		trace_util_0.Count(_infoschema_00000, 52)
		schemas = append(schemas, v.dbInfo)
	}
	trace_util_0.Count(_infoschema_00000, 51)
	return
}

func (is *infoSchema) SchemaTables(schema model.CIStr) (tables []table.Table) {
	trace_util_0.Count(_infoschema_00000, 53)
	schemaTables, ok := is.schemaMap[schema.L]
	if !ok {
		trace_util_0.Count(_infoschema_00000, 56)
		return
	}
	trace_util_0.Count(_infoschema_00000, 54)
	for _, tbl := range schemaTables.tables {
		trace_util_0.Count(_infoschema_00000, 57)
		tables = append(tables, tbl)
	}
	trace_util_0.Count(_infoschema_00000, 55)
	return
}

func (is *infoSchema) Clone() (result []*model.DBInfo) {
	trace_util_0.Count(_infoschema_00000, 58)
	for _, v := range is.schemaMap {
		trace_util_0.Count(_infoschema_00000, 60)
		result = append(result, v.dbInfo.Clone())
	}
	trace_util_0.Count(_infoschema_00000, 59)
	return
}

// Handle handles information schema, including getting and setting.
type Handle struct {
	value atomic.Value
	store kv.Storage
}

// NewHandle creates a new Handle.
func NewHandle(store kv.Storage) *Handle {
	trace_util_0.Count(_infoschema_00000, 61)
	h := &Handle{
		store: store,
	}
	return h
}

// Get gets information schema from Handle.
func (h *Handle) Get() InfoSchema {
	trace_util_0.Count(_infoschema_00000, 62)
	v := h.value.Load()
	schema, _ := v.(InfoSchema)
	return schema
}

// EmptyClone creates a new Handle with the same store and memSchema, but the value is not set.
func (h *Handle) EmptyClone() *Handle {
	trace_util_0.Count(_infoschema_00000, 63)
	newHandle := &Handle{
		store: h.store,
	}
	return newHandle
}

// Schema error codes.
const (
	codeDBDropExists      terror.ErrCode = 1008
	codeDatabaseNotExists                = 1049
	codeTableNotExists                   = 1146
	codeColumnNotExists                  = 1054

	codeCannotAddForeign    = 1215
	codeForeignKeyNotExists = 1091
	codeWrongFkDef          = 1239

	codeDatabaseExists   = 1007
	codeTableExists      = 1050
	codeBadTable         = 1051
	codeBadUser          = 3162
	codeColumnExists     = 1060
	codeIndexExists      = 1831
	codeMultiplePriKey   = 1068
	codeTooManyKeyParts  = 1070
	codeKeyNameDuplicate = 1061
	codeKeyNotExists     = 1176
)

func init() {
	trace_util_0.Count(_infoschema_00000, 64)
	schemaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDBDropExists:        mysql.ErrDBDropExists,
		codeDatabaseNotExists:   mysql.ErrBadDB,
		codeTableNotExists:      mysql.ErrNoSuchTable,
		codeColumnNotExists:     mysql.ErrBadField,
		codeCannotAddForeign:    mysql.ErrCannotAddForeign,
		codeWrongFkDef:          mysql.ErrWrongFkDef,
		codeForeignKeyNotExists: mysql.ErrCantDropFieldOrKey,
		codeDatabaseExists:      mysql.ErrDBCreateExists,
		codeTableExists:         mysql.ErrTableExists,
		codeBadTable:            mysql.ErrBadTable,
		codeBadUser:             mysql.ErrBadUser,
		codeColumnExists:        mysql.ErrDupFieldName,
		codeIndexExists:         mysql.ErrDupIndex,
		codeMultiplePriKey:      mysql.ErrMultiplePriKey,
		codeTooManyKeyParts:     mysql.ErrTooManyKeyParts,
		codeKeyNameDuplicate:    mysql.ErrDupKeyName,
		codeKeyNotExists:        mysql.ErrKeyDoesNotExist,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSchema] = schemaMySQLErrCodes
	initInfoSchemaDB()
}

var (
	infoSchemaDB *model.DBInfo
)

func initInfoSchemaDB() {
	trace_util_0.Count(_infoschema_00000, 65)
	dbID := autoid.GenLocalSchemaID()
	infoSchemaTables := make([]*model.TableInfo, 0, len(tableNameToColumns))
	for name, cols := range tableNameToColumns {
		trace_util_0.Count(_infoschema_00000, 67)
		tableInfo := buildTableMeta(name, cols)
		infoSchemaTables = append(infoSchemaTables, tableInfo)
		tableInfo.ID = autoid.GenLocalSchemaID()
		for _, c := range tableInfo.Columns {
			trace_util_0.Count(_infoschema_00000, 68)
			c.ID = autoid.GenLocalSchemaID()
		}
	}
	trace_util_0.Count(_infoschema_00000, 66)
	infoSchemaDB = &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(Name),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  infoSchemaTables,
	}
}

// IsMemoryDB checks if the db is in memory.
func IsMemoryDB(dbName string) bool {
	trace_util_0.Count(_infoschema_00000, 69)
	if dbName == "information_schema" {
		trace_util_0.Count(_infoschema_00000, 72)
		return true
	}
	trace_util_0.Count(_infoschema_00000, 70)
	for _, driver := range drivers {
		trace_util_0.Count(_infoschema_00000, 73)
		if driver.DBInfo.Name.L == dbName {
			trace_util_0.Count(_infoschema_00000, 74)
			return true
		}
	}
	trace_util_0.Count(_infoschema_00000, 71)
	return false
}

var _infoschema_00000 = "infoschema/infoschema.go"
