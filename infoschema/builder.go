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
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/trace_util_0"
)

// Builder builds a new InfoSchema.
type Builder struct {
	is     *infoSchema
	handle *Handle
}

// ApplyDiff applies SchemaDiff to the new InfoSchema.
// Return the detail updated table IDs that are produced from SchemaDiff and an error.
func (b *Builder) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	trace_util_0.Count(_builder_00000, 0)
	b.is.schemaMetaVersion = diff.Version
	if diff.Type == model.ActionCreateSchema {
		trace_util_0.Count(_builder_00000, 6)
		return nil, b.applyCreateSchema(m, diff)
	} else {
		trace_util_0.Count(_builder_00000, 7)
		if diff.Type == model.ActionDropSchema {
			trace_util_0.Count(_builder_00000, 8)
			tblIDs := b.applyDropSchema(diff.SchemaID)
			return tblIDs, nil
		} else {
			trace_util_0.Count(_builder_00000, 9)
			if diff.Type == model.ActionModifySchemaCharsetAndCollate {
				trace_util_0.Count(_builder_00000, 10)
				return nil, b.applyModifySchemaCharsetAndCollate(m, diff)
			}
		}
	}

	trace_util_0.Count(_builder_00000, 1)
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		trace_util_0.Count(_builder_00000, 11)
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	trace_util_0.Count(_builder_00000, 2)
	var oldTableID, newTableID int64
	tblIDs := make([]int64, 0, 2)
	switch diff.Type {
	case model.ActionCreateTable, model.ActionRecoverTable:
		trace_util_0.Count(_builder_00000, 12)
		newTableID = diff.TableID
		tblIDs = append(tblIDs, newTableID)
	case model.ActionDropTable, model.ActionDropView:
		trace_util_0.Count(_builder_00000, 13)
		oldTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID)
	case model.ActionTruncateTable:
		trace_util_0.Count(_builder_00000, 14)
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID, newTableID)
	default:
		trace_util_0.Count(_builder_00000, 15)
		oldTableID = diff.TableID
		newTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID)
	}
	trace_util_0.Count(_builder_00000, 3)
	dbInfo := b.copySchemaTables(roDBInfo.Name.L)
	b.copySortedTables(oldTableID, newTableID)

	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var alloc autoid.Allocator
	if tableIDIsValid(oldTableID) {
		trace_util_0.Count(_builder_00000, 16)
		if oldTableID == newTableID && diff.Type != model.ActionRenameTable && diff.Type != model.ActionRebaseAutoID {
			trace_util_0.Count(_builder_00000, 18)
			alloc, _ = b.is.AllocByID(oldTableID)
		}
		trace_util_0.Count(_builder_00000, 17)
		if diff.Type == model.ActionRenameTable && diff.OldSchemaID != diff.SchemaID {
			trace_util_0.Count(_builder_00000, 19)
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				trace_util_0.Count(_builder_00000, 21)
				return nil, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			trace_util_0.Count(_builder_00000, 20)
			oldDBInfo := b.copySchemaTables(oldRoDBInfo.Name.L)
			b.applyDropTable(oldDBInfo, oldTableID)
		} else {
			trace_util_0.Count(_builder_00000, 22)
			{
				b.applyDropTable(dbInfo, oldTableID)
			}
		}
	}
	trace_util_0.Count(_builder_00000, 4)
	if tableIDIsValid(newTableID) {
		trace_util_0.Count(_builder_00000, 23)
		// All types except DropTableOrView.
		err := b.applyCreateTable(m, dbInfo, newTableID, alloc)
		if err != nil {
			trace_util_0.Count(_builder_00000, 24)
			return nil, errors.Trace(err)
		}
	}
	trace_util_0.Count(_builder_00000, 5)
	return tblIDs, nil
}

// copySortedTables copies sortedTables for old table and new table for later modification.
func (b *Builder) copySortedTables(oldTableID, newTableID int64) {
	trace_util_0.Count(_builder_00000, 25)
	if tableIDIsValid(oldTableID) {
		trace_util_0.Count(_builder_00000, 27)
		b.copySortedTablesBucket(tableBucketIdx(oldTableID))
	}
	trace_util_0.Count(_builder_00000, 26)
	if tableIDIsValid(newTableID) && newTableID != oldTableID {
		trace_util_0.Count(_builder_00000, 28)
		b.copySortedTablesBucket(tableBucketIdx(newTableID))
	}
}

func (b *Builder) applyCreateSchema(m *meta.Meta, diff *model.SchemaDiff) error {
	trace_util_0.Count(_builder_00000, 29)
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		trace_util_0.Count(_builder_00000, 32)
		return errors.Trace(err)
	}
	trace_util_0.Count(_builder_00000, 30)
	if di == nil {
		trace_util_0.Count(_builder_00000, 33)
		// When we apply an old schema diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	trace_util_0.Count(_builder_00000, 31)
	b.is.schemaMap[di.Name.L] = &schemaTables{dbInfo: di, tables: make(map[string]table.Table)}
	return nil
}

func (b *Builder) applyModifySchemaCharsetAndCollate(m *meta.Meta, diff *model.SchemaDiff) error {
	trace_util_0.Count(_builder_00000, 34)
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		trace_util_0.Count(_builder_00000, 37)
		return errors.Trace(err)
	}
	trace_util_0.Count(_builder_00000, 35)
	if di == nil {
		trace_util_0.Count(_builder_00000, 38)
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	trace_util_0.Count(_builder_00000, 36)
	newDbInfo := b.copySchemaTables(di.Name.O)
	newDbInfo.Charset = di.Charset
	newDbInfo.Collate = di.Collate
	return nil
}

func (b *Builder) applyDropSchema(schemaID int64) []int64 {
	trace_util_0.Count(_builder_00000, 39)
	di, ok := b.is.SchemaByID(schemaID)
	if !ok {
		trace_util_0.Count(_builder_00000, 44)
		return nil
	}
	trace_util_0.Count(_builder_00000, 40)
	delete(b.is.schemaMap, di.Name.L)

	// Copy the sortedTables that contain the table we are going to drop.
	bucketIdxMap := make(map[int]struct{})
	for _, tbl := range di.Tables {
		trace_util_0.Count(_builder_00000, 45)
		bucketIdxMap[tableBucketIdx(tbl.ID)] = struct{}{}
	}
	trace_util_0.Count(_builder_00000, 41)
	for bucketIdx := range bucketIdxMap {
		trace_util_0.Count(_builder_00000, 46)
		b.copySortedTablesBucket(bucketIdx)
	}

	trace_util_0.Count(_builder_00000, 42)
	ids := make([]int64, 0, len(di.Tables))
	di = di.Clone()
	for _, tbl := range di.Tables {
		trace_util_0.Count(_builder_00000, 47)
		b.applyDropTable(di, tbl.ID)
		// TODO: If the table ID doesn't exist.
		ids = append(ids, tbl.ID)
	}
	trace_util_0.Count(_builder_00000, 43)
	return ids
}

func (b *Builder) copySortedTablesBucket(bucketIdx int) {
	trace_util_0.Count(_builder_00000, 48)
	oldSortedTables := b.is.sortedTablesBuckets[bucketIdx]
	newSortedTables := make(sortedTables, len(oldSortedTables))
	copy(newSortedTables, oldSortedTables)
	b.is.sortedTablesBuckets[bucketIdx] = newSortedTables
}

func (b *Builder) applyCreateTable(m *meta.Meta, dbInfo *model.DBInfo, tableID int64, alloc autoid.Allocator) error {
	trace_util_0.Count(_builder_00000, 49)
	tblInfo, err := m.GetTable(dbInfo.ID, tableID)
	if err != nil {
		trace_util_0.Count(_builder_00000, 55)
		return errors.Trace(err)
	}
	trace_util_0.Count(_builder_00000, 50)
	if tblInfo == nil {
		trace_util_0.Count(_builder_00000, 56)
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}
	trace_util_0.Count(_builder_00000, 51)
	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	if alloc == nil {
		trace_util_0.Count(_builder_00000, 57)
		schemaID := dbInfo.ID
		alloc = autoid.NewAllocator(b.handle.store, tblInfo.GetDBID(schemaID), tblInfo.IsAutoIncColUnsigned())
	}
	trace_util_0.Count(_builder_00000, 52)
	tbl, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		trace_util_0.Count(_builder_00000, 58)
		return errors.Trace(err)
	}
	trace_util_0.Count(_builder_00000, 53)
	tableNames := b.is.schemaMap[dbInfo.Name.L]
	tableNames.tables[tblInfo.Name.L] = tbl
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	sortedTbls = append(sortedTbls, tbl)
	sort.Sort(sortedTbls)
	b.is.sortedTablesBuckets[bucketIdx] = sortedTbls

	newTbl, ok := b.is.TableByID(tableID)
	if ok {
		trace_util_0.Count(_builder_00000, 59)
		dbInfo.Tables = append(dbInfo.Tables, newTbl.Meta())
	}
	trace_util_0.Count(_builder_00000, 54)
	return nil
}

// ConvertCharsetCollateToLowerCaseIfNeed convert the charset / collation of table and its columns to lower case,
// if the table's version is prior to TableInfoVersion3.
func ConvertCharsetCollateToLowerCaseIfNeed(tbInfo *model.TableInfo) {
	trace_util_0.Count(_builder_00000, 60)
	if tbInfo.Version >= model.TableInfoVersion3 {
		trace_util_0.Count(_builder_00000, 62)
		return
	}
	trace_util_0.Count(_builder_00000, 61)
	tbInfo.Charset = strings.ToLower(tbInfo.Charset)
	tbInfo.Collate = strings.ToLower(tbInfo.Collate)
	for _, col := range tbInfo.Columns {
		trace_util_0.Count(_builder_00000, 63)
		col.Charset = strings.ToLower(col.Charset)
		col.Collate = strings.ToLower(col.Collate)
	}
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *model.TableInfo) {
	trace_util_0.Count(_builder_00000, 64)
	if tbInfo.Version >= model.TableInfoVersion2 || !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
		trace_util_0.Count(_builder_00000, 67)
		return
	}
	trace_util_0.Count(_builder_00000, 65)
	if tbInfo.Charset == charset.CharsetUTF8 {
		trace_util_0.Count(_builder_00000, 68)
		tbInfo.Charset = charset.CharsetUTF8MB4
		tbInfo.Collate = charset.CollationUTF8MB4
	}
	trace_util_0.Count(_builder_00000, 66)
	for _, col := range tbInfo.Columns {
		trace_util_0.Count(_builder_00000, 69)
		if col.Version < model.ColumnInfoVersion2 && col.Charset == charset.CharsetUTF8 {
			trace_util_0.Count(_builder_00000, 70)
			col.Charset = charset.CharsetUTF8MB4
			col.Collate = charset.CollationUTF8MB4
		}
	}
}

func (b *Builder) applyDropTable(dbInfo *model.DBInfo, tableID int64) {
	trace_util_0.Count(_builder_00000, 71)
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		trace_util_0.Count(_builder_00000, 74)
		return
	}
	trace_util_0.Count(_builder_00000, 72)
	if tableNames, ok := b.is.schemaMap[dbInfo.Name.L]; ok {
		trace_util_0.Count(_builder_00000, 75)
		delete(tableNames.tables, sortedTbls[idx].Meta().Name.L)
	}
	// Remove the table in sorted table slice.
	trace_util_0.Count(_builder_00000, 73)
	b.is.sortedTablesBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// The old DBInfo still holds a reference to old table info, we need to remove it.
	for i, tblInfo := range dbInfo.Tables {
		trace_util_0.Count(_builder_00000, 76)
		if tblInfo.ID == tableID {
			trace_util_0.Count(_builder_00000, 77)
			if i == len(dbInfo.Tables)-1 {
				trace_util_0.Count(_builder_00000, 79)
				dbInfo.Tables = dbInfo.Tables[:i]
			} else {
				trace_util_0.Count(_builder_00000, 80)
				{
					dbInfo.Tables = append(dbInfo.Tables[:i], dbInfo.Tables[i+1:]...)
				}
			}
			trace_util_0.Count(_builder_00000, 78)
			break
		}
	}
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema() *Builder {
	trace_util_0.Count(_builder_00000, 81)
	oldIS := b.handle.Get().(*infoSchema)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	copy(b.is.sortedTablesBuckets, oldIS.sortedTablesBuckets)
	return b
}

func (b *Builder) copySchemasMap(oldIS *infoSchema) {
	trace_util_0.Count(_builder_00000, 82)
	for k, v := range oldIS.schemaMap {
		trace_util_0.Count(_builder_00000, 83)
		b.is.schemaMap[k] = v
	}
}

// copySchemaTables creates a new schemaTables instance when a table in the database has changed.
// It also does modifications on the new one because old schemaTables must be read-only.
func (b *Builder) copySchemaTables(dbName string) *model.DBInfo {
	trace_util_0.Count(_builder_00000, 84)
	oldSchemaTables := b.is.schemaMap[dbName]
	newSchemaTables := &schemaTables{
		dbInfo: oldSchemaTables.dbInfo.Copy(),
		tables: make(map[string]table.Table, len(oldSchemaTables.tables)),
	}
	for k, v := range oldSchemaTables.tables {
		trace_util_0.Count(_builder_00000, 86)
		newSchemaTables.tables[k] = v
	}
	trace_util_0.Count(_builder_00000, 85)
	b.is.schemaMap[dbName] = newSchemaTables
	return newSchemaTables.dbInfo
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, schemaVersion int64) (*Builder, error) {
	trace_util_0.Count(_builder_00000, 87)
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, di := range dbInfos {
		trace_util_0.Count(_builder_00000, 91)
		err := b.createSchemaTablesForDB(di, tables.TableFromMeta)
		if err != nil {
			trace_util_0.Count(_builder_00000, 92)
			return nil, errors.Trace(err)
		}
	}

	// Initialize virtual tables.
	trace_util_0.Count(_builder_00000, 88)
	for _, driver := range drivers {
		trace_util_0.Count(_builder_00000, 93)
		err := b.createSchemaTablesForDB(driver.DBInfo, driver.TableFromMeta)
		if err != nil {
			trace_util_0.Count(_builder_00000, 94)
			return nil, errors.Trace(err)
		}
	}
	// TODO: Update INFORMATION_SCHEMA schema to use virtual table.
	trace_util_0.Count(_builder_00000, 89)
	b.createSchemaTablesForInfoSchemaDB()
	for _, v := range info.sortedTablesBuckets {
		trace_util_0.Count(_builder_00000, 95)
		sort.Sort(v)
	}
	trace_util_0.Count(_builder_00000, 90)
	return b, nil
}

type tableFromMetaFunc func(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error)

func (b *Builder) createSchemaTablesForDB(di *model.DBInfo, tableFromMeta tableFromMetaFunc) error {
	trace_util_0.Count(_builder_00000, 96)
	schTbls := &schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(di.Tables)),
	}
	b.is.schemaMap[di.Name.L] = schTbls
	for _, t := range di.Tables {
		trace_util_0.Count(_builder_00000, 98)
		schemaID := di.ID
		alloc := autoid.NewAllocator(b.handle.store, t.GetDBID(schemaID), t.IsAutoIncColUnsigned())
		var tbl table.Table
		tbl, err := tableFromMeta(alloc, t)
		if err != nil {
			trace_util_0.Count(_builder_00000, 100)
			return errors.Trace(err)
		}
		trace_util_0.Count(_builder_00000, 99)
		schTbls.tables[t.Name.L] = tbl
		sortedTbls := b.is.sortedTablesBuckets[tableBucketIdx(t.ID)]
		b.is.sortedTablesBuckets[tableBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}
	trace_util_0.Count(_builder_00000, 97)
	return nil
}

type virtualTableDriver struct {
	*model.DBInfo
	TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error)
}

var drivers []*virtualTableDriver

// RegisterVirtualTable register virtual tables to the builder.
func RegisterVirtualTable(dbInfo *model.DBInfo, tableFromMeta tableFromMetaFunc) {
	trace_util_0.Count(_builder_00000, 101)
	drivers = append(drivers, &virtualTableDriver{dbInfo, tableFromMeta})
}

func (b *Builder) createSchemaTablesForInfoSchemaDB() {
	trace_util_0.Count(_builder_00000, 102)
	infoSchemaSchemaTables := &schemaTables{
		dbInfo: infoSchemaDB,
		tables: make(map[string]table.Table, len(infoSchemaDB.Tables)),
	}
	b.is.schemaMap[infoSchemaDB.Name.L] = infoSchemaSchemaTables
	for _, t := range infoSchemaDB.Tables {
		trace_util_0.Count(_builder_00000, 103)
		tbl := createInfoSchemaTable(b.handle, t)
		infoSchemaSchemaTables.tables[t.Name.L] = tbl
		bucketIdx := tableBucketIdx(t.ID)
		b.is.sortedTablesBuckets[bucketIdx] = append(b.is.sortedTablesBuckets[bucketIdx], tbl)
	}
}

// Build sets new InfoSchema to the handle in the Builder.
func (b *Builder) Build() {
	trace_util_0.Count(_builder_00000, 104)
	b.handle.value.Store(b.is)
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(handle *Handle) *Builder {
	trace_util_0.Count(_builder_00000, 105)
	b := new(Builder)
	b.handle = handle
	b.is = &infoSchema{
		schemaMap:           map[string]*schemaTables{},
		sortedTablesBuckets: make([]sortedTables, bucketCount),
	}
	return b
}

func tableBucketIdx(tableID int64) int {
	trace_util_0.Count(_builder_00000, 106)
	return int(tableID % bucketCount)
}

func tableIDIsValid(tableID int64) bool {
	trace_util_0.Count(_builder_00000, 107)
	return tableID != 0
}

var _builder_00000 = "infoschema/builder.go"
