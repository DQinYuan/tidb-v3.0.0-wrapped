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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/trace_util_0"
)

func onCreateSchema(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_schema_00000, 0)
	schemaID := job.SchemaID
	dbInfo := &model.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		trace_util_0.Count(_schema_00000, 4)
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 1)
	dbInfo.ID = schemaID
	dbInfo.State = model.StateNone

	err := checkSchemaNotExists(d, t, schemaID, dbInfo)
	if err != nil {
		trace_util_0.Count(_schema_00000, 5)
		if infoschema.ErrDatabaseExists.Equal(err) {
			trace_util_0.Count(_schema_00000, 7)
			// The database already exists, can't create it, we should cancel this job now.
			job.State = model.JobStateCancelled
		}
		trace_util_0.Count(_schema_00000, 6)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 2)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_schema_00000, 8)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 3)
	switch dbInfo.State {
	case model.StateNone:
		trace_util_0.Count(_schema_00000, 9)
		// none -> public
		dbInfo.State = model.StatePublic
		err = t.CreateDatabase(dbInfo)
		if err != nil {
			trace_util_0.Count(_schema_00000, 12)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_schema_00000, 10)
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	default:
		trace_util_0.Count(_schema_00000, 11)
		// We can't enter here.
		return ver, errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

func checkSchemaNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, dbInfo *model.DBInfo) error {
	trace_util_0.Count(_schema_00000, 13)
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil {
		trace_util_0.Count(_schema_00000, 17)
		return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
	}
	// Try to use memory schema info to check first.
	trace_util_0.Count(_schema_00000, 14)
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		trace_util_0.Count(_schema_00000, 18)
		return err
	}
	trace_util_0.Count(_schema_00000, 15)
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		trace_util_0.Count(_schema_00000, 19)
		return checkSchemaNotExistsFromInfoSchema(is, schemaID, dbInfo)
	}
	trace_util_0.Count(_schema_00000, 16)
	return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
}

func checkSchemaNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, dbInfo *model.DBInfo) error {
	trace_util_0.Count(_schema_00000, 20)
	// Check database exists by name.
	if is.SchemaExists(dbInfo.Name) {
		trace_util_0.Count(_schema_00000, 23)
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	// Check database exists by ID.
	trace_util_0.Count(_schema_00000, 21)
	if _, ok := is.SchemaByID(schemaID); ok {
		trace_util_0.Count(_schema_00000, 24)
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	trace_util_0.Count(_schema_00000, 22)
	return nil
}

func checkSchemaNotExistsFromStore(t *meta.Meta, schemaID int64, dbInfo *model.DBInfo) error {
	trace_util_0.Count(_schema_00000, 25)
	dbs, err := t.ListDatabases()
	if err != nil {
		trace_util_0.Count(_schema_00000, 28)
		return errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 26)
	for _, db := range dbs {
		trace_util_0.Count(_schema_00000, 29)
		if db.Name.L == dbInfo.Name.L {
			trace_util_0.Count(_schema_00000, 30)
			if db.ID != schemaID {
				trace_util_0.Count(_schema_00000, 32)
				return infoschema.ErrDatabaseExists.GenWithStackByArgs(db.Name)
			}
			trace_util_0.Count(_schema_00000, 31)
			dbInfo = db
		}
	}
	trace_util_0.Count(_schema_00000, 27)
	return nil
}

func onModifySchemaCharsetAndCollate(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_schema_00000, 33)
	var toCharset, toCollate string
	if err := job.DecodeArgs(&toCharset, &toCollate); err != nil {
		trace_util_0.Count(_schema_00000, 39)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 34)
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		trace_util_0.Count(_schema_00000, 40)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 35)
	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		trace_util_0.Count(_schema_00000, 41)
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}

	trace_util_0.Count(_schema_00000, 36)
	dbInfo.Charset = toCharset
	dbInfo.Collate = toCollate

	if err = t.UpdateDatabase(dbInfo); err != nil {
		trace_util_0.Count(_schema_00000, 42)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_schema_00000, 37)
	if ver, err = updateSchemaVersion(t, job); err != nil {
		trace_util_0.Count(_schema_00000, 43)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_schema_00000, 38)
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

func onDropSchema(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_schema_00000, 44)
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		trace_util_0.Count(_schema_00000, 48)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_schema_00000, 45)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		trace_util_0.Count(_schema_00000, 49)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_schema_00000, 46)
	switch dbInfo.State {
	case model.StatePublic:
		trace_util_0.Count(_schema_00000, 50)
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateWriteOnly:
		trace_util_0.Count(_schema_00000, 51)
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateDeleteOnly:
		trace_util_0.Count(_schema_00000, 52)
		dbInfo.State = model.StateNone
		var tables []*model.TableInfo
		tables, err = t.ListTables(job.SchemaID)
		if err != nil {
			trace_util_0.Count(_schema_00000, 58)
			return ver, errors.Trace(err)
		}

		trace_util_0.Count(_schema_00000, 53)
		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			trace_util_0.Count(_schema_00000, 59)
			return ver, errors.Trace(err)
		}
		trace_util_0.Count(_schema_00000, 54)
		if err = t.DropDatabase(dbInfo.ID); err != nil {
			trace_util_0.Count(_schema_00000, 60)
			break
		}

		// Finish this job.
		trace_util_0.Count(_schema_00000, 55)
		if len(tables) > 0 {
			trace_util_0.Count(_schema_00000, 61)
			job.Args = append(job.Args, getIDs(tables))
		}
		trace_util_0.Count(_schema_00000, 56)
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, dbInfo)
	default:
		trace_util_0.Count(_schema_00000, 57)
		// We can't enter here.
		err = errors.Errorf("invalid db state %v", dbInfo.State)
	}

	trace_util_0.Count(_schema_00000, 47)
	return ver, errors.Trace(err)
}

func checkSchemaExistAndCancelNotExistJob(t *meta.Meta, job *model.Job) (*model.DBInfo, error) {
	trace_util_0.Count(_schema_00000, 62)
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		trace_util_0.Count(_schema_00000, 65)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_schema_00000, 63)
	if dbInfo == nil {
		trace_util_0.Count(_schema_00000, 66)
		job.State = model.JobStateCancelled
		return nil, infoschema.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	trace_util_0.Count(_schema_00000, 64)
	return dbInfo, nil
}

func getIDs(tables []*model.TableInfo) []int64 {
	trace_util_0.Count(_schema_00000, 67)
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		trace_util_0.Count(_schema_00000, 69)
		ids = append(ids, t.ID)
		if t.GetPartitionInfo() != nil {
			trace_util_0.Count(_schema_00000, 70)
			ids = append(ids, getPartitionIDs(t)...)
		}
	}

	trace_util_0.Count(_schema_00000, 68)
	return ids
}

var _schema_00000 = "ddl/schema.go"
