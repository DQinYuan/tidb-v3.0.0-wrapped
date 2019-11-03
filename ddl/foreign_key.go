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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/trace_util_0"
)

func onCreateForeignKey(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_foreign_key_00000, 0)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_foreign_key_00000, 3)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_foreign_key_00000, 1)
	var fkInfo model.FKInfo
	err = job.DecodeArgs(&fkInfo)
	if err != nil {
		trace_util_0.Count(_foreign_key_00000, 4)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_foreign_key_00000, 2)
	fkInfo.ID = allocateIndexID(tblInfo)
	tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)

	originalState := fkInfo.State
	switch fkInfo.State {
	case model.StateNone:
		trace_util_0.Count(_foreign_key_00000, 5)
		// We just support record the foreign key, so we just make it public.
		// none -> public
		fkInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			trace_util_0.Count(_foreign_key_00000, 8)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_foreign_key_00000, 6)
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		trace_util_0.Count(_foreign_key_00000, 7)
		return ver, ErrInvalidForeignKeyState.GenWithStack("invalid fk state %v", fkInfo.State)
	}
}

func onDropForeignKey(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_foreign_key_00000, 9)
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		trace_util_0.Count(_foreign_key_00000, 15)
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_foreign_key_00000, 10)
	var (
		fkName model.CIStr
		found  bool
		fkInfo model.FKInfo
	)
	err = job.DecodeArgs(&fkName)
	if err != nil {
		trace_util_0.Count(_foreign_key_00000, 16)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	trace_util_0.Count(_foreign_key_00000, 11)
	for _, fk := range tblInfo.ForeignKeys {
		trace_util_0.Count(_foreign_key_00000, 17)
		if fk.Name.L == fkName.L {
			trace_util_0.Count(_foreign_key_00000, 18)
			found = true
			fkInfo = *fk
		}
	}

	trace_util_0.Count(_foreign_key_00000, 12)
	if !found {
		trace_util_0.Count(_foreign_key_00000, 19)
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
	}

	trace_util_0.Count(_foreign_key_00000, 13)
	nfks := tblInfo.ForeignKeys[:0]
	for _, fk := range tblInfo.ForeignKeys {
		trace_util_0.Count(_foreign_key_00000, 20)
		if fk.Name.L != fkName.L {
			trace_util_0.Count(_foreign_key_00000, 21)
			nfks = append(nfks, fk)
		}
	}
	trace_util_0.Count(_foreign_key_00000, 14)
	tblInfo.ForeignKeys = nfks

	originalState := fkInfo.State
	switch fkInfo.State {
	case model.StatePublic:
		trace_util_0.Count(_foreign_key_00000, 22)
		// We just support record the foreign key, so we just make it none.
		// public -> none
		fkInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			trace_util_0.Count(_foreign_key_00000, 25)
			return ver, errors.Trace(err)
		}
		// Finish this job.
		trace_util_0.Count(_foreign_key_00000, 23)
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		return ver, nil
	default:
		trace_util_0.Count(_foreign_key_00000, 24)
		return ver, ErrInvalidForeignKeyState.GenWithStack("invalid fk state %v", fkInfo.State)
	}

}

var _foreign_key_00000 = "ddl/foreign_key.go"
