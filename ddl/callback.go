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
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// Interceptor is used for DDL.
type Interceptor interface {
	// OnGetInfoSchema is an intercept which is called in the function ddl.GetInfoSchema(). It is used in the tests.
	OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema
}

// BaseInterceptor implements Interceptor.
type BaseInterceptor struct{}

// OnGetInfoSchema implements Interceptor.OnGetInfoSchema interface.
func (bi *BaseInterceptor) OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	trace_util_0.Count(_callback_00000, 0)
	return is
}

// Callback is used for DDL.
type Callback interface {
	// OnChanged is called after schema is changed.
	OnChanged(err error) error
	// OnJobRunBefore is called before running job.
	OnJobRunBefore(job *model.Job)
	// OnJobUpdated is called after the running job is updated.
	OnJobUpdated(job *model.Job)
	// OnWatched is called after watching owner is completed.
	OnWatched(ctx context.Context)
}

// BaseCallback implements Callback.OnChanged interface.
type BaseCallback struct {
}

// OnChanged implements Callback interface.
func (c *BaseCallback) OnChanged(err error) error {
	trace_util_0.Count(_callback_00000, 1)
	return err
}

// OnJobRunBefore implements Callback.OnJobRunBefore interface.
func (c *BaseCallback) OnJobRunBefore(job *model.Job) {
	trace_util_0.Count(_callback_00000, 2)
	// Nothing to do.
}

// OnJobUpdated implements Callback.OnJobUpdated interface.
func (c *BaseCallback) OnJobUpdated(job *model.Job) {
	trace_util_0.Count(_callback_00000, 3)
	// Nothing to do.
}

// OnWatched implements Callback.OnWatched interface.
func (c *BaseCallback) OnWatched(ctx context.Context) {
	trace_util_0.Count(_callback_00000, 4)
	// Nothing to do.
}

var _callback_00000 = "ddl/callback.go"
