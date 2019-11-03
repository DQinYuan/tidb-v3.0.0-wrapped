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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_set_00000, 0)
	req.Reset()
	if e.done {
		trace_util_0.Count(_set_00000, 3)
		return nil
	}
	trace_util_0.Count(_set_00000, 1)
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		trace_util_0.Count(_set_00000, 4)
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames {
			trace_util_0.Count(_set_00000, 7)
			// This is set charset stmt.
			dt, err := v.Expr.(*expression.Constant).Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_set_00000, 11)
				return err
			}
			trace_util_0.Count(_set_00000, 8)
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				trace_util_0.Count(_set_00000, 12)
				co = v.ExtendValue.Value.GetString()
			}
			trace_util_0.Count(_set_00000, 9)
			err = e.setCharset(cs, co)
			if err != nil {
				trace_util_0.Count(_set_00000, 13)
				return err
			}
			trace_util_0.Count(_set_00000, 10)
			continue
		}
		trace_util_0.Count(_set_00000, 5)
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			trace_util_0.Count(_set_00000, 14)
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Row{})
			if err != nil {
				trace_util_0.Count(_set_00000, 17)
				return err
			}

			trace_util_0.Count(_set_00000, 15)
			if value.IsNull() {
				trace_util_0.Count(_set_00000, 18)
				delete(sessionVars.Users, name)
			} else {
				trace_util_0.Count(_set_00000, 19)
				{
					svalue, err1 := value.ToString()
					if err1 != nil {
						trace_util_0.Count(_set_00000, 21)
						return err1
					}
					trace_util_0.Count(_set_00000, 20)
					sessionVars.Users[name] = fmt.Sprintf("%v", svalue)
				}
			}
			trace_util_0.Count(_set_00000, 16)
			continue
		}

		trace_util_0.Count(_set_00000, 6)
		syns := e.getSynonyms(name)
		// Set system variable
		for _, n := range syns {
			trace_util_0.Count(_set_00000, 22)
			err := e.setSysVariable(n, v)
			if err != nil {
				trace_util_0.Count(_set_00000, 23)
				return err
			}
		}
	}
	trace_util_0.Count(_set_00000, 2)
	return nil
}

func (e *SetExecutor) getSynonyms(varName string) []string {
	trace_util_0.Count(_set_00000, 24)
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		trace_util_0.Count(_set_00000, 26)
		return synonyms
	}

	trace_util_0.Count(_set_00000, 25)
	synonyms = []string{varName}
	return synonyms
}

func (e *SetExecutor) setSysVariable(name string, v *expression.VarAssignment) error {
	trace_util_0.Count(_set_00000, 27)
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		trace_util_0.Count(_set_00000, 31)
		return variable.UnknownSystemVar.GenWithStackByArgs(name)
	}
	trace_util_0.Count(_set_00000, 28)
	if sysVar.Scope == variable.ScopeNone {
		trace_util_0.Count(_set_00000, 32)
		return errors.Errorf("Variable '%s' is a read only variable", name)
	}
	trace_util_0.Count(_set_00000, 29)
	if v.IsGlobal {
		trace_util_0.Count(_set_00000, 33)
		// Set global scope system variable.
		if sysVar.Scope&variable.ScopeGlobal == 0 {
			trace_util_0.Count(_set_00000, 40)
			return errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
		}
		trace_util_0.Count(_set_00000, 34)
		value, err := e.getVarValue(v, sysVar)
		if err != nil {
			trace_util_0.Count(_set_00000, 41)
			return err
		}
		trace_util_0.Count(_set_00000, 35)
		if value.IsNull() {
			trace_util_0.Count(_set_00000, 42)
			value.SetString("")
		}
		trace_util_0.Count(_set_00000, 36)
		svalue, err := value.ToString()
		if err != nil {
			trace_util_0.Count(_set_00000, 43)
			return err
		}
		trace_util_0.Count(_set_00000, 37)
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, svalue)
		if err != nil {
			trace_util_0.Count(_set_00000, 44)
			return err
		}
		trace_util_0.Count(_set_00000, 38)
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			trace_util_0.Count(_set_00000, 45)
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				trace_util_0.Count(_set_00000, 47)
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.ctx.GetSessionVars(), name, svalue)
			}
			trace_util_0.Count(_set_00000, 46)
			return nil
		})
		trace_util_0.Count(_set_00000, 39)
		if err != nil {
			trace_util_0.Count(_set_00000, 48)
			return err
		}
	} else {
		trace_util_0.Count(_set_00000, 49)
		{
			// Set session scope system variable.
			if sysVar.Scope&variable.ScopeSession == 0 {
				trace_util_0.Count(_set_00000, 57)
				return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
			}
			trace_util_0.Count(_set_00000, 50)
			value, err := e.getVarValue(v, nil)
			if err != nil {
				trace_util_0.Count(_set_00000, 58)
				return err
			}
			trace_util_0.Count(_set_00000, 51)
			oldSnapshotTS := sessionVars.SnapshotTS
			if name == variable.TxnIsolationOneShot && sessionVars.InTxn() {
				trace_util_0.Count(_set_00000, 59)
				return errors.Trace(ErrCantChangeTxCharacteristics)
			}
			trace_util_0.Count(_set_00000, 52)
			err = variable.SetSessionSystemVar(sessionVars, name, value)
			if err != nil {
				trace_util_0.Count(_set_00000, 60)
				return err
			}
			trace_util_0.Count(_set_00000, 53)
			newSnapshotIsSet := sessionVars.SnapshotTS > 0 && sessionVars.SnapshotTS != oldSnapshotTS
			if newSnapshotIsSet {
				trace_util_0.Count(_set_00000, 61)
				err = gcutil.ValidateSnapshot(e.ctx, sessionVars.SnapshotTS)
				if err != nil {
					trace_util_0.Count(_set_00000, 62)
					sessionVars.SnapshotTS = oldSnapshotTS
					return err
				}
			}
			trace_util_0.Count(_set_00000, 54)
			err = e.loadSnapshotInfoSchemaIfNeeded(name)
			if err != nil {
				trace_util_0.Count(_set_00000, 63)
				sessionVars.SnapshotTS = oldSnapshotTS
				return err
			}
			trace_util_0.Count(_set_00000, 55)
			var valStr string
			if value.IsNull() {
				trace_util_0.Count(_set_00000, 64)
				valStr = "NULL"
			} else {
				trace_util_0.Count(_set_00000, 65)
				{
					var err error
					valStr, err = value.ToString()
					terror.Log(err)
				}
			}
			trace_util_0.Count(_set_00000, 56)
			logutil.Logger(context.Background()).Info("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		}
	}

	trace_util_0.Count(_set_00000, 30)
	return nil
}

func (e *SetExecutor) setCharset(cs, co string) error {
	trace_util_0.Count(_set_00000, 66)
	var err error
	if len(co) == 0 {
		trace_util_0.Count(_set_00000, 69)
		co, err = charset.GetDefaultCollation(cs)
		if err != nil {
			trace_util_0.Count(_set_00000, 70)
			return err
		}
	}
	trace_util_0.Count(_set_00000, 67)
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range variable.SetNamesVariables {
		trace_util_0.Count(_set_00000, 71)
		terror.Log(sessionVars.SetSystemVar(v, cs))
	}
	trace_util_0.Count(_set_00000, 68)
	terror.Log(sessionVars.SetSystemVar(variable.CollationConnection, co))
	return nil
}

func (e *SetExecutor) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value types.Datum, err error) {
	trace_util_0.Count(_set_00000, 72)
	if v.IsDefault {
		trace_util_0.Count(_set_00000, 74)
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			trace_util_0.Count(_set_00000, 76)
			value = types.NewStringDatum(sysVar.Value)
		} else {
			trace_util_0.Count(_set_00000, 77)
			{
				s, err1 := variable.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
				if err1 != nil {
					trace_util_0.Count(_set_00000, 79)
					return value, err1
				}
				trace_util_0.Count(_set_00000, 78)
				value = types.NewStringDatum(s)
			}
		}
		trace_util_0.Count(_set_00000, 75)
		return
	}
	trace_util_0.Count(_set_00000, 73)
	value, err = v.Expr.Eval(chunk.Row{})
	return value, err
}

func (e *SetExecutor) loadSnapshotInfoSchemaIfNeeded(name string) error {
	trace_util_0.Count(_set_00000, 80)
	if name != variable.TiDBSnapshot {
		trace_util_0.Count(_set_00000, 84)
		return nil
	}
	trace_util_0.Count(_set_00000, 81)
	vars := e.ctx.GetSessionVars()
	if vars.SnapshotTS == 0 {
		trace_util_0.Count(_set_00000, 85)
		vars.SnapshotInfoschema = nil
		return nil
	}
	trace_util_0.Count(_set_00000, 82)
	logutil.Logger(context.Background()).Info("load snapshot info schema", zap.Uint64("conn", vars.ConnectionID), zap.Uint64("SnapshotTS", vars.SnapshotTS))
	dom := domain.GetDomain(e.ctx)
	snapInfo, err := dom.GetSnapshotInfoSchema(vars.SnapshotTS)
	if err != nil {
		trace_util_0.Count(_set_00000, 86)
		return err
	}
	trace_util_0.Count(_set_00000, 83)
	vars.SnapshotInfoschema = snapInfo
	return nil
}

var _set_00000 = "executor/set.go"
