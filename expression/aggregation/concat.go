// Copyright 2017 PingCAP, Inc.
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

package aggregation

import (
	"bytes"
	"fmt"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type concatFunction struct {
	aggFunction
	separator string
	sepInited bool
	maxLen    uint64
	// truncated according to MySQL, a 'group_concat' function generates exactly one 'truncated' warning during its life time, no matter
	// how many group actually truncated. 'truncated' acts as a sentinel to indicate whether this warning has already been
	// generated.
	truncated bool
}

func (cf *concatFunction) writeValue(evalCtx *AggEvaluateContext, val types.Datum) {
	trace_util_0.Count(_concat_00000, 0)
	if val.Kind() == types.KindBytes {
		trace_util_0.Count(_concat_00000, 1)
		evalCtx.Buffer.Write(val.GetBytes())
	} else {
		trace_util_0.Count(_concat_00000, 2)
		{
			evalCtx.Buffer.WriteString(fmt.Sprintf("%v", val.GetValue()))
		}
	}
}

func (cf *concatFunction) initSeparator(sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_concat_00000, 3)
	sepArg := cf.Args[len(cf.Args)-1]
	sepDatum, err := sepArg.Eval(row)
	if err != nil {
		trace_util_0.Count(_concat_00000, 6)
		return err
	}
	trace_util_0.Count(_concat_00000, 4)
	if sepDatum.IsNull() {
		trace_util_0.Count(_concat_00000, 7)
		return errors.Errorf("Invalid separator argument.")
	}
	trace_util_0.Count(_concat_00000, 5)
	cf.separator, err = sepDatum.ToString()
	return err
}

// Update implements Aggregation interface.
func (cf *concatFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	trace_util_0.Count(_concat_00000, 8)
	datumBuf := make([]types.Datum, 0, len(cf.Args))
	if !cf.sepInited {
		trace_util_0.Count(_concat_00000, 15)
		err := cf.initSeparator(sc, row)
		if err != nil {
			trace_util_0.Count(_concat_00000, 17)
			return err
		}
		trace_util_0.Count(_concat_00000, 16)
		cf.sepInited = true
	}

	// The last parameter is the concat separator, we only concat the first "len(cf.Args)-1" parameters.
	trace_util_0.Count(_concat_00000, 9)
	for i, length := 0, len(cf.Args)-1; i < length; i++ {
		trace_util_0.Count(_concat_00000, 18)
		value, err := cf.Args[i].Eval(row)
		if err != nil {
			trace_util_0.Count(_concat_00000, 21)
			return err
		}
		trace_util_0.Count(_concat_00000, 19)
		if value.IsNull() {
			trace_util_0.Count(_concat_00000, 22)
			return nil
		}
		trace_util_0.Count(_concat_00000, 20)
		datumBuf = append(datumBuf, value)
	}
	trace_util_0.Count(_concat_00000, 10)
	if cf.HasDistinct {
		trace_util_0.Count(_concat_00000, 23)
		d, err := evalCtx.DistinctChecker.Check(datumBuf)
		if err != nil {
			trace_util_0.Count(_concat_00000, 25)
			return err
		}
		trace_util_0.Count(_concat_00000, 24)
		if !d {
			trace_util_0.Count(_concat_00000, 26)
			return nil
		}
	}
	trace_util_0.Count(_concat_00000, 11)
	if evalCtx.Buffer == nil {
		trace_util_0.Count(_concat_00000, 27)
		evalCtx.Buffer = &bytes.Buffer{}
	} else {
		trace_util_0.Count(_concat_00000, 28)
		{
			evalCtx.Buffer.WriteString(cf.separator)
		}
	}
	trace_util_0.Count(_concat_00000, 12)
	for _, val := range datumBuf {
		trace_util_0.Count(_concat_00000, 29)
		cf.writeValue(evalCtx, val)
	}
	trace_util_0.Count(_concat_00000, 13)
	if cf.maxLen > 0 && uint64(evalCtx.Buffer.Len()) > cf.maxLen {
		trace_util_0.Count(_concat_00000, 30)
		i := mathutil.MaxInt
		if uint64(i) > cf.maxLen {
			trace_util_0.Count(_concat_00000, 33)
			i = int(cf.maxLen)
		}
		trace_util_0.Count(_concat_00000, 31)
		evalCtx.Buffer.Truncate(i)
		if !cf.truncated {
			trace_util_0.Count(_concat_00000, 34)
			sc.AppendWarning(expression.ErrCutValueGroupConcat.GenWithStackByArgs(cf.Args[0].String()))
		}
		trace_util_0.Count(_concat_00000, 32)
		cf.truncated = true
	}
	trace_util_0.Count(_concat_00000, 14)
	return nil
}

// GetResult implements Aggregation interface.
func (cf *concatFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	trace_util_0.Count(_concat_00000, 35)
	if evalCtx.Buffer != nil {
		trace_util_0.Count(_concat_00000, 37)
		d.SetString(evalCtx.Buffer.String())
	} else {
		trace_util_0.Count(_concat_00000, 38)
		{
			d.SetNull()
		}
	}
	trace_util_0.Count(_concat_00000, 36)
	return d
}

func (cf *concatFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	trace_util_0.Count(_concat_00000, 39)
	if cf.HasDistinct {
		trace_util_0.Count(_concat_00000, 41)
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	trace_util_0.Count(_concat_00000, 40)
	evalCtx.Buffer = nil
}

// GetPartialResult implements Aggregation interface.
func (cf *concatFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	trace_util_0.Count(_concat_00000, 42)
	return []types.Datum{cf.GetResult(evalCtx)}
}

var _concat_00000 = "expression/aggregation/concat.go"
