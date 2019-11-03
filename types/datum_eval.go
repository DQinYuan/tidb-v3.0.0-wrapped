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

package types

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/trace_util_0"
)

// ComputePlus computes the result of a+b.
func ComputePlus(a, b Datum) (d Datum, err error) {
	trace_util_0.Count(_datum_eval_00000, 0)
	switch a.Kind() {
	case KindInt64:
		trace_util_0.Count(_datum_eval_00000, 2)
		switch b.Kind() {
		case KindInt64:
			trace_util_0.Count(_datum_eval_00000, 6)
			r, err1 := AddInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			trace_util_0.Count(_datum_eval_00000, 7)
			r, err1 := AddInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindUint64:
		trace_util_0.Count(_datum_eval_00000, 3)
		switch b.Kind() {
		case KindInt64:
			trace_util_0.Count(_datum_eval_00000, 8)
			r, err1 := AddInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			trace_util_0.Count(_datum_eval_00000, 9)
			r, err1 := AddUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindFloat64:
		trace_util_0.Count(_datum_eval_00000, 4)
		switch b.Kind() {
		case KindFloat64:
			trace_util_0.Count(_datum_eval_00000, 10)
			r := a.GetFloat64() + b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case KindMysqlDecimal:
		trace_util_0.Count(_datum_eval_00000, 5)
		switch b.Kind() {
		case KindMysqlDecimal:
			trace_util_0.Count(_datum_eval_00000, 11)
			r := new(MyDecimal)
			err = DecimalAdd(a.GetMysqlDecimal(), b.GetMysqlDecimal(), r)
			d.SetMysqlDecimal(r)
			d.SetFrac(mathutil.Max(a.Frac(), b.Frac()))
			return d, err
		}
	}
	trace_util_0.Count(_datum_eval_00000, 1)
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Plus)
	return d, err
}

var _datum_eval_00000 = "types/datum_eval.go"
