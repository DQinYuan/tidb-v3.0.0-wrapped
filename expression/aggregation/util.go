// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
)

// distinctChecker stores existing keys and checks if given data is distinct.
type distinctChecker struct {
	existingKeys *mvmap.MVMap
	key          []byte
	vals         [][]byte
	sc           *stmtctx.StatementContext
}

// createDistinctChecker creates a new distinct checker.
func createDistinctChecker(sc *stmtctx.StatementContext) *distinctChecker {
	trace_util_0.Count(_util_00000, 0)
	return &distinctChecker{
		existingKeys: mvmap.NewMVMap(),
		sc:           sc,
	}
}

// Check checks if values is distinct.
func (d *distinctChecker) Check(values []types.Datum) (bool, error) {
	trace_util_0.Count(_util_00000, 1)
	d.key = d.key[:0]
	var err error
	d.key, err = codec.EncodeValue(d.sc, d.key, values...)
	if err != nil {
		trace_util_0.Count(_util_00000, 4)
		return false, err
	}
	trace_util_0.Count(_util_00000, 2)
	d.vals = d.existingKeys.Get(d.key, d.vals[:0])
	if len(d.vals) > 0 {
		trace_util_0.Count(_util_00000, 5)
		return false, nil
	}
	trace_util_0.Count(_util_00000, 3)
	d.existingKeys.Put(d.key, []byte{})
	return true, nil
}

// calculateSum adds v to sum.
func calculateSum(sc *stmtctx.StatementContext, sum, v types.Datum) (data types.Datum, err error) {
	trace_util_0.Count(_util_00000, 6)
	// for avg and sum calculation
	// avg and sum use decimal for integer and decimal type, use float for others
	// see https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html

	switch v.Kind() {
	case types.KindNull:
		trace_util_0.Count(_util_00000, 10)
	case types.KindInt64, types.KindUint64:
		trace_util_0.Count(_util_00000, 11)
		var d *types.MyDecimal
		d, err = v.ToDecimal(sc)
		if err == nil {
			trace_util_0.Count(_util_00000, 14)
			data = types.NewDecimalDatum(d)
		}
	case types.KindMysqlDecimal:
		trace_util_0.Count(_util_00000, 12)
		data = types.CloneDatum(v)
	default:
		trace_util_0.Count(_util_00000, 13)
		var f float64
		f, err = v.ToFloat64(sc)
		if err == nil {
			trace_util_0.Count(_util_00000, 15)
			data = types.NewFloat64Datum(f)
		}
	}

	trace_util_0.Count(_util_00000, 7)
	if err != nil {
		trace_util_0.Count(_util_00000, 16)
		return data, err
	}
	trace_util_0.Count(_util_00000, 8)
	if data.IsNull() {
		trace_util_0.Count(_util_00000, 17)
		return sum, nil
	}
	trace_util_0.Count(_util_00000, 9)
	switch sum.Kind() {
	case types.KindNull:
		trace_util_0.Count(_util_00000, 18)
		return data, nil
	case types.KindFloat64, types.KindMysqlDecimal:
		trace_util_0.Count(_util_00000, 19)
		return types.ComputePlus(sum, data)
	default:
		trace_util_0.Count(_util_00000, 20)
		return data, errors.Errorf("invalid value %v for aggregate", sum.Kind())
	}
}

var _util_00000 = "expression/aggregation/util.go"
