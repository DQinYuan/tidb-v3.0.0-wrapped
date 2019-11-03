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

package codec

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// EncodeDecimal encodes a decimal into a byte slice which can be sorted lexicographically later.
func EncodeDecimal(b []byte, dec *types.MyDecimal, precision, frac int) ([]byte, error) {
	trace_util_0.Count(_decimal_00000, 0)
	if precision == 0 {
		trace_util_0.Count(_decimal_00000, 2)
		precision, frac = dec.PrecisionAndFrac()
	}
	trace_util_0.Count(_decimal_00000, 1)
	b = append(b, byte(precision), byte(frac))
	bin, err := dec.ToBin(precision, frac)
	b = append(b, bin...)
	return b, errors.Trace(err)
}

// DecodeDecimal decodes bytes to decimal.
func DecodeDecimal(b []byte) ([]byte, *types.MyDecimal, int, int, error) {
	trace_util_0.Count(_decimal_00000, 3)
	failpoint.Inject("errorInDecodeDecimal", func(val failpoint.Value) {
		trace_util_0.Count(_decimal_00000, 7)
		if val.(bool) {
			trace_util_0.Count(_decimal_00000, 8)
			failpoint.Return(b, nil, 0, 0, errors.New("gofail error"))
		}
	})

	trace_util_0.Count(_decimal_00000, 4)
	if len(b) < 3 {
		trace_util_0.Count(_decimal_00000, 9)
		return b, nil, 0, 0, errors.New("insufficient bytes to decode value")
	}
	trace_util_0.Count(_decimal_00000, 5)
	precision := int(b[0])
	frac := int(b[1])
	b = b[2:]
	dec := new(types.MyDecimal)
	binSize, err := dec.FromBin(b, precision, frac)
	b = b[binSize:]
	if err != nil {
		trace_util_0.Count(_decimal_00000, 10)
		return b, nil, precision, frac, errors.Trace(err)
	}
	trace_util_0.Count(_decimal_00000, 6)
	return b, dec, precision, frac, nil
}

var _decimal_00000 = "util/codec/decimal.go"
