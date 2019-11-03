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
	"github.com/pingcap/tidb/trace_util_0"
)

// ExplainAggFunc generates explain information for a aggregation function.
func ExplainAggFunc(agg *AggFuncDesc) string {
	trace_util_0.Count(_explain_00000, 0)
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", agg.Name)
	if agg.HasDistinct {
		trace_util_0.Count(_explain_00000, 3)
		buffer.WriteString("distinct ")
	}
	trace_util_0.Count(_explain_00000, 1)
	for i, arg := range agg.Args {
		trace_util_0.Count(_explain_00000, 4)
		buffer.WriteString(arg.ExplainInfo())
		if i+1 < len(agg.Args) {
			trace_util_0.Count(_explain_00000, 5)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 2)
	buffer.WriteString(")")
	return buffer.String()
}

var _explain_00000 = "expression/aggregation/explain.go"
