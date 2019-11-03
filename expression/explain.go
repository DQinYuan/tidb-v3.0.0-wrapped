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

package expression

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 0)
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	for i, arg := range expr.GetArgs() {
		trace_util_0.Count(_explain_00000, 2)
		buffer.WriteString(arg.ExplainInfo())
		if i+1 < len(expr.GetArgs()) {
			trace_util_0.Count(_explain_00000, 3)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 1)
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainInfo implements the Expression interface.
func (expr *Column) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 4)
	return expr.String()
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo() string {
	trace_util_0.Count(_explain_00000, 5)
	dt, err := expr.Eval(chunk.Row{})
	if err != nil {
		trace_util_0.Count(_explain_00000, 7)
		return "not recognized const vanue"
	}
	trace_util_0.Count(_explain_00000, 6)
	return expr.format(dt)
}

func (expr *Constant) format(dt types.Datum) string {
	trace_util_0.Count(_explain_00000, 8)
	switch dt.Kind() {
	case types.KindNull:
		trace_util_0.Count(_explain_00000, 10)
		return "NULL"
	case types.KindString, types.KindBytes, types.KindMysqlEnum, types.KindMysqlSet,
		types.KindMysqlJSON, types.KindBinaryLiteral, types.KindMysqlBit:
		trace_util_0.Count(_explain_00000, 11)
		return fmt.Sprintf("\"%v\"", dt.GetValue())
	}
	trace_util_0.Count(_explain_00000, 9)
	return fmt.Sprintf("%v", dt.GetValue())
}

// ExplainExpressionList generates explain information for a list of expressions.
func ExplainExpressionList(exprs []Expression) []byte {
	trace_util_0.Count(_explain_00000, 12)
	buffer := bytes.NewBufferString("")
	for i, expr := range exprs {
		trace_util_0.Count(_explain_00000, 14)
		buffer.WriteString(expr.ExplainInfo())
		if i+1 < len(exprs) {
			trace_util_0.Count(_explain_00000, 15)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 13)
	return buffer.Bytes()
}

// SortedExplainExpressionList generates explain information for a list of expressions in order.
// In some scenarios, the expr's order may not be stable when executing multiple times.
// So we add a sort to make its explain result stable.
func SortedExplainExpressionList(exprs []Expression) []byte {
	trace_util_0.Count(_explain_00000, 16)
	buffer := bytes.NewBufferString("")
	exprInfos := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		trace_util_0.Count(_explain_00000, 19)
		exprInfos = append(exprInfos, expr.ExplainInfo())
	}
	trace_util_0.Count(_explain_00000, 17)
	sort.Strings(exprInfos)
	for i, info := range exprInfos {
		trace_util_0.Count(_explain_00000, 20)
		buffer.WriteString(info)
		if i+1 < len(exprInfos) {
			trace_util_0.Count(_explain_00000, 21)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 18)
	return buffer.Bytes()
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(cols []*Column) []byte {
	trace_util_0.Count(_explain_00000, 22)
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		trace_util_0.Count(_explain_00000, 24)
		buffer.WriteString(col.ExplainInfo())
		if i+1 < len(cols) {
			trace_util_0.Count(_explain_00000, 25)
			buffer.WriteString(", ")
		}
	}
	trace_util_0.Count(_explain_00000, 23)
	return buffer.Bytes()
}

var _explain_00000 = "expression/explain.go"
