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

package aggfuncs

import (
	"bytes"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
)

type baseGroupConcat4String struct {
	baseAggFunc

	sep    string
	maxLen uint64
	// According to MySQL, a 'group_concat' function generates exactly one 'truncated' warning during its life time, no matter
	// how many group actually truncated. 'truncated' acts as a sentinel to indicate whether this warning has already been
	// generated.
	truncated *int32
}

func (e *baseGroupConcat4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	trace_util_0.Count(_func_group_concat_00000, 0)
	p := (*partialResult4GroupConcat)(pr)
	if p.buffer == nil {
		trace_util_0.Count(_func_group_concat_00000, 2)
		chk.AppendNull(e.ordinal)
		return nil
	}
	trace_util_0.Count(_func_group_concat_00000, 1)
	chk.AppendString(e.ordinal, p.buffer.String())
	return nil
}

func (e *baseGroupConcat4String) truncatePartialResultIfNeed(sctx sessionctx.Context, buffer *bytes.Buffer) (err error) {
	trace_util_0.Count(_func_group_concat_00000, 3)
	if e.maxLen > 0 && uint64(buffer.Len()) > e.maxLen {
		trace_util_0.Count(_func_group_concat_00000, 5)
		i := mathutil.MaxInt
		if uint64(i) > e.maxLen {
			trace_util_0.Count(_func_group_concat_00000, 7)
			i = int(e.maxLen)
		}
		trace_util_0.Count(_func_group_concat_00000, 6)
		buffer.Truncate(i)
		if atomic.CompareAndSwapInt32(e.truncated, 0, 1) {
			trace_util_0.Count(_func_group_concat_00000, 8)
			if !sctx.GetSessionVars().StmtCtx.TruncateAsWarning {
				trace_util_0.Count(_func_group_concat_00000, 10)
				return expression.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String())
			}
			trace_util_0.Count(_func_group_concat_00000, 9)
			sctx.GetSessionVars().StmtCtx.AppendWarning(expression.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String()))
		}
	}
	trace_util_0.Count(_func_group_concat_00000, 4)
	return nil
}

type basePartialResult4GroupConcat struct {
	valsBuf *bytes.Buffer
	buffer  *bytes.Buffer
}

type partialResult4GroupConcat struct {
	basePartialResult4GroupConcat
}

type groupConcat struct {
	baseGroupConcat4String
}

func (e *groupConcat) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_group_concat_00000, 11)
	p := new(partialResult4GroupConcat)
	p.valsBuf = &bytes.Buffer{}
	return PartialResult(p)
}

func (e *groupConcat) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_group_concat_00000, 12)
	p := (*partialResult4GroupConcat)(pr)
	p.buffer = nil
}

func (e *groupConcat) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	trace_util_0.Count(_func_group_concat_00000, 13)
	p := (*partialResult4GroupConcat)(pr)
	v, isNull := "", false
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_group_concat_00000, 16)
		p.valsBuf.Reset()
		for _, arg := range e.args {
			trace_util_0.Count(_func_group_concat_00000, 20)
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				trace_util_0.Count(_func_group_concat_00000, 23)
				return err
			}
			trace_util_0.Count(_func_group_concat_00000, 21)
			if isNull {
				trace_util_0.Count(_func_group_concat_00000, 24)
				break
			}
			trace_util_0.Count(_func_group_concat_00000, 22)
			p.valsBuf.WriteString(v)
		}
		trace_util_0.Count(_func_group_concat_00000, 17)
		if isNull {
			trace_util_0.Count(_func_group_concat_00000, 25)
			continue
		}
		trace_util_0.Count(_func_group_concat_00000, 18)
		if p.buffer == nil {
			trace_util_0.Count(_func_group_concat_00000, 26)
			p.buffer = &bytes.Buffer{}
		} else {
			trace_util_0.Count(_func_group_concat_00000, 27)
			{
				p.buffer.WriteString(e.sep)
			}
		}
		trace_util_0.Count(_func_group_concat_00000, 19)
		p.buffer.WriteString(p.valsBuf.String())
	}
	trace_util_0.Count(_func_group_concat_00000, 14)
	if p.buffer != nil {
		trace_util_0.Count(_func_group_concat_00000, 28)
		return e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	trace_util_0.Count(_func_group_concat_00000, 15)
	return nil
}

func (e *groupConcat) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	trace_util_0.Count(_func_group_concat_00000, 29)
	p1, p2 := (*partialResult4GroupConcat)(src), (*partialResult4GroupConcat)(dst)
	if p1.buffer == nil {
		trace_util_0.Count(_func_group_concat_00000, 32)
		return nil
	}
	trace_util_0.Count(_func_group_concat_00000, 30)
	if p2.buffer == nil {
		trace_util_0.Count(_func_group_concat_00000, 33)
		p2.buffer = p1.buffer
		return nil
	}
	trace_util_0.Count(_func_group_concat_00000, 31)
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	return e.truncatePartialResultIfNeed(sctx, p2.buffer)
}

// SetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) SetTruncated(t *int32) {
	trace_util_0.Count(_func_group_concat_00000, 34)
	e.truncated = t
}

// GetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) GetTruncated() *int32 {
	trace_util_0.Count(_func_group_concat_00000, 35)
	return e.truncated
}

type partialResult4GroupConcatDistinct struct {
	basePartialResult4GroupConcat
	valSet            set.StringSet
	encodeBytesBuffer []byte
}

type groupConcatDistinct struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinct) AllocPartialResult() PartialResult {
	trace_util_0.Count(_func_group_concat_00000, 36)
	p := new(partialResult4GroupConcatDistinct)
	p.valsBuf = &bytes.Buffer{}
	p.valSet = set.NewStringSet()
	return PartialResult(p)
}

func (e *groupConcatDistinct) ResetPartialResult(pr PartialResult) {
	trace_util_0.Count(_func_group_concat_00000, 37)
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.buffer, p.valSet = nil, set.NewStringSet()
}

func (e *groupConcatDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	trace_util_0.Count(_func_group_concat_00000, 38)
	p := (*partialResult4GroupConcatDistinct)(pr)
	v, isNull := "", false
	for _, row := range rowsInGroup {
		trace_util_0.Count(_func_group_concat_00000, 41)
		p.valsBuf.Reset()
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for _, arg := range e.args {
			trace_util_0.Count(_func_group_concat_00000, 46)
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				trace_util_0.Count(_func_group_concat_00000, 49)
				return err
			}
			trace_util_0.Count(_func_group_concat_00000, 47)
			if isNull {
				trace_util_0.Count(_func_group_concat_00000, 50)
				break
			}
			trace_util_0.Count(_func_group_concat_00000, 48)
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, hack.Slice(v))
			p.valsBuf.WriteString(v)
		}
		trace_util_0.Count(_func_group_concat_00000, 42)
		if isNull {
			trace_util_0.Count(_func_group_concat_00000, 51)
			continue
		}
		trace_util_0.Count(_func_group_concat_00000, 43)
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			trace_util_0.Count(_func_group_concat_00000, 52)
			continue
		}
		trace_util_0.Count(_func_group_concat_00000, 44)
		p.valSet.Insert(joinedVal)
		// write separator
		if p.buffer == nil {
			trace_util_0.Count(_func_group_concat_00000, 53)
			p.buffer = &bytes.Buffer{}
		} else {
			trace_util_0.Count(_func_group_concat_00000, 54)
			{
				p.buffer.WriteString(e.sep)
			}
		}
		// write values
		trace_util_0.Count(_func_group_concat_00000, 45)
		p.buffer.WriteString(p.valsBuf.String())
	}
	trace_util_0.Count(_func_group_concat_00000, 39)
	if p.buffer != nil {
		trace_util_0.Count(_func_group_concat_00000, 55)
		return e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	trace_util_0.Count(_func_group_concat_00000, 40)
	return nil
}

// SetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) SetTruncated(t *int32) {
	trace_util_0.Count(_func_group_concat_00000, 56)
	e.truncated = t
}

// GetTruncated will be called in `executorBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) GetTruncated() *int32 {
	trace_util_0.Count(_func_group_concat_00000, 57)
	return e.truncated
}

var _func_group_concat_00000 = "executor/aggfuncs/func_group_concat.go"
