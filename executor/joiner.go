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

package executor

import (
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ joiner = &semiJoiner{}
	_ joiner = &antiSemiJoiner{}
	_ joiner = &leftOuterSemiJoiner{}
	_ joiner = &antiLeftOuterSemiJoiner{}
	_ joiner = &leftOuterJoiner{}
	_ joiner = &rightOuterJoiner{}
	_ joiner = &innerJoiner{}
)

// joiner is used to generate join results according to the join type.
// A typical instruction flow is:
//
//     hasMatch, hasNull := false, false
//     for innerIter.Current() != innerIter.End() {
//         matched, isNull, err := j.tryToMatch(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//         hasNull = hasNull || isNull
//     }
//     if !hasMatch {
//         j.onMissMatch(hasNull, outer, chk)
//     }
//
// NOTE: This interface is **not** thread-safe.
type joiner interface {
	// tryToMatch tries to join an outer row with a batch of inner rows. When
	// 'inners.Len != 0' but all the joined rows are filtered, the outer row is
	// considered unmatched. Otherwise, the outer row is matched and some joined
	// rows are appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	// Note that when the outer row is considered unmatched, we need to differentiate
	// whether the join conditions return null or false, because that matters for
	// AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemijoin, and the result is reflected
	// by the second return value; for other join types, we always return false.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer row, and dicide whether the outer row can be
	// matched with at lease one inner row.
	tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, bool, error)

	// onMissMatch operates on the unmatched outer row according to the join
	// type. An outer row can be considered miss matched if:
	//   1. it can not pass the filter on the outer table side.
	//   2. there is no inner row with the same join key.
	//   3. all the joined rows can not pass the filter on the join result.
	//
	// On these conditions, the caller calls this function to handle the
	// unmatched outer rows according to the current join type:
	//   1. 'SemiJoin': ignores the unmatched outer row.
	//   2. 'AntiSemiJoin': appends the unmatched outer row to the result buffer.
	//   3. 'LeftOuterSemiJoin': concats the unmatched outer row with 0 and
	//      appends it to the result buffer.
	//   4. 'AntiLeftOuterSemiJoin': concats the unmatched outer row with 0 and
	//      appends it to the result buffer.
	//   5. 'LeftOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   6. 'RightOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   7. 'InnerJoin': ignores the unmatched outer row.
	//
	// Note that, for LeftOuterSemiJoin, AntiSemiJoin and AntiLeftOuterSemiJoin,
	// we need to know the reason of outer row being treated as unmatched:
	// whether the join condition returns false, or returns null, because
	// it decides if this outer row should be outputted, hence we have a `hasNull`
	// parameter passed to `onMissMatch`.
	onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk)
}

func newJoiner(ctx sessionctx.Context, joinType plannercore.JoinType,
	outerIsRight bool, defaultInner []types.Datum, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) joiner {
	trace_util_0.Count(_joiner_00000, 0)
	base := baseJoiner{
		ctx:          ctx,
		conditions:   filter,
		outerIsRight: outerIsRight,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	colTypes := make([]*types.FieldType, 0, len(lhsColTypes)+len(rhsColTypes))
	colTypes = append(colTypes, lhsColTypes...)
	colTypes = append(colTypes, rhsColTypes...)
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	if joinType == plannercore.LeftOuterJoin || joinType == plannercore.RightOuterJoin {
		trace_util_0.Count(_joiner_00000, 3)
		innerColTypes := lhsColTypes
		if !outerIsRight {
			trace_util_0.Count(_joiner_00000, 5)
			innerColTypes = rhsColTypes
		}
		trace_util_0.Count(_joiner_00000, 4)
		base.initDefaultInner(innerColTypes, defaultInner)
	}
	trace_util_0.Count(_joiner_00000, 1)
	switch joinType {
	case plannercore.SemiJoin:
		trace_util_0.Count(_joiner_00000, 6)
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &semiJoiner{base}
	case plannercore.AntiSemiJoin:
		trace_util_0.Count(_joiner_00000, 7)
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &antiSemiJoiner{base}
	case plannercore.LeftOuterSemiJoin:
		trace_util_0.Count(_joiner_00000, 8)
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &leftOuterSemiJoiner{base}
	case plannercore.AntiLeftOuterSemiJoin:
		trace_util_0.Count(_joiner_00000, 9)
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &antiLeftOuterSemiJoiner{base}
	case plannercore.LeftOuterJoin:
		trace_util_0.Count(_joiner_00000, 10)
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &leftOuterJoiner{base}
	case plannercore.RightOuterJoin:
		trace_util_0.Count(_joiner_00000, 11)
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &rightOuterJoiner{base}
	case plannercore.InnerJoin:
		trace_util_0.Count(_joiner_00000, 12)
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &innerJoiner{base}
	}
	trace_util_0.Count(_joiner_00000, 2)
	panic("unsupported join type in func newJoiner()")
}

type baseJoiner struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	shallowRow   chunk.MutRow
	selected     []bool
	maxChunkSize int
}

func (j *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
	trace_util_0.Count(_joiner_00000, 13)
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	j.defaultInner = mutableRow.ToRow()
}

func (j *baseJoiner) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	trace_util_0.Count(_joiner_00000, 14)
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

// makeShallowJoinRow shallow copies `inner` and `outer` into `shallowRow`.
func (j *baseJoiner) makeShallowJoinRow(isRightJoin bool, inner, outer chunk.Row) {
	trace_util_0.Count(_joiner_00000, 15)
	if !isRightJoin {
		trace_util_0.Count(_joiner_00000, 17)
		inner, outer = outer, inner
	}
	trace_util_0.Count(_joiner_00000, 16)
	j.shallowRow.ShallowCopyPartialRow(0, inner)
	j.shallowRow.ShallowCopyPartialRow(inner.Len(), outer)
}

func (j *baseJoiner) filter(input, output *chunk.Chunk, outerColsLen int) (bool, error) {
	trace_util_0.Count(_joiner_00000, 18)
	var err error
	j.selected, err = expression.VectorizedFilter(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		trace_util_0.Count(_joiner_00000, 21)
		return false, err
	}
	// Batch copies selected rows to output chunk.
	trace_util_0.Count(_joiner_00000, 19)
	innerColOffset, outerColOffset := 0, input.NumCols()-outerColsLen
	if !j.outerIsRight {
		trace_util_0.Count(_joiner_00000, 22)
		innerColOffset, outerColOffset = outerColsLen, 0
	}
	trace_util_0.Count(_joiner_00000, 20)
	return chunk.CopySelectedJoinRows(input, innerColOffset, outerColOffset, j.selected, output), nil
}

type semiJoiner struct {
	baseJoiner
}

func (j *semiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	trace_util_0.Count(_joiner_00000, 23)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 27)
		return false, false, nil
	}

	trace_util_0.Count(_joiner_00000, 24)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 28)
		chk.AppendPartialRow(0, outer)
		inners.ReachEnd()
		return true, false, nil
	}

	trace_util_0.Count(_joiner_00000, 25)
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		trace_util_0.Count(_joiner_00000, 29)
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err = expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			trace_util_0.Count(_joiner_00000, 31)
			return false, false, err
		}
		trace_util_0.Count(_joiner_00000, 30)
		if matched {
			trace_util_0.Count(_joiner_00000, 32)
			chk.AppendPartialRow(0, outer)
			inners.ReachEnd()
			return true, false, nil
		}
	}
	trace_util_0.Count(_joiner_00000, 26)
	return false, false, nil
}

func (j *semiJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 33)
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *antiSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	trace_util_0.Count(_joiner_00000, 34)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 38)
		return false, false, nil
	}

	trace_util_0.Count(_joiner_00000, 35)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 39)
		inners.ReachEnd()
		return true, false, nil
	}

	trace_util_0.Count(_joiner_00000, 36)
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		trace_util_0.Count(_joiner_00000, 40)
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			trace_util_0.Count(_joiner_00000, 43)
			return false, false, err
		}
		trace_util_0.Count(_joiner_00000, 41)
		if matched {
			trace_util_0.Count(_joiner_00000, 44)
			inners.ReachEnd()
			return true, false, nil
		}
		trace_util_0.Count(_joiner_00000, 42)
		hasNull = hasNull || isNull
	}
	trace_util_0.Count(_joiner_00000, 37)
	return false, hasNull, nil
}

func (j *antiSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 45)
	if !hasNull {
		trace_util_0.Count(_joiner_00000, 46)
		chk.AppendRow(outer)
	}
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *leftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	trace_util_0.Count(_joiner_00000, 47)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 51)
		return false, false, nil
	}

	trace_util_0.Count(_joiner_00000, 48)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 52)
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	trace_util_0.Count(_joiner_00000, 49)
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		trace_util_0.Count(_joiner_00000, 53)
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			trace_util_0.Count(_joiner_00000, 56)
			return false, false, err
		}
		trace_util_0.Count(_joiner_00000, 54)
		if matched {
			trace_util_0.Count(_joiner_00000, 57)
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		trace_util_0.Count(_joiner_00000, 55)
		hasNull = hasNull || isNull
	}
	trace_util_0.Count(_joiner_00000, 50)
	return false, hasNull, nil
}

func (j *leftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 58)
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

func (j *leftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 59)
	chk.AppendPartialRow(0, outer)
	if hasNull {
		trace_util_0.Count(_joiner_00000, 60)
		chk.AppendNull(outer.Len())
	} else {
		trace_util_0.Count(_joiner_00000, 61)
		{
			chk.AppendInt64(outer.Len(), 0)
		}
	}
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *antiLeftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	trace_util_0.Count(_joiner_00000, 62)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 66)
		return false, false, nil
	}

	trace_util_0.Count(_joiner_00000, 63)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 67)
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	trace_util_0.Count(_joiner_00000, 64)
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		trace_util_0.Count(_joiner_00000, 68)
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			trace_util_0.Count(_joiner_00000, 71)
			return false, false, err
		}
		trace_util_0.Count(_joiner_00000, 69)
		if matched {
			trace_util_0.Count(_joiner_00000, 72)
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		trace_util_0.Count(_joiner_00000, 70)
		hasNull = hasNull || isNull
	}
	trace_util_0.Count(_joiner_00000, 65)
	return false, hasNull, nil
}

func (j *antiLeftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 73)
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

func (j *antiLeftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 74)
	chk.AppendPartialRow(0, outer)
	if hasNull {
		trace_util_0.Count(_joiner_00000, 75)
		chk.AppendNull(outer.Len())
	} else {
		trace_util_0.Count(_joiner_00000, 76)
		{
			chk.AppendInt64(outer.Len(), 1)
		}
	}
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *leftOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, bool, error) {
	trace_util_0.Count(_joiner_00000, 77)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 83)
		return false, false, nil
	}
	trace_util_0.Count(_joiner_00000, 78)
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 84)
		chkForJoin = chk
	}

	trace_util_0.Count(_joiner_00000, 79)
	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		trace_util_0.Count(_joiner_00000, 85)
		j.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	trace_util_0.Count(_joiner_00000, 80)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 86)
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	trace_util_0.Count(_joiner_00000, 81)
	matched, err := j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		trace_util_0.Count(_joiner_00000, 87)
		return false, false, err
	}
	trace_util_0.Count(_joiner_00000, 82)
	return matched, false, nil
}

func (j *leftOuterJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 88)
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), j.defaultInner)
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *rightOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, bool, error) {
	trace_util_0.Count(_joiner_00000, 89)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 95)
		return false, false, nil
	}

	trace_util_0.Count(_joiner_00000, 90)
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 96)
		chkForJoin = chk
	}

	trace_util_0.Count(_joiner_00000, 91)
	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		trace_util_0.Count(_joiner_00000, 97)
		j.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	trace_util_0.Count(_joiner_00000, 92)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 98)
		return true, false, nil
	}

	trace_util_0.Count(_joiner_00000, 93)
	matched, err := j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		trace_util_0.Count(_joiner_00000, 99)
		return false, false, err
	}
	trace_util_0.Count(_joiner_00000, 94)
	return matched, false, nil
}

func (j *rightOuterJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 100)
	chk.AppendPartialRow(0, j.defaultInner)
	chk.AppendPartialRow(j.defaultInner.Len(), outer)
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *innerJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, bool, error) {
	trace_util_0.Count(_joiner_00000, 101)
	if inners.Len() == 0 {
		trace_util_0.Count(_joiner_00000, 107)
		return false, false, nil
	}
	trace_util_0.Count(_joiner_00000, 102)
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 108)
		chkForJoin = chk
	}
	trace_util_0.Count(_joiner_00000, 103)
	inner, numToAppend := inners.Current(), chk.RequiredRows()-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		trace_util_0.Count(_joiner_00000, 109)
		if j.outerIsRight {
			trace_util_0.Count(_joiner_00000, 110)
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			trace_util_0.Count(_joiner_00000, 111)
			{
				j.makeJoinRowToChunk(chkForJoin, outer, inner)
			}
		}
	}
	trace_util_0.Count(_joiner_00000, 104)
	if len(j.conditions) == 0 {
		trace_util_0.Count(_joiner_00000, 112)
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	trace_util_0.Count(_joiner_00000, 105)
	matched, err := j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		trace_util_0.Count(_joiner_00000, 113)
		return false, false, err
	}
	trace_util_0.Count(_joiner_00000, 106)
	return matched, false, nil
}

func (j *innerJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	trace_util_0.Count(_joiner_00000, 114)
}

var _joiner_00000 = "executor/joiner.go"
