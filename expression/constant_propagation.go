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

package expression

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disjointset"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

type basePropConstSolver struct {
	colMapper map[int64]int       // colMapper maps column to its index
	eqList    []*Constant         // if eqList[i] != nil, it means col_i = eqList[i]
	unionSet  *disjointset.IntSet // unionSet stores the relations like col_i = col_j
	columns   []*Column           // columns stores all columns appearing in the conditions
	ctx       sessionctx.Context
}

func (s *basePropConstSolver) getColID(col *Column) int {
	trace_util_0.Count(_constant_propagation_00000, 0)
	return s.colMapper[col.UniqueID]
}

func (s *basePropConstSolver) insertCol(col *Column) {
	trace_util_0.Count(_constant_propagation_00000, 1)
	_, ok := s.colMapper[col.UniqueID]
	if !ok {
		trace_util_0.Count(_constant_propagation_00000, 2)
		s.colMapper[col.UniqueID] = len(s.colMapper)
		s.columns = append(s.columns, col)
	}
}

// tryToUpdateEQList tries to update the eqList. When the eqList has store this column with a different constant, like
// a = 1 and a = 2, we set the second return value to false.
func (s *basePropConstSolver) tryToUpdateEQList(col *Column, con *Constant) (bool, bool) {
	trace_util_0.Count(_constant_propagation_00000, 3)
	if con.Value.IsNull() {
		trace_util_0.Count(_constant_propagation_00000, 6)
		return false, true
	}
	trace_util_0.Count(_constant_propagation_00000, 4)
	id := s.getColID(col)
	oldCon := s.eqList[id]
	if oldCon != nil {
		trace_util_0.Count(_constant_propagation_00000, 7)
		return false, !oldCon.Equal(s.ctx, con)
	}
	trace_util_0.Count(_constant_propagation_00000, 5)
	s.eqList[id] = con
	return true, false
}

// validEqualCond checks if the cond is an expression like [column eq constant].
func validEqualCond(cond Expression) (*Column, *Constant) {
	trace_util_0.Count(_constant_propagation_00000, 8)
	if eq, ok := cond.(*ScalarFunction); ok {
		trace_util_0.Count(_constant_propagation_00000, 10)
		if eq.FuncName.L != ast.EQ {
			trace_util_0.Count(_constant_propagation_00000, 13)
			return nil, nil
		}
		trace_util_0.Count(_constant_propagation_00000, 11)
		if col, colOk := eq.GetArgs()[0].(*Column); colOk {
			trace_util_0.Count(_constant_propagation_00000, 14)
			if con, conOk := eq.GetArgs()[1].(*Constant); conOk {
				trace_util_0.Count(_constant_propagation_00000, 15)
				return col, con
			}
		}
		trace_util_0.Count(_constant_propagation_00000, 12)
		if col, colOk := eq.GetArgs()[1].(*Column); colOk {
			trace_util_0.Count(_constant_propagation_00000, 16)
			if con, conOk := eq.GetArgs()[0].(*Constant); conOk {
				trace_util_0.Count(_constant_propagation_00000, 17)
				return col, con
			}
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 9)
	return nil, nil
}

// tryToReplaceCond aims to replace all occurrences of column 'src' and try to replace it with 'tgt' in 'cond'
// It returns
//  bool: if a replacement happened
//  bool: if 'cond' contains non-deterministic expression
//  Expression: the replaced expression, or original 'cond' if the replacement didn't happen
//
// For example:
//  for 'a, b, a < 3', it returns 'true, false, b < 3'
//  for 'a, b, sin(a) + cos(a) = 5', it returns 'true, false, returns sin(b) + cos(b) = 5'
//  for 'a, b, cast(a) < rand()', it returns 'false, true, cast(a) < rand()'
func tryToReplaceCond(ctx sessionctx.Context, src *Column, tgt *Column, cond Expression) (bool, bool, Expression) {
	trace_util_0.Count(_constant_propagation_00000, 18)
	sf, ok := cond.(*ScalarFunction)
	if !ok {
		trace_util_0.Count(_constant_propagation_00000, 24)
		return false, false, cond
	}
	trace_util_0.Count(_constant_propagation_00000, 19)
	replaced := false
	var args []Expression
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		trace_util_0.Count(_constant_propagation_00000, 25)
		return false, true, cond
	}
	trace_util_0.Count(_constant_propagation_00000, 20)
	if _, ok := inequalFunctions[sf.FuncName.L]; ok {
		trace_util_0.Count(_constant_propagation_00000, 26)
		return false, true, cond
	}
	trace_util_0.Count(_constant_propagation_00000, 21)
	for idx, expr := range sf.GetArgs() {
		trace_util_0.Count(_constant_propagation_00000, 27)
		if src.Equal(nil, expr) {
			trace_util_0.Count(_constant_propagation_00000, 28)
			replaced = true
			if args == nil {
				trace_util_0.Count(_constant_propagation_00000, 30)
				args = make([]Expression, len(sf.GetArgs()))
				copy(args, sf.GetArgs())
			}
			trace_util_0.Count(_constant_propagation_00000, 29)
			args[idx] = tgt
		} else {
			trace_util_0.Count(_constant_propagation_00000, 31)
			{
				subReplaced, isNonDeterministic, subExpr := tryToReplaceCond(ctx, src, tgt, expr)
				if isNonDeterministic {
					trace_util_0.Count(_constant_propagation_00000, 32)
					return false, true, cond
				} else {
					trace_util_0.Count(_constant_propagation_00000, 33)
					if subReplaced {
						trace_util_0.Count(_constant_propagation_00000, 34)
						replaced = true
						if args == nil {
							trace_util_0.Count(_constant_propagation_00000, 36)
							args = make([]Expression, len(sf.GetArgs()))
							copy(args, sf.GetArgs())
						}
						trace_util_0.Count(_constant_propagation_00000, 35)
						args[idx] = subExpr
					}
				}
			}
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 22)
	if replaced {
		trace_util_0.Count(_constant_propagation_00000, 37)
		return true, false, NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), args...)
	}
	trace_util_0.Count(_constant_propagation_00000, 23)
	return false, false, cond
}

type propConstSolver struct {
	basePropConstSolver
	conditions []Expression
}

// propagateConstantEQ propagates expressions like 'column = constant' by substituting the constant for column, the
// procedure repeats multiple times. An example runs as following:
// a = d & b * 2 = c & c = d + 2 & b = 1 & a = 4, we pick eq cond b = 1 and a = 4
// d = 4 & 2 = c & c = d + 2 & b = 1 & a = 4, we propagate b = 1 and a = 4 and pick eq cond c = 2 and d = 4
// d = 4 & 2 = c & false & b = 1 & a = 4, we propagate c = 2 and d = 4, and do constant folding: c = d + 2 will be folded as false.
func (s *propConstSolver) propagateConstantEQ() {
	trace_util_0.Count(_constant_propagation_00000, 38)
	s.eqList = make([]*Constant, len(s.columns))
	visited := make([]bool, len(s.conditions))
	for i := 0; i < MaxPropagateColsCnt; i++ {
		trace_util_0.Count(_constant_propagation_00000, 39)
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			trace_util_0.Count(_constant_propagation_00000, 42)
			return
		}
		trace_util_0.Count(_constant_propagation_00000, 40)
		cols := make([]*Column, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			trace_util_0.Count(_constant_propagation_00000, 43)
			cols = append(cols, s.columns[id])
			cons = append(cons, con)
		}
		trace_util_0.Count(_constant_propagation_00000, 41)
		for i, cond := range s.conditions {
			trace_util_0.Count(_constant_propagation_00000, 44)
			if !visited[i] {
				trace_util_0.Count(_constant_propagation_00000, 45)
				s.conditions[i] = ColumnSubstitute(cond, NewSchema(cols...), cons)
			}
		}
	}
}

// propagateColumnEQ propagates expressions like 'column A = column B' by adding extra filters
// 'expression(..., column B, ...)' propagated from 'expression(..., column A, ...)' as long as:
//
//  1. The expression is deterministic
//  2. The expression doesn't have any side effect
//
// e.g. For expression a = b and b = c and c = d and c < 1 , we can get extra a < 1 and b < 1 and d < 1.
// However, for a = b and a < rand(), we cannot propagate a < rand() to b < rand() because rand() is non-deterministic
//
// This propagation may bring redundancies that we need to resolve later, for example:
// for a = b and a < 3 and b < 3, we get new a < 3 and b < 3, which are redundant
// for a = b and a < 3 and 3 > b, we get new b < 3 and 3 > a, which are redundant
// for a = b and a < 3 and b < 4, we get new a < 4 and b < 3 but should expect a < 3 and b < 3
// for a = b and a in (3) and b in (4), we get b in (3) and a in (4) but should expect 'false'
//
// TODO: remove redundancies later
//
// We maintain a unionSet representing the equivalent for every two columns.
func (s *propConstSolver) propagateColumnEQ() {
	trace_util_0.Count(_constant_propagation_00000, 46)
	visited := make([]bool, len(s.conditions))
	s.unionSet = disjointset.NewIntSet(len(s.columns))
	for i := range s.conditions {
		trace_util_0.Count(_constant_propagation_00000, 48)
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			trace_util_0.Count(_constant_propagation_00000, 49)
			lCol, lOk := fun.GetArgs()[0].(*Column)
			rCol, rOk := fun.GetArgs()[1].(*Column)
			if lOk && rOk {
				trace_util_0.Count(_constant_propagation_00000, 50)
				lID := s.getColID(lCol)
				rID := s.getColID(rCol)
				s.unionSet.Union(lID, rID)
				visited[i] = true
			}
		}
	}

	trace_util_0.Count(_constant_propagation_00000, 47)
	condsLen := len(s.conditions)
	for i, coli := range s.columns {
		trace_util_0.Count(_constant_propagation_00000, 51)
		for j := i + 1; j < len(s.columns); j++ {
			trace_util_0.Count(_constant_propagation_00000, 52)
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				trace_util_0.Count(_constant_propagation_00000, 54)
				continue
			}
			trace_util_0.Count(_constant_propagation_00000, 53)
			colj := s.columns[j]
			for k := 0; k < condsLen; k++ {
				trace_util_0.Count(_constant_propagation_00000, 55)
				if visited[k] {
					trace_util_0.Count(_constant_propagation_00000, 58)
					// cond_k has been used to retrieve equality relation
					continue
				}
				trace_util_0.Count(_constant_propagation_00000, 56)
				cond := s.conditions[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, coli, colj, cond)
				if replaced {
					trace_util_0.Count(_constant_propagation_00000, 59)
					s.conditions = append(s.conditions, newExpr)
				}
				trace_util_0.Count(_constant_propagation_00000, 57)
				replaced, _, newExpr = tryToReplaceCond(s.ctx, colj, coli, cond)
				if replaced {
					trace_util_0.Count(_constant_propagation_00000, 60)
					s.conditions = append(s.conditions, newExpr)
				}
			}
		}
	}
}

func (s *propConstSolver) setConds2ConstFalse() {
	trace_util_0.Count(_constant_propagation_00000, 61)
	s.conditions = []Expression{&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}}
}

// pickNewEQConds tries to pick new equal conds and puts them to retMapper.
func (s *propConstSolver) pickNewEQConds(visited []bool) (retMapper map[int]*Constant) {
	trace_util_0.Count(_constant_propagation_00000, 62)
	retMapper = make(map[int]*Constant)
	for i, cond := range s.conditions {
		trace_util_0.Count(_constant_propagation_00000, 64)
		if visited[i] {
			trace_util_0.Count(_constant_propagation_00000, 68)
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 65)
		col, con := validEqualCond(cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			trace_util_0.Count(_constant_propagation_00000, 69)
			if con, ok = cond.(*Constant); ok {
				trace_util_0.Count(_constant_propagation_00000, 71)
				value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
				if err != nil {
					trace_util_0.Count(_constant_propagation_00000, 73)
					terror.Log(err)
					return nil
				}
				trace_util_0.Count(_constant_propagation_00000, 72)
				if !value {
					trace_util_0.Count(_constant_propagation_00000, 74)
					s.setConds2ConstFalse()
					return nil
				}
			}
			trace_util_0.Count(_constant_propagation_00000, 70)
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 66)
		visited[i] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			trace_util_0.Count(_constant_propagation_00000, 75)
			s.setConds2ConstFalse()
			return nil
		}
		trace_util_0.Count(_constant_propagation_00000, 67)
		if updated {
			trace_util_0.Count(_constant_propagation_00000, 76)
			retMapper[s.getColID(col)] = con
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 63)
	return
}

func (s *propConstSolver) solve(conditions []Expression) []Expression {
	trace_util_0.Count(_constant_propagation_00000, 77)
	cols := make([]*Column, 0, len(conditions))
	for _, cond := range conditions {
		trace_util_0.Count(_constant_propagation_00000, 81)
		s.conditions = append(s.conditions, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	trace_util_0.Count(_constant_propagation_00000, 78)
	for _, col := range cols {
		trace_util_0.Count(_constant_propagation_00000, 82)
		s.insertCol(col)
	}
	trace_util_0.Count(_constant_propagation_00000, 79)
	if len(s.columns) > MaxPropagateColsCnt {
		trace_util_0.Count(_constant_propagation_00000, 83)
		logutil.Logger(context.Background()).Warn("too many columns in a single CNF",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return conditions
	}
	trace_util_0.Count(_constant_propagation_00000, 80)
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.conditions = propagateConstantDNF(s.ctx, s.conditions)
	return s.conditions
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	trace_util_0.Count(_constant_propagation_00000, 84)
	return newPropConstSolver().PropagateConstant(ctx, conditions)
}

type propOuterJoinConstSolver struct {
	basePropConstSolver
	joinConds   []Expression
	filterConds []Expression
	outerSchema *Schema
	innerSchema *Schema
	// nullSensitive indicates if this outer join is null sensitive, if true, we cannot generate
	// additional `col is not null` condition from column equal conditions. Specifically, this value
	// is true for LeftOuterSemiJoin and AntiLeftOuterSemiJoin.
	nullSensitive bool
}

func (s *propOuterJoinConstSolver) setConds2ConstFalse(filterConds bool) {
	trace_util_0.Count(_constant_propagation_00000, 85)
	s.joinConds = []Expression{&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}}
	if filterConds {
		trace_util_0.Count(_constant_propagation_00000, 86)
		s.filterConds = []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}
}

// pickEQCondsOnOuterCol picks constant equal expression from specified conditions.
func (s *propOuterJoinConstSolver) pickEQCondsOnOuterCol(retMapper map[int]*Constant, visited []bool, filterConds bool) map[int]*Constant {
	trace_util_0.Count(_constant_propagation_00000, 87)
	var conds []Expression
	var condsOffset int
	if filterConds {
		trace_util_0.Count(_constant_propagation_00000, 90)
		conds = s.filterConds
	} else {
		trace_util_0.Count(_constant_propagation_00000, 91)
		{
			conds = s.joinConds
			condsOffset = len(s.filterConds)
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 88)
	for i, cond := range conds {
		trace_util_0.Count(_constant_propagation_00000, 92)
		if visited[i+condsOffset] {
			trace_util_0.Count(_constant_propagation_00000, 97)
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 93)
		col, con := validEqualCond(cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			trace_util_0.Count(_constant_propagation_00000, 98)
			if con, ok = cond.(*Constant); ok {
				trace_util_0.Count(_constant_propagation_00000, 100)
				value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
				if err != nil {
					trace_util_0.Count(_constant_propagation_00000, 102)
					terror.Log(err)
					return nil
				}
				trace_util_0.Count(_constant_propagation_00000, 101)
				if !value {
					trace_util_0.Count(_constant_propagation_00000, 103)
					s.setConds2ConstFalse(filterConds)
					return nil
				}
			}
			trace_util_0.Count(_constant_propagation_00000, 99)
			continue
		}
		// Only extract `outerCol = const` expressions.
		trace_util_0.Count(_constant_propagation_00000, 94)
		if !s.outerSchema.Contains(col) {
			trace_util_0.Count(_constant_propagation_00000, 104)
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 95)
		visited[i+condsOffset] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			trace_util_0.Count(_constant_propagation_00000, 105)
			s.setConds2ConstFalse(filterConds)
			return nil
		}
		trace_util_0.Count(_constant_propagation_00000, 96)
		if updated {
			trace_util_0.Count(_constant_propagation_00000, 106)
			retMapper[s.getColID(col)] = con
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 89)
	return retMapper
}

// pickNewEQConds picks constant equal expressions from join and filter conditions.
func (s *propOuterJoinConstSolver) pickNewEQConds(visited []bool) map[int]*Constant {
	trace_util_0.Count(_constant_propagation_00000, 107)
	retMapper := make(map[int]*Constant)
	retMapper = s.pickEQCondsOnOuterCol(retMapper, visited, true)
	if retMapper == nil {
		trace_util_0.Count(_constant_propagation_00000, 109)
		// Filter is constant false or error occurred, enforce early termination.
		return nil
	}
	trace_util_0.Count(_constant_propagation_00000, 108)
	retMapper = s.pickEQCondsOnOuterCol(retMapper, visited, false)
	return retMapper
}

// propagateConstantEQ propagates expressions like `outerCol = const` by substituting `outerCol` in *JOIN* condition
// with `const`, the procedure repeats multiple times.
func (s *propOuterJoinConstSolver) propagateConstantEQ() {
	trace_util_0.Count(_constant_propagation_00000, 110)
	s.eqList = make([]*Constant, len(s.columns))
	lenFilters := len(s.filterConds)
	visited := make([]bool, lenFilters+len(s.joinConds))
	for i := 0; i < MaxPropagateColsCnt; i++ {
		trace_util_0.Count(_constant_propagation_00000, 111)
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			trace_util_0.Count(_constant_propagation_00000, 114)
			return
		}
		trace_util_0.Count(_constant_propagation_00000, 112)
		cols := make([]*Column, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			trace_util_0.Count(_constant_propagation_00000, 115)
			cols = append(cols, s.columns[id])
			cons = append(cons, con)
		}
		trace_util_0.Count(_constant_propagation_00000, 113)
		for i, cond := range s.joinConds {
			trace_util_0.Count(_constant_propagation_00000, 116)
			if !visited[i+lenFilters] {
				trace_util_0.Count(_constant_propagation_00000, 117)
				s.joinConds[i] = ColumnSubstitute(cond, NewSchema(cols...), cons)
			}
		}
	}
}

func (s *propOuterJoinConstSolver) colsFromOuterAndInner(col1, col2 *Column) (*Column, *Column) {
	trace_util_0.Count(_constant_propagation_00000, 118)
	if s.outerSchema.Contains(col1) && s.innerSchema.Contains(col2) {
		trace_util_0.Count(_constant_propagation_00000, 121)
		return col1, col2
	}
	trace_util_0.Count(_constant_propagation_00000, 119)
	if s.outerSchema.Contains(col2) && s.innerSchema.Contains(col1) {
		trace_util_0.Count(_constant_propagation_00000, 122)
		return col2, col1
	}
	trace_util_0.Count(_constant_propagation_00000, 120)
	return nil, nil
}

// validColEqualCond checks if expression is column equal condition that we can use for constant
// propagation over outer join. We only use expression like `outerCol = innerCol`, for expressions like
// `outerCol1 = outerCol2` or `innerCol1 = innerCol2`, they do not help deriving new inner table conditions
// which can be pushed down to children plan nodes, so we do not pick them.
func (s *propOuterJoinConstSolver) validColEqualCond(cond Expression) (*Column, *Column) {
	trace_util_0.Count(_constant_propagation_00000, 123)
	if fun, ok := cond.(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
		trace_util_0.Count(_constant_propagation_00000, 125)
		lCol, lOk := fun.GetArgs()[0].(*Column)
		rCol, rOk := fun.GetArgs()[1].(*Column)
		if lOk && rOk {
			trace_util_0.Count(_constant_propagation_00000, 126)
			return s.colsFromOuterAndInner(lCol, rCol)
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 124)
	return nil, nil

}

// deriveConds given `outerCol = innerCol`, derive new expression for specified conditions.
func (s *propOuterJoinConstSolver) deriveConds(outerCol, innerCol *Column, schema *Schema, fCondsOffset int, visited []bool, filterConds bool) []bool {
	trace_util_0.Count(_constant_propagation_00000, 127)
	var offset, condsLen int
	var conds []Expression
	if filterConds {
		trace_util_0.Count(_constant_propagation_00000, 130)
		conds = s.filterConds
		offset = fCondsOffset
		condsLen = len(s.filterConds)
	} else {
		trace_util_0.Count(_constant_propagation_00000, 131)
		{
			conds = s.joinConds
			condsLen = fCondsOffset
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 128)
	for k := 0; k < condsLen; k++ {
		trace_util_0.Count(_constant_propagation_00000, 132)
		if visited[k+offset] {
			trace_util_0.Count(_constant_propagation_00000, 135)
			// condition has been used to retrieve equality relation or contains column beyond children schema.
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 133)
		cond := conds[k]
		if !ExprFromSchema(cond, schema) {
			trace_util_0.Count(_constant_propagation_00000, 136)
			visited[k+offset] = true
			continue
		}
		trace_util_0.Count(_constant_propagation_00000, 134)
		replaced, _, newExpr := tryToReplaceCond(s.ctx, outerCol, innerCol, cond)
		if replaced {
			trace_util_0.Count(_constant_propagation_00000, 137)
			s.joinConds = append(s.joinConds, newExpr)
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 129)
	return visited
}

// propagateColumnEQ propagates expressions like 'outerCol = innerCol' by adding extra filters
// 'expression(..., innerCol, ...)' derived from 'expression(..., outerCol, ...)' as long as
// 'expression(..., outerCol, ...)' does not reference columns outside children schemas of join node.
// Derived new expressions must be appended into join condition, not filter condition.
func (s *propOuterJoinConstSolver) propagateColumnEQ() {
	trace_util_0.Count(_constant_propagation_00000, 138)
	visited := make([]bool, 2*len(s.joinConds)+len(s.filterConds))
	s.unionSet = disjointset.NewIntSet(len(s.columns))
	var outerCol, innerCol *Column
	// Only consider column equal condition in joinConds.
	// If we have column equal in filter condition, the outer join should have been simplified already.
	for i := range s.joinConds {
		trace_util_0.Count(_constant_propagation_00000, 140)
		outerCol, innerCol = s.validColEqualCond(s.joinConds[i])
		if outerCol != nil {
			trace_util_0.Count(_constant_propagation_00000, 141)
			outerID := s.getColID(outerCol)
			innerID := s.getColID(innerCol)
			s.unionSet.Union(outerID, innerID)
			visited[i] = true
			// Generate `innerCol is not null` from `outerCol = innerCol`. Note that `outerCol is not null`
			// does not hold since we are in outer join.
			// For AntiLeftOuterSemiJoin, this does not work, for example:
			// `select *, t1.a not in (select t2.b from t t2) from t t1` does not imply `t2.b is not null`.
			// For LeftOuterSemiJoin, this does not work either, for example:
			// `select *, t1.a in (select t2.b from t t2) from t t1`
			// rows with t2.b is null would impact whether LeftOuterSemiJoin should output 0 or null if there
			// is no row satisfying t2.b = t1.a
			if s.nullSensitive {
				trace_util_0.Count(_constant_propagation_00000, 143)
				continue
			}
			trace_util_0.Count(_constant_propagation_00000, 142)
			childCol := s.innerSchema.RetrieveColumn(innerCol)
			if !mysql.HasNotNullFlag(childCol.RetType.Flag) {
				trace_util_0.Count(_constant_propagation_00000, 144)
				notNullExpr := BuildNotNullExpr(s.ctx, childCol)
				s.joinConds = append(s.joinConds, notNullExpr)
			}
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 139)
	lenJoinConds := len(s.joinConds)
	mergedSchema := MergeSchema(s.outerSchema, s.innerSchema)
	for i, coli := range s.columns {
		trace_util_0.Count(_constant_propagation_00000, 145)
		for j := i + 1; j < len(s.columns); j++ {
			trace_util_0.Count(_constant_propagation_00000, 146)
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation.
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				trace_util_0.Count(_constant_propagation_00000, 149)
				continue
			}
			trace_util_0.Count(_constant_propagation_00000, 147)
			colj := s.columns[j]
			outerCol, innerCol = s.colsFromOuterAndInner(coli, colj)
			if outerCol == nil {
				trace_util_0.Count(_constant_propagation_00000, 150)
				continue
			}
			trace_util_0.Count(_constant_propagation_00000, 148)
			visited = s.deriveConds(outerCol, innerCol, mergedSchema, lenJoinConds, visited, false)
			visited = s.deriveConds(outerCol, innerCol, mergedSchema, lenJoinConds, visited, true)
		}
	}
}

func (s *propOuterJoinConstSolver) solve(joinConds, filterConds []Expression) ([]Expression, []Expression) {
	trace_util_0.Count(_constant_propagation_00000, 151)
	cols := make([]*Column, 0, len(joinConds)+len(filterConds))
	for _, cond := range joinConds {
		trace_util_0.Count(_constant_propagation_00000, 156)
		s.joinConds = append(s.joinConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	trace_util_0.Count(_constant_propagation_00000, 152)
	for _, cond := range filterConds {
		trace_util_0.Count(_constant_propagation_00000, 157)
		s.filterConds = append(s.filterConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	trace_util_0.Count(_constant_propagation_00000, 153)
	for _, col := range cols {
		trace_util_0.Count(_constant_propagation_00000, 158)
		s.insertCol(col)
	}
	trace_util_0.Count(_constant_propagation_00000, 154)
	if len(s.columns) > MaxPropagateColsCnt {
		trace_util_0.Count(_constant_propagation_00000, 159)
		logutil.Logger(context.Background()).Warn("too many columns",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return joinConds, filterConds
	}
	trace_util_0.Count(_constant_propagation_00000, 155)
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.joinConds = propagateConstantDNF(s.ctx, s.joinConds)
	s.filterConds = propagateConstantDNF(s.ctx, s.filterConds)
	return s.joinConds, s.filterConds
}

// propagateConstantDNF find DNF item from CNF, and propagate constant inside DNF.
func propagateConstantDNF(ctx sessionctx.Context, conds []Expression) []Expression {
	trace_util_0.Count(_constant_propagation_00000, 160)
	for i, cond := range conds {
		trace_util_0.Count(_constant_propagation_00000, 162)
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			trace_util_0.Count(_constant_propagation_00000, 163)
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				trace_util_0.Count(_constant_propagation_00000, 165)
				dnfItems[j] = ComposeCNFCondition(ctx, PropagateConstant(ctx, []Expression{item})...)
			}
			trace_util_0.Count(_constant_propagation_00000, 164)
			conds[i] = ComposeDNFCondition(ctx, dnfItems...)
		}
	}
	trace_util_0.Count(_constant_propagation_00000, 161)
	return conds
}

// PropConstOverOuterJoin propagate constant equal and column equal conditions over outer join.
// First step is to extract `outerCol = const` from join conditions and filter conditions,
// and substitute `outerCol` in join conditions with `const`;
// Second step is to extract `outerCol = innerCol` from join conditions, and derive new join
// conditions based on this column equal condition and `outerCol` related
// expressions in join conditions and filter conditions;
func PropConstOverOuterJoin(ctx sessionctx.Context, joinConds, filterConds []Expression,
	outerSchema, innerSchema *Schema, nullSensitive bool) ([]Expression, []Expression) {
	trace_util_0.Count(_constant_propagation_00000, 166)
	solver := &propOuterJoinConstSolver{
		outerSchema:   outerSchema,
		innerSchema:   innerSchema,
		nullSensitive: nullSensitive,
	}
	solver.colMapper = make(map[int64]int)
	solver.ctx = ctx
	return solver.solve(joinConds, filterConds)
}

// PropagateConstantSolver is a constant propagate solver.
type PropagateConstantSolver interface {
	PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression
}

// newPropConstSolver returns a PropagateConstantSolver.
func newPropConstSolver() PropagateConstantSolver {
	trace_util_0.Count(_constant_propagation_00000, 167)
	solver := &propConstSolver{}
	solver.colMapper = make(map[int64]int)
	return solver
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func (s *propConstSolver) PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	trace_util_0.Count(_constant_propagation_00000, 168)
	s.ctx = ctx
	return s.solve(conditions)
}

var _constant_propagation_00000 = "expression/constant_propagation.go"
