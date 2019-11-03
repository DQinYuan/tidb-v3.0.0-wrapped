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

package expression

import (
	"bytes"
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// exprSet is a Set container for expressions, each expression in it is unique.
// `tombstone` is deleted mark, if tombstone[i] is true, data[i] is invalid.
// `index` use expr.HashCode() as key, to implement the unique property.
type exprSet struct {
	data       []Expression
	tombstone  []bool
	exists     map[string]struct{}
	constfalse bool
}

func (s *exprSet) Append(sc *stmtctx.StatementContext, e Expression) bool {
	trace_util_0.Count(_constraint_propagation_00000, 0)
	if _, ok := s.exists[string(e.HashCode(sc))]; ok {
		trace_util_0.Count(_constraint_propagation_00000, 2)
		return false
	}

	trace_util_0.Count(_constraint_propagation_00000, 1)
	s.data = append(s.data, e)
	s.tombstone = append(s.tombstone, false)
	s.exists[string(e.HashCode(sc))] = struct{}{}
	return true
}

// Slice returns the valid expressions in the exprSet, this function has side effect.
func (s *exprSet) Slice() []Expression {
	trace_util_0.Count(_constraint_propagation_00000, 3)
	if s.constfalse {
		trace_util_0.Count(_constraint_propagation_00000, 6)
		return []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}

	trace_util_0.Count(_constraint_propagation_00000, 4)
	idx := 0
	for i := 0; i < len(s.data); i++ {
		trace_util_0.Count(_constraint_propagation_00000, 7)
		if !s.tombstone[i] {
			trace_util_0.Count(_constraint_propagation_00000, 8)
			s.data[idx] = s.data[i]
			idx++
		}
	}
	trace_util_0.Count(_constraint_propagation_00000, 5)
	return s.data[:idx]
}

func (s *exprSet) SetConstFalse() {
	trace_util_0.Count(_constraint_propagation_00000, 9)
	s.constfalse = true
}

func newExprSet(ctx sessionctx.Context, conditions []Expression) *exprSet {
	trace_util_0.Count(_constraint_propagation_00000, 10)
	var exprs exprSet
	exprs.data = make([]Expression, 0, len(conditions))
	exprs.tombstone = make([]bool, 0, len(conditions))
	exprs.exists = make(map[string]struct{}, len(conditions))
	sc := ctx.GetSessionVars().StmtCtx
	for _, v := range conditions {
		trace_util_0.Count(_constraint_propagation_00000, 12)
		exprs.Append(sc, v)
	}
	trace_util_0.Count(_constraint_propagation_00000, 11)
	return &exprs
}

type constraintSolver []constraintPropagateRule

func newConstraintSolver(rules ...constraintPropagateRule) constraintSolver {
	trace_util_0.Count(_constraint_propagation_00000, 13)
	return constraintSolver(rules)
}

type pgSolver2 struct{}

func (s pgSolver2) PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	trace_util_0.Count(_constraint_propagation_00000, 14)
	solver := newConstraintSolver(ruleConstantFalse, ruleColumnEQConst)
	return solver.Solve(ctx, conditions)
}

// Solve propagate constraint according to the rules in the constraintSolver.
func (s constraintSolver) Solve(ctx sessionctx.Context, conditions []Expression) []Expression {
	trace_util_0.Count(_constraint_propagation_00000, 15)
	exprs := newExprSet(ctx, conditions)
	s.fixPoint(ctx, exprs)
	return exprs.Slice()
}

// fixPoint is the core of the constraint propagation algorithm.
// It will iterate the expression set over and over again, pick two expressions,
// apply one to another.
// If new conditions can be inferred, they will be append into the expression set.
// Until no more conditions can be inferred from the set, the algorithm finish.
func (s constraintSolver) fixPoint(ctx sessionctx.Context, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 16)
	for {
		trace_util_0.Count(_constraint_propagation_00000, 17)
		saveLen := len(exprs.data)
		s.iterOnce(ctx, exprs)
		if saveLen == len(exprs.data) {
			trace_util_0.Count(_constraint_propagation_00000, 18)
			break
		}
	}
}

// iterOnce picks two expressions from the set, try to propagate new conditions from them.
func (s constraintSolver) iterOnce(ctx sessionctx.Context, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 19)
	for i := 0; i < len(exprs.data); i++ {
		trace_util_0.Count(_constraint_propagation_00000, 20)
		if exprs.tombstone[i] {
			trace_util_0.Count(_constraint_propagation_00000, 22)
			continue
		}
		trace_util_0.Count(_constraint_propagation_00000, 21)
		for j := 0; j < len(exprs.data); j++ {
			trace_util_0.Count(_constraint_propagation_00000, 23)
			if exprs.tombstone[j] {
				trace_util_0.Count(_constraint_propagation_00000, 26)
				continue
			}
			trace_util_0.Count(_constraint_propagation_00000, 24)
			if i == j {
				trace_util_0.Count(_constraint_propagation_00000, 27)
				continue
			}
			trace_util_0.Count(_constraint_propagation_00000, 25)
			s.solve(ctx, i, j, exprs)
		}
	}
}

// solve uses exprs[i] exprs[j] to propagate new conditions.
func (s constraintSolver) solve(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 28)
	for _, rule := range s {
		trace_util_0.Count(_constraint_propagation_00000, 29)
		rule(ctx, i, j, exprs)
	}
}

type constraintPropagateRule func(ctx sessionctx.Context, i, j int, exprs *exprSet)

// ruleConstantFalse propagates from CNF condition that false plus anything returns false.
// false, a = 1, b = c ... => false
func ruleConstantFalse(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 30)
	cond := exprs.data[i]
	if cons, ok := cond.(*Constant); ok {
		trace_util_0.Count(_constraint_propagation_00000, 31)
		v, isNull, err := cons.EvalInt(ctx, chunk.Row{})
		if err != nil {
			trace_util_0.Count(_constraint_propagation_00000, 33)
			logutil.Logger(context.Background()).Warn("eval constant", zap.Error(err))
			return
		}
		trace_util_0.Count(_constraint_propagation_00000, 32)
		if !isNull && v == 0 {
			trace_util_0.Count(_constraint_propagation_00000, 34)
			exprs.SetConstFalse()
		}
	}
}

// ruleColumnEQConst propagates the "column = const" condition.
// "a = 3, b = a, c = a, d = b" => "a = 3, b = 3, c = 3, d = 3"
func ruleColumnEQConst(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 35)
	col, cons := validEqualCond(exprs.data[i])
	if col != nil {
		trace_util_0.Count(_constraint_propagation_00000, 36)
		expr := ColumnSubstitute(exprs.data[j], NewSchema(col), []Expression{cons})
		stmtctx := ctx.GetSessionVars().StmtCtx
		if !bytes.Equal(expr.HashCode(stmtctx), exprs.data[j].HashCode(stmtctx)) {
			trace_util_0.Count(_constraint_propagation_00000, 37)
			exprs.Append(stmtctx, expr)
			exprs.tombstone[j] = true
		}
	}
}

// ruleColumnOPConst propagates the "column OP const" condition.
func ruleColumnOPConst(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	trace_util_0.Count(_constraint_propagation_00000, 38)
	cond := exprs.data[i]
	f1, ok := cond.(*ScalarFunction)
	if !ok {
		trace_util_0.Count(_constraint_propagation_00000, 51)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 39)
	if f1.FuncName.L != ast.GE && f1.FuncName.L != ast.GT &&
		f1.FuncName.L != ast.LE && f1.FuncName.L != ast.LT {
		trace_util_0.Count(_constraint_propagation_00000, 52)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 40)
	OP1 := f1.FuncName.L

	var col1 *Column
	var con1 *Constant
	col1, ok = f1.GetArgs()[0].(*Column)
	if !ok {
		trace_util_0.Count(_constraint_propagation_00000, 53)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 41)
	con1, ok = f1.GetArgs()[1].(*Constant)
	if !ok {
		trace_util_0.Count(_constraint_propagation_00000, 54)
		return
	}

	trace_util_0.Count(_constraint_propagation_00000, 42)
	expr := exprs.data[j]
	f2, ok := expr.(*ScalarFunction)
	if !ok {
		trace_util_0.Count(_constraint_propagation_00000, 55)
		return
	}

	// The simple case:
	// col >= c1, col < c2, c1 >= c2 => false
	// col >= c1, col <= c2, c1 > c2 => false
	// col >= c1, col OP c2, c1 ^OP c2, where OP in [< , <=] => false
	// col OP1 c1 where OP1 in [>= , <], col OP2 c2 where OP1 opsite OP2, c1 ^OP2 c2 => false
	//
	// The extended case:
	// col >= c1, f(col) < c2, f is monotonous, f(c1) >= c2 => false
	//
	// Proof:
	// col > c1, f is monotonous => f(col) > f(c1)
	// f(col) > f(c1), f(col) < c2, f(c1) >= c2 => false
	trace_util_0.Count(_constraint_propagation_00000, 43)
	OP2 := f2.FuncName.L
	if !opsiteOP(OP1, OP2) {
		trace_util_0.Count(_constraint_propagation_00000, 56)
		return
	}

	trace_util_0.Count(_constraint_propagation_00000, 44)
	con2, ok := f2.GetArgs()[1].(*Constant)
	if !ok {
		trace_util_0.Count(_constraint_propagation_00000, 57)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 45)
	arg0 := f2.GetArgs()[0]
	// The simple case.
	var fc1 Expression
	col2, ok := arg0.(*Column)
	if ok {
		trace_util_0.Count(_constraint_propagation_00000, 58)
		fc1 = con1
	} else {
		trace_util_0.Count(_constraint_propagation_00000, 59)
		{
			// The extended case.
			scalarFunc, ok := arg0.(*ScalarFunction)
			if !ok {
				trace_util_0.Count(_constraint_propagation_00000, 63)
				return
			}
			trace_util_0.Count(_constraint_propagation_00000, 60)
			_, ok = monotoneIncFuncs[scalarFunc.FuncName.L]
			if !ok {
				trace_util_0.Count(_constraint_propagation_00000, 64)
				return
			}
			trace_util_0.Count(_constraint_propagation_00000, 61)
			col2, ok = scalarFunc.GetArgs()[0].(*Column)
			if !ok {
				trace_util_0.Count(_constraint_propagation_00000, 65)
				return
			}
			trace_util_0.Count(_constraint_propagation_00000, 62)
			var err error
			fc1, err = NewFunction(ctx, scalarFunc.FuncName.L, scalarFunc.RetType, con1)
			if err != nil {
				trace_util_0.Count(_constraint_propagation_00000, 66)
				logutil.Logger(context.Background()).Warn("build new function in ruleColumnOPConst", zap.Error(err))
				return
			}
		}
	}

	// Make sure col1 and col2 are the same column.
	// Can't use col1.Equal(ctx, col2) here, because they are not generated in one
	// expression and their UniqueID are not the same.
	trace_util_0.Count(_constraint_propagation_00000, 46)
	if col1.ColName.L != col2.ColName.L {
		trace_util_0.Count(_constraint_propagation_00000, 67)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 47)
	if col1.OrigColName.L != "" &&
		col2.OrigColName.L != "" &&
		col1.OrigColName.L != col2.OrigColName.L {
		trace_util_0.Count(_constraint_propagation_00000, 68)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 48)
	if col1.OrigTblName.L != "" &&
		col2.OrigTblName.L != "" &&
		col1.OrigColName.L != col2.OrigColName.L {
		trace_util_0.Count(_constraint_propagation_00000, 69)
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 49)
	v, isNull, err := compareConstant(ctx, negOP(OP2), fc1, con2)
	if err != nil {
		trace_util_0.Count(_constraint_propagation_00000, 70)
		logutil.Logger(context.Background()).Warn("comparing constant in ruleColumnOPConst", zap.Error(err))
		return
	}
	trace_util_0.Count(_constraint_propagation_00000, 50)
	if !isNull && v > 0 {
		trace_util_0.Count(_constraint_propagation_00000, 71)
		exprs.SetConstFalse()
	}
}

// opsiteOP the opsite direction of a compare operation, used in ruleColumnOPConst.
func opsiteOP(op1, op2 string) bool {
	trace_util_0.Count(_constraint_propagation_00000, 72)
	switch {
	case op1 == ast.GE || op1 == ast.GT:
		trace_util_0.Count(_constraint_propagation_00000, 74)
		return op2 == ast.LT || op2 == ast.LE
	case op1 == ast.LE || op1 == ast.LT:
		trace_util_0.Count(_constraint_propagation_00000, 75)
		return op2 == ast.GT || op2 == ast.GE
	}
	trace_util_0.Count(_constraint_propagation_00000, 73)
	return false
}

func negOP(cmp string) string {
	trace_util_0.Count(_constraint_propagation_00000, 76)
	switch cmp {
	case ast.LT:
		trace_util_0.Count(_constraint_propagation_00000, 78)
		return ast.GE
	case ast.LE:
		trace_util_0.Count(_constraint_propagation_00000, 79)
		return ast.GT
	case ast.GT:
		trace_util_0.Count(_constraint_propagation_00000, 80)
		return ast.LE
	case ast.GE:
		trace_util_0.Count(_constraint_propagation_00000, 81)
		return ast.LT
	}
	trace_util_0.Count(_constraint_propagation_00000, 77)
	return ""
}

// monotoneIncFuncs are those functions that for any x y, if x > y => f(x) > f(y)
var monotoneIncFuncs = map[string]struct{}{
	ast.ToDays: {},
}

// compareConstant compares two expressions. c1 and c2 should be constant with the same type.
func compareConstant(ctx sessionctx.Context, fn string, c1, c2 Expression) (int64, bool, error) {
	trace_util_0.Count(_constraint_propagation_00000, 82)
	cmp, err := NewFunction(ctx, fn, types.NewFieldType(mysql.TypeTiny), c1, c2)
	if err != nil {
		trace_util_0.Count(_constraint_propagation_00000, 84)
		return 0, false, err
	}
	trace_util_0.Count(_constraint_propagation_00000, 83)
	return cmp.EvalInt(ctx, chunk.Row{})
}

// NewPartitionPruneSolver returns a constraintSolver for partition pruning.
func NewPartitionPruneSolver() constraintSolver {
	trace_util_0.Count(_constraint_propagation_00000, 85)
	return newConstraintSolver(ruleColumnOPConst)
}

var _constraint_propagation_00000 = "expression/constraint_propagation.go"
