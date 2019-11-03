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

package core

import (
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// canPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) canPullUpAgg() bool {
	trace_util_0.Count(_rule_decorrelate_00000, 0)
	if la.JoinType != InnerJoin && la.JoinType != LeftOuterJoin {
		trace_util_0.Count(_rule_decorrelate_00000, 3)
		return false
	}
	trace_util_0.Count(_rule_decorrelate_00000, 1)
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		trace_util_0.Count(_rule_decorrelate_00000, 4)
		return false
	}
	trace_util_0.Count(_rule_decorrelate_00000, 2)
	return len(la.children[0].Schema().Keys) > 0
}

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) canPullUp() bool {
	trace_util_0.Count(_rule_decorrelate_00000, 5)
	if len(la.GroupByItems) > 0 {
		trace_util_0.Count(_rule_decorrelate_00000, 8)
		return false
	}
	trace_util_0.Count(_rule_decorrelate_00000, 6)
	for _, f := range la.AggFuncs {
		trace_util_0.Count(_rule_decorrelate_00000, 9)
		for _, arg := range f.Args {
			trace_util_0.Count(_rule_decorrelate_00000, 10)
			expr := expression.EvaluateExprWithNull(la.ctx, la.children[0].Schema(), arg)
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				trace_util_0.Count(_rule_decorrelate_00000, 11)
				return false
			}
		}
	}
	trace_util_0.Count(_rule_decorrelate_00000, 7)
	return true
}

// deCorColFromEqExpr checks whether it's an equal condition of form `col = correlated col`. If so we will change the decorrelated
// column to normal column to make a new equal condition.
func (la *LogicalApply) deCorColFromEqExpr(expr expression.Expression) expression.Expression {
	trace_util_0.Count(_rule_decorrelate_00000, 12)
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		trace_util_0.Count(_rule_decorrelate_00000, 16)
		return nil
	}
	trace_util_0.Count(_rule_decorrelate_00000, 13)
	if col, lOk := sf.GetArgs()[0].(*expression.Column); lOk {
		trace_util_0.Count(_rule_decorrelate_00000, 17)
		if corCol, rOk := sf.GetArgs()[1].(*expression.CorrelatedColumn); rOk {
			trace_util_0.Count(_rule_decorrelate_00000, 18)
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				trace_util_0.Count(_rule_decorrelate_00000, 20)
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			trace_util_0.Count(_rule_decorrelate_00000, 19)
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	trace_util_0.Count(_rule_decorrelate_00000, 14)
	if corCol, lOk := sf.GetArgs()[0].(*expression.CorrelatedColumn); lOk {
		trace_util_0.Count(_rule_decorrelate_00000, 21)
		if col, rOk := sf.GetArgs()[1].(*expression.Column); rOk {
			trace_util_0.Count(_rule_decorrelate_00000, 22)
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				trace_util_0.Count(_rule_decorrelate_00000, 24)
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			trace_util_0.Count(_rule_decorrelate_00000, 23)
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	trace_util_0.Count(_rule_decorrelate_00000, 15)
	return nil
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

func (s *decorrelateSolver) aggDefaultValueMap(agg *LogicalAggregation) map[int]*expression.Constant {
	trace_util_0.Count(_rule_decorrelate_00000, 25)
	defaultValueMap := make(map[int]*expression.Constant)
	for i, f := range agg.AggFuncs {
		trace_util_0.Count(_rule_decorrelate_00000, 27)
		switch f.Name {
		case ast.AggFuncBitOr, ast.AggFuncBitXor, ast.AggFuncCount:
			trace_util_0.Count(_rule_decorrelate_00000, 28)
			defaultValueMap[i] = expression.Zero.Clone().(*expression.Constant)
		case ast.AggFuncBitAnd:
			trace_util_0.Count(_rule_decorrelate_00000, 29)
			defaultValueMap[i] = &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: types.NewFieldType(mysql.TypeLonglong)}
		}
	}
	trace_util_0.Count(_rule_decorrelate_00000, 26)
	return defaultValueMap
}

// optimize implements logicalOptRule interface.
func (s *decorrelateSolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	trace_util_0.Count(_rule_decorrelate_00000, 30)
	if apply, ok := p.(*LogicalApply); ok {
		trace_util_0.Count(_rule_decorrelate_00000, 33)
		outerPlan := apply.children[0]
		innerPlan := apply.children[1]
		apply.corCols = extractCorColumnsBySchema(apply.children[1], apply.children[0].Schema())
		if len(apply.corCols) == 0 {
			trace_util_0.Count(_rule_decorrelate_00000, 34)
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.self = join
			p = join
		} else {
			trace_util_0.Count(_rule_decorrelate_00000, 35)
			if sel, ok := innerPlan.(*LogicalSelection); ok {
				trace_util_0.Count(_rule_decorrelate_00000, 36)
				// If the inner plan is a selection, we add this condition to join predicates.
				// Notice that no matter what kind of join is, it's always right.
				newConds := make([]expression.Expression, 0, len(sel.Conditions))
				for _, cond := range sel.Conditions {
					trace_util_0.Count(_rule_decorrelate_00000, 38)
					newConds = append(newConds, cond.Decorrelate(outerPlan.Schema()))
				}
				trace_util_0.Count(_rule_decorrelate_00000, 37)
				apply.attachOnConds(newConds)
				innerPlan = sel.children[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(p)
			} else {
				trace_util_0.Count(_rule_decorrelate_00000, 39)
				if m, ok := innerPlan.(*LogicalMaxOneRow); ok {
					trace_util_0.Count(_rule_decorrelate_00000, 40)
					if m.children[0].MaxOneRow() {
						trace_util_0.Count(_rule_decorrelate_00000, 41)
						innerPlan = m.children[0]
						apply.SetChildren(outerPlan, innerPlan)
						return s.optimize(p)
					}
				} else {
					trace_util_0.Count(_rule_decorrelate_00000, 42)
					if proj, ok := innerPlan.(*LogicalProjection); ok {
						trace_util_0.Count(_rule_decorrelate_00000, 43)
						for i, expr := range proj.Exprs {
							trace_util_0.Count(_rule_decorrelate_00000, 46)
							proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
						}
						trace_util_0.Count(_rule_decorrelate_00000, 44)
						apply.columnSubstitute(proj.Schema(), proj.Exprs)
						innerPlan = proj.children[0]
						apply.SetChildren(outerPlan, innerPlan)
						if apply.JoinType != SemiJoin && apply.JoinType != LeftOuterSemiJoin && apply.JoinType != AntiSemiJoin && apply.JoinType != AntiLeftOuterSemiJoin {
							trace_util_0.Count(_rule_decorrelate_00000, 47)
							proj.SetSchema(apply.Schema())
							proj.Exprs = append(expression.Column2Exprs(outerPlan.Schema().Clone().Columns), proj.Exprs...)
							apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
							np, err := s.optimize(p)
							if err != nil {
								trace_util_0.Count(_rule_decorrelate_00000, 49)
								return nil, err
							}
							trace_util_0.Count(_rule_decorrelate_00000, 48)
							proj.SetChildren(np)
							return proj, nil
						}
						trace_util_0.Count(_rule_decorrelate_00000, 45)
						return s.optimize(p)
					} else {
						trace_util_0.Count(_rule_decorrelate_00000, 50)
						if agg, ok := innerPlan.(*LogicalAggregation); ok {
							trace_util_0.Count(_rule_decorrelate_00000, 51)
							if apply.canPullUpAgg() && agg.canPullUp() {
								trace_util_0.Count(_rule_decorrelate_00000, 53)
								innerPlan = agg.children[0]
								apply.JoinType = LeftOuterJoin
								apply.SetChildren(outerPlan, innerPlan)
								agg.SetSchema(apply.Schema())
								agg.GroupByItems = expression.Column2Exprs(outerPlan.Schema().Keys[0])
								newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

								outerColsInSchema := make([]*expression.Column, 0, outerPlan.Schema().Len())
								for i, col := range outerPlan.Schema().Columns {
									trace_util_0.Count(_rule_decorrelate_00000, 56)
									first, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
									if err != nil {
										trace_util_0.Count(_rule_decorrelate_00000, 58)
										return nil, err
									}
									trace_util_0.Count(_rule_decorrelate_00000, 57)
									newAggFuncs = append(newAggFuncs, first)

									outerCol, _ := outerPlan.Schema().Columns[i].Clone().(*expression.Column)
									outerCol.RetType = first.RetTp
									outerColsInSchema = append(outerColsInSchema, outerCol)
								}
								trace_util_0.Count(_rule_decorrelate_00000, 54)
								newAggFuncs = append(newAggFuncs, agg.AggFuncs...)
								agg.AggFuncs = newAggFuncs
								apply.SetSchema(expression.MergeSchema(expression.NewSchema(outerColsInSchema...), innerPlan.Schema()))
								np, err := s.optimize(p)
								if err != nil {
									trace_util_0.Count(_rule_decorrelate_00000, 59)
									return nil, err
								}
								trace_util_0.Count(_rule_decorrelate_00000, 55)
								agg.SetChildren(np)
								// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
								// agg.buildProjectionIfNecessary()
								agg.collectGroupByColumns()
								return agg, nil
							}
							// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
							// the equal conditions contain the correlated column of this apply.
							trace_util_0.Count(_rule_decorrelate_00000, 52)
							if sel, ok := agg.children[0].(*LogicalSelection); ok && apply.JoinType == LeftOuterJoin {
								trace_util_0.Count(_rule_decorrelate_00000, 60)
								var (
									eqCondWithCorCol []*expression.ScalarFunction
									remainedExpr     []expression.Expression
								)
								// Extract the equal condition.
								for _, cond := range sel.Conditions {
									trace_util_0.Count(_rule_decorrelate_00000, 62)
									if expr := apply.deCorColFromEqExpr(cond); expr != nil {
										trace_util_0.Count(_rule_decorrelate_00000, 63)
										eqCondWithCorCol = append(eqCondWithCorCol, expr.(*expression.ScalarFunction))
									} else {
										trace_util_0.Count(_rule_decorrelate_00000, 64)
										{
											remainedExpr = append(remainedExpr, cond)
										}
									}
								}
								trace_util_0.Count(_rule_decorrelate_00000, 61)
								if len(eqCondWithCorCol) > 0 {
									trace_util_0.Count(_rule_decorrelate_00000, 65)
									originalExpr := sel.Conditions
									sel.Conditions = remainedExpr
									apply.corCols = extractCorColumnsBySchema(apply.children[1], apply.children[0].Schema())
									// There's no other correlated column.
									if len(apply.corCols) == 0 {
										trace_util_0.Count(_rule_decorrelate_00000, 67)
										join := &apply.LogicalJoin
										join.EqualConditions = append(join.EqualConditions, eqCondWithCorCol...)
										for _, eqCond := range eqCondWithCorCol {
											trace_util_0.Count(_rule_decorrelate_00000, 71)
											clonedCol := eqCond.GetArgs()[1]
											// If the join key is not in the aggregation's schema, add first row function.
											if agg.schema.ColumnIndex(eqCond.GetArgs()[1].(*expression.Column)) == -1 {
												trace_util_0.Count(_rule_decorrelate_00000, 73)
												newFunc, err := aggregation.NewAggFuncDesc(apply.ctx, ast.AggFuncFirstRow, []expression.Expression{clonedCol}, false)
												if err != nil {
													trace_util_0.Count(_rule_decorrelate_00000, 75)
													return nil, err
												}
												trace_util_0.Count(_rule_decorrelate_00000, 74)
												agg.AggFuncs = append(agg.AggFuncs, newFunc)
												agg.schema.Append(clonedCol.(*expression.Column))
												agg.schema.Columns[agg.schema.Len()-1].RetType = newFunc.RetTp
											}
											// If group by cols don't contain the join key, add it into this.
											trace_util_0.Count(_rule_decorrelate_00000, 72)
											if agg.getGbyColIndex(eqCond.GetArgs()[1].(*expression.Column)) == -1 {
												trace_util_0.Count(_rule_decorrelate_00000, 76)
												agg.GroupByItems = append(agg.GroupByItems, clonedCol)
											}
										}
										trace_util_0.Count(_rule_decorrelate_00000, 68)
										agg.collectGroupByColumns()
										// The selection may be useless, check and remove it.
										if len(sel.Conditions) == 0 {
											trace_util_0.Count(_rule_decorrelate_00000, 77)
											agg.SetChildren(sel.children[0])
										}
										trace_util_0.Count(_rule_decorrelate_00000, 69)
										defaultValueMap := s.aggDefaultValueMap(agg)
										// We should use it directly, rather than building a projection.
										if len(defaultValueMap) > 0 {
											trace_util_0.Count(_rule_decorrelate_00000, 78)
											proj := LogicalProjection{}.Init(agg.ctx)
											proj.SetSchema(apply.schema)
											proj.Exprs = expression.Column2Exprs(apply.schema.Columns)
											for i, val := range defaultValueMap {
												trace_util_0.Count(_rule_decorrelate_00000, 80)
												pos := proj.schema.ColumnIndex(agg.schema.Columns[i])
												ifNullFunc := expression.NewFunctionInternal(agg.ctx, ast.Ifnull, types.NewFieldType(mysql.TypeLonglong), agg.schema.Columns[i], val)
												proj.Exprs[pos] = ifNullFunc
											}
											trace_util_0.Count(_rule_decorrelate_00000, 79)
											proj.SetChildren(apply)
											p = proj
										}
										trace_util_0.Count(_rule_decorrelate_00000, 70)
										return s.optimize(p)
									}
									trace_util_0.Count(_rule_decorrelate_00000, 66)
									sel.Conditions = originalExpr
									apply.corCols = extractCorColumnsBySchema(apply.children[1], apply.children[0].Schema())
								}
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_rule_decorrelate_00000, 31)
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		trace_util_0.Count(_rule_decorrelate_00000, 81)
		np, err := s.optimize(child)
		if err != nil {
			trace_util_0.Count(_rule_decorrelate_00000, 83)
			return nil, err
		}
		trace_util_0.Count(_rule_decorrelate_00000, 82)
		newChildren = append(newChildren, np)
	}
	trace_util_0.Count(_rule_decorrelate_00000, 32)
	p.SetChildren(newChildren...)
	return p, nil
}

var _rule_decorrelate_00000 = "planner/core/rule_decorrelate.go"
