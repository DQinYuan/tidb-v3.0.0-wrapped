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

package ranger

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
)

// detachColumnCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachColumnCNFConditions(sctx sessionctx.Context, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, []expression.Expression) {
	trace_util_0.Count(_detacher_00000, 0)
	var accessConditions, filterConditions []expression.Expression
	for _, cond := range conditions {
		trace_util_0.Count(_detacher_00000, 2)
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			trace_util_0.Count(_detacher_00000, 5)
			dnfItems := expression.FlattenDNFConditions(sf)
			colulmnDNFItems, hasResidual := detachColumnDNFConditions(sctx, dnfItems, checker)
			// If this CNF has expression that cannot be resolved as access condition, then the total DNF expression
			// should be also appended into filter condition.
			if hasResidual {
				trace_util_0.Count(_detacher_00000, 8)
				filterConditions = append(filterConditions, cond)
			}
			trace_util_0.Count(_detacher_00000, 6)
			if len(colulmnDNFItems) == 0 {
				trace_util_0.Count(_detacher_00000, 9)
				continue
			}
			trace_util_0.Count(_detacher_00000, 7)
			rebuildDNF := expression.ComposeDNFCondition(sctx, colulmnDNFItems...)
			accessConditions = append(accessConditions, rebuildDNF)
			continue
		}
		trace_util_0.Count(_detacher_00000, 3)
		if !checker.check(cond) {
			trace_util_0.Count(_detacher_00000, 10)
			filterConditions = append(filterConditions, cond)
			continue
		}
		trace_util_0.Count(_detacher_00000, 4)
		accessConditions = append(accessConditions, cond)
		if checker.shouldReserve {
			trace_util_0.Count(_detacher_00000, 11)
			filterConditions = append(filterConditions, cond)
			checker.shouldReserve = checker.length != types.UnspecifiedLength
		}
	}
	trace_util_0.Count(_detacher_00000, 1)
	return accessConditions, filterConditions
}

// detachColumnDNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is DNF form.
func detachColumnDNFConditions(sctx sessionctx.Context, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, bool) {
	trace_util_0.Count(_detacher_00000, 12)
	var (
		hasResidualConditions bool
		accessConditions      []expression.Expression
	)
	for _, cond := range conditions {
		trace_util_0.Count(_detacher_00000, 14)
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			trace_util_0.Count(_detacher_00000, 15)
			cnfItems := expression.FlattenCNFConditions(sf)
			columnCNFItems, others := detachColumnCNFConditions(sctx, cnfItems, checker)
			if len(others) > 0 {
				trace_util_0.Count(_detacher_00000, 18)
				hasResidualConditions = true
			}
			// If one part of DNF has no access condition. Then this DNF cannot get range.
			trace_util_0.Count(_detacher_00000, 16)
			if len(columnCNFItems) == 0 {
				trace_util_0.Count(_detacher_00000, 19)
				return nil, true
			}
			trace_util_0.Count(_detacher_00000, 17)
			rebuildCNF := expression.ComposeCNFCondition(sctx, columnCNFItems...)
			accessConditions = append(accessConditions, rebuildCNF)
		} else {
			trace_util_0.Count(_detacher_00000, 20)
			if checker.check(cond) {
				trace_util_0.Count(_detacher_00000, 21)
				accessConditions = append(accessConditions, cond)
				if checker.shouldReserve {
					trace_util_0.Count(_detacher_00000, 22)
					hasResidualConditions = true
					checker.shouldReserve = checker.length != types.UnspecifiedLength
				}
			} else {
				trace_util_0.Count(_detacher_00000, 23)
				{
					return nil, true
				}
			}
		}
	}
	trace_util_0.Count(_detacher_00000, 13)
	return accessConditions, hasResidualConditions
}

// getEqOrInColOffset checks if the expression is a eq function that one side is constant and another is column or an
// in function which is `column in (constant list)`.
// If so, it will return the offset of this column in the slice, otherwise return -1 for not found.
func getEqOrInColOffset(expr expression.Expression, cols []*expression.Column) int {
	trace_util_0.Count(_detacher_00000, 24)
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		trace_util_0.Count(_detacher_00000, 28)
		return -1
	}
	trace_util_0.Count(_detacher_00000, 25)
	if f.FuncName.L == ast.EQ {
		trace_util_0.Count(_detacher_00000, 29)
		if c, ok := f.GetArgs()[0].(*expression.Column); ok {
			trace_util_0.Count(_detacher_00000, 31)
			if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
				trace_util_0.Count(_detacher_00000, 32)
				for i, col := range cols {
					trace_util_0.Count(_detacher_00000, 33)
					if col.Equal(nil, c) {
						trace_util_0.Count(_detacher_00000, 34)
						return i
					}
				}
			}
		}
		trace_util_0.Count(_detacher_00000, 30)
		if c, ok := f.GetArgs()[1].(*expression.Column); ok {
			trace_util_0.Count(_detacher_00000, 35)
			if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
				trace_util_0.Count(_detacher_00000, 36)
				for i, col := range cols {
					trace_util_0.Count(_detacher_00000, 37)
					if col.Equal(nil, c) {
						trace_util_0.Count(_detacher_00000, 38)
						return i
					}
				}
			}
		}
	}
	trace_util_0.Count(_detacher_00000, 26)
	if f.FuncName.L == ast.In {
		trace_util_0.Count(_detacher_00000, 39)
		c, ok := f.GetArgs()[0].(*expression.Column)
		if !ok {
			trace_util_0.Count(_detacher_00000, 42)
			return -1
		}
		trace_util_0.Count(_detacher_00000, 40)
		for _, arg := range f.GetArgs()[1:] {
			trace_util_0.Count(_detacher_00000, 43)
			if _, ok := arg.(*expression.Constant); !ok {
				trace_util_0.Count(_detacher_00000, 44)
				return -1
			}
		}
		trace_util_0.Count(_detacher_00000, 41)
		for i, col := range cols {
			trace_util_0.Count(_detacher_00000, 45)
			if col.Equal(nil, c) {
				trace_util_0.Count(_detacher_00000, 46)
				return i
			}
		}
	}
	trace_util_0.Count(_detacher_00000, 27)
	return -1
}

// detachCNFCondAndBuildRangeForIndex will detach the index filters from table filters. These conditions are connected with `and`
// It will first find the point query column and then extract the range query column.
// considerDNF is true means it will try to extract access conditions from the DNF expressions.
func detachCNFCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	tpSlice []*types.FieldType, lengths []int, considerDNF bool) (*DetachRangeResult, error) {
	trace_util_0.Count(_detacher_00000, 47)
	var (
		eqCount int
		ranges  []*Range
		err     error
	)
	res := &DetachRangeResult{}

	accessConds, filterConds, newConditions, emptyRange := ExtractEqAndInCondition(sctx, conditions, cols, lengths)
	if emptyRange {
		trace_util_0.Count(_detacher_00000, 52)
		return res, nil
	}

	trace_util_0.Count(_detacher_00000, 48)
	for ; eqCount < len(accessConds); eqCount++ {
		trace_util_0.Count(_detacher_00000, 53)
		if accessConds[eqCount].(*expression.ScalarFunction).FuncName.L != ast.EQ {
			trace_util_0.Count(_detacher_00000, 54)
			break
		}
	}
	trace_util_0.Count(_detacher_00000, 49)
	eqOrInCount := len(accessConds)
	res.EqCondCount = eqCount
	res.EqOrInCount = eqOrInCount
	if eqOrInCount == len(cols) {
		trace_util_0.Count(_detacher_00000, 55)
		filterConds = append(filterConds, newConditions...)
		ranges, err = buildCNFIndexRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
		if err != nil {
			trace_util_0.Count(_detacher_00000, 57)
			return res, err
		}
		trace_util_0.Count(_detacher_00000, 56)
		res.Ranges = ranges
		res.AccessConds = accessConds
		res.RemainedConds = filterConds
		return res, nil
	}
	trace_util_0.Count(_detacher_00000, 50)
	checker := &conditionChecker{
		colUniqueID:   cols[eqOrInCount].UniqueID,
		length:        lengths[eqOrInCount],
		shouldReserve: lengths[eqOrInCount] != types.UnspecifiedLength,
	}
	if considerDNF {
		trace_util_0.Count(_detacher_00000, 58)
		accesses, filters := detachColumnCNFConditions(sctx, newConditions, checker)
		accessConds = append(accessConds, accesses...)
		filterConds = append(filterConds, filters...)
	} else {
		trace_util_0.Count(_detacher_00000, 59)
		{
			for _, cond := range newConditions {
				trace_util_0.Count(_detacher_00000, 60)
				if !checker.check(cond) {
					trace_util_0.Count(_detacher_00000, 62)
					filterConds = append(filterConds, cond)
					continue
				}
				trace_util_0.Count(_detacher_00000, 61)
				accessConds = append(accessConds, cond)
			}
		}
	}
	trace_util_0.Count(_detacher_00000, 51)
	ranges, err = buildCNFIndexRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, err
}

// ExtractEqAndInCondition will split the given condition into three parts by the information of index columns and their lengths.
// accesses: The condition will be used to build range.
// filters: filters is the part that some access conditions need to be evaluate again since it's only the prefix part of char column.
// newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same column.
//   e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
// bool: indicate whether there's nil range when merging eq and in conditions.
func ExtractEqAndInCondition(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column, lengths []int) ([]expression.Expression, []expression.Expression, []expression.Expression, bool) {
	trace_util_0.Count(_detacher_00000, 63)
	var filters []expression.Expression
	rb := builder{sc: sctx.GetSessionVars().StmtCtx}
	accesses := make([]expression.Expression, len(cols))
	points := make([][]point, len(cols))
	mergedAccesses := make([]expression.Expression, len(cols))
	newConditions := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		trace_util_0.Count(_detacher_00000, 67)
		offset := getEqOrInColOffset(cond, cols)
		if offset == -1 {
			trace_util_0.Count(_detacher_00000, 71)
			newConditions = append(newConditions, cond)
			continue
		}
		trace_util_0.Count(_detacher_00000, 68)
		if accesses[offset] == nil {
			trace_util_0.Count(_detacher_00000, 72)
			accesses[offset] = cond
			continue
		}
		// Multiple Eq/In conditions for one column in CNF, apply intersection on them
		// Lazily compute the points for the previously visited Eq/In
		trace_util_0.Count(_detacher_00000, 69)
		if mergedAccesses[offset] == nil {
			trace_util_0.Count(_detacher_00000, 73)
			mergedAccesses[offset] = accesses[offset]
			points[offset] = rb.build(accesses[offset])
		}
		trace_util_0.Count(_detacher_00000, 70)
		points[offset] = rb.intersection(points[offset], rb.build(cond))
		// Early termination if false expression found
		if len(points[offset]) == 0 {
			trace_util_0.Count(_detacher_00000, 74)
			return nil, nil, nil, true
		}
	}
	trace_util_0.Count(_detacher_00000, 64)
	for i, ma := range mergedAccesses {
		trace_util_0.Count(_detacher_00000, 75)
		if ma == nil {
			trace_util_0.Count(_detacher_00000, 77)
			if accesses[i] != nil {
				trace_util_0.Count(_detacher_00000, 79)
				newConditions = append(newConditions, accesses[i])
			}
			trace_util_0.Count(_detacher_00000, 78)
			continue
		}
		trace_util_0.Count(_detacher_00000, 76)
		accesses[i] = points2EqOrInCond(sctx, points[i], mergedAccesses[i])
		newConditions = append(newConditions, accesses[i])
	}
	trace_util_0.Count(_detacher_00000, 65)
	for i, cond := range accesses {
		trace_util_0.Count(_detacher_00000, 80)
		if cond == nil {
			trace_util_0.Count(_detacher_00000, 82)
			accesses = accesses[:i]
			break
		}
		trace_util_0.Count(_detacher_00000, 81)
		if lengths[i] != types.UnspecifiedLength {
			trace_util_0.Count(_detacher_00000, 83)
			filters = append(filters, cond)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	trace_util_0.Count(_detacher_00000, 66)
	newConditions = removeAccessConditions(newConditions, accesses)
	return accesses, filters, newConditions, false
}

// detachDNFCondAndBuildRangeForIndex will detach the index filters from table filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func detachDNFCondAndBuildRangeForIndex(sctx sessionctx.Context, condition *expression.ScalarFunction,
	cols []*expression.Column, newTpSlice []*types.FieldType, lengths []int) ([]*Range, []expression.Expression, bool, error) {
	trace_util_0.Count(_detacher_00000, 84)
	sc := sctx.GetSessionVars().StmtCtx
	firstColumnChecker := &conditionChecker{
		colUniqueID:   cols[0].UniqueID,
		shouldReserve: lengths[0] != types.UnspecifiedLength,
		length:        lengths[0],
	}
	rb := builder{sc: sc}
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))
	var totalRanges []*Range
	hasResidual := false
	for _, item := range dnfItems {
		trace_util_0.Count(_detacher_00000, 87)
		if sf, ok := item.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			trace_util_0.Count(_detacher_00000, 88)
			cnfItems := expression.FlattenCNFConditions(sf)
			var accesses, filters []expression.Expression
			res, err := detachCNFCondAndBuildRangeForIndex(sctx, cnfItems, cols, newTpSlice, lengths, true)
			if err != nil {
				trace_util_0.Count(_detacher_00000, 92)
				return nil, nil, false, nil
			}
			trace_util_0.Count(_detacher_00000, 89)
			ranges := res.Ranges
			accesses = res.AccessConds
			filters = res.RemainedConds
			if len(accesses) == 0 {
				trace_util_0.Count(_detacher_00000, 93)
				return FullRange(), nil, true, nil
			}
			trace_util_0.Count(_detacher_00000, 90)
			if len(filters) > 0 {
				trace_util_0.Count(_detacher_00000, 94)
				hasResidual = true
			}
			trace_util_0.Count(_detacher_00000, 91)
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(sctx, accesses...))
		} else {
			trace_util_0.Count(_detacher_00000, 95)
			if firstColumnChecker.check(item) {
				trace_util_0.Count(_detacher_00000, 96)
				if firstColumnChecker.shouldReserve {
					trace_util_0.Count(_detacher_00000, 99)
					hasResidual = true
					firstColumnChecker.shouldReserve = lengths[0] != types.UnspecifiedLength
				}
				trace_util_0.Count(_detacher_00000, 97)
				points := rb.build(item)
				ranges, err := points2Ranges(sc, points, newTpSlice[0])
				if err != nil {
					trace_util_0.Count(_detacher_00000, 100)
					return nil, nil, false, errors.Trace(err)
				}
				trace_util_0.Count(_detacher_00000, 98)
				totalRanges = append(totalRanges, ranges...)
				newAccessItems = append(newAccessItems, item)
			} else {
				trace_util_0.Count(_detacher_00000, 101)
				{
					return FullRange(), nil, true, nil
				}
			}
		}
	}

	trace_util_0.Count(_detacher_00000, 85)
	totalRanges, err := unionRanges(sc, totalRanges)
	if err != nil {
		trace_util_0.Count(_detacher_00000, 102)
		return nil, nil, false, errors.Trace(err)
	}

	trace_util_0.Count(_detacher_00000, 86)
	return totalRanges, []expression.Expression{expression.ComposeDNFCondition(sctx, newAccessItems...)}, hasResidual, nil
}

// DetachRangeResult wraps up results when detaching conditions and builing ranges.
type DetachRangeResult struct {
	// Ranges is the ranges extracted and built from conditions.
	Ranges []*Range
	// AccessConds is the extracted conditions for access.
	AccessConds []expression.Expression
	// RemainedConds is the filter conditions which should be kept after access.
	RemainedConds []expression.Expression
	// EqCondCount is the number of equal conditions extracted.
	EqCondCount int
	// EqOrInCount is the number of equal/in conditions extracted.
	EqOrInCount int
	// IsDNFCond indicates if the top layer of conditions are in DNF.
	IsDNFCond bool
}

// DetachCondAndBuildRangeForIndex will detach the index filters from table filters.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (*DetachRangeResult, error) {
	trace_util_0.Count(_detacher_00000, 103)
	res := &DetachRangeResult{}
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_detacher_00000, 106)
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	trace_util_0.Count(_detacher_00000, 104)
	if len(conditions) == 1 {
		trace_util_0.Count(_detacher_00000, 107)
		if sf, ok := conditions[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			trace_util_0.Count(_detacher_00000, 108)
			ranges, accesses, hasResidual, err := detachDNFCondAndBuildRangeForIndex(sctx, sf, cols, newTpSlice, lengths)
			if err != nil {
				trace_util_0.Count(_detacher_00000, 111)
				return res, errors.Trace(err)
			}
			trace_util_0.Count(_detacher_00000, 109)
			res.Ranges = ranges
			res.AccessConds = accesses
			res.IsDNFCond = true
			// If this DNF have something cannot be to calculate range, then all this DNF should be pushed as filter condition.
			if hasResidual {
				trace_util_0.Count(_detacher_00000, 112)
				res.RemainedConds = conditions
				return res, nil
			}
			trace_util_0.Count(_detacher_00000, 110)
			return res, nil
		}
	}
	trace_util_0.Count(_detacher_00000, 105)
	return detachCNFCondAndBuildRangeForIndex(sctx, conditions, cols, newTpSlice, lengths, true)
}

// DetachSimpleCondAndBuildRangeForIndex will detach the index filters from table filters.
// It will find the point query column firstly and then extract the range query column.
func DetachSimpleCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column, lengths []int) ([]*Range, []expression.Expression, error) {
	trace_util_0.Count(_detacher_00000, 113)
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		trace_util_0.Count(_detacher_00000, 115)
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	trace_util_0.Count(_detacher_00000, 114)
	res, err := detachCNFCondAndBuildRangeForIndex(sctx, conditions, cols, newTpSlice, lengths, false)
	return res.Ranges, res.AccessConds, err
}

func removeAccessConditions(conditions, accessConds []expression.Expression) []expression.Expression {
	trace_util_0.Count(_detacher_00000, 116)
	filterConds := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		trace_util_0.Count(_detacher_00000, 118)
		if !expression.Contains(accessConds, cond) {
			trace_util_0.Count(_detacher_00000, 119)
			filterConds = append(filterConds, cond)
		}
	}
	trace_util_0.Count(_detacher_00000, 117)
	return filterConds
}

// ExtractAccessConditionsForColumn extracts the access conditions used for range calculation. Since
// we don't need to return the remained filter conditions, it is much simpler than DetachCondsForColumn.
func ExtractAccessConditionsForColumn(conds []expression.Expression, uniqueID int64) []expression.Expression {
	trace_util_0.Count(_detacher_00000, 120)
	checker := conditionChecker{
		colUniqueID: uniqueID,
		length:      types.UnspecifiedLength,
	}
	accessConds := make([]expression.Expression, 0, 8)
	return expression.Filter(accessConds, conds, checker.check)
}

// DetachCondsForColumn detaches access conditions for specified column from other filter conditions.
func DetachCondsForColumn(sctx sessionctx.Context, conds []expression.Expression, col *expression.Column) (accessConditions, otherConditions []expression.Expression) {
	trace_util_0.Count(_detacher_00000, 121)
	checker := &conditionChecker{
		colUniqueID: col.UniqueID,
		length:      types.UnspecifiedLength,
	}
	return detachColumnCNFConditions(sctx, conds, checker)
}

var _detacher_00000 = "util/ranger/detacher.go"
