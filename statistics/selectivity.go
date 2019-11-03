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

package statistics

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// StatsNode is used for calculating selectivity.
type StatsNode struct {
	Tp int
	ID int64
	// mask is a bit pattern whose ith bit will indicate whether the ith expression is covered by this index/column.
	mask int64
	// Ranges contains all the Ranges we got.
	Ranges []*ranger.Range
	// Selectivity indicates the Selectivity of this column/index.
	Selectivity float64
	// numCols is the number of columns contained in the index or column(which is always 1).
	numCols int
	// partCover indicates whether the bit in the mask is for a full cover or partial cover. It is only true
	// when the condition is a DNF expression on index, and the expression is not totally extracted as access condition.
	partCover bool
}

// The type of the StatsNode.
const (
	IndexType = iota
	PkType
	ColType
)

const unknownColumnID = math.MinInt64

// getConstantColumnID receives two expressions and if one of them is column and another is constant, it returns the
// ID of the column.
func getConstantColumnID(e []expression.Expression) int64 {
	trace_util_0.Count(_selectivity_00000, 0)
	if len(e) != 2 {
		trace_util_0.Count(_selectivity_00000, 4)
		return unknownColumnID
	}
	trace_util_0.Count(_selectivity_00000, 1)
	col, ok1 := e[0].(*expression.Column)
	_, ok2 := e[1].(*expression.Constant)
	if ok1 && ok2 {
		trace_util_0.Count(_selectivity_00000, 5)
		return col.ID
	}
	trace_util_0.Count(_selectivity_00000, 2)
	col, ok1 = e[1].(*expression.Column)
	_, ok2 = e[0].(*expression.Constant)
	if ok1 && ok2 {
		trace_util_0.Count(_selectivity_00000, 6)
		return col.ID
	}
	trace_util_0.Count(_selectivity_00000, 3)
	return unknownColumnID
}

func pseudoSelectivity(coll *HistColl, exprs []expression.Expression) float64 {
	trace_util_0.Count(_selectivity_00000, 7)
	minFactor := selectionFactor
	colExists := make(map[string]bool)
	for _, expr := range exprs {
		trace_util_0.Count(_selectivity_00000, 11)
		fun, ok := expr.(*expression.ScalarFunction)
		if !ok {
			trace_util_0.Count(_selectivity_00000, 14)
			continue
		}
		trace_util_0.Count(_selectivity_00000, 12)
		colID := getConstantColumnID(fun.GetArgs())
		if colID == unknownColumnID {
			trace_util_0.Count(_selectivity_00000, 15)
			continue
		}
		trace_util_0.Count(_selectivity_00000, 13)
		switch fun.FuncName.L {
		case ast.EQ, ast.NullEQ, ast.In:
			trace_util_0.Count(_selectivity_00000, 16)
			minFactor = math.Min(minFactor, 1.0/pseudoEqualRate)
			col, ok := coll.Columns[colID]
			if !ok {
				trace_util_0.Count(_selectivity_00000, 19)
				continue
			}
			trace_util_0.Count(_selectivity_00000, 17)
			colExists[col.Info.Name.L] = true
			if mysql.HasUniKeyFlag(col.Info.Flag) {
				trace_util_0.Count(_selectivity_00000, 20)
				return 1.0 / float64(coll.Count)
			}
		case ast.GE, ast.GT, ast.LE, ast.LT:
			trace_util_0.Count(_selectivity_00000, 18)
			minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
			// FIXME: To resolve the between case.
		}
	}
	trace_util_0.Count(_selectivity_00000, 8)
	if len(colExists) == 0 {
		trace_util_0.Count(_selectivity_00000, 21)
		return minFactor
	}
	// use the unique key info
	trace_util_0.Count(_selectivity_00000, 9)
	for _, idx := range coll.Indices {
		trace_util_0.Count(_selectivity_00000, 22)
		if !idx.Info.Unique {
			trace_util_0.Count(_selectivity_00000, 25)
			continue
		}
		trace_util_0.Count(_selectivity_00000, 23)
		unique := true
		for _, col := range idx.Info.Columns {
			trace_util_0.Count(_selectivity_00000, 26)
			if !colExists[col.Name.L] {
				trace_util_0.Count(_selectivity_00000, 27)
				unique = false
				break
			}
		}
		trace_util_0.Count(_selectivity_00000, 24)
		if unique {
			trace_util_0.Count(_selectivity_00000, 28)
			return 1.0 / float64(coll.Count)
		}
	}
	trace_util_0.Count(_selectivity_00000, 10)
	return minFactor
}

// isColEqCorCol checks if the expression is a eq function that one side is correlated column and another is column.
// If so, it will return the column's reference. Otherwise return nil instead.
func isColEqCorCol(filter expression.Expression) *expression.Column {
	trace_util_0.Count(_selectivity_00000, 29)
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		trace_util_0.Count(_selectivity_00000, 33)
		return nil
	}
	trace_util_0.Count(_selectivity_00000, 30)
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		trace_util_0.Count(_selectivity_00000, 34)
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			trace_util_0.Count(_selectivity_00000, 35)
			return c
		}
	}
	trace_util_0.Count(_selectivity_00000, 31)
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		trace_util_0.Count(_selectivity_00000, 36)
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			trace_util_0.Count(_selectivity_00000, 37)
			return c
		}
	}
	trace_util_0.Count(_selectivity_00000, 32)
	return nil
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// Currently the time complexity is o(n^2).
func (coll *HistColl) Selectivity(ctx sessionctx.Context, exprs []expression.Expression) (float64, []*StatsNode, error) {
	trace_util_0.Count(_selectivity_00000, 38)
	// If table's count is zero or conditions are empty, we should return 100% selectivity.
	if coll.Count == 0 || len(exprs) == 0 {
		trace_util_0.Count(_selectivity_00000, 46)
		return 1, nil, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	trace_util_0.Count(_selectivity_00000, 39)
	if len(exprs) > 63 || (len(coll.Columns) == 0 && len(coll.Indices) == 0) {
		trace_util_0.Count(_selectivity_00000, 47)
		return pseudoSelectivity(coll, exprs), nil, nil
	}
	trace_util_0.Count(_selectivity_00000, 40)
	ret := 1.0
	var nodes []*StatsNode
	sc := ctx.GetSessionVars().StmtCtx

	remainedExprs := make([]expression.Expression, 0, len(exprs))

	// Deal with the correlated column.
	for _, expr := range exprs {
		trace_util_0.Count(_selectivity_00000, 48)
		c := isColEqCorCol(expr)
		if c == nil {
			trace_util_0.Count(_selectivity_00000, 51)
			remainedExprs = append(remainedExprs, expr)
			continue
		}

		trace_util_0.Count(_selectivity_00000, 49)
		if colHist := coll.Columns[c.UniqueID]; colHist == nil || colHist.IsInvalid(sc, coll.Pseudo) {
			trace_util_0.Count(_selectivity_00000, 52)
			ret *= 1.0 / pseudoEqualRate
			continue
		}

		trace_util_0.Count(_selectivity_00000, 50)
		colHist := coll.Columns[c.UniqueID]
		if colHist.NDV > 0 {
			trace_util_0.Count(_selectivity_00000, 53)
			ret *= 1 / float64(colHist.NDV)
		} else {
			trace_util_0.Count(_selectivity_00000, 54)
			{
				ret *= 1.0 / pseudoEqualRate
			}
		}
	}

	trace_util_0.Count(_selectivity_00000, 41)
	extractedCols := make([]*expression.Column, 0, len(coll.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, remainedExprs, nil)
	for id, colInfo := range coll.Columns {
		trace_util_0.Count(_selectivity_00000, 55)
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		if col != nil {
			trace_util_0.Count(_selectivity_00000, 56)
			maskCovered, ranges, _, err := getMaskAndRanges(ctx, remainedExprs, ranger.ColumnRangeType, nil, col)
			if err != nil {
				trace_util_0.Count(_selectivity_00000, 60)
				return 0, nil, errors.Trace(err)
			}
			trace_util_0.Count(_selectivity_00000, 57)
			nodes = append(nodes, &StatsNode{Tp: ColType, ID: id, mask: maskCovered, Ranges: ranges, numCols: 1})
			if colInfo.IsHandle {
				trace_util_0.Count(_selectivity_00000, 61)
				nodes[len(nodes)-1].Tp = PkType
				var cnt float64
				cnt, err = coll.GetRowCountByIntColumnRanges(sc, id, ranges)
				if err != nil {
					trace_util_0.Count(_selectivity_00000, 63)
					return 0, nil, errors.Trace(err)
				}
				trace_util_0.Count(_selectivity_00000, 62)
				nodes[len(nodes)-1].Selectivity = cnt / float64(coll.Count)
				continue
			}
			trace_util_0.Count(_selectivity_00000, 58)
			cnt, err := coll.GetRowCountByColumnRanges(sc, id, ranges)
			if err != nil {
				trace_util_0.Count(_selectivity_00000, 64)
				return 0, nil, errors.Trace(err)
			}
			trace_util_0.Count(_selectivity_00000, 59)
			nodes[len(nodes)-1].Selectivity = cnt / float64(coll.Count)
		}
	}
	trace_util_0.Count(_selectivity_00000, 42)
	for id, idxInfo := range coll.Indices {
		trace_util_0.Count(_selectivity_00000, 65)
		idxCols := expression.FindPrefixOfIndex(extractedCols, coll.Idx2ColumnIDs[id])
		if len(idxCols) > 0 {
			trace_util_0.Count(_selectivity_00000, 66)
			lengths := make([]int, 0, len(idxCols))
			for i := 0; i < len(idxCols); i++ {
				trace_util_0.Count(_selectivity_00000, 70)
				lengths = append(lengths, idxInfo.Info.Columns[i].Length)
			}
			trace_util_0.Count(_selectivity_00000, 67)
			maskCovered, ranges, partCover, err := getMaskAndRanges(ctx, remainedExprs, ranger.IndexRangeType, lengths, idxCols...)
			if err != nil {
				trace_util_0.Count(_selectivity_00000, 71)
				return 0, nil, errors.Trace(err)
			}
			trace_util_0.Count(_selectivity_00000, 68)
			cnt, err := coll.GetRowCountByIndexRanges(sc, id, ranges)
			if err != nil {
				trace_util_0.Count(_selectivity_00000, 72)
				return 0, nil, errors.Trace(err)
			}
			trace_util_0.Count(_selectivity_00000, 69)
			selectivity := cnt / float64(coll.Count)
			nodes = append(nodes, &StatsNode{
				Tp:          IndexType,
				ID:          id,
				mask:        maskCovered,
				Ranges:      ranges,
				numCols:     len(idxInfo.Info.Columns),
				Selectivity: selectivity,
				partCover:   partCover,
			})
		}
	}
	trace_util_0.Count(_selectivity_00000, 43)
	usedSets := getUsableSetsByGreedy(nodes)
	// Initialize the mask with the full set.
	mask := (int64(1) << uint(len(remainedExprs))) - 1
	for _, set := range usedSets {
		trace_util_0.Count(_selectivity_00000, 73)
		mask &^= set.mask
		ret *= set.Selectivity
		// If `partCover` is true, it means that the conditions are in DNF form, and only part
		// of the DNF expressions are extracted as access conditions, so besides from the selectivity
		// of the extracted access conditions, we multiply another selectionFactor for the residual
		// conditions.
		if set.partCover {
			trace_util_0.Count(_selectivity_00000, 74)
			ret *= selectionFactor
		}
	}
	// If there's still conditions which cannot be calculated, we will multiply a selectionFactor.
	trace_util_0.Count(_selectivity_00000, 44)
	if mask > 0 {
		trace_util_0.Count(_selectivity_00000, 75)
		ret *= selectionFactor
	}
	trace_util_0.Count(_selectivity_00000, 45)
	return ret, nodes, nil
}

func getMaskAndRanges(ctx sessionctx.Context, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cols ...*expression.Column) (mask int64, ranges []*ranger.Range, partCover bool, err error) {
	trace_util_0.Count(_selectivity_00000, 76)
	sc := ctx.GetSessionVars().StmtCtx
	isDNF := false
	var accessConds, remainedConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		trace_util_0.Count(_selectivity_00000, 81)
		accessConds = ranger.ExtractAccessConditionsForColumn(exprs, cols[0].UniqueID)
		ranges, err = ranger.BuildColumnRange(accessConds, sc, cols[0].RetType, types.UnspecifiedLength)
	case ranger.IndexRangeType:
		trace_util_0.Count(_selectivity_00000, 82)
		var res *ranger.DetachRangeResult
		res, err = ranger.DetachCondAndBuildRangeForIndex(ctx, exprs, cols, lengths)
		ranges, accessConds, remainedConds, isDNF = res.Ranges, res.AccessConds, res.RemainedConds, res.IsDNFCond
	default:
		trace_util_0.Count(_selectivity_00000, 83)
		panic("should never be here")
	}
	trace_util_0.Count(_selectivity_00000, 77)
	if err != nil {
		trace_util_0.Count(_selectivity_00000, 84)
		return 0, nil, false, err
	}
	trace_util_0.Count(_selectivity_00000, 78)
	if isDNF && len(accessConds) > 0 {
		trace_util_0.Count(_selectivity_00000, 85)
		mask |= 1
		return mask, ranges, len(remainedConds) > 0, nil
	}
	trace_util_0.Count(_selectivity_00000, 79)
	for i := range exprs {
		trace_util_0.Count(_selectivity_00000, 86)
		for j := range accessConds {
			trace_util_0.Count(_selectivity_00000, 87)
			if exprs[i].Equal(ctx, accessConds[j]) {
				trace_util_0.Count(_selectivity_00000, 88)
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	trace_util_0.Count(_selectivity_00000, 80)
	return mask, ranges, false, nil
}

// getUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func getUsableSetsByGreedy(nodes []*StatsNode) (newBlocks []*StatsNode) {
	trace_util_0.Count(_selectivity_00000, 89)
	marked := make([]bool, len(nodes))
	mask := int64(math.MaxInt64)
	for {
		trace_util_0.Count(_selectivity_00000, 91)
		// Choose the index that covers most.
		bestID, bestCount, bestTp, bestNumCols, bestMask := -1, 0, ColType, 0, int64(0)
		for i, set := range nodes {
			trace_util_0.Count(_selectivity_00000, 94)
			if marked[i] {
				trace_util_0.Count(_selectivity_00000, 97)
				continue
			}
			trace_util_0.Count(_selectivity_00000, 95)
			curMask := set.mask & mask
			bits := popCount(curMask)
			// This set cannot cover any thing, just skip it.
			if bits == 0 {
				trace_util_0.Count(_selectivity_00000, 98)
				continue
			}
			// We greedy select the stats info based on:
			// (1): The stats type, always prefer the primary key or index.
			// (2): The number of expression that it covers, the more the better.
			// (3): The number of columns that it contains, the less the better.
			trace_util_0.Count(_selectivity_00000, 96)
			if (bestTp == ColType && set.Tp != ColType) || bestCount < bits || (bestCount == bits && bestNumCols > set.numCols) {
				trace_util_0.Count(_selectivity_00000, 99)
				bestID, bestCount, bestTp, bestNumCols, bestMask = i, bits, set.Tp, set.numCols, curMask
			}
		}
		trace_util_0.Count(_selectivity_00000, 92)
		if bestCount == 0 {
			trace_util_0.Count(_selectivity_00000, 100)
			break
		}

		// Update the mask, remove the bit that nodes[bestID].mask has.
		trace_util_0.Count(_selectivity_00000, 93)
		mask &^= bestMask

		newBlocks = append(newBlocks, nodes[bestID])
		marked[bestID] = true
	}
	trace_util_0.Count(_selectivity_00000, 90)
	return
}

// popCount is the digit sum of the binary representation of the number x.
func popCount(x int64) int {
	trace_util_0.Count(_selectivity_00000, 101)
	ret := 0
	// x -= x & -x, remove the lowest bit of the x.
	// e.g. result will be 2 if x is 3.
	for ; x > 0; x -= x & -x {
		trace_util_0.Count(_selectivity_00000, 103)
		ret++
	}
	trace_util_0.Count(_selectivity_00000, 102)
	return ret
}

var _selectivity_00000 = "statistics/selectivity.go"
