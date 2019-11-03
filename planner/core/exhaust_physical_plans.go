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
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 0)
	childProp := prop.Clone()
	us := PhysicalUnionScan{Conditions: p.conditions}.Init(p.ctx, p.stats, childProp)
	return []PhysicalPlan{us}
}

func getMaxSortPrefix(sortCols, allCols []*expression.Column) []int {
	trace_util_0.Count(_exhaust_physical_plans_00000, 1)
	tmpSchema := expression.NewSchema(allCols...)
	sortColOffsets := make([]int, 0, len(sortCols))
	for _, sortCol := range sortCols {
		trace_util_0.Count(_exhaust_physical_plans_00000, 3)
		offset := tmpSchema.ColumnIndex(sortCol)
		if offset == -1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 5)
			return sortColOffsets
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 4)
		sortColOffsets = append(sortColOffsets, offset)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 2)
	return sortColOffsets
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	trace_util_0.Count(_exhaust_physical_plans_00000, 6)
	maxLen := 0
	for _, candidateKeys := range candidates {
		trace_util_0.Count(_exhaust_physical_plans_00000, 8)
		matchedLen := 0
		for i := range keys {
			trace_util_0.Count(_exhaust_physical_plans_00000, 10)
			if i < len(candidateKeys) && keys[i].Equal(nil, candidateKeys[i]) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 11)
				matchedLen++
			} else {
				trace_util_0.Count(_exhaust_physical_plans_00000, 12)
				{
					break
				}
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 9)
		if matchedLen > maxLen {
			trace_util_0.Count(_exhaust_physical_plans_00000, 13)
			maxLen = matchedLen
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 7)
	return maxLen
}

func (p *LogicalJoin) moveEqualToOtherConditions(offsets []int) []expression.Expression {
	trace_util_0.Count(_exhaust_physical_plans_00000, 14)
	// Construct used equal condition set based on the equal condition offsets.
	usedEqConds := set.NewIntSet()
	for _, eqCondIdx := range offsets {
		trace_util_0.Count(_exhaust_physical_plans_00000, 17)
		usedEqConds.Insert(eqCondIdx)
	}

	// Construct otherConds, which is composed of the original other conditions
	// and the remained unused equal conditions.
	trace_util_0.Count(_exhaust_physical_plans_00000, 15)
	numOtherConds := len(p.OtherConditions) + len(p.EqualConditions) - len(offsets)
	otherConds := make([]expression.Expression, len(p.OtherConditions), numOtherConds)
	copy(otherConds, p.OtherConditions)
	for eqCondIdx := range p.EqualConditions {
		trace_util_0.Count(_exhaust_physical_plans_00000, 18)
		if !usedEqConds.Exist(eqCondIdx) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 19)
			otherConds = append(otherConds, p.EqualConditions[eqCondIdx])
		}
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 16)
	return otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *property.PhysicalProperty) ([]*property.PhysicalProperty, bool) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 20)
	lProp := property.NewPhysicalProperty(property.RootTaskType, p.LeftKeys, false, math.MaxFloat64, false)
	rProp := property.NewPhysicalProperty(property.RootTaskType, p.RightKeys, false, math.MaxFloat64, false)
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 22)
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		all, desc := prop.AllSameOrder()
		if !all || desc {
			trace_util_0.Count(_exhaust_physical_plans_00000, 26)
			return nil, false
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 23)
		if !prop.IsPrefix(lProp) && !prop.IsPrefix(rProp) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 27)
			return nil, false
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 24)
		if prop.IsPrefix(rProp) && p.JoinType == LeftOuterJoin {
			trace_util_0.Count(_exhaust_physical_plans_00000, 28)
			return nil, false
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 25)
		if prop.IsPrefix(lProp) && p.JoinType == RightOuterJoin {
			trace_util_0.Count(_exhaust_physical_plans_00000, 29)
			return nil, false
		}
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 21)
	return []*property.PhysicalProperty{lProp, rProp}, true
}

func (p *LogicalJoin) getMergeJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 30)
	joins := make([]PhysicalPlan, 0, len(p.leftProperties))
	// The leftProperties caches all the possible properties that are provided by its children.
	for _, lhsChildProperty := range p.leftProperties {
		trace_util_0.Count(_exhaust_physical_plans_00000, 33)
		offsets := getMaxSortPrefix(lhsChildProperty, p.LeftJoinKeys)
		if len(offsets) == 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 36)
			continue
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 34)
		leftKeys := lhsChildProperty[:len(offsets)]
		rightKeys := expression.NewSchema(p.RightJoinKeys...).ColumnsByIndices(offsets)

		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 37)
			continue
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 35)
		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		offsets = offsets[:prefixLen]
		mergeJoin := PhysicalMergeJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			LeftKeys:        leftKeys,
			RightKeys:       rightKeys,
		}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
		mergeJoin.SetSchema(p.schema)
		mergeJoin.OtherConditions = p.moveEqualToOtherConditions(offsets)
		mergeJoin.initCompareFuncs()
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			trace_util_0.Count(_exhaust_physical_plans_00000, 38)
			// Adjust expected count for children nodes.
			if prop.ExpectedCnt < p.stats.RowCount {
				trace_util_0.Count(_exhaust_physical_plans_00000, 40)
				expCntScale := prop.ExpectedCnt / p.stats.RowCount
				reqProps[0].ExpectedCnt = p.children[0].statsInfo().RowCount * expCntScale
				reqProps[1].ExpectedCnt = p.children[1].statsInfo().RowCount * expCntScale
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 39)
			mergeJoin.childrenReqProps = reqProps
			joins = append(joins, mergeJoin)
		}
	}
	// If TiDB_SMJ hint is existed && no join keys in children property,
	// it should to enforce merge join.
	trace_util_0.Count(_exhaust_physical_plans_00000, 31)
	if len(joins) == 0 && (p.preferJoinType&preferMergeJoin) > 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 41)
		return p.getEnforcedMergeJoin(prop)
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 32)
	return joins
}

// Change JoinKeys order, by offsets array
// offsets array is generate by prop check
func getNewJoinKeysByOffsets(oldJoinKeys []*expression.Column, offsets []int) []*expression.Column {
	trace_util_0.Count(_exhaust_physical_plans_00000, 42)
	newKeys := make([]*expression.Column, 0, len(oldJoinKeys))
	for _, offset := range offsets {
		trace_util_0.Count(_exhaust_physical_plans_00000, 45)
		newKeys = append(newKeys, oldJoinKeys[offset])
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 43)
	for pos, key := range oldJoinKeys {
		trace_util_0.Count(_exhaust_physical_plans_00000, 46)
		isExist := false
		for _, p := range offsets {
			trace_util_0.Count(_exhaust_physical_plans_00000, 48)
			if p == pos {
				trace_util_0.Count(_exhaust_physical_plans_00000, 49)
				isExist = true
				break
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 47)
		if !isExist {
			trace_util_0.Count(_exhaust_physical_plans_00000, 50)
			newKeys = append(newKeys, key)
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 44)
	return newKeys
}

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 51)
	// Check whether SMJ can satisfy the required property
	offsets := make([]int, 0, len(p.LeftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		trace_util_0.Count(_exhaust_physical_plans_00000, 54)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 52)
	for _, item := range prop.Items {
		trace_util_0.Count(_exhaust_physical_plans_00000, 55)
		isExist := false
		for joinKeyPos := 0; joinKeyPos < len(p.LeftJoinKeys); joinKeyPos++ {
			trace_util_0.Count(_exhaust_physical_plans_00000, 57)
			var key *expression.Column
			if item.Col.Equal(p.ctx, p.LeftJoinKeys[joinKeyPos]) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 63)
				key = p.LeftJoinKeys[joinKeyPos]
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 58)
			if item.Col.Equal(p.ctx, p.RightJoinKeys[joinKeyPos]) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 64)
				key = p.RightJoinKeys[joinKeyPos]
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 59)
			if key == nil {
				trace_util_0.Count(_exhaust_physical_plans_00000, 65)
				continue
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 60)
			for i := 0; i < len(offsets); i++ {
				trace_util_0.Count(_exhaust_physical_plans_00000, 66)
				if offsets[i] == joinKeyPos {
					trace_util_0.Count(_exhaust_physical_plans_00000, 67)
					isExist = true
					break
				}
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 61)
			if !isExist {
				trace_util_0.Count(_exhaust_physical_plans_00000, 68)
				offsets = append(offsets, joinKeyPos)
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 62)
			isExist = true
			break
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 56)
		if !isExist {
			trace_util_0.Count(_exhaust_physical_plans_00000, 69)
			return nil
		}
	}
	// Generate the enforced sort merge join
	trace_util_0.Count(_exhaust_physical_plans_00000, 53)
	leftKeys := getNewJoinKeysByOffsets(p.LeftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(p.RightJoinKeys, offsets)
	lProp := property.NewPhysicalProperty(property.RootTaskType, leftKeys, desc, math.MaxFloat64, true)
	rProp := property.NewPhysicalProperty(property.RootTaskType, rightKeys, desc, math.MaxFloat64, true)
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftKeys:        leftKeys,
		RightKeys:       rightKeys,
		OtherConditions: p.OtherConditions,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	enforcedPhysicalMergeJoin.SetSchema(p.schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	trace_util_0.Count(_exhaust_physical_plans_00000, 70)
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftKeys))
	for i := range p.LeftKeys {
		trace_util_0.Count(_exhaust_physical_plans_00000, 71)
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.LeftKeys[i], p.RightKeys[i]))
	}
}

func (p *LogicalJoin) getHashJoins(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 72)
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 75) // hash join doesn't promise any orders
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 73)
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 76)
		joins = append(joins, p.getHashJoin(prop, 1))
	case RightOuterJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 77)
		joins = append(joins, p.getHashJoin(prop, 0))
	case InnerJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 78)
		joins = append(joins, p.getHashJoin(prop, 1))
		joins = append(joins, p.getHashJoin(prop, 0))
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 74)
	return joins
}

func (p *LogicalJoin) getHashJoin(prop *property.PhysicalProperty, innerIdx int) *PhysicalHashJoin {
	trace_util_0.Count(_exhaust_physical_plans_00000, 79)
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if prop.ExpectedCnt < p.stats.RowCount {
		trace_util_0.Count(_exhaust_physical_plans_00000, 81)
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = p.children[1-innerIdx].statsInfo().RowCount * expCntScale
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 80)
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     uint(p.ctx.GetSessionVars().HashJoinConcurrency),
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// joinKeysMatchIndex checks whether the join key is in the index.
// It returns a slice a[] what a[i] means keys[i] is related with indexCols[a[i]], -1 for no matching column.
// It will return nil if there's no column that matches index.
func joinKeysMatchIndex(keys, indexCols []*expression.Column, colLengths []int) []int {
	trace_util_0.Count(_exhaust_physical_plans_00000, 82)
	keyOff2IdxOff := make([]int, len(keys))
	for i := range keyOff2IdxOff {
		trace_util_0.Count(_exhaust_physical_plans_00000, 86)
		keyOff2IdxOff[i] = -1
	}
	// There should be at least one column in join keys which can match the index's column.
	trace_util_0.Count(_exhaust_physical_plans_00000, 83)
	matched := false
	tmpSchema := expression.NewSchema(keys...)
	for i, idxCol := range indexCols {
		trace_util_0.Count(_exhaust_physical_plans_00000, 87)
		if colLengths[i] != types.UnspecifiedLength {
			trace_util_0.Count(_exhaust_physical_plans_00000, 90)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 88)
		keyOff := tmpSchema.ColumnIndex(idxCol)
		if keyOff == -1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 91)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 89)
		matched = true
		keyOff2IdxOff[keyOff] = i
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 84)
	if !matched {
		trace_util_0.Count(_exhaust_physical_plans_00000, 92)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 85)
	return keyOff2IdxOff
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(prop *property.PhysicalProperty, outerIdx int, innerPlan PhysicalPlan,
	ranges []*ranger.Range, keyOff2IdxOff []int, compareFilters *ColWithCmpFuncManager) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 93)
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 98)
		outerJoinKeys = p.LeftJoinKeys
		innerJoinKeys = p.RightJoinKeys
	} else {
		trace_util_0.Count(_exhaust_physical_plans_00000, 99)
		{
			innerJoinKeys = p.LeftJoinKeys
			outerJoinKeys = p.RightJoinKeys
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 94)
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerSchema) || !all {
		trace_util_0.Count(_exhaust_physical_plans_00000, 100)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 95)
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, Items: prop.Items}
	if prop.ExpectedCnt < p.stats.RowCount {
		trace_util_0.Count(_exhaust_physical_plans_00000, 101)
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[outerIdx].ExpectedCnt = p.children[outerIdx].statsInfo().RowCount * expCntScale
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 96)
	newInnerKeys := make([]*expression.Column, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerJoinKeys))
	newKeyOff := make([]int, 0, len(keyOff2IdxOff))
	newOtherConds := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(newOtherConds, p.OtherConditions)
	for keyOff, idxOff := range keyOff2IdxOff {
		trace_util_0.Count(_exhaust_physical_plans_00000, 102)
		if keyOff2IdxOff[keyOff] < 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 104)
			newOtherConds = append(newOtherConds, p.EqualConditions[keyOff])
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 103)
		newInnerKeys = append(newInnerKeys, innerJoinKeys[keyOff])
		newOuterKeys = append(newOuterKeys, outerJoinKeys[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 97)
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		DefaultValues:   p.DefaultValues,
		innerPlan:       innerPlan,
		KeyOff2IdxOff:   newKeyOff,
		Ranges:          ranges,
		CompareFilters:  compareFilters,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	join.SetSchema(p.schema)
	return []PhysicalPlan{join}
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *property.PhysicalProperty, outerIdx int) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 105)
	innerChild := p.children[1-outerIdx]
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 113)
		outerJoinKeys = p.LeftJoinKeys
		innerJoinKeys = p.RightJoinKeys
	} else {
		trace_util_0.Count(_exhaust_physical_plans_00000, 114)
		{
			innerJoinKeys = p.LeftJoinKeys
			outerJoinKeys = p.RightJoinKeys
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 106)
	ds, isDataSource := innerChild.(*DataSource)
	us, isUnionScan := innerChild.(*LogicalUnionScan)
	if !isDataSource && !isUnionScan {
		trace_util_0.Count(_exhaust_physical_plans_00000, 115)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 107)
	if isUnionScan {
		trace_util_0.Count(_exhaust_physical_plans_00000, 116)
		// The child of union scan may be union all for partition table.
		ds, isDataSource = us.Children()[0].(*DataSource)
		if !isDataSource {
			trace_util_0.Count(_exhaust_physical_plans_00000, 117)
			return nil
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 108)
	var tblPath *accessPath
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_exhaust_physical_plans_00000, 118)
		if path.isTablePath {
			trace_util_0.Count(_exhaust_physical_plans_00000, 119)
			tblPath = path
			break
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 109)
	if pkCol := ds.getPKIsHandleCol(); pkCol != nil && tblPath != nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 120)
		keyOff2IdxOff := make([]int, len(innerJoinKeys))
		pkMatched := false
		for i, key := range innerJoinKeys {
			trace_util_0.Count(_exhaust_physical_plans_00000, 122)
			if !key.Equal(nil, pkCol) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 124)
				keyOff2IdxOff[i] = -1
				continue
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 123)
			pkMatched = true
			keyOff2IdxOff[i] = 0
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 121)
		if pkMatched {
			trace_util_0.Count(_exhaust_physical_plans_00000, 125)
			innerPlan := p.constructInnerTableScan(ds, pkCol, outerJoinKeys, us)
			// Since the primary key means one value corresponding to exact one row, this will always be a no worse one
			// comparing to other index.
			return p.constructIndexJoin(prop, outerIdx, innerPlan, nil, keyOff2IdxOff, nil)
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 110)
	helper := &indexJoinBuildHelper{join: p}
	for _, path := range ds.possibleAccessPaths {
		trace_util_0.Count(_exhaust_physical_plans_00000, 126)
		if path.isTablePath {
			trace_util_0.Count(_exhaust_physical_plans_00000, 128)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 127)
		indexInfo := path.index
		err := helper.analyzeLookUpFilters(indexInfo, ds, innerJoinKeys)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 129)
			logutil.Logger(context.Background()).Warn("build index join failed", zap.Error(err))
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 111)
	if helper.chosenIndexInfo != nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 130)
		keyOff2IdxOff := make([]int, len(innerJoinKeys))
		for i := range keyOff2IdxOff {
			trace_util_0.Count(_exhaust_physical_plans_00000, 133)
			keyOff2IdxOff[i] = -1
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 131)
		for idxOff, keyOff := range helper.idxOff2KeyOff {
			trace_util_0.Count(_exhaust_physical_plans_00000, 134)
			if keyOff != -1 {
				trace_util_0.Count(_exhaust_physical_plans_00000, 135)
				keyOff2IdxOff[keyOff] = idxOff
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 132)
		idxCols, _ := expression.IndexInfo2Cols(ds.schema.Columns, helper.chosenIndexInfo)
		rangeInfo := helper.buildRangeDecidedByInformation(idxCols, outerJoinKeys)
		innerPlan := p.constructInnerIndexScan(ds, helper.chosenIndexInfo, helper.chosenRemained, outerJoinKeys, us, rangeInfo)
		return p.constructIndexJoin(prop, outerIdx, innerPlan, helper.chosenRanges, keyOff2IdxOff, helper.lastColManager)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 112)
	return nil
}

type indexJoinBuildHelper struct {
	join *LogicalJoin

	chosenIndexInfo *model.IndexInfo
	maxUsedCols     int
	chosenAccess    []expression.Expression
	chosenRemained  []expression.Expression
	idxOff2KeyOff   []int
	lastColManager  *ColWithCmpFuncManager
	chosenRanges    []*ranger.Range

	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	curIdxOff2KeyOff    []int
}

func (ijHelper *indexJoinBuildHelper) buildRangeDecidedByInformation(idxCols []*expression.Column, outerJoinKeys []*expression.Column) string {
	trace_util_0.Count(_exhaust_physical_plans_00000, 136)
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range ijHelper.idxOff2KeyOff {
		trace_util_0.Count(_exhaust_physical_plans_00000, 139)
		if keyOff == -1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 142)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 140)
		if !isFirst {
			trace_util_0.Count(_exhaust_physical_plans_00000, 143)
			buffer.WriteString(" ")
		} else {
			trace_util_0.Count(_exhaust_physical_plans_00000, 144)
			{
				isFirst = false
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 141)
		buffer.WriteString(fmt.Sprintf("eq(%v, %v)", idxCols[idxOff], outerJoinKeys[keyOff]))
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 137)
	for _, access := range ijHelper.chosenAccess {
		trace_util_0.Count(_exhaust_physical_plans_00000, 145)
		if !isFirst {
			trace_util_0.Count(_exhaust_physical_plans_00000, 147)
			buffer.WriteString(" ")
		} else {
			trace_util_0.Count(_exhaust_physical_plans_00000, 148)
			{
				isFirst = false
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 146)
		buffer.WriteString(fmt.Sprintf("%v", access))
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 138)
	buffer.WriteString("]")
	return buffer.String()
}

// constructInnerTableScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerTableScan(ds *DataSource, pk *expression.Column, outerJoinKeys []*expression.Column, us *LogicalUnionScan) PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 149)
	ranges := ranger.FullIntRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		filterCondition: ds.pushedDownConds,
		Ranges:          ranges,
		rangeDecidedBy:  outerJoinKeys,
	}.Init(ds.ctx)
	ts.SetSchema(ds.schema)

	ts.stats = property.NewSimpleStats(1)
	ts.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		trace_util_0.Count(_exhaust_physical_plans_00000, 151)
		ts.stats.StatsVersion = statistics.PseudoVersion
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 150)
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
	}
	selStats := ts.stats.Scale(selectionFactor)
	ts.addPushedDownSelection(copTask, selStats)
	t := finishCopTask(ds.ctx, copTask)
	reader := t.plan()
	return p.constructInnerUnionScan(us, reader)
}

func (p *LogicalJoin) constructInnerUnionScan(us *LogicalUnionScan, reader PhysicalPlan) PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 152)
	if us == nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 154)
		return reader
	}
	// Use `reader.stats` instead of `us.stats` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	trace_util_0.Count(_exhaust_physical_plans_00000, 153)
	physicalUnionScan := PhysicalUnionScan{Conditions: us.conditions}.Init(us.ctx, reader.statsInfo(), nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// constructInnerIndexScan is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScan(ds *DataSource, idx *model.IndexInfo, filterConds []expression.Expression,
	outerJoinKeys []*expression.Column, us *LogicalUnionScan, rangeInfo string) PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 155)
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		dataSourceSchema: ds.schema,
		KeepOrder:        false,
		Ranges:           ranger.FullRange(),
		rangeInfo:        rangeInfo,
	}.Init(ds.ctx)

	var rowCount float64
	idxHist, ok := ds.statisticTable.Indices[idx.ID]
	if ok && !ds.statisticTable.Pseudo {
		trace_util_0.Count(_exhaust_physical_plans_00000, 160)
		rowCount = idxHist.AvgCountPerNotNullValue(ds.statisticTable.Count)
	} else {
		trace_util_0.Count(_exhaust_physical_plans_00000, 161)
		{
			rowCount = ds.statisticTable.PseudoAvgCountPerValue()
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 156)
	is.stats = property.NewSimpleStats(rowCount)
	is.stats.StatsVersion = ds.statisticTable.Version
	if ds.statisticTable.Pseudo {
		trace_util_0.Count(_exhaust_physical_plans_00000, 162)
		is.stats.StatsVersion = statistics.PseudoVersion
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 157)
	cop := &copTask{
		indexPlan: is,
	}
	if !isCoveringIndex(ds.schema.Columns, is.Index.Columns, is.Table.PKIsHandle) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 163)
		// On this way, it's double read case.
		ts := PhysicalTableScan{Columns: ds.Columns, Table: is.Table}.Init(ds.ctx)
		ts.SetSchema(is.dataSourceSchema)
		cop.tablePlan = ts
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 158)
	is.initSchema(ds.id, idx, cop.tablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(filterConds, idx.Columns, ds.tableInfo)
	path := &accessPath{
		indexFilters:     indexConds,
		tableFilters:     tblConds,
		countAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(indexConds) > 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 164)
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, indexConds)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 166)
			logutil.Logger(context.Background()).Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 165)
		path.countAfterIndex = rowCount * selectivity
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 159)
	selectivity := ds.stats.RowCount / ds.tableStats.RowCount
	finalStats := ds.stats.ScaleByExpectCnt(selectivity * rowCount)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	t := finishCopTask(ds.ctx, cop)
	reader := t.plan()
	return p.constructInnerUnionScan(us, reader)
}

var symmetricOp = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}

// ColWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type ColWithCmpFuncManager struct {
	targetCol         *expression.Column
	colLength         int
	OpType            []string
	opArg             []expression.Expression
	tmpConstant       []*expression.Constant
	affectedColSchema *expression.Schema
	compareFuncs      []chunk.CompareFunc
}

func (cwc *ColWithCmpFuncManager) appendNewExpr(opName string, arg expression.Expression, affectedCols []*expression.Column) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 167)
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.tmpConstant = append(cwc.tmpConstant, &expression.Constant{RetType: cwc.targetCol.RetType})
	for _, col := range affectedCols {
		trace_util_0.Count(_exhaust_physical_plans_00000, 168)
		if cwc.affectedColSchema.Contains(col) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 170)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 169)
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.affectedColSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *ColWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	trace_util_0.Count(_exhaust_physical_plans_00000, 171)
	for i, col := range cwc.affectedColSchema.Columns {
		trace_util_0.Count(_exhaust_physical_plans_00000, 173)
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 174)
			return ret
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 172)
	return 0
}

// BuildRangesByRow will build range of the given row. It will eval each function's arg then call BuildRange.
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx sessionctx.Context, row chunk.Row) ([]*ranger.Range, error) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 175)
	exprs := make([]expression.Expression, len(cwc.OpType))
	for i, opType := range cwc.OpType {
		trace_util_0.Count(_exhaust_physical_plans_00000, 178)
		constantArg, err := cwc.opArg[i].Eval(row)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 181)
			return nil, err
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 179)
		cwc.tmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(ctx, opType, types.NewFieldType(mysql.TypeTiny), cwc.targetCol, cwc.tmpConstant[i])
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 182)
			return nil, err
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 180)
		exprs = append(exprs, newExpr)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 176)
	ranges, err := ranger.BuildColumnRange(exprs, ctx.GetSessionVars().StmtCtx, cwc.targetCol.RetType, cwc.colLength)
	if err != nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 183)
		return nil, err
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 177)
	return ranges, nil
}

func (cwc *ColWithCmpFuncManager) resolveIndices(schema *expression.Schema) (err error) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 184)
	for i := range cwc.opArg {
		trace_util_0.Count(_exhaust_physical_plans_00000, 186)
		cwc.opArg[i], err = cwc.opArg[i].ResolveIndices(schema)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 187)
			return err
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 185)
	return nil
}

// String implements Stringer interface.
func (cwc *ColWithCmpFuncManager) String() string {
	trace_util_0.Count(_exhaust_physical_plans_00000, 188)
	buffer := bytes.NewBufferString("")
	for i := range cwc.OpType {
		trace_util_0.Count(_exhaust_physical_plans_00000, 190)
		buffer.WriteString(fmt.Sprintf("%v(%v, %v)", cwc.OpType[i], cwc.targetCol, cwc.opArg[i]))
		if i < len(cwc.OpType)-1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 191)
			buffer.WriteString(" ")
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 189)
	return buffer.String()
}

func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.Column, idxCols []*expression.Column, colLens []int) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 192)
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxCols))
	ijHelper.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	ijHelper.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		trace_util_0.Count(_exhaust_physical_plans_00000, 193)
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 195)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 194)
		ijHelper.curNotUsedIndexCols = append(ijHelper.curNotUsedIndexCols, idxCol)
		ijHelper.curNotUsedColLens = append(ijHelper.curNotUsedColLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// uselessFilters is the conditions which cannot be used for building ranges.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(innerPlan *DataSource) (usefulEqOrInFilters, uselessFilters, remainingRangeCandidates []expression.Expression) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 196)
	uselessFilters = make([]expression.Expression, 0, len(innerPlan.pushedDownConds))
	var remainedEqOrIn []expression.Expression
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _ = ranger.ExtractEqAndInCondition(
		innerPlan.ctx, innerPlan.pushedDownConds,
		ijHelper.curNotUsedIndexCols,
		ijHelper.curNotUsedColLens,
	)
	uselessFilters = append(uselessFilters, remainedEqOrIn...)
	return usefulEqOrInFilters, uselessFilters, remainingRangeCandidates
}

// buildLastColManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func (ijHelper *indexJoinBuildHelper) buildLastColManager(nextCol *expression.Column,
	innerPlan *DataSource, cwc *ColWithCmpFuncManager) []expression.Expression {
	trace_util_0.Count(_exhaust_physical_plans_00000, 197)
	var lastColAccesses []expression.Expression
loopOtherConds:
	for _, filter := range ijHelper.join.OtherConditions {
		trace_util_0.Count(_exhaust_physical_plans_00000, 199)
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 204)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 200)
		var funcName string
		var anotherArg expression.Expression
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.Equal(nil, nextCol) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 205)
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else {
			trace_util_0.Count(_exhaust_physical_plans_00000, 206)
			if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.Equal(nil, nextCol) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 207)
				anotherArg = sf.GetArgs()[0]
				// The column manager always build expression in the form of col op arg1.
				// So we need use the symmetric one of the current function.
				funcName = symmetricOp[sf.FuncName.L]
			} else {
				trace_util_0.Count(_exhaust_physical_plans_00000, 208)
				{
					continue
				}
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 201)
		affectedCols := expression.ExtractColumns(anotherArg)
		if len(affectedCols) == 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 209)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 202)
		for _, col := range affectedCols {
			trace_util_0.Count(_exhaust_physical_plans_00000, 210)
			if innerPlan.schema.Contains(col) {
				trace_util_0.Count(_exhaust_physical_plans_00000, 211)
				continue loopOtherConds
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 203)
		lastColAccesses = append(lastColAccesses, sf)
		cwc.appendNewExpr(funcName, anotherArg, affectedCols)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 198)
	return lastColAccesses
}

// removeUselessEqAndInFunc removes the useless eq/in conditions. It's designed for the following case:
//   t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//   In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//   It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//   [0 -1 -1].
//   So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(
	idxCols []*expression.Column,
	notKeyEqAndIn []expression.Expression) (
	usefulEqAndIn, uselessOnes []expression.Expression,
) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 212)
	ijHelper.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		trace_util_0.Count(_exhaust_physical_plans_00000, 214)
		if ijHelper.curIdxOff2KeyOff[idxColPos] != -1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 218)
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 215)
		if notKeyColPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexCols[notKeyColPos].Equal(nil, idxCols[idxColPos]) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 219)
			notKeyColPos++
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 216)
		for i := idxColPos + 1; i < len(idxCols); i++ {
			trace_util_0.Count(_exhaust_physical_plans_00000, 220)
			ijHelper.curIdxOff2KeyOff[i] = -1
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 217)
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyColPos)
		remained = append(remained, notKeyEqAndIn[notKeyColPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyColPos]
		return notKeyEqAndIn, remained
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 213)
	return notKeyEqAndIn, nil
}

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(indexInfo *model.IndexInfo, innerPlan *DataSource, innerJoinKeys []*expression.Column) error {
	trace_util_0.Count(_exhaust_physical_plans_00000, 221)
	idxCols, colLengths := expression.IndexInfo2Cols(innerPlan.schema.Columns, indexInfo)
	if len(idxCols) == 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 228)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 222)
	accesses := make([]expression.Expression, 0, len(idxCols))
	ijHelper.resetContextForIndex(innerJoinKeys, idxCols, colLengths)
	notKeyEqAndIn, remained, rangeFilterCandidates := ijHelper.findUsefulEqAndInFilters(innerPlan)
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(idxCols, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 229)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 223)
	accesses = append(accesses, notKeyEqAndIn...)
	remained = append(remained, remainedEqAndIn...)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 230)
		return nil
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	trace_util_0.Count(_exhaust_physical_plans_00000, 224)
	if lastColPos == len(idxCols) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 231)
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 234)
			return nil
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 232)
		remained = append(remained, rangeFilterCandidates...)
		ranges, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 235)
			return err
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 233)
		ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, nil)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 225)
	lastPossibleCol := idxCols[lastColPos]
	lastColManager := &ColWithCmpFuncManager{
		targetCol:         lastPossibleCol,
		colLength:         colLengths[lastColPos],
		affectedColSchema: expression.NewSchema(),
	}
	lastColAccess := ijHelper.buildLastColManager(lastPossibleCol, innerPlan, lastColManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastColAccess) == 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 236)
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 241)
			return nil
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 237)
		colAccesses, colRemained := ranger.DetachCondsForColumn(ijHelper.join.ctx, rangeFilterCandidates, lastPossibleCol)
		var ranges, nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 242)
			nextColRange, err = ranger.BuildColumnRange(colAccesses, ijHelper.join.ctx.GetSessionVars().StmtCtx, lastPossibleCol.RetType, colLengths[lastColPos])
			if err != nil {
				trace_util_0.Count(_exhaust_physical_plans_00000, 243)
				return err
			}
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 238)
		ranges, err = ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextColRange, false)
		if err != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 244)
			return err
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 239)
		remained = append(remained, colRemained...)
		if colLengths[lastColPos] != types.UnspecifiedLength {
			trace_util_0.Count(_exhaust_physical_plans_00000, 245)
			remained = append(remained, colAccesses...)
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 240)
		accesses = append(accesses, colAccesses...)
		ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, nil)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 226)
	accesses = append(accesses, lastColAccess...)
	remained = append(remained, rangeFilterCandidates...)
	ranges, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true)
	if err != nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 246)
		return err
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 227)
	ijHelper.updateBestChoice(ranges, indexInfo, accesses, remained, lastColManager)
	return nil
}

func (ijHelper *indexJoinBuildHelper) updateBestChoice(ranges []*ranger.Range, idxInfo *model.IndexInfo, accesses,
	remained []expression.Expression, lastColManager *ColWithCmpFuncManager) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 247)
	// We choose the index by the number of used columns of the range, the much the better.
	// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
	// But obviously when the range is nil, we don't need index join.
	if len(ranges) > 0 && len(ranges[0].LowVal) > ijHelper.maxUsedCols {
		trace_util_0.Count(_exhaust_physical_plans_00000, 248)
		ijHelper.chosenIndexInfo = idxInfo
		ijHelper.maxUsedCols = len(ranges[0].LowVal)
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.chosenRemained = remained
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		ijHelper.lastColManager = lastColManager
	}
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextColRange []*ranger.Range, haveExtraCol bool) (ranges []*ranger.Range, err error) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 249)
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	if nextColRange != nil {
		trace_util_0.Count(_exhaust_physical_plans_00000, 252)
		for _, colRan := range nextColRange {
			trace_util_0.Count(_exhaust_physical_plans_00000, 253)
			// The range's exclude status is the same with last col's.
			ran := &ranger.Range{
				LowVal:      make([]types.Datum, pointLength, pointLength+1),
				HighVal:     make([]types.Datum, pointLength, pointLength+1),
				LowExclude:  colRan.LowExclude,
				HighExclude: colRan.HighExclude,
			}
			ran.LowVal = append(ran.LowVal, colRan.LowVal[0])
			ran.HighVal = append(ran.HighVal, colRan.HighVal[0])
			ranges = append(ranges, ran)
		}
	} else {
		trace_util_0.Count(_exhaust_physical_plans_00000, 254)
		if haveExtraCol {
			trace_util_0.Count(_exhaust_physical_plans_00000, 255)
			// Reserve a position for the last col.
			ranges = append(ranges, &ranger.Range{
				LowVal:  make([]types.Datum, pointLength+1, pointLength+1),
				HighVal: make([]types.Datum, pointLength+1, pointLength+1),
			})
		} else {
			trace_util_0.Count(_exhaust_physical_plans_00000, 256)
			{
				ranges = append(ranges, &ranger.Range{
					LowVal:  make([]types.Datum, pointLength, pointLength),
					HighVal: make([]types.Datum, pointLength, pointLength),
				})
			}
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 250)
	emptyRow := chunk.Row{}
	for i, j := 0, 0; j < len(eqAndInFuncs); i++ {
		trace_util_0.Count(_exhaust_physical_plans_00000, 257)
		// This position is occupied by join key.
		if ijHelper.curIdxOff2KeyOff[i] != -1 {
			trace_util_0.Count(_exhaust_physical_plans_00000, 261)
			continue
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 258)
		sf := eqAndInFuncs[j].(*expression.ScalarFunction)
		// Deal with the first two args.
		if _, ok := sf.GetArgs()[0].(*expression.Column); ok {
			trace_util_0.Count(_exhaust_physical_plans_00000, 262)
			for _, ran := range ranges {
				trace_util_0.Count(_exhaust_physical_plans_00000, 263)
				ran.LowVal[i], err = sf.GetArgs()[1].Eval(emptyRow)
				if err != nil {
					trace_util_0.Count(_exhaust_physical_plans_00000, 265)
					return nil, err
				}
				trace_util_0.Count(_exhaust_physical_plans_00000, 264)
				ran.HighVal[i] = ran.LowVal[i]
			}
		} else {
			trace_util_0.Count(_exhaust_physical_plans_00000, 266)
			{
				for _, ran := range ranges {
					trace_util_0.Count(_exhaust_physical_plans_00000, 267)
					ran.LowVal[i], err = sf.GetArgs()[0].Eval(emptyRow)
					if err != nil {
						trace_util_0.Count(_exhaust_physical_plans_00000, 269)
						return nil, err
					}
					trace_util_0.Count(_exhaust_physical_plans_00000, 268)
					ran.HighVal[i] = ran.LowVal[i]
				}
			}
		}
		// If the length of in function's constant list is more than one, we will expand ranges.
		trace_util_0.Count(_exhaust_physical_plans_00000, 259)
		curRangeLen := len(ranges)
		for argIdx := 2; argIdx < len(sf.GetArgs()); argIdx++ {
			trace_util_0.Count(_exhaust_physical_plans_00000, 270)
			newRanges := make([]*ranger.Range, 0, curRangeLen)
			for oldRangeIdx := 0; oldRangeIdx < curRangeLen; oldRangeIdx++ {
				trace_util_0.Count(_exhaust_physical_plans_00000, 272)
				newRange := ranges[oldRangeIdx].Clone()
				newRange.LowVal[i], err = sf.GetArgs()[argIdx].Eval(emptyRow)
				if err != nil {
					trace_util_0.Count(_exhaust_physical_plans_00000, 274)
					return nil, err
				}
				trace_util_0.Count(_exhaust_physical_plans_00000, 273)
				newRange.HighVal[i] = newRange.LowVal[i]
				newRanges = append(newRanges, newRange)
			}
			trace_util_0.Count(_exhaust_physical_plans_00000, 271)
			ranges = append(ranges, newRanges...)
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 260)
		j++
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 251)
	return ranges, nil
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *property.PhysicalProperty) (indexJoins []PhysicalPlan, forced bool) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 275)
	rightOuter := (p.preferJoinType & preferLeftAsIndexInner) > 0
	leftOuter := (p.preferJoinType & preferRightAsIndexInner) > 0
	hasIndexJoinHint := leftOuter || rightOuter

	defer func() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 278)
		if !forced && hasIndexJoinHint {
			trace_util_0.Count(_exhaust_physical_plans_00000, 279)
			// Construct warning message prefix.
			errMsg := "Optimizer Hint TIDB_INLJ is inapplicable"
			if p.hintInfo != nil {
				trace_util_0.Count(_exhaust_physical_plans_00000, 282)
				errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(TiDBIndexNestedLoopJoin, p.hintInfo.indexNestedLoopJoinTables))
			}

			// Append inapplicable reason.
			trace_util_0.Count(_exhaust_physical_plans_00000, 280)
			if len(p.EqualConditions) == 0 {
				trace_util_0.Count(_exhaust_physical_plans_00000, 283)
				errMsg += " without column equal ON condition"
			}

			// Generate warning message to client.
			trace_util_0.Count(_exhaust_physical_plans_00000, 281)
			warning := ErrInternal.GenWithStack(errMsg)
			p.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		}
	}()

	trace_util_0.Count(_exhaust_physical_plans_00000, 276)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 284)
		join := p.getIndexJoinByOuterIdx(prop, 0)
		return join, join != nil && leftOuter
	case RightOuterJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 285)
		join := p.getIndexJoinByOuterIdx(prop, 1)
		return join, join != nil && rightOuter
	case InnerJoin:
		trace_util_0.Count(_exhaust_physical_plans_00000, 286)
		lhsCardinality := p.Children()[0].statsInfo().Count()
		rhsCardinality := p.Children()[1].statsInfo().Count()

		leftJoins := p.getIndexJoinByOuterIdx(prop, 0)
		if leftJoins != nil && leftOuter && !rightOuter {
			trace_util_0.Count(_exhaust_physical_plans_00000, 291)
			return leftJoins, true
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 287)
		rightJoins := p.getIndexJoinByOuterIdx(prop, 1)
		if rightJoins != nil && rightOuter && !leftOuter {
			trace_util_0.Count(_exhaust_physical_plans_00000, 292)
			return rightJoins, true
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 288)
		if leftJoins != nil && lhsCardinality < rhsCardinality {
			trace_util_0.Count(_exhaust_physical_plans_00000, 293)
			return leftJoins, hasIndexJoinHint
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 289)
		if rightJoins != nil && rhsCardinality < lhsCardinality {
			trace_util_0.Count(_exhaust_physical_plans_00000, 294)
			return rightJoins, hasIndexJoinHint
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 290)
		joins := append(leftJoins, rightJoins...)
		return joins, hasIndexJoinHint && len(joins) != 0
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 277)
	return nil, false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 295)
	mergeJoins := p.getMergeJoin(prop)
	if (p.preferJoinType & preferMergeJoin) > 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 299)
		return mergeJoins
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 296)
	joins := make([]PhysicalPlan, 0, 5)
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin(prop)
	if forced {
		trace_util_0.Count(_exhaust_physical_plans_00000, 300)
		return indexJoins
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 297)
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins(prop)
	if (p.preferJoinType & preferHashJoin) > 0 {
		trace_util_0.Count(_exhaust_physical_plans_00000, 301)
		return hashJoins
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 298)
	joins = append(joins, hashJoins...)
	return joins
}

// tryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) tryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	trace_util_0.Count(_exhaust_physical_plans_00000, 302)
	newProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: prop.ExpectedCnt}
	newCols := make([]property.Item, 0, len(prop.Items))
	for _, col := range prop.Items {
		trace_util_0.Count(_exhaust_physical_plans_00000, 304)
		idx := p.schema.ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			trace_util_0.Count(_exhaust_physical_plans_00000, 305)
			newCols = append(newCols, property.Item{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			trace_util_0.Count(_exhaust_physical_plans_00000, 306)
			return nil, false
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 303)
	newProp.Items = newCols
	return newProp, true
}

func (p *LogicalProjection) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 307)
	newProp, ok := p.tryToGetChildProp(prop)
	if !ok {
		trace_util_0.Count(_exhaust_physical_plans_00000, 309)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 308)
	proj := PhysicalProjection{
		Exprs:                p.Exprs,
		CalculateNoDelay:     p.calculateNoDelay,
		AvoidColumnEvaluator: p.avoidColumnEvaluator,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), newProp)
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}
}

func (lt *LogicalTopN) getPhysTopN() []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 310)
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		trace_util_0.Count(_exhaust_physical_plans_00000, 312)
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64}
		topN := PhysicalTopN{
			ByItems: lt.ByItems,
			Count:   lt.Count,
			Offset:  lt.Offset,
		}.Init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, topN)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 311)
	return ret
}

func (lt *LogicalTopN) getPhysLimits() []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 313)
	prop, canPass := getPropByOrderByItems(lt.ByItems)
	if !canPass {
		trace_util_0.Count(_exhaust_physical_plans_00000, 316)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 314)
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		trace_util_0.Count(_exhaust_physical_plans_00000, 317)
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), Items: prop.Items}
		limit := PhysicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.Init(lt.ctx, lt.stats, resultProp)
		ret = append(ret, limit)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 315)
	return ret
}

// Check if this prop's columns can match by items totally.
func matchItems(p *property.PhysicalProperty, items []*ByItems) bool {
	trace_util_0.Count(_exhaust_physical_plans_00000, 318)
	if len(items) < len(p.Items) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 321)
		return false
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 319)
	for i, col := range p.Items {
		trace_util_0.Count(_exhaust_physical_plans_00000, 322)
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !sortItem.Expr.Equal(nil, col.Col) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 323)
			return false
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 320)
	return true
}

func (lt *LogicalTopN) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 324)
	if matchItems(prop, lt.ByItems) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 326)
		return append(lt.getPhysTopN(), lt.getPhysLimits()...)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 325)
	return nil
}

func (la *LogicalApply) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 327)
	if !prop.AllColsFromSchema(la.children[0].Schema()) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 329) // for convenient, we don't pass through any prop
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 328)
	join := la.getHashJoin(prop, 1)
	apply := PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.corCols,
		rightChOffset:    la.children[0].Schema().Len(),
	}.Init(la.ctx,
		la.stats.ScaleByExpectCnt(prop.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: prop.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schema)
	return []PhysicalPlan{apply}
}

func (p *LogicalWindow) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 330)
	var byItems []property.Item
	byItems = append(byItems, p.PartitionBy...)
	byItems = append(byItems, p.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems, Enforced: true}
	if !prop.IsPrefix(childProperty) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 332)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 331)
	window := PhysicalWindow{
		WindowFuncDescs: p.WindowFuncDescs,
		PartitionBy:     p.PartitionBy,
		OrderBy:         p.OrderBy,
		Frame:           p.Frame,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProperty)
	window.SetSchema(p.Schema())
	return []PhysicalPlan{window}
}

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 333)
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 334)
	all, desc := prop.AllSameOrder()
	if len(la.possibleProperties) == 0 || !all {
		trace_util_0.Count(_exhaust_physical_plans_00000, 339)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 335)
	for _, aggFunc := range la.AggFuncs {
		trace_util_0.Count(_exhaust_physical_plans_00000, 340)
		if aggFunc.Mode == aggregation.FinalMode {
			trace_util_0.Count(_exhaust_physical_plans_00000, 341)
			return nil
		}
	}
	// group by a + b is not interested in any order.
	trace_util_0.Count(_exhaust_physical_plans_00000, 336)
	if len(la.groupByCols) != len(la.GroupByItems) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 342)
		return nil
	}

	trace_util_0.Count(_exhaust_physical_plans_00000, 337)
	streamAggs := make([]PhysicalPlan, 0, len(la.possibleProperties)*(len(wholeTaskTypes)-1))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		trace_util_0.Count(_exhaust_physical_plans_00000, 343)
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByCols)
		if len(sortColOffsets) != len(la.groupByCols) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 346)
			continue
		}

		trace_util_0.Count(_exhaust_physical_plans_00000, 344)
		childProp.Items = property.ItemsFromCols(possibleChildProperty[:len(sortColOffsets)], desc)
		if !prop.IsPrefix(childProp) {
			trace_util_0.Count(_exhaust_physical_plans_00000, 347)
			continue
		}

		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		trace_util_0.Count(_exhaust_physical_plans_00000, 345)
		for _, taskTp := range []property.TaskType{property.CopSingleReadTaskType, property.RootTaskType} {
			trace_util_0.Count(_exhaust_physical_plans_00000, 348)
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), copiedChildProperty)
			agg.SetSchema(la.schema.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 338)
	return streamAggs
}

func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 349)
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 352)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 350)
	hashAggs := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, taskTp := range wholeTaskTypes {
		trace_util_0.Count(_exhaust_physical_plans_00000, 353)
		agg := basePhysicalAgg{
			GroupByItems: la.GroupByItems,
			AggFuncs:     la.AggFuncs,
		}.initForHash(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp})
		agg.SetSchema(la.schema.Clone())
		hashAggs = append(hashAggs, agg)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 351)
	return hashAggs
}

func (la *LogicalAggregation) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 354)
	aggs := make([]PhysicalPlan, 0, len(la.possibleProperties)+1)
	aggs = append(aggs, la.getHashAggs(prop)...)
	aggs = append(aggs, la.getStreamAggs(prop)...)
	return aggs
}

func (p *LogicalSelection) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 355)
	childProp := prop.Clone()
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 356)
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 359)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 357)
	ret := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		trace_util_0.Count(_exhaust_physical_plans_00000, 360)
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset: p.Offset,
			Count:  p.Count,
		}.Init(p.ctx, p.stats, resultProp)
		ret = append(ret, limit)
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 358)
	return ret
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 361)
	childProp := prop.Clone()
	lock := PhysicalLock{
		Lock: p.Lock,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 362)
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 365)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 363)
	chReqProps := make([]*property.PhysicalProperty, 0, len(p.children))
	for range p.children {
		trace_util_0.Count(_exhaust_physical_plans_00000, 366)
		chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt})
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 364)
	ua := PhysicalUnionAll{}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	ua.SetSchema(p.Schema())
	return []PhysicalPlan{ua}
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	trace_util_0.Count(_exhaust_physical_plans_00000, 367)
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	trace_util_0.Count(_exhaust_physical_plans_00000, 368)
	prop, canPass := getPropByOrderByItems(ls.ByItems)
	if !canPass {
		trace_util_0.Count(_exhaust_physical_plans_00000, 370)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 369)
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{}.Init(ls.ctx, prop)
	return ps
}

func (ls *LogicalSort) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 371)
	if matchItems(prop, ls.ByItems) {
		trace_util_0.Count(_exhaust_physical_plans_00000, 373)
		ret := make([]PhysicalPlan, 0, 2)
		ret = append(ret, ls.getPhysicalSort(prop))
		ns := ls.getNominalSort(prop)
		if ns != nil {
			trace_util_0.Count(_exhaust_physical_plans_00000, 375)
			ret = append(ret, ns)
		}
		trace_util_0.Count(_exhaust_physical_plans_00000, 374)
		return ret
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 372)
	return nil
}

func (p *LogicalMaxOneRow) exhaustPhysicalPlans(prop *property.PhysicalProperty) []PhysicalPlan {
	trace_util_0.Count(_exhaust_physical_plans_00000, 376)
	if !prop.IsEmpty() {
		trace_util_0.Count(_exhaust_physical_plans_00000, 378)
		return nil
	}
	trace_util_0.Count(_exhaust_physical_plans_00000, 377)
	mor := PhysicalMaxOneRow{}.Init(p.ctx, p.stats, &property.PhysicalProperty{ExpectedCnt: 2})
	return []PhysicalPlan{mor}
}

var _exhaust_physical_plans_00000 = "planner/core/exhaust_physical_plans.go"
