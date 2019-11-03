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

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/disjointset"
)

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 0)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 4)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 1)
	for i, expr := range p.Exprs {
		trace_util_0.Count(_resolve_indices_00000, 5)
		p.Exprs[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 6)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 2)
	childProj, isProj := p.children[0].(*PhysicalProjection)
	if !isProj {
		trace_util_0.Count(_resolve_indices_00000, 7)
		return
	}
	trace_util_0.Count(_resolve_indices_00000, 3)
	refine4NeighbourProj(p, childProj)
	return
}

// refine4NeighbourProj refines the index for p.Exprs whose type is *Column when
// there is two neighbouring Projections.
// This function is introduced because that different childProj.Expr may refer
// to the same index of childProj.Schema, so we need to keep this relation
// between the specified expressions in the parent Projection.
func refine4NeighbourProj(p, childProj *PhysicalProjection) {
	trace_util_0.Count(_resolve_indices_00000, 8)
	inputIdx2OutputIdxes := make(map[int][]int)
	for i, expr := range childProj.Exprs {
		trace_util_0.Count(_resolve_indices_00000, 11)
		col, isCol := expr.(*expression.Column)
		if !isCol {
			trace_util_0.Count(_resolve_indices_00000, 13)
			continue
		}
		trace_util_0.Count(_resolve_indices_00000, 12)
		inputIdx2OutputIdxes[col.Index] = append(inputIdx2OutputIdxes[col.Index], i)
	}
	trace_util_0.Count(_resolve_indices_00000, 9)
	childSchemaUnionSet := disjointset.NewIntSet(childProj.schema.Len())
	for _, outputIdxes := range inputIdx2OutputIdxes {
		trace_util_0.Count(_resolve_indices_00000, 14)
		if len(outputIdxes) <= 1 {
			trace_util_0.Count(_resolve_indices_00000, 16)
			continue
		}
		trace_util_0.Count(_resolve_indices_00000, 15)
		for i := 1; i < len(outputIdxes); i++ {
			trace_util_0.Count(_resolve_indices_00000, 17)
			childSchemaUnionSet.Union(outputIdxes[0], outputIdxes[i])
		}
	}

	trace_util_0.Count(_resolve_indices_00000, 10)
	for _, expr := range p.Exprs {
		trace_util_0.Count(_resolve_indices_00000, 18)
		col, isCol := expr.(*expression.Column)
		if !isCol {
			trace_util_0.Count(_resolve_indices_00000, 20)
			continue
		}
		trace_util_0.Count(_resolve_indices_00000, 19)
		col.Index = childSchemaUnionSet.FindRoot(col.Index)
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 21)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 27)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 22)
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		trace_util_0.Count(_resolve_indices_00000, 28)
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 31)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 29)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 32)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 30)
		p.EqualConditions[i] = expression.NewFunctionInternal(fun.GetCtx(), fun.FuncName.L, fun.GetType(), lArg, rArg).(*expression.ScalarFunction)
	}
	trace_util_0.Count(_resolve_indices_00000, 23)
	for i, expr := range p.LeftConditions {
		trace_util_0.Count(_resolve_indices_00000, 33)
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 34)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 24)
	for i, expr := range p.RightConditions {
		trace_util_0.Count(_resolve_indices_00000, 35)
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 36)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 25)
	for i, expr := range p.OtherConditions {
		trace_util_0.Count(_resolve_indices_00000, 37)
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 38)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 26)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 39)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 46)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 40)
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftKeys {
		trace_util_0.Count(_resolve_indices_00000, 47)
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 49)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 48)
		p.LeftKeys[i] = newKey.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 41)
	for i, col := range p.RightKeys {
		trace_util_0.Count(_resolve_indices_00000, 50)
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 52)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 51)
		p.RightKeys[i] = newKey.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 42)
	for i, expr := range p.LeftConditions {
		trace_util_0.Count(_resolve_indices_00000, 53)
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 54)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 43)
	for i, expr := range p.RightConditions {
		trace_util_0.Count(_resolve_indices_00000, 55)
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 56)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 44)
	for i, expr := range p.OtherConditions {
		trace_util_0.Count(_resolve_indices_00000, 57)
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 58)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 45)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 59)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 66)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 60)
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		trace_util_0.Count(_resolve_indices_00000, 67)
		newOuterKey, err := p.OuterJoinKeys[i].ResolveIndices(p.children[p.OuterIndex].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 70)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 68)
		p.OuterJoinKeys[i] = newOuterKey.(*expression.Column)
		newInnerKey, err := p.InnerJoinKeys[i].ResolveIndices(p.children[1-p.OuterIndex].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 71)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 69)
		p.InnerJoinKeys[i] = newInnerKey.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 61)
	for i, expr := range p.LeftConditions {
		trace_util_0.Count(_resolve_indices_00000, 72)
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 73)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 62)
	for i, expr := range p.RightConditions {
		trace_util_0.Count(_resolve_indices_00000, 74)
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 75)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 63)
	mergedSchema := expression.MergeSchema(lSchema, rSchema)
	for i, expr := range p.OtherConditions {
		trace_util_0.Count(_resolve_indices_00000, 76)
		p.OtherConditions[i], err = expr.ResolveIndices(mergedSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 77)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 64)
	if p.CompareFilters != nil {
		trace_util_0.Count(_resolve_indices_00000, 78)
		err = p.CompareFilters.resolveIndices(p.children[p.OuterIndex].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 80)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 79)
		for i := range p.CompareFilters.affectedColSchema.Columns {
			trace_util_0.Count(_resolve_indices_00000, 81)
			resolvedCol, err1 := p.CompareFilters.affectedColSchema.Columns[i].ResolveIndices(p.children[p.OuterIndex].Schema())
			if err1 != nil {
				trace_util_0.Count(_resolve_indices_00000, 83)
				return err1
			}
			trace_util_0.Count(_resolve_indices_00000, 82)
			p.CompareFilters.affectedColSchema.Columns[i] = resolvedCol.(*expression.Column)
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 65)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 84)
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 87)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 85)
	for i, expr := range p.Conditions {
		trace_util_0.Count(_resolve_indices_00000, 88)
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 89)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 86)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableReader) ResolveIndices() error {
	trace_util_0.Count(_resolve_indices_00000, 90)
	return p.tablePlan.ResolveIndices()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 91)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 95)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 92)
	err = p.indexPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 96)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 93)
	for i, col := range p.OutputColumns {
		trace_util_0.Count(_resolve_indices_00000, 97)
		newCol, err := col.ResolveIndices(p.indexPlan.Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 99)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 98)
		p.OutputColumns[i] = newCol.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 94)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 100)
	err = p.tablePlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 103)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 101)
	err = p.indexPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 104)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 102)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSelection) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 105)
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 108)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 106)
	for i, expr := range p.Conditions {
		trace_util_0.Count(_resolve_indices_00000, 109)
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 110)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 107)
	return
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 111)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 115)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 112)
	for _, aggFun := range p.AggFuncs {
		trace_util_0.Count(_resolve_indices_00000, 116)
		for i, arg := range aggFun.Args {
			trace_util_0.Count(_resolve_indices_00000, 117)
			aggFun.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				trace_util_0.Count(_resolve_indices_00000, 118)
				return err
			}
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 113)
	for i, item := range p.GroupByItems {
		trace_util_0.Count(_resolve_indices_00000, 119)
		p.GroupByItems[i], err = item.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 120)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 114)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 121)
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 124)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 122)
	for _, item := range p.ByItems {
		trace_util_0.Count(_resolve_indices_00000, 125)
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 126)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 123)
	return err
}

// ResolveIndices implements Plan interface.
func (p *PhysicalWindow) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 127)
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 134)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 128)
	for i := 0; i < len(p.Schema().Columns)-len(p.WindowFuncDescs); i++ {
		trace_util_0.Count(_resolve_indices_00000, 135)
		col := p.Schema().Columns[i]
		newCol, err := col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 137)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 136)
		p.Schema().Columns[i] = newCol.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 129)
	for i, item := range p.PartitionBy {
		trace_util_0.Count(_resolve_indices_00000, 138)
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 140)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 139)
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 130)
	for i, item := range p.OrderBy {
		trace_util_0.Count(_resolve_indices_00000, 141)
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 143)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 142)
		p.OrderBy[i].Col = newCol.(*expression.Column)
	}
	trace_util_0.Count(_resolve_indices_00000, 131)
	for _, desc := range p.WindowFuncDescs {
		trace_util_0.Count(_resolve_indices_00000, 144)
		for i, arg := range desc.Args {
			trace_util_0.Count(_resolve_indices_00000, 145)
			desc.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				trace_util_0.Count(_resolve_indices_00000, 146)
				return err
			}
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 132)
	if p.Frame != nil {
		trace_util_0.Count(_resolve_indices_00000, 147)
		for i := range p.Frame.Start.CalcFuncs {
			trace_util_0.Count(_resolve_indices_00000, 149)
			p.Frame.Start.CalcFuncs[i], err = p.Frame.Start.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				trace_util_0.Count(_resolve_indices_00000, 150)
				return err
			}
		}
		trace_util_0.Count(_resolve_indices_00000, 148)
		for i := range p.Frame.End.CalcFuncs {
			trace_util_0.Count(_resolve_indices_00000, 151)
			p.Frame.End.CalcFuncs[i], err = p.Frame.End.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				trace_util_0.Count(_resolve_indices_00000, 152)
				return err
			}
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 133)
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 153)
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 156)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 154)
	for _, item := range p.ByItems {
		trace_util_0.Count(_resolve_indices_00000, 157)
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 158)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 155)
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 159)
	err = p.PhysicalHashJoin.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 163)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 160)
	for _, col := range p.OuterSchema {
		trace_util_0.Count(_resolve_indices_00000, 164)
		newCol, err := col.Column.ResolveIndices(p.children[0].Schema())
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 166)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 165)
		col.Column = *newCol.(*expression.Column)
	}
	// Resolve index for equal conditions again, because apply is different from
	// hash join on the fact that equal conditions are evaluated against the join result,
	// so columns from equal conditions come from merged schema of children, instead of
	// single child's schema.
	trace_util_0.Count(_resolve_indices_00000, 161)
	joinedSchema := expression.MergeSchema(p.children[0].Schema(), p.children[1].Schema())
	for i, cond := range p.PhysicalHashJoin.EqualConditions {
		trace_util_0.Count(_resolve_indices_00000, 167)
		newSf, err := cond.ResolveIndices(joinedSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 169)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 168)
		p.PhysicalHashJoin.EqualConditions[i] = newSf.(*expression.ScalarFunction)
	}
	trace_util_0.Count(_resolve_indices_00000, 162)
	return
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 170)
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 173)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 171)
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		trace_util_0.Count(_resolve_indices_00000, 174)
		newCol, err := assign.Col.ResolveIndices(schema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 176)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 175)
		assign.Col = newCol.(*expression.Column)
		assign.Expr, err = assign.Expr.ResolveIndices(schema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 177)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 172)
	return
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 178)
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 184)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 179)
	for _, asgn := range p.OnDuplicate {
		trace_util_0.Count(_resolve_indices_00000, 185)
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 187)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 186)
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 188)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 180)
	for _, set := range p.SetList {
		trace_util_0.Count(_resolve_indices_00000, 189)
		newCol, err := set.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 191)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 190)
		set.Col = newCol.(*expression.Column)
		set.Expr, err = set.Expr.ResolveIndices(p.tableSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 192)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 181)
	for i, expr := range p.GenCols.Exprs {
		trace_util_0.Count(_resolve_indices_00000, 193)
		p.GenCols.Exprs[i], err = expr.ResolveIndices(p.tableSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 194)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 182)
	for _, asgn := range p.GenCols.OnDuplicates {
		trace_util_0.Count(_resolve_indices_00000, 195)
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 197)
			return err
		}
		trace_util_0.Count(_resolve_indices_00000, 196)
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 198)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 183)
	return
}

// ResolveIndices implements Plan interface.
func (p *Show) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 199)
	for i, expr := range p.Conditions {
		trace_util_0.Count(_resolve_indices_00000, 201)
		p.Conditions[i], err = expr.ResolveIndices(p.schema)
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 202)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 200)
	return err
}

func (p *physicalSchemaProducer) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 203)
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		trace_util_0.Count(_resolve_indices_00000, 206)
		return err
	}
	trace_util_0.Count(_resolve_indices_00000, 204)
	if p.schema != nil {
		trace_util_0.Count(_resolve_indices_00000, 207)
		for i, cols := range p.schema.TblID2Handle {
			trace_util_0.Count(_resolve_indices_00000, 208)
			for j, col := range cols {
				trace_util_0.Count(_resolve_indices_00000, 209)
				resolvedCol, err := col.ResolveIndices(p.schema)
				if err != nil {
					trace_util_0.Count(_resolve_indices_00000, 211)
					return err
				}
				trace_util_0.Count(_resolve_indices_00000, 210)
				p.schema.TblID2Handle[i][j] = resolvedCol.(*expression.Column)
			}
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 205)
	return
}

func (p *baseSchemaProducer) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 212)
	if p.schema != nil {
		trace_util_0.Count(_resolve_indices_00000, 214)
		for i, cols := range p.schema.TblID2Handle {
			trace_util_0.Count(_resolve_indices_00000, 215)
			for j, col := range cols {
				trace_util_0.Count(_resolve_indices_00000, 216)
				resolvedCol, err := col.ResolveIndices(p.schema)
				if err != nil {
					trace_util_0.Count(_resolve_indices_00000, 218)
					return err
				}
				trace_util_0.Count(_resolve_indices_00000, 217)
				p.schema.TblID2Handle[i][j] = resolvedCol.(*expression.Column)
			}
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 213)
	return
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalPlan) ResolveIndices() (err error) {
	trace_util_0.Count(_resolve_indices_00000, 219)
	for _, child := range p.children {
		trace_util_0.Count(_resolve_indices_00000, 221)
		err = child.ResolveIndices()
		if err != nil {
			trace_util_0.Count(_resolve_indices_00000, 222)
			return err
		}
	}
	trace_util_0.Count(_resolve_indices_00000, 220)
	return
}

var _resolve_indices_00000 = "planner/core/resolve_indices.go"
