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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/chunk"
)

type columnEvaluator struct {
	inputIdxToOutputIdxes map[int][]int
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//	     since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx sessionctx.Context, input, output *chunk.Chunk) {
	trace_util_0.Count(_evaluator_00000, 0)
	for inputIdx, outputIdxes := range e.inputIdxToOutputIdxes {
		trace_util_0.Count(_evaluator_00000, 1)
		output.SwapColumn(outputIdxes[0], input, inputIdx)
		for i, length := 1, len(outputIdxes); i < length; i++ {
			trace_util_0.Count(_evaluator_00000, 2)
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
}

type defaultEvaluator struct {
	outputIdxes  []int
	exprs        []Expression
	vectorizable bool
}

func (e *defaultEvaluator) run(ctx sessionctx.Context, input, output *chunk.Chunk) error {
	trace_util_0.Count(_evaluator_00000, 3)
	iter := chunk.NewIterator4Chunk(input)
	if e.vectorizable {
		trace_util_0.Count(_evaluator_00000, 6)
		for i := range e.outputIdxes {
			trace_util_0.Count(_evaluator_00000, 8)
			err := evalOneColumn(ctx, e.exprs[i], iter, output, e.outputIdxes[i])
			if err != nil {
				trace_util_0.Count(_evaluator_00000, 9)
				return err
			}
		}
		trace_util_0.Count(_evaluator_00000, 7)
		return nil
	}

	trace_util_0.Count(_evaluator_00000, 4)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		trace_util_0.Count(_evaluator_00000, 10)
		for i := range e.outputIdxes {
			trace_util_0.Count(_evaluator_00000, 11)
			err := evalOneCell(ctx, e.exprs[i], row, output, e.outputIdxes[i])
			if err != nil {
				trace_util_0.Count(_evaluator_00000, 12)
				return err
			}
		}
	}
	trace_util_0.Count(_evaluator_00000, 5)
	return nil
}

// EvaluatorSuite is responsible for the evaluation of a list of expressions.
// It separates them to "column" and "other" expressions and evaluates "other"
// expressions before "column" expressions.
type EvaluatorSuite struct {
	*columnEvaluator  // Evaluator for column expressions.
	*defaultEvaluator // Evaluator for other expressions.
}

// NewEvaluatorSuite creates an EvaluatorSuite to evaluate all the exprs.
// avoidColumnEvaluator can be removed after column pool is supported.
func NewEvaluatorSuite(exprs []Expression, avoidColumnEvaluator bool) *EvaluatorSuite {
	trace_util_0.Count(_evaluator_00000, 13)
	e := &EvaluatorSuite{}

	for i := 0; i < len(exprs); i++ {
		trace_util_0.Count(_evaluator_00000, 16)
		if col, isCol := exprs[i].(*Column); isCol && !avoidColumnEvaluator {
			trace_util_0.Count(_evaluator_00000, 19)
			if e.columnEvaluator == nil {
				trace_util_0.Count(_evaluator_00000, 21)
				e.columnEvaluator = &columnEvaluator{inputIdxToOutputIdxes: make(map[int][]int)}
			}
			trace_util_0.Count(_evaluator_00000, 20)
			inputIdx, outputIdx := col.Index, i
			e.columnEvaluator.inputIdxToOutputIdxes[inputIdx] = append(e.columnEvaluator.inputIdxToOutputIdxes[inputIdx], outputIdx)
			continue
		}
		trace_util_0.Count(_evaluator_00000, 17)
		if e.defaultEvaluator == nil {
			trace_util_0.Count(_evaluator_00000, 22)
			e.defaultEvaluator = &defaultEvaluator{
				outputIdxes: make([]int, 0, len(exprs)),
				exprs:       make([]Expression, 0, len(exprs)),
			}
		}
		trace_util_0.Count(_evaluator_00000, 18)
		e.defaultEvaluator.exprs = append(e.defaultEvaluator.exprs, exprs[i])
		e.defaultEvaluator.outputIdxes = append(e.defaultEvaluator.outputIdxes, i)
	}

	trace_util_0.Count(_evaluator_00000, 14)
	if e.defaultEvaluator != nil {
		trace_util_0.Count(_evaluator_00000, 23)
		e.defaultEvaluator.vectorizable = Vectorizable(e.defaultEvaluator.exprs)
	}
	trace_util_0.Count(_evaluator_00000, 15)
	return e
}

// Vectorizable checks whether this EvaluatorSuite can use vectorizd execution mode.
func (e *EvaluatorSuite) Vectorizable() bool {
	trace_util_0.Count(_evaluator_00000, 24)
	return e.defaultEvaluator == nil || e.defaultEvaluator.vectorizable
}

// Run evaluates all the expressions hold by this EvaluatorSuite.
// NOTE: "defaultEvaluator" must be evaluated before "columnEvaluator".
func (e *EvaluatorSuite) Run(ctx sessionctx.Context, input, output *chunk.Chunk) error {
	trace_util_0.Count(_evaluator_00000, 25)
	if e.defaultEvaluator != nil {
		trace_util_0.Count(_evaluator_00000, 28)
		err := e.defaultEvaluator.run(ctx, input, output)
		if err != nil {
			trace_util_0.Count(_evaluator_00000, 29)
			return err
		}
	}

	trace_util_0.Count(_evaluator_00000, 26)
	if e.columnEvaluator != nil {
		trace_util_0.Count(_evaluator_00000, 30)
		e.columnEvaluator.run(ctx, input, output)
	}
	trace_util_0.Count(_evaluator_00000, 27)
	return nil
}

var _evaluator_00000 = "expression/evaluator.go"
