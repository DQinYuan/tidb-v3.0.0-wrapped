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

package executor

import (
	"context"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/set"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type aggPartialResultMapper map[string][]aggfuncs.PartialResult

// baseHashAggWorker stores the common attributes of HashAggFinalWorker and HashAggPartialWorker.
type baseHashAggWorker struct {
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
}

func newBaseHashAggWorker(finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) baseHashAggWorker {
	trace_util_0.Count(_aggregate_00000, 0)
	return baseHashAggWorker{
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
	}
}

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker

	inputCh           chan *chunk.Chunk
	outputChs         []chan *HashAggIntermData
	globalOutputCh    chan *AfFinalResult
	giveBackCh        chan<- *HashAggInput
	partialResultsMap aggPartialResultMapper
	groupByItems      []expression.Expression
	groupKey          []byte
	groupValDatums    []types.Datum
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk
}

// HashAggFinalWorker indicates the final workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_final_concurrency`.
type HashAggFinalWorker struct {
	baseHashAggWorker

	rowBuffer           []types.Datum
	mutableRow          chunk.MutRow
	partialResultMap    aggPartialResultMapper
	groupSet            set.StringSet
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
}

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk *chunk.Chunk
	err error
}

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in PartialAggFuncs.
// The parallel execution flow is as the following graph shows:
//
//                            +-------------+
//                            | Main Thread |
//                            +------+------+
//                                   ^
//                                   |
//                                   +
//                              +-+-            +-+
//                              | |    ......   | |  finalOutputCh
//                              +++-            +-+
//                               ^
//                               |
//                               +---------------+
//                               |               |
//                 +--------------+             +--------------+
//                 | final worker |     ......  | final worker |
//                 +------------+-+             +-+------------+
//                              ^                 ^
//                              |                 |
//                             +-+  +-+  ......  +-+
//                             | |  | |          | |
//                             ...  ...          ...    partialOutputChs
//                             | |  | |          | |
//                             +++  +++          +++
//                              ^    ^            ^
//          +-+                 |    |            |
//          | |        +--------o----+            |
// inputCh  +-+        |        +-----------------+---+
//          | |        |                              |
//          ...    +---+------------+            +----+-----------+
//          | |    | partial worker |   ......   | partial worker |
//          +++    +--------------+-+            +-+--------------+
//           |                     ^                ^
//           |                     |                |
//      +----v---------+          +++ +-+          +++
//      | data fetcher | +------> | | | |  ......  | |   partialInputChs
//      +--------------+          +-+ +-+          +-+
type HashAggExec struct {
	baseExecutor

	prepared         bool
	sc               *stmtctx.StatementContext
	PartialAggFuncs  []aggfuncs.AggFunc
	FinalAggFuncs    []aggfuncs.AggFunc
	partialResultMap aggPartialResultMapper
	groupSet         set.StringSet
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []expression.Expression
	groupKeyBuffer   []byte
	groupValDatums   []types.Datum

	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	isUnparallelExec bool

	finishCh         chan struct{}
	finalOutputCh    chan *AfFinalResult
	finalInputCh     chan *chunk.Chunk
	partialOutputChs []chan *HashAggIntermData
	inputCh          chan *HashAggInput
	partialInputChs  []chan *chunk.Chunk
	partialWorkers   []HashAggPartialWorker
	finalWorkers     []HashAggFinalWorker
	defaultVal       *chunk.Chunk
	// isChildReturnEmpty indicates whether the child executor only returns an empty input.
	isChildReturnEmpty bool

	childResult *chunk.Chunk
}

// HashAggInput indicates the input of hash agg exec.
type HashAggInput struct {
	chk *chunk.Chunk
	// giveBackCh is bound with specific partial worker,
	// it's used to reuse the `chk`,
	// and tell the data-fetcher which partial worker it should send data to.
	giveBackCh chan<- *chunk.Chunk
}

// HashAggIntermData indicates the intermediate data of aggregation execution.
type HashAggIntermData struct {
	groupKeys        []string
	cursor           int
	partialResultMap aggPartialResultMapper
}

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(sc *stmtctx.StatementContext, prs [][]aggfuncs.PartialResult, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	trace_util_0.Count(_aggregate_00000, 1)
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(prs) < maxChunkSize; d.cursor++ {
		trace_util_0.Count(_aggregate_00000, 4)
		prs = append(prs, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	trace_util_0.Count(_aggregate_00000, 2)
	if d.cursor == len(d.groupKeys) {
		trace_util_0.Count(_aggregate_00000, 5)
		reachEnd = true
	}
	trace_util_0.Count(_aggregate_00000, 3)
	return prs, d.groupKeys[keyStart:d.cursor], reachEnd
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	trace_util_0.Count(_aggregate_00000, 6)
	if e.isUnparallelExec {
		trace_util_0.Count(_aggregate_00000, 11)
		e.childResult = nil
		e.groupSet = nil
		e.partialResultMap = nil
		return e.baseExecutor.Close()
	}
	// `Close` may be called after `Open` without calling `Next` in test.
	trace_util_0.Count(_aggregate_00000, 7)
	if !e.prepared {
		trace_util_0.Count(_aggregate_00000, 12)
		close(e.inputCh)
		for _, ch := range e.partialOutputChs {
			trace_util_0.Count(_aggregate_00000, 14)
			close(ch)
		}
		trace_util_0.Count(_aggregate_00000, 13)
		close(e.finalOutputCh)
	}
	trace_util_0.Count(_aggregate_00000, 8)
	close(e.finishCh)
	for _, ch := range e.partialOutputChs {
		trace_util_0.Count(_aggregate_00000, 15)
		for range ch {
			trace_util_0.Count(_aggregate_00000, 16)
		}
	}
	trace_util_0.Count(_aggregate_00000, 9)
	for range e.finalOutputCh {
		trace_util_0.Count(_aggregate_00000, 17)
	}
	trace_util_0.Count(_aggregate_00000, 10)
	return e.baseExecutor.Close()
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open(ctx context.Context) error {
	trace_util_0.Count(_aggregate_00000, 18)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_aggregate_00000, 21)
		return err
	}
	trace_util_0.Count(_aggregate_00000, 19)
	e.prepared = false

	if e.isUnparallelExec {
		trace_util_0.Count(_aggregate_00000, 22)
		e.initForUnparallelExec()
		return nil
	}
	trace_util_0.Count(_aggregate_00000, 20)
	e.initForParallelExec(e.ctx)
	return nil
}

func (e *HashAggExec) initForUnparallelExec() {
	trace_util_0.Count(_aggregate_00000, 23)
	e.groupSet = set.NewStringSet()
	e.partialResultMap = make(aggPartialResultMapper)
	e.groupKeyBuffer = make([]byte, 0, 8)
	e.groupValDatums = make([]types.Datum, 0, len(e.groupKeyBuffer))
	e.childResult = newFirstChunk(e.children[0])
}

func (e *HashAggExec) initForParallelExec(ctx sessionctx.Context) {
	trace_util_0.Count(_aggregate_00000, 24)
	sessionVars := e.ctx.GetSessionVars()
	finalConcurrency := sessionVars.HashAggFinalConcurrency
	partialConcurrency := sessionVars.HashAggPartialConcurrency
	e.isChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency)
	e.finalInputCh = make(chan *chunk.Chunk, finalConcurrency)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		trace_util_0.Count(_aggregate_00000, 28)
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	trace_util_0.Count(_aggregate_00000, 25)
	e.partialOutputChs = make([]chan *HashAggIntermData, finalConcurrency)
	for i := range e.partialOutputChs {
		trace_util_0.Count(_aggregate_00000, 29)
		e.partialOutputChs[i] = make(chan *HashAggIntermData, partialConcurrency)
	}

	trace_util_0.Count(_aggregate_00000, 26)
	e.partialWorkers = make([]HashAggPartialWorker, partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, finalConcurrency)

	// Init partial workers.
	for i := 0; i < partialConcurrency; i++ {
		trace_util_0.Count(_aggregate_00000, 30)
		w := HashAggPartialWorker{
			baseHashAggWorker: newBaseHashAggWorker(e.finishCh, e.PartialAggFuncs, e.maxChunkSize),
			inputCh:           e.partialInputChs[i],
			outputChs:         e.partialOutputChs,
			giveBackCh:        e.inputCh,
			globalOutputCh:    e.finalOutputCh,
			partialResultsMap: make(aggPartialResultMapper),
			groupByItems:      e.GroupByItems,
			groupValDatums:    make([]types.Datum, 0, len(e.GroupByItems)),
			chk:               newFirstChunk(e.children[0]),
		}

		e.partialWorkers[i] = w
		e.inputCh <- &HashAggInput{
			chk:        newFirstChunk(e.children[0]),
			giveBackCh: w.inputCh,
		}
	}

	// Init final workers.
	trace_util_0.Count(_aggregate_00000, 27)
	for i := 0; i < finalConcurrency; i++ {
		trace_util_0.Count(_aggregate_00000, 31)
		e.finalWorkers[i] = HashAggFinalWorker{
			baseHashAggWorker:   newBaseHashAggWorker(e.finishCh, e.FinalAggFuncs, e.maxChunkSize),
			partialResultMap:    make(aggPartialResultMapper),
			groupSet:            set.NewStringSet(),
			inputCh:             e.partialOutputChs[i],
			outputCh:            e.finalOutputCh,
			finalResultHolderCh: e.finalInputCh,
			rowBuffer:           make([]types.Datum, 0, e.Schema().Len()),
			mutableRow:          chunk.MutRowFromTypes(retTypes(e)),
		}
	}
}

func (w *HashAggPartialWorker) getChildInput() bool {
	trace_util_0.Count(_aggregate_00000, 32)
	select {
	case <-w.finishCh:
		trace_util_0.Count(_aggregate_00000, 34)
		return false
	case chk, ok := <-w.inputCh:
		trace_util_0.Count(_aggregate_00000, 35)
		if !ok {
			trace_util_0.Count(_aggregate_00000, 37)
			return false
		}
		trace_util_0.Count(_aggregate_00000, 36)
		w.chk.SwapColumns(chk)
		w.giveBackCh <- &HashAggInput{
			chk:        chk,
			giveBackCh: w.inputCh,
		}
	}
	trace_util_0.Count(_aggregate_00000, 33)
	return true
}

func recoveryHashAgg(output chan *AfFinalResult, r interface{}) {
	trace_util_0.Count(_aggregate_00000, 38)
	err := errors.Errorf("%v", r)
	output <- &AfFinalResult{err: errors.Errorf("%v", r)}
	logutil.Logger(context.Background()).Error("parallel hash aggregation panicked", zap.Error(err))
}

func (w *HashAggPartialWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	trace_util_0.Count(_aggregate_00000, 39)
	needShuffle, sc := false, ctx.GetSessionVars().StmtCtx
	defer func() {
		trace_util_0.Count(_aggregate_00000, 41)
		if r := recover(); r != nil {
			trace_util_0.Count(_aggregate_00000, 44)
			recoveryHashAgg(w.globalOutputCh, r)
		}
		trace_util_0.Count(_aggregate_00000, 42)
		if needShuffle {
			trace_util_0.Count(_aggregate_00000, 45)
			w.shuffleIntermData(sc, finalConcurrency)
		}
		trace_util_0.Count(_aggregate_00000, 43)
		waitGroup.Done()
	}()
	trace_util_0.Count(_aggregate_00000, 40)
	for {
		trace_util_0.Count(_aggregate_00000, 46)
		if !w.getChildInput() {
			trace_util_0.Count(_aggregate_00000, 49)
			return
		}
		trace_util_0.Count(_aggregate_00000, 47)
		if err := w.updatePartialResult(ctx, sc, w.chk, len(w.partialResultsMap)); err != nil {
			trace_util_0.Count(_aggregate_00000, 50)
			w.globalOutputCh <- &AfFinalResult{err: err}
			return
		}
		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		trace_util_0.Count(_aggregate_00000, 48)
		needShuffle = true
	}
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, finalConcurrency int) (err error) {
	trace_util_0.Count(_aggregate_00000, 51)
	inputIter := chunk.NewIterator4Chunk(chk)
	for row := inputIter.Begin(); row != inputIter.End(); row = inputIter.Next() {
		trace_util_0.Count(_aggregate_00000, 53)
		groupKey, err := w.getGroupKey(sc, row)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 55)
			return err
		}
		trace_util_0.Count(_aggregate_00000, 54)
		partialResults := w.getPartialResult(sc, groupKey, w.partialResultsMap)
		for i, af := range w.aggFuncs {
			trace_util_0.Count(_aggregate_00000, 56)
			if err = af.UpdatePartialResult(ctx, []chunk.Row{row}, partialResults[i]); err != nil {
				trace_util_0.Count(_aggregate_00000, 57)
				return err
			}
		}
	}
	trace_util_0.Count(_aggregate_00000, 52)
	return nil
}

// shuffleIntermData shuffles the intermediate data of partial workers to corresponded final workers.
// We only support parallel execution for single-machine, so process of encode and decode can be skipped.
func (w *HashAggPartialWorker) shuffleIntermData(sc *stmtctx.StatementContext, finalConcurrency int) {
	trace_util_0.Count(_aggregate_00000, 58)
	groupKeysSlice := make([][]string, finalConcurrency)
	for groupKey := range w.partialResultsMap {
		trace_util_0.Count(_aggregate_00000, 60)
		finalWorkerIdx := int(murmur3.Sum32([]byte(groupKey))) % finalConcurrency
		if groupKeysSlice[finalWorkerIdx] == nil {
			trace_util_0.Count(_aggregate_00000, 62)
			groupKeysSlice[finalWorkerIdx] = make([]string, 0, len(w.partialResultsMap)/finalConcurrency)
		}
		trace_util_0.Count(_aggregate_00000, 61)
		groupKeysSlice[finalWorkerIdx] = append(groupKeysSlice[finalWorkerIdx], groupKey)
	}

	trace_util_0.Count(_aggregate_00000, 59)
	for i := range groupKeysSlice {
		trace_util_0.Count(_aggregate_00000, 63)
		if groupKeysSlice[i] == nil {
			trace_util_0.Count(_aggregate_00000, 65)
			continue
		}
		trace_util_0.Count(_aggregate_00000, 64)
		w.outputChs[i] <- &HashAggIntermData{
			groupKeys:        groupKeysSlice[i],
			partialResultMap: w.partialResultsMap,
		}
	}
}

// getGroupKey evaluates the group items and args of aggregate functions.
func (w *HashAggPartialWorker) getGroupKey(sc *stmtctx.StatementContext, row chunk.Row) ([]byte, error) {
	trace_util_0.Count(_aggregate_00000, 66)
	w.groupValDatums = w.groupValDatums[:0]
	for _, item := range w.groupByItems {
		trace_util_0.Count(_aggregate_00000, 68)
		v, err := item.Eval(row)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 71)
			return nil, err
		}
		// This check is used to avoid error during the execution of `EncodeDecimal`.
		trace_util_0.Count(_aggregate_00000, 69)
		if item.GetType().Tp == mysql.TypeNewDecimal {
			trace_util_0.Count(_aggregate_00000, 72)
			v.SetLength(0)
		}
		trace_util_0.Count(_aggregate_00000, 70)
		w.groupValDatums = append(w.groupValDatums, v)
	}
	trace_util_0.Count(_aggregate_00000, 67)
	var err error
	w.groupKey, err = codec.EncodeValue(sc, w.groupKey[:0], w.groupValDatums...)
	return w.groupKey, err
}

func (w baseHashAggWorker) getPartialResult(sc *stmtctx.StatementContext, groupKey []byte, mapper aggPartialResultMapper) []aggfuncs.PartialResult {
	trace_util_0.Count(_aggregate_00000, 73)
	partialResults, ok := mapper[string(groupKey)]
	if !ok {
		trace_util_0.Count(_aggregate_00000, 75)
		partialResults = make([]aggfuncs.PartialResult, 0, len(w.aggFuncs))
		for _, af := range w.aggFuncs {
			trace_util_0.Count(_aggregate_00000, 77)
			partialResults = append(partialResults, af.AllocPartialResult())
		}
		trace_util_0.Count(_aggregate_00000, 76)
		mapper[string(groupKey)] = partialResults
	}
	trace_util_0.Count(_aggregate_00000, 74)
	return partialResults
}

func (w *HashAggFinalWorker) getPartialInput() (input *HashAggIntermData, ok bool) {
	trace_util_0.Count(_aggregate_00000, 78)
	select {
	case <-w.finishCh:
		trace_util_0.Count(_aggregate_00000, 80)
		return nil, false
	case input, ok = <-w.inputCh:
		trace_util_0.Count(_aggregate_00000, 81)
		if !ok {
			trace_util_0.Count(_aggregate_00000, 82)
			return nil, false
		}
	}
	trace_util_0.Count(_aggregate_00000, 79)
	return
}

func (w *HashAggFinalWorker) consumeIntermData(sctx sessionctx.Context) (err error) {
	trace_util_0.Count(_aggregate_00000, 83)
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetSessionVars().StmtCtx
	)
	for {
		trace_util_0.Count(_aggregate_00000, 84)
		if input, ok = w.getPartialInput(); !ok {
			trace_util_0.Count(_aggregate_00000, 87)
			return nil
		}
		trace_util_0.Count(_aggregate_00000, 85)
		if intermDataBuffer == nil {
			trace_util_0.Count(_aggregate_00000, 88)
			intermDataBuffer = make([][]aggfuncs.PartialResult, 0, w.maxChunkSize)
		}
		// Consume input in batches, size of every batch is less than w.maxChunkSize.
		trace_util_0.Count(_aggregate_00000, 86)
		for reachEnd := false; !reachEnd; {
			trace_util_0.Count(_aggregate_00000, 89)
			intermDataBuffer, groupKeys, reachEnd = input.getPartialResultBatch(sc, intermDataBuffer[:0], w.aggFuncs, w.maxChunkSize)
			for i, groupKey := range groupKeys {
				trace_util_0.Count(_aggregate_00000, 90)
				if !w.groupSet.Exist(groupKey) {
					trace_util_0.Count(_aggregate_00000, 92)
					w.groupSet.Insert(groupKey)
				}
				trace_util_0.Count(_aggregate_00000, 91)
				prs := intermDataBuffer[i]
				finalPartialResults := w.getPartialResult(sc, []byte(groupKey), w.partialResultMap)
				for j, af := range w.aggFuncs {
					trace_util_0.Count(_aggregate_00000, 93)
					if err = af.MergePartialResult(sctx, prs[j], finalPartialResults[j]); err != nil {
						trace_util_0.Count(_aggregate_00000, 94)
						return err
					}
				}
			}
		}
	}
}

func (w *HashAggFinalWorker) getFinalResult(sctx sessionctx.Context) {
	trace_util_0.Count(_aggregate_00000, 95)
	result, finished := w.receiveFinalResultHolder()
	if finished {
		trace_util_0.Count(_aggregate_00000, 98)
		return
	}
	trace_util_0.Count(_aggregate_00000, 96)
	for groupKey := range w.groupSet {
		trace_util_0.Count(_aggregate_00000, 99)
		partialResults := w.getPartialResult(sctx.GetSessionVars().StmtCtx, []byte(groupKey), w.partialResultMap)
		for i, af := range w.aggFuncs {
			trace_util_0.Count(_aggregate_00000, 102)
			if err := af.AppendFinalResult2Chunk(sctx, partialResults[i], result); err != nil {
				trace_util_0.Count(_aggregate_00000, 103)
				logutil.Logger(context.Background()).Error("HashAggFinalWorker failed to append final result to Chunk", zap.Error(err))
			}
		}
		trace_util_0.Count(_aggregate_00000, 100)
		if len(w.aggFuncs) == 0 {
			trace_util_0.Count(_aggregate_00000, 104)
			result.SetNumVirtualRows(result.NumRows() + 1)
		}
		trace_util_0.Count(_aggregate_00000, 101)
		if result.IsFull() {
			trace_util_0.Count(_aggregate_00000, 105)
			w.outputCh <- &AfFinalResult{chk: result}
			result, finished = w.receiveFinalResultHolder()
			if finished {
				trace_util_0.Count(_aggregate_00000, 106)
				return
			}
		}
	}
	trace_util_0.Count(_aggregate_00000, 97)
	w.outputCh <- &AfFinalResult{chk: result}
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	trace_util_0.Count(_aggregate_00000, 107)
	select {
	case <-w.finishCh:
		trace_util_0.Count(_aggregate_00000, 108)
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		trace_util_0.Count(_aggregate_00000, 109)
		return result, !ok
	}
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	trace_util_0.Count(_aggregate_00000, 110)
	defer func() {
		trace_util_0.Count(_aggregate_00000, 113)
		if r := recover(); r != nil {
			trace_util_0.Count(_aggregate_00000, 115)
			recoveryHashAgg(w.outputCh, r)
		}
		trace_util_0.Count(_aggregate_00000, 114)
		waitGroup.Done()
	}()
	trace_util_0.Count(_aggregate_00000, 111)
	if err := w.consumeIntermData(ctx); err != nil {
		trace_util_0.Count(_aggregate_00000, 116)
		w.outputCh <- &AfFinalResult{err: err}
	}
	trace_util_0.Count(_aggregate_00000, 112)
	w.getFinalResult(ctx)
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_aggregate_00000, 117)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_aggregate_00000, 121)
		span1 := span.Tracer().StartSpan("hashagg.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_aggregate_00000, 118)
	if e.runtimeStats != nil {
		trace_util_0.Count(_aggregate_00000, 122)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_aggregate_00000, 123)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_aggregate_00000, 119)
	req.Reset()
	if e.isUnparallelExec {
		trace_util_0.Count(_aggregate_00000, 124)
		return e.unparallelExec(ctx, req.Chunk)
	}
	trace_util_0.Count(_aggregate_00000, 120)
	return e.parallelExec(ctx, req.Chunk)
}

func (e *HashAggExec) fetchChildData(ctx context.Context) {
	trace_util_0.Count(_aggregate_00000, 125)
	var (
		input *HashAggInput
		chk   *chunk.Chunk
		ok    bool
		err   error
	)
	defer func() {
		trace_util_0.Count(_aggregate_00000, 127)
		if r := recover(); r != nil {
			trace_util_0.Count(_aggregate_00000, 129)
			recoveryHashAgg(e.finalOutputCh, r)
		}
		trace_util_0.Count(_aggregate_00000, 128)
		for i := range e.partialInputChs {
			trace_util_0.Count(_aggregate_00000, 130)
			close(e.partialInputChs[i])
		}
	}()
	trace_util_0.Count(_aggregate_00000, 126)
	for {
		trace_util_0.Count(_aggregate_00000, 131)
		select {
		case <-e.finishCh:
			trace_util_0.Count(_aggregate_00000, 135)
			return
		case input, ok = <-e.inputCh:
			trace_util_0.Count(_aggregate_00000, 136)
			if !ok {
				trace_util_0.Count(_aggregate_00000, 138)
				return
			}
			trace_util_0.Count(_aggregate_00000, 137)
			chk = input.chk
		}
		trace_util_0.Count(_aggregate_00000, 132)
		err = Next(ctx, e.children[0], chunk.NewRecordBatch(chk))
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 139)
			e.finalOutputCh <- &AfFinalResult{err: err}
			return
		}
		trace_util_0.Count(_aggregate_00000, 133)
		if chk.NumRows() == 0 {
			trace_util_0.Count(_aggregate_00000, 140)
			return
		}
		trace_util_0.Count(_aggregate_00000, 134)
		input.giveBackCh <- chk
	}
}

func (e *HashAggExec) waitPartialWorkerAndCloseOutputChs(waitGroup *sync.WaitGroup) {
	trace_util_0.Count(_aggregate_00000, 141)
	waitGroup.Wait()
	for _, ch := range e.partialOutputChs {
		trace_util_0.Count(_aggregate_00000, 142)
		close(ch)
	}
}

func (e *HashAggExec) waitFinalWorkerAndCloseFinalOutput(waitGroup *sync.WaitGroup) {
	trace_util_0.Count(_aggregate_00000, 143)
	waitGroup.Wait()
	close(e.finalOutputCh)
}

func (e *HashAggExec) prepare4ParallelExec(ctx context.Context) {
	trace_util_0.Count(_aggregate_00000, 144)
	go e.fetchChildData(ctx)

	partialWorkerWaitGroup := &sync.WaitGroup{}
	partialWorkerWaitGroup.Add(len(e.partialWorkers))
	for i := range e.partialWorkers {
		trace_util_0.Count(_aggregate_00000, 147)
		go e.partialWorkers[i].run(e.ctx, partialWorkerWaitGroup, len(e.finalWorkers))
	}
	trace_util_0.Count(_aggregate_00000, 145)
	go e.waitPartialWorkerAndCloseOutputChs(partialWorkerWaitGroup)

	finalWorkerWaitGroup := &sync.WaitGroup{}
	finalWorkerWaitGroup.Add(len(e.finalWorkers))
	for i := range e.finalWorkers {
		trace_util_0.Count(_aggregate_00000, 148)
		go e.finalWorkers[i].run(e.ctx, finalWorkerWaitGroup)
	}
	trace_util_0.Count(_aggregate_00000, 146)
	go e.waitFinalWorkerAndCloseFinalOutput(finalWorkerWaitGroup)
}

// HashAggExec employs one input reader, M partial workers and N final workers to execute parallelly.
// The parallel execution flow is:
// 1. input reader reads data from child executor and send them to partial workers.
// 2. partial worker receives the input data, updates the partial results, and shuffle the partial results to the final workers.
// 3. final worker receives partial results from all the partial workers, evaluates the final results and sends the final results to the main thread.
func (e *HashAggExec) parallelExec(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_aggregate_00000, 149)
	if !e.prepared {
		trace_util_0.Count(_aggregate_00000, 153)
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	trace_util_0.Count(_aggregate_00000, 150)
	failpoint.Inject("parallelHashAggError", func(val failpoint.Value) {
		trace_util_0.Count(_aggregate_00000, 154)
		if val.(bool) {
			trace_util_0.Count(_aggregate_00000, 155)
			failpoint.Return(errors.New("HashAggExec.parallelExec error"))
		}
	})

	trace_util_0.Count(_aggregate_00000, 151)
	for !chk.IsFull() {
		trace_util_0.Count(_aggregate_00000, 156)
		e.finalInputCh <- chk
		result, ok := <-e.finalOutputCh
		if !ok {
			trace_util_0.Count(_aggregate_00000, 159) // all finalWorkers exited
			if chk.NumRows() > 0 {
				trace_util_0.Count(_aggregate_00000, 162) // but there are some data left
				return nil
			}
			trace_util_0.Count(_aggregate_00000, 160)
			if e.isChildReturnEmpty && e.defaultVal != nil {
				trace_util_0.Count(_aggregate_00000, 163)
				chk.Append(e.defaultVal, 0, 1)
			}
			trace_util_0.Count(_aggregate_00000, 161)
			e.isChildReturnEmpty = false
			return nil
		}
		trace_util_0.Count(_aggregate_00000, 157)
		if result.err != nil {
			trace_util_0.Count(_aggregate_00000, 164)
			return result.err
		}
		trace_util_0.Count(_aggregate_00000, 158)
		if chk.NumRows() > 0 {
			trace_util_0.Count(_aggregate_00000, 165)
			e.isChildReturnEmpty = false
		}
	}
	trace_util_0.Count(_aggregate_00000, 152)
	return nil
}

// unparallelExec executes hash aggregation algorithm in single thread.
func (e *HashAggExec) unparallelExec(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_aggregate_00000, 166)
	// In this stage we consider all data from src as a single group.
	if !e.prepared {
		trace_util_0.Count(_aggregate_00000, 169)
		err := e.execute(ctx)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 172)
			return err
		}
		trace_util_0.Count(_aggregate_00000, 170)
		if (len(e.groupSet) == 0) && len(e.GroupByItems) == 0 {
			trace_util_0.Count(_aggregate_00000, 173)
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groupSet.Insert("")
			e.groupKeys = append(e.groupKeys, "")
		}
		trace_util_0.Count(_aggregate_00000, 171)
		e.prepared = true
	}
	trace_util_0.Count(_aggregate_00000, 167)
	chk.Reset()

	// Since we return e.maxChunkSize rows every time, so we should not traverse
	// `groupSet` because of its randomness.
	for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
		trace_util_0.Count(_aggregate_00000, 174)
		partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
		if len(e.PartialAggFuncs) == 0 {
			trace_util_0.Count(_aggregate_00000, 177)
			chk.SetNumVirtualRows(chk.NumRows() + 1)
		}
		trace_util_0.Count(_aggregate_00000, 175)
		for i, af := range e.PartialAggFuncs {
			trace_util_0.Count(_aggregate_00000, 178)
			if err := af.AppendFinalResult2Chunk(e.ctx, partialResults[i], chk); err != nil {
				trace_util_0.Count(_aggregate_00000, 179)
				return err
			}
		}
		trace_util_0.Count(_aggregate_00000, 176)
		if chk.IsFull() {
			trace_util_0.Count(_aggregate_00000, 180)
			e.cursor4GroupKey++
			return nil
		}
	}
	trace_util_0.Count(_aggregate_00000, 168)
	return nil
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	trace_util_0.Count(_aggregate_00000, 181)
	inputIter := chunk.NewIterator4Chunk(e.childResult)
	for {
		trace_util_0.Count(_aggregate_00000, 182)
		err := Next(ctx, e.children[0], chunk.NewRecordBatch(e.childResult))
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 186)
			return err
		}

		trace_util_0.Count(_aggregate_00000, 183)
		failpoint.Inject("unparallelHashAggError", func(val failpoint.Value) {
			trace_util_0.Count(_aggregate_00000, 187)
			if val.(bool) {
				trace_util_0.Count(_aggregate_00000, 188)
				failpoint.Return(errors.New("HashAggExec.unparallelExec error"))
			}
		})

		// no more data.
		trace_util_0.Count(_aggregate_00000, 184)
		if e.childResult.NumRows() == 0 {
			trace_util_0.Count(_aggregate_00000, 189)
			return nil
		}
		trace_util_0.Count(_aggregate_00000, 185)
		for row := inputIter.Begin(); row != inputIter.End(); row = inputIter.Next() {
			trace_util_0.Count(_aggregate_00000, 190)
			groupKey, err := e.getGroupKey(row)
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 193)
				return err
			}
			trace_util_0.Count(_aggregate_00000, 191)
			if !e.groupSet.Exist(groupKey) {
				trace_util_0.Count(_aggregate_00000, 194)
				e.groupSet.Insert(groupKey)
				e.groupKeys = append(e.groupKeys, groupKey)
			}
			trace_util_0.Count(_aggregate_00000, 192)
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				trace_util_0.Count(_aggregate_00000, 195)
				err = af.UpdatePartialResult(e.ctx, []chunk.Row{row}, partialResults[i])
				if err != nil {
					trace_util_0.Count(_aggregate_00000, 196)
					return err
				}
			}
		}
	}
}

func (e *HashAggExec) getGroupKey(row chunk.Row) (string, error) {
	trace_util_0.Count(_aggregate_00000, 197)
	e.groupValDatums = e.groupValDatums[:0]
	for _, item := range e.GroupByItems {
		trace_util_0.Count(_aggregate_00000, 200)
		v, err := item.Eval(row)
		if item.GetType().Tp == mysql.TypeNewDecimal {
			trace_util_0.Count(_aggregate_00000, 203)
			v.SetLength(0)
		}
		trace_util_0.Count(_aggregate_00000, 201)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 204)
			return "", err
		}
		trace_util_0.Count(_aggregate_00000, 202)
		e.groupValDatums = append(e.groupValDatums, v)
	}
	trace_util_0.Count(_aggregate_00000, 198)
	var err error
	e.groupKeyBuffer, err = codec.EncodeValue(e.sc, e.groupKeyBuffer[:0], e.groupValDatums...)
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 205)
		return "", err
	}
	trace_util_0.Count(_aggregate_00000, 199)
	return string(e.groupKeyBuffer), nil
}

func (e *HashAggExec) getPartialResults(groupKey string) []aggfuncs.PartialResult {
	trace_util_0.Count(_aggregate_00000, 206)
	partialResults, ok := e.partialResultMap[groupKey]
	if !ok {
		trace_util_0.Count(_aggregate_00000, 208)
		partialResults = make([]aggfuncs.PartialResult, 0, len(e.PartialAggFuncs))
		for _, af := range e.PartialAggFuncs {
			trace_util_0.Count(_aggregate_00000, 210)
			partialResults = append(partialResults, af.AllocPartialResult())
		}
		trace_util_0.Count(_aggregate_00000, 209)
		e.partialResultMap[groupKey] = partialResults
	}
	trace_util_0.Count(_aggregate_00000, 207)
	return partialResults
}

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed bool
	// isChildReturnEmpty indicates whether the child executor only returns an empty input.
	isChildReturnEmpty bool
	defaultVal         *chunk.Chunk
	groupChecker       *groupChecker
	inputIter          *chunk.Iterator4Chunk
	inputRow           chunk.Row
	aggFuncs           []aggfuncs.AggFunc
	partialResults     []aggfuncs.PartialResult
	groupRows          []chunk.Row
	childResult        *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(ctx context.Context) error {
	trace_util_0.Count(_aggregate_00000, 211)
	if err := e.baseExecutor.Open(ctx); err != nil {
		trace_util_0.Count(_aggregate_00000, 214)
		return err
	}
	trace_util_0.Count(_aggregate_00000, 212)
	e.childResult = newFirstChunk(e.children[0])
	e.executed = false
	e.isChildReturnEmpty = true
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()

	e.partialResults = make([]aggfuncs.PartialResult, 0, len(e.aggFuncs))
	for _, aggFunc := range e.aggFuncs {
		trace_util_0.Count(_aggregate_00000, 215)
		e.partialResults = append(e.partialResults, aggFunc.AllocPartialResult())
	}

	trace_util_0.Count(_aggregate_00000, 213)
	return nil
}

// Close implements the Executor Close interface.
func (e *StreamAggExec) Close() error {
	trace_util_0.Count(_aggregate_00000, 216)
	e.childResult = nil
	e.groupChecker.reset()
	return e.baseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_aggregate_00000, 217)
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		trace_util_0.Count(_aggregate_00000, 221)
		span1 := span.Tracer().StartSpan("streamAgg.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	trace_util_0.Count(_aggregate_00000, 218)
	if e.runtimeStats != nil {
		trace_util_0.Count(_aggregate_00000, 222)
		start := time.Now()
		defer func() {
			trace_util_0.Count(_aggregate_00000, 223)
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	trace_util_0.Count(_aggregate_00000, 219)
	req.Reset()
	for !e.executed && !req.IsFull() {
		trace_util_0.Count(_aggregate_00000, 224)
		err := e.consumeOneGroup(ctx, req.Chunk)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 225)
			e.executed = true
			return err
		}
	}
	trace_util_0.Count(_aggregate_00000, 220)
	return nil
}

func (e *StreamAggExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	trace_util_0.Count(_aggregate_00000, 226)
	for !e.executed {
		trace_util_0.Count(_aggregate_00000, 228)
		if err := e.fetchChildIfNecessary(ctx, chk); err != nil {
			trace_util_0.Count(_aggregate_00000, 230)
			return err
		}
		trace_util_0.Count(_aggregate_00000, 229)
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			trace_util_0.Count(_aggregate_00000, 231)
			meetNewGroup, err := e.groupChecker.meetNewGroup(e.inputRow)
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 234)
				return err
			}
			trace_util_0.Count(_aggregate_00000, 232)
			if meetNewGroup {
				trace_util_0.Count(_aggregate_00000, 235)
				err := e.consumeGroupRows()
				if err != nil {
					trace_util_0.Count(_aggregate_00000, 237)
					return err
				}
				trace_util_0.Count(_aggregate_00000, 236)
				err = e.appendResult2Chunk(chk)
				if err != nil {
					trace_util_0.Count(_aggregate_00000, 238)
					return err
				}
			}
			trace_util_0.Count(_aggregate_00000, 233)
			e.groupRows = append(e.groupRows, e.inputRow)
			if meetNewGroup {
				trace_util_0.Count(_aggregate_00000, 239)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
	}
	trace_util_0.Count(_aggregate_00000, 227)
	return nil
}

func (e *StreamAggExec) consumeGroupRows() error {
	trace_util_0.Count(_aggregate_00000, 240)
	if len(e.groupRows) == 0 {
		trace_util_0.Count(_aggregate_00000, 243)
		return nil
	}

	trace_util_0.Count(_aggregate_00000, 241)
	for i, aggFunc := range e.aggFuncs {
		trace_util_0.Count(_aggregate_00000, 244)
		err := aggFunc.UpdatePartialResult(e.ctx, e.groupRows, e.partialResults[i])
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 245)
			return err
		}
	}
	trace_util_0.Count(_aggregate_00000, 242)
	e.groupRows = e.groupRows[:0]
	return nil
}

func (e *StreamAggExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) (err error) {
	trace_util_0.Count(_aggregate_00000, 246)
	if e.inputRow != e.inputIter.End() {
		trace_util_0.Count(_aggregate_00000, 251)
		return nil
	}

	// Before fetching a new batch of input, we should consume the last group.
	trace_util_0.Count(_aggregate_00000, 247)
	err = e.consumeGroupRows()
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 252)
		return err
	}

	trace_util_0.Count(_aggregate_00000, 248)
	err = Next(ctx, e.children[0], chunk.NewRecordBatch(e.childResult))
	if err != nil {
		trace_util_0.Count(_aggregate_00000, 253)
		return err
	}

	// No more data.
	trace_util_0.Count(_aggregate_00000, 249)
	if e.childResult.NumRows() == 0 {
		trace_util_0.Count(_aggregate_00000, 254)
		if !e.isChildReturnEmpty {
			trace_util_0.Count(_aggregate_00000, 256)
			err = e.appendResult2Chunk(chk)
		} else {
			trace_util_0.Count(_aggregate_00000, 257)
			if e.defaultVal != nil {
				trace_util_0.Count(_aggregate_00000, 258)
				chk.Append(e.defaultVal, 0, 1)
			}
		}
		trace_util_0.Count(_aggregate_00000, 255)
		e.executed = true
		return err
	}
	// Reach here, "e.childrenResults[0].NumRows() > 0" is guaranteed.
	trace_util_0.Count(_aggregate_00000, 250)
	e.isChildReturnEmpty = false
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and reset the evaluation context for each aggregation.
func (e *StreamAggExec) appendResult2Chunk(chk *chunk.Chunk) error {
	trace_util_0.Count(_aggregate_00000, 259)
	for i, aggFunc := range e.aggFuncs {
		trace_util_0.Count(_aggregate_00000, 262)
		err := aggFunc.AppendFinalResult2Chunk(e.ctx, e.partialResults[i], chk)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 264)
			return err
		}
		trace_util_0.Count(_aggregate_00000, 263)
		aggFunc.ResetPartialResult(e.partialResults[i])
	}
	trace_util_0.Count(_aggregate_00000, 260)
	if len(e.aggFuncs) == 0 {
		trace_util_0.Count(_aggregate_00000, 265)
		chk.SetNumVirtualRows(chk.NumRows() + 1)
	}
	trace_util_0.Count(_aggregate_00000, 261)
	return nil
}

type groupChecker struct {
	StmtCtx      *stmtctx.StatementContext
	GroupByItems []expression.Expression
	curGroupKey  []types.Datum
	tmpGroupKey  []types.Datum
}

func newGroupChecker(stmtCtx *stmtctx.StatementContext, items []expression.Expression) *groupChecker {
	trace_util_0.Count(_aggregate_00000, 266)
	return &groupChecker{
		StmtCtx:      stmtCtx,
		GroupByItems: items,
	}
}

// meetNewGroup returns a value that represents if the new group is different from last group.
// TODO: Since all the group by items are only a column reference, guaranteed by building projection below aggregation, we can directly compare data in a chunk.
func (e *groupChecker) meetNewGroup(row chunk.Row) (bool, error) {
	trace_util_0.Count(_aggregate_00000, 267)
	if len(e.GroupByItems) == 0 {
		trace_util_0.Count(_aggregate_00000, 273)
		return false, nil
	}
	trace_util_0.Count(_aggregate_00000, 268)
	e.tmpGroupKey = e.tmpGroupKey[:0]
	matched, firstGroup := true, false
	if len(e.curGroupKey) == 0 {
		trace_util_0.Count(_aggregate_00000, 274)
		matched, firstGroup = false, true
	}
	trace_util_0.Count(_aggregate_00000, 269)
	for i, item := range e.GroupByItems {
		trace_util_0.Count(_aggregate_00000, 275)
		v, err := item.Eval(row)
		if err != nil {
			trace_util_0.Count(_aggregate_00000, 278)
			return false, err
		}
		trace_util_0.Count(_aggregate_00000, 276)
		if matched {
			trace_util_0.Count(_aggregate_00000, 279)
			c, err := v.CompareDatum(e.StmtCtx, &e.curGroupKey[i])
			if err != nil {
				trace_util_0.Count(_aggregate_00000, 281)
				return false, err
			}
			trace_util_0.Count(_aggregate_00000, 280)
			matched = c == 0
		}
		trace_util_0.Count(_aggregate_00000, 277)
		e.tmpGroupKey = append(e.tmpGroupKey, v)
	}
	trace_util_0.Count(_aggregate_00000, 270)
	if matched {
		trace_util_0.Count(_aggregate_00000, 282)
		return false, nil
	}
	trace_util_0.Count(_aggregate_00000, 271)
	e.curGroupKey = e.curGroupKey[:0]
	for _, v := range e.tmpGroupKey {
		trace_util_0.Count(_aggregate_00000, 283)
		e.curGroupKey = append(e.curGroupKey, *((&v).Copy()))
	}
	trace_util_0.Count(_aggregate_00000, 272)
	return !firstGroup, nil
}

func (e *groupChecker) reset() {
	trace_util_0.Count(_aggregate_00000, 284)
	if e.curGroupKey != nil {
		trace_util_0.Count(_aggregate_00000, 286)
		e.curGroupKey = e.curGroupKey[:0]
	}
	trace_util_0.Count(_aggregate_00000, 285)
	if e.tmpGroupKey != nil {
		trace_util_0.Count(_aggregate_00000, 287)
		e.tmpGroupKey = e.tmpGroupKey[:0]
	}
}

var _aggregate_00000 = "executor/aggregate.go"
