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
	"bytes"
	"context"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	baseExecutor
	tasks []*analyzeTask
	wg    *sync.WaitGroup
}

var (
	// MaxSampleSize is the size of samples for once analyze.
	// It's public for test.
	MaxSampleSize = 10000
	// RandSeed is the seed for randing package.
	// It's public for test.
	RandSeed = int64(1)
)

const (
	maxRegionSampleSize  = 1000
	maxSketchSize        = 10000
	defaultCMSketchDepth = 5
	defaultCMSketchWidth = 2048
)

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_analyze_00000, 0)
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 8)
		return err
	}
	trace_util_0.Count(_analyze_00000, 1)
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan analyzeResult, len(e.tasks))
	e.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		trace_util_0.Count(_analyze_00000, 9)
		go e.analyzeWorker(taskCh, resultCh, i == 0)
	}
	trace_util_0.Count(_analyze_00000, 2)
	for _, task := range e.tasks {
		trace_util_0.Count(_analyze_00000, 10)
		statistics.AddNewAnalyzeJob(task.job)
	}
	trace_util_0.Count(_analyze_00000, 3)
	for _, task := range e.tasks {
		trace_util_0.Count(_analyze_00000, 11)
		taskCh <- task
	}
	trace_util_0.Count(_analyze_00000, 4)
	close(taskCh)
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	panicCnt := 0
	for panicCnt < concurrency {
		trace_util_0.Count(_analyze_00000, 12)
		result, ok := <-resultCh
		if !ok {
			trace_util_0.Count(_analyze_00000, 16)
			break
		}
		trace_util_0.Count(_analyze_00000, 13)
		if result.Err != nil {
			trace_util_0.Count(_analyze_00000, 17)
			err = result.Err
			if err == errAnalyzeWorkerPanic {
				trace_util_0.Count(_analyze_00000, 19)
				panicCnt++
			} else {
				trace_util_0.Count(_analyze_00000, 20)
				{
					logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
				}
			}
			trace_util_0.Count(_analyze_00000, 18)
			result.job.Finish(true)
			continue
		}
		trace_util_0.Count(_analyze_00000, 14)
		for i, hg := range result.Hist {
			trace_util_0.Count(_analyze_00000, 21)
			err1 := statsHandle.SaveStatsToStorage(result.PhysicalTableID, result.Count, result.IsIndex, hg, result.Cms[i], 1)
			if err1 != nil {
				trace_util_0.Count(_analyze_00000, 22)
				err = err1
				logutil.Logger(ctx).Error("save stats to storage failed", zap.Error(err))
				result.job.Finish(true)
				continue
			}
		}
		trace_util_0.Count(_analyze_00000, 15)
		result.job.Finish(false)
	}
	trace_util_0.Count(_analyze_00000, 5)
	for _, task := range e.tasks {
		trace_util_0.Count(_analyze_00000, 23)
		statistics.MoveToHistory(task.job)
	}
	trace_util_0.Count(_analyze_00000, 6)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 24)
		return err
	}
	trace_util_0.Count(_analyze_00000, 7)
	return statsHandle.Update(GetInfoSchema(e.ctx))
}

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	trace_util_0.Count(_analyze_00000, 25)
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 27)
		return 0, err
	}
	trace_util_0.Count(_analyze_00000, 26)
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

type taskType int

const (
	colTask taskType = iota
	idxTask
	fastTask
	pkIncrementalTask
	idxIncrementalTask
)

type analyzeTask struct {
	taskType           taskType
	idxExec            *AnalyzeIndexExec
	colExec            *AnalyzeColumnsExec
	fastExec           *AnalyzeFastExec
	idxIncrementalExec *analyzeIndexIncrementalExec
	colIncrementalExec *analyzePKIncrementalExec
	job                *statistics.AnalyzeJob
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- analyzeResult, isCloseChanThread bool) {
	trace_util_0.Count(_analyze_00000, 28)
	var task *analyzeTask
	defer func() {
		trace_util_0.Count(_analyze_00000, 30)
		if r := recover(); r != nil {
			trace_util_0.Count(_analyze_00000, 32)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(context.Background()).Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- analyzeResult{
				Err: errAnalyzeWorkerPanic,
				job: task.job,
			}
		}
		trace_util_0.Count(_analyze_00000, 31)
		e.wg.Done()
		if isCloseChanThread {
			trace_util_0.Count(_analyze_00000, 33)
			e.wg.Wait()
			close(resultCh)
		}
	}()
	trace_util_0.Count(_analyze_00000, 29)
	for {
		trace_util_0.Count(_analyze_00000, 34)
		var ok bool
		task, ok = <-taskCh
		if !ok {
			trace_util_0.Count(_analyze_00000, 36)
			break
		}
		trace_util_0.Count(_analyze_00000, 35)
		task.job.Start()
		switch task.taskType {
		case colTask:
			trace_util_0.Count(_analyze_00000, 37)
			task.colExec.job = task.job
			resultCh <- analyzeColumnsPushdown(task.colExec)
		case idxTask:
			trace_util_0.Count(_analyze_00000, 38)
			task.idxExec.job = task.job
			resultCh <- analyzeIndexPushdown(task.idxExec)
		case fastTask:
			trace_util_0.Count(_analyze_00000, 39)
			task.fastExec.job = task.job
			task.job.Start()
			for _, result := range analyzeFastExec(task.fastExec) {
				trace_util_0.Count(_analyze_00000, 42)
				resultCh <- result
			}
		case pkIncrementalTask:
			trace_util_0.Count(_analyze_00000, 40)
			task.colIncrementalExec.job = task.job
			resultCh <- analyzePKIncremental(task.colIncrementalExec)
		case idxIncrementalTask:
			trace_util_0.Count(_analyze_00000, 41)
			task.idxIncrementalExec.job = task.job
			resultCh <- analyzeIndexIncremental(task.idxIncrementalExec)
		}
	}
}

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) analyzeResult {
	trace_util_0.Count(_analyze_00000, 43)
	ranges := ranger.FullRange()
	// For single-column index, we do not load null rows from TiKV, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-column index is always 0 then.
	if len(idxExec.idxInfo.Columns) == 1 {
		trace_util_0.Count(_analyze_00000, 47)
		ranges = ranger.FullNotNullRange()
	}
	trace_util_0.Count(_analyze_00000, 44)
	hist, cms, err := idxExec.buildStats(ranges, true)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 48)
		return analyzeResult{Err: err, job: idxExec.job}
	}
	trace_util_0.Count(_analyze_00000, 45)
	result := analyzeResult{
		PhysicalTableID: idxExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{cms},
		IsIndex:         1,
		job:             idxExec.job,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		trace_util_0.Count(_analyze_00000, 49)
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	trace_util_0.Count(_analyze_00000, 46)
	return result
}

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	idxInfo         *model.IndexInfo
	concurrency     int
	priority        int
	analyzePB       *tipb.AnalyzeReq
	result          distsql.SelectResult
	countNullRes    distsql.SelectResult
	maxNumBuckets   uint64
	job             *statistics.AnalyzeJob
}

// fetchAnalyzeResult builds and dispatches the `kv.Request` from given ranges, and stores the `SelectResult`
// in corresponding fields based on the input `isNullRange` argument, which indicates if the range is the
// special null range for single-column index to get the null count.
func (e *AnalyzeIndexExec) fetchAnalyzeResult(ranges []*ranger.Range, isNullRange bool) error {
	trace_util_0.Count(_analyze_00000, 50)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.idxInfo.ID, ranges).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		trace_util_0.Count(_analyze_00000, 54)
		return err
	}
	trace_util_0.Count(_analyze_00000, 51)
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 55)
		return err
	}
	trace_util_0.Count(_analyze_00000, 52)
	result.Fetch(ctx)
	if isNullRange {
		trace_util_0.Count(_analyze_00000, 56)
		e.countNullRes = result
	} else {
		trace_util_0.Count(_analyze_00000, 57)
		{
			e.result = result
		}
	}
	trace_util_0.Count(_analyze_00000, 53)
	return nil
}

func (e *AnalyzeIndexExec) open(ranges []*ranger.Range, considerNull bool) error {
	trace_util_0.Count(_analyze_00000, 58)
	err := e.fetchAnalyzeResult(ranges, false)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 61)
		return err
	}
	trace_util_0.Count(_analyze_00000, 59)
	if considerNull && len(e.idxInfo.Columns) == 1 {
		trace_util_0.Count(_analyze_00000, 62)
		ranges = ranger.NullRange()
		err = e.fetchAnalyzeResult(ranges, true)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 63)
			return err
		}
	}
	trace_util_0.Count(_analyze_00000, 60)
	return nil
}

func (e *AnalyzeIndexExec) buildStatsFromResult(result distsql.SelectResult, needCMS bool) (*statistics.Histogram, *statistics.CMSketch, error) {
	trace_util_0.Count(_analyze_00000, 64)
	failpoint.Inject("buildStatsFromResult", func(val failpoint.Value) {
		trace_util_0.Count(_analyze_00000, 68)
		if val.(bool) {
			trace_util_0.Count(_analyze_00000, 69)
			failpoint.Return(nil, nil, errors.New("mock buildStatsFromResult error"))
		}
	})
	trace_util_0.Count(_analyze_00000, 65)
	hist := &statistics.Histogram{}
	var cms *statistics.CMSketch
	if needCMS {
		trace_util_0.Count(_analyze_00000, 70)
		cms = statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth)
	}
	trace_util_0.Count(_analyze_00000, 66)
	for {
		trace_util_0.Count(_analyze_00000, 71)
		data, err := result.NextRaw(context.TODO())
		if err != nil {
			trace_util_0.Count(_analyze_00000, 76)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 72)
		if data == nil {
			trace_util_0.Count(_analyze_00000, 77)
			break
		}
		trace_util_0.Count(_analyze_00000, 73)
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 78)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 74)
		respHist := statistics.HistogramFromProto(resp.Hist)
		e.job.Update(int64(respHist.TotalRowCount()))
		hist, err = statistics.MergeHistograms(e.ctx.GetSessionVars().StmtCtx, hist, respHist, int(e.maxNumBuckets))
		if err != nil {
			trace_util_0.Count(_analyze_00000, 79)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 75)
		if needCMS {
			trace_util_0.Count(_analyze_00000, 80)
			if resp.Cms == nil {
				trace_util_0.Count(_analyze_00000, 81)
				logutil.Logger(context.TODO()).Warn("nil CMS in response", zap.String("table", e.idxInfo.Table.O), zap.String("index", e.idxInfo.Name.O))
			} else {
				trace_util_0.Count(_analyze_00000, 82)
				if err := cms.MergeCMSketch(statistics.CMSketchFromProto(resp.Cms), 0); err != nil {
					trace_util_0.Count(_analyze_00000, 83)
					return nil, nil, err
				}
			}
		}
	}
	trace_util_0.Count(_analyze_00000, 67)
	return hist, cms, nil
}

func (e *AnalyzeIndexExec) buildStats(ranges []*ranger.Range, considerNull bool) (hist *statistics.Histogram, cms *statistics.CMSketch, err error) {
	trace_util_0.Count(_analyze_00000, 84)
	if err = e.open(ranges, considerNull); err != nil {
		trace_util_0.Count(_analyze_00000, 89)
		return nil, nil, err
	}
	trace_util_0.Count(_analyze_00000, 85)
	defer func() {
		trace_util_0.Count(_analyze_00000, 90)
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			trace_util_0.Count(_analyze_00000, 91)
			err = err1
		}
	}()
	trace_util_0.Count(_analyze_00000, 86)
	hist, cms, err = e.buildStatsFromResult(e.result, true)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 92)
		return nil, nil, err
	}
	trace_util_0.Count(_analyze_00000, 87)
	if e.countNullRes != nil {
		trace_util_0.Count(_analyze_00000, 93)
		nullHist, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 95)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 94)
		if l := nullHist.Len(); l > 0 {
			trace_util_0.Count(_analyze_00000, 96)
			hist.NullCount = nullHist.Buckets[l-1].Count
		}
	}
	trace_util_0.Count(_analyze_00000, 88)
	hist.ID = e.idxInfo.ID
	return hist, cms, nil
}

func analyzeColumnsPushdown(colExec *AnalyzeColumnsExec) analyzeResult {
	trace_util_0.Count(_analyze_00000, 97)
	var ranges []*ranger.Range
	if colExec.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 101)
		ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(colExec.pkInfo.Flag))
	} else {
		trace_util_0.Count(_analyze_00000, 102)
		{
			ranges = ranger.FullIntRange(false)
		}
	}
	trace_util_0.Count(_analyze_00000, 98)
	hists, cms, err := colExec.buildStats(ranges)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 103)
		return analyzeResult{Err: err, job: colExec.job}
	}
	trace_util_0.Count(_analyze_00000, 99)
	result := analyzeResult{
		PhysicalTableID: colExec.physicalTableID,
		Hist:            hists,
		Cms:             cms,
		job:             colExec.job,
	}
	hist := hists[0]
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		trace_util_0.Count(_analyze_00000, 104)
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	trace_util_0.Count(_analyze_00000, 100)
	return result
}

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	colsInfo        []*model.ColumnInfo
	pkInfo          *model.ColumnInfo
	concurrency     int
	priority        int
	analyzePB       *tipb.AnalyzeReq
	resultHandler   *tableResultHandler
	maxNumBuckets   uint64
	job             *statistics.AnalyzeJob
}

func (e *AnalyzeColumnsExec) open(ranges []*ranger.Range) error {
	trace_util_0.Count(_analyze_00000, 105)
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(ranges, true, false)
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 109)
		return err
	}
	trace_util_0.Count(_analyze_00000, 106)
	if len(secondPartRanges) == 0 {
		trace_util_0.Count(_analyze_00000, 110)
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	trace_util_0.Count(_analyze_00000, 107)
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 111)
		return err
	}
	trace_util_0.Count(_analyze_00000, 108)
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeColumnsExec) buildResp(ranges []*ranger.Range) (distsql.SelectResult, error) {
	trace_util_0.Count(_analyze_00000, 112)
	var builder distsql.RequestBuilder
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of columns.
	kvReq, err := builder.SetTableRanges(e.physicalTableID, ranges, nil).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		trace_util_0.Count(_analyze_00000, 115)
		return nil, err
	}
	trace_util_0.Count(_analyze_00000, 113)
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 116)
		return nil, err
	}
	trace_util_0.Count(_analyze_00000, 114)
	result.Fetch(ctx)
	return result, nil
}

func (e *AnalyzeColumnsExec) buildStats(ranges []*ranger.Range) (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	trace_util_0.Count(_analyze_00000, 117)
	if err = e.open(ranges); err != nil {
		trace_util_0.Count(_analyze_00000, 124)
		return nil, nil, err
	}
	trace_util_0.Count(_analyze_00000, 118)
	defer func() {
		trace_util_0.Count(_analyze_00000, 125)
		if err1 := e.resultHandler.Close(); err1 != nil {
			trace_util_0.Count(_analyze_00000, 126)
			hists = nil
			cms = nil
			err = err1
		}
	}()
	trace_util_0.Count(_analyze_00000, 119)
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		trace_util_0.Count(_analyze_00000, 127)
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: int64(MaxSampleSize),
			CMSketch:      statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth),
		}
	}
	trace_util_0.Count(_analyze_00000, 120)
	for {
		trace_util_0.Count(_analyze_00000, 128)
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			trace_util_0.Count(_analyze_00000, 134)
			return nil, nil, err1
		}
		trace_util_0.Count(_analyze_00000, 129)
		if data == nil {
			trace_util_0.Count(_analyze_00000, 135)
			break
		}
		trace_util_0.Count(_analyze_00000, 130)
		resp := &tipb.AnalyzeColumnsResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 136)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 131)
		sc := e.ctx.GetSessionVars().StmtCtx
		rowCount := int64(0)
		if e.pkInfo != nil {
			trace_util_0.Count(_analyze_00000, 137)
			respHist := statistics.HistogramFromProto(resp.PkHist)
			rowCount = int64(respHist.TotalRowCount())
			pkHist, err = statistics.MergeHistograms(sc, pkHist, respHist, int(e.maxNumBuckets))
			if err != nil {
				trace_util_0.Count(_analyze_00000, 138)
				return nil, nil, err
			}
		}
		trace_util_0.Count(_analyze_00000, 132)
		for i, rc := range resp.Collectors {
			trace_util_0.Count(_analyze_00000, 139)
			respSample := statistics.SampleCollectorFromProto(rc)
			rowCount = respSample.Count + respSample.NullCount
			collectors[i].MergeSampleCollector(sc, respSample)
		}
		trace_util_0.Count(_analyze_00000, 133)
		e.job.Update(rowCount)
	}
	trace_util_0.Count(_analyze_00000, 121)
	timeZone := e.ctx.GetSessionVars().Location()
	if e.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 140)
		pkHist.ID = e.pkInfo.ID
		err = pkHist.DecodeTo(&e.pkInfo.FieldType, timeZone)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 142)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 141)
		hists = append(hists, pkHist)
		cms = append(cms, nil)
	}
	trace_util_0.Count(_analyze_00000, 122)
	for i, col := range e.colsInfo {
		trace_util_0.Count(_analyze_00000, 143)
		for j, s := range collectors[i].Samples {
			trace_util_0.Count(_analyze_00000, 146)
			collectors[i].Samples[j].Ordinal = j
			collectors[i].Samples[j].Value, err = tablecodec.DecodeColumnValue(s.Value.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				trace_util_0.Count(_analyze_00000, 147)
				return nil, nil, err
			}
		}
		trace_util_0.Count(_analyze_00000, 144)
		hg, err := statistics.BuildColumn(e.ctx, int64(e.maxNumBuckets), col.ID, collectors[i], &col.FieldType)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 148)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 145)
		hists = append(hists, hg)
		cms = append(cms, collectors[i].CMSketch)
	}
	trace_util_0.Count(_analyze_00000, 123)
	return hists, cms, nil
}

func analyzeFastExec(exec *AnalyzeFastExec) []analyzeResult {
	trace_util_0.Count(_analyze_00000, 149)
	hists, cms, err := exec.buildStats()
	if err != nil {
		trace_util_0.Count(_analyze_00000, 154)
		return []analyzeResult{{Err: err, job: exec.job}}
	}
	trace_util_0.Count(_analyze_00000, 150)
	var results []analyzeResult
	hasPKInfo := 0
	if exec.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 155)
		hasPKInfo = 1
	}
	trace_util_0.Count(_analyze_00000, 151)
	if len(exec.idxsInfo) > 0 {
		trace_util_0.Count(_analyze_00000, 156)
		for i := hasPKInfo + len(exec.colsInfo); i < len(hists); i++ {
			trace_util_0.Count(_analyze_00000, 157)
			idxResult := analyzeResult{
				PhysicalTableID: exec.physicalTableID,
				Hist:            []*statistics.Histogram{hists[i]},
				Cms:             []*statistics.CMSketch{cms[i]},
				IsIndex:         1,
				Count:           hists[i].NullCount,
				job:             exec.job,
			}
			if hists[i].Len() > 0 {
				trace_util_0.Count(_analyze_00000, 159)
				idxResult.Count += hists[i].Buckets[hists[i].Len()-1].Count
			}
			trace_util_0.Count(_analyze_00000, 158)
			results = append(results, idxResult)
		}
	}
	trace_util_0.Count(_analyze_00000, 152)
	hist := hists[0]
	colResult := analyzeResult{
		PhysicalTableID: exec.physicalTableID,
		Hist:            hists[:hasPKInfo+len(exec.colsInfo)],
		Cms:             cms[:hasPKInfo+len(exec.colsInfo)],
		Count:           hist.NullCount,
		job:             exec.job,
	}
	if hist.Len() > 0 {
		trace_util_0.Count(_analyze_00000, 160)
		colResult.Count += hist.Buckets[hist.Len()-1].Count
	}
	trace_util_0.Count(_analyze_00000, 153)
	results = append(results, colResult)
	return results
}

// AnalyzeFastTask is the task for build stats.
type AnalyzeFastTask struct {
	Location    *tikv.KeyLocation
	SampSize    uint64
	BeginOffset uint64
	EndOffset   uint64
}

// AnalyzeFastExec represents Fast Analyze executor.
type AnalyzeFastExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	pkInfo          *model.ColumnInfo
	colsInfo        []*model.ColumnInfo
	idxsInfo        []*model.IndexInfo
	concurrency     int
	maxNumBuckets   uint64
	tblInfo         *model.TableInfo
	cache           *tikv.RegionCache
	wg              *sync.WaitGroup
	sampLocs        chan *tikv.KeyLocation
	rowCount        uint64
	sampCursor      int32
	sampTasks       []*AnalyzeFastTask
	scanTasks       []*tikv.KeyLocation
	collectors      []*statistics.SampleCollector
	randSeed        int64
	job             *statistics.AnalyzeJob
}

func (e *AnalyzeFastExec) getSampRegionsRowCount(bo *tikv.Backoffer, needRebuild *bool, err *error, sampTasks *[]*AnalyzeFastTask) {
	trace_util_0.Count(_analyze_00000, 161)
	defer func() {
		trace_util_0.Count(_analyze_00000, 163)
		if *needRebuild == true {
			trace_util_0.Count(_analyze_00000, 165)
			for ok := true; ok; _, ok = <-e.sampLocs {
				trace_util_0.Count(_analyze_00000, 166)
				// Do nothing, just clear the channel.
			}
		}
		trace_util_0.Count(_analyze_00000, 164)
		e.wg.Done()
	}()
	trace_util_0.Count(_analyze_00000, 162)
	client := e.ctx.GetStore().(tikv.Storage).GetTiKVClient()
	for {
		trace_util_0.Count(_analyze_00000, 167)
		loc, ok := <-e.sampLocs
		if !ok {
			trace_util_0.Count(_analyze_00000, 172)
			return
		}
		trace_util_0.Count(_analyze_00000, 168)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDebugGetRegionProperties,
			DebugGetRegionProperties: &debugpb.GetRegionPropertiesRequest{
				RegionId: loc.Region.GetID(),
			},
		}
		var resp *tikvrpc.Response
		var rpcCtx *tikv.RPCContext
		rpcCtx, *err = e.cache.GetRPCContext(bo, loc.Region)
		if *err != nil {
			trace_util_0.Count(_analyze_00000, 173)
			return
		}
		trace_util_0.Count(_analyze_00000, 169)
		ctx := context.Background()
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			trace_util_0.Count(_analyze_00000, 174)
			return
		}
		trace_util_0.Count(_analyze_00000, 170)
		if resp.DebugGetRegionProperties == nil || len(resp.DebugGetRegionProperties.Props) == 0 {
			trace_util_0.Count(_analyze_00000, 175)
			*needRebuild = true
			return
		}
		trace_util_0.Count(_analyze_00000, 171)
		for _, prop := range resp.DebugGetRegionProperties.Props {
			trace_util_0.Count(_analyze_00000, 176)
			if prop.Name == "mvcc.num_rows" {
				trace_util_0.Count(_analyze_00000, 177)
				var cnt uint64
				cnt, *err = strconv.ParseUint(prop.Value, 10, 64)
				if *err != nil {
					trace_util_0.Count(_analyze_00000, 179)
					return
				}
				trace_util_0.Count(_analyze_00000, 178)
				newCount := atomic.AddUint64(&e.rowCount, cnt)
				task := &AnalyzeFastTask{
					Location:    loc,
					BeginOffset: newCount - cnt,
					EndOffset:   newCount,
				}
				*sampTasks = append(*sampTasks, task)
				break
			}
		}
	}
}

// getNextSampleKey gets the next sample key after last failed request. It only retries the needed region.
// Different from other requests, each request range must be the whole region because the region row count
// is only for a whole region. So we need to first find the longest successive prefix ranges of previous request,
// then the next sample key should be the last range that could align with the region bound.
func (e *AnalyzeFastExec) getNextSampleKey(bo *tikv.Backoffer, startKey kv.Key) (kv.Key, error) {
	trace_util_0.Count(_analyze_00000, 180)
	if len(e.sampTasks) == 0 {
		trace_util_0.Count(_analyze_00000, 187)
		e.scanTasks = e.scanTasks[:0]
		return startKey, nil
	}
	trace_util_0.Count(_analyze_00000, 181)
	sort.Slice(e.sampTasks, func(i, j int) bool {
		trace_util_0.Count(_analyze_00000, 188)
		return bytes.Compare(e.sampTasks[i].Location.StartKey, e.sampTasks[j].Location.StartKey) < 0
	})
	// The sample task should be consecutive with scan task.
	trace_util_0.Count(_analyze_00000, 182)
	if len(e.scanTasks) > 0 && bytes.Equal(e.scanTasks[0].StartKey, startKey) && !bytes.Equal(e.scanTasks[0].EndKey, e.sampTasks[0].Location.StartKey) {
		trace_util_0.Count(_analyze_00000, 189)
		e.scanTasks = e.scanTasks[:0]
		e.sampTasks = e.sampTasks[:0]
		return startKey, nil
	}
	trace_util_0.Count(_analyze_00000, 183)
	prefixLen := 0
	for ; prefixLen < len(e.sampTasks)-1; prefixLen++ {
		trace_util_0.Count(_analyze_00000, 190)
		if !bytes.Equal(e.sampTasks[prefixLen].Location.EndKey, e.sampTasks[prefixLen+1].Location.StartKey) {
			trace_util_0.Count(_analyze_00000, 191)
			break
		}
	}
	// Find the last one that could align with region bound.
	trace_util_0.Count(_analyze_00000, 184)
	for ; prefixLen >= 0; prefixLen-- {
		trace_util_0.Count(_analyze_00000, 192)
		loc, err := e.cache.LocateKey(bo, e.sampTasks[prefixLen].Location.EndKey)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 194)
			return nil, err
		}
		trace_util_0.Count(_analyze_00000, 193)
		if bytes.Compare(loc.StartKey, e.sampTasks[prefixLen].Location.EndKey) == 0 {
			trace_util_0.Count(_analyze_00000, 195)
			startKey = loc.StartKey
			break
		}
	}
	trace_util_0.Count(_analyze_00000, 185)
	e.sampTasks = e.sampTasks[:prefixLen+1]
	for i := len(e.scanTasks) - 1; i >= 0; i-- {
		trace_util_0.Count(_analyze_00000, 196)
		if bytes.Compare(startKey, e.scanTasks[i].EndKey) < 0 {
			trace_util_0.Count(_analyze_00000, 197)
			e.scanTasks = e.scanTasks[:i]
		}
	}
	trace_util_0.Count(_analyze_00000, 186)
	return startKey, nil
}

// buildSampTask returns two variables, the first bool is whether the task meets region error
// and need to rebuild.
func (e *AnalyzeFastExec) buildSampTask() (needRebuild bool, err error) {
	trace_util_0.Count(_analyze_00000, 198)
	// Do get regions row count.
	bo := tikv.NewBackoffer(context.Background(), 500)
	needRebuildForRoutine := make([]bool, e.concurrency)
	errs := make([]error, e.concurrency)
	sampTasksForRoutine := make([][]*AnalyzeFastTask, e.concurrency)
	e.sampLocs = make(chan *tikv.KeyLocation, e.concurrency)
	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		trace_util_0.Count(_analyze_00000, 204)
		go e.getSampRegionsRowCount(bo, &needRebuildForRoutine[i], &errs[i], &sampTasksForRoutine[i])
	}

	trace_util_0.Count(_analyze_00000, 199)
	defer func() {
		trace_util_0.Count(_analyze_00000, 205)
		close(e.sampLocs)
		e.wg.Wait()
		if err != nil {
			trace_util_0.Count(_analyze_00000, 207)
			return
		}
		trace_util_0.Count(_analyze_00000, 206)
		for i := 0; i < e.concurrency; i++ {
			trace_util_0.Count(_analyze_00000, 208)
			if errs[i] != nil {
				trace_util_0.Count(_analyze_00000, 210)
				err = errs[i]
			}
			trace_util_0.Count(_analyze_00000, 209)
			needRebuild = needRebuild || needRebuildForRoutine[i]
			e.sampTasks = append(e.sampTasks, sampTasksForRoutine[i]...)
		}
	}()

	trace_util_0.Count(_analyze_00000, 200)
	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(e.physicalTableID)
	targetKey, err := e.getNextSampleKey(bo, startKey)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 211)
		return false, err
	}
	trace_util_0.Count(_analyze_00000, 201)
	e.rowCount = 0
	for _, task := range e.sampTasks {
		trace_util_0.Count(_analyze_00000, 212)
		cnt := task.EndOffset - task.BeginOffset
		task.BeginOffset = e.rowCount
		task.EndOffset = e.rowCount + cnt
		e.rowCount += cnt
	}
	trace_util_0.Count(_analyze_00000, 202)
	for {
		trace_util_0.Count(_analyze_00000, 213)
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 218)
			return false, err
		}
		trace_util_0.Count(_analyze_00000, 214)
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			trace_util_0.Count(_analyze_00000, 219)
			break
		}
		// Set the next search key.
		trace_util_0.Count(_analyze_00000, 215)
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			trace_util_0.Count(_analyze_00000, 220)
			e.sampLocs <- loc
			continue
		}

		trace_util_0.Count(_analyze_00000, 216)
		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			trace_util_0.Count(_analyze_00000, 221)
			loc.StartKey = startKey
		}
		trace_util_0.Count(_analyze_00000, 217)
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			trace_util_0.Count(_analyze_00000, 222)
			loc.EndKey = endKey
			break
		}
	}

	trace_util_0.Count(_analyze_00000, 203)
	return false, nil
}

func (e *AnalyzeFastExec) decodeValues(sValue []byte) (values map[int64]types.Datum, err error) {
	trace_util_0.Count(_analyze_00000, 223)
	colID2FieldTypes := make(map[int64]*types.FieldType, len(e.colsInfo))
	if e.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 226)
		colID2FieldTypes[e.pkInfo.ID] = &e.pkInfo.FieldType
	}
	trace_util_0.Count(_analyze_00000, 224)
	for _, col := range e.colsInfo {
		trace_util_0.Count(_analyze_00000, 227)
		colID2FieldTypes[col.ID] = &col.FieldType
	}
	trace_util_0.Count(_analyze_00000, 225)
	return tablecodec.DecodeRow(sValue, colID2FieldTypes, e.ctx.GetSessionVars().Location())
}

func (e *AnalyzeFastExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	trace_util_0.Count(_analyze_00000, 228)
	val, ok := values[colInfo.ID]
	if !ok {
		trace_util_0.Count(_analyze_00000, 230)
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	trace_util_0.Count(_analyze_00000, 229)
	return val, nil
}

func (e *AnalyzeFastExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32, hasPKInfo int) (err error) {
	trace_util_0.Count(_analyze_00000, 231)
	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(sValue)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 237)
		return err
	}
	trace_util_0.Count(_analyze_00000, 232)
	var rowID int64
	rowID, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 238)
		return err
	}
	// Update the primary key collector.
	trace_util_0.Count(_analyze_00000, 233)
	if hasPKInfo > 0 {
		trace_util_0.Count(_analyze_00000, 239)
		v, ok := values[e.pkInfo.ID]
		if !ok {
			trace_util_0.Count(_analyze_00000, 242)
			var key int64
			_, key, err = tablecodec.DecodeRecordKey(sKey)
			if err != nil {
				trace_util_0.Count(_analyze_00000, 244)
				return err
			}
			trace_util_0.Count(_analyze_00000, 243)
			v = types.NewIntDatum(key)
		}
		trace_util_0.Count(_analyze_00000, 240)
		if e.collectors[0].Samples[samplePos] == nil {
			trace_util_0.Count(_analyze_00000, 245)
			e.collectors[0].Samples[samplePos] = &statistics.SampleItem{}
		}
		trace_util_0.Count(_analyze_00000, 241)
		e.collectors[0].Samples[samplePos].RowID = rowID
		e.collectors[0].Samples[samplePos].Value = v
	}
	// Update the columns' collectors.
	trace_util_0.Count(_analyze_00000, 234)
	for j, colInfo := range e.colsInfo {
		trace_util_0.Count(_analyze_00000, 246)
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 249)
			return err
		}
		trace_util_0.Count(_analyze_00000, 247)
		if e.collectors[hasPKInfo+j].Samples[samplePos] == nil {
			trace_util_0.Count(_analyze_00000, 250)
			e.collectors[hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		trace_util_0.Count(_analyze_00000, 248)
		e.collectors[hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[hasPKInfo+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	trace_util_0.Count(_analyze_00000, 235)
	for j, idxInfo := range e.idxsInfo {
		trace_util_0.Count(_analyze_00000, 251)
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			trace_util_0.Count(_analyze_00000, 255)
			for _, colInfo := range e.colsInfo {
				trace_util_0.Count(_analyze_00000, 256)
				if colInfo.Name == idxCol.Name {
					trace_util_0.Count(_analyze_00000, 257)
					v, err := e.getValueByInfo(colInfo, values)
					if err != nil {
						trace_util_0.Count(_analyze_00000, 259)
						return err
					}
					trace_util_0.Count(_analyze_00000, 258)
					idxVals = append(idxVals, v)
					break
				}
			}
		}
		trace_util_0.Count(_analyze_00000, 252)
		var bytes []byte
		bytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, bytes, idxVals...)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 260)
			return err
		}
		trace_util_0.Count(_analyze_00000, 253)
		if e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] == nil {
			trace_util_0.Count(_analyze_00000, 261)
			e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		trace_util_0.Count(_analyze_00000, 254)
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].Value = types.NewBytesDatum(bytes)
	}
	trace_util_0.Count(_analyze_00000, 236)
	return nil
}

func (e *AnalyzeFastExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	trace_util_0.Count(_analyze_00000, 262)
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	hasPKInfo := 0
	if e.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 265)
		hasPKInfo = 1
	}
	trace_util_0.Count(_analyze_00000, 263)
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		trace_util_0.Count(_analyze_00000, 266)
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos, hasPKInfo)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 268)
			return err
		}
		trace_util_0.Count(_analyze_00000, 267)
		samplePos++
	}
	trace_util_0.Count(_analyze_00000, 264)
	return nil
}

func (e *AnalyzeFastExec) handleScanIter(iter kv.Iterator) (err error) {
	trace_util_0.Count(_analyze_00000, 269)
	hasPKInfo := 0
	if e.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 272)
		hasPKInfo = 1
	}
	trace_util_0.Count(_analyze_00000, 270)
	rander := rand.New(rand.NewSource(e.randSeed + int64(e.rowCount)))
	for ; iter.Valid() && err == nil; err = iter.Next() {
		trace_util_0.Count(_analyze_00000, 273)
		// reservoir sampling
		e.rowCount++
		randNum := rander.Int63n(int64(e.rowCount))
		if randNum > int64(MaxSampleSize) && e.sampCursor == int32(MaxSampleSize) {
			trace_util_0.Count(_analyze_00000, 276)
			continue
		}

		trace_util_0.Count(_analyze_00000, 274)
		p := rander.Int31n(int32(MaxSampleSize))
		if e.sampCursor < int32(MaxSampleSize) {
			trace_util_0.Count(_analyze_00000, 277)
			p = e.sampCursor
			e.sampCursor++
		}

		trace_util_0.Count(_analyze_00000, 275)
		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p, hasPKInfo)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 278)
			return err
		}
	}
	trace_util_0.Count(_analyze_00000, 271)
	return err
}

func (e *AnalyzeFastExec) handleScanTasks(bo *tikv.Backoffer) error {
	trace_util_0.Count(_analyze_00000, 279)
	snapshot, err := e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 282)
		return err
	}
	trace_util_0.Count(_analyze_00000, 280)
	for _, t := range e.scanTasks {
		trace_util_0.Count(_analyze_00000, 283)
		iter, err := snapshot.Iter(t.StartKey, t.EndKey)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 285)
			return err
		}
		trace_util_0.Count(_analyze_00000, 284)
		err = e.handleScanIter(iter)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 286)
			return err
		}
	}
	trace_util_0.Count(_analyze_00000, 281)
	return nil
}

func (e *AnalyzeFastExec) handleSampTasks(bo *tikv.Backoffer, workID int, err *error) {
	trace_util_0.Count(_analyze_00000, 287)
	defer e.wg.Done()
	var snapshot kv.Snapshot
	snapshot, *err = e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	rander := rand.New(rand.NewSource(e.randSeed + int64(workID)))
	if *err != nil {
		trace_util_0.Count(_analyze_00000, 289)
		return
	}
	trace_util_0.Count(_analyze_00000, 288)
	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		trace_util_0.Count(_analyze_00000, 290)
		task := e.sampTasks[i]
		if task.SampSize == 0 {
			trace_util_0.Count(_analyze_00000, 296)
			continue
		}

		trace_util_0.Count(_analyze_00000, 291)
		var tableID, minRowID, maxRowID int64
		startKey, endKey := task.Location.StartKey, task.Location.EndKey
		tableID, minRowID, *err = tablecodec.DecodeRecordKey(startKey)
		if *err != nil {
			trace_util_0.Count(_analyze_00000, 297)
			return
		}
		trace_util_0.Count(_analyze_00000, 292)
		_, maxRowID, *err = tablecodec.DecodeRecordKey(endKey)
		if *err != nil {
			trace_util_0.Count(_analyze_00000, 298)
			return
		}

		trace_util_0.Count(_analyze_00000, 293)
		keys := make([]kv.Key, 0, task.SampSize)
		for i := 0; i < int(task.SampSize); i++ {
			trace_util_0.Count(_analyze_00000, 299)
			randKey := rander.Int63n(maxRowID-minRowID) + minRowID
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(tableID, randKey))
		}

		trace_util_0.Count(_analyze_00000, 294)
		kvMap := make(map[string][]byte, len(keys))
		for _, key := range keys {
			trace_util_0.Count(_analyze_00000, 300)
			var iter kv.Iterator
			iter, *err = snapshot.Iter(key, endKey)
			if *err != nil {
				trace_util_0.Count(_analyze_00000, 302)
				return
			}
			trace_util_0.Count(_analyze_00000, 301)
			kvMap[string(iter.Key())] = iter.Value()
		}

		trace_util_0.Count(_analyze_00000, 295)
		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			trace_util_0.Count(_analyze_00000, 303)
			return
		}
	}
}

func (e *AnalyzeFastExec) buildColumnStats(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	trace_util_0.Count(_analyze_00000, 304)
	data := make([][]byte, 0, len(collector.Samples))
	for i, sample := range collector.Samples {
		trace_util_0.Count(_analyze_00000, 306)
		sample.Ordinal = i
		if sample.Value.IsNull() {
			trace_util_0.Count(_analyze_00000, 309)
			collector.NullCount++
			continue
		}
		trace_util_0.Count(_analyze_00000, 307)
		bytes, err := tablecodec.EncodeValue(e.ctx.GetSessionVars().StmtCtx, sample.Value)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 310)
			return nil, nil, err
		}
		trace_util_0.Count(_analyze_00000, 308)
		data = append(data, bytes)
	}
	// Build CMSketch.
	trace_util_0.Count(_analyze_00000, 305)
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data, 20, uint64(rowCount))
	// Build Histogram.
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.maxNumBuckets), ID, collector, tp, rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) buildIndexStats(idxInfo *model.IndexInfo, collector *statistics.SampleCollector, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	trace_util_0.Count(_analyze_00000, 311)
	data := make([][][]byte, len(idxInfo.Columns), len(idxInfo.Columns))
	for _, sample := range collector.Samples {
		trace_util_0.Count(_analyze_00000, 314)
		var preLen int
		remained := sample.Value.GetBytes()
		// We need to insert each prefix values into CM Sketch.
		for i := 0; i < len(idxInfo.Columns); i++ {
			trace_util_0.Count(_analyze_00000, 315)
			var err error
			var value []byte
			value, remained, err = codec.CutOne(remained)
			if err != nil {
				trace_util_0.Count(_analyze_00000, 317)
				return nil, nil, err
			}
			trace_util_0.Count(_analyze_00000, 316)
			preLen += len(value)
			data[i] = append(data[i], sample.Value.GetBytes()[:preLen])
		}
	}
	trace_util_0.Count(_analyze_00000, 312)
	numTop := uint32(20)
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data[0], numTop, uint64(rowCount))
	// Build CM Sketch for each prefix and merge them into one.
	for i := 1; i < len(idxInfo.Columns); i++ {
		trace_util_0.Count(_analyze_00000, 318)
		var curCMSketch *statistics.CMSketch
		// `ndv` should be the ndv of full index, so just rewrite it here.
		curCMSketch, ndv, scaleRatio = statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data[i], numTop, uint64(rowCount))
		err := cmSketch.MergeCMSketch(curCMSketch, numTop)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 319)
			return nil, nil, err
		}
	}
	// Build Histogram.
	trace_util_0.Count(_analyze_00000, 313)
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.maxNumBuckets), idxInfo.ID, collector, types.NewFieldType(mysql.TypeBlob), rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, error) {
	trace_util_0.Count(_analyze_00000, 320)
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		trace_util_0.Count(_analyze_00000, 328)
		hasPKInfo = 1
	}
	// collect column samples and primary key samples and index samples.
	trace_util_0.Count(_analyze_00000, 321)
	length := len(e.colsInfo) + hasPKInfo + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		trace_util_0.Count(_analyze_00000, 329)
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(MaxSampleSize),
			Samples:       make([]*statistics.SampleItem, MaxSampleSize),
		}
	}

	trace_util_0.Count(_analyze_00000, 322)
	e.wg.Add(e.concurrency)
	bo := tikv.NewBackoffer(context.Background(), 500)
	for i := 0; i < e.concurrency; i++ {
		trace_util_0.Count(_analyze_00000, 330)
		go e.handleSampTasks(bo, i, &errs[i])
	}
	trace_util_0.Count(_analyze_00000, 323)
	e.wg.Wait()
	for _, err := range errs {
		trace_util_0.Count(_analyze_00000, 331)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 332)
			return nil, nil, err
		}
	}

	trace_util_0.Count(_analyze_00000, 324)
	err := e.handleScanTasks(bo)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 333)
		return nil, nil, err
	}

	trace_util_0.Count(_analyze_00000, 325)
	handle := domain.GetDomain(e.ctx).StatsHandle()
	tblStats := handle.GetTableStats(e.tblInfo)
	rowCount := int64(e.rowCount)
	if handle.Lease() > 0 && !tblStats.Pseudo {
		trace_util_0.Count(_analyze_00000, 334)
		rowCount = mathutil.MinInt64(tblStats.Count, rowCount)
	}
	// Adjust the row count in case the count of `tblStats` is not accurate and too small.
	trace_util_0.Count(_analyze_00000, 326)
	rowCount = mathutil.MaxInt64(rowCount, int64(e.sampCursor))
	hists, cms := make([]*statistics.Histogram, length), make([]*statistics.CMSketch, length)
	for i := 0; i < length; i++ {
		trace_util_0.Count(_analyze_00000, 335)
		// Build collector properties.
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool {
			trace_util_0.Count(_analyze_00000, 338)
			return collector.Samples[i].RowID < collector.Samples[j].RowID
		})
		trace_util_0.Count(_analyze_00000, 336)
		collector.CalcTotalSize()
		// Scale the total column size.
		collector.TotalSize *= rowCount / int64(len(collector.Samples))
		if i < hasPKInfo {
			trace_util_0.Count(_analyze_00000, 339)
			hists[i], cms[i], err = e.buildColumnStats(e.pkInfo.ID, e.collectors[i], &e.pkInfo.FieldType, rowCount)
		} else {
			trace_util_0.Count(_analyze_00000, 340)
			if i < hasPKInfo+len(e.colsInfo) {
				trace_util_0.Count(_analyze_00000, 341)
				hists[i], cms[i], err = e.buildColumnStats(e.colsInfo[i-hasPKInfo].ID, e.collectors[i], &e.colsInfo[i-hasPKInfo].FieldType, rowCount)
			} else {
				trace_util_0.Count(_analyze_00000, 342)
				{
					hists[i], cms[i], err = e.buildIndexStats(e.idxsInfo[i-hasPKInfo-len(e.colsInfo)], e.collectors[i], rowCount)
				}
			}
		}
		trace_util_0.Count(_analyze_00000, 337)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 343)
			return nil, nil, err
		}
	}
	trace_util_0.Count(_analyze_00000, 327)
	return hists, cms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	trace_util_0.Count(_analyze_00000, 344)
	// To set rand seed, it's for unit test.
	// To ensure that random sequences are different in non-test environments, RandSeed must be set time.Now().
	if RandSeed == 1 {
		trace_util_0.Count(_analyze_00000, 352)
		e.randSeed = time.Now().UnixNano()
	} else {
		trace_util_0.Count(_analyze_00000, 353)
		{
			e.randSeed = RandSeed
		}
	}
	trace_util_0.Count(_analyze_00000, 345)
	rander := rand.New(rand.NewSource(e.randSeed))

	// Only four rebuilds for sample task are allowed.
	needRebuild, maxBuildTimes := true, 5
	for counter := maxBuildTimes; needRebuild && counter > 0; counter-- {
		trace_util_0.Count(_analyze_00000, 354)
		needRebuild, err = e.buildSampTask()
		if err != nil {
			trace_util_0.Count(_analyze_00000, 355)
			return nil, nil, err
		}
	}
	trace_util_0.Count(_analyze_00000, 346)
	if needRebuild {
		trace_util_0.Count(_analyze_00000, 356)
		errMsg := "build fast analyze task failed, exceed maxBuildTimes: %v"
		return nil, nil, errors.Errorf(errMsg, maxBuildTimes)
	}

	trace_util_0.Count(_analyze_00000, 347)
	defer e.job.Update(int64(e.rowCount))

	// If total row count of the table is smaller than 2*MaxSampleSize, we
	// translate all the sample tasks to scan tasks.
	if e.rowCount < uint64(MaxSampleSize)*2 {
		trace_util_0.Count(_analyze_00000, 357)
		for _, task := range e.sampTasks {
			trace_util_0.Count(_analyze_00000, 359)
			e.scanTasks = append(e.scanTasks, task.Location)
		}
		trace_util_0.Count(_analyze_00000, 358)
		e.sampTasks = e.sampTasks[:0]
		e.rowCount = 0
		return e.runTasks()
	}

	trace_util_0.Count(_analyze_00000, 348)
	randPos := make([]uint64, 0, MaxSampleSize+1)
	for i := 0; i < MaxSampleSize; i++ {
		trace_util_0.Count(_analyze_00000, 360)
		randPos = append(randPos, uint64(rander.Int63n(int64(e.rowCount))))
	}
	trace_util_0.Count(_analyze_00000, 349)
	sort.Slice(randPos, func(i, j int) bool { trace_util_0.Count(_analyze_00000, 361); return randPos[i] < randPos[j] })

	trace_util_0.Count(_analyze_00000, 350)
	for _, task := range e.sampTasks {
		trace_util_0.Count(_analyze_00000, 362)
		begin := sort.Search(len(randPos), func(i int) bool { trace_util_0.Count(_analyze_00000, 365); return randPos[i] >= task.BeginOffset })
		trace_util_0.Count(_analyze_00000, 363)
		end := sort.Search(len(randPos), func(i int) bool { trace_util_0.Count(_analyze_00000, 366); return randPos[i] >= task.EndOffset })
		trace_util_0.Count(_analyze_00000, 364)
		task.SampSize = uint64(end - begin)
	}
	trace_util_0.Count(_analyze_00000, 351)
	return e.runTasks()
}

// AnalyzeTestFastExec is for fast sample in unit test.
type AnalyzeTestFastExec struct {
	AnalyzeFastExec
	Ctx             sessionctx.Context
	PhysicalTableID int64
	PKInfo          *model.ColumnInfo
	ColsInfo        []*model.ColumnInfo
	IdxsInfo        []*model.IndexInfo
	Concurrency     int
	Collectors      []*statistics.SampleCollector
	TblInfo         *model.TableInfo
}

// TestFastSample only test the fast sample in unit test.
func (e *AnalyzeTestFastExec) TestFastSample() error {
	trace_util_0.Count(_analyze_00000, 367)
	e.ctx = e.Ctx
	e.pkInfo = e.PKInfo
	e.colsInfo = e.ColsInfo
	e.idxsInfo = e.IdxsInfo
	e.concurrency = e.Concurrency
	e.physicalTableID = e.PhysicalTableID
	e.wg = &sync.WaitGroup{}
	e.job = &statistics.AnalyzeJob{}
	e.tblInfo = e.TblInfo
	_, _, err := e.buildStats()
	e.Collectors = e.collectors
	return err
}

type analyzeIndexIncrementalExec struct {
	AnalyzeIndexExec
	oldHist *statistics.Histogram
	oldCMS  *statistics.CMSketch
}

func analyzeIndexIncremental(idxExec *analyzeIndexIncrementalExec) analyzeResult {
	trace_util_0.Count(_analyze_00000, 368)
	startPos := idxExec.oldHist.GetUpper(idxExec.oldHist.Len() - 1)
	values, err := codec.DecodeRange(startPos.GetBytes(), len(idxExec.idxInfo.Columns))
	if err != nil {
		trace_util_0.Count(_analyze_00000, 374)
		return analyzeResult{Err: err, job: idxExec.job}
	}
	trace_util_0.Count(_analyze_00000, 369)
	ran := ranger.Range{LowVal: values, HighVal: []types.Datum{types.MaxValueDatum()}}
	hist, cms, err := idxExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		trace_util_0.Count(_analyze_00000, 375)
		return analyzeResult{Err: err, job: idxExec.job}
	}
	trace_util_0.Count(_analyze_00000, 370)
	hist, err = statistics.MergeHistograms(idxExec.ctx.GetSessionVars().StmtCtx, idxExec.oldHist, hist, int(idxExec.maxNumBuckets))
	if err != nil {
		trace_util_0.Count(_analyze_00000, 376)
		return analyzeResult{Err: err, job: idxExec.job}
	}
	trace_util_0.Count(_analyze_00000, 371)
	if idxExec.oldCMS != nil && cms != nil {
		trace_util_0.Count(_analyze_00000, 377)
		err = cms.MergeCMSketch4IncrementalAnalyze(idxExec.oldCMS)
		if err != nil {
			trace_util_0.Count(_analyze_00000, 378)
			return analyzeResult{Err: err, job: idxExec.job}
		}
	}
	trace_util_0.Count(_analyze_00000, 372)
	result := analyzeResult{
		PhysicalTableID: idxExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{cms},
		IsIndex:         1,
		job:             idxExec.job,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		trace_util_0.Count(_analyze_00000, 379)
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	trace_util_0.Count(_analyze_00000, 373)
	return result
}

type analyzePKIncrementalExec struct {
	AnalyzeColumnsExec
	oldHist *statistics.Histogram
}

func analyzePKIncremental(colExec *analyzePKIncrementalExec) analyzeResult {
	trace_util_0.Count(_analyze_00000, 380)
	var maxVal types.Datum
	if mysql.HasUnsignedFlag(colExec.pkInfo.Flag) {
		trace_util_0.Count(_analyze_00000, 385)
		maxVal = types.NewUintDatum(math.MaxUint64)
	} else {
		trace_util_0.Count(_analyze_00000, 386)
		{
			maxVal = types.NewIntDatum(math.MaxInt64)
		}
	}
	trace_util_0.Count(_analyze_00000, 381)
	startPos := *colExec.oldHist.GetUpper(colExec.oldHist.Len() - 1)
	ran := ranger.Range{LowVal: []types.Datum{startPos}, LowExclude: true, HighVal: []types.Datum{maxVal}}
	hists, _, err := colExec.buildStats([]*ranger.Range{&ran})
	if err != nil {
		trace_util_0.Count(_analyze_00000, 387)
		return analyzeResult{Err: err, job: colExec.job}
	}
	trace_util_0.Count(_analyze_00000, 382)
	hist := hists[0]
	hist, err = statistics.MergeHistograms(colExec.ctx.GetSessionVars().StmtCtx, colExec.oldHist, hist, int(colExec.maxNumBuckets))
	if err != nil {
		trace_util_0.Count(_analyze_00000, 388)
		return analyzeResult{Err: err, job: colExec.job}
	}
	trace_util_0.Count(_analyze_00000, 383)
	result := analyzeResult{
		PhysicalTableID: colExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{nil},
		job:             colExec.job,
	}
	if hist.Len() > 0 {
		trace_util_0.Count(_analyze_00000, 389)
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	trace_util_0.Count(_analyze_00000, 384)
	return result
}

// analyzeResult is used to represent analyze result.
type analyzeResult struct {
	// PhysicalTableID is the id of a partition or a table.
	PhysicalTableID int64
	Hist            []*statistics.Histogram
	Cms             []*statistics.CMSketch
	Count           int64
	IsIndex         int
	Err             error
	job             *statistics.AnalyzeJob
}

var _analyze_00000 = "executor/analyze.go"
