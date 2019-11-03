// Copyright 2019 PingCAP, Inc.
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
	"encoding/binary"
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	tableInfo  *model.TableInfo
	indexInfo  *model.IndexInfo
	lower      []types.Datum
	upper      []types.Datum
	num        int
	valueLists [][]types.Datum
}

type splitableStore interface {
	SplitRegionAndScatter(splitKey kv.Key) (uint64, error)
	WaitScatterRegionFinish(regionID uint64) error
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	trace_util_0.Count(_split_00000, 0)
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		trace_util_0.Count(_split_00000, 6)
		return nil
	}
	trace_util_0.Count(_split_00000, 1)
	splitIdxKeys, err := e.getSplitIdxKeys()
	if err != nil {
		trace_util_0.Count(_split_00000, 7)
		return err
	}
	trace_util_0.Count(_split_00000, 2)
	regionIDs := make([]uint64, 0, len(splitIdxKeys))
	for _, idxKey := range splitIdxKeys {
		trace_util_0.Count(_split_00000, 8)
		regionID, err := s.SplitRegionAndScatter(idxKey)
		if err != nil {
			trace_util_0.Count(_split_00000, 10)
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
			continue
		}
		trace_util_0.Count(_split_00000, 9)
		regionIDs = append(regionIDs, regionID)

	}
	trace_util_0.Count(_split_00000, 3)
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		trace_util_0.Count(_split_00000, 11)
		return nil
	}
	trace_util_0.Count(_split_00000, 4)
	for _, regionID := range regionIDs {
		trace_util_0.Count(_split_00000, 12)
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			trace_util_0.Count(_split_00000, 13)
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
		}
	}
	trace_util_0.Count(_split_00000, 5)
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	trace_util_0.Count(_split_00000, 14)
	var idxKeys [][]byte
	if e.num > 0 {
		trace_util_0.Count(_split_00000, 20)
		idxKeys = make([][]byte, 0, e.num)
	} else {
		trace_util_0.Count(_split_00000, 21)
		{
			idxKeys = make([][]byte, 0, len(e.valueLists)+1)
		}
	}
	// Split in the start of the index key.
	trace_util_0.Count(_split_00000, 15)
	startIdxKey := tablecodec.EncodeTableIndexPrefix(e.tableInfo.ID, e.indexInfo.ID)
	idxKeys = append(idxKeys, startIdxKey)

	index := tables.NewIndex(e.tableInfo.ID, e.tableInfo, e.indexInfo)
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		trace_util_0.Count(_split_00000, 22)
		for _, v := range e.valueLists {
			trace_util_0.Count(_split_00000, 24)
			idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, v, math.MinInt64, nil)
			if err != nil {
				trace_util_0.Count(_split_00000, 26)
				return nil, err
			}
			trace_util_0.Count(_split_00000, 25)
			idxKeys = append(idxKeys, idxKey)
		}
		trace_util_0.Count(_split_00000, 23)
		return idxKeys, nil
	}
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	trace_util_0.Count(_split_00000, 16)
	lowerIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.lower, math.MinInt64, nil)
	if err != nil {
		trace_util_0.Count(_split_00000, 27)
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	trace_util_0.Count(_split_00000, 17)
	upperIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.upper, math.MinInt64, nil)
	if err != nil {
		trace_util_0.Count(_split_00000, 28)
		return nil, err
	}
	trace_util_0.Count(_split_00000, 18)
	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		trace_util_0.Count(_split_00000, 29)
		lowerStr, err1 := datumSliceToString(e.lower)
		upperStr, err2 := datumSliceToString(e.upper)
		if err1 != nil || err2 != nil {
			trace_util_0.Count(_split_00000, 31)
			return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, e.lower, e.upper)
		}
		trace_util_0.Count(_split_00000, 30)
		return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, lowerStr, upperStr)
	}
	trace_util_0.Count(_split_00000, 19)
	return getValuesList(lowerIdxKey, upperIdxKey, e.num, idxKeys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// Then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper,lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(lower, upper []byte, num int, valuesList [][]byte) [][]byte {
	trace_util_0.Count(_split_00000, 32)
	commonPrefixIdx := longestCommonPrefixLen(lower, upper)
	step := getStepValue(lower[commonPrefixIdx:], upper[commonPrefixIdx:], num)
	startV := getUint64FromBytes(lower[commonPrefixIdx:], 0)
	// To get `num` regions, only need to split `num-1` idx keys.
	buf := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		trace_util_0.Count(_split_00000, 34)
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, lower[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(buf, startV)
		value = append(value, buf...)
		valuesList = append(valuesList, value)
	}
	trace_util_0.Count(_split_00000, 33)
	return valuesList
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	trace_util_0.Count(_split_00000, 35)
	l := mathutil.Min(len(s1), len(s2))
	i := 0
	for ; i < l; i++ {
		trace_util_0.Count(_split_00000, 37)
		if s1[i] != s2[i] {
			trace_util_0.Count(_split_00000, 38)
			break
		}
	}
	trace_util_0.Count(_split_00000, 36)
	return i
}

// getStepValue gets the step of between the lower and upper value. step = (upper-lower)/num.
// Convert byte slice to uint64 first.
func getStepValue(lower, upper []byte, num int) uint64 {
	trace_util_0.Count(_split_00000, 39)
	lowerUint := getUint64FromBytes(lower, 0)
	upperUint := getUint64FromBytes(upper, 0xff)
	return (upperUint - lowerUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	trace_util_0.Count(_split_00000, 40)
	buf := bs
	if len(buf) < 8 {
		trace_util_0.Count(_split_00000, 42)
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			trace_util_0.Count(_split_00000, 43)
			buf = append(buf, pad)
		}
	}
	trace_util_0.Count(_split_00000, 41)
	return binary.BigEndian.Uint64(buf)
}

func datumSliceToString(ds []types.Datum) (string, error) {
	trace_util_0.Count(_split_00000, 44)
	str := "("
	for i, d := range ds {
		trace_util_0.Count(_split_00000, 46)
		s, err := d.ToString()
		if err != nil {
			trace_util_0.Count(_split_00000, 49)
			return str, err
		}
		trace_util_0.Count(_split_00000, 47)
		if i > 0 {
			trace_util_0.Count(_split_00000, 50)
			str += ","
		}
		trace_util_0.Count(_split_00000, 48)
		str += s
	}
	trace_util_0.Count(_split_00000, 45)
	str += ")"
	return str, nil
}

// SplitTableRegionExec represents a split table regions executor.
type SplitTableRegionExec struct {
	baseExecutor

	tableInfo  *model.TableInfo
	lower      types.Datum
	upper      types.Datum
	num        int
	valueLists [][]types.Datum
}

// Next implements the Executor Next interface.
func (e *SplitTableRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	trace_util_0.Count(_split_00000, 51)
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		trace_util_0.Count(_split_00000, 57)
		return nil
	}
	trace_util_0.Count(_split_00000, 52)
	splitKeys, err := e.getSplitTableKeys()
	if err != nil {
		trace_util_0.Count(_split_00000, 58)
		return err
	}
	trace_util_0.Count(_split_00000, 53)
	regionIDs := make([]uint64, 0, len(splitKeys))
	for _, key := range splitKeys {
		trace_util_0.Count(_split_00000, 59)
		regionID, err := s.SplitRegionAndScatter(key)
		if err != nil {
			trace_util_0.Count(_split_00000, 61)
			logutil.Logger(context.Background()).Warn("split table region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.Error(err))
			continue
		}
		trace_util_0.Count(_split_00000, 60)
		regionIDs = append(regionIDs, regionID)
	}
	trace_util_0.Count(_split_00000, 54)
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		trace_util_0.Count(_split_00000, 62)
		return nil
	}
	trace_util_0.Count(_split_00000, 55)
	for _, regionID := range regionIDs {
		trace_util_0.Count(_split_00000, 63)
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			trace_util_0.Count(_split_00000, 64)
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.Error(err))
		}
	}
	trace_util_0.Count(_split_00000, 56)
	return nil
}

var minRegionStepValue = uint64(1000)

func (e *SplitTableRegionExec) getSplitTableKeys() ([][]byte, error) {
	trace_util_0.Count(_split_00000, 65)
	var keys [][]byte
	if e.num > 0 {
		trace_util_0.Count(_split_00000, 72)
		keys = make([][]byte, 0, e.num)
	} else {
		trace_util_0.Count(_split_00000, 73)
		{
			keys = make([][]byte, 0, len(e.valueLists))
		}
	}
	trace_util_0.Count(_split_00000, 66)
	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	if len(e.valueLists) > 0 {
		trace_util_0.Count(_split_00000, 74)
		for _, v := range e.valueLists {
			trace_util_0.Count(_split_00000, 76)
			key := tablecodec.EncodeRecordKey(recordPrefix, v[0].GetInt64())
			keys = append(keys, key)
		}
		trace_util_0.Count(_split_00000, 75)
		return keys, nil
	}
	trace_util_0.Count(_split_00000, 67)
	isUnsigned := false
	if e.tableInfo.PKIsHandle {
		trace_util_0.Count(_split_00000, 77)
		if pkCol := e.tableInfo.GetPkColInfo(); pkCol != nil {
			trace_util_0.Count(_split_00000, 78)
			isUnsigned = mysql.HasUnsignedFlag(pkCol.Flag)
		}
	}
	trace_util_0.Count(_split_00000, 68)
	var step uint64
	var lowerValue int64
	if isUnsigned {
		trace_util_0.Count(_split_00000, 79)
		lowerRecordID := e.lower.GetUint64()
		upperRecordID := e.upper.GetUint64()
		if upperRecordID <= lowerRecordID {
			trace_util_0.Count(_split_00000, 81)
			return nil, errors.Errorf("Split table `%s` region lower value %v should less than the upper value %v", e.tableInfo.Name, lowerRecordID, upperRecordID)
		}
		trace_util_0.Count(_split_00000, 80)
		step = (upperRecordID - lowerRecordID) / uint64(e.num)
		lowerValue = int64(lowerRecordID)
	} else {
		trace_util_0.Count(_split_00000, 82)
		{
			lowerRecordID := e.lower.GetInt64()
			upperRecordID := e.upper.GetInt64()
			if upperRecordID <= lowerRecordID {
				trace_util_0.Count(_split_00000, 84)
				return nil, errors.Errorf("Split table `%s` region lower value %v should less than the upper value %v", e.tableInfo.Name, lowerRecordID, upperRecordID)
			}
			trace_util_0.Count(_split_00000, 83)
			step = uint64(upperRecordID-lowerRecordID) / uint64(e.num)
			lowerValue = lowerRecordID
		}
	}
	trace_util_0.Count(_split_00000, 69)
	if step < minRegionStepValue {
		trace_util_0.Count(_split_00000, 85)
		return nil, errors.Errorf("Split table `%s` region step value should more than %v, step %v is invalid", e.tableInfo.Name, minRegionStepValue, step)
	}

	trace_util_0.Count(_split_00000, 70)
	recordID := lowerValue
	for i := 1; i < e.num; i++ {
		trace_util_0.Count(_split_00000, 86)
		recordID += int64(step)
		key := tablecodec.EncodeRecordKey(recordPrefix, recordID)
		keys = append(keys, key)
	}
	trace_util_0.Count(_split_00000, 71)
	return keys, nil
}

var _split_00000 = "executor/split.go"
