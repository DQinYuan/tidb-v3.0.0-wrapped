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
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/atomic"
)

const (
	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	pseudoRowCount    = 10000
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40

	outOfRangeBetweenRate = 100
)

// PseudoVersion means the pseudo statistics version is 0.
const PseudoVersion uint64 = 0

// Table represents statistics for a table.
type Table struct {
	HistColl
	Version uint64
	Name    string
}

// HistColl is a collection of histogram. It collects enough information for plan to calculate the selectivity.
type HistColl struct {
	PhysicalID int64
	// HavePhysicalID is true means this HistColl is from single table and have its ID's information.
	// The physical id is used when try to load column stats from storage.
	HavePhysicalID bool
	Columns        map[int64]*Column
	Indices        map[int64]*Index
	// Idx2ColumnIDs maps the index id to its column ids. It's used to calculate the selectivity in planner.
	Idx2ColumnIDs map[int64][]int64
	// ColID2IdxID maps the column id to index id whose first column is it. It's used to calculate the selectivity in planner.
	ColID2IdxID map[int64]int64
	Pseudo      bool
	Count       int64
	ModifyCount int64 // Total modify count in a table.
}

// Copy copies the current table.
func (t *Table) Copy() *Table {
	trace_util_0.Count(_table_00000, 0)
	newHistColl := HistColl{
		PhysicalID:     t.PhysicalID,
		HavePhysicalID: t.HavePhysicalID,
		Count:          t.Count,
		Columns:        make(map[int64]*Column),
		Indices:        make(map[int64]*Index),
		Pseudo:         t.Pseudo,
		ModifyCount:    t.ModifyCount,
	}
	for id, col := range t.Columns {
		trace_util_0.Count(_table_00000, 3)
		newHistColl.Columns[id] = col
	}
	trace_util_0.Count(_table_00000, 1)
	for id, idx := range t.Indices {
		trace_util_0.Count(_table_00000, 4)
		newHistColl.Indices[id] = idx
	}
	trace_util_0.Count(_table_00000, 2)
	nt := &Table{
		HistColl: newHistColl,
		Version:  t.Version,
		Name:     t.Name,
	}
	return nt
}

// String implements Stringer interface.
func (t *Table) String() string {
	trace_util_0.Count(_table_00000, 5)
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d Count:%d", t.PhysicalID, t.Count))
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		trace_util_0.Count(_table_00000, 12)
		cols = append(cols, col)
	}
	trace_util_0.Count(_table_00000, 6)
	sort.Slice(cols, func(i, j int) bool { trace_util_0.Count(_table_00000, 13); return cols[i].ID < cols[j].ID })
	trace_util_0.Count(_table_00000, 7)
	for _, col := range cols {
		trace_util_0.Count(_table_00000, 14)
		strs = append(strs, col.String())
	}
	trace_util_0.Count(_table_00000, 8)
	idxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		trace_util_0.Count(_table_00000, 15)
		idxs = append(idxs, idx)
	}
	trace_util_0.Count(_table_00000, 9)
	sort.Slice(idxs, func(i, j int) bool { trace_util_0.Count(_table_00000, 16); return idxs[i].ID < idxs[j].ID })
	trace_util_0.Count(_table_00000, 10)
	for _, idx := range idxs {
		trace_util_0.Count(_table_00000, 17)
		strs = append(strs, idx.String())
	}
	trace_util_0.Count(_table_00000, 11)
	return strings.Join(strs, "\n")
}

// IndexStartWithColumn finds the first index whose first column is the given column.
func (t *Table) IndexStartWithColumn(colName string) *Index {
	trace_util_0.Count(_table_00000, 18)
	for _, index := range t.Indices {
		trace_util_0.Count(_table_00000, 20)
		if index.Info.Columns[0].Name.L == colName {
			trace_util_0.Count(_table_00000, 21)
			return index
		}
	}
	trace_util_0.Count(_table_00000, 19)
	return nil
}

// ColumnByName finds the statistics.Column for the given column.
func (t *Table) ColumnByName(colName string) *Column {
	trace_util_0.Count(_table_00000, 22)
	for _, c := range t.Columns {
		trace_util_0.Count(_table_00000, 24)
		if c.Info.Name.L == colName {
			trace_util_0.Count(_table_00000, 25)
			return c
		}
	}
	trace_util_0.Count(_table_00000, 23)
	return nil
}

type tableColumnID struct {
	TableID  int64
	ColumnID int64
}

type neededColumnMap struct {
	m    sync.Mutex
	cols map[tableColumnID]struct{}
}

func (n *neededColumnMap) AllCols() []tableColumnID {
	trace_util_0.Count(_table_00000, 26)
	n.m.Lock()
	keys := make([]tableColumnID, 0, len(n.cols))
	for key := range n.cols {
		trace_util_0.Count(_table_00000, 28)
		keys = append(keys, key)
	}
	trace_util_0.Count(_table_00000, 27)
	n.m.Unlock()
	return keys
}

func (n *neededColumnMap) insert(col tableColumnID) {
	trace_util_0.Count(_table_00000, 29)
	n.m.Lock()
	n.cols[col] = struct{}{}
	n.m.Unlock()
}

func (n *neededColumnMap) Delete(col tableColumnID) {
	trace_util_0.Count(_table_00000, 30)
	n.m.Lock()
	delete(n.cols, col)
	n.m.Unlock()
}

// RatioOfPseudoEstimate means if modifyCount / statsTblCount is greater than this ratio, we think the stats is invalid
// and use pseudo estimation.
var RatioOfPseudoEstimate = atomic.NewFloat64(0.7)

// IsOutdated returns true if the table stats is outdated.
func (t *Table) IsOutdated() bool {
	trace_util_0.Count(_table_00000, 31)
	if t.Count > 0 && float64(t.ModifyCount)/float64(t.Count) > RatioOfPseudoEstimate.Load() {
		trace_util_0.Count(_table_00000, 33)
		return true
	}
	trace_util_0.Count(_table_00000, 32)
	return false
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	trace_util_0.Count(_table_00000, 34)
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		trace_util_0.Count(_table_00000, 36)
		return float64(t.Count) / pseudoLessRate
	}
	trace_util_0.Count(_table_00000, 35)
	return c.greaterRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnLessRowCount estimates the row count where the column less than value. Note that null values are not counted.
func (t *Table) ColumnLessRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	trace_util_0.Count(_table_00000, 37)
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		trace_util_0.Count(_table_00000, 39)
		return float64(t.Count) / pseudoLessRate
	}
	trace_util_0.Count(_table_00000, 38)
	return c.lessRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *stmtctx.StatementContext, a, b types.Datum, colID int64) float64 {
	trace_util_0.Count(_table_00000, 40)
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		trace_util_0.Count(_table_00000, 43)
		return float64(t.Count) / pseudoBetweenRate
	}
	trace_util_0.Count(_table_00000, 41)
	count := c.BetweenRowCount(a, b)
	if a.IsNull() {
		trace_util_0.Count(_table_00000, 44)
		count += float64(c.NullCount)
	}
	trace_util_0.Count(_table_00000, 42)
	return count * c.GetIncreaseFactor(t.Count)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) (float64, error) {
	trace_util_0.Count(_table_00000, 45)
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		trace_util_0.Count(_table_00000, 47)
		return float64(t.Count) / pseudoEqualRate, nil
	}
	trace_util_0.Count(_table_00000, 46)
	result, err := c.equalRowCount(sc, value, t.ModifyCount)
	result *= c.GetIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (coll *HistColl) GetRowCountByIntColumnRanges(sc *stmtctx.StatementContext, colID int64, intRanges []*ranger.Range) (float64, error) {
	trace_util_0.Count(_table_00000, 48)
	c, ok := coll.Columns[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		trace_util_0.Count(_table_00000, 50)
		if len(intRanges) == 0 {
			trace_util_0.Count(_table_00000, 53)
			return 0, nil
		}
		trace_util_0.Count(_table_00000, 51)
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			trace_util_0.Count(_table_00000, 54)
			return getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.Count)), nil
		}
		trace_util_0.Count(_table_00000, 52)
		return getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.Count)), nil
	}
	trace_util_0.Count(_table_00000, 49)
	result, err := c.GetColumnRowCount(sc, intRanges, coll.ModifyCount)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByColumnRanges(sc *stmtctx.StatementContext, colID int64, colRanges []*ranger.Range) (float64, error) {
	trace_util_0.Count(_table_00000, 55)
	c, ok := coll.Columns[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		trace_util_0.Count(_table_00000, 57)
		return GetPseudoRowCountByColumnRanges(sc, float64(coll.Count), colRanges, 0)
	}
	trace_util_0.Count(_table_00000, 56)
	result, err := c.GetColumnRowCount(sc, colRanges, coll.ModifyCount)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByIndexRanges(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	trace_util_0.Count(_table_00000, 58)
	idx := coll.Indices[idxID]
	if idx == nil || idx.IsInvalid(coll.Pseudo) {
		trace_util_0.Count(_table_00000, 61)
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			trace_util_0.Count(_table_00000, 63)
			colsLen = len(idx.Info.Columns)
		}
		trace_util_0.Count(_table_00000, 62)
		return getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.Count), colsLen)
	}
	trace_util_0.Count(_table_00000, 59)
	var result float64
	var err error
	if idx.CMSketch != nil && idx.StatsVer == Version1 {
		trace_util_0.Count(_table_00000, 64)
		result, err = coll.getIndexRowCount(sc, idxID, indexRanges)
	} else {
		trace_util_0.Count(_table_00000, 65)
		{
			result, err = idx.GetRowCount(sc, indexRanges, coll.ModifyCount)
		}
	}
	trace_util_0.Count(_table_00000, 60)
	result *= idx.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// PseudoAvgCountPerValue gets a pseudo average count if histogram not exists.
func (t *Table) PseudoAvgCountPerValue() float64 {
	trace_util_0.Count(_table_00000, 66)
	return float64(t.Count) / pseudoEqualRate
}

// GetOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func GetOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	trace_util_0.Count(_table_00000, 67)
	for i := range ran.LowVal {
		trace_util_0.Count(_table_00000, 69)
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.CompareDatum(sc, &b)
		if err != nil {
			trace_util_0.Count(_table_00000, 71)
			return 0
		}
		trace_util_0.Count(_table_00000, 70)
		if cmp != 0 {
			trace_util_0.Count(_table_00000, 72)
			return i
		}
	}
	trace_util_0.Count(_table_00000, 68)
	return len(ran.LowVal)
}

// GenerateHistCollFromColumnInfo generates a new HistColl whose ColID2IdxID and IdxID2ColIDs is built from the given parameter.
func (coll *HistColl) GenerateHistCollFromColumnInfo(infos []*model.ColumnInfo, columns []*expression.Column) *HistColl {
	trace_util_0.Count(_table_00000, 73)
	newColHistMap := make(map[int64]*Column)
	colInfoID2UniqueID := make(map[int64]int64)
	colNames2UniqueID := make(map[string]int64)
	for _, col := range columns {
		trace_util_0.Count(_table_00000, 78)
		colInfoID2UniqueID[col.ID] = col.UniqueID
	}
	trace_util_0.Count(_table_00000, 74)
	for _, colInfo := range infos {
		trace_util_0.Count(_table_00000, 79)
		uniqueID, ok := colInfoID2UniqueID[colInfo.ID]
		if ok {
			trace_util_0.Count(_table_00000, 80)
			colNames2UniqueID[colInfo.Name.L] = uniqueID
		}
	}
	trace_util_0.Count(_table_00000, 75)
	for id, colHist := range coll.Columns {
		trace_util_0.Count(_table_00000, 81)
		uniqueID, ok := colInfoID2UniqueID[id]
		// Collect the statistics by the given columns.
		if ok {
			trace_util_0.Count(_table_00000, 82)
			newColHistMap[uniqueID] = colHist
		}
	}
	trace_util_0.Count(_table_00000, 76)
	newIdxHistMap := make(map[int64]*Index)
	idx2Columns := make(map[int64][]int64)
	colID2IdxID := make(map[int64]int64)
	for _, idxHist := range coll.Indices {
		trace_util_0.Count(_table_00000, 83)
		ids := make([]int64, 0, len(idxHist.Info.Columns))
		for _, idxCol := range idxHist.Info.Columns {
			trace_util_0.Count(_table_00000, 86)
			uniqueID, ok := colNames2UniqueID[idxCol.Name.L]
			if !ok {
				trace_util_0.Count(_table_00000, 88)
				break
			}
			trace_util_0.Count(_table_00000, 87)
			ids = append(ids, uniqueID)
		}
		// If the length of the id list is 0, this index won't be used in this query.
		trace_util_0.Count(_table_00000, 84)
		if len(ids) == 0 {
			trace_util_0.Count(_table_00000, 89)
			continue
		}
		trace_util_0.Count(_table_00000, 85)
		colID2IdxID[ids[0]] = idxHist.ID
		newIdxHistMap[idxHist.ID] = idxHist
		idx2Columns[idxHist.ID] = ids
	}
	trace_util_0.Count(_table_00000, 77)
	newColl := &HistColl{
		PhysicalID:     coll.PhysicalID,
		HavePhysicalID: coll.HavePhysicalID,
		Pseudo:         coll.Pseudo,
		Count:          coll.Count,
		ModifyCount:    coll.ModifyCount,
		Columns:        newColHistMap,
		Indices:        newIdxHistMap,
		ColID2IdxID:    colID2IdxID,
		Idx2ColumnIDs:  idx2Columns,
	}
	return newColl
}

// isSingleColIdxNullRange checks if a range is [NULL, NULL] on a single-column index.
func isSingleColIdxNullRange(idx *Index, ran *ranger.Range) bool {
	trace_util_0.Count(_table_00000, 90)
	if len(idx.Info.Columns) > 1 {
		trace_util_0.Count(_table_00000, 93)
		return false
	}
	trace_util_0.Count(_table_00000, 91)
	l, h := ran.LowVal[0], ran.HighVal[0]
	if l.IsNull() && h.IsNull() {
		trace_util_0.Count(_table_00000, 94)
		return true
	}
	trace_util_0.Count(_table_00000, 92)
	return false
}

func (coll *HistColl) getIndexRowCount(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	trace_util_0.Count(_table_00000, 95)
	idx := coll.Indices[idxID]
	totalCount := float64(0)
	for _, ran := range indexRanges {
		trace_util_0.Count(_table_00000, 98)
		rangePosition := GetOrdinalOfRangeCond(sc, ran)
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleColIdxNullRange(idx, ran) {
			trace_util_0.Count(_table_00000, 103)
			count, err := idx.GetRowCount(sc, []*ranger.Range{ran}, coll.ModifyCount)
			if err != nil {
				trace_util_0.Count(_table_00000, 105)
				return 0, errors.Trace(err)
			}
			trace_util_0.Count(_table_00000, 104)
			totalCount += count
			continue
		}
		trace_util_0.Count(_table_00000, 99)
		var selectivity float64
		// use CM Sketch to estimate the equal conditions
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			trace_util_0.Count(_table_00000, 106)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_table_00000, 100)
		val := types.NewBytesDatum(bytes)
		if idx.outOfRange(val) {
			trace_util_0.Count(_table_00000, 107)
			// When the value is out of range, we could not found this value in the CM Sketch,
			// so we use heuristic methods to estimate the selectivity.
			if idx.NDV > 0 && len(ran.LowVal) == len(idx.Info.Columns) && rangePosition == len(ran.LowVal) {
				trace_util_0.Count(_table_00000, 108)
				// for equality queries
				selectivity = float64(coll.ModifyCount) / float64(idx.NDV) / idx.TotalRowCount()
			} else {
				trace_util_0.Count(_table_00000, 109)
				{
					// for range queries
					selectivity = float64(coll.ModifyCount) / outOfRangeBetweenRate / idx.TotalRowCount()
				}
			}
		} else {
			trace_util_0.Count(_table_00000, 110)
			{
				selectivity = float64(idx.CMSketch.QueryBytes(bytes)) / float64(idx.TotalRowCount())
			}
		}
		// use histogram to estimate the range condition
		trace_util_0.Count(_table_00000, 101)
		if rangePosition != len(ran.LowVal) {
			trace_util_0.Count(_table_00000, 111)
			rang := ranger.Range{
				LowVal:      []types.Datum{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Datum{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
			}
			var count float64
			var err error
			colIDs := coll.Idx2ColumnIDs[idxID]
			var colID int64
			if rangePosition >= len(colIDs) {
				trace_util_0.Count(_table_00000, 115)
				colID = -1
			} else {
				trace_util_0.Count(_table_00000, 116)
				{
					colID = colIDs[rangePosition]
				}
			}
			// prefer index stats over column stats
			trace_util_0.Count(_table_00000, 112)
			if idx, ok := coll.ColID2IdxID[colID]; ok {
				trace_util_0.Count(_table_00000, 117)
				count, err = coll.GetRowCountByIndexRanges(sc, idx, []*ranger.Range{&rang})
			} else {
				trace_util_0.Count(_table_00000, 118)
				{
					count, err = coll.GetRowCountByColumnRanges(sc, colID, []*ranger.Range{&rang})
				}
			}
			trace_util_0.Count(_table_00000, 113)
			if err != nil {
				trace_util_0.Count(_table_00000, 119)
				return 0, errors.Trace(err)
			}
			trace_util_0.Count(_table_00000, 114)
			selectivity = selectivity * count / float64(idx.TotalRowCount())
		}
		trace_util_0.Count(_table_00000, 102)
		totalCount += selectivity * float64(idx.TotalRowCount())
	}
	trace_util_0.Count(_table_00000, 96)
	if totalCount > idx.TotalRowCount() {
		trace_util_0.Count(_table_00000, 120)
		totalCount = idx.TotalRowCount()
	}
	trace_util_0.Count(_table_00000, 97)
	return totalCount, nil
}

const fakePhysicalID int64 = -1

// PseudoTable creates a pseudo table statistics.
func PseudoTable(tblInfo *model.TableInfo) *Table {
	trace_util_0.Count(_table_00000, 121)
	pseudoHistColl := HistColl{
		Count:          pseudoRowCount,
		PhysicalID:     tblInfo.ID,
		HavePhysicalID: true,
		Columns:        make(map[int64]*Column, len(tblInfo.Columns)),
		Indices:        make(map[int64]*Index, len(tblInfo.Indices)),
		Pseudo:         true,
	}
	t := &Table{
		HistColl: pseudoHistColl,
	}
	for _, col := range tblInfo.Columns {
		trace_util_0.Count(_table_00000, 124)
		if col.State == model.StatePublic {
			trace_util_0.Count(_table_00000, 125)
			t.Columns[col.ID] = &Column{
				PhysicalID: fakePhysicalID,
				Info:       col,
				IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
			}
		}
	}
	trace_util_0.Count(_table_00000, 122)
	for _, idx := range tblInfo.Indices {
		trace_util_0.Count(_table_00000, 126)
		if idx.State == model.StatePublic {
			trace_util_0.Count(_table_00000, 127)
			t.Indices[idx.ID] = &Index{Info: idx}
		}
	}
	trace_util_0.Count(_table_00000, 123)
	return t
}

func getPseudoRowCountByIndexRanges(sc *stmtctx.StatementContext, indexRanges []*ranger.Range,
	tableRowCount float64, colsLen int) (float64, error) {
	trace_util_0.Count(_table_00000, 128)
	if tableRowCount == 0 {
		trace_util_0.Count(_table_00000, 132)
		return 0, nil
	}
	trace_util_0.Count(_table_00000, 129)
	var totalCount float64
	for _, indexRange := range indexRanges {
		trace_util_0.Count(_table_00000, 133)
		count := tableRowCount
		i, err := indexRange.PrefixEqualLen(sc)
		if err != nil {
			trace_util_0.Count(_table_00000, 139)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_table_00000, 134)
		if i == colsLen && !indexRange.LowExclude && !indexRange.HighExclude {
			trace_util_0.Count(_table_00000, 140)
			totalCount += 1.0
			continue
		}
		trace_util_0.Count(_table_00000, 135)
		if i >= len(indexRange.LowVal) {
			trace_util_0.Count(_table_00000, 141)
			i = len(indexRange.LowVal) - 1
		}
		trace_util_0.Count(_table_00000, 136)
		rowCount, err := GetPseudoRowCountByColumnRanges(sc, tableRowCount, []*ranger.Range{indexRange}, i)
		if err != nil {
			trace_util_0.Count(_table_00000, 142)
			return 0, errors.Trace(err)
		}
		trace_util_0.Count(_table_00000, 137)
		count = count / tableRowCount * rowCount
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			trace_util_0.Count(_table_00000, 143)
			count = count / float64(100)
		}
		trace_util_0.Count(_table_00000, 138)
		totalCount += count
	}
	trace_util_0.Count(_table_00000, 130)
	if totalCount > tableRowCount {
		trace_util_0.Count(_table_00000, 144)
		totalCount = tableRowCount / 3.0
	}
	trace_util_0.Count(_table_00000, 131)
	return totalCount, nil
}

// GetPseudoRowCountByColumnRanges calculate the row count by the ranges if there's no statistics information for this column.
func GetPseudoRowCountByColumnRanges(sc *stmtctx.StatementContext, tableRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	trace_util_0.Count(_table_00000, 145)
	var rowCount float64
	var err error
	for _, ran := range columnRanges {
		trace_util_0.Count(_table_00000, 148)
		if ran.LowVal[colIdx].Kind() == types.KindNull && ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			trace_util_0.Count(_table_00000, 150)
			rowCount += tableRowCount
		} else {
			trace_util_0.Count(_table_00000, 151)
			if ran.LowVal[colIdx].Kind() == types.KindMinNotNull {
				trace_util_0.Count(_table_00000, 152)
				nullCount := tableRowCount / pseudoEqualRate
				if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
					trace_util_0.Count(_table_00000, 153)
					rowCount += tableRowCount - nullCount
				} else {
					trace_util_0.Count(_table_00000, 154)
					if err == nil {
						trace_util_0.Count(_table_00000, 155)
						lessCount := tableRowCount / pseudoLessRate
						rowCount += lessCount - nullCount
					}
				}
			} else {
				trace_util_0.Count(_table_00000, 156)
				if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
					trace_util_0.Count(_table_00000, 157)
					rowCount += tableRowCount / pseudoLessRate
				} else {
					trace_util_0.Count(_table_00000, 158)
					{
						compare, err1 := ran.LowVal[colIdx].CompareDatum(sc, &ran.HighVal[colIdx])
						if err1 != nil {
							trace_util_0.Count(_table_00000, 160)
							return 0, errors.Trace(err1)
						}
						trace_util_0.Count(_table_00000, 159)
						if compare == 0 {
							trace_util_0.Count(_table_00000, 161)
							rowCount += tableRowCount / pseudoEqualRate
						} else {
							trace_util_0.Count(_table_00000, 162)
							{
								rowCount += tableRowCount / pseudoBetweenRate
							}
						}
					}
				}
			}
		}
		trace_util_0.Count(_table_00000, 149)
		if err != nil {
			trace_util_0.Count(_table_00000, 163)
			return 0, errors.Trace(err)
		}
	}
	trace_util_0.Count(_table_00000, 146)
	if rowCount > tableRowCount {
		trace_util_0.Count(_table_00000, 164)
		rowCount = tableRowCount
	}
	trace_util_0.Count(_table_00000, 147)
	return rowCount, nil
}

func getPseudoRowCountBySignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	trace_util_0.Count(_table_00000, 165)
	var rowCount float64
	for _, rg := range intRanges {
		trace_util_0.Count(_table_00000, 168)
		var cnt float64
		low := rg.LowVal[0].GetInt64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			trace_util_0.Count(_table_00000, 173)
			low = math.MinInt64
		}
		trace_util_0.Count(_table_00000, 169)
		high := rg.HighVal[0].GetInt64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			trace_util_0.Count(_table_00000, 174)
			high = math.MaxInt64
		}
		trace_util_0.Count(_table_00000, 170)
		if low == math.MinInt64 && high == math.MaxInt64 {
			trace_util_0.Count(_table_00000, 175)
			cnt = tableRowCount
		} else {
			trace_util_0.Count(_table_00000, 176)
			if low == math.MinInt64 {
				trace_util_0.Count(_table_00000, 177)
				cnt = tableRowCount / pseudoLessRate
			} else {
				trace_util_0.Count(_table_00000, 178)
				if high == math.MaxInt64 {
					trace_util_0.Count(_table_00000, 179)
					cnt = tableRowCount / pseudoLessRate
				} else {
					trace_util_0.Count(_table_00000, 180)
					{
						if low == high {
							trace_util_0.Count(_table_00000, 181)
							cnt = 1 // When primary key is handle, the equal row count is at most one.
						} else {
							trace_util_0.Count(_table_00000, 182)
							{
								cnt = tableRowCount / pseudoBetweenRate
							}
						}
					}
				}
			}
		}
		trace_util_0.Count(_table_00000, 171)
		if high-low > 0 && cnt > float64(high-low) {
			trace_util_0.Count(_table_00000, 183)
			cnt = float64(high - low)
		}
		trace_util_0.Count(_table_00000, 172)
		rowCount += cnt
	}
	trace_util_0.Count(_table_00000, 166)
	if rowCount > tableRowCount {
		trace_util_0.Count(_table_00000, 184)
		rowCount = tableRowCount
	}
	trace_util_0.Count(_table_00000, 167)
	return rowCount
}

func getPseudoRowCountByUnsignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	trace_util_0.Count(_table_00000, 185)
	var rowCount float64
	for _, rg := range intRanges {
		trace_util_0.Count(_table_00000, 188)
		var cnt float64
		low := rg.LowVal[0].GetUint64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			trace_util_0.Count(_table_00000, 193)
			low = 0
		}
		trace_util_0.Count(_table_00000, 189)
		high := rg.HighVal[0].GetUint64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			trace_util_0.Count(_table_00000, 194)
			high = math.MaxUint64
		}
		trace_util_0.Count(_table_00000, 190)
		if low == 0 && high == math.MaxUint64 {
			trace_util_0.Count(_table_00000, 195)
			cnt = tableRowCount
		} else {
			trace_util_0.Count(_table_00000, 196)
			if low == 0 {
				trace_util_0.Count(_table_00000, 197)
				cnt = tableRowCount / pseudoLessRate
			} else {
				trace_util_0.Count(_table_00000, 198)
				if high == math.MaxUint64 {
					trace_util_0.Count(_table_00000, 199)
					cnt = tableRowCount / pseudoLessRate
				} else {
					trace_util_0.Count(_table_00000, 200)
					{
						if low == high {
							trace_util_0.Count(_table_00000, 201)
							cnt = 1 // When primary key is handle, the equal row count is at most one.
						} else {
							trace_util_0.Count(_table_00000, 202)
							{
								cnt = tableRowCount / pseudoBetweenRate
							}
						}
					}
				}
			}
		}
		trace_util_0.Count(_table_00000, 191)
		if high > low && cnt > float64(high-low) {
			trace_util_0.Count(_table_00000, 203)
			cnt = float64(high - low)
		}
		trace_util_0.Count(_table_00000, 192)
		rowCount += cnt
	}
	trace_util_0.Count(_table_00000, 186)
	if rowCount > tableRowCount {
		trace_util_0.Count(_table_00000, 204)
		rowCount = tableRowCount
	}
	trace_util_0.Count(_table_00000, 187)
	return rowCount
}

var _table_00000 = "statistics/table.go"
