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

package tables

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

// Both partition and partitionedTable implement the table.Table interface.
var _ table.Table = &partition{}
var _ table.Table = &partitionedTable{}

// partitionedTable implements the table.PartitionedTable interface.
var _ table.PartitionedTable = &partitionedTable{}

// partition is a feature from MySQL:
// See https://dev.mysql.com/doc/refman/8.0/en/partitioning.html
// A partition table may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal table (a table with no
// partitions) is basically the same.
// partition also implements the table.Table interface.
type partition struct {
	tableCommon
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	trace_util_0.Count(_partition_00000, 0)
	return p.physicalTableID
}

// partitionedTable implements the table.PartitionedTable interface.
// partitionedTable is a table, it contains many Partitions.
type partitionedTable struct {
	Table
	partitionExpr *PartitionExpr
	partitions    map[int64]*partition
}

func newPartitionedTable(tbl *Table, tblInfo *model.TableInfo) (table.Table, error) {
	trace_util_0.Count(_partition_00000, 1)
	ret := &partitionedTable{Table: *tbl}
	pi := tblInfo.GetPartitionInfo()
	var partitionExpr *PartitionExpr
	var err error
	switch pi.Type {
	case model.PartitionTypeRange:
		trace_util_0.Count(_partition_00000, 6)
		partitionExpr, err = generatePartitionExpr(tblInfo)
	case model.PartitionTypeHash:
		trace_util_0.Count(_partition_00000, 7)
		partitionExpr, err = generateHashPartitionExpr(tblInfo)
	}
	trace_util_0.Count(_partition_00000, 2)
	if err != nil {
		trace_util_0.Count(_partition_00000, 8)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 3)
	ret.partitionExpr = partitionExpr

	if err := initTableIndices(&ret.tableCommon); err != nil {
		trace_util_0.Count(_partition_00000, 9)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 4)
	partitions := make(map[int64]*partition)
	for _, p := range pi.Definitions {
		trace_util_0.Count(_partition_00000, 10)
		var t partition
		err := initTableCommonWithIndices(&t.tableCommon, tblInfo, p.ID, tbl.Columns, tbl.alloc)
		if err != nil {
			trace_util_0.Count(_partition_00000, 12)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 11)
		partitions[p.ID] = &t
	}
	trace_util_0.Count(_partition_00000, 5)
	ret.partitions = partitions
	return ret, nil
}

// PartitionExpr is the partition definition expressions.
// There are two expressions exist, because Locate use binary search, which requires:
// Given a compare function, for any partition range i, if cmp[i] > 0, then cmp[i+1] > 0.
// While partition prune must use the accurate range to do prunning.
// partition by range (x)
//   (partition
//      p1 values less than (y1)
//      p2 values less than (y2)
//      p3 values less than (y3))
// Ranges: (x < y1 or x is null); (y1 <= x < y2); (y2 <= x < y3)
// UpperBounds: (x < y1); (x < y2); (x < y3)
type PartitionExpr struct {
	// Column is the column appeared in the by range expression, partition pruning need this to work.
	Column      *expression.Column
	Ranges      []expression.Expression
	UpperBounds []expression.Expression
	// Expr is the hash partition expression.
	Expr expression.Expression
}

// rangePartitionString returns the partition string for a range typed partition.
func rangePartitionString(pi *model.PartitionInfo) string {
	trace_util_0.Count(_partition_00000, 13)
	// partition by range expr
	if len(pi.Columns) == 0 {
		trace_util_0.Count(_partition_00000, 16)
		return pi.Expr
	}

	// partition by range columns (c1)
	trace_util_0.Count(_partition_00000, 14)
	if len(pi.Columns) == 1 {
		trace_util_0.Count(_partition_00000, 17)
		return pi.Columns[0].L
	}

	// partition by range columns (c1, c2, ...)
	trace_util_0.Count(_partition_00000, 15)
	panic("create table assert len(columns) = 1")
}

func generatePartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	trace_util_0.Count(_partition_00000, 18)
	var column *expression.Column
	// The caller should assure partition info is not nil.
	pi := tblInfo.GetPartitionInfo()
	ctx := mock.NewContext()
	partitionPruneExprs := make([]expression.Expression, 0, len(pi.Definitions))
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns := expression.ColumnInfos2ColumnsWithDBName(ctx, dbName, tblInfo.Name, tblInfo.Columns)
	schema := expression.NewSchema(columns...)
	partStr := rangePartitionString(pi)
	for i := 0; i < len(pi.Definitions); i++ {
		trace_util_0.Count(_partition_00000, 20)

		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			trace_util_0.Count(_partition_00000, 25)
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			trace_util_0.Count(_partition_00000, 26)
			{
				fmt.Fprintf(&buf, "((%s) < (%s))", partStr, pi.Definitions[i].LessThan[0])
			}
		}

		trace_util_0.Count(_partition_00000, 21)
		exprs, err := expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			trace_util_0.Count(_partition_00000, 27)
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 22)
		locateExprs = append(locateExprs, exprs[0])

		if i > 0 {
			trace_util_0.Count(_partition_00000, 28)
			fmt.Fprintf(&buf, " and ((%s) >= (%s))", partStr, pi.Definitions[i-1].LessThan[0])
		} else {
			trace_util_0.Count(_partition_00000, 29)
			{
				// NULL will locate in the first partition, so its expression is (expr < value or expr is null).
				fmt.Fprintf(&buf, " or ((%s) is null)", partStr)

				// Extracts the column of the partition expression, it will be used by partition prunning.
				if tmps, err1 := expression.ParseSimpleExprsWithSchema(ctx, partStr, schema); err1 == nil {
					trace_util_0.Count(_partition_00000, 31)
					if col, ok := tmps[0].(*expression.Column); ok {
						trace_util_0.Count(_partition_00000, 32)
						column = col
					}
				}
				trace_util_0.Count(_partition_00000, 30)
				if column == nil {
					trace_util_0.Count(_partition_00000, 33)
					logutil.Logger(context.Background()).Warn("partition pruning not applicable", zap.String("expression", partStr))
				}
			}
		}

		trace_util_0.Count(_partition_00000, 23)
		exprs, err = expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			trace_util_0.Count(_partition_00000, 34)
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 24)
		partitionPruneExprs = append(partitionPruneExprs, exprs[0])
		buf.Reset()
	}
	trace_util_0.Count(_partition_00000, 19)
	return &PartitionExpr{
		Column:      column,
		Ranges:      partitionPruneExprs,
		UpperBounds: locateExprs,
	}, nil
}

func generateHashPartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	trace_util_0.Count(_partition_00000, 35)
	var column *expression.Column
	// The caller should assure partition info is not nil.
	pi := tblInfo.GetPartitionInfo()
	ctx := mock.NewContext()
	partitionPruneExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns := expression.ColumnInfos2ColumnsWithDBName(ctx, dbName, tblInfo.Name, tblInfo.Columns)
	schema := expression.NewSchema(columns...)
	for i := 0; i < int(pi.Num); i++ {
		trace_util_0.Count(_partition_00000, 39)
		fmt.Fprintf(&buf, "MOD(ABS(%s),(%d))=%d", pi.Expr, pi.Num, i)
		exprs, err := expression.ParseSimpleExprsWithSchema(ctx, buf.String(), schema)
		if err != nil {
			trace_util_0.Count(_partition_00000, 41)
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 40)
		partitionPruneExprs = append(partitionPruneExprs, exprs[0])
		buf.Reset()
	}
	trace_util_0.Count(_partition_00000, 36)
	exprs, err := expression.ParseSimpleExprsWithSchema(ctx, pi.Expr, schema)
	if err != nil {
		trace_util_0.Count(_partition_00000, 42)
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.Logger(context.Background()).Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 37)
	if col, ok := exprs[0].(*expression.Column); ok {
		trace_util_0.Count(_partition_00000, 43)
		column = col
	}
	trace_util_0.Count(_partition_00000, 38)
	return &PartitionExpr{
		Column: column,
		Expr:   exprs[0],
		Ranges: partitionPruneExprs,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedTable) PartitionExpr() *PartitionExpr {
	trace_util_0.Count(_partition_00000, 44)
	return t.partitionExpr
}

func partitionRecordKey(pid int64, handle int64) kv.Key {
	trace_util_0.Count(_partition_00000, 45)
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, handle)
}

// locatePartition returns the partition ID of the input record.
func (t *partitionedTable) locatePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int64, error) {
	trace_util_0.Count(_partition_00000, 46)
	var err error
	var idx int
	switch t.meta.Partition.Type {
	case model.PartitionTypeRange:
		trace_util_0.Count(_partition_00000, 49)
		idx, err = t.locateRangePartition(ctx, pi, r)
	case model.PartitionTypeHash:
		trace_util_0.Count(_partition_00000, 50)
		idx, err = t.locateHashPartition(ctx, pi, r)
	}
	trace_util_0.Count(_partition_00000, 47)
	if err != nil {
		trace_util_0.Count(_partition_00000, 51)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 48)
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedTable) locateRangePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	trace_util_0.Count(_partition_00000, 52)
	var err error
	var isNull bool
	partitionExprs := t.partitionExpr.UpperBounds
	idx := sort.Search(len(partitionExprs), func(i int) bool {
		trace_util_0.Count(_partition_00000, 57)
		var ret int64
		ret, isNull, err = partitionExprs[i].EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
		if err != nil {
			trace_util_0.Count(_partition_00000, 60)
			return true // Break the search.
		}
		trace_util_0.Count(_partition_00000, 58)
		if isNull {
			trace_util_0.Count(_partition_00000, 61)
			// If the column value used to determine the partition is NULL, the row is inserted into the lowest partition.
			// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Break the search.
		}
		trace_util_0.Count(_partition_00000, 59)
		return ret > 0
	})
	trace_util_0.Count(_partition_00000, 53)
	if err != nil {
		trace_util_0.Count(_partition_00000, 62)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 54)
	if isNull {
		trace_util_0.Count(_partition_00000, 63)
		idx = 0
	}
	trace_util_0.Count(_partition_00000, 55)
	if idx < 0 || idx >= len(partitionExprs) {
		trace_util_0.Count(_partition_00000, 64)
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
		if err != nil {
			trace_util_0.Count(_partition_00000, 67)
			return 0, errors.Trace(err)
		}

		trace_util_0.Count(_partition_00000, 65)
		ret, _, err2 := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
		if err2 != nil {
			trace_util_0.Count(_partition_00000, 68)
			return 0, errors.Trace(err2)
		}
		trace_util_0.Count(_partition_00000, 66)
		return 0, errors.Trace(table.ErrNoPartitionForGivenValue.GenWithStackByArgs(fmt.Sprintf("%d", ret)))
	}
	trace_util_0.Count(_partition_00000, 56)
	return idx, nil
}

// TODO: supports linear hashing
func (t *partitionedTable) locateHashPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	trace_util_0.Count(_partition_00000, 69)
	ret, isNull, err := t.partitionExpr.Expr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
	if err != nil {
		trace_util_0.Count(_partition_00000, 73)
		return 0, err
	}
	trace_util_0.Count(_partition_00000, 70)
	if isNull {
		trace_util_0.Count(_partition_00000, 74)
		return 0, nil
	}
	trace_util_0.Count(_partition_00000, 71)
	if ret < 0 {
		trace_util_0.Count(_partition_00000, 75)
		ret = 0 - ret
	}
	trace_util_0.Count(_partition_00000, 72)
	return int(ret % int64(t.meta.Partition.Num)), nil
}

// GetPartition returns a Table, which is actually a partition.
func (t *partitionedTable) GetPartition(pid int64) table.PhysicalTable {
	trace_util_0.Count(_partition_00000, 76)
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a kind of `table.PhysicalTable`
	p, ok := t.partitions[pid]
	if !ok {
		trace_util_0.Count(_partition_00000, 78)
		return nil
	}
	trace_util_0.Count(_partition_00000, 77)
	return p
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionedTable) GetPartitionByRow(ctx sessionctx.Context, r []types.Datum) (table.Table, error) {
	trace_util_0.Count(_partition_00000, 79)
	pid, err := t.locatePartition(ctx, t.Meta().GetPartitionInfo(), r)
	if err != nil {
		trace_util_0.Count(_partition_00000, 81)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 80)
	return t.partitions[pid], nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...*table.AddRecordOpt) (recordID int64, err error) {
	trace_util_0.Count(_partition_00000, 82)
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		trace_util_0.Count(_partition_00000, 84)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_partition_00000, 83)
	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, opts...)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *partitionedTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	trace_util_0.Count(_partition_00000, 85)
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		trace_util_0.Count(_partition_00000, 87)
		return errors.Trace(err)
	}

	trace_util_0.Count(_partition_00000, 86)
	tbl := t.GetPartition(pid)
	return tbl.RemoveRecord(ctx, h, r)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *partitionedTable) UpdateRecord(ctx sessionctx.Context, h int64, currData, newData []types.Datum, touched []bool) error {
	trace_util_0.Count(_partition_00000, 88)
	partitionInfo := t.meta.GetPartitionInfo()
	from, err := t.locatePartition(ctx, partitionInfo, currData)
	if err != nil {
		trace_util_0.Count(_partition_00000, 92)
		return errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 89)
	to, err := t.locatePartition(ctx, partitionInfo, newData)
	if err != nil {
		trace_util_0.Count(_partition_00000, 93)
		return errors.Trace(err)
	}

	// The old and new data locate in different partitions.
	// Remove record from old partition and add record to new partition.
	trace_util_0.Count(_partition_00000, 90)
	if from != to {
		trace_util_0.Count(_partition_00000, 94)
		_, err = t.GetPartition(to).AddRecord(ctx, newData)
		if err != nil {
			trace_util_0.Count(_partition_00000, 97)
			return errors.Trace(err)
		}
		// UpdateRecord should be side effect free, but there're two steps here.
		// What would happen if step1 succeed but step2 meets error? It's hard
		// to rollback.
		// So this special order is chosen: add record first, errors such as
		// 'Key Already Exists' will generally happen during step1, errors are
		// unlikely to happen in step2.
		trace_util_0.Count(_partition_00000, 95)
		err = t.GetPartition(from).RemoveRecord(ctx, h, currData)
		if err != nil {
			trace_util_0.Count(_partition_00000, 98)
			logutil.Logger(context.Background()).Error("update partition record fails", zap.String("message", "new record inserted while old record is not removed"), zap.Error(err))
			return errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 96)
		return nil
	}

	trace_util_0.Count(_partition_00000, 91)
	tbl := t.GetPartition(to)
	return tbl.UpdateRecord(ctx, h, currData, newData, touched)
}

// FindPartitionByName finds partition in table meta by name.
func FindPartitionByName(meta *model.TableInfo, parName string) (int64, error) {
	trace_util_0.Count(_partition_00000, 99)
	// Hash partition table use p0, p1, p2, p3 as partition names automatically.
	parName = strings.ToLower(parName)
	for _, def := range meta.Partition.Definitions {
		trace_util_0.Count(_partition_00000, 101)
		if strings.EqualFold(def.Name.L, parName) {
			trace_util_0.Count(_partition_00000, 102)
			return def.ID, nil
		}
	}
	trace_util_0.Count(_partition_00000, 100)
	return -1, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(parName, meta.Name.O))
}

var _partition_00000 = "table/tables/partition.go"
