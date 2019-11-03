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

package ddl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	partitionMaxValue = "MAXVALUE"
	primarykey        = "PRIMARY KEY"
)

// buildTablePartitionInfo builds partition info and checks for some errors.
func buildTablePartitionInfo(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt) (*model.PartitionInfo, error) {
	trace_util_0.Count(_partition_00000, 0)
	if s.Partition == nil {
		trace_util_0.Count(_partition_00000, 6)
		return nil, nil
	}
	trace_util_0.Count(_partition_00000, 1)
	var enable bool
	switch ctx.GetSessionVars().EnableTablePartition {
	case "on":
		trace_util_0.Count(_partition_00000, 7)
		enable = true
	case "off":
		trace_util_0.Count(_partition_00000, 8)
		enable = false
	default:
		trace_util_0.Count(_partition_00000, 9)
		// When tidb_enable_table_partition = 'auto',
		if s.Partition.Tp == model.PartitionTypeRange {
			trace_util_0.Count(_partition_00000, 11)
			// Partition by range expression is enabled by default.
			if s.Partition.ColumnNames == nil {
				trace_util_0.Count(_partition_00000, 13)
				enable = true
			}
			// Partition by range columns and just one column.
			trace_util_0.Count(_partition_00000, 12)
			if len(s.Partition.ColumnNames) == 1 {
				trace_util_0.Count(_partition_00000, 14)
				enable = true
			}
		}
		// Partition by hash is enabled by default.
		// Note that linear hash is not enabled.
		trace_util_0.Count(_partition_00000, 10)
		if s.Partition.Tp == model.PartitionTypeHash {
			trace_util_0.Count(_partition_00000, 15)
			enable = true
		}
	}
	trace_util_0.Count(_partition_00000, 2)
	if !enable {
		trace_util_0.Count(_partition_00000, 16)
		ctx.GetSessionVars().StmtCtx.AppendWarning(errUnsupportedCreatePartition)
	}

	trace_util_0.Count(_partition_00000, 3)
	pi := &model.PartitionInfo{
		Type:   s.Partition.Tp,
		Enable: enable,
		Num:    s.Partition.Num,
	}
	if s.Partition.Expr != nil {
		trace_util_0.Count(_partition_00000, 17)
		buf := new(bytes.Buffer)
		s.Partition.Expr.Format(buf)
		pi.Expr = buf.String()
	} else {
		trace_util_0.Count(_partition_00000, 18)
		if s.Partition.ColumnNames != nil {
			trace_util_0.Count(_partition_00000, 19)
			// TODO: Support multiple columns for 'PARTITION BY RANGE COLUMNS'.
			if len(s.Partition.ColumnNames) != 1 {
				trace_util_0.Count(_partition_00000, 21)
				pi.Enable = false
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrUnsupportedPartitionByRangeColumns)
			}
			trace_util_0.Count(_partition_00000, 20)
			pi.Columns = make([]model.CIStr, 0, len(s.Partition.ColumnNames))
			for _, cn := range s.Partition.ColumnNames {
				trace_util_0.Count(_partition_00000, 22)
				pi.Columns = append(pi.Columns, cn.Name)
			}
		}
	}

	trace_util_0.Count(_partition_00000, 4)
	if s.Partition.Tp == model.PartitionTypeRange {
		trace_util_0.Count(_partition_00000, 23)
		if err := buildRangePartitionDefinitions(ctx, d, s, pi); err != nil {
			trace_util_0.Count(_partition_00000, 24)
			return nil, errors.Trace(err)
		}
	} else {
		trace_util_0.Count(_partition_00000, 25)
		if s.Partition.Tp == model.PartitionTypeHash {
			trace_util_0.Count(_partition_00000, 26)
			if err := buildHashPartitionDefinitions(ctx, d, s, pi); err != nil {
				trace_util_0.Count(_partition_00000, 27)
				return nil, errors.Trace(err)
			}
		}
	}
	trace_util_0.Count(_partition_00000, 5)
	return pi, nil
}

func buildHashPartitionDefinitions(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt, pi *model.PartitionInfo) error {
	trace_util_0.Count(_partition_00000, 28)
	genIDs, err := d.genGlobalIDs(int(pi.Num))
	if err != nil {
		trace_util_0.Count(_partition_00000, 31)
		return errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 29)
	defs := make([]model.PartitionDefinition, pi.Num)
	for i := 0; i < len(defs); i++ {
		trace_util_0.Count(_partition_00000, 32)
		defs[i].ID = genIDs[i]
		defs[i].Name = model.NewCIStr(fmt.Sprintf("p%v", i))
	}
	trace_util_0.Count(_partition_00000, 30)
	pi.Definitions = defs
	return nil
}

func buildRangePartitionDefinitions(ctx sessionctx.Context, d *ddl, s *ast.CreateTableStmt, pi *model.PartitionInfo) error {
	trace_util_0.Count(_partition_00000, 33)
	genIDs, err := d.genGlobalIDs(len(s.Partition.Definitions))
	if err != nil {
		trace_util_0.Count(_partition_00000, 36)
		return err
	}
	trace_util_0.Count(_partition_00000, 34)
	for ith, def := range s.Partition.Definitions {
		trace_util_0.Count(_partition_00000, 37)
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			ID:      genIDs[ith],
			Comment: def.Comment,
		}

		if s.Partition.ColumnNames == nil && len(def.LessThan) != 1 {
			trace_util_0.Count(_partition_00000, 40)
			return ErrTooManyValues.GenWithStackByArgs(s.Partition.Tp.String())
		}
		trace_util_0.Count(_partition_00000, 38)
		buf := new(bytes.Buffer)
		// Range columns partitions support multi-column partitions.
		for _, expr := range def.LessThan {
			trace_util_0.Count(_partition_00000, 41)
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		trace_util_0.Count(_partition_00000, 39)
		pi.Definitions = append(pi.Definitions, piDef)
	}
	trace_util_0.Count(_partition_00000, 35)
	return nil
}

func checkPartitionNameUnique(tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
	trace_util_0.Count(_partition_00000, 42)
	partNames := make(map[string]struct{})
	if tbInfo.Partition != nil {
		trace_util_0.Count(_partition_00000, 45)
		oldPars := tbInfo.Partition.Definitions
		for _, oldPar := range oldPars {
			trace_util_0.Count(_partition_00000, 46)
			partNames[oldPar.Name.L] = struct{}{}
		}
	}
	trace_util_0.Count(_partition_00000, 43)
	newPars := pi.Definitions
	for _, newPar := range newPars {
		trace_util_0.Count(_partition_00000, 47)
		if _, ok := partNames[newPar.Name.L]; ok {
			trace_util_0.Count(_partition_00000, 49)
			return ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		trace_util_0.Count(_partition_00000, 48)
		partNames[newPar.Name.L] = struct{}{}
	}
	trace_util_0.Count(_partition_00000, 44)
	return nil
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	trace_util_0.Count(_partition_00000, 50)
	switch v := expr.(type) {
	case *ast.FuncCastExpr, *ast.CaseExpr:
		trace_util_0.Count(_partition_00000, 52)
		return errors.Trace(ErrPartitionFunctionIsNotAllowed)
	case *ast.FuncCallExpr:
		trace_util_0.Count(_partition_00000, 53)
		// check function which allowed in partitioning expressions
		// see https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-limitations-functions.html
		switch v.FnName.L {
		case ast.Abs, ast.Ceiling, ast.DateDiff, ast.Day, ast.DayOfMonth, ast.DayOfWeek, ast.DayOfYear, ast.Extract, ast.Floor,
			ast.Hour, ast.MicroSecond, ast.Minute, ast.Mod, ast.Month, ast.Quarter, ast.Second, ast.TimeToSec, ast.ToDays,
			ast.ToSeconds, ast.Weekday, ast.Year, ast.YearWeek:
			trace_util_0.Count(_partition_00000, 58)
			return nil
		case ast.UnixTimestamp:
			trace_util_0.Count(_partition_00000, 59)
			if len(v.Args) == 1 {
				trace_util_0.Count(_partition_00000, 60)
				col, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, v.Args[0])
				if err != nil {
					trace_util_0.Count(_partition_00000, 63)
					return errors.Trace(err)
				}
				trace_util_0.Count(_partition_00000, 61)
				if col.GetType().Tp != mysql.TypeTimestamp {
					trace_util_0.Count(_partition_00000, 64)
					return errors.Trace(errWrongExprInPartitionFunc)
				}
				trace_util_0.Count(_partition_00000, 62)
				return nil
			}
		}
		trace_util_0.Count(_partition_00000, 54)
		return errors.Trace(ErrPartitionFunctionIsNotAllowed)
	case *ast.BinaryOperationExpr:
		trace_util_0.Count(_partition_00000, 55)
		// The DIV operator (opcode.IntDiv) is also supported; the / operator ( opcode.Div ) is not permitted.
		// see https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html
		switch v.Op {
		case opcode.Or, opcode.And, opcode.Xor, opcode.LeftShift, opcode.RightShift, opcode.BitNeg, opcode.Div:
			trace_util_0.Count(_partition_00000, 65)
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
		trace_util_0.Count(_partition_00000, 56)
		return nil
	case *ast.UnaryOperationExpr:
		trace_util_0.Count(_partition_00000, 57)
		if v.Op == opcode.BitNeg {
			trace_util_0.Count(_partition_00000, 66)
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
	}
	trace_util_0.Count(_partition_00000, 51)
	return nil
}

// checkPartitionFuncType checks partition function return type.
func checkPartitionFuncType(ctx sessionctx.Context, s *ast.CreateTableStmt, cols []*table.Column, tblInfo *model.TableInfo) error {
	trace_util_0.Count(_partition_00000, 67)
	if s.Partition.Expr == nil {
		trace_util_0.Count(_partition_00000, 73)
		return nil
	}
	trace_util_0.Count(_partition_00000, 68)
	buf := new(bytes.Buffer)
	s.Partition.Expr.Format(buf)
	exprStr := buf.String()
	if s.Partition.Tp == model.PartitionTypeRange || s.Partition.Tp == model.PartitionTypeHash {
		trace_util_0.Count(_partition_00000, 74)
		// if partition by columnExpr, check the column type
		if _, ok := s.Partition.Expr.(*ast.ColumnNameExpr); ok {
			trace_util_0.Count(_partition_00000, 75)
			for _, col := range cols {
				trace_util_0.Count(_partition_00000, 76)
				name := strings.Replace(col.Name.String(), ".", "`.`", -1)
				// Range partitioning key supported types: tinyint, smallint, mediumint, int and bigint.
				if !validRangePartitionType(col) && fmt.Sprintf("`%s`", name) == exprStr {
					trace_util_0.Count(_partition_00000, 77)
					return errors.Trace(ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr))
				}
			}
		}
	}

	trace_util_0.Count(_partition_00000, 69)
	e, err := expression.ParseSimpleExprWithTableInfo(ctx, exprStr, tblInfo)
	if err != nil {
		trace_util_0.Count(_partition_00000, 78)
		return errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 70)
	if e.GetType().EvalType() == types.ETInt {
		trace_util_0.Count(_partition_00000, 79)
		return nil
	}
	trace_util_0.Count(_partition_00000, 71)
	if s.Partition.Tp == model.PartitionTypeHash {
		trace_util_0.Count(_partition_00000, 80)
		if _, ok := s.Partition.Expr.(*ast.ColumnNameExpr); ok {
			trace_util_0.Count(_partition_00000, 81)
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr)
		}
	}

	trace_util_0.Count(_partition_00000, 72)
	return ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION")
}

// checkCreatePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkCreatePartitionValue(ctx sessionctx.Context, tblInfo *model.TableInfo, pi *model.PartitionInfo, cols []*table.Column) error {
	trace_util_0.Count(_partition_00000, 82)
	defs := pi.Definitions
	if len(defs) <= 1 {
		trace_util_0.Count(_partition_00000, 86)
		return nil
	}

	trace_util_0.Count(_partition_00000, 83)
	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		trace_util_0.Count(_partition_00000, 87)
		defs = defs[:len(defs)-1]
	}
	trace_util_0.Count(_partition_00000, 84)
	isUnsignedBigint := isRangePartitionColUnsignedBigint(cols, pi)
	var prevRangeValue interface{}
	for i := 0; i < len(defs); i++ {
		trace_util_0.Count(_partition_00000, 88)
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			trace_util_0.Count(_partition_00000, 94)
			return errors.Trace(ErrPartitionMaxvalue)
		}

		trace_util_0.Count(_partition_00000, 89)
		currentRangeValue, fromExpr, err := getRangeValue(ctx, tblInfo, defs[i].LessThan[0], isUnsignedBigint)
		if err != nil {
			trace_util_0.Count(_partition_00000, 95)
			return errors.Trace(err)
		}
		trace_util_0.Count(_partition_00000, 90)
		if fromExpr {
			trace_util_0.Count(_partition_00000, 96)
			// Constant fold the expression.
			defs[i].LessThan[0] = fmt.Sprintf("%d", currentRangeValue)
		}

		trace_util_0.Count(_partition_00000, 91)
		if i == 0 {
			trace_util_0.Count(_partition_00000, 97)
			prevRangeValue = currentRangeValue
			continue
		}

		trace_util_0.Count(_partition_00000, 92)
		if isUnsignedBigint {
			trace_util_0.Count(_partition_00000, 98)
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				trace_util_0.Count(_partition_00000, 99)
				return errors.Trace(ErrRangeNotIncreasing)
			}
		} else {
			trace_util_0.Count(_partition_00000, 100)
			{
				if currentRangeValue.(int64) <= prevRangeValue.(int64) {
					trace_util_0.Count(_partition_00000, 101)
					return errors.Trace(ErrRangeNotIncreasing)
				}
			}
		}
		trace_util_0.Count(_partition_00000, 93)
		prevRangeValue = currentRangeValue
	}
	trace_util_0.Count(_partition_00000, 85)
	return nil
}

// getRangeValue gets an integer from the range value string.
// The returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx sessionctx.Context, tblInfo *model.TableInfo, str string, unsignedBigint bool) (interface{}, bool, error) {
	trace_util_0.Count(_partition_00000, 102)
	// Unsigned bigint was converted to uint64 handle.
	if unsignedBigint {
		trace_util_0.Count(_partition_00000, 104)
		if value, err := strconv.ParseUint(str, 10, 64); err == nil {
			trace_util_0.Count(_partition_00000, 106)
			return value, false, nil
		}

		trace_util_0.Count(_partition_00000, 105)
		if e, err1 := expression.ParseSimpleExprWithTableInfo(ctx, str, tblInfo); err1 == nil {
			trace_util_0.Count(_partition_00000, 107)
			res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
			if err2 == nil && !isNull {
				trace_util_0.Count(_partition_00000, 108)
				return uint64(res), true, nil
			}
		}
	} else {
		trace_util_0.Count(_partition_00000, 109)
		{
			if value, err := strconv.ParseInt(str, 10, 64); err == nil {
				trace_util_0.Count(_partition_00000, 111)
				return value, false, nil
			}
			// The range value maybe not an integer, it could be a constant expression.
			// For example, the following two cases are the same:
			// PARTITION p0 VALUES LESS THAN (TO_SECONDS('2004-01-01'))
			// PARTITION p0 VALUES LESS THAN (63340531200)
			trace_util_0.Count(_partition_00000, 110)
			if e, err1 := expression.ParseSimpleExprWithTableInfo(ctx, str, tblInfo); err1 == nil {
				trace_util_0.Count(_partition_00000, 112)
				res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
				if err2 == nil && !isNull {
					trace_util_0.Count(_partition_00000, 113)
					return res, true, nil
				}
			}
		}
	}
	trace_util_0.Count(_partition_00000, 103)
	return 0, false, ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// validRangePartitionType checks the type supported by the range partitioning key.
func validRangePartitionType(col *table.Column) bool {
	trace_util_0.Count(_partition_00000, 114)
	switch col.FieldType.EvalType() {
	case types.ETInt:
		trace_util_0.Count(_partition_00000, 115)
		return true
	default:
		trace_util_0.Count(_partition_00000, 116)
		return false
	}
}

// checkDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func checkDropTablePartition(meta *model.TableInfo, partName string) error {
	trace_util_0.Count(_partition_00000, 117)
	pi := meta.Partition
	if pi.Type != model.PartitionTypeRange && pi.Type != model.PartitionTypeList {
		trace_util_0.Count(_partition_00000, 120)
		return errOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}
	trace_util_0.Count(_partition_00000, 118)
	oldDefs := pi.Definitions
	for _, def := range oldDefs {
		trace_util_0.Count(_partition_00000, 121)
		if strings.EqualFold(def.Name.L, strings.ToLower(partName)) {
			trace_util_0.Count(_partition_00000, 122)
			if len(oldDefs) == 1 {
				trace_util_0.Count(_partition_00000, 124)
				return errors.Trace(ErrDropLastPartition)
			}
			trace_util_0.Count(_partition_00000, 123)
			return nil
		}
	}
	trace_util_0.Count(_partition_00000, 119)
	return errors.Trace(ErrDropPartitionNonExistent.GenWithStackByArgs(partName))
}

// removePartitionInfo each ddl job deletes a partition.
func removePartitionInfo(tblInfo *model.TableInfo, partName string) int64 {
	trace_util_0.Count(_partition_00000, 125)
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-1)
	var pid int64
	for i := 0; i < len(oldDefs); i++ {
		trace_util_0.Count(_partition_00000, 127)
		if !strings.EqualFold(oldDefs[i].Name.L, strings.ToLower(partName)) {
			trace_util_0.Count(_partition_00000, 129)
			continue
		}
		trace_util_0.Count(_partition_00000, 128)
		pid = oldDefs[i].ID
		newDefs = append(oldDefs[:i], oldDefs[i+1:]...)
		break
	}
	trace_util_0.Count(_partition_00000, 126)
	tblInfo.Partition.Definitions = newDefs
	return pid
}

// onDropTablePartition deletes old partition meta.
func onDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	trace_util_0.Count(_partition_00000, 130)
	var partName string
	if err := job.DecodeArgs(&partName); err != nil {
		trace_util_0.Count(_partition_00000, 135)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 131)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_partition_00000, 136)
		return ver, errors.Trace(err)
	}
	// If an error occurs, it returns that it cannot delete all partitions or that the partition doesn't exist.
	trace_util_0.Count(_partition_00000, 132)
	err = checkDropTablePartition(tblInfo, partName)
	if err != nil {
		trace_util_0.Count(_partition_00000, 137)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 133)
	physicalTableID := removePartitionInfo(tblInfo, partName)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_partition_00000, 138)
		return ver, errors.Trace(err)
	}

	// Finish this job.
	trace_util_0.Count(_partition_00000, 134)
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	// A background job will be created to delete old partition data.
	job.Args = []interface{}{physicalTableID}
	return ver, nil
}

// onDropTablePartition truncates old partition meta.
func onTruncateTablePartition(t *meta.Meta, job *model.Job) (int64, error) {
	trace_util_0.Count(_partition_00000, 139)
	var ver int64
	var oldID int64
	if err := job.DecodeArgs(&oldID); err != nil {
		trace_util_0.Count(_partition_00000, 146)
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 140)
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		trace_util_0.Count(_partition_00000, 147)
		return ver, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 141)
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		trace_util_0.Count(_partition_00000, 148)
		return ver, errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	trace_util_0.Count(_partition_00000, 142)
	var find bool
	for i := 0; i < len(pi.Definitions); i++ {
		trace_util_0.Count(_partition_00000, 149)
		def := &pi.Definitions[i]
		if def.ID == oldID {
			trace_util_0.Count(_partition_00000, 150)
			pid, err1 := t.GenGlobalID()
			if err != nil {
				trace_util_0.Count(_partition_00000, 152)
				return ver, errors.Trace(err1)
			}
			trace_util_0.Count(_partition_00000, 151)
			def.ID = pid
			find = true
			break
		}
	}
	trace_util_0.Count(_partition_00000, 143)
	if !find {
		trace_util_0.Count(_partition_00000, 153)
		return ver, table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O)
	}

	trace_util_0.Count(_partition_00000, 144)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		trace_util_0.Count(_partition_00000, 154)
		return ver, errors.Trace(err)
	}

	// Finish this job.
	trace_util_0.Count(_partition_00000, 145)
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	// A background job will be created to delete old partition data.
	job.Args = []interface{}{oldID}
	return ver, nil
}

func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	trace_util_0.Count(_partition_00000, 155)
	if piDefs > uint64(PartitionCountLimit) {
		trace_util_0.Count(_partition_00000, 157)
		return errors.Trace(ErrTooManyPartitions)
	}
	trace_util_0.Count(_partition_00000, 156)
	return nil
}

func checkNoHashPartitions(ctx sessionctx.Context, partitionNum uint64) error {
	trace_util_0.Count(_partition_00000, 158)
	if partitionNum == 0 {
		trace_util_0.Count(_partition_00000, 160)
		return ErrNoParts.GenWithStackByArgs("partitions")
	}
	trace_util_0.Count(_partition_00000, 159)
	return nil
}

func checkNoRangePartitions(partitionNum int) error {
	trace_util_0.Count(_partition_00000, 161)
	if partitionNum == 0 {
		trace_util_0.Count(_partition_00000, 163)
		return errors.Trace(ErrPartitionsMustBeDefined)
	}
	trace_util_0.Count(_partition_00000, 162)
	return nil
}

func getPartitionIDs(table *model.TableInfo) []int64 {
	trace_util_0.Count(_partition_00000, 164)
	if table.GetPartitionInfo() == nil {
		trace_util_0.Count(_partition_00000, 167)
		return []int64{}
	}
	trace_util_0.Count(_partition_00000, 165)
	physicalTableIDs := make([]int64, 0, len(table.Partition.Definitions))
	for _, def := range table.Partition.Definitions {
		trace_util_0.Count(_partition_00000, 168)
		physicalTableIDs = append(physicalTableIDs, def.ID)
	}
	trace_util_0.Count(_partition_00000, 166)
	return physicalTableIDs
}

// checkRangePartitioningKeysConstraints checks that the range partitioning key is included in the table constraint.
func checkRangePartitioningKeysConstraints(sctx sessionctx.Context, s *ast.CreateTableStmt, tblInfo *model.TableInfo, constraints []*ast.Constraint) error {
	trace_util_0.Count(_partition_00000, 169)
	// Returns directly if there is no constraint in the partition table.
	// TODO: Remove the test 's.Partition.Expr == nil' when we support 'PARTITION BY RANGE COLUMNS'
	if len(constraints) == 0 || s.Partition.Expr == nil {
		trace_util_0.Count(_partition_00000, 173)
		return nil
	}

	// Parse partitioning key, extract the column names in the partitioning key to slice.
	trace_util_0.Count(_partition_00000, 170)
	buf := new(bytes.Buffer)
	s.Partition.Expr.Format(buf)
	partCols, err := extractPartitionColumns(sctx, buf.String(), tblInfo)
	if err != nil {
		trace_util_0.Count(_partition_00000, 174)
		return err
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	trace_util_0.Count(_partition_00000, 171)
	for _, constraint := range constraints {
		trace_util_0.Count(_partition_00000, 175)
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			trace_util_0.Count(_partition_00000, 176)
			if !checkUniqueKeyIncludePartKey(partCols, constraint.Keys) {
				trace_util_0.Count(_partition_00000, 177)
				if constraint.Tp == ast.ConstraintPrimaryKey {
					trace_util_0.Count(_partition_00000, 179)
					return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
				}
				trace_util_0.Count(_partition_00000, 178)
				return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
			}
		}
	}
	trace_util_0.Count(_partition_00000, 172)
	return nil
}

func checkPartitionKeysConstraint(sctx sessionctx.Context, partExpr string, idxColNames []*ast.IndexColName, tblInfo *model.TableInfo) error {
	trace_util_0.Count(_partition_00000, 180)
	// Parse partitioning key, extract the column names in the partitioning key to slice.
	partCols, err := extractPartitionColumns(sctx, partExpr, tblInfo)
	if err != nil {
		trace_util_0.Count(_partition_00000, 183)
		return err
	}

	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	trace_util_0.Count(_partition_00000, 181)
	if !checkUniqueKeyIncludePartKey(partCols, idxColNames) {
		trace_util_0.Count(_partition_00000, 184)
		return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
	}
	trace_util_0.Count(_partition_00000, 182)
	return nil
}

func extractPartitionColumns(sctx sessionctx.Context, partExpr string, tblInfo *model.TableInfo) ([]*expression.Column, error) {
	trace_util_0.Count(_partition_00000, 185)
	e, err := expression.ParseSimpleExprWithTableInfo(sctx, partExpr, tblInfo)
	if err != nil {
		trace_util_0.Count(_partition_00000, 187)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_partition_00000, 186)
	return expression.ExtractColumns(e), nil
}

// checkUniqueKeyIncludePartKey checks that the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partCols []*expression.Column, idxCols []*ast.IndexColName) bool {
	trace_util_0.Count(_partition_00000, 188)
	for _, partCol := range partCols {
		trace_util_0.Count(_partition_00000, 190)
		if !findColumnInIndexCols(partCol, idxCols) {
			trace_util_0.Count(_partition_00000, 191)
			return false
		}
	}
	trace_util_0.Count(_partition_00000, 189)
	return true
}

// isRangePartitionColUnsignedBigint returns true if the partitioning key column type is unsigned bigint type.
func isRangePartitionColUnsignedBigint(cols []*table.Column, pi *model.PartitionInfo) bool {
	trace_util_0.Count(_partition_00000, 192)
	for _, col := range cols {
		trace_util_0.Count(_partition_00000, 194)
		isUnsigned := col.Tp == mysql.TypeLonglong && mysql.HasUnsignedFlag(col.Flag)
		if isUnsigned && strings.Contains(strings.ToLower(pi.Expr), col.Name.L) {
			trace_util_0.Count(_partition_00000, 195)
			return true
		}
	}
	trace_util_0.Count(_partition_00000, 193)
	return false
}

// truncateTableByReassignPartitionIDs reassigns new partition ids.
func truncateTableByReassignPartitionIDs(t *meta.Meta, tblInfo *model.TableInfo) error {
	trace_util_0.Count(_partition_00000, 196)
	newDefs := make([]model.PartitionDefinition, 0, len(tblInfo.Partition.Definitions))
	for _, def := range tblInfo.Partition.Definitions {
		trace_util_0.Count(_partition_00000, 198)
		pid, err := t.GenGlobalID()
		if err != nil {
			trace_util_0.Count(_partition_00000, 201)
			return errors.Trace(err)
		}

		trace_util_0.Count(_partition_00000, 199)
		var newDef model.PartitionDefinition
		if tblInfo.Partition.Type == model.PartitionTypeHash {
			trace_util_0.Count(_partition_00000, 202)
			newDef = model.PartitionDefinition{
				ID: pid,
			}
		} else {
			trace_util_0.Count(_partition_00000, 203)
			if tblInfo.Partition.Type == model.PartitionTypeRange {
				trace_util_0.Count(_partition_00000, 204)
				newDef = model.PartitionDefinition{
					ID:       pid,
					Name:     def.Name,
					LessThan: def.LessThan,
					Comment:  def.Comment,
				}
			}
		}
		trace_util_0.Count(_partition_00000, 200)
		newDefs = append(newDefs, newDef)
	}
	trace_util_0.Count(_partition_00000, 197)
	tblInfo.Partition.Definitions = newDefs
	return nil
}

var _partition_00000 = "ddl/partition.go"
