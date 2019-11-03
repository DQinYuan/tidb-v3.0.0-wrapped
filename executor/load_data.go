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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	baseExecutor

	IsLocal      bool
	OnDuplicate  ast.OnDuplicateKeyHandlingType
	loadDataInfo *LoadDataInfo
}

var insertValuesLabel fmt.Stringer = stringutil.StringerStr("InsertValues")

// NewLoadDataInfo returns a LoadDataInfo structure, and it's only used for tests now.
func NewLoadDataInfo(ctx sessionctx.Context, row []types.Datum, tbl table.Table, cols []*table.Column) *LoadDataInfo {
	trace_util_0.Count(_load_data_00000, 0)
	insertVal := &InsertValues{baseExecutor: newBaseExecutor(ctx, nil, insertValuesLabel), Table: tbl}
	return &LoadDataInfo{
		row:          row,
		InsertValues: insertVal,
		Table:        tbl,
		Ctx:          ctx,
	}
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	trace_util_0.Count(_load_data_00000, 1)
	req.GrowAndReset(e.maxChunkSize)
	// TODO: support load data without local field.
	if !e.IsLocal {
		trace_util_0.Count(_load_data_00000, 7)
		return errors.New("Load Data: don't support load data without local field")
	}
	// TODO: support load data with replace field.
	trace_util_0.Count(_load_data_00000, 2)
	if e.OnDuplicate == ast.OnDuplicateKeyHandlingReplace {
		trace_util_0.Count(_load_data_00000, 8)
		return errors.New("Load Data: don't support load data with replace field")
	}
	// TODO: support lines terminated is "".
	trace_util_0.Count(_load_data_00000, 3)
	if len(e.loadDataInfo.LinesInfo.Terminated) == 0 {
		trace_util_0.Count(_load_data_00000, 9)
		return errors.New("Load Data: don't support load data terminated is nil")
	}

	trace_util_0.Count(_load_data_00000, 4)
	sctx := e.loadDataInfo.ctx
	val := sctx.Value(LoadDataVarKey)
	if val != nil {
		trace_util_0.Count(_load_data_00000, 10)
		sctx.SetValue(LoadDataVarKey, nil)
		return errors.New("Load Data: previous load data option isn't closed normal")
	}
	trace_util_0.Count(_load_data_00000, 5)
	if e.loadDataInfo.Path == "" {
		trace_util_0.Count(_load_data_00000, 11)
		return errors.New("Load Data: infile path is empty")
	}
	trace_util_0.Count(_load_data_00000, 6)
	sctx.SetValue(LoadDataVarKey, e.loadDataInfo)

	return nil
}

// Close implements the Executor Close interface.
func (e *LoadDataExec) Close() error {
	trace_util_0.Count(_load_data_00000, 12)
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadDataExec) Open(ctx context.Context) error {
	trace_util_0.Count(_load_data_00000, 13)
	if e.loadDataInfo.insertColumns != nil {
		trace_util_0.Count(_load_data_00000, 15)
		e.loadDataInfo.initEvalBuffer()
	}
	trace_util_0.Count(_load_data_00000, 14)
	return nil
}

// LoadDataInfo saves the information of loading data operation.
type LoadDataInfo struct {
	*InsertValues

	row         []types.Datum
	Path        string
	Table       table.Table
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines uint64
	Ctx         sessionctx.Context
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (e *LoadDataInfo) SetMaxRowsInBatch(limit uint64) {
	trace_util_0.Count(_load_data_00000, 16)
	e.maxRowsInBatch = limit
}

// getValidData returns prevData and curData that starts from starting symbol.
// If the data doesn't have starting symbol, prevData is nil and curData is curData[len(curData)-startingLen+1:].
// If curData size less than startingLen, curData is returned directly.
func (e *LoadDataInfo) getValidData(prevData, curData []byte) ([]byte, []byte) {
	trace_util_0.Count(_load_data_00000, 17)
	startingLen := len(e.LinesInfo.Starting)
	if startingLen == 0 {
		trace_util_0.Count(_load_data_00000, 22)
		return prevData, curData
	}

	trace_util_0.Count(_load_data_00000, 18)
	prevLen := len(prevData)
	if prevLen > 0 {
		trace_util_0.Count(_load_data_00000, 23)
		// starting symbol in the prevData
		idx := strings.Index(string(prevData), e.LinesInfo.Starting)
		if idx != -1 {
			trace_util_0.Count(_load_data_00000, 26)
			return prevData[idx:], curData
		}

		// starting symbol in the middle of prevData and curData
		trace_util_0.Count(_load_data_00000, 24)
		restStart := curData
		if len(curData) >= startingLen {
			trace_util_0.Count(_load_data_00000, 27)
			restStart = curData[:startingLen-1]
		}
		trace_util_0.Count(_load_data_00000, 25)
		prevData = append(prevData, restStart...)
		idx = strings.Index(string(prevData), e.LinesInfo.Starting)
		if idx != -1 {
			trace_util_0.Count(_load_data_00000, 28)
			return prevData[idx:prevLen], curData
		}
	}

	// starting symbol in the curData
	trace_util_0.Count(_load_data_00000, 19)
	idx := strings.Index(string(curData), e.LinesInfo.Starting)
	if idx != -1 {
		trace_util_0.Count(_load_data_00000, 29)
		return nil, curData[idx:]
	}

	// no starting symbol
	trace_util_0.Count(_load_data_00000, 20)
	if len(curData) >= startingLen {
		trace_util_0.Count(_load_data_00000, 30)
		curData = curData[len(curData)-startingLen+1:]
	}
	trace_util_0.Count(_load_data_00000, 21)
	return nil, curData
}

// getLine returns a line, curData, the next data start index and a bool value.
// If it has starting symbol the bool is true, otherwise is false.
func (e *LoadDataInfo) getLine(prevData, curData []byte) ([]byte, []byte, bool) {
	trace_util_0.Count(_load_data_00000, 31)
	startingLen := len(e.LinesInfo.Starting)
	prevData, curData = e.getValidData(prevData, curData)
	if prevData == nil && len(curData) < startingLen {
		trace_util_0.Count(_load_data_00000, 38)
		return nil, curData, false
	}

	trace_util_0.Count(_load_data_00000, 32)
	prevLen := len(prevData)
	terminatedLen := len(e.LinesInfo.Terminated)
	curStartIdx := 0
	if prevLen < startingLen {
		trace_util_0.Count(_load_data_00000, 39)
		curStartIdx = startingLen - prevLen
	}
	trace_util_0.Count(_load_data_00000, 33)
	endIdx := -1
	if len(curData) >= curStartIdx {
		trace_util_0.Count(_load_data_00000, 40)
		endIdx = strings.Index(string(curData[curStartIdx:]), e.LinesInfo.Terminated)
	}
	trace_util_0.Count(_load_data_00000, 34)
	if endIdx == -1 {
		trace_util_0.Count(_load_data_00000, 41)
		// no terminated symbol
		if len(prevData) == 0 {
			trace_util_0.Count(_load_data_00000, 44)
			return nil, curData, true
		}

		// terminated symbol in the middle of prevData and curData
		trace_util_0.Count(_load_data_00000, 42)
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(curData[startingLen:]), e.LinesInfo.Terminated)
		if endIdx != -1 {
			trace_util_0.Count(_load_data_00000, 45)
			nextDataIdx := startingLen + endIdx + terminatedLen
			return curData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
		}
		// no terminated symbol
		trace_util_0.Count(_load_data_00000, 43)
		return nil, curData, true
	}

	// terminated symbol in the curData
	trace_util_0.Count(_load_data_00000, 35)
	nextDataIdx := curStartIdx + endIdx + terminatedLen
	if len(prevData) == 0 {
		trace_util_0.Count(_load_data_00000, 46)
		return curData[curStartIdx : curStartIdx+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the curData
	trace_util_0.Count(_load_data_00000, 36)
	prevData = append(prevData, curData[:nextDataIdx]...)
	endIdx = strings.Index(string(prevData[startingLen:]), e.LinesInfo.Terminated)
	if endIdx >= prevLen {
		trace_util_0.Count(_load_data_00000, 47)
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	trace_util_0.Count(_load_data_00000, 37)
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}

// InsertData inserts data into specified table according to the specified format.
// If it has the rest of data isn't completed the processing, then it returns without completed data.
// If the number of inserted rows reaches the batchRows, then the second return value is true.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true.
func (e *LoadDataInfo) InsertData(prevData, curData []byte) ([]byte, bool, error) {
	trace_util_0.Count(_load_data_00000, 48)
	if len(prevData) == 0 && len(curData) == 0 {
		trace_util_0.Count(_load_data_00000, 53)
		return nil, false, nil
	}
	trace_util_0.Count(_load_data_00000, 49)
	var line []byte
	var isEOF, hasStarting, reachLimit bool
	if len(prevData) > 0 && len(curData) == 0 {
		trace_util_0.Count(_load_data_00000, 54)
		isEOF = true
		prevData, curData = curData, prevData
	}
	trace_util_0.Count(_load_data_00000, 50)
	rows := make([][]types.Datum, 0, e.maxRowsInBatch)
	for len(curData) > 0 {
		trace_util_0.Count(_load_data_00000, 55)
		line, curData, hasStarting = e.getLine(prevData, curData)
		prevData = nil
		// If it doesn't find the terminated symbol and this data isn't the last data,
		// the data can't be inserted.
		if line == nil && !isEOF {
			trace_util_0.Count(_load_data_00000, 61)
			break
		}
		// If doesn't find starting symbol, this data can't be inserted.
		trace_util_0.Count(_load_data_00000, 56)
		if !hasStarting {
			trace_util_0.Count(_load_data_00000, 62)
			if isEOF {
				trace_util_0.Count(_load_data_00000, 64)
				curData = nil
			}
			trace_util_0.Count(_load_data_00000, 63)
			break
		}
		trace_util_0.Count(_load_data_00000, 57)
		if line == nil && isEOF {
			trace_util_0.Count(_load_data_00000, 65)
			line = curData[len(e.LinesInfo.Starting):]
			curData = nil
		}

		trace_util_0.Count(_load_data_00000, 58)
		if e.IgnoreLines > 0 {
			trace_util_0.Count(_load_data_00000, 66)
			e.IgnoreLines--
			continue
		}
		trace_util_0.Count(_load_data_00000, 59)
		cols, err := e.getFieldsFromLine(line)
		if err != nil {
			trace_util_0.Count(_load_data_00000, 67)
			return nil, false, err
		}
		trace_util_0.Count(_load_data_00000, 60)
		rows = append(rows, e.colsToRow(cols))
		e.rowCount++
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			trace_util_0.Count(_load_data_00000, 68)
			reachLimit = true
			logutil.Logger(context.Background()).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			break
		}
	}
	trace_util_0.Count(_load_data_00000, 51)
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(rows)))
	err := e.batchCheckAndInsert(rows, e.addRecordLD)
	if err != nil {
		trace_util_0.Count(_load_data_00000, 69)
		return nil, reachLimit, err
	}
	trace_util_0.Count(_load_data_00000, 52)
	return curData, reachLimit, nil
}

// SetMessage sets info message(ERR_LOAD_INFO) generated by LOAD statement, it is public because of the special way that
// LOAD statement is handled.
func (e *LoadDataInfo) SetMessage() {
	trace_util_0.Count(_load_data_00000, 70)
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	numDeletes := 0
	numSkipped := numRecords - stmtCtx.CopiedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo], numRecords, numDeletes, numSkipped, numWarnings)
	e.ctx.GetSessionVars().StmtCtx.SetMessage(msg)
}

func (e *LoadDataInfo) colsToRow(cols []field) []types.Datum {
	trace_util_0.Count(_load_data_00000, 71)
	for i := 0; i < len(e.row); i++ {
		trace_util_0.Count(_load_data_00000, 74)
		if i >= len(cols) {
			trace_util_0.Count(_load_data_00000, 76)
			e.row[i].SetNull()
			continue
		}
		// The field with only "\N" in it is handled as NULL in the csv file.
		// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
		trace_util_0.Count(_load_data_00000, 75)
		if cols[i].maybeNull && string(cols[i].str) == "N" {
			trace_util_0.Count(_load_data_00000, 77)
			e.row[i].SetNull()
		} else {
			trace_util_0.Count(_load_data_00000, 78)
			{
				e.row[i].SetString(string(cols[i].str))
			}
		}
	}
	trace_util_0.Count(_load_data_00000, 72)
	row, err := e.getRow(e.row)
	if err != nil {
		trace_util_0.Count(_load_data_00000, 79)
		e.handleWarning(err)
		return nil
	}
	trace_util_0.Count(_load_data_00000, 73)
	return row
}

func (e *LoadDataInfo) addRecordLD(row []types.Datum) (int64, error) {
	trace_util_0.Count(_load_data_00000, 80)
	if row == nil {
		trace_util_0.Count(_load_data_00000, 83)
		return 0, nil
	}
	trace_util_0.Count(_load_data_00000, 81)
	h, err := e.addRecord(row)
	if err != nil {
		trace_util_0.Count(_load_data_00000, 84)
		e.handleWarning(err)
	}
	trace_util_0.Count(_load_data_00000, 82)
	return h, nil
}

type field struct {
	str       []byte
	maybeNull bool
	enclosed  bool
}

type fieldWriter struct {
	pos           int
	enclosedChar  byte
	fieldTermChar byte
	term          *string
	isEnclosed    bool
	isLineStart   bool
	isFieldStart  bool
	ReadBuf       *[]byte
	OutputBuf     []byte
}

func (w *fieldWriter) Init(enclosedChar byte, fieldTermChar byte, readBuf *[]byte, term *string) {
	trace_util_0.Count(_load_data_00000, 85)
	w.isEnclosed = false
	w.isLineStart = true
	w.isFieldStart = true
	w.ReadBuf = readBuf
	w.enclosedChar = enclosedChar
	w.fieldTermChar = fieldTermChar
	w.term = term
}

func (w *fieldWriter) putback() {
	trace_util_0.Count(_load_data_00000, 86)
	w.pos--
}

func (w *fieldWriter) getChar() (bool, byte) {
	trace_util_0.Count(_load_data_00000, 87)
	if w.pos < len(*w.ReadBuf) {
		trace_util_0.Count(_load_data_00000, 89)
		ret := (*w.ReadBuf)[w.pos]
		w.pos++
		return true, ret
	}
	trace_util_0.Count(_load_data_00000, 88)
	return false, 0
}

func (w *fieldWriter) isTerminator() bool {
	trace_util_0.Count(_load_data_00000, 90)
	chkpt, isterm := w.pos, true
	for i := 1; i < len(*w.term); i++ {
		trace_util_0.Count(_load_data_00000, 93)
		flag, ch := w.getChar()
		if !flag || ch != (*w.term)[i] {
			trace_util_0.Count(_load_data_00000, 94)
			isterm = false
			break
		}
	}
	trace_util_0.Count(_load_data_00000, 91)
	if !isterm {
		trace_util_0.Count(_load_data_00000, 95)
		w.pos = chkpt
		return false
	}
	trace_util_0.Count(_load_data_00000, 92)
	return true
}

func (w *fieldWriter) outputField(enclosed bool) field {
	trace_util_0.Count(_load_data_00000, 96)
	var fild []byte
	start := 0
	if enclosed {
		trace_util_0.Count(_load_data_00000, 100)
		start = 1
	}
	trace_util_0.Count(_load_data_00000, 97)
	for i := start; i < len(w.OutputBuf); i++ {
		trace_util_0.Count(_load_data_00000, 101)
		fild = append(fild, w.OutputBuf[i])
	}
	trace_util_0.Count(_load_data_00000, 98)
	if len(fild) == 0 {
		trace_util_0.Count(_load_data_00000, 102)
		fild = []byte("")
	}
	trace_util_0.Count(_load_data_00000, 99)
	w.OutputBuf = w.OutputBuf[0:0]
	w.isEnclosed = false
	w.isFieldStart = true
	return field{fild, false, enclosed}
}

func (w *fieldWriter) GetField() (bool, field) {
	trace_util_0.Count(_load_data_00000, 103)
	// The first return value implies whether fieldWriter read the last character of line.
	if w.isLineStart {
		trace_util_0.Count(_load_data_00000, 105)
		_, ch := w.getChar()
		if ch == w.enclosedChar {
			trace_util_0.Count(_load_data_00000, 106)
			w.isEnclosed = true
			w.isFieldStart, w.isLineStart = false, false
			w.OutputBuf = append(w.OutputBuf, ch)
		} else {
			trace_util_0.Count(_load_data_00000, 107)
			{
				w.putback()
			}
		}
	}
	trace_util_0.Count(_load_data_00000, 104)
	for {
		trace_util_0.Count(_load_data_00000, 108)
		flag, ch := w.getChar()
		if !flag {
			trace_util_0.Count(_load_data_00000, 111)
			ret := w.outputField(false)
			return true, ret
		}
		trace_util_0.Count(_load_data_00000, 109)
		if ch == w.enclosedChar && w.isFieldStart {
			trace_util_0.Count(_load_data_00000, 112)
			// If read enclosed char at field start.
			w.isEnclosed = true
			w.OutputBuf = append(w.OutputBuf, ch)
			w.isLineStart, w.isFieldStart = false, false
			continue
		}
		trace_util_0.Count(_load_data_00000, 110)
		w.isLineStart, w.isFieldStart = false, false
		if ch == w.fieldTermChar && !w.isEnclosed {
			trace_util_0.Count(_load_data_00000, 113)
			// If read filed terminate char.
			if w.isTerminator() {
				trace_util_0.Count(_load_data_00000, 115)
				ret := w.outputField(false)
				return false, ret
			}
			trace_util_0.Count(_load_data_00000, 114)
			w.OutputBuf = append(w.OutputBuf, ch)
		} else {
			trace_util_0.Count(_load_data_00000, 116)
			if ch == w.enclosedChar && w.isEnclosed {
				trace_util_0.Count(_load_data_00000, 117)
				// If read enclosed char, look ahead.
				flag, ch = w.getChar()
				if !flag {
					trace_util_0.Count(_load_data_00000, 118)
					ret := w.outputField(true)
					return true, ret
				} else {
					trace_util_0.Count(_load_data_00000, 119)
					if ch == w.enclosedChar {
						trace_util_0.Count(_load_data_00000, 120)
						w.OutputBuf = append(w.OutputBuf, ch)
						continue
					} else {
						trace_util_0.Count(_load_data_00000, 121)
						if ch == w.fieldTermChar {
							trace_util_0.Count(_load_data_00000, 122)
							// If the next char is fieldTermChar, look ahead.
							if w.isTerminator() {
								trace_util_0.Count(_load_data_00000, 124)
								ret := w.outputField(true)
								return false, ret
							}
							trace_util_0.Count(_load_data_00000, 123)
							w.OutputBuf = append(w.OutputBuf, ch)
						} else {
							trace_util_0.Count(_load_data_00000, 125)
							{
								// If there is no terminator behind enclosedChar, put the char back.
								w.OutputBuf = append(w.OutputBuf, w.enclosedChar)
								w.putback()
							}
						}
					}
				}
			} else {
				trace_util_0.Count(_load_data_00000, 126)
				if ch == '\\' {
					trace_util_0.Count(_load_data_00000, 127)
					// TODO: escape only support '\'
					w.OutputBuf = append(w.OutputBuf, ch)
					flag, ch = w.getChar()
					if flag {
						trace_util_0.Count(_load_data_00000, 128)
						if ch == w.enclosedChar {
							trace_util_0.Count(_load_data_00000, 129)
							w.OutputBuf = append(w.OutputBuf, ch)
						} else {
							trace_util_0.Count(_load_data_00000, 130)
							{
								w.putback()
							}
						}
					}
				} else {
					trace_util_0.Count(_load_data_00000, 131)
					{
						w.OutputBuf = append(w.OutputBuf, ch)
					}
				}
			}
		}
	}
}

// getFieldsFromLine splits line according to fieldsInfo.
func (e *LoadDataInfo) getFieldsFromLine(line []byte) ([]field, error) {
	trace_util_0.Count(_load_data_00000, 132)
	var (
		reader fieldWriter
		fields []field
	)

	if len(line) == 0 {
		trace_util_0.Count(_load_data_00000, 135)
		str := []byte("")
		fields = append(fields, field{str, false, false})
		return fields, nil
	}

	trace_util_0.Count(_load_data_00000, 133)
	reader.Init(e.FieldsInfo.Enclosed, e.FieldsInfo.Terminated[0], &line, &e.FieldsInfo.Terminated)
	for {
		trace_util_0.Count(_load_data_00000, 136)
		eol, f := reader.GetField()
		f = f.escape()
		if string(f.str) == "NULL" && !f.enclosed {
			trace_util_0.Count(_load_data_00000, 138)
			f.str = []byte{'N'}
			f.maybeNull = true
		}
		trace_util_0.Count(_load_data_00000, 137)
		fields = append(fields, f)
		if eol {
			trace_util_0.Count(_load_data_00000, 139)
			break
		}
	}
	trace_util_0.Count(_load_data_00000, 134)
	return fields, nil
}

// escape handles escape characters when running load data statement.
// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
// TODO: escape only support '\' as the `ESCAPED BY` character, it should support specify characters.
func (f *field) escape() field {
	trace_util_0.Count(_load_data_00000, 140)
	pos := 0
	for i := 0; i < len(f.str); i++ {
		trace_util_0.Count(_load_data_00000, 142)
		c := f.str[i]
		if i+1 < len(f.str) && f.str[i] == '\\' {
			trace_util_0.Count(_load_data_00000, 144)
			c = f.escapeChar(f.str[i+1])
			i++
		}

		trace_util_0.Count(_load_data_00000, 143)
		f.str[pos] = c
		pos++
	}
	trace_util_0.Count(_load_data_00000, 141)
	return field{f.str[:pos], f.maybeNull, f.enclosed}
}

func (f *field) escapeChar(c byte) byte {
	trace_util_0.Count(_load_data_00000, 145)
	switch c {
	case '0':
		trace_util_0.Count(_load_data_00000, 146)
		return 0
	case 'b':
		trace_util_0.Count(_load_data_00000, 147)
		return '\b'
	case 'n':
		trace_util_0.Count(_load_data_00000, 148)
		return '\n'
	case 'r':
		trace_util_0.Count(_load_data_00000, 149)
		return '\r'
	case 't':
		trace_util_0.Count(_load_data_00000, 150)
		return '\t'
	case 'Z':
		trace_util_0.Count(_load_data_00000, 151)
		return 26
	case 'N':
		trace_util_0.Count(_load_data_00000, 152)
		f.maybeNull = true
		return c
	case '\\':
		trace_util_0.Count(_load_data_00000, 153)
		return c
	default:
		trace_util_0.Count(_load_data_00000, 154)
		return c
	}
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadDataVarKeyType) String() string {
	trace_util_0.Count(_load_data_00000, 155)
	return "load_data_var"
}

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0

var _load_data_00000 = "executor/load_data.go"
