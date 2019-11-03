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

package filesort

import (
	"container/heap"
	"encoding/binary"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

type comparableRow struct {
	key    []types.Datum
	val    []types.Datum
	handle int64
}

type item struct {
	index int // source file index
	value *comparableRow
}

// rowHeap maintains a min-heap property of comparableRows.
type rowHeap struct {
	sc     *stmtctx.StatementContext
	ims    []*item
	byDesc []bool
	err    error
}

var headSize = 8

func lessThan(sc *stmtctx.StatementContext, i []types.Datum, j []types.Datum, byDesc []bool) (bool, error) {
	trace_util_0.Count(_filesort_00000, 0)
	for k := range byDesc {
		trace_util_0.Count(_filesort_00000, 2)
		v1 := i[k]
		v2 := j[k]

		ret, err := v1.CompareDatum(sc, &v2)
		if err != nil {
			trace_util_0.Count(_filesort_00000, 5)
			return false, errors.Trace(err)
		}

		trace_util_0.Count(_filesort_00000, 3)
		if byDesc[k] {
			trace_util_0.Count(_filesort_00000, 6)
			ret = -ret
		}

		trace_util_0.Count(_filesort_00000, 4)
		if ret < 0 {
			trace_util_0.Count(_filesort_00000, 7)
			return true, nil
		} else {
			trace_util_0.Count(_filesort_00000, 8)
			if ret > 0 {
				trace_util_0.Count(_filesort_00000, 9)
				return false, nil
			}
		}
	}
	trace_util_0.Count(_filesort_00000, 1)
	return false, nil
}

// Len implements heap.Interface Len interface.
func (rh *rowHeap) Len() int { trace_util_0.Count(_filesort_00000, 10); return len(rh.ims) }

// Swap implements heap.Interface Swap interface.
func (rh *rowHeap) Swap(i, j int) {
	trace_util_0.Count(_filesort_00000, 11)
	rh.ims[i], rh.ims[j] = rh.ims[j], rh.ims[i]
}

// Less implements heap.Interface Less interface.
func (rh *rowHeap) Less(i, j int) bool {
	trace_util_0.Count(_filesort_00000, 12)
	l := rh.ims[i].value.key
	r := rh.ims[j].value.key
	ret, err := lessThan(rh.sc, l, r, rh.byDesc)
	if rh.err == nil {
		trace_util_0.Count(_filesort_00000, 14)
		rh.err = err
	}
	trace_util_0.Count(_filesort_00000, 13)
	return ret
}

// Push pushes an element into rowHeap.
func (rh *rowHeap) Push(x interface{}) {
	trace_util_0.Count(_filesort_00000, 15)
	rh.ims = append(rh.ims, x.(*item))
}

// Pop pops the last element from rowHeap.
func (rh *rowHeap) Pop() interface{} {
	trace_util_0.Count(_filesort_00000, 16)
	old := rh.ims
	n := len(old)
	x := old[n-1]
	rh.ims = old[0 : n-1]
	return x
}

// FileSorter sorts the given rows according to the byDesc order.
// FileSorter can sort rows that exceed predefined memory capacity.
type FileSorter struct {
	sc     *stmtctx.StatementContext
	byDesc []bool

	workers  []*Worker
	nWorkers int // number of workers used in async sorting
	cWorker  int // the next worker to which the sorting job is sent

	mu       sync.Mutex
	wg       sync.WaitGroup
	tmpDir   string
	files    []string
	nFiles   int
	closed   bool
	fetched  bool
	external bool // mark the necessity of performing external file sort
	cursor   int  // required when performing full in-memory sort

	rowHeap    *rowHeap
	fds        []*os.File
	rowBytes   []byte
	head       []byte
	dcod       []types.Datum
	keySize    int
	valSize    int
	maxRowSize int
}

// Worker sorts file asynchronously.
type Worker struct {
	ctx     *FileSorter
	busy    int32
	keySize int
	valSize int
	rowSize int
	bufSize int
	buf     []*comparableRow
	head    []byte
	err     error
}

// Builder builds a new FileSorter.
type Builder struct {
	sc       *stmtctx.StatementContext
	keySize  int
	valSize  int
	bufSize  int
	nWorkers int
	byDesc   []bool
	tmpDir   string
}

// SetSC sets StatementContext instance which is required in row comparison.
func (b *Builder) SetSC(sc *stmtctx.StatementContext) *Builder {
	trace_util_0.Count(_filesort_00000, 17)
	b.sc = sc
	return b
}

// SetSchema sets the schema of row, including key size and value size.
func (b *Builder) SetSchema(keySize, valSize int) *Builder {
	trace_util_0.Count(_filesort_00000, 18)
	b.keySize = keySize
	b.valSize = valSize
	return b
}

// SetBuf sets the number of rows FileSorter can hold in memory at a time.
func (b *Builder) SetBuf(bufSize int) *Builder {
	trace_util_0.Count(_filesort_00000, 19)
	b.bufSize = bufSize
	return b
}

// SetWorkers sets the number of workers used in async sorting.
func (b *Builder) SetWorkers(nWorkers int) *Builder {
	trace_util_0.Count(_filesort_00000, 20)
	b.nWorkers = nWorkers
	return b
}

// SetDesc sets the ordering rule of row comparison.
func (b *Builder) SetDesc(byDesc []bool) *Builder {
	trace_util_0.Count(_filesort_00000, 21)
	b.byDesc = byDesc
	return b
}

// SetDir sets the working directory for FileSorter.
func (b *Builder) SetDir(tmpDir string) *Builder {
	trace_util_0.Count(_filesort_00000, 22)
	b.tmpDir = tmpDir
	return b
}

// Build creates a FileSorter instance using given data.
func (b *Builder) Build() (*FileSorter, error) {
	trace_util_0.Count(_filesort_00000, 23)
	// Sanity checks
	if b.sc == nil {
		trace_util_0.Count(_filesort_00000, 32)
		return nil, errors.New("StatementContext is nil")
	}
	trace_util_0.Count(_filesort_00000, 24)
	if b.keySize != len(b.byDesc) {
		trace_util_0.Count(_filesort_00000, 33)
		return nil, errors.New("mismatch in key size and byDesc slice")
	}
	trace_util_0.Count(_filesort_00000, 25)
	if b.keySize <= 0 {
		trace_util_0.Count(_filesort_00000, 34)
		return nil, errors.New("key size is not positive")
	}
	trace_util_0.Count(_filesort_00000, 26)
	if b.valSize <= 0 {
		trace_util_0.Count(_filesort_00000, 35)
		return nil, errors.New("value size is not positive")
	}
	trace_util_0.Count(_filesort_00000, 27)
	if b.bufSize <= 0 {
		trace_util_0.Count(_filesort_00000, 36)
		return nil, errors.New("buffer size is not positive")
	}
	trace_util_0.Count(_filesort_00000, 28)
	_, err := os.Stat(b.tmpDir)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 37)
		if os.IsNotExist(err) {
			trace_util_0.Count(_filesort_00000, 39)
			return nil, errors.New("tmpDir does not exist")
		}
		trace_util_0.Count(_filesort_00000, 38)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_filesort_00000, 29)
	ws := make([]*Worker, b.nWorkers)
	for i := range ws {
		trace_util_0.Count(_filesort_00000, 40)
		ws[i] = &Worker{
			keySize: b.keySize,
			valSize: b.valSize,
			rowSize: b.keySize + b.valSize + 1,
			bufSize: b.bufSize / b.nWorkers,
			buf:     make([]*comparableRow, 0, b.bufSize/b.nWorkers),
			head:    make([]byte, headSize),
		}
	}

	trace_util_0.Count(_filesort_00000, 30)
	rh := &rowHeap{sc: b.sc,
		ims:    make([]*item, 0),
		byDesc: b.byDesc,
	}

	fs := &FileSorter{sc: b.sc,
		workers:  ws,
		nWorkers: b.nWorkers,
		cWorker:  0,

		head:    make([]byte, headSize),
		dcod:    make([]types.Datum, 0, b.keySize+b.valSize+1),
		keySize: b.keySize,
		valSize: b.valSize,

		tmpDir:  b.tmpDir,
		files:   make([]string, 0),
		byDesc:  b.byDesc,
		rowHeap: rh,
	}

	for i := 0; i < b.nWorkers; i++ {
		trace_util_0.Count(_filesort_00000, 41)
		fs.workers[i].ctx = fs
	}

	trace_util_0.Count(_filesort_00000, 31)
	return fs, nil
}

func (fs *FileSorter) getUniqueFileName() string {
	trace_util_0.Count(_filesort_00000, 42)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ret := path.Join(fs.tmpDir, strconv.Itoa(fs.nFiles))
	fs.nFiles++
	return ret
}

func (fs *FileSorter) appendFileName(fn string) {
	trace_util_0.Count(_filesort_00000, 43)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files = append(fs.files, fn)
}

func (fs *FileSorter) closeAllFiles() error {
	trace_util_0.Count(_filesort_00000, 44)
	var reportErr error
	for _, fd := range fs.fds {
		trace_util_0.Count(_filesort_00000, 48)
		err := fd.Close()
		if reportErr == nil {
			trace_util_0.Count(_filesort_00000, 49)
			reportErr = err
		}
	}
	trace_util_0.Count(_filesort_00000, 45)
	err := os.RemoveAll(fs.tmpDir)
	if reportErr == nil {
		trace_util_0.Count(_filesort_00000, 50)
		reportErr = err
	}
	trace_util_0.Count(_filesort_00000, 46)
	if reportErr != nil {
		trace_util_0.Count(_filesort_00000, 51)
		return errors.Trace(reportErr)
	}
	trace_util_0.Count(_filesort_00000, 47)
	return nil
}

// internalSort performs full in-memory sort.
func (fs *FileSorter) internalSort() (*comparableRow, error) {
	trace_util_0.Count(_filesort_00000, 52)
	w := fs.workers[fs.cWorker]

	if !fs.fetched {
		trace_util_0.Count(_filesort_00000, 55)
		sort.Sort(w)
		if w.err != nil {
			trace_util_0.Count(_filesort_00000, 57)
			return nil, errors.Trace(w.err)
		}
		trace_util_0.Count(_filesort_00000, 56)
		fs.fetched = true
	}
	trace_util_0.Count(_filesort_00000, 53)
	if fs.cursor < len(w.buf) {
		trace_util_0.Count(_filesort_00000, 58)
		r := w.buf[fs.cursor]
		fs.cursor++
		return r, nil
	}
	trace_util_0.Count(_filesort_00000, 54)
	return nil, nil
}

// externalSort performs external file sort.
func (fs *FileSorter) externalSort() (*comparableRow, error) {
	trace_util_0.Count(_filesort_00000, 59)
	if !fs.fetched {
		trace_util_0.Count(_filesort_00000, 62)
		// flush all remaining content to file (if any)
		for _, w := range fs.workers {
			trace_util_0.Count(_filesort_00000, 68)
			if atomic.LoadInt32(&(w.busy)) == 0 && len(w.buf) > 0 {
				trace_util_0.Count(_filesort_00000, 69)
				fs.wg.Add(1)
				go w.flushToFile()
			}
		}

		// wait for all workers to finish
		trace_util_0.Count(_filesort_00000, 63)
		fs.wg.Wait()

		// check errors from workers
		for _, w := range fs.workers {
			trace_util_0.Count(_filesort_00000, 70)
			if w.err != nil {
				trace_util_0.Count(_filesort_00000, 72)
				return nil, errors.Trace(w.err)
			}
			trace_util_0.Count(_filesort_00000, 71)
			if w.rowSize > fs.maxRowSize {
				trace_util_0.Count(_filesort_00000, 73)
				fs.maxRowSize = w.rowSize
			}
		}

		trace_util_0.Count(_filesort_00000, 64)
		heap.Init(fs.rowHeap)
		if fs.rowHeap.err != nil {
			trace_util_0.Count(_filesort_00000, 74)
			return nil, errors.Trace(fs.rowHeap.err)
		}

		trace_util_0.Count(_filesort_00000, 65)
		fs.rowBytes = make([]byte, fs.maxRowSize)

		err := fs.openAllFiles()
		if err != nil {
			trace_util_0.Count(_filesort_00000, 75)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_filesort_00000, 66)
		for id := range fs.fds {
			trace_util_0.Count(_filesort_00000, 76)
			row, err := fs.fetchNextRow(id)
			if err != nil {
				trace_util_0.Count(_filesort_00000, 79)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_filesort_00000, 77)
			if row == nil {
				trace_util_0.Count(_filesort_00000, 80)
				return nil, errors.New("file is empty")
			}

			trace_util_0.Count(_filesort_00000, 78)
			im := &item{
				index: id,
				value: row,
			}

			heap.Push(fs.rowHeap, im)
			if fs.rowHeap.err != nil {
				trace_util_0.Count(_filesort_00000, 81)
				return nil, errors.Trace(fs.rowHeap.err)
			}
		}

		trace_util_0.Count(_filesort_00000, 67)
		fs.fetched = true
	}

	trace_util_0.Count(_filesort_00000, 60)
	if fs.rowHeap.Len() > 0 {
		trace_util_0.Count(_filesort_00000, 82)
		im := heap.Pop(fs.rowHeap).(*item)
		if fs.rowHeap.err != nil {
			trace_util_0.Count(_filesort_00000, 86)
			return nil, errors.Trace(fs.rowHeap.err)
		}

		trace_util_0.Count(_filesort_00000, 83)
		row, err := fs.fetchNextRow(im.index)
		if err != nil {
			trace_util_0.Count(_filesort_00000, 87)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_filesort_00000, 84)
		if row != nil {
			trace_util_0.Count(_filesort_00000, 88)
			nextIm := &item{
				index: im.index,
				value: row,
			}

			heap.Push(fs.rowHeap, nextIm)
			if fs.rowHeap.err != nil {
				trace_util_0.Count(_filesort_00000, 89)
				return nil, errors.Trace(fs.rowHeap.err)
			}
		}

		trace_util_0.Count(_filesort_00000, 85)
		return im.value, nil
	}

	trace_util_0.Count(_filesort_00000, 61)
	return nil, nil
}

func (fs *FileSorter) openAllFiles() error {
	trace_util_0.Count(_filesort_00000, 90)
	for _, fname := range fs.files {
		trace_util_0.Count(_filesort_00000, 92)
		fd, err := os.Open(fname)
		if err != nil {
			trace_util_0.Count(_filesort_00000, 94)
			return errors.Trace(err)
		}
		trace_util_0.Count(_filesort_00000, 93)
		fs.fds = append(fs.fds, fd)
	}
	trace_util_0.Count(_filesort_00000, 91)
	return nil
}

// fetchNextRow fetches the next row given the source file index.
func (fs *FileSorter) fetchNextRow(index int) (*comparableRow, error) {
	trace_util_0.Count(_filesort_00000, 95)
	n, err := fs.fds[index].Read(fs.head)
	if err == io.EOF {
		trace_util_0.Count(_filesort_00000, 102)
		return nil, nil
	}
	trace_util_0.Count(_filesort_00000, 96)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 103)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_filesort_00000, 97)
	if n != headSize {
		trace_util_0.Count(_filesort_00000, 104)
		return nil, errors.New("incorrect header")
	}
	trace_util_0.Count(_filesort_00000, 98)
	rowSize := int(binary.BigEndian.Uint64(fs.head))

	n, err = fs.fds[index].Read(fs.rowBytes)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 105)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_filesort_00000, 99)
	if n != rowSize {
		trace_util_0.Count(_filesort_00000, 106)
		return nil, errors.New("incorrect row")
	}

	trace_util_0.Count(_filesort_00000, 100)
	fs.dcod, err = codec.Decode(fs.rowBytes, fs.keySize+fs.valSize+1)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 107)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_filesort_00000, 101)
	return &comparableRow{
		key:    fs.dcod[:fs.keySize],
		val:    fs.dcod[fs.keySize : fs.keySize+fs.valSize],
		handle: fs.dcod[fs.keySize+fs.valSize:][0].GetInt64(),
	}, nil
}

// Input adds one row into FileSorter.
// Caller should not call Input after calling Output.
func (fs *FileSorter) Input(key []types.Datum, val []types.Datum, handle int64) error {
	trace_util_0.Count(_filesort_00000, 108)
	if fs.closed {
		trace_util_0.Count(_filesort_00000, 112)
		return errors.New("FileSorter has been closed")
	}
	trace_util_0.Count(_filesort_00000, 109)
	if fs.fetched {
		trace_util_0.Count(_filesort_00000, 113)
		return errors.New("call input after output")
	}

	trace_util_0.Count(_filesort_00000, 110)
	assigned := false
	abortTime := time.Duration(1) * time.Minute           // 1 minute
	cooldownTime := time.Duration(100) * time.Millisecond // 100 milliseconds
	row := &comparableRow{
		key:    key,
		val:    val,
		handle: handle,
	}

	origin := time.Now()
	// assign input row to some worker in a round-robin way
	for {
		trace_util_0.Count(_filesort_00000, 114)
		for i := 0; i < fs.nWorkers; i++ {
			trace_util_0.Count(_filesort_00000, 117)
			wid := (fs.cWorker + i) % fs.nWorkers
			if atomic.LoadInt32(&(fs.workers[wid].busy)) == 0 {
				trace_util_0.Count(_filesort_00000, 118)
				fs.workers[wid].input(row)
				assigned = true
				fs.cWorker = wid
				break
			}
		}
		trace_util_0.Count(_filesort_00000, 115)
		if assigned {
			trace_util_0.Count(_filesort_00000, 119)
			break
		}

		// all workers are busy now, cooldown and retry
		trace_util_0.Count(_filesort_00000, 116)
		time.Sleep(cooldownTime)

		if time.Since(origin) >= abortTime {
			trace_util_0.Count(_filesort_00000, 120)
			// weird: all workers are busy for at least 1 min
			// choose to abort for safety
			return errors.New("can not make progress since all workers are busy")
		}
	}
	trace_util_0.Count(_filesort_00000, 111)
	return nil
}

// Output gets the next sorted row.
func (fs *FileSorter) Output() ([]types.Datum, []types.Datum, int64, error) {
	trace_util_0.Count(_filesort_00000, 121)
	var (
		r   *comparableRow
		err error
	)
	if fs.closed {
		trace_util_0.Count(_filesort_00000, 124)
		return nil, nil, 0, errors.New("FileSorter has been closed")
	}

	trace_util_0.Count(_filesort_00000, 122)
	if fs.external {
		trace_util_0.Count(_filesort_00000, 125)
		r, err = fs.externalSort()
	} else {
		trace_util_0.Count(_filesort_00000, 126)
		{
			r, err = fs.internalSort()
		}
	}

	trace_util_0.Count(_filesort_00000, 123)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 127)
		return nil, nil, 0, errors.Trace(err)
	} else {
		trace_util_0.Count(_filesort_00000, 128)
		if r != nil {
			trace_util_0.Count(_filesort_00000, 129)
			return r.key, r.val, r.handle, nil
		} else {
			trace_util_0.Count(_filesort_00000, 130)
			{
				return nil, nil, 0, nil
			}
		}
	}
}

// Close terminates the input or output process and discards all remaining data.
func (fs *FileSorter) Close() error {
	trace_util_0.Count(_filesort_00000, 131)
	if fs.closed {
		trace_util_0.Count(_filesort_00000, 135)
		return nil
	}
	trace_util_0.Count(_filesort_00000, 132)
	fs.wg.Wait()
	for _, w := range fs.workers {
		trace_util_0.Count(_filesort_00000, 136)
		w.buf = w.buf[:0]
	}
	trace_util_0.Count(_filesort_00000, 133)
	fs.closed = true
	err := fs.closeAllFiles()
	if err != nil {
		trace_util_0.Count(_filesort_00000, 137)
		return errors.Trace(err)
	}
	trace_util_0.Count(_filesort_00000, 134)
	return nil
}

func (w *Worker) Len() int { trace_util_0.Count(_filesort_00000, 138); return len(w.buf) }

func (w *Worker) Swap(i, j int) {
	trace_util_0.Count(_filesort_00000, 139)
	w.buf[i], w.buf[j] = w.buf[j], w.buf[i]
}

func (w *Worker) Less(i, j int) bool {
	trace_util_0.Count(_filesort_00000, 140)
	l := w.buf[i].key
	r := w.buf[j].key
	ret, err := lessThan(w.ctx.sc, l, r, w.ctx.byDesc)
	if w.err == nil {
		trace_util_0.Count(_filesort_00000, 142)
		w.err = errors.Trace(err)
	}
	trace_util_0.Count(_filesort_00000, 141)
	return ret
}

func (w *Worker) input(row *comparableRow) {
	trace_util_0.Count(_filesort_00000, 143)
	w.buf = append(w.buf, row)

	if len(w.buf) > w.bufSize {
		trace_util_0.Count(_filesort_00000, 144)
		atomic.StoreInt32(&(w.busy), int32(1))
		w.ctx.wg.Add(1)
		w.ctx.external = true
		go w.flushToFile()
	}
}

// flushToFile flushes the buffer to file if it is full.
func (w *Worker) flushToFile() {
	trace_util_0.Count(_filesort_00000, 145)
	defer w.ctx.wg.Done()
	var (
		outputByte []byte
		prevLen    int
	)

	sort.Sort(w)
	if w.err != nil {
		trace_util_0.Count(_filesort_00000, 150)
		return
	}

	trace_util_0.Count(_filesort_00000, 146)
	fileName := w.ctx.getUniqueFileName()

	outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 151)
		w.err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_filesort_00000, 147)
	defer terror.Call(outputFile.Close)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, row := range w.buf {
		trace_util_0.Count(_filesort_00000, 152)
		prevLen = len(outputByte)
		outputByte = append(outputByte, w.head...)
		outputByte, err = codec.EncodeKey(sc, outputByte, row.key...)
		if err != nil {
			trace_util_0.Count(_filesort_00000, 157)
			w.err = errors.Trace(err)
			return
		}
		trace_util_0.Count(_filesort_00000, 153)
		outputByte, err = codec.EncodeKey(sc, outputByte, row.val...)
		if err != nil {
			trace_util_0.Count(_filesort_00000, 158)
			w.err = errors.Trace(err)
			return
		}
		trace_util_0.Count(_filesort_00000, 154)
		outputByte, err = codec.EncodeKey(sc, outputByte, types.NewIntDatum(row.handle))
		if err != nil {
			trace_util_0.Count(_filesort_00000, 159)
			w.err = errors.Trace(err)
			return
		}

		trace_util_0.Count(_filesort_00000, 155)
		if len(outputByte)-prevLen-headSize > w.rowSize {
			trace_util_0.Count(_filesort_00000, 160)
			w.rowSize = len(outputByte) - prevLen - headSize
		}
		trace_util_0.Count(_filesort_00000, 156)
		binary.BigEndian.PutUint64(w.head, uint64(len(outputByte)-prevLen-headSize))
		for i := 0; i < headSize; i++ {
			trace_util_0.Count(_filesort_00000, 161)
			outputByte[prevLen+i] = w.head[i]
		}
	}

	trace_util_0.Count(_filesort_00000, 148)
	_, err = outputFile.Write(outputByte)
	if err != nil {
		trace_util_0.Count(_filesort_00000, 162)
		w.err = errors.Trace(err)
		return
	}

	trace_util_0.Count(_filesort_00000, 149)
	w.ctx.appendFileName(fileName)
	w.buf = w.buf[:0]
	atomic.StoreInt32(&(w.busy), int32(0))
	return
}

var _filesort_00000 = "util/filesort/filesort.go"
