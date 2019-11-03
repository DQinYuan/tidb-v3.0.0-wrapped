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

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime/pprof"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/filesort"
	"github.com/pingcap/tidb/util/logutil"
)

type comparableRow struct {
	key    []types.Datum
	val    []types.Datum
	handle int64
}

var (
	genCmd = flag.NewFlagSet("gen", flag.ExitOnError)
	runCmd = flag.NewFlagSet("run", flag.ExitOnError)

	logLevel    = "warn"
	cpuprofile  string
	tmpDir      string
	keySize     int
	valSize     int
	bufSize     int
	scale       int
	nWorkers    int
	inputRatio  int
	outputRatio int
)

func nextRow(r *rand.Rand, keySize int, valSize int) *comparableRow {
	trace_util_0.Count(_main_00000, 0)
	key := make([]types.Datum, keySize)
	for i := range key {
		trace_util_0.Count(_main_00000, 3)
		key[i] = types.NewDatum(r.Int())
	}

	trace_util_0.Count(_main_00000, 1)
	val := make([]types.Datum, valSize)
	for j := range val {
		trace_util_0.Count(_main_00000, 4)
		val[j] = types.NewDatum(r.Int())
	}

	trace_util_0.Count(_main_00000, 2)
	handle := r.Int63()
	return &comparableRow{key: key, val: val, handle: handle}
}

func encodeRow(b []byte, row *comparableRow) ([]byte, error) {
	trace_util_0.Count(_main_00000, 5)
	var (
		err  error
		head = make([]byte, 8)
		body []byte
	)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	body, err = codec.EncodeKey(sc, body, row.key...)
	if err != nil {
		trace_util_0.Count(_main_00000, 9)
		return b, errors.Trace(err)
	}
	trace_util_0.Count(_main_00000, 6)
	body, err = codec.EncodeKey(sc, body, row.val...)
	if err != nil {
		trace_util_0.Count(_main_00000, 10)
		return b, errors.Trace(err)
	}
	trace_util_0.Count(_main_00000, 7)
	body, err = codec.EncodeKey(sc, body, types.NewIntDatum(row.handle))
	if err != nil {
		trace_util_0.Count(_main_00000, 11)
		return b, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 8)
	binary.BigEndian.PutUint64(head, uint64(len(body)))

	b = append(b, head...)
	b = append(b, body...)

	return b, nil
}

func decodeRow(fd *os.File) (*comparableRow, error) {
	trace_util_0.Count(_main_00000, 12)
	var (
		err  error
		n    int
		head = make([]byte, 8)
		dcod = make([]types.Datum, 0, keySize+valSize+1)
	)

	n, err = fd.Read(head)
	if n != 8 {
		trace_util_0.Count(_main_00000, 18)
		return nil, errors.New("incorrect header")
	}
	trace_util_0.Count(_main_00000, 13)
	if err != nil {
		trace_util_0.Count(_main_00000, 19)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 14)
	rowSize := int(binary.BigEndian.Uint64(head))
	rowBytes := make([]byte, rowSize)

	n, err = fd.Read(rowBytes)
	if n != rowSize {
		trace_util_0.Count(_main_00000, 20)
		return nil, errors.New("incorrect row")
	}
	trace_util_0.Count(_main_00000, 15)
	if err != nil {
		trace_util_0.Count(_main_00000, 21)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 16)
	dcod, err = codec.Decode(rowBytes, keySize+valSize+1)
	if err != nil {
		trace_util_0.Count(_main_00000, 22)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 17)
	return &comparableRow{
		key:    dcod[:keySize],
		val:    dcod[keySize : keySize+valSize],
		handle: dcod[keySize+valSize:][0].GetInt64(),
	}, nil
}

func encodeMeta(b []byte, scale int, keySize int, valSize int) []byte {
	trace_util_0.Count(_main_00000, 23)
	meta := make([]byte, 8)

	binary.BigEndian.PutUint64(meta, uint64(scale))
	b = append(b, meta...)
	binary.BigEndian.PutUint64(meta, uint64(keySize))
	b = append(b, meta...)
	binary.BigEndian.PutUint64(meta, uint64(valSize))
	b = append(b, meta...)

	return b
}

func decodeMeta(fd *os.File) error {
	trace_util_0.Count(_main_00000, 24)
	meta := make([]byte, 24)
	if n, err := fd.Read(meta); err != nil || n != 24 {
		trace_util_0.Count(_main_00000, 29)
		if n != 24 {
			trace_util_0.Count(_main_00000, 31)
			return errors.New("incorrect meta data")
		}
		trace_util_0.Count(_main_00000, 30)
		return errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 25)
	scale = int(binary.BigEndian.Uint64(meta[:8]))
	if scale <= 0 {
		trace_util_0.Count(_main_00000, 32)
		return errors.New("number of rows must be positive")
	}

	trace_util_0.Count(_main_00000, 26)
	keySize = int(binary.BigEndian.Uint64(meta[8:16]))
	if keySize <= 0 {
		trace_util_0.Count(_main_00000, 33)
		return errors.New("key size must be positive")
	}

	trace_util_0.Count(_main_00000, 27)
	valSize = int(binary.BigEndian.Uint64(meta[16:]))
	if valSize <= 0 {
		trace_util_0.Count(_main_00000, 34)
		return errors.New("value size must be positive")
	}

	trace_util_0.Count(_main_00000, 28)
	return nil
}

/*
 * The synthetic data is exported as a binary format.
 * The encoding format is:
 *   1) Meta Data
 *      Three 64-bit integers represent scale size, key size and value size.
 *   2) Row Data
 *      Each row is encoded as:
 *		One 64-bit integer represent the row size in bytes, followed by the
 *      the actual row bytes.
 */
func export() error {
	trace_util_0.Count(_main_00000, 35)
	var outputBytes []byte

	fileName := path.Join(tmpDir, "data.out")
	outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		trace_util_0.Count(_main_00000, 38)
		return errors.Trace(err)
	}
	trace_util_0.Count(_main_00000, 36)
	defer terror.Call(outputFile.Close)

	outputBytes = encodeMeta(outputBytes, scale, keySize, valSize)

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	for i := 1; i <= scale; i++ {
		trace_util_0.Count(_main_00000, 39)
		outputBytes, err = encodeRow(outputBytes, nextRow(r, keySize, valSize))
		if err != nil {
			trace_util_0.Count(_main_00000, 42)
			return errors.Trace(err)
		}
		trace_util_0.Count(_main_00000, 40)
		_, err = outputFile.Write(outputBytes)
		if err != nil {
			trace_util_0.Count(_main_00000, 43)
			return errors.Trace(err)
		}
		trace_util_0.Count(_main_00000, 41)
		outputBytes = outputBytes[:0]
	}

	trace_util_0.Count(_main_00000, 37)
	return nil
}

func load(ratio int) ([]*comparableRow, error) {
	trace_util_0.Count(_main_00000, 44)
	var (
		err error
		fd  *os.File
	)

	fileName := path.Join(tmpDir, "data.out")
	fd, err = os.Open(fileName)
	if os.IsNotExist(err) {
		trace_util_0.Count(_main_00000, 49)
		return nil, errors.New("data file (data.out) does not exist")
	}
	trace_util_0.Count(_main_00000, 45)
	if err != nil {
		trace_util_0.Count(_main_00000, 50)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_main_00000, 46)
	defer terror.Call(fd.Close)

	err = decodeMeta(fd)
	if err != nil {
		trace_util_0.Count(_main_00000, 51)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_main_00000, 47)
	cLogf("\tnumber of rows = %d, key size = %d, value size = %d", scale, keySize, valSize)

	var (
		row  *comparableRow
		rows = make([]*comparableRow, 0, scale)
	)

	totalRows := int(float64(scale) * (float64(ratio) / 100.0))
	cLogf("\tload %d rows", totalRows)
	for i := 1; i <= totalRows; i++ {
		trace_util_0.Count(_main_00000, 52)
		row, err = decodeRow(fd)
		if err != nil {
			trace_util_0.Count(_main_00000, 54)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_main_00000, 53)
		rows = append(rows, row)
	}

	trace_util_0.Count(_main_00000, 48)
	return rows, nil
}

func driveGenCmd() {
	trace_util_0.Count(_main_00000, 55)
	err := genCmd.Parse(os.Args[2:])
	terror.MustNil(err)
	// Sanity checks
	if keySize <= 0 {
		trace_util_0.Count(_main_00000, 60)
		log.Fatal("key size must be positive")
	}
	trace_util_0.Count(_main_00000, 56)
	if valSize <= 0 {
		trace_util_0.Count(_main_00000, 61)
		log.Fatal("value size must be positive")
	}
	trace_util_0.Count(_main_00000, 57)
	if scale <= 0 {
		trace_util_0.Count(_main_00000, 62)
		log.Fatal("scale must be positive")
	}
	trace_util_0.Count(_main_00000, 58)
	if _, err = os.Stat(tmpDir); err != nil {
		trace_util_0.Count(_main_00000, 63)
		if os.IsNotExist(err) {
			trace_util_0.Count(_main_00000, 65)
			log.Fatal("tmpDir does not exist")
		}
		trace_util_0.Count(_main_00000, 64)
		log.Fatal(err.Error())
	}

	trace_util_0.Count(_main_00000, 59)
	cLog("Generating...")
	start := time.Now()
	err = export()
	terror.MustNil(err)
	cLog("Done!")
	cLogf("Data placed in: %s", path.Join(tmpDir, "data.out"))
	cLog("Time used: ", time.Since(start))
	cLog("=================================")
}

func driveRunCmd() {
	trace_util_0.Count(_main_00000, 66)
	err := runCmd.Parse(os.Args[2:])
	terror.MustNil(err)
	// Sanity checks
	if bufSize <= 0 {
		trace_util_0.Count(_main_00000, 78)
		log.Fatal("buffer size must be positive")
	}
	trace_util_0.Count(_main_00000, 67)
	if nWorkers <= 0 {
		trace_util_0.Count(_main_00000, 79)
		log.Fatal("the number of workers must be positive")
	}
	trace_util_0.Count(_main_00000, 68)
	if inputRatio < 0 || inputRatio > 100 {
		trace_util_0.Count(_main_00000, 80)
		log.Fatal("input ratio must between 0 and 100 (inclusive)")
	}
	trace_util_0.Count(_main_00000, 69)
	if outputRatio < 0 || outputRatio > 100 {
		trace_util_0.Count(_main_00000, 81)
		log.Fatal("output ratio must between 0 and 100 (inclusive)")
	}
	trace_util_0.Count(_main_00000, 70)
	if _, err = os.Stat(tmpDir); err != nil {
		trace_util_0.Count(_main_00000, 82)
		if os.IsNotExist(err) {
			trace_util_0.Count(_main_00000, 84)
			log.Fatal("tmpDir does not exist")
		}
		trace_util_0.Count(_main_00000, 83)
		terror.MustNil(err)
	}

	trace_util_0.Count(_main_00000, 71)
	var (
		dir     string
		profile *os.File
		fs      *filesort.FileSorter
	)
	cLog("Loading...")
	start := time.Now()
	data, err := load(inputRatio)
	terror.MustNil(err)
	cLog("Done!")
	cLogf("Loaded %d rows", len(data))
	cLog("Time used: ", time.Since(start))
	cLog("=================================")

	sc := new(stmtctx.StatementContext)
	fsBuilder := new(filesort.Builder)
	byDesc := make([]bool, keySize)
	for i := 0; i < keySize; i++ {
		trace_util_0.Count(_main_00000, 85)
		byDesc[i] = false
	}
	trace_util_0.Count(_main_00000, 72)
	dir, err = ioutil.TempDir(tmpDir, "benchfilesort_test")
	terror.MustNil(err)
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(nWorkers).SetDesc(byDesc).SetDir(dir).Build()
	terror.MustNil(err)

	if cpuprofile != "" {
		trace_util_0.Count(_main_00000, 86)
		profile, err = os.Create(cpuprofile)
		terror.MustNil(err)
	}

	trace_util_0.Count(_main_00000, 73)
	cLog("Inputing...")
	start = time.Now()
	for _, r := range data {
		trace_util_0.Count(_main_00000, 87)
		err = fs.Input(r.key, r.val, r.handle)
		terror.MustNil(err)
	}
	trace_util_0.Count(_main_00000, 74)
	cLog("Done!")
	cLogf("Input %d rows", len(data))
	cLog("Time used: ", time.Since(start))
	cLog("=================================")

	cLog("Outputing...")
	totalRows := int(float64(len(data)) * (float64(outputRatio) / 100.0))
	start = time.Now()
	if cpuprofile != "" {
		trace_util_0.Count(_main_00000, 88)
		err = pprof.StartCPUProfile(profile)
		terror.MustNil(err)
	}
	trace_util_0.Count(_main_00000, 75)
	for i := 0; i < totalRows; i++ {
		trace_util_0.Count(_main_00000, 89)
		_, _, _, err = fs.Output()
		terror.MustNil(err)
	}
	trace_util_0.Count(_main_00000, 76)
	if cpuprofile != "" {
		trace_util_0.Count(_main_00000, 90)
		pprof.StopCPUProfile()
	}
	trace_util_0.Count(_main_00000, 77)
	cLog("Done!")
	cLogf("Output %d rows", totalRows)
	cLog("Time used: ", time.Since(start))
	cLog("=================================")

	cLog("Closing...")
	start = time.Now()
	err = fs.Close()
	terror.MustNil(err)
	cLog("Done!")
	cLog("Time used: ", time.Since(start))
	cLog("=================================")
}

func init() {
	trace_util_0.Count(_main_00000, 91)
	err := logutil.InitZapLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	terror.MustNil(err)
	cwd, err1 := os.Getwd()
	terror.MustNil(err1)

	genCmd.StringVar(&tmpDir, "dir", cwd, "where to store the generated rows")
	genCmd.IntVar(&keySize, "keySize", 8, "the size of key")
	genCmd.IntVar(&valSize, "valSize", 8, "the size of value")
	genCmd.IntVar(&scale, "scale", 100, "how many rows to generate")
	genCmd.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")

	runCmd.StringVar(&tmpDir, "dir", cwd, "where to load the generated rows")
	runCmd.IntVar(&bufSize, "bufSize", 500000, "how many rows held in memory at a time")
	runCmd.IntVar(&nWorkers, "nWorkers", 1, "how many workers used in async sorting")
	runCmd.IntVar(&inputRatio, "inputRatio", 100, "input percentage")
	runCmd.IntVar(&outputRatio, "outputRatio", 100, "output percentage")
	runCmd.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
}

func main() {
	trace_util_0.Count(_main_00000, 92)
	flag.Parse()

	if len(os.Args) == 1 {
		trace_util_0.Count(_main_00000, 94)
		fmt.Printf("Usage:\n\n")
		fmt.Printf("\tbenchfilesort command [arguments]\n\n")
		fmt.Printf("The commands are:\n\n")
		fmt.Println("\tgen\t", "generate rows")
		fmt.Println("\trun\t", "run tests")
		fmt.Println("")
		fmt.Println("Checkout benchfilesort/README for more information.")
		return
	}

	trace_util_0.Count(_main_00000, 93)
	switch os.Args[1] {
	case "gen":
		trace_util_0.Count(_main_00000, 95)
		driveGenCmd()
	case "run":
		trace_util_0.Count(_main_00000, 96)
		driveRunCmd()
	default:
		trace_util_0.Count(_main_00000, 97)
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}
}

func cLogf(format string, args ...interface{}) {
	trace_util_0.Count(_main_00000, 98)
	str := fmt.Sprintf(format, args...)
	fmt.Println("\033[0;32m" + str + "\033[0m")
}

func cLog(args ...interface{}) {
	trace_util_0.Count(_main_00000, 99)
	str := fmt.Sprint(args...)
	fmt.Println("\033[0;32m" + str + "\033[0m")
}

var _main_00000 = "cmd/benchfilesort/main.go"
