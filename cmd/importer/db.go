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

package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/trace_util_0"
	"go.uber.org/zap"
)

func intRangeValue(column *column, min int64, max int64) (int64, int64) {
	trace_util_0.Count(_db_00000, 0)
	var err error
	if len(column.min) > 0 {
		trace_util_0.Count(_db_00000, 2)
		min, err = strconv.ParseInt(column.min, 10, 64)
		if err != nil {
			trace_util_0.Count(_db_00000, 4)
			log.Fatal(err.Error())
		}

		trace_util_0.Count(_db_00000, 3)
		if len(column.max) > 0 {
			trace_util_0.Count(_db_00000, 5)
			max, err = strconv.ParseInt(column.max, 10, 64)
			if err != nil {
				trace_util_0.Count(_db_00000, 6)
				log.Fatal(err.Error())
			}
		}
	}

	trace_util_0.Count(_db_00000, 1)
	return min, max
}

func randStringValue(column *column, n int) string {
	trace_util_0.Count(_db_00000, 7)
	if column.hist != nil {
		trace_util_0.Count(_db_00000, 10)
		if column.hist.avgLen == 0 {
			trace_util_0.Count(_db_00000, 12)
			column.hist.avgLen = column.hist.getAvgLen(n)
		}
		trace_util_0.Count(_db_00000, 11)
		return column.hist.randString()
	}
	trace_util_0.Count(_db_00000, 8)
	if len(column.set) > 0 {
		trace_util_0.Count(_db_00000, 13)
		idx := randInt(0, len(column.set)-1)
		return column.set[idx]
	}
	trace_util_0.Count(_db_00000, 9)
	return randString(randInt(1, n))
}

func randInt64Value(column *column, min int64, max int64) int64 {
	trace_util_0.Count(_db_00000, 14)
	if column.hist != nil {
		trace_util_0.Count(_db_00000, 17)
		return column.hist.randInt()
	}
	trace_util_0.Count(_db_00000, 15)
	if len(column.set) > 0 {
		trace_util_0.Count(_db_00000, 18)
		idx := randInt(0, len(column.set)-1)
		data, err := strconv.ParseInt(column.set[idx], 10, 64)
		if err != nil {
			trace_util_0.Count(_db_00000, 20)
			log.Warn("rand int64 failed", zap.Error(err))
		}
		trace_util_0.Count(_db_00000, 19)
		return data
	}

	trace_util_0.Count(_db_00000, 16)
	min, max = intRangeValue(column, min, max)
	return randInt64(min, max)
}

func nextInt64Value(column *column, min int64, max int64) int64 {
	trace_util_0.Count(_db_00000, 21)
	min, max = intRangeValue(column, min, max)
	column.data.setInitInt64Value(min, max)
	return column.data.nextInt64()
}

func intToDecimalString(intValue int64, decimal int) string {
	trace_util_0.Count(_db_00000, 22)
	data := fmt.Sprintf("%d", intValue)

	// add leading zero
	if len(data) < decimal {
		trace_util_0.Count(_db_00000, 26)
		data = strings.Repeat("0", decimal-len(data)) + data
	}

	trace_util_0.Count(_db_00000, 23)
	dec := data[len(data)-decimal:]
	if data = data[:len(data)-decimal]; data == "" {
		trace_util_0.Count(_db_00000, 27)
		data = "0"
	}
	trace_util_0.Count(_db_00000, 24)
	if dec != "" {
		trace_util_0.Count(_db_00000, 28)
		data = data + "." + dec
	}
	trace_util_0.Count(_db_00000, 25)
	return data
}

func genRowDatas(table *table, count int) ([]string, error) {
	trace_util_0.Count(_db_00000, 29)
	datas := make([]string, 0, count)
	for i := 0; i < count; i++ {
		trace_util_0.Count(_db_00000, 31)
		data, err := genRowData(table)
		if err != nil {
			trace_util_0.Count(_db_00000, 33)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_db_00000, 32)
		datas = append(datas, data)
	}

	trace_util_0.Count(_db_00000, 30)
	return datas, nil
}

func genRowData(table *table) (string, error) {
	trace_util_0.Count(_db_00000, 34)
	var values []byte
	for _, column := range table.columns {
		trace_util_0.Count(_db_00000, 36)
		data, err := genColumnData(table, column)
		if err != nil {
			trace_util_0.Count(_db_00000, 38)
			return "", errors.Trace(err)
		}
		trace_util_0.Count(_db_00000, 37)
		values = append(values, []byte(data)...)
		values = append(values, ',')
	}

	trace_util_0.Count(_db_00000, 35)
	values = values[:len(values)-1]
	sql := fmt.Sprintf("insert into %s (%s) values (%s);", table.name, table.columnList, string(values))
	return sql, nil
}

func genColumnData(table *table, column *column) (string, error) {
	trace_util_0.Count(_db_00000, 39)
	tp := column.tp
	incremental := column.incremental
	if incremental {
		trace_util_0.Count(_db_00000, 42)
		incremental = uint32(rand.Int31n(100))+1 <= column.data.probability
		// If incremental, there is only one worker, so it is safe to directly access datum.
		if !incremental && column.data.remains > 0 {
			trace_util_0.Count(_db_00000, 43)
			column.data.remains--
		}
	}
	trace_util_0.Count(_db_00000, 40)
	if _, ok := table.uniqIndices[column.name]; ok {
		trace_util_0.Count(_db_00000, 44)
		incremental = true
	}
	trace_util_0.Count(_db_00000, 41)
	isUnsigned := mysql.HasUnsignedFlag(tp.Flag)

	switch tp.Tp {
	case mysql.TypeTiny:
		trace_util_0.Count(_db_00000, 45)
		var data int64
		if incremental {
			trace_util_0.Count(_db_00000, 69)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 70)
				data = nextInt64Value(column, 0, math.MaxUint8)
			} else {
				trace_util_0.Count(_db_00000, 71)
				{
					data = nextInt64Value(column, math.MinInt8, math.MaxInt8)
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 72)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 73)
					data = randInt64Value(column, 0, math.MaxUint8)
				} else {
					trace_util_0.Count(_db_00000, 74)
					{
						data = randInt64Value(column, math.MinInt8, math.MaxInt8)
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 46)
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeShort:
		trace_util_0.Count(_db_00000, 47)
		var data int64
		if incremental {
			trace_util_0.Count(_db_00000, 75)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 76)
				data = nextInt64Value(column, 0, math.MaxUint16)
			} else {
				trace_util_0.Count(_db_00000, 77)
				{
					data = nextInt64Value(column, math.MinInt16, math.MaxInt16)
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 78)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 79)
					data = randInt64Value(column, 0, math.MaxUint16)
				} else {
					trace_util_0.Count(_db_00000, 80)
					{
						data = randInt64Value(column, math.MinInt16, math.MaxInt16)
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 48)
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLong:
		trace_util_0.Count(_db_00000, 49)
		var data int64
		if incremental {
			trace_util_0.Count(_db_00000, 81)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 82)
				data = nextInt64Value(column, 0, math.MaxUint32)
			} else {
				trace_util_0.Count(_db_00000, 83)
				{
					data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 84)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 85)
					data = randInt64Value(column, 0, math.MaxUint32)
				} else {
					trace_util_0.Count(_db_00000, 86)
					{
						data = randInt64Value(column, math.MinInt32, math.MaxInt32)
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 50)
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLonglong:
		trace_util_0.Count(_db_00000, 51)
		var data int64
		if incremental {
			trace_util_0.Count(_db_00000, 87)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 88)
				data = nextInt64Value(column, 0, math.MaxInt64-1)
			} else {
				trace_util_0.Count(_db_00000, 89)
				{
					data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 90)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 91)
					data = randInt64Value(column, 0, math.MaxInt64-1)
				} else {
					trace_util_0.Count(_db_00000, 92)
					{
						data = randInt64Value(column, math.MinInt32, math.MaxInt32)
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 52)
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		trace_util_0.Count(_db_00000, 53)
		data := []byte{'\''}
		if incremental {
			trace_util_0.Count(_db_00000, 93)
			data = append(data, []byte(column.data.nextString(tp.Flen))...)
		} else {
			trace_util_0.Count(_db_00000, 94)
			{
				data = append(data, []byte(randStringValue(column, tp.Flen))...)
			}
		}

		trace_util_0.Count(_db_00000, 54)
		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeFloat, mysql.TypeDouble:
		trace_util_0.Count(_db_00000, 55)
		var data float64
		if incremental {
			trace_util_0.Count(_db_00000, 95)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 96)
				data = float64(nextInt64Value(column, 0, math.MaxInt64-1))
			} else {
				trace_util_0.Count(_db_00000, 97)
				{
					data = float64(nextInt64Value(column, math.MinInt32, math.MaxInt32))
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 98)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 99)
					data = float64(randInt64Value(column, 0, math.MaxInt64-1))
				} else {
					trace_util_0.Count(_db_00000, 100)
					{
						data = float64(randInt64Value(column, math.MinInt32, math.MaxInt32))
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 56)
		return strconv.FormatFloat(data, 'f', -1, 64), nil
	case mysql.TypeDate:
		trace_util_0.Count(_db_00000, 57)
		data := []byte{'\''}
		if incremental {
			trace_util_0.Count(_db_00000, 101)
			data = append(data, []byte(column.data.nextDate())...)
		} else {
			trace_util_0.Count(_db_00000, 102)
			{
				data = append(data, []byte(randDate(column))...)
			}
		}

		trace_util_0.Count(_db_00000, 58)
		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		trace_util_0.Count(_db_00000, 59)
		data := []byte{'\''}
		if incremental {
			trace_util_0.Count(_db_00000, 103)
			data = append(data, []byte(column.data.nextTimestamp())...)
		} else {
			trace_util_0.Count(_db_00000, 104)
			{
				data = append(data, []byte(randTimestamp(column))...)
			}
		}

		trace_util_0.Count(_db_00000, 60)
		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDuration:
		trace_util_0.Count(_db_00000, 61)
		data := []byte{'\''}
		if incremental {
			trace_util_0.Count(_db_00000, 105)
			data = append(data, []byte(column.data.nextTime())...)
		} else {
			trace_util_0.Count(_db_00000, 106)
			{
				data = append(data, []byte(randTime(column))...)
			}
		}

		trace_util_0.Count(_db_00000, 62)
		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeYear:
		trace_util_0.Count(_db_00000, 63)
		data := []byte{'\''}
		if incremental {
			trace_util_0.Count(_db_00000, 107)
			data = append(data, []byte(column.data.nextYear())...)
		} else {
			trace_util_0.Count(_db_00000, 108)
			{
				data = append(data, []byte(randYear(column))...)
			}
		}

		trace_util_0.Count(_db_00000, 64)
		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		trace_util_0.Count(_db_00000, 65)
		var limit = int64(math.Pow10(tp.Flen))
		var intVal int64
		if limit < 0 {
			trace_util_0.Count(_db_00000, 109)
			limit = math.MaxInt64
		}
		trace_util_0.Count(_db_00000, 66)
		if incremental {
			trace_util_0.Count(_db_00000, 110)
			if isUnsigned {
				trace_util_0.Count(_db_00000, 111)
				intVal = nextInt64Value(column, 0, limit-1)
			} else {
				trace_util_0.Count(_db_00000, 112)
				{
					intVal = nextInt64Value(column, (-limit+1)/2, (limit-1)/2)
				}
			}
		} else {
			trace_util_0.Count(_db_00000, 113)
			{
				if isUnsigned {
					trace_util_0.Count(_db_00000, 114)
					intVal = randInt64Value(column, 0, limit-1)
				} else {
					trace_util_0.Count(_db_00000, 115)
					{
						intVal = randInt64Value(column, (-limit+1)/2, (limit-1)/2)
					}
				}
			}
		}
		trace_util_0.Count(_db_00000, 67)
		return intToDecimalString(intVal, tp.Decimal), nil
	default:
		trace_util_0.Count(_db_00000, 68)
		return "", errors.Errorf("unsupported column type - %v", column)
	}
}

func execSQL(db *sql.DB, sql string) error {
	trace_util_0.Count(_db_00000, 116)
	if len(sql) == 0 {
		trace_util_0.Count(_db_00000, 119)
		return nil
	}

	trace_util_0.Count(_db_00000, 117)
	_, err := db.Exec(sql)
	if err != nil {
		trace_util_0.Count(_db_00000, 120)
		return errors.Trace(err)
	}

	trace_util_0.Count(_db_00000, 118)
	return nil
}

func createDB(cfg DBConfig) (*sql.DB, error) {
	trace_util_0.Count(_db_00000, 121)
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		trace_util_0.Count(_db_00000, 123)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_db_00000, 122)
	return db, nil
}

func closeDB(db *sql.DB) error {
	trace_util_0.Count(_db_00000, 124)
	return errors.Trace(db.Close())
}

func createDBs(cfg DBConfig, count int) ([]*sql.DB, error) {
	trace_util_0.Count(_db_00000, 125)
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		trace_util_0.Count(_db_00000, 127)
		db, err := createDB(cfg)
		if err != nil {
			trace_util_0.Count(_db_00000, 129)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_db_00000, 128)
		dbs = append(dbs, db)
	}

	trace_util_0.Count(_db_00000, 126)
	return dbs, nil
}

func closeDBs(dbs []*sql.DB) {
	trace_util_0.Count(_db_00000, 130)
	for _, db := range dbs {
		trace_util_0.Count(_db_00000, 131)
		err := closeDB(db)
		if err != nil {
			trace_util_0.Count(_db_00000, 132)
			log.Error("close DB failed", zap.Error(err))
		}
	}
}

var _db_00000 = "cmd/importer/db.go"
