// Copyright 2015 PingCAP, Inc.
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

package types

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	tidbMath "github.com/pingcap/tidb/util/math"
)

// Time format without fractional seconds precision.
const (
	DateFormat = "2006-01-02"
	TimeFormat = "2006-01-02 15:04:05"
	// TimeFSPFormat is time format with fractional seconds precision.
	TimeFSPFormat = "2006-01-02 15:04:05.000000"
)

const (
	// MinYear is the minimum for mysql year type.
	MinYear int16 = 1901
	// MaxYear is the maximum for mysql year type.
	MaxYear int16 = 2155
	// MaxDuration is the maximum for duration.
	MaxDuration int64 = 838*10000 + 59*100 + 59
	// MinTime is the minimum for mysql time type.
	MinTime = -gotime.Duration(838*3600+59*60+59) * gotime.Second
	// MaxTime is the maximum for mysql time type.
	MaxTime = gotime.Duration(838*3600+59*60+59) * gotime.Second
	// ZeroDatetimeStr is the string representation of a zero datetime.
	ZeroDatetimeStr = "0000-00-00 00:00:00"
	// ZeroDateStr is the string representation of a zero date.
	ZeroDateStr = "0000-00-00"

	// TimeMaxHour is the max hour for mysql time type.
	TimeMaxHour = 838
	// TimeMaxMinute is the max minute for mysql time type.
	TimeMaxMinute = 59
	// TimeMaxSecond is the max second for mysql time type.
	TimeMaxSecond = 59
	// TimeMaxValue is the maximum value for mysql time type.
	TimeMaxValue = TimeMaxHour*10000 + TimeMaxMinute*100 + TimeMaxSecond
	// TimeMaxValueSeconds is the maximum second value for mysql time type.
	TimeMaxValueSeconds = TimeMaxHour*3600 + TimeMaxMinute*60 + TimeMaxSecond
)

const (
	// YearIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	YearIndex = 0 + iota
	// MonthIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MonthIndex
	// DayIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	DayIndex
	// HourIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	HourIndex
	// MinuteIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MinuteIndex
	// SecondIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	SecondIndex
	// MicrosecondIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MicrosecondIndex
)

const (
	// YearMonthMaxCnt is max parameters count 'YEARS-MONTHS' expr Format allowed
	YearMonthMaxCnt = 2
	// DayHourMaxCnt is max parameters count 'DAYS HOURS' expr Format allowed
	DayHourMaxCnt = 2
	// DayMinuteMaxCnt is max parameters count 'DAYS HOURS:MINUTES' expr Format allowed
	DayMinuteMaxCnt = 3
	// DaySecondMaxCnt is max parameters count 'DAYS HOURS:MINUTES:SECONDS' expr Format allowed
	DaySecondMaxCnt = 4
	// DayMicrosecondMaxCnt is max parameters count 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	DayMicrosecondMaxCnt = 5
	// HourMinuteMaxCnt is max parameters count 'HOURS:MINUTES' expr Format allowed
	HourMinuteMaxCnt = 2
	// HourSecondMaxCnt is max parameters count 'HOURS:MINUTES:SECONDS' expr Format allowed
	HourSecondMaxCnt = 3
	// HourMicrosecondMaxCnt is max parameters count 'HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	HourMicrosecondMaxCnt = 4
	// MinuteSecondMaxCnt is max parameters count 'MINUTES:SECONDS' expr Format allowed
	MinuteSecondMaxCnt = 2
	// MinuteMicrosecondMaxCnt is max parameters count 'MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	MinuteMicrosecondMaxCnt = 3
	// SecondMicrosecondMaxCnt is max parameters count 'SECONDS.MICROSECONDS' expr Format allowed
	SecondMicrosecondMaxCnt = 2
	// TimeValueCnt is parameters count 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	TimeValueCnt = 7
)

// Zero values for different types.
var (
	// ZeroDuration is the zero value for Duration type.
	ZeroDuration = Duration{Duration: gotime.Duration(0), Fsp: DefaultFsp}

	// ZeroTime is the zero value for TimeInternal type.
	ZeroTime = MysqlTime{}

	// ZeroDatetime is the zero value for datetime Time.
	ZeroDatetime = Time{
		Time: ZeroTime,
		Type: mysql.TypeDatetime,
		Fsp:  DefaultFsp,
	}

	// ZeroTimestamp is the zero value for timestamp Time.
	ZeroTimestamp = Time{
		Time: ZeroTime,
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}

	// ZeroDate is the zero value for date Time.
	ZeroDate = Time{
		Time: ZeroTime,
		Type: mysql.TypeDate,
		Fsp:  DefaultFsp,
	}
)

var (
	// MinDatetime is the minimum for mysql datetime type.
	MinDatetime = FromDate(1000, 1, 1, 0, 0, 0, 0)
	// MaxDatetime is the maximum for mysql datetime type.
	MaxDatetime = FromDate(9999, 12, 31, 23, 59, 59, 999999)

	// BoundTimezone is the timezone for min and max timestamp.
	BoundTimezone = gotime.UTC
	// MinTimestamp is the minimum for mysql timestamp type.
	MinTimestamp = Time{
		Time: FromDate(1970, 1, 1, 0, 0, 1, 0),
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}
	// MaxTimestamp is the maximum for mysql timestamp type.
	MaxTimestamp = Time{
		Time: FromDate(2038, 1, 19, 3, 14, 7, 999999),
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}

	// WeekdayNames lists names of weekdays, which are used in builtin time function `dayname`.
	WeekdayNames = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// MonthNames lists names of months, which are used in builtin time function `monthname`.
	MonthNames = []string{
		"January", "February",
		"March", "April",
		"May", "June",
		"July", "August",
		"September", "October",
		"November", "December",
	}
)

const (
	// GoDurationDay is the gotime.Duration which equals to a Day.
	GoDurationDay = gotime.Hour * 24
	// GoDurationWeek is the gotime.Duration which equals to a Week.
	GoDurationWeek = GoDurationDay * 7
)

// FromGoTime translates time.Time to mysql time internal representation.
func FromGoTime(t gotime.Time) MysqlTime {
	trace_util_0.Count(_time_00000, 0)
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	// Nanosecond plus 500 then divided 1000 means rounding to microseconds.
	microsecond := (t.Nanosecond() + 500) / 1000
	return FromDate(year, int(month), day, hour, minute, second, microsecond)
}

// FromDate makes a internal time representation from the given date.
func FromDate(year int, month int, day int, hour int, minute int, second int, microsecond int) MysqlTime {
	trace_util_0.Count(_time_00000, 1)
	return MysqlTime{
		uint16(year),
		uint8(month),
		uint8(day),
		hour,
		uint8(minute),
		uint8(second),
		uint32(microsecond),
	}
}

// Clock returns the hour, minute, and second within the day specified by t.
func (t Time) Clock() (hour int, minute int, second int) {
	trace_util_0.Count(_time_00000, 2)
	return t.Time.Hour(), t.Time.Minute(), t.Time.Second()
}

// Time is the struct for handling datetime, timestamp and date.
// TODO: check if need a NewTime function to set Fsp default value?
type Time struct {
	Time MysqlTime
	Type uint8
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

// MaxMySQLTime returns Time with maximum mysql time type.
func MaxMySQLTime(fsp int) Time {
	trace_util_0.Count(_time_00000, 3)
	return Time{Time: FromDate(0, 0, 0, TimeMaxHour, TimeMaxMinute, TimeMaxSecond, 0), Type: mysql.TypeDuration, Fsp: fsp}
}

// CurrentTime returns current time with type tp.
func CurrentTime(tp uint8) Time {
	trace_util_0.Count(_time_00000, 4)
	return Time{Time: FromGoTime(gotime.Now()), Type: tp, Fsp: 0}
}

// ConvertTimeZone converts the time value from one timezone to another.
// The input time should be a valid timestamp.
func (t *Time) ConvertTimeZone(from, to *gotime.Location) error {
	trace_util_0.Count(_time_00000, 5)
	if !t.IsZero() {
		trace_util_0.Count(_time_00000, 7)
		raw, err := t.Time.GoTime(from)
		if err != nil {
			trace_util_0.Count(_time_00000, 9)
			return errors.Trace(err)
		}
		trace_util_0.Count(_time_00000, 8)
		converted := raw.In(to)
		t.Time = FromGoTime(converted)
	}
	trace_util_0.Count(_time_00000, 6)
	return nil
}

func (t Time) String() string {
	trace_util_0.Count(_time_00000, 10)
	if t.Type == mysql.TypeDate {
		trace_util_0.Count(_time_00000, 13)
		// We control the format, so no error would occur.
		str, err := t.DateFormat("%Y-%m-%d")
		terror.Log(errors.Trace(err))
		return str
	}

	trace_util_0.Count(_time_00000, 11)
	str, err := t.DateFormat("%Y-%m-%d %H:%i:%s")
	terror.Log(errors.Trace(err))
	if t.Fsp > 0 {
		trace_util_0.Count(_time_00000, 14)
		tmp := fmt.Sprintf(".%06d", t.Time.Microsecond())
		str = str + tmp[:1+t.Fsp]
	}

	trace_util_0.Count(_time_00000, 12)
	return str
}

// IsZero returns a boolean indicating whether the time is equal to ZeroTime.
func (t Time) IsZero() bool {
	trace_util_0.Count(_time_00000, 15)
	return compareTime(t.Time, ZeroTime) == 0
}

// InvalidZero returns a boolean indicating whether the month or day is zero.
func (t Time) InvalidZero() bool {
	trace_util_0.Count(_time_00000, 16)
	return t.Time.Month() == 0 || t.Time.Day() == 0
}

const numberFormat = "%Y%m%d%H%i%s"
const dateFormat = "%Y%m%d"

// ToNumber returns a formatted number.
// e.g,
// 2012-12-12 -> 20121212
// 2012-12-12T10:10:10 -> 20121212101010
// 2012-12-12T10:10:10.123456 -> 20121212101010.123456
func (t Time) ToNumber() *MyDecimal {
	trace_util_0.Count(_time_00000, 17)
	if t.IsZero() {
		trace_util_0.Count(_time_00000, 22)
		return &MyDecimal{}
	}

	// Fix issue #1046
	// Prevents from converting 2012-12-12 to 20121212000000
	trace_util_0.Count(_time_00000, 18)
	var tfStr string
	if t.Type == mysql.TypeDate {
		trace_util_0.Count(_time_00000, 23)
		tfStr = dateFormat
	} else {
		trace_util_0.Count(_time_00000, 24)
		{
			tfStr = numberFormat
		}
	}

	trace_util_0.Count(_time_00000, 19)
	s, err := t.DateFormat(tfStr)
	if err != nil {
		trace_util_0.Count(_time_00000, 25)
		logutil.Logger(context.Background()).Error("[fatal] never happen because we've control the format!")
	}

	trace_util_0.Count(_time_00000, 20)
	if t.Fsp > 0 {
		trace_util_0.Count(_time_00000, 26)
		s1 := fmt.Sprintf("%s.%06d", s, t.Time.Microsecond())
		s = s1[:len(s)+t.Fsp+1]
	}

	// We skip checking error here because time formatted string can be parsed certainly.
	trace_util_0.Count(_time_00000, 21)
	dec := new(MyDecimal)
	err = dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
	return dec
}

// Convert converts t with type tp.
func (t Time) Convert(sc *stmtctx.StatementContext, tp uint8) (Time, error) {
	trace_util_0.Count(_time_00000, 27)
	if t.Type == tp || t.IsZero() {
		trace_util_0.Count(_time_00000, 29)
		return Time{Time: t.Time, Type: tp, Fsp: t.Fsp}, nil
	}

	trace_util_0.Count(_time_00000, 28)
	t1 := Time{Time: t.Time, Type: tp, Fsp: t.Fsp}
	err := t1.check(sc)
	return t1, errors.Trace(err)
}

// ConvertToDuration converts mysql datetime, timestamp and date to mysql time type.
// e.g,
// 2012-12-12T10:10:10 -> 10:10:10
// 2012-12-12 -> 0
func (t Time) ConvertToDuration() (Duration, error) {
	trace_util_0.Count(_time_00000, 30)
	if t.IsZero() {
		trace_util_0.Count(_time_00000, 32)
		return ZeroDuration, nil
	}

	trace_util_0.Count(_time_00000, 31)
	hour, minute, second := t.Clock()
	frac := t.Time.Microsecond() * 1000

	d := gotime.Duration(hour*3600+minute*60+second)*gotime.Second + gotime.Duration(frac)
	// TODO: check convert validation
	return Duration{Duration: d, Fsp: t.Fsp}, nil
}

// Compare returns an integer comparing the time instant t to o.
// If t is after o, return 1, equal o, return 0, before o, return -1.
func (t Time) Compare(o Time) int {
	trace_util_0.Count(_time_00000, 33)
	return compareTime(t.Time, o.Time)
}

// compareTime compare two MysqlTime.
// return:
//  0: if a == b
//  1: if a > b
// -1: if a < b
func compareTime(a, b MysqlTime) int {
	trace_util_0.Count(_time_00000, 34)
	ta := datetimeToUint64(a)
	tb := datetimeToUint64(b)

	switch {
	case ta < tb:
		trace_util_0.Count(_time_00000, 37)
		return -1
	case ta > tb:
		trace_util_0.Count(_time_00000, 38)
		return 1
	}

	trace_util_0.Count(_time_00000, 35)
	switch {
	case a.Microsecond() < b.Microsecond():
		trace_util_0.Count(_time_00000, 39)
		return -1
	case a.Microsecond() > b.Microsecond():
		trace_util_0.Count(_time_00000, 40)
		return 1
	}

	trace_util_0.Count(_time_00000, 36)
	return 0
}

// CompareString is like Compare,
// but parses string to Time then compares.
func (t Time) CompareString(sc *stmtctx.StatementContext, str string) (int, error) {
	trace_util_0.Count(_time_00000, 41)
	// use MaxFsp to parse the string
	o, err := ParseTime(sc, str, t.Type, MaxFsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 43)
		return 0, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 42)
	return t.Compare(o), nil
}

// roundTime rounds the time value according to digits count specified by fsp.
func roundTime(t gotime.Time, fsp int) gotime.Time {
	trace_util_0.Count(_time_00000, 44)
	d := gotime.Duration(math.Pow10(9 - fsp))
	return t.Round(d)
}

// RoundFrac rounds the fraction part of a time-type value according to `fsp`.
func (t Time) RoundFrac(sc *stmtctx.StatementContext, fsp int) (Time, error) {
	trace_util_0.Count(_time_00000, 45)
	if t.Type == mysql.TypeDate || t.IsZero() {
		trace_util_0.Count(_time_00000, 50)
		// date type has no fsp
		return t, nil
	}

	trace_util_0.Count(_time_00000, 46)
	fsp, err := CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 51)
		return t, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 47)
	if fsp == t.Fsp {
		trace_util_0.Count(_time_00000, 52)
		// have same fsp
		return t, nil
	}

	trace_util_0.Count(_time_00000, 48)
	var nt MysqlTime
	if t1, err := t.Time.GoTime(sc.TimeZone); err == nil {
		trace_util_0.Count(_time_00000, 53)
		t1 = roundTime(t1, fsp)
		nt = FromGoTime(t1)
	} else {
		trace_util_0.Count(_time_00000, 54)
		{
			// Take the hh:mm:ss part out to avoid handle month or day = 0.
			hour, minute, second, microsecond := t.Time.Hour(), t.Time.Minute(), t.Time.Second(), t.Time.Microsecond()
			t1 := gotime.Date(1, 1, 1, hour, minute, second, microsecond*1000, gotime.Local)
			t2 := roundTime(t1, fsp)
			hour, minute, second = t2.Clock()
			microsecond = t2.Nanosecond() / 1000

			// TODO: when hh:mm:ss overflow one day after rounding, it should be add to yy:mm:dd part,
			// but mm:dd may contain 0, it makes the code complex, so we ignore it here.
			if t2.Day()-1 > 0 {
				trace_util_0.Count(_time_00000, 56)
				return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t.String()))
			}
			trace_util_0.Count(_time_00000, 55)
			nt = FromDate(t.Time.Year(), t.Time.Month(), t.Time.Day(), hour, minute, second, microsecond)
		}
	}

	trace_util_0.Count(_time_00000, 49)
	return Time{Time: nt, Type: t.Type, Fsp: fsp}, nil
}

// GetFsp gets the fsp of a string.
func GetFsp(s string) (fsp int) {
	trace_util_0.Count(_time_00000, 57)
	index := GetFracIndex(s)
	if index < 0 {
		trace_util_0.Count(_time_00000, 60)
		fsp = 0
	} else {
		trace_util_0.Count(_time_00000, 61)
		{
			fsp = len(s) - index - 1
		}
	}

	trace_util_0.Count(_time_00000, 58)
	if fsp == len(s) {
		trace_util_0.Count(_time_00000, 62)
		fsp = 0
	} else {
		trace_util_0.Count(_time_00000, 63)
		if fsp > 6 {
			trace_util_0.Count(_time_00000, 64)
			fsp = 6
		}
	}
	trace_util_0.Count(_time_00000, 59)
	return
}

// GetFracIndex finds the last '.' for get fracStr, index = -1 means fracStr not found.
// but for format like '2019.01.01 00:00:00', the index should be -1.
func GetFracIndex(s string) (index int) {
	trace_util_0.Count(_time_00000, 65)
	index = -1
	for i := len(s) - 1; i >= 0; i-- {
		trace_util_0.Count(_time_00000, 67)
		if unicode.IsPunct(rune(s[i])) {
			trace_util_0.Count(_time_00000, 68)
			if s[i] == '.' {
				trace_util_0.Count(_time_00000, 70)
				index = i
			}
			trace_util_0.Count(_time_00000, 69)
			break
		}
	}

	trace_util_0.Count(_time_00000, 66)
	return index
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:11
// and 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func RoundFrac(t gotime.Time, fsp int) (gotime.Time, error) {
	trace_util_0.Count(_time_00000, 71)
	_, err := CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 73)
		return t, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 72)
	return t.Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond), nil
}

// TruncateFrac truncates fractional seconds precision with new fsp and returns a new one.
// 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:10
// 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func TruncateFrac(t gotime.Time, fsp int) (gotime.Time, error) {
	trace_util_0.Count(_time_00000, 74)
	if _, err := CheckFsp(fsp); err != nil {
		trace_util_0.Count(_time_00000, 76)
		return t, err
	}
	trace_util_0.Count(_time_00000, 75)
	return t.Truncate(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond), nil
}

// ToPackedUint encodes Time to a packed uint64 value.
//
//    1 bit  0
//   17 bits year*13+month   (year 0-9999, month 0-12)
//    5 bits day             (0-31)
//    5 bits hour            (0-23)
//    6 bits minute          (0-59)
//    6 bits second          (0-59)
//   24 bits microseconds    (0-999999)
//
//   Total: 64 bits = 8 bytes
//
//   0YYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
//
func (t Time) ToPackedUint() (uint64, error) {
	trace_util_0.Count(_time_00000, 77)
	tm := t.Time
	if t.IsZero() {
		trace_util_0.Count(_time_00000, 79)
		return 0, nil
	}
	trace_util_0.Count(_time_00000, 78)
	year, month, day := tm.Year(), tm.Month(), tm.Day()
	hour, minute, sec := tm.Hour(), tm.Minute(), tm.Second()
	ymd := uint64(((year*13 + month) << 5) | day)
	hms := uint64(hour<<12 | minute<<6 | sec)
	micro := uint64(tm.Microsecond())
	return ((ymd<<17 | hms) << 24) | micro, nil
}

// FromPackedUint decodes Time from a packed uint64 value.
func (t *Time) FromPackedUint(packed uint64) error {
	trace_util_0.Count(_time_00000, 80)
	if packed == 0 {
		trace_util_0.Count(_time_00000, 82)
		t.Time = ZeroTime
		return nil
	}
	trace_util_0.Count(_time_00000, 81)
	ymdhms := packed >> 24
	ymd := ymdhms >> 17
	day := int(ymd & (1<<5 - 1))
	ym := ymd >> 5
	month := int(ym % 13)
	year := int(ym / 13)

	hms := ymdhms & (1<<17 - 1)
	second := int(hms & (1<<6 - 1))
	minute := int((hms >> 6) & (1<<6 - 1))
	hour := int(hms >> 12)
	microsec := int(packed % (1 << 24))

	t.Time = FromDate(year, month, day, hour, minute, second, microsec)

	return nil
}

// check whether t matches valid Time format.
// If allowZeroInDate is false, it returns ErrZeroDate when month or day is zero.
// FIXME: See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_no_zero_in_date
func (t *Time) check(sc *stmtctx.StatementContext) error {
	trace_util_0.Count(_time_00000, 83)
	allowZeroInDate := false
	allowInvalidDate := false
	// We should avoid passing sc as nil here as far as possible.
	if sc != nil {
		trace_util_0.Count(_time_00000, 86)
		allowZeroInDate = sc.IgnoreZeroInDate
		allowInvalidDate = sc.AllowInvalidDate
	}
	trace_util_0.Count(_time_00000, 84)
	var err error
	switch t.Type {
	case mysql.TypeTimestamp:
		trace_util_0.Count(_time_00000, 87)
		err = checkTimestampType(sc, t.Time)
	case mysql.TypeDatetime:
		trace_util_0.Count(_time_00000, 88)
		err = checkDatetimeType(t.Time, allowZeroInDate, allowInvalidDate)
	case mysql.TypeDate:
		trace_util_0.Count(_time_00000, 89)
		err = checkDateType(t.Time, allowZeroInDate, allowInvalidDate)
	}
	trace_util_0.Count(_time_00000, 85)
	return errors.Trace(err)
}

// Check if 't' is valid
func (t *Time) Check(sc *stmtctx.StatementContext) error {
	trace_util_0.Count(_time_00000, 90)
	return t.check(sc)
}

// Sub subtracts t1 from t, returns a duration value.
// Note that sub should not be done on different time types.
func (t *Time) Sub(sc *stmtctx.StatementContext, t1 *Time) Duration {
	trace_util_0.Count(_time_00000, 91)
	var duration gotime.Duration
	if t.Type == mysql.TypeTimestamp && t1.Type == mysql.TypeTimestamp {
		trace_util_0.Count(_time_00000, 94)
		a, err := t.Time.GoTime(sc.TimeZone)
		terror.Log(errors.Trace(err))
		b, err := t1.Time.GoTime(sc.TimeZone)
		terror.Log(errors.Trace(err))
		duration = a.Sub(b)
	} else {
		trace_util_0.Count(_time_00000, 95)
		{
			seconds, microseconds, neg := calcTimeDiff(t.Time, t1.Time, 1)
			duration = gotime.Duration(seconds*1e9 + microseconds*1e3)
			if neg {
				trace_util_0.Count(_time_00000, 96)
				duration = -duration
			}
		}
	}

	trace_util_0.Count(_time_00000, 92)
	fsp := t.Fsp
	if fsp < t1.Fsp {
		trace_util_0.Count(_time_00000, 97)
		fsp = t1.Fsp
	}
	trace_util_0.Count(_time_00000, 93)
	return Duration{
		Duration: duration,
		Fsp:      fsp,
	}
}

// Add adds d to t, returns the result time value.
func (t *Time) Add(sc *stmtctx.StatementContext, d Duration) (Time, error) {
	trace_util_0.Count(_time_00000, 98)
	sign, hh, mm, ss, micro := splitDuration(d.Duration)
	seconds, microseconds, _ := calcTimeDiff(t.Time, FromDate(0, 0, 0, hh, mm, ss, micro), -sign)
	days := seconds / secondsIn24Hour
	year, month, day := getDateFromDaynr(uint(days))
	var tm MysqlTime
	tm.year, tm.month, tm.day = uint16(year), uint8(month), uint8(day)
	calcTimeFromSec(&tm, seconds%secondsIn24Hour, microseconds)
	if t.Type == mysql.TypeDate {
		trace_util_0.Count(_time_00000, 101)
		tm.hour = 0
		tm.minute = 0
		tm.second = 0
		tm.microsecond = 0
	}
	trace_util_0.Count(_time_00000, 99)
	fsp := t.Fsp
	if d.Fsp > fsp {
		trace_util_0.Count(_time_00000, 102)
		fsp = d.Fsp
	}
	trace_util_0.Count(_time_00000, 100)
	ret := Time{
		Time: tm,
		Type: t.Type,
		Fsp:  fsp,
	}
	return ret, ret.Check(sc)
}

// TimestampDiff returns t2 - t1 where t1 and t2 are date or datetime expressions.
// The unit for the result (an integer) is given by the unit argument.
// The legal values for unit are "YEAR" "QUARTER" "MONTH" "DAY" "HOUR" "SECOND" and so on.
func TimestampDiff(unit string, t1 Time, t2 Time) int64 {
	trace_util_0.Count(_time_00000, 103)
	return timestampDiff(unit, t1.Time, t2.Time)
}

// ParseDateFormat parses a formatted date string and returns separated components.
func ParseDateFormat(format string) []string {
	trace_util_0.Count(_time_00000, 104)
	format = strings.TrimSpace(format)

	start := 0
	// Initialize `seps` with capacity of 6. The input `format` is typically
	// a date time of the form "2006-01-02 15:04:05", which has 6 numeric parts
	// (the fractional second part is usually removed by `splitDateTime`).
	// Setting `seps`'s capacity to 6 avoids reallocation in this common case.
	seps := make([]string, 0, 6)
	for i := 0; i < len(format); i++ {
		trace_util_0.Count(_time_00000, 106)
		// Date format must start and end with number.
		if i == 0 || i == len(format)-1 {
			trace_util_0.Count(_time_00000, 108)
			if !unicode.IsNumber(rune(format[i])) {
				trace_util_0.Count(_time_00000, 110)
				return nil
			}

			trace_util_0.Count(_time_00000, 109)
			continue
		}

		// Separator is a single none-number char.
		trace_util_0.Count(_time_00000, 107)
		if !unicode.IsNumber(rune(format[i])) {
			trace_util_0.Count(_time_00000, 111)
			if !unicode.IsNumber(rune(format[i-1])) {
				trace_util_0.Count(_time_00000, 113)
				return nil
			}

			trace_util_0.Count(_time_00000, 112)
			seps = append(seps, format[start:i])
			start = i + 1
		}

	}

	trace_util_0.Count(_time_00000, 105)
	seps = append(seps, format[start:])
	return seps
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
// The only delimiter recognized between a date and time part and a fractional seconds part is the decimal point.
func splitDateTime(format string) (seps []string, fracStr string) {
	trace_util_0.Count(_time_00000, 114)
	index := GetFracIndex(format)
	if index > 0 {
		trace_util_0.Count(_time_00000, 116)
		fracStr = format[index+1:]
		format = format[:index]
	}

	trace_util_0.Count(_time_00000, 115)
	seps = ParseDateFormat(format)
	return
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
func parseDatetime(sc *stmtctx.StatementContext, str string, fsp int, isFloat bool) (Time, error) {
	trace_util_0.Count(_time_00000, 117)
	// Try to split str with delimiter.
	// TODO: only punctuation can be the delimiter for date parts or time parts.
	// But only space and T can be the delimiter between the date and time part.
	var (
		year, month, day, hour, minute, second int
		fracStr                                string
		hhmmss                                 bool
		err                                    error
	)

	seps, fracStr := splitDateTime(str)
	var truncatedOrIncorrect bool
	switch len(seps) {
	case 1:
		trace_util_0.Count(_time_00000, 123)
		l := len(seps[0])
		switch l {
		case 14:
			trace_util_0.Count(_time_00000, 134) // No delimiter.
			// YYYYMMDDHHMMSS
			_, err = fmt.Sscanf(seps[0], "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			hhmmss = true
		case 12:
			trace_util_0.Count(_time_00000, 135) // YYMMDDHHMMSS
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
			hhmmss = true
		case 11:
			trace_util_0.Count(_time_00000, 136) // YYMMDDHHMMS
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
			hhmmss = true
		case 10:
			trace_util_0.Count(_time_00000, 137) // YYMMDDHHMM
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute)
			year = adjustYear(year)
		case 9:
			trace_util_0.Count(_time_00000, 138) // YYMMDDHHM
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute)
			year = adjustYear(year)
		case 8:
			trace_util_0.Count(_time_00000, 139) // YYYYMMDD
			_, err = fmt.Sscanf(seps[0], "%4d%2d%2d", &year, &month, &day)
		case 6, 5:
			trace_util_0.Count(_time_00000, 140)
			// YYMMDD && YYMMD
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d", &year, &month, &day)
			year = adjustYear(year)
		default:
			trace_util_0.Count(_time_00000, 141)
			return ZeroDatetime, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(str))
		}
		trace_util_0.Count(_time_00000, 124)
		if l == 5 || l == 6 || l == 8 {
			trace_util_0.Count(_time_00000, 142)
			// YYMMDD or YYYYMMDD
			// We must handle float => string => datetime, the difference is that fractional
			// part of float type is discarded directly, while fractional part of string type
			// is parsed to HH:MM:SS.
			if isFloat {
				trace_util_0.Count(_time_00000, 143)
				// 20170118.123423 => 2017-01-18 00:00:00
			} else {
				trace_util_0.Count(_time_00000, 144)
				{
					// '20170118.123423' => 2017-01-18 12:34:23.234
					switch len(fracStr) {
					case 0:
						trace_util_0.Count(_time_00000, 146)
					case 1, 2:
						trace_util_0.Count(_time_00000, 147)
						_, err = fmt.Sscanf(fracStr, "%2d ", &hour)
					case 3, 4:
						trace_util_0.Count(_time_00000, 148)
						_, err = fmt.Sscanf(fracStr, "%2d%2d ", &hour, &minute)
					default:
						trace_util_0.Count(_time_00000, 149)
						_, err = fmt.Sscanf(fracStr, "%2d%2d%2d ", &hour, &minute, &second)
					}
					trace_util_0.Count(_time_00000, 145)
					truncatedOrIncorrect = err != nil
				}
			}
		}
		trace_util_0.Count(_time_00000, 125)
		if l == 9 || l == 10 {
			trace_util_0.Count(_time_00000, 150)
			if len(fracStr) == 0 {
				trace_util_0.Count(_time_00000, 152)
				second = 0
			} else {
				trace_util_0.Count(_time_00000, 153)
				{
					_, err = fmt.Sscanf(fracStr, "%2d ", &second)
				}
			}
			trace_util_0.Count(_time_00000, 151)
			truncatedOrIncorrect = err != nil
		}
		trace_util_0.Count(_time_00000, 126)
		if truncatedOrIncorrect && sc != nil {
			trace_util_0.Count(_time_00000, 154)
			sc.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs("datetime", str))
			err = nil
		}
	case 2:
		trace_util_0.Count(_time_00000, 127)
		// YYYY-MM is not valid
		if len(fracStr) == 0 {
			trace_util_0.Count(_time_00000, 155)
			return ZeroDatetime, errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(str))
		}

		// YYYY-MM.DD, DD is treat as fracStr
		trace_util_0.Count(_time_00000, 128)
		err = scanTimeArgs(append(seps, fracStr), &year, &month, &day)
		fracStr = ""
	case 3:
		trace_util_0.Count(_time_00000, 129)
		// YYYY-MM-DD
		err = scanTimeArgs(seps, &year, &month, &day)
	case 4:
		trace_util_0.Count(_time_00000, 130)
		// YYYY-MM-DD HH
		err = scanTimeArgs(seps, &year, &month, &day, &hour)
	case 5:
		trace_util_0.Count(_time_00000, 131)
		// YYYY-MM-DD HH-MM
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute)
	case 6:
		trace_util_0.Count(_time_00000, 132)
		// We don't have fractional seconds part.
		// YYYY-MM-DD HH-MM-SS
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute, &second)
		hhmmss = true
	default:
		trace_util_0.Count(_time_00000, 133)
		return ZeroDatetime, errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(str))
	}
	trace_util_0.Count(_time_00000, 118)
	if err != nil {
		trace_util_0.Count(_time_00000, 156)
		return ZeroDatetime, errors.Trace(err)
	}

	// If str is sepereated by delimiters, the first one is year, and if the year is 2 digit,
	// we should adjust it.
	// TODO: adjust year is very complex, now we only consider the simplest way.
	trace_util_0.Count(_time_00000, 119)
	if len(seps[0]) == 2 {
		trace_util_0.Count(_time_00000, 157)
		if year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && fracStr == "" {
			trace_util_0.Count(_time_00000, 158)
			// Skip a special case "00-00-00".
		} else {
			trace_util_0.Count(_time_00000, 159)
			{
				year = adjustYear(year)
			}
		}
	}

	trace_util_0.Count(_time_00000, 120)
	var microsecond int
	var overflow bool
	if hhmmss {
		trace_util_0.Count(_time_00000, 160)
		// If input string is "20170118.999", without hhmmss, fsp is meanless.
		microsecond, overflow, err = ParseFrac(fracStr, fsp)
		if err != nil {
			trace_util_0.Count(_time_00000, 161)
			return ZeroDatetime, errors.Trace(err)
		}
	}

	trace_util_0.Count(_time_00000, 121)
	tmp := FromDate(year, month, day, hour, minute, second, microsecond)
	if overflow {
		trace_util_0.Count(_time_00000, 162)
		// Convert to Go time and add 1 second, to handle input like 2017-01-05 08:40:59.575601
		t1, err := tmp.GoTime(gotime.Local)
		if err != nil {
			trace_util_0.Count(_time_00000, 164)
			return ZeroDatetime, errors.Trace(err)
		}
		trace_util_0.Count(_time_00000, 163)
		tmp = FromGoTime(t1.Add(gotime.Second))
	}

	trace_util_0.Count(_time_00000, 122)
	nt := Time{
		Time: tmp,
		Type: mysql.TypeDatetime,
		Fsp:  fsp}

	return nt, nil
}

func scanTimeArgs(seps []string, args ...*int) error {
	trace_util_0.Count(_time_00000, 165)
	if len(seps) != len(args) {
		trace_util_0.Count(_time_00000, 168)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(seps))
	}

	trace_util_0.Count(_time_00000, 166)
	var err error
	for i, s := range seps {
		trace_util_0.Count(_time_00000, 169)
		*args[i], err = strconv.Atoi(s)
		if err != nil {
			trace_util_0.Count(_time_00000, 170)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_time_00000, 167)
	return nil
}

// ParseYear parses a formatted string and returns a year number.
func ParseYear(str string) (int16, error) {
	trace_util_0.Count(_time_00000, 171)
	v, err := strconv.ParseInt(str, 10, 16)
	if err != nil {
		trace_util_0.Count(_time_00000, 175)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 172)
	y := int16(v)

	if len(str) == 4 {
		trace_util_0.Count(_time_00000, 176)
		// Nothing to do.
	} else {
		trace_util_0.Count(_time_00000, 177)
		if len(str) == 2 || len(str) == 1 {
			trace_util_0.Count(_time_00000, 178)
			y = int16(adjustYear(int(y)))
		} else {
			trace_util_0.Count(_time_00000, 179)
			{
				return 0, errors.Trace(ErrInvalidYearFormat)
			}
		}
	}

	trace_util_0.Count(_time_00000, 173)
	if y < MinYear || y > MaxYear {
		trace_util_0.Count(_time_00000, 180)
		return 0, errors.Trace(ErrInvalidYearFormat)
	}

	trace_util_0.Count(_time_00000, 174)
	return y, nil
}

// adjustYear adjusts year according to y.
// See https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html
func adjustYear(y int) int {
	trace_util_0.Count(_time_00000, 181)
	if y >= 0 && y <= 69 {
		trace_util_0.Count(_time_00000, 183)
		y = 2000 + y
	} else {
		trace_util_0.Count(_time_00000, 184)
		if y >= 70 && y <= 99 {
			trace_util_0.Count(_time_00000, 185)
			y = 1900 + y
		}
	}
	trace_util_0.Count(_time_00000, 182)
	return y
}

// AdjustYear is used for adjusting year and checking its validation.
func AdjustYear(y int64, shouldAdjust bool) (int64, error) {
	trace_util_0.Count(_time_00000, 186)
	if y == 0 && !shouldAdjust {
		trace_util_0.Count(_time_00000, 189)
		return y, nil
	}
	trace_util_0.Count(_time_00000, 187)
	y = int64(adjustYear(int(y)))
	if y < int64(MinYear) || y > int64(MaxYear) {
		trace_util_0.Count(_time_00000, 190)
		return 0, errors.Trace(ErrInvalidYear)
	}

	trace_util_0.Count(_time_00000, 188)
	return y, nil
}

// Duration is the type for MySQL TIME type.
type Duration struct {
	gotime.Duration
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

//Add adds d to d, returns a duration value.
func (d Duration) Add(v Duration) (Duration, error) {
	trace_util_0.Count(_time_00000, 191)
	if &v == nil {
		trace_util_0.Count(_time_00000, 195)
		return d, nil
	}
	trace_util_0.Count(_time_00000, 192)
	dsum, err := AddInt64(int64(d.Duration), int64(v.Duration))
	if err != nil {
		trace_util_0.Count(_time_00000, 196)
		return Duration{}, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 193)
	if d.Fsp >= v.Fsp {
		trace_util_0.Count(_time_00000, 197)
		return Duration{Duration: gotime.Duration(dsum), Fsp: d.Fsp}, nil
	}
	trace_util_0.Count(_time_00000, 194)
	return Duration{Duration: gotime.Duration(dsum), Fsp: v.Fsp}, nil
}

// Sub subtracts d to d, returns a duration value.
func (d Duration) Sub(v Duration) (Duration, error) {
	trace_util_0.Count(_time_00000, 198)
	if &v == nil {
		trace_util_0.Count(_time_00000, 202)
		return d, nil
	}
	trace_util_0.Count(_time_00000, 199)
	dsum, err := SubInt64(int64(d.Duration), int64(v.Duration))
	if err != nil {
		trace_util_0.Count(_time_00000, 203)
		return Duration{}, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 200)
	if d.Fsp >= v.Fsp {
		trace_util_0.Count(_time_00000, 204)
		return Duration{Duration: gotime.Duration(dsum), Fsp: d.Fsp}, nil
	}
	trace_util_0.Count(_time_00000, 201)
	return Duration{Duration: gotime.Duration(dsum), Fsp: v.Fsp}, nil
}

// String returns the time formatted using default TimeFormat and fsp.
func (d Duration) String() string {
	trace_util_0.Count(_time_00000, 205)
	var buf bytes.Buffer

	sign, hours, minutes, seconds, fraction := splitDuration(d.Duration)
	if sign < 0 {
		trace_util_0.Count(_time_00000, 208)
		buf.WriteByte('-')
	}

	trace_util_0.Count(_time_00000, 206)
	fmt.Fprintf(&buf, "%02d:%02d:%02d", hours, minutes, seconds)
	if d.Fsp > 0 {
		trace_util_0.Count(_time_00000, 209)
		buf.WriteString(".")
		buf.WriteString(d.formatFrac(fraction))
	}

	trace_util_0.Count(_time_00000, 207)
	p := buf.String()

	return p
}

func (d Duration) formatFrac(frac int) string {
	trace_util_0.Count(_time_00000, 210)
	s := fmt.Sprintf("%06d", frac)
	return s[0:d.Fsp]
}

// ToNumber changes duration to number format.
// e.g,
// 10:10:10 -> 101010
func (d Duration) ToNumber() *MyDecimal {
	trace_util_0.Count(_time_00000, 211)
	sign, hours, minutes, seconds, fraction := splitDuration(d.Duration)
	var (
		s       string
		signStr string
	)

	if sign < 0 {
		trace_util_0.Count(_time_00000, 214)
		signStr = "-"
	}

	trace_util_0.Count(_time_00000, 212)
	if d.Fsp == 0 {
		trace_util_0.Count(_time_00000, 215)
		s = fmt.Sprintf("%s%02d%02d%02d", signStr, hours, minutes, seconds)
	} else {
		trace_util_0.Count(_time_00000, 216)
		{
			s = fmt.Sprintf("%s%02d%02d%02d.%s", signStr, hours, minutes, seconds, d.formatFrac(fraction))
		}
	}

	// We skip checking error here because time formatted string can be parsed certainly.
	trace_util_0.Count(_time_00000, 213)
	dec := new(MyDecimal)
	err := dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
	return dec
}

// ConvertToTime converts duration to Time.
// Tp is TypeDatetime, TypeTimestamp and TypeDate.
func (d Duration) ConvertToTime(sc *stmtctx.StatementContext, tp uint8) (Time, error) {
	trace_util_0.Count(_time_00000, 217)
	year, month, day := gotime.Now().In(sc.TimeZone).Date()
	sign, hour, minute, second, frac := splitDuration(d.Duration)
	datePart := FromDate(year, int(month), day, 0, 0, 0, 0)
	timePart := FromDate(0, 0, 0, hour, minute, second, frac)
	mixDateAndTime(&datePart, &timePart, sign < 0)

	t := Time{
		Time: datePart,
		Type: mysql.TypeDatetime,
		Fsp:  d.Fsp,
	}
	return t.Convert(sc, tp)
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 10:10:10.999999 round 0 -> 10:10:11
// and 10:10:10.000000 round 0 -> 10:10:10
func (d Duration) RoundFrac(fsp int) (Duration, error) {
	trace_util_0.Count(_time_00000, 218)
	fsp, err := CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 221)
		return d, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 219)
	if fsp == d.Fsp {
		trace_util_0.Count(_time_00000, 222)
		return d, nil
	}

	trace_util_0.Count(_time_00000, 220)
	n := gotime.Date(0, 0, 0, 0, 0, 0, 0, gotime.Local)
	nd := n.Add(d.Duration).Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond).Sub(n)
	return Duration{Duration: nd, Fsp: fsp}, nil
}

// Compare returns an integer comparing the Duration instant t to o.
// If d is after o, return 1, equal o, return 0, before o, return -1.
func (d Duration) Compare(o Duration) int {
	trace_util_0.Count(_time_00000, 223)
	if d.Duration > o.Duration {
		trace_util_0.Count(_time_00000, 224)
		return 1
	} else {
		trace_util_0.Count(_time_00000, 225)
		if d.Duration == o.Duration {
			trace_util_0.Count(_time_00000, 226)
			return 0
		} else {
			trace_util_0.Count(_time_00000, 227)
			{
				return -1
			}
		}
	}
}

// CompareString is like Compare,
// but parses str to Duration then compares.
func (d Duration) CompareString(sc *stmtctx.StatementContext, str string) (int, error) {
	trace_util_0.Count(_time_00000, 228)
	// use MaxFsp to parse the string
	o, err := ParseDuration(sc, str, MaxFsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 230)
		return 0, err
	}

	trace_util_0.Count(_time_00000, 229)
	return d.Compare(o), nil
}

// Hour returns current hour.
// e.g, hour("11:11:11") -> 11
func (d Duration) Hour() int {
	trace_util_0.Count(_time_00000, 231)
	_, hour, _, _, _ := splitDuration(d.Duration)
	return hour
}

// Minute returns current minute.
// e.g, hour("11:11:11") -> 11
func (d Duration) Minute() int {
	trace_util_0.Count(_time_00000, 232)
	_, _, minute, _, _ := splitDuration(d.Duration)
	return minute
}

// Second returns current second.
// e.g, hour("11:11:11") -> 11
func (d Duration) Second() int {
	trace_util_0.Count(_time_00000, 233)
	_, _, _, second, _ := splitDuration(d.Duration)
	return second
}

// MicroSecond returns current microsecond.
// e.g, hour("11:11:11.11") -> 110000
func (d Duration) MicroSecond() int {
	trace_util_0.Count(_time_00000, 234)
	_, _, _, _, frac := splitDuration(d.Duration)
	return frac
}

// ParseDuration parses the time form a formatted string with a fractional seconds part,
// returns the duration type Time value.
// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
func ParseDuration(sc *stmtctx.StatementContext, str string, fsp int) (Duration, error) {
	trace_util_0.Count(_time_00000, 235)
	var (
		day, hour, minute, second int
		err                       error
		sign                      = 0
		dayExists                 = false
		origStr                   = str
	)

	fsp, err = CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 245)
		return ZeroDuration, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 236)
	if len(str) == 0 {
		trace_util_0.Count(_time_00000, 246)
		return ZeroDuration, nil
	} else {
		trace_util_0.Count(_time_00000, 247)
		if str[0] == '-' {
			trace_util_0.Count(_time_00000, 248)
			str = str[1:]
			sign = -1
		}
	}

	// Time format may has day.
	trace_util_0.Count(_time_00000, 237)
	if n := strings.IndexByte(str, ' '); n >= 0 {
		trace_util_0.Count(_time_00000, 249)
		if day, err = strconv.Atoi(str[:n]); err == nil {
			trace_util_0.Count(_time_00000, 251)
			dayExists = true
		}
		trace_util_0.Count(_time_00000, 250)
		str = str[n+1:]
	}

	trace_util_0.Count(_time_00000, 238)
	var (
		integeralPart = str
		fracPart      int
		overflow      bool
	)
	if n := strings.IndexByte(str, '.'); n >= 0 {
		trace_util_0.Count(_time_00000, 252)
		// It has fractional precision parts.
		fracStr := str[n+1:]
		fracPart, overflow, err = ParseFrac(fracStr, fsp)
		if err != nil {
			trace_util_0.Count(_time_00000, 254)
			return ZeroDuration, errors.Trace(err)
		}
		trace_util_0.Count(_time_00000, 253)
		integeralPart = str[0:n]
	}

	// It tries to split integeralPart with delimiter, time delimiter must be :
	trace_util_0.Count(_time_00000, 239)
	seps := strings.Split(integeralPart, ":")

	switch len(seps) {
	case 1:
		trace_util_0.Count(_time_00000, 255)
		if dayExists {
			trace_util_0.Count(_time_00000, 259)
			hour, err = strconv.Atoi(seps[0])
		} else {
			trace_util_0.Count(_time_00000, 260)
			{
				// No delimiter.
				switch len(integeralPart) {
				case 7:
					trace_util_0.Count(_time_00000, 261) // HHHMMSS
					_, err = fmt.Sscanf(integeralPart, "%3d%2d%2d", &hour, &minute, &second)
				case 6:
					trace_util_0.Count(_time_00000, 262) // HHMMSS
					_, err = fmt.Sscanf(integeralPart, "%2d%2d%2d", &hour, &minute, &second)
				case 5:
					trace_util_0.Count(_time_00000, 263) // HMMSS
					_, err = fmt.Sscanf(integeralPart, "%1d%2d%2d", &hour, &minute, &second)
				case 4:
					trace_util_0.Count(_time_00000, 264) // MMSS
					_, err = fmt.Sscanf(integeralPart, "%2d%2d", &minute, &second)
				case 3:
					trace_util_0.Count(_time_00000, 265) // MSS
					_, err = fmt.Sscanf(integeralPart, "%1d%2d", &minute, &second)
				case 2:
					trace_util_0.Count(_time_00000, 266) // SS
					_, err = fmt.Sscanf(integeralPart, "%2d", &second)
				case 1:
					trace_util_0.Count(_time_00000, 267) // 0S
					_, err = fmt.Sscanf(integeralPart, "%1d", &second)
				default:
					trace_util_0.Count(_time_00000, 268) // Maybe contains date.
					t, err1 := ParseDatetime(sc, str)
					if err1 != nil {
						trace_util_0.Count(_time_00000, 271)
						return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
					}
					trace_util_0.Count(_time_00000, 269)
					var dur Duration
					dur, err1 = t.ConvertToDuration()
					if err1 != nil {
						trace_util_0.Count(_time_00000, 272)
						return ZeroDuration, errors.Trace(err)
					}
					trace_util_0.Count(_time_00000, 270)
					return dur.RoundFrac(fsp)
				}
			}
		}
	case 2:
		trace_util_0.Count(_time_00000, 256)
		// HH:MM
		_, err = fmt.Sscanf(integeralPart, "%2d:%2d", &hour, &minute)
	case 3:
		trace_util_0.Count(_time_00000, 257)
		// Time format maybe HH:MM:SS or HHH:MM:SS.
		// See https://dev.mysql.com/doc/refman/5.7/en/time.html
		if len(seps[0]) == 3 {
			trace_util_0.Count(_time_00000, 273)
			_, err = fmt.Sscanf(integeralPart, "%3d:%2d:%2d", &hour, &minute, &second)
		} else {
			trace_util_0.Count(_time_00000, 274)
			{
				_, err = fmt.Sscanf(integeralPart, "%2d:%2d:%2d", &hour, &minute, &second)
			}
		}
	default:
		trace_util_0.Count(_time_00000, 258)
		return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
	}

	trace_util_0.Count(_time_00000, 240)
	if err != nil {
		trace_util_0.Count(_time_00000, 275)
		return ZeroDuration, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 241)
	if overflow {
		trace_util_0.Count(_time_00000, 276)
		second++
		fracPart = 0
	}
	// Invalid TIME values are converted to '00:00:00'.
	// See https://dev.mysql.com/doc/refman/5.7/en/time.html
	trace_util_0.Count(_time_00000, 242)
	if minute >= 60 || second > 60 || (!overflow && second == 60) {
		trace_util_0.Count(_time_00000, 277)
		return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
	}
	trace_util_0.Count(_time_00000, 243)
	d := gotime.Duration(day*24*3600+hour*3600+minute*60+second)*gotime.Second + gotime.Duration(fracPart)*gotime.Microsecond
	if sign == -1 {
		trace_util_0.Count(_time_00000, 278)
		d = -d
	}

	trace_util_0.Count(_time_00000, 244)
	d, err = TruncateOverflowMySQLTime(d)
	return Duration{Duration: d, Fsp: fsp}, errors.Trace(err)
}

// TruncateOverflowMySQLTime truncates d when it overflows, and return ErrTruncatedWrongVal.
func TruncateOverflowMySQLTime(d gotime.Duration) (gotime.Duration, error) {
	trace_util_0.Count(_time_00000, 279)
	if d > MaxTime {
		trace_util_0.Count(_time_00000, 281)
		return MaxTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
	} else {
		trace_util_0.Count(_time_00000, 282)
		if d < MinTime {
			trace_util_0.Count(_time_00000, 283)
			return MinTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
		}
	}

	trace_util_0.Count(_time_00000, 280)
	return d, nil
}

func splitDuration(t gotime.Duration) (int, int, int, int, int) {
	trace_util_0.Count(_time_00000, 284)
	sign := 1
	if t < 0 {
		trace_util_0.Count(_time_00000, 286)
		t = -t
		sign = -1
	}

	trace_util_0.Count(_time_00000, 285)
	hours := t / gotime.Hour
	t -= hours * gotime.Hour
	minutes := t / gotime.Minute
	t -= minutes * gotime.Minute
	seconds := t / gotime.Second
	t -= seconds * gotime.Second
	fraction := t / gotime.Microsecond

	return sign, int(hours), int(minutes), int(seconds), int(fraction)
}

var maxDaysInMonth = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func getTime(sc *stmtctx.StatementContext, num int64, tp byte) (Time, error) {
	trace_util_0.Count(_time_00000, 287)
	s1 := num / 1000000
	s2 := num - s1*1000000

	year := int(s1 / 10000)
	s1 %= 10000
	month := int(s1 / 100)
	day := int(s1 % 100)

	hour := int(s2 / 10000)
	s2 %= 10000
	minute := int(s2 / 100)
	second := int(s2 % 100)

	t := Time{
		Time: FromDate(year, month, day, hour, minute, second, 0),
		Type: tp,
		Fsp:  DefaultFsp,
	}
	err := t.check(sc)
	return t, errors.Trace(err)
}

// parseDateTimeFromNum parses date time from num.
// See number_to_datetime function.
// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
func parseDateTimeFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	trace_util_0.Count(_time_00000, 288)
	t := ZeroDate
	// Check zero.
	if num == 0 {
		trace_util_0.Count(_time_00000, 301)
		return t, nil
	}

	// Check datetime type.
	trace_util_0.Count(_time_00000, 289)
	if num >= 10000101000000 {
		trace_util_0.Count(_time_00000, 302)
		t.Type = mysql.TypeDatetime
		return getTime(sc, num, t.Type)
	}

	// Check MMDD.
	trace_util_0.Count(_time_00000, 290)
	if num < 101 {
		trace_util_0.Count(_time_00000, 303)
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDD, year: 2000-2069
	trace_util_0.Count(_time_00000, 291)
	if num <= (70-1)*10000+1231 {
		trace_util_0.Count(_time_00000, 304)
		num = (num + 20000000) * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check YYMMDD.
	trace_util_0.Count(_time_00000, 292)
	if num < 70*10000+101 {
		trace_util_0.Count(_time_00000, 305)
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDD, year: 1970-1999
	trace_util_0.Count(_time_00000, 293)
	if num <= 991231 {
		trace_util_0.Count(_time_00000, 306)
		num = (num + 19000000) * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check YYYYMMDD.
	trace_util_0.Count(_time_00000, 294)
	if num < 10000101 {
		trace_util_0.Count(_time_00000, 307)
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust hour/min/second.
	trace_util_0.Count(_time_00000, 295)
	if num <= 99991231 {
		trace_util_0.Count(_time_00000, 308)
		num = num * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check MMDDHHMMSS.
	trace_util_0.Count(_time_00000, 296)
	if num < 101000000 {
		trace_util_0.Count(_time_00000, 309)
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Set TypeDatetime type.
	trace_util_0.Count(_time_00000, 297)
	t.Type = mysql.TypeDatetime

	// Adjust year
	// YYMMDDHHMMSS, 2000-2069
	if num <= 69*10000000000+1231235959 {
		trace_util_0.Count(_time_00000, 310)
		num = num + 20000000000000
		return getTime(sc, num, t.Type)
	}

	// Check YYYYMMDDHHMMSS.
	trace_util_0.Count(_time_00000, 298)
	if num < 70*10000000000+101000000 {
		trace_util_0.Count(_time_00000, 311)
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDDHHMMSS, 1970-1999
	trace_util_0.Count(_time_00000, 299)
	if num <= 991231235959 {
		trace_util_0.Count(_time_00000, 312)
		num = num + 19000000000000
		return getTime(sc, num, t.Type)
	}

	trace_util_0.Count(_time_00000, 300)
	return getTime(sc, num, t.Type)
}

// ParseTime parses a formatted string with type tp and specific fsp.
// Type is TypeDatetime, TypeTimestamp and TypeDate.
// Fsp is in range [0, 6].
// MySQL supports many valid datetime format, but still has some limitation.
// If delimiter exists, the date part and time part is separated by a space or T,
// other punctuation character can be used as the delimiter between date parts or time parts.
// If no delimiter, the format must be YYYYMMDDHHMMSS or YYMMDDHHMMSS
// If we have fractional seconds part, we must use decimal points as the delimiter.
// The valid datetime range is from '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
// The valid timestamp range is from '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
// The valid date range is from '1000-01-01' to '9999-12-31'
func ParseTime(sc *stmtctx.StatementContext, str string, tp byte, fsp int) (Time, error) {
	trace_util_0.Count(_time_00000, 313)
	return parseTime(sc, str, tp, fsp, false)
}

// ParseTimeFromFloatString is similar to ParseTime, except that it's used to parse a float converted string.
func ParseTimeFromFloatString(sc *stmtctx.StatementContext, str string, tp byte, fsp int) (Time, error) {
	trace_util_0.Count(_time_00000, 314)
	return parseTime(sc, str, tp, fsp, true)
}

func parseTime(sc *stmtctx.StatementContext, str string, tp byte, fsp int, isFloat bool) (Time, error) {
	trace_util_0.Count(_time_00000, 315)
	fsp, err := CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 319)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 316)
	t, err := parseDatetime(sc, str, fsp, isFloat)
	if err != nil {
		trace_util_0.Count(_time_00000, 320)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 317)
	t.Type = tp
	if err = t.check(sc); err != nil {
		trace_util_0.Count(_time_00000, 321)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 318)
	return t, nil
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(sc *stmtctx.StatementContext, str string) (Time, error) {
	trace_util_0.Count(_time_00000, 322)
	return ParseTime(sc, str, mysql.TypeDatetime, GetFsp(str))
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(sc *stmtctx.StatementContext, str string) (Time, error) {
	trace_util_0.Count(_time_00000, 323)
	return ParseTime(sc, str, mysql.TypeTimestamp, GetFsp(str))
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(sc *stmtctx.StatementContext, str string) (Time, error) {
	trace_util_0.Count(_time_00000, 324)
	// date has no fractional seconds precision
	return ParseTime(sc, str, mysql.TypeDate, MinFsp)
}

// ParseTimeFromNum parses a formatted int64,
// returns the value which type is tp.
func ParseTimeFromNum(sc *stmtctx.StatementContext, num int64, tp byte, fsp int) (Time, error) {
	trace_util_0.Count(_time_00000, 325)
	fsp, err := CheckFsp(fsp)
	if err != nil {
		trace_util_0.Count(_time_00000, 329)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 326)
	t, err := parseDateTimeFromNum(sc, num)
	if err != nil {
		trace_util_0.Count(_time_00000, 330)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 327)
	t.Type = tp
	t.Fsp = fsp
	if err := t.check(sc); err != nil {
		trace_util_0.Count(_time_00000, 331)
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	trace_util_0.Count(_time_00000, 328)
	return t, nil
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	trace_util_0.Count(_time_00000, 332)
	return ParseTimeFromNum(sc, num, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	trace_util_0.Count(_time_00000, 333)
	return ParseTimeFromNum(sc, num, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	trace_util_0.Count(_time_00000, 334)
	// date has no fractional seconds precision
	return ParseTimeFromNum(sc, num, mysql.TypeDate, MinFsp)
}

// TimeFromDays Converts a day number to a date.
func TimeFromDays(num int64) Time {
	trace_util_0.Count(_time_00000, 335)
	if num < 0 {
		trace_util_0.Count(_time_00000, 337)
		return Time{
			Time: FromDate(0, 0, 0, 0, 0, 0, 0),
			Type: mysql.TypeDate,
			Fsp:  0,
		}
	}
	trace_util_0.Count(_time_00000, 336)
	year, month, day := getDateFromDaynr(uint(num))

	return Time{
		Time: FromDate(int(year), int(month), int(day), 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0,
	}
}

func checkDateType(t MysqlTime, allowZeroInDate, allowInvalidDate bool) error {
	trace_util_0.Count(_time_00000, 338)
	year, month, day := t.Year(), t.Month(), t.Day()
	if year == 0 && month == 0 && day == 0 {
		trace_util_0.Count(_time_00000, 343)
		return nil
	}

	trace_util_0.Count(_time_00000, 339)
	if !allowZeroInDate && (month == 0 || day == 0) {
		trace_util_0.Count(_time_00000, 344)
		return ErrIncorrectDatetimeValue.GenWithStackByArgs(fmt.Sprintf("%04d-%02d-%02d", year, month, day))
	}

	trace_util_0.Count(_time_00000, 340)
	if err := checkDateRange(t); err != nil {
		trace_util_0.Count(_time_00000, 345)
		return errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 341)
	if err := checkMonthDay(year, month, day, allowInvalidDate); err != nil {
		trace_util_0.Count(_time_00000, 346)
		return errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 342)
	return nil
}

func checkDateRange(t MysqlTime) error {
	trace_util_0.Count(_time_00000, 347)
	// Oddly enough, MySQL document says date range should larger than '1000-01-01',
	// but we can insert '0001-01-01' actually.
	if t.Year() < 0 || t.Month() < 0 || t.Day() < 0 {
		trace_util_0.Count(_time_00000, 350)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}
	trace_util_0.Count(_time_00000, 348)
	if compareTime(t, MaxDatetime) > 0 {
		trace_util_0.Count(_time_00000, 351)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}
	trace_util_0.Count(_time_00000, 349)
	return nil
}

func checkMonthDay(year, month, day int, allowInvalidDate bool) error {
	trace_util_0.Count(_time_00000, 352)
	if month < 0 || month > 12 {
		trace_util_0.Count(_time_00000, 356)
		return errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(month))
	}

	trace_util_0.Count(_time_00000, 353)
	maxDay := 31
	if !allowInvalidDate {
		trace_util_0.Count(_time_00000, 357)
		if month > 0 {
			trace_util_0.Count(_time_00000, 359)
			maxDay = maxDaysInMonth[month-1]
		}
		trace_util_0.Count(_time_00000, 358)
		if month == 2 && !isLeapYear(uint16(year)) {
			trace_util_0.Count(_time_00000, 360)
			maxDay = 28
		}
	}

	trace_util_0.Count(_time_00000, 354)
	if day < 0 || day > maxDay {
		trace_util_0.Count(_time_00000, 361)
		return errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(day))
	}
	trace_util_0.Count(_time_00000, 355)
	return nil
}

func checkTimestampType(sc *stmtctx.StatementContext, t MysqlTime) error {
	trace_util_0.Count(_time_00000, 362)
	if compareTime(t, ZeroTime) == 0 {
		trace_util_0.Count(_time_00000, 368)
		return nil
	}

	trace_util_0.Count(_time_00000, 363)
	if sc == nil {
		trace_util_0.Count(_time_00000, 369)
		return errors.New("statementContext is required during checkTimestampType")
	}

	trace_util_0.Count(_time_00000, 364)
	var checkTime MysqlTime
	if sc.TimeZone != BoundTimezone {
		trace_util_0.Count(_time_00000, 370)
		convertTime := Time{Time: t, Type: mysql.TypeTimestamp}
		err := convertTime.ConvertTimeZone(sc.TimeZone, BoundTimezone)
		if err != nil {
			trace_util_0.Count(_time_00000, 372)
			return err
		}
		trace_util_0.Count(_time_00000, 371)
		checkTime = convertTime.Time
	} else {
		trace_util_0.Count(_time_00000, 373)
		{
			checkTime = t
		}
	}
	trace_util_0.Count(_time_00000, 365)
	if compareTime(checkTime, MaxTimestamp.Time) > 0 || compareTime(checkTime, MinTimestamp.Time) < 0 {
		trace_util_0.Count(_time_00000, 374)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}

	trace_util_0.Count(_time_00000, 366)
	if _, err := t.GoTime(gotime.Local); err != nil {
		trace_util_0.Count(_time_00000, 375)
		return errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 367)
	return nil
}

func checkDatetimeType(t MysqlTime, allowZeroInDate, allowInvalidDate bool) error {
	trace_util_0.Count(_time_00000, 376)
	if err := checkDateType(t, allowZeroInDate, allowInvalidDate); err != nil {
		trace_util_0.Count(_time_00000, 381)
		return errors.Trace(err)
	}

	trace_util_0.Count(_time_00000, 377)
	hour, minute, second := t.Hour(), t.Minute(), t.Second()
	if hour < 0 || hour >= 24 {
		trace_util_0.Count(_time_00000, 382)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(hour))
	}
	trace_util_0.Count(_time_00000, 378)
	if minute < 0 || minute >= 60 {
		trace_util_0.Count(_time_00000, 383)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(minute))
	}
	trace_util_0.Count(_time_00000, 379)
	if second < 0 || second >= 60 {
		trace_util_0.Count(_time_00000, 384)
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(second))
	}

	trace_util_0.Count(_time_00000, 380)
	return nil
}

// ExtractDatetimeNum extracts time value number from datetime unit and format.
func ExtractDatetimeNum(t *Time, unit string) (int64, error) {
	trace_util_0.Count(_time_00000, 385)
	// TODO: Consider time_zone variable.
	switch strings.ToUpper(unit) {
	case "DAY":
		trace_util_0.Count(_time_00000, 386)
		return int64(t.Time.Day()), nil
	case "WEEK":
		trace_util_0.Count(_time_00000, 387)
		week := t.Time.Week(0)
		return int64(week), nil
	case "MONTH":
		trace_util_0.Count(_time_00000, 388)
		return int64(t.Time.Month()), nil
	case "QUARTER":
		trace_util_0.Count(_time_00000, 389)
		m := int64(t.Time.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		trace_util_0.Count(_time_00000, 390)
		return int64(t.Time.Year()), nil
	case "DAY_MICROSECOND":
		trace_util_0.Count(_time_00000, 391)
		h, m, s := t.Clock()
		d := t.Time.Day()
		return int64(d*1000000+h*10000+m*100+s)*1000000 + int64(t.Time.Microsecond()), nil
	case "DAY_SECOND":
		trace_util_0.Count(_time_00000, 392)
		h, m, s := t.Clock()
		d := t.Time.Day()
		return int64(d)*1000000 + int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "DAY_MINUTE":
		trace_util_0.Count(_time_00000, 393)
		h, m, _ := t.Clock()
		d := t.Time.Day()
		return int64(d)*10000 + int64(h)*100 + int64(m), nil
	case "DAY_HOUR":
		trace_util_0.Count(_time_00000, 394)
		h, _, _ := t.Clock()
		d := t.Time.Day()
		return int64(d)*100 + int64(h), nil
	case "YEAR_MONTH":
		trace_util_0.Count(_time_00000, 395)
		y, m := t.Time.Year(), t.Time.Month()
		return int64(y)*100 + int64(m), nil
	default:
		trace_util_0.Count(_time_00000, 396)
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// ExtractDurationNum extracts duration value number from duration unit and format.
func ExtractDurationNum(d *Duration, unit string) (int64, error) {
	trace_util_0.Count(_time_00000, 397)
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		trace_util_0.Count(_time_00000, 398)
		return int64(d.MicroSecond()), nil
	case "SECOND":
		trace_util_0.Count(_time_00000, 399)
		return int64(d.Second()), nil
	case "MINUTE":
		trace_util_0.Count(_time_00000, 400)
		return int64(d.Minute()), nil
	case "HOUR":
		trace_util_0.Count(_time_00000, 401)
		return int64(d.Hour()), nil
	case "SECOND_MICROSECOND":
		trace_util_0.Count(_time_00000, 402)
		return int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "MINUTE_MICROSECOND":
		trace_util_0.Count(_time_00000, 403)
		return int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "MINUTE_SECOND":
		trace_util_0.Count(_time_00000, 404)
		return int64(d.Minute()*100 + d.Second()), nil
	case "HOUR_MICROSECOND":
		trace_util_0.Count(_time_00000, 405)
		return int64(d.Hour())*10000000000 + int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "HOUR_SECOND":
		trace_util_0.Count(_time_00000, 406)
		return int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second()), nil
	case "HOUR_MINUTE":
		trace_util_0.Count(_time_00000, 407)
		return int64(d.Hour())*100 + int64(d.Minute()), nil
	default:
		trace_util_0.Count(_time_00000, 408)
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// parseSingleTimeValue parse the format according the given unit. If we set strictCheck true, we'll check whether
// the converted value not exceed the range of MySQL's TIME type.
// The first four returned values are year, month, day and nanosecond.
func parseSingleTimeValue(unit string, format string, strictCheck bool) (int64, int64, int64, int64, error) {
	trace_util_0.Count(_time_00000, 409)
	// Format is a preformatted number, it format should be A[.[B]].
	decimalPointPos := strings.IndexRune(format, '.')
	if decimalPointPos == -1 {
		trace_util_0.Count(_time_00000, 415)
		decimalPointPos = len(format)
	}
	trace_util_0.Count(_time_00000, 410)
	sign := int64(1)
	if len(format) > 0 && format[0] == '-' {
		trace_util_0.Count(_time_00000, 416)
		sign = int64(-1)
	}
	trace_util_0.Count(_time_00000, 411)
	iv, err := strconv.ParseInt(format[0:decimalPointPos], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 417)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
	}
	trace_util_0.Count(_time_00000, 412)
	riv := iv // Rounded integer value

	dv := int64(0)
	lf := len(format) - 1
	// Has fraction part
	if decimalPointPos < lf {
		trace_util_0.Count(_time_00000, 418)
		if lf-decimalPointPos >= 6 {
			trace_util_0.Count(_time_00000, 421)
			// MySQL rounds down to 1e-6.
			if dv, err = strconv.ParseInt(format[decimalPointPos+1:decimalPointPos+7], 10, 64); err != nil {
				trace_util_0.Count(_time_00000, 422)
				return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
			}
		} else {
			trace_util_0.Count(_time_00000, 423)
			{
				if dv, err = strconv.ParseInt(format[decimalPointPos+1:]+"000000"[:6-(lf-decimalPointPos)], 10, 64); err != nil {
					trace_util_0.Count(_time_00000, 424)
					return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
				}
			}
		}
		trace_util_0.Count(_time_00000, 419)
		if dv >= 500000 {
			trace_util_0.Count(_time_00000, 425) // Round up, and we should keep 6 digits for microsecond, so dv should in [000000, 999999].
			riv += sign
		}
		trace_util_0.Count(_time_00000, 420)
		if unit != "SECOND" {
			trace_util_0.Count(_time_00000, 426)
			err = ErrTruncatedWrongValue.GenWithStackByArgs(format)
		}
	}
	trace_util_0.Count(_time_00000, 413)
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		trace_util_0.Count(_time_00000, 427)
		if strictCheck && tidbMath.Abs(riv) > TimeMaxValueSeconds*1000 {
			trace_util_0.Count(_time_00000, 445)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 428)
		dayCount := riv / int64(GoDurationDay/gotime.Microsecond)
		riv %= int64(GoDurationDay / gotime.Microsecond)
		return 0, 0, dayCount, riv * int64(gotime.Microsecond), err
	case "SECOND":
		trace_util_0.Count(_time_00000, 429)
		if strictCheck && tidbMath.Abs(iv) > TimeMaxValueSeconds {
			trace_util_0.Count(_time_00000, 446)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 430)
		dayCount := iv / int64(GoDurationDay/gotime.Second)
		iv %= int64(GoDurationDay / gotime.Second)
		return 0, 0, dayCount, iv*int64(gotime.Second) + dv*int64(gotime.Microsecond), err
	case "MINUTE":
		trace_util_0.Count(_time_00000, 431)
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour*60+TimeMaxMinute {
			trace_util_0.Count(_time_00000, 447)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 432)
		dayCount := riv / int64(GoDurationDay/gotime.Minute)
		riv %= int64(GoDurationDay / gotime.Minute)
		return 0, 0, dayCount, riv * int64(gotime.Minute), err
	case "HOUR":
		trace_util_0.Count(_time_00000, 433)
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour {
			trace_util_0.Count(_time_00000, 448)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 434)
		dayCount := riv / 24
		riv %= 24
		return 0, 0, dayCount, riv * int64(gotime.Hour), err
	case "DAY":
		trace_util_0.Count(_time_00000, 435)
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour/24 {
			trace_util_0.Count(_time_00000, 449)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 436)
		return 0, 0, riv, 0, err
	case "WEEK":
		trace_util_0.Count(_time_00000, 437)
		if strictCheck && 7*tidbMath.Abs(riv) > TimeMaxHour/24 {
			trace_util_0.Count(_time_00000, 450)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 438)
		return 0, 0, 7 * riv, 0, err
	case "MONTH":
		trace_util_0.Count(_time_00000, 439)
		if strictCheck && tidbMath.Abs(riv) > 1 {
			trace_util_0.Count(_time_00000, 451)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 440)
		return 0, riv, 0, 0, err
	case "QUARTER":
		trace_util_0.Count(_time_00000, 441)
		if strictCheck {
			trace_util_0.Count(_time_00000, 452)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 442)
		return 0, 3 * riv, 0, 0, err
	case "YEAR":
		trace_util_0.Count(_time_00000, 443)
		if strictCheck {
			trace_util_0.Count(_time_00000, 453)
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		trace_util_0.Count(_time_00000, 444)
		return riv, 0, 0, 0, err
	}

	trace_util_0.Count(_time_00000, 414)
	return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// parseTimeValue gets years, months, days, nanoseconds from a string
// nanosecond will not exceed length of single day
// MySQL permits any punctuation delimiter in the expr format.
// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
func parseTimeValue(format string, index, cnt int) (int64, int64, int64, int64, error) {
	trace_util_0.Count(_time_00000, 454)
	neg := false
	originalFmt := format
	format = strings.TrimSpace(format)
	if len(format) > 0 && format[0] == '-' {
		trace_util_0.Count(_time_00000, 466)
		neg = true
		format = format[1:]
	}
	trace_util_0.Count(_time_00000, 455)
	fields := make([]string, TimeValueCnt)
	for i := range fields {
		trace_util_0.Count(_time_00000, 467)
		fields[i] = "0"
	}
	trace_util_0.Count(_time_00000, 456)
	matches := numericRegex.FindAllString(format, -1)
	if len(matches) > cnt {
		trace_util_0.Count(_time_00000, 468)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 457)
	for i := range matches {
		trace_util_0.Count(_time_00000, 469)
		if neg {
			trace_util_0.Count(_time_00000, 471)
			fields[index] = "-" + matches[len(matches)-1-i]
		} else {
			trace_util_0.Count(_time_00000, 472)
			{
				fields[index] = matches[len(matches)-1-i]
			}
		}
		trace_util_0.Count(_time_00000, 470)
		index--
	}

	trace_util_0.Count(_time_00000, 458)
	years, err := strconv.ParseInt(fields[YearIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 473)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 459)
	months, err := strconv.ParseInt(fields[MonthIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 474)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 460)
	days, err := strconv.ParseInt(fields[DayIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 475)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}

	trace_util_0.Count(_time_00000, 461)
	hours, err := strconv.ParseInt(fields[HourIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 476)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 462)
	minutes, err := strconv.ParseInt(fields[MinuteIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 477)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 463)
	seconds, err := strconv.ParseInt(fields[SecondIndex], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 478)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 464)
	microseconds, err := strconv.ParseInt(alignFrac(fields[MicrosecondIndex], MaxFsp), 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 479)
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	trace_util_0.Count(_time_00000, 465)
	seconds = hours*3600 + minutes*60 + seconds
	days += seconds / (3600 * 24)
	seconds %= 3600 * 24
	return years, months, days, seconds*int64(gotime.Second) + microseconds*int64(gotime.Microsecond), nil
}

func parseAndValidateDurationValue(format string, index, cnt int) (int64, error) {
	trace_util_0.Count(_time_00000, 480)
	year, month, day, nano, err := parseTimeValue(format, index, cnt)
	if err != nil {
		trace_util_0.Count(_time_00000, 484)
		return 0, err
	}
	trace_util_0.Count(_time_00000, 481)
	if year != 0 || month != 0 || tidbMath.Abs(day) > TimeMaxHour/24 {
		trace_util_0.Count(_time_00000, 485)
		return 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	trace_util_0.Count(_time_00000, 482)
	dur := day*int64(GoDurationDay) + nano
	if tidbMath.Abs(dur) > int64(MaxTime) {
		trace_util_0.Count(_time_00000, 486)
		return 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	trace_util_0.Count(_time_00000, 483)
	return dur, nil
}

// ParseDurationValue parses time value from time unit and format.
// Returns y years m months d days + n nanoseconds
// Nanoseconds will no longer than one day.
func ParseDurationValue(unit string, format string) (y int64, m int64, d int64, n int64, _ error) {
	trace_util_0.Count(_time_00000, 487)
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		trace_util_0.Count(_time_00000, 488)
		return parseSingleTimeValue(unit, format, false)
	case "SECOND_MICROSECOND":
		trace_util_0.Count(_time_00000, 489)
		return parseTimeValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
	case "MINUTE_MICROSECOND":
		trace_util_0.Count(_time_00000, 490)
		return parseTimeValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
	case "MINUTE_SECOND":
		trace_util_0.Count(_time_00000, 491)
		return parseTimeValue(format, SecondIndex, MinuteSecondMaxCnt)
	case "HOUR_MICROSECOND":
		trace_util_0.Count(_time_00000, 492)
		return parseTimeValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
	case "HOUR_SECOND":
		trace_util_0.Count(_time_00000, 493)
		return parseTimeValue(format, SecondIndex, HourSecondMaxCnt)
	case "HOUR_MINUTE":
		trace_util_0.Count(_time_00000, 494)
		return parseTimeValue(format, MinuteIndex, HourMinuteMaxCnt)
	case "DAY_MICROSECOND":
		trace_util_0.Count(_time_00000, 495)
		return parseTimeValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
	case "DAY_SECOND":
		trace_util_0.Count(_time_00000, 496)
		return parseTimeValue(format, SecondIndex, DaySecondMaxCnt)
	case "DAY_MINUTE":
		trace_util_0.Count(_time_00000, 497)
		return parseTimeValue(format, MinuteIndex, DayMinuteMaxCnt)
	case "DAY_HOUR":
		trace_util_0.Count(_time_00000, 498)
		return parseTimeValue(format, HourIndex, DayHourMaxCnt)
	case "YEAR_MONTH":
		trace_util_0.Count(_time_00000, 499)
		return parseTimeValue(format, MonthIndex, YearMonthMaxCnt)
	default:
		trace_util_0.Count(_time_00000, 500)
		return 0, 0, 0, 0, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// ExtractDurationValue extract the value from format to Duration.
func ExtractDurationValue(unit string, format string) (Duration, error) {
	trace_util_0.Count(_time_00000, 501)
	unit = strings.ToUpper(unit)
	switch unit {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		trace_util_0.Count(_time_00000, 502)
		_, month, day, nano, err := parseSingleTimeValue(unit, format, true)
		if err != nil {
			trace_util_0.Count(_time_00000, 528)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 503)
		dur := Duration{Duration: gotime.Duration((month*30+day)*int64(GoDurationDay) + nano)}
		if unit == "MICROSECOND" {
			trace_util_0.Count(_time_00000, 529)
			dur.Fsp = MaxFsp
		}
		trace_util_0.Count(_time_00000, 504)
		return dur, err
	case "SECOND_MICROSECOND":
		trace_util_0.Count(_time_00000, 505)
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 530)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 506)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "MINUTE_MICROSECOND":
		trace_util_0.Count(_time_00000, 507)
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 531)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 508)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "MINUTE_SECOND":
		trace_util_0.Count(_time_00000, 509)
		d, err := parseAndValidateDurationValue(format, SecondIndex, MinuteSecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 532)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 510)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_MICROSECOND":
		trace_util_0.Count(_time_00000, 511)
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 533)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 512)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_SECOND":
		trace_util_0.Count(_time_00000, 513)
		d, err := parseAndValidateDurationValue(format, SecondIndex, HourSecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 534)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 514)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_MINUTE":
		trace_util_0.Count(_time_00000, 515)
		d, err := parseAndValidateDurationValue(format, MinuteIndex, HourMinuteMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 535)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 516)
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "DAY_MICROSECOND":
		trace_util_0.Count(_time_00000, 517)
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 536)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 518)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "DAY_SECOND":
		trace_util_0.Count(_time_00000, 519)
		d, err := parseAndValidateDurationValue(format, SecondIndex, DaySecondMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 537)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 520)
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "DAY_MINUTE":
		trace_util_0.Count(_time_00000, 521)
		d, err := parseAndValidateDurationValue(format, MinuteIndex, DayMinuteMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 538)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 522)
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "DAY_HOUR":
		trace_util_0.Count(_time_00000, 523)
		d, err := parseAndValidateDurationValue(format, HourIndex, DayHourMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 539)
			return ZeroDuration, err
		}
		trace_util_0.Count(_time_00000, 524)
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "YEAR_MONTH":
		trace_util_0.Count(_time_00000, 525)
		_, err := parseAndValidateDurationValue(format, MonthIndex, YearMonthMaxCnt)
		if err != nil {
			trace_util_0.Count(_time_00000, 540)
			return ZeroDuration, err
		}
		// MONTH must exceed the limit of mysql's duration. So just return overflow error.
		trace_util_0.Count(_time_00000, 526)
		return ZeroDuration, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	default:
		trace_util_0.Count(_time_00000, 527)
		return ZeroDuration, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// IsClockUnit returns true when unit is interval unit with hour, minute or second.
func IsClockUnit(unit string) bool {
	trace_util_0.Count(_time_00000, 541)
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR",
		"SECOND_MICROSECOND", "MINUTE_MICROSECOND", "MINUTE_SECOND",
		"HOUR_MICROSECOND", "HOUR_SECOND", "HOUR_MINUTE",
		"DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR":
		trace_util_0.Count(_time_00000, 542)
		return true
	default:
		trace_util_0.Count(_time_00000, 543)
		return false
	}
}

// IsDateFormat returns true when the specified time format could contain only date.
func IsDateFormat(format string) bool {
	trace_util_0.Count(_time_00000, 544)
	format = strings.TrimSpace(format)
	seps := ParseDateFormat(format)
	length := len(format)
	switch len(seps) {
	case 1:
		trace_util_0.Count(_time_00000, 546)
		if (length == 8) || (length == 6) {
			trace_util_0.Count(_time_00000, 548)
			return true
		}
	case 3:
		trace_util_0.Count(_time_00000, 547)
		return true
	}
	trace_util_0.Count(_time_00000, 545)
	return false
}

// ParseTimeFromInt64 parses mysql time value from int64.
func ParseTimeFromInt64(sc *stmtctx.StatementContext, num int64) (Time, error) {
	trace_util_0.Count(_time_00000, 549)
	return parseDateTimeFromNum(sc, num)
}

// DateFormat returns a textual representation of the time value formatted
// according to layout.
// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t Time) DateFormat(layout string) (string, error) {
	trace_util_0.Count(_time_00000, 550)
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range layout {
		trace_util_0.Count(_time_00000, 552)
		if inPatternMatch {
			trace_util_0.Count(_time_00000, 554)
			if err := t.convertDateFormat(b, &buf); err != nil {
				trace_util_0.Count(_time_00000, 556)
				return "", errors.Trace(err)
			}
			trace_util_0.Count(_time_00000, 555)
			inPatternMatch = false
			continue
		}

		// It's not in pattern match now.
		trace_util_0.Count(_time_00000, 553)
		if b == '%' {
			trace_util_0.Count(_time_00000, 557)
			inPatternMatch = true
		} else {
			trace_util_0.Count(_time_00000, 558)
			{
				buf.WriteRune(b)
			}
		}
	}
	trace_util_0.Count(_time_00000, 551)
	return buf.String(), nil
}

var abbrevWeekdayName = []string{
	"Sun", "Mon", "Tue",
	"Wed", "Thu", "Fri", "Sat",
}

func (t Time) convertDateFormat(b rune, buf *bytes.Buffer) error {
	trace_util_0.Count(_time_00000, 559)
	switch b {
	case 'b':
		trace_util_0.Count(_time_00000, 561)
		m := t.Time.Month()
		if m == 0 || m > 12 {
			trace_util_0.Count(_time_00000, 593)
			return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(m))
		}
		trace_util_0.Count(_time_00000, 562)
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		trace_util_0.Count(_time_00000, 563)
		m := t.Time.Month()
		if m == 0 || m > 12 {
			trace_util_0.Count(_time_00000, 594)
			return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(m))
		}
		trace_util_0.Count(_time_00000, 564)
		buf.WriteString(MonthNames[m-1])
	case 'm':
		trace_util_0.Count(_time_00000, 565)
		fmt.Fprintf(buf, "%02d", t.Time.Month())
	case 'c':
		trace_util_0.Count(_time_00000, 566)
		fmt.Fprintf(buf, "%d", t.Time.Month())
	case 'D':
		trace_util_0.Count(_time_00000, 567)
		fmt.Fprintf(buf, "%d%s", t.Time.Day(), abbrDayOfMonth(t.Time.Day()))
	case 'd':
		trace_util_0.Count(_time_00000, 568)
		fmt.Fprintf(buf, "%02d", t.Time.Day())
	case 'e':
		trace_util_0.Count(_time_00000, 569)
		fmt.Fprintf(buf, "%d", t.Time.Day())
	case 'j':
		trace_util_0.Count(_time_00000, 570)
		fmt.Fprintf(buf, "%03d", t.Time.YearDay())
	case 'H':
		trace_util_0.Count(_time_00000, 571)
		fmt.Fprintf(buf, "%02d", t.Time.Hour())
	case 'k':
		trace_util_0.Count(_time_00000, 572)
		fmt.Fprintf(buf, "%d", t.Time.Hour())
	case 'h', 'I':
		trace_util_0.Count(_time_00000, 573)
		t := t.Time.Hour()
		if t%12 == 0 {
			trace_util_0.Count(_time_00000, 595)
			fmt.Fprintf(buf, "%02d", 12)
		} else {
			trace_util_0.Count(_time_00000, 596)
			{
				fmt.Fprintf(buf, "%02d", t%12)
			}
		}
	case 'l':
		trace_util_0.Count(_time_00000, 574)
		t := t.Time.Hour()
		if t%12 == 0 {
			trace_util_0.Count(_time_00000, 597)
			fmt.Fprintf(buf, "%d", 12)
		} else {
			trace_util_0.Count(_time_00000, 598)
			{
				fmt.Fprintf(buf, "%d", t%12)
			}
		}
	case 'i':
		trace_util_0.Count(_time_00000, 575)
		fmt.Fprintf(buf, "%02d", t.Time.Minute())
	case 'p':
		trace_util_0.Count(_time_00000, 576)
		hour := t.Time.Hour()
		if hour/12%2 == 0 {
			trace_util_0.Count(_time_00000, 599)
			buf.WriteString("AM")
		} else {
			trace_util_0.Count(_time_00000, 600)
			{
				buf.WriteString("PM")
			}
		}
	case 'r':
		trace_util_0.Count(_time_00000, 577)
		h := t.Time.Hour()
		h %= 24
		switch {
		case h == 0:
			trace_util_0.Count(_time_00000, 601)
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Time.Minute(), t.Time.Second())
		case h == 12:
			trace_util_0.Count(_time_00000, 602)
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Time.Minute(), t.Time.Second())
		case h < 12:
			trace_util_0.Count(_time_00000, 603)
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Time.Minute(), t.Time.Second())
		default:
			trace_util_0.Count(_time_00000, 604)
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Time.Minute(), t.Time.Second())
		}
	case 'T':
		trace_util_0.Count(_time_00000, 578)
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Time.Hour(), t.Time.Minute(), t.Time.Second())
	case 'S', 's':
		trace_util_0.Count(_time_00000, 579)
		fmt.Fprintf(buf, "%02d", t.Time.Second())
	case 'f':
		trace_util_0.Count(_time_00000, 580)
		fmt.Fprintf(buf, "%06d", t.Time.Microsecond())
	case 'U':
		trace_util_0.Count(_time_00000, 581)
		w := t.Time.Week(0)
		fmt.Fprintf(buf, "%02d", w)
	case 'u':
		trace_util_0.Count(_time_00000, 582)
		w := t.Time.Week(1)
		fmt.Fprintf(buf, "%02d", w)
	case 'V':
		trace_util_0.Count(_time_00000, 583)
		w := t.Time.Week(2)
		fmt.Fprintf(buf, "%02d", w)
	case 'v':
		trace_util_0.Count(_time_00000, 584)
		_, w := t.Time.YearWeek(3)
		fmt.Fprintf(buf, "%02d", w)
	case 'a':
		trace_util_0.Count(_time_00000, 585)
		weekday := t.Time.Weekday()
		buf.WriteString(abbrevWeekdayName[weekday])
	case 'W':
		trace_util_0.Count(_time_00000, 586)
		buf.WriteString(t.Time.Weekday().String())
	case 'w':
		trace_util_0.Count(_time_00000, 587)
		fmt.Fprintf(buf, "%d", t.Time.Weekday())
	case 'X':
		trace_util_0.Count(_time_00000, 588)
		year, _ := t.Time.YearWeek(2)
		if year < 0 {
			trace_util_0.Count(_time_00000, 605)
			fmt.Fprintf(buf, "%v", uint64(math.MaxUint32))
		} else {
			trace_util_0.Count(_time_00000, 606)
			{
				fmt.Fprintf(buf, "%04d", year)
			}
		}
	case 'x':
		trace_util_0.Count(_time_00000, 589)
		year, _ := t.Time.YearWeek(3)
		if year < 0 {
			trace_util_0.Count(_time_00000, 607)
			fmt.Fprintf(buf, "%v", uint64(math.MaxUint32))
		} else {
			trace_util_0.Count(_time_00000, 608)
			{
				fmt.Fprintf(buf, "%04d", year)
			}
		}
	case 'Y':
		trace_util_0.Count(_time_00000, 590)
		fmt.Fprintf(buf, "%04d", t.Time.Year())
	case 'y':
		trace_util_0.Count(_time_00000, 591)
		str := fmt.Sprintf("%04d", t.Time.Year())
		buf.WriteString(str[2:])
	default:
		trace_util_0.Count(_time_00000, 592)
		buf.WriteRune(b)
	}

	trace_util_0.Count(_time_00000, 560)
	return nil
}

func abbrDayOfMonth(day int) string {
	trace_util_0.Count(_time_00000, 609)
	var str string
	switch day {
	case 1, 21, 31:
		trace_util_0.Count(_time_00000, 611)
		str = "st"
	case 2, 22:
		trace_util_0.Count(_time_00000, 612)
		str = "nd"
	case 3, 23:
		trace_util_0.Count(_time_00000, 613)
		str = "rd"
	default:
		trace_util_0.Count(_time_00000, 614)
		str = "th"
	}
	trace_util_0.Count(_time_00000, 610)
	return str
}

// StrToDate converts date string according to format.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t *Time) StrToDate(sc *stmtctx.StatementContext, date, format string) bool {
	trace_util_0.Count(_time_00000, 615)
	ctx := make(map[string]int)
	var tm MysqlTime
	if !strToDate(&tm, date, format, ctx) {
		trace_util_0.Count(_time_00000, 618)
		t.Time = ZeroTime
		t.Type = mysql.TypeDatetime
		t.Fsp = 0
		return false
	}
	trace_util_0.Count(_time_00000, 616)
	if err := mysqlTimeFix(&tm, ctx); err != nil {
		trace_util_0.Count(_time_00000, 619)
		return false
	}

	trace_util_0.Count(_time_00000, 617)
	t.Time = tm
	t.Type = mysql.TypeDatetime
	return t.check(sc) == nil
}

// mysqlTimeFix fixes the MysqlTime use the values in the context.
func mysqlTimeFix(t *MysqlTime, ctx map[string]int) error {
	trace_util_0.Count(_time_00000, 620)
	// Key of the ctx is the format char, such as `%j` `%p` and so on.
	if yearOfDay, ok := ctx["%j"]; ok {
		trace_util_0.Count(_time_00000, 623)
		// TODO: Implement the function that converts day of year to yy:mm:dd.
		_ = yearOfDay
	}
	trace_util_0.Count(_time_00000, 621)
	if valueAMorPm, ok := ctx["%p"]; ok {
		trace_util_0.Count(_time_00000, 624)
		if _, ok := ctx["%H"]; ok {
			trace_util_0.Count(_time_00000, 628)
			return ErrInvalidTimeFormat.GenWithStackByArgs(t)
		}
		trace_util_0.Count(_time_00000, 625)
		if t.hour == 0 {
			trace_util_0.Count(_time_00000, 629)
			return ErrInvalidTimeFormat.GenWithStackByArgs(t)
		}
		trace_util_0.Count(_time_00000, 626)
		if t.hour == 12 {
			trace_util_0.Count(_time_00000, 630)
			// 12 is a special hour.
			switch valueAMorPm {
			case constForAM:
				trace_util_0.Count(_time_00000, 632)
				t.hour = 0
			case constForPM:
				trace_util_0.Count(_time_00000, 633)
				t.hour = 12
			}
			trace_util_0.Count(_time_00000, 631)
			return nil
		}
		trace_util_0.Count(_time_00000, 627)
		if valueAMorPm == constForPM {
			trace_util_0.Count(_time_00000, 634)
			t.hour += 12
		}
	}
	trace_util_0.Count(_time_00000, 622)
	return nil
}

// strToDate converts date string according to format, returns true on success,
// the value will be stored in argument t or ctx.
func strToDate(t *MysqlTime, date string, format string, ctx map[string]int) bool {
	trace_util_0.Count(_time_00000, 635)
	date = skipWhiteSpace(date)
	format = skipWhiteSpace(format)

	token, formatRemain, succ := getFormatToken(format)
	if !succ {
		trace_util_0.Count(_time_00000, 639)
		return false
	}

	trace_util_0.Count(_time_00000, 636)
	if token == "" {
		trace_util_0.Count(_time_00000, 640)
		// Extra characters at the end of date are ignored.
		return true
	}

	trace_util_0.Count(_time_00000, 637)
	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		trace_util_0.Count(_time_00000, 641)
		return false
	}

	trace_util_0.Count(_time_00000, 638)
	return strToDate(t, dateRemain, formatRemain, ctx)
}

// getFormatToken takes one format control token from the string.
// format "%d %H %m" will get token "%d" and the remain is " %H %m".
func getFormatToken(format string) (token string, remain string, succ bool) {
	trace_util_0.Count(_time_00000, 642)
	if len(format) == 0 {
		trace_util_0.Count(_time_00000, 646)
		return "", "", true
	}

	// Just one character.
	trace_util_0.Count(_time_00000, 643)
	if len(format) == 1 {
		trace_util_0.Count(_time_00000, 647)
		if format[0] == '%' {
			trace_util_0.Count(_time_00000, 649)
			return "", "", false
		}
		trace_util_0.Count(_time_00000, 648)
		return format, "", true
	}

	// More than one character.
	trace_util_0.Count(_time_00000, 644)
	if format[0] == '%' {
		trace_util_0.Count(_time_00000, 650)
		return format[:2], format[2:], true
	}

	trace_util_0.Count(_time_00000, 645)
	return format[:1], format[1:], true
}

func skipWhiteSpace(input string) string {
	trace_util_0.Count(_time_00000, 651)
	for i, c := range input {
		trace_util_0.Count(_time_00000, 653)
		if !unicode.IsSpace(c) {
			trace_util_0.Count(_time_00000, 654)
			return input[i:]
		}
	}
	trace_util_0.Count(_time_00000, 652)
	return ""
}

var weekdayAbbrev = map[string]gotime.Weekday{
	"Sun": gotime.Sunday,
	"Mon": gotime.Monday,
	"Tue": gotime.Tuesday,
	"Wed": gotime.Wednesday,
	"Thu": gotime.Tuesday,
	"Fri": gotime.Friday,
	"Sat": gotime.Saturday,
}

var monthAbbrev = map[string]gotime.Month{
	"Jan": gotime.January,
	"Feb": gotime.February,
	"Mar": gotime.March,
	"Apr": gotime.April,
	"May": gotime.May,
	"Jun": gotime.June,
	"Jul": gotime.July,
	"Aug": gotime.August,
	"Sep": gotime.September,
	"Oct": gotime.October,
	"Nov": gotime.November,
	"Dec": gotime.December,
}

type dateFormatParser func(t *MysqlTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,      // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,          // Month, numeric (0..12)
	"%d": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%e": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%f": microSeconds,          // Microseconds (000000..999999)
	"%h": hour24TwoDigits,       // Hour (01..12)
	"%H": hour24Numeric,         // Hour (00..23)
	"%I": hour12Numeric,         // Hour (01..12)
	"%i": minutesNumeric,        // Minutes, numeric (00..59)
	"%j": dayOfYearThreeDigits,  // Day of year (001..366)
	"%k": hour24Numeric,         // Hour (0..23)
	"%l": hour12Numeric,         // Hour (1..12)
	"%M": fullNameMonth,         // Month name (January..December)
	"%m": monthNumeric,          // Month, numeric (00..12)
	"%p": isAMOrPM,              // AM or PM
	"%r": time12Hour,            // Time, 12-hour (hh:mm:ss followed by AM or PM)
	"%s": secondsNumeric,        // Seconds (00..59)
	"%S": secondsNumeric,        // Seconds (00..59)
	"%T": time24Hour,            // Time, 24-hour (hh:mm:ss)
	"%Y": yearNumericFourDigits, // Year, numeric, four digits
	// Deprecated since MySQL 5.7.5
	"%y": yearNumericTwoDigits, // Year, numeric (two digits)
	// TODO: Add the following...
	// "%a": abbreviatedWeekday,         // Abbreviated weekday name (Sun..Sat)
	// "%D": dayOfMonthWithSuffix,       // Day of the month with English suffix (0th, 1st, 2nd, 3rd)
	// "%U": weekMode0,                  // Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
	// "%u": weekMode1,                  // Week (00..53), where Monday is the first day of the week; WEEK() mode 1
	// "%V": weekMode2,                  // Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
	// "%v": weekMode3,                  // Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
	// "%W": weekdayName,                // Weekday name (Sunday..Saturday)
	// "%w": dayOfWeek,                  // Day of the week (0=Sunday..6=Saturday)
	// "%X": yearOfWeek,                 // Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
	// "%x": yearOfWeek,                 // Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
}

// GetFormatType checks the type(Duration, Date or Datetime) of a format string.
func GetFormatType(format string) (isDuration, isDate bool) {
	trace_util_0.Count(_time_00000, 655)
	format = skipWhiteSpace(format)
	var token string
	var succ bool
	for {
		trace_util_0.Count(_time_00000, 657)
		token, format, succ = getFormatToken(format)
		if len(token) == 0 {
			trace_util_0.Count(_time_00000, 662)
			break
		}
		trace_util_0.Count(_time_00000, 658)
		if !succ {
			trace_util_0.Count(_time_00000, 663)
			isDuration, isDate = false, false
			break
		}
		trace_util_0.Count(_time_00000, 659)
		var durationTokens bool
		var dateTokens bool
		if len(token) >= 2 && token[0] == '%' {
			trace_util_0.Count(_time_00000, 664)
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l':
				trace_util_0.Count(_time_00000, 665)
				durationTokens = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				trace_util_0.Count(_time_00000, 666)
				dateTokens = true
			}
		}
		trace_util_0.Count(_time_00000, 660)
		if durationTokens {
			trace_util_0.Count(_time_00000, 667)
			isDuration = true
		} else {
			trace_util_0.Count(_time_00000, 668)
			if dateTokens {
				trace_util_0.Count(_time_00000, 669)
				isDate = true
			}
		}
		trace_util_0.Count(_time_00000, 661)
		if isDuration && isDate {
			trace_util_0.Count(_time_00000, 670)
			break
		}
	}
	trace_util_0.Count(_time_00000, 656)
	return
}

func matchDateWithToken(t *MysqlTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	trace_util_0.Count(_time_00000, 671)
	if parse, ok := dateFormatParserTable[token]; ok {
		trace_util_0.Count(_time_00000, 674)
		return parse(t, date, ctx)
	}

	trace_util_0.Count(_time_00000, 672)
	if strings.HasPrefix(date, token) {
		trace_util_0.Count(_time_00000, 675)
		return date[len(token):], true
	}
	trace_util_0.Count(_time_00000, 673)
	return date, false
}

func parseDigits(input string, count int) (int, bool) {
	trace_util_0.Count(_time_00000, 676)
	if count <= 0 || len(input) < count {
		trace_util_0.Count(_time_00000, 679)
		return 0, false
	}

	trace_util_0.Count(_time_00000, 677)
	v, err := strconv.ParseUint(input[:count], 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 680)
		return int(v), false
	}
	trace_util_0.Count(_time_00000, 678)
	return int(v), true
}

func hour24TwoDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 681)
	v, succ := parseDigits(input, 2)
	if !succ || v >= 24 {
		trace_util_0.Count(_time_00000, 683)
		return input, false
	}
	trace_util_0.Count(_time_00000, 682)
	t.hour = v
	return input[2:], true
}

func secondsNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 684)
	result := oneOrTwoDigitRegex.FindString(input)
	length := len(result)

	v, succ := parseDigits(input, length)
	if !succ || v >= 60 {
		trace_util_0.Count(_time_00000, 686)
		return input, false
	}
	trace_util_0.Count(_time_00000, 685)
	t.second = uint8(v)
	return input[length:], true
}

func minutesNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 687)
	result := oneOrTwoDigitRegex.FindString(input)
	length := len(result)

	v, succ := parseDigits(input, length)
	if !succ || v >= 60 {
		trace_util_0.Count(_time_00000, 689)
		return input, false
	}
	trace_util_0.Count(_time_00000, 688)
	t.minute = uint8(v)
	return input[length:], true
}

const time12HourLen = len("hh:mm:ssAM")

func time12Hour(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 690)
	// hh:mm:ss AM
	if len(input) < time12HourLen {
		trace_util_0.Count(_time_00000, 696)
		return input, false
	}
	trace_util_0.Count(_time_00000, 691)
	hour, succ := parseDigits(input, 2)
	if !succ || hour > 12 || hour == 0 || input[2] != ':' {
		trace_util_0.Count(_time_00000, 697)
		return input, false
	}

	trace_util_0.Count(_time_00000, 692)
	minute, succ := parseDigits(input[3:], 2)
	if !succ || minute > 59 || input[5] != ':' {
		trace_util_0.Count(_time_00000, 698)
		return input, false
	}

	trace_util_0.Count(_time_00000, 693)
	second, succ := parseDigits(input[6:], 2)
	if !succ || second > 59 {
		trace_util_0.Count(_time_00000, 699)
		return input, false
	}

	trace_util_0.Count(_time_00000, 694)
	remain := skipWhiteSpace(input[8:])
	switch {
	case strings.HasPrefix(remain, "AM"):
		trace_util_0.Count(_time_00000, 700)
		t.hour = hour
	case strings.HasPrefix(remain, "PM"):
		trace_util_0.Count(_time_00000, 701)
		t.hour = hour + 12
	default:
		trace_util_0.Count(_time_00000, 702)
		return input, false
	}

	trace_util_0.Count(_time_00000, 695)
	t.minute = uint8(minute)
	t.second = uint8(second)
	return remain, true
}

const time24HourLen = len("hh:mm:ss")

func time24Hour(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 703)
	// hh:mm:ss
	if len(input) < time24HourLen {
		trace_util_0.Count(_time_00000, 708)
		return input, false
	}

	trace_util_0.Count(_time_00000, 704)
	hour, succ := parseDigits(input, 2)
	if !succ || hour > 23 || input[2] != ':' {
		trace_util_0.Count(_time_00000, 709)
		return input, false
	}

	trace_util_0.Count(_time_00000, 705)
	minute, succ := parseDigits(input[3:], 2)
	if !succ || minute > 59 || input[5] != ':' {
		trace_util_0.Count(_time_00000, 710)
		return input, false
	}

	trace_util_0.Count(_time_00000, 706)
	second, succ := parseDigits(input[6:], 2)
	if !succ || second > 59 {
		trace_util_0.Count(_time_00000, 711)
		return input, false
	}

	trace_util_0.Count(_time_00000, 707)
	t.hour = hour
	t.minute = uint8(minute)
	t.second = uint8(second)
	return input[8:], true
}

const (
	constForAM = 1 + iota
	constForPM
)

func isAMOrPM(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 712)
	if len(input) < 2 {
		trace_util_0.Count(_time_00000, 715)
		return input, false
	}

	trace_util_0.Count(_time_00000, 713)
	s := strings.ToLower(input[:2])
	switch s {
	case "am":
		trace_util_0.Count(_time_00000, 716)
		ctx["%p"] = constForAM
	case "pm":
		trace_util_0.Count(_time_00000, 717)
		ctx["%p"] = constForPM
	default:
		trace_util_0.Count(_time_00000, 718)
		return input, false
	}
	trace_util_0.Count(_time_00000, 714)
	return input[2:], true
}

// digitRegex: it was used to scan a variable-length monthly day or month in the string. Ex:  "01" or "1" or "30"
var oneOrTwoDigitRegex = regexp.MustCompile("^[0-9]{1,2}")

// twoDigitRegex: it was just for two digit number string. Ex: "01" or "12"
var twoDigitRegex = regexp.MustCompile("^[1-9][0-9]?")

// oneToSixDigitRegex: it was just for [0, 999999]
var oneToSixDigitRegex = regexp.MustCompile("^[0-9]{0,6}")

// numericRegex: it was for any numeric characters
var numericRegex = regexp.MustCompile("[0-9]+")

// parseTwoNumeric is used for pattens 0..31 0..24 0..60 and so on.
// It returns the parsed int, and remain data after parse.
func parseTwoNumeric(input string) (int, string) {
	trace_util_0.Count(_time_00000, 719)
	if len(input) > 1 && input[0] == '0' {
		trace_util_0.Count(_time_00000, 723)
		return 0, input[1:]
	}
	trace_util_0.Count(_time_00000, 720)
	matched := twoDigitRegex.FindAllString(input, -1)
	if len(matched) == 0 {
		trace_util_0.Count(_time_00000, 724)
		return 0, input
	}

	trace_util_0.Count(_time_00000, 721)
	str := matched[0]
	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		trace_util_0.Count(_time_00000, 725)
		return 0, input
	}
	trace_util_0.Count(_time_00000, 722)
	return int(v), input[len(str):]
}

func dayOfMonthNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 726)
	result := oneOrTwoDigitRegex.FindString(input) // 0..31
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 31 {
		trace_util_0.Count(_time_00000, 728)
		return input, false
	}
	trace_util_0.Count(_time_00000, 727)
	t.day = uint8(v)
	return input[length:], true
}

func hour24Numeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 729)
	result := oneOrTwoDigitRegex.FindString(input) // 0..23
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 23 {
		trace_util_0.Count(_time_00000, 731)
		return input, false
	}
	trace_util_0.Count(_time_00000, 730)
	t.hour = v
	ctx["%H"] = v
	return input[length:], true
}

func hour12Numeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 732)
	result := oneOrTwoDigitRegex.FindString(input) // 1..12
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 12 || v == 0 {
		trace_util_0.Count(_time_00000, 734)
		return input, false
	}
	trace_util_0.Count(_time_00000, 733)
	t.hour = v
	return input[length:], true
}

func microSeconds(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 735)
	result := oneToSixDigitRegex.FindString(input)
	length := len(result)
	if length == 0 {
		trace_util_0.Count(_time_00000, 739)
		t.microsecond = 0
		return input, true
	}

	trace_util_0.Count(_time_00000, 736)
	v, ok := parseDigits(input, length)

	if !ok {
		trace_util_0.Count(_time_00000, 740)
		return input, false
	}
	trace_util_0.Count(_time_00000, 737)
	for v > 0 && v*10 < 1000000 {
		trace_util_0.Count(_time_00000, 741)
		v *= 10
	}
	trace_util_0.Count(_time_00000, 738)
	t.microsecond = uint32(v)
	return input[length:], true
}

func yearNumericFourDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 742)
	return yearNumericNDigits(t, input, ctx, 4)
}

func yearNumericTwoDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 743)
	return yearNumericNDigits(t, input, ctx, 2)
}

func yearNumericNDigits(t *MysqlTime, input string, ctx map[string]int, n int) (string, bool) {
	trace_util_0.Count(_time_00000, 744)
	effectiveCount, effectiveValue := 0, 0
	for effectiveCount+1 <= n {
		trace_util_0.Count(_time_00000, 748)
		value, succeed := parseDigits(input, effectiveCount+1)
		if !succeed {
			trace_util_0.Count(_time_00000, 750)
			break
		}
		trace_util_0.Count(_time_00000, 749)
		effectiveCount++
		effectiveValue = value
	}
	trace_util_0.Count(_time_00000, 745)
	if effectiveCount == 0 {
		trace_util_0.Count(_time_00000, 751)
		return input, false
	}
	trace_util_0.Count(_time_00000, 746)
	if effectiveCount <= 2 {
		trace_util_0.Count(_time_00000, 752)
		effectiveValue = adjustYear(effectiveValue)
	}
	trace_util_0.Count(_time_00000, 747)
	t.year = uint16(effectiveValue)
	return input[effectiveCount:], true
}

func dayOfYearThreeDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 753)
	v, succ := parseDigits(input, 3)
	if !succ || v == 0 || v > 366 {
		trace_util_0.Count(_time_00000, 755)
		return input, false
	}
	trace_util_0.Count(_time_00000, 754)
	ctx["%j"] = v
	return input[3:], true
}

func abbreviatedWeekday(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 756)
	if len(input) >= 3 {
		trace_util_0.Count(_time_00000, 758)
		dayName := input[:3]
		if _, ok := weekdayAbbrev[dayName]; ok {
			trace_util_0.Count(_time_00000, 759)
			// TODO: We need refact mysql time to support this.
			return input, false
		}
	}
	trace_util_0.Count(_time_00000, 757)
	return input, false
}

func abbreviatedMonth(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 760)
	if len(input) >= 3 {
		trace_util_0.Count(_time_00000, 762)
		monthName := input[:3]
		if month, ok := monthAbbrev[monthName]; ok {
			trace_util_0.Count(_time_00000, 763)
			t.month = uint8(month)
			return input[len(monthName):], true
		}
	}
	trace_util_0.Count(_time_00000, 761)
	return input, false
}

func fullNameMonth(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 764)
	for i, month := range MonthNames {
		trace_util_0.Count(_time_00000, 766)
		if strings.HasPrefix(input, month) {
			trace_util_0.Count(_time_00000, 767)
			t.month = uint8(i + 1)
			return input[len(month):], true
		}
	}
	trace_util_0.Count(_time_00000, 765)
	return input, false
}

func monthNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 768)
	result := oneOrTwoDigitRegex.FindString(input) // 1..12
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 12 {
		trace_util_0.Count(_time_00000, 770)
		return input, false
	}
	trace_util_0.Count(_time_00000, 769)
	t.month = uint8(v)
	return input[length:], true
}

//  dayOfMonthWithSuffix returns different suffix according t being which day. i.e. 0 return th. 1 return st.
func dayOfMonthWithSuffix(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	trace_util_0.Count(_time_00000, 771)
	month, remain := parseOrdinalNumbers(input)
	if month >= 0 {
		trace_util_0.Count(_time_00000, 773)
		t.month = uint8(month)
		return remain, true
	}
	trace_util_0.Count(_time_00000, 772)
	return input, false
}

func parseOrdinalNumbers(input string) (value int, remain string) {
	trace_util_0.Count(_time_00000, 774)
	for i, c := range input {
		trace_util_0.Count(_time_00000, 777)
		if !unicode.IsDigit(c) {
			trace_util_0.Count(_time_00000, 778)
			v, err := strconv.ParseUint(input[:i], 10, 64)
			if err != nil {
				trace_util_0.Count(_time_00000, 780)
				return -1, input
			}
			trace_util_0.Count(_time_00000, 779)
			value = int(v)
			break
		}
	}
	trace_util_0.Count(_time_00000, 775)
	switch {
	case strings.HasPrefix(remain, "st"):
		trace_util_0.Count(_time_00000, 781)
		if value == 1 {
			trace_util_0.Count(_time_00000, 784)
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "nd"):
		trace_util_0.Count(_time_00000, 782)
		if value == 2 {
			trace_util_0.Count(_time_00000, 785)
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "th"):
		trace_util_0.Count(_time_00000, 783)
		remain = remain[2:]
		return
	}
	trace_util_0.Count(_time_00000, 776)
	return -1, input
}

// DateFSP gets fsp from date string.
func DateFSP(date string) (fsp int) {
	trace_util_0.Count(_time_00000, 786)
	i := strings.LastIndex(date, ".")
	if i != -1 {
		trace_util_0.Count(_time_00000, 788)
		fsp = len(date) - i - 1
	}
	trace_util_0.Count(_time_00000, 787)
	return
}

// DateTimeIsOverflow return if this date is overflow.
// See: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DateTimeIsOverflow(sc *stmtctx.StatementContext, date Time) (bool, error) {
	trace_util_0.Count(_time_00000, 789)
	tz := sc.TimeZone
	if tz == nil {
		trace_util_0.Count(_time_00000, 793)
		tz = gotime.Local
	}

	trace_util_0.Count(_time_00000, 790)
	var err error
	var b, e, t gotime.Time
	switch date.Type {
	case mysql.TypeDate, mysql.TypeDatetime:
		trace_util_0.Count(_time_00000, 794)
		if b, err = MinDatetime.GoTime(tz); err != nil {
			trace_util_0.Count(_time_00000, 800)
			return false, err
		}
		trace_util_0.Count(_time_00000, 795)
		if e, err = MaxDatetime.GoTime(tz); err != nil {
			trace_util_0.Count(_time_00000, 801)
			return false, err
		}
	case mysql.TypeTimestamp:
		trace_util_0.Count(_time_00000, 796)
		minTS, maxTS := MinTimestamp, MaxTimestamp
		if tz != gotime.UTC {
			trace_util_0.Count(_time_00000, 802)
			if err = minTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				trace_util_0.Count(_time_00000, 804)
				return false, err
			}
			trace_util_0.Count(_time_00000, 803)
			if err = maxTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				trace_util_0.Count(_time_00000, 805)
				return false, err
			}
		}
		trace_util_0.Count(_time_00000, 797)
		if b, err = minTS.Time.GoTime(tz); err != nil {
			trace_util_0.Count(_time_00000, 806)
			return false, err
		}
		trace_util_0.Count(_time_00000, 798)
		if e, err = maxTS.Time.GoTime(tz); err != nil {
			trace_util_0.Count(_time_00000, 807)
			return false, err
		}
	default:
		trace_util_0.Count(_time_00000, 799)
		return false, nil
	}

	trace_util_0.Count(_time_00000, 791)
	if t, err = date.Time.GoTime(tz); err != nil {
		trace_util_0.Count(_time_00000, 808)
		return false, err
	}

	trace_util_0.Count(_time_00000, 792)
	inRange := (t.After(b) || t.Equal(b)) && (t.Before(e) || t.Equal(e))
	return !inRange, nil
}

var _time_00000 = "types/time.go"
