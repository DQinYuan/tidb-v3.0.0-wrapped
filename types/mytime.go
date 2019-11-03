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

package types

import (
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

// MysqlTime is the internal struct type for Time.
type MysqlTime struct {
	year  uint16 // year <= 9999
	month uint8  // month <= 12
	day   uint8  // day <= 31
	// When it's type is Time, HH:MM:SS may be 839:59:59, so use int to avoid overflow.
	hour        int   // hour <= 23
	minute      uint8 // minute <= 59
	second      uint8 // second <= 59
	microsecond uint32
}

// Year returns the year value.
func (t MysqlTime) Year() int {
	trace_util_0.Count(_mytime_00000, 0)
	return int(t.year)
}

// Month returns the month value.
func (t MysqlTime) Month() int {
	trace_util_0.Count(_mytime_00000, 1)
	return int(t.month)
}

// Day returns the day value.
func (t MysqlTime) Day() int {
	trace_util_0.Count(_mytime_00000, 2)
	return int(t.day)
}

// Hour returns the hour value.
func (t MysqlTime) Hour() int {
	trace_util_0.Count(_mytime_00000, 3)
	return int(t.hour)
}

// Minute returns the minute value.
func (t MysqlTime) Minute() int {
	trace_util_0.Count(_mytime_00000, 4)
	return int(t.minute)
}

// Second returns the second value.
func (t MysqlTime) Second() int {
	trace_util_0.Count(_mytime_00000, 5)
	return int(t.second)
}

// Microsecond returns the microsecond value.
func (t MysqlTime) Microsecond() int {
	trace_util_0.Count(_mytime_00000, 6)
	return int(t.microsecond)
}

// Weekday returns the Weekday value.
func (t MysqlTime) Weekday() gotime.Weekday {
	trace_util_0.Count(_mytime_00000, 7)
	// TODO: Consider time_zone variable.
	t1, err := t.GoTime(gotime.Local)
	if err != nil {
		trace_util_0.Count(_mytime_00000, 9)
		return 0
	}
	trace_util_0.Count(_mytime_00000, 8)
	return t1.Weekday()
}

// YearDay returns day in year.
func (t MysqlTime) YearDay() int {
	trace_util_0.Count(_mytime_00000, 10)
	if t.month == 0 || t.day == 0 {
		trace_util_0.Count(_mytime_00000, 12)
		return 0
	}
	trace_util_0.Count(_mytime_00000, 11)
	return calcDaynr(int(t.year), int(t.month), int(t.day)) -
		calcDaynr(int(t.year), 1, 1) + 1
}

// YearWeek return year and week.
func (t MysqlTime) YearWeek(mode int) (int, int) {
	trace_util_0.Count(_mytime_00000, 13)
	behavior := weekMode(mode) | weekBehaviourYear
	return calcWeek(&t, behavior)
}

// Week returns the week value.
func (t MysqlTime) Week(mode int) int {
	trace_util_0.Count(_mytime_00000, 14)
	if t.month == 0 || t.day == 0 {
		trace_util_0.Count(_mytime_00000, 16)
		return 0
	}
	trace_util_0.Count(_mytime_00000, 15)
	_, week := calcWeek(&t, weekMode(mode))
	return week
}

// GoTime converts MysqlTime to GoTime.
func (t MysqlTime) GoTime(loc *gotime.Location) (gotime.Time, error) {
	trace_util_0.Count(_mytime_00000, 17)
	// gotime.Time can't represent month 0 or day 0, date contains 0 would be converted to a nearest date,
	// For example, 2006-12-00 00:00:00 would become 2015-11-30 23:59:59.
	tm := gotime.Date(t.Year(), gotime.Month(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Microsecond()*1000, loc)
	year, month, day := tm.Date()
	hour, minute, second := tm.Clock()
	microsec := tm.Nanosecond() / 1000
	// This function will check the result, and return an error if it's not the same with the origin input.
	if year != t.Year() || int(month) != t.Month() || day != t.Day() ||
		hour != t.Hour() || minute != t.Minute() || second != t.Second() ||
		microsec != t.Microsecond() {
		trace_util_0.Count(_mytime_00000, 19)
		return tm, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}
	trace_util_0.Count(_mytime_00000, 18)
	return tm, nil
}

// IsLeapYear returns if it's leap year.
func (t MysqlTime) IsLeapYear() bool {
	trace_util_0.Count(_mytime_00000, 20)
	return isLeapYear(t.year)
}

func isLeapYear(year uint16) bool {
	trace_util_0.Count(_mytime_00000, 21)
	return (year%4 == 0 && year%100 != 0) || year%400 == 0
}

var daysByMonth = [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// GetLastDay returns the last day of the month
func GetLastDay(year, month int) int {
	trace_util_0.Count(_mytime_00000, 22)
	var day = 0
	if month > 0 && month <= 12 {
		trace_util_0.Count(_mytime_00000, 25)
		day = daysByMonth[month-1]
	}
	trace_util_0.Count(_mytime_00000, 23)
	if month == 2 && isLeapYear(uint16(year)) {
		trace_util_0.Count(_mytime_00000, 26)
		day = 29
	}
	trace_util_0.Count(_mytime_00000, 24)
	return day
}

func getFixDays(year, month, day int, ot gotime.Time) int {
	trace_util_0.Count(_mytime_00000, 27)
	if (year != 0 || month != 0) && day == 0 {
		trace_util_0.Count(_mytime_00000, 29)
		od := ot.Day()
		t := ot.AddDate(year, month, day)
		td := t.Day()
		if od != td {
			trace_util_0.Count(_mytime_00000, 30)
			tm := int(t.Month()) - 1
			tMax := GetLastDay(t.Year(), tm)
			dd := tMax - od
			return dd
		}
	}
	trace_util_0.Count(_mytime_00000, 28)
	return 0
}

// AddDate fix gap between mysql and golang api
// When we execute select date_add('2018-01-31',interval 1 month) in mysql we got 2018-02-28
// but in tidb we got 2018-03-03.
// Dig it and we found it's caused by golang api time.Date(year int, month Month, day, hour, min, sec, nsec int, loc *Location) Time ,
// it says October 32 converts to November 1 ,it conflits with mysql.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
func AddDate(year, month, day int64, ot gotime.Time) (nt gotime.Time) {
	trace_util_0.Count(_mytime_00000, 31)
	df := getFixDays(int(year), int(month), int(day), ot)
	if df != 0 {
		trace_util_0.Count(_mytime_00000, 33)
		nt = ot.AddDate(int(year), int(month), df)
	} else {
		trace_util_0.Count(_mytime_00000, 34)
		{
			nt = ot.AddDate(int(year), int(month), int(day))
		}
	}
	trace_util_0.Count(_mytime_00000, 32)
	return nt
}

func calcTimeFromSec(to *MysqlTime, seconds, microseconds int) {
	trace_util_0.Count(_mytime_00000, 35)
	to.hour = seconds / 3600
	seconds = seconds % 3600
	to.minute = uint8(seconds / 60)
	to.second = uint8(seconds % 60)
	to.microsecond = uint32(microseconds)
}

const secondsIn24Hour = 86400

// calcTimeDiff calculates difference between two datetime values as seconds + microseconds.
// t1 and t2 should be TIME/DATE/DATETIME value.
// sign can be +1 or -1, and t2 is preprocessed with sign first.
func calcTimeDiff(t1, t2 MysqlTime, sign int) (seconds, microseconds int, neg bool) {
	trace_util_0.Count(_mytime_00000, 36)
	days := calcDaynr(t1.Year(), t1.Month(), t1.Day())
	days2 := calcDaynr(t2.Year(), t2.Month(), t2.Day())
	days -= sign * days2

	tmp := (int64(days)*secondsIn24Hour+
		int64(t1.Hour())*3600+int64(t1.Minute())*60+
		int64(t1.Second())-
		int64(sign)*(int64(t2.Hour())*3600+int64(t2.Minute())*60+
			int64(t2.Second())))*
		1e6 +
		int64(t1.Microsecond()) - int64(sign)*int64(t2.Microsecond())

	if tmp < 0 {
		trace_util_0.Count(_mytime_00000, 38)
		tmp = -tmp
		neg = true
	}
	trace_util_0.Count(_mytime_00000, 37)
	seconds = int(tmp / 1e6)
	microseconds = int(tmp % 1e6)
	return
}

// datetimeToUint64 converts time value to integer in YYYYMMDDHHMMSS format.
func datetimeToUint64(t MysqlTime) uint64 {
	trace_util_0.Count(_mytime_00000, 39)
	return dateToUint64(t)*1e6 + timeToUint64(t)
}

// dateToUint64 converts time value to integer in YYYYMMDD format.
func dateToUint64(t MysqlTime) uint64 {
	trace_util_0.Count(_mytime_00000, 40)
	return uint64(t.Year())*10000 +
		uint64(t.Month())*100 +
		uint64(t.Day())
}

// timeToUint64 converts time value to integer in HHMMSS format.
func timeToUint64(t MysqlTime) uint64 {
	trace_util_0.Count(_mytime_00000, 41)
	return uint64(t.Hour())*10000 +
		uint64(t.Minute())*100 +
		uint64(t.Second())
}

// calcDaynr calculates days since 0000-00-00.
func calcDaynr(year, month, day int) int {
	trace_util_0.Count(_mytime_00000, 42)
	if year == 0 && month == 0 {
		trace_util_0.Count(_mytime_00000, 45)
		return 0
	}

	trace_util_0.Count(_mytime_00000, 43)
	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		trace_util_0.Count(_mytime_00000, 46)
		year--
	} else {
		trace_util_0.Count(_mytime_00000, 47)
		{
			delsum -= (month*4 + 23) / 10
		}
	}
	trace_util_0.Count(_mytime_00000, 44)
	temp := ((year/100 + 1) * 3) / 4
	return delsum + year/4 - temp
}

// DateDiff calculates number of days between two days.
func DateDiff(startTime, endTime MysqlTime) int {
	trace_util_0.Count(_mytime_00000, 48)
	return calcDaynr(startTime.Year(), startTime.Month(), startTime.Day()) - calcDaynr(endTime.Year(), endTime.Month(), endTime.Day())
}

// calcDaysInYear calculates days in one year, it works with 0 <= year <= 99.
func calcDaysInYear(year int) int {
	trace_util_0.Count(_mytime_00000, 49)
	if (year&3) == 0 && (year%100 != 0 || (year%400 == 0 && (year != 0))) {
		trace_util_0.Count(_mytime_00000, 51)
		return 366
	}
	trace_util_0.Count(_mytime_00000, 50)
	return 365
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
func calcWeekday(daynr int, sundayFirstDayOfWeek bool) int {
	trace_util_0.Count(_mytime_00000, 52)
	daynr += 5
	if sundayFirstDayOfWeek {
		trace_util_0.Count(_mytime_00000, 54)
		daynr++
	}
	trace_util_0.Count(_mytime_00000, 53)
	return daynr % 7
}

type weekBehaviour uint

const (
	// weekBehaviourMondayFirst set Monday as first day of week; otherwise Sunday is first day of week
	weekBehaviourMondayFirst weekBehaviour = 1 << iota
	// If set, Week is in range 1-53, otherwise Week is in range 0-53.
	// Note that this flag is only relevant if WEEK_JANUARY is not set.
	weekBehaviourYear
	// If not set, Weeks are numbered according to ISO 8601:1988.
	// If set, the week that contains the first 'first-day-of-week' is week 1.
	weekBehaviourFirstWeekday
)

func (v weekBehaviour) test(flag weekBehaviour) bool {
	trace_util_0.Count(_mytime_00000, 55)
	return (v & flag) != 0
}

func weekMode(mode int) weekBehaviour {
	trace_util_0.Count(_mytime_00000, 56)
	weekFormat := weekBehaviour(mode & 7)
	if (weekFormat & weekBehaviourMondayFirst) == 0 {
		trace_util_0.Count(_mytime_00000, 58)
		weekFormat ^= weekBehaviourFirstWeekday
	}
	trace_util_0.Count(_mytime_00000, 57)
	return weekFormat
}

// calcWeek calculates week and year for the time.
func calcWeek(t *MysqlTime, wb weekBehaviour) (year int, week int) {
	trace_util_0.Count(_mytime_00000, 59)
	var days int
	daynr := calcDaynr(int(t.year), int(t.month), int(t.day))
	firstDaynr := calcDaynr(int(t.year), 1, 1)
	mondayFirst := wb.test(weekBehaviourMondayFirst)
	weekYear := wb.test(weekBehaviourYear)
	firstWeekday := wb.test(weekBehaviourFirstWeekday)

	weekday := calcWeekday(firstDaynr, !mondayFirst)

	year = int(t.year)

	if t.month == 1 && int(t.day) <= 7-weekday {
		trace_util_0.Count(_mytime_00000, 63)
		if !weekYear &&
			((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			trace_util_0.Count(_mytime_00000, 65)
			week = 0
			return
		}
		trace_util_0.Count(_mytime_00000, 64)
		weekYear = true
		year--
		days = calcDaysInYear(year)
		firstDaynr -= days
		weekday = (weekday + 53*7 - days) % 7
	}

	trace_util_0.Count(_mytime_00000, 60)
	if (firstWeekday && weekday != 0) ||
		(!firstWeekday && weekday >= 4) {
		trace_util_0.Count(_mytime_00000, 66)
		days = daynr - (firstDaynr + 7 - weekday)
	} else {
		trace_util_0.Count(_mytime_00000, 67)
		{
			days = daynr - (firstDaynr - weekday)
		}
	}

	trace_util_0.Count(_mytime_00000, 61)
	if weekYear && days >= 52*7 {
		trace_util_0.Count(_mytime_00000, 68)
		weekday = (weekday + calcDaysInYear(year)) % 7
		if (!firstWeekday && weekday < 4) ||
			(firstWeekday && weekday == 0) {
			trace_util_0.Count(_mytime_00000, 69)
			year++
			week = 1
			return
		}
	}
	trace_util_0.Count(_mytime_00000, 62)
	week = days/7 + 1
	return
}

// mixDateAndTime mixes a date value and a time value.
func mixDateAndTime(date, time *MysqlTime, neg bool) {
	trace_util_0.Count(_mytime_00000, 70)
	if !neg && time.hour < 24 {
		trace_util_0.Count(_mytime_00000, 73)
		date.hour = time.hour
		date.minute = time.minute
		date.second = time.second
		date.microsecond = time.microsecond
		return
	}

	// Time is negative or outside of 24 hours internal.
	trace_util_0.Count(_mytime_00000, 71)
	sign := -1
	if neg {
		trace_util_0.Count(_mytime_00000, 74)
		sign = 1
	}
	trace_util_0.Count(_mytime_00000, 72)
	seconds, microseconds, _ := calcTimeDiff(*date, *time, sign)

	// If we want to use this function with arbitrary dates, this code will need
	// to cover cases when time is negative and "date < -time".

	days := seconds / secondsIn24Hour
	calcTimeFromSec(date, seconds%secondsIn24Hour, microseconds)
	year, month, day := getDateFromDaynr(uint(days))
	date.year = uint16(year)
	date.month = uint8(month)
	date.day = uint8(day)
}

var daysInMonth = []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// getDateFromDaynr changes a daynr to year, month and day,
// daynr 0 is returned as date 00.00.00
func getDateFromDaynr(daynr uint) (year uint, month uint, day uint) {
	trace_util_0.Count(_mytime_00000, 75)
	if daynr <= 365 || daynr >= 3652500 {
		trace_util_0.Count(_mytime_00000, 80)
		return
	}

	trace_util_0.Count(_mytime_00000, 76)
	year = daynr * 100 / 36525
	temp := (((year-1)/100 + 1) * 3) / 4
	dayOfYear := daynr - year*365 - (year-1)/4 + temp

	daysInYear := calcDaysInYear(int(year))
	for dayOfYear > uint(daysInYear) {
		trace_util_0.Count(_mytime_00000, 81)
		dayOfYear -= uint(daysInYear)
		year++
		daysInYear = calcDaysInYear(int(year))
	}

	trace_util_0.Count(_mytime_00000, 77)
	leapDay := uint(0)
	if daysInYear == 366 {
		trace_util_0.Count(_mytime_00000, 82)
		if dayOfYear > 31+28 {
			trace_util_0.Count(_mytime_00000, 83)
			dayOfYear--
			if dayOfYear == 31+28 {
				trace_util_0.Count(_mytime_00000, 84)
				// Handle leapyears leapday.
				leapDay = 1
			}
		}
	}

	trace_util_0.Count(_mytime_00000, 78)
	month = 1
	for _, days := range daysInMonth {
		trace_util_0.Count(_mytime_00000, 85)
		if dayOfYear <= uint(days) {
			trace_util_0.Count(_mytime_00000, 87)
			break
		}
		trace_util_0.Count(_mytime_00000, 86)
		dayOfYear -= uint(days)
		month++
	}

	trace_util_0.Count(_mytime_00000, 79)
	day = dayOfYear + leapDay
	return
}

const (
	intervalYEAR        = "YEAR"
	intervalQUARTER     = "QUARTER"
	intervalMONTH       = "MONTH"
	intervalWEEK        = "WEEK"
	intervalDAY         = "DAY"
	intervalHOUR        = "HOUR"
	intervalMINUTE      = "MINUTE"
	intervalSECOND      = "SECOND"
	intervalMICROSECOND = "MICROSECOND"
)

func timestampDiff(intervalType string, t1 MysqlTime, t2 MysqlTime) int64 {
	trace_util_0.Count(_mytime_00000, 88)
	seconds, microseconds, neg := calcTimeDiff(t2, t1, 1)
	months := uint(0)
	if intervalType == intervalYEAR || intervalType == intervalQUARTER ||
		intervalType == intervalMONTH {
		trace_util_0.Count(_mytime_00000, 92)
		var (
			yearBeg, yearEnd, monthBeg, monthEnd, dayBeg, dayEnd uint
			secondBeg, secondEnd, microsecondBeg, microsecondEnd uint
		)

		if neg {
			trace_util_0.Count(_mytime_00000, 96)
			yearBeg = uint(t2.Year())
			yearEnd = uint(t1.Year())
			monthBeg = uint(t2.Month())
			monthEnd = uint(t1.Month())
			dayBeg = uint(t2.Day())
			dayEnd = uint(t1.Day())
			secondBeg = uint(t2.Hour()*3600 + t2.Minute()*60 + t2.Second())
			secondEnd = uint(t1.Hour()*3600 + t1.Minute()*60 + t1.Second())
			microsecondBeg = uint(t2.Microsecond())
			microsecondEnd = uint(t1.Microsecond())
		} else {
			trace_util_0.Count(_mytime_00000, 97)
			{
				yearBeg = uint(t1.Year())
				yearEnd = uint(t2.Year())
				monthBeg = uint(t1.Month())
				monthEnd = uint(t2.Month())
				dayBeg = uint(t1.Day())
				dayEnd = uint(t2.Day())
				secondBeg = uint(t1.Hour()*3600 + t1.Minute()*60 + t1.Second())
				secondEnd = uint(t2.Hour()*3600 + t2.Minute()*60 + t2.Second())
				microsecondBeg = uint(t1.Microsecond())
				microsecondEnd = uint(t2.Microsecond())
			}
		}

		// calc years
		trace_util_0.Count(_mytime_00000, 93)
		years := yearEnd - yearBeg
		if monthEnd < monthBeg ||
			(monthEnd == monthBeg && dayEnd < dayBeg) {
			trace_util_0.Count(_mytime_00000, 98)
			years--
		}

		// calc months
		trace_util_0.Count(_mytime_00000, 94)
		months = 12 * years
		if monthEnd < monthBeg ||
			(monthEnd == monthBeg && dayEnd < dayBeg) {
			trace_util_0.Count(_mytime_00000, 99)
			months += 12 - (monthBeg - monthEnd)
		} else {
			trace_util_0.Count(_mytime_00000, 100)
			{
				months += monthEnd - monthBeg
			}
		}

		trace_util_0.Count(_mytime_00000, 95)
		if dayEnd < dayBeg {
			trace_util_0.Count(_mytime_00000, 101)
			months--
		} else {
			trace_util_0.Count(_mytime_00000, 102)
			if (dayEnd == dayBeg) &&
				((secondEnd < secondBeg) ||
					(secondEnd == secondBeg && microsecondEnd < microsecondBeg)) {
				trace_util_0.Count(_mytime_00000, 103)
				months--
			}
		}
	}

	trace_util_0.Count(_mytime_00000, 89)
	negV := int64(1)
	if neg {
		trace_util_0.Count(_mytime_00000, 104)
		negV = -1
	}
	trace_util_0.Count(_mytime_00000, 90)
	switch intervalType {
	case intervalYEAR:
		trace_util_0.Count(_mytime_00000, 105)
		return int64(months) / 12 * negV
	case intervalQUARTER:
		trace_util_0.Count(_mytime_00000, 106)
		return int64(months) / 3 * negV
	case intervalMONTH:
		trace_util_0.Count(_mytime_00000, 107)
		return int64(months) * negV
	case intervalWEEK:
		trace_util_0.Count(_mytime_00000, 108)
		return int64(seconds) / secondsIn24Hour / 7 * negV
	case intervalDAY:
		trace_util_0.Count(_mytime_00000, 109)
		return int64(seconds) / secondsIn24Hour * negV
	case intervalHOUR:
		trace_util_0.Count(_mytime_00000, 110)
		return int64(seconds) / 3600 * negV
	case intervalMINUTE:
		trace_util_0.Count(_mytime_00000, 111)
		return int64(seconds) / 60 * negV
	case intervalSECOND:
		trace_util_0.Count(_mytime_00000, 112)
		return int64(seconds) * negV
	case intervalMICROSECOND:
		trace_util_0.Count(_mytime_00000, 113)
		// In MySQL difference between any two valid datetime values
		// in microseconds fits into longlong.
		return int64(seconds*1000000+microseconds) * negV
	}

	trace_util_0.Count(_mytime_00000, 91)
	return 0
}

var _mytime_00000 = "types/mytime.go"
