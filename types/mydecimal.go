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
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
)

// RoundMode is the type for round mode.
type RoundMode int32

// constant values.
const (
	ten0 = 1
	ten1 = 10
	ten2 = 100
	ten3 = 1000
	ten4 = 10000
	ten5 = 100000
	ten6 = 1000000
	ten7 = 10000000
	ten8 = 100000000
	ten9 = 1000000000

	maxWordBufLen = 9 // A MyDecimal holds 9 words.
	digitsPerWord = 9 // A word holds 9 digits.
	wordSize      = 4 // A word is 4 bytes int32.
	digMask       = ten8
	wordBase      = ten9
	wordMax       = wordBase - 1
	notFixedDec   = 31

	DivFracIncr = 4

	// ModeHalfEven rounds normally.
	ModeHalfEven RoundMode = 5
	// Truncate just truncates the decimal.
	ModeTruncate RoundMode = 10
	// Ceiling is not supported now.
	modeCeiling RoundMode = 0
)

var (
	wordBufLen = 9
	mod9       = [128]int8{
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1, 2, 3, 4, 5, 6, 7, 8,
		0, 1,
	}
	div9 = [128]int{
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1, 1,
		2, 2, 2, 2, 2, 2, 2, 2, 2,
		3, 3, 3, 3, 3, 3, 3, 3, 3,
		4, 4, 4, 4, 4, 4, 4, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5, 5,
		6, 6, 6, 6, 6, 6, 6, 6, 6,
		7, 7, 7, 7, 7, 7, 7, 7, 7,
		8, 8, 8, 8, 8, 8, 8, 8, 8,
		9, 9, 9, 9, 9, 9, 9, 9, 9,
		10, 10, 10, 10, 10, 10, 10, 10, 10,
		11, 11, 11, 11, 11, 11, 11, 11, 11,
		12, 12, 12, 12, 12, 12, 12, 12, 12,
		13, 13, 13, 13, 13, 13, 13, 13, 13,
		14, 14,
	}
	powers10  = [10]int32{ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9}
	dig2bytes = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	fracMax   = [8]int32{
		900000000,
		990000000,
		999000000,
		999900000,
		999990000,
		999999000,
		999999900,
		999999990,
	}
	zeroMyDecimal = MyDecimal{}
)

// add adds a and b and carry, returns the sum and new carry.
func add(a, b, carry int32) (int32, int32) {
	trace_util_0.Count(_mydecimal_00000, 0)
	sum := a + b + carry
	if sum >= wordBase {
		trace_util_0.Count(_mydecimal_00000, 2)
		carry = 1
		sum -= wordBase
	} else {
		trace_util_0.Count(_mydecimal_00000, 3)
		{
			carry = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 1)
	return sum, carry
}

// add2 adds a and b and carry, returns the sum and new carry.
// It is only used in DecimalMul.
func add2(a, b, carry int32) (int32, int32) {
	trace_util_0.Count(_mydecimal_00000, 4)
	sum := int64(a) + int64(b) + int64(carry)
	if sum >= wordBase {
		trace_util_0.Count(_mydecimal_00000, 7)
		carry = 1
		sum -= wordBase
	} else {
		trace_util_0.Count(_mydecimal_00000, 8)
		{
			carry = 0
		}
	}

	trace_util_0.Count(_mydecimal_00000, 5)
	if sum >= wordBase {
		trace_util_0.Count(_mydecimal_00000, 9)
		sum -= wordBase
		carry++
	}
	trace_util_0.Count(_mydecimal_00000, 6)
	return int32(sum), carry
}

// sub subtracts b and carry from a, returns the diff and new carry.
func sub(a, b, carry int32) (int32, int32) {
	trace_util_0.Count(_mydecimal_00000, 10)
	diff := a - b - carry
	if diff < 0 {
		trace_util_0.Count(_mydecimal_00000, 12)
		carry = 1
		diff += wordBase
	} else {
		trace_util_0.Count(_mydecimal_00000, 13)
		{
			carry = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 11)
	return diff, carry
}

// sub2 subtracts b and carry from a, returns the diff and new carry.
// the new carry may be 2.
func sub2(a, b, carry int32) (int32, int32) {
	trace_util_0.Count(_mydecimal_00000, 14)
	diff := a - b - carry
	if diff < 0 {
		trace_util_0.Count(_mydecimal_00000, 17)
		carry = 1
		diff += wordBase
	} else {
		trace_util_0.Count(_mydecimal_00000, 18)
		{
			carry = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 15)
	if diff < 0 {
		trace_util_0.Count(_mydecimal_00000, 19)
		diff += wordBase
		carry++
	}
	trace_util_0.Count(_mydecimal_00000, 16)
	return diff, carry
}

// fixWordCntError limits word count in wordBufLen, and returns overflow or truncate error.
func fixWordCntError(wordsInt, wordsFrac int) (newWordsInt int, newWordsFrac int, err error) {
	trace_util_0.Count(_mydecimal_00000, 20)
	if wordsInt+wordsFrac > wordBufLen {
		trace_util_0.Count(_mydecimal_00000, 22)
		if wordsInt > wordBufLen {
			trace_util_0.Count(_mydecimal_00000, 24)
			return wordBufLen, 0, ErrOverflow
		}
		trace_util_0.Count(_mydecimal_00000, 23)
		return wordsInt, wordBufLen - wordsInt, ErrTruncated
	}
	trace_util_0.Count(_mydecimal_00000, 21)
	return wordsInt, wordsFrac, nil
}

/*
  countLeadingZeroes returns the number of leading zeroes that can be removed from fraction.

  @param   i    start index
  @param   word value to compare against list of powers of 10
*/
func countLeadingZeroes(i int, word int32) int {
	trace_util_0.Count(_mydecimal_00000, 25)
	leading := 0
	for word < powers10[i] {
		trace_util_0.Count(_mydecimal_00000, 27)
		i--
		leading++
	}
	trace_util_0.Count(_mydecimal_00000, 26)
	return leading
}

/*
  countTrailingZeros returns the number of trailing zeroes that can be removed from fraction.

  @param   i    start index
  @param   word  value to compare against list of powers of 10
*/
func countTrailingZeroes(i int, word int32) int {
	trace_util_0.Count(_mydecimal_00000, 28)
	trailing := 0
	for word%powers10[i] == 0 {
		trace_util_0.Count(_mydecimal_00000, 30)
		i++
		trailing++
	}
	trace_util_0.Count(_mydecimal_00000, 29)
	return trailing
}

func digitsToWords(digits int) int {
	trace_util_0.Count(_mydecimal_00000, 31)
	if digits+digitsPerWord-1 >= 0 && digits+digitsPerWord-1 < 128 {
		trace_util_0.Count(_mydecimal_00000, 33)
		return div9[digits+digitsPerWord-1]
	}
	trace_util_0.Count(_mydecimal_00000, 32)
	return (digits + digitsPerWord - 1) / digitsPerWord
}

// MyDecimalStructSize is the struct size of MyDecimal.
const MyDecimalStructSize = 40

// MyDecimal represents a decimal value.
type MyDecimal struct {
	digitsInt int8 // the number of *decimal* digits before the point.

	digitsFrac int8 // the number of decimal digits after the point.

	resultFrac int8 // result fraction digits.

	negative bool

	//  wordBuf is an array of int32 words.
	// A word is an int32 value can hold 9 digits.(0 <= word < wordBase)
	wordBuf [maxWordBufLen]int32
}

// IsNegative returns whether a decimal is negative.
func (d *MyDecimal) IsNegative() bool {
	trace_util_0.Count(_mydecimal_00000, 34)
	return d.negative
}

// GetDigitsFrac returns the digitsFrac.
func (d *MyDecimal) GetDigitsFrac() int8 {
	trace_util_0.Count(_mydecimal_00000, 35)
	return d.digitsFrac
}

// String returns the decimal string representation rounded to resultFrac.
func (d *MyDecimal) String() string {
	trace_util_0.Count(_mydecimal_00000, 36)
	tmp := *d
	err := tmp.Round(&tmp, int(tmp.resultFrac), ModeHalfEven)
	terror.Log(errors.Trace(err))
	return string(tmp.ToString())
}

func (d *MyDecimal) stringSize() int {
	trace_util_0.Count(_mydecimal_00000, 37)
	// sign, zero integer and dot.
	return int(d.digitsInt + d.digitsFrac + 3)
}

func (d *MyDecimal) removeLeadingZeros() (wordIdx int, digitsInt int) {
	trace_util_0.Count(_mydecimal_00000, 38)
	digitsInt = int(d.digitsInt)
	i := ((digitsInt - 1) % digitsPerWord) + 1
	for digitsInt > 0 && d.wordBuf[wordIdx] == 0 {
		trace_util_0.Count(_mydecimal_00000, 41)
		digitsInt -= i
		i = digitsPerWord
		wordIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 39)
	if digitsInt > 0 {
		trace_util_0.Count(_mydecimal_00000, 42)
		digitsInt -= countLeadingZeroes((digitsInt-1)%digitsPerWord, d.wordBuf[wordIdx])
	} else {
		trace_util_0.Count(_mydecimal_00000, 43)
		{
			digitsInt = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 40)
	return
}

func (d *MyDecimal) removeTrailingZeros() (lastWordIdx int, digitsFrac int) {
	trace_util_0.Count(_mydecimal_00000, 44)
	digitsFrac = int(d.digitsFrac)
	i := ((digitsFrac - 1) % digitsPerWord) + 1
	lastWordIdx = digitsToWords(int(d.digitsInt)) + digitsToWords(int(d.digitsFrac))
	for digitsFrac > 0 && d.wordBuf[lastWordIdx-1] == 0 {
		trace_util_0.Count(_mydecimal_00000, 47)
		digitsFrac -= i
		i = digitsPerWord
		lastWordIdx--
	}
	trace_util_0.Count(_mydecimal_00000, 45)
	if digitsFrac > 0 {
		trace_util_0.Count(_mydecimal_00000, 48)
		digitsFrac -= countTrailingZeroes(9-((digitsFrac-1)%digitsPerWord), d.wordBuf[lastWordIdx-1])
	} else {
		trace_util_0.Count(_mydecimal_00000, 49)
		{
			digitsFrac = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 46)
	return
}

// ToString converts decimal to its printable string representation without rounding.
//
//  RETURN VALUE
//
//      str       - result string
//      errCode   - eDecOK/eDecTruncate/eDecOverflow
//
func (d *MyDecimal) ToString() (str []byte) {
	trace_util_0.Count(_mydecimal_00000, 50)
	str = make([]byte, d.stringSize())
	digitsFrac := int(d.digitsFrac)
	wordStartIdx, digitsInt := d.removeLeadingZeros()
	if digitsInt+digitsFrac == 0 {
		trace_util_0.Count(_mydecimal_00000, 60)
		digitsInt = 1
		wordStartIdx = 0
	}

	trace_util_0.Count(_mydecimal_00000, 51)
	digitsIntLen := digitsInt
	if digitsIntLen == 0 {
		trace_util_0.Count(_mydecimal_00000, 61)
		digitsIntLen = 1
	}
	trace_util_0.Count(_mydecimal_00000, 52)
	digitsFracLen := digitsFrac
	length := digitsIntLen + digitsFracLen
	if d.negative {
		trace_util_0.Count(_mydecimal_00000, 62)
		length++
	}
	trace_util_0.Count(_mydecimal_00000, 53)
	if digitsFrac > 0 {
		trace_util_0.Count(_mydecimal_00000, 63)
		length++
	}
	trace_util_0.Count(_mydecimal_00000, 54)
	str = str[:length]
	strIdx := 0
	if d.negative {
		trace_util_0.Count(_mydecimal_00000, 64)
		str[strIdx] = '-'
		strIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 55)
	var fill int
	if digitsFrac > 0 {
		trace_util_0.Count(_mydecimal_00000, 65)
		fracIdx := strIdx + digitsIntLen
		fill = digitsFracLen - digitsFrac
		wordIdx := wordStartIdx + digitsToWords(digitsInt)
		str[fracIdx] = '.'
		fracIdx++
		for ; digitsFrac > 0; digitsFrac -= digitsPerWord {
			trace_util_0.Count(_mydecimal_00000, 67)
			x := d.wordBuf[wordIdx]
			wordIdx++
			for i := myMin(digitsFrac, digitsPerWord); i > 0; i-- {
				trace_util_0.Count(_mydecimal_00000, 68)
				y := x / digMask
				str[fracIdx] = byte(y) + '0'
				fracIdx++
				x -= y * digMask
				x *= 10
			}
		}
		trace_util_0.Count(_mydecimal_00000, 66)
		for ; fill > 0; fill-- {
			trace_util_0.Count(_mydecimal_00000, 69)
			str[fracIdx] = '0'
			fracIdx++
		}
	}
	trace_util_0.Count(_mydecimal_00000, 56)
	fill = digitsIntLen - digitsInt
	if digitsInt == 0 {
		trace_util_0.Count(_mydecimal_00000, 70)
		fill-- /* symbol 0 before digital point */
	}
	trace_util_0.Count(_mydecimal_00000, 57)
	for ; fill > 0; fill-- {
		trace_util_0.Count(_mydecimal_00000, 71)
		str[strIdx] = '0'
		strIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 58)
	if digitsInt > 0 {
		trace_util_0.Count(_mydecimal_00000, 72)
		strIdx += digitsInt
		wordIdx := wordStartIdx + digitsToWords(digitsInt)
		for ; digitsInt > 0; digitsInt -= digitsPerWord {
			trace_util_0.Count(_mydecimal_00000, 73)
			wordIdx--
			x := d.wordBuf[wordIdx]
			for i := myMin(digitsInt, digitsPerWord); i > 0; i-- {
				trace_util_0.Count(_mydecimal_00000, 74)
				y := x / 10
				strIdx--
				str[strIdx] = '0' + byte(x-y*10)
				x = y
			}
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 75)
		{
			str[strIdx] = '0'
		}
	}
	trace_util_0.Count(_mydecimal_00000, 59)
	return
}

// FromString parses decimal from string.
func (d *MyDecimal) FromString(str []byte) error {
	trace_util_0.Count(_mydecimal_00000, 76)
	for i := 0; i < len(str); i++ {
		trace_util_0.Count(_mydecimal_00000, 91)
		if !isSpace(str[i]) {
			trace_util_0.Count(_mydecimal_00000, 92)
			str = str[i:]
			break
		}
	}
	trace_util_0.Count(_mydecimal_00000, 77)
	if len(str) == 0 {
		trace_util_0.Count(_mydecimal_00000, 93)
		*d = zeroMyDecimal
		return ErrBadNumber
	}
	trace_util_0.Count(_mydecimal_00000, 78)
	switch str[0] {
	case '-':
		trace_util_0.Count(_mydecimal_00000, 94)
		d.negative = true
		fallthrough
	case '+':
		trace_util_0.Count(_mydecimal_00000, 95)
		str = str[1:]
	}
	trace_util_0.Count(_mydecimal_00000, 79)
	var strIdx int
	for strIdx < len(str) && isDigit(str[strIdx]) {
		trace_util_0.Count(_mydecimal_00000, 96)
		strIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 80)
	digitsInt := strIdx
	var digitsFrac int
	var endIdx int
	if strIdx < len(str) && str[strIdx] == '.' {
		trace_util_0.Count(_mydecimal_00000, 97)
		endIdx = strIdx + 1
		for endIdx < len(str) && isDigit(str[endIdx]) {
			trace_util_0.Count(_mydecimal_00000, 99)
			endIdx++
		}
		trace_util_0.Count(_mydecimal_00000, 98)
		digitsFrac = endIdx - strIdx - 1
	} else {
		trace_util_0.Count(_mydecimal_00000, 100)
		{
			digitsFrac = 0
			endIdx = strIdx
		}
	}
	trace_util_0.Count(_mydecimal_00000, 81)
	if digitsInt+digitsFrac == 0 {
		trace_util_0.Count(_mydecimal_00000, 101)
		*d = zeroMyDecimal
		return ErrBadNumber
	}
	trace_util_0.Count(_mydecimal_00000, 82)
	wordsInt := digitsToWords(digitsInt)
	wordsFrac := digitsToWords(digitsFrac)
	wordsInt, wordsFrac, err := fixWordCntError(wordsInt, wordsFrac)
	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 102)
		digitsFrac = wordsFrac * digitsPerWord
		if err == ErrOverflow {
			trace_util_0.Count(_mydecimal_00000, 103)
			digitsInt = wordsInt * digitsPerWord
		}
	}
	trace_util_0.Count(_mydecimal_00000, 83)
	d.digitsInt = int8(digitsInt)
	d.digitsFrac = int8(digitsFrac)
	wordIdx := wordsInt
	strIdxTmp := strIdx
	var word int32
	var innerIdx int
	for digitsInt > 0 {
		trace_util_0.Count(_mydecimal_00000, 104)
		digitsInt--
		strIdx--
		word += int32(str[strIdx]-'0') * powers10[innerIdx]
		innerIdx++
		if innerIdx == digitsPerWord {
			trace_util_0.Count(_mydecimal_00000, 105)
			wordIdx--
			d.wordBuf[wordIdx] = word
			word = 0
			innerIdx = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 84)
	if innerIdx != 0 {
		trace_util_0.Count(_mydecimal_00000, 106)
		wordIdx--
		d.wordBuf[wordIdx] = word
	}

	trace_util_0.Count(_mydecimal_00000, 85)
	wordIdx = wordsInt
	strIdx = strIdxTmp
	word = 0
	innerIdx = 0
	for digitsFrac > 0 {
		trace_util_0.Count(_mydecimal_00000, 107)
		digitsFrac--
		strIdx++
		word = int32(str[strIdx]-'0') + word*10
		innerIdx++
		if innerIdx == digitsPerWord {
			trace_util_0.Count(_mydecimal_00000, 108)
			d.wordBuf[wordIdx] = word
			wordIdx++
			word = 0
			innerIdx = 0
		}
	}
	trace_util_0.Count(_mydecimal_00000, 86)
	if innerIdx != 0 {
		trace_util_0.Count(_mydecimal_00000, 109)
		d.wordBuf[wordIdx] = word * powers10[digitsPerWord-innerIdx]
	}
	trace_util_0.Count(_mydecimal_00000, 87)
	if endIdx+1 <= len(str) && (str[endIdx] == 'e' || str[endIdx] == 'E') {
		trace_util_0.Count(_mydecimal_00000, 110)
		exponent, err1 := strToInt(string(str[endIdx+1:]))
		if err1 != nil {
			trace_util_0.Count(_mydecimal_00000, 114)
			err = errors.Cause(err1)
			if err != ErrTruncated {
				trace_util_0.Count(_mydecimal_00000, 115)
				*d = zeroMyDecimal
			}
		}
		trace_util_0.Count(_mydecimal_00000, 111)
		if exponent > math.MaxInt32/2 {
			trace_util_0.Count(_mydecimal_00000, 116)
			negative := d.negative
			maxDecimal(wordBufLen*digitsPerWord, 0, d)
			d.negative = negative
			err = ErrOverflow
		}
		trace_util_0.Count(_mydecimal_00000, 112)
		if exponent < math.MinInt32/2 && err != ErrOverflow {
			trace_util_0.Count(_mydecimal_00000, 117)
			*d = zeroMyDecimal
			err = ErrTruncated
		}
		trace_util_0.Count(_mydecimal_00000, 113)
		if err != ErrOverflow {
			trace_util_0.Count(_mydecimal_00000, 118)
			shiftErr := d.Shift(int(exponent))
			if shiftErr != nil {
				trace_util_0.Count(_mydecimal_00000, 119)
				if shiftErr == ErrOverflow {
					trace_util_0.Count(_mydecimal_00000, 121)
					negative := d.negative
					maxDecimal(wordBufLen*digitsPerWord, 0, d)
					d.negative = negative
				}
				trace_util_0.Count(_mydecimal_00000, 120)
				err = shiftErr
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 88)
	allZero := true
	for i := 0; i < wordBufLen; i++ {
		trace_util_0.Count(_mydecimal_00000, 122)
		if d.wordBuf[i] != 0 {
			trace_util_0.Count(_mydecimal_00000, 123)
			allZero = false
			break
		}
	}
	trace_util_0.Count(_mydecimal_00000, 89)
	if allZero {
		trace_util_0.Count(_mydecimal_00000, 124)
		d.negative = false
	}
	trace_util_0.Count(_mydecimal_00000, 90)
	d.resultFrac = d.digitsFrac
	return err
}

// Shift shifts decimal digits in given number (with rounding if it need), shift > 0 means shift to left shift,
// shift < 0 means right shift. In fact it is multiplying on 10^shift.
//
// RETURN
//   eDecOK          OK
//   eDecOverflow    operation lead to overflow, number is untoched
//   eDecTruncated   number was rounded to fit into buffer
//
func (d *MyDecimal) Shift(shift int) error {
	trace_util_0.Count(_mydecimal_00000, 125)
	var err error
	if shift == 0 {
		trace_util_0.Count(_mydecimal_00000, 135)
		return nil
	}
	trace_util_0.Count(_mydecimal_00000, 126)
	var (
		// digitBegin is index of first non zero digit (all indexes from 0).
		digitBegin int
		// digitEnd is index of position after last decimal digit.
		digitEnd int
		// point is index of digit position just after point.
		point = digitsToWords(int(d.digitsInt)) * digitsPerWord
		// new point position.
		newPoint = point + shift
		// number of digits in result.
		digitsInt, digitsFrac int
		newFront              int
	)
	digitBegin, digitEnd = d.digitBounds()
	if digitBegin == digitEnd {
		trace_util_0.Count(_mydecimal_00000, 136)
		*d = zeroMyDecimal
		return nil
	}

	trace_util_0.Count(_mydecimal_00000, 127)
	digitsInt = newPoint - digitBegin
	if digitsInt < 0 {
		trace_util_0.Count(_mydecimal_00000, 137)
		digitsInt = 0
	}
	trace_util_0.Count(_mydecimal_00000, 128)
	digitsFrac = digitEnd - newPoint
	if digitsFrac < 0 {
		trace_util_0.Count(_mydecimal_00000, 138)
		digitsFrac = 0
	}
	trace_util_0.Count(_mydecimal_00000, 129)
	wordsInt := digitsToWords(digitsInt)
	wordsFrac := digitsToWords(digitsFrac)
	newLen := wordsInt + wordsFrac
	if newLen > wordBufLen {
		trace_util_0.Count(_mydecimal_00000, 139)
		lack := newLen - wordBufLen
		if wordsFrac < lack {
			trace_util_0.Count(_mydecimal_00000, 142)
			return ErrOverflow
		}
		/* cut off fraction part to allow new number to fit in our buffer */
		trace_util_0.Count(_mydecimal_00000, 140)
		err = ErrTruncated
		wordsFrac -= lack
		diff := digitsFrac - wordsFrac*digitsPerWord
		err1 := d.Round(d, digitEnd-point-diff, ModeHalfEven)
		if err1 != nil {
			trace_util_0.Count(_mydecimal_00000, 143)
			return errors.Trace(err1)
		}
		trace_util_0.Count(_mydecimal_00000, 141)
		digitEnd -= diff
		digitsFrac = wordsFrac * digitsPerWord
		if digitEnd <= digitBegin {
			trace_util_0.Count(_mydecimal_00000, 144)
			/*
			   We lost all digits (they will be shifted out of buffer), so we can
			   just return 0.
			*/
			*d = zeroMyDecimal
			return ErrTruncated
		}
	}

	trace_util_0.Count(_mydecimal_00000, 130)
	if shift%digitsPerWord != 0 {
		trace_util_0.Count(_mydecimal_00000, 145)
		var lMiniShift, rMiniShift, miniShift int
		var doLeft bool
		/*
		   Calculate left/right shift to align decimal digits inside our bug
		   digits correctly.
		*/
		if shift > 0 {
			trace_util_0.Count(_mydecimal_00000, 149)
			lMiniShift = shift % digitsPerWord
			rMiniShift = digitsPerWord - lMiniShift
			doLeft = lMiniShift <= digitBegin
		} else {
			trace_util_0.Count(_mydecimal_00000, 150)
			{
				rMiniShift = (-shift) % digitsPerWord
				lMiniShift = digitsPerWord - rMiniShift
				doLeft = (digitsPerWord*wordBufLen - digitEnd) < rMiniShift
			}
		}
		trace_util_0.Count(_mydecimal_00000, 146)
		if doLeft {
			trace_util_0.Count(_mydecimal_00000, 151)
			d.doMiniLeftShift(lMiniShift, digitBegin, digitEnd)
			miniShift = -lMiniShift
		} else {
			trace_util_0.Count(_mydecimal_00000, 152)
			{
				d.doMiniRightShift(rMiniShift, digitBegin, digitEnd)
				miniShift = rMiniShift
			}
		}
		trace_util_0.Count(_mydecimal_00000, 147)
		newPoint += miniShift
		/*
		   If number is shifted and correctly aligned in buffer we can finish.
		*/
		if shift+miniShift == 0 && (newPoint-digitsInt) < digitsPerWord {
			trace_util_0.Count(_mydecimal_00000, 153)
			d.digitsInt = int8(digitsInt)
			d.digitsFrac = int8(digitsFrac)
			return err /* already shifted as it should be */
		}
		trace_util_0.Count(_mydecimal_00000, 148)
		digitBegin += miniShift
		digitEnd += miniShift
	}

	/* if new 'decimal front' is in first digit, we do not need move digits */
	trace_util_0.Count(_mydecimal_00000, 131)
	newFront = newPoint - digitsInt
	if newFront >= digitsPerWord || newFront < 0 {
		trace_util_0.Count(_mydecimal_00000, 154)
		/* need to move digits */
		var wordShift int
		if newFront > 0 {
			trace_util_0.Count(_mydecimal_00000, 156)
			/* move left */
			wordShift = newFront / digitsPerWord
			to := digitBegin/digitsPerWord - wordShift
			barier := (digitEnd-1)/digitsPerWord - wordShift
			for ; to <= barier; to++ {
				trace_util_0.Count(_mydecimal_00000, 159)
				d.wordBuf[to] = d.wordBuf[to+wordShift]
			}
			trace_util_0.Count(_mydecimal_00000, 157)
			for barier += wordShift; to <= barier; to++ {
				trace_util_0.Count(_mydecimal_00000, 160)
				d.wordBuf[to] = 0
			}
			trace_util_0.Count(_mydecimal_00000, 158)
			wordShift = -wordShift
		} else {
			trace_util_0.Count(_mydecimal_00000, 161)
			{
				/* move right */
				wordShift = (1 - newFront) / digitsPerWord
				to := (digitEnd-1)/digitsPerWord + wordShift
				barier := digitBegin/digitsPerWord + wordShift
				for ; to >= barier; to-- {
					trace_util_0.Count(_mydecimal_00000, 163)
					d.wordBuf[to] = d.wordBuf[to-wordShift]
				}
				trace_util_0.Count(_mydecimal_00000, 162)
				for barier -= wordShift; to >= barier; to-- {
					trace_util_0.Count(_mydecimal_00000, 164)
					d.wordBuf[to] = 0
				}
			}
		}
		trace_util_0.Count(_mydecimal_00000, 155)
		digitShift := wordShift * digitsPerWord
		digitBegin += digitShift
		digitEnd += digitShift
		newPoint += digitShift
	}
	/*
	   If there are gaps then fill them with 0.

	   Only one of following 'for' loops will work because wordIdxBegin <= wordIdxEnd.
	*/
	trace_util_0.Count(_mydecimal_00000, 132)
	wordIdxBegin := digitBegin / digitsPerWord
	wordIdxEnd := (digitEnd - 1) / digitsPerWord
	wordIdxNewPoint := 0

	/* We don't want negative new_point below */
	if newPoint != 0 {
		trace_util_0.Count(_mydecimal_00000, 165)
		wordIdxNewPoint = (newPoint - 1) / digitsPerWord
	}
	trace_util_0.Count(_mydecimal_00000, 133)
	if wordIdxNewPoint > wordIdxEnd {
		trace_util_0.Count(_mydecimal_00000, 166)
		for wordIdxNewPoint > wordIdxEnd {
			trace_util_0.Count(_mydecimal_00000, 167)
			d.wordBuf[wordIdxNewPoint] = 0
			wordIdxNewPoint--
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 168)
		{
			for ; wordIdxNewPoint < wordIdxBegin; wordIdxNewPoint++ {
				trace_util_0.Count(_mydecimal_00000, 169)
				d.wordBuf[wordIdxNewPoint] = 0
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 134)
	d.digitsInt = int8(digitsInt)
	d.digitsFrac = int8(digitsFrac)
	return err
}

/*
  digitBounds returns bounds of decimal digits in the number.

      start - index (from 0 ) of first decimal digits.
      end   - index of position just after last decimal digit.
*/
func (d *MyDecimal) digitBounds() (start, end int) {
	trace_util_0.Count(_mydecimal_00000, 170)
	var i int
	bufBeg := 0
	bufLen := digitsToWords(int(d.digitsInt)) + digitsToWords(int(d.digitsFrac))
	bufEnd := bufLen - 1

	/* find non-zero digit from number beginning */
	for bufBeg < bufLen && d.wordBuf[bufBeg] == 0 {
		trace_util_0.Count(_mydecimal_00000, 177)
		bufBeg++
	}
	trace_util_0.Count(_mydecimal_00000, 171)
	if bufBeg >= bufLen {
		trace_util_0.Count(_mydecimal_00000, 178)
		return 0, 0
	}

	/* find non-zero decimal digit from number beginning */
	trace_util_0.Count(_mydecimal_00000, 172)
	if bufBeg == 0 && d.digitsInt > 0 {
		trace_util_0.Count(_mydecimal_00000, 179)
		i = (int(d.digitsInt) - 1) % digitsPerWord
		start = digitsPerWord - i - 1
	} else {
		trace_util_0.Count(_mydecimal_00000, 180)
		{
			i = digitsPerWord - 1
			start = bufBeg * digitsPerWord
		}
	}
	trace_util_0.Count(_mydecimal_00000, 173)
	if bufBeg < bufLen {
		trace_util_0.Count(_mydecimal_00000, 181)
		start += countLeadingZeroes(i, d.wordBuf[bufBeg])
	}

	/* find non-zero digit at the end */
	trace_util_0.Count(_mydecimal_00000, 174)
	for bufEnd > bufBeg && d.wordBuf[bufEnd] == 0 {
		trace_util_0.Count(_mydecimal_00000, 182)
		bufEnd--
	}
	/* find non-zero decimal digit from the end */
	trace_util_0.Count(_mydecimal_00000, 175)
	if bufEnd == bufLen-1 && d.digitsFrac > 0 {
		trace_util_0.Count(_mydecimal_00000, 183)
		i = (int(d.digitsFrac)-1)%digitsPerWord + 1
		end = bufEnd*digitsPerWord + i
		i = digitsPerWord - i + 1
	} else {
		trace_util_0.Count(_mydecimal_00000, 184)
		{
			end = (bufEnd + 1) * digitsPerWord
			i = 1
		}
	}
	trace_util_0.Count(_mydecimal_00000, 176)
	end -= countTrailingZeroes(i, d.wordBuf[bufEnd])
	return start, end
}

/*
  doMiniLeftShift does left shift for alignment of data in buffer.

    shift   number of decimal digits on which it should be shifted
    beg/end bounds of decimal digits (see digitsBounds())

  NOTE
    Result fitting in the buffer should be garanted.
    'shift' have to be from 1 to digitsPerWord-1 (inclusive)
*/
func (d *MyDecimal) doMiniLeftShift(shift, beg, end int) {
	trace_util_0.Count(_mydecimal_00000, 185)
	bufFrom := beg / digitsPerWord
	bufEnd := (end - 1) / digitsPerWord
	cShift := digitsPerWord - shift
	if beg%digitsPerWord < shift {
		trace_util_0.Count(_mydecimal_00000, 188)
		d.wordBuf[bufFrom-1] = d.wordBuf[bufFrom] / powers10[cShift]
	}
	trace_util_0.Count(_mydecimal_00000, 186)
	for bufFrom < bufEnd {
		trace_util_0.Count(_mydecimal_00000, 189)
		d.wordBuf[bufFrom] = (d.wordBuf[bufFrom]%powers10[cShift])*powers10[shift] + d.wordBuf[bufFrom+1]/powers10[cShift]
		bufFrom++
	}
	trace_util_0.Count(_mydecimal_00000, 187)
	d.wordBuf[bufFrom] = (d.wordBuf[bufFrom] % powers10[cShift]) * powers10[shift]
}

/*
  doMiniRightShift does right shift for alignment of data in buffer.

    shift   number of decimal digits on which it should be shifted
    beg/end bounds of decimal digits (see digitsBounds())

  NOTE
    Result fitting in the buffer should be garanted.
    'shift' have to be from 1 to digitsPerWord-1 (inclusive)
*/
func (d *MyDecimal) doMiniRightShift(shift, beg, end int) {
	trace_util_0.Count(_mydecimal_00000, 190)
	bufFrom := (end - 1) / digitsPerWord
	bufEnd := beg / digitsPerWord
	cShift := digitsPerWord - shift
	if digitsPerWord-((end-1)%digitsPerWord+1) < shift {
		trace_util_0.Count(_mydecimal_00000, 193)
		d.wordBuf[bufFrom+1] = (d.wordBuf[bufFrom] % powers10[shift]) * powers10[cShift]
	}
	trace_util_0.Count(_mydecimal_00000, 191)
	for bufFrom > bufEnd {
		trace_util_0.Count(_mydecimal_00000, 194)
		d.wordBuf[bufFrom] = d.wordBuf[bufFrom]/powers10[shift] + (d.wordBuf[bufFrom-1]%powers10[shift])*powers10[cShift]
		bufFrom--
	}
	trace_util_0.Count(_mydecimal_00000, 192)
	d.wordBuf[bufFrom] = d.wordBuf[bufFrom] / powers10[shift]
}

// Round rounds the decimal to "frac" digits.
//
//    to			- result buffer. d == to is allowed
//    frac			- to what position after fraction point to round. can be negative!
//    roundMode		- round to nearest even or truncate
// 			ModeHalfEven rounds normally.
// 			Truncate just truncates the decimal.
//
// NOTES
//  scale can be negative !
//  one TRUNCATED error (line XXX below) isn't treated very logical :(
//
// RETURN VALUE
//  eDecOK/eDecTruncated
func (d *MyDecimal) Round(to *MyDecimal, frac int, roundMode RoundMode) (err error) {
	trace_util_0.Count(_mydecimal_00000, 195)
	// wordsFracTo is the number of fraction words in buffer.
	wordsFracTo := (frac + 1) / digitsPerWord
	if frac > 0 {
		trace_util_0.Count(_mydecimal_00000, 207)
		wordsFracTo = digitsToWords(frac)
	}
	trace_util_0.Count(_mydecimal_00000, 196)
	wordsFrac := digitsToWords(int(d.digitsFrac))
	wordsInt := digitsToWords(int(d.digitsInt))

	roundDigit := int32(roundMode)
	/* TODO - fix this code as it won't work for CEILING mode */

	if wordsInt+wordsFracTo > wordBufLen {
		trace_util_0.Count(_mydecimal_00000, 208)
		wordsFracTo = wordBufLen - wordsInt
		frac = wordsFracTo * digitsPerWord
		err = ErrTruncated
	}
	trace_util_0.Count(_mydecimal_00000, 197)
	if int(d.digitsInt)+frac < 0 {
		trace_util_0.Count(_mydecimal_00000, 209)
		*to = zeroMyDecimal
		return nil
	}
	trace_util_0.Count(_mydecimal_00000, 198)
	if to != d {
		trace_util_0.Count(_mydecimal_00000, 210)
		copy(to.wordBuf[:], d.wordBuf[:])
		to.negative = d.negative
		to.digitsInt = int8(myMin(wordsInt, wordBufLen) * digitsPerWord)
	}
	trace_util_0.Count(_mydecimal_00000, 199)
	if wordsFracTo > wordsFrac {
		trace_util_0.Count(_mydecimal_00000, 211)
		idx := wordsInt + wordsFrac
		for wordsFracTo > wordsFrac {
			trace_util_0.Count(_mydecimal_00000, 213)
			wordsFracTo--
			to.wordBuf[idx] = 0
			idx++
		}
		trace_util_0.Count(_mydecimal_00000, 212)
		to.digitsFrac = int8(frac)
		to.resultFrac = to.digitsFrac
		return
	}
	trace_util_0.Count(_mydecimal_00000, 200)
	if frac >= int(d.digitsFrac) {
		trace_util_0.Count(_mydecimal_00000, 214)
		to.digitsFrac = int8(frac)
		to.resultFrac = to.digitsFrac
		return
	}

	// Do increment.
	trace_util_0.Count(_mydecimal_00000, 201)
	toIdx := wordsInt + wordsFracTo - 1
	if frac == wordsFracTo*digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 215)
		doInc := false
		switch roundMode {
		// Notice: No support for ceiling mode now.
		case modeCeiling:
			trace_util_0.Count(_mydecimal_00000, 217)
			// If any word after scale is not zero, do increment.
			// e.g ceiling 3.0001 to scale 1, gets 3.1
			idx := toIdx + (wordsFrac - wordsFracTo)
			for idx > toIdx {
				trace_util_0.Count(_mydecimal_00000, 220)
				if d.wordBuf[idx] != 0 {
					trace_util_0.Count(_mydecimal_00000, 222)
					doInc = true
					break
				}
				trace_util_0.Count(_mydecimal_00000, 221)
				idx--
			}
		case ModeHalfEven:
			trace_util_0.Count(_mydecimal_00000, 218)
			digAfterScale := d.wordBuf[toIdx+1] / digMask // the first digit after scale.
			// If first digit after scale is 5 and round even, do increment if digit at scale is odd.
			doInc = (digAfterScale > 5) || (digAfterScale == 5)
		case ModeTruncate:
			trace_util_0.Count(_mydecimal_00000, 219)
			// Never round, just truncate.
			doInc = false
		}
		trace_util_0.Count(_mydecimal_00000, 216)
		if doInc {
			trace_util_0.Count(_mydecimal_00000, 223)
			if toIdx >= 0 {
				trace_util_0.Count(_mydecimal_00000, 224)
				to.wordBuf[toIdx]++
			} else {
				trace_util_0.Count(_mydecimal_00000, 225)
				{
					toIdx++
					to.wordBuf[toIdx] = wordBase
				}
			}
		} else {
			trace_util_0.Count(_mydecimal_00000, 226)
			if wordsInt+wordsFracTo == 0 {
				trace_util_0.Count(_mydecimal_00000, 227)
				*to = zeroMyDecimal
				return nil
			}
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 228)
		{
			/* TODO - fix this code as it won't work for CEILING mode */
			pos := wordsFracTo*digitsPerWord - frac - 1
			shiftedNumber := to.wordBuf[toIdx] / powers10[pos]
			digAfterScale := shiftedNumber % 10
			if digAfterScale > roundDigit || (roundDigit == 5 && digAfterScale == 5) {
				trace_util_0.Count(_mydecimal_00000, 230)
				shiftedNumber += 10
			}
			trace_util_0.Count(_mydecimal_00000, 229)
			to.wordBuf[toIdx] = powers10[pos] * (shiftedNumber - digAfterScale)
		}
	}
	/*
	   In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal words inside
	   the buffer are as follows.

	   Before <1, 5e8>
	   After  <2, 5e8>

	   Hence we need to set the 2nd field to 0.
	   The same holds if we round 1.5e-9 to 2e-9.
	*/
	trace_util_0.Count(_mydecimal_00000, 202)
	if wordsFracTo < wordsFrac {
		trace_util_0.Count(_mydecimal_00000, 231)
		idx := wordsInt + wordsFracTo
		if frac == 0 && wordsInt == 0 {
			trace_util_0.Count(_mydecimal_00000, 233)
			idx = 1
		}
		trace_util_0.Count(_mydecimal_00000, 232)
		for idx < wordBufLen {
			trace_util_0.Count(_mydecimal_00000, 234)
			to.wordBuf[idx] = 0
			idx++
		}
	}

	// Handle carry.
	trace_util_0.Count(_mydecimal_00000, 203)
	var carry int32
	if to.wordBuf[toIdx] >= wordBase {
		trace_util_0.Count(_mydecimal_00000, 235)
		carry = 1
		to.wordBuf[toIdx] -= wordBase
		for carry == 1 && toIdx > 0 {
			trace_util_0.Count(_mydecimal_00000, 237)
			toIdx--
			to.wordBuf[toIdx], carry = add(to.wordBuf[toIdx], 0, carry)
		}
		trace_util_0.Count(_mydecimal_00000, 236)
		if carry > 0 {
			trace_util_0.Count(_mydecimal_00000, 238)
			if wordsInt+wordsFracTo >= wordBufLen {
				trace_util_0.Count(_mydecimal_00000, 241)
				wordsFracTo--
				frac = wordsFracTo * digitsPerWord
				err = ErrTruncated
			}
			trace_util_0.Count(_mydecimal_00000, 239)
			for toIdx = wordsInt + myMax(wordsFracTo, 0); toIdx > 0; toIdx-- {
				trace_util_0.Count(_mydecimal_00000, 242)
				if toIdx < wordBufLen {
					trace_util_0.Count(_mydecimal_00000, 243)
					to.wordBuf[toIdx] = to.wordBuf[toIdx-1]
				} else {
					trace_util_0.Count(_mydecimal_00000, 244)
					{
						err = ErrOverflow
					}
				}
			}
			trace_util_0.Count(_mydecimal_00000, 240)
			to.wordBuf[toIdx] = 1
			/* We cannot have more than 9 * 9 = 81 digits. */
			if int(to.digitsInt) < digitsPerWord*wordBufLen {
				trace_util_0.Count(_mydecimal_00000, 245)
				to.digitsInt++
			} else {
				trace_util_0.Count(_mydecimal_00000, 246)
				{
					err = ErrOverflow
				}
			}
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 247)
		{
			for {
				trace_util_0.Count(_mydecimal_00000, 248)
				if to.wordBuf[toIdx] != 0 {
					trace_util_0.Count(_mydecimal_00000, 251)
					break
				}
				trace_util_0.Count(_mydecimal_00000, 249)
				if toIdx == 0 {
					trace_util_0.Count(_mydecimal_00000, 252)
					/* making 'zero' with the proper scale */
					idx := wordsFracTo + 1
					to.digitsInt = 1
					to.digitsFrac = int8(myMax(frac, 0))
					to.negative = false
					for toIdx < idx {
						trace_util_0.Count(_mydecimal_00000, 254)
						to.wordBuf[toIdx] = 0
						toIdx++
					}
					trace_util_0.Count(_mydecimal_00000, 253)
					to.resultFrac = to.digitsFrac
					return nil
				}
				trace_util_0.Count(_mydecimal_00000, 250)
				toIdx--
			}
		}
	}
	/* Here we check 999.9 -> 1000 case when we need to increase intDigCnt */
	trace_util_0.Count(_mydecimal_00000, 204)
	firstDig := mod9[to.digitsInt]
	if firstDig > 0 && to.wordBuf[toIdx] >= powers10[firstDig] {
		trace_util_0.Count(_mydecimal_00000, 255)
		to.digitsInt++
	}
	trace_util_0.Count(_mydecimal_00000, 205)
	if frac < 0 {
		trace_util_0.Count(_mydecimal_00000, 256)
		frac = 0
	}
	trace_util_0.Count(_mydecimal_00000, 206)
	to.digitsFrac = int8(frac)
	to.resultFrac = to.digitsFrac
	return
}

// FromInt sets the decimal value from int64.
func (d *MyDecimal) FromInt(val int64) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 257)
	var uVal uint64
	if val < 0 {
		trace_util_0.Count(_mydecimal_00000, 259)
		d.negative = true
		uVal = uint64(-val)
	} else {
		trace_util_0.Count(_mydecimal_00000, 260)
		{
			uVal = uint64(val)
		}
	}
	trace_util_0.Count(_mydecimal_00000, 258)
	return d.FromUint(uVal)
}

// FromUint sets the decimal value from uint64.
func (d *MyDecimal) FromUint(val uint64) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 261)
	x := val
	wordIdx := 1
	for x >= wordBase {
		trace_util_0.Count(_mydecimal_00000, 264)
		wordIdx++
		x /= wordBase
	}
	trace_util_0.Count(_mydecimal_00000, 262)
	d.digitsFrac = 0
	d.digitsInt = int8(wordIdx * digitsPerWord)
	x = val
	for wordIdx > 0 {
		trace_util_0.Count(_mydecimal_00000, 265)
		wordIdx--
		y := x / wordBase
		d.wordBuf[wordIdx] = int32(x - y*wordBase)
		x = y
	}
	trace_util_0.Count(_mydecimal_00000, 263)
	return d
}

// ToInt returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToInt() (int64, error) {
	trace_util_0.Count(_mydecimal_00000, 266)
	var x int64
	wordIdx := 0
	for i := d.digitsInt; i > 0; i -= digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 271)
		y := x
		/*
		   Attention: trick!
		   we're calculating -|from| instead of |from| here
		   because |LONGLONG_MIN| > LONGLONG_MAX
		   so we can convert -9223372036854775808 correctly
		*/
		x = x*wordBase - int64(d.wordBuf[wordIdx])
		wordIdx++
		if y < math.MinInt64/wordBase || x > y {
			trace_util_0.Count(_mydecimal_00000, 272)
			/*
			   the decimal is bigger than any possible integer
			   return border integer depending on the sign
			*/
			if d.negative {
				trace_util_0.Count(_mydecimal_00000, 274)
				return math.MinInt64, ErrOverflow
			}
			trace_util_0.Count(_mydecimal_00000, 273)
			return math.MaxInt64, ErrOverflow
		}
	}
	/* boundary case: 9223372036854775808 */
	trace_util_0.Count(_mydecimal_00000, 267)
	if !d.negative && x == math.MinInt64 {
		trace_util_0.Count(_mydecimal_00000, 275)
		return math.MaxInt64, ErrOverflow
	}
	trace_util_0.Count(_mydecimal_00000, 268)
	if !d.negative {
		trace_util_0.Count(_mydecimal_00000, 276)
		x = -x
	}
	trace_util_0.Count(_mydecimal_00000, 269)
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 277)
		if d.wordBuf[wordIdx] != 0 {
			trace_util_0.Count(_mydecimal_00000, 279)
			return x, ErrTruncated
		}
		trace_util_0.Count(_mydecimal_00000, 278)
		wordIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 270)
	return x, nil
}

// ToUint returns int part of the decimal, returns the result and errcode.
func (d *MyDecimal) ToUint() (uint64, error) {
	trace_util_0.Count(_mydecimal_00000, 280)
	if d.negative {
		trace_util_0.Count(_mydecimal_00000, 284)
		return 0, ErrOverflow
	}
	trace_util_0.Count(_mydecimal_00000, 281)
	var x uint64
	wordIdx := 0
	for i := d.digitsInt; i > 0; i -= digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 285)
		y := x
		x = x*wordBase + uint64(d.wordBuf[wordIdx])
		wordIdx++
		if y > math.MaxUint64/wordBase || x < y {
			trace_util_0.Count(_mydecimal_00000, 286)
			return math.MaxUint64, ErrOverflow
		}
	}
	trace_util_0.Count(_mydecimal_00000, 282)
	for i := d.digitsFrac; i > 0; i -= digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 287)
		if d.wordBuf[wordIdx] != 0 {
			trace_util_0.Count(_mydecimal_00000, 289)
			return x, ErrTruncated
		}
		trace_util_0.Count(_mydecimal_00000, 288)
		wordIdx++
	}
	trace_util_0.Count(_mydecimal_00000, 283)
	return x, nil
}

// FromFloat64 creates a decimal from float64 value.
func (d *MyDecimal) FromFloat64(f float64) error {
	trace_util_0.Count(_mydecimal_00000, 290)
	s := strconv.FormatFloat(f, 'g', -1, 64)
	return d.FromString([]byte(s))
}

// ToFloat64 converts decimal to float64 value.
func (d *MyDecimal) ToFloat64() (float64, error) {
	trace_util_0.Count(_mydecimal_00000, 291)
	f, err := strconv.ParseFloat(d.String(), 64)
	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 293)
		err = ErrOverflow
	}
	trace_util_0.Count(_mydecimal_00000, 292)
	return f, err
}

/*
ToBin converts decimal to its binary fixed-length representation
two representations of the same length can be compared with memcmp
with the correct -1/0/+1 result

  PARAMS
		precision/frac - if precision is 0, internal value of the decimal will be used,
		then the encoded value is not memory comparable.

  NOTE
    the buffer is assumed to be of the size decimalBinSize(precision, frac)

  RETURN VALUE
  	bin     - binary value
    errCode - eDecOK/eDecTruncate/eDecOverflow

  DESCRIPTION
    for storage decimal numbers are converted to the "binary" format.

    This format has the following properties:
      1. length of the binary representation depends on the {precision, frac}
      as provided by the caller and NOT on the digitsInt/digitsFrac of the decimal to
      convert.
      2. binary representations of the same {precision, frac} can be compared
      with memcmp - with the same result as DecimalCompare() of the original
      decimals (not taking into account possible precision loss during
      conversion).

    This binary format is as follows:
      1. First the number is converted to have a requested precision and frac.
      2. Every full digitsPerWord digits of digitsInt part are stored in 4 bytes
         as is
      3. The first digitsInt % digitesPerWord digits are stored in the reduced
         number of bytes (enough bytes to store this number of digits -
         see dig2bytes)
      4. same for frac - full word are stored as is,
         the last frac % digitsPerWord digits - in the reduced number of bytes.
      5. If the number is negative - every byte is inversed.
      5. The very first bit of the resulting byte array is inverted (because
         memcmp compares unsigned bytes, see property 2 above)

    Example:

      1234567890.1234

    internally is represented as 3 words

      1 234567890 123400000

    (assuming we want a binary representation with precision=14, frac=4)
    in hex it's

      00-00-00-01  0D-FB-38-D2  07-5A-EF-40

    now, middle word is full - it stores 9 decimal digits. It goes
    into binary representation as is:


      ...........  0D-FB-38-D2 ............

    First word has only one decimal digit. We can store one digit in
    one byte, no need to waste four:

                01 0D-FB-38-D2 ............

    now, last word. It's 123400000. We can store 1234 in two bytes:

                01 0D-FB-38-D2 04-D2

    So, we've packed 12 bytes number in 7 bytes.
    And now we invert the highest bit to get the final result:

                81 0D FB 38 D2 04 D2

    And for -1234567890.1234 it would be

                7E F2 04 C7 2D FB 2D
*/
func (d *MyDecimal) ToBin(precision, frac int) ([]byte, error) {
	trace_util_0.Count(_mydecimal_00000, 294)
	if precision > digitsPerWord*maxWordBufLen || precision < 0 || frac > mysql.MaxDecimalScale || frac < 0 {
		trace_util_0.Count(_mydecimal_00000, 304)
		return nil, ErrBadNumber
	}
	trace_util_0.Count(_mydecimal_00000, 295)
	var err error
	var mask int32
	if d.negative {
		trace_util_0.Count(_mydecimal_00000, 305)
		mask = -1
	}
	trace_util_0.Count(_mydecimal_00000, 296)
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	leadingDigits := digitsInt - wordsInt*digitsPerWord
	wordsFrac := frac / digitsPerWord
	trailingDigits := frac - wordsFrac*digitsPerWord

	wordsFracFrom := int(d.digitsFrac) / digitsPerWord
	trailingDigitsFrom := int(d.digitsFrac) - wordsFracFrom*digitsPerWord
	intSize := wordsInt*wordSize + dig2bytes[leadingDigits]
	fracSize := wordsFrac*wordSize + dig2bytes[trailingDigits]
	fracSizeFrom := wordsFracFrom*wordSize + dig2bytes[trailingDigitsFrom]
	originIntSize := intSize
	originFracSize := fracSize
	bin := make([]byte, intSize+fracSize)
	binIdx := 0
	wordIdxFrom, digitsIntFrom := d.removeLeadingZeros()
	if digitsIntFrom+fracSizeFrom == 0 {
		trace_util_0.Count(_mydecimal_00000, 306)
		mask = 0
		digitsInt = 1
	}

	trace_util_0.Count(_mydecimal_00000, 297)
	wordsIntFrom := digitsIntFrom / digitsPerWord
	leadingDigitsFrom := digitsIntFrom - wordsIntFrom*digitsPerWord
	iSizeFrom := wordsIntFrom*wordSize + dig2bytes[leadingDigitsFrom]

	if digitsInt < digitsIntFrom {
		trace_util_0.Count(_mydecimal_00000, 307)
		wordIdxFrom += wordsIntFrom - wordsInt
		if leadingDigitsFrom > 0 {
			trace_util_0.Count(_mydecimal_00000, 310)
			wordIdxFrom++
		}
		trace_util_0.Count(_mydecimal_00000, 308)
		if leadingDigits > 0 {
			trace_util_0.Count(_mydecimal_00000, 311)
			wordIdxFrom--
		}
		trace_util_0.Count(_mydecimal_00000, 309)
		wordsIntFrom = wordsInt
		leadingDigitsFrom = leadingDigits
		err = ErrOverflow
	} else {
		trace_util_0.Count(_mydecimal_00000, 312)
		if intSize > iSizeFrom {
			trace_util_0.Count(_mydecimal_00000, 313)
			for intSize > iSizeFrom {
				trace_util_0.Count(_mydecimal_00000, 314)
				intSize--
				bin[binIdx] = byte(mask)
				binIdx++
			}
		}
	}

	trace_util_0.Count(_mydecimal_00000, 298)
	if fracSize < fracSizeFrom {
		trace_util_0.Count(_mydecimal_00000, 315)
		wordsFracFrom = wordsFrac
		trailingDigitsFrom = trailingDigits
		err = ErrTruncated
	} else {
		trace_util_0.Count(_mydecimal_00000, 316)
		if fracSize > fracSizeFrom && trailingDigitsFrom > 0 {
			trace_util_0.Count(_mydecimal_00000, 317)
			if wordsFrac == wordsFracFrom {
				trace_util_0.Count(_mydecimal_00000, 318)
				trailingDigitsFrom = trailingDigits
				fracSize = fracSizeFrom
			} else {
				trace_util_0.Count(_mydecimal_00000, 319)
				{
					wordsFracFrom++
					trailingDigitsFrom = 0
				}
			}
		}
	}
	// xIntFrom part
	trace_util_0.Count(_mydecimal_00000, 299)
	if leadingDigitsFrom > 0 {
		trace_util_0.Count(_mydecimal_00000, 320)
		i := dig2bytes[leadingDigitsFrom]
		x := (d.wordBuf[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask
		wordIdxFrom++
		writeWord(bin[binIdx:], x, i)
		binIdx += i
	}

	// wordsInt + wordsFrac part.
	trace_util_0.Count(_mydecimal_00000, 300)
	for stop := wordIdxFrom + wordsIntFrom + wordsFracFrom; wordIdxFrom < stop; binIdx += wordSize {
		trace_util_0.Count(_mydecimal_00000, 321)
		x := d.wordBuf[wordIdxFrom] ^ mask
		wordIdxFrom++
		writeWord(bin[binIdx:], x, 4)
	}

	// xFracFrom part
	trace_util_0.Count(_mydecimal_00000, 301)
	if trailingDigitsFrom > 0 {
		trace_util_0.Count(_mydecimal_00000, 322)
		var x int32
		i := dig2bytes[trailingDigitsFrom]
		lim := trailingDigits
		if wordsFracFrom < wordsFrac {
			trace_util_0.Count(_mydecimal_00000, 325)
			lim = digitsPerWord
		}

		trace_util_0.Count(_mydecimal_00000, 323)
		for trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i {
			trace_util_0.Count(_mydecimal_00000, 326)
			trailingDigitsFrom++
		}
		trace_util_0.Count(_mydecimal_00000, 324)
		x = (d.wordBuf[wordIdxFrom] / powers10[digitsPerWord-trailingDigitsFrom]) ^ mask
		writeWord(bin[binIdx:], x, i)
		binIdx += i
	}
	trace_util_0.Count(_mydecimal_00000, 302)
	if fracSize > fracSizeFrom {
		trace_util_0.Count(_mydecimal_00000, 327)
		binIdxEnd := originIntSize + originFracSize
		for fracSize > fracSizeFrom && binIdx < binIdxEnd {
			trace_util_0.Count(_mydecimal_00000, 328)
			fracSize--
			bin[binIdx] = byte(mask)
			binIdx++
		}
	}
	trace_util_0.Count(_mydecimal_00000, 303)
	bin[0] ^= 0x80
	return bin, err
}

// ToHashKey removes the leading and trailing zeros and generates a hash key.
// Two Decimals dec0 and dec1 with different fraction will generate the same hash keys if dec0.Compare(dec1) == 0.
func (d *MyDecimal) ToHashKey() ([]byte, error) {
	trace_util_0.Count(_mydecimal_00000, 329)
	_, digitsInt := d.removeLeadingZeros()
	_, digitsFrac := d.removeTrailingZeros()
	prec := digitsInt + digitsFrac
	if prec == 0 {
		trace_util_0.Count(_mydecimal_00000, 332) // zeroDecimal
		prec = 1
	}
	trace_util_0.Count(_mydecimal_00000, 330)
	buf, err := d.ToBin(prec, digitsFrac)
	if err == ErrTruncated {
		trace_util_0.Count(_mydecimal_00000, 333)
		// This err is caused by shorter digitsFrac;
		// After removing the trailing zeros from a Decimal,
		// so digitsFrac may be less than the real digitsFrac of the Decimal,
		// thus ErrTruncated may be raised, we can ignore it here.
		err = nil
	}
	trace_util_0.Count(_mydecimal_00000, 331)
	return buf, err
}

// PrecisionAndFrac returns the internal precision and frac number.
func (d *MyDecimal) PrecisionAndFrac() (precision, frac int) {
	trace_util_0.Count(_mydecimal_00000, 334)
	frac = int(d.digitsFrac)
	_, digitsInt := d.removeLeadingZeros()
	precision = digitsInt + frac
	if precision == 0 {
		trace_util_0.Count(_mydecimal_00000, 336)
		precision = 1
	}
	trace_util_0.Count(_mydecimal_00000, 335)
	return
}

// IsZero checks whether it's a zero decimal.
func (d *MyDecimal) IsZero() bool {
	trace_util_0.Count(_mydecimal_00000, 337)
	isZero := true
	for _, val := range d.wordBuf {
		trace_util_0.Count(_mydecimal_00000, 339)
		if val != 0 {
			trace_util_0.Count(_mydecimal_00000, 340)
			isZero = false
			break
		}
	}
	trace_util_0.Count(_mydecimal_00000, 338)
	return isZero
}

// FromBin Restores decimal from its binary fixed-length representation.
func (d *MyDecimal) FromBin(bin []byte, precision, frac int) (binSize int, err error) {
	trace_util_0.Count(_mydecimal_00000, 341)
	if len(bin) == 0 {
		trace_util_0.Count(_mydecimal_00000, 352)
		*d = zeroMyDecimal
		return 0, ErrBadNumber
	}
	trace_util_0.Count(_mydecimal_00000, 342)
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	leadingDigits := digitsInt - wordsInt*digitsPerWord
	wordsFrac := frac / digitsPerWord
	trailingDigits := frac - wordsFrac*digitsPerWord
	wordsIntTo := wordsInt
	if leadingDigits > 0 {
		trace_util_0.Count(_mydecimal_00000, 353)
		wordsIntTo++
	}
	trace_util_0.Count(_mydecimal_00000, 343)
	wordsFracTo := wordsFrac
	if trailingDigits > 0 {
		trace_util_0.Count(_mydecimal_00000, 354)
		wordsFracTo++
	}

	trace_util_0.Count(_mydecimal_00000, 344)
	binIdx := 0
	mask := int32(-1)
	if bin[binIdx]&0x80 > 0 {
		trace_util_0.Count(_mydecimal_00000, 355)
		mask = 0
	}
	trace_util_0.Count(_mydecimal_00000, 345)
	binSize = decimalBinSize(precision, frac)
	dCopy := make([]byte, 40)
	dCopy = dCopy[:binSize]
	copy(dCopy, bin)
	dCopy[0] ^= 0x80
	bin = dCopy
	oldWordsIntTo := wordsIntTo
	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 356)
		if wordsIntTo < oldWordsIntTo {
			trace_util_0.Count(_mydecimal_00000, 357)
			binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize
		} else {
			trace_util_0.Count(_mydecimal_00000, 358)
			{
				trailingDigits = 0
				wordsFrac = wordsFracTo
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 346)
	d.negative = mask != 0
	d.digitsInt = int8(wordsInt*digitsPerWord + leadingDigits)
	d.digitsFrac = int8(wordsFrac*digitsPerWord + trailingDigits)

	wordIdx := 0
	if leadingDigits > 0 {
		trace_util_0.Count(_mydecimal_00000, 359)
		i := dig2bytes[leadingDigits]
		x := readWord(bin[binIdx:], i)
		binIdx += i
		d.wordBuf[wordIdx] = x ^ mask
		if uint64(d.wordBuf[wordIdx]) >= uint64(powers10[leadingDigits+1]) {
			trace_util_0.Count(_mydecimal_00000, 361)
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
		trace_util_0.Count(_mydecimal_00000, 360)
		if wordIdx > 0 || d.wordBuf[wordIdx] != 0 {
			trace_util_0.Count(_mydecimal_00000, 362)
			wordIdx++
		} else {
			trace_util_0.Count(_mydecimal_00000, 363)
			{
				d.digitsInt -= int8(leadingDigits)
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 347)
	for stop := binIdx + wordsInt*wordSize; binIdx < stop; binIdx += wordSize {
		trace_util_0.Count(_mydecimal_00000, 364)
		d.wordBuf[wordIdx] = readWord(bin[binIdx:], 4) ^ mask
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			trace_util_0.Count(_mydecimal_00000, 366)
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
		trace_util_0.Count(_mydecimal_00000, 365)
		if wordIdx > 0 || d.wordBuf[wordIdx] != 0 {
			trace_util_0.Count(_mydecimal_00000, 367)
			wordIdx++
		} else {
			trace_util_0.Count(_mydecimal_00000, 368)
			{
				d.digitsInt -= digitsPerWord
			}
		}
	}

	trace_util_0.Count(_mydecimal_00000, 348)
	for stop := binIdx + wordsFrac*wordSize; binIdx < stop; binIdx += wordSize {
		trace_util_0.Count(_mydecimal_00000, 369)
		d.wordBuf[wordIdx] = readWord(bin[binIdx:], 4) ^ mask
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			trace_util_0.Count(_mydecimal_00000, 371)
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
		trace_util_0.Count(_mydecimal_00000, 370)
		wordIdx++
	}

	trace_util_0.Count(_mydecimal_00000, 349)
	if trailingDigits > 0 {
		trace_util_0.Count(_mydecimal_00000, 372)
		i := dig2bytes[trailingDigits]
		x := readWord(bin[binIdx:], i)
		d.wordBuf[wordIdx] = (x ^ mask) * powers10[digitsPerWord-trailingDigits]
		if uint32(d.wordBuf[wordIdx]) > wordMax {
			trace_util_0.Count(_mydecimal_00000, 373)
			*d = zeroMyDecimal
			return binSize, ErrBadNumber
		}
	}

	trace_util_0.Count(_mydecimal_00000, 350)
	if d.digitsInt == 0 && d.digitsFrac == 0 {
		trace_util_0.Count(_mydecimal_00000, 374)
		*d = zeroMyDecimal
	}
	trace_util_0.Count(_mydecimal_00000, 351)
	d.resultFrac = int8(frac)
	return binSize, err
}

// decimalBinSize returns the size of array to hold a binary representation of a decimal.
func decimalBinSize(precision, frac int) int {
	trace_util_0.Count(_mydecimal_00000, 375)
	digitsInt := precision - frac
	wordsInt := digitsInt / digitsPerWord
	wordsFrac := frac / digitsPerWord
	xInt := digitsInt - wordsInt*digitsPerWord
	xFrac := frac - wordsFrac*digitsPerWord
	return wordsInt*wordSize + dig2bytes[xInt] + wordsFrac*wordSize + dig2bytes[xFrac]
}

func readWord(b []byte, size int) int32 {
	trace_util_0.Count(_mydecimal_00000, 376)
	var x int32
	switch size {
	case 1:
		trace_util_0.Count(_mydecimal_00000, 378)
		x = int32(int8(b[0]))
	case 2:
		trace_util_0.Count(_mydecimal_00000, 379)
		x = int32(int8(b[0]))<<8 + int32(b[1])
	case 3:
		trace_util_0.Count(_mydecimal_00000, 380)
		if b[0]&128 > 0 {
			trace_util_0.Count(_mydecimal_00000, 382)
			x = int32(uint32(255)<<24 | uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
		} else {
			trace_util_0.Count(_mydecimal_00000, 383)
			{
				x = int32(uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
			}
		}
	case 4:
		trace_util_0.Count(_mydecimal_00000, 381)
		x = int32(b[3]) + int32(b[2])<<8 + int32(b[1])<<16 + int32(int8(b[0]))<<24
	}
	trace_util_0.Count(_mydecimal_00000, 377)
	return x
}

func writeWord(b []byte, word int32, size int) {
	trace_util_0.Count(_mydecimal_00000, 384)
	v := uint32(word)
	switch size {
	case 1:
		trace_util_0.Count(_mydecimal_00000, 385)
		b[0] = byte(word)
	case 2:
		trace_util_0.Count(_mydecimal_00000, 386)
		b[0] = byte(v >> 8)
		b[1] = byte(v)
	case 3:
		trace_util_0.Count(_mydecimal_00000, 387)
		b[0] = byte(v >> 16)
		b[1] = byte(v >> 8)
		b[2] = byte(v)
	case 4:
		trace_util_0.Count(_mydecimal_00000, 388)
		b[0] = byte(v >> 24)
		b[1] = byte(v >> 16)
		b[2] = byte(v >> 8)
		b[3] = byte(v)
	}
}

// Compare compares one decimal to another, returns -1/0/1.
func (d *MyDecimal) Compare(to *MyDecimal) int {
	trace_util_0.Count(_mydecimal_00000, 389)
	if d.negative == to.negative {
		trace_util_0.Count(_mydecimal_00000, 392)
		cmp, err := doSub(d, to, nil)
		terror.Log(errors.Trace(err))
		return cmp
	}
	trace_util_0.Count(_mydecimal_00000, 390)
	if d.negative {
		trace_util_0.Count(_mydecimal_00000, 393)
		return -1
	}
	trace_util_0.Count(_mydecimal_00000, 391)
	return 1
}

// DecimalNeg reverses decimal's sign.
func DecimalNeg(from *MyDecimal) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 394)
	to := *from
	if from.IsZero() {
		trace_util_0.Count(_mydecimal_00000, 396)
		return &to
	}
	trace_util_0.Count(_mydecimal_00000, 395)
	to.negative = !from.negative
	return &to
}

// DecimalAdd adds two decimals, sets the result to 'to'.
// Note: DO NOT use `from1` or `from2` as `to` since the metadata
// of `to` may be changed during evaluating.
func DecimalAdd(from1, from2, to *MyDecimal) error {
	trace_util_0.Count(_mydecimal_00000, 397)
	to.resultFrac = myMaxInt8(from1.resultFrac, from2.resultFrac)
	if from1.negative == from2.negative {
		trace_util_0.Count(_mydecimal_00000, 399)
		return doAdd(from1, from2, to)
	}
	trace_util_0.Count(_mydecimal_00000, 398)
	_, err := doSub(from1, from2, to)
	return err
}

// DecimalSub subs one decimal from another, sets the result to 'to'.
func DecimalSub(from1, from2, to *MyDecimal) error {
	trace_util_0.Count(_mydecimal_00000, 400)
	to.resultFrac = myMaxInt8(from1.resultFrac, from2.resultFrac)
	if from1.negative == from2.negative {
		trace_util_0.Count(_mydecimal_00000, 402)
		_, err := doSub(from1, from2, to)
		return err
	}
	trace_util_0.Count(_mydecimal_00000, 401)
	return doAdd(from1, from2, to)
}

func doSub(from1, from2, to *MyDecimal) (cmp int, err error) {
	trace_util_0.Count(_mydecimal_00000, 403)
	var (
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsFracTo = myMax(wordsFrac1, wordsFrac2)

		start1 = 0
		stop1  = wordsInt1
		idx1   = 0
		start2 = 0
		stop2  = wordsInt2
		idx2   = 0
	)
	if from1.wordBuf[idx1] == 0 {
		trace_util_0.Count(_mydecimal_00000, 416)
		for idx1 < stop1 && from1.wordBuf[idx1] == 0 {
			trace_util_0.Count(_mydecimal_00000, 418)
			idx1++
		}
		trace_util_0.Count(_mydecimal_00000, 417)
		start1 = idx1
		wordsInt1 = stop1 - idx1
	}
	trace_util_0.Count(_mydecimal_00000, 404)
	if from2.wordBuf[idx2] == 0 {
		trace_util_0.Count(_mydecimal_00000, 419)
		for idx2 < stop2 && from2.wordBuf[idx2] == 0 {
			trace_util_0.Count(_mydecimal_00000, 421)
			idx2++
		}
		trace_util_0.Count(_mydecimal_00000, 420)
		start2 = idx2
		wordsInt2 = stop2 - idx2
	}

	trace_util_0.Count(_mydecimal_00000, 405)
	var carry int32
	if wordsInt2 > wordsInt1 {
		trace_util_0.Count(_mydecimal_00000, 422)
		carry = 1
	} else {
		trace_util_0.Count(_mydecimal_00000, 423)
		if wordsInt2 == wordsInt1 {
			trace_util_0.Count(_mydecimal_00000, 424)
			end1 := stop1 + wordsFrac1 - 1
			end2 := stop2 + wordsFrac2 - 1
			for idx1 <= end1 && from1.wordBuf[end1] == 0 {
				trace_util_0.Count(_mydecimal_00000, 428)
				end1--
			}
			trace_util_0.Count(_mydecimal_00000, 425)
			for idx2 <= end2 && from2.wordBuf[end2] == 0 {
				trace_util_0.Count(_mydecimal_00000, 429)
				end2--
			}
			trace_util_0.Count(_mydecimal_00000, 426)
			wordsFrac1 = end1 - stop1 + 1
			wordsFrac2 = end2 - stop2 + 1
			for idx1 <= end1 && idx2 <= end2 && from1.wordBuf[idx1] == from2.wordBuf[idx2] {
				trace_util_0.Count(_mydecimal_00000, 430)
				idx1++
				idx2++
			}
			trace_util_0.Count(_mydecimal_00000, 427)
			if idx1 <= end1 {
				trace_util_0.Count(_mydecimal_00000, 431)
				if idx2 <= end2 && from2.wordBuf[idx2] > from1.wordBuf[idx1] {
					trace_util_0.Count(_mydecimal_00000, 432)
					carry = 1
				} else {
					trace_util_0.Count(_mydecimal_00000, 433)
					{
						carry = 0
					}
				}
			} else {
				trace_util_0.Count(_mydecimal_00000, 434)
				{
					if idx2 <= end2 {
						trace_util_0.Count(_mydecimal_00000, 435)
						carry = 1
					} else {
						trace_util_0.Count(_mydecimal_00000, 436)
						{
							if to == nil {
								trace_util_0.Count(_mydecimal_00000, 438)
								return 0, nil
							}
							trace_util_0.Count(_mydecimal_00000, 437)
							*to = zeroMyDecimal
							return 0, nil
						}
					}
				}
			}
		}
	}

	trace_util_0.Count(_mydecimal_00000, 406)
	if to == nil {
		trace_util_0.Count(_mydecimal_00000, 439)
		if carry > 0 == from1.negative {
			trace_util_0.Count(_mydecimal_00000, 441) // from2 is negative too.
			return 1, nil
		}
		trace_util_0.Count(_mydecimal_00000, 440)
		return -1, nil
	}

	trace_util_0.Count(_mydecimal_00000, 407)
	to.negative = from1.negative

	/* ensure that always idx1 > idx2 (and wordsInt1 >= wordsInt2) */
	if carry > 0 {
		trace_util_0.Count(_mydecimal_00000, 442)
		from1, from2 = from2, from1
		start1, start2 = start2, start1
		wordsInt1, wordsInt2 = wordsInt2, wordsInt1
		wordsFrac1, wordsFrac2 = wordsFrac2, wordsFrac1
		to.negative = !to.negative
	}

	trace_util_0.Count(_mydecimal_00000, 408)
	wordsInt1, wordsFracTo, err = fixWordCntError(wordsInt1, wordsFracTo)
	idxTo := wordsInt1 + wordsFracTo
	to.digitsFrac = from1.digitsFrac
	if to.digitsFrac < from2.digitsFrac {
		trace_util_0.Count(_mydecimal_00000, 443)
		to.digitsFrac = from2.digitsFrac
	}
	trace_util_0.Count(_mydecimal_00000, 409)
	to.digitsInt = int8(wordsInt1 * digitsPerWord)
	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 444)
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			trace_util_0.Count(_mydecimal_00000, 448)
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
		}
		trace_util_0.Count(_mydecimal_00000, 445)
		if wordsFrac1 > wordsFracTo {
			trace_util_0.Count(_mydecimal_00000, 449)
			wordsFrac1 = wordsFracTo
		}
		trace_util_0.Count(_mydecimal_00000, 446)
		if wordsFrac2 > wordsFracTo {
			trace_util_0.Count(_mydecimal_00000, 450)
			wordsFrac2 = wordsFracTo
		}
		trace_util_0.Count(_mydecimal_00000, 447)
		if wordsInt2 > wordsInt1 {
			trace_util_0.Count(_mydecimal_00000, 451)
			wordsInt2 = wordsInt1
		}
	}
	trace_util_0.Count(_mydecimal_00000, 410)
	carry = 0

	/* part 1 - max(frac) ... min (frac) */
	if wordsFrac1 > wordsFrac2 {
		trace_util_0.Count(_mydecimal_00000, 452)
		idx1 = start1 + wordsInt1 + wordsFrac1
		stop1 = start1 + wordsInt1 + wordsFrac2
		idx2 = start2 + wordsInt2 + wordsFrac2
		for wordsFracTo > wordsFrac1 {
			trace_util_0.Count(_mydecimal_00000, 454)
			wordsFracTo--
			idxTo--
			to.wordBuf[idxTo] = 0
		}
		trace_util_0.Count(_mydecimal_00000, 453)
		for idx1 > stop1 {
			trace_util_0.Count(_mydecimal_00000, 455)
			idxTo--
			idx1--
			to.wordBuf[idxTo] = from1.wordBuf[idx1]
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 456)
		{
			idx1 = start1 + wordsInt1 + wordsFrac1
			idx2 = start2 + wordsInt2 + wordsFrac2
			stop2 = start2 + wordsInt2 + wordsFrac1
			for wordsFracTo > wordsFrac2 {
				trace_util_0.Count(_mydecimal_00000, 458)
				wordsFracTo--
				idxTo--
				to.wordBuf[idxTo] = 0
			}
			trace_util_0.Count(_mydecimal_00000, 457)
			for idx2 > stop2 {
				trace_util_0.Count(_mydecimal_00000, 459)
				idxTo--
				idx2--
				to.wordBuf[idxTo], carry = sub(0, from2.wordBuf[idx2], carry)
			}
		}
	}

	/* part 2 - min(frac) ... wordsInt2 */
	trace_util_0.Count(_mydecimal_00000, 411)
	for idx2 > start2 {
		trace_util_0.Count(_mydecimal_00000, 460)
		idxTo--
		idx1--
		idx2--
		to.wordBuf[idxTo], carry = sub(from1.wordBuf[idx1], from2.wordBuf[idx2], carry)
	}

	/* part 3 - wordsInt2 ... wordsInt1 */
	trace_util_0.Count(_mydecimal_00000, 412)
	for carry > 0 && idx1 > start1 {
		trace_util_0.Count(_mydecimal_00000, 461)
		idxTo--
		idx1--
		to.wordBuf[idxTo], carry = sub(from1.wordBuf[idx1], 0, carry)
	}
	trace_util_0.Count(_mydecimal_00000, 413)
	for idx1 > start1 {
		trace_util_0.Count(_mydecimal_00000, 462)
		idxTo--
		idx1--
		to.wordBuf[idxTo] = from1.wordBuf[idx1]
	}
	trace_util_0.Count(_mydecimal_00000, 414)
	for idxTo > 0 {
		trace_util_0.Count(_mydecimal_00000, 463)
		idxTo--
		to.wordBuf[idxTo] = 0
	}
	trace_util_0.Count(_mydecimal_00000, 415)
	return 0, err
}

func doAdd(from1, from2, to *MyDecimal) error {
	trace_util_0.Count(_mydecimal_00000, 464)
	var (
		err         error
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsIntTo  = myMax(wordsInt1, wordsInt2)
		wordsFracTo = myMax(wordsFrac1, wordsFrac2)
	)

	var x int32
	if wordsInt1 > wordsInt2 {
		trace_util_0.Count(_mydecimal_00000, 475)
		x = from1.wordBuf[0]
	} else {
		trace_util_0.Count(_mydecimal_00000, 476)
		if wordsInt2 > wordsInt1 {
			trace_util_0.Count(_mydecimal_00000, 477)
			x = from2.wordBuf[0]
		} else {
			trace_util_0.Count(_mydecimal_00000, 478)
			{
				x = from1.wordBuf[0] + from2.wordBuf[0]
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 465)
	if x > wordMax-1 {
		trace_util_0.Count(_mydecimal_00000, 479) /* yes, there is */
		wordsIntTo++
		to.wordBuf[0] = 0 /* safety */
	}

	trace_util_0.Count(_mydecimal_00000, 466)
	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	if err == ErrOverflow {
		trace_util_0.Count(_mydecimal_00000, 480)
		maxDecimal(wordBufLen*digitsPerWord, 0, to)
		return err
	}
	trace_util_0.Count(_mydecimal_00000, 467)
	idxTo := wordsIntTo + wordsFracTo
	to.negative = from1.negative
	to.digitsInt = int8(wordsIntTo * digitsPerWord)
	to.digitsFrac = myMaxInt8(from1.digitsFrac, from2.digitsFrac)

	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 481)
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			trace_util_0.Count(_mydecimal_00000, 486)
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
		}
		trace_util_0.Count(_mydecimal_00000, 482)
		if wordsFrac1 > wordsFracTo {
			trace_util_0.Count(_mydecimal_00000, 487)
			wordsFrac1 = wordsFracTo
		}
		trace_util_0.Count(_mydecimal_00000, 483)
		if wordsFrac2 > wordsFracTo {
			trace_util_0.Count(_mydecimal_00000, 488)
			wordsFrac2 = wordsFracTo
		}
		trace_util_0.Count(_mydecimal_00000, 484)
		if wordsInt1 > wordsIntTo {
			trace_util_0.Count(_mydecimal_00000, 489)
			wordsInt1 = wordsIntTo
		}
		trace_util_0.Count(_mydecimal_00000, 485)
		if wordsInt2 > wordsIntTo {
			trace_util_0.Count(_mydecimal_00000, 490)
			wordsInt2 = wordsIntTo
		}
	}
	trace_util_0.Count(_mydecimal_00000, 468)
	var dec1, dec2 = from1, from2
	var idx1, idx2, stop, stop2 int
	/* part 1 - max(frac) ... min (frac) */
	if wordsFrac1 > wordsFrac2 {
		trace_util_0.Count(_mydecimal_00000, 491)
		idx1 = wordsInt1 + wordsFrac1
		stop = wordsInt1 + wordsFrac2
		idx2 = wordsInt2 + wordsFrac2
		if wordsInt1 > wordsInt2 {
			trace_util_0.Count(_mydecimal_00000, 492)
			stop2 = wordsInt1 - wordsInt2
		}
	} else {
		trace_util_0.Count(_mydecimal_00000, 493)
		{
			idx1 = wordsInt2 + wordsFrac2
			stop = wordsInt2 + wordsFrac1
			idx2 = wordsInt1 + wordsFrac1
			if wordsInt2 > wordsInt1 {
				trace_util_0.Count(_mydecimal_00000, 495)
				stop2 = wordsInt2 - wordsInt1
			}
			trace_util_0.Count(_mydecimal_00000, 494)
			dec1, dec2 = from2, from1
		}
	}
	trace_util_0.Count(_mydecimal_00000, 469)
	for idx1 > stop {
		trace_util_0.Count(_mydecimal_00000, 496)
		idxTo--
		idx1--
		to.wordBuf[idxTo] = dec1.wordBuf[idx1]
	}

	/* part 2 - min(frac) ... min(digitsInt) */
	trace_util_0.Count(_mydecimal_00000, 470)
	carry := int32(0)
	for idx1 > stop2 {
		trace_util_0.Count(_mydecimal_00000, 497)
		idx1--
		idx2--
		idxTo--
		to.wordBuf[idxTo], carry = add(dec1.wordBuf[idx1], dec2.wordBuf[idx2], carry)
	}

	/* part 3 - min(digitsInt) ... max(digitsInt) */
	trace_util_0.Count(_mydecimal_00000, 471)
	stop = 0
	if wordsInt1 > wordsInt2 {
		trace_util_0.Count(_mydecimal_00000, 498)
		idx1 = wordsInt1 - wordsInt2
		dec1, dec2 = from1, from2
	} else {
		trace_util_0.Count(_mydecimal_00000, 499)
		{
			idx1 = wordsInt2 - wordsInt1
			dec1, dec2 = from2, from1
		}
	}
	trace_util_0.Count(_mydecimal_00000, 472)
	for idx1 > stop {
		trace_util_0.Count(_mydecimal_00000, 500)
		idxTo--
		idx1--
		to.wordBuf[idxTo], carry = add(dec1.wordBuf[idx1], 0, carry)
	}
	trace_util_0.Count(_mydecimal_00000, 473)
	if carry > 0 {
		trace_util_0.Count(_mydecimal_00000, 501)
		idxTo--
		to.wordBuf[idxTo] = 1
	}
	trace_util_0.Count(_mydecimal_00000, 474)
	return err
}

func maxDecimal(precision, frac int, to *MyDecimal) {
	trace_util_0.Count(_mydecimal_00000, 502)
	digitsInt := precision - frac
	to.negative = false
	to.digitsInt = int8(digitsInt)
	idx := 0
	if digitsInt > 0 {
		trace_util_0.Count(_mydecimal_00000, 504)
		firstWordDigits := digitsInt % digitsPerWord
		if firstWordDigits > 0 {
			trace_util_0.Count(_mydecimal_00000, 506)
			to.wordBuf[idx] = powers10[firstWordDigits] - 1 /* get 9 99 999 ... */
			idx++
		}
		trace_util_0.Count(_mydecimal_00000, 505)
		for digitsInt /= digitsPerWord; digitsInt > 0; digitsInt-- {
			trace_util_0.Count(_mydecimal_00000, 507)
			to.wordBuf[idx] = wordMax
			idx++
		}
	}
	trace_util_0.Count(_mydecimal_00000, 503)
	to.digitsFrac = int8(frac)
	if frac > 0 {
		trace_util_0.Count(_mydecimal_00000, 508)
		lastDigits := frac % digitsPerWord
		for frac /= digitsPerWord; frac > 0; frac-- {
			trace_util_0.Count(_mydecimal_00000, 510)
			to.wordBuf[idx] = wordMax
			idx++
		}
		trace_util_0.Count(_mydecimal_00000, 509)
		if lastDigits > 0 {
			trace_util_0.Count(_mydecimal_00000, 511)
			to.wordBuf[idx] = fracMax[lastDigits-1]
		}
	}
}

/*
DecimalMul multiplies two decimals.

      from1, from2 - factors
      to      - product

  RETURN VALUE
    E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW;

  NOTES
    in this implementation, with wordSize=4 we have digitsPerWord=9,
    and 63-digit number will take only 7 words (basically a 7-digit
    "base 999999999" number).  Thus there's no need in fast multiplication
    algorithms, 7-digit numbers can be multiplied with a naive O(n*n)
    method.

    XXX if this library is to be used with huge numbers of thousands of
    digits, fast multiplication must be implemented.
*/
func DecimalMul(from1, from2, to *MyDecimal) error {
	trace_util_0.Count(_mydecimal_00000, 512)
	var (
		err         error
		wordsInt1   = digitsToWords(int(from1.digitsInt))
		wordsFrac1  = digitsToWords(int(from1.digitsFrac))
		wordsInt2   = digitsToWords(int(from2.digitsInt))
		wordsFrac2  = digitsToWords(int(from2.digitsFrac))
		wordsIntTo  = digitsToWords(int(from1.digitsInt) + int(from2.digitsInt))
		wordsFracTo = wordsFrac1 + wordsFrac2
		idx1        = wordsInt1
		idx2        = wordsInt2
		idxTo       int
		tmp1        = wordsIntTo
		tmp2        = wordsFracTo
	)
	to.resultFrac = myMinInt8(from1.resultFrac+from2.resultFrac, mysql.MaxDecimalScale)
	wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
	to.negative = from1.negative != from2.negative
	to.digitsFrac = from1.digitsFrac + from2.digitsFrac
	if to.digitsFrac > notFixedDec {
		trace_util_0.Count(_mydecimal_00000, 520)
		to.digitsFrac = notFixedDec
	}
	trace_util_0.Count(_mydecimal_00000, 513)
	to.digitsInt = int8(wordsIntTo * digitsPerWord)
	if err == ErrOverflow {
		trace_util_0.Count(_mydecimal_00000, 521)
		return err
	}
	trace_util_0.Count(_mydecimal_00000, 514)
	if err != nil {
		trace_util_0.Count(_mydecimal_00000, 522)
		if to.digitsFrac > int8(wordsFracTo*digitsPerWord) {
			trace_util_0.Count(_mydecimal_00000, 525)
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
		}
		trace_util_0.Count(_mydecimal_00000, 523)
		if to.digitsInt > int8(wordsIntTo*digitsPerWord) {
			trace_util_0.Count(_mydecimal_00000, 526)
			to.digitsInt = int8(wordsIntTo * digitsPerWord)
		}
		trace_util_0.Count(_mydecimal_00000, 524)
		if tmp1 > wordsIntTo {
			trace_util_0.Count(_mydecimal_00000, 527)
			tmp1 -= wordsIntTo
			tmp2 = tmp1 >> 1
			wordsInt2 -= tmp1 - tmp2
			wordsFrac1 = 0
			wordsFrac2 = 0
		} else {
			trace_util_0.Count(_mydecimal_00000, 528)
			{
				tmp2 -= wordsFracTo
				tmp1 = tmp2 >> 1
				if wordsFrac1 <= wordsFrac2 {
					trace_util_0.Count(_mydecimal_00000, 529)
					wordsFrac1 -= tmp1
					wordsFrac2 -= tmp2 - tmp1
				} else {
					trace_util_0.Count(_mydecimal_00000, 530)
					{
						wordsFrac2 -= tmp1
						wordsFrac1 -= tmp2 - tmp1
					}
				}
			}
		}
	}
	trace_util_0.Count(_mydecimal_00000, 515)
	startTo := wordsIntTo + wordsFracTo - 1
	start2 := idx2 + wordsFrac2 - 1
	stop1 := idx1 - wordsInt1
	stop2 := idx2 - wordsInt2
	to.wordBuf = zeroMyDecimal.wordBuf

	for idx1 += wordsFrac1 - 1; idx1 >= stop1; idx1-- {
		trace_util_0.Count(_mydecimal_00000, 531)
		carry := int32(0)
		idxTo = startTo
		idx2 = start2
		for idx2 >= stop2 {
			trace_util_0.Count(_mydecimal_00000, 535)
			var hi, lo int32
			p := int64(from1.wordBuf[idx1]) * int64(from2.wordBuf[idx2])
			hi = int32(p / wordBase)
			lo = int32(p - int64(hi)*wordBase)
			to.wordBuf[idxTo], carry = add2(to.wordBuf[idxTo], lo, carry)
			carry += hi
			idx2--
			idxTo--
		}
		trace_util_0.Count(_mydecimal_00000, 532)
		if carry > 0 {
			trace_util_0.Count(_mydecimal_00000, 536)
			if idxTo < 0 {
				trace_util_0.Count(_mydecimal_00000, 538)
				return ErrOverflow
			}
			trace_util_0.Count(_mydecimal_00000, 537)
			to.wordBuf[idxTo], carry = add2(to.wordBuf[idxTo], 0, carry)
		}
		trace_util_0.Count(_mydecimal_00000, 533)
		for idxTo--; carry > 0; idxTo-- {
			trace_util_0.Count(_mydecimal_00000, 539)
			if idxTo < 0 {
				trace_util_0.Count(_mydecimal_00000, 541)
				return ErrOverflow
			}
			trace_util_0.Count(_mydecimal_00000, 540)
			to.wordBuf[idxTo], carry = add(to.wordBuf[idxTo], 0, carry)
		}
		trace_util_0.Count(_mydecimal_00000, 534)
		startTo--
	}

	/* Now we have to check for -0.000 case */
	trace_util_0.Count(_mydecimal_00000, 516)
	if to.negative {
		trace_util_0.Count(_mydecimal_00000, 542)
		idx := 0
		end := wordsIntTo + wordsFracTo
		for {
			trace_util_0.Count(_mydecimal_00000, 543)
			if to.wordBuf[idx] != 0 {
				trace_util_0.Count(_mydecimal_00000, 545)
				break
			}
			trace_util_0.Count(_mydecimal_00000, 544)
			idx++
			/* We got decimal zero */
			if idx == end {
				trace_util_0.Count(_mydecimal_00000, 546)
				*to = zeroMyDecimal
				break
			}
		}
	}

	trace_util_0.Count(_mydecimal_00000, 517)
	idxTo = 0
	dToMove := wordsIntTo + digitsToWords(int(to.digitsFrac))
	for to.wordBuf[idxTo] == 0 && to.digitsInt > digitsPerWord {
		trace_util_0.Count(_mydecimal_00000, 547)
		idxTo++
		to.digitsInt -= digitsPerWord
		dToMove--
	}
	trace_util_0.Count(_mydecimal_00000, 518)
	if idxTo > 0 {
		trace_util_0.Count(_mydecimal_00000, 548)
		curIdx := 0
		for dToMove > 0 {
			trace_util_0.Count(_mydecimal_00000, 549)
			to.wordBuf[curIdx] = to.wordBuf[idxTo]
			curIdx++
			idxTo++
			dToMove--
		}
	}
	trace_util_0.Count(_mydecimal_00000, 519)
	return err
}

// DecimalDiv does division of two decimals.
//
// from1    - dividend
// from2    - divisor
// to       - quotient
// fracIncr - increment of fraction
func DecimalDiv(from1, from2, to *MyDecimal, fracIncr int) error {
	trace_util_0.Count(_mydecimal_00000, 550)
	to.resultFrac = myMinInt8(from1.resultFrac+int8(fracIncr), mysql.MaxDecimalScale)
	return doDivMod(from1, from2, to, nil, fracIncr)
}

/*
DecimalMod does modulus of two decimals.

      from1   - dividend
      from2   - divisor
      to      - modulus

  RETURN VALUE
    E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW/E_DEC_DIV_ZERO;

  NOTES
    see do_div_mod()

  DESCRIPTION
    the modulus R in    R = M mod N

   is defined as

     0 <= |R| < |M|
     sign R == sign M
     R = M - k*N, where k is integer

   thus, there's no requirement for M or N to be integers
*/
func DecimalMod(from1, from2, to *MyDecimal) error {
	trace_util_0.Count(_mydecimal_00000, 551)
	to.resultFrac = myMaxInt8(from1.resultFrac, from2.resultFrac)
	return doDivMod(from1, from2, nil, to, 0)
}

func doDivMod(from1, from2, to, mod *MyDecimal, fracIncr int) error {
	trace_util_0.Count(_mydecimal_00000, 552)
	var (
		frac1 = digitsToWords(int(from1.digitsFrac)) * digitsPerWord
		prec1 = int(from1.digitsInt) + frac1
		frac2 = digitsToWords(int(from2.digitsFrac)) * digitsPerWord
		prec2 = int(from2.digitsInt) + frac2
	)
	if mod != nil {
		trace_util_0.Count(_mydecimal_00000, 570)
		to = mod
	}

	/* removing all the leading zeros */
	trace_util_0.Count(_mydecimal_00000, 553)
	i := ((prec2 - 1) % digitsPerWord) + 1
	idx2 := 0
	for prec2 > 0 && from2.wordBuf[idx2] == 0 {
		trace_util_0.Count(_mydecimal_00000, 571)
		prec2 -= i
		i = digitsPerWord
		idx2++
	}
	trace_util_0.Count(_mydecimal_00000, 554)
	if prec2 <= 0 {
		trace_util_0.Count(_mydecimal_00000, 572)
		/* short-circuit everything: from2 == 0 */
		return ErrDivByZero
	}

	trace_util_0.Count(_mydecimal_00000, 555)
	prec2 -= countLeadingZeroes((prec2-1)%digitsPerWord, from2.wordBuf[idx2])
	i = ((prec1 - 1) % digitsPerWord) + 1
	idx1 := 0
	for prec1 > 0 && from1.wordBuf[idx1] == 0 {
		trace_util_0.Count(_mydecimal_00000, 573)
		prec1 -= i
		i = digitsPerWord
		idx1++
	}
	trace_util_0.Count(_mydecimal_00000, 556)
	if prec1 <= 0 {
		trace_util_0.Count(_mydecimal_00000, 574)
		/* short-circuit everything: from1 == 0 */
		*to = zeroMyDecimal
		return nil
	}
	trace_util_0.Count(_mydecimal_00000, 557)
	prec1 -= countLeadingZeroes((prec1-1)%digitsPerWord, from1.wordBuf[idx1])

	/* let's fix fracIncr, taking into account frac1,frac2 increase */
	fracIncr -= frac1 - int(from1.digitsFrac) + frac2 - int(from2.digitsFrac)
	if fracIncr < 0 {
		trace_util_0.Count(_mydecimal_00000, 575)
		fracIncr = 0
	}

	trace_util_0.Count(_mydecimal_00000, 558)
	digitsIntTo := (prec1 - frac1) - (prec2 - frac2)
	if from1.wordBuf[idx1] >= from2.wordBuf[idx2] {
		trace_util_0.Count(_mydecimal_00000, 576)
		digitsIntTo++
	}
	trace_util_0.Count(_mydecimal_00000, 559)
	var wordsIntTo int
	if digitsIntTo < 0 {
		trace_util_0.Count(_mydecimal_00000, 577)
		digitsIntTo /= digitsPerWord
		wordsIntTo = 0
	} else {
		trace_util_0.Count(_mydecimal_00000, 578)
		{
			wordsIntTo = digitsToWords(digitsIntTo)
		}
	}
	trace_util_0.Count(_mydecimal_00000, 560)
	var wordsFracTo int
	var err error
	if mod != nil {
		trace_util_0.Count(_mydecimal_00000, 579)
		// we're calculating N1 % N2.
		// The result will have
		// digitsFrac=max(frac1, frac2), as for subtraction
		// digitsInt=from2.digitsInt
		to.negative = from1.negative
		to.digitsFrac = myMaxInt8(from1.digitsFrac, from2.digitsFrac)
	} else {
		trace_util_0.Count(_mydecimal_00000, 580)
		{
			wordsFracTo = digitsToWords(frac1 + frac2 + fracIncr)
			wordsIntTo, wordsFracTo, err = fixWordCntError(wordsIntTo, wordsFracTo)
			to.negative = from1.negative != from2.negative
			to.digitsInt = int8(wordsIntTo * digitsPerWord)
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
		}
	}
	trace_util_0.Count(_mydecimal_00000, 561)
	idxTo := 0
	stopTo := wordsIntTo + wordsFracTo
	if mod == nil {
		trace_util_0.Count(_mydecimal_00000, 581)
		for digitsIntTo < 0 && idxTo < wordBufLen {
			trace_util_0.Count(_mydecimal_00000, 582)
			to.wordBuf[idxTo] = 0
			idxTo++
			digitsIntTo++
		}
	}
	trace_util_0.Count(_mydecimal_00000, 562)
	i = digitsToWords(prec1)
	len1 := i + digitsToWords(2*frac2+fracIncr+1) + 1
	if len1 < 3 {
		trace_util_0.Count(_mydecimal_00000, 583)
		len1 = 3
	}

	trace_util_0.Count(_mydecimal_00000, 563)
	tmp1 := make([]int32, len1)
	copy(tmp1, from1.wordBuf[idx1:idx1+i])

	start1 := 0
	var stop1 int
	start2 := idx2
	stop2 := idx2 + digitsToWords(prec2) - 1

	/* removing end zeroes */
	for from2.wordBuf[stop2] == 0 && stop2 >= start2 {
		trace_util_0.Count(_mydecimal_00000, 584)
		stop2--
	}
	trace_util_0.Count(_mydecimal_00000, 564)
	len2 := stop2 - start2
	stop2++

	/*
	   calculating norm2 (normalized from2.wordBuf[start2]) - we need from2.wordBuf[start2] to be large
	   (at least > DIG_BASE/2), but unlike Knuth's Alg. D we don't want to
	   normalize input numbers (as we don't make a copy of the divisor).
	   Thus we normalize first dec1 of buf2 only, and we'll normalize tmp1[start1]
	   on the fly for the purpose of guesstimation only.
	   It's also faster, as we're saving on normalization of from2.
	*/
	normFactor := wordBase / int64(from2.wordBuf[start2]+1)
	norm2 := int32(normFactor * int64(from2.wordBuf[start2]))
	if len2 > 0 {
		trace_util_0.Count(_mydecimal_00000, 585)
		norm2 += int32(normFactor * int64(from2.wordBuf[start2+1]) / wordBase)
	}
	trace_util_0.Count(_mydecimal_00000, 565)
	dcarry := int32(0)
	if tmp1[start1] < from2.wordBuf[start2] {
		trace_util_0.Count(_mydecimal_00000, 586)
		dcarry = tmp1[start1]
		start1++
	}

	// main loop
	trace_util_0.Count(_mydecimal_00000, 566)
	var guess int64
	for ; idxTo < stopTo; idxTo++ {
		trace_util_0.Count(_mydecimal_00000, 587)
		/* short-circuit, if possible */
		if dcarry == 0 && tmp1[start1] < from2.wordBuf[start2] {
			trace_util_0.Count(_mydecimal_00000, 590)
			guess = 0
		} else {
			trace_util_0.Count(_mydecimal_00000, 591)
			{
				/* D3: make a guess */
				x := int64(tmp1[start1]) + int64(dcarry)*wordBase
				y := int64(tmp1[start1+1])
				guess = (normFactor*x + normFactor*y/wordBase) / int64(norm2)
				if guess >= wordBase {
					trace_util_0.Count(_mydecimal_00000, 596)
					guess = wordBase - 1
				}

				trace_util_0.Count(_mydecimal_00000, 592)
				if len2 > 0 {
					trace_util_0.Count(_mydecimal_00000, 597)
					/* remove normalization */
					if int64(from2.wordBuf[start2+1])*guess > (x-guess*int64(from2.wordBuf[start2]))*wordBase+y {
						trace_util_0.Count(_mydecimal_00000, 599)
						guess--
					}
					trace_util_0.Count(_mydecimal_00000, 598)
					if int64(from2.wordBuf[start2+1])*guess > (x-guess*int64(from2.wordBuf[start2]))*wordBase+y {
						trace_util_0.Count(_mydecimal_00000, 600)
						guess--
					}
				}

				/* D4: multiply and subtract */
				trace_util_0.Count(_mydecimal_00000, 593)
				idx2 = stop2
				idx1 = start1 + len2
				var carry int32
				for carry = 0; idx2 > start2; idx1-- {
					trace_util_0.Count(_mydecimal_00000, 601)
					var hi, lo int32
					idx2--
					x = guess * int64(from2.wordBuf[idx2])
					hi = int32(x / wordBase)
					lo = int32(x - int64(hi)*wordBase)
					tmp1[idx1], carry = sub2(tmp1[idx1], lo, carry)
					carry += hi
				}
				trace_util_0.Count(_mydecimal_00000, 594)
				if dcarry < carry {
					trace_util_0.Count(_mydecimal_00000, 602)
					carry = 1
				} else {
					trace_util_0.Count(_mydecimal_00000, 603)
					{
						carry = 0
					}
				}

				/* D5: check the remainder */
				trace_util_0.Count(_mydecimal_00000, 595)
				if carry > 0 {
					trace_util_0.Count(_mydecimal_00000, 604)
					/* D6: correct the guess */
					guess--
					idx2 = stop2
					idx1 = start1 + len2
					for carry = 0; idx2 > start2; idx1-- {
						trace_util_0.Count(_mydecimal_00000, 605)
						idx2--
						tmp1[idx1], carry = add(tmp1[idx1], from2.wordBuf[idx2], carry)
					}
				}
			}
		}
		trace_util_0.Count(_mydecimal_00000, 588)
		if mod == nil {
			trace_util_0.Count(_mydecimal_00000, 606)
			to.wordBuf[idxTo] = int32(guess)
		}
		trace_util_0.Count(_mydecimal_00000, 589)
		dcarry = tmp1[start1]
		start1++
	}
	trace_util_0.Count(_mydecimal_00000, 567)
	if mod != nil {
		trace_util_0.Count(_mydecimal_00000, 607)
		/*
		   now the result is in tmp1, it has
		   digitsInt=prec1-frac1
		   digitsFrac=max(frac1, frac2)
		*/
		if dcarry != 0 {
			trace_util_0.Count(_mydecimal_00000, 613)
			start1--
			tmp1[start1] = dcarry
		}
		trace_util_0.Count(_mydecimal_00000, 608)
		idxTo = 0

		digitsIntTo = prec1 - frac1 - start1*digitsPerWord
		if digitsIntTo < 0 {
			trace_util_0.Count(_mydecimal_00000, 614)
			/* If leading zeroes in the fractional part were earlier stripped */
			wordsIntTo = digitsIntTo / digitsPerWord
		} else {
			trace_util_0.Count(_mydecimal_00000, 615)
			{
				wordsIntTo = digitsToWords(digitsIntTo)
			}
		}

		trace_util_0.Count(_mydecimal_00000, 609)
		wordsFracTo = digitsToWords(int(to.digitsFrac))
		err = nil
		if wordsIntTo == 0 && wordsFracTo == 0 {
			trace_util_0.Count(_mydecimal_00000, 616)
			*to = zeroMyDecimal
			return err
		}
		trace_util_0.Count(_mydecimal_00000, 610)
		if wordsIntTo <= 0 {
			trace_util_0.Count(_mydecimal_00000, 617)
			if -wordsIntTo >= wordBufLen {
				trace_util_0.Count(_mydecimal_00000, 619)
				*to = zeroMyDecimal
				return ErrTruncated
			}
			trace_util_0.Count(_mydecimal_00000, 618)
			stop1 = start1 + wordsIntTo + wordsFracTo
			wordsFracTo += wordsIntTo
			to.digitsInt = 0
			for wordsIntTo < 0 {
				trace_util_0.Count(_mydecimal_00000, 620)
				to.wordBuf[idxTo] = 0
				idxTo++
				wordsIntTo++
			}
		} else {
			trace_util_0.Count(_mydecimal_00000, 621)
			{
				if wordsIntTo > wordBufLen {
					trace_util_0.Count(_mydecimal_00000, 623)
					to.digitsInt = int8(digitsPerWord * wordBufLen)
					to.digitsFrac = 0
					return ErrOverflow
				}
				trace_util_0.Count(_mydecimal_00000, 622)
				stop1 = start1 + wordsIntTo + wordsFracTo
				to.digitsInt = int8(myMin(wordsIntTo*digitsPerWord, int(from2.digitsInt)))
			}
		}
		trace_util_0.Count(_mydecimal_00000, 611)
		if wordsIntTo+wordsFracTo > wordBufLen {
			trace_util_0.Count(_mydecimal_00000, 624)
			stop1 -= wordsIntTo + wordsFracTo - wordBufLen
			wordsFracTo = wordBufLen - wordsIntTo
			to.digitsFrac = int8(wordsFracTo * digitsPerWord)
			err = ErrTruncated
		}
		trace_util_0.Count(_mydecimal_00000, 612)
		for start1 < stop1 {
			trace_util_0.Count(_mydecimal_00000, 625)
			to.wordBuf[idxTo] = tmp1[start1]
			idxTo++
			start1++
		}
	}
	trace_util_0.Count(_mydecimal_00000, 568)
	idxTo, digitsIntTo = to.removeLeadingZeros()
	to.digitsInt = int8(digitsIntTo)
	if idxTo != 0 {
		trace_util_0.Count(_mydecimal_00000, 626)
		copy(to.wordBuf[:], to.wordBuf[idxTo:])
	}
	trace_util_0.Count(_mydecimal_00000, 569)
	return err
}

// DecimalPeak returns the length of the encoded decimal.
func DecimalPeak(b []byte) (int, error) {
	trace_util_0.Count(_mydecimal_00000, 627)
	if len(b) < 3 {
		trace_util_0.Count(_mydecimal_00000, 629)
		return 0, ErrBadNumber
	}
	trace_util_0.Count(_mydecimal_00000, 628)
	precision := int(b[0])
	frac := int(b[1])
	return decimalBinSize(precision, frac) + 2, nil
}

// NewDecFromInt creates a MyDecimal from int.
func NewDecFromInt(i int64) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 630)
	return new(MyDecimal).FromInt(i)
}

// NewDecFromUint creates a MyDecimal from uint.
func NewDecFromUint(i uint64) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 631)
	return new(MyDecimal).FromUint(i)
}

// NewDecFromFloatForTest creates a MyDecimal from float, as it returns no error, it should only be used in test.
func NewDecFromFloatForTest(f float64) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 632)
	dec := new(MyDecimal)
	err := dec.FromFloat64(f)
	terror.Log(errors.Trace(err))
	return dec
}

// NewDecFromStringForTest creates a MyDecimal from string, as it returns no error, it should only be used in test.
func NewDecFromStringForTest(s string) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 633)
	dec := new(MyDecimal)
	err := dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
	return dec
}

// NewMaxOrMinDec returns the max or min value decimal for given precision and fraction.
func NewMaxOrMinDec(negative bool, prec, frac int) *MyDecimal {
	trace_util_0.Count(_mydecimal_00000, 634)
	str := make([]byte, prec+2)
	for i := 0; i < len(str); i++ {
		trace_util_0.Count(_mydecimal_00000, 637)
		str[i] = '9'
	}
	trace_util_0.Count(_mydecimal_00000, 635)
	if negative {
		trace_util_0.Count(_mydecimal_00000, 638)
		str[0] = '-'
	} else {
		trace_util_0.Count(_mydecimal_00000, 639)
		{
			str[0] = '+'
		}
	}
	trace_util_0.Count(_mydecimal_00000, 636)
	str[1+prec-frac] = '.'
	dec := new(MyDecimal)
	err := dec.FromString(str)
	terror.Log(errors.Trace(err))
	return dec
}

var _mydecimal_00000 = "types/mydecimal.go"
