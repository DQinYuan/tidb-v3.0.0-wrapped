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

package stringutil

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/hack"
)

// ErrSyntax indicates that a value does not have the right syntax for the target type.
var ErrSyntax = errors.New("invalid syntax")

// UnquoteChar decodes the first character or byte in the escaped string
// or character literal represented by the string s.
// It returns four values:
//
//1) value, the decoded Unicode code point or byte value;
//2) multibyte, a boolean indicating whether the decoded character requires a multibyte UTF-8 representation;
//3) tail, the remainder of the string after the character; and
//4) an error that will be nil if the character is syntactically valid.
//
// The second argument, quote, specifies the type of literal being parsed
// and therefore which escaped quote character is permitted.
// If set to a single quote, it permits the sequence \' and disallows unescaped '.
// If set to a double quote, it permits \" and disallows unescaped ".
// If set to zero, it does not permit either escape and allows both quote characters to appear unescaped.
// Different with strconv.UnquoteChar, it permits unnecessary backslash.
func UnquoteChar(s string, quote byte) (value []byte, tail string, err error) {
	trace_util_0.Count(_string_util_00000, 0)
	// easy cases
	switch c := s[0]; {
	case c == quote:
		trace_util_0.Count(_string_util_00000, 4)
		err = errors.Trace(ErrSyntax)
		return
	case c >= utf8.RuneSelf:
		trace_util_0.Count(_string_util_00000, 5)
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError {
			trace_util_0.Count(_string_util_00000, 8)
			value = append(value, c)
			return value, s[1:], nil
		}
		trace_util_0.Count(_string_util_00000, 6)
		value = append(value, string(r)...)
		return value, s[size:], nil
	case c != '\\':
		trace_util_0.Count(_string_util_00000, 7)
		value = append(value, c)
		return value, s[1:], nil
	}
	// hard case: c is backslash
	trace_util_0.Count(_string_util_00000, 1)
	if len(s) <= 1 {
		trace_util_0.Count(_string_util_00000, 9)
		err = errors.Trace(ErrSyntax)
		return
	}
	trace_util_0.Count(_string_util_00000, 2)
	c := s[1]
	s = s[2:]
	switch c {
	case 'b':
		trace_util_0.Count(_string_util_00000, 10)
		value = append(value, '\b')
	case 'n':
		trace_util_0.Count(_string_util_00000, 11)
		value = append(value, '\n')
	case 'r':
		trace_util_0.Count(_string_util_00000, 12)
		value = append(value, '\r')
	case 't':
		trace_util_0.Count(_string_util_00000, 13)
		value = append(value, '\t')
	case 'Z':
		trace_util_0.Count(_string_util_00000, 14)
		value = append(value, '\032')
	case '0':
		trace_util_0.Count(_string_util_00000, 15)
		value = append(value, '\000')
	case '_', '%':
		trace_util_0.Count(_string_util_00000, 16)
		value = append(value, '\\')
		value = append(value, c)
	case '\\':
		trace_util_0.Count(_string_util_00000, 17)
		value = append(value, '\\')
	case '\'', '"':
		trace_util_0.Count(_string_util_00000, 18)
		value = append(value, c)
	default:
		trace_util_0.Count(_string_util_00000, 19)
		value = append(value, c)
	}
	trace_util_0.Count(_string_util_00000, 3)
	tail = s
	return
}

// Unquote interprets s as a single-quoted, double-quoted,
// or backquoted Go string literal, returning the string value
// that s quotes. For example: test=`"\"\n"` (hex: 22 5c 22 5c 6e 22)
// should be converted to `"\n` (hex: 22 0a).
func Unquote(s string) (t string, err error) {
	trace_util_0.Count(_string_util_00000, 20)
	n := len(s)
	if n < 2 {
		trace_util_0.Count(_string_util_00000, 26)
		return "", errors.Trace(ErrSyntax)
	}
	trace_util_0.Count(_string_util_00000, 21)
	quote := s[0]
	if quote != s[n-1] {
		trace_util_0.Count(_string_util_00000, 27)
		return "", errors.Trace(ErrSyntax)
	}
	trace_util_0.Count(_string_util_00000, 22)
	s = s[1 : n-1]
	if quote != '"' && quote != '\'' {
		trace_util_0.Count(_string_util_00000, 28)
		return "", errors.Trace(ErrSyntax)
	}
	// Avoid allocation. No need to convert if there is no '\'
	trace_util_0.Count(_string_util_00000, 23)
	if strings.IndexByte(s, '\\') == -1 && strings.IndexByte(s, quote) == -1 {
		trace_util_0.Count(_string_util_00000, 29)
		return s, nil
	}
	trace_util_0.Count(_string_util_00000, 24)
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		trace_util_0.Count(_string_util_00000, 30)
		mb, ss, err := UnquoteChar(s, quote)
		if err != nil {
			trace_util_0.Count(_string_util_00000, 32)
			return "", errors.Trace(err)
		}
		trace_util_0.Count(_string_util_00000, 31)
		s = ss
		buf = append(buf, mb...)
	}
	trace_util_0.Count(_string_util_00000, 25)
	return string(buf), nil
}

const (
	patMatch = iota + 1
	patOne
	patAny
)

// CompilePattern handles escapes and wild cards convert pattern characters and
// pattern types.
func CompilePattern(pattern string, escape byte) (patChars, patTypes []byte) {
	trace_util_0.Count(_string_util_00000, 33)
	var lastAny bool
	patChars = make([]byte, len(pattern))
	patTypes = make([]byte, len(pattern))
	patLen := 0
	for i := 0; i < len(pattern); i++ {
		trace_util_0.Count(_string_util_00000, 35)
		var tp byte
		var c = pattern[i]
		switch c {
		case escape:
			trace_util_0.Count(_string_util_00000, 37)
			lastAny = false
			tp = patMatch
			if i < len(pattern)-1 {
				trace_util_0.Count(_string_util_00000, 43)
				i++
				c = pattern[i]
				if c == escape || c == '_' || c == '%' {
					trace_util_0.Count(_string_util_00000, 44)
					// Valid escape.
				} else {
					trace_util_0.Count(_string_util_00000, 45)
					{
						// Invalid escape, fall back to escape byte.
						// mysql will treat escape character as the origin value even
						// the escape sequence is invalid in Go or C.
						// e.g., \m is invalid in Go, but in MySQL we will get "m" for select '\m'.
						// Following case is correct just for escape \, not for others like +.
						// TODO: Add more checks for other escapes.
						i--
						c = escape
					}
				}
			}
		case '_':
			trace_util_0.Count(_string_util_00000, 38)
			if lastAny {
				trace_util_0.Count(_string_util_00000, 46)
				patChars[patLen-1], patTypes[patLen-1] = c, patOne
				patChars[patLen], patTypes[patLen] = '%', patAny
				patLen++
				continue
			}
			trace_util_0.Count(_string_util_00000, 39)
			tp = patOne
		case '%':
			trace_util_0.Count(_string_util_00000, 40)
			if lastAny {
				trace_util_0.Count(_string_util_00000, 47)
				continue
			}
			trace_util_0.Count(_string_util_00000, 41)
			lastAny = true
			tp = patAny
		default:
			trace_util_0.Count(_string_util_00000, 42)
			lastAny = false
			tp = patMatch
		}
		trace_util_0.Count(_string_util_00000, 36)
		patChars[patLen] = c
		patTypes[patLen] = tp
		patLen++
	}
	trace_util_0.Count(_string_util_00000, 34)
	patChars = patChars[:patLen]
	patTypes = patTypes[:patLen]
	return
}

const caseDiff = 'a' - 'A'

// NOTE: Currently tikv's like function is case sensitive, so we keep its behavior here.
func matchByteCI(a, b byte) bool {
	trace_util_0.Count(_string_util_00000, 48)
	return a == b
	// We may reuse below code block when like function go back to case insensitive.
	/*
		if a == b {
			return true
		}
		if a >= 'a' && a <= 'z' && a-caseDiff == b {
			return true
		}
		return a >= 'A' && a <= 'Z' && a+caseDiff == b
	*/
}

// DoMatch matches the string with patChars and patTypes.
func DoMatch(str string, patChars, patTypes []byte) bool {
	trace_util_0.Count(_string_util_00000, 49)
	var sIdx int
	for i := 0; i < len(patChars); i++ {
		trace_util_0.Count(_string_util_00000, 51)
		switch patTypes[i] {
		case patMatch:
			trace_util_0.Count(_string_util_00000, 52)
			if sIdx >= len(str) || !matchByteCI(str[sIdx], patChars[i]) {
				trace_util_0.Count(_string_util_00000, 58)
				return false
			}
			trace_util_0.Count(_string_util_00000, 53)
			sIdx++
		case patOne:
			trace_util_0.Count(_string_util_00000, 54)
			sIdx++
			if sIdx > len(str) {
				trace_util_0.Count(_string_util_00000, 59)
				return false
			}
		case patAny:
			trace_util_0.Count(_string_util_00000, 55)
			i++
			if i == len(patChars) {
				trace_util_0.Count(_string_util_00000, 60)
				return true
			}
			trace_util_0.Count(_string_util_00000, 56)
			for sIdx < len(str) {
				trace_util_0.Count(_string_util_00000, 61)
				if matchByteCI(patChars[i], str[sIdx]) && DoMatch(str[sIdx:], patChars[i:], patTypes[i:]) {
					trace_util_0.Count(_string_util_00000, 63)
					return true
				}
				trace_util_0.Count(_string_util_00000, 62)
				sIdx++
			}
			trace_util_0.Count(_string_util_00000, 57)
			return false
		}
	}
	trace_util_0.Count(_string_util_00000, 50)
	return sIdx == len(str)
}

// IsExactMatch return true if no wildcard character
func IsExactMatch(patTypes []byte) bool {
	trace_util_0.Count(_string_util_00000, 64)
	for _, pt := range patTypes {
		trace_util_0.Count(_string_util_00000, 66)
		if pt != patMatch {
			trace_util_0.Count(_string_util_00000, 67)
			return false
		}
	}
	trace_util_0.Count(_string_util_00000, 65)
	return true
}

// Copy deep copies a string.
func Copy(src string) string {
	trace_util_0.Count(_string_util_00000, 68)
	return string(hack.Slice(src))
}

// stringerFunc defines string func implement fmt.Stringer.
type stringerFunc func() string

// String implements fmt.Stringer
func (l stringerFunc) String() string {
	trace_util_0.Count(_string_util_00000, 69)
	return l()
}

// MemoizeStr returns memoized version of stringFunc.
func MemoizeStr(l func() string) fmt.Stringer {
	trace_util_0.Count(_string_util_00000, 70)
	var result string
	return stringerFunc(func() string {
		trace_util_0.Count(_string_util_00000, 71)
		if result != "" {
			trace_util_0.Count(_string_util_00000, 73)
			return result
		}
		trace_util_0.Count(_string_util_00000, 72)
		result = l()
		return result
	})
}

// StringerStr defines a alias to normal string.
// implement fmt.Stringer
type StringerStr string

// String implements fmt.Stringer
func (i StringerStr) String() string {
	trace_util_0.Count(_string_util_00000, 74)
	return string(i)
}

var _string_util_00000 = "util/stringutil/string_util.go"
