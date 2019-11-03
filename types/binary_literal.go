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

package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/trace_util_0"
)

// BinaryLiteral is the internal type for storing bit / hex literal type.
type BinaryLiteral []byte

// BitLiteral is the bit literal type.
type BitLiteral BinaryLiteral

// HexLiteral is the hex literal type.
type HexLiteral BinaryLiteral

// ZeroBinaryLiteral is a BinaryLiteral literal with zero value.
var ZeroBinaryLiteral = BinaryLiteral{}

func trimLeadingZeroBytes(bytes []byte) []byte {
	trace_util_0.Count(_binary_literal_00000, 0)
	if len(bytes) == 0 {
		trace_util_0.Count(_binary_literal_00000, 3)
		return bytes
	}
	trace_util_0.Count(_binary_literal_00000, 1)
	pos, posMax := 0, len(bytes)-1
	for ; pos < posMax; pos++ {
		trace_util_0.Count(_binary_literal_00000, 4)
		if bytes[pos] != 0 {
			trace_util_0.Count(_binary_literal_00000, 5)
			break
		}
	}
	trace_util_0.Count(_binary_literal_00000, 2)
	return bytes[pos:]
}

// NewBinaryLiteralFromUint creates a new BinaryLiteral instance by the given uint value in BitEndian.
// byteSize will be used as the length of the new BinaryLiteral, with leading bytes filled to zero.
// If byteSize is -1, the leading zeros in new BinaryLiteral will be trimmed.
func NewBinaryLiteralFromUint(value uint64, byteSize int) BinaryLiteral {
	trace_util_0.Count(_binary_literal_00000, 6)
	if byteSize != -1 && (byteSize < 1 || byteSize > 8) {
		trace_util_0.Count(_binary_literal_00000, 9)
		panic("Invalid byteSize")
	}
	trace_util_0.Count(_binary_literal_00000, 7)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	if byteSize == -1 {
		trace_util_0.Count(_binary_literal_00000, 10)
		buf = trimLeadingZeroBytes(buf)
	} else {
		trace_util_0.Count(_binary_literal_00000, 11)
		{
			buf = buf[8-byteSize:]
		}
	}
	trace_util_0.Count(_binary_literal_00000, 8)
	return buf
}

// String implements fmt.Stringer interface.
func (b BinaryLiteral) String() string {
	trace_util_0.Count(_binary_literal_00000, 12)
	if len(b) == 0 {
		trace_util_0.Count(_binary_literal_00000, 14)
		return ""
	}
	trace_util_0.Count(_binary_literal_00000, 13)
	return "0x" + hex.EncodeToString(b)
}

// ToString returns the string representation for the literal.
func (b BinaryLiteral) ToString() string {
	trace_util_0.Count(_binary_literal_00000, 15)
	return string(b)
}

// ToBitLiteralString returns the bit literal representation for the literal.
func (b BinaryLiteral) ToBitLiteralString(trimLeadingZero bool) string {
	trace_util_0.Count(_binary_literal_00000, 16)
	if len(b) == 0 {
		trace_util_0.Count(_binary_literal_00000, 20)
		return "b''"
	}
	trace_util_0.Count(_binary_literal_00000, 17)
	var buf bytes.Buffer
	for _, data := range b {
		trace_util_0.Count(_binary_literal_00000, 21)
		fmt.Fprintf(&buf, "%08b", data)
	}
	trace_util_0.Count(_binary_literal_00000, 18)
	ret := buf.Bytes()
	if trimLeadingZero {
		trace_util_0.Count(_binary_literal_00000, 22)
		ret = bytes.TrimLeft(ret, "0")
		if len(ret) == 0 {
			trace_util_0.Count(_binary_literal_00000, 23)
			ret = []byte{'0'}
		}
	}
	trace_util_0.Count(_binary_literal_00000, 19)
	return fmt.Sprintf("b'%s'", string(ret))
}

// ToInt returns the int value for the literal.
func (b BinaryLiteral) ToInt(sc *stmtctx.StatementContext) (uint64, error) {
	trace_util_0.Count(_binary_literal_00000, 24)
	buf := trimLeadingZeroBytes(b)
	length := len(buf)
	if length == 0 {
		trace_util_0.Count(_binary_literal_00000, 28)
		return 0, nil
	}
	trace_util_0.Count(_binary_literal_00000, 25)
	if length > 8 {
		trace_util_0.Count(_binary_literal_00000, 29)
		var err error = ErrTruncatedWrongVal.GenWithStackByArgs("BINARY", b)
		if sc != nil {
			trace_util_0.Count(_binary_literal_00000, 31)
			err = sc.HandleTruncate(err)
		}
		trace_util_0.Count(_binary_literal_00000, 30)
		return math.MaxUint64, err
	}
	// Note: the byte-order is BigEndian.
	trace_util_0.Count(_binary_literal_00000, 26)
	val := uint64(buf[0])
	for i := 1; i < length; i++ {
		trace_util_0.Count(_binary_literal_00000, 32)
		val = (val << 8) | uint64(buf[i])
	}
	trace_util_0.Count(_binary_literal_00000, 27)
	return val, nil
}

// Compare compares BinaryLiteral to another one
func (b BinaryLiteral) Compare(b2 BinaryLiteral) int {
	trace_util_0.Count(_binary_literal_00000, 33)
	bufB := trimLeadingZeroBytes(b)
	bufB2 := trimLeadingZeroBytes(b2)
	if len(bufB) > len(bufB2) {
		trace_util_0.Count(_binary_literal_00000, 36)
		return 1
	}
	trace_util_0.Count(_binary_literal_00000, 34)
	if len(bufB) < len(bufB2) {
		trace_util_0.Count(_binary_literal_00000, 37)
		return -1
	}
	trace_util_0.Count(_binary_literal_00000, 35)
	return bytes.Compare(bufB, bufB2)
}

// ParseBitStr parses bit string.
// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html
func ParseBitStr(s string) (BinaryLiteral, error) {
	trace_util_0.Count(_binary_literal_00000, 38)
	if len(s) == 0 {
		trace_util_0.Count(_binary_literal_00000, 43)
		return nil, errors.Errorf("invalid empty string for parsing bit type")
	}

	trace_util_0.Count(_binary_literal_00000, 39)
	if s[0] == 'b' || s[0] == 'B' {
		trace_util_0.Count(_binary_literal_00000, 44)
		// format is b'val' or B'val'
		s = strings.Trim(s[1:], "'")
	} else {
		trace_util_0.Count(_binary_literal_00000, 45)
		if strings.HasPrefix(s, "0b") {
			trace_util_0.Count(_binary_literal_00000, 46)
			s = s[2:]
		} else {
			trace_util_0.Count(_binary_literal_00000, 47)
			{
				// here means format is not b'val', B'val' or 0bval.
				return nil, errors.Errorf("invalid bit type format %s", s)
			}
		}
	}

	trace_util_0.Count(_binary_literal_00000, 40)
	if len(s) == 0 {
		trace_util_0.Count(_binary_literal_00000, 48)
		return ZeroBinaryLiteral, nil
	}

	trace_util_0.Count(_binary_literal_00000, 41)
	alignedLength := (len(s) + 7) &^ 7
	s = ("00000000" + s)[len(s)+8-alignedLength:] // Pad with zero (slice from `-alignedLength`)
	byteLength := len(s) >> 3
	buf := make([]byte, byteLength)

	for i := 0; i < byteLength; i++ {
		trace_util_0.Count(_binary_literal_00000, 49)
		strPosition := i << 3
		val, err := strconv.ParseUint(s[strPosition:strPosition+8], 2, 8)
		if err != nil {
			trace_util_0.Count(_binary_literal_00000, 51)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_binary_literal_00000, 50)
		buf[i] = byte(val)
	}

	trace_util_0.Count(_binary_literal_00000, 42)
	return buf, nil
}

// NewBitLiteral parses bit string as BitLiteral type.
func NewBitLiteral(s string) (BitLiteral, error) {
	trace_util_0.Count(_binary_literal_00000, 52)
	b, err := ParseBitStr(s)
	if err != nil {
		trace_util_0.Count(_binary_literal_00000, 54)
		return BitLiteral{}, err
	}
	trace_util_0.Count(_binary_literal_00000, 53)
	return BitLiteral(b), nil
}

// ParseHexStr parses hexadecimal string literal.
// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func ParseHexStr(s string) (BinaryLiteral, error) {
	trace_util_0.Count(_binary_literal_00000, 55)
	if len(s) == 0 {
		trace_util_0.Count(_binary_literal_00000, 61)
		return nil, errors.Errorf("invalid empty string for parsing hexadecimal literal")
	}

	trace_util_0.Count(_binary_literal_00000, 56)
	if s[0] == 'x' || s[0] == 'X' {
		trace_util_0.Count(_binary_literal_00000, 62)
		// format is x'val' or X'val'
		s = strings.Trim(s[1:], "'")
		if len(s)%2 != 0 {
			trace_util_0.Count(_binary_literal_00000, 63)
			return nil, errors.Errorf("invalid hexadecimal format, must even numbers, but %d", len(s))
		}
	} else {
		trace_util_0.Count(_binary_literal_00000, 64)
		if strings.HasPrefix(s, "0x") {
			trace_util_0.Count(_binary_literal_00000, 65)
			s = s[2:]
		} else {
			trace_util_0.Count(_binary_literal_00000, 66)
			{
				// here means format is not x'val', X'val' or 0xval.
				return nil, errors.Errorf("invalid hexadecimal format %s", s)
			}
		}
	}

	trace_util_0.Count(_binary_literal_00000, 57)
	if len(s) == 0 {
		trace_util_0.Count(_binary_literal_00000, 67)
		return ZeroBinaryLiteral, nil
	}

	trace_util_0.Count(_binary_literal_00000, 58)
	if len(s)%2 != 0 {
		trace_util_0.Count(_binary_literal_00000, 68)
		s = "0" + s
	}
	trace_util_0.Count(_binary_literal_00000, 59)
	buf, err := hex.DecodeString(s)
	if err != nil {
		trace_util_0.Count(_binary_literal_00000, 69)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_binary_literal_00000, 60)
	return buf, nil
}

// NewHexLiteral parses hexadecimal string as HexLiteral type.
func NewHexLiteral(s string) (HexLiteral, error) {
	trace_util_0.Count(_binary_literal_00000, 70)
	h, err := ParseHexStr(s)
	if err != nil {
		trace_util_0.Count(_binary_literal_00000, 72)
		return HexLiteral{}, err
	}
	trace_util_0.Count(_binary_literal_00000, 71)
	return HexLiteral(h), nil
}

var _binary_literal_00000 = "types/binary_literal.go"
