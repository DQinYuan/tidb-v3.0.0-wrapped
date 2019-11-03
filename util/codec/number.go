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

package codec

import (
	"encoding/binary"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

const signMask uint64 = 0x8000000000000000

// EncodeIntToCmpUint make int v to comparable uint type
func EncodeIntToCmpUint(v int64) uint64 {
	trace_util_0.Count(_number_00000, 0)
	return uint64(v) ^ signMask
}

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
func DecodeCmpUintToInt(u uint64) int64 {
	trace_util_0.Count(_number_00000, 1)
	return int64(u ^ signMask)
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	trace_util_0.Count(_number_00000, 2)
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
// EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
func EncodeIntDesc(b []byte, v int64) []byte {
	trace_util_0.Count(_number_00000, 3)
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], ^u)
	return append(b, data[:]...)
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	trace_util_0.Count(_number_00000, 4)
	if len(b) < 8 {
		trace_util_0.Count(_number_00000, 6)
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	trace_util_0.Count(_number_00000, 5)
	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

// DecodeIntDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeIntDesc(b []byte) ([]byte, int64, error) {
	trace_util_0.Count(_number_00000, 7)
	if len(b) < 8 {
		trace_util_0.Count(_number_00000, 9)
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	trace_util_0.Count(_number_00000, 8)
	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(^u)
	b = b[8:]
	return b, v, nil
}

// EncodeUint appends the encoded value to slice b and returns the appended slice.
// EncodeUint guarantees that the encoded value is in ascending order for comparison.
func EncodeUint(b []byte, v uint64) []byte {
	trace_util_0.Count(_number_00000, 10)
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], v)
	return append(b, data[:]...)
}

// EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
// EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
func EncodeUintDesc(b []byte, v uint64) []byte {
	trace_util_0.Count(_number_00000, 11)
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], ^v)
	return append(b, data[:]...)
}

// DecodeUint decodes value encoded by EncodeUint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUint(b []byte) ([]byte, uint64, error) {
	trace_util_0.Count(_number_00000, 12)
	if len(b) < 8 {
		trace_util_0.Count(_number_00000, 14)
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	trace_util_0.Count(_number_00000, 13)
	v := binary.BigEndian.Uint64(b[:8])
	b = b[8:]
	return b, v, nil
}

// DecodeUintDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	trace_util_0.Count(_number_00000, 15)
	if len(b) < 8 {
		trace_util_0.Count(_number_00000, 17)
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	trace_util_0.Count(_number_00000, 16)
	data := b[:8]
	v := binary.BigEndian.Uint64(data)
	b = b[8:]
	return b, ^v, nil
}

// EncodeVarint appends the encoded value to slice b and returns the appended slice.
// Note that the encoded result is not memcomparable.
func EncodeVarint(b []byte, v int64) []byte {
	trace_util_0.Count(_number_00000, 18)
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeVarint decodes value encoded by EncodeVarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeVarint(b []byte) ([]byte, int64, error) {
	trace_util_0.Count(_number_00000, 19)
	v, n := binary.Varint(b)
	if n > 0 {
		trace_util_0.Count(_number_00000, 22)
		return b[n:], v, nil
	}
	trace_util_0.Count(_number_00000, 20)
	if n < 0 {
		trace_util_0.Count(_number_00000, 23)
		return nil, 0, errors.New("value larger than 64 bits")
	}
	trace_util_0.Count(_number_00000, 21)
	return nil, 0, errors.New("insufficient bytes to decode value")
}

// EncodeUvarint appends the encoded value to slice b and returns the appended slice.
// Note that the encoded result is not memcomparable.
func EncodeUvarint(b []byte, v uint64) []byte {
	trace_util_0.Count(_number_00000, 24)
	var data [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeUvarint decodes value encoded by EncodeUvarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUvarint(b []byte) ([]byte, uint64, error) {
	trace_util_0.Count(_number_00000, 25)
	v, n := binary.Uvarint(b)
	if n > 0 {
		trace_util_0.Count(_number_00000, 28)
		return b[n:], v, nil
	}
	trace_util_0.Count(_number_00000, 26)
	if n < 0 {
		trace_util_0.Count(_number_00000, 29)
		return nil, 0, errors.New("value larger than 64 bits")
	}
	trace_util_0.Count(_number_00000, 27)
	return nil, 0, errors.New("insufficient bytes to decode value")
}

const (
	negativeTagEnd   = 8        // negative tag is (negativeTagEnd - length).
	positiveTagStart = 0xff - 8 // Positive tag is (positiveTagStart + length).
)

// EncodeComparableVarint encodes an int64 to a mem-comparable bytes.
func EncodeComparableVarint(b []byte, v int64) []byte {
	trace_util_0.Count(_number_00000, 30)
	if v < 0 {
		trace_util_0.Count(_number_00000, 32)
		// All negative value has a tag byte prefix (negativeTagEnd - length).
		// Smaller negative value encodes to more bytes, has smaller tag.
		if v >= -0xff {
			trace_util_0.Count(_number_00000, 34)
			return append(b, negativeTagEnd-1, byte(v))
		} else {
			trace_util_0.Count(_number_00000, 35)
			if v >= -0xffff {
				trace_util_0.Count(_number_00000, 36)
				return append(b, negativeTagEnd-2, byte(v>>8), byte(v))
			} else {
				trace_util_0.Count(_number_00000, 37)
				if v >= -0xffffff {
					trace_util_0.Count(_number_00000, 38)
					return append(b, negativeTagEnd-3, byte(v>>16), byte(v>>8), byte(v))
				} else {
					trace_util_0.Count(_number_00000, 39)
					if v >= -0xffffffff {
						trace_util_0.Count(_number_00000, 40)
						return append(b, negativeTagEnd-4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
					} else {
						trace_util_0.Count(_number_00000, 41)
						if v >= -0xffffffffff {
							trace_util_0.Count(_number_00000, 42)
							return append(b, negativeTagEnd-5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
						} else {
							trace_util_0.Count(_number_00000, 43)
							if v >= -0xffffffffffff {
								trace_util_0.Count(_number_00000, 44)
								return append(b, negativeTagEnd-6, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
									byte(v))
							} else {
								trace_util_0.Count(_number_00000, 45)
								if v >= -0xffffffffffffff {
									trace_util_0.Count(_number_00000, 46)
									return append(b, negativeTagEnd-7, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
										byte(v>>8), byte(v))
								}
							}
						}
					}
				}
			}
		}
		trace_util_0.Count(_number_00000, 33)
		return append(b, negativeTagEnd-8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	}
	trace_util_0.Count(_number_00000, 31)
	return EncodeComparableUvarint(b, uint64(v))
}

// EncodeComparableUvarint encodes uint64 into mem-comparable bytes.
func EncodeComparableUvarint(b []byte, v uint64) []byte {
	trace_util_0.Count(_number_00000, 47)
	// The first byte has 256 values, [0, 7] is reserved for negative tags,
	// [248, 255] is reserved for larger positive tags,
	// So we can store value [0, 239] in a single byte.
	// Values cannot be stored in single byte has a tag byte prefix (positiveTagStart+length).
	// Larger value encodes to more bytes, has larger tag.
	if v <= positiveTagStart-negativeTagEnd {
		trace_util_0.Count(_number_00000, 49)
		return append(b, byte(v)+negativeTagEnd)
	} else {
		trace_util_0.Count(_number_00000, 50)
		if v <= 0xff {
			trace_util_0.Count(_number_00000, 51)
			return append(b, positiveTagStart+1, byte(v))
		} else {
			trace_util_0.Count(_number_00000, 52)
			if v <= 0xffff {
				trace_util_0.Count(_number_00000, 53)
				return append(b, positiveTagStart+2, byte(v>>8), byte(v))
			} else {
				trace_util_0.Count(_number_00000, 54)
				if v <= 0xffffff {
					trace_util_0.Count(_number_00000, 55)
					return append(b, positiveTagStart+3, byte(v>>16), byte(v>>8), byte(v))
				} else {
					trace_util_0.Count(_number_00000, 56)
					if v <= 0xffffffff {
						trace_util_0.Count(_number_00000, 57)
						return append(b, positiveTagStart+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
					} else {
						trace_util_0.Count(_number_00000, 58)
						if v <= 0xffffffffff {
							trace_util_0.Count(_number_00000, 59)
							return append(b, positiveTagStart+5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
						} else {
							trace_util_0.Count(_number_00000, 60)
							if v <= 0xffffffffffff {
								trace_util_0.Count(_number_00000, 61)
								return append(b, positiveTagStart+6, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
									byte(v))
							} else {
								trace_util_0.Count(_number_00000, 62)
								if v <= 0xffffffffffffff {
									trace_util_0.Count(_number_00000, 63)
									return append(b, positiveTagStart+7, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
										byte(v>>8), byte(v))
								}
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_number_00000, 48)
	return append(b, positiveTagStart+8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
		byte(v>>16), byte(v>>8), byte(v))
}

var (
	errDecodeInsufficient = errors.New("insufficient bytes to decode value")
	errDecodeInvalid      = errors.New("invalid bytes to decode value")
)

// DecodeComparableUvarint decodes mem-comparable uvarint.
func DecodeComparableUvarint(b []byte) ([]byte, uint64, error) {
	trace_util_0.Count(_number_00000, 64)
	if len(b) == 0 {
		trace_util_0.Count(_number_00000, 70)
		return nil, 0, errDecodeInsufficient
	}
	trace_util_0.Count(_number_00000, 65)
	first := b[0]
	b = b[1:]
	if first < negativeTagEnd {
		trace_util_0.Count(_number_00000, 71)
		return nil, 0, errors.Trace(errDecodeInvalid)
	}
	trace_util_0.Count(_number_00000, 66)
	if first <= positiveTagStart {
		trace_util_0.Count(_number_00000, 72)
		return b, uint64(first) - negativeTagEnd, nil
	}
	trace_util_0.Count(_number_00000, 67)
	length := int(first) - positiveTagStart
	if len(b) < length {
		trace_util_0.Count(_number_00000, 73)
		return nil, 0, errors.Trace(errDecodeInsufficient)
	}
	trace_util_0.Count(_number_00000, 68)
	var v uint64
	for _, c := range b[:length] {
		trace_util_0.Count(_number_00000, 74)
		v = (v << 8) | uint64(c)
	}
	trace_util_0.Count(_number_00000, 69)
	return b[length:], v, nil
}

// DecodeComparableVarint decodes mem-comparable varint.
func DecodeComparableVarint(b []byte) ([]byte, int64, error) {
	trace_util_0.Count(_number_00000, 75)
	if len(b) == 0 {
		trace_util_0.Count(_number_00000, 82)
		return nil, 0, errors.Trace(errDecodeInsufficient)
	}
	trace_util_0.Count(_number_00000, 76)
	first := b[0]
	if first >= negativeTagEnd && first <= positiveTagStart {
		trace_util_0.Count(_number_00000, 83)
		return b, int64(first) - negativeTagEnd, nil
	}
	trace_util_0.Count(_number_00000, 77)
	b = b[1:]
	var length int
	var v uint64
	if first < negativeTagEnd {
		trace_util_0.Count(_number_00000, 84)
		length = negativeTagEnd - int(first)
		v = math.MaxUint64 // negative value has all bits on by default.
	} else {
		trace_util_0.Count(_number_00000, 85)
		{
			length = int(first) - positiveTagStart
		}
	}
	trace_util_0.Count(_number_00000, 78)
	if len(b) < length {
		trace_util_0.Count(_number_00000, 86)
		return nil, 0, errors.Trace(errDecodeInsufficient)
	}
	trace_util_0.Count(_number_00000, 79)
	for _, c := range b[:length] {
		trace_util_0.Count(_number_00000, 87)
		v = (v << 8) | uint64(c)
	}
	trace_util_0.Count(_number_00000, 80)
	if first > positiveTagStart && v > math.MaxInt64 {
		trace_util_0.Count(_number_00000, 88)
		return nil, 0, errors.Trace(errDecodeInvalid)
	} else {
		trace_util_0.Count(_number_00000, 89)
		if first < negativeTagEnd && v <= math.MaxInt64 {
			trace_util_0.Count(_number_00000, 90)
			return nil, 0, errors.Trace(errDecodeInvalid)
		}
	}
	trace_util_0.Count(_number_00000, 81)
	return b[length:], int64(v), nil
}

var _number_00000 = "util/codec/number.go"
