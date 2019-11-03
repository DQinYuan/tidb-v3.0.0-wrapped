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
	"runtime"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var (
	pads    = make([]byte, encGroupSize)
	encPads = []byte{encPad}
)

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes slice which is padding with 0.
//  marker is `0xFF - padding 0 count`
// For example:
//   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func EncodeBytes(b []byte, data []byte) []byte {
	trace_util_0.Count(_bytes_00000, 0)
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	reallocSize := (dLen/encGroupSize + 1) * (encGroupSize + 1)
	result := reallocBytes(b, reallocSize)
	for idx := 0; idx <= dLen; idx += encGroupSize {
		trace_util_0.Count(_bytes_00000, 2)
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			trace_util_0.Count(_bytes_00000, 4)
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			trace_util_0.Count(_bytes_00000, 5)
			{
				padCount = encGroupSize - remain
				result = append(result, data[idx:]...)
				result = append(result, pads[:padCount]...)
			}
		}

		trace_util_0.Count(_bytes_00000, 3)
		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}

	trace_util_0.Count(_bytes_00000, 1)
	return result
}

func decodeBytes(b []byte, buf []byte, reverse bool) ([]byte, []byte, error) {
	trace_util_0.Count(_bytes_00000, 6)
	if buf == nil {
		trace_util_0.Count(_bytes_00000, 10)
		buf = make([]byte, 0, len(b))
	}
	trace_util_0.Count(_bytes_00000, 7)
	buf = buf[:0]
	for {
		trace_util_0.Count(_bytes_00000, 11)
		if len(b) < encGroupSize+1 {
			trace_util_0.Count(_bytes_00000, 15)
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		trace_util_0.Count(_bytes_00000, 12)
		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		var padCount byte
		if reverse {
			trace_util_0.Count(_bytes_00000, 16)
			padCount = marker
		} else {
			trace_util_0.Count(_bytes_00000, 17)
			{
				padCount = encMarker - marker
			}
		}
		trace_util_0.Count(_bytes_00000, 13)
		if padCount > encGroupSize {
			trace_util_0.Count(_bytes_00000, 18)
			return nil, nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		trace_util_0.Count(_bytes_00000, 14)
		realGroupSize := encGroupSize - padCount
		buf = append(buf, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			trace_util_0.Count(_bytes_00000, 19)
			var padByte = encPad
			if reverse {
				trace_util_0.Count(_bytes_00000, 22)
				padByte = encMarker
			}
			// Check validity of padding bytes.
			trace_util_0.Count(_bytes_00000, 20)
			for _, v := range group[realGroupSize:] {
				trace_util_0.Count(_bytes_00000, 23)
				if v != padByte {
					trace_util_0.Count(_bytes_00000, 24)
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			trace_util_0.Count(_bytes_00000, 21)
			break
		}
	}
	trace_util_0.Count(_bytes_00000, 8)
	if reverse {
		trace_util_0.Count(_bytes_00000, 25)
		reverseBytes(buf)
	}
	trace_util_0.Count(_bytes_00000, 9)
	return b, buf, nil
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
// `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	trace_util_0.Count(_bytes_00000, 26)
	return decodeBytes(b, buf, false)
}

// EncodeBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
// encoded value to guarantee the encoded value is in descending order for comparison.
func EncodeBytesDesc(b []byte, data []byte) []byte {
	trace_util_0.Count(_bytes_00000, 27)
	n := len(b)
	b = EncodeBytes(b, data)
	reverseBytes(b[n:])
	return b
}

// DecodeBytesDesc decodes bytes which is encoded by EncodeBytesDesc before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytesDesc(b []byte, buf []byte) ([]byte, []byte, error) {
	trace_util_0.Count(_bytes_00000, 28)
	return decodeBytes(b, buf, true)
}

// EncodeCompactBytes joins bytes with its length into a byte slice. It is more
// efficient in both space and time compare to EncodeBytes. Note that the encoded
// result is not memcomparable.
func EncodeCompactBytes(b []byte, data []byte) []byte {
	trace_util_0.Count(_bytes_00000, 29)
	b = reallocBytes(b, binary.MaxVarintLen64+len(data))
	b = EncodeVarint(b, int64(len(data)))
	return append(b, data...)
}

// DecodeCompactBytes decodes bytes which is encoded by EncodeCompactBytes before.
func DecodeCompactBytes(b []byte) ([]byte, []byte, error) {
	trace_util_0.Count(_bytes_00000, 30)
	b, n, err := DecodeVarint(b)
	if err != nil {
		trace_util_0.Count(_bytes_00000, 33)
		return nil, nil, errors.Trace(err)
	}
	trace_util_0.Count(_bytes_00000, 31)
	if int64(len(b)) < n {
		trace_util_0.Count(_bytes_00000, 34)
		return nil, nil, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	trace_util_0.Count(_bytes_00000, 32)
	return b[n:], b[:n], nil
}

// See https://golang.org/src/crypto/cipher/xor.go
const wordSize = int(unsafe.Sizeof(uintptr(0)))
const supportsUnaligned = runtime.GOARCH == "386" || runtime.GOARCH == "amd64"

func fastReverseBytes(b []byte) {
	trace_util_0.Count(_bytes_00000, 35)
	n := len(b)
	w := n / wordSize
	if w > 0 {
		trace_util_0.Count(_bytes_00000, 37)
		bw := *(*[]uintptr)(unsafe.Pointer(&b))
		for i := 0; i < w; i++ {
			trace_util_0.Count(_bytes_00000, 38)
			bw[i] = ^bw[i]
		}
	}

	trace_util_0.Count(_bytes_00000, 36)
	for i := w * wordSize; i < n; i++ {
		trace_util_0.Count(_bytes_00000, 39)
		b[i] = ^b[i]
	}
}

func safeReverseBytes(b []byte) {
	trace_util_0.Count(_bytes_00000, 40)
	for i := range b {
		trace_util_0.Count(_bytes_00000, 41)
		b[i] = ^b[i]
	}
}

func reverseBytes(b []byte) {
	trace_util_0.Count(_bytes_00000, 42)
	if supportsUnaligned {
		trace_util_0.Count(_bytes_00000, 44)
		fastReverseBytes(b)
		return
	}

	trace_util_0.Count(_bytes_00000, 43)
	safeReverseBytes(b)
}

// reallocBytes is like realloc.
func reallocBytes(b []byte, n int) []byte {
	trace_util_0.Count(_bytes_00000, 45)
	newSize := len(b) + n
	if cap(b) < newSize {
		trace_util_0.Count(_bytes_00000, 47)
		bs := make([]byte, len(b), newSize)
		copy(bs, b)
		return bs
	}

	// slice b has capability to store n bytes
	trace_util_0.Count(_bytes_00000, 46)
	return b
}

var _bytes_00000 = "util/codec/bytes.go"
