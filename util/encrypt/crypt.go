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

package encrypt

import "github.com/pingcap/tidb/trace_util_0"

type randStruct struct {
	seed1       uint32
	seed2       uint32
	maxValue    uint32
	maxValueDbl float64
}

// randomInit random generation structure initialization
func (rs *randStruct) randomInit(password []byte, length int) {
	trace_util_0.Count(_crypt_00000, 0)
	// Generate binary hash from raw text string
	var nr, add, nr2, tmp uint32
	nr = 1345345333
	add = 7
	nr2 = 0x12345671

	for i := 0; i < length; i++ {
		trace_util_0.Count(_crypt_00000, 2)
		pswChar := password[i]
		if pswChar == ' ' || pswChar == '\t' {
			trace_util_0.Count(_crypt_00000, 4)
			continue
		}
		trace_util_0.Count(_crypt_00000, 3)
		tmp = uint32(pswChar)
		nr ^= (((nr & 63) + add) * tmp) + (nr << 8)
		nr2 += (nr2 << 8) ^ nr
		add += tmp
	}

	trace_util_0.Count(_crypt_00000, 1)
	seed1 := nr & ((uint32(1) << 31) - uint32(1))
	seed2 := nr2 & ((uint32(1) << 31) - uint32(1))

	//  New (MySQL 3.21+) random generation structure initialization
	rs.maxValue = 0x3FFFFFFF
	rs.maxValueDbl = float64(rs.maxValue)
	rs.seed1 = seed1 % rs.maxValue
	rs.seed2 = seed2 % rs.maxValue
}

func (rs *randStruct) myRand() float64 {
	trace_util_0.Count(_crypt_00000, 5)
	rs.seed1 = (rs.seed1*3 + rs.seed2) % rs.maxValue
	rs.seed2 = (rs.seed1 + rs.seed2 + 33) % rs.maxValue

	return ((float64(rs.seed1)) / rs.maxValueDbl)
}

// sqlCrypt use to store initialization results
type sqlCrypt struct {
	rand    randStruct
	orgRand randStruct

	decodeBuff [256]byte
	encodeBuff [256]byte
	shift      uint32
}

func (sc *sqlCrypt) init(password []byte, length int) {
	trace_util_0.Count(_crypt_00000, 6)
	sc.rand.randomInit(password, length)

	for i := 0; i <= 255; i++ {
		trace_util_0.Count(_crypt_00000, 10)
		sc.decodeBuff[i] = byte(i)
	}

	trace_util_0.Count(_crypt_00000, 7)
	for i := 0; i <= 255; i++ {
		trace_util_0.Count(_crypt_00000, 11)
		idx := uint32(sc.rand.myRand() * 255.0)
		a := sc.decodeBuff[idx]
		sc.decodeBuff[idx] = sc.decodeBuff[i]
		sc.decodeBuff[i] = a
	}

	trace_util_0.Count(_crypt_00000, 8)
	for i := 0; i <= 255; i++ {
		trace_util_0.Count(_crypt_00000, 12)
		sc.encodeBuff[sc.decodeBuff[i]] = byte(i)
	}

	trace_util_0.Count(_crypt_00000, 9)
	sc.orgRand = sc.rand
	sc.shift = 0
}

func (sc *sqlCrypt) reinit() {
	trace_util_0.Count(_crypt_00000, 13)
	sc.shift = 0
	sc.rand = sc.orgRand
}

func (sc *sqlCrypt) encode(str []byte, length int) {
	trace_util_0.Count(_crypt_00000, 14)
	for i := 0; i < length; i++ {
		trace_util_0.Count(_crypt_00000, 15)
		sc.shift ^= uint32(sc.rand.myRand() * 255.0)
		idx := uint32(str[i])
		str[i] = sc.encodeBuff[idx] ^ byte(sc.shift)
		sc.shift ^= idx
	}
}

func (sc *sqlCrypt) decode(str []byte, length int) {
	trace_util_0.Count(_crypt_00000, 16)
	for i := 0; i < length; i++ {
		trace_util_0.Count(_crypt_00000, 17)
		sc.shift ^= uint32(sc.rand.myRand() * 255.0)
		idx := uint32(str[i] ^ byte(sc.shift))
		str[i] = sc.decodeBuff[idx]
		sc.shift ^= uint32(str[i])
	}
}

//SQLDecode Function to handle the decode() function
func SQLDecode(str string, password string) (string, error) {
	trace_util_0.Count(_crypt_00000, 18)
	var sc sqlCrypt

	strByte := []byte(str)
	passwdByte := []byte(password)

	sc.init(passwdByte, len(passwdByte))
	sc.decode(strByte, len(strByte))

	return string(strByte), nil
}

// SQLEncode Function to handle the encode() function
func SQLEncode(cryptStr string, password string) (string, error) {
	trace_util_0.Count(_crypt_00000, 19)
	var sc sqlCrypt

	cryptStrByte := []byte(cryptStr)
	passwdByte := []byte(password)

	sc.init(passwdByte, len(passwdByte))
	sc.encode(cryptStrByte, len(cryptStrByte))

	return string(cryptStrByte), nil
}

var _crypt_00000 = "util/encrypt/crypt.go"
