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

package json

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"unicode/utf8"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/hack"
)

// Type returns type of BinaryJSON as string.
func (bj BinaryJSON) Type() string {
	trace_util_0.Count(_binary_functions_00000, 0)
	switch bj.TypeCode {
	case TypeCodeObject:
		trace_util_0.Count(_binary_functions_00000, 1)
		return "OBJECT"
	case TypeCodeArray:
		trace_util_0.Count(_binary_functions_00000, 2)
		return "ARRAY"
	case TypeCodeLiteral:
		trace_util_0.Count(_binary_functions_00000, 3)
		switch bj.Value[0] {
		case LiteralNil:
			trace_util_0.Count(_binary_functions_00000, 9)
			return "NULL"
		default:
			trace_util_0.Count(_binary_functions_00000, 10)
			return "BOOLEAN"
		}
	case TypeCodeInt64:
		trace_util_0.Count(_binary_functions_00000, 4)
		return "INTEGER"
	case TypeCodeUint64:
		trace_util_0.Count(_binary_functions_00000, 5)
		return "UNSIGNED INTEGER"
	case TypeCodeFloat64:
		trace_util_0.Count(_binary_functions_00000, 6)
		return "DOUBLE"
	case TypeCodeString:
		trace_util_0.Count(_binary_functions_00000, 7)
		return "STRING"
	default:
		trace_util_0.Count(_binary_functions_00000, 8)
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, bj.TypeCode)
		panic(msg)
	}
}

// Quote is for JSON_QUOTE
func (bj BinaryJSON) Quote() string {
	trace_util_0.Count(_binary_functions_00000, 11)
	str := hack.String(bj.GetString())
	return strconv.Quote(string(str))
}

// Unquote is for JSON_UNQUOTE.
func (bj BinaryJSON) Unquote() (string, error) {
	trace_util_0.Count(_binary_functions_00000, 12)
	switch bj.TypeCode {
	case TypeCodeString:
		trace_util_0.Count(_binary_functions_00000, 13)
		tmp := string(hack.String(bj.GetString()))
		s, err := unquoteString(tmp)
		if err != nil {
			trace_util_0.Count(_binary_functions_00000, 17)
			return "", errors.Trace(err)
		}
		// Remove prefix and suffix '"'.
		trace_util_0.Count(_binary_functions_00000, 14)
		slen := len(s)
		if slen > 1 {
			trace_util_0.Count(_binary_functions_00000, 18)
			head, tail := s[0], s[slen-1]
			if head == '"' && tail == '"' {
				trace_util_0.Count(_binary_functions_00000, 19)
				return s[1 : slen-1], nil
			}
		}
		trace_util_0.Count(_binary_functions_00000, 15)
		return s, nil
	default:
		trace_util_0.Count(_binary_functions_00000, 16)
		return bj.String(), nil
	}
}

// unquoteString recognizes the escape sequences shown in:
// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#json-unquote-character-escape-sequences
func unquoteString(s string) (string, error) {
	trace_util_0.Count(_binary_functions_00000, 20)
	ret := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		trace_util_0.Count(_binary_functions_00000, 22)
		if s[i] == '\\' {
			trace_util_0.Count(_binary_functions_00000, 23)
			i++
			if i == len(s) {
				trace_util_0.Count(_binary_functions_00000, 25)
				return "", errors.New("Missing a closing quotation mark in string")
			}
			trace_util_0.Count(_binary_functions_00000, 24)
			switch s[i] {
			case '"':
				trace_util_0.Count(_binary_functions_00000, 26)
				ret.WriteByte('"')
			case 'b':
				trace_util_0.Count(_binary_functions_00000, 27)
				ret.WriteByte('\b')
			case 'f':
				trace_util_0.Count(_binary_functions_00000, 28)
				ret.WriteByte('\f')
			case 'n':
				trace_util_0.Count(_binary_functions_00000, 29)
				ret.WriteByte('\n')
			case 'r':
				trace_util_0.Count(_binary_functions_00000, 30)
				ret.WriteByte('\r')
			case 't':
				trace_util_0.Count(_binary_functions_00000, 31)
				ret.WriteByte('\t')
			case '\\':
				trace_util_0.Count(_binary_functions_00000, 32)
				ret.WriteByte('\\')
			case 'u':
				trace_util_0.Count(_binary_functions_00000, 33)
				if i+4 > len(s) {
					trace_util_0.Count(_binary_functions_00000, 37)
					return "", errors.Errorf("Invalid unicode: %s", s[i+1:])
				}
				trace_util_0.Count(_binary_functions_00000, 34)
				char, size, err := decodeEscapedUnicode(hack.Slice(s[i+1 : i+5]))
				if err != nil {
					trace_util_0.Count(_binary_functions_00000, 38)
					return "", errors.Trace(err)
				}
				trace_util_0.Count(_binary_functions_00000, 35)
				ret.Write(char[0:size])
				i += 4
			default:
				trace_util_0.Count(_binary_functions_00000, 36)
				// For all other escape sequences, backslash is ignored.
				ret.WriteByte(s[i])
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 39)
			{
				ret.WriteByte(s[i])
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 21)
	return ret.String(), nil
}

// decodeEscapedUnicode decodes unicode into utf8 bytes specified in RFC 3629.
// According RFC 3629, the max length of utf8 characters is 4 bytes.
// And MySQL use 4 bytes to represent the unicode which must be in [0, 65536).
func decodeEscapedUnicode(s []byte) (char [4]byte, size int, err error) {
	trace_util_0.Count(_binary_functions_00000, 40)
	size, err = hex.Decode(char[0:2], s)
	if err != nil || size != 2 {
		trace_util_0.Count(_binary_functions_00000, 43)
		// The unicode must can be represented in 2 bytes.
		return char, 0, errors.Trace(err)
	}
	trace_util_0.Count(_binary_functions_00000, 41)
	var unicode uint16
	err = binary.Read(bytes.NewReader(char[0:2]), binary.BigEndian, &unicode)
	if err != nil {
		trace_util_0.Count(_binary_functions_00000, 44)
		return char, 0, errors.Trace(err)
	}
	trace_util_0.Count(_binary_functions_00000, 42)
	size = utf8.RuneLen(rune(unicode))
	utf8.EncodeRune(char[0:size], rune(unicode))
	return
}

// quoteString escapes interior quote and other characters for JSON_QUOTE
// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-quote
// TODO: add JSON_QUOTE builtin
func quoteString(s string) string {
	trace_util_0.Count(_binary_functions_00000, 45)
	var escapeByteMap = map[byte]string{
		'\\': "\\\\",
		'"':  "\\\"",
		'\b': "\\b",
		'\f': "\\f",
		'\n': "\\n",
		'\r': "\\r",
		'\t': "\\t",
	}

	ret := new(bytes.Buffer)
	ret.WriteByte('"')

	start := 0
	hasEscaped := false

	for i := 0; i < len(s); {
		trace_util_0.Count(_binary_functions_00000, 49)
		if b := s[i]; b < utf8.RuneSelf {
			trace_util_0.Count(_binary_functions_00000, 50)
			escaped, ok := escapeByteMap[b]
			if ok {
				trace_util_0.Count(_binary_functions_00000, 51)
				if start < i {
					trace_util_0.Count(_binary_functions_00000, 53)
					ret.WriteString(s[start:i])
				}
				trace_util_0.Count(_binary_functions_00000, 52)
				hasEscaped = true
				ret.WriteString(escaped)
				i++
				start = i
			} else {
				trace_util_0.Count(_binary_functions_00000, 54)
				{
					i++
				}
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 55)
			{
				c, size := utf8.DecodeRune([]byte(s[i:]))
				if c == utf8.RuneError && size == 1 {
					trace_util_0.Count(_binary_functions_00000, 57) // refer to codes of `binary.marshalStringTo`
					if start < i {
						trace_util_0.Count(_binary_functions_00000, 59)
						ret.WriteString(s[start:i])
					}
					trace_util_0.Count(_binary_functions_00000, 58)
					hasEscaped = true
					ret.WriteString(`\ufffd`)
					i += size
					start = i
					continue
				}
				trace_util_0.Count(_binary_functions_00000, 56)
				i += size
			}
		}
	}

	trace_util_0.Count(_binary_functions_00000, 46)
	if start < len(s) {
		trace_util_0.Count(_binary_functions_00000, 60)
		ret.WriteString(s[start:])
	}

	trace_util_0.Count(_binary_functions_00000, 47)
	if hasEscaped {
		trace_util_0.Count(_binary_functions_00000, 61)
		ret.WriteByte('"')
		return ret.String()
	}
	trace_util_0.Count(_binary_functions_00000, 48)
	return ret.String()[1:]
}

// Extract receives several path expressions as arguments, matches them in bj, and returns:
//  ret: target JSON matched any path expressions. maybe autowrapped as an array.
//  found: true if any path expressions matched.
func (bj BinaryJSON) Extract(pathExprList []PathExpression) (ret BinaryJSON, found bool) {
	trace_util_0.Count(_binary_functions_00000, 62)
	buf := make([]BinaryJSON, 0, 1)
	for _, pathExpr := range pathExprList {
		trace_util_0.Count(_binary_functions_00000, 65)
		buf = bj.extractTo(buf, pathExpr)
	}
	trace_util_0.Count(_binary_functions_00000, 63)
	if len(buf) == 0 {
		trace_util_0.Count(_binary_functions_00000, 66)
		found = false
	} else {
		trace_util_0.Count(_binary_functions_00000, 67)
		if len(pathExprList) == 1 && len(buf) == 1 {
			trace_util_0.Count(_binary_functions_00000, 68)
			// If pathExpr contains asterisks, len(elemList) won't be 1
			// even if len(pathExprList) equals to 1.
			found = true
			ret = buf[0]
		} else {
			trace_util_0.Count(_binary_functions_00000, 69)
			{
				found = true
				ret = buildBinaryArray(buf)
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 64)
	return
}

func (bj BinaryJSON) extractTo(buf []BinaryJSON, pathExpr PathExpression) []BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 70)
	if len(pathExpr.legs) == 0 {
		trace_util_0.Count(_binary_functions_00000, 73)
		return append(buf, bj)
	}
	trace_util_0.Count(_binary_functions_00000, 71)
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == pathLegIndex {
		trace_util_0.Count(_binary_functions_00000, 74)
		if bj.TypeCode != TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 76)
			if currentLeg.arrayIndex <= 0 && currentLeg.arrayIndex != arrayIndexAsterisk {
				trace_util_0.Count(_binary_functions_00000, 78)
				buf = bj.extractTo(buf, subPathExpr)
			}
			trace_util_0.Count(_binary_functions_00000, 77)
			return buf
		}
		trace_util_0.Count(_binary_functions_00000, 75)
		elemCount := bj.GetElemCount()
		if currentLeg.arrayIndex == arrayIndexAsterisk {
			trace_util_0.Count(_binary_functions_00000, 79)
			for i := 0; i < elemCount; i++ {
				trace_util_0.Count(_binary_functions_00000, 80)
				buf = bj.arrayGetElem(i).extractTo(buf, subPathExpr)
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 81)
			if currentLeg.arrayIndex < elemCount {
				trace_util_0.Count(_binary_functions_00000, 82)
				buf = bj.arrayGetElem(currentLeg.arrayIndex).extractTo(buf, subPathExpr)
			}
		}
	} else {
		trace_util_0.Count(_binary_functions_00000, 83)
		if currentLeg.typ == pathLegKey && bj.TypeCode == TypeCodeObject {
			trace_util_0.Count(_binary_functions_00000, 84)
			elemCount := bj.GetElemCount()
			if currentLeg.dotKey == "*" {
				trace_util_0.Count(_binary_functions_00000, 85)
				for i := 0; i < elemCount; i++ {
					trace_util_0.Count(_binary_functions_00000, 86)
					buf = bj.objectGetVal(i).extractTo(buf, subPathExpr)
				}
			} else {
				trace_util_0.Count(_binary_functions_00000, 87)
				{
					child, ok := bj.objectSearchKey(hack.Slice(currentLeg.dotKey))
					if ok {
						trace_util_0.Count(_binary_functions_00000, 88)
						buf = child.extractTo(buf, subPathExpr)
					}
				}
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 89)
			if currentLeg.typ == pathLegDoubleAsterisk {
				trace_util_0.Count(_binary_functions_00000, 90)
				buf = bj.extractTo(buf, subPathExpr)
				if bj.TypeCode == TypeCodeArray {
					trace_util_0.Count(_binary_functions_00000, 91)
					elemCount := bj.GetElemCount()
					for i := 0; i < elemCount; i++ {
						trace_util_0.Count(_binary_functions_00000, 92)
						buf = bj.arrayGetElem(i).extractTo(buf, pathExpr)
					}
				} else {
					trace_util_0.Count(_binary_functions_00000, 93)
					if bj.TypeCode == TypeCodeObject {
						trace_util_0.Count(_binary_functions_00000, 94)
						elemCount := bj.GetElemCount()
						for i := 0; i < elemCount; i++ {
							trace_util_0.Count(_binary_functions_00000, 95)
							buf = bj.objectGetVal(i).extractTo(buf, pathExpr)
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 72)
	return buf
}

func (bj BinaryJSON) objectSearchKey(key []byte) (BinaryJSON, bool) {
	trace_util_0.Count(_binary_functions_00000, 96)
	elemCount := bj.GetElemCount()
	idx := sort.Search(elemCount, func(i int) bool {
		trace_util_0.Count(_binary_functions_00000, 99)
		return bytes.Compare(bj.objectGetKey(i), key) >= 0
	})
	trace_util_0.Count(_binary_functions_00000, 97)
	if idx < elemCount && bytes.Equal(bj.objectGetKey(idx), key) {
		trace_util_0.Count(_binary_functions_00000, 100)
		return bj.objectGetVal(idx), true
	}
	trace_util_0.Count(_binary_functions_00000, 98)
	return BinaryJSON{}, false
}

func buildBinaryArray(elems []BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 101)
	totalSize := headerSize + len(elems)*valEntrySize
	for _, elem := range elems {
		trace_util_0.Count(_binary_functions_00000, 103)
		if elem.TypeCode != TypeCodeLiteral {
			trace_util_0.Count(_binary_functions_00000, 104)
			totalSize += len(elem.Value)
		}
	}
	trace_util_0.Count(_binary_functions_00000, 102)
	buf := make([]byte, headerSize+len(elems)*valEntrySize, totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	buf = buildBinaryElements(buf, headerSize, elems)
	return BinaryJSON{TypeCode: TypeCodeArray, Value: buf}
}

func buildBinaryElements(buf []byte, entryStart int, elems []BinaryJSON) []byte {
	trace_util_0.Count(_binary_functions_00000, 105)
	for i, elem := range elems {
		trace_util_0.Count(_binary_functions_00000, 107)
		buf[entryStart+i*valEntrySize] = elem.TypeCode
		if elem.TypeCode == TypeCodeLiteral {
			trace_util_0.Count(_binary_functions_00000, 108)
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Value[0]
		} else {
			trace_util_0.Count(_binary_functions_00000, 109)
			{
				endian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
				buf = append(buf, elem.Value...)
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 106)
	return buf
}

func buildBinaryObject(keys [][]byte, elems []BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 110)
	totalSize := headerSize + len(elems)*(keyEntrySize+valEntrySize)
	for i, elem := range elems {
		trace_util_0.Count(_binary_functions_00000, 113)
		if elem.TypeCode != TypeCodeLiteral {
			trace_util_0.Count(_binary_functions_00000, 115)
			totalSize += len(elem.Value)
		}
		trace_util_0.Count(_binary_functions_00000, 114)
		totalSize += len(keys[i])
	}
	trace_util_0.Count(_binary_functions_00000, 111)
	buf := make([]byte, headerSize+len(elems)*(keyEntrySize+valEntrySize), totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	for i, key := range keys {
		trace_util_0.Count(_binary_functions_00000, 116)
		endian.PutUint32(buf[headerSize+i*keyEntrySize:], uint32(len(buf)))
		endian.PutUint16(buf[headerSize+i*keyEntrySize+keyLenOff:], uint16(len(key)))
		buf = append(buf, key...)
	}
	trace_util_0.Count(_binary_functions_00000, 112)
	entryStart := headerSize + len(elems)*keyEntrySize
	buf = buildBinaryElements(buf, entryStart, elems)
	return BinaryJSON{TypeCode: TypeCodeObject, Value: buf}
}

// Modify modifies a JSON object by insert, replace or set.
// All path expressions cannot contain * or ** wildcard.
// If any error occurs, the input won't be changed.
func (bj BinaryJSON) Modify(pathExprList []PathExpression, values []BinaryJSON, mt ModifyType) (retj BinaryJSON, err error) {
	trace_util_0.Count(_binary_functions_00000, 117)
	if len(pathExprList) != len(values) {
		trace_util_0.Count(_binary_functions_00000, 121)
		// TODO: should return 1582(42000)
		return retj, errors.New("Incorrect parameter count")
	}
	trace_util_0.Count(_binary_functions_00000, 118)
	for _, pathExpr := range pathExprList {
		trace_util_0.Count(_binary_functions_00000, 122)
		if pathExpr.flags.containsAnyAsterisk() {
			trace_util_0.Count(_binary_functions_00000, 123)
			// TODO: should return 3149(42000)
			return retj, errors.New("Invalid path expression")
		}
	}
	trace_util_0.Count(_binary_functions_00000, 119)
	for i := 0; i < len(pathExprList); i++ {
		trace_util_0.Count(_binary_functions_00000, 124)
		pathExpr, value := pathExprList[i], values[i]
		modifier := &binaryModifier{bj: bj}
		switch mt {
		case ModifyInsert:
			trace_util_0.Count(_binary_functions_00000, 125)
			bj = modifier.insert(pathExpr, value)
		case ModifyReplace:
			trace_util_0.Count(_binary_functions_00000, 126)
			bj = modifier.replace(pathExpr, value)
		case ModifySet:
			trace_util_0.Count(_binary_functions_00000, 127)
			bj = modifier.set(pathExpr, value)
		}
	}
	trace_util_0.Count(_binary_functions_00000, 120)
	return bj, nil
}

// Remove removes the elements indicated by pathExprList from JSON.
func (bj BinaryJSON) Remove(pathExprList []PathExpression) (BinaryJSON, error) {
	trace_util_0.Count(_binary_functions_00000, 128)
	for _, pathExpr := range pathExprList {
		trace_util_0.Count(_binary_functions_00000, 130)
		if len(pathExpr.legs) == 0 {
			trace_util_0.Count(_binary_functions_00000, 133)
			// TODO: should return 3153(42000)
			return bj, errors.New("Invalid path expression")
		}
		trace_util_0.Count(_binary_functions_00000, 131)
		if pathExpr.flags.containsAnyAsterisk() {
			trace_util_0.Count(_binary_functions_00000, 134)
			// TODO: should return 3149(42000)
			return bj, errors.New("Invalid path expression")
		}
		trace_util_0.Count(_binary_functions_00000, 132)
		modifer := &binaryModifier{bj: bj}
		bj = modifer.remove(pathExpr)
	}
	trace_util_0.Count(_binary_functions_00000, 129)
	return bj, nil
}

type binaryModifier struct {
	bj          BinaryJSON
	modifyPtr   *byte
	modifyValue BinaryJSON
}

func (bm *binaryModifier) set(path PathExpression, newBj BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 135)
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) > 0 {
		trace_util_0.Count(_binary_functions_00000, 137)
		bm.modifyPtr = &result[0].Value[0]
		bm.modifyValue = newBj
		return bm.rebuild()
	}
	trace_util_0.Count(_binary_functions_00000, 136)
	bm.doInsert(path, newBj)
	return bm.rebuild()
}

func (bm *binaryModifier) replace(path PathExpression, newBj BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 138)
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) == 0 {
		trace_util_0.Count(_binary_functions_00000, 140)
		return bm.bj
	}
	trace_util_0.Count(_binary_functions_00000, 139)
	bm.modifyPtr = &result[0].Value[0]
	bm.modifyValue = newBj
	return bm.rebuild()
}

func (bm *binaryModifier) insert(path PathExpression, newBj BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 141)
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) > 0 {
		trace_util_0.Count(_binary_functions_00000, 143)
		return bm.bj
	}
	trace_util_0.Count(_binary_functions_00000, 142)
	bm.doInsert(path, newBj)
	return bm.rebuild()
}

// doInsert inserts the newBj to its parent, and builds the new parent.
func (bm *binaryModifier) doInsert(path PathExpression, newBj BinaryJSON) {
	trace_util_0.Count(_binary_functions_00000, 144)
	parentPath, lastLeg := path.popOneLastLeg()
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, parentPath)
	if len(result) == 0 {
		trace_util_0.Count(_binary_functions_00000, 151)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 145)
	parentBj := result[0]
	if lastLeg.typ == pathLegIndex {
		trace_util_0.Count(_binary_functions_00000, 152)
		bm.modifyPtr = &parentBj.Value[0]
		if parentBj.TypeCode != TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 155)
			bm.modifyValue = buildBinaryArray([]BinaryJSON{parentBj, newBj})
			return
		}
		trace_util_0.Count(_binary_functions_00000, 153)
		elemCount := parentBj.GetElemCount()
		elems := make([]BinaryJSON, 0, elemCount+1)
		for i := 0; i < elemCount; i++ {
			trace_util_0.Count(_binary_functions_00000, 156)
			elems = append(elems, parentBj.arrayGetElem(i))
		}
		trace_util_0.Count(_binary_functions_00000, 154)
		elems = append(elems, newBj)
		bm.modifyValue = buildBinaryArray(elems)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 146)
	if parentBj.TypeCode != TypeCodeObject {
		trace_util_0.Count(_binary_functions_00000, 157)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 147)
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.GetElemCount()
	insertKey := hack.Slice(lastLeg.dotKey)
	insertIdx := sort.Search(elemCount, func(i int) bool {
		trace_util_0.Count(_binary_functions_00000, 158)
		return bytes.Compare(parentBj.objectGetKey(i), insertKey) >= 0
	})
	trace_util_0.Count(_binary_functions_00000, 148)
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := 0; i < elemCount; i++ {
		trace_util_0.Count(_binary_functions_00000, 159)
		if i == insertIdx {
			trace_util_0.Count(_binary_functions_00000, 161)
			keys = append(keys, insertKey)
			elems = append(elems, newBj)
		}
		trace_util_0.Count(_binary_functions_00000, 160)
		keys = append(keys, parentBj.objectGetKey(i))
		elems = append(elems, parentBj.objectGetVal(i))
	}
	trace_util_0.Count(_binary_functions_00000, 149)
	if insertIdx == elemCount {
		trace_util_0.Count(_binary_functions_00000, 162)
		keys = append(keys, insertKey)
		elems = append(elems, newBj)
	}
	trace_util_0.Count(_binary_functions_00000, 150)
	bm.modifyValue = buildBinaryObject(keys, elems)
}

func (bm *binaryModifier) remove(path PathExpression) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 163)
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) == 0 {
		trace_util_0.Count(_binary_functions_00000, 165)
		return bm.bj
	}
	trace_util_0.Count(_binary_functions_00000, 164)
	bm.doRemove(path)
	return bm.rebuild()
}

func (bm *binaryModifier) doRemove(path PathExpression) {
	trace_util_0.Count(_binary_functions_00000, 166)
	parentPath, lastLeg := path.popOneLastLeg()
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, parentPath)
	if len(result) == 0 {
		trace_util_0.Count(_binary_functions_00000, 171)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 167)
	parentBj := result[0]
	if lastLeg.typ == pathLegIndex {
		trace_util_0.Count(_binary_functions_00000, 172)
		if parentBj.TypeCode != TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 175)
			return
		}
		trace_util_0.Count(_binary_functions_00000, 173)
		bm.modifyPtr = &parentBj.Value[0]
		elemCount := parentBj.GetElemCount()
		elems := make([]BinaryJSON, 0, elemCount-1)
		for i := 0; i < elemCount; i++ {
			trace_util_0.Count(_binary_functions_00000, 176)
			if i != lastLeg.arrayIndex {
				trace_util_0.Count(_binary_functions_00000, 177)
				elems = append(elems, parentBj.arrayGetElem(i))
			}
		}
		trace_util_0.Count(_binary_functions_00000, 174)
		bm.modifyValue = buildBinaryArray(elems)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 168)
	if parentBj.TypeCode != TypeCodeObject {
		trace_util_0.Count(_binary_functions_00000, 178)
		return
	}
	trace_util_0.Count(_binary_functions_00000, 169)
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.GetElemCount()
	removeKey := hack.Slice(lastLeg.dotKey)
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := 0; i < elemCount; i++ {
		trace_util_0.Count(_binary_functions_00000, 179)
		key := parentBj.objectGetKey(i)
		if !bytes.Equal(key, removeKey) {
			trace_util_0.Count(_binary_functions_00000, 180)
			keys = append(keys, parentBj.objectGetKey(i))
			elems = append(elems, parentBj.objectGetVal(i))
		}
	}
	trace_util_0.Count(_binary_functions_00000, 170)
	bm.modifyValue = buildBinaryObject(keys, elems)
}

// rebuild merges the old and the modified JSON into a new BinaryJSON
func (bm *binaryModifier) rebuild() BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 181)
	buf := make([]byte, 0, len(bm.bj.Value)+len(bm.modifyValue.Value))
	value, tpCode := bm.rebuildTo(buf)
	return BinaryJSON{TypeCode: tpCode, Value: value}
}

func (bm *binaryModifier) rebuildTo(buf []byte) ([]byte, TypeCode) {
	trace_util_0.Count(_binary_functions_00000, 182)
	if bm.modifyPtr == &bm.bj.Value[0] {
		trace_util_0.Count(_binary_functions_00000, 187)
		bm.modifyPtr = nil
		return append(buf, bm.modifyValue.Value...), bm.modifyValue.TypeCode
	} else {
		trace_util_0.Count(_binary_functions_00000, 188)
		if bm.modifyPtr == nil {
			trace_util_0.Count(_binary_functions_00000, 189)
			return append(buf, bm.bj.Value...), bm.bj.TypeCode
		}
	}
	trace_util_0.Count(_binary_functions_00000, 183)
	bj := bm.bj
	switch bj.TypeCode {
	case TypeCodeLiteral, TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64, TypeCodeString:
		trace_util_0.Count(_binary_functions_00000, 190)
		return append(buf, bj.Value...), bj.TypeCode
	}
	trace_util_0.Count(_binary_functions_00000, 184)
	docOff := len(buf)
	elemCount := bj.GetElemCount()
	var valEntryStart int
	if bj.TypeCode == TypeCodeArray {
		trace_util_0.Count(_binary_functions_00000, 191)
		copySize := headerSize + elemCount*valEntrySize
		valEntryStart = headerSize
		buf = append(buf, bj.Value[:copySize]...)
	} else {
		trace_util_0.Count(_binary_functions_00000, 192)
		{
			copySize := headerSize + elemCount*(keyEntrySize+valEntrySize)
			valEntryStart = headerSize + elemCount*keyEntrySize
			buf = append(buf, bj.Value[:copySize]...)
			if elemCount > 0 {
				trace_util_0.Count(_binary_functions_00000, 193)
				firstKeyOff := int(endian.Uint32(bj.Value[headerSize:]))
				lastKeyOff := int(endian.Uint32(bj.Value[headerSize+(elemCount-1)*keyEntrySize:]))
				lastKeyLen := int(endian.Uint16(bj.Value[headerSize+(elemCount-1)*keyEntrySize+keyLenOff:]))
				buf = append(buf, bj.Value[firstKeyOff:lastKeyOff+lastKeyLen]...)
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 185)
	for i := 0; i < elemCount; i++ {
		trace_util_0.Count(_binary_functions_00000, 194)
		valEntryOff := valEntryStart + i*valEntrySize
		elem := bj.valEntryGet(valEntryOff)
		bm.bj = elem
		var tpCode TypeCode
		valOff := len(buf) - docOff
		buf, tpCode = bm.rebuildTo(buf)
		buf[docOff+valEntryOff] = tpCode
		if tpCode == TypeCodeLiteral {
			trace_util_0.Count(_binary_functions_00000, 195)
			lastIdx := len(buf) - 1
			endian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(buf[lastIdx]))
			buf = buf[:lastIdx]
		} else {
			trace_util_0.Count(_binary_functions_00000, 196)
			{
				endian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(valOff))
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 186)
	endian.PutUint32(buf[docOff+dataSizeOff:], uint32(len(buf)-docOff))
	return buf, bj.TypeCode
}

// floatEpsilon is the acceptable error quantity when comparing two float numbers.
const floatEpsilon = 1.e-8

// compareFloat64 returns an integer comparing the float64 x to y,
// allowing precision loss.
func compareFloat64PrecisionLoss(x, y float64) int {
	trace_util_0.Count(_binary_functions_00000, 197)
	if x-y < floatEpsilon && y-x < floatEpsilon {
		trace_util_0.Count(_binary_functions_00000, 199)
		return 0
	} else {
		trace_util_0.Count(_binary_functions_00000, 200)
		if x-y < 0 {
			trace_util_0.Count(_binary_functions_00000, 201)
			return -1
		}
	}
	trace_util_0.Count(_binary_functions_00000, 198)
	return 1
}

// CompareBinary compares two binary json objects. Returns -1 if left < right,
// 0 if left == right, else returns 1.
func CompareBinary(left, right BinaryJSON) int {
	trace_util_0.Count(_binary_functions_00000, 202)
	precedence1 := jsonTypePrecedences[left.Type()]
	precedence2 := jsonTypePrecedences[right.Type()]
	var cmp int
	if precedence1 == precedence2 {
		trace_util_0.Count(_binary_functions_00000, 204)
		if precedence1 == jsonTypePrecedences["NULL"] {
			trace_util_0.Count(_binary_functions_00000, 206)
			// for JSON null.
			cmp = 0
		}
		trace_util_0.Count(_binary_functions_00000, 205)
		switch left.TypeCode {
		case TypeCodeLiteral:
			trace_util_0.Count(_binary_functions_00000, 207)
			// false is less than true.
			cmp = int(right.Value[0]) - int(left.Value[0])
		case TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64:
			trace_util_0.Count(_binary_functions_00000, 208)
			leftFloat := i64AsFloat64(left.GetInt64(), left.TypeCode)
			rightFloat := i64AsFloat64(right.GetInt64(), right.TypeCode)
			cmp = compareFloat64PrecisionLoss(leftFloat, rightFloat)
		case TypeCodeString:
			trace_util_0.Count(_binary_functions_00000, 209)
			cmp = bytes.Compare(left.GetString(), right.GetString())
		case TypeCodeArray:
			trace_util_0.Count(_binary_functions_00000, 210)
			leftCount := left.GetElemCount()
			rightCount := right.GetElemCount()
			for i := 0; i < leftCount && i < rightCount; i++ {
				trace_util_0.Count(_binary_functions_00000, 213)
				elem1 := left.arrayGetElem(i)
				elem2 := right.arrayGetElem(i)
				cmp = CompareBinary(elem1, elem2)
				if cmp != 0 {
					trace_util_0.Count(_binary_functions_00000, 214)
					return cmp
				}
			}
			trace_util_0.Count(_binary_functions_00000, 211)
			cmp = leftCount - rightCount
		case TypeCodeObject:
			trace_util_0.Count(_binary_functions_00000, 212)
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			cmp = bytes.Compare(left.Value, right.Value)
		}
	} else {
		trace_util_0.Count(_binary_functions_00000, 215)
		{
			cmp = precedence1 - precedence2
		}
	}
	trace_util_0.Count(_binary_functions_00000, 203)
	return cmp
}

func i64AsFloat64(i64 int64, typeCode TypeCode) float64 {
	trace_util_0.Count(_binary_functions_00000, 216)
	switch typeCode {
	case TypeCodeLiteral, TypeCodeInt64:
		trace_util_0.Count(_binary_functions_00000, 217)
		return float64(i64)
	case TypeCodeUint64:
		trace_util_0.Count(_binary_functions_00000, 218)
		u64 := *(*uint64)(unsafe.Pointer(&i64))
		return float64(u64)
	case TypeCodeFloat64:
		trace_util_0.Count(_binary_functions_00000, 219)
		return *(*float64)(unsafe.Pointer(&i64))
	default:
		trace_util_0.Count(_binary_functions_00000, 220)
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, typeCode)
		panic(msg)
	}
}

// MergeBinary merges multiple BinaryJSON into one according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func MergeBinary(bjs []BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 221)
	var remain = bjs
	var objects []BinaryJSON
	var results []BinaryJSON
	for len(remain) > 0 {
		trace_util_0.Count(_binary_functions_00000, 224)
		if remain[0].TypeCode != TypeCodeObject {
			trace_util_0.Count(_binary_functions_00000, 225)
			results = append(results, remain[0])
			remain = remain[1:]
		} else {
			trace_util_0.Count(_binary_functions_00000, 226)
			{
				objects, remain = getAdjacentObjects(remain)
				results = append(results, mergeBinaryObject(objects))
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 222)
	if len(results) == 1 {
		trace_util_0.Count(_binary_functions_00000, 227)
		return results[0]
	}
	trace_util_0.Count(_binary_functions_00000, 223)
	return mergeBinaryArray(results)
}

func getAdjacentObjects(bjs []BinaryJSON) (objects, remain []BinaryJSON) {
	trace_util_0.Count(_binary_functions_00000, 228)
	for i := 0; i < len(bjs); i++ {
		trace_util_0.Count(_binary_functions_00000, 230)
		if bjs[i].TypeCode != TypeCodeObject {
			trace_util_0.Count(_binary_functions_00000, 231)
			return bjs[:i], bjs[i:]
		}
	}
	trace_util_0.Count(_binary_functions_00000, 229)
	return bjs, nil
}

func mergeBinaryArray(elems []BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 232)
	buf := make([]BinaryJSON, 0, len(elems))
	for i := 0; i < len(elems); i++ {
		trace_util_0.Count(_binary_functions_00000, 234)
		elem := elems[i]
		if elem.TypeCode != TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 235)
			buf = append(buf, elem)
		} else {
			trace_util_0.Count(_binary_functions_00000, 236)
			{
				childCount := elem.GetElemCount()
				for j := 0; j < childCount; j++ {
					trace_util_0.Count(_binary_functions_00000, 237)
					buf = append(buf, elem.arrayGetElem(j))
				}
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 233)
	return buildBinaryArray(buf)
}

func mergeBinaryObject(objects []BinaryJSON) BinaryJSON {
	trace_util_0.Count(_binary_functions_00000, 238)
	keyValMap := make(map[string]BinaryJSON)
	keys := make([][]byte, 0, len(keyValMap))
	for _, obj := range objects {
		trace_util_0.Count(_binary_functions_00000, 242)
		elemCount := obj.GetElemCount()
		for i := 0; i < elemCount; i++ {
			trace_util_0.Count(_binary_functions_00000, 243)
			key := obj.objectGetKey(i)
			val := obj.objectGetVal(i)
			if old, ok := keyValMap[string(key)]; ok {
				trace_util_0.Count(_binary_functions_00000, 244)
				keyValMap[string(key)] = MergeBinary([]BinaryJSON{old, val})
			} else {
				trace_util_0.Count(_binary_functions_00000, 245)
				{
					keyValMap[string(key)] = val
					keys = append(keys, key)
				}
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 239)
	sort.Slice(keys, func(i, j int) bool {
		trace_util_0.Count(_binary_functions_00000, 246)
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	trace_util_0.Count(_binary_functions_00000, 240)
	values := make([]BinaryJSON, len(keys))
	for i, key := range keys {
		trace_util_0.Count(_binary_functions_00000, 247)
		values[i] = keyValMap[string(key)]
	}
	trace_util_0.Count(_binary_functions_00000, 241)
	return buildBinaryObject(keys, values)
}

// PeekBytesAsJSON trys to peek some bytes from b, until
// we can deserialize a JSON from those bytes.
func PeekBytesAsJSON(b []byte) (n int, err error) {
	trace_util_0.Count(_binary_functions_00000, 248)
	if len(b) <= 0 {
		trace_util_0.Count(_binary_functions_00000, 251)
		err = errors.New("Cant peek from empty bytes")
		return
	}
	trace_util_0.Count(_binary_functions_00000, 249)
	switch c := TypeCode(b[0]); c {
	case TypeCodeObject, TypeCodeArray:
		trace_util_0.Count(_binary_functions_00000, 252)
		if len(b) >= valTypeSize+headerSize {
			trace_util_0.Count(_binary_functions_00000, 256)
			size := endian.Uint32(b[valTypeSize+dataSizeOff:])
			n = valTypeSize + int(size)
			return
		}
	case TypeCodeString:
		trace_util_0.Count(_binary_functions_00000, 253)
		strLen, lenLen := binary.Uvarint(b[valTypeSize:])
		return valTypeSize + int(strLen) + lenLen, nil
	case TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64:
		trace_util_0.Count(_binary_functions_00000, 254)
		n = valTypeSize + 8
		return
	case TypeCodeLiteral:
		trace_util_0.Count(_binary_functions_00000, 255)
		n = valTypeSize + 1
		return
	}
	trace_util_0.Count(_binary_functions_00000, 250)
	err = errors.New("Invalid JSON bytes")
	return
}

// ContainsBinary check whether JSON document contains specific target according the following rules:
// 1) object contains a target object if and only if every key is contained in source object and the value associated with the target key is contained in the value associated with the source key;
// 2) array contains a target nonarray if and only if the target is contained in some element of the array;
// 3) array contains a target array if and only if every element is contained in some element of the array;
// 4) scalar contains a target scalar if and only if they are comparable and are equal;
func ContainsBinary(obj, target BinaryJSON) bool {
	trace_util_0.Count(_binary_functions_00000, 257)
	switch obj.TypeCode {
	case TypeCodeObject:
		trace_util_0.Count(_binary_functions_00000, 258)
		if target.TypeCode == TypeCodeObject {
			trace_util_0.Count(_binary_functions_00000, 264)
			len := target.GetElemCount()
			for i := 0; i < len; i++ {
				trace_util_0.Count(_binary_functions_00000, 266)
				key := target.objectGetKey(i)
				val := target.objectGetVal(i)
				if exp, exists := obj.objectSearchKey(key); !exists || !ContainsBinary(exp, val) {
					trace_util_0.Count(_binary_functions_00000, 267)
					return false
				}
			}
			trace_util_0.Count(_binary_functions_00000, 265)
			return true
		}
		trace_util_0.Count(_binary_functions_00000, 259)
		return false
	case TypeCodeArray:
		trace_util_0.Count(_binary_functions_00000, 260)
		if target.TypeCode == TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 268)
			len := target.GetElemCount()
			for i := 0; i < len; i++ {
				trace_util_0.Count(_binary_functions_00000, 270)
				if !ContainsBinary(obj, target.arrayGetElem(i)) {
					trace_util_0.Count(_binary_functions_00000, 271)
					return false
				}
			}
			trace_util_0.Count(_binary_functions_00000, 269)
			return true
		}
		trace_util_0.Count(_binary_functions_00000, 261)
		len := obj.GetElemCount()
		for i := 0; i < len; i++ {
			trace_util_0.Count(_binary_functions_00000, 272)
			if ContainsBinary(obj.arrayGetElem(i), target) {
				trace_util_0.Count(_binary_functions_00000, 273)
				return true
			}
		}
		trace_util_0.Count(_binary_functions_00000, 262)
		return false
	default:
		trace_util_0.Count(_binary_functions_00000, 263)
		return CompareBinary(obj, target) == 0
	}
}

// GetElemDepth for JSON_DEPTH
// Returns the maximum depth of a JSON document
// rules referenced by MySQL JSON_DEPTH function
// [https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-depth]
// 1) An empty array, empty object, or scalar value has depth 1.
// 2) A nonempty array containing only elements of depth 1 or nonempty object containing only member values of depth 1 has depth 2.
// 3) Otherwise, a JSON document has depth greater than 2.
// e.g. depth of '{}', '[]', 'true': 1
// e.g. depth of '[10, 20]', '[[], {}]': 2
// e.g. depth of '[10, {"a": 20}]': 3
func (bj BinaryJSON) GetElemDepth() int {
	trace_util_0.Count(_binary_functions_00000, 274)
	switch bj.TypeCode {
	case TypeCodeObject:
		trace_util_0.Count(_binary_functions_00000, 275)
		len := bj.GetElemCount()
		maxDepth := 0
		for i := 0; i < len; i++ {
			trace_util_0.Count(_binary_functions_00000, 280)
			obj := bj.objectGetVal(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				trace_util_0.Count(_binary_functions_00000, 281)
				maxDepth = depth
			}
		}
		trace_util_0.Count(_binary_functions_00000, 276)
		return maxDepth + 1
	case TypeCodeArray:
		trace_util_0.Count(_binary_functions_00000, 277)
		len := bj.GetElemCount()
		maxDepth := 0
		for i := 0; i < len; i++ {
			trace_util_0.Count(_binary_functions_00000, 282)
			obj := bj.arrayGetElem(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				trace_util_0.Count(_binary_functions_00000, 283)
				maxDepth = depth
			}
		}
		trace_util_0.Count(_binary_functions_00000, 278)
		return maxDepth + 1
	default:
		trace_util_0.Count(_binary_functions_00000, 279)
		return 1
	}
}

// extractCallbackFn: the type of CALLBACK function for extractToCallback
type extractCallbackFn func(fullpath PathExpression, bj BinaryJSON) (stop bool, err error)

// extractToCallback: callback alternative of extractTo
//     would be more effective when walk through the whole JSON is unnecessary
// NOTICE: path [0] & [*] for JSON object other than array is INVALID, which is different from extractTo.
func (bj BinaryJSON) extractToCallback(pathExpr PathExpression, callbackFn extractCallbackFn, fullpath PathExpression) (stop bool, err error) {
	trace_util_0.Count(_binary_functions_00000, 284)
	if len(pathExpr.legs) == 0 {
		trace_util_0.Count(_binary_functions_00000, 287)
		return callbackFn(fullpath, bj)
	}

	trace_util_0.Count(_binary_functions_00000, 285)
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == pathLegIndex && bj.TypeCode == TypeCodeArray {
		trace_util_0.Count(_binary_functions_00000, 288)
		elemCount := bj.GetElemCount()
		if currentLeg.arrayIndex == arrayIndexAsterisk {
			trace_util_0.Count(_binary_functions_00000, 289)
			for i := 0; i < elemCount; i++ {
				trace_util_0.Count(_binary_functions_00000, 290)
				//buf = bj.arrayGetElem(i).extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneIndexLeg(i)
				stop, err = bj.arrayGetElem(i).extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					trace_util_0.Count(_binary_functions_00000, 291)
					return
				}
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 292)
			if currentLeg.arrayIndex < elemCount {
				trace_util_0.Count(_binary_functions_00000, 293)
				//buf = bj.arrayGetElem(currentLeg.arrayIndex).extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneIndexLeg(currentLeg.arrayIndex)
				stop, err = bj.arrayGetElem(currentLeg.arrayIndex).extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					trace_util_0.Count(_binary_functions_00000, 294)
					return
				}
			}
		}
	} else {
		trace_util_0.Count(_binary_functions_00000, 295)
		if currentLeg.typ == pathLegKey && bj.TypeCode == TypeCodeObject {
			trace_util_0.Count(_binary_functions_00000, 296)
			elemCount := bj.GetElemCount()
			if currentLeg.dotKey == "*" {
				trace_util_0.Count(_binary_functions_00000, 297)
				for i := 0; i < elemCount; i++ {
					trace_util_0.Count(_binary_functions_00000, 298)
					//buf = bj.objectGetVal(i).extractTo(buf, subPathExpr)
					path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
					stop, err = bj.objectGetVal(i).extractToCallback(subPathExpr, callbackFn, path)
					if stop || err != nil {
						trace_util_0.Count(_binary_functions_00000, 299)
						return
					}
				}
			} else {
				trace_util_0.Count(_binary_functions_00000, 300)
				{
					child, ok := bj.objectSearchKey(hack.Slice(currentLeg.dotKey))
					if ok {
						trace_util_0.Count(_binary_functions_00000, 301)
						//buf = child.extractTo(buf, subPathExpr)
						path := fullpath.pushBackOneKeyLeg(currentLeg.dotKey)
						stop, err = child.extractToCallback(subPathExpr, callbackFn, path)
						if stop || err != nil {
							trace_util_0.Count(_binary_functions_00000, 302)
							return
						}
					}
				}
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 303)
			if currentLeg.typ == pathLegDoubleAsterisk {
				trace_util_0.Count(_binary_functions_00000, 304)
				//buf = bj.extractTo(buf, subPathExpr)
				stop, err = bj.extractToCallback(subPathExpr, callbackFn, fullpath)
				if stop || err != nil {
					trace_util_0.Count(_binary_functions_00000, 306)
					return
				}

				trace_util_0.Count(_binary_functions_00000, 305)
				if bj.TypeCode == TypeCodeArray {
					trace_util_0.Count(_binary_functions_00000, 307)
					elemCount := bj.GetElemCount()
					for i := 0; i < elemCount; i++ {
						trace_util_0.Count(_binary_functions_00000, 308)
						//buf = bj.arrayGetElem(i).extractTo(buf, pathExpr)
						path := fullpath.pushBackOneIndexLeg(i)
						stop, err = bj.arrayGetElem(i).extractToCallback(pathExpr, callbackFn, path)
						if stop || err != nil {
							trace_util_0.Count(_binary_functions_00000, 309)
							return
						}
					}
				} else {
					trace_util_0.Count(_binary_functions_00000, 310)
					if bj.TypeCode == TypeCodeObject {
						trace_util_0.Count(_binary_functions_00000, 311)
						elemCount := bj.GetElemCount()
						for i := 0; i < elemCount; i++ {
							trace_util_0.Count(_binary_functions_00000, 312)
							//buf = bj.objectGetVal(i).extractTo(buf, pathExpr)
							path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
							stop, err = bj.objectGetVal(i).extractToCallback(pathExpr, callbackFn, path)
							if stop || err != nil {
								trace_util_0.Count(_binary_functions_00000, 313)
								return
							}
						}
					}
				}
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 286)
	return false, nil
}

// BinaryJSONWalkFunc is used as callback function for BinaryJSON.Walk
type BinaryJSONWalkFunc func(fullpath PathExpression, bj BinaryJSON) (stop bool, err error)

// Walk traverse BinaryJSON objects
func (bj BinaryJSON) Walk(walkFn BinaryJSONWalkFunc, pathExprList ...PathExpression) (err error) {
	trace_util_0.Count(_binary_functions_00000, 314)
	pathSet := make(map[string]bool)

	var doWalk extractCallbackFn
	doWalk = func(fullpath PathExpression, bj BinaryJSON) (stop bool, err error) {
		trace_util_0.Count(_binary_functions_00000, 317)
		pathStr := fullpath.String()
		if _, ok := pathSet[pathStr]; ok {
			trace_util_0.Count(_binary_functions_00000, 321)
			return false, nil
		}

		trace_util_0.Count(_binary_functions_00000, 318)
		stop, err = walkFn(fullpath, bj)
		pathSet[pathStr] = true
		if stop || err != nil {
			trace_util_0.Count(_binary_functions_00000, 322)
			return
		}

		trace_util_0.Count(_binary_functions_00000, 319)
		if bj.TypeCode == TypeCodeArray {
			trace_util_0.Count(_binary_functions_00000, 323)
			elemCount := bj.GetElemCount()
			for i := 0; i < elemCount; i++ {
				trace_util_0.Count(_binary_functions_00000, 324)
				path := fullpath.pushBackOneIndexLeg(i)
				stop, err = doWalk(path, bj.arrayGetElem(i))
				if stop || err != nil {
					trace_util_0.Count(_binary_functions_00000, 325)
					return
				}
			}
		} else {
			trace_util_0.Count(_binary_functions_00000, 326)
			if bj.TypeCode == TypeCodeObject {
				trace_util_0.Count(_binary_functions_00000, 327)
				elemCount := bj.GetElemCount()
				for i := 0; i < elemCount; i++ {
					trace_util_0.Count(_binary_functions_00000, 328)
					path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
					stop, err = doWalk(path, bj.objectGetVal(i))
					if stop || err != nil {
						trace_util_0.Count(_binary_functions_00000, 329)
						return
					}
				}
			}
		}
		trace_util_0.Count(_binary_functions_00000, 320)
		return false, nil
	}

	trace_util_0.Count(_binary_functions_00000, 315)
	fullpath := PathExpression{legs: make([]pathLeg, 0, 32), flags: pathExpressionFlag(0)}
	if len(pathExprList) > 0 {
		trace_util_0.Count(_binary_functions_00000, 330)
		for _, pathExpr := range pathExprList {
			trace_util_0.Count(_binary_functions_00000, 331)
			var stop bool
			stop, err = bj.extractToCallback(pathExpr, doWalk, fullpath)
			if stop || err != nil {
				trace_util_0.Count(_binary_functions_00000, 332)
				return err
			}
		}
	} else {
		trace_util_0.Count(_binary_functions_00000, 333)
		{
			_, err = doWalk(fullpath, bj)
			if err != nil {
				trace_util_0.Count(_binary_functions_00000, 334)
				return
			}
		}
	}
	trace_util_0.Count(_binary_functions_00000, 316)
	return nil
}

var _binary_functions_00000 = "types/json/binary_functions.go"
