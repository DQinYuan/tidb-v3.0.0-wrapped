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
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/trace_util_0"
)

/*
	From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope (pathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		pathLeg ::= member | arrayLocation | '**'
		member ::= '.' (keyName | '*')
		arrayLocation ::= '[' (non-negative-integer | '*') ']'
		keyName ::= ECMAScript-identifier | ECMAScript-string-literal

	And some implementation limits in MySQL 5.7:
		1) columnReference in scope must be empty now;
		2) double asterisk(**) could not be last leg;

	Examples:
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]
*/

// [a-zA-Z_][a-zA-Z0-9_]* matches any identifier;
// "[^"\\]*(\\.[^"\\]*)*" matches any string literal which can carry escaped quotes;
var jsonPathExprLegRe = regexp.MustCompile(`(\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)`)

type pathLegType byte

const (
	// pathLegKey indicates the path leg with '.key'.
	pathLegKey pathLegType = 0x01
	// pathLegIndex indicates the path leg with form '[number]'.
	pathLegIndex pathLegType = 0x02
	// pathLegDoubleAsterisk indicates the path leg with form '**'.
	pathLegDoubleAsterisk pathLegType = 0x03
)

// pathLeg is only used by PathExpression.
type pathLeg struct {
	typ        pathLegType
	arrayIndex int    // if typ is pathLegIndex, the value should be parsed into here.
	dotKey     string // if typ is pathLegKey, the key should be parsed into here.
}

// arrayIndexAsterisk is for parsing `*` into a number.
// we need this number represent "all".
const arrayIndexAsterisk = -1

// pathExpressionFlag holds attributes of PathExpression
type pathExpressionFlag byte

const (
	pathExpressionContainsAsterisk       pathExpressionFlag = 0x01
	pathExpressionContainsDoubleAsterisk pathExpressionFlag = 0x02
)

// containsAnyAsterisk returns true if pef contains any asterisk.
func (pef pathExpressionFlag) containsAnyAsterisk() bool {
	trace_util_0.Count(_path_expr_00000, 0)
	pef &= pathExpressionContainsAsterisk | pathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// PathExpression is for JSON path expression.
type PathExpression struct {
	legs  []pathLeg
	flags pathExpressionFlag
}

// popOneLeg returns a pathLeg, and a child PathExpression without that leg.
func (pe PathExpression) popOneLeg() (pathLeg, PathExpression) {
	trace_util_0.Count(_path_expr_00000, 1)
	newPe := PathExpression{
		legs:  pe.legs[1:],
		flags: 0,
	}
	for _, leg := range newPe.legs {
		trace_util_0.Count(_path_expr_00000, 3)
		if leg.typ == pathLegIndex && leg.arrayIndex == -1 {
			trace_util_0.Count(_path_expr_00000, 4)
			newPe.flags |= pathExpressionContainsAsterisk
		} else {
			trace_util_0.Count(_path_expr_00000, 5)
			if leg.typ == pathLegKey && leg.dotKey == "*" {
				trace_util_0.Count(_path_expr_00000, 6)
				newPe.flags |= pathExpressionContainsAsterisk
			} else {
				trace_util_0.Count(_path_expr_00000, 7)
				if leg.typ == pathLegDoubleAsterisk {
					trace_util_0.Count(_path_expr_00000, 8)
					newPe.flags |= pathExpressionContainsDoubleAsterisk
				}
			}
		}
	}
	trace_util_0.Count(_path_expr_00000, 2)
	return pe.legs[0], newPe
}

// popOneLastLeg returns the a parent PathExpression and the last pathLeg
func (pe PathExpression) popOneLastLeg() (PathExpression, pathLeg) {
	trace_util_0.Count(_path_expr_00000, 9)
	lastLegIdx := len(pe.legs) - 1
	lastLeg := pe.legs[lastLegIdx]
	// It is used only in modification, it has been checked that there is no asterisks.
	return PathExpression{legs: pe.legs[:lastLegIdx]}, lastLeg
}

// pushBackOneIndexLeg pushback one leg of INDEX type
func (pe PathExpression) pushBackOneIndexLeg(index int) PathExpression {
	trace_util_0.Count(_path_expr_00000, 10)
	newPe := PathExpression{
		legs:  append(pe.legs, pathLeg{typ: pathLegIndex, arrayIndex: index}),
		flags: pe.flags,
	}
	if index == -1 {
		trace_util_0.Count(_path_expr_00000, 12)
		newPe.flags |= pathExpressionContainsAsterisk
	}
	trace_util_0.Count(_path_expr_00000, 11)
	return newPe
}

// pushBackOneKeyLeg pushback one leg of KEY type
func (pe PathExpression) pushBackOneKeyLeg(key string) PathExpression {
	trace_util_0.Count(_path_expr_00000, 13)
	newPe := PathExpression{
		legs:  append(pe.legs, pathLeg{typ: pathLegKey, dotKey: key}),
		flags: pe.flags,
	}
	if key == "*" {
		trace_util_0.Count(_path_expr_00000, 15)
		newPe.flags |= pathExpressionContainsAsterisk
	}
	trace_util_0.Count(_path_expr_00000, 14)
	return newPe
}

// ContainsAnyAsterisk returns true if pe contains any asterisk.
func (pe PathExpression) ContainsAnyAsterisk() bool {
	trace_util_0.Count(_path_expr_00000, 16)
	return pe.flags.containsAnyAsterisk()
}

// ParseJSONPathExpr parses a JSON path expression. Returns a PathExpression
// object which can be used in JSON_EXTRACT, JSON_SET and so on.
func ParseJSONPathExpr(pathExpr string) (pe PathExpression, err error) {
	trace_util_0.Count(_path_expr_00000, 17)
	// Find the position of first '$'. If any no-blank characters in
	// pathExpr[0: dollarIndex), return an ErrInvalidJSONPath error.
	dollarIndex := strings.Index(pathExpr, "$")
	if dollarIndex < 0 {
		trace_util_0.Count(_path_expr_00000, 23)
		err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
		return
	}
	trace_util_0.Count(_path_expr_00000, 18)
	for i := 0; i < dollarIndex; i++ {
		trace_util_0.Count(_path_expr_00000, 24)
		if !isBlank(rune(pathExpr[i])) {
			trace_util_0.Count(_path_expr_00000, 25)
			err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
			return
		}
	}

	trace_util_0.Count(_path_expr_00000, 19)
	pathExprSuffix := strings.TrimFunc(pathExpr[dollarIndex+1:], isBlank)
	indices := jsonPathExprLegRe.FindAllStringIndex(pathExprSuffix, -1)
	if len(indices) == 0 && len(pathExprSuffix) != 0 {
		trace_util_0.Count(_path_expr_00000, 26)
		err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
		return
	}

	trace_util_0.Count(_path_expr_00000, 20)
	pe.legs = make([]pathLeg, 0, len(indices))
	pe.flags = pathExpressionFlag(0)

	lastEnd := 0
	for _, indice := range indices {
		trace_util_0.Count(_path_expr_00000, 27)
		start, end := indice[0], indice[1]

		// Check all characters between two legs are blank.
		for i := lastEnd; i < start; i++ {
			trace_util_0.Count(_path_expr_00000, 29)
			if !isBlank(rune(pathExprSuffix[i])) {
				trace_util_0.Count(_path_expr_00000, 30)
				err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
				return
			}
		}
		trace_util_0.Count(_path_expr_00000, 28)
		lastEnd = end

		if pathExprSuffix[start] == '[' {
			trace_util_0.Count(_path_expr_00000, 31)
			// The leg is an index of a JSON array.
			var leg = strings.TrimFunc(pathExprSuffix[start+1:end], isBlank)
			var indexStr = strings.TrimFunc(leg[0:len(leg)-1], isBlank)
			var index int
			if len(indexStr) == 1 && indexStr[0] == '*' {
				trace_util_0.Count(_path_expr_00000, 33)
				pe.flags |= pathExpressionContainsAsterisk
				index = arrayIndexAsterisk
			} else {
				trace_util_0.Count(_path_expr_00000, 34)
				{
					if index, err = strconv.Atoi(indexStr); err != nil {
						trace_util_0.Count(_path_expr_00000, 35)
						err = errors.Trace(err)
						return
					}
				}
			}
			trace_util_0.Count(_path_expr_00000, 32)
			pe.legs = append(pe.legs, pathLeg{typ: pathLegIndex, arrayIndex: index})
		} else {
			trace_util_0.Count(_path_expr_00000, 36)
			if pathExprSuffix[start] == '.' {
				trace_util_0.Count(_path_expr_00000, 37)
				// The leg is a key of a JSON object.
				var key = strings.TrimFunc(pathExprSuffix[start+1:end], isBlank)
				if len(key) == 1 && key[0] == '*' {
					trace_util_0.Count(_path_expr_00000, 39)
					pe.flags |= pathExpressionContainsAsterisk
				} else {
					trace_util_0.Count(_path_expr_00000, 40)
					if key[0] == '"' {
						trace_util_0.Count(_path_expr_00000, 41)
						// We need unquote the origin string.
						if key, err = unquoteString(key[1 : len(key)-1]); err != nil {
							trace_util_0.Count(_path_expr_00000, 42)
							err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
							return
						}
					}
				}
				trace_util_0.Count(_path_expr_00000, 38)
				pe.legs = append(pe.legs, pathLeg{typ: pathLegKey, dotKey: key})
			} else {
				trace_util_0.Count(_path_expr_00000, 43)
				{
					// The leg is '**'.
					pe.flags |= pathExpressionContainsDoubleAsterisk
					pe.legs = append(pe.legs, pathLeg{typ: pathLegDoubleAsterisk})
				}
			}
		}
	}
	trace_util_0.Count(_path_expr_00000, 21)
	if len(pe.legs) > 0 {
		trace_util_0.Count(_path_expr_00000, 44)
		// The last leg of a path expression cannot be '**'.
		if pe.legs[len(pe.legs)-1].typ == pathLegDoubleAsterisk {
			trace_util_0.Count(_path_expr_00000, 45)
			err = ErrInvalidJSONPath.GenWithStackByArgs(pathExpr)
			return
		}
	}
	trace_util_0.Count(_path_expr_00000, 22)
	return
}

func isBlank(c rune) bool {
	trace_util_0.Count(_path_expr_00000, 46)
	if c == '\n' || c == '\r' || c == '\t' || c == ' ' {
		trace_util_0.Count(_path_expr_00000, 48)
		return true
	}
	trace_util_0.Count(_path_expr_00000, 47)
	return false
}

func (pe PathExpression) String() string {
	trace_util_0.Count(_path_expr_00000, 49)
	var s strings.Builder

	s.WriteString("$")
	for _, leg := range pe.legs {
		trace_util_0.Count(_path_expr_00000, 51)
		switch leg.typ {
		case pathLegIndex:
			trace_util_0.Count(_path_expr_00000, 52)
			if leg.arrayIndex == -1 {
				trace_util_0.Count(_path_expr_00000, 55)
				s.WriteString("[*]")
			} else {
				trace_util_0.Count(_path_expr_00000, 56)
				{
					s.WriteString("[")
					s.WriteString(strconv.Itoa(leg.arrayIndex))
					s.WriteString("]")
				}
			}
		case pathLegKey:
			trace_util_0.Count(_path_expr_00000, 53)
			s.WriteString(".")
			s.WriteString(quoteString(leg.dotKey))
		case pathLegDoubleAsterisk:
			trace_util_0.Count(_path_expr_00000, 54)
			s.WriteString("**")
		}
	}
	trace_util_0.Count(_path_expr_00000, 50)
	return s.String()
}

var _path_expr_00000 = "types/json/path_expr.go"
