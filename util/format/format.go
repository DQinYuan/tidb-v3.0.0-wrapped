// Copyright (c) 2014 The sortutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/STRUTIL-LICENSE file.

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

package format

import (
	"bytes"
	"fmt"
	"github.com/pingcap/tidb/trace_util_0"
	"io"
)

const (
	st0 = iota
	stBOL
	stPERC
	stBOLPERC
)

// Formatter is an io.Writer extended formatter by a fmt.Printf like function Format.
type Formatter interface {
	io.Writer
	Format(format string, args ...interface{}) (n int, errno error)
}

type indentFormatter struct {
	io.Writer
	indent      []byte
	indentLevel int
	state       int
}

var replace = map[rune]string{
	'\000': "\\0",
	'\'':   "''",
	'\n':   "\\n",
	'\r':   "\\r",
}

// IndentFormatter returns a new Formatter which interprets %i and %u in the
// Format() formats string as indent and unindent commands. The commands can
// nest. The Formatter writes to io.Writer 'w' and inserts one 'indent'
// string per current indent level value.
// Behaviour of commands reaching negative indent levels is undefined.
//  IndentFormatter(os.Stdout, "\t").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
// output:
//  abc3%e
//      x
//      y
//  z
// The Go quoted string literal form of the above is:
//  "abc%%e\n\tx\n\tx\nz\n"
// The commands can be scattered between separate invocations of Format(),
// i.e. the formatter keeps track of the indent level and knows if it is
// positioned on start of a line and should emit indentation(s).
// The same output as above can be produced by e.g.:
//  f := IndentFormatter(os.Stdout, " ")
//  f.Format("abc%d%%e%i\nx\n", 3)
//  f.Format("y\n%uz\n")
func IndentFormatter(w io.Writer, indent string) Formatter {
	trace_util_0.Count(_format_00000, 0)
	return &indentFormatter{w, []byte(indent), 0, stBOL}
}

func (f *indentFormatter) format(flat bool, format string, args ...interface{}) (n int, errno error) {
	trace_util_0.Count(_format_00000, 1)
	var buf = make([]byte, 0)
	for i := 0; i < len(format); i++ {
		trace_util_0.Count(_format_00000, 4)
		c := format[i]
		switch f.state {
		case st0:
			trace_util_0.Count(_format_00000, 5)
			switch c {
			case '\n':
				trace_util_0.Count(_format_00000, 10)
				cc := c
				if flat && f.indentLevel != 0 {
					trace_util_0.Count(_format_00000, 14)
					cc = ' '
				}
				trace_util_0.Count(_format_00000, 11)
				buf = append(buf, cc)
				f.state = stBOL
			case '%':
				trace_util_0.Count(_format_00000, 12)
				f.state = stPERC
			default:
				trace_util_0.Count(_format_00000, 13)
				buf = append(buf, c)
			}
		case stBOL:
			trace_util_0.Count(_format_00000, 6)
			switch c {
			case '\n':
				trace_util_0.Count(_format_00000, 15)
				cc := c
				if flat && f.indentLevel != 0 {
					trace_util_0.Count(_format_00000, 20)
					cc = ' '
				}
				trace_util_0.Count(_format_00000, 16)
				buf = append(buf, cc)
			case '%':
				trace_util_0.Count(_format_00000, 17)
				f.state = stBOLPERC
			default:
				trace_util_0.Count(_format_00000, 18)
				if !flat {
					trace_util_0.Count(_format_00000, 21)
					for i := 0; i < f.indentLevel; i++ {
						trace_util_0.Count(_format_00000, 22)
						buf = append(buf, f.indent...)
					}
				}
				trace_util_0.Count(_format_00000, 19)
				buf = append(buf, c)
				f.state = st0
			}
		case stBOLPERC:
			trace_util_0.Count(_format_00000, 7)
			switch c {
			case 'i':
				trace_util_0.Count(_format_00000, 23)
				f.indentLevel++
				f.state = stBOL
			case 'u':
				trace_util_0.Count(_format_00000, 24)
				f.indentLevel--
				f.state = stBOL
			default:
				trace_util_0.Count(_format_00000, 25)
				if !flat {
					trace_util_0.Count(_format_00000, 27)
					for i := 0; i < f.indentLevel; i++ {
						trace_util_0.Count(_format_00000, 28)
						buf = append(buf, f.indent...)
					}
				}
				trace_util_0.Count(_format_00000, 26)
				buf = append(buf, '%', c)
				f.state = st0
			}
		case stPERC:
			trace_util_0.Count(_format_00000, 8)
			switch c {
			case 'i':
				trace_util_0.Count(_format_00000, 29)
				f.indentLevel++
				f.state = st0
			case 'u':
				trace_util_0.Count(_format_00000, 30)
				f.indentLevel--
				f.state = st0
			default:
				trace_util_0.Count(_format_00000, 31)
				buf = append(buf, '%', c)
				f.state = st0
			}
		default:
			trace_util_0.Count(_format_00000, 9)
			panic("unexpected state")
		}
	}
	trace_util_0.Count(_format_00000, 2)
	switch f.state {
	case stPERC, stBOLPERC:
		trace_util_0.Count(_format_00000, 32)
		buf = append(buf, '%')
	}
	trace_util_0.Count(_format_00000, 3)
	return f.Write([]byte(fmt.Sprintf(string(buf), args...)))
}

// Format implements Format interface.
func (f *indentFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	trace_util_0.Count(_format_00000, 33)
	return f.format(false, format, args...)
}

type flatFormatter indentFormatter

// FlatFormatter returns a newly created Formatter with the same functionality as the one returned
// by IndentFormatter except it allows a newline in the 'format' string argument of Format
// to pass through if the indent level is current zero.
//
// If the indent level is non-zero then such new lines are changed to a space character.
// There is no indent string, the %i and %u format verbs are used solely to determine the indent level.
//
// The FlatFormatter is intended for flattening of normally nested structure textual representation to
// a one top level structure per line form.
//  FlatFormatter(os.Stdout, " ").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
// output in the form of a Go quoted string literal:
//  "abc3%%e x y z\n"
func FlatFormatter(w io.Writer) Formatter {
	trace_util_0.Count(_format_00000, 34)
	return (*flatFormatter)(IndentFormatter(w, "").(*indentFormatter))
}

// Format implements Format interface.
func (f *flatFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	trace_util_0.Count(_format_00000, 35)
	return (*indentFormatter)(f).format(true, format, args...)
}

// OutputFormat output escape character with backslash.
func OutputFormat(s string) string {
	trace_util_0.Count(_format_00000, 36)
	var buf bytes.Buffer
	for _, old := range s {
		trace_util_0.Count(_format_00000, 38)
		if newVal, ok := replace[old]; ok {
			trace_util_0.Count(_format_00000, 40)
			buf.WriteString(newVal)
			continue
		}
		trace_util_0.Count(_format_00000, 39)
		buf.WriteRune(old)
	}

	trace_util_0.Count(_format_00000, 37)
	return buf.String()
}

var _format_00000 = "util/format/format.go"
