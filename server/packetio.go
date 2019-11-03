// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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

package server

import (
	"bufio"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
)

const defaultWriterSize = 16 * 1024

// packetIO is a helper to read and write data in packet format.
type packetIO struct {
	bufReadConn *bufferedReadConn
	bufWriter   *bufio.Writer
	sequence    uint8
	readTimeout time.Duration
}

func newPacketIO(bufReadConn *bufferedReadConn) *packetIO {
	trace_util_0.Count(_packetio_00000, 0)
	p := &packetIO{sequence: 0}
	p.setBufferedReadConn(bufReadConn)
	return p
}

func (p *packetIO) setBufferedReadConn(bufReadConn *bufferedReadConn) {
	trace_util_0.Count(_packetio_00000, 1)
	p.bufReadConn = bufReadConn
	p.bufWriter = bufio.NewWriterSize(bufReadConn, defaultWriterSize)
}

func (p *packetIO) setReadTimeout(timeout time.Duration) {
	trace_util_0.Count(_packetio_00000, 2)
	p.readTimeout = timeout
}

func (p *packetIO) readOnePacket() ([]byte, error) {
	trace_util_0.Count(_packetio_00000, 3)
	var header [4]byte
	if p.readTimeout > 0 {
		trace_util_0.Count(_packetio_00000, 9)
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			trace_util_0.Count(_packetio_00000, 10)
			return nil, err
		}
	}
	trace_util_0.Count(_packetio_00000, 4)
	if _, err := io.ReadFull(p.bufReadConn, header[:]); err != nil {
		trace_util_0.Count(_packetio_00000, 11)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_packetio_00000, 5)
	sequence := header[3]
	if sequence != p.sequence {
		trace_util_0.Count(_packetio_00000, 12)
		return nil, errInvalidSequence.GenWithStack("invalid sequence %d != %d", sequence, p.sequence)
	}

	trace_util_0.Count(_packetio_00000, 6)
	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if p.readTimeout > 0 {
		trace_util_0.Count(_packetio_00000, 13)
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			trace_util_0.Count(_packetio_00000, 14)
			return nil, err
		}
	}
	trace_util_0.Count(_packetio_00000, 7)
	if _, err := io.ReadFull(p.bufReadConn, data); err != nil {
		trace_util_0.Count(_packetio_00000, 15)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_packetio_00000, 8)
	return data, nil
}

func (p *packetIO) readPacket() ([]byte, error) {
	trace_util_0.Count(_packetio_00000, 16)
	data, err := p.readOnePacket()
	if err != nil {
		trace_util_0.Count(_packetio_00000, 20)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_packetio_00000, 17)
	if len(data) < mysql.MaxPayloadLen {
		trace_util_0.Count(_packetio_00000, 21)
		return data, nil
	}

	// handle multi-packet
	trace_util_0.Count(_packetio_00000, 18)
	for {
		trace_util_0.Count(_packetio_00000, 22)
		buf, err := p.readOnePacket()
		if err != nil {
			trace_util_0.Count(_packetio_00000, 24)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_packetio_00000, 23)
		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			trace_util_0.Count(_packetio_00000, 25)
			break
		}
	}

	trace_util_0.Count(_packetio_00000, 19)
	return data, nil
}

// writePacket writes data that already have header
func (p *packetIO) writePacket(data []byte) error {
	trace_util_0.Count(_packetio_00000, 26)
	length := len(data) - 4

	for length >= mysql.MaxPayloadLen {
		trace_util_0.Count(_packetio_00000, 28)
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.sequence

		if n, err := p.bufWriter.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			trace_util_0.Count(_packetio_00000, 29)
			return errors.Trace(mysql.ErrBadConn)
		} else {
			trace_util_0.Count(_packetio_00000, 30)
			if n != (4 + mysql.MaxPayloadLen) {
				trace_util_0.Count(_packetio_00000, 31)
				return errors.Trace(mysql.ErrBadConn)
			} else {
				trace_util_0.Count(_packetio_00000, 32)
				{
					p.sequence++
					length -= mysql.MaxPayloadLen
					data = data[mysql.MaxPayloadLen:]
				}
			}
		}
	}

	trace_util_0.Count(_packetio_00000, 27)
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.sequence

	if n, err := p.bufWriter.Write(data); err != nil {
		trace_util_0.Count(_packetio_00000, 33)
		terror.Log(errors.Trace(err))
		return errors.Trace(mysql.ErrBadConn)
	} else {
		trace_util_0.Count(_packetio_00000, 34)
		if n != len(data) {
			trace_util_0.Count(_packetio_00000, 35)
			return errors.Trace(mysql.ErrBadConn)
		} else {
			trace_util_0.Count(_packetio_00000, 36)
			{
				p.sequence++
				return nil
			}
		}
	}
}

func (p *packetIO) flush() error {
	trace_util_0.Count(_packetio_00000, 37)
	err := p.bufWriter.Flush()
	if err != nil {
		trace_util_0.Count(_packetio_00000, 39)
		return errors.Trace(err)
	}
	trace_util_0.Count(_packetio_00000, 38)
	return err
}

var _packetio_00000 = "server/packetio.go"
