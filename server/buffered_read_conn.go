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

package server

import (
	"bufio"
	"github.com/pingcap/tidb/trace_util_0"
	"net"
)

const defaultReaderSize = 16 * 1024

// bufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type bufferedReadConn struct {
	net.Conn
	rb *bufio.Reader
}

func (conn bufferedReadConn) Read(b []byte) (n int, err error) {
	trace_util_0.Count(_buffered_read_conn_00000, 0)
	return conn.rb.Read(b)
}

func newBufferedReadConn(conn net.Conn) *bufferedReadConn {
	trace_util_0.Count(_buffered_read_conn_00000, 1)
	return &bufferedReadConn{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, defaultReaderSize),
	}
}

var _buffered_read_conn_00000 = "server/buffered_read_conn.go"
