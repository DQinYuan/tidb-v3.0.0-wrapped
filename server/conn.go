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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

var (
	queryTotalCounterComSleepOK           = metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK")
	queryTotalCounterComSleepError        = metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error")
	queryTotalCounterComQuitOK            = metrics.QueryTotalCounter.WithLabelValues("Quit", "OK")
	queryTotalCounterComQuitError         = metrics.QueryTotalCounter.WithLabelValues("Quit", "Error")
	queryTotalCounterComInitDBOK          = metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK")
	queryTotalCounterComInitDBError       = metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error")
	queryTotalCounterComQueryOK           = metrics.QueryTotalCounter.WithLabelValues("Query", "OK")
	queryTotalCounterComQueryError        = metrics.QueryTotalCounter.WithLabelValues("Query", "Error")
	queryTotalCounterComPingOK            = metrics.QueryTotalCounter.WithLabelValues("Ping", "OK")
	queryTotalCounterComPingError         = metrics.QueryTotalCounter.WithLabelValues("Ping", "Error")
	queryTotalCounterComFieldListOK       = metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK")
	queryTotalCounterComFieldListError    = metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error")
	queryTotalCounterComPrepareOK         = metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK")
	queryTotalCounterComPrepareError      = metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error")
	queryTotalCounterComExecuteOK         = metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "OK")
	queryTotalCounterComExecuteError      = metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "Error")
	queryTotalCounterComFetchOK           = metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK")
	queryTotalCounterComFetchError        = metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error")
	queryTotalCounterComCloseOK           = metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK")
	queryTotalCounterComCloseError        = metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error")
	queryTotalCounterComSendLongDataOK    = metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK")
	queryTotalCounterComSendLongDataError = metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error")
	queryTotalCounterComResetOK           = metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK")
	queryTotalCounterComResetError        = metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error")
	queryTotalCounterComSetOptionOK       = metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK")
	queryTotalCounterComSetOptionError    = metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error")

	queryDurationHistogramUse      = metrics.QueryDurationHistogram.WithLabelValues("Use")
	queryDurationHistogramShow     = metrics.QueryDurationHistogram.WithLabelValues("Show")
	queryDurationHistogramBegin    = metrics.QueryDurationHistogram.WithLabelValues("Begin")
	queryDurationHistogramCommit   = metrics.QueryDurationHistogram.WithLabelValues("Commit")
	queryDurationHistogramRollback = metrics.QueryDurationHistogram.WithLabelValues("Rollback")
	queryDurationHistogramInsert   = metrics.QueryDurationHistogram.WithLabelValues("Insert")
	queryDurationHistogramReplace  = metrics.QueryDurationHistogram.WithLabelValues("Replace")
	queryDurationHistogramDelete   = metrics.QueryDurationHistogram.WithLabelValues("Delete")
	queryDurationHistogramUpdate   = metrics.QueryDurationHistogram.WithLabelValues("Update")
	queryDurationHistogramSelect   = metrics.QueryDurationHistogram.WithLabelValues("Select")
	queryDurationHistogramExecute  = metrics.QueryDurationHistogram.WithLabelValues("Execute")
	queryDurationHistogramSet      = metrics.QueryDurationHistogram.WithLabelValues("Set")
	queryDurationHistogramGeneral  = metrics.QueryDurationHistogram.WithLabelValues(metrics.LblGeneral)
)

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	trace_util_0.Count(_conn_00000, 0)
	return &clientConn{
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
	}
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	collation    uint8             // collation used by client, may be different from the collation used by database.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	lastCmd      string            // latest sql query string, currently used for logging error.
	ctx          QueryCtx          // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	status       int32             // dispatching/reading/shutdown/waitshutdown
	peerHost     string            // peer host
	peerPort     string            // peer port
	lastCode     uint16            // last error code
}

func (cc *clientConn) String() string {
	trace_util_0.Count(_conn_00000, 1)
	collationStr := mysql.Collations[cc.collation]
	return fmt.Sprintf("id:%d, addr:%s status:%d, collation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	)
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
func (cc *clientConn) handshake(ctx context.Context) error {
	trace_util_0.Count(_conn_00000, 2)
	if err := cc.writeInitialHandshake(); err != nil {
		trace_util_0.Count(_conn_00000, 7)
		return err
	}
	trace_util_0.Count(_conn_00000, 3)
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		trace_util_0.Count(_conn_00000, 8)
		err1 := cc.writeError(err)
		if err1 != nil {
			trace_util_0.Count(_conn_00000, 10)
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		trace_util_0.Count(_conn_00000, 9)
		return err
	}
	trace_util_0.Count(_conn_00000, 4)
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		trace_util_0.Count(_conn_00000, 11)
		data = dumpUint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	trace_util_0.Count(_conn_00000, 5)
	err := cc.writePacket(data)
	cc.pkt.sequence = 0
	if err != nil {
		trace_util_0.Count(_conn_00000, 12)
		return err
	}

	trace_util_0.Count(_conn_00000, 6)
	return cc.flush()
}

func (cc *clientConn) Close() error {
	trace_util_0.Count(_conn_00000, 13)
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	return closeConn(cc, connections)
}

func closeConn(cc *clientConn, connections int) error {
	trace_util_0.Count(_conn_00000, 14)
	metrics.ConnGauge.Set(float64(connections))
	err := cc.bufReadConn.Close()
	terror.Log(err)
	if cc.ctx != nil {
		trace_util_0.Count(_conn_00000, 16)
		return cc.ctx.Close()
	}
	trace_util_0.Count(_conn_00000, 15)
	return nil
}

func (cc *clientConn) closeWithoutLock() error {
	trace_util_0.Count(_conn_00000, 17)
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc, len(cc.server.clients))
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake() error {
	trace_util_0.Count(_conn_00000, 18)
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.collation == 0 {
		trace_util_0.Count(_conn_00000, 21)
		cc.collation = uint8(mysql.DefaultCollationID)
	}
	trace_util_0.Count(_conn_00000, 19)
	data = append(data, cc.collation)
	// status
	data = dumpUint16(data, mysql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		trace_util_0.Count(_conn_00000, 22)
		return err
	}
	trace_util_0.Count(_conn_00000, 20)
	return cc.flush()
}

func (cc *clientConn) readPacket() ([]byte, error) {
	trace_util_0.Count(_conn_00000, 23)
	return cc.pkt.readPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	trace_util_0.Count(_conn_00000, 24)
	return cc.pkt.writePacket(data)
}

// getSessionVarsWaitTimeout get session variable wait_timeout
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	trace_util_0.Count(_conn_00000, 25)
	valStr, exists := cc.ctx.GetSessionVars().GetSystemVar(variable.WaitTimeout)
	if !exists {
		trace_util_0.Count(_conn_00000, 28)
		return variable.DefWaitTimeout
	}
	trace_util_0.Count(_conn_00000, 26)
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_conn_00000, 29)
		logutil.Logger(ctx).Warn("get sysval wait_timeout error, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return variable.DefWaitTimeout
	}
	trace_util_0.Count(_conn_00000, 27)
	return waitTimeout
}

type handshakeResponse41 struct {
	Capability uint32
	Collation  uint8
	User       string
	DBName     string
	Auth       []byte
	Attrs      map[string]string
}

// parseOldHandshakeResponseHeader parses the old version handshake header HandshakeResponse320
func parseOldHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	trace_util_0.Count(_conn_00000, 30)
	// Ensure there are enough data to read:
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse320
	logutil.Logger(ctx).Debug("try to parse hanshake response as Protocol::HandshakeResponse320", zap.ByteString("packetData", data))
	if len(data) < 2+3 {
		trace_util_0.Count(_conn_00000, 32)
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}
	trace_util_0.Count(_conn_00000, 31)
	offset := 0
	// capability
	capability := binary.LittleEndian.Uint16(data[:2])
	packet.Capability = uint32(capability)

	// be compatible with Protocol::HandshakeResponse41
	packet.Capability = packet.Capability | mysql.ClientProtocol41

	offset += 2
	// skip max packet size
	offset += 3
	// usa default CharsetID
	packet.Collation = mysql.CollationNames["utf8mb4_general_ci"]

	return offset, nil
}

// parseOldHandshakeResponseBody parse the HandshakeResponse for Protocol::HandshakeResponse320 (except the common header part).
func parseOldHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	trace_util_0.Count(_conn_00000, 33)
	defer func() {
		trace_util_0.Count(_conn_00000, 36)
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			trace_util_0.Count(_conn_00000, 37)
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	trace_util_0.Count(_conn_00000, 34)
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		trace_util_0.Count(_conn_00000, 38)
		if len(data[offset:]) > 0 {
			trace_util_0.Count(_conn_00000, 40)
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
		trace_util_0.Count(_conn_00000, 39)
		if len(data[offset:]) > 0 {
			trace_util_0.Count(_conn_00000, 41)
			packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	} else {
		trace_util_0.Count(_conn_00000, 42)
		{
			packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	}

	trace_util_0.Count(_conn_00000, 35)
	return nil
}

// parseHandshakeResponseHeader parses the common header of SSLRequest and HandshakeResponse41.
func parseHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	trace_util_0.Count(_conn_00000, 43)
	// Ensure there are enough data to read:
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if len(data) < 4+4+1+23 {
		trace_util_0.Count(_conn_00000, 45)
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}

	trace_util_0.Count(_conn_00000, 44)
	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// parseHandshakeResponseBody parse the HandshakeResponse (except the common header part).
func parseHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	trace_util_0.Count(_conn_00000, 46)
	defer func() {
		trace_util_0.Count(_conn_00000, 52)
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			trace_util_0.Count(_conn_00000, 53)
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	trace_util_0.Count(_conn_00000, 47)
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
		trace_util_0.Count(_conn_00000, 54)
		// MySQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/mysql/mysql-server/blob/5.7/sql-common/client.c#L3478
		num, null, off := parseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			trace_util_0.Count(_conn_00000, 55)
			packet.Auth = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else {
		trace_util_0.Count(_conn_00000, 56)
		if packet.Capability&mysql.ClientSecureConnection > 0 {
			trace_util_0.Count(_conn_00000, 57)
			// auth length and auth
			authLen := int(data[offset])
			offset++
			packet.Auth = data[offset : offset+authLen]
			offset += authLen
		} else {
			trace_util_0.Count(_conn_00000, 58)
			{
				packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
				offset += len(packet.Auth) + 1
			}
		}
	}

	trace_util_0.Count(_conn_00000, 48)
	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		trace_util_0.Count(_conn_00000, 59)
		if len(data[offset:]) > 0 {
			trace_util_0.Count(_conn_00000, 60)
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	trace_util_0.Count(_conn_00000, 49)
	if packet.Capability&mysql.ClientPluginAuth > 0 {
		trace_util_0.Count(_conn_00000, 61)
		// TODO: Support mysql.ClientPluginAuth, skip it now
		idx := bytes.IndexByte(data[offset:], 0)
		offset = offset + idx + 1
	}

	trace_util_0.Count(_conn_00000, 50)
	if packet.Capability&mysql.ClientConnectAtts > 0 {
		trace_util_0.Count(_conn_00000, 62)
		if len(data[offset:]) == 0 {
			trace_util_0.Count(_conn_00000, 64)
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		trace_util_0.Count(_conn_00000, 63)
		if num, null, off := parseLengthEncodedInt(data[offset:]); !null {
			trace_util_0.Count(_conn_00000, 65)
			offset += off
			row := data[offset : offset+int(num)]
			attrs, err := parseAttrs(row)
			if err != nil {
				trace_util_0.Count(_conn_00000, 67)
				logutil.Logger(ctx).Warn("parse attrs failed", zap.Error(err))
				return nil
			}
			trace_util_0.Count(_conn_00000, 66)
			packet.Attrs = attrs
		}
	}

	trace_util_0.Count(_conn_00000, 51)
	return nil
}

func parseAttrs(data []byte) (map[string]string, error) {
	trace_util_0.Count(_conn_00000, 68)
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		trace_util_0.Count(_conn_00000, 70)
		key, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			trace_util_0.Count(_conn_00000, 73)
			return attrs, err
		}
		trace_util_0.Count(_conn_00000, 71)
		pos += off
		value, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			trace_util_0.Count(_conn_00000, 74)
			return attrs, err
		}
		trace_util_0.Count(_conn_00000, 72)
		pos += off

		attrs[string(key)] = string(value)
	}
	trace_util_0.Count(_conn_00000, 69)
	return attrs, nil
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	trace_util_0.Count(_conn_00000, 75)
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		trace_util_0.Count(_conn_00000, 83)
		return err
	}

	trace_util_0.Count(_conn_00000, 76)
	isOldVersion := false

	var resp handshakeResponse41
	var pos int

	if len(data) < 2 {
		trace_util_0.Count(_conn_00000, 84)
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return mysql.ErrMalformPacket
	}

	trace_util_0.Count(_conn_00000, 77)
	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 > 0 {
		trace_util_0.Count(_conn_00000, 85)
		pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
	} else {
		trace_util_0.Count(_conn_00000, 86)
		{
			pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
			isOldVersion = true
		}
	}

	trace_util_0.Count(_conn_00000, 78)
	if err != nil {
		trace_util_0.Count(_conn_00000, 87)
		return err
	}

	trace_util_0.Count(_conn_00000, 79)
	if (resp.Capability&mysql.ClientSSL > 0) && cc.server.tlsConfig != nil {
		trace_util_0.Count(_conn_00000, 88)
		// The packet is a SSLRequest, let's switch to TLS.
		if err = cc.upgradeToTLS(cc.server.tlsConfig); err != nil {
			trace_util_0.Count(_conn_00000, 92)
			return err
		}
		// Read the following HandshakeResponse packet.
		trace_util_0.Count(_conn_00000, 89)
		data, err = cc.readPacket()
		if err != nil {
			trace_util_0.Count(_conn_00000, 93)
			return err
		}
		trace_util_0.Count(_conn_00000, 90)
		if isOldVersion {
			trace_util_0.Count(_conn_00000, 94)
			pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
		} else {
			trace_util_0.Count(_conn_00000, 95)
			{
				pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
			}
		}
		trace_util_0.Count(_conn_00000, 91)
		if err != nil {
			trace_util_0.Count(_conn_00000, 96)
			return err
		}
	}

	// Read the remaining part of the packet.
	trace_util_0.Count(_conn_00000, 80)
	if isOldVersion {
		trace_util_0.Count(_conn_00000, 97)
		err = parseOldHandshakeResponseBody(ctx, &resp, data, pos)
	} else {
		trace_util_0.Count(_conn_00000, 98)
		{
			err = parseHandshakeResponseBody(ctx, &resp, data, pos)
		}
	}
	trace_util_0.Count(_conn_00000, 81)
	if err != nil {
		trace_util_0.Count(_conn_00000, 99)
		return err
	}

	trace_util_0.Count(_conn_00000, 82)
	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs

	err = cc.openSessionAndDoAuth(resp.Auth)
	return err
}

func (cc *clientConn) openSessionAndDoAuth(authData []byte) error {
	trace_util_0.Count(_conn_00000, 100)
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		trace_util_0.Count(_conn_00000, 107)
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	trace_util_0.Count(_conn_00000, 101)
	var err error
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, cc.collation, cc.dbname, tlsStatePtr)
	if err != nil {
		trace_util_0.Count(_conn_00000, 108)
		return err
	}
	trace_util_0.Count(_conn_00000, 102)
	hasPassword := "YES"
	if len(authData) == 0 {
		trace_util_0.Count(_conn_00000, 109)
		hasPassword = "NO"
	}
	trace_util_0.Count(_conn_00000, 103)
	host, err := cc.PeerHost(hasPassword)
	if err != nil {
		trace_util_0.Count(_conn_00000, 110)
		return err
	}
	trace_util_0.Count(_conn_00000, 104)
	if !cc.ctx.Auth(&auth.UserIdentity{Username: cc.user, Hostname: host}, authData, cc.salt) {
		trace_util_0.Count(_conn_00000, 111)
		return errAccessDenied.GenWithStackByArgs(cc.user, host, hasPassword)
	}
	trace_util_0.Count(_conn_00000, 105)
	if cc.dbname != "" {
		trace_util_0.Count(_conn_00000, 112)
		err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			trace_util_0.Count(_conn_00000, 113)
			return err
		}
	}
	trace_util_0.Count(_conn_00000, 106)
	cc.ctx.SetSessionManager(cc.server)
	return nil
}

func (cc *clientConn) PeerHost(hasPassword string) (host string, err error) {
	trace_util_0.Count(_conn_00000, 114)
	if len(cc.peerHost) > 0 {
		trace_util_0.Count(_conn_00000, 118)
		return cc.peerHost, nil
	}
	trace_util_0.Count(_conn_00000, 115)
	host = variable.DefHostname
	if cc.server.isUnixSocket() {
		trace_util_0.Count(_conn_00000, 119)
		cc.peerHost = host
		return
	}
	trace_util_0.Count(_conn_00000, 116)
	addr := cc.bufReadConn.RemoteAddr().String()
	var port string
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		trace_util_0.Count(_conn_00000, 120)
		err = errAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	trace_util_0.Count(_conn_00000, 117)
	cc.peerHost = host
	cc.peerPort = port
	return
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	trace_util_0.Count(_conn_00000, 121)
	const size = 4096
	defer func() {
		trace_util_0.Count(_conn_00000, 123)
		r := recover()
		if r != nil {
			trace_util_0.Count(_conn_00000, 125)
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.String("lastCmd", cc.lastCmd),
				zap.Reflect("err", r),
				zap.String("stack", string(buf)),
			)
			metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		trace_util_0.Count(_conn_00000, 124)
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			trace_util_0.Count(_conn_00000, 126)
			err := cc.Close()
			terror.Log(err)
		}
	}()
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	trace_util_0.Count(_conn_00000, 122)
	for {
		trace_util_0.Count(_conn_00000, 127)
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) {
			trace_util_0.Count(_conn_00000, 132)
			return
		}

		trace_util_0.Count(_conn_00000, 128)
		cc.alloc.Reset()
		// close connection when idle time is more than wait_timout
		waitTimeout := cc.getSessionVarsWaitTimeout(ctx)
		cc.pkt.setReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			trace_util_0.Count(_conn_00000, 133)
			if terror.ErrorNotEqual(err, io.EOF) {
				trace_util_0.Count(_conn_00000, 135)
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					trace_util_0.Count(_conn_00000, 136)
					idleTime := time.Since(start)
					logutil.Logger(ctx).Info("read packet timeout, close this connection",
						zap.Duration("idle", idleTime),
						zap.Uint64("waitTimeout", waitTimeout),
					)
				} else {
					trace_util_0.Count(_conn_00000, 137)
					{
						errStack := errors.ErrorStack(err)
						if !strings.Contains(errStack, "use of closed network connection") {
							trace_util_0.Count(_conn_00000, 138)
							logutil.Logger(ctx).Error("read packet failed, close this connection", zap.Error(err))
						}
					}
				}
			}
			trace_util_0.Count(_conn_00000, 134)
			return
		}

		trace_util_0.Count(_conn_00000, 129)
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			trace_util_0.Count(_conn_00000, 139)
			return
		}

		trace_util_0.Count(_conn_00000, 130)
		startTime := time.Now()
		if err = cc.dispatch(ctx, data); err != nil {
			trace_util_0.Count(_conn_00000, 140)
			if terror.ErrorEqual(err, io.EOF) {
				trace_util_0.Count(_conn_00000, 142)
				cc.addMetrics(data[0], startTime, nil)
				return
			} else {
				trace_util_0.Count(_conn_00000, 143)
				if terror.ErrResultUndetermined.Equal(err) {
					trace_util_0.Count(_conn_00000, 144)
					logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
					return
				} else {
					trace_util_0.Count(_conn_00000, 145)
					if terror.ErrCritical.Equal(err) {
						trace_util_0.Count(_conn_00000, 146)
						logutil.Logger(ctx).Error("critical error, stop the server listener", zap.Error(err))
						metrics.CriticalErrorCounter.Add(1)
						select {
						case cc.server.stopListenerCh <- struct{}{}:
							trace_util_0.Count(_conn_00000, 148)
						default:
							trace_util_0.Count(_conn_00000, 149)
						}
						trace_util_0.Count(_conn_00000, 147)
						return
					}
				}
			}
			trace_util_0.Count(_conn_00000, 141)
			logutil.Logger(ctx).Warn("dispatch error",
				zap.String("connInfo", cc.String()),
				zap.String("sql", queryStrForLog(string(data[1:]))),
				zap.String("err", errStrForLog(err)),
			)
			err1 := cc.writeError(err)
			terror.Log(err1)
		}
		trace_util_0.Count(_conn_00000, 131)
		cc.addMetrics(data[0], startTime, err)
		cc.pkt.sequence = 0
	}
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
func (cc *clientConn) ShutdownOrNotify() bool {
	trace_util_0.Count(_conn_00000, 150)
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		trace_util_0.Count(_conn_00000, 153)
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	trace_util_0.Count(_conn_00000, 151)
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		trace_util_0.Count(_conn_00000, 154)
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the client will detect it
	// and then exit.
	trace_util_0.Count(_conn_00000, 152)
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

func queryStrForLog(query string) string {
	trace_util_0.Count(_conn_00000, 155)
	const size = 4096
	if len(query) > size {
		trace_util_0.Count(_conn_00000, 157)
		return query[:size] + fmt.Sprintf("(len: %d)", len(query))
	}
	trace_util_0.Count(_conn_00000, 156)
	return query
}

func errStrForLog(err error) string {
	trace_util_0.Count(_conn_00000, 158)
	if kv.ErrKeyExists.Equal(err) {
		trace_util_0.Count(_conn_00000, 160)
		// Do not log stack for duplicated entry error.
		return err.Error()
	}
	trace_util_0.Count(_conn_00000, 159)
	return errors.ErrorStack(err)
}

func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {
	trace_util_0.Count(_conn_00000, 161)
	switch cmd {
	case mysql.ComSleep:
		trace_util_0.Count(_conn_00000, 164)
		if err != nil {
			trace_util_0.Count(_conn_00000, 179)
			queryTotalCounterComSleepError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 180)
			{
				queryTotalCounterComSleepOK.Inc()
			}
		}
	case mysql.ComQuit:
		trace_util_0.Count(_conn_00000, 165)
		if err != nil {
			trace_util_0.Count(_conn_00000, 181)
			queryTotalCounterComQuitError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 182)
			{
				queryTotalCounterComQuitOK.Inc()
			}
		}
	case mysql.ComQuery:
		trace_util_0.Count(_conn_00000, 166)
		if cc.ctx.Value(sessionctx.LastExecuteDDL) != nil {
			trace_util_0.Count(_conn_00000, 183)
			// Don't take DDL execute time into account.
			// It's already recorded by other metrics in ddl package.
			return
		}
		trace_util_0.Count(_conn_00000, 167)
		if err != nil {
			trace_util_0.Count(_conn_00000, 184)
			queryTotalCounterComQueryError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 185)
			{
				queryTotalCounterComQueryOK.Inc()
			}
		}
	case mysql.ComPing:
		trace_util_0.Count(_conn_00000, 168)
		if err != nil {
			trace_util_0.Count(_conn_00000, 186)
			queryTotalCounterComPingError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 187)
			{
				queryTotalCounterComPingOK.Inc()
			}
		}
	case mysql.ComInitDB:
		trace_util_0.Count(_conn_00000, 169)
		if err != nil {
			trace_util_0.Count(_conn_00000, 188)
			queryTotalCounterComInitDBError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 189)
			{
				queryTotalCounterComInitDBOK.Inc()
			}
		}
	case mysql.ComFieldList:
		trace_util_0.Count(_conn_00000, 170)
		if err != nil {
			trace_util_0.Count(_conn_00000, 190)
			queryTotalCounterComFieldListError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 191)
			{
				queryTotalCounterComFieldListOK.Inc()
			}
		}
	case mysql.ComStmtPrepare:
		trace_util_0.Count(_conn_00000, 171)
		if err != nil {
			trace_util_0.Count(_conn_00000, 192)
			queryTotalCounterComPrepareError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 193)
			{
				queryTotalCounterComPrepareOK.Inc()
			}
		}
	case mysql.ComStmtExecute:
		trace_util_0.Count(_conn_00000, 172)
		if err != nil {
			trace_util_0.Count(_conn_00000, 194)
			queryTotalCounterComExecuteError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 195)
			{
				queryTotalCounterComExecuteOK.Inc()
			}
		}
	case mysql.ComStmtFetch:
		trace_util_0.Count(_conn_00000, 173)
		if err != nil {
			trace_util_0.Count(_conn_00000, 196)
			queryTotalCounterComFetchError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 197)
			{
				queryTotalCounterComFetchOK.Inc()
			}
		}
	case mysql.ComStmtClose:
		trace_util_0.Count(_conn_00000, 174)
		if err != nil {
			trace_util_0.Count(_conn_00000, 198)
			queryTotalCounterComCloseError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 199)
			{
				queryTotalCounterComCloseOK.Inc()
			}
		}
	case mysql.ComStmtSendLongData:
		trace_util_0.Count(_conn_00000, 175)
		if err != nil {
			trace_util_0.Count(_conn_00000, 200)
			queryTotalCounterComSendLongDataError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 201)
			{
				queryTotalCounterComSendLongDataOK.Inc()
			}
		}
	case mysql.ComStmtReset:
		trace_util_0.Count(_conn_00000, 176)
		if err != nil {
			trace_util_0.Count(_conn_00000, 202)
			queryTotalCounterComResetError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 203)
			{
				queryTotalCounterComResetOK.Inc()
			}
		}
	case mysql.ComSetOption:
		trace_util_0.Count(_conn_00000, 177)
		if err != nil {
			trace_util_0.Count(_conn_00000, 204)
			queryTotalCounterComSetOptionError.Inc()
		} else {
			trace_util_0.Count(_conn_00000, 205)
			{
				queryTotalCounterComSetOptionOK.Inc()
			}
		}
	default:
		trace_util_0.Count(_conn_00000, 178)
		label := strconv.Itoa(int(cmd))
		if err != nil {
			trace_util_0.Count(_conn_00000, 206)
			metrics.QueryTotalCounter.WithLabelValues(label, "ERROR").Inc()
		} else {
			trace_util_0.Count(_conn_00000, 207)
			{
				metrics.QueryTotalCounter.WithLabelValues(label, "OK").Inc()
			}
		}
	}
	trace_util_0.Count(_conn_00000, 162)
	stmtType := cc.ctx.GetSessionVars().StmtCtx.StmtType
	sqlType := metrics.LblGeneral
	if stmtType != "" {
		trace_util_0.Count(_conn_00000, 208)
		sqlType = stmtType
	}

	trace_util_0.Count(_conn_00000, 163)
	switch sqlType {
	case "Use":
		trace_util_0.Count(_conn_00000, 209)
		queryDurationHistogramUse.Observe(time.Since(startTime).Seconds())
	case "Show":
		trace_util_0.Count(_conn_00000, 210)
		queryDurationHistogramShow.Observe(time.Since(startTime).Seconds())
	case "Begin":
		trace_util_0.Count(_conn_00000, 211)
		queryDurationHistogramBegin.Observe(time.Since(startTime).Seconds())
	case "Commit":
		trace_util_0.Count(_conn_00000, 212)
		queryDurationHistogramCommit.Observe(time.Since(startTime).Seconds())
	case "Rollback":
		trace_util_0.Count(_conn_00000, 213)
		queryDurationHistogramRollback.Observe(time.Since(startTime).Seconds())
	case "Insert":
		trace_util_0.Count(_conn_00000, 214)
		queryDurationHistogramInsert.Observe(time.Since(startTime).Seconds())
	case "Replace":
		trace_util_0.Count(_conn_00000, 215)
		queryDurationHistogramReplace.Observe(time.Since(startTime).Seconds())
	case "Delete":
		trace_util_0.Count(_conn_00000, 216)
		queryDurationHistogramDelete.Observe(time.Since(startTime).Seconds())
	case "Update":
		trace_util_0.Count(_conn_00000, 217)
		queryDurationHistogramUpdate.Observe(time.Since(startTime).Seconds())
	case "Select":
		trace_util_0.Count(_conn_00000, 218)
		queryDurationHistogramSelect.Observe(time.Since(startTime).Seconds())
	case "Execute":
		trace_util_0.Count(_conn_00000, 219)
		queryDurationHistogramExecute.Observe(time.Since(startTime).Seconds())
	case "Set":
		trace_util_0.Count(_conn_00000, 220)
		queryDurationHistogramSet.Observe(time.Since(startTime).Seconds())
	case metrics.LblGeneral:
		trace_util_0.Count(_conn_00000, 221)
		queryDurationHistogramGeneral.Observe(time.Since(startTime).Seconds())
	default:
		trace_util_0.Count(_conn_00000, 222)
		metrics.QueryDurationHistogram.WithLabelValues(sqlType).Observe(time.Since(startTime).Seconds())
	}
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	trace_util_0.Count(_conn_00000, 223)
	span := opentracing.StartSpan("server.dispatch")

	t := time.Now()
	cmd := data[0]
	data = data[1:]
	cc.lastCmd = string(hack.String(data))
	token := cc.server.getToken()
	defer func() {
		trace_util_0.Count(_conn_00000, 227)
		cc.ctx.SetProcessInfo("", t, mysql.ComSleep)
		cc.server.releaseToken(token)
		span.Finish()
	}()

	trace_util_0.Count(_conn_00000, 224)
	vars := cc.ctx.GetSessionVars()
	atomic.StoreUint32(&vars.Killed, 0)
	if cmd < mysql.ComEnd {
		trace_util_0.Count(_conn_00000, 228)
		cc.ctx.SetCommandValue(cmd)
	}

	trace_util_0.Count(_conn_00000, 225)
	dataStr := string(hack.String(data))
	switch cmd {
	case mysql.ComPing, mysql.ComStmtClose, mysql.ComStmtSendLongData, mysql.ComStmtReset,
		mysql.ComSetOption, mysql.ComChangeUser:
		trace_util_0.Count(_conn_00000, 229)
		cc.ctx.SetProcessInfo("", t, cmd)
	case mysql.ComInitDB:
		trace_util_0.Count(_conn_00000, 230)
		cc.ctx.SetProcessInfo("use "+dataStr, t, cmd)
	}

	trace_util_0.Count(_conn_00000, 226)
	switch cmd {
	case mysql.ComSleep:
		trace_util_0.Count(_conn_00000, 231)
		// TODO: According to mysql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil
	case mysql.ComQuit:
		trace_util_0.Count(_conn_00000, 232)
		return io.EOF
	case mysql.ComQuery:
		trace_util_0.Count(_conn_00000, 233) // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			trace_util_0.Count(_conn_00000, 248)
			data = data[:len(data)-1]
			dataStr = string(hack.String(data))
		}
		trace_util_0.Count(_conn_00000, 234)
		return cc.handleQuery(ctx, dataStr)
	case mysql.ComPing:
		trace_util_0.Count(_conn_00000, 235)
		return cc.writeOK()
	case mysql.ComInitDB:
		trace_util_0.Count(_conn_00000, 236)
		if err := cc.useDB(ctx, dataStr); err != nil {
			trace_util_0.Count(_conn_00000, 249)
			return err
		}
		trace_util_0.Count(_conn_00000, 237)
		return cc.writeOK()
	case mysql.ComFieldList:
		trace_util_0.Count(_conn_00000, 238)
		return cc.handleFieldList(dataStr)
	case mysql.ComStmtPrepare:
		trace_util_0.Count(_conn_00000, 239)
		return cc.handleStmtPrepare(dataStr)
	case mysql.ComStmtExecute:
		trace_util_0.Count(_conn_00000, 240)
		return cc.handleStmtExecute(ctx, data)
	case mysql.ComStmtFetch:
		trace_util_0.Count(_conn_00000, 241)
		return cc.handleStmtFetch(ctx, data)
	case mysql.ComStmtClose:
		trace_util_0.Count(_conn_00000, 242)
		return cc.handleStmtClose(data)
	case mysql.ComStmtSendLongData:
		trace_util_0.Count(_conn_00000, 243)
		return cc.handleStmtSendLongData(data)
	case mysql.ComStmtReset:
		trace_util_0.Count(_conn_00000, 244)
		return cc.handleStmtReset(data)
	case mysql.ComSetOption:
		trace_util_0.Count(_conn_00000, 245)
		return cc.handleSetOption(data)
	case mysql.ComChangeUser:
		trace_util_0.Count(_conn_00000, 246)
		return cc.handleChangeUser(ctx, data)
	default:
		trace_util_0.Count(_conn_00000, 247)
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
}

func (cc *clientConn) useDB(ctx context.Context, db string) (err error) {
	trace_util_0.Count(_conn_00000, 250)
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = cc.ctx.Execute(ctx, "use `"+db+"`")
	if err != nil {
		trace_util_0.Count(_conn_00000, 252)
		return err
	}
	trace_util_0.Count(_conn_00000, 251)
	cc.dbname = db
	return
}

func (cc *clientConn) flush() error {
	trace_util_0.Count(_conn_00000, 253)
	return cc.pkt.flush()
}

func (cc *clientConn) writeOK() error {
	trace_util_0.Count(_conn_00000, 254)
	msg := cc.ctx.LastMessage()
	enclen := 0
	if len(msg) > 0 {
		trace_util_0.Count(_conn_00000, 259)
		enclen = lengthEncodedIntSize(uint64(len(msg))) + len(msg)
	}

	trace_util_0.Count(_conn_00000, 255)
	data := cc.alloc.AllocWithLen(4, 32+enclen)
	data = append(data, mysql.OKHeader)
	data = dumpLengthEncodedInt(data, cc.ctx.AffectedRows())
	data = dumpLengthEncodedInt(data, cc.ctx.LastInsertID())
	if cc.capability&mysql.ClientProtocol41 > 0 {
		trace_util_0.Count(_conn_00000, 260)
		data = dumpUint16(data, cc.ctx.Status())
		data = dumpUint16(data, cc.ctx.WarningCount())
	}
	trace_util_0.Count(_conn_00000, 256)
	if enclen > 0 {
		trace_util_0.Count(_conn_00000, 261)
		// although MySQL manual says the info message is string<EOF>(https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html),
		// it is actually string<lenenc>
		data = dumpLengthEncodedString(data, []byte(msg))
	}

	trace_util_0.Count(_conn_00000, 257)
	err := cc.writePacket(data)
	if err != nil {
		trace_util_0.Count(_conn_00000, 262)
		return err
	}

	trace_util_0.Count(_conn_00000, 258)
	return cc.flush()
}

func (cc *clientConn) writeError(e error) error {
	trace_util_0.Count(_conn_00000, 263)
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		trace_util_0.Count(_conn_00000, 267)
		m = te.ToSQLError()
	} else {
		trace_util_0.Count(_conn_00000, 268)
		{
			m = mysql.NewErrf(mysql.ErrUnknown, "%s", e.Error())
		}
	}

	trace_util_0.Count(_conn_00000, 264)
	cc.lastCode = m.Code
	data := cc.alloc.AllocWithLen(4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&mysql.ClientProtocol41 > 0 {
		trace_util_0.Count(_conn_00000, 269)
		data = append(data, '#')
		data = append(data, m.State...)
	}

	trace_util_0.Count(_conn_00000, 265)
	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		trace_util_0.Count(_conn_00000, 270)
		return err
	}
	trace_util_0.Count(_conn_00000, 266)
	return cc.flush()
}

// writeEOF writes an EOF packet.
// Note this function won't flush the stream because maybe there are more
// packets following it.
// serverStatus, a flag bit represents server information
// in the packet.
func (cc *clientConn) writeEOF(serverStatus uint16) error {
	trace_util_0.Count(_conn_00000, 271)
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		trace_util_0.Count(_conn_00000, 273)
		data = dumpUint16(data, cc.ctx.WarningCount())
		status := cc.ctx.Status()
		status |= serverStatus
		data = dumpUint16(data, status)
	}

	trace_util_0.Count(_conn_00000, 272)
	err := cc.writePacket(data)
	return err
}

func (cc *clientConn) writeReq(filePath string) error {
	trace_util_0.Count(_conn_00000, 274)
	data := cc.alloc.AllocWithLen(4, 5+len(filePath))
	data = append(data, mysql.LocalInFileHeader)
	data = append(data, filePath...)

	err := cc.writePacket(data)
	if err != nil {
		trace_util_0.Count(_conn_00000, 276)
		return err
	}

	trace_util_0.Count(_conn_00000, 275)
	return cc.flush()
}

var defaultLoadDataBatchCnt uint64 = 20000

func insertDataWithCommit(ctx context.Context, prevData, curData []byte, loadDataInfo *executor.LoadDataInfo) ([]byte, error) {
	trace_util_0.Count(_conn_00000, 277)
	var err error
	var reachLimit bool
	for {
		trace_util_0.Count(_conn_00000, 279)
		prevData, reachLimit, err = loadDataInfo.InsertData(prevData, curData)
		if err != nil {
			trace_util_0.Count(_conn_00000, 284)
			return nil, err
		}
		trace_util_0.Count(_conn_00000, 280)
		if !reachLimit {
			trace_util_0.Count(_conn_00000, 285)
			break
		}
		trace_util_0.Count(_conn_00000, 281)
		if err = loadDataInfo.Ctx.StmtCommit(); err != nil {
			trace_util_0.Count(_conn_00000, 286)
			return nil, err
		}
		// Make sure that there are no retries when committing.
		trace_util_0.Count(_conn_00000, 282)
		if err = loadDataInfo.Ctx.RefreshTxnCtx(ctx); err != nil {
			trace_util_0.Count(_conn_00000, 287)
			return nil, err
		}
		trace_util_0.Count(_conn_00000, 283)
		curData = prevData
		prevData = nil
	}
	trace_util_0.Count(_conn_00000, 278)
	return prevData, nil
}

// handleLoadData does the additional work after processing the 'load data' query.
// It sends client a file path, then reads the file content from client, inserts data into database.
func (cc *clientConn) handleLoadData(ctx context.Context, loadDataInfo *executor.LoadDataInfo) error {
	trace_util_0.Count(_conn_00000, 288)
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&mysql.ClientLocalFiles == 0 {
		trace_util_0.Count(_conn_00000, 296)
		return errNotAllowedCommand
	}
	trace_util_0.Count(_conn_00000, 289)
	if loadDataInfo == nil {
		trace_util_0.Count(_conn_00000, 297)
		return errors.New("load data info is empty")
	}

	trace_util_0.Count(_conn_00000, 290)
	err := cc.writeReq(loadDataInfo.Path)
	if err != nil {
		trace_util_0.Count(_conn_00000, 298)
		return err
	}

	trace_util_0.Count(_conn_00000, 291)
	var shouldBreak bool
	var prevData, curData []byte
	// TODO: Make the loadDataRowCnt settable.
	loadDataInfo.SetMaxRowsInBatch(defaultLoadDataBatchCnt)
	err = loadDataInfo.Ctx.NewTxn(ctx)
	if err != nil {
		trace_util_0.Count(_conn_00000, 299)
		return err
	}
	trace_util_0.Count(_conn_00000, 292)
	for {
		trace_util_0.Count(_conn_00000, 300)
		curData, err = cc.readPacket()
		if err != nil {
			trace_util_0.Count(_conn_00000, 304)
			if terror.ErrorNotEqual(err, io.EOF) {
				trace_util_0.Count(_conn_00000, 305)
				logutil.Logger(ctx).Error("read packet failed", zap.Error(err))
				break
			}
		}
		trace_util_0.Count(_conn_00000, 301)
		if len(curData) == 0 {
			trace_util_0.Count(_conn_00000, 306)
			shouldBreak = true
			if len(prevData) == 0 {
				trace_util_0.Count(_conn_00000, 307)
				break
			}
		}
		trace_util_0.Count(_conn_00000, 302)
		prevData, err = insertDataWithCommit(ctx, prevData, curData, loadDataInfo)
		if err != nil {
			trace_util_0.Count(_conn_00000, 308)
			break
		}
		trace_util_0.Count(_conn_00000, 303)
		if shouldBreak {
			trace_util_0.Count(_conn_00000, 309)
			break
		}
	}
	trace_util_0.Count(_conn_00000, 293)
	loadDataInfo.SetMessage()

	if err != nil {
		trace_util_0.Count(_conn_00000, 310)
		loadDataInfo.Ctx.StmtRollback()
	} else {
		trace_util_0.Count(_conn_00000, 311)
		{
			err = loadDataInfo.Ctx.StmtCommit()
		}
	}

	trace_util_0.Count(_conn_00000, 294)
	var txn kv.Transaction
	var err1 error
	txn, err1 = loadDataInfo.Ctx.Txn(true)
	if err1 == nil {
		trace_util_0.Count(_conn_00000, 312)
		if txn != nil && txn.Valid() {
			trace_util_0.Count(_conn_00000, 313)
			if err != nil {
				trace_util_0.Count(_conn_00000, 315)
				if err1 := txn.Rollback(); err1 != nil {
					trace_util_0.Count(_conn_00000, 317)
					logutil.Logger(ctx).Error("load data rollback failed", zap.Error(err1))
				}
				trace_util_0.Count(_conn_00000, 316)
				return err
			}
			trace_util_0.Count(_conn_00000, 314)
			return cc.ctx.CommitTxn(sessionctx.SetCommitCtx(ctx, loadDataInfo.Ctx))
		}
	}
	// Should never reach here.
	trace_util_0.Count(_conn_00000, 295)
	panic(err1)
}

// handleLoadStats does the additional work after processing the 'load stats' query.
// It sends client a file path, then reads the file content from client, loads it into the storage.
func (cc *clientConn) handleLoadStats(ctx context.Context, loadStatsInfo *executor.LoadStatsInfo) error {
	trace_util_0.Count(_conn_00000, 318)
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&mysql.ClientLocalFiles == 0 {
		trace_util_0.Count(_conn_00000, 324)
		return errNotAllowedCommand
	}
	trace_util_0.Count(_conn_00000, 319)
	if loadStatsInfo == nil {
		trace_util_0.Count(_conn_00000, 325)
		return errors.New("load stats: info is empty")
	}
	trace_util_0.Count(_conn_00000, 320)
	err := cc.writeReq(loadStatsInfo.Path)
	if err != nil {
		trace_util_0.Count(_conn_00000, 326)
		return err
	}
	trace_util_0.Count(_conn_00000, 321)
	var prevData, curData []byte
	for {
		trace_util_0.Count(_conn_00000, 327)
		curData, err = cc.readPacket()
		if err != nil && terror.ErrorNotEqual(err, io.EOF) {
			trace_util_0.Count(_conn_00000, 330)
			return err
		}
		trace_util_0.Count(_conn_00000, 328)
		if len(curData) == 0 {
			trace_util_0.Count(_conn_00000, 331)
			break
		}
		trace_util_0.Count(_conn_00000, 329)
		prevData = append(prevData, curData...)
	}
	trace_util_0.Count(_conn_00000, 322)
	if len(prevData) == 0 {
		trace_util_0.Count(_conn_00000, 332)
		return nil
	}
	trace_util_0.Count(_conn_00000, 323)
	return loadStatsInfo.Update(prevData)
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// There is a special query `load data` that does not return result, which is handled differently.
// Query `load stats` does not return result either.
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {
	trace_util_0.Register(sql)
	trace_util_0.Count(_conn_00000, 333)
	rs, err := cc.ctx.Execute(ctx, sql)
	if err != nil {
		trace_util_0.Count(_conn_00000, 337)
		metrics.ExecuteErrorCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err)).Inc()
		return err
	}
	trace_util_0.Count(_conn_00000, 334)
	status := atomic.LoadInt32(&cc.status)
	if status == connStatusShutdown || status == connStatusWaitShutdown {
		trace_util_0.Count(_conn_00000, 338)
		killConn(cc)
		return errors.New("killed by another connection")
	}
	trace_util_0.Count(_conn_00000, 335)
	if rs != nil {
		trace_util_0.Count(_conn_00000, 339)
		if len(rs) == 1 {
			trace_util_0.Count(_conn_00000, 340)
			err = cc.writeResultset(ctx, rs[0], false, 0, 0)
		} else {
			trace_util_0.Count(_conn_00000, 341)
			{
				err = cc.writeMultiResultset(ctx, rs, false)
			}
		}
	} else {
		trace_util_0.Count(_conn_00000, 342)
		{
			loadDataInfo := cc.ctx.Value(executor.LoadDataVarKey)
			if loadDataInfo != nil {
				trace_util_0.Count(_conn_00000, 345)
				defer cc.ctx.SetValue(executor.LoadDataVarKey, nil)
				if err = cc.handleLoadData(ctx, loadDataInfo.(*executor.LoadDataInfo)); err != nil {
					trace_util_0.Count(_conn_00000, 346)
					return err
				}
			}

			trace_util_0.Count(_conn_00000, 343)
			loadStats := cc.ctx.Value(executor.LoadStatsVarKey)
			if loadStats != nil {
				trace_util_0.Count(_conn_00000, 347)
				defer cc.ctx.SetValue(executor.LoadStatsVarKey, nil)
				if err = cc.handleLoadStats(ctx, loadStats.(*executor.LoadStatsInfo)); err != nil {
					trace_util_0.Count(_conn_00000, 348)
					return err
				}
			}

			trace_util_0.Count(_conn_00000, 344)
			err = cc.writeOK()
		}
	}
	trace_util_0.Count(_conn_00000, 336)
	return err
}

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
func (cc *clientConn) handleFieldList(sql string) (err error) {
	trace_util_0.Count(_conn_00000, 349)
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		trace_util_0.Count(_conn_00000, 353)
		return err
	}
	trace_util_0.Count(_conn_00000, 350)
	data := cc.alloc.AllocWithLen(4, 1024)
	for _, column := range columns {
		trace_util_0.Count(_conn_00000, 354)
		// Current we doesn't output defaultValue but reserve defaultValue length byte to make mariadb client happy.
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
		// TODO: fill the right DefaultValues.
		column.DefaultValueLength = 0
		column.DefaultValue = []byte{}

		data = data[0:4]
		data = column.Dump(data)
		if err := cc.writePacket(data); err != nil {
			trace_util_0.Count(_conn_00000, 355)
			return err
		}
	}
	trace_util_0.Count(_conn_00000, 351)
	if err := cc.writeEOF(0); err != nil {
		trace_util_0.Count(_conn_00000, 356)
		return err
	}
	trace_util_0.Count(_conn_00000, 352)
	return cc.flush()
}

// writeResultset writes data into a resultset and uses rs.Next to get row data back.
// If binary is true, the data would be encoded in BINARY format.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
// resultsets, it's used to support the MULTI_RESULTS capability in mysql protocol.
func (cc *clientConn) writeResultset(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16, fetchSize int) (runErr error) {
	trace_util_0.Count(_conn_00000, 357)
	defer func() {
		trace_util_0.Count(_conn_00000, 361)
		// close ResultSet when cursor doesn't exist
		if !mysql.HasCursorExistsFlag(serverStatus) {
			trace_util_0.Count(_conn_00000, 365)
			terror.Call(rs.Close)
		}
		trace_util_0.Count(_conn_00000, 362)
		r := recover()
		if r == nil {
			trace_util_0.Count(_conn_00000, 366)
			return
		}
		trace_util_0.Count(_conn_00000, 363)
		if str, ok := r.(string); !ok || !strings.HasPrefix(str, memory.PanicMemoryExceed) {
			trace_util_0.Count(_conn_00000, 367)
			panic(r)
		}
		// TODO(jianzhang.zj: add metrics here)
		trace_util_0.Count(_conn_00000, 364)
		runErr = errors.Errorf("%v", r)
		buf := make([]byte, 4096)
		stackSize := runtime.Stack(buf, false)
		buf = buf[:stackSize]
		logutil.Logger(ctx).Error("write query result panic", zap.String("lastCmd", cc.lastCmd), zap.String("stack", string(buf)))
	}()
	trace_util_0.Count(_conn_00000, 358)
	var err error
	if mysql.HasCursorExistsFlag(serverStatus) {
		trace_util_0.Count(_conn_00000, 368)
		err = cc.writeChunksWithFetchSize(ctx, rs, serverStatus, fetchSize)
	} else {
		trace_util_0.Count(_conn_00000, 369)
		{
			err = cc.writeChunks(ctx, rs, binary, serverStatus)
		}
	}
	trace_util_0.Count(_conn_00000, 359)
	if err != nil {
		trace_util_0.Count(_conn_00000, 370)
		return err
	}
	trace_util_0.Count(_conn_00000, 360)
	return cc.flush()
}

func (cc *clientConn) writeColumnInfo(columns []*ColumnInfo, serverStatus uint16) error {
	trace_util_0.Count(_conn_00000, 371)
	data := cc.alloc.AllocWithLen(4, 1024)
	data = dumpLengthEncodedInt(data, uint64(len(columns)))
	if err := cc.writePacket(data); err != nil {
		trace_util_0.Count(_conn_00000, 374)
		return err
	}
	trace_util_0.Count(_conn_00000, 372)
	for _, v := range columns {
		trace_util_0.Count(_conn_00000, 375)
		data = data[0:4]
		data = v.Dump(data)
		if err := cc.writePacket(data); err != nil {
			trace_util_0.Count(_conn_00000, 376)
			return err
		}
	}
	trace_util_0.Count(_conn_00000, 373)
	return cc.writeEOF(serverStatus)
}

// writeChunks writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information
func (cc *clientConn) writeChunks(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16) error {
	trace_util_0.Count(_conn_00000, 377)
	data := cc.alloc.AllocWithLen(4, 1024)
	req := rs.NewRecordBatch()
	gotColumnInfo := false
	for {
		trace_util_0.Count(_conn_00000, 379)
		// Here server.tidbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_conn_00000, 383)
			return err
		}
		trace_util_0.Count(_conn_00000, 380)
		if !gotColumnInfo {
			trace_util_0.Count(_conn_00000, 384)
			// We need to call Next before we get columns.
			// Otherwise, we will get incorrect columns info.
			columns := rs.Columns()
			err = cc.writeColumnInfo(columns, serverStatus)
			if err != nil {
				trace_util_0.Count(_conn_00000, 386)
				return err
			}
			trace_util_0.Count(_conn_00000, 385)
			gotColumnInfo = true
		}
		trace_util_0.Count(_conn_00000, 381)
		rowCount := req.NumRows()
		if rowCount == 0 {
			trace_util_0.Count(_conn_00000, 387)
			break
		}
		trace_util_0.Count(_conn_00000, 382)
		for i := 0; i < rowCount; i++ {
			trace_util_0.Count(_conn_00000, 388)
			data = data[0:4]
			if binary {
				trace_util_0.Count(_conn_00000, 391)
				data, err = dumpBinaryRow(data, rs.Columns(), req.GetRow(i))
			} else {
				trace_util_0.Count(_conn_00000, 392)
				{
					data, err = dumpTextRow(data, rs.Columns(), req.GetRow(i))
				}
			}
			trace_util_0.Count(_conn_00000, 389)
			if err != nil {
				trace_util_0.Count(_conn_00000, 393)
				return err
			}
			trace_util_0.Count(_conn_00000, 390)
			if err = cc.writePacket(data); err != nil {
				trace_util_0.Count(_conn_00000, 394)
				return err
			}
		}
	}
	trace_util_0.Count(_conn_00000, 378)
	return cc.writeEOF(serverStatus)
}

// writeChunksWithFetchSize writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeChunksWithFetchSize(ctx context.Context, rs ResultSet, serverStatus uint16, fetchSize int) error {
	trace_util_0.Count(_conn_00000, 395)
	fetchedRows := rs.GetFetchedRows()

	// if fetchedRows is not enough, getting data from recordSet.
	req := rs.NewRecordBatch()
	for len(fetchedRows) < fetchSize {
		trace_util_0.Count(_conn_00000, 400)
		// Here server.tidbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			trace_util_0.Count(_conn_00000, 404)
			return err
		}
		trace_util_0.Count(_conn_00000, 401)
		rowCount := req.NumRows()
		if rowCount == 0 {
			trace_util_0.Count(_conn_00000, 405)
			break
		}
		// filling fetchedRows with chunk
		trace_util_0.Count(_conn_00000, 402)
		for i := 0; i < rowCount; i++ {
			trace_util_0.Count(_conn_00000, 406)
			fetchedRows = append(fetchedRows, req.GetRow(i))
		}
		trace_util_0.Count(_conn_00000, 403)
		req.Chunk = chunk.Renew(req.Chunk, cc.ctx.GetSessionVars().MaxChunkSize)
	}

	// tell the client COM_STMT_FETCH has finished by setting proper serverStatus,
	// and close ResultSet.
	trace_util_0.Count(_conn_00000, 396)
	if len(fetchedRows) == 0 {
		trace_util_0.Count(_conn_00000, 407)
		serverStatus |= mysql.ServerStatusLastRowSend
		terror.Call(rs.Close)
		return cc.writeEOF(serverStatus)
	}

	// construct the rows sent to the client according to fetchSize.
	trace_util_0.Count(_conn_00000, 397)
	var curRows []chunk.Row
	if fetchSize < len(fetchedRows) {
		trace_util_0.Count(_conn_00000, 408)
		curRows = fetchedRows[:fetchSize]
		fetchedRows = fetchedRows[fetchSize:]
	} else {
		trace_util_0.Count(_conn_00000, 409)
		{
			curRows = fetchedRows[:]
			fetchedRows = fetchedRows[:0]
		}
	}
	trace_util_0.Count(_conn_00000, 398)
	rs.StoreFetchedRows(fetchedRows)

	data := cc.alloc.AllocWithLen(4, 1024)
	var err error
	for _, row := range curRows {
		trace_util_0.Count(_conn_00000, 410)
		data = data[0:4]
		data, err = dumpBinaryRow(data, rs.Columns(), row)
		if err != nil {
			trace_util_0.Count(_conn_00000, 412)
			return err
		}
		trace_util_0.Count(_conn_00000, 411)
		if err = cc.writePacket(data); err != nil {
			trace_util_0.Count(_conn_00000, 413)
			return err
		}
	}
	trace_util_0.Count(_conn_00000, 399)
	return cc.writeEOF(serverStatus)
}

func (cc *clientConn) writeMultiResultset(ctx context.Context, rss []ResultSet, binary bool) error {
	trace_util_0.Count(_conn_00000, 414)
	for _, rs := range rss {
		trace_util_0.Count(_conn_00000, 416)
		if err := cc.writeResultset(ctx, rs, binary, mysql.ServerMoreResultsExists, 0); err != nil {
			trace_util_0.Count(_conn_00000, 417)
			return err
		}
	}
	trace_util_0.Count(_conn_00000, 415)
	return cc.writeOK()
}

func (cc *clientConn) setConn(conn net.Conn) {
	trace_util_0.Count(_conn_00000, 418)
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		trace_util_0.Count(_conn_00000, 419)
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		trace_util_0.Count(_conn_00000, 420)
		{
			// Preserve current sequence number.
			cc.pkt.setBufferedReadConn(cc.bufReadConn)
		}
	}
}

func (cc *clientConn) upgradeToTLS(tlsConfig *tls.Config) error {
	trace_util_0.Count(_conn_00000, 421)
	// Important: read from buffered reader instead of the original net.Conn because it may contain data we need.
	tlsConn := tls.Server(cc.bufReadConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		trace_util_0.Count(_conn_00000, 423)
		return err
	}
	trace_util_0.Count(_conn_00000, 422)
	cc.setConn(tlsConn)
	cc.tlsConn = tlsConn
	return nil
}

func (cc *clientConn) handleChangeUser(ctx context.Context, data []byte) error {
	trace_util_0.Count(_conn_00000, 424)
	user, data := parseNullTermString(data)
	cc.user = string(hack.String(user))
	if len(data) < 1 {
		trace_util_0.Count(_conn_00000, 431)
		return mysql.ErrMalformPacket
	}
	trace_util_0.Count(_conn_00000, 425)
	passLen := int(data[0])
	data = data[1:]
	if passLen > len(data) {
		trace_util_0.Count(_conn_00000, 432)
		return mysql.ErrMalformPacket
	}
	trace_util_0.Count(_conn_00000, 426)
	pass := data[:passLen]
	data = data[passLen:]
	dbName, _ := parseNullTermString(data)
	cc.dbname = string(hack.String(dbName))
	err := cc.ctx.Close()
	if err != nil {
		trace_util_0.Count(_conn_00000, 433)
		logutil.Logger(ctx).Debug("close old context error", zap.Error(err))
	}

	trace_util_0.Count(_conn_00000, 427)
	err = cc.openSessionAndDoAuth(pass)
	if err != nil {
		trace_util_0.Count(_conn_00000, 434)
		return err
	}

	trace_util_0.Count(_conn_00000, 428)
	err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		trace_util_0.Count(_conn_00000, 435)
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			trace_util_0.Count(_conn_00000, 437)
			connInfo := cc.connectInfo()
			err = authPlugin.OnConnectionEvent(context.Background(), &auth.UserIdentity{Hostname: connInfo.Host}, plugin.ChangeUser, connInfo)
			if err != nil {
				trace_util_0.Count(_conn_00000, 438)
				return err
			}
		}
		trace_util_0.Count(_conn_00000, 436)
		return nil
	})
	trace_util_0.Count(_conn_00000, 429)
	if err != nil {
		trace_util_0.Count(_conn_00000, 439)
		return err
	}

	trace_util_0.Count(_conn_00000, 430)
	return cc.writeOK()
}

var _conn_00000 = "server/conn.go"
