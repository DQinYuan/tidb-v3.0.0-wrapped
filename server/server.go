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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/user"
	"sync"
	"sync/atomic"
	"time"

	// For pprof
	_ "net/http/pprof"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sys/linux"
	"go.uber.org/zap"
)

var (
	baseConnID uint32
	serverPID  int
	osUser     string
	osVersion  string
)

func init() {
	trace_util_0.Count(_server_00000, 0)
	serverPID = os.Getpid()
	currentUser, err := user.Current()
	if err != nil {
		trace_util_0.Count(_server_00000, 2)
		osUser = ""
	} else {
		trace_util_0.Count(_server_00000, 3)
		{
			osUser = currentUser.Name
		}
	}
	trace_util_0.Count(_server_00000, 1)
	osVersion, err = linux.OSVersion()
	if err != nil {
		trace_util_0.Count(_server_00000, 4)
		osVersion = ""
	}
}

var (
	errUnknownFieldType  = terror.ClassServer.New(codeUnknownFieldType, "unknown field type")
	errInvalidPayloadLen = terror.ClassServer.New(codeInvalidPayloadLen, "invalid payload length")
	errInvalidSequence   = terror.ClassServer.New(codeInvalidSequence, "invalid sequence")
	errInvalidType       = terror.ClassServer.New(codeInvalidType, "invalid type")
	errNotAllowedCommand = terror.ClassServer.New(codeNotAllowedCommand, "the used command is not allowed with this TiDB version")
	errAccessDenied      = terror.ClassServer.New(codeAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

// Server is the MySQL protocol server
type Server struct {
	cfg               *config.Config
	tlsConfig         *tls.Config
	driver            IDriver
	listener          net.Listener
	socket            net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *TokenLimiter
	clients           map[uint32]*clientConn
	capability        uint32

	// stopListenerCh is used when a critical error occurred, we don't want to exit the process, because there may be
	// a supervisor automatically restart it, then new client connection will be created, but we can't server it.
	// So we just stop the listener and store to force clients to chose other TiDB servers.
	stopListenerCh chan struct{}
	statusServer   *http.Server
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	trace_util_0.Count(_server_00000, 5)
	var cnt int
	s.rwlock.RLock()
	cnt = len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) getToken() *Token {
	trace_util_0.Count(_server_00000, 6)
	start := time.Now()
	tok := s.concurrentLimiter.Get()
	// Note that data smaller than one microsecond is ignored, because that case can be viewed as non-block.
	metrics.GetTokenDurationHistogram.Observe(float64(time.Since(start).Nanoseconds() / 1e3))
	return tok
}

func (s *Server) releaseToken(token *Token) {
	trace_util_0.Count(_server_00000, 7)
	s.concurrentLimiter.Put(token)
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	trace_util_0.Count(_server_00000, 8)
	cc := newClientConn(s)
	if s.cfg.Performance.TCPKeepAlive {
		trace_util_0.Count(_server_00000, 10)
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			trace_util_0.Count(_server_00000, 11)
			if err := tcpConn.SetKeepAlive(true); err != nil {
				trace_util_0.Count(_server_00000, 12)
				logutil.Logger(context.Background()).Error("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}
	trace_util_0.Count(_server_00000, 9)
	cc.setConn(conn)
	cc.salt = util.RandomBuf(20)
	return cc
}

func (s *Server) isUnixSocket() bool {
	trace_util_0.Count(_server_00000, 13)
	return s.cfg.Socket != ""
}

func (s *Server) forwardUnixSocketToTCP() {
	trace_util_0.Count(_server_00000, 14)
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	for {
		trace_util_0.Count(_server_00000, 15)
		if s.listener == nil {
			trace_util_0.Count(_server_00000, 17)
			return // server shutdown has started
		}
		trace_util_0.Count(_server_00000, 16)
		if uconn, err := s.socket.Accept(); err == nil {
			trace_util_0.Count(_server_00000, 18)
			logutil.Logger(context.Background()).Info("server socket forwarding", zap.String("from", s.cfg.Socket), zap.String("to", addr))
			go s.handleForwardedConnection(uconn, addr)
		} else {
			trace_util_0.Count(_server_00000, 19)
			{
				if s.listener != nil {
					trace_util_0.Count(_server_00000, 20)
					logutil.Logger(context.Background()).Error("server failed to forward", zap.String("from", s.cfg.Socket), zap.String("to", addr), zap.Error(err))
				}
			}
		}
	}
}

func (s *Server) handleForwardedConnection(uconn net.Conn, addr string) {
	trace_util_0.Count(_server_00000, 21)
	defer terror.Call(uconn.Close)
	if tconn, err := net.Dial("tcp", addr); err == nil {
		trace_util_0.Count(_server_00000, 22)
		go func() {
			trace_util_0.Count(_server_00000, 24)
			if _, err := io.Copy(uconn, tconn); err != nil {
				trace_util_0.Count(_server_00000, 25)
				logutil.Logger(context.Background()).Warn("copy server to socket failed", zap.Error(err))
			}
		}()
		trace_util_0.Count(_server_00000, 23)
		if _, err := io.Copy(tconn, uconn); err != nil {
			trace_util_0.Count(_server_00000, 26)
			logutil.Logger(context.Background()).Warn("socket forward copy failed", zap.Error(err))
		}
	} else {
		trace_util_0.Count(_server_00000, 27)
		{
			logutil.Logger(context.Background()).Warn("socket forward failed: could not connect", zap.String("addr", addr), zap.Error(err))
		}
	}
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	trace_util_0.Count(_server_00000, 28)
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]*clientConn),
		stopListenerCh:    make(chan struct{}, 1),
	}
	s.loadTLSCertificates()

	s.capability = defaultCapability
	if s.tlsConfig != nil {
		trace_util_0.Count(_server_00000, 33)
		s.capability |= mysql.ClientSSL
	}

	trace_util_0.Count(_server_00000, 29)
	var err error

	if s.cfg.Host != "" && s.cfg.Port != 0 {
		trace_util_0.Count(_server_00000, 34)
		addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
		if s.listener, err = net.Listen("tcp", addr); err == nil {
			trace_util_0.Count(_server_00000, 35)
			logutil.Logger(context.Background()).Info("server is running MySQL protocol", zap.String("addr", addr))
			if cfg.Socket != "" {
				trace_util_0.Count(_server_00000, 36)
				if s.socket, err = net.Listen("unix", s.cfg.Socket); err == nil {
					trace_util_0.Count(_server_00000, 37)
					logutil.Logger(context.Background()).Info("server redirecting", zap.String("from", s.cfg.Socket), zap.String("to", addr))
					go s.forwardUnixSocketToTCP()
				}
			}
		}
	} else {
		trace_util_0.Count(_server_00000, 38)
		if cfg.Socket != "" {
			trace_util_0.Count(_server_00000, 39)
			if s.listener, err = net.Listen("unix", cfg.Socket); err == nil {
				trace_util_0.Count(_server_00000, 40)
				logutil.Logger(context.Background()).Info("server is running MySQL protocol", zap.String("socket", cfg.Socket))
			}
		} else {
			trace_util_0.Count(_server_00000, 41)
			{
				err = errors.New("Server not configured to listen on either -socket or -host and -port")
			}
		}
	}

	trace_util_0.Count(_server_00000, 30)
	if cfg.ProxyProtocol.Networks != "" {
		trace_util_0.Count(_server_00000, 42)
		pplistener, errProxy := proxyprotocol.NewListener(s.listener, cfg.ProxyProtocol.Networks,
			int(cfg.ProxyProtocol.HeaderTimeout))
		if errProxy != nil {
			trace_util_0.Count(_server_00000, 44)
			logutil.Logger(context.Background()).Error("ProxyProtocol networks parameter invalid")
			return nil, errors.Trace(errProxy)
		}
		trace_util_0.Count(_server_00000, 43)
		logutil.Logger(context.Background()).Info("server is running MySQL protocol (through PROXY protocol)", zap.String("host", s.cfg.Host))
		s.listener = pplistener
	}

	trace_util_0.Count(_server_00000, 31)
	if err != nil {
		trace_util_0.Count(_server_00000, 45)
		return nil, errors.Trace(err)
	}

	// Init rand seed for randomBuf()
	trace_util_0.Count(_server_00000, 32)
	rand.Seed(time.Now().UTC().UnixNano())
	return s, nil
}

func (s *Server) loadTLSCertificates() {
	trace_util_0.Count(_server_00000, 46)
	defer func() {
		trace_util_0.Count(_server_00000, 51)
		if s.tlsConfig != nil {
			trace_util_0.Count(_server_00000, 52)
			logutil.Logger(context.Background()).Info("secure connection is enabled", zap.Bool("client verification enabled", len(variable.SysVars["ssl_ca"].Value) > 0))
			variable.SysVars["have_openssl"].Value = "YES"
			variable.SysVars["have_ssl"].Value = "YES"
			variable.SysVars["ssl_cert"].Value = s.cfg.Security.SSLCert
			variable.SysVars["ssl_key"].Value = s.cfg.Security.SSLKey
		} else {
			trace_util_0.Count(_server_00000, 53)
			{
				logutil.Logger(context.Background()).Warn("secure connection is not enabled")
			}
		}
	}()

	trace_util_0.Count(_server_00000, 47)
	if len(s.cfg.Security.SSLCert) == 0 || len(s.cfg.Security.SSLKey) == 0 {
		trace_util_0.Count(_server_00000, 54)
		s.tlsConfig = nil
		return
	}

	trace_util_0.Count(_server_00000, 48)
	tlsCert, err := tls.LoadX509KeyPair(s.cfg.Security.SSLCert, s.cfg.Security.SSLKey)
	if err != nil {
		trace_util_0.Count(_server_00000, 55)
		logutil.Logger(context.Background()).Warn("load x509 failed", zap.Error(err))
		s.tlsConfig = nil
		return
	}

	// Try loading CA cert.
	trace_util_0.Count(_server_00000, 49)
	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(s.cfg.Security.SSLCA) > 0 {
		trace_util_0.Count(_server_00000, 56)
		caCert, err := ioutil.ReadFile(s.cfg.Security.SSLCA)
		if err != nil {
			trace_util_0.Count(_server_00000, 57)
			logutil.Logger(context.Background()).Warn("read file failed", zap.Error(err))
		} else {
			trace_util_0.Count(_server_00000, 58)
			{
				certPool = x509.NewCertPool()
				if certPool.AppendCertsFromPEM(caCert) {
					trace_util_0.Count(_server_00000, 60)
					clientAuthPolicy = tls.VerifyClientCertIfGiven
				}
				trace_util_0.Count(_server_00000, 59)
				variable.SysVars["ssl_ca"].Value = s.cfg.Security.SSLCA
			}
		}
	}
	trace_util_0.Count(_server_00000, 50)
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
		MinVersion:   0,
	}
}

// Run runs the server.
func (s *Server) Run() error {
	trace_util_0.Count(_server_00000, 61)
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// Start HTTP API to report tidb info such as TPS.
	if s.cfg.Status.ReportStatus {
		trace_util_0.Count(_server_00000, 64)
		s.startStatusHTTP()
	}
	trace_util_0.Count(_server_00000, 62)
	for {
		trace_util_0.Count(_server_00000, 65)
		conn, err := s.listener.Accept()
		if err != nil {
			trace_util_0.Count(_server_00000, 70)
			if opErr, ok := err.(*net.OpError); ok {
				trace_util_0.Count(_server_00000, 73)
				if opErr.Err.Error() == "use of closed network connection" {
					trace_util_0.Count(_server_00000, 74)
					return nil
				}
			}

			// If we got PROXY protocol error, we should continue accept.
			trace_util_0.Count(_server_00000, 71)
			if proxyprotocol.IsProxyProtocolError(err) {
				trace_util_0.Count(_server_00000, 75)
				logutil.Logger(context.Background()).Error("PROXY protocol failed", zap.Error(err))
				continue
			}

			trace_util_0.Count(_server_00000, 72)
			logutil.Logger(context.Background()).Error("accept failed", zap.Error(err))
			return errors.Trace(err)
		}
		trace_util_0.Count(_server_00000, 66)
		if s.shouldStopListener() {
			trace_util_0.Count(_server_00000, 76)
			err = conn.Close()
			terror.Log(errors.Trace(err))
			break
		}

		trace_util_0.Count(_server_00000, 67)
		clientConn := s.newConn(conn)

		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			trace_util_0.Count(_server_00000, 77)
			authPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if authPlugin.OnConnectionEvent != nil {
				trace_util_0.Count(_server_00000, 79)
				host, err := clientConn.PeerHost("")
				if err != nil {
					trace_util_0.Count(_server_00000, 81)
					logutil.Logger(context.Background()).Error("get peer host failed", zap.Error(err))
					terror.Log(clientConn.Close())
					return errors.Trace(err)
				}
				trace_util_0.Count(_server_00000, 80)
				err = authPlugin.OnConnectionEvent(context.Background(), &auth.UserIdentity{Hostname: host}, plugin.PreAuth, nil)
				if err != nil {
					trace_util_0.Count(_server_00000, 82)
					logutil.Logger(context.Background()).Info("do connection event failed", zap.Error(err))
					terror.Log(clientConn.Close())
					return errors.Trace(err)
				}
			}
			trace_util_0.Count(_server_00000, 78)
			return nil
		})
		trace_util_0.Count(_server_00000, 68)
		if err != nil {
			trace_util_0.Count(_server_00000, 83)
			continue
		}

		trace_util_0.Count(_server_00000, 69)
		go s.onConn(clientConn)
	}
	trace_util_0.Count(_server_00000, 63)
	err := s.listener.Close()
	terror.Log(errors.Trace(err))
	s.listener = nil
	for {
		trace_util_0.Count(_server_00000, 84)
		metrics.ServerEventCounter.WithLabelValues(metrics.EventHang).Inc()
		logutil.Logger(context.Background()).Error("listener stopped, waiting for manual kill.")
		time.Sleep(time.Minute)
	}
}

func (s *Server) shouldStopListener() bool {
	trace_util_0.Count(_server_00000, 85)
	select {
	case <-s.stopListenerCh:
		trace_util_0.Count(_server_00000, 86)
		return true
	default:
		trace_util_0.Count(_server_00000, 87)
		return false
	}
}

// Close closes the server.
func (s *Server) Close() {
	trace_util_0.Count(_server_00000, 88)
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		trace_util_0.Count(_server_00000, 92)
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	trace_util_0.Count(_server_00000, 89)
	if s.socket != nil {
		trace_util_0.Count(_server_00000, 93)
		err := s.socket.Close()
		terror.Log(errors.Trace(err))
		s.socket = nil
	}
	trace_util_0.Count(_server_00000, 90)
	if s.statusServer != nil {
		trace_util_0.Count(_server_00000, 94)
		err := s.statusServer.Close()
		terror.Log(errors.Trace(err))
		s.statusServer = nil
	}
	trace_util_0.Count(_server_00000, 91)
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(conn *clientConn) {
	trace_util_0.Count(_server_00000, 95)
	ctx := logutil.WithConnID(context.Background(), conn.connectionID)
	if err := conn.handshake(ctx); err != nil {
		trace_util_0.Count(_server_00000, 101)
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we only record metrics.
		metrics.HandShakeErrorCounter.Inc()
		err = conn.Close()
		terror.Log(errors.Trace(err))
		return
	}

	trace_util_0.Count(_server_00000, 96)
	logutil.Logger(ctx).Info("new connection", zap.String("remoteAddr", conn.bufReadConn.RemoteAddr().String()))

	defer func() {
		trace_util_0.Count(_server_00000, 102)
		logutil.Logger(ctx).Info("connection closed")
	}()
	trace_util_0.Count(_server_00000, 97)
	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	connections := len(s.clients)
	s.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))

	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		trace_util_0.Count(_server_00000, 103)
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			trace_util_0.Count(_server_00000, 105)
			connInfo := conn.connectInfo()
			return authPlugin.OnConnectionEvent(context.Background(), conn.ctx.GetSessionVars().User, plugin.Connected, connInfo)
		}
		trace_util_0.Count(_server_00000, 104)
		return nil
	})
	trace_util_0.Count(_server_00000, 98)
	if err != nil {
		trace_util_0.Count(_server_00000, 106)
		return
	}

	trace_util_0.Count(_server_00000, 99)
	connectedTime := time.Now()
	conn.Run(ctx)

	err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		trace_util_0.Count(_server_00000, 107)
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			trace_util_0.Count(_server_00000, 109)
			connInfo := conn.connectInfo()
			connInfo.Duration = float64(time.Since(connectedTime)) / float64(time.Millisecond)
			err := authPlugin.OnConnectionEvent(context.Background(), conn.ctx.GetSessionVars().User, plugin.Disconnect, connInfo)
			if err != nil {
				trace_util_0.Count(_server_00000, 110)
				logutil.Logger(context.Background()).Warn("do connection event failed", zap.String("plugin", authPlugin.Name), zap.Error(err))
			}
		}
		trace_util_0.Count(_server_00000, 108)
		return nil
	})
	trace_util_0.Count(_server_00000, 100)
	if err != nil {
		trace_util_0.Count(_server_00000, 111)
		return
	}
}

func (cc *clientConn) connectInfo() *variable.ConnectionInfo {
	trace_util_0.Count(_server_00000, 112)
	connType := "Socket"
	if cc.server.isUnixSocket() {
		trace_util_0.Count(_server_00000, 114)
		connType = "UnixSocket"
	} else {
		trace_util_0.Count(_server_00000, 115)
		if cc.tlsConn != nil {
			trace_util_0.Count(_server_00000, 116)
			connType = "SSL/TLS"
		}
	}
	trace_util_0.Count(_server_00000, 113)
	connInfo := &variable.ConnectionInfo{
		ConnectionID:      cc.connectionID,
		ConnectionType:    connType,
		Host:              cc.peerHost,
		ClientIP:          cc.peerHost,
		ClientPort:        cc.peerPort,
		ServerID:          1,
		ServerPort:        int(cc.server.cfg.Port),
		Duration:          0,
		User:              cc.user,
		ServerOSLoginUser: osUser,
		OSVersion:         osVersion,
		ClientVersion:     "",
		ServerVersion:     mysql.TiDBReleaseVersion,
		SSLVersion:        "v1.2.0", // for current go version
		PID:               serverPID,
		DB:                cc.dbname,
	}
	return connInfo
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]*util.ProcessInfo {
	trace_util_0.Count(_server_00000, 117)
	s.rwlock.RLock()
	rs := make(map[uint64]*util.ProcessInfo, len(s.clients))
	for _, client := range s.clients {
		trace_util_0.Count(_server_00000, 119)
		if atomic.LoadInt32(&client.status) == connStatusWaitShutdown {
			trace_util_0.Count(_server_00000, 121)
			continue
		}
		trace_util_0.Count(_server_00000, 120)
		if pi := client.ctx.ShowProcess(); pi != nil {
			trace_util_0.Count(_server_00000, 122)
			rs[pi.ID] = pi
		}
	}
	trace_util_0.Count(_server_00000, 118)
	s.rwlock.RUnlock()
	return rs
}

// GetProcessInfo implements the SessionManager interface.
func (s *Server) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	trace_util_0.Count(_server_00000, 123)
	s.rwlock.RLock()
	conn, ok := s.clients[uint32(id)]
	s.rwlock.RUnlock()
	if !ok || atomic.LoadInt32(&conn.status) == connStatusWaitShutdown {
		trace_util_0.Count(_server_00000, 125)
		return &util.ProcessInfo{}, false
	}
	trace_util_0.Count(_server_00000, 124)
	return conn.ctx.ShowProcess(), ok
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	trace_util_0.Count(_server_00000, 126)
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	logutil.Logger(context.Background()).Info("kill", zap.Uint64("connID", connectionID), zap.Bool("query", query))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventKill).Inc()

	conn, ok := s.clients[uint32(connectionID)]
	if !ok {
		trace_util_0.Count(_server_00000, 129)
		return
	}

	trace_util_0.Count(_server_00000, 127)
	if !query {
		trace_util_0.Count(_server_00000, 130)
		// Mark the client connection status as WaitShutdown, when the goroutine detect
		// this, it will end the dispatch loop and exit.
		atomic.StoreInt32(&conn.status, connStatusWaitShutdown)
	}
	trace_util_0.Count(_server_00000, 128)
	killConn(conn)
}

func killConn(conn *clientConn) {
	trace_util_0.Count(_server_00000, 131)
	sessVars := conn.ctx.GetSessionVars()
	atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
}

// KillAllConnections kills all connections when server is not gracefully shutdown.
func (s *Server) KillAllConnections() {
	trace_util_0.Count(_server_00000, 132)
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	logutil.Logger(context.Background()).Info("[server] kill all connections.")

	for _, conn := range s.clients {
		trace_util_0.Count(_server_00000, 133)
		atomic.StoreInt32(&conn.status, connStatusShutdown)
		terror.Log(errors.Trace(conn.closeWithoutLock()))
		killConn(conn)
	}
}

var gracefulCloseConnectionsTimeout = 15 * time.Second

// TryGracefulDown will try to gracefully close all connection first with timeout. if timeout, will close all connection directly.
func (s *Server) TryGracefulDown() {
	trace_util_0.Count(_server_00000, 134)
	ctx, cancel := context.WithTimeout(context.Background(), gracefulCloseConnectionsTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		trace_util_0.Count(_server_00000, 136)
		s.GracefulDown(ctx, done)
	}()
	trace_util_0.Count(_server_00000, 135)
	select {
	case <-ctx.Done():
		trace_util_0.Count(_server_00000, 137)
		s.KillAllConnections()
	case <-done:
		trace_util_0.Count(_server_00000, 138)
		return
	}
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown(ctx context.Context, done chan struct{}) {
	trace_util_0.Count(_server_00000, 139)
	logutil.Logger(ctx).Info("[server] graceful shutdown.")
	metrics.ServerEventCounter.WithLabelValues(metrics.EventGracefulDown).Inc()

	count := s.ConnectionCount()
	for i := 0; count > 0; i++ {
		trace_util_0.Count(_server_00000, 141)
		s.kickIdleConnection()

		count = s.ConnectionCount()
		if count == 0 {
			trace_util_0.Count(_server_00000, 144)
			break
		}
		// Print information for every 30s.
		trace_util_0.Count(_server_00000, 142)
		if i%30 == 0 {
			trace_util_0.Count(_server_00000, 145)
			logutil.Logger(ctx).Info("graceful shutdown...", zap.Int("conn count", count))
		}
		trace_util_0.Count(_server_00000, 143)
		ticker := time.After(time.Second)
		select {
		case <-ctx.Done():
			trace_util_0.Count(_server_00000, 146)
			return
		case <-ticker:
			trace_util_0.Count(_server_00000, 147)
		}
	}
	trace_util_0.Count(_server_00000, 140)
	close(done)
}

func (s *Server) kickIdleConnection() {
	trace_util_0.Count(_server_00000, 148)
	var conns []*clientConn
	s.rwlock.RLock()
	for _, cc := range s.clients {
		trace_util_0.Count(_server_00000, 150)
		if cc.ShutdownOrNotify() {
			trace_util_0.Count(_server_00000, 151)
			// Shutdowned conn will be closed by us, and notified conn will exist themselves.
			conns = append(conns, cc)
		}
	}
	trace_util_0.Count(_server_00000, 149)
	s.rwlock.RUnlock()

	for _, cc := range conns {
		trace_util_0.Count(_server_00000, 152)
		err := cc.Close()
		if err != nil {
			trace_util_0.Count(_server_00000, 153)
			logutil.Logger(context.Background()).Error("close connection", zap.Error(err))
		}
	}
}

// Server error codes.
const (
	codeUnknownFieldType  = 1
	codeInvalidPayloadLen = 2
	codeInvalidSequence   = 3
	codeInvalidType       = 4

	codeNotAllowedCommand = 1148
	codeAccessDenied      = mysql.ErrAccessDenied
)

func init() {
	trace_util_0.Count(_server_00000, 154)
	serverMySQLErrCodes := map[terror.ErrCode]uint16{
		codeNotAllowedCommand: mysql.ErrNotAllowedCommand,
		codeAccessDenied:      mysql.ErrAccessDenied,
	}
	terror.ErrClassToMySQLCodes[terror.ClassServer] = serverMySQLErrCodes
}

var _server_00000 = "server/server.go"
