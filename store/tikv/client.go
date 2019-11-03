// Copyright 2016 PingCAP, Inc.
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

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"context"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MaxSendMsgSize set max gRPC request message size sent to server. If any request message size is larger than
// current value, an error will be reported from gRPC.
var MaxSendMsgSize = 10 * 1024 * 1024

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64

// Timeout durations.
const (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

type connArray struct {
	// The target host.
	target string

	index uint32
	v     []*grpc.ClientConn
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease

	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idle       bool
	idleDetect *time.Timer
}

type batchCommandsClient struct {
	// The target host.
	target string

	conn                   *grpc.ClientConn
	client                 tikvpb.Tikv_BatchCommandsClient
	batched                sync.Map
	idAlloc                uint64
	tikvTransportLayerLoad *uint64

	// closed indicates the batch client is closed explicitly or not.
	closed int32
	// clientLock protects client when re-create the streaming.
	clientLock sync.Mutex
}

func (c *batchCommandsClient) isStopped() bool {
	trace_util_0.Count(_client_00000, 0)
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *batchCommandsClient) failPendingRequests(err error) {
	trace_util_0.Count(_client_00000, 1)
	c.batched.Range(func(key, value interface{}) bool {
		trace_util_0.Count(_client_00000, 2)
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		entry.err = err
		close(entry.res)
		c.batched.Delete(id)
		return true
	})
}

func (c *batchCommandsClient) batchRecvLoop(cfg config.TiKVClient) {
	trace_util_0.Count(_client_00000, 3)
	defer func() {
		trace_util_0.Count(_client_00000, 5)
		if r := recover(); r != nil {
			trace_util_0.Count(_client_00000, 6)
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.Logger(context.Background()).Error("batchRecvLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.Logger(context.Background()).Info("restart batchRecvLoop")
			go c.batchRecvLoop(cfg)
		}
	}()

	trace_util_0.Count(_client_00000, 4)
	for {
		trace_util_0.Count(_client_00000, 7)
		// When `conn.Close()` is called, `client.Recv()` will return an error.
		resp, err := c.client.Recv()
		if err != nil {
			trace_util_0.Count(_client_00000, 10)
			now := time.Now()
			for {
				trace_util_0.Count(_client_00000, 12) // try to re-create the streaming in the loop.
				if c.isStopped() {
					trace_util_0.Count(_client_00000, 15)
					return
				}
				trace_util_0.Count(_client_00000, 13)
				logutil.Logger(context.Background()).Error(
					"batchRecvLoop error when receive",
					zap.String("target", c.target),
					zap.Error(err),
				)

				// Hold the lock to forbid batchSendLoop using the old client.
				c.clientLock.Lock()
				c.failPendingRequests(err) // fail all pending requests.

				// Re-establish a application layer stream. TCP layer is handled by gRPC.
				tikvClient := tikvpb.NewTikvClient(c.conn)
				streamClient, err := tikvClient.BatchCommands(context.TODO())
				c.clientLock.Unlock()

				if err == nil {
					trace_util_0.Count(_client_00000, 16)
					logutil.Logger(context.Background()).Info(
						"batchRecvLoop re-create streaming success",
						zap.String("target", c.target),
					)
					c.client = streamClient
					break
				}
				trace_util_0.Count(_client_00000, 14)
				logutil.Logger(context.Background()).Error(
					"batchRecvLoop re-create streaming fail",
					zap.String("target", c.target),
					zap.Error(err),
				)
				// TODO: Use a more smart backoff strategy.
				time.Sleep(time.Second)
			}
			trace_util_0.Count(_client_00000, 11)
			metrics.TiKVBatchClientUnavailable.Observe(time.Since(now).Seconds())
			continue
		}

		trace_util_0.Count(_client_00000, 8)
		responses := resp.GetResponses()
		for i, requestID := range resp.GetRequestIds() {
			trace_util_0.Count(_client_00000, 17)
			value, ok := c.batched.Load(requestID)
			if !ok {
				trace_util_0.Count(_client_00000, 20)
				// There shouldn't be any unknown responses because if the old entries
				// are cleaned by `failPendingRequests`, the stream must be re-created
				// so that old responses will be never received.
				panic("batchRecvLoop receives a unknown response")
			}
			trace_util_0.Count(_client_00000, 18)
			entry := value.(*batchCommandsEntry)
			if atomic.LoadInt32(&entry.canceled) == 0 {
				trace_util_0.Count(_client_00000, 21)
				// Put the response only if the request is not canceled.
				entry.res <- responses[i]
			}
			trace_util_0.Count(_client_00000, 19)
			c.batched.Delete(requestID)
		}

		trace_util_0.Count(_client_00000, 9)
		tikvTransportLayerLoad := resp.GetTransportLayerLoad()
		if tikvTransportLayerLoad > 0.0 && cfg.MaxBatchWaitTime > 0 {
			trace_util_0.Count(_client_00000, 22)
			// We need to consider TiKV load only if batch-wait strategy is enabled.
			atomic.StoreUint64(c.tikvTransportLayerLoad, tikvTransportLayerLoad)
		}
	}
}

func newConnArray(maxSize uint, addr string, security config.Security, idleNotify *uint32) (*connArray, error) {
	trace_util_0.Count(_client_00000, 23)
	cfg := config.GetGlobalConfig()

	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),

		batchCommandsCh:        make(chan *batchCommandsEntry, cfg.TiKVClient.MaxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, maxSize),
		tikvTransportLayerLoad: 0,

		idleNotify: idleNotify,
		idleDetect: time.NewTimer(idleTimeout),
	}
	if err := a.Init(addr, security); err != nil {
		trace_util_0.Count(_client_00000, 25)
		return nil, err
	}
	trace_util_0.Count(_client_00000, 24)
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security) error {
	trace_util_0.Count(_client_00000, 26)
	a.target = addr

	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		trace_util_0.Count(_client_00000, 31)
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			trace_util_0.Count(_client_00000, 33)
			return errors.Trace(err)
		}
		trace_util_0.Count(_client_00000, 32)
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	trace_util_0.Count(_client_00000, 27)
	cfg := config.GetGlobalConfig()
	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if cfg.OpenTracing.Enable {
		trace_util_0.Count(_client_00000, 34)
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	trace_util_0.Count(_client_00000, 28)
	allowBatch := cfg.TiKVClient.MaxBatchSize > 0
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
	for i := range a.v {
		trace_util_0.Count(_client_00000, 35)
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			opt,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(time.Second*3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(keepAlive) * time.Second,
				Timeout:             time.Duration(keepAliveTimeout) * time.Second,
				PermitWithoutStream: true,
			}),
		)
		cancel()
		if err != nil {
			trace_util_0.Count(_client_00000, 37)
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		trace_util_0.Count(_client_00000, 36)
		a.v[i] = conn

		if allowBatch {
			trace_util_0.Count(_client_00000, 38)
			// Initialize batch streaming clients.
			tikvClient := tikvpb.NewTikvClient(conn)
			streamClient, err := tikvClient.BatchCommands(context.TODO())
			if err != nil {
				trace_util_0.Count(_client_00000, 40)
				a.Close()
				return errors.Trace(err)
			}
			trace_util_0.Count(_client_00000, 39)
			batchClient := &batchCommandsClient{
				target:                 a.target,
				conn:                   conn,
				client:                 streamClient,
				batched:                sync.Map{},
				idAlloc:                0,
				tikvTransportLayerLoad: &a.tikvTransportLayerLoad,
				closed:                 0,
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
			go batchClient.batchRecvLoop(cfg.TiKVClient)
		}
	}
	trace_util_0.Count(_client_00000, 29)
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout)
	if allowBatch {
		trace_util_0.Count(_client_00000, 41)
		go a.batchSendLoop(cfg.TiKVClient)
	}

	trace_util_0.Count(_client_00000, 30)
	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	trace_util_0.Count(_client_00000, 42)
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	trace_util_0.Count(_client_00000, 43)
	// Close all batchRecvLoop.
	for _, c := range a.batchCommandsClients {
		trace_util_0.Count(_client_00000, 46)
		// After connections are closed, `batchRecvLoop`s will check the flag.
		atomic.StoreInt32(&c.closed, 1)
	}
	trace_util_0.Count(_client_00000, 44)
	close(a.batchCommandsCh)

	for i, c := range a.v {
		trace_util_0.Count(_client_00000, 47)
		if c != nil {
			trace_util_0.Count(_client_00000, 48)
			err := c.Close()
			terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}
	trace_util_0.Count(_client_00000, 45)
	close(a.streamTimeout)
}

type batchCommandsEntry struct {
	req *tikvpb.BatchCommandsRequest_Request
	res chan *tikvpb.BatchCommandsResponse_Response

	// canceled indicated the request is canceled or not.
	canceled int32
	err      error
}

func (b *batchCommandsEntry) isCanceled() bool {
	trace_util_0.Count(_client_00000, 49)
	return atomic.LoadInt32(&b.canceled) == 1
}

const idleTimeout = 3 * time.Minute

// fetchAllPendingRequests fetches all pending requests from the channel.
func (a *connArray) fetchAllPendingRequests(
	maxBatchSize int,
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request,
) {
	trace_util_0.Count(_client_00000, 50)
	// Block on the first element.
	var headEntry *batchCommandsEntry
	select {
	case headEntry = <-a.batchCommandsCh:
		trace_util_0.Count(_client_00000, 53)
		if !a.idleDetect.Stop() {
			trace_util_0.Count(_client_00000, 56)
			<-a.idleDetect.C
		}
		trace_util_0.Count(_client_00000, 54)
		a.idleDetect.Reset(idleTimeout)
	case <-a.idleDetect.C:
		trace_util_0.Count(_client_00000, 55)
		a.idleDetect.Reset(idleTimeout)
		a.idle = true
		atomic.CompareAndSwapUint32(a.idleNotify, 0, 1)
		// This connArray to be recycled
		return
	}
	trace_util_0.Count(_client_00000, 51)
	if headEntry == nil {
		trace_util_0.Count(_client_00000, 57)
		return
	}
	trace_util_0.Count(_client_00000, 52)
	*entries = append(*entries, headEntry)
	*requests = append(*requests, headEntry.req)

	// This loop is for trying best to collect more requests.
	for len(*entries) < maxBatchSize {
		trace_util_0.Count(_client_00000, 58)
		select {
		case entry := <-a.batchCommandsCh:
			trace_util_0.Count(_client_00000, 59)
			if entry == nil {
				trace_util_0.Count(_client_00000, 62)
				return
			}
			trace_util_0.Count(_client_00000, 60)
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		default:
			trace_util_0.Count(_client_00000, 61)
			return
		}
	}
}

// fetchMorePendingRequests fetches more pending requests from the channel.
func fetchMorePendingRequests(
	ch chan *batchCommandsEntry,
	maxBatchSize int,
	batchWaitSize int,
	maxWaitTime time.Duration,
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request,
) {
	trace_util_0.Count(_client_00000, 63)
	waitStart := time.Now()

	// Try to collect `batchWaitSize` requests, or wait `maxWaitTime`.
	after := time.NewTimer(maxWaitTime)
	for len(*entries) < batchWaitSize {
		trace_util_0.Count(_client_00000, 65)
		select {
		case entry := <-ch:
			trace_util_0.Count(_client_00000, 66)
			if entry == nil {
				trace_util_0.Count(_client_00000, 69)
				return
			}
			trace_util_0.Count(_client_00000, 67)
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		case waitEnd := <-after.C:
			trace_util_0.Count(_client_00000, 68)
			metrics.TiKVBatchWaitDuration.Observe(float64(waitEnd.Sub(waitStart)))
			return
		}
	}
	trace_util_0.Count(_client_00000, 64)
	after.Stop()

	// Do an additional non-block try. Here we test the lengh with `maxBatchSize` instead
	// of `batchWaitSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `batchWaitSize` dynamically.
	for len(*entries) < maxBatchSize {
		trace_util_0.Count(_client_00000, 70)
		select {
		case entry := <-ch:
			trace_util_0.Count(_client_00000, 71)
			if entry == nil {
				trace_util_0.Count(_client_00000, 74)
				return
			}
			trace_util_0.Count(_client_00000, 72)
			*entries = append(*entries, entry)
			*requests = append(*requests, entry.req)
		default:
			trace_util_0.Count(_client_00000, 73)
			metrics.TiKVBatchWaitDuration.Observe(float64(time.Since(waitStart)))
			return
		}
	}
}

func (a *connArray) batchSendLoop(cfg config.TiKVClient) {
	trace_util_0.Count(_client_00000, 75)
	defer func() {
		trace_util_0.Count(_client_00000, 77)
		if r := recover(); r != nil {
			trace_util_0.Count(_client_00000, 78)
			metrics.PanicCounter.WithLabelValues(metrics.LabelBatchSendLoop).Inc()
			logutil.Logger(context.Background()).Error("batchSendLoop",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			logutil.Logger(context.Background()).Info("restart batchSendLoop")
			go a.batchSendLoop(cfg)
		}
	}()

	trace_util_0.Count(_client_00000, 76)
	entries := make([]*batchCommandsEntry, 0, cfg.MaxBatchSize)
	requests := make([]*tikvpb.BatchCommandsRequest_Request, 0, cfg.MaxBatchSize)
	requestIDs := make([]uint64, 0, cfg.MaxBatchSize)

	var bestBatchWaitSize = cfg.BatchWaitSize
	for {
		trace_util_0.Count(_client_00000, 79)
		// Choose a connection by round-robbin.
		next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
		batchCommandsClient := a.batchCommandsClients[next]

		entries = entries[:0]
		requests = requests[:0]
		requestIDs = requestIDs[:0]

		metrics.TiKVPendingBatchRequests.Set(float64(len(a.batchCommandsCh)))
		a.fetchAllPendingRequests(int(cfg.MaxBatchSize), &entries, &requests)

		if len(entries) < int(cfg.MaxBatchSize) && cfg.MaxBatchWaitTime > 0 {
			trace_util_0.Count(_client_00000, 85)
			tikvTransportLayerLoad := atomic.LoadUint64(batchCommandsClient.tikvTransportLayerLoad)
			// If the target TiKV is overload, wait a while to collect more requests.
			if uint(tikvTransportLayerLoad) >= cfg.OverloadThreshold {
				trace_util_0.Count(_client_00000, 86)
				fetchMorePendingRequests(
					a.batchCommandsCh, int(cfg.MaxBatchSize), int(bestBatchWaitSize),
					cfg.MaxBatchWaitTime, &entries, &requests,
				)
			}
		}
		trace_util_0.Count(_client_00000, 80)
		length := len(requests)
		if uint(length) == 0 {
			trace_util_0.Count(_client_00000, 87)
			// The batch command channel is closed.
			return
		} else {
			trace_util_0.Count(_client_00000, 88)
			if uint(length) < bestBatchWaitSize && bestBatchWaitSize > 1 {
				trace_util_0.Count(_client_00000, 89)
				// Waits too long to collect requests, reduce the target batch size.
				bestBatchWaitSize -= 1
			} else {
				trace_util_0.Count(_client_00000, 90)
				if uint(length) > bestBatchWaitSize+4 && bestBatchWaitSize < cfg.MaxBatchSize {
					trace_util_0.Count(_client_00000, 91)
					bestBatchWaitSize += 1
				}
			}
		}

		trace_util_0.Count(_client_00000, 81)
		length = removeCanceledRequests(&entries, &requests)
		if length == 0 {
			trace_util_0.Count(_client_00000, 92)
			continue // All requests are canceled.
		}
		trace_util_0.Count(_client_00000, 82)
		maxBatchID := atomic.AddUint64(&batchCommandsClient.idAlloc, uint64(length))
		for i := 0; i < length; i++ {
			trace_util_0.Count(_client_00000, 93)
			requestID := uint64(i) + maxBatchID - uint64(length)
			requestIDs = append(requestIDs, requestID)
		}

		trace_util_0.Count(_client_00000, 83)
		request := &tikvpb.BatchCommandsRequest{
			Requests:   requests,
			RequestIds: requestIDs,
		}

		// Use the lock to protect the stream client won't be replaced by RecvLoop,
		// and new added request won't be removed by `failPendingRequests`.
		batchCommandsClient.clientLock.Lock()
		for i, requestID := range request.RequestIds {
			trace_util_0.Count(_client_00000, 94)
			batchCommandsClient.batched.Store(requestID, entries[i])
		}
		trace_util_0.Count(_client_00000, 84)
		err := batchCommandsClient.client.Send(request)
		batchCommandsClient.clientLock.Unlock()
		if err != nil {
			trace_util_0.Count(_client_00000, 95)
			logutil.Logger(context.Background()).Error(
				"batch commands send error",
				zap.String("target", a.target),
				zap.Error(err),
			)
			batchCommandsClient.failPendingRequests(err)
		}
	}
}

// removeCanceledRequests removes canceled requests before sending.
func removeCanceledRequests(
	entries *[]*batchCommandsEntry,
	requests *[]*tikvpb.BatchCommandsRequest_Request) int {
	trace_util_0.Count(_client_00000, 96)
	validEntries := (*entries)[:0]
	validRequets := (*requests)[:0]
	for _, e := range *entries {
		trace_util_0.Count(_client_00000, 98)
		if !e.isCanceled() {
			trace_util_0.Count(_client_00000, 99)
			validEntries = append(validEntries, e)
			validRequets = append(validRequets, e.req)
		}
	}
	trace_util_0.Count(_client_00000, 97)
	*entries = validEntries
	*requests = validRequets
	return len(*entries)
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
	security config.Security

	// Implement background cleanup.
	// Periodically check whether there is any connection that is idle and then close and remove these idle connections.
	idleNotify uint32
}

func newRPCClient(security config.Security) *rpcClient {
	trace_util_0.Count(_client_00000, 100)
	return &rpcClient{
		conns:    make(map[string]*connArray),
		security: security,
	}
}

func (c *rpcClient) getConnArray(addr string) (*connArray, error) {
	trace_util_0.Count(_client_00000, 101)
	c.RLock()
	if c.isClosed {
		trace_util_0.Count(_client_00000, 104)
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	trace_util_0.Count(_client_00000, 102)
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		trace_util_0.Count(_client_00000, 105)
		var err error
		array, err = c.createConnArray(addr)
		if err != nil {
			trace_util_0.Count(_client_00000, 106)
			return nil, err
		}
	}
	trace_util_0.Count(_client_00000, 103)
	return array, nil
}

func (c *rpcClient) createConnArray(addr string) (*connArray, error) {
	trace_util_0.Count(_client_00000, 107)
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		trace_util_0.Count(_client_00000, 109)
		var err error
		connCount := config.GetGlobalConfig().TiKVClient.GrpcConnectionCount
		array, err = newConnArray(connCount, addr, c.security, &c.idleNotify)
		if err != nil {
			trace_util_0.Count(_client_00000, 111)
			return nil, err
		}
		trace_util_0.Count(_client_00000, 110)
		c.conns[addr] = array
	}
	trace_util_0.Count(_client_00000, 108)
	return array, nil
}

func (c *rpcClient) closeConns() {
	trace_util_0.Count(_client_00000, 112)
	c.Lock()
	if !c.isClosed {
		trace_util_0.Count(_client_00000, 114)
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			trace_util_0.Count(_client_00000, 115)
			array.Close()
		}
	}
	trace_util_0.Count(_client_00000, 113)
	c.Unlock()
}

func sendBatchRequest(
	ctx context.Context,
	addr string,
	connArray *connArray,
	req *tikvpb.BatchCommandsRequest_Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	trace_util_0.Count(_client_00000, 116)
	entry := &batchCommandsEntry{
		req:      req,
		res:      make(chan *tikvpb.BatchCommandsResponse_Response, 1),
		canceled: 0,
		err:      nil,
	}
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case connArray.batchCommandsCh <- entry:
		trace_util_0.Count(_client_00000, 118)
	case <-ctx1.Done():
		trace_util_0.Count(_client_00000, 119)
		logutil.Logger(context.Background()).Warn("send request is cancelled",
			zap.String("to", addr), zap.String("cause", ctx1.Err().Error()))
		return nil, errors.Trace(ctx1.Err())
	}

	trace_util_0.Count(_client_00000, 117)
	select {
	case res, ok := <-entry.res:
		trace_util_0.Count(_client_00000, 120)
		if !ok {
			trace_util_0.Count(_client_00000, 123)
			return nil, errors.Trace(entry.err)
		}
		trace_util_0.Count(_client_00000, 121)
		return tikvrpc.FromBatchCommandsResponse(res), nil
	case <-ctx1.Done():
		trace_util_0.Count(_client_00000, 122)
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.Logger(context.Background()).Warn("wait response is cancelled",
			zap.String("to", addr), zap.String("cause", ctx1.Err().Error()))
		return nil, errors.Trace(ctx1.Err())
	}
}

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	trace_util_0.Count(_client_00000, 124)
	start := time.Now()
	reqType := req.Type.String()
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	defer func() {
		trace_util_0.Count(_client_00000, 133)
		metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeID).Observe(time.Since(start).Seconds())
	}()

	trace_util_0.Count(_client_00000, 125)
	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		trace_util_0.Count(_client_00000, 134)
		c.recycleIdleConnArray()
	}

	trace_util_0.Count(_client_00000, 126)
	connArray, err := c.getConnArray(addr)
	if err != nil {
		trace_util_0.Count(_client_00000, 135)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_client_00000, 127)
	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 {
		trace_util_0.Count(_client_00000, 136)
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			trace_util_0.Count(_client_00000, 137)
			return sendBatchRequest(ctx, addr, connArray, batchReq, timeout)
		}
	}

	trace_util_0.Count(_client_00000, 128)
	if req.IsDebugReq() {
		trace_util_0.Count(_client_00000, 138)
		client := debugpb.NewDebugClient(connArray.Get())
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallDebugRPC(ctx1, client, req)
	}

	trace_util_0.Count(_client_00000, 129)
	client := tikvpb.NewTikvClient(connArray.Get())

	if req.Type != tikvrpc.CmdCopStream {
		trace_util_0.Count(_client_00000, 139)
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallRPC(ctx1, client, req)
	}

	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	trace_util_0.Count(_client_00000, 130)
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		trace_util_0.Count(_client_00000, 140)
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	trace_util_0.Count(_client_00000, 131)
	copStream := resp.CopStream
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.Response
	first, err = copStream.Recv()
	if err != nil {
		trace_util_0.Count(_client_00000, 141)
		if errors.Cause(err) != io.EOF {
			trace_util_0.Count(_client_00000, 143)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_client_00000, 142)
		logutil.Logger(context.Background()).Debug("copstream returns nothing for the request.")
	}
	trace_util_0.Count(_client_00000, 132)
	copStream.Response = first
	return resp, nil
}

func (c *rpcClient) Close() error {
	trace_util_0.Count(_client_00000, 144)
	c.closeConns()
	return nil
}

func (c *rpcClient) recycleIdleConnArray() {
	trace_util_0.Count(_client_00000, 145)
	var addrs []string
	c.RLock()
	for _, conn := range c.conns {
		trace_util_0.Count(_client_00000, 147)
		if conn.idle {
			trace_util_0.Count(_client_00000, 148)
			addrs = append(addrs, conn.target)
		}
	}
	trace_util_0.Count(_client_00000, 146)
	c.RUnlock()

	for _, addr := range addrs {
		trace_util_0.Count(_client_00000, 149)
		c.Lock()
		conn, ok := c.conns[addr]
		if ok {
			trace_util_0.Count(_client_00000, 151)
			delete(c.conns, addr)
			logutil.Logger(context.Background()).Info("recycle idle connection",
				zap.String("target", addr))
		}
		trace_util_0.Count(_client_00000, 150)
		c.Unlock()
		if conn != nil {
			trace_util_0.Count(_client_00000, 152)
			conn.Close()
		}
	}
}

var _client_00000 = "store/tikv/client.go"
