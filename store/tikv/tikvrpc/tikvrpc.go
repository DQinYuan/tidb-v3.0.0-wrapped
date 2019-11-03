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

package tikvrpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/trace_util_0"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange
	CmdPessimisticLock
	CmdPessimisticRollback

	CmdRawGet CmdType = 256 + iota
	CmdRawBatchGet
	CmdRawPut
	CmdRawBatchPut
	CmdRawDelete
	CmdRawBatchDelete
	CmdRawDeleteRange
	CmdRawScan

	CmdUnsafeDestroyRange

	CmdCop CmdType = 512 + iota
	CmdCopStream

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion

	CmdDebugGetRegionProperties CmdType = 2048 + iota
)

func (t CmdType) String() string {
	trace_util_0.Count(_tikvrpc_00000, 0)
	switch t {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 2)
		return "Get"
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 3)
		return "Scan"
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 4)
		return "Prewrite"
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 5)
		return "PessimisticLock"
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 6)
		return "PessimisticRollback"
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 7)
		return "Commit"
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 8)
		return "Cleanup"
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 9)
		return "BatchGet"
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 10)
		return "BatchRollback"
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 11)
		return "ScanLock"
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 12)
		return "ResolveLock"
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 13)
		return "GC"
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 14)
		return "DeleteRange"
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 15)
		return "RawGet"
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 16)
		return "RawBatchGet"
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 17)
		return "RawPut"
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 18)
		return "RawBatchPut"
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 19)
		return "RawDelete"
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 20)
		return "RawBatchDelete"
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 21)
		return "RawDeleteRange"
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 22)
		return "RawScan"
	case CmdUnsafeDestroyRange:
		trace_util_0.Count(_tikvrpc_00000, 23)
		return "UnsafeDestroyRange"
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 24)
		return "Cop"
	case CmdCopStream:
		trace_util_0.Count(_tikvrpc_00000, 25)
		return "CopStream"
	case CmdMvccGetByKey:
		trace_util_0.Count(_tikvrpc_00000, 26)
		return "MvccGetByKey"
	case CmdMvccGetByStartTs:
		trace_util_0.Count(_tikvrpc_00000, 27)
		return "MvccGetByStartTS"
	case CmdSplitRegion:
		trace_util_0.Count(_tikvrpc_00000, 28)
		return "SplitRegion"
	case CmdDebugGetRegionProperties:
		trace_util_0.Count(_tikvrpc_00000, 29)
		return "DebugGetRegionProperties"
	}
	trace_util_0.Count(_tikvrpc_00000, 1)
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	kvrpcpb.Context
	Type               CmdType
	Get                *kvrpcpb.GetRequest
	Scan               *kvrpcpb.ScanRequest
	Prewrite           *kvrpcpb.PrewriteRequest
	Commit             *kvrpcpb.CommitRequest
	Cleanup            *kvrpcpb.CleanupRequest
	BatchGet           *kvrpcpb.BatchGetRequest
	BatchRollback      *kvrpcpb.BatchRollbackRequest
	ScanLock           *kvrpcpb.ScanLockRequest
	ResolveLock        *kvrpcpb.ResolveLockRequest
	GC                 *kvrpcpb.GCRequest
	DeleteRange        *kvrpcpb.DeleteRangeRequest
	RawGet             *kvrpcpb.RawGetRequest
	RawBatchGet        *kvrpcpb.RawBatchGetRequest
	RawPut             *kvrpcpb.RawPutRequest
	RawBatchPut        *kvrpcpb.RawBatchPutRequest
	RawDelete          *kvrpcpb.RawDeleteRequest
	RawBatchDelete     *kvrpcpb.RawBatchDeleteRequest
	RawDeleteRange     *kvrpcpb.RawDeleteRangeRequest
	RawScan            *kvrpcpb.RawScanRequest
	UnsafeDestroyRange *kvrpcpb.UnsafeDestroyRangeRequest
	Cop                *coprocessor.Request
	MvccGetByKey       *kvrpcpb.MvccGetByKeyRequest
	MvccGetByStartTs   *kvrpcpb.MvccGetByStartTsRequest
	SplitRegion        *kvrpcpb.SplitRegionRequest

	PessimisticLock     *kvrpcpb.PessimisticLockRequest
	PessimisticRollback *kvrpcpb.PessimisticRollbackRequest

	DebugGetRegionProperties *debugpb.GetRegionPropertiesRequest
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request {
	trace_util_0.Count(_tikvrpc_00000, 30)
	switch req.Type {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 32)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: req.Get}}
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 33)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan}}
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 34)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite}}
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 35)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit}}
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 36)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup}}
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 37)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet}}
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 38)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback}}
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 39)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock}}
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 40)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock}}
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 41)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GC{GC: req.GC}}
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 42)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange}}
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 43)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet}}
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 44)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet}}
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 45)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut}}
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 46)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut}}
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 47)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete}}
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 48)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete}}
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 49)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange}}
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 50)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan}}
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 51)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Cop}}
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 52)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock}}
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 53)
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback}}
	}
	trace_util_0.Count(_tikvrpc_00000, 31)
	return nil
}

// IsDebugReq check whether the req is debug req.
func (req *Request) IsDebugReq() bool {
	trace_util_0.Count(_tikvrpc_00000, 54)
	switch req.Type {
	case CmdDebugGetRegionProperties:
		trace_util_0.Count(_tikvrpc_00000, 56)
		return true
	}
	trace_util_0.Count(_tikvrpc_00000, 55)
	return false
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Type               CmdType
	Get                *kvrpcpb.GetResponse
	Scan               *kvrpcpb.ScanResponse
	Prewrite           *kvrpcpb.PrewriteResponse
	Commit             *kvrpcpb.CommitResponse
	Cleanup            *kvrpcpb.CleanupResponse
	BatchGet           *kvrpcpb.BatchGetResponse
	BatchRollback      *kvrpcpb.BatchRollbackResponse
	ScanLock           *kvrpcpb.ScanLockResponse
	ResolveLock        *kvrpcpb.ResolveLockResponse
	GC                 *kvrpcpb.GCResponse
	DeleteRange        *kvrpcpb.DeleteRangeResponse
	RawGet             *kvrpcpb.RawGetResponse
	RawBatchGet        *kvrpcpb.RawBatchGetResponse
	RawPut             *kvrpcpb.RawPutResponse
	RawBatchPut        *kvrpcpb.RawBatchPutResponse
	RawDelete          *kvrpcpb.RawDeleteResponse
	RawBatchDelete     *kvrpcpb.RawBatchDeleteResponse
	RawDeleteRange     *kvrpcpb.RawDeleteRangeResponse
	RawScan            *kvrpcpb.RawScanResponse
	UnsafeDestroyRange *kvrpcpb.UnsafeDestroyRangeResponse
	Cop                *coprocessor.Response
	CopStream          *CopStreamResponse
	MvccGetByKey       *kvrpcpb.MvccGetByKeyResponse
	MvccGetByStartTS   *kvrpcpb.MvccGetByStartTsResponse
	SplitRegion        *kvrpcpb.SplitRegionResponse

	PessimisticLock     *kvrpcpb.PessimisticLockResponse
	PessimisticRollback *kvrpcpb.PessimisticRollbackResponse

	DebugGetRegionProperties *debugpb.GetRegionPropertiesResponse
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) *Response {
	trace_util_0.Count(_tikvrpc_00000, 57)
	switch res := res.GetCmd().(type) {
	case *tikvpb.BatchCommandsResponse_Response_Get:
		trace_util_0.Count(_tikvrpc_00000, 59)
		return &Response{Type: CmdGet, Get: res.Get}
	case *tikvpb.BatchCommandsResponse_Response_Scan:
		trace_util_0.Count(_tikvrpc_00000, 60)
		return &Response{Type: CmdScan, Scan: res.Scan}
	case *tikvpb.BatchCommandsResponse_Response_Prewrite:
		trace_util_0.Count(_tikvrpc_00000, 61)
		return &Response{Type: CmdPrewrite, Prewrite: res.Prewrite}
	case *tikvpb.BatchCommandsResponse_Response_Commit:
		trace_util_0.Count(_tikvrpc_00000, 62)
		return &Response{Type: CmdCommit, Commit: res.Commit}
	case *tikvpb.BatchCommandsResponse_Response_Cleanup:
		trace_util_0.Count(_tikvrpc_00000, 63)
		return &Response{Type: CmdCleanup, Cleanup: res.Cleanup}
	case *tikvpb.BatchCommandsResponse_Response_BatchGet:
		trace_util_0.Count(_tikvrpc_00000, 64)
		return &Response{Type: CmdBatchGet, BatchGet: res.BatchGet}
	case *tikvpb.BatchCommandsResponse_Response_BatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 65)
		return &Response{Type: CmdBatchRollback, BatchRollback: res.BatchRollback}
	case *tikvpb.BatchCommandsResponse_Response_ScanLock:
		trace_util_0.Count(_tikvrpc_00000, 66)
		return &Response{Type: CmdScanLock, ScanLock: res.ScanLock}
	case *tikvpb.BatchCommandsResponse_Response_ResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 67)
		return &Response{Type: CmdResolveLock, ResolveLock: res.ResolveLock}
	case *tikvpb.BatchCommandsResponse_Response_GC:
		trace_util_0.Count(_tikvrpc_00000, 68)
		return &Response{Type: CmdGC, GC: res.GC}
	case *tikvpb.BatchCommandsResponse_Response_DeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 69)
		return &Response{Type: CmdDeleteRange, DeleteRange: res.DeleteRange}
	case *tikvpb.BatchCommandsResponse_Response_RawGet:
		trace_util_0.Count(_tikvrpc_00000, 70)
		return &Response{Type: CmdRawGet, RawGet: res.RawGet}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 71)
		return &Response{Type: CmdRawBatchGet, RawBatchGet: res.RawBatchGet}
	case *tikvpb.BatchCommandsResponse_Response_RawPut:
		trace_util_0.Count(_tikvrpc_00000, 72)
		return &Response{Type: CmdRawPut, RawPut: res.RawPut}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 73)
		return &Response{Type: CmdRawBatchPut, RawBatchPut: res.RawBatchPut}
	case *tikvpb.BatchCommandsResponse_Response_RawDelete:
		trace_util_0.Count(_tikvrpc_00000, 74)
		return &Response{Type: CmdRawDelete, RawDelete: res.RawDelete}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 75)
		return &Response{Type: CmdRawBatchDelete, RawBatchDelete: res.RawBatchDelete}
	case *tikvpb.BatchCommandsResponse_Response_RawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 76)
		return &Response{Type: CmdRawDeleteRange, RawDeleteRange: res.RawDeleteRange}
	case *tikvpb.BatchCommandsResponse_Response_RawScan:
		trace_util_0.Count(_tikvrpc_00000, 77)
		return &Response{Type: CmdRawScan, RawScan: res.RawScan}
	case *tikvpb.BatchCommandsResponse_Response_Coprocessor:
		trace_util_0.Count(_tikvrpc_00000, 78)
		return &Response{Type: CmdCop, Cop: res.Coprocessor}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 79)
		return &Response{Type: CmdPessimisticLock, PessimisticLock: res.PessimisticLock}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 80)
		return &Response{Type: CmdPessimisticRollback, PessimisticRollback: res.PessimisticRollback}
	}
	trace_util_0.Count(_tikvrpc_00000, 58)
	return nil
}

// CopStreamResponse combinates tikvpb.Tikv_CoprocessorStreamClient and the first Recv() result together.
// In streaming API, get grpc stream client may not involve any network packet, then region error have
// to be handled in Recv() function. This struct facilitates the error handling.
type CopStreamResponse struct {
	tikvpb.Tikv_CoprocessorStreamClient
	*coprocessor.Response // The first result of Recv()
	Timeout               time.Duration
	Lease                 // Shared by this object and a background goroutine.
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	trace_util_0.Count(_tikvrpc_00000, 81)
	ctx := &req.Context
	ctx.RegionId = region.Id
	ctx.RegionEpoch = region.RegionEpoch
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 83)
		req.Get.Context = ctx
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 84)
		req.Scan.Context = ctx
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 85)
		req.Prewrite.Context = ctx
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 86)
		req.PessimisticLock.Context = ctx
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 87)
		req.PessimisticRollback.Context = ctx
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 88)
		req.Commit.Context = ctx
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 89)
		req.Cleanup.Context = ctx
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 90)
		req.BatchGet.Context = ctx
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 91)
		req.BatchRollback.Context = ctx
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 92)
		req.ScanLock.Context = ctx
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 93)
		req.ResolveLock.Context = ctx
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 94)
		req.GC.Context = ctx
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 95)
		req.DeleteRange.Context = ctx
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 96)
		req.RawGet.Context = ctx
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 97)
		req.RawBatchGet.Context = ctx
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 98)
		req.RawPut.Context = ctx
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 99)
		req.RawBatchPut.Context = ctx
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 100)
		req.RawDelete.Context = ctx
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 101)
		req.RawBatchDelete.Context = ctx
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 102)
		req.RawDeleteRange.Context = ctx
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 103)
		req.RawScan.Context = ctx
	case CmdUnsafeDestroyRange:
		trace_util_0.Count(_tikvrpc_00000, 104)
		req.UnsafeDestroyRange.Context = ctx
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 105)
		req.Cop.Context = ctx
	case CmdCopStream:
		trace_util_0.Count(_tikvrpc_00000, 106)
		req.Cop.Context = ctx
	case CmdMvccGetByKey:
		trace_util_0.Count(_tikvrpc_00000, 107)
		req.MvccGetByKey.Context = ctx
	case CmdMvccGetByStartTs:
		trace_util_0.Count(_tikvrpc_00000, 108)
		req.MvccGetByStartTs.Context = ctx
	case CmdSplitRegion:
		trace_util_0.Count(_tikvrpc_00000, 109)
		req.SplitRegion.Context = ctx
	default:
		trace_util_0.Count(_tikvrpc_00000, 110)
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	trace_util_0.Count(_tikvrpc_00000, 82)
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	trace_util_0.Count(_tikvrpc_00000, 111)
	resp := &Response{}
	resp.Type = req.Type
	switch req.Type {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 113)
		resp.Get = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 114)
		resp.Scan = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 115)
		resp.Prewrite = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 116)
		resp.PessimisticLock = &kvrpcpb.PessimisticLockResponse{
			RegionError: e,
		}
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 117)
		resp.PessimisticRollback = &kvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		}
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 118)
		resp.Commit = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 119)
		resp.Cleanup = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 120)
		resp.BatchGet = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 121)
		resp.BatchRollback = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 122)
		resp.ScanLock = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 123)
		resp.ResolveLock = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 124)
		resp.GC = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 125)
		resp.DeleteRange = &kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 126)
		resp.RawGet = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 127)
		resp.RawBatchGet = &kvrpcpb.RawBatchGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 128)
		resp.RawPut = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 129)
		resp.RawBatchPut = &kvrpcpb.RawBatchPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 130)
		resp.RawDelete = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 131)
		resp.RawBatchDelete = &kvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		}
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 132)
		resp.RawDeleteRange = &kvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 133)
		resp.RawScan = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdUnsafeDestroyRange:
		trace_util_0.Count(_tikvrpc_00000, 134)
		resp.UnsafeDestroyRange = &kvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		}
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 135)
		resp.Cop = &coprocessor.Response{
			RegionError: e,
		}
	case CmdCopStream:
		trace_util_0.Count(_tikvrpc_00000, 136)
		resp.CopStream = &CopStreamResponse{
			Response: &coprocessor.Response{
				RegionError: e,
			},
		}
	case CmdMvccGetByKey:
		trace_util_0.Count(_tikvrpc_00000, 137)
		resp.MvccGetByKey = &kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		trace_util_0.Count(_tikvrpc_00000, 138)
		resp.MvccGetByStartTS = &kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		trace_util_0.Count(_tikvrpc_00000, 139)
		resp.SplitRegion = &kvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	default:
		trace_util_0.Count(_tikvrpc_00000, 140)
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	trace_util_0.Count(_tikvrpc_00000, 112)
	return resp, nil
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	trace_util_0.Count(_tikvrpc_00000, 141)
	var e *errorpb.Error
	switch resp.Type {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 143)
		e = resp.Get.GetRegionError()
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 144)
		e = resp.Scan.GetRegionError()
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 145)
		e = resp.PessimisticLock.GetRegionError()
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 146)
		e = resp.PessimisticRollback.GetRegionError()
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 147)
		e = resp.Prewrite.GetRegionError()
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 148)
		e = resp.Commit.GetRegionError()
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 149)
		e = resp.Cleanup.GetRegionError()
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 150)
		e = resp.BatchGet.GetRegionError()
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 151)
		e = resp.BatchRollback.GetRegionError()
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 152)
		e = resp.ScanLock.GetRegionError()
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 153)
		e = resp.ResolveLock.GetRegionError()
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 154)
		e = resp.GC.GetRegionError()
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 155)
		e = resp.DeleteRange.GetRegionError()
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 156)
		e = resp.RawGet.GetRegionError()
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 157)
		e = resp.RawBatchGet.GetRegionError()
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 158)
		e = resp.RawPut.GetRegionError()
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 159)
		e = resp.RawBatchPut.GetRegionError()
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 160)
		e = resp.RawDelete.GetRegionError()
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 161)
		e = resp.RawBatchDelete.GetRegionError()
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 162)
		e = resp.RawDeleteRange.GetRegionError()
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 163)
		e = resp.RawScan.GetRegionError()
	case CmdUnsafeDestroyRange:
		trace_util_0.Count(_tikvrpc_00000, 164)
		e = resp.UnsafeDestroyRange.GetRegionError()
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 165)
		e = resp.Cop.GetRegionError()
	case CmdCopStream:
		trace_util_0.Count(_tikvrpc_00000, 166)
		e = resp.CopStream.Response.GetRegionError()
	case CmdMvccGetByKey:
		trace_util_0.Count(_tikvrpc_00000, 167)
		e = resp.MvccGetByKey.GetRegionError()
	case CmdMvccGetByStartTs:
		trace_util_0.Count(_tikvrpc_00000, 168)
		e = resp.MvccGetByStartTS.GetRegionError()
	case CmdSplitRegion:
		trace_util_0.Count(_tikvrpc_00000, 169)
		e = resp.SplitRegion.GetRegionError()
	default:
		trace_util_0.Count(_tikvrpc_00000, 170)
		return nil, fmt.Errorf("invalid response type %v", resp.Type)
	}
	trace_util_0.Count(_tikvrpc_00000, 142)
	return e, nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaing, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	trace_util_0.Count(_tikvrpc_00000, 171)
	resp := &Response{}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case CmdGet:
		trace_util_0.Count(_tikvrpc_00000, 174)
		resp.Get, err = client.KvGet(ctx, req.Get)
	case CmdScan:
		trace_util_0.Count(_tikvrpc_00000, 175)
		resp.Scan, err = client.KvScan(ctx, req.Scan)
	case CmdPrewrite:
		trace_util_0.Count(_tikvrpc_00000, 176)
		resp.Prewrite, err = client.KvPrewrite(ctx, req.Prewrite)
	case CmdPessimisticLock:
		trace_util_0.Count(_tikvrpc_00000, 177)
		resp.PessimisticLock, err = client.KvPessimisticLock(ctx, req.PessimisticLock)
	case CmdPessimisticRollback:
		trace_util_0.Count(_tikvrpc_00000, 178)
		resp.PessimisticRollback, err = client.KVPessimisticRollback(ctx, req.PessimisticRollback)
	case CmdCommit:
		trace_util_0.Count(_tikvrpc_00000, 179)
		resp.Commit, err = client.KvCommit(ctx, req.Commit)
	case CmdCleanup:
		trace_util_0.Count(_tikvrpc_00000, 180)
		resp.Cleanup, err = client.KvCleanup(ctx, req.Cleanup)
	case CmdBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 181)
		resp.BatchGet, err = client.KvBatchGet(ctx, req.BatchGet)
	case CmdBatchRollback:
		trace_util_0.Count(_tikvrpc_00000, 182)
		resp.BatchRollback, err = client.KvBatchRollback(ctx, req.BatchRollback)
	case CmdScanLock:
		trace_util_0.Count(_tikvrpc_00000, 183)
		resp.ScanLock, err = client.KvScanLock(ctx, req.ScanLock)
	case CmdResolveLock:
		trace_util_0.Count(_tikvrpc_00000, 184)
		resp.ResolveLock, err = client.KvResolveLock(ctx, req.ResolveLock)
	case CmdGC:
		trace_util_0.Count(_tikvrpc_00000, 185)
		resp.GC, err = client.KvGC(ctx, req.GC)
	case CmdDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 186)
		resp.DeleteRange, err = client.KvDeleteRange(ctx, req.DeleteRange)
	case CmdRawGet:
		trace_util_0.Count(_tikvrpc_00000, 187)
		resp.RawGet, err = client.RawGet(ctx, req.RawGet)
	case CmdRawBatchGet:
		trace_util_0.Count(_tikvrpc_00000, 188)
		resp.RawBatchGet, err = client.RawBatchGet(ctx, req.RawBatchGet)
	case CmdRawPut:
		trace_util_0.Count(_tikvrpc_00000, 189)
		resp.RawPut, err = client.RawPut(ctx, req.RawPut)
	case CmdRawBatchPut:
		trace_util_0.Count(_tikvrpc_00000, 190)
		resp.RawBatchPut, err = client.RawBatchPut(ctx, req.RawBatchPut)
	case CmdRawDelete:
		trace_util_0.Count(_tikvrpc_00000, 191)
		resp.RawDelete, err = client.RawDelete(ctx, req.RawDelete)
	case CmdRawBatchDelete:
		trace_util_0.Count(_tikvrpc_00000, 192)
		resp.RawBatchDelete, err = client.RawBatchDelete(ctx, req.RawBatchDelete)
	case CmdRawDeleteRange:
		trace_util_0.Count(_tikvrpc_00000, 193)
		resp.RawDeleteRange, err = client.RawDeleteRange(ctx, req.RawDeleteRange)
	case CmdRawScan:
		trace_util_0.Count(_tikvrpc_00000, 194)
		resp.RawScan, err = client.RawScan(ctx, req.RawScan)
	case CmdUnsafeDestroyRange:
		trace_util_0.Count(_tikvrpc_00000, 195)
		resp.UnsafeDestroyRange, err = client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange)
	case CmdCop:
		trace_util_0.Count(_tikvrpc_00000, 196)
		resp.Cop, err = client.Coprocessor(ctx, req.Cop)
	case CmdCopStream:
		trace_util_0.Count(_tikvrpc_00000, 197)
		var streamClient tikvpb.Tikv_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Cop)
		resp.CopStream = &CopStreamResponse{
			Tikv_CoprocessorStreamClient: streamClient,
		}
	case CmdMvccGetByKey:
		trace_util_0.Count(_tikvrpc_00000, 198)
		resp.MvccGetByKey, err = client.MvccGetByKey(ctx, req.MvccGetByKey)
	case CmdMvccGetByStartTs:
		trace_util_0.Count(_tikvrpc_00000, 199)
		resp.MvccGetByStartTS, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs)
	case CmdSplitRegion:
		trace_util_0.Count(_tikvrpc_00000, 200)
		resp.SplitRegion, err = client.SplitRegion(ctx, req.SplitRegion)
	default:
		trace_util_0.Count(_tikvrpc_00000, 201)
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	trace_util_0.Count(_tikvrpc_00000, 172)
	if err != nil {
		trace_util_0.Count(_tikvrpc_00000, 202)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_tikvrpc_00000, 173)
	return resp, nil
}

// CallDebugRPC launches a debug rpc call.
func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	trace_util_0.Count(_tikvrpc_00000, 203)
	resp := &Response{Type: req.Type}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		trace_util_0.Count(_tikvrpc_00000, 205)
		resp.DebugGetRegionProperties, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties)
	default:
		trace_util_0.Count(_tikvrpc_00000, 206)
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	trace_util_0.Count(_tikvrpc_00000, 204)
	return resp, err
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel   context.CancelFunc
	deadline int64 // A time.UnixNano value, if time.Now().UnixNano() > deadline, cancel() would be called.
}

// Recv overrides the stream client Recv() function.
func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error) {
	trace_util_0.Count(_tikvrpc_00000, 207)
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_CoprocessorStreamClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.Trace(err)
}

// Close closes the CopStreamResponse object.
func (resp *CopStreamResponse) Close() {
	trace_util_0.Count(_tikvrpc_00000, 208)
	atomic.StoreInt64(&resp.Lease.deadline, 1)
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timeouted.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
func CheckStreamTimeoutLoop(ch <-chan *Lease) {
	trace_util_0.Count(_tikvrpc_00000, 209)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		trace_util_0.Count(_tikvrpc_00000, 210)
		select {
		case item, ok := <-ch:
			trace_util_0.Count(_tikvrpc_00000, 211)
			if !ok {
				trace_util_0.Count(_tikvrpc_00000, 214)
				// This channel close means goroutine should return.
				return
			}
			trace_util_0.Count(_tikvrpc_00000, 212)
			array = append(array, item)
		case now := <-ticker.C:
			trace_util_0.Count(_tikvrpc_00000, 213)
			array = keepOnlyActive(array, now.UnixNano())
		}
	}
}

// keepOnlyActive removes completed items, call cancel function for timeout items.
func keepOnlyActive(array []*Lease, now int64) []*Lease {
	trace_util_0.Count(_tikvrpc_00000, 215)
	idx := 0
	for i := 0; i < len(array); i++ {
		trace_util_0.Count(_tikvrpc_00000, 217)
		item := array[i]
		deadline := atomic.LoadInt64(&item.deadline)
		if deadline == 0 || deadline > now {
			trace_util_0.Count(_tikvrpc_00000, 218)
			array[idx] = array[i]
			idx++
		} else {
			trace_util_0.Count(_tikvrpc_00000, 219)
			{
				item.Cancel()
			}
		}
	}
	trace_util_0.Count(_tikvrpc_00000, 216)
	return array[:idx]
}

var _tikvrpc_00000 = "store/tikv/tikvrpc/tikvrpc.go"
