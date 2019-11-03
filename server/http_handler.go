// Copyright 2018 PingCAP, Inc.
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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	pDBName     = "db"
	pHexKey     = "hexKey"
	pIndexName  = "index"
	pHandle     = "handle"
	pRegionID   = "regionID"
	pStartTS    = "startTS"
	pTableName  = "table"
	pTableID    = "tableID"
	pColumnID   = "colID"
	pColumnTp   = "colTp"
	pColumnFlag = "colFlag"
	pColumnLen  = "colLen"
	pRowBin     = "rowBin"
	pSnapshot   = "snapshot"
)

// For query string
const qTableID = "table_id"
const qLimit = "limit"

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

func writeError(w http.ResponseWriter, err error) {
	trace_util_0.Count(_http_handler_00000, 0)
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

func writeData(w http.ResponseWriter, data interface{}) {
	trace_util_0.Count(_http_handler_00000, 1)
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 3)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 2)
	logutil.Logger(context.Background()).Info(string(js))
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

type tikvHandlerTool struct {
	helper.Helper
}

// newTikvHandlerTool checks and prepares for tikv handler.
// It would panic when any error happens.
func (s *Server) newTikvHandlerTool() *tikvHandlerTool {
	trace_util_0.Count(_http_handler_00000, 4)
	var tikvStore tikv.Storage
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		trace_util_0.Count(_http_handler_00000, 7)
		panic("Invalid KvStore with illegal driver")
	}

	trace_util_0.Count(_http_handler_00000, 5)
	if tikvStore, ok = store.store.(tikv.Storage); !ok {
		trace_util_0.Count(_http_handler_00000, 8)
		panic("Invalid KvStore with illegal store")
	}

	trace_util_0.Count(_http_handler_00000, 6)
	regionCache := tikvStore.GetRegionCache()

	return &tikvHandlerTool{
		helper.Helper{
			RegionCache: regionCache,
			Store:       tikvStore,
		},
	}
}

type mvccKV struct {
	Key   string                        `json:"key"`
	Value *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

func (t *tikvHandlerTool) getMvccByHandle(tableID, handle int64) (*mvccKV, error) {
	trace_util_0.Count(_http_handler_00000, 9)
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	data, err := t.GetMvccByEncodedKey(encodedKey)
	return &mvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), Value: data}, err
}

func (t *tikvHandlerTool) getMvccByStartTs(startTS uint64, startKey, endKey []byte) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	trace_util_0.Count(_http_handler_00000, 10)
	bo := tikv.NewBackoffer(context.Background(), 5000)
	for {
		trace_util_0.Count(_http_handler_00000, 11)
		curRegion, err := t.RegionCache.LocateKey(bo, startKey)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 19)
			logutil.Logger(context.Background()).Error("get MVCC by startTS failed", zap.Uint64("txnStartTS", startTS), zap.Binary("startKey", startKey), zap.Error(err))
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_http_handler_00000, 12)
		tikvReq := &tikvrpc.Request{
			Type: tikvrpc.CmdMvccGetByStartTs,
			MvccGetByStartTs: &kvrpcpb.MvccGetByStartTsRequest{
				StartTs: startTS,
			},
		}
		tikvReq.Context.Priority = kvrpcpb.CommandPri_Low
		kvResp, err := t.Store.SendReq(bo, tikvReq, curRegion.Region, time.Hour)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 20)
			logutil.Logger(context.Background()).Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Binary("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Binary("curRegion startKey", curRegion.StartKey),
				zap.Binary("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.Error(err))
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_http_handler_00000, 13)
		data := kvResp.MvccGetByStartTS
		if err := data.GetRegionError(); err != nil {
			trace_util_0.Count(_http_handler_00000, 21)
			logutil.Logger(context.Background()).Warn("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Binary("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Binary("curRegion startKey", curRegion.StartKey),
				zap.Binary("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.Stringer("error", err))
			continue
		}

		trace_util_0.Count(_http_handler_00000, 14)
		if len(data.GetError()) > 0 {
			trace_util_0.Count(_http_handler_00000, 22)
			logutil.Logger(context.Background()).Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Binary("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Binary("curRegion startKey", curRegion.StartKey),
				zap.Binary("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.String("error", data.GetError()))
			return nil, errors.New(data.GetError())
		}

		trace_util_0.Count(_http_handler_00000, 15)
		key := data.GetKey()
		if len(key) > 0 {
			trace_util_0.Count(_http_handler_00000, 23)
			return data, nil
		}

		trace_util_0.Count(_http_handler_00000, 16)
		if len(endKey) > 0 && curRegion.Contains(endKey) {
			trace_util_0.Count(_http_handler_00000, 24)
			return nil, nil
		}
		trace_util_0.Count(_http_handler_00000, 17)
		if len(curRegion.EndKey) == 0 {
			trace_util_0.Count(_http_handler_00000, 25)
			return nil, nil
		}
		trace_util_0.Count(_http_handler_00000, 18)
		startKey = curRegion.EndKey
	}
}

func (t *tikvHandlerTool) getMvccByIdxValue(idx table.Index, values url.Values, idxCols []*model.ColumnInfo, handleStr string) (*mvccKV, error) {
	trace_util_0.Count(_http_handler_00000, 26)
	sc := new(stmtctx.StatementContext)
	// HTTP request is not a database session, set timezone to UTC directly here.
	// See https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md for more details.
	sc.TimeZone = time.UTC
	idxRow, err := t.formValue2DatumRow(sc, values, idxCols)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 30)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 27)
	handle, err := strconv.ParseInt(handleStr, 10, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 31)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 28)
	encodedKey, _, err := idx.GenIndexKey(sc, idxRow, handle, nil)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 32)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 29)
	data, err := t.GetMvccByEncodedKey(encodedKey)
	return &mvccKV{strings.ToUpper(hex.EncodeToString(encodedKey)), data}, err
}

// formValue2DatumRow converts URL query string to a Datum Row.
func (t *tikvHandlerTool) formValue2DatumRow(sc *stmtctx.StatementContext, values url.Values, idxCols []*model.ColumnInfo) ([]types.Datum, error) {
	trace_util_0.Count(_http_handler_00000, 33)
	data := make([]types.Datum, len(idxCols))
	for i, col := range idxCols {
		trace_util_0.Count(_http_handler_00000, 35)
		colName := col.Name.String()
		vals, ok := values[colName]
		if !ok {
			trace_util_0.Count(_http_handler_00000, 37)
			return nil, errors.BadRequestf("Missing value for index column %s.", colName)
		}

		trace_util_0.Count(_http_handler_00000, 36)
		switch len(vals) {
		case 0:
			trace_util_0.Count(_http_handler_00000, 38)
			data[i].SetNull()
		case 1:
			trace_util_0.Count(_http_handler_00000, 39)
			bDatum := types.NewStringDatum(vals[0])
			cDatum, err := bDatum.ConvertTo(sc, &col.FieldType)
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 42)
				return nil, errors.Trace(err)
			}
			trace_util_0.Count(_http_handler_00000, 40)
			data[i] = cDatum
		default:
			trace_util_0.Count(_http_handler_00000, 41)
			return nil, errors.BadRequestf("Invalid query form for column '%s', it's values are %v."+
				" Column value should be unique for one index record.", colName, vals)
		}
	}
	trace_util_0.Count(_http_handler_00000, 34)
	return data, nil
}

func (t *tikvHandlerTool) getTableID(dbName, tableName string) (int64, error) {
	trace_util_0.Count(_http_handler_00000, 43)
	tbInfo, err := t.getTable(dbName, tableName)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 45)
		return 0, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 44)
	return tbInfo.ID, nil
}

func (t *tikvHandlerTool) getTable(dbName, tableName string) (*model.TableInfo, error) {
	trace_util_0.Count(_http_handler_00000, 46)
	schema, err := t.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 49)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 47)
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 50)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 48)
	return tableVal.Meta(), nil
}

func (t *tikvHandlerTool) schema() (infoschema.InfoSchema, error) {
	trace_util_0.Count(_http_handler_00000, 51)
	session, err := session.CreateSession(t.Store)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 53)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 52)
	return domain.GetDomain(session.(sessionctx.Context)).InfoSchema(), nil
}

func (t *tikvHandlerTool) handleMvccGetByHex(params map[string]string) (interface{}, error) {
	trace_util_0.Count(_http_handler_00000, 54)
	encodedKey, err := hex.DecodeString(params[pHexKey])
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 56)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 55)
	return t.GetMvccByEncodedKey(encodedKey)
}

// settingsHandler is the handler for list tidb server settings.
type settingsHandler struct {
}

// binlogRecover is used to recover binlog service.
// When config binlog IgnoreError, binlog service will stop after meeting the first error.
// It can be recovered using HTTP API.
type binlogRecover struct{}

// schemaHandler is the handler for list database or table schemas.
type schemaHandler struct {
	*tikvHandlerTool
}

type dbTableHandler struct {
	*tikvHandlerTool
}

// regionHandler is the common field for http handler. It contains
// some common functions for all handlers.
type regionHandler struct {
	*tikvHandlerTool
}

// tableHandler is the handler for list table's regions.
type tableHandler struct {
	*tikvHandlerTool
	op string
}

// ddlHistoryJobHandler is the handler for list job history.
type ddlHistoryJobHandler struct {
	*tikvHandlerTool
}

// ddlResignOwnerHandler is the handler for resigning ddl owner.
type ddlResignOwnerHandler struct {
	store kv.Storage
}

type serverInfoHandler struct {
	*tikvHandlerTool
}

type allServerInfoHandler struct {
	*tikvHandlerTool
}

// valueHandler is the handler for get value.
type valueHandler struct {
}

const (
	opTableRegions     = "regions"
	opTableDiskUsage   = "disk-usage"
	opTableScatter     = "scatter-table"
	opStopTableScatter = "stop-scatter-table"
)

// mvccTxnHandler is the handler for txn debugger.
type mvccTxnHandler struct {
	*tikvHandlerTool
	op string
}

const (
	opMvccGetByHex = "hex"
	opMvccGetByKey = "key"
	opMvccGetByIdx = "idx"
	opMvccGetByTxn = "txn"
)

// ServeHTTP handles request of list a database or table's schemas.
func (vh valueHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 57)
	// parse params
	params := mux.Vars(req)

	colID, err := strconv.ParseInt(params[pColumnID], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 68)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 58)
	colTp, err := strconv.ParseInt(params[pColumnTp], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 69)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 59)
	colFlag, err := strconv.ParseUint(params[pColumnFlag], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 70)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 60)
	colLen, err := strconv.ParseInt(params[pColumnLen], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 71)
		writeError(w, err)
		return
	}

	// Get the unchanged binary.
	trace_util_0.Count(_http_handler_00000, 61)
	if req.URL == nil {
		trace_util_0.Count(_http_handler_00000, 72)
		err = errors.BadRequestf("Invalid URL")
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 62)
	values := make(url.Values)
	shouldUnescape := false
	err = parseQuery(req.URL.RawQuery, values, shouldUnescape)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 73)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 63)
	if len(values[pRowBin]) != 1 {
		trace_util_0.Count(_http_handler_00000, 74)
		err = errors.BadRequestf("Invalid Query:%v", values[pRowBin])
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 64)
	bin := values[pRowBin][0]
	valData, err := base64.StdEncoding.DecodeString(bin)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 75)
		writeError(w, err)
		return
	}
	// Construct field type.
	trace_util_0.Count(_http_handler_00000, 65)
	defaultDecimal := 6
	ft := &types.FieldType{
		Tp:      byte(colTp),
		Flag:    uint(colFlag),
		Flen:    int(colLen),
		Decimal: defaultDecimal,
	}
	// Decode a column.
	m := make(map[int64]*types.FieldType, 1)
	m[int64(colID)] = ft
	loc := time.UTC
	vals, err := tablecodec.DecodeRow(valData, m, loc)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 76)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_http_handler_00000, 66)
	v := vals[int64(colID)]
	val, err := v.ToString()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 77)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 67)
	writeData(w, val)
}

// TableRegions is the response data for list table's regions.
// It contains regions list for record and indices.
type TableRegions struct {
	TableName     string         `json:"name"`
	TableID       int64          `json:"id"`
	RecordRegions []RegionMeta   `json:"record_regions"`
	Indices       []IndexRegions `json:"indices"`
}

// RegionMeta contains a region's peer detail
type RegionMeta struct {
	ID          uint64              `json:"region_id"`
	Leader      *metapb.Peer        `json:"leader"`
	Peers       []*metapb.Peer      `json:"peers"`
	RegionEpoch *metapb.RegionEpoch `json:"region_epoch"`
}

// IndexRegions is the region info for one index.
type IndexRegions struct {
	Name    string       `json:"name"`
	ID      int64        `json:"id"`
	Regions []RegionMeta `json:"regions"`
}

// RegionDetail is the response data for get region by ID
// it includes indices and records detail in current region.
type RegionDetail struct {
	RegionID uint64              `json:"region_id"`
	StartKey []byte              `json:"start_key"`
	EndKey   []byte              `json:"end_key"`
	Frames   []*helper.FrameItem `json:"frames"`
}

// addTableInRange insert a table into RegionDetail
// with index's id or record in the range if r.
func (rt *RegionDetail) addTableInRange(dbName string, curTable *model.TableInfo, r *helper.RegionFrameRange) {
	trace_util_0.Count(_http_handler_00000, 78)
	tName := curTable.Name.String()
	tID := curTable.ID

	for _, index := range curTable.Indices {
		trace_util_0.Count(_http_handler_00000, 80)
		if f := r.GetIndexFrame(tID, index.ID, dbName, tName, index.Name.String()); f != nil {
			trace_util_0.Count(_http_handler_00000, 81)
			rt.Frames = append(rt.Frames, f)
		}
	}
	trace_util_0.Count(_http_handler_00000, 79)
	if f := r.GetRecordFrame(tID, dbName, tName); f != nil {
		trace_util_0.Count(_http_handler_00000, 82)
		rt.Frames = append(rt.Frames, f)
	}
}

// FrameItem includes a index's or record's meta data with table's info.
type FrameItem struct {
	DBName      string   `json:"db_name"`
	TableName   string   `json:"table_name"`
	TableID     int64    `json:"table_id"`
	IsRecord    bool     `json:"is_record"`
	RecordID    int64    `json:"record_id,omitempty"`
	IndexName   string   `json:"index_name,omitempty"`
	IndexID     int64    `json:"index_id,omitempty"`
	IndexValues []string `json:"index_values,omitempty"`
}

// RegionFrameRange contains a frame range info which the region covered.
type RegionFrameRange struct {
	first  *FrameItem        // start frame of the region
	last   *FrameItem        // end frame of the region
	region *tikv.KeyLocation // the region
}

func (t *tikvHandlerTool) getRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	trace_util_0.Count(_http_handler_00000, 83)
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		trace_util_0.Count(_http_handler_00000, 85)
		meta, leader, err := t.RegionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 89)
			return nil, errors.Trace(err)
		}

		trace_util_0.Count(_http_handler_00000, 86)
		failpoint.Inject("errGetRegionByIDEmpty", func(val failpoint.Value) {
			trace_util_0.Count(_http_handler_00000, 90)
			if val.(bool) {
				trace_util_0.Count(_http_handler_00000, 91)
				meta = nil
			}
		})

		trace_util_0.Count(_http_handler_00000, 87)
		if meta == nil {
			trace_util_0.Count(_http_handler_00000, 92)
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		trace_util_0.Count(_http_handler_00000, 88)
		regions[i] = RegionMeta{
			ID:          regionID,
			Leader:      leader,
			Peers:       meta.Peers,
			RegionEpoch: meta.RegionEpoch,
		}

	}
	trace_util_0.Count(_http_handler_00000, 84)
	return regions, nil
}

// ServeHTTP handles request of list tidb server settings.
func (h settingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 93)
	if req.Method == "POST" {
		trace_util_0.Count(_http_handler_00000, 94)
		err := req.ParseForm()
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 99)
			writeError(w, err)
			return
		}
		trace_util_0.Count(_http_handler_00000, 95)
		if levelStr := req.Form.Get("log_level"); levelStr != "" {
			trace_util_0.Count(_http_handler_00000, 100)
			err1 := logutil.SetLevel(levelStr)
			if err1 != nil {
				trace_util_0.Count(_http_handler_00000, 103)
				writeError(w, err1)
				return
			}

			trace_util_0.Count(_http_handler_00000, 101)
			l, err1 := log.ParseLevel(levelStr)
			if err1 != nil {
				trace_util_0.Count(_http_handler_00000, 104)
				writeError(w, err1)
				return
			}
			trace_util_0.Count(_http_handler_00000, 102)
			log.SetLevel(l)

			config.GetGlobalConfig().Log.Level = levelStr
		}
		trace_util_0.Count(_http_handler_00000, 96)
		if generalLog := req.Form.Get("tidb_general_log"); generalLog != "" {
			trace_util_0.Count(_http_handler_00000, 105)
			switch generalLog {
			case "0":
				trace_util_0.Count(_http_handler_00000, 106)
				atomic.StoreUint32(&variable.ProcessGeneralLog, 0)
			case "1":
				trace_util_0.Count(_http_handler_00000, 107)
				atomic.StoreUint32(&variable.ProcessGeneralLog, 1)
			default:
				trace_util_0.Count(_http_handler_00000, 108)
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
		trace_util_0.Count(_http_handler_00000, 97)
		if ddlSlowThreshold := req.Form.Get("ddl_slow_threshold"); ddlSlowThreshold != "" {
			trace_util_0.Count(_http_handler_00000, 109)
			threshold, err1 := strconv.Atoi(ddlSlowThreshold)
			if err1 != nil {
				trace_util_0.Count(_http_handler_00000, 111)
				writeError(w, err1)
				return
			}
			trace_util_0.Count(_http_handler_00000, 110)
			if threshold > 0 {
				trace_util_0.Count(_http_handler_00000, 112)
				atomic.StoreUint32(&variable.DDLSlowOprThreshold, uint32(threshold))
			}
		}
		trace_util_0.Count(_http_handler_00000, 98)
		if checkMb4ValueInUtf8 := req.Form.Get("check_mb4_value_in_utf8"); checkMb4ValueInUtf8 != "" {
			trace_util_0.Count(_http_handler_00000, 113)
			switch checkMb4ValueInUtf8 {
			case "0":
				trace_util_0.Count(_http_handler_00000, 114)
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = false
			case "1":
				trace_util_0.Count(_http_handler_00000, 115)
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = true
			default:
				trace_util_0.Count(_http_handler_00000, 116)
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
	} else {
		trace_util_0.Count(_http_handler_00000, 117)
		{
			writeData(w, config.GetGlobalConfig())
		}
	}
}

// configReloadHandler is the handler for reloading config online.
type configReloadHandler struct {
}

// ServeHTTP handles request of reloading config for this server.
func (h configReloadHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 118)
	if err := config.ReloadGlobalConfig(); err != nil {
		trace_util_0.Count(_http_handler_00000, 119)
		writeError(w, err)
	} else {
		trace_util_0.Count(_http_handler_00000, 120)
		{
			writeData(w, "success!")
		}
	}
}

// ServeHTTP recovers binlog service.
func (h binlogRecover) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 121)
	binloginfo.DisableSkipBinlogFlag()
}

// ServeHTTP handles request of list a database or table's schemas.
func (h schemaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 122)
	schema, err := h.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 126)
		writeError(w, err)
		return
	}

	// parse params
	trace_util_0.Count(_http_handler_00000, 123)
	params := mux.Vars(req)

	if dbName, ok := params[pDBName]; ok {
		trace_util_0.Count(_http_handler_00000, 127)
		cDBName := model.NewCIStr(dbName)
		if tableName, ok := params[pTableName]; ok {
			trace_util_0.Count(_http_handler_00000, 130)
			// table schema of a specified table name
			cTableName := model.NewCIStr(tableName)
			data, err := schema.TableByName(cDBName, cTableName)
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 132)
				writeError(w, err)
				return
			}
			trace_util_0.Count(_http_handler_00000, 131)
			writeData(w, data.Meta())
			return
		}
		// all table schemas in a specified database
		trace_util_0.Count(_http_handler_00000, 128)
		if schema.SchemaExists(cDBName) {
			trace_util_0.Count(_http_handler_00000, 133)
			tbs := schema.SchemaTables(cDBName)
			tbsInfo := make([]*model.TableInfo, len(tbs))
			for i := range tbsInfo {
				trace_util_0.Count(_http_handler_00000, 135)
				tbsInfo[i] = tbs[i].Meta()
			}
			trace_util_0.Count(_http_handler_00000, 134)
			writeData(w, tbsInfo)
			return
		}
		trace_util_0.Count(_http_handler_00000, 129)
		writeError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName))
		return
	}

	trace_util_0.Count(_http_handler_00000, 124)
	if tableID := req.FormValue(qTableID); len(tableID) > 0 {
		trace_util_0.Count(_http_handler_00000, 136)
		// table schema of a specified tableID
		tid, err := strconv.Atoi(tableID)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 140)
			writeError(w, err)
			return
		}
		trace_util_0.Count(_http_handler_00000, 137)
		if tid < 0 {
			trace_util_0.Count(_http_handler_00000, 141)
			writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		trace_util_0.Count(_http_handler_00000, 138)
		if data, ok := schema.TableByID(int64(tid)); ok {
			trace_util_0.Count(_http_handler_00000, 142)
			writeData(w, data.Meta())
			return
		}
		trace_util_0.Count(_http_handler_00000, 139)
		writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
		return
	}

	// all databases' schemas
	trace_util_0.Count(_http_handler_00000, 125)
	writeData(w, schema.AllSchemas())
}

// ServeHTTP handles table related requests, such as table's region information, disk usage.
func (h tableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 143)
	// parse params
	params := mux.Vars(req)
	dbName := params[pDBName]
	tableName := params[pTableName]
	schema, err := h.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 146)
		writeError(w, err)
		return
	}
	// get table's schema.
	trace_util_0.Count(_http_handler_00000, 144)
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 147)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_http_handler_00000, 145)
	switch h.op {
	case opTableRegions:
		trace_util_0.Count(_http_handler_00000, 148)
		h.handleRegionRequest(schema, tableVal, w, req)
	case opTableDiskUsage:
		trace_util_0.Count(_http_handler_00000, 149)
		h.handleDiskUsageRequest(schema, tableVal, w, req)
	case opTableScatter:
		trace_util_0.Count(_http_handler_00000, 150)
		h.handleScatterTableRequest(schema, tableVal, w, req)
	case opStopTableScatter:
		trace_util_0.Count(_http_handler_00000, 151)
		h.handleStopScatterTableRequest(schema, tableVal, w, req)
	default:
		trace_util_0.Count(_http_handler_00000, 152)
		writeError(w, errors.New("method not found"))
	}
}

// ServeHTTP handles request of ddl jobs history.
func (h ddlHistoryJobHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 153)
	if limitID := req.FormValue(qLimit); len(limitID) > 0 {
		trace_util_0.Count(_http_handler_00000, 156)
		lid, err := strconv.Atoi(limitID)

		if err != nil {
			trace_util_0.Count(_http_handler_00000, 161)
			writeError(w, err)
			return
		}

		trace_util_0.Count(_http_handler_00000, 157)
		if lid < 1 {
			trace_util_0.Count(_http_handler_00000, 162)
			writeError(w, errors.New("ddl history limit must be greater than 1"))
			return
		}

		trace_util_0.Count(_http_handler_00000, 158)
		jobs, err := h.getAllHistoryDDL()
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 163)
			writeError(w, errors.New("ddl history not found"))
			return
		}

		trace_util_0.Count(_http_handler_00000, 159)
		jobsLen := len(jobs)
		if jobsLen > lid {
			trace_util_0.Count(_http_handler_00000, 164)
			start := jobsLen - lid
			jobs = jobs[start:]
		}

		trace_util_0.Count(_http_handler_00000, 160)
		writeData(w, jobs)
		return
	}
	trace_util_0.Count(_http_handler_00000, 154)
	jobs, err := h.getAllHistoryDDL()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 165)
		writeError(w, errors.New("ddl history not found"))
		return
	}
	trace_util_0.Count(_http_handler_00000, 155)
	writeData(w, jobs)
}

func (h ddlHistoryJobHandler) getAllHistoryDDL() ([]*model.Job, error) {
	trace_util_0.Count(_http_handler_00000, 166)
	s, err := session.CreateSession(h.Store.(kv.Storage))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 171)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_http_handler_00000, 167)
	if s != nil {
		trace_util_0.Count(_http_handler_00000, 172)
		defer s.Close()
	}

	trace_util_0.Count(_http_handler_00000, 168)
	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, err := store.Begin()

	if err != nil {
		trace_util_0.Count(_http_handler_00000, 173)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 169)
	txnMeta := meta.NewMeta(txn)

	jobs, err := txnMeta.GetAllHistoryDDLJobs()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 174)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 170)
	return jobs, nil
}

func (h ddlResignOwnerHandler) resignDDLOwner() error {
	trace_util_0.Count(_http_handler_00000, 175)
	dom, err := session.GetDomain(h.store)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 178)
		return errors.Trace(err)
	}

	trace_util_0.Count(_http_handler_00000, 176)
	ownerMgr := dom.DDL().OwnerManager()
	err = ownerMgr.ResignOwner(context.Background())
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 179)
		return errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 177)
	return nil
}

// ServeHTTP handles request of resigning ddl owner.
func (h ddlResignOwnerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 180)
	if req.Method != http.MethodPost {
		trace_util_0.Count(_http_handler_00000, 183)
		writeError(w, errors.Errorf("This api only support POST method."))
		return
	}

	trace_util_0.Count(_http_handler_00000, 181)
	err := h.resignDDLOwner()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 184)
		log.Error(err)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_http_handler_00000, 182)
	writeData(w, "success!")
}

func (h tableHandler) getPDAddr() ([]string, error) {
	trace_util_0.Count(_http_handler_00000, 185)
	var pdAddrs []string
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		trace_util_0.Count(_http_handler_00000, 188)
		return nil, errors.New("not implemented")
	}
	trace_util_0.Count(_http_handler_00000, 186)
	pdAddrs = etcd.EtcdAddrs()
	if len(pdAddrs) < 0 {
		trace_util_0.Count(_http_handler_00000, 189)
		return nil, errors.New("pd unavailable")
	}
	trace_util_0.Count(_http_handler_00000, 187)
	return pdAddrs, nil
}

func (h tableHandler) addScatterSchedule(startKey, endKey []byte, name string) error {
	trace_util_0.Count(_http_handler_00000, 190)
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 195)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 191)
	input := map[string]string{
		"name":       "scatter-range",
		"start_key":  string(startKey),
		"end_key":    string(endKey),
		"range_name": name,
	}
	v, err := json.Marshal(input)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 196)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 192)
	scheduleURL := fmt.Sprintf("http://%s/pd/api/v1/schedulers", pdAddrs[0])
	resp, err := http.Post(scheduleURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 197)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 193)
	if err := resp.Body.Close(); err != nil {
		trace_util_0.Count(_http_handler_00000, 198)
		log.Error(err)
	}
	trace_util_0.Count(_http_handler_00000, 194)
	return nil
}

func (h tableHandler) deleteScatterSchedule(name string) error {
	trace_util_0.Count(_http_handler_00000, 199)
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 204)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 200)
	scheduleURL := fmt.Sprintf("http://%s/pd/api/v1/schedulers/scatter-range-%s", pdAddrs[0], name)
	req, err := http.NewRequest(http.MethodDelete, scheduleURL, nil)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 205)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 201)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 206)
		return err
	}
	trace_util_0.Count(_http_handler_00000, 202)
	if err := resp.Body.Close(); err != nil {
		trace_util_0.Count(_http_handler_00000, 207)
		log.Error(err)
	}
	trace_util_0.Count(_http_handler_00000, 203)
	return nil
}

func (h tableHandler) handleScatterTableRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 208)
	// for record
	tableID := tbl.Meta().ID
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	tableName := tbl.Meta().Name.String()
	err := h.addScatterSchedule(startKey, endKey, tableName)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 211)
		writeError(w, errors.Annotate(err, "scatter record error"))
		return
	}
	// for indices
	trace_util_0.Count(_http_handler_00000, 209)
	for _, index := range tbl.Indices() {
		trace_util_0.Count(_http_handler_00000, 212)
		indexID := index.Meta().ID
		indexName := index.Meta().Name.String()
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		startKey = codec.EncodeBytes([]byte{}, startKey)
		endKey = codec.EncodeBytes([]byte{}, endKey)
		name := tableName + "-" + indexName
		err := h.addScatterSchedule(startKey, endKey, name)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 213)
			writeError(w, errors.Annotatef(err, "scatter index(%s) error", name))
			return
		}
	}
	trace_util_0.Count(_http_handler_00000, 210)
	writeData(w, "success!")
}

func (h tableHandler) handleStopScatterTableRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 214)
	// for record
	tableName := tbl.Meta().Name.String()
	err := h.deleteScatterSchedule(tableName)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 217)
		writeError(w, errors.Annotate(err, "stop scatter record error"))
		return
	}
	// for indices
	trace_util_0.Count(_http_handler_00000, 215)
	for _, index := range tbl.Indices() {
		trace_util_0.Count(_http_handler_00000, 218)
		indexName := index.Meta().Name.String()
		name := tableName + "-" + indexName
		err := h.deleteScatterSchedule(name)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 219)
			writeError(w, errors.Annotatef(err, "delete scatter index(%s) error", name))
			return
		}
	}
	trace_util_0.Count(_http_handler_00000, 216)
	writeData(w, "success!")
}

func (h tableHandler) handleRegionRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 220)
	tableID := tbl.Meta().ID
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 224)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 221)
	recordRegions, err := h.getRegionsMeta(recordRegionIDs)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 225)
		writeError(w, err)
		return
	}

	// for indices
	trace_util_0.Count(_http_handler_00000, 222)
	indices := make([]IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		trace_util_0.Count(_http_handler_00000, 226)
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		rIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 228)
			writeError(w, err)
			return
		}
		trace_util_0.Count(_http_handler_00000, 227)
		indices[i].Regions, err = h.getRegionsMeta(rIDs)
		if err != nil {
			trace_util_0.Count(_http_handler_00000, 229)
			writeError(w, err)
			return
		}
	}

	trace_util_0.Count(_http_handler_00000, 223)
	tableRegions := &TableRegions{
		TableName:     tbl.Meta().Name.O,
		TableID:       tableID,
		Indices:       indices,
		RecordRegions: recordRegions,
	}

	writeData(w, tableRegions)
}

// pdRegionStats is the json response from PD.
type pdRegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
}

func (h tableHandler) handleDiskUsageRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 230)
	tableID := tbl.Meta().ID
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 235)
		writeError(w, err)
		return
	}

	// Include table and index data, because their range located in tableID_i tableID_r
	trace_util_0.Count(_http_handler_00000, 231)
	startKey := tablecodec.EncodeTablePrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	statURL := fmt.Sprintf("http://%s/pd/api/v1/stats/region?start_key=%s&end_key=%s",
		pdAddrs[0],
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))

	resp, err := http.Get(statURL)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 236)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 232)
	defer func() {
		trace_util_0.Count(_http_handler_00000, 237)
		if err := resp.Body.Close(); err != nil {
			trace_util_0.Count(_http_handler_00000, 238)
			log.Error(err)
		}
	}()

	trace_util_0.Count(_http_handler_00000, 233)
	var stats pdRegionStats
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&stats); err != nil {
		trace_util_0.Count(_http_handler_00000, 239)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 234)
	writeData(w, stats.StorageSize)
}

type hotRegion struct {
	helper.TblIndex
	helper.RegionMetric
}
type hotRegions []hotRegion

func (rs hotRegions) Len() int {
	trace_util_0.Count(_http_handler_00000, 240)
	return len(rs)
}

func (rs hotRegions) Less(i, j int) bool {
	trace_util_0.Count(_http_handler_00000, 241)
	return rs[i].MaxHotDegree > rs[j].MaxHotDegree || (rs[i].MaxHotDegree == rs[j].MaxHotDegree && rs[i].FlowBytes > rs[j].FlowBytes)
}

func (rs hotRegions) Swap(i, j int) {
	trace_util_0.Count(_http_handler_00000, 242)
	rs[i], rs[j] = rs[j], rs[i]
}

// ServeHTTP handles request of get region by ID.
func (h regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 243)
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[pRegionID]; !ok {
		trace_util_0.Count(_http_handler_00000, 250)
		router := mux.CurrentRoute(req).GetName()
		if router == "RegionsMeta" {
			trace_util_0.Count(_http_handler_00000, 253)
			startKey := []byte{'m'}
			endKey := []byte{'n'}

			recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 256)
				writeError(w, err)
				return
			}

			trace_util_0.Count(_http_handler_00000, 254)
			recordRegions, err := h.getRegionsMeta(recordRegionIDs)
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 257)
				writeError(w, err)
				return
			}
			trace_util_0.Count(_http_handler_00000, 255)
			writeData(w, recordRegions)
			return
		}
		trace_util_0.Count(_http_handler_00000, 251)
		if router == "RegionHot" {
			trace_util_0.Count(_http_handler_00000, 258)
			schema, err := h.schema()
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 263)
				writeError(w, err)
				return
			}
			trace_util_0.Count(_http_handler_00000, 259)
			hotRead, err := h.ScrapeHotInfo(pdapi.HotRead, schema.AllSchemas())
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 264)
				writeError(w, err)
				return
			}
			trace_util_0.Count(_http_handler_00000, 260)
			hotWrite, err := h.ScrapeHotInfo(pdapi.HotWrite, schema.AllSchemas())
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 265)
				writeError(w, err)
				return
			}
			trace_util_0.Count(_http_handler_00000, 261)
			asSortedEntry := func(metric map[helper.TblIndex]helper.RegionMetric) hotRegions {
				trace_util_0.Count(_http_handler_00000, 266)
				hs := make(hotRegions, 0, len(metric))
				for key, value := range metric {
					trace_util_0.Count(_http_handler_00000, 268)
					hs = append(hs, hotRegion{key, value})
				}
				trace_util_0.Count(_http_handler_00000, 267)
				sort.Sort(hs)
				return hs
			}
			trace_util_0.Count(_http_handler_00000, 262)
			writeData(w, map[string]interface{}{
				"write": asSortedEntry(hotWrite),
				"read":  asSortedEntry(hotRead),
			})
			return
		}
		trace_util_0.Count(_http_handler_00000, 252)
		return
	}

	trace_util_0.Count(_http_handler_00000, 244)
	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 269)
		writeError(w, err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 245)
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.RegionCache.LocateRegionByID(tikv.NewBackoffer(context.Background(), 500), regionID)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 270)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_http_handler_00000, 246)
	frameRange, err := helper.NewRegionFrameRange(region)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 271)
		writeError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	trace_util_0.Count(_http_handler_00000, 247)
	regionDetail := &RegionDetail{
		RegionID: regionID,
		StartKey: region.StartKey,
		EndKey:   region.EndKey,
	}
	schema, err := h.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 272)
		writeError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	trace_util_0.Count(_http_handler_00000, 248)
	for _, db := range schema.AllSchemas() {
		trace_util_0.Count(_http_handler_00000, 273)
		for _, tableVal := range db.Tables {
			trace_util_0.Count(_http_handler_00000, 274)
			regionDetail.addTableInRange(db.Name.String(), tableVal, frameRange)
		}
	}
	trace_util_0.Count(_http_handler_00000, 249)
	writeData(w, regionDetail)
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	trace_util_0.Count(_http_handler_00000, 275)
	frame = &FrameItem{}
	frame.TableID, frame.IndexID, frame.IsRecord, err = tablecodec.DecodeKeyHead(key)
	if err == nil {
		trace_util_0.Count(_http_handler_00000, 279)
		if frame.IsRecord {
			trace_util_0.Count(_http_handler_00000, 281)
			_, frame.RecordID, err = tablecodec.DecodeRecordKey(key)
		} else {
			trace_util_0.Count(_http_handler_00000, 282)
			{
				_, _, frame.IndexValues, err = tablecodec.DecodeIndexKey(key)
			}
		}
		trace_util_0.Count(_http_handler_00000, 280)
		log.Warnf("decode region key %q fail: %v", key, err)
		// Ignore decode errors.
		err = nil
		return
	}
	trace_util_0.Count(_http_handler_00000, 276)
	if bytes.HasPrefix(key, tablecodec.TablePrefix()) {
		trace_util_0.Count(_http_handler_00000, 283)
		// If SplitTable is enabled, the key may be `t{id}`.
		if len(key) == tablecodec.TableSplitKeyLen {
			trace_util_0.Count(_http_handler_00000, 285)
			frame.TableID = tablecodec.DecodeTableID(key)
			return frame, nil
		}
		trace_util_0.Count(_http_handler_00000, 284)
		return nil, errors.Trace(err)
	}

	// key start with tablePrefix must be either record key or index key
	// That's means table's record key and index key are always together
	// in the continuous interval. And for key with prefix smaller than
	// tablePrefix, is smaller than all tables. While for key with prefix
	// bigger than tablePrefix, means is bigger than all tables.
	trace_util_0.Count(_http_handler_00000, 277)
	err = nil
	if bytes.Compare(key, tablecodec.TablePrefix()) < 0 {
		trace_util_0.Count(_http_handler_00000, 286)
		frame.TableID = math.MinInt64
		frame.IndexID = math.MinInt64
		frame.IsRecord = false
		return
	}
	// bigger than tablePrefix, means is bigger than all tables.
	trace_util_0.Count(_http_handler_00000, 278)
	frame.TableID = math.MaxInt64
	frame.TableID = math.MaxInt64
	frame.IsRecord = true
	return
}

// parseQuery is used to parse query string in URL with shouldUnescape, due to golang http package can not distinguish
// query like "?a=" and "?a". We rewrite it to separate these two queries. e.g.
// "?a=" which means that a is an empty string "";
// "?a"  which means that a is null.
// If shouldUnescape is true, we use QueryUnescape to handle keys and values that will be put in m.
// If shouldUnescape is false, we don't use QueryUnescap to handle.
func parseQuery(query string, m url.Values, shouldUnescape bool) error {
	trace_util_0.Count(_http_handler_00000, 287)
	var err error
	for query != "" {
		trace_util_0.Count(_http_handler_00000, 289)
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			trace_util_0.Count(_http_handler_00000, 292)
			key, query = key[:i], key[i+1:]
		} else {
			trace_util_0.Count(_http_handler_00000, 293)
			{
				query = ""
			}
		}
		trace_util_0.Count(_http_handler_00000, 290)
		if key == "" {
			trace_util_0.Count(_http_handler_00000, 294)
			continue
		}
		trace_util_0.Count(_http_handler_00000, 291)
		if i := strings.Index(key, "="); i >= 0 {
			trace_util_0.Count(_http_handler_00000, 295)
			value := ""
			key, value = key[:i], key[i+1:]
			if shouldUnescape {
				trace_util_0.Count(_http_handler_00000, 297)
				key, err = url.QueryUnescape(key)
				if err != nil {
					trace_util_0.Count(_http_handler_00000, 299)
					return errors.Trace(err)
				}
				trace_util_0.Count(_http_handler_00000, 298)
				value, err = url.QueryUnescape(value)
				if err != nil {
					trace_util_0.Count(_http_handler_00000, 300)
					return errors.Trace(err)
				}
			}
			trace_util_0.Count(_http_handler_00000, 296)
			m[key] = append(m[key], value)
		} else {
			trace_util_0.Count(_http_handler_00000, 301)
			{
				if shouldUnescape {
					trace_util_0.Count(_http_handler_00000, 303)
					key, err = url.QueryUnescape(key)
					if err != nil {
						trace_util_0.Count(_http_handler_00000, 304)
						return errors.Trace(err)
					}
				}
				trace_util_0.Count(_http_handler_00000, 302)
				if _, ok := m[key]; !ok {
					trace_util_0.Count(_http_handler_00000, 305)
					m[key] = nil
				}
			}
		}
	}
	trace_util_0.Count(_http_handler_00000, 288)
	return errors.Trace(err)
}

// ServeHTTP handles request of list a table's regions.
func (h mvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 306)
	var data interface{}
	params := mux.Vars(req)
	var err error
	switch h.op {
	case opMvccGetByHex:
		trace_util_0.Count(_http_handler_00000, 308)
		data, err = h.handleMvccGetByHex(params)
	case opMvccGetByIdx:
		trace_util_0.Count(_http_handler_00000, 309)
		if req.URL == nil {
			trace_util_0.Count(_http_handler_00000, 314)
			err = errors.BadRequestf("Invalid URL")
			break
		}
		trace_util_0.Count(_http_handler_00000, 310)
		values := make(url.Values)
		err = parseQuery(req.URL.RawQuery, values, true)
		if err == nil {
			trace_util_0.Count(_http_handler_00000, 315)
			data, err = h.handleMvccGetByIdx(params, values)
		}
	case opMvccGetByKey:
		trace_util_0.Count(_http_handler_00000, 311)
		decode := len(req.URL.Query().Get("decode")) > 0
		data, err = h.handleMvccGetByKey(params, decode)
	case opMvccGetByTxn:
		trace_util_0.Count(_http_handler_00000, 312)
		data, err = h.handleMvccGetByTxn(params)
	default:
		trace_util_0.Count(_http_handler_00000, 313)
		err = errors.NotSupportedf("Operation not supported.")
	}
	trace_util_0.Count(_http_handler_00000, 307)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 316)
		writeError(w, err)
	} else {
		trace_util_0.Count(_http_handler_00000, 317)
		{
			writeData(w, data)
		}
	}
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h mvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (interface{}, error) {
	trace_util_0.Count(_http_handler_00000, 318)
	dbName := params[pDBName]
	tableName := params[pTableName]
	handleStr := params[pHandle]
	schema, err := h.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 323)
		return nil, errors.Trace(err)
	}
	// get table's schema.
	trace_util_0.Count(_http_handler_00000, 319)
	t, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 324)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 320)
	var idxCols []*model.ColumnInfo
	var idx table.Index
	for _, v := range t.Indices() {
		trace_util_0.Count(_http_handler_00000, 325)
		if strings.EqualFold(v.Meta().Name.String(), params[pIndexName]) {
			trace_util_0.Count(_http_handler_00000, 326)
			for _, c := range v.Meta().Columns {
				trace_util_0.Count(_http_handler_00000, 328)
				idxCols = append(idxCols, t.Meta().Columns[c.Offset])
			}
			trace_util_0.Count(_http_handler_00000, 327)
			idx = v
			break
		}
	}
	trace_util_0.Count(_http_handler_00000, 321)
	if idx == nil {
		trace_util_0.Count(_http_handler_00000, 329)
		return nil, errors.NotFoundf("Index %s not found!", params[pIndexName])
	}
	trace_util_0.Count(_http_handler_00000, 322)
	return h.getMvccByIdxValue(idx, values, idxCols, handleStr)
}

func (h mvccTxnHandler) handleMvccGetByKey(params map[string]string, decodeData bool) (interface{}, error) {
	trace_util_0.Count(_http_handler_00000, 330)
	handle, err := strconv.ParseInt(params[pHandle], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 337)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_http_handler_00000, 331)
	tb, err := h.getTable(params[pDBName], params[pTableName])
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 338)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 332)
	resp, err := h.getMvccByHandle(tb.ID, handle)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 339)
		return nil, err
	}
	trace_util_0.Count(_http_handler_00000, 333)
	if !decodeData {
		trace_util_0.Count(_http_handler_00000, 340)
		return resp, nil
	}
	trace_util_0.Count(_http_handler_00000, 334)
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range tb.Columns {
		trace_util_0.Count(_http_handler_00000, 341)
		colMap[col.ID] = &col.FieldType
	}

	trace_util_0.Count(_http_handler_00000, 335)
	respValue := resp.Value
	var result interface{} = resp
	if respValue.Info != nil {
		trace_util_0.Count(_http_handler_00000, 342)
		datas := make(map[string][]map[string]string)
		for _, w := range respValue.Info.Writes {
			trace_util_0.Count(_http_handler_00000, 345)
			if len(w.ShortValue) > 0 {
				trace_util_0.Count(_http_handler_00000, 346)
				datas[strconv.FormatUint(w.StartTs, 10)], err = h.decodeMvccData(w.ShortValue, colMap, tb)
			}
		}

		trace_util_0.Count(_http_handler_00000, 343)
		for _, v := range respValue.Info.Values {
			trace_util_0.Count(_http_handler_00000, 347)
			if len(v.Value) > 0 {
				trace_util_0.Count(_http_handler_00000, 348)
				datas[strconv.FormatUint(v.StartTs, 10)], err = h.decodeMvccData(v.Value, colMap, tb)
			}
		}

		trace_util_0.Count(_http_handler_00000, 344)
		if len(datas) > 0 {
			trace_util_0.Count(_http_handler_00000, 349)
			re := map[string]interface{}{
				"key":  resp.Key,
				"info": respValue.Info,
				"data": datas,
			}
			if err != nil {
				trace_util_0.Count(_http_handler_00000, 351)
				re["decode_error"] = err.Error()
			}
			trace_util_0.Count(_http_handler_00000, 350)
			result = re
		}
	}

	trace_util_0.Count(_http_handler_00000, 336)
	return result, nil
}

func (h mvccTxnHandler) decodeMvccData(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) ([]map[string]string, error) {
	trace_util_0.Count(_http_handler_00000, 352)
	rs, err := tablecodec.DecodeRow(bs, colMap, time.UTC)
	var record []map[string]string
	for _, col := range tb.Columns {
		trace_util_0.Count(_http_handler_00000, 354)
		if c, ok := rs[col.ID]; ok {
			trace_util_0.Count(_http_handler_00000, 355)
			data := "nil"
			if !c.IsNull() {
				trace_util_0.Count(_http_handler_00000, 357)
				data, err = c.ToString()
			}
			trace_util_0.Count(_http_handler_00000, 356)
			record = append(record, map[string]string{col.Name.O: data})
		}
	}
	trace_util_0.Count(_http_handler_00000, 353)
	return record, err
}

func (h *mvccTxnHandler) handleMvccGetByTxn(params map[string]string) (interface{}, error) {
	trace_util_0.Count(_http_handler_00000, 358)
	startTS, err := strconv.ParseInt(params[pStartTS], 0, 64)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 361)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 359)
	tableID, err := h.getTableID(params[pDBName], params[pTableName])
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 362)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_http_handler_00000, 360)
	startKey := tablecodec.EncodeTablePrefix(tableID)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	return h.getMvccByStartTs(uint64(startTS), startKey, endKey)
}

// serverInfo is used to report the servers info when do http request.
type serverInfo struct {
	IsOwner bool `json:"is_owner"`
	*domain.ServerInfo
}

// ServeHTTP handles request of ddl server info.
func (h serverInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 363)
	do, err := session.GetDomain(h.Store.(kv.Storage))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 365)
		writeError(w, errors.New("create session error"))
		log.Error(err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 364)
	info := serverInfo{}
	info.ServerInfo = do.InfoSyncer().GetServerInfo()
	info.IsOwner = do.DDL().OwnerManager().IsOwner()
	writeData(w, info)
}

// clusterServerInfo is used to report cluster servers info when do http request.
type clusterServerInfo struct {
	ServersNum                   int                           `json:"servers_num,omitempty"`
	OwnerID                      string                        `json:"owner_id"`
	IsAllServerVersionConsistent bool                          `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []domain.ServerVersionInfo    `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*domain.ServerInfo `json:"all_servers_info,omitempty"`
}

// ServeHTTP handles request of all ddl servers info.
func (h allServerInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 366)
	do, err := session.GetDomain(h.Store.(kv.Storage))
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 372)
		writeError(w, errors.New("create session error"))
		log.Error(err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 367)
	ctx := context.Background()
	allServersInfo, err := do.InfoSyncer().GetAllServerInfo(ctx)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 373)
		writeError(w, errors.New("ddl server information not found"))
		log.Error(err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 368)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ownerID, err := do.DDL().OwnerManager().GetOwnerID(ctx)
	cancel()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 374)
		writeError(w, errors.New("ddl server information not found"))
		log.Error(err)
		return
	}
	trace_util_0.Count(_http_handler_00000, 369)
	allVersionsMap := map[domain.ServerVersionInfo]struct{}{}
	allVersions := make([]domain.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		trace_util_0.Count(_http_handler_00000, 375)
		if _, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			trace_util_0.Count(_http_handler_00000, 377)
			continue
		}
		trace_util_0.Count(_http_handler_00000, 376)
		allVersionsMap[v.ServerVersionInfo] = struct{}{}
		allVersions = append(allVersions, v.ServerVersionInfo)
	}
	trace_util_0.Count(_http_handler_00000, 370)
	clusterInfo := clusterServerInfo{
		ServersNum: len(allServersInfo),
		OwnerID:    ownerID,
		// len(allVersions) = 1 indicates there has only 1 tidb version in cluster, so all server versions are consistent.
		IsAllServerVersionConsistent: len(allVersions) == 1,
		AllServersInfo:               allServersInfo,
	}
	// if IsAllServerVersionConsistent is false, return the all tidb servers version.
	if !clusterInfo.IsAllServerVersionConsistent {
		trace_util_0.Count(_http_handler_00000, 378)
		clusterInfo.AllServersDiffVersions = allVersions
	}
	trace_util_0.Count(_http_handler_00000, 371)
	writeData(w, clusterInfo)
}

// dbTableInfo is used to report the database, table information and the current schema version.
type dbTableInfo struct {
	DBInfo        *model.DBInfo    `json:"db_info"`
	TableInfo     *model.TableInfo `json:"table_info"`
	SchemaVersion int64            `json:"schema_version"`
}

//ServeHTTP handles request of database information and table information by tableID.
func (h dbTableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	trace_util_0.Count(_http_handler_00000, 379)
	params := mux.Vars(req)
	tableID := params[pTableID]
	physicalID, err := strconv.Atoi(tableID)
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 384)
		writeError(w, errors.Errorf("Wrong tableID: %v", tableID))
		return
	}

	trace_util_0.Count(_http_handler_00000, 380)
	schema, err := h.schema()
	if err != nil {
		trace_util_0.Count(_http_handler_00000, 385)
		writeError(w, err)
		return
	}

	trace_util_0.Count(_http_handler_00000, 381)
	dbTblInfo := dbTableInfo{
		SchemaVersion: schema.SchemaMetaVersion(),
	}
	tbl, ok := schema.TableByID(int64(physicalID))
	if ok {
		trace_util_0.Count(_http_handler_00000, 386)
		dbTblInfo.TableInfo = tbl.Meta()
		dbInfo, ok := schema.SchemaByTable(dbTblInfo.TableInfo)
		if !ok {
			trace_util_0.Count(_http_handler_00000, 388)
			log.Warnf("can not find the database of table id: %v, table name: %v", dbTblInfo.TableInfo.ID, dbTblInfo.TableInfo.Name)
			writeData(w, dbTblInfo)
			return
		}
		trace_util_0.Count(_http_handler_00000, 387)
		dbTblInfo.DBInfo = dbInfo
		writeData(w, dbTblInfo)
		return
	}
	// The physicalID maybe a partition ID of the partition-table.
	trace_util_0.Count(_http_handler_00000, 382)
	dbTblInfo.TableInfo, dbTblInfo.DBInfo = findTableByPartitionID(schema, int64(physicalID))
	if dbTblInfo.TableInfo == nil {
		trace_util_0.Count(_http_handler_00000, 389)
		writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
		return
	}
	trace_util_0.Count(_http_handler_00000, 383)
	writeData(w, dbTblInfo)
}

// findTableByPartitionID finds the partition-table info by the partitionID.
// This function will traverse all the tables to find the partitionID partition in which partition-table.
func findTableByPartitionID(schema infoschema.InfoSchema, partitionID int64) (*model.TableInfo, *model.DBInfo) {
	trace_util_0.Count(_http_handler_00000, 390)
	allDBs := schema.AllSchemas()
	for _, db := range allDBs {
		trace_util_0.Count(_http_handler_00000, 392)
		allTables := schema.SchemaTables(db.Name)
		for _, tbl := range allTables {
			trace_util_0.Count(_http_handler_00000, 393)
			if tbl.Meta().ID > partitionID || tbl.Meta().GetPartitionInfo() == nil {
				trace_util_0.Count(_http_handler_00000, 395)
				continue
			}
			trace_util_0.Count(_http_handler_00000, 394)
			info := tbl.Meta().GetPartitionInfo()
			tb := tbl.(table.PartitionedTable)
			for _, def := range info.Definitions {
				trace_util_0.Count(_http_handler_00000, 396)
				pid := def.ID
				partition := tb.GetPartition(pid)
				if partition.GetPhysicalID() == partitionID {
					trace_util_0.Count(_http_handler_00000, 397)
					return tbl.Meta(), db
				}
			}
		}
	}
	trace_util_0.Count(_http_handler_00000, 391)
	return nil, nil
}

var _http_handler_00000 = "server/http_handler.go"
