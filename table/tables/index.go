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

package tables

import (
	"bytes"
	"encoding/binary"
	"io"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// EncodeHandle encodes handle in data.
func EncodeHandle(h int64) []byte {
	trace_util_0.Count(_index_00000, 0)
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, h)
	if err != nil {
		trace_util_0.Count(_index_00000, 2)
		panic(err)
	}
	trace_util_0.Count(_index_00000, 1)
	return buf.Bytes()
}

// DecodeHandle decodes handle in data.
func DecodeHandle(data []byte) (int64, error) {
	trace_util_0.Count(_index_00000, 3)
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, err
}

// indexIter is for KV store index iterator.
type indexIter struct {
	it     kv.Iterator
	idx    *index
	prefix kv.Key
}

// Close does the clean up works when KV store index iterator is closed.
func (c *indexIter) Close() {
	trace_util_0.Count(_index_00000, 4)
	if c.it != nil {
		trace_util_0.Count(_index_00000, 5)
		c.it.Close()
		c.it = nil
	}
}

// Next returns current key and moves iterator to the next step.
func (c *indexIter) Next() (val []types.Datum, h int64, err error) {
	trace_util_0.Count(_index_00000, 6)
	if !c.it.Valid() {
		trace_util_0.Count(_index_00000, 12)
		return nil, 0, errors.Trace(io.EOF)
	}
	trace_util_0.Count(_index_00000, 7)
	if !c.it.Key().HasPrefix(c.prefix) {
		trace_util_0.Count(_index_00000, 13)
		return nil, 0, errors.Trace(io.EOF)
	}
	// get indexedValues
	trace_util_0.Count(_index_00000, 8)
	buf := c.it.Key()[len(c.prefix):]
	vv, err := codec.Decode(buf, len(c.idx.idxInfo.Columns))
	if err != nil {
		trace_util_0.Count(_index_00000, 14)
		return nil, 0, err
	}
	trace_util_0.Count(_index_00000, 9)
	if len(vv) > len(c.idx.idxInfo.Columns) {
		trace_util_0.Count(_index_00000, 15)
		h = vv[len(vv)-1].GetInt64()
		val = vv[0 : len(vv)-1]
	} else {
		trace_util_0.Count(_index_00000, 16)
		{
			// If the index is unique and the value isn't nil, the handle is in value.
			h, err = DecodeHandle(c.it.Value())
			if err != nil {
				trace_util_0.Count(_index_00000, 18)
				return nil, 0, err
			}
			trace_util_0.Count(_index_00000, 17)
			val = vv
		}
	}
	// update new iter to next
	trace_util_0.Count(_index_00000, 10)
	err = c.it.Next()
	if err != nil {
		trace_util_0.Count(_index_00000, 19)
		return nil, 0, err
	}
	trace_util_0.Count(_index_00000, 11)
	return
}

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo *model.IndexInfo
	tblInfo *model.TableInfo
	prefix  kv.Key
}

// NewIndex builds a new Index object.
func NewIndex(physicalID int64, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) table.Index {
	trace_util_0.Count(_index_00000, 20)
	index := &index{
		idxInfo: indexInfo,
		tblInfo: tblInfo,
		// The prefix can't encode from tblInfo.ID, because table partition may change the id to partition id.
		prefix: tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID),
	}
	return index
}

// Meta returns index info.
func (c *index) Meta() *model.IndexInfo {
	trace_util_0.Count(_index_00000, 21)
	return c.idxInfo
}

func (c *index) getIndexKeyBuf(buf []byte, defaultCap int) []byte {
	trace_util_0.Count(_index_00000, 22)
	if buf != nil {
		trace_util_0.Count(_index_00000, 24)
		return buf[:0]
	}

	trace_util_0.Count(_index_00000, 23)
	return make([]byte, 0, defaultCap)
}

// TruncateIndexValuesIfNeeded truncates the index values created using only the leading part of column values.
func TruncateIndexValuesIfNeeded(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, indexedValues []types.Datum) []types.Datum {
	trace_util_0.Count(_index_00000, 25)
	for i := 0; i < len(indexedValues); i++ {
		trace_util_0.Count(_index_00000, 27)
		v := &indexedValues[i]
		if v.Kind() == types.KindString || v.Kind() == types.KindBytes {
			trace_util_0.Count(_index_00000, 28)
			ic := idxInfo.Columns[i]
			colCharset := tblInfo.Columns[ic.Offset].Charset
			colValue := v.GetBytes()
			isUTF8Charset := colCharset == charset.CharsetUTF8 || colCharset == charset.CharsetUTF8MB4
			origKind := v.Kind()
			if isUTF8Charset {
				trace_util_0.Count(_index_00000, 29)
				if ic.Length != types.UnspecifiedLength && utf8.RuneCount(colValue) > ic.Length {
					trace_util_0.Count(_index_00000, 30)
					rs := bytes.Runes(colValue)
					truncateStr := string(rs[:ic.Length])
					// truncate value and limit its length
					v.SetString(truncateStr)
					if origKind == types.KindBytes {
						trace_util_0.Count(_index_00000, 31)
						v.SetBytes(v.GetBytes())
					}
				}
			} else {
				trace_util_0.Count(_index_00000, 32)
				if ic.Length != types.UnspecifiedLength && len(colValue) > ic.Length {
					trace_util_0.Count(_index_00000, 33)
					// truncate value and limit its length
					v.SetBytes(colValue[:ic.Length])
					if origKind == types.KindString {
						trace_util_0.Count(_index_00000, 34)
						v.SetString(v.GetString())
					}
				}
			}
		}
	}

	trace_util_0.Count(_index_00000, 26)
	return indexedValues
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum, h int64, buf []byte) (key []byte, distinct bool, err error) {
	trace_util_0.Count(_index_00000, 35)
	if c.idxInfo.Unique {
		trace_util_0.Count(_index_00000, 39)
		// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			trace_util_0.Count(_index_00000, 40)
			if cv.IsNull() {
				trace_util_0.Count(_index_00000, 41)
				distinct = false
				break
			}
		}
	}

	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	trace_util_0.Count(_index_00000, 36)
	indexedValues = TruncateIndexValuesIfNeeded(c.tblInfo, c.idxInfo, indexedValues)
	key = c.getIndexKeyBuf(buf, len(c.prefix)+len(indexedValues)*9+9)
	key = append(key, []byte(c.prefix)...)
	key, err = codec.EncodeKey(sc, key, indexedValues...)
	if !distinct && err == nil {
		trace_util_0.Count(_index_00000, 42)
		key, err = codec.EncodeKey(sc, key, types.NewDatum(h))
	}
	trace_util_0.Count(_index_00000, 37)
	if err != nil {
		trace_util_0.Count(_index_00000, 43)
		return nil, false, err
	}
	trace_util_0.Count(_index_00000, 38)
	return
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
func (c *index) Create(ctx sessionctx.Context, rm kv.RetrieverMutator, indexedValues []types.Datum, h int64,
	opts ...*table.CreateIdxOpt) (int64, error) {
	trace_util_0.Count(_index_00000, 44)
	writeBufs := ctx.GetSessionVars().GetWriteStmtBufs()
	skipCheck := ctx.GetSessionVars().LightningMode || ctx.GetSessionVars().StmtCtx.BatchCheck
	key, distinct, err := c.GenIndexKey(ctx.GetSessionVars().StmtCtx, indexedValues, h, writeBufs.IndexKeyBuf)
	if err != nil {
		trace_util_0.Count(_index_00000, 50)
		return 0, err
	}
	// save the key buffer to reuse.
	trace_util_0.Count(_index_00000, 45)
	writeBufs.IndexKeyBuf = key
	if !distinct {
		trace_util_0.Count(_index_00000, 51)
		// non-unique index doesn't need store value, write a '0' to reduce space
		err = rm.Set(key, []byte{'0'})
		return 0, err
	}

	trace_util_0.Count(_index_00000, 46)
	var value []byte
	if !skipCheck {
		trace_util_0.Count(_index_00000, 52)
		value, err = rm.Get(key)
	}

	trace_util_0.Count(_index_00000, 47)
	if skipCheck || kv.IsErrNotFound(err) {
		trace_util_0.Count(_index_00000, 53)
		err = rm.Set(key, EncodeHandle(h))
		return 0, err
	}

	trace_util_0.Count(_index_00000, 48)
	handle, err := DecodeHandle(value)
	if err != nil {
		trace_util_0.Count(_index_00000, 54)
		return 0, err
	}
	trace_util_0.Count(_index_00000, 49)
	return handle, kv.ErrKeyExists
}

// Delete removes the entry for handle h and indexdValues from KV index.
func (c *index) Delete(sc *stmtctx.StatementContext, m kv.Mutator, indexedValues []types.Datum, h int64, ss kv.Transaction) error {
	trace_util_0.Count(_index_00000, 55)
	key, _, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		trace_util_0.Count(_index_00000, 58)
		return err
	}
	trace_util_0.Count(_index_00000, 56)
	err = m.Delete(key)
	if ss != nil {
		trace_util_0.Count(_index_00000, 59)
		switch c.idxInfo.State {
		case model.StatePublic:
			trace_util_0.Count(_index_00000, 60)
			// If the index is in public state, delete this index means it must exists.
			ss.SetAssertion(key, kv.Exist)
		default:
			trace_util_0.Count(_index_00000, 61)
			ss.SetAssertion(key, kv.None)
		}
	}
	trace_util_0.Count(_index_00000, 57)
	return err
}

// Drop removes the KV index from store.
func (c *index) Drop(rm kv.RetrieverMutator) error {
	trace_util_0.Count(_index_00000, 62)
	it, err := rm.Iter(c.prefix, c.prefix.PrefixNext())
	if err != nil {
		trace_util_0.Count(_index_00000, 65)
		return err
	}
	trace_util_0.Count(_index_00000, 63)
	defer it.Close()

	// remove all indices
	for it.Valid() {
		trace_util_0.Count(_index_00000, 66)
		if !it.Key().HasPrefix(c.prefix) {
			trace_util_0.Count(_index_00000, 69)
			break
		}
		trace_util_0.Count(_index_00000, 67)
		err := rm.Delete(it.Key())
		if err != nil {
			trace_util_0.Count(_index_00000, 70)
			return err
		}
		trace_util_0.Count(_index_00000, 68)
		err = it.Next()
		if err != nil {
			trace_util_0.Count(_index_00000, 71)
			return err
		}
	}
	trace_util_0.Count(_index_00000, 64)
	return nil
}

// Seek searches KV index for the entry with indexedValues.
func (c *index) Seek(sc *stmtctx.StatementContext, r kv.Retriever, indexedValues []types.Datum) (iter table.IndexIterator, hit bool, err error) {
	trace_util_0.Count(_index_00000, 72)
	key, _, err := c.GenIndexKey(sc, indexedValues, 0, nil)
	if err != nil {
		trace_util_0.Count(_index_00000, 76)
		return nil, false, err
	}

	trace_util_0.Count(_index_00000, 73)
	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(key, upperBound)
	if err != nil {
		trace_util_0.Count(_index_00000, 77)
		return nil, false, err
	}
	// check if hit
	trace_util_0.Count(_index_00000, 74)
	hit = false
	if it.Valid() && it.Key().Cmp(key) == 0 {
		trace_util_0.Count(_index_00000, 78)
		hit = true
	}
	trace_util_0.Count(_index_00000, 75)
	return &indexIter{it: it, idx: c, prefix: c.prefix}, hit, nil
}

// SeekFirst returns an iterator which points to the first entry of the KV index.
func (c *index) SeekFirst(r kv.Retriever) (iter table.IndexIterator, err error) {
	trace_util_0.Count(_index_00000, 79)
	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(c.prefix, upperBound)
	if err != nil {
		trace_util_0.Count(_index_00000, 81)
		return nil, err
	}
	trace_util_0.Count(_index_00000, 80)
	return &indexIter{it: it, idx: c, prefix: c.prefix}, nil
}

func (c *index) Exist(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, indexedValues []types.Datum, h int64) (bool, int64, error) {
	trace_util_0.Count(_index_00000, 82)
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		trace_util_0.Count(_index_00000, 87)
		return false, 0, err
	}

	trace_util_0.Count(_index_00000, 83)
	value, err := rm.Get(key)
	if kv.IsErrNotFound(err) {
		trace_util_0.Count(_index_00000, 88)
		return false, 0, nil
	}
	trace_util_0.Count(_index_00000, 84)
	if err != nil {
		trace_util_0.Count(_index_00000, 89)
		return false, 0, err
	}

	// For distinct index, the value of key is handle.
	trace_util_0.Count(_index_00000, 85)
	if distinct {
		trace_util_0.Count(_index_00000, 90)
		handle, err := DecodeHandle(value)
		if err != nil {
			trace_util_0.Count(_index_00000, 93)
			return false, 0, err
		}

		trace_util_0.Count(_index_00000, 91)
		if handle != h {
			trace_util_0.Count(_index_00000, 94)
			return true, handle, kv.ErrKeyExists
		}

		trace_util_0.Count(_index_00000, 92)
		return true, handle, nil
	}

	trace_util_0.Count(_index_00000, 86)
	return true, h, nil
}

func (c *index) FetchValues(r []types.Datum, vals []types.Datum) ([]types.Datum, error) {
	trace_util_0.Count(_index_00000, 95)
	needLength := len(c.idxInfo.Columns)
	if vals == nil || cap(vals) < needLength {
		trace_util_0.Count(_index_00000, 98)
		vals = make([]types.Datum, needLength)
	}
	trace_util_0.Count(_index_00000, 96)
	vals = vals[:needLength]
	for i, ic := range c.idxInfo.Columns {
		trace_util_0.Count(_index_00000, 99)
		if ic.Offset < 0 || ic.Offset >= len(r) {
			trace_util_0.Count(_index_00000, 101)
			return nil, table.ErrIndexOutBound.GenWithStack("Index column %s offset out of bound, offset: %d, row: %v",
				ic.Name, ic.Offset, r)
		}
		trace_util_0.Count(_index_00000, 100)
		vals[i] = r[ic.Offset]
	}
	trace_util_0.Count(_index_00000, 97)
	return vals, nil
}

var _index_00000 = "table/tables/index.go"
