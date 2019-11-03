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

package mocktikv

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sort"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/codec"
)

type mvccValueType int

const (
	typePut mvccValueType = iota
	typeDelete
	typeRollback
)

type mvccValue struct {
	valueType mvccValueType
	startTS   uint64
	commitTS  uint64
	value     []byte
}

type mvccLock struct {
	startTS     uint64
	primary     []byte
	value       []byte
	op          kvrpcpb.Op
	ttl         uint64
	forUpdateTS uint64
	txnSize     uint64
}

type mvccEntry struct {
	key    MvccKey
	values []mvccValue
	lock   *mvccLock
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() ([]byte, error) {
	trace_util_0.Count(_mvcc_00000, 0)
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, l.startTS)
	mh.WriteSlice(&buf, l.primary)
	mh.WriteSlice(&buf, l.value)
	mh.WriteNumber(&buf, l.op)
	mh.WriteNumber(&buf, l.ttl)
	mh.WriteNumber(&buf, l.forUpdateTS)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) error {
	trace_util_0.Count(_mvcc_00000, 1)
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	mh.ReadNumber(buf, &l.startTS)
	mh.ReadSlice(buf, &l.primary)
	mh.ReadSlice(buf, &l.value)
	mh.ReadNumber(buf, &l.op)
	mh.ReadNumber(buf, &l.ttl)
	mh.ReadNumber(buf, &l.forUpdateTS)
	return errors.Trace(mh.err)
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() ([]byte, error) {
	trace_util_0.Count(_mvcc_00000, 2)
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, int64(v.valueType))
	mh.WriteNumber(&buf, v.startTS)
	mh.WriteNumber(&buf, v.commitTS)
	mh.WriteSlice(&buf, v.value)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) error {
	trace_util_0.Count(_mvcc_00000, 3)
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	var vt int64
	mh.ReadNumber(buf, &vt)
	v.valueType = mvccValueType(vt)
	mh.ReadNumber(buf, &v.startTS)
	mh.ReadNumber(buf, &v.commitTS)
	mh.ReadSlice(buf, &v.value)
	return errors.Trace(mh.err)
}

type marshalHelper struct {
	err error
}

func (mh *marshalHelper) WriteSlice(buf io.Writer, slice []byte) {
	trace_util_0.Count(_mvcc_00000, 4)
	if mh.err != nil {
		trace_util_0.Count(_mvcc_00000, 7)
		return
	}
	trace_util_0.Count(_mvcc_00000, 5)
	var tmp [binary.MaxVarintLen64]byte
	off := binary.PutUvarint(tmp[:], uint64(len(slice)))
	if err := writeFull(buf, tmp[:off]); err != nil {
		trace_util_0.Count(_mvcc_00000, 8)
		mh.err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_mvcc_00000, 6)
	if err := writeFull(buf, slice); err != nil {
		trace_util_0.Count(_mvcc_00000, 9)
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) WriteNumber(buf io.Writer, n interface{}) {
	trace_util_0.Count(_mvcc_00000, 10)
	if mh.err != nil {
		trace_util_0.Count(_mvcc_00000, 12)
		return
	}
	trace_util_0.Count(_mvcc_00000, 11)
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		trace_util_0.Count(_mvcc_00000, 13)
		mh.err = errors.Trace(err)
	}
}

func writeFull(w io.Writer, slice []byte) error {
	trace_util_0.Count(_mvcc_00000, 14)
	written := 0
	for written < len(slice) {
		trace_util_0.Count(_mvcc_00000, 16)
		n, err := w.Write(slice[written:])
		if err != nil {
			trace_util_0.Count(_mvcc_00000, 18)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_00000, 17)
		written += n
	}
	trace_util_0.Count(_mvcc_00000, 15)
	return nil
}

func (mh *marshalHelper) ReadNumber(r io.Reader, n interface{}) {
	trace_util_0.Count(_mvcc_00000, 19)
	if mh.err != nil {
		trace_util_0.Count(_mvcc_00000, 21)
		return
	}
	trace_util_0.Count(_mvcc_00000, 20)
	err := binary.Read(r, binary.LittleEndian, n)
	if err != nil {
		trace_util_0.Count(_mvcc_00000, 22)
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) ReadSlice(r *bytes.Buffer, slice *[]byte) {
	trace_util_0.Count(_mvcc_00000, 23)
	if mh.err != nil {
		trace_util_0.Count(_mvcc_00000, 28)
		return
	}
	trace_util_0.Count(_mvcc_00000, 24)
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		trace_util_0.Count(_mvcc_00000, 29)
		mh.err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_mvcc_00000, 25)
	const c10M = 10 * 1024 * 1024
	if sz > c10M {
		trace_util_0.Count(_mvcc_00000, 30)
		mh.err = errors.New("too large slice, maybe something wrong")
		return
	}
	trace_util_0.Count(_mvcc_00000, 26)
	data := make([]byte, sz)
	if _, err := io.ReadFull(r, data); err != nil {
		trace_util_0.Count(_mvcc_00000, 31)
		mh.err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_mvcc_00000, 27)
	*slice = data
}

func newEntry(key MvccKey) *mvccEntry {
	trace_util_0.Count(_mvcc_00000, 32)
	return &mvccEntry{
		key: key,
	}
}

// lockErr returns ErrLocked.
// Note that parameter key is raw key, while key in ErrLocked is mvcc key.
func (l *mvccLock) lockErr(key []byte) error {
	trace_util_0.Count(_mvcc_00000, 33)
	return &ErrLocked{
		Key:     mvccEncode(key, lockVer),
		Primary: l.primary,
		StartTS: l.startTS,
		TTL:     l.ttl,
	}
}

func (l *mvccLock) check(ts uint64, key []byte) (uint64, error) {
	trace_util_0.Count(_mvcc_00000, 34)
	// ignore when ts is older than lock or lock's type is Lock.
	// Pessimistic lock doesn't block read.
	if l.startTS > ts || l.op == kvrpcpb.Op_Lock || l.op == kvrpcpb.Op_PessimisticLock {
		trace_util_0.Count(_mvcc_00000, 37)
		return ts, nil
	}
	// for point get latest version.
	trace_util_0.Count(_mvcc_00000, 35)
	if ts == math.MaxUint64 && bytes.Equal(l.primary, key) {
		trace_util_0.Count(_mvcc_00000, 38)
		return l.startTS - 1, nil
	}
	trace_util_0.Count(_mvcc_00000, 36)
	return 0, l.lockErr(key)
}

func (e *mvccEntry) Clone() *mvccEntry {
	trace_util_0.Count(_mvcc_00000, 39)
	var entry mvccEntry
	entry.key = append([]byte(nil), e.key...)
	for _, v := range e.values {
		trace_util_0.Count(_mvcc_00000, 42)
		entry.values = append(entry.values, mvccValue{
			valueType: v.valueType,
			startTS:   v.startTS,
			commitTS:  v.commitTS,
			value:     append([]byte(nil), v.value...),
		})
	}
	trace_util_0.Count(_mvcc_00000, 40)
	if e.lock != nil {
		trace_util_0.Count(_mvcc_00000, 43)
		entry.lock = &mvccLock{
			startTS: e.lock.startTS,
			primary: append([]byte(nil), e.lock.primary...),
			value:   append([]byte(nil), e.lock.value...),
			op:      e.lock.op,
			ttl:     e.lock.ttl,
		}
	}
	trace_util_0.Count(_mvcc_00000, 41)
	return &entry
}

func (e *mvccEntry) Less(than btree.Item) bool {
	trace_util_0.Count(_mvcc_00000, 44)
	return bytes.Compare(e.key, than.(*mvccEntry).key) < 0
}

func (e *mvccEntry) Get(ts uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	trace_util_0.Count(_mvcc_00000, 45)
	if isoLevel == kvrpcpb.IsolationLevel_SI && e.lock != nil {
		trace_util_0.Count(_mvcc_00000, 48)
		var err error
		ts, err = e.lock.check(ts, e.key.Raw())
		if err != nil {
			trace_util_0.Count(_mvcc_00000, 49)
			return nil, err
		}
	}
	trace_util_0.Count(_mvcc_00000, 46)
	for _, v := range e.values {
		trace_util_0.Count(_mvcc_00000, 50)
		if v.commitTS <= ts && v.valueType != typeRollback {
			trace_util_0.Count(_mvcc_00000, 51)
			return v.value, nil
		}
	}
	trace_util_0.Count(_mvcc_00000, 47)
	return nil, nil
}

func (e *mvccEntry) Prewrite(mutation *kvrpcpb.Mutation, startTS uint64, primary []byte, ttl uint64) error {
	trace_util_0.Count(_mvcc_00000, 52)
	if len(e.values) > 0 {
		trace_util_0.Count(_mvcc_00000, 55)
		if e.values[0].commitTS >= startTS {
			trace_util_0.Count(_mvcc_00000, 56)
			return &ErrConflict{
				StartTS:    startTS,
				ConflictTS: e.values[0].commitTS,
				Key:        mutation.Key,
			}
		}
	}
	trace_util_0.Count(_mvcc_00000, 53)
	if e.lock != nil {
		trace_util_0.Count(_mvcc_00000, 57)
		if e.lock.startTS != startTS {
			trace_util_0.Count(_mvcc_00000, 59)
			return e.lock.lockErr(e.key.Raw())
		}
		trace_util_0.Count(_mvcc_00000, 58)
		return nil
	}
	trace_util_0.Count(_mvcc_00000, 54)
	e.lock = &mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.GetOp(),
		ttl:     ttl,
	}
	return nil
}

func (e *mvccEntry) getTxnCommitInfo(startTS uint64) *mvccValue {
	trace_util_0.Count(_mvcc_00000, 60)
	for _, v := range e.values {
		trace_util_0.Count(_mvcc_00000, 62)
		if v.startTS == startTS {
			trace_util_0.Count(_mvcc_00000, 63)
			return &v
		}
	}
	trace_util_0.Count(_mvcc_00000, 61)
	return nil
}

func (e *mvccEntry) Commit(startTS, commitTS uint64) error {
	trace_util_0.Count(_mvcc_00000, 64)
	if e.lock == nil || e.lock.startTS != startTS {
		trace_util_0.Count(_mvcc_00000, 67)
		if c := e.getTxnCommitInfo(startTS); c != nil && c.valueType != typeRollback {
			trace_util_0.Count(_mvcc_00000, 69)
			return nil
		}
		trace_util_0.Count(_mvcc_00000, 68)
		return ErrRetryable("txn not found")
	}
	trace_util_0.Count(_mvcc_00000, 65)
	if e.lock.op != kvrpcpb.Op_Lock {
		trace_util_0.Count(_mvcc_00000, 70)
		var valueType mvccValueType
		if e.lock.op == kvrpcpb.Op_Put {
			trace_util_0.Count(_mvcc_00000, 72)
			valueType = typePut
		} else {
			trace_util_0.Count(_mvcc_00000, 73)
			{
				valueType = typeDelete
			}
		}
		trace_util_0.Count(_mvcc_00000, 71)
		e.addValue(mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     e.lock.value,
		})
	}
	trace_util_0.Count(_mvcc_00000, 66)
	e.lock = nil
	return nil
}

func (e *mvccEntry) Rollback(startTS uint64) error {
	trace_util_0.Count(_mvcc_00000, 74)
	// If current transaction's lock exist.
	if e.lock != nil && e.lock.startTS == startTS {
		trace_util_0.Count(_mvcc_00000, 77)
		e.lock = nil
		e.addValue(mvccValue{
			valueType: typeRollback,
			startTS:   startTS,
			commitTS:  startTS,
		})
		return nil
	}

	// If current transaction's lock not exist.
	// If commit info of current transaction exist.
	trace_util_0.Count(_mvcc_00000, 75)
	if c := e.getTxnCommitInfo(startTS); c != nil {
		trace_util_0.Count(_mvcc_00000, 78)
		// If current transaction is already committed.
		if c.valueType != typeRollback {
			trace_util_0.Count(_mvcc_00000, 80)
			return ErrAlreadyCommitted(c.commitTS)
		}
		// If current transaction is already rollback.
		trace_util_0.Count(_mvcc_00000, 79)
		return nil
	}
	// If current transaction is not prewritted before.
	trace_util_0.Count(_mvcc_00000, 76)
	e.addValue(mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	})
	return nil
}

func (e *mvccEntry) addValue(v mvccValue) {
	trace_util_0.Count(_mvcc_00000, 81)
	i := sort.Search(len(e.values), func(i int) bool { trace_util_0.Count(_mvcc_00000, 83); return e.values[i].commitTS <= v.commitTS })
	trace_util_0.Count(_mvcc_00000, 82)
	if i >= len(e.values) {
		trace_util_0.Count(_mvcc_00000, 84)
		e.values = append(e.values, v)
	} else {
		trace_util_0.Count(_mvcc_00000, 85)
		{
			e.values = append(e.values[:i+1], e.values[i:]...)
			e.values[i] = v
		}
	}
}

func (e *mvccEntry) containsStartTS(startTS uint64) bool {
	trace_util_0.Count(_mvcc_00000, 86)
	if e.lock != nil && e.lock.startTS == startTS {
		trace_util_0.Count(_mvcc_00000, 89)
		return true
	}
	trace_util_0.Count(_mvcc_00000, 87)
	for _, item := range e.values {
		trace_util_0.Count(_mvcc_00000, 90)
		if item.startTS == startTS {
			trace_util_0.Count(_mvcc_00000, 92)
			return true
		}
		trace_util_0.Count(_mvcc_00000, 91)
		if item.commitTS < startTS {
			trace_util_0.Count(_mvcc_00000, 93)
			return false
		}
	}
	trace_util_0.Count(_mvcc_00000, 88)
	return false
}

func (e *mvccEntry) dumpMvccInfo() *kvrpcpb.MvccInfo {
	trace_util_0.Count(_mvcc_00000, 94)
	info := &kvrpcpb.MvccInfo{}
	if e.lock != nil {
		trace_util_0.Count(_mvcc_00000, 97)
		info.Lock = &kvrpcpb.MvccLock{
			Type:       e.lock.op,
			StartTs:    e.lock.startTS,
			Primary:    e.lock.primary,
			ShortValue: e.lock.value,
		}
	}

	trace_util_0.Count(_mvcc_00000, 95)
	info.Writes = make([]*kvrpcpb.MvccWrite, len(e.values))
	info.Values = make([]*kvrpcpb.MvccValue, len(e.values))

	for id, item := range e.values {
		trace_util_0.Count(_mvcc_00000, 98)
		var tp kvrpcpb.Op
		switch item.valueType {
		case typePut:
			trace_util_0.Count(_mvcc_00000, 100)
			tp = kvrpcpb.Op_Put
		case typeDelete:
			trace_util_0.Count(_mvcc_00000, 101)
			tp = kvrpcpb.Op_Del
		case typeRollback:
			trace_util_0.Count(_mvcc_00000, 102)
			tp = kvrpcpb.Op_Rollback
		}
		trace_util_0.Count(_mvcc_00000, 99)
		info.Writes[id] = &kvrpcpb.MvccWrite{
			Type:     tp,
			StartTs:  item.startTS,
			CommitTs: item.commitTS,
		}

		info.Values[id] = &kvrpcpb.MvccValue{
			Value:   item.value,
			StartTs: item.startTS,
		}
	}
	trace_util_0.Count(_mvcc_00000, 96)
	return info
}

type rawEntry struct {
	key   []byte
	value []byte
}

func newRawEntry(key []byte) *rawEntry {
	trace_util_0.Count(_mvcc_00000, 103)
	return &rawEntry{
		key: key,
	}
}

func (e *rawEntry) Less(than btree.Item) bool {
	trace_util_0.Count(_mvcc_00000, 104)
	return bytes.Compare(e.key, than.(*rawEntry).key) < 0
}

// MVCCStore is a mvcc key-value storage.
type MVCCStore interface {
	Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error)
	Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	PessimisticLock(mutations []*kvrpcpb.Mutation, primary []byte, startTS, forUpdateTS uint64, ttl uint64) []error
	PessimisticRollback(keys [][]byte, startTS, forUpdateTS uint64) []error
	Prewrite(req *kvrpcpb.PrewriteRequest) []error
	Commit(keys [][]byte, startTS, commitTS uint64) error
	Rollback(keys [][]byte, startTS uint64) error
	Cleanup(key []byte, startTS uint64) error
	ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error)
	ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error
	BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error
	DeleteRange(startKey, endKey []byte) error
	Close() error
}

// RawKV is a key-value storage. MVCCStore can be implemented upon it with timestamp encoded into key.
type RawKV interface {
	RawGet(key []byte) []byte
	RawBatchGet(keys [][]byte) [][]byte
	RawScan(startKey, endKey []byte, limit int) []Pair        // Scan the range of [startKey, endKey)
	RawReverseScan(startKey, endKey []byte, limit int) []Pair // Scan the range of [endKey, startKey)
	RawPut(key, value []byte)
	RawBatchPut(keys, values [][]byte)
	RawDelete(key []byte)
	RawBatchDelete(keys [][]byte)
	RawDeleteRange(startKey, endKey []byte)
}

// MVCCDebugger is for debugging.
type MVCCDebugger interface {
	MvccGetByStartTS(startKey, endKey []byte, starTS uint64) (*kvrpcpb.MvccInfo, []byte)
	MvccGetByKey(key []byte) *kvrpcpb.MvccInfo
}

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}

func regionContains(startKey []byte, endKey []byte, key []byte) bool {
	trace_util_0.Count(_mvcc_00000, 105)
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}

// MvccKey is the encoded key type.
// On TiKV, keys are encoded before they are saved into storage engine.
type MvccKey []byte

// NewMvccKey encodes a key into MvccKey.
func NewMvccKey(key []byte) MvccKey {
	trace_util_0.Count(_mvcc_00000, 106)
	if len(key) == 0 {
		trace_util_0.Count(_mvcc_00000, 108)
		return nil
	}
	trace_util_0.Count(_mvcc_00000, 107)
	return codec.EncodeBytes(nil, key)
}

// Raw decodes a MvccKey to original key.
func (key MvccKey) Raw() []byte {
	trace_util_0.Count(_mvcc_00000, 109)
	if len(key) == 0 {
		trace_util_0.Count(_mvcc_00000, 112)
		return nil
	}
	trace_util_0.Count(_mvcc_00000, 110)
	_, k, err := codec.DecodeBytes(key, nil)
	if err != nil {
		trace_util_0.Count(_mvcc_00000, 113)
		panic(err)
	}
	trace_util_0.Count(_mvcc_00000, 111)
	return k
}

var _mvcc_00000 = "store/mockstore/mocktikv/mvcc.go"
