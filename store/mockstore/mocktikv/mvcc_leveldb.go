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

package mocktikv

import (
	"bytes"
	"context"
	"math"
	"sync"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"github.com/pingcap/goleveldb/leveldb/storage"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/deadlock"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// MVCCLevelDB implements the MVCCStore interface.
type MVCCLevelDB struct {
	// Key layout:
	// ...
	// Key_lock        -- (0)
	// Key_verMax      -- (1)
	// ...
	// Key_ver+1       -- (2)
	// Key_ver         -- (3)
	// Key_ver-1       -- (4)
	// ...
	// Key_0           -- (5)
	// NextKey_lock    -- (6)
	// NextKey_verMax  -- (7)
	// ...
	// NextKey_ver+1   -- (8)
	// NextKey_ver     -- (9)
	// NextKey_ver-1   -- (10)
	// ...
	// NextKey_0       -- (11)
	// ...
	// EOF

	// db represents leveldb
	db *leveldb.DB
	// mu used for lock
	// leveldb can not guarantee multiple operations to be atomic, for example, read
	// then write, another write may happen during it, so this lock is necessory.
	mu               sync.RWMutex
	deadlockDetector *deadlock.Detector
}

const lockVer uint64 = math.MaxUint64

// ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
var ErrInvalidEncodedKey = errors.New("invalid encoded key")

// mvccEncode returns the encoded key.
func mvccEncode(key []byte, ver uint64) []byte {
	trace_util_0.Count(_mvcc_leveldb_00000, 0)
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver)
	return ret
}

// mvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func mvccDecode(encodedKey []byte) ([]byte, uint64, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 1)
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes(encodedKey, nil)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 6)
		// should never happen
		return nil, 0, errors.Trace(err)
	}
	// if it's meta key
	trace_util_0.Count(_mvcc_leveldb_00000, 2)
	if len(remainBytes) == 0 {
		trace_util_0.Count(_mvcc_leveldb_00000, 7)
		return key, 0, nil
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 3)
	var ver uint64
	remainBytes, ver, err = codec.DecodeUintDesc(remainBytes)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 8)
		// should never happen
		return nil, 0, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 4)
	if len(remainBytes) != 0 {
		trace_util_0.Count(_mvcc_leveldb_00000, 9)
		return nil, 0, ErrInvalidEncodedKey
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 5)
	return key, ver, nil
}

// MustNewMVCCStore is used for testing, use NewMVCCLevelDB instead.
func MustNewMVCCStore() MVCCStore {
	trace_util_0.Count(_mvcc_leveldb_00000, 10)
	mvccStore, err := NewMVCCLevelDB("")
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 12)
		panic(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 11)
	return mvccStore
}

// NewMVCCLevelDB returns a new MVCCLevelDB object.
func NewMVCCLevelDB(path string) (*MVCCLevelDB, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 13)
	var (
		d   *leveldb.DB
		err error
	)
	if path == "" {
		trace_util_0.Count(_mvcc_leveldb_00000, 15)
		d, err = leveldb.Open(storage.NewMemStorage(), nil)
	} else {
		trace_util_0.Count(_mvcc_leveldb_00000, 16)
		{
			d, err = leveldb.OpenFile(path, &opt.Options{BlockCacheCapacity: 600 * 1024 * 1024})
		}
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 14)
	return &MVCCLevelDB{db: d, deadlockDetector: deadlock.NewDetector()}, errors.Trace(err)
}

// Iterator wraps iterator.Iterator to provide Valid() method.
type Iterator struct {
	iterator.Iterator
	valid bool
}

// Next moves the iterator to the next key/value pair.
func (iter *Iterator) Next() {
	trace_util_0.Count(_mvcc_leveldb_00000, 17)
	iter.valid = iter.Iterator.Next()
}

// Valid returns whether the iterator is exhausted.
func (iter *Iterator) Valid() bool {
	trace_util_0.Count(_mvcc_leveldb_00000, 18)
	return iter.valid
}

func newIterator(db *leveldb.DB, slice *util.Range) *Iterator {
	trace_util_0.Count(_mvcc_leveldb_00000, 19)
	iter := &Iterator{db.NewIterator(slice, nil), true}
	iter.Next()
	return iter
}

func newScanIterator(db *leveldb.DB, startKey, endKey []byte) (*Iterator, []byte, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 20)
	var start, end []byte
	if len(startKey) > 0 {
		trace_util_0.Count(_mvcc_leveldb_00000, 24)
		start = mvccEncode(startKey, lockVer)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 21)
	if len(endKey) > 0 {
		trace_util_0.Count(_mvcc_leveldb_00000, 25)
		end = mvccEncode(endKey, lockVer)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 22)
	iter := newIterator(db, &util.Range{
		Start: start,
		Limit: end,
	})
	// newScanIterator must handle startKey is nil, in this case, the real startKey
	// should be change the frist key of the store.
	if len(startKey) == 0 && iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 26)
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 28)
			return nil, nil, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 27)
		startKey = key
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 23)
	return iter, startKey, nil
}

// iterDecoder tries to decode an Iterator value.
// If current iterator value can be decoded by this decoder, store the value and call iter.Next(),
// Otherwise current iterator is not touched and returns false.
type iterDecoder interface {
	Decode(iter *Iterator) (bool, error)
}

type lockDecoder struct {
	lock      mvccLock
	expectKey []byte
}

// Decode decodes the lock value if current iterator is at expectKey::lock.
func (dec *lockDecoder) Decode(iter *Iterator) (bool, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 29)
	if iter.Error() != nil || !iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 35)
		return false, iter.Error()
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 30)
	iterKey := iter.Key()
	key, ver, err := mvccDecode(iterKey)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 36)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 31)
	if !bytes.Equal(key, dec.expectKey) {
		trace_util_0.Count(_mvcc_leveldb_00000, 37)
		return false, nil
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 32)
	if ver != lockVer {
		trace_util_0.Count(_mvcc_leveldb_00000, 38)
		return false, nil
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 33)
	var lock mvccLock
	err = lock.UnmarshalBinary(iter.Value())
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 39)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 34)
	dec.lock = lock
	iter.Next()
	return true, nil
}

type valueDecoder struct {
	value     mvccValue
	expectKey []byte
}

// Decode decodes a mvcc value if iter key is expectKey.
func (dec *valueDecoder) Decode(iter *Iterator) (bool, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 40)
	if iter.Error() != nil || !iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 46)
		return false, iter.Error()
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 41)
	key, ver, err := mvccDecode(iter.Key())
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 47)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 42)
	if !bytes.Equal(key, dec.expectKey) {
		trace_util_0.Count(_mvcc_leveldb_00000, 48)
		return false, nil
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 43)
	if ver == lockVer {
		trace_util_0.Count(_mvcc_leveldb_00000, 49)
		return false, nil
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 44)
	var value mvccValue
	err = value.UnmarshalBinary(iter.Value())
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 50)
		return false, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 45)
	dec.value = value
	iter.Next()
	return true, nil
}

type skipDecoder struct {
	currKey []byte
}

// Decode skips the iterator as long as its key is currKey, the new key would be stored.
func (dec *skipDecoder) Decode(iter *Iterator) (bool, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 51)
	if iter.Error() != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 54)
		return false, iter.Error()
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 52)
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 55)
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 58)
			return false, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 56)
		if !bytes.Equal(key, dec.currKey) {
			trace_util_0.Count(_mvcc_leveldb_00000, 59)
			dec.currKey = key
			return true, nil
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 57)
		iter.Next()
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 53)
	return false, nil
}

type mvccEntryDecoder struct {
	expectKey []byte
	// mvccEntry represents values and lock is valid.
	mvccEntry
}

// Decode decodes a mvcc entry.
func (dec *mvccEntryDecoder) Decode(iter *Iterator) (bool, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 60)
	ldec := lockDecoder{expectKey: dec.expectKey}
	ok, err := ldec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 64)
		return ok, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 61)
	if ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 65)
		dec.mvccEntry.lock = &ldec.lock
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 62)
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 66)
		vdec := valueDecoder{expectKey: dec.expectKey}
		ok, err = vdec.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 69)
			return ok, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 67)
		if !ok {
			trace_util_0.Count(_mvcc_leveldb_00000, 70)
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 68)
		dec.mvccEntry.values = append(dec.mvccEntry.values, vdec.value)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 63)
	succ := dec.mvccEntry.lock != nil || len(dec.mvccEntry.values) > 0
	return succ, nil
}

// Get implements the MVCCStore interface.
// key cannot be nil or []byte{}
func (mvcc *MVCCLevelDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 71)
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	return mvcc.getValue(key, startTS, isoLevel)
}

func (mvcc *MVCCLevelDB) getValue(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 72)
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	return getValue(iter, key, startTS, isoLevel)
}

func getValue(iter *Iterator, key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 73)
	dec1 := lockDecoder{expectKey: key}
	ok, err := dec1.Decode(iter)
	if ok && isoLevel == kvrpcpb.IsolationLevel_SI {
		trace_util_0.Count(_mvcc_leveldb_00000, 77)
		startTS, err = dec1.lock.check(startTS, key)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 74)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 78)
		return nil, errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 75)
	dec2 := valueDecoder{expectKey: key}
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 79)
		ok, err := dec2.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 83)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 80)
		if !ok {
			trace_util_0.Count(_mvcc_leveldb_00000, 84)
			break
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 81)
		value := &dec2.value
		if value.valueType == typeRollback {
			trace_util_0.Count(_mvcc_leveldb_00000, 85)
			continue
		}
		// Read the first committed value that can be seen at startTS.
		trace_util_0.Count(_mvcc_leveldb_00000, 82)
		if value.commitTS <= startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 86)
			if value.valueType == typeDelete {
				trace_util_0.Count(_mvcc_leveldb_00000, 88)
				return nil, nil
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 87)
			return value.value, nil
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 76)
	return nil, nil
}

// BatchGet implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	trace_util_0.Count(_mvcc_leveldb_00000, 89)
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	pairs := make([]Pair, 0, len(ks))
	for _, k := range ks {
		trace_util_0.Count(_mvcc_leveldb_00000, 91)
		v, err := mvcc.getValue(k, startTS, isoLevel)
		if v == nil && err == nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 93)
			continue
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 92)
		pairs = append(pairs, Pair{
			Key:   k,
			Value: v,
			Err:   errors.Trace(err),
		})
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 90)
	return pairs
}

// Scan implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	trace_util_0.Count(_mvcc_leveldb_00000, 94)
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 97)
		logutil.Logger(context.Background()).Error("scan new iterator fail", zap.Error(err))
		return nil
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 95)
	ok := true
	var pairs []Pair
	for len(pairs) < limit && ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 98)
		value, err := getValue(iter, currKey, startTS, isoLevel)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 102)
			pairs = append(pairs, Pair{
				Key: currKey,
				Err: errors.Trace(err),
			})
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 99)
		if value != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 103)
			pairs = append(pairs, Pair{
				Key:   currKey,
				Value: value,
			})
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 100)
		skip := skipDecoder{currKey}
		ok, err = skip.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 104)
			logutil.Logger(context.Background()).Error("seek to next key error", zap.Error(err))
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 101)
		currKey = skip.currKey
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 96)
	return pairs
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (mvcc *MVCCLevelDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	trace_util_0.Count(_mvcc_leveldb_00000, 105)
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	var mvccEnd []byte
	if len(endKey) != 0 {
		trace_util_0.Count(_mvcc_leveldb_00000, 109)
		mvccEnd = mvccEncode(endKey, lockVer)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 106)
	iter := mvcc.db.NewIterator(&util.Range{
		Limit: mvccEnd,
	}, nil)
	defer iter.Release()

	succ := iter.Last()
	currKey, _, err := mvccDecode(iter.Key())
	// TODO: return error.
	terror.Log(errors.Trace(err))
	helper := reverseScanHelper{
		startTS:  startTS,
		isoLevel: isoLevel,
		currKey:  currKey,
	}

	for succ && len(helper.pairs) < limit {
		trace_util_0.Count(_mvcc_leveldb_00000, 110)
		key, ver, err := mvccDecode(iter.Key())
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 116)
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 111)
		if bytes.Compare(key, startKey) < 0 {
			trace_util_0.Count(_mvcc_leveldb_00000, 117)
			break
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 112)
		if !bytes.Equal(key, helper.currKey) {
			trace_util_0.Count(_mvcc_leveldb_00000, 118)
			helper.finishEntry()
			helper.currKey = key
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 113)
		if ver == lockVer {
			trace_util_0.Count(_mvcc_leveldb_00000, 119)
			var lock mvccLock
			err = lock.UnmarshalBinary(iter.Value())
			helper.entry.lock = &lock
		} else {
			trace_util_0.Count(_mvcc_leveldb_00000, 120)
			{
				var value mvccValue
				err = value.UnmarshalBinary(iter.Value())
				helper.entry.values = append(helper.entry.values, value)
			}
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 114)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 121)
			logutil.Logger(context.Background()).Error("unmarshal fail", zap.Error(err))
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 115)
		succ = iter.Prev()
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 107)
	if len(helper.pairs) < limit {
		trace_util_0.Count(_mvcc_leveldb_00000, 122)
		helper.finishEntry()
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 108)
	return helper.pairs
}

type reverseScanHelper struct {
	startTS  uint64
	isoLevel kvrpcpb.IsolationLevel
	currKey  []byte
	entry    mvccEntry
	pairs    []Pair
}

func (helper *reverseScanHelper) finishEntry() {
	trace_util_0.Count(_mvcc_leveldb_00000, 123)
	reverse(helper.entry.values)
	helper.entry.key = NewMvccKey(helper.currKey)
	val, err := helper.entry.Get(helper.startTS, helper.isoLevel)
	if len(val) != 0 || err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 125)
		helper.pairs = append(helper.pairs, Pair{
			Key:   helper.currKey,
			Value: val,
			Err:   err,
		})
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 124)
	helper.entry = mvccEntry{}
}

func reverse(values []mvccValue) {
	trace_util_0.Count(_mvcc_leveldb_00000, 126)
	i, j := 0, len(values)-1
	for i < j {
		trace_util_0.Count(_mvcc_leveldb_00000, 127)
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
}

// PessimisticLock writes the pessimistic lock.
func (mvcc *MVCCLevelDB) PessimisticLock(mutations []*kvrpcpb.Mutation, primary []byte, startTS, forUpdateTS uint64, ttl uint64) []error {
	trace_util_0.Count(_mvcc_leveldb_00000, 128)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(mutations))
	for _, m := range mutations {
		trace_util_0.Count(_mvcc_leveldb_00000, 132)
		err := mvcc.pessimisticLockMutation(batch, m, startTS, forUpdateTS, primary, ttl)
		errs = append(errs, err)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 133)
			anyError = true
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 129)
	if anyError {
		trace_util_0.Count(_mvcc_leveldb_00000, 134)
		return errs
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 130)
	if err := mvcc.db.Write(batch, nil); err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 135)
		return []error{err}
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 131)
	return errs
}

func (mvcc *MVCCLevelDB) pessimisticLockMutation(batch *leveldb.Batch, mutation *kvrpcpb.Mutation, startTS, forUpdateTS uint64, primary []byte, ttl uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 136)
	startKey := mvccEncode(mutation.Key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: mutation.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 141)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 137)
	if ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 142)
		if dec.lock.startTS != startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 144)
			errDeadlock := mvcc.deadlockDetector.Detect(startTS, dec.lock.startTS, farm.Fingerprint64(mutation.Key))
			if errDeadlock != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 146)
				return &ErrDeadlock{
					LockKey:        mutation.Key,
					LockTS:         dec.lock.startTS,
					DealockKeyHash: errDeadlock.KeyHash,
				}
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 145)
			return dec.lock.lockErr(mutation.Key)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 143)
		return nil
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 138)
	if err = checkConflictValue(iter, mutation, forUpdateTS); err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 147)
		return err
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 139)
	lock := mvccLock{
		startTS:     startTS,
		primary:     primary,
		op:          kvrpcpb.Op_PessimisticLock,
		ttl:         ttl,
		forUpdateTS: forUpdateTS,
	}
	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 148)
		return errors.Trace(err)
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 140)
	batch.Put(writeKey, writeValue)
	return nil
}

// PessimisticRollback implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) PessimisticRollback(keys [][]byte, startTS, forUpdateTS uint64) []error {
	trace_util_0.Count(_mvcc_leveldb_00000, 149)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(keys))
	for _, key := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 153)
		err := pessimisticRollbackKey(mvcc.db, batch, key, startTS, forUpdateTS)
		errs = append(errs, err)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 154)
			anyError = true
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 150)
	if anyError {
		trace_util_0.Count(_mvcc_leveldb_00000, 155)
		return errs
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 151)
	if err := mvcc.db.Write(batch, nil); err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 156)
		return []error{err}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 152)
	return errs
}

func pessimisticRollbackKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS, forUpdateTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 157)
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 160)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 158)
	if ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 161)
		lock := dec.lock
		if lock.op == kvrpcpb.Op_PessimisticLock && lock.startTS == startTS && lock.forUpdateTS <= forUpdateTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 162)
			batch.Delete(startKey)
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 159)
	return nil
}

// Prewrite implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Prewrite(req *kvrpcpb.PrewriteRequest) []error {
	trace_util_0.Count(_mvcc_leveldb_00000, 163)
	mutations := req.Mutations
	primary := req.PrimaryLock
	startTS := req.StartVersion
	ttl := req.LockTtl
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(mutations))
	txnSize := len(mutations)
	for i, m := range mutations {
		trace_util_0.Count(_mvcc_leveldb_00000, 167)
		// If the operation is Insert, check if key is exists at first.
		var err error
		if m.GetOp() == kvrpcpb.Op_Insert {
			trace_util_0.Count(_mvcc_leveldb_00000, 169)
			v, err := mvcc.getValue(m.Key, startTS, kvrpcpb.IsolationLevel_SI)
			if err != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 171)
				errs = append(errs, err)
				anyError = true
				continue
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 170)
			if v != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 172)
				err = &ErrKeyAlreadyExist{
					Key: m.Key,
				}
				errs = append(errs, err)
				anyError = true
				continue
			}
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 168)
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		err = prewriteMutation(mvcc.db, batch, m, startTS, primary, ttl, uint64(txnSize), isPessimisticLock)
		errs = append(errs, err)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 173)
			anyError = true
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 164)
	if anyError {
		trace_util_0.Count(_mvcc_leveldb_00000, 174)
		return errs
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 165)
	if err := mvcc.db.Write(batch, nil); err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 175)
		return []error{err}
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 166)
	return errs
}

func checkConflictValue(iter *Iterator, m *kvrpcpb.Mutation, startTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 176)
	dec := valueDecoder{
		expectKey: m.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 181)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 177)
	if !ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 182)
		return nil
	}
	// Note that it's a write conflict here, even if the value is a rollback one.
	trace_util_0.Count(_mvcc_leveldb_00000, 178)
	if dec.value.commitTS >= startTS {
		trace_util_0.Count(_mvcc_leveldb_00000, 183)
		return &ErrConflict{
			StartTS:    startTS,
			ConflictTS: dec.value.commitTS,
			Key:        m.Key,
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 179)
	if m.Op == kvrpcpb.Op_PessimisticLock && m.Assertion == kvrpcpb.Assertion_NotExist {
		trace_util_0.Count(_mvcc_leveldb_00000, 184)
		return &ErrKeyAlreadyExist{
			Key: m.Key,
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 180)
	return nil
}

func prewriteMutation(db *leveldb.DB, batch *leveldb.Batch, mutation *kvrpcpb.Mutation, startTS uint64, primary []byte, ttl uint64, txnSize uint64, isPessimisticLock bool) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 185)
	startKey := mvccEncode(mutation.Key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: mutation.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 191)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 186)
	if ok {
		trace_util_0.Count(_mvcc_leveldb_00000, 192)
		if dec.lock.startTS != startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 194)
			return dec.lock.lockErr(mutation.Key)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 193)
		if dec.lock.op != kvrpcpb.Op_PessimisticLock {
			trace_util_0.Count(_mvcc_leveldb_00000, 195)
			return nil
		}
		// Overwrite the pessimistic lock.
	} else {
		trace_util_0.Count(_mvcc_leveldb_00000, 196)
		{
			if isPessimisticLock {
				trace_util_0.Count(_mvcc_leveldb_00000, 198)
				return ErrAbort("pessimistic lock not found")
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 197)
			err = checkConflictValue(iter, mutation, startTS)
			if err != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 199)
				return err
			}
		}
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 187)
	op := mutation.GetOp()
	if op == kvrpcpb.Op_Insert {
		trace_util_0.Count(_mvcc_leveldb_00000, 200)
		op = kvrpcpb.Op_Put
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 188)
	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      op,
		ttl:     ttl,
		txnSize: txnSize,
	}
	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 201)
		return errors.Trace(err)
	}

	// Check assertions.
	trace_util_0.Count(_mvcc_leveldb_00000, 189)
	if (ok && mutation.Assertion == kvrpcpb.Assertion_NotExist) ||
		(!ok && mutation.Assertion == kvrpcpb.Assertion_Exist) {
		trace_util_0.Count(_mvcc_leveldb_00000, 202)
		logutil.Logger(context.Background()).Error("ASSERTION FAIL!!!", zap.Stringer("mutation", mutation))
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 190)
	batch.Put(writeKey, writeValue)
	return nil
}

// Commit implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Commit(keys [][]byte, startTS, commitTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 203)
	mvcc.mu.Lock()
	defer func() {
		trace_util_0.Count(_mvcc_leveldb_00000, 206)
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	trace_util_0.Count(_mvcc_leveldb_00000, 204)
	batch := &leveldb.Batch{}
	for _, k := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 207)
		err := commitKey(mvcc.db, batch, k, startTS, commitTS)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 208)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 205)
	return mvcc.db.Write(batch, nil)
}

func commitKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS, commitTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 209)
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 213)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 210)
	if !ok || dec.lock.startTS != startTS {
		trace_util_0.Count(_mvcc_leveldb_00000, 214)
		// If the lock of this transaction is not found, or the lock is replaced by
		// another transaction, check commit information of this transaction.
		c, ok, err1 := getTxnCommitInfo(iter, key, startTS)
		if err1 != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 217)
			return errors.Trace(err1)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 215)
		if ok && c.valueType != typeRollback {
			trace_util_0.Count(_mvcc_leveldb_00000, 218)
			// c.valueType != typeRollback means the transaction is already committed, do nothing.
			return nil
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 216)
		return ErrRetryable("txn not found")
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 211)
	if err = commitLock(batch, dec.lock, key, startTS, commitTS); err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 219)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 212)
	return nil
}

func commitLock(batch *leveldb.Batch, lock mvccLock, key []byte, startTS, commitTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 220)
	if lock.op != kvrpcpb.Op_Lock {
		trace_util_0.Count(_mvcc_leveldb_00000, 222)
		var valueType mvccValueType
		if lock.op == kvrpcpb.Op_Put {
			trace_util_0.Count(_mvcc_leveldb_00000, 225)
			valueType = typePut
		} else {
			trace_util_0.Count(_mvcc_leveldb_00000, 226)
			{
				valueType = typeDelete
			}
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 223)
		value := mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     lock.value,
		}
		writeKey := mvccEncode(key, commitTS)
		writeValue, err := value.MarshalBinary()
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 227)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 224)
		batch.Put(writeKey, writeValue)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 221)
	batch.Delete(mvccEncode(key, lockVer))
	return nil
}

// Rollback implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Rollback(keys [][]byte, startTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 228)
	mvcc.mu.Lock()
	defer func() {
		trace_util_0.Count(_mvcc_leveldb_00000, 231)
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	trace_util_0.Count(_mvcc_leveldb_00000, 229)
	batch := &leveldb.Batch{}
	for _, k := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 232)
		err := rollbackKey(mvcc.db, batch, k, startTS)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 233)
			return errors.Trace(err)
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 230)
	return mvcc.db.Write(batch, nil)
}

func rollbackKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 234)
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	if iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 237)
		dec := lockDecoder{
			expectKey: key,
		}
		ok, err := dec.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 241)
			return errors.Trace(err)
		}
		// If current transaction's lock exist.
		trace_util_0.Count(_mvcc_leveldb_00000, 238)
		if ok && dec.lock.startTS == startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 242)
			if err = rollbackLock(batch, dec.lock, key, startTS); err != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 244)
				return errors.Trace(err)
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 243)
			return nil
		}

		// If current transaction's lock not exist.
		// If commit info of current transaction exist.
		trace_util_0.Count(_mvcc_leveldb_00000, 239)
		c, ok, err := getTxnCommitInfo(iter, key, startTS)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 245)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 240)
		if ok {
			trace_util_0.Count(_mvcc_leveldb_00000, 246)
			// If current transaction is already committed.
			if c.valueType != typeRollback {
				trace_util_0.Count(_mvcc_leveldb_00000, 248)
				return ErrAlreadyCommitted(c.commitTS)
			}
			// If current transaction is already rollback.
			trace_util_0.Count(_mvcc_leveldb_00000, 247)
			return nil
		}
	}

	// If current transaction is not prewritted before.
	trace_util_0.Count(_mvcc_leveldb_00000, 235)
	value := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvccEncode(key, startTS)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 249)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 236)
	batch.Put(writeKey, writeValue)
	return nil
}

func rollbackLock(batch *leveldb.Batch, lock mvccLock, key []byte, startTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 250)
	tomb := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvccEncode(key, startTS)
	writeValue, err := tomb.MarshalBinary()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 252)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 251)
	batch.Put(writeKey, writeValue)
	batch.Delete(mvccEncode(key, lockVer))
	return nil
}

func getTxnCommitInfo(iter *Iterator, expectKey []byte, startTS uint64) (mvccValue, bool, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 253)
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 255)
		dec := valueDecoder{
			expectKey: expectKey,
		}
		ok, err := dec.Decode(iter)
		if err != nil || !ok {
			trace_util_0.Count(_mvcc_leveldb_00000, 257)
			return mvccValue{}, ok, errors.Trace(err)
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 256)
		if dec.value.startTS == startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 258)
			return dec.value, true, nil
		}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 254)
	return mvccValue{}, false, nil
}

// Cleanup implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Cleanup(key []byte, startTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 259)
	mvcc.mu.Lock()
	defer func() {
		trace_util_0.Count(_mvcc_leveldb_00000, 262)
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	trace_util_0.Count(_mvcc_leveldb_00000, 260)
	batch := &leveldb.Batch{}
	err := rollbackKey(mvcc.db, batch, key, startTS)
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 263)
		return errors.Trace(err)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 261)
	return mvcc.db.Write(batch, nil)
}

// ScanLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	trace_util_0.Count(_mvcc_leveldb_00000, 264)
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 267)
		return nil, errors.Trace(err)
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 265)
	var locks []*kvrpcpb.LockInfo
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 268)
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 272)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 269)
		if ok && dec.lock.startTS <= maxTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 273)
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: dec.lock.primary,
				LockVersion: dec.lock.startTS,
				Key:         currKey,
			})
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 270)
		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 274)
			return nil, errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 271)
		currKey = skip.currKey
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 266)
	return locks, nil
}

// ResolveLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 275)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 278)
		return errors.Trace(err)
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 276)
	batch := &leveldb.Batch{}
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 279)
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 283)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 280)
		if ok && dec.lock.startTS == startTS {
			trace_util_0.Count(_mvcc_leveldb_00000, 284)
			if commitTS > 0 {
				trace_util_0.Count(_mvcc_leveldb_00000, 286)
				err = commitLock(batch, dec.lock, currKey, startTS, commitTS)
			} else {
				trace_util_0.Count(_mvcc_leveldb_00000, 287)
				{
					err = rollbackLock(batch, dec.lock, currKey, startTS)
				}
			}
			trace_util_0.Count(_mvcc_leveldb_00000, 285)
			if err != nil {
				trace_util_0.Count(_mvcc_leveldb_00000, 288)
				return errors.Trace(err)
			}
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 281)
		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 289)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 282)
		currKey = skip.currKey
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 277)
	return mvcc.db.Write(batch, nil)
}

// BatchResolveLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 290)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 293)
		return errors.Trace(err)
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 291)
	batch := &leveldb.Batch{}
	for iter.Valid() {
		trace_util_0.Count(_mvcc_leveldb_00000, 294)
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 298)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 295)
		if ok {
			trace_util_0.Count(_mvcc_leveldb_00000, 299)
			if commitTS, ok := txnInfos[dec.lock.startTS]; ok {
				trace_util_0.Count(_mvcc_leveldb_00000, 300)
				if commitTS > 0 {
					trace_util_0.Count(_mvcc_leveldb_00000, 302)
					err = commitLock(batch, dec.lock, currKey, dec.lock.startTS, commitTS)
				} else {
					trace_util_0.Count(_mvcc_leveldb_00000, 303)
					{
						err = rollbackLock(batch, dec.lock, currKey, dec.lock.startTS)
					}
				}
				trace_util_0.Count(_mvcc_leveldb_00000, 301)
				if err != nil {
					trace_util_0.Count(_mvcc_leveldb_00000, 304)
					return errors.Trace(err)
				}
			}
		}

		trace_util_0.Count(_mvcc_leveldb_00000, 296)
		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 305)
			return errors.Trace(err)
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 297)
		currKey = skip.currKey
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 292)
	return mvcc.db.Write(batch, nil)
}

// DeleteRange implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) DeleteRange(startKey, endKey []byte) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 306)
	return mvcc.doRawDeleteRange(codec.EncodeBytes(nil, startKey), codec.EncodeBytes(nil, endKey))
}

// Close calls leveldb's Close to free resources.
func (mvcc *MVCCLevelDB) Close() error {
	trace_util_0.Count(_mvcc_leveldb_00000, 307)
	return mvcc.db.Close()
}

// RawPut implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawPut(key, value []byte) {
	trace_util_0.Count(_mvcc_leveldb_00000, 308)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	if value == nil {
		trace_util_0.Count(_mvcc_leveldb_00000, 310)
		value = []byte{}
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 309)
	terror.Log(mvcc.db.Put(key, value, nil))
}

// RawBatchPut implements the RawKV interface
func (mvcc *MVCCLevelDB) RawBatchPut(keys, values [][]byte) {
	trace_util_0.Count(_mvcc_leveldb_00000, 311)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}
	for i, key := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 313)
		value := values[i]
		if value == nil {
			trace_util_0.Count(_mvcc_leveldb_00000, 315)
			value = []byte{}
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 314)
		batch.Put(key, value)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 312)
	terror.Log(mvcc.db.Write(batch, nil))
}

// RawGet implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawGet(key []byte) []byte {
	trace_util_0.Count(_mvcc_leveldb_00000, 316)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	ret, err := mvcc.db.Get(key, nil)
	terror.Log(err)
	return ret
}

// RawBatchGet implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawBatchGet(keys [][]byte) [][]byte {
	trace_util_0.Count(_mvcc_leveldb_00000, 317)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	values := make([][]byte, 0, len(keys))
	for _, key := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 319)
		value, err := mvcc.db.Get(key, nil)
		terror.Log(err)
		values = append(values, value)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 318)
	return values
}

// RawDelete implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawDelete(key []byte) {
	trace_util_0.Count(_mvcc_leveldb_00000, 320)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	terror.Log(mvcc.db.Delete(key, nil))
}

// RawBatchDelete implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawBatchDelete(keys [][]byte) {
	trace_util_0.Count(_mvcc_leveldb_00000, 321)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}
	for _, key := range keys {
		trace_util_0.Count(_mvcc_leveldb_00000, 323)
		batch.Delete(key)
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 322)
	terror.Log(mvcc.db.Write(batch, nil))
}

// RawScan implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawScan(startKey, endKey []byte, limit int) []Pair {
	trace_util_0.Count(_mvcc_leveldb_00000, 324)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
	}, nil)

	var pairs []Pair
	for iter.Next() && len(pairs) < limit {
		trace_util_0.Count(_mvcc_leveldb_00000, 326)
		key := iter.Key()
		value := iter.Value()
		err := iter.Error()
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			trace_util_0.Count(_mvcc_leveldb_00000, 328)
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 327)
		pairs = append(pairs, Pair{
			Key:   append([]byte{}, key...),
			Value: append([]byte{}, value...),
			Err:   err,
		})
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 325)
	return pairs
}

// RawReverseScan implements the RawKV interface.
// Scan the range of [endKey, startKey)
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (mvcc *MVCCLevelDB) RawReverseScan(startKey, endKey []byte, limit int) []Pair {
	trace_util_0.Count(_mvcc_leveldb_00000, 329)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter := mvcc.db.NewIterator(&util.Range{
		Limit: startKey,
	}, nil)

	success := iter.Last()

	var pairs []Pair
	for success && len(pairs) < limit {
		trace_util_0.Count(_mvcc_leveldb_00000, 331)
		key := iter.Key()
		value := iter.Value()
		err := iter.Error()
		if bytes.Compare(key, endKey) < 0 {
			trace_util_0.Count(_mvcc_leveldb_00000, 333)
			break
		}
		trace_util_0.Count(_mvcc_leveldb_00000, 332)
		pairs = append(pairs, Pair{
			Key:   append([]byte{}, key...),
			Value: append([]byte{}, value...),
			Err:   err,
		})
		success = iter.Prev()
	}
	trace_util_0.Count(_mvcc_leveldb_00000, 330)
	return pairs
}

// RawDeleteRange implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawDeleteRange(startKey, endKey []byte) {
	trace_util_0.Count(_mvcc_leveldb_00000, 334)
	terror.Log(mvcc.doRawDeleteRange(startKey, endKey))
}

// doRawDeleteRange deletes all keys in a range and return the error if any.
func (mvcc *MVCCLevelDB) doRawDeleteRange(startKey, endKey []byte) error {
	trace_util_0.Count(_mvcc_leveldb_00000, 335)
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}

	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
		Limit: endKey,
	}, nil)
	for iter.Next() {
		trace_util_0.Count(_mvcc_leveldb_00000, 337)
		batch.Delete(iter.Key())
	}

	trace_util_0.Count(_mvcc_leveldb_00000, 336)
	return mvcc.db.Write(batch, nil)
}

var _mvcc_leveldb_00000 = "store/mockstore/mocktikv/mvcc_leveldb.go"
