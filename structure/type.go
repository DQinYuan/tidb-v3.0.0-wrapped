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

package structure

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/codec"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag byte

const (
	// StringMeta is the flag for string meta.
	StringMeta TypeFlag = 'S'
	// StringData is the flag for string data.
	StringData TypeFlag = 's'
	// HashMeta is the flag for hash meta.
	HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func (t *TxStructure) encodeStringDataKey(key []byte) kv.Key {
	trace_util_0.Count(_type_00000, 0)
	// for codec Encode, we may add extra bytes data, so here and following encode
	// we will use extra length like 4 for a little optimization.
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(StringData))
}

func (t *TxStructure) encodeHashMetaKey(key []byte) kv.Key {
	trace_util_0.Count(_type_00000, 1)
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashMeta))
}

func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) kv.Key {
	trace_util_0.Count(_type_00000, 2)
	ek := make([]byte, 0, len(t.prefix)+len(key)+len(field)+30)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(HashData))
	return codec.EncodeBytes(ek, field)
}

// EncodeHashDataKey exports for tests.
func (t *TxStructure) EncodeHashDataKey(key []byte, field []byte) kv.Key {
	trace_util_0.Count(_type_00000, 3)
	return t.encodeHashDataKey(key, field)
}

func (t *TxStructure) decodeHashDataKey(ek kv.Key) ([]byte, []byte, error) {
	trace_util_0.Count(_type_00000, 4)
	var (
		key   []byte
		field []byte
		err   error
		tp    uint64
	)

	if !bytes.HasPrefix(ek, t.prefix) {
		trace_util_0.Count(_type_00000, 8)
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}

	trace_util_0.Count(_type_00000, 5)
	ek = ek[len(t.prefix):]

	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		trace_util_0.Count(_type_00000, 9)
		return nil, nil, errors.Trace(err)
	}

	trace_util_0.Count(_type_00000, 6)
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		trace_util_0.Count(_type_00000, 10)
		return nil, nil, errors.Trace(err)
	} else {
		trace_util_0.Count(_type_00000, 11)
		if TypeFlag(tp) != HashData {
			trace_util_0.Count(_type_00000, 12)
			return nil, nil, errInvalidHashKeyFlag.GenWithStack("invalid encoded hash data key flag %c", byte(tp))
		}
	}

	trace_util_0.Count(_type_00000, 7)
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

func (t *TxStructure) hashDataKeyPrefix(key []byte) kv.Key {
	trace_util_0.Count(_type_00000, 13)
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashData))
}

func (t *TxStructure) encodeListMetaKey(key []byte) kv.Key {
	trace_util_0.Count(_type_00000, 14)
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(ListMeta))
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) kv.Key {
	trace_util_0.Count(_type_00000, 15)
	ek := make([]byte, 0, len(t.prefix)+len(key)+36)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(ListData))
	return codec.EncodeInt(ek, index)
}

var _type_00000 = "structure/type.go"
