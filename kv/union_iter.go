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

package kv

import (
	"context"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// UnionIter is the iterator on an UnionStore.
type UnionIter struct {
	dirtyIt    Iterator
	snapshotIt Iterator

	dirtyValid    bool
	snapshotValid bool

	curIsDirty bool
	isValid    bool
	reverse    bool
}

// NewUnionIter returns a union iterator for BufferStore.
func NewUnionIter(dirtyIt Iterator, snapshotIt Iterator, reverse bool) (*UnionIter, error) {
	trace_util_0.Count(_union_iter_00000, 0)
	it := &UnionIter{
		dirtyIt:       dirtyIt,
		snapshotIt:    snapshotIt,
		dirtyValid:    dirtyIt.Valid(),
		snapshotValid: snapshotIt.Valid(),
		reverse:       reverse,
	}
	err := it.updateCur()
	if err != nil {
		trace_util_0.Count(_union_iter_00000, 2)
		return nil, err
	}
	trace_util_0.Count(_union_iter_00000, 1)
	return it, nil
}

// dirtyNext makes iter.dirtyIt go and update valid status.
func (iter *UnionIter) dirtyNext() error {
	trace_util_0.Count(_union_iter_00000, 3)
	err := iter.dirtyIt.Next()
	iter.dirtyValid = iter.dirtyIt.Valid()
	return err
}

// snapshotNext makes iter.snapshotIt go and update valid status.
func (iter *UnionIter) snapshotNext() error {
	trace_util_0.Count(_union_iter_00000, 4)
	err := iter.snapshotIt.Next()
	iter.snapshotValid = iter.snapshotIt.Valid()
	return err
}

func (iter *UnionIter) updateCur() error {
	trace_util_0.Count(_union_iter_00000, 5)
	iter.isValid = true
	for {
		trace_util_0.Count(_union_iter_00000, 7)
		if !iter.dirtyValid && !iter.snapshotValid {
			trace_util_0.Count(_union_iter_00000, 11)
			iter.isValid = false
			break
		}

		trace_util_0.Count(_union_iter_00000, 8)
		if !iter.dirtyValid {
			trace_util_0.Count(_union_iter_00000, 12)
			iter.curIsDirty = false
			break
		}

		trace_util_0.Count(_union_iter_00000, 9)
		if !iter.snapshotValid {
			trace_util_0.Count(_union_iter_00000, 13)
			iter.curIsDirty = true
			// if delete it
			if len(iter.dirtyIt.Value()) == 0 {
				trace_util_0.Count(_union_iter_00000, 15)
				if err := iter.dirtyNext(); err != nil {
					trace_util_0.Count(_union_iter_00000, 17)
					return err
				}
				trace_util_0.Count(_union_iter_00000, 16)
				continue
			}
			trace_util_0.Count(_union_iter_00000, 14)
			break
		}

		// both valid
		trace_util_0.Count(_union_iter_00000, 10)
		if iter.snapshotValid && iter.dirtyValid {
			trace_util_0.Count(_union_iter_00000, 18)
			snapshotKey := iter.snapshotIt.Key()
			dirtyKey := iter.dirtyIt.Key()
			cmp := dirtyKey.Cmp(snapshotKey)
			if iter.reverse {
				trace_util_0.Count(_union_iter_00000, 20)
				cmp = -cmp
			}
			// if equal, means both have value
			trace_util_0.Count(_union_iter_00000, 19)
			if cmp == 0 {
				trace_util_0.Count(_union_iter_00000, 21)
				if len(iter.dirtyIt.Value()) == 0 {
					trace_util_0.Count(_union_iter_00000, 24)
					// snapshot has a record, but txn says we have deleted it
					// just go next
					if err := iter.dirtyNext(); err != nil {
						trace_util_0.Count(_union_iter_00000, 27)
						return err
					}
					trace_util_0.Count(_union_iter_00000, 25)
					if err := iter.snapshotNext(); err != nil {
						trace_util_0.Count(_union_iter_00000, 28)
						return err
					}
					trace_util_0.Count(_union_iter_00000, 26)
					continue
				}
				// both go next
				trace_util_0.Count(_union_iter_00000, 22)
				if err := iter.snapshotNext(); err != nil {
					trace_util_0.Count(_union_iter_00000, 29)
					return err
				}
				trace_util_0.Count(_union_iter_00000, 23)
				iter.curIsDirty = true
				break
			} else {
				trace_util_0.Count(_union_iter_00000, 30)
				if cmp > 0 {
					trace_util_0.Count(_union_iter_00000, 31)
					// record from snapshot comes first
					iter.curIsDirty = false
					break
				} else {
					trace_util_0.Count(_union_iter_00000, 32)
					{
						// record from dirty comes first
						if len(iter.dirtyIt.Value()) == 0 {
							trace_util_0.Count(_union_iter_00000, 34)
							logutil.Logger(context.Background()).Warn("delete a record not exists?",
								zap.Binary("key", iter.dirtyIt.Key()))
							// jump over this deletion
							if err := iter.dirtyNext(); err != nil {
								trace_util_0.Count(_union_iter_00000, 36)
								return err
							}
							trace_util_0.Count(_union_iter_00000, 35)
							continue
						}
						trace_util_0.Count(_union_iter_00000, 33)
						iter.curIsDirty = true
						break
					}
				}
			}
		}
	}
	trace_util_0.Count(_union_iter_00000, 6)
	return nil
}

// Next implements the Iterator Next interface.
func (iter *UnionIter) Next() error {
	trace_util_0.Count(_union_iter_00000, 37)
	var err error
	if !iter.curIsDirty {
		trace_util_0.Count(_union_iter_00000, 40)
		err = iter.snapshotNext()
	} else {
		trace_util_0.Count(_union_iter_00000, 41)
		{
			err = iter.dirtyNext()
		}
	}
	trace_util_0.Count(_union_iter_00000, 38)
	if err != nil {
		trace_util_0.Count(_union_iter_00000, 42)
		return err
	}
	trace_util_0.Count(_union_iter_00000, 39)
	err = iter.updateCur()
	return err
}

// Value implements the Iterator Value interface.
// Multi columns
func (iter *UnionIter) Value() []byte {
	trace_util_0.Count(_union_iter_00000, 43)
	if !iter.curIsDirty {
		trace_util_0.Count(_union_iter_00000, 45)
		return iter.snapshotIt.Value()
	}
	trace_util_0.Count(_union_iter_00000, 44)
	return iter.dirtyIt.Value()
}

// Key implements the Iterator Key interface.
func (iter *UnionIter) Key() Key {
	trace_util_0.Count(_union_iter_00000, 46)
	if !iter.curIsDirty {
		trace_util_0.Count(_union_iter_00000, 48)
		return iter.snapshotIt.Key()
	}
	trace_util_0.Count(_union_iter_00000, 47)
	return iter.dirtyIt.Key()
}

// Valid implements the Iterator Valid interface.
func (iter *UnionIter) Valid() bool {
	trace_util_0.Count(_union_iter_00000, 49)
	return iter.isValid
}

// Close implements the Iterator Close interface.
func (iter *UnionIter) Close() {
	trace_util_0.Count(_union_iter_00000, 50)
	if iter.snapshotIt != nil {
		trace_util_0.Count(_union_iter_00000, 52)
		iter.snapshotIt.Close()
		iter.snapshotIt = nil
	}
	trace_util_0.Count(_union_iter_00000, 51)
	if iter.dirtyIt != nil {
		trace_util_0.Count(_union_iter_00000, 53)
		iter.dirtyIt.Close()
		iter.dirtyIt = nil
	}
}

var _union_iter_00000 = "kv/union_iter.go"
