// Copyright 2019 PingCAP, Inc.
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

package deadlock

import (
	"fmt"
	"github.com/pingcap/tidb/trace_util_0"
	"sync"
)

// Detector detects deadlock.
type Detector struct {
	waitForMap map[uint64]*txnList
	lock       sync.Mutex
}

type txnList struct {
	txns []txnKeyHashPair
}

type txnKeyHashPair struct {
	txn     uint64
	keyHash uint64
}

// NewDetector creates a new Detector.
func NewDetector() *Detector {
	trace_util_0.Count(_deadlock_00000, 0)
	return &Detector{
		waitForMap: map[uint64]*txnList{},
	}
}

// ErrDeadlock is returned when deadlock is detected.
type ErrDeadlock struct {
	KeyHash uint64
}

func (e *ErrDeadlock) Error() string {
	trace_util_0.Count(_deadlock_00000, 1)
	return fmt.Sprintf("deadlock(%d)", e.KeyHash)
}

// Detect detects deadlock for the sourceTxn on a locked key.
func (d *Detector) Detect(sourceTxn, waitForTxn, keyHash uint64) *ErrDeadlock {
	trace_util_0.Count(_deadlock_00000, 2)
	d.lock.Lock()
	err := d.doDetect(sourceTxn, waitForTxn)
	if err == nil {
		trace_util_0.Count(_deadlock_00000, 4)
		d.register(sourceTxn, waitForTxn, keyHash)
	}
	trace_util_0.Count(_deadlock_00000, 3)
	d.lock.Unlock()
	return err
}

func (d *Detector) doDetect(sourceTxn, waitForTxn uint64) *ErrDeadlock {
	trace_util_0.Count(_deadlock_00000, 5)
	list := d.waitForMap[waitForTxn]
	if list == nil {
		trace_util_0.Count(_deadlock_00000, 8)
		return nil
	}
	trace_util_0.Count(_deadlock_00000, 6)
	for _, nextTarget := range list.txns {
		trace_util_0.Count(_deadlock_00000, 9)
		if nextTarget.txn == sourceTxn {
			trace_util_0.Count(_deadlock_00000, 11)
			return &ErrDeadlock{KeyHash: nextTarget.keyHash}
		}
		trace_util_0.Count(_deadlock_00000, 10)
		if err := d.doDetect(sourceTxn, nextTarget.txn); err != nil {
			trace_util_0.Count(_deadlock_00000, 12)
			return err
		}
	}
	trace_util_0.Count(_deadlock_00000, 7)
	return nil
}

func (d *Detector) register(sourceTxn, waitForTxn, keyHash uint64) {
	trace_util_0.Count(_deadlock_00000, 13)
	list := d.waitForMap[sourceTxn]
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	if list == nil {
		trace_util_0.Count(_deadlock_00000, 16)
		d.waitForMap[sourceTxn] = &txnList{txns: []txnKeyHashPair{pair}}
		return
	}
	trace_util_0.Count(_deadlock_00000, 14)
	for _, tar := range list.txns {
		trace_util_0.Count(_deadlock_00000, 17)
		if tar.txn == waitForTxn && tar.keyHash == keyHash {
			trace_util_0.Count(_deadlock_00000, 18)
			return
		}
	}
	trace_util_0.Count(_deadlock_00000, 15)
	list.txns = append(list.txns, pair)
}

// CleanUp removes the wait for entry for the transaction.
func (d *Detector) CleanUp(txn uint64) {
	trace_util_0.Count(_deadlock_00000, 19)
	d.lock.Lock()
	delete(d.waitForMap, txn)
	d.lock.Unlock()
}

// CleanUpWaitFor removes a key in the wait for entry for the transaction.
func (d *Detector) CleanUpWaitFor(txn, waitForTxn, keyHash uint64) {
	trace_util_0.Count(_deadlock_00000, 20)
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	d.lock.Lock()
	l := d.waitForMap[txn]
	if l != nil {
		trace_util_0.Count(_deadlock_00000, 22)
		for i, tar := range l.txns {
			trace_util_0.Count(_deadlock_00000, 24)
			if tar == pair {
				trace_util_0.Count(_deadlock_00000, 25)
				l.txns = append(l.txns[:i], l.txns[i+1:]...)
				break
			}
		}
		trace_util_0.Count(_deadlock_00000, 23)
		if len(l.txns) == 0 {
			trace_util_0.Count(_deadlock_00000, 26)
			delete(d.waitForMap, txn)
		}
	}
	trace_util_0.Count(_deadlock_00000, 21)
	d.lock.Unlock()

}

// Expire removes entries with TS smaller than minTS.
func (d *Detector) Expire(minTS uint64) {
	trace_util_0.Count(_deadlock_00000, 27)
	d.lock.Lock()
	for ts := range d.waitForMap {
		trace_util_0.Count(_deadlock_00000, 29)
		if ts < minTS {
			trace_util_0.Count(_deadlock_00000, 30)
			delete(d.waitForMap, ts)
		}
	}
	trace_util_0.Count(_deadlock_00000, 28)
	d.lock.Unlock()
}

var _deadlock_00000 = "util/deadlock/deadlock.go"
