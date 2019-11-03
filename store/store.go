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

package store

import (
	"context"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var stores = make(map[string]kv.Driver)

// Register registers a kv storage with unique name and its associated Driver.
func Register(name string, driver kv.Driver) error {
	trace_util_0.Count(_store_00000, 0)
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		trace_util_0.Count(_store_00000, 2)
		return errors.Errorf("%s is already registered", name)
	}

	trace_util_0.Count(_store_00000, 1)
	stores[name] = driver
	return nil
}

// New creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// session.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func New(path string) (kv.Storage, error) {
	trace_util_0.Count(_store_00000, 3)
	return newStoreWithRetry(path, util.DefaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	trace_util_0.Count(_store_00000, 4)
	storeURL, err := url.Parse(path)
	if err != nil {
		trace_util_0.Count(_store_00000, 9)
		return nil, err
	}

	trace_util_0.Count(_store_00000, 5)
	name := strings.ToLower(storeURL.Scheme)
	d, ok := stores[name]
	if !ok {
		trace_util_0.Count(_store_00000, 10)
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	trace_util_0.Count(_store_00000, 6)
	var s kv.Storage
	err = util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		trace_util_0.Count(_store_00000, 11)
		logutil.Logger(context.Background()).Info("new store", zap.String("path", path))
		s, err = d.Open(path)
		return kv.IsTxnRetryableError(err), err
	})

	trace_util_0.Count(_store_00000, 7)
	if err == nil {
		trace_util_0.Count(_store_00000, 12)
		logutil.Logger(context.Background()).Info("new store with retry success")
	} else {
		trace_util_0.Count(_store_00000, 13)
		{
			logutil.Logger(context.Background()).Warn("new store with retry failed", zap.Error(err))
		}
	}
	trace_util_0.Count(_store_00000, 8)
	return s, errors.Trace(err)
}

var _store_00000 = "store/store.go"
