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
	"math"
	"github.com/pingcap/tidb/trace_util_0"

	// VersionProvider provides increasing IDs.
)

type VersionProvider interface {
	CurrentVersion() (Version, error)
}

// Version is the wrapper of KV's version.
type Version struct {
	Ver uint64
}

var (
	// MaxVersion is the maximum version, notice that it's not a valid version.
	MaxVersion = Version{Ver: math.MaxUint64}
	// MinVersion is the minimum version, it's not a valid version, too.
	MinVersion = Version{Ver: 0}
)

// NewVersion creates a new Version struct.
func NewVersion(v uint64) Version {
	trace_util_0.Count(_version_00000, 0)
	return Version{
		Ver: v,
	}
}

// Cmp returns the comparison result of two versions.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (v Version) Cmp(another Version) int {
	trace_util_0.Count(_version_00000, 1)
	if v.Ver > another.Ver {
		trace_util_0.Count(_version_00000, 3)
		return 1
	} else {
		trace_util_0.Count(_version_00000, 4)
		if v.Ver < another.Ver {
			trace_util_0.Count(_version_00000, 5)
			return -1
		}
	}
	trace_util_0.Count(_version_00000, 2)
	return 0
}

var _version_00000 = "kv/version.go"
