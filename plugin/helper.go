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

package plugin

import (
	"github.com/pingcap/tidb/trace_util_0"
	"strings"
	"unsafe"
)

// DeclareAuditManifest declares manifest as AuditManifest.
func DeclareAuditManifest(m *Manifest) *AuditManifest {
	trace_util_0.Count(_helper_00000, 0)
	return (*AuditManifest)(unsafe.Pointer(m))
}

// DeclareAuthenticationManifest declares manifest as AuthenticationManifest.
func DeclareAuthenticationManifest(m *Manifest) *AuthenticationManifest {
	trace_util_0.Count(_helper_00000, 1)
	return (*AuthenticationManifest)(unsafe.Pointer(m))
}

// DeclareSchemaManifest declares manifest as SchemaManifest.
func DeclareSchemaManifest(m *Manifest) *SchemaManifest {
	trace_util_0.Count(_helper_00000, 2)
	return (*SchemaManifest)(unsafe.Pointer(m))
}

// DeclareDaemonManifest declares manifest as DaemonManifest.
func DeclareDaemonManifest(m *Manifest) *DaemonManifest {
	trace_util_0.Count(_helper_00000, 3)
	return (*DaemonManifest)(unsafe.Pointer(m))
}

// ID present plugin identity.
type ID string

// Decode decodes a plugin id into name, version parts.
func (n ID) Decode() (name string, version string, err error) {
	trace_util_0.Count(_helper_00000, 4)
	splits := strings.Split(string(n), "-")
	if len(splits) != 2 {
		trace_util_0.Count(_helper_00000, 6)
		err = errInvalidPluginID.GenWithStackByArgs(string(n))
		return
	}
	trace_util_0.Count(_helper_00000, 5)
	name = splits[0]
	version = splits[1]
	return
}

var _helper_00000 = "plugin/helper.go"
