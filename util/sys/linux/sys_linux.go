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
// +build linux

package linux

import (
	"syscall"
	"github.com/pingcap/tidb/trace_util_0"

	// OSVersion returns version info of operation system.
	// e.g. Linux 4.15.0-45-generic.x86_64
)

func OSVersion() (osVersion string, err error) {
	trace_util_0.Count(_sys_linux_00000, 0)
	var un syscall.Utsname
	err = syscall.Uname(&un)
	if err != nil {
		trace_util_0.Count(_sys_linux_00000, 3)
		return
	}
	trace_util_0.Count(_sys_linux_00000, 1)
	charsToString := func(ca []int8) string {
		trace_util_0.Count(_sys_linux_00000, 4)
		s := make([]byte, len(ca))
		var lens int
		for ; lens < len(ca); lens++ {
			trace_util_0.Count(_sys_linux_00000, 6)
			if ca[lens] == 0 {
				trace_util_0.Count(_sys_linux_00000, 8)
				break
			}
			trace_util_0.Count(_sys_linux_00000, 7)
			s[lens] = uint8(ca[lens])
		}
		trace_util_0.Count(_sys_linux_00000, 5)
		return string(s[0:lens])
	}
	trace_util_0.Count(_sys_linux_00000, 2)
	osVersion = charsToString(un.Sysname[:]) + " " + charsToString(un.Release[:]) + "." + charsToString(un.Machine[:])
	return
}

var _sys_linux_00000 = "util/sys/linux/sys_linux.go"
