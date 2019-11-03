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
// +build linux darwin freebsd unix

package signal

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SetupSignalHandler setup signal handler for TiDB Server
func SetupSignalHandler(shudownFunc func(bool)) {
	trace_util_0.Count(_signal_posix_00000, 0)
	usrDefSignalChan := make(chan os.Signal, 1)

	signal.Notify(usrDefSignalChan, syscall.SIGUSR1)
	go func() {
		trace_util_0.Count(_signal_posix_00000, 2)
		buf := make([]byte, 1<<16)
		for {
			trace_util_0.Count(_signal_posix_00000, 3)
			sig := <-usrDefSignalChan
			if sig == syscall.SIGUSR1 {
				trace_util_0.Count(_signal_posix_00000, 4)
				stackLen := runtime.Stack(buf, true)
				log.Printf("\n=== Got signal [%s] to dump goroutine stack. ===\n%s\n=== Finished dumping goroutine stack. ===\n", sig, buf[:stackLen])
			}
		}
	}()

	trace_util_0.Count(_signal_posix_00000, 1)
	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		trace_util_0.Count(_signal_posix_00000, 5)
		sig := <-closeSignalChan
		logutil.Logger(context.Background()).Info("got signal to exit", zap.Stringer("signal", sig))
		shudownFunc(sig == syscall.SIGQUIT)
	}()
}

var _signal_posix_00000 = "util/signal/signal_posix.go"
