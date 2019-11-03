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

package logutil

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/trace_util_0"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	defaultLogLevel  = log.InfoLevel
	// DefaultSlowThreshold is the default slow log threshold in millisecond.
	DefaultSlowThreshold = 300
	// DefaultQueryLogMaxLen is the default max length of the query in the log.
	DefaultQueryLogMaxLen = 2048
)

// EmptyFileLogConfig is an empty FileLogConfig.
var EmptyFileLogConfig = FileLogConfig{}

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	zaplog.FileLogConfig
}

// NewFileLogConfig creates a FileLogConfig.
func NewFileLogConfig(rotate bool, maxSize uint) FileLogConfig {
	trace_util_0.Count(_log_00000, 0)
	return FileLogConfig{FileLogConfig: zaplog.FileLogConfig{
		LogRotate: rotate,
		MaxSize:   int(maxSize),
	},
	}
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	zaplog.Config

	// SlowQueryFile filename, default to File log config on empty.
	SlowQueryFile string
}

// NewLogConfig creates a LogConfig.
func NewLogConfig(level, format, slowQueryFile string, fileCfg FileLogConfig, disableTimestamp bool) *LogConfig {
	trace_util_0.Count(_log_00000, 1)
	return &LogConfig{
		Config: zaplog.Config{
			Level:            level,
			Format:           format,
			DisableTimestamp: disableTimestamp,
			File:             fileCfg.FileLogConfig,
		},
		SlowQueryFile: slowQueryFile,
	}
}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	trace_util_0.Count(_log_00000, 2)
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	trace_util_0.Count(_log_00000, 3)
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		trace_util_0.Count(_log_00000, 5)
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			trace_util_0.Count(_log_00000, 6)
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	trace_util_0.Count(_log_00000, 4)
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	trace_util_0.Count(_log_00000, 7)
	return log.AllLevels
}

func stringToLogLevel(level string) log.Level {
	trace_util_0.Count(_log_00000, 8)
	switch strings.ToLower(level) {
	case "fatal":
		trace_util_0.Count(_log_00000, 10)
		return log.FatalLevel
	case "error":
		trace_util_0.Count(_log_00000, 11)
		return log.ErrorLevel
	case "warn", "warning":
		trace_util_0.Count(_log_00000, 12)
		return log.WarnLevel
	case "debug":
		trace_util_0.Count(_log_00000, 13)
		return log.DebugLevel
	case "info":
		trace_util_0.Count(_log_00000, 14)
		return log.InfoLevel
	}
	trace_util_0.Count(_log_00000, 9)
	return defaultLogLevel
}

// logTypeToColor converts the Level to a color string.
func logTypeToColor(level log.Level) string {
	trace_util_0.Count(_log_00000, 15)
	switch level {
	case log.DebugLevel:
		trace_util_0.Count(_log_00000, 17)
		return "[0;37"
	case log.InfoLevel:
		trace_util_0.Count(_log_00000, 18)
		return "[0;36"
	case log.WarnLevel:
		trace_util_0.Count(_log_00000, 19)
		return "[0;33"
	case log.ErrorLevel:
		trace_util_0.Count(_log_00000, 20)
		return "[0;31"
	case log.FatalLevel:
		trace_util_0.Count(_log_00000, 21)
		return "[0;31"
	case log.PanicLevel:
		trace_util_0.Count(_log_00000, 22)
		return "[0;31"
	}

	trace_util_0.Count(_log_00000, 16)
	return "[0;37"
}

// textFormatter is for compatibility with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
	EnableColors     bool
	EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	trace_util_0.Count(_log_00000, 23)
	var b *bytes.Buffer
	if entry.Buffer != nil {
		trace_util_0.Count(_log_00000, 30)
		b = entry.Buffer
	} else {
		trace_util_0.Count(_log_00000, 31)
		{
			b = &bytes.Buffer{}
		}
	}

	trace_util_0.Count(_log_00000, 24)
	if f.EnableColors {
		trace_util_0.Count(_log_00000, 32)
		colorStr := logTypeToColor(entry.Level)
		fmt.Fprintf(b, "\033%sm ", colorStr)
	}

	trace_util_0.Count(_log_00000, 25)
	if !f.DisableTimestamp {
		trace_util_0.Count(_log_00000, 33)
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	trace_util_0.Count(_log_00000, 26)
	if file, ok := entry.Data["file"]; ok {
		trace_util_0.Count(_log_00000, 34)
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	trace_util_0.Count(_log_00000, 27)
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		trace_util_0.Count(_log_00000, 35)
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			trace_util_0.Count(_log_00000, 37)
			if k != "file" && k != "line" {
				trace_util_0.Count(_log_00000, 38)
				keys = append(keys, k)
			}
		}
		trace_util_0.Count(_log_00000, 36)
		sort.Strings(keys)
		for _, k := range keys {
			trace_util_0.Count(_log_00000, 39)
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		trace_util_0.Count(_log_00000, 40)
		{
			for k, v := range entry.Data {
				trace_util_0.Count(_log_00000, 41)
				if k != "file" && k != "line" {
					trace_util_0.Count(_log_00000, 42)
					fmt.Fprintf(b, " %v=%v", k, v)
				}
			}
		}
	}

	trace_util_0.Count(_log_00000, 28)
	b.WriteByte('\n')

	if f.EnableColors {
		trace_util_0.Count(_log_00000, 43)
		b.WriteString("\033[0m")
	}
	trace_util_0.Count(_log_00000, 29)
	return b.Bytes(), nil
}

const (
	// SlowLogTimeFormat is the time format for slow log.
	SlowLogTimeFormat = time.RFC3339Nano
	// OldSlowLogTimeFormat is the first version of the the time format for slow log, This is use for compatibility.
	OldSlowLogTimeFormat = "2006-01-02-15:04:05.999999999 -0700"
)

type slowLogFormatter struct{}

func (f *slowLogFormatter) Format(entry *log.Entry) ([]byte, error) {
	trace_util_0.Count(_log_00000, 44)
	var b *bytes.Buffer
	if entry.Buffer != nil {
		trace_util_0.Count(_log_00000, 46)
		b = entry.Buffer
	} else {
		trace_util_0.Count(_log_00000, 47)
		{
			b = &bytes.Buffer{}
		}
	}

	trace_util_0.Count(_log_00000, 45)
	fmt.Fprintf(b, "# Time: %s\n", entry.Time.Format(SlowLogTimeFormat))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b.Bytes(), nil
}

func stringToLogFormatter(format string, disableTimestamp bool) log.Formatter {
	trace_util_0.Count(_log_00000, 48)
	switch strings.ToLower(format) {
	case "text":
		trace_util_0.Count(_log_00000, 49)
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
		}
	case "json":
		trace_util_0.Count(_log_00000, 50)
		return &log.JSONFormatter{
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "console":
		trace_util_0.Count(_log_00000, 51)
		return &log.TextFormatter{
			FullTimestamp:    true,
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "highlight":
		trace_util_0.Count(_log_00000, 52)
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
			EnableColors:     true,
		}
	default:
		trace_util_0.Count(_log_00000, 53)
		return &textFormatter{}
	}
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *zaplog.FileLogConfig, logger *log.Logger) error {
	trace_util_0.Count(_log_00000, 54)
	if st, err := os.Stat(cfg.Filename); err == nil {
		trace_util_0.Count(_log_00000, 58)
		if st.IsDir() {
			trace_util_0.Count(_log_00000, 59)
			return errors.New("can't use directory as log file name")
		}
	}
	trace_util_0.Count(_log_00000, 55)
	if cfg.MaxSize == 0 {
		trace_util_0.Count(_log_00000, 60)
		cfg.MaxSize = DefaultLogMaxSize
	}

	// use lumberjack to logrotate
	trace_util_0.Count(_log_00000, 56)
	output := &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    int(cfg.MaxSize),
		MaxBackups: int(cfg.MaxBackups),
		MaxAge:     int(cfg.MaxDays),
		LocalTime:  true,
	}

	if logger == nil {
		trace_util_0.Count(_log_00000, 61)
		log.SetOutput(output)
	} else {
		trace_util_0.Count(_log_00000, 62)
		{
			logger.Out = output
		}
	}
	trace_util_0.Count(_log_00000, 57)
	return nil
}

// SlowQueryLogger is used to log slow query, InitLogger will modify it according to config file.
var SlowQueryLogger = log.StandardLogger()

// SlowQueryZapLogger is used to log slow query, InitZapLogger will modify it according to config file.
var SlowQueryZapLogger = zaplog.L()

// InitLogger initializes PD's logger.
func InitLogger(cfg *LogConfig) error {
	trace_util_0.Count(_log_00000, 63)
	log.SetLevel(stringToLogLevel(cfg.Level))
	log.AddHook(&contextHook{})

	if cfg.Format == "" {
		trace_util_0.Count(_log_00000, 67)
		cfg.Format = DefaultLogFormat
	}
	trace_util_0.Count(_log_00000, 64)
	formatter := stringToLogFormatter(cfg.Format, cfg.DisableTimestamp)
	log.SetFormatter(formatter)

	if len(cfg.File.Filename) != 0 {
		trace_util_0.Count(_log_00000, 68)
		if err := initFileLog(&cfg.File, nil); err != nil {
			trace_util_0.Count(_log_00000, 69)
			return errors.Trace(err)
		}
	}

	trace_util_0.Count(_log_00000, 65)
	if len(cfg.SlowQueryFile) != 0 {
		trace_util_0.Count(_log_00000, 70)
		SlowQueryLogger = log.New()
		tmp := cfg.File
		tmp.Filename = cfg.SlowQueryFile
		if err := initFileLog(&tmp, SlowQueryLogger); err != nil {
			trace_util_0.Count(_log_00000, 72)
			return errors.Trace(err)
		}
		trace_util_0.Count(_log_00000, 71)
		SlowQueryLogger.Formatter = &slowLogFormatter{}
	}

	trace_util_0.Count(_log_00000, 66)
	return nil
}

// InitZapLogger initializes a zap logger with cfg.
func InitZapLogger(cfg *LogConfig) error {
	trace_util_0.Count(_log_00000, 73)
	gl, props, err := zaplog.InitLogger(&cfg.Config)
	if err != nil {
		trace_util_0.Count(_log_00000, 76)
		return errors.Trace(err)
	}
	trace_util_0.Count(_log_00000, 74)
	zaplog.ReplaceGlobals(gl, props)

	if len(cfg.SlowQueryFile) != 0 {
		trace_util_0.Count(_log_00000, 77)
		sqfCfg := zaplog.FileLogConfig{
			LogRotate: cfg.File.LogRotate,
			MaxSize:   cfg.File.MaxSize,
			Filename:  cfg.SlowQueryFile,
		}
		sqCfg := &zaplog.Config{
			Level:            cfg.Level,
			Format:           cfg.Format,
			DisableTimestamp: cfg.DisableTimestamp,
			File:             sqfCfg,
		}
		sqLogger, _, err := zaplog.InitLogger(sqCfg)
		if err != nil {
			trace_util_0.Count(_log_00000, 79)
			return errors.Trace(err)
		}
		trace_util_0.Count(_log_00000, 78)
		SlowQueryZapLogger = sqLogger
	} else {
		trace_util_0.Count(_log_00000, 80)
		{
			SlowQueryZapLogger = gl
		}
	}

	trace_util_0.Count(_log_00000, 75)
	return nil
}

// SetLevel sets the zap logger's level.
func SetLevel(level string) error {
	trace_util_0.Count(_log_00000, 81)
	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(level)); err != nil {
		trace_util_0.Count(_log_00000, 83)
		return errors.Trace(err)
	}
	trace_util_0.Count(_log_00000, 82)
	zaplog.SetLevel(l.Level())
	return nil
}

type ctxKeyType int

const ctxLogKey ctxKeyType = iota

// Logger gets a contextual logger from current context.
// contextual logger will output common fields from context.
func Logger(ctx context.Context) *zap.Logger {
	trace_util_0.Count(_log_00000, 84)
	if ctxlogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		trace_util_0.Count(_log_00000, 86)
		return ctxlogger
	}
	trace_util_0.Count(_log_00000, 85)
	return zaplog.L()
}

// WithConnID attaches connId to context.
func WithConnID(ctx context.Context, connID uint32) context.Context {
	trace_util_0.Count(_log_00000, 87)
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		trace_util_0.Count(_log_00000, 89)
		logger = ctxLogger
	} else {
		trace_util_0.Count(_log_00000, 90)
		{
			logger = zaplog.L()
		}
	}
	trace_util_0.Count(_log_00000, 88)
	return context.WithValue(ctx, ctxLogKey, logger.With(zap.Uint32("conn", connID)))
}

// WithKeyValue attaches key/value to context.
func WithKeyValue(ctx context.Context, key, value string) context.Context {
	trace_util_0.Count(_log_00000, 91)
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		trace_util_0.Count(_log_00000, 93)
		logger = ctxLogger
	} else {
		trace_util_0.Count(_log_00000, 94)
		{
			logger = zaplog.L()
		}
	}
	trace_util_0.Count(_log_00000, 92)
	return context.WithValue(ctx, ctxLogKey, logger.With(zap.String(key, value)))
}

var _log_00000 = "util/logutil/log.go"
