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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/pd/client"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/signal"
	"github.com/pingcap/tidb/util/systimemon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/struCoder/pidusage"
	"go.uber.org/zap"
)

// Flag Names
const (
	nmVersion          = "V"
	nmConfig           = "config"
	nmConfigCheck      = "config-check"
	nmConfigStrict     = "config-strict"
	nmStore            = "store"
	nmStorePath        = "path"
	nmHost             = "host"
	nmAdvertiseAddress = "advertise-address"
	nmPort             = "P"
	nmCors             = "cors"
	nmSocket           = "socket"
	nmEnableBinlog     = "enable-binlog"
	nmRunDDL           = "run-ddl"
	nmLogLevel         = "L"
	nmLogFile          = "log-file"
	nmLogSlowQuery     = "log-slow-query"
	nmReportStatus     = "report-status"
	nmStatusHost       = "status-host"
	nmStatusPort       = "status"
	nmMetricsAddr      = "metrics-addr"
	nmMetricsInterval  = "metrics-interval"
	nmDdlLease         = "lease"
	nmTokenLimit       = "token-limit"
	nmPluginDir        = "plugin-dir"
	nmPluginLoad       = "plugin-load"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")

	// Base
	store            = flag.String(nmStore, "mocktikv", "registered store name, [tikv, mocktikv]")
	storePath        = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "tidb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.String(nmPort, "4000", "tidb server port")
	cors             = flag.String(nmCors, "", "tidb server allow cors origin")
	socket           = flag.String(nmSocket, "", "The socket file to use for connection.")
	enableBinlog     = flagBoolean(nmEnableBinlog, false, "enable generate binlog")
	runDDL           = flagBoolean(nmRunDDL, true, "run ddl worker on this tidb-server")
	ddlLease         = flag.String(nmDdlLease, "45s", "schema lease duration, very dangerous to change only if you know what you do")
	tokenLimit       = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed sessions")
	pluginDir        = flag.String(nmPluginDir, "/data/deploy/plugin", "the folder that hold plugin")
	pluginLoad       = flag.String(nmPluginLoad, "", "wait load plugin name(separated by comma)")

	// Log
	logLevel     = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile      = flag.String(nmLogFile, "", "log file path")
	logSlowQuery = flag.String(nmLogSlowQuery, "", "slow query file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost      = flag.String(nmStatusHost, "0.0.0.0", "tidb server status host")
	statusPort      = flag.String(nmStatusPort, "10080", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Uint(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// PROXY Protocol
	proxyProtocolNetworks      = flag.String(nmProxyProtocolNetworks, "", "proxy protocol networks allowed IP or *, empty mean disable proxy protocol support")
	proxyProtocolHeaderTimeout = flag.Uint(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second.")
)

var (
	cfg      *config.Config
	storage  kv.Storage
	dom      *domain.Domain
	svr      *server.Server
	graceful bool
)

func main() {
	trace_util_0.Count(_main_00000, 0)
	flag.Parse()
	if *version {
		trace_util_0.Count(_main_00000, 5)
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}
	trace_util_0.Count(_main_00000, 1)
	registerStores()
	registerMetrics()
	configWarning := loadConfig()
	overrideConfig()
	if err := cfg.Valid(); err != nil {
		trace_util_0.Count(_main_00000, 6)
		fmt.Fprintln(os.Stderr, "invalid config", err)
		os.Exit(1)
	}
	trace_util_0.Count(_main_00000, 2)
	if *configCheck {
		trace_util_0.Count(_main_00000, 7)
		fmt.Println("config check successful")
		os.Exit(0)
	}
	trace_util_0.Count(_main_00000, 3)
	setGlobalVars()
	setupLog()
	// If configStrict had been specified, and there had been an error, the server would already
	// have exited by now. If configWarning is not an empty string, write it to the log now that
	// it's been properly set up.
	if configWarning != "" {
		trace_util_0.Count(_main_00000, 8)
		log.Warn(configWarning)
	}
	trace_util_0.Count(_main_00000, 4)
	setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	setupMetrics()
	createStoreAndDomain()
	createServer()
	signal.SetupSignalHandler(serverShutdown)
	runServer()
	cleanup()
	exit()
}

func exit() {
	trace_util_0.Count(_main_00000, 9)
	if err := log.Sync(); err != nil {
		trace_util_0.Count(_main_00000, 11)
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
	trace_util_0.Count(_main_00000, 10)
	os.Exit(0)
}

func registerStores() {
	trace_util_0.Count(_main_00000, 12)
	err := kvstore.Register("tikv", tikv.Driver{})
	terror.MustNil(err)
	tikv.NewGCHandlerFunc = gcworker.NewGCWorker
	err = kvstore.Register("mocktikv", mockstore.MockDriver{})
	terror.MustNil(err)
}

func registerMetrics() {
	trace_util_0.Count(_main_00000, 13)
	metrics.RegisterMetrics()
}

func createStoreAndDomain() {
	trace_util_0.Count(_main_00000, 14)
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err = kvstore.New(fullPath)
	terror.MustNil(err)
	// Bootstrap a session to load information schema.
	dom, err = session.BootstrapSession(storage)
	terror.MustNil(err)
}

func setupBinlogClient() {
	trace_util_0.Count(_main_00000, 15)
	if !cfg.Binlog.Enable {
		trace_util_0.Count(_main_00000, 19)
		return
	}

	trace_util_0.Count(_main_00000, 16)
	if cfg.Binlog.IgnoreError {
		trace_util_0.Count(_main_00000, 20)
		binloginfo.SetIgnoreError(true)
	}

	trace_util_0.Count(_main_00000, 17)
	var (
		client *pumpcli.PumpsClient
		err    error
	)

	securityOption := pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}

	if len(cfg.Binlog.BinlogSocket) == 0 {
		trace_util_0.Count(_main_00000, 21)
		client, err = pumpcli.NewPumpsClient(cfg.Path, cfg.Binlog.Strategy, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	} else {
		trace_util_0.Count(_main_00000, 22)
		{
			client, err = pumpcli.NewLocalPumpsClient(cfg.Path, cfg.Binlog.BinlogSocket, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
		}
	}

	trace_util_0.Count(_main_00000, 18)
	terror.MustNil(err)

	err = pumpcli.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	binloginfo.SetPumpsClient(client)
	log.Info("tidb-server", zap.Bool("create pumps client success, ignore binlog error", cfg.Binlog.IgnoreError))
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushes metrics in background.
func pushMetric(addr string, interval time.Duration) {
	trace_util_0.Count(_main_00000, 23)
	if interval == zeroDuration || len(addr) == 0 {
		trace_util_0.Count(_main_00000, 25)
		log.Info("disable Prometheus push client")
		return
	}
	trace_util_0.Count(_main_00000, 24)
	log.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	trace_util_0.Count(_main_00000, 26)
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	for {
		trace_util_0.Count(_main_00000, 27)
		err := push.AddFromGatherer(
			job,
			map[string]string{"instance": instanceName()},
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			trace_util_0.Count(_main_00000, 29)
			log.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
		}
		trace_util_0.Count(_main_00000, 28)
		time.Sleep(interval)
	}
}

func instanceName() string {
	trace_util_0.Count(_main_00000, 30)
	hostname, err := os.Hostname()
	if err != nil {
		trace_util_0.Count(_main_00000, 32)
		return "unknown"
	}
	trace_util_0.Count(_main_00000, 31)
	return fmt.Sprintf("%s_%d", hostname, cfg.Port)
}

// parseDuration parses lease argument string.
func parseDuration(lease string) time.Duration {
	trace_util_0.Count(_main_00000, 33)
	dur, err := time.ParseDuration(lease)
	if err != nil {
		trace_util_0.Count(_main_00000, 36)
		dur, err = time.ParseDuration(lease + "s")
	}
	trace_util_0.Count(_main_00000, 34)
	if err != nil || dur < 0 {
		trace_util_0.Count(_main_00000, 37)
		log.Fatal("invalid lease duration", zap.String("lease", lease))
	}
	trace_util_0.Count(_main_00000, 35)
	return dur
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	trace_util_0.Count(_main_00000, 38)
	if !defaultVal {
		trace_util_0.Count(_main_00000, 40)
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	trace_util_0.Count(_main_00000, 39)
	return flag.Bool(name, defaultVal, usage)
}

func loadConfig() string {
	trace_util_0.Count(_main_00000, 41)
	cfg = config.GetGlobalConfig()
	if *configPath != "" {
		trace_util_0.Count(_main_00000, 43)
		// Not all config items are supported now.
		config.SetConfReloader(*configPath, reloadConfig, hotReloadConfigItems...)

		err := cfg.Load(*configPath)
		// This block is to accommodate an interim situation where strict config checking
		// is not the default behavior of TiDB. The warning message must be deferred until
		// logging has been set up. After strict config checking is the default behavior,
		// This should all be removed.
		if _, ok := err.(*config.ErrConfigValidationFailed); ok && !*configCheck && !*configStrict {
			trace_util_0.Count(_main_00000, 45)
			return err.Error()
		}
		trace_util_0.Count(_main_00000, 44)
		terror.MustNil(err)
	}
	trace_util_0.Count(_main_00000, 42)
	return ""
}

// hotReloadConfigItems lists all config items which support hot-reload.
var hotReloadConfigItems = []string{"Performance.MaxProcs", "Performance.MaxMemory", "Performance.CrossJoin",
	"Performance.FeedbackProbability", "Performance.QueryFeedbackLimit", "Performance.PseudoEstimateRatio",
	"OOMAction", "MemQuotaQuery"}

func reloadConfig(nc, c *config.Config) {
	trace_util_0.Count(_main_00000, 46)
	// Just a part of config items need to be reload explicitly.
	// Some of them like OOMAction are always used by getting from global config directly
	// like config.GetGlobalConfig().OOMAction.
	// These config items will become available naturally after the global config pointer
	// is updated in function ReloadGlobalConfig.
	if nc.Performance.MaxProcs != c.Performance.MaxProcs {
		trace_util_0.Count(_main_00000, 52)
		runtime.GOMAXPROCS(int(nc.Performance.MaxProcs))
	}
	trace_util_0.Count(_main_00000, 47)
	if nc.Performance.MaxMemory != c.Performance.MaxMemory {
		trace_util_0.Count(_main_00000, 53)
		plannercore.PreparedPlanCacheMaxMemory.Store(nc.Performance.MaxMemory)
	}
	trace_util_0.Count(_main_00000, 48)
	if nc.Performance.CrossJoin != c.Performance.CrossJoin {
		trace_util_0.Count(_main_00000, 54)
		plannercore.AllowCartesianProduct.Store(nc.Performance.CrossJoin)
	}
	trace_util_0.Count(_main_00000, 49)
	if nc.Performance.FeedbackProbability != c.Performance.FeedbackProbability {
		trace_util_0.Count(_main_00000, 55)
		statistics.FeedbackProbability.Store(nc.Performance.FeedbackProbability)
	}
	trace_util_0.Count(_main_00000, 50)
	if nc.Performance.QueryFeedbackLimit != c.Performance.QueryFeedbackLimit {
		trace_util_0.Count(_main_00000, 56)
		handle.MaxQueryFeedbackCount.Store(int64(nc.Performance.QueryFeedbackLimit))
	}
	trace_util_0.Count(_main_00000, 51)
	if nc.Performance.PseudoEstimateRatio != c.Performance.PseudoEstimateRatio {
		trace_util_0.Count(_main_00000, 57)
		statistics.RatioOfPseudoEstimate.Store(nc.Performance.PseudoEstimateRatio)
	}
}

func overrideConfig() {
	trace_util_0.Count(_main_00000, 58)
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		trace_util_0.Count(_main_00000, 82)
		actualFlags[f.Name] = true
	})

	// Base
	trace_util_0.Count(_main_00000, 59)
	if actualFlags[nmHost] {
		trace_util_0.Count(_main_00000, 83)
		cfg.Host = *host
	}
	trace_util_0.Count(_main_00000, 60)
	if actualFlags[nmAdvertiseAddress] {
		trace_util_0.Count(_main_00000, 84)
		cfg.AdvertiseAddress = *advertiseAddress
	}
	trace_util_0.Count(_main_00000, 61)
	var err error
	if actualFlags[nmPort] {
		trace_util_0.Count(_main_00000, 85)
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	trace_util_0.Count(_main_00000, 62)
	if actualFlags[nmCors] {
		trace_util_0.Count(_main_00000, 86)
		fmt.Println(cors)
		cfg.Cors = *cors
	}
	trace_util_0.Count(_main_00000, 63)
	if actualFlags[nmStore] {
		trace_util_0.Count(_main_00000, 87)
		cfg.Store = *store
	}
	trace_util_0.Count(_main_00000, 64)
	if actualFlags[nmStorePath] {
		trace_util_0.Count(_main_00000, 88)
		cfg.Path = *storePath
	}
	trace_util_0.Count(_main_00000, 65)
	if actualFlags[nmSocket] {
		trace_util_0.Count(_main_00000, 89)
		cfg.Socket = *socket
	}
	trace_util_0.Count(_main_00000, 66)
	if actualFlags[nmEnableBinlog] {
		trace_util_0.Count(_main_00000, 90)
		cfg.Binlog.Enable = *enableBinlog
	}
	trace_util_0.Count(_main_00000, 67)
	if actualFlags[nmRunDDL] {
		trace_util_0.Count(_main_00000, 91)
		cfg.RunDDL = *runDDL
	}
	trace_util_0.Count(_main_00000, 68)
	if actualFlags[nmDdlLease] {
		trace_util_0.Count(_main_00000, 92)
		cfg.Lease = *ddlLease
	}
	trace_util_0.Count(_main_00000, 69)
	if actualFlags[nmTokenLimit] {
		trace_util_0.Count(_main_00000, 93)
		cfg.TokenLimit = uint(*tokenLimit)
	}
	trace_util_0.Count(_main_00000, 70)
	if actualFlags[nmPluginLoad] {
		trace_util_0.Count(_main_00000, 94)
		cfg.Plugin.Load = *pluginLoad
	}
	trace_util_0.Count(_main_00000, 71)
	if actualFlags[nmPluginDir] {
		trace_util_0.Count(_main_00000, 95)
		cfg.Plugin.Dir = *pluginDir
	}

	// Log
	trace_util_0.Count(_main_00000, 72)
	if actualFlags[nmLogLevel] {
		trace_util_0.Count(_main_00000, 96)
		cfg.Log.Level = *logLevel
	}
	trace_util_0.Count(_main_00000, 73)
	if actualFlags[nmLogFile] {
		trace_util_0.Count(_main_00000, 97)
		cfg.Log.File.Filename = *logFile
	}
	trace_util_0.Count(_main_00000, 74)
	if actualFlags[nmLogSlowQuery] {
		trace_util_0.Count(_main_00000, 98)
		cfg.Log.SlowQueryFile = *logSlowQuery
	}

	// Status
	trace_util_0.Count(_main_00000, 75)
	if actualFlags[nmReportStatus] {
		trace_util_0.Count(_main_00000, 99)
		cfg.Status.ReportStatus = *reportStatus
	}
	trace_util_0.Count(_main_00000, 76)
	if actualFlags[nmStatusHost] {
		trace_util_0.Count(_main_00000, 100)
		cfg.Status.StatusHost = *statusHost
	}
	trace_util_0.Count(_main_00000, 77)
	if actualFlags[nmStatusPort] {
		trace_util_0.Count(_main_00000, 101)
		var p int
		p, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
		cfg.Status.StatusPort = uint(p)
	}
	trace_util_0.Count(_main_00000, 78)
	if actualFlags[nmMetricsAddr] {
		trace_util_0.Count(_main_00000, 102)
		cfg.Status.MetricsAddr = *metricsAddr
	}
	trace_util_0.Count(_main_00000, 79)
	if actualFlags[nmMetricsInterval] {
		trace_util_0.Count(_main_00000, 103)
		cfg.Status.MetricsInterval = *metricsInterval
	}

	// PROXY Protocol
	trace_util_0.Count(_main_00000, 80)
	if actualFlags[nmProxyProtocolNetworks] {
		trace_util_0.Count(_main_00000, 104)
		cfg.ProxyProtocol.Networks = *proxyProtocolNetworks
	}
	trace_util_0.Count(_main_00000, 81)
	if actualFlags[nmProxyProtocolHeaderTimeout] {
		trace_util_0.Count(_main_00000, 105)
		cfg.ProxyProtocol.HeaderTimeout = *proxyProtocolHeaderTimeout
	}
}

func setGlobalVars() {
	trace_util_0.Count(_main_00000, 106)
	ddlLeaseDuration := parseDuration(cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)
	runtime.GOMAXPROCS(int(cfg.Performance.MaxProcs))
	statsLeaseDuration := parseDuration(cfg.Performance.StatsLease)
	session.SetStatsLease(statsLeaseDuration)
	bindinfo.Lease = parseDuration(cfg.Performance.BindInfoLease)
	domain.RunAutoAnalyze = cfg.Performance.RunAutoAnalyze
	statistics.FeedbackProbability.Store(cfg.Performance.FeedbackProbability)
	handle.MaxQueryFeedbackCount.Store(int64(cfg.Performance.QueryFeedbackLimit))
	statistics.RatioOfPseudoEstimate.Store(cfg.Performance.PseudoEstimateRatio)
	ddl.RunWorker = cfg.RunDDL
	if cfg.SplitTable {
		trace_util_0.Count(_main_00000, 109)
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	}
	trace_util_0.Count(_main_00000, 107)
	plannercore.AllowCartesianProduct.Store(cfg.Performance.CrossJoin)
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable

	priority := mysql.Str2Priority(cfg.Performance.ForcePriority)
	variable.ForcePriority = int32(priority)
	variable.SysVars[variable.TiDBForcePriority].Value = mysql.Priority2Str[priority]

	variable.SysVars[variable.TIDBMemQuotaQuery].Value = strconv.FormatInt(cfg.MemQuotaQuery, 10)
	variable.SysVars["lower_case_table_names"].Value = strconv.Itoa(cfg.LowerCaseTableNames)
	variable.SysVars[variable.LogBin].Value = variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)

	variable.SysVars[variable.Port].Value = fmt.Sprintf("%d", cfg.Port)
	variable.SysVars[variable.Socket].Value = cfg.Socket
	variable.SysVars[variable.DataDir].Value = cfg.Path
	variable.SysVars[variable.TiDBSlowQueryFile].Value = cfg.Log.SlowQueryFile

	// For CI environment we default enable prepare-plan-cache.
	plannercore.SetPreparedPlanCache(config.CheckTableBeforeDrop || cfg.PreparedPlanCache.Enabled)
	if plannercore.PreparedPlanCacheEnabled() {
		trace_util_0.Count(_main_00000, 110)
		plannercore.PreparedPlanCacheCapacity = cfg.PreparedPlanCache.Capacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = cfg.PreparedPlanCache.MemoryGuardRatio
		if plannercore.PreparedPlanCacheMemoryGuardRatio < 0.0 || plannercore.PreparedPlanCacheMemoryGuardRatio > 1.0 {
			trace_util_0.Count(_main_00000, 112)
			plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		}
		trace_util_0.Count(_main_00000, 111)
		plannercore.PreparedPlanCacheMaxMemory.Store(cfg.Performance.MaxMemory)
		total, err := memory.MemTotal()
		terror.MustNil(err)
		if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
			trace_util_0.Count(_main_00000, 113)
			plannercore.PreparedPlanCacheMaxMemory.Store(total)
		}
	}

	trace_util_0.Count(_main_00000, 108)
	tikv.CommitMaxBackoff = int(parseDuration(cfg.TiKVClient.CommitTimeout).Seconds() * 1000)
	tikv.PessimisticLockTTL = uint64(parseDuration(cfg.PessimisticTxn.TTL).Seconds() * 1000)
}

func setupLog() {
	trace_util_0.Count(_main_00000, 114)
	err := logutil.InitZapLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)
}

func printInfo() {
	trace_util_0.Count(_main_00000, 115)
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func createServer() {
	trace_util_0.Count(_main_00000, 116)
	driver := server.NewTiDBDriver(storage)
	var err error
	svr, err = server.NewServer(cfg, driver)
	// Both domain and storage have started, so we have to clean them before exiting.
	terror.MustNil(err, closeDomainAndStorage)
	go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()
}

func serverShutdown(isgraceful bool) {
	trace_util_0.Count(_main_00000, 117)
	if isgraceful {
		trace_util_0.Count(_main_00000, 119)
		graceful = true
	}
	trace_util_0.Count(_main_00000, 118)
	svr.Close()
}

func setupMetrics() {
	trace_util_0.Count(_main_00000, 120)
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		trace_util_0.Count(_main_00000, 123)
		metrics.TimeJumpBackCounter.Inc()
	}
	trace_util_0.Count(_main_00000, 121)
	callBackCount := 0
	sucessCallBack := func() {
		trace_util_0.Count(_main_00000, 124)
		callBackCount++
		// It is callback by monitor per second, we increase metrics.KeepAliveCounter per 5s.
		if callBackCount >= 5 {
			trace_util_0.Count(_main_00000, 125)
			callBackCount = 0
			metrics.KeepAliveCounter.Inc()
			updateCPUUsageMetrics()
		}
	}
	trace_util_0.Count(_main_00000, 122)
	go systimemon.StartMonitor(time.Now, systimeErrHandler, sucessCallBack)

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func updateCPUUsageMetrics() {
	trace_util_0.Count(_main_00000, 126)
	sysInfo, err := pidusage.GetStat(os.Getpid())
	if err != nil {
		trace_util_0.Count(_main_00000, 128)
		return
	}
	trace_util_0.Count(_main_00000, 127)
	metrics.CPUUsagePercentageGauge.Set(sysInfo.CPU)
}

func setupTracing() {
	trace_util_0.Count(_main_00000, 129)
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracer, _, err := tracingCfg.New("TiDB")
	if err != nil {
		trace_util_0.Count(_main_00000, 131)
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	trace_util_0.Count(_main_00000, 130)
	opentracing.SetGlobalTracer(tracer)
}

func runServer() {
	trace_util_0.Count(_main_00000, 132)
	err := svr.Run()
	terror.MustNil(err)
}

func closeDomainAndStorage() {
	trace_util_0.Count(_main_00000, 133)
	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func cleanup() {
	trace_util_0.Count(_main_00000, 134)
	if graceful {
		trace_util_0.Count(_main_00000, 136)
		svr.GracefulDown(context.Background(), nil)
	} else {
		trace_util_0.Count(_main_00000, 137)
		{
			svr.TryGracefulDown()
		}
	}
	trace_util_0.Count(_main_00000, 135)
	plugin.Shutdown(context.Background())
	closeDomainAndStorage()
}

var _main_00000 = "tidb-server/main.go"
