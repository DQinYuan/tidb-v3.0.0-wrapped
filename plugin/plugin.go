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
	"context"
	"path/filepath"
	gplugin "plugin"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/trace_util_0"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// pluginGlobal holds all global variables for plugin.
var pluginGlobal copyOnWriteContext

// copyOnWriteContext wraps a context follow COW idiom.
type copyOnWriteContext struct {
	tiPlugins unsafe.Pointer // *plugins
}

// plugins collects loaded plugins info.
type plugins struct {
	plugins      map[Kind][]Plugin
	versions     map[string]uint16
	dyingPlugins []Plugin
}

// clone deep copies plugins info.
func (p *plugins) clone() *plugins {
	trace_util_0.Count(_plugin_00000, 0)
	np := &plugins{
		plugins:  make(map[Kind][]Plugin, len(p.plugins)),
		versions: make(map[string]uint16, len(p.versions)),
	}
	for key, value := range p.plugins {
		trace_util_0.Count(_plugin_00000, 4)
		np.plugins[key] = append([]Plugin(nil), value...)
	}
	trace_util_0.Count(_plugin_00000, 1)
	for key, value := range p.versions {
		trace_util_0.Count(_plugin_00000, 5)
		np.versions[key] = value
	}
	trace_util_0.Count(_plugin_00000, 2)
	for key, value := range p.dyingPlugins {
		trace_util_0.Count(_plugin_00000, 6)
		np.dyingPlugins[key] = value
	}
	trace_util_0.Count(_plugin_00000, 3)
	return np
}

// add adds a plugin to loaded plugin collection.
func (p plugins) add(plugin *Plugin) {
	trace_util_0.Count(_plugin_00000, 7)
	plugins, ok := p.plugins[plugin.Kind]
	if !ok {
		trace_util_0.Count(_plugin_00000, 9)
		plugins = make([]Plugin, 0)
	}
	trace_util_0.Count(_plugin_00000, 8)
	plugins = append(plugins, *plugin)
	p.plugins[plugin.Kind] = plugins
	p.versions[plugin.Name] = plugin.Version
}

// plugins got plugin in COW context.
func (p copyOnWriteContext) plugins() *plugins {
	trace_util_0.Count(_plugin_00000, 10)
	return (*plugins)(atomic.LoadPointer(&p.tiPlugins))
}

// Config presents the init configuration for plugin framework.
type Config struct {
	Plugins        []string
	PluginDir      string
	GlobalSysVar   *map[string]*variable.SysVar
	PluginVarNames *[]string
	SkipWhenFail   bool
	EnvVersion     map[string]uint16
	EtcdClient     *clientv3.Client
}

// Plugin presents a TiDB plugin.
type Plugin struct {
	*Manifest
	library *gplugin.Plugin
	State   State
	Path    string
}

type validateMode int

const (
	initMode validateMode = iota
	reloadMode
)

func (p *Plugin) validate(ctx context.Context, tiPlugins *plugins, mode validateMode) error {
	trace_util_0.Count(_plugin_00000, 11)
	if mode == reloadMode {
		trace_util_0.Count(_plugin_00000, 16)
		var oldPlugin *Plugin
		for i, item := range tiPlugins.plugins[p.Kind] {
			trace_util_0.Count(_plugin_00000, 20)
			if item.Name == p.Name {
				trace_util_0.Count(_plugin_00000, 21)
				oldPlugin = &tiPlugins.plugins[p.Kind][i]
				break
			}
		}
		trace_util_0.Count(_plugin_00000, 17)
		if oldPlugin == nil {
			trace_util_0.Count(_plugin_00000, 22)
			return errUnsupportedReloadPlugin.GenWithStackByArgs(p.Name)
		}
		trace_util_0.Count(_plugin_00000, 18)
		if len(p.SysVars) != len(oldPlugin.SysVars) {
			trace_util_0.Count(_plugin_00000, 23)
			return errUnsupportedReloadPluginVar.GenWithStackByArgs("")
		}
		trace_util_0.Count(_plugin_00000, 19)
		for varName, varVal := range p.SysVars {
			trace_util_0.Count(_plugin_00000, 24)
			if oldPlugin.SysVars[varName] == nil || *oldPlugin.SysVars[varName] != *varVal {
				trace_util_0.Count(_plugin_00000, 25)
				return errUnsupportedReloadPluginVar.GenWithStackByArgs(varVal)
			}
		}
	}
	trace_util_0.Count(_plugin_00000, 12)
	if p.RequireVersion != nil {
		trace_util_0.Count(_plugin_00000, 26)
		for component, reqVer := range p.RequireVersion {
			trace_util_0.Count(_plugin_00000, 27)
			if ver, ok := tiPlugins.versions[component]; !ok || ver < reqVer {
				trace_util_0.Count(_plugin_00000, 28)
				return errRequireVersionCheckFail.GenWithStackByArgs(p.Name, component, reqVer, ver)
			}
		}
	}
	trace_util_0.Count(_plugin_00000, 13)
	if p.SysVars != nil {
		trace_util_0.Count(_plugin_00000, 29)
		for varName := range p.SysVars {
			trace_util_0.Count(_plugin_00000, 30)
			if !strings.HasPrefix(varName, p.Name) {
				trace_util_0.Count(_plugin_00000, 31)
				return errInvalidPluginSysVarName.GenWithStackByArgs(p.Name, varName, p.Name)
			}
		}
	}
	trace_util_0.Count(_plugin_00000, 14)
	if p.Manifest.Validate != nil {
		trace_util_0.Count(_plugin_00000, 32)
		if err := p.Manifest.Validate(ctx, p.Manifest); err != nil {
			trace_util_0.Count(_plugin_00000, 33)
			return err
		}
	}
	trace_util_0.Count(_plugin_00000, 15)
	return nil
}

// Load load plugin by config param.
// This method need be called before domain init to inject global variable info during bootstrap.
func Load(ctx context.Context, cfg Config) (err error) {
	trace_util_0.Count(_plugin_00000, 34)
	tiPlugins := &plugins{
		plugins:      make(map[Kind][]Plugin),
		versions:     make(map[string]uint16),
		dyingPlugins: make([]Plugin, 0),
	}

	// Setup component version info for plugin running env.
	for component, version := range cfg.EnvVersion {
		trace_util_0.Count(_plugin_00000, 38)
		tiPlugins.versions[component] = version
	}

	// Load plugin dl & manifest.
	trace_util_0.Count(_plugin_00000, 35)
	for _, pluginID := range cfg.Plugins {
		trace_util_0.Count(_plugin_00000, 39)
		var pName string
		pName, _, err = ID(pluginID).Decode()
		if err != nil {
			trace_util_0.Count(_plugin_00000, 43)
			err = errors.Trace(err)
			return
		}
		// Check duplicate.
		trace_util_0.Count(_plugin_00000, 40)
		_, dup := tiPlugins.versions[pName]
		if dup {
			trace_util_0.Count(_plugin_00000, 44)
			if cfg.SkipWhenFail {
				trace_util_0.Count(_plugin_00000, 46)
				logutil.Logger(ctx).Warn("duplicate load %s and ignored", zap.String("pluginName", pName))
				continue
			}
			trace_util_0.Count(_plugin_00000, 45)
			err = errDuplicatePlugin.GenWithStackByArgs(pluginID)
			return
		}
		// Load dl.
		trace_util_0.Count(_plugin_00000, 41)
		var plugin Plugin
		plugin, err = loadOne(cfg.PluginDir, ID(pluginID))
		if err != nil {
			trace_util_0.Count(_plugin_00000, 47)
			if cfg.SkipWhenFail {
				trace_util_0.Count(_plugin_00000, 49)
				logutil.Logger(ctx).Warn("load plugin failure and ignored", zap.String("pluginID", pluginID), zap.Error(err))
				continue
			}
			trace_util_0.Count(_plugin_00000, 48)
			return
		}
		trace_util_0.Count(_plugin_00000, 42)
		tiPlugins.add(&plugin)
	}

	// Cross validate & Load plugins.
	trace_util_0.Count(_plugin_00000, 36)
	for kind := range tiPlugins.plugins {
		trace_util_0.Count(_plugin_00000, 50)
		for i := range tiPlugins.plugins[kind] {
			trace_util_0.Count(_plugin_00000, 51)
			if err = tiPlugins.plugins[kind][i].validate(ctx, tiPlugins, initMode); err != nil {
				trace_util_0.Count(_plugin_00000, 53)
				if cfg.SkipWhenFail {
					trace_util_0.Count(_plugin_00000, 55)
					logutil.Logger(ctx).Warn("validate plugin fail and disable plugin",
						zap.String("plugin", tiPlugins.plugins[kind][i].Name), zap.Error(err))
					tiPlugins.plugins[kind][i].State = Disable
					err = nil
					continue
				}
				trace_util_0.Count(_plugin_00000, 54)
				return
			}
			trace_util_0.Count(_plugin_00000, 52)
			if cfg.GlobalSysVar != nil {
				trace_util_0.Count(_plugin_00000, 56)
				for key, value := range tiPlugins.plugins[kind][i].SysVars {
					trace_util_0.Count(_plugin_00000, 57)
					(*cfg.GlobalSysVar)[key] = value
					if value.Scope != variable.ScopeSession && cfg.PluginVarNames != nil {
						trace_util_0.Count(_plugin_00000, 58)
						*cfg.PluginVarNames = append(*cfg.PluginVarNames, key)
					}
				}
			}
		}
	}
	trace_util_0.Count(_plugin_00000, 37)
	pluginGlobal = copyOnWriteContext{tiPlugins: unsafe.Pointer(tiPlugins)}
	err = nil
	return
}

// Init initializes the loaded plugin by config param.
// This method must be called after `Load` but before any other plugin method call, so it call got TiDB domain info.
func Init(ctx context.Context, cfg Config) (err error) {
	trace_util_0.Count(_plugin_00000, 59)
	tiPlugins := pluginGlobal.plugins()
	if tiPlugins == nil {
		trace_util_0.Count(_plugin_00000, 62)
		return nil
	}
	trace_util_0.Count(_plugin_00000, 60)
	for kind := range tiPlugins.plugins {
		trace_util_0.Count(_plugin_00000, 63)
		for i := range tiPlugins.plugins[kind] {
			trace_util_0.Count(_plugin_00000, 64)
			p := tiPlugins.plugins[kind][i]
			if err = p.OnInit(ctx, p.Manifest); err != nil {
				trace_util_0.Count(_plugin_00000, 67)
				if cfg.SkipWhenFail {
					trace_util_0.Count(_plugin_00000, 69)
					logutil.Logger(ctx).Warn("call Plugin OnInit failure, err: %v",
						zap.String("plugin", p.Name), zap.Error(err))
					tiPlugins.plugins[kind][i].State = Disable
					err = nil
					continue
				}
				trace_util_0.Count(_plugin_00000, 68)
				return
			}
			trace_util_0.Count(_plugin_00000, 65)
			if p.OnFlush != nil && cfg.EtcdClient != nil {
				trace_util_0.Count(_plugin_00000, 70)
				const pluginWatchPrefix = "/tidb/plugins/"
				ctx, cancel := context.WithCancel(context.Background())
				watcher := &flushWatcher{
					ctx:      ctx,
					cancel:   cancel,
					path:     pluginWatchPrefix + tiPlugins.plugins[kind][i].Name,
					etcd:     cfg.EtcdClient,
					manifest: tiPlugins.plugins[kind][i].Manifest,
				}
				tiPlugins.plugins[kind][i].flushWatcher = watcher
				go util.WithRecovery(watcher.watchLoop, nil)
			}
			trace_util_0.Count(_plugin_00000, 66)
			tiPlugins.plugins[kind][i].State = Ready
		}
	}
	trace_util_0.Count(_plugin_00000, 61)
	return
}

type flushWatcher struct {
	ctx      context.Context
	cancel   context.CancelFunc
	path     string
	etcd     *clientv3.Client
	manifest *Manifest
}

func (w *flushWatcher) watchLoop() {
	trace_util_0.Count(_plugin_00000, 71)
	watchChan := w.etcd.Watch(w.ctx, w.path)
	for {
		trace_util_0.Count(_plugin_00000, 72)
		select {
		case <-w.ctx.Done():
			trace_util_0.Count(_plugin_00000, 73)
			return
		case <-watchChan:
			trace_util_0.Count(_plugin_00000, 74)
			err := w.manifest.OnFlush(w.ctx, w.manifest)
			if err != nil {
				trace_util_0.Count(_plugin_00000, 75)
				logutil.Logger(context.Background()).Error("notify plugin flush event failed", zap.String("plugin", w.manifest.Name), zap.Error(err))
			}
		}
	}
}

func loadOne(dir string, pluginID ID) (plugin Plugin, err error) {
	trace_util_0.Count(_plugin_00000, 76)
	plugin.Path = filepath.Join(dir, string(pluginID)+LibrarySuffix)
	plugin.library, err = gplugin.Open(plugin.Path)
	if err != nil {
		trace_util_0.Count(_plugin_00000, 83)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_plugin_00000, 77)
	manifestSym, err := plugin.library.Lookup(ManifestSymbol)
	if err != nil {
		trace_util_0.Count(_plugin_00000, 84)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_plugin_00000, 78)
	manifest, ok := manifestSym.(func() *Manifest)
	if !ok {
		trace_util_0.Count(_plugin_00000, 85)
		err = errInvalidPluginManifest.GenWithStackByArgs(string(pluginID))
		return
	}
	trace_util_0.Count(_plugin_00000, 79)
	pName, pVersion, err := pluginID.Decode()
	if err != nil {
		trace_util_0.Count(_plugin_00000, 86)
		err = errors.Trace(err)
		return
	}
	trace_util_0.Count(_plugin_00000, 80)
	plugin.Manifest = manifest()
	if plugin.Name != pName {
		trace_util_0.Count(_plugin_00000, 87)
		err = errInvalidPluginName.GenWithStackByArgs(string(pluginID), plugin.Name)
		return
	}
	trace_util_0.Count(_plugin_00000, 81)
	if strconv.Itoa(int(plugin.Version)) != pVersion {
		trace_util_0.Count(_plugin_00000, 88)
		err = errInvalidPluginVersion.GenWithStackByArgs(string(pluginID))
		return
	}
	trace_util_0.Count(_plugin_00000, 82)
	return
}

// Shutdown cleanups all plugin resources.
// Notice: it just cleanups the resource of plugin, but cannot unload plugins(limited by go plugin).
func Shutdown(ctx context.Context) {
	trace_util_0.Count(_plugin_00000, 89)
	for {
		trace_util_0.Count(_plugin_00000, 90)
		tiPlugins := pluginGlobal.plugins()
		if tiPlugins == nil {
			trace_util_0.Count(_plugin_00000, 93)
			return
		}
		trace_util_0.Count(_plugin_00000, 91)
		for _, plugins := range tiPlugins.plugins {
			trace_util_0.Count(_plugin_00000, 94)
			for _, p := range plugins {
				trace_util_0.Count(_plugin_00000, 95)
				p.State = Dying
				if p.flushWatcher != nil {
					trace_util_0.Count(_plugin_00000, 97)
					p.flushWatcher.cancel()
				}
				trace_util_0.Count(_plugin_00000, 96)
				if err := p.OnShutdown(ctx, p.Manifest); err != nil {
					trace_util_0.Count(_plugin_00000, 98)
					logutil.Logger(ctx).Error("call OnShutdown for failure",
						zap.String("plugin", p.Name), zap.Error(err))
				}
			}
		}
		trace_util_0.Count(_plugin_00000, 92)
		if atomic.CompareAndSwapPointer(&pluginGlobal.tiPlugins, unsafe.Pointer(tiPlugins), nil) {
			trace_util_0.Count(_plugin_00000, 99)
			return
		}
	}
}

// Get finds and returns plugin by kind and name parameters.
func Get(kind Kind, name string) *Plugin {
	trace_util_0.Count(_plugin_00000, 100)
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		trace_util_0.Count(_plugin_00000, 103)
		return nil
	}
	trace_util_0.Count(_plugin_00000, 101)
	for _, p := range plugins.plugins[kind] {
		trace_util_0.Count(_plugin_00000, 104)
		if p.Name == name {
			trace_util_0.Count(_plugin_00000, 105)
			return &p
		}
	}
	trace_util_0.Count(_plugin_00000, 102)
	return nil
}

// ForeachPlugin loops all ready plugins.
func ForeachPlugin(kind Kind, fn func(plugin *Plugin) error) error {
	trace_util_0.Count(_plugin_00000, 106)
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		trace_util_0.Count(_plugin_00000, 109)
		return nil
	}
	trace_util_0.Count(_plugin_00000, 107)
	for i := range plugins.plugins[kind] {
		trace_util_0.Count(_plugin_00000, 110)
		p := &plugins.plugins[kind][i]
		if p.State != Ready {
			trace_util_0.Count(_plugin_00000, 112)
			continue
		}
		trace_util_0.Count(_plugin_00000, 111)
		err := fn(p)
		if err != nil {
			trace_util_0.Count(_plugin_00000, 113)
			return err
		}
	}
	trace_util_0.Count(_plugin_00000, 108)
	return nil
}

// GetAll finds and returns all plugins.
func GetAll() map[Kind][]Plugin {
	trace_util_0.Count(_plugin_00000, 114)
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		trace_util_0.Count(_plugin_00000, 116)
		return nil
	}
	trace_util_0.Count(_plugin_00000, 115)
	return plugins.plugins
}

// NotifyFlush notify plugins to do flush logic.
func NotifyFlush(dom *domain.Domain, pluginName string) error {
	trace_util_0.Count(_plugin_00000, 117)
	p := getByName(pluginName)
	if p == nil || p.Manifest.flushWatcher == nil || p.State != Ready {
		trace_util_0.Count(_plugin_00000, 120)
		return errors.Errorf("plugin %s doesn't exists or unsupported flush or doesn't start with PD", pluginName)
	}
	trace_util_0.Count(_plugin_00000, 118)
	_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, "")
	if err != nil {
		trace_util_0.Count(_plugin_00000, 121)
		return err
	}
	trace_util_0.Count(_plugin_00000, 119)
	return nil
}

func getByName(pluginName string) *Plugin {
	trace_util_0.Count(_plugin_00000, 122)
	for _, plugins := range GetAll() {
		trace_util_0.Count(_plugin_00000, 124)
		for _, p := range plugins {
			trace_util_0.Count(_plugin_00000, 125)
			if p.Name == pluginName {
				trace_util_0.Count(_plugin_00000, 126)
				return &p
			}
		}
	}
	trace_util_0.Count(_plugin_00000, 123)
	return nil
}

var _plugin_00000 = "plugin/plugin.go"
