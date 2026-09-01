package server

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
	"zotregistry.dev/zot/v2/pkg/log"
)

const (
	// configEventDebounceInterval coalesces bursts of fsnotify events into one reload.
	configEventDebounceInterval = 150 * time.Millisecond
	// configStatCheckInterval is the polling interval used as a backstop when
	// inotify watching is unavailable or events were dropped.
	configStatCheckInterval = 1 * time.Second
)

// HotReloader reloads the server configuration when the config file (or the
// LDAP credentials file it references) changes. The file watch gives
// low-latency reloads; the stat fingerprint poll is the guarantee, catching
// changes the watch cannot see (Kubernetes ConfigMap/Secret symlink swaps).
type HotReloader struct {
	watcher             *fsnotify.Watcher
	configPath          string
	ldapCredentialsPath string
	ctlr                *api.Controller
	logger              log.Logger

	// mu protects the fields below.
	mu            sync.Mutex
	debounceTimer *time.Timer
	// stat fingerprints at the last successful reload; polling compares
	// identity, size and mtime against them.
	configInfo os.FileInfo
	ldapInfo   os.FileInfo

	done chan struct{}
	// closed by the watcher loop on exit, so Stop can wait out an in-flight reload
	stopped  chan struct{}
	stopOnce sync.Once
}

func NewHotReloader(ctlr *api.Controller, filePath, ldapCredentialsPath string) (*HotReloader, error) {
	// creates a new file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	hotReloader := &HotReloader{
		watcher:             watcher,
		configPath:          filePath,
		ldapCredentialsPath: ldapCredentialsPath,
		ctlr:                ctlr,
		logger:              log.NewLogger("info", ""),
		done:                make(chan struct{}),
	}

	return hotReloader, nil
}

func signalHandler(ctlr *api.Controller, hr *HotReloader, sigCh chan os.Signal) {
	// if signal then shutdown
	if sig, ok := <-sigCh; ok {
		ctlr.Log.Info().Interface("signal", sig).Msg("received signal")

		hr.Stop()
		// gracefully shutdown http server
		ctlr.Shutdown() //nolint: contextcheck
	}
}

func initShutDownRoutine(ctlr *api.Controller, hr *HotReloader) {
	sigCh := make(chan os.Signal, 1)

	go signalHandler(ctlr, hr, sigCh)

	// block all async signals to this server
	signal.Ignore()

	// handle SIGINT and SIGHUP.
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
}

func (hr *HotReloader) Stop() {
	hr.stopOnce.Do(func() {
		if hr.done != nil {
			close(hr.done)
		}
		if hr.watcher != nil {
			_ = hr.watcher.Close()
		}

		hr.mu.Lock()
		stopped := hr.stopped
		hr.mu.Unlock()

		// wait for the loop and any in-flight reload; nil if Start never ran
		if stopped != nil {
			<-stopped
		}
	})
}

func (hr *HotReloader) Start() {
	// watch failures are tolerated: the fingerprint poll is the backstop
	if err := hr.watcher.Add(hr.configPath); err != nil {
		hr.logger.Error().Err(err).Str("config", hr.configPath).
			Msg("failed to add config file to fsnotify watcher, relying on stat-based polling")
	}

	if hr.ldapCredentialsPath != "" {
		if err := hr.watcher.Add(hr.ldapCredentialsPath); err != nil {
			hr.logger.Error().Err(err).Str("ldap-credentials", hr.ldapCredentialsPath).
				Msg("failed to add ldap-credentials to fsnotify watcher, relying on stat-based polling")
		}
	}

	// baseline the already-loaded config so the first poll tick does not reload
	hr.mu.Lock()
	hr.configInfo = statOrNil(hr.configPath)
	hr.ldapInfo = statOrNil(hr.ldapCredentialsPath)
	hr.stopped = make(chan struct{})
	stopped := hr.stopped
	hr.mu.Unlock()

	go hr.loop(stopped)
}

func (hr *HotReloader) loop(stopped chan struct{}) {
	defer close(stopped)

	statTicker := time.NewTicker(configStatCheckInterval)
	defer statTicker.Stop()

	for {
		select {
		case <-hr.done:
			return

		// watch for events
		case event, ok := <-hr.watcher.Events:
			if !ok {
				return
			}

			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}

			if !samePath(event.Name, hr.configPath) &&
				!samePath(event.Name, hr.getLdapCredentialsPath()) {
				continue
			}

			// an atomic replacement retires the watched inode; re-add for the next one
			if event.Op&(fsnotify.Remove|fsnotify.Rename|fsnotify.Create) != 0 {
				hr.readdWatches()
			}

			hr.scheduleReload()

		case <-hr.getDebounceChannel():
			hr.mu.Lock()
			hr.debounceTimer = nil
			hr.mu.Unlock()

			select {
			case <-hr.done:
				return
			default:
			}

			hr.reloadConfig("debounced file change")

		case <-statTicker.C:
			// always poll: it covers whatever the watch missed
			if hr.checkFilesChanged() {
				hr.readdWatches()
				hr.reloadConfig("stat-based polling")
			}

		// watch for errors
		case err, ok := <-hr.watcher.Errors:
			if !ok {
				return
			}

			hr.logger.Error().Err(err).Str("config", hr.configPath).Msg("fsnotify error while watching config")

			// events may have been dropped; resync sooner than the next poll tick
			hr.scheduleReload()
		}
	}
}

func (hr *HotReloader) reloadConfig(reason string) {
	hr.logger.Info().Str("config", hr.configPath).Str("reason", reason).
		Msg("config file changed, trying to reload config")

	// sample fingerprints before reading, so a replacement mid-read is re-detected
	configInfo := statOrNil(hr.configPath)
	ldapInfo := statOrNil(hr.getLdapCredentialsPath())

	newConfig := config.New()

	err := LoadConfiguration(newConfig, hr.configPath)
	if err != nil {
		// through the controller's logger so it reaches the configured log output
		hr.ctlr.Log.Error().Err(err).Str("config", hr.configPath).
			Msg("failed to reload config, retry writing it.")

		// baseline the bad file so the poll does not retry every tick; the
		// next actual change retries
		hr.mu.Lock()
		hr.configInfo = configInfo
		hr.ldapInfo = ldapInfo
		hr.mu.Unlock()

		return
	}

	// follow the credentials file: moved, newly added, or removed with LDAP
	newLdapPath := ""
	if newConfig.HTTP.Auth != nil && newConfig.HTTP.Auth.LDAP != nil {
		newLdapPath = newConfig.HTTP.Auth.LDAP.CredentialsFile
	}

	if oldLdapPath := hr.getLdapCredentialsPath(); newLdapPath != oldLdapPath {
		if oldLdapPath != "" {
			err = hr.watcher.Remove(oldLdapPath)
			if err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
				hr.logger.Error().Err(err).Msg("failed to remove old watch for the credentials file")
			}
		}

		hr.mu.Lock()
		hr.ldapCredentialsPath = newLdapPath
		hr.mu.Unlock()

		if newLdapPath != "" {
			if err := hr.watcher.Add(newLdapPath); err != nil {
				hr.logger.Error().Err(err).Str("ldap-credentials-file", newLdapPath).
					Msg("failed to watch ldap credentials file, relying on stat-based polling")
			}
		}

		ldapInfo = statOrNil(newLdapPath)
	}

	// stop background tasks gracefully
	hr.ctlr.StopBackgroundTasks()

	// load new config
	hr.ctlr.LoadNewConfig(newConfig)

	// start background tasks based on new loaded config
	hr.ctlr.StartBackgroundTasks()

	hr.warnNonReloadableChanges(newConfig)

	hr.mu.Lock()
	hr.configInfo = configInfo
	hr.ldapInfo = ldapInfo
	hr.mu.Unlock()
}

// warnNonReloadableChanges logs config fields that still differ between the
// file and the effective configuration after a reload: they need a restart.
func (hr *HotReloader) warnNonReloadableChanges(newConfig *config.Config) {
	effective, err := configAsMap(hr.ctlr.Config.Sanitize())
	if err != nil {
		return
	}

	desired, err := configAsMap(newConfig.Sanitize())
	if err != nil {
		return
	}

	fields := diffConfigPaths("", effective, desired)
	if len(fields) > 0 {
		// via the controller's logger so it reaches the configured log output
		hr.ctlr.Log.Warn().Strs("fields", fields).
			Msg("config changes are outside the reloadable set and need a restart to take effect")
	}
}

func configAsMap(cfg *config.Config) (map[string]any, error) {
	buf, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		return nil, err
	}

	return out, nil
}

// diffConfigPaths returns the dotted paths whose values differ between two
// JSON object trees; nested objects recurse, anything else compares wholesale.
func diffConfigPaths(prefix string, effective, desired map[string]any) []string {
	keys := make(map[string]struct{})
	for key := range effective {
		keys[key] = struct{}{}
	}

	for key := range desired {
		keys[key] = struct{}{}
	}

	paths := []string{}

	for _, key := range slices.Sorted(maps.Keys(keys)) {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}

		effectiveVal, desiredVal := effective[key], desired[key]

		effectiveMap, effectiveIsMap := effectiveVal.(map[string]any)

		desiredMap, desiredIsMap := desiredVal.(map[string]any)
		if effectiveIsMap && desiredIsMap {
			paths = append(paths, diffConfigPaths(path, effectiveMap, desiredMap)...)

			continue
		}

		if !reflect.DeepEqual(effectiveVal, desiredVal) {
			paths = append(paths, path)
		}
	}

	return paths
}

// checkFilesChanged reports whether the config or LDAP credentials file differs
// from the baseline fingerprint recorded at the last successful reload.
// Identity (os.SameFile) catches atomic-rename replacements even when the new
// file preserves the old timestamp; size catches same-inode rewrites within one
// coarse-timestamp granule; mtime catches plain in-place edits.
func (hr *HotReloader) checkFilesChanged() bool {
	hr.mu.Lock()
	configInfo, ldapInfo := hr.configInfo, hr.ldapInfo
	ldapPath := hr.ldapCredentialsPath
	hr.mu.Unlock()

	return fileChanged(hr.configPath, configInfo) || fileChanged(ldapPath, ldapInfo)
}

func fileChanged(path string, prev os.FileInfo) bool {
	if path == "" {
		return false
	}

	info, err := os.Stat(path)
	if err != nil {
		// Transient during atomic replacements; the next tick sees the new file.
		return false
	}

	if prev == nil {
		return true
	}

	return !os.SameFile(prev, info) || prev.Size() != info.Size() || !prev.ModTime().Equal(info.ModTime())
}

func (hr *HotReloader) getDebounceChannel() <-chan time.Time {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.debounceTimer == nil {
		return nil
	}

	return hr.debounceTimer.C
}

func (hr *HotReloader) scheduleReload() {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.debounceTimer != nil {
		if !hr.debounceTimer.Stop() {
			select {
			case <-hr.debounceTimer.C:
			default:
			}
		}
		hr.debounceTimer.Reset(configEventDebounceInterval)

		return
	}

	hr.debounceTimer = time.NewTimer(configEventDebounceInterval)
}

// getLdapCredentialsPath returns the current LDAP credentials path, which a
// reload can rewrite.
func (hr *HotReloader) getLdapCredentialsPath() string {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	return hr.ldapCredentialsPath
}

func (hr *HotReloader) readdWatches() {
	if err := hr.watcher.Add(hr.configPath); err != nil {
		hr.logger.Debug().Err(err).Str("config", hr.configPath).
			Msg("failed to re-add config watch, relying on stat-based polling")
	}

	ldapPath := hr.getLdapCredentialsPath()

	if ldapPath != "" {
		if err := hr.watcher.Add(ldapPath); err != nil {
			hr.logger.Debug().Err(err).Str("ldap-credentials", ldapPath).
				Msg("failed to re-add ldap-credentials watch, relying on stat-based polling")
		}
	}
}

// samePath reports whether a fsnotify event name refers to the watched file.
func samePath(eventName, filePath string) bool {
	if eventName == "" || filePath == "" {
		return false
	}

	return filepath.Clean(eventName) == filepath.Clean(filePath)
}

func statOrNil(path string) os.FileInfo {
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil
	}

	return info
}
