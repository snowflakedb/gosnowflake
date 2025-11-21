package gosnowflake

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var miniCoreOnce sync.Once
var miniCoreMutex sync.RWMutex
var miniCoreLoaded = make(chan struct{})

var miniCoreLoader = newMiniCoreLoader()
var miniCoreInstance miniCore

// miniCoreErrorType represents the category of minicore error that occurred.
type miniCoreErrorType int

// Error type constants for categorizing minicore failures.
const (
	miniCoreErrorTypeLoad   miniCoreErrorType = iota // Library loading failed
	miniCoreErrorTypeSymbol                          // Symbol lookup failed
	miniCoreErrorTypeCall                            // Function call failed
	miniCoreErrorTypeInit                            // Initialization failed
	miniCoreErrorTypeWrite                           // File write failed
)

// String returns a human-readable string representation of the error type.
func (et miniCoreErrorType) String() string {
	switch et {
	case miniCoreErrorTypeLoad:
		return "load"
	case miniCoreErrorTypeSymbol:
		return "symbol"
	case miniCoreErrorTypeCall:
		return "call"
	case miniCoreErrorTypeInit:
		return "init"
	case miniCoreErrorTypeWrite:
		return "write"
	default:
		return "unknown"
	}
}

// miniCoreError represents a structured error from minicore operations.
// It provides detailed context about what went wrong, where, and why.
type miniCoreError struct {
	errorType miniCoreErrorType // errorType categorizes the kind of error
	platform  string            // platform identifies the OS where error occurred
	path      string            // path to the library file, if applicable
	err       error             // err wraps the underlying error cause
}

// Error returns a formatted error message with context about the failure.
func (e *miniCoreError) Error() string {
	if e.path != "" {
		return fmt.Sprintf("minicore %s on %s (path: %s): %v", e.errorType, e.platform, e.path, e.err)
	}
	return fmt.Sprintf("minicore %s on %s: %v", e.errorType, e.platform, e.err)
}

// Unwrap returns the underlying error for error chain inspection.
func (e *miniCoreError) Unwrap() error {
	return e.err
}

// newMiniCoreError creates a new structured minicore error with full context.
func newMiniCoreError(errType miniCoreErrorType, platform, path string, err error) *miniCoreError {
	return &miniCoreError{
		errorType: errType,
		platform:  platform,
		path:      path,
		err:       err,
	}
}

// corePlatformConfigType holds platform-specific minicore configuration.
type corePlatformConfigType struct {
	initialized bool   // initialized indicates if the platform is supported
	coreLib     []byte // coreLib contains the embedded native library
	extension   string // extension is the platform-specific library file extension
}

// corePlatformConfig holds platform-specific configuration. If not initialized, minicore is unsupported.
var corePlatformConfig = corePlatformConfigType{}

// miniCore defines the interface for interacting with the native minicore library.
type miniCore interface {
	// FullVersion returns the version string from the native library.
	FullVersion() (string, error)
}

// erroredMiniCore implements miniCore but always returns an error.
// It's used when minicore initialization fails.
type erroredMiniCore struct {
	err error
}

// newErroredMiniCore creates a miniCore implementation that always returns the given error.
func newErroredMiniCore(err error) *erroredMiniCore {
	return &erroredMiniCore{err: err}
}

// FullVersion always returns an empty string and the stored error.
func (emc erroredMiniCore) FullVersion() (string, error) {
	return "", emc.err
}

// miniCoreLoaderType manages the loading and initialization of the minicore native library.
type miniCoreLoaderType struct {
	searchDirs []string // searchDirs contains directories to search for the library
}

// newMiniCoreLoader creates a new minicore miniCoreLoaderType with platform-appropriate search directories.
func newMiniCoreLoader() *miniCoreLoaderType {
	return &miniCoreLoaderType{
		searchDirs: buildMiniCoreSearchDirs(),
	}
}

// buildMiniCoreSearchDirs constructs the list of directories to search for the minicore library.
func buildMiniCoreSearchDirs() []string {
	var dirs []string

	// Add temp directory
	if tempDir, err := os.MkdirTemp("", "gosnowflake-cgo"); err == nil && tempDir != "" {
		dirs = append(dirs, tempDir)
	} else {
		logger.Debugf("cannot create temp directory for gosnowflakecore: %v", err)
	}

	// Add current working directory
	if cwd, err := os.Getwd(); err == nil {
		dirs = append(dirs, cwd)
	} else {
		logger.Debugf("cannot get current working directory: %v", err)
	}

	// Add platform-specific cache directory
	if cacheDir := getMiniCoreCacheDir(); cacheDir != "" {
		dirs = append(dirs, cacheDir)
	}

	return dirs
}

// getMiniCoreCacheDir returns the platform-specific cache directory for storing the minicore library.
func getMiniCoreCacheDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		logger.Debugf("cannot get user home directory: %v", err)
		return ""
	}

	switch runtime.GOOS {
	case "windows":
		return filepath.Join(homeDir, "AppData", "Local", "Snowflake", "Caches", "minicore")
	case "darwin":
		return filepath.Join(homeDir, "Library", "Caches", "Snowflake", "minicore")
	default:
		return filepath.Join(homeDir, ".cache", "snowflake", "minicore")
	}
}

// loadCore loads and initializes the minicore native library.
func (l *miniCoreLoaderType) loadCore() miniCore {
	if !corePlatformConfig.initialized {
		return newErroredMiniCore(newMiniCoreError(miniCoreErrorTypeInit, runtime.GOOS, "",
			fmt.Errorf("minicore is not supported on %v/%v platform", runtime.GOOS, runtime.GOARCH)))
	}

	libPath, err := l.writeLibraryToFile()
	if err != nil {
		return newErroredMiniCore(err)
	}

	return l.loadFromPath(libPath)
}

// writeLibraryToFile writes the embedded library to the first available directory
func (l *miniCoreLoaderType) writeLibraryToFile() (string, error) {
	filename := "libsf_mini_core." + corePlatformConfig.extension
	var errs []error

	for _, dir := range l.searchDirs {
		libPath := filepath.Join(dir, filename)
		if err := os.WriteFile(libPath, corePlatformConfig.coreLib, 0755); err != nil {
			logger.Debugf("Failed to write embedded library to %s: %v", libPath, err)
			errs = append(errs, fmt.Errorf("failed to write to %v: %v", libPath, err))
			continue
		}
		logger.Debugf("Successfully wrote embedded library to %s", libPath)
		return libPath, nil
	}

	return "", newMiniCoreError(miniCoreErrorTypeWrite, runtime.GOOS, "",
		fmt.Errorf("failed to write embedded library to any directory (errors: %v)", errs))
}

// getMiniCore returns the minicore instance, loading it asynchronously if needed.
func getMiniCore() miniCore {
	miniCoreOnce.Do(func() {
		go func() {
			logger.Debugf("Starting asynchronous minicore loading")
			core := miniCoreLoader.loadCore()

			miniCoreMutex.Lock()
			miniCoreInstance = core
			miniCoreMutex.Unlock()

			// Signal that loading is complete
			close(miniCoreLoaded)
			logger.Debugf("Minicore loading completed asynchronously")
		}()
	})

	// Return current instance (may be nil initially)
	miniCoreMutex.RLock()
	defer miniCoreMutex.RUnlock()
	return miniCoreInstance
}

func init() {
	// Start async minicore loading but don't block initialization.
	// This allows the application to start quickly while minicore loads in the background.
	// Use getMiniCore() for non-blocking access or getMiniCoreBlocking() when you need to wait.
	getMiniCore()
}
