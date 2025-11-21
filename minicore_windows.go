package gosnowflake

import (
	_ "embed"
	"fmt"
	"golang.org/x/sys/windows"
	"syscall"
	"unsafe"
)

//go:embed libsf_mini_core_windows_x86_64.dll
var coreLib []byte

var _ = initFirst()

func initFirst() any {
	corePlatformConfig.initialized = true
	corePlatformConfig.coreLib = coreLib
	corePlatformConfig.extension = "dll"
	return nil
}

type windowsMiniCore struct {
	// fullVersion holds the version string returned from the library
	fullVersion string
	// coreInitError holds any error that occurred during initialization
	coreInitError error
}

func (wmc *windowsMiniCore) FullVersion() (string, error) {
	return wmc.fullVersion, wmc.coreInitError
}

func (l *miniCoreLoaderType) loadFromPath(libPath string) miniCore {
	logger.Debugf("Loading minicore library from: %s", libPath)
	dllHandle, err := windows.LoadLibrary(libPath)
	if err != nil {
		mcErr := newMiniCoreError(miniCoreErrorTypeLoad, "windows", libPath, fmt.Errorf("failed to load shared library: %v", err))
		return newErroredMiniCore(mcErr)
	}

	// Release the DLL handle, because we cache minicore fullVersion result.
	defer windows.FreeLibrary(dllHandle)

	// The name of the exported function from your C/Rust DLL
	procName := "sf_core_full_version"

	// Get the address of the function
	procAddr, err := windows.GetProcAddress(dllHandle, procName)
	if err != nil {
		mcErr := newMiniCoreError(miniCoreErrorTypeSymbol, "windows", libPath, fmt.Errorf("procedure '%s' not found: %v", procName, err))
		return newErroredMiniCore(mcErr)
	}

	// Second return value - omitted, required for syscalls that returns more values
	ret, _, callErr := syscall.Syscall(
		procAddr,
		0, // nargs: Number of arguments is ZERO
		0, // a1: Argument 1 (unused)
		0, // a2: Argument 2 (unused)
		0, // a3: Argument 3 (unused)
	)

	if callErr != 0 {
		mcErr := newMiniCoreError(miniCoreErrorTypeCall, "windows", libPath, fmt.Errorf("system call failed with error code: %v", callErr))
		return newErroredMiniCore(mcErr)
	}

	cStrPtr := (*byte)(unsafe.Pointer(ret))
	if cStrPtr == nil {
		mcErr := newMiniCoreError(miniCoreErrorTypeCall, "windows", libPath, fmt.Errorf("native function returned null pointer (error code: %v)", callErr))
		return newErroredMiniCore(mcErr)
	}

	goStr := windows.BytePtrToString(cStrPtr)
	return &windowsMiniCore{
		fullVersion: goStr,
	}
}
