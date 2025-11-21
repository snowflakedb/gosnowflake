//go:build !windows

package gosnowflake

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>

static void* dlOpen(const char* path) {
    return dlopen(path, RTLD_LAZY);
}

static void* dlSym(void* handle, const char* name) {
    return dlsym(handle, name);
}

static char* dlError() {
	return dlerror();
}

typedef const char* (*coreFullVersion)();

static const char* callCoreFullVersion(coreFullVersion f) {
    return f();
}
*/
import "C"
import (
	_ "embed"
	"errors"
	"fmt"
	"unsafe"
)

type posixMiniCore struct {
	// coreFullVersionFunc is the function pointer to the Rust function that returns the version string.
	coreFullVersionFunc C.coreFullVersion
	// fullVersion holds the version string returned from Rust, just to not invoke it multiple times.
	fullVersion string
	// coreInitError holds any error that occurred during initialization.
	coreInitError error
}

func newPosixMiniCore(coreFullVersionFunc C.coreFullVersion, libPath string) *posixMiniCore {
	nmc := &posixMiniCore{}
	nmc.coreFullVersionFunc = coreFullVersionFunc
	nmc.fullVersion = C.GoString(C.callCoreFullVersion(nmc.coreFullVersionFunc))
	if nmc.fullVersion == "" {
		nmc.coreInitError = newMiniCoreError(miniCoreErrorTypeCall, "posix", libPath, errors.New("failed to get version from core library function"))
	}
	return nmc
}

func (pmc *posixMiniCore) FullVersion() (string, error) {
	return pmc.fullVersion, pmc.coreInitError
}

func (l *miniCoreLoaderType) loadFromPath(libPath string) miniCore {
	logger.Debugf("Loading minicore library from: %s", libPath)
	cLibPath := C.CString(libPath)
	defer C.free(unsafe.Pointer(cLibPath))
	handle := C.dlOpen(cLibPath)
	if handle == nil {
		err := C.dlError()
		mcErr := newMiniCoreError(miniCoreErrorTypeLoad, "posix", libPath, fmt.Errorf("failed to load shared library: %v", C.GoString(err)))
		return newErroredMiniCore(mcErr)
	}
	symbolName := C.CString("sf_core_full_version")
	defer C.free(unsafe.Pointer(symbolName))
	coreFullVersionSymbol := C.dlSym(handle, symbolName)
	if coreFullVersionSymbol == nil {
		err := C.dlError()
		mcErr := newMiniCoreError(miniCoreErrorTypeSymbol, "posix", libPath, fmt.Errorf("symbol 'sf_core_full_version' not found: %v", C.GoString(err)))
		return newErroredMiniCore(mcErr)
	}
	var coreFullVersion C.coreFullVersion = (C.coreFullVersion)(coreFullVersionSymbol)
	nmc := newPosixMiniCore(coreFullVersion, libPath)
	return nmc
}
