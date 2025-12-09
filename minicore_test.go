package gosnowflake

import (
	"database/sql"
	"os"
	"testing"
)

func TestMiniCoreLoadSuccess(t *testing.T) {
	mcl := newMiniCoreLoader()
	checkLoadCore(t, mcl)
}

func checkLoadCore(t *testing.T, mcl *miniCoreLoaderType) {
	core := mcl.loadCore()
	assertNotNilF(t, core)
	fullVersion, err := core.FullVersion()
	assertNilF(t, err)
	assertEqualE(t, fullVersion, "0.0.1")
}

func TestMiniCoreLoaderChoosesCorrectCandidates(t *testing.T) {
	skipOnMissingHome(t)
	assertNilF(t, os.RemoveAll(getMiniCoreCacheDirInHome()))
	mcl := newMiniCoreLoader()
	checkAllLoadDirsAvailable(t, mcl)
}

func TestMiniCoreLoaderChoosesCorrectCandidatesWhenHomeCacheDirAlreadyExists(t *testing.T) {
	skipOnMissingHome(t)
	mcl := newMiniCoreLoader()
	checkAllLoadDirsAvailable(t, mcl)
	mcl = newMiniCoreLoader()
	checkAllLoadDirsAvailable(t, mcl)
}

func checkAllLoadDirsAvailable(t *testing.T, mcl *miniCoreLoaderType) {
	assertEqualF(t, len(mcl.searchDirs), 3)
	assertEqualE(t, mcl.searchDirs[0].dirType, "temp")
	assertEqualE(t, mcl.searchDirs[1].dirType, "home")
	assertEqualE(t, mcl.searchDirs[2].dirType, "cwd")
}

func TestMiniCoreNoFolderCandidate(t *testing.T) {
	mcl := newMiniCoreLoader()
	mcl.searchDirs = []minicoreDirCandidate{}
	core := mcl.loadCore()
	version, err := core.FullVersion()
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "failed to write embedded library to any directory")
	assertEqualE(t, version, "")
}

func TestMiniCoreNoWritableFolder(t *testing.T) {
	skipOnWindows(t, "permission system is different")
	tempDir := t.TempDir()
	err := os.Chmod(tempDir, 0000)
	assertNilF(t, err)
	defer os.Chmod(tempDir, 0700)
	mcl := newMiniCoreLoader()
	mcl.searchDirs = []minicoreDirCandidate{newMinicoreDirCandidate("test", tempDir)}
	core := mcl.loadCore()
	assertNotNilF(t, core)
	_, err = core.FullVersion()
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "failed to write embedded library to any directory")
}

func TestMiniCoreNoWritableFirstFolder(t *testing.T) {
	tempDir := t.TempDir()
	err := os.Chmod(tempDir, 0000)
	defer os.Chmod(tempDir, 0700)
	tempDir2 := t.TempDir()
	assertNilF(t, err)
	mcl := newMiniCoreLoader()
	mcl.searchDirs = []minicoreDirCandidate{newMinicoreDirCandidate("test", tempDir), newMinicoreDirCandidate("test", tempDir2)}
	checkLoadCore(t, mcl)
}

func TestMiniCoreInvalidDynamicLibrary(t *testing.T) {
	origCoreLib := corePlatformConfig.coreLib
	defer func() {
		corePlatformConfig.coreLib = origCoreLib
	}()
	corePlatformConfig.coreLib = []byte("invalid content")
	mcl := newMiniCoreLoader()
	core := mcl.loadCore()
	assertNotNilF(t, core)
	_, err := core.FullVersion()
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "failed to load shared library")
}

func TestMiniCoreNotInitialized(t *testing.T) {
	defer func() {
		corePlatformConfig.initialized = true
	}()
	corePlatformConfig.initialized = false
	mcl := newMiniCoreLoader()
	core := mcl.loadCore()
	assertNotNilF(t, core)
	_, err := core.FullVersion()
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "minicore is not supported on")
}

func TestMiniCoreLoadedE2E(t *testing.T) {
	wiremock.registerMappings(t, newWiremockMapping("minicore/auth/successful_flow.json"), newWiremockMapping("select1.json"))
	cfg := wiremock.connectionConfig()
	connector := NewConnector(SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	runSmokeQuery(t, db)
}
