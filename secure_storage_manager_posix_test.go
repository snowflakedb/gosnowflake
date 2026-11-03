//go:build !windows

package gosnowflake

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFileBasedCacheRejectsUnsafePaths(t *testing.T) {
	newManager := func(t *testing.T) *fileBasedSecureStorageManager {
		credCacheDir := t.TempDir()
		assertNilF(t, os.Chmod(credCacheDir, 0700))
		dirEnv := overrideEnv(credCacheDirEnv, credCacheDir)
		t.Cleanup(dirEnv.rollback)
		nameEnv := overrideEnv(credCacheFileNameEnv, "")
		t.Cleanup(nameEnv.rollback)
		ssm, err := newFileBasedSecureStorageManager()
		assertNilF(t, err)
		return ssm
	}
	tokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "johndoe"})

	t.Run("does not create the lock file when the cache dir has wrong permission", func(t *testing.T) {
		ssm := newManager(t)
		assertNilF(t, os.Chmod(ssm.credDirPath, 0777))
		defer func() {
			assertNilE(t, os.Chmod(ssm.credDirPath, 0700))
		}()
		ssm.setCredential(tokenSpec, "token")
		_, err := os.Stat(ssm.lockPath)
		assertTrueE(t, os.IsNotExist(err))
		_, err = os.Stat(ssm.credFilePath)
		assertTrueE(t, os.IsNotExist(err))
	})

	t.Run("refuses a symlink at the cache path", func(t *testing.T) {
		ssm := newManager(t)
		target := filepath.Join(t.TempDir(), "target.json")
		assertNilF(t, os.WriteFile(target, []byte("{}"), 0600))
		assertNilF(t, os.Symlink(target, ssm.credFilePath))
		ssm.setCredential(tokenSpec, "token")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		content, err := os.ReadFile(target)
		assertNilF(t, err)
		assertEqualE(t, string(content), "{}")
	})

	t.Run("refuses a symlink at the lock path", func(t *testing.T) {
		ssm := newManager(t)
		target := filepath.Join(t.TempDir(), "target.lck")
		assertNilF(t, os.WriteFile(target, nil, 0600))
		assertNilF(t, os.Symlink(target, ssm.lockPath))
		ssm.setCredential(tokenSpec, "token")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		_, err := os.Stat(ssm.credFilePath)
		assertTrueE(t, os.IsNotExist(err))
	})

	t.Run("rejects a FIFO at the cache path", func(t *testing.T) {
		ssm := newManager(t)
		assertNilF(t, unix.Mkfifo(ssm.credFilePath, 0600))
		done := make(chan struct{})
		go func() {
			ssm.setCredential(tokenSpec, "token")
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("setCredential blocked on a FIFO cache path")
		}
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
	})
}
