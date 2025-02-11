// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/99designs/keyring"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type tokenType string

const (
	idToken  tokenType = "ID_TOKEN"
	mfaToken tokenType = "MFATOKEN"
)

const (
	driverName        = "SNOWFLAKE-GO-DRIVER"
	credCacheDirEnv   = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR"
	credCacheFileName = "temporary_credential.json"
)

type secureTokenSpec struct {
	host, user string
	tokenType  tokenType
}

func (t *secureTokenSpec) buildKey() string {
	return buildCredentialsKey(t.host, t.user, t.tokenType)
}

func newMfaTokenSpec(host, user string) *secureTokenSpec {
	return &secureTokenSpec{
		host,
		user,
		mfaToken,
	}
}

func newIDTokenSpec(host, user string) *secureTokenSpec {
	return &secureTokenSpec{
		host,
		user,
		idToken,
	}
}

type secureStorageManager interface {
	setCredential(tokenSpec *secureTokenSpec, value string)
	getCredential(tokenSpec *secureTokenSpec) string
	deleteCredential(tokenSpec *secureTokenSpec)
}

var credentialsStorage = newSecureStorageManager()

func newSecureStorageManager() secureStorageManager {
	switch runtime.GOOS {
	case "linux":
		ssm, err := newFileBasedSecureStorageManager()
		if err != nil {
			logger.Debugf("failed to create credentials cache dir. %v", err)
			return newNoopSecureStorageManager()
		}
		return ssm
	case "darwin", "windows":
		return newKeyringBasedSecureStorageManager()
	default:
		return newNoopSecureStorageManager()
	}
}

type fileBasedSecureStorageManager struct {
	credDirPath string
}

func newFileBasedSecureStorageManager() (*fileBasedSecureStorageManager, error) {
	credDirPath := buildCredCacheDirPath()
	if credDirPath == "" {
		return nil, fmt.Errorf("failed to build cache dir path")
	}
	ssm := &fileBasedSecureStorageManager{
		credDirPath: credDirPath,
	}
	return ssm, nil
}

func (ssm *fileBasedSecureStorageManager) createCacheDir(credCacheDir string) error {
	_, err := os.Stat(credCacheDir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(credCacheDir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create cache directory. %v, err: %v", credCacheDir, err)
		}
		return nil
	}
	return err
}

func lookupCacheDir(envVar string, pathSegments ...string) (string, error) {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return "", fmt.Errorf("environment variable %s not set", envVar)
	}

	fileInfo, err := os.Stat(envVal)
	if err != nil {
		return "", fmt.Errorf("failed to stat %s=%s, due to %w", envVar, envVal, err)
	}

	if !fileInfo.IsDir() {
		return "", fmt.Errorf("environment variable %s=%s is not a directory", envVar, envVal)
	}

	cacheDir := envVal

	if len(pathSegments) > 0 {
		for _, pathSegment := range pathSegments {
			err := os.Mkdir(pathSegment, os.ModePerm)
			if err != nil {
				return "", fmt.Errorf("failed to create cache directory. %v, err: %w", pathSegment, err)
			}
			cacheDir = filepath.Join(cacheDir, pathSegment)
		}
		fileInfo, err = os.Stat(cacheDir)
		if err != nil {
			return "", fmt.Errorf("failed to stat %s=%s, due to %w", envVar, cacheDir, err)
		}
	}

	if fileInfo.Mode().Perm() != 0o700 {
		err := os.Chmod(cacheDir, 0o700)
		if err != nil {
			return "", fmt.Errorf("failed to chmod cache directory. %v, err: %w", cacheDir, err)
		}
	}

	return cacheDir, nil
}

func buildCredCacheDirPath() string {
	type cacheDirConf struct {
		envVar       string
		pathSegments []string
	}
	confs := []cacheDirConf{
		{envVar: credCacheDirEnv, pathSegments: []string{}},
		{envVar: "XDG_CACHE_DIR", pathSegments: []string{"snowflake"}},
		{envVar: "HOME", pathSegments: []string{".cache", "snowflake"}},
	}
	for _, conf := range confs {
		path, err := lookupCacheDir(conf.envVar, conf.pathSegments...)
		if err != nil {
			logger.Debugf("Skipping %s in cache directory lookup due to %w", conf.envVar, err)
		} else {
			logger.Infof("Using %s as cache directory", path)
			return path
		}
	}

	return ""
}

func (ssm *fileBasedSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	credentialsKey := tokenSpec.buildKey()
	err := ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	credCache, err := ssm.readTemporaryCacheFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to read cache. %v", err)
		return
	}

	credCache["tokens"][credentialsKey] = value

	err = ssm.writeTemporaryCacheFile(credCache)
	if err != nil {
		logger.Warnf("Set credential failed. Unable to write cache. %v", err)
		return
	}

	return
}

func (ssm *fileBasedSecureStorageManager) lockPath() string {
	return filepath.Join(ssm.credDirPath, credCacheFileName+".lck")
}

func (ssm *fileBasedSecureStorageManager) lockFile() error {
	const NUM_RETRIES = 10
	const RETRY_INTERVAL = 100 * time.Millisecond
	lockPath := ssm.lockPath()
	locked := false
	for i := 0; i < NUM_RETRIES; i++ {
		err := os.Mkdir(lockPath, 0o700)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				time.Sleep(RETRY_INTERVAL)
				continue
			}
			return fmt.Errorf("failed to create cache lock: %v, err: %v", lockPath, err)
		}
		locked = true
	}

	if !locked {
		logger.Warnf("failed to lock cache lock. lockPath: %v.", lockPath)
		var stat unix.Stat_t
		err := unix.Stat(lockPath, &stat)
		if err != nil {
			return fmt.Errorf("failed to stat %v and determine if lock is stale. err: %v", lockPath, err)
		}

		if stat.Ctim.Nano()+time.Second.Nanoseconds() < time.Now().UnixNano() {
			err := os.Remove(lockPath)
			if err != nil {
				return fmt.Errorf("failed to remove %v while trying to remove stale lock. err: %v", lockPath, err)
			}
			err = os.Mkdir(lockPath, 0o700)
			if err != nil {
				return fmt.Errorf("failed to recreate cache lock after removing stale lock. %v, err: %v", lockPath, err)
			}
		}
		return fmt.Errorf("failed to lock cache lock %v", lockPath)
	}
	return nil
}

func (ssm *fileBasedSecureStorageManager) unlockFile() {
	lockPath := ssm.lockPath()
	err := os.Remove(lockPath)
	if err != nil {
		logger.Warnf("Failed to unlock cache lock: %v. %v", lockPath, err)
	}
}

func (ssm *fileBasedSecureStorageManager) getCredential(tokenSpec *secureTokenSpec) string {
	credentialsKey := tokenSpec.buildKey()
	credCache := map[string]map[string]string{}

	err := ssm.lockFile()
	if err != nil {
		logger.Warn("Failed to lock credential cache file.")
		return ""
	}

	credCache, err = ssm.readTemporaryCacheFile()
	ssm.unlockFile()
	if err != nil {
		logger.Warnf("Failed to read temporary cache file. %v.\n", err)
		return ""
	}

	cred := credCache["tokens"][credentialsKey]
	if cred != "" {
		logger.Debug("Successfully read token. Returning as string")
	} else {
		logger.Debug("Returned credential is empty")
	}

	return cred
}

func (ssm *fileBasedSecureStorageManager) credFilePath() string {
	return filepath.Join(ssm.credDirPath, credCacheFileName)
}

func (ssm *fileBasedSecureStorageManager) ensurePermissions() error {
	dirInfo, err := os.Stat(ssm.credDirPath)
	if err != nil {
		return err
	}

	if dirInfo.Mode().Perm() != 0o700 {
		return fmt.Errorf("incorrect permissions(%o, expected 700) for %s.", dirInfo.Mode().Perm(), ssm.credDirPath)
	}

	fileInfo, err := os.Stat(ssm.credFilePath())
	if err != nil {
		return err
	}

	if fileInfo.Mode().Perm() != 0o600 {
		logger.Debugf("Incorrect permissions(%o, expected 600) for credential file.", fileInfo.Mode().Perm())
		err := os.Chmod(ssm.credFilePath(), 0o600)
		if err != nil {
			return fmt.Errorf("Failed to chmod credential file: %v", err)
		}
		logger.Debug("Successfully fixed credential file permissions.")
	}

	return nil
}

func (ssm *fileBasedSecureStorageManager) readTemporaryCacheFile() (map[string]map[string]string, error) {
	err := ssm.ensurePermissions()
	if err != nil {
		return nil, err
	}

	jsonData, err := os.ReadFile(ssm.credFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to read credential cache file: %w", err)
	}

	credentialsMap := map[string]map[string]string{}
	err = json.Unmarshal([]byte(jsonData), &credentialsMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal credential cache file: %w", err)
	}

	return credentialsMap, nil
}

func (ssm *fileBasedSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	credentialsKey := tokenSpec.buildKey()
	err := ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	credCache, err := ssm.readTemporaryCacheFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to read cache. %v", err)
		return
	}

	delete(credCache["tokens"], credentialsKey)

	err = ssm.writeTemporaryCacheFile(credCache)
	if err != nil {
		logger.Warnf("Set credential failed. Unable to write cache. %v", err)
		return
	}

	return
}

func (ssm *fileBasedSecureStorageManager) writeTemporaryCacheFile(cache map[string]map[string]string) error {
	bytes, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to marshal credential cache map. %w", err)
	}

	err = os.WriteFile(ssm.credFilePath(), bytes, 0600)
	if err != nil {
		return fmt.Errorf("failed to write the credential cache file: %w", err)
	}
	return nil
}

type keyringSecureStorageManager struct {
}

func newKeyringBasedSecureStorageManager() *keyringSecureStorageManager {
	return &keyringSecureStorageManager{}
}

func (ssm *keyringSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	if value == "" {
		logger.Debug("no token provided")
	} else {
		credentialsKey := tokenSpec.buildKey()
		if runtime.GOOS == "windows" {
			ring, _ := keyring.Open(keyring.Config{
				WinCredPrefix: strings.ToUpper(tokenSpec.host),
				ServiceName:   strings.ToUpper(tokenSpec.user),
			})
			item := keyring.Item{
				Key:  credentialsKey,
				Data: []byte(value),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to Windows credential manager. Err: %v", err)
			}
		} else if runtime.GOOS == "darwin" {
			ring, _ := keyring.Open(keyring.Config{
				ServiceName: credentialsKey,
			})
			account := strings.ToUpper(tokenSpec.user)
			item := keyring.Item{
				Key:  account,
				Data: []byte(value),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to keychain. Err: %v", err)
			}
		}
	}
}

func (ssm *keyringSecureStorageManager) getCredential(tokenSpec *secureTokenSpec) string {
	cred := ""
	credentialsKey := tokenSpec.buildKey()
	if runtime.GOOS == "windows" {
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: strings.ToUpper(tokenSpec.host),
			ServiceName:   strings.ToUpper(tokenSpec.user),
		})
		i, err := ring.Get(credentialsKey)
		if err != nil {
			logger.Debugf("Failed to read credentialsKey or could not find it in Windows Credential Manager. Error: %v", err)
		}
		cred = string(i.Data)
	} else if runtime.GOOS == "darwin" {
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: credentialsKey,
		})
		account := strings.ToUpper(tokenSpec.user)
		i, err := ring.Get(account)
		if err != nil {
			logger.Debugf("Failed to find the item in keychain or item does not exist. Error: %v", err)
		}
		cred = string(i.Data)
		if cred == "" {
			logger.Debug("Returned credential is empty")
		} else {
			logger.Debug("Successfully read token. Returning as string")
		}
	}
	return cred
}

func (ssm *keyringSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	credentialsKey := tokenSpec.buildKey()
	if runtime.GOOS == "windows" {
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: strings.ToUpper(tokenSpec.host),
			ServiceName:   strings.ToUpper(tokenSpec.user),
		})
		err := ring.Remove(string(credentialsKey))
		if err != nil {
			logger.Debugf("Failed to delete credentialsKey in Windows Credential Manager. Error: %v", err)
		}
	} else if runtime.GOOS == "darwin" {
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: credentialsKey,
		})
		account := strings.ToUpper(tokenSpec.user)
		err := ring.Remove(account)
		if err != nil {
			logger.Debugf("Failed to delete credentialsKey in keychain. Error: %v", err)
		}
	}
}

func buildCredentialsKey(host, user string, credType tokenType) string {
	host = strings.ToUpper(host)
	user = strings.ToUpper(user)
	credTypeStr := strings.ToUpper(string(credType))
	return host + ":" + user + ":" + credTypeStr
}

type noopSecureStorageManager struct {
}

func newNoopSecureStorageManager() *noopSecureStorageManager {
	return &noopSecureStorageManager{}
}

func (ssm *noopSecureStorageManager) setCredential(_ *secureTokenSpec, _ string) {
}

func (ssm *noopSecureStorageManager) getCredential(_ *secureTokenSpec) string {
	return ""
}

func (ssm *noopSecureStorageManager) deleteCredential(_ *secureTokenSpec) {
}
