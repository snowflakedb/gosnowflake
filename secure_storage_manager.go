// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/99designs/keyring"
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
	credCacheDirEnv   = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR"
	credCacheFileName = "credential_cache_v1.json"
)

type cacheDirConf struct {
	envVar       string
	pathSegments []string
}

var defaultLinuxCacheDirConf = []cacheDirConf{
	{envVar: credCacheDirEnv, pathSegments: []string{}},
	{envVar: "XDG_CACHE_DIR", pathSegments: []string{"snowflake"}},
	{envVar: "HOME", pathSegments: []string{".cache", "snowflake"}},
}

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
		logger.Infof("OS %v does not support credentials cache", runtime.GOOS)
		return newNoopSecureStorageManager()
	}
}

type fileBasedSecureStorageManager struct {
	credDirPath string
}

func newFileBasedSecureStorageManager() (*fileBasedSecureStorageManager, error) {
	credDirPath, err := buildCredCacheDirPath(defaultLinuxCacheDirConf)
	if err != nil {
		return nil, err
	}
	ssm := &fileBasedSecureStorageManager{
		credDirPath: credDirPath,
	}
	return ssm, nil
}

func lookupCacheDir(envVar string, pathSegments ...string) (string, error) {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return "", fmt.Errorf("environment variable %s not set", envVar)
	}

	fileInfo, err := os.Stat(envVal)
	if err != nil {
		return "", fmt.Errorf("failed to stat %s=%s, due to %v", envVar, envVal, err)
	}

	if !fileInfo.IsDir() {
		return "", fmt.Errorf("environment variable %s=%s is not a directory", envVar, envVal)
	}

	cacheDir := filepath.Join(envVal, filepath.Join(pathSegments...))

	if err = os.MkdirAll(cacheDir, os.FileMode(0o755)); err != nil {
		return "", err
	}
	fileInfo, err = os.Stat(cacheDir)
	if err != nil {
		return "", fmt.Errorf("failed to stat %s=%s, due to %w", envVar, cacheDir, err)
	}

	if fileInfo.Mode().Perm() != 0o700 {
		err := os.Chmod(cacheDir, 0o700)
		if err != nil {
			return "", fmt.Errorf("failed to chmod cache directory. %v, err: %w", cacheDir, err)
		}
	}

	return cacheDir, nil
}

func buildCredCacheDirPath(confs []cacheDirConf) (string, error) {
	for _, conf := range confs {
		path, err := lookupCacheDir(conf.envVar, conf.pathSegments...)
		if err != nil {
			logger.Debugf("Skipping %s in cache directory lookup due to %v", conf.envVar, err)
		} else {
			logger.Infof("Using %s as cache directory", path)
			return path, nil
		}
	}

	return "", errors.New("no credentials cache directory found")
}

func (ssm *fileBasedSecureStorageManager) getTokens(data map[string]any) map[string]interface{} {
	val, ok := data["tokens"]
	emptyMap := map[string]interface{}{}
	if !ok {
		data["tokens"] = emptyMap
		return emptyMap
	}

	tokens, ok := val.(map[string]interface{})
	if !ok {
		data["tokens"] = emptyMap
		return emptyMap
	}

	return tokens
}

func (ssm *fileBasedSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	credentialsKey := tokenSpec.buildKey()
	err := ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	credCache := ssm.readTemporaryCacheFile()
	ssm.getTokens(credCache)[credentialsKey] = value

	err = ssm.writeTemporaryCacheFile(credCache)
	if err != nil {
		logger.Warnf("Set credential failed. Unable to write cache. %v", err)
	}
}

func (ssm *fileBasedSecureStorageManager) lockPath() string {
	return filepath.Join(ssm.credDirPath, credCacheFileName+".lck")
}

func (ssm *fileBasedSecureStorageManager) lockFile() error {
	const numRetries = 10
	const retryInterval = 100 * time.Millisecond
	lockPath := ssm.lockPath()
	locked := false
	for i := 0; i < numRetries; i++ {
		err := os.Mkdir(lockPath, 0o700)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				time.Sleep(retryInterval)
				continue
			}
			return fmt.Errorf("failed to create cache lock: %v, err: %v", lockPath, err)
		}
		locked = true
		break
	}

	if !locked {
		logger.Warnf("failed to lock cache lock. lockPath: %v.", lockPath)
		fileInfo, err := os.Stat(lockPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to stat %v and determine if lock is stale. err: %v", lockPath, err)
		}

		if fileInfo.ModTime().Add(time.Second).UnixNano() < time.Now().UnixNano() {
			logger.Debugf("removing credentials cache lock file, stale for %v", time.Now().UnixNano()-fileInfo.ModTime().UnixNano())
			err := os.Remove(lockPath)
			if err != nil {
				return fmt.Errorf("failed to remove %v while trying to remove stale lock. err: %v", lockPath, err)
			}
			err = os.Mkdir(lockPath, 0o700)
			if err != nil {
				return fmt.Errorf("failed to recreate cache lock after removing stale lock. %v, err: %v", lockPath, err)
			}
		}
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
	err := ssm.lockFile()
	if err != nil {
		logger.Warn("Failed to lock credential cache file.")
		return ""
	}

	credCache := ssm.readTemporaryCacheFile()
	ssm.unlockFile()
	cred, ok := ssm.getTokens(credCache)[credentialsKey]
	if !ok {
		return ""
	}

	credStr, ok := cred.(string)
	if !ok {
		return ""
	}

	return credStr
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
		return fmt.Errorf("incorrect permissions(%o, expected 700) for %s", dirInfo.Mode().Perm(), ssm.credDirPath)
	}

	fileInfo, err := os.Stat(ssm.credFilePath())
	if err != nil {
		return err
	}

	if fileInfo.Mode().Perm() != 0o600 {
		logger.Debugf("Incorrect permissions(%o, expected 600) for credential file.", fileInfo.Mode().Perm())
		err := os.Chmod(ssm.credFilePath(), 0o600)
		if err != nil {
			return fmt.Errorf("failed to chmod credential file: %v", err)
		}
		logger.Debug("Successfully fixed credential file permissions.")
	}

	return nil
}

func (ssm *fileBasedSecureStorageManager) readTemporaryCacheFile() map[string]any {
	err := ssm.ensurePermissions()
	if err != nil {
		logger.Warnf("Failed to ensure permission for temporary cache file. %v.\n", err)
		return map[string]any{}
	}

	jsonData, err := os.ReadFile(ssm.credFilePath())
	if err != nil {
		logger.Warnf("Failed to read credential cache file. %v.\n", err)
		return map[string]any{}
	}

	credentialsMap := map[string]any{}
	err = json.Unmarshal(jsonData, &credentialsMap)
	if err != nil {
		logger.Warnf("Failed to unmarshal credential cache file. %v.\n", err)
	}

	return credentialsMap
}

func (ssm *fileBasedSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	credentialsKey := tokenSpec.buildKey()
	err := ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	credCache := ssm.readTemporaryCacheFile()
	delete(ssm.getTokens(credCache), credentialsKey)

	err = ssm.writeTemporaryCacheFile(credCache)
	if err != nil {
		logger.Warnf("Set credential failed. Unable to write cache. %v", err)
	}
}

func (ssm *fileBasedSecureStorageManager) writeTemporaryCacheFile(cache map[string]any) error {
	bytes, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to marshal credential cache map. %w", err)
	}

	stat, err := os.Stat(ssm.credFilePath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if stat.Mode() != 0600 {
			if err = os.Chmod(ssm.credFilePath(), 0600); err != nil {
				return fmt.Errorf("cannot chmod file %v to 600. %v", ssm.credFilePath(), err)
			}
		}
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
	plainCredKey := host + ":" + user + ":" + string(credType)
	checksum := sha256.New()
	checksum.Write([]byte(plainCredKey))
	return hex.EncodeToString(checksum.Sum(nil))
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
