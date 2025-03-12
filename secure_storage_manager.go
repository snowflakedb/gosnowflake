// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/99designs/keyring"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

func (t *secureTokenSpec) buildKey() (string, error) {
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
		return &threadSafeSecureStorageManager{&sync.Mutex{}, ssm}
	case "darwin", "windows":
		return &threadSafeSecureStorageManager{&sync.Mutex{}, newKeyringBasedSecureStorageManager()}
	default:
		logger.Warnf("OS %v does not support credentials cache", runtime.GOOS)
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

	if err = os.Chmod(cacheDir, os.FileMode(0700)); err != nil {
		return "", err
	}

	return cacheDir, nil
}

func buildCredCacheDirPath(confs []cacheDirConf) (string, error) {
	for _, conf := range confs {
		path, err := lookupCacheDir(conf.envVar, conf.pathSegments...)
		if err != nil {
			logger.Debugf("Skipping %s in cache directory lookup due to %v", conf.envVar, err)
		} else {
			logger.Debugf("Using %s as cache directory", path)
			return path, nil
		}
	}

	return "", errors.New("no credentials cache directory found")
}

func (ssm *fileBasedSecureStorageManager) getTokens(data map[string]any) map[string]interface{} {
	val, ok := data["tokens"]
	if !ok {
		return map[string]interface{}{}
	}

	tokens, ok := val.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}

	return tokens
}

func (ssm *fileBasedSecureStorageManager) withCacheFile(action func(*os.File)) {
	cacheFile, err := os.OpenFile(ssm.credFilePath(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.Warn("cannot access %v. %v", ssm.credFilePath(), err)
		return
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Warnf("cannot release file descriptor for %v. %v", ssm.credFilePath(), err)
		}
	}(cacheFile)
	action(cacheFile)
}

func (ssm *fileBasedSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warn(err)
		return
	}
	err = ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	ssm.withCacheFile(func(cacheFile *os.File) {
		credCache, err := ssm.readTemporaryCacheFile(cacheFile)
		if err != nil {
			logger.Warnf("Error while reading cache file. %v", err)
			return
		}
		tokens := ssm.getTokens(credCache)
		tokens[credentialsKey] = value
		credCache["tokens"] = tokens
		err = ssm.writeTemporaryCacheFile(credCache, cacheFile)
		if err != nil {
			logger.Warnf("Set credential failed. Unable to write cache. %v", err)
		}
	})
}

func (ssm *fileBasedSecureStorageManager) lockPath() string {
	return filepath.Join(ssm.credDirPath, credCacheFileName+".lck")
}

func (ssm *fileBasedSecureStorageManager) lockFile() error {
	const numRetries = 10
	const retryInterval = 100 * time.Millisecond
	lockPath := ssm.lockPath()

	fileInfo, err := os.Stat(lockPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat %v and determine if lock is stale. err: %v", lockPath, err)
	}

	// removing stale lock
	now := time.Now()
	if !errors.Is(err, os.ErrNotExist) && fileInfo.ModTime().Add(time.Second).UnixNano() < now.UnixNano() {
		logger.Debugf("removing credentials cache lock file, stale for %vms", (now.UnixNano()-fileInfo.ModTime().UnixNano())/1000/1000)
		err = os.Remove(lockPath)
		if err != nil {
			return fmt.Errorf("failed to remove %v while trying to remove stale lock. err: %v", lockPath, err)
		}
	}

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
		return fmt.Errorf("failed to lock cache. lockPath: %v", lockPath)
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
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warn(err)
		return ""
	}
	err = ssm.lockFile()
	if err != nil {
		logger.Warnf("Failed to lock credential cache file. %v", err)
		return ""
	}
	defer ssm.unlockFile()

	ret := ""
	ssm.withCacheFile(func(cacheFile *os.File) {
		credCache, err := ssm.readTemporaryCacheFile(cacheFile)
		if err != nil {
			logger.Warnf("Error while reading cache file. %v", err)
			return
		}
		cred, ok := ssm.getTokens(credCache)[credentialsKey]
		if !ok {
			return
		}

		credStr, ok := cred.(string)
		if !ok {
			return
		}

		ret = credStr
	})
	return ret
}

func (ssm *fileBasedSecureStorageManager) credFilePath() string {
	return filepath.Join(ssm.credDirPath, credCacheFileName)
}

func (ssm *fileBasedSecureStorageManager) ensurePermissions(cacheFile *os.File) error {
	dirInfo, err := os.Stat(ssm.credDirPath)
	if err != nil {
		return err
	}

	if dirInfo.Mode().Perm() != 0o700&os.ModePerm {
		return fmt.Errorf("incorrect permissions(%o, expected 700) for %s", dirInfo.Mode().Perm(), ssm.credDirPath)
	}

	fileInfo, err := cacheFile.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Mode().Perm() != 0o600&os.ModePerm {
		return fmt.Errorf("incorrect permissions(%v, expected 600) for credential file", fileInfo.Mode().Perm())
	}

	return nil
}

func (ssm *fileBasedSecureStorageManager) ensureOwnerForDir(filePath string) error {
	ownerUID, err := providePathOwner(filePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return ssm.ensureOwner(ownerUID)
}

func (ssm *fileBasedSecureStorageManager) ensureOwnerForFile(file *os.File) error {
	ownerUID, err := provideFileOwner(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return ssm.ensureOwner(ownerUID)
}

func (ssm *fileBasedSecureStorageManager) ensureOwner(ownerId uint32) error {
	currentUser, err := user.Current()
	if err != nil {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if strconv.Itoa(int(ownerId)) != currentUser.Uid {
		return errors.New("incorrect owner of " + ssm.credDirPath)
	}
	return nil
}

func (ssm *fileBasedSecureStorageManager) readTemporaryCacheFile(cacheFile *os.File) (map[string]any, error) {
	if err := ssm.ensurePermissions(cacheFile); err != nil {
		return map[string]any{}, fmt.Errorf("failed to ensure permission for temporary cache file. %v", err)
	}
	if err := ssm.ensureOwnerForDir(ssm.credDirPath); err != nil {
		return map[string]any{}, fmt.Errorf("failed to ensure owner for %v. %v", ssm.credDirPath, err)
	}
	if err := ssm.ensureOwnerForFile(cacheFile); err != nil {
		return map[string]any{}, fmt.Errorf("failed to ensure owner for %v. %v", ssm.credFilePath(), err)
	}

	jsonData, err := io.ReadAll(cacheFile)
	if err != nil {
		logger.Warnf("Failed to read credential cache file. %v.\n", err)
		return map[string]any{}, nil
	}
	if _, err = cacheFile.Seek(0, 0); err != nil {
		return map[string]any{}, fmt.Errorf("cannot seek to the beginning of a cache file. %v", err)
	}

	if len(jsonData) == 0 {
		// Happens when the file didn't exist before.
		return map[string]any{}, nil
	}

	credentialsMap := map[string]any{}
	err = json.Unmarshal(jsonData, &credentialsMap)
	if err != nil {
		return map[string]any{}, fmt.Errorf("Failed to unmarshal credential cache file. %v.\n", err)
	}

	return credentialsMap, nil
}

func (ssm *fileBasedSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warn(err)
		return
	}
	err = ssm.lockFile()
	if err != nil {
		logger.Warnf("Set credential failed. Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	ssm.withCacheFile(func(cacheFile *os.File) {
		credCache, err := ssm.readTemporaryCacheFile(cacheFile)
		if err != nil {
			logger.Warnf("Error while reading cache file. %v", err)
			return
		}
		delete(ssm.getTokens(credCache), credentialsKey)

		err = ssm.writeTemporaryCacheFile(credCache, cacheFile)
		if err != nil {
			logger.Warnf("Set credential failed. Unable to write cache. %v", err)
		}
	})
}

func (ssm *fileBasedSecureStorageManager) writeTemporaryCacheFile(cache map[string]any, cacheFile *os.File) error {
	if err := ssm.ensureOwnerForDir(ssm.credDirPath); err != nil {
		return err
	}

	bytes, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to marshal credential cache map. %w", err)
	}

	if err = cacheFile.Truncate(0); err != nil {
		return fmt.Errorf("error while truncating credentials cache. %v", err)
	}
	_, err = cacheFile.Write(bytes)
	if err != nil {
		return fmt.Errorf("failed to write the credential cache file: %w", err)
	}
	cacheFile.Seek(0, 0)
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
		credentialsKey, err := tokenSpec.buildKey()
		if err != nil {
			logger.Warn(err)
			return
		}
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
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warn(err)
		return ""
	}
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
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warn(err)
		return
	}
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

func buildCredentialsKey(host, user string, credType tokenType) (string, error) {
	if host == "" {
		return "", errors.New("host is not provided to store in token cache, skipping")
	}
	if user == "" {
		return "", errors.New("user is not provided to store in token cache, skipping")
	}
	plainCredKey := host + ":" + user + ":" + string(credType)
	checksum := sha256.New()
	checksum.Write([]byte(plainCredKey))
	return hex.EncodeToString(checksum.Sum(nil)), nil
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

type threadSafeSecureStorageManager struct {
	mu       *sync.Mutex
	delegate secureStorageManager
}

func (ssm *threadSafeSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.delegate.setCredential(tokenSpec, value)
}

func (ssm *threadSafeSecureStorageManager) getCredential(tokenSpec *secureTokenSpec) string {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	return ssm.delegate.getCredential(tokenSpec)
}

func (ssm *threadSafeSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.delegate.deleteCredential(tokenSpec)
}
